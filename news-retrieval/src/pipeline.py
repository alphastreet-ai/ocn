"""News aggregation pipeline: fetch articles from configured sources."""
import html
import json
import logging
import os
import re
import threading
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from functools import lru_cache, partial
from typing import Any
from urllib.parse import urlparse
import feedparser
import httpx
import trafilatura
from openai import OpenAI
from trafilatura.settings import use_config

# Optional - only used as a second attempt for URLs Trafilatura cannot
# fetch (see _fetch_body_impersonated). Imported defensively so an
# environment without it keeps working exactly as before rather than
# failing to start: every call site already treats a null body as a
# normal outcome.
try:
    from curl_cffi import requests as _curl_cffi_requests
except ImportError:  # pragma: no cover - depends on the installed env
    _curl_cffi_requests = None

from china_code import resolve_cninfo_org_ids, resolve_hkex_stock_ids
from dart_corp_code import resolve_corp_codes
from models.articles import (
    append_also_reported_by,
    get_already_stored_urls,
    get_recent_gdelt_articles_for_ticker,
    iter_recent_articles_for_domain,
)
from models.sources import load_sources
from sec_edgar import _USER_AGENT as _SEC_USER_AGENT

logger = logging.getLogger(__name__)

_NEWSAPI_PAGE_SIZE = 30        # articles fetched per category per request
_SERPAPI_RESULTS_PER_QUERY = 30  # articles fetched per query from Google News
_HTML_TAG_RE = re.compile(r"<[^>]+>")
_SERPAPI_DATE_RE = re.compile(
    r"(\d{2}/\d{2}/\d{4}), (\d{1,2}:\d{2} [AP]M), \+0000 UTC"
)


def _parse_newsapi_date(date_str: str) -> datetime | None:
    """Parse a NewsAPI ISO-8601 publishedAt string to a UTC datetime."""
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

class _RateLimiter:
    """Token-bucket rate limiter, safe for concurrent threads."""

    def __init__(self, rate: float) -> None:
        """Args: rate: maximum calls per second (also the burst cap)."""
        self._rate = rate
        self._tokens = float(rate)
        self._last_refill = time.perf_counter()
        self._lock = threading.Lock()

    def acquire(self) -> None:
        """Block until a token is available."""
        while True:
            with self._lock:
                now = time.perf_counter()
                self._tokens = min(
                    self._rate,
                    self._tokens + (now - self._last_refill) * self._rate,
                )
                self._last_refill = now
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
            time.sleep(0.05)


def _make_client(api_key: str | None = None) -> OpenAI:
    """Return an OpenAI-compatible client pointed at OpenRouter."""
    return OpenAI(
        api_key=api_key or os.environ.get("OPENROUTER_API_KEY"),
        base_url="https://openrouter.ai/api/v1",
        http_client=httpx.Client(http2=False, timeout=60.0),
    )


def _clean_summary(raw: str) -> str:
    """Strip HTML tags, unescape entities, and collapse whitespace."""
    text = _HTML_TAG_RE.sub(" ", raw)
    text = html.unescape(text)
    return " ".join(text.split())


# ---------------------------------------------------------------------------
# Step 1 - fetch
# ---------------------------------------------------------------------------

# trafilatura.extract() is NOT safe to run concurrently: it parses the page
# into an lxml tree and then copy.copy()s that tree several times
# (_prepare_tree, _extract_and_compare in trafilatura/core.py). Those copies
# are libxml2 C-level operations, and running them from several threads at
# once corrupts the heap and kills the whole process - not a Python
# exception that one worker could absorb, the interpreter aborts.
#
# Confirmed live from two real faulthandler tracebacks on the
# geopolitical_news 02:00 UTC run, both with several threads inside
# trafilatura.extract() at the moment of death:
#   run 550 (2026-10-02): "Fatal Python error: Segmentation fault",
#       crashing thread in core.py bare_extraction
#   run 560 (2026-10-03): "double free or corruption (!prev)" /
#       "Fatal Python error: Aborted", crashing thread in
#       copy.py -> core.py _prepare_tree, with a SECOND thread also in
#       _prepare_tree and others inside lxml text_content()
# Both runs died ~7s in and sat as stuck 'running' rows until the watchdog
# failed them, so the domain produced nothing on either day.
#
# Different trafilatura versions on the two days (line numbers differ, and
# 2.3.0 only released 2026-10-02 16:53 UTC - after run 550 had already
# crashed), so the version bump is not the cause; concurrent extract() is.
# Fetching stays parallel - that part is I/O-bound and safe - and only the
# extraction is serialized, so the win from 10 concurrent downloads is kept.
_EXTRACT_LOCK = threading.Lock()


# A subscription wall extracts cleanly and is NOT an article. Confirmed
# live: ft.com answers curl_cffi with HTTP 200 and 146KB, from which
# trafilatura pulls 1,046 characters reading "Try unlimited access - Only
# Rs100 for 4 weeks. Then Rs4499 per month..." - a plausible-looking body
# of plausible length. The article's own words ("Beijing", "national
# security") are not in that HTML at all; the server never sent them.
#
# Storing that is worse than storing nothing: downstream would tag a
# sales page as a geopolitical signal, and no length threshold
# distinguishes it from a short real article. Matched on the offer
# vocabulary instead, and only in the first 400 characters - a real
# article may mention a subscription further down, but one that OPENS
# with an offer is the wall itself.
_PAYWALL_TEASER_MARKERS: tuple[str, ...] = (
    "subscribe to unlock",
    "try unlimited access",
    "complete digital access",
    "cancel anytime during your trial",
    "subscribe to continue",
    "to read this article",
    "already a subscriber",
    "sign in to read",
    "create a free account to",
    # Bot checks, which are not paywalls but fail the same way: a 200
    # response carrying a few hundred characters of plausible-looking
    # text that is not the article. Confirmed live on bloomberg.com -
    # the first request returned the real story, repeat requests
    # returned "To continue, please click the box below to let us know
    # you're not a robot", which no subscription-offer marker catches
    # and which is short enough to look like a brief article.
    "let us know you're not a robot",
    "please click the box below",
    "browser supports javascript",
    "enable javascript and cookies",
    "verify you are a human",
    "checking your browser before",
)
_PAYWALL_TEASER_SCAN_CHARS = 400


def _looks_like_paywall_teaser(text: str) -> bool:
    head = " ".join(text[:_PAYWALL_TEASER_SCAN_CHARS].lower().split())
    return any(m in head for m in _PAYWALL_TEASER_MARKERS)


# Domains whose fetchers opt into the curl_cffi second attempt. One entry
# today, deliberately: the blocks it works around (US/EU paper-of-record
# WAFs and TLS-fingerprint rejection) are specific to geopolitical_news's
# own source allowlist. Every other domain fetches exchange filings,
# government notices or regional press, none of which exhibited this
# failure. Kept as a set rather than an `if slug == ...` so adding the
# next domain is a one-line change with a visible list of who opted in.
_IMPERSONATE_FALLBACK_DOMAINS: frozenset[str] = frozenset({"geopolitical_news"})


def _impersonate_for(domain_slug: str) -> bool:
    return domain_slug in _IMPERSONATE_FALLBACK_DOMAINS


# Source domains whose articles are worth paying a body fetch for, per
# news domain. A body fetch is the single most expensive step in
# retrieval - a Trafilatura download plus a serialized extract (see
# _EXTRACT_LOCK for the segfaults that forced the serialization) - and
# for geopolitical_news it was being spent on articles the consumer
# discards minutes later.
#
# Measured on the stored history: signal-detection-agent's Stage A drops
# every article whose domain is not on its own allowlist, and that was
# 5,280 of 5,487 rows - so roughly 9 of every 10 body fetches produced
# text that nothing ever read. A single run fetched bodies from
# kalkine.ca, britannica.com, facebook.com and einnews.com, none of
# which can ever become a signal.
#
# The article itself is STILL FETCHED AND STORED - only the body is
# skipped. Dropping the row instead would break the consumer: its Stage
# A skips by article_id, so an article with no row is re-examined on
# every later pass forever, and a dropped article would stop being
# distinguishable from one that was never retrieved.
#


def _fetch_body_or_none(
    url: str | None, *, impersonate_fallback: bool = False,
) -> str | None:
    """_fetch_body, but a None url means "deliberately not fetched".

    Lets a caller decide per-article whether to spend a fetch while still
    mapping over one list, so the result stays positionally aligned with
    the articles it belongs to.
    """
    if not url:
        return None
    return _fetch_body(url, impersonate_fallback=impersonate_fallback)


def _should_fetch_body(url: str, domain_slug: str) -> bool:
    """Whether this article's body is worth fetching. Always True.

    There was a per-domain allowlist here (_BODY_FETCH_DOMAINS), and it
    existed only as the mirror of Stage A's own domain filter in
    signal-detection-agent: skipping the body for a publisher Stage A
    was going to discard anyway. That filter is gone, so this one had no
    remaining purpose - and keeping it would be actively harmful, since
    an article that now reaches Stage B with no body is judged on its
    headline alone, and a headline-only verdict is irreversible.

    geopolitical_news was the only domain that ever had an entry, so
    removing it changes nothing for any other caller. The function
    stays, rather than its two call sites being edited away, because
    "should this url be fetched" is a real question a future source may
    answer differently - a paywalled domain, a known-dead host - and
    this is where that would go.
    """
    return True


def _fetch_body_impersonated(url: str) -> str | None:
    """Second attempt for a URL Trafilatura could not fetch: re-request it
    with a real browser's TLS/HTTP2 fingerprint via curl_cffi.

    Some WAFs reject on the TLS handshake rather than on the User-Agent,
    so a browser UA alone (which _get_trafilatura_config already sets) is
    not enough - the connection is refused before any header is read.
    Confirmed live on apnews.com: 403 with Trafilatura's fetch AND with a
    browser UA on plain curl, but HTTP 200 and 4,616 characters of real
    article text when the TLS fingerprint matches Chrome.

    It changes how we look at the transport layer and nothing else - no
    credentials, no JS execution. A site that refuses an unauthenticated
    request still refuses it (confirmed: reuters.com answers 401 to every
    impersonation profile tried, and the Internet Archive's own crawler
    recorded the same 401).

    Also clears globalsources.com's HTTP 429. That one looked like rate
    limiting and is not: five requests spaced four seconds apart all
    returned 429 with `Retry-After: 300`, while this same call returns
    200 and 12,539 characters immediately. The 429 is CloudFront
    rejecting the TLS fingerprint, so throttling would have waited five
    minutes to be refused again.
    """
    if _curl_cffi_requests is None:
        return None
    try:
        resp = _curl_cffi_requests.get(
            url, impersonate="chrome124", timeout=20,
        )
        if resp.status_code == 429:
            # The server told us how long to wait. Honouring it matters
            # more than this one article: ignoring Retry-After is what
            # turns a soft throttle into an IP block, and the next run
            # needs this domain to still answer.
            retry_after = resp.headers.get("Retry-After")
            logger.warning(
                "[BODY_FETCH] %s rate-limited (429), Retry-After=%s - "
                "skipping this article", url, retry_after or "unset",
            )
            host = (urlparse(url or "").hostname or "").lower()
            if host.startswith("www."):
                host = host[len("www."):]
            try:
                penalty = float(retry_after) if retry_after else 60.0
            except (TypeError, ValueError):
                penalty = 60.0
            with _DOMAIN_FETCH_LOCK:
                _domain_last_fetch_at[host] = time.monotonic() + penalty
            return None
        if resp.status_code != 200 or not resp.text:
            return None
        with _EXTRACT_LOCK:
            text = trafilatura.extract(resp.text)
    except Exception as exc:
        logger.debug("[BODY_FETCH] impersonated fetch failed url=%r: %s", url, exc)
        return None
    if not text or _looks_like_paywall_teaser(text):
        return None
    return text


# Third and last attempt, for articles whose publisher serves no body to
# an unauthenticated reader at all. Measured against the real stored
# failures, after the two attempts above had already failed:
#
#   ft.com        6,064 chars   bloomberg.com  4,224 chars
#   scmp.com      5,841 chars   nytimes.com    8,315-63,399 chars
#   wsj.com       3,954 chars   reuters.com    not archived (404)
#
# Two alternatives were tested on the same URLs and rejected:
#   - The Wayback availability API returns `archived_snapshots: {}` for
#     reuters/wsj/nytimes, and where it DOES hold a Reuters snapshot the
#     snapshot is itself a 401 - the crawler was refused too.
#   - Publisher RSS carries teasers, not articles: WSJ/NYT/BBC feeds
#     return descriptions around 180 characters, and WSJ's feed contains
#     zero <content:encoded> blocks (an earlier count of 20 was matching
#     the namespace declaration, not content).
#   - A headless browser does WORSE than this: Playwright extracted 1,054
#     chars from scmp.com against 5,841 here, and bloomberg.com answered
#     403 to a real Chromium. It also pulls a browser binary per
#     architecture, which this service's AMD64 ECS target makes its own
#     problem.
#
# Runs ONLY after both direct attempts have failed, so a reachable
# publisher is never routed through a mirror. Rate-limited to one
# in-flight request at a time: archive.ph is an unaffiliated service with
# no SLA, and a fetch loop hitting it concurrently is both rude and
# likely to be throttled.
# Only these publishers are ever looked up in the archive. Everything
# else that fails both direct attempts is simply left without a body.
#
# This is the real rate-limit control, not the interval below. archive.ph
# is fronted by Cloudflare and starts answering 429 after a short burst
# (measured during development: a handful of back-to-back lookups earned
# a block lasting minutes, and the 429 page is itself 59KB of challenge
# HTML that looks like a successful fetch). Restricting WHO gets looked
# up bounds the request count structurally - a day's run makes at most a
# handful of archive calls instead of one per failed body.
#
# Each entry earned its place by failing both direct attempts AND
# returning real article text from a snapshot:
#   ft.com 6,064 chars · bloomberg.com 4,224 · scmp.com 5,841
#   nytimes.com 8,315-63,399 · wsj.com 3,954
# reuters.com is deliberately ABSENT: it is not archived (404), and the
# one Wayback snapshot that exists is itself a 401 - the crawler was
# refused the same way we are. Adding it would spend a lookup to learn
# nothing, every single run.
_ARCHIVE_LOOKUP_DOMAINS: frozenset[str] = frozenset({
    "ft.com",
    "bloomberg.com",
    "nytimes.com",
    "wsj.com",
    "scmp.com",
})

_ARCHIVE_SNAPSHOT_URL = "https://archive.ph/newest/"
_ARCHIVE_LOCK = threading.Lock()
# 2.0 was measured to be too aggressive - a short burst of requests
# earned a 429 within a handful of calls. This path only ever runs on
# articles two direct attempts already failed on (a few per day at most),
# so a slow interval costs nothing real.
_ARCHIVE_MIN_INTERVAL_SECS = 6.0
_archive_last_request_at = 0.0

# Below this, a snapshot page is the mirror's own "not archived" notice
# (measured at 322-4,573 characters across reuters/ft/apnews misses)
# rather than an article. Set above the largest observed miss so a real
# short article still has room, and paired with the teaser check below.
_ARCHIVE_MIN_USABLE_CHARS = 600


# Jina AI's reader endpoint: prepend it to a URL and it returns the page
# as markdown. The LAST tier, after Trafilatura, curl_cffi and
# archive.ph have all failed, because it sends the article URL to a
# third party - which is a real cost (that party learns which articles
# this pipeline cares about), justified only for the handful per run
# that nothing else can read.
#
# Measured across the 19 bodyless articles of one real run: 13 recovered,
# including all three bloomberg.com items at 13.7k-19.1k characters.
# ft.com still returns a 500-byte paywall stub, so this is a partial
# answer, not a general one.
_READER_PROXY_URL = "https://r.jina.ai/"
# Published by the endpoint itself: "x-ratelimit-limit: 20, 20;w=60" on
# an anonymous request. A token bucket rather than a fixed interval,
# because the real constraint is 20 per rolling minute, not a minimum
# gap - this lets a run of 3-4 blocked articles go straight through
# while still refusing to exceed the published ceiling.
_READER_RATE_LIMIT = 20
_READER_RATE_WINDOW_SECS = 60.0
_READER_LOCK = threading.Lock()
_reader_request_times: list[float] = []
# A paywall stub comes back small. ft.com measured 500 bytes, ejiltalk 0,
# consilium.europa.eu 479; the smallest real article recovered was
# 6,223. The gap is wide enough that a single threshold separates them.
_READER_MIN_USABLE_CHARS = 1500


def _fetch_body_from_archive(url: str) -> str | None:
    """Last attempt: read a public archive snapshot of the article.

    Returns None - never a partial - when the publisher is not on
    _ARCHIVE_LOOKUP_DOMAINS, when no snapshot exists, when the mirror
    answers with its own miss page, or when the snapshot itself turns
    out to be a subscription wall.
    """
    global _archive_last_request_at
    if not _curl_cffi_requests:
        return None
    host = (urlparse(url or "").hostname or "").lower()
    if host.startswith("www."):
        host = host[len("www."):]
    if host not in _ARCHIVE_LOOKUP_DOMAINS:
        return None
    try:
        # Skip rather than wait. An earlier version slept inside this
        # lock to honour the interval, which made every other worker that
        # reached this path queue behind it - with 10 GDELT workers and a
        # 6s interval, one slow mirror could stall a whole batch for a
        # minute. A body is optional by design, so declining to look up
        # THIS article is strictly better than blocking the fetch of
        # others: the article still gets a row, just without a body.
        with _ARCHIVE_LOCK:
            since = time.monotonic() - _archive_last_request_at
            if since < _ARCHIVE_MIN_INTERVAL_SECS:
                logger.debug(
                    "[BODY_FETCH] archive interval not elapsed, skipping %r", url,
                )
                return None
            _archive_last_request_at = time.monotonic()
        resp = _curl_cffi_requests.get(
            f"{_ARCHIVE_SNAPSHOT_URL}{url}",
            impersonate="chrome124",
            timeout=35,
            allow_redirects=True,
        )
        # Status is checked, never bypassed. A 429 here still returns a
        # large page that redirected to a real snapshot URL - 59KB in the
        # case measured - but the content is Cloudflare's rate-limit
        # notice ("What can I do to prevent this in the future?"), not
        # the article. Reading the body on a 429 looks like it recovers
        # content and actually stores a challenge page.
        if resp.status_code != 200 or not resp.text:
            if resp.status_code == 429:
                logger.warning(
                    "[BODY_FETCH] archive rate-limited (429) - backing off"
                    " for %r", url,
                )
            return None
        with _EXTRACT_LOCK:
            text = trafilatura.extract(resp.text)
    except Exception as exc:
        logger.debug("[BODY_FETCH] archive fetch failed url=%r: %s", url, exc)
        return None
    if not text or len(text) < _ARCHIVE_MIN_USABLE_CHARS:
        return None
    if _looks_like_paywall_teaser(text):
        return None
    logger.info("[BODY_FETCH] recovered %d chars from archive for %r", len(text), url)
    return text


# Per-domain pacing for geopolitical_news body fetches. Without it a run with several
# articles from one publisher fires them concurrently - the GDELT and
# SerpAPI paths both use ThreadPoolExecutor(max_workers=10) - and a
# burst is exactly what trips an edge WAF. Confirmed live on
# bloomberg.com, which had 3 articles in one real run: the first
# request returned the article, later ones returned "To continue,
# please click the box below to let us know you're not a robot".
#
# The interval is per HOST, not global: two different publishers are
# unrelated as far as either one's WAF is concerned, so serialising
# across all of them would slow the run for no benefit. Jittered rather
# than fixed - an exactly-periodic request pattern is itself a bot
# signature.
_DOMAIN_FETCH_INTERVALS: dict[str, float] = {
    "bloomberg.com": 12.0,   # TLS fingerprinting + Kasada challenges
    "reuters.com": 8.0,      # token-gated, 401s on anonymous reads
    "wsj.com": 8.0,
    "nytimes.com": 8.0,
    "ft.com": 8.0,
    "scmp.com": 4.0,
    "abcnews.com": 4.0,
    "globalsources.com": 4.0,  # plain 429 with Retry-After
    # Named explicitly rather than left on the 1.0s default: confirmed
    # live that an unpaced burst of full-text fetches gets every
    # subsequent one 302'd to unblock.federalregister.gov, on any user
    # agent, while the JSON API keeps answering 200. A real day carries
    # ~9 documents, so 2s each costs 18 seconds on a once-daily job -
    # nothing, against a block that silently empties every body.
    "federalregister.gov": 2.0,
}
_DEFAULT_DOMAIN_INTERVAL_SECS = 1.0
_DOMAIN_FETCH_LOCK = threading.Lock()
_domain_last_fetch_at: dict[str, float] = {}


def _throttle_domain(url: str) -> None:
    """Sleep just long enough that this host is not hit faster than its
    own interval allows.

    Blocking is correct here, unlike the archive lookup's skip-if-busy:
    a body fetch has nothing else to fall back on, so waiting two
    seconds beats being served a CAPTCHA page that then has to be
    detected and discarded.
    """
    host = (urlparse(url or "").hostname or "").lower()
    if host.startswith("www."):
        host = host[len("www."):]
    interval = _DOMAIN_FETCH_INTERVALS.get(host, _DEFAULT_DOMAIN_INTERVAL_SECS)
    with _DOMAIN_FETCH_LOCK:
        wait = interval - (time.monotonic() - _domain_last_fetch_at.get(host, 0.0))
        if wait > 0:
            time.sleep(wait + random.uniform(0, interval * 0.25))
        _domain_last_fetch_at[host] = time.monotonic()


def _reader_slot_available() -> bool:
    """Claim one slot against the reader endpoint's 20-per-60s budget.

    Skip rather than wait, same discipline as the archive tier: a body is
    optional, so declining THIS article beats blocking the workers
    fetching others. Returns False when the window is already full.
    """
    now = time.monotonic()
    with _READER_LOCK:
        cutoff = now - _READER_RATE_WINDOW_SECS
        _reader_request_times[:] = [t for t in _reader_request_times if t > cutoff]
        if len(_reader_request_times) >= _READER_RATE_LIMIT:
            return False
        _reader_request_times.append(now)
        return True


def _fetch_body_from_reader(url: str) -> str | None:
    """Final tier: read the article through Jina AI's reader endpoint.

    Returns None - never a partial - when the rate budget is spent, the
    endpoint errors, or what comes back is a paywall stub rather than an
    article.

    The markdown is run back through Trafilatura, not stored raw: the
    reader returns the page's navigation, footer and cookie banner
    alongside the article, and the same extraction the other tiers use
    strips those. The paywall-teaser and bot-check markers apply here
    too - a reader proxy can fetch a teaser page as faithfully as any
    other client.
    """
    if not _reader_slot_available():
        logger.debug("[BODY_FETCH] reader rate budget spent, skipping %r", url)
        return None
    try:
        resp = httpx.get(
            f"{_READER_PROXY_URL}{url}",
            timeout=40.0,
            follow_redirects=True,
            headers={"x-return-format": "markdown"},
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.debug("[BODY_FETCH] reader failed for %r: %s", url, exc)
        return None

    raw = resp.text or ""
    if len(raw) < _READER_MIN_USABLE_CHARS:
        logger.debug(
            "[BODY_FETCH] reader returned %d chars for %r - paywall stub, not an"
            " article", len(raw), url,
        )
        return None

    # Markdown in, article text out. Same serialized Trafilatura call
    # the direct path uses - see _EXTRACT_LOCK for the segfaults that
    # forced it - so a page that is mostly chrome reduces to nothing
    # here rather than being stored as a body. Trafilatura reads
    # markdown's own structure poorly, so a failed extraction falls back
    # to the cleaned raw text rather than discarding a real article.
    try:
        with _EXTRACT_LOCK:
            text = trafilatura.extract(raw)
    except Exception:
        text = None
    text = _clean_summary(text or raw)
    if not text or len(text) < _READER_MIN_USABLE_CHARS:
        return None
    if _looks_like_paywall_teaser(text):
        logger.debug("[BODY_FETCH] reader body for %r is a teaser/bot page", url)
        return None
    logger.info("[BODY_FETCH] reader recovered %d chars for %r", len(text), url)
    return text


def _fetch_body(url: str, *, impersonate_fallback: bool = False) -> str | None:
    """Fetch url and return its extracted body text, or None when the fetch
    or extraction yields nothing (paywall/bot-detection block, or a dead
    source URL).

    The download runs concurrently with other workers; the extraction is
    serialized behind _EXTRACT_LOCK - see that constant's comment for the
    real crashes that forced this.

    Shared by all three Trafilatura-backed body-fetch call sites (RSS
    content:encoded fallback, SerpAPI, GDELT) so the behavior stays
    identical across all of them rather than drifting.

    ``impersonate_fallback`` opts a caller into a second attempt via
    curl_cffi when Trafilatura comes back empty (see
    _fetch_body_impersonated). **Off by default, and currently passed
    only by the geopolitical_news fetch path.** Not a general default,
    for two reasons: it doubles the work on every genuinely dead URL
    (every other domain's fetchers walk large listings where a failed
    body is routine), and the domains it was built for - US/EU
    paper-of-record paywalls - are specific to geopolitical_news's own
    allowlist. Taiwan, Japan, Korea and China fetch from exchange
    filings, government notices and regional press, none of which block
    on a TLS fingerprint. Widening it should follow the same discipline
    as the allowlist itself: a confirmed live miss first, not a
    speculative default.

    The paywall-teaser check applies to BOTH paths, since a wall can come
    back from the normal fetch too - that check belongs to the result,
    not to the transport.
    """
    # Scoped to the callers that opt into the geopolitical fetch chain,
    # the same gate the curl_cffi and archive tiers use. The eight
    # throttled hosts are geopolitical-only in practice (Taiwan, Japan,
    # Korea and China fetch exchange filings and regional press), and
    # leaving it global would serialise those domains' own GDELT paths,
    # which can pull many articles from ONE outlet in a run and have
    # never had a WAF problem.
    if impersonate_fallback:
        _throttle_domain(url)
    downloaded = trafilatura.fetch_url(url, config=_get_trafilatura_config())
    text = None
    if downloaded:
        with _EXTRACT_LOCK:
            text = trafilatura.extract(downloaded)
        if text and _looks_like_paywall_teaser(text):
            # The publisher answered, but with a wall. Treat that as "no
            # body from the direct path" rather than as a result, so the
            # fallbacks below still get their turn - this is exactly the
            # ft.com case, which returns HTTP 200 and an offer.
            text = None
    if text:
        return text
    if not impersonate_fallback:
        return None
    # Three fallbacks, cheapest and most self-contained first:
    #   impersonated  our own request with a browser TLS fingerprint
    #   archive       a public snapshot, 5 publishers only
    #   reader        a third party fetches it for us - last, because it
    #                 is the only tier that tells someone else which
    #                 articles this pipeline reads
    return (
        _fetch_body_impersonated(url)
        or _fetch_body_from_archive(url)
        or _fetch_body_from_reader(url)
    )


def _extract_body(entry: Any, url: str, no_fetch: bool) -> str | None:
    """Return the best available body text for an article entry.

    Tries ``content:encoded`` first (≥ 150 words). Falls back to a
    Trafilatura fetch when the source permits it (``no_fetch=False``).

    Args:
        entry: feedparser entry object.
        url: Article URL used for Trafilatura fallback fetch.
        no_fetch: When True, skip the Trafilatura fetch.

    Returns:
        Cleaned body string, or None if unavailable.
    """
    content_list = entry.get("content", [])
    raw_body = (
        content_list[0].get("value", "") if content_list else ""
    )
    clean_body = _clean_summary(raw_body) if raw_body else ""
    if len(clean_body.split()) >= 150:
        return clean_body
    if no_fetch:
        return None
    return _fetch_body(url)


# feedparser's own default User-Agent identifies it as a bot and is
# rejected outright (HTTP 403) by at least one real, on-target Korean RSS
# feed (hankyung.com) that returns clean 200s for a normal browser
# User-Agent - see _parse_feed below. Applied to every RSS source, not
# just the one feed where this was caught, since any other WAF-protected
# feed would fail the same silent way (bozo=True, 0 entries, easy to
# misread as "this source is dead" rather than "this source is blocking
# feedparser specifically").
_RSS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Trafilatura's own default fetch User-Agent identifies it as a bot and is
# rejected outright (HTTP 403) by at least one real, on-target source
# (dailymail.com, confirmed live: 403 with Trafilatura's default UA, clean
# 200 with a normal browser UA) - same root cause _RSS_USER_AGENT above
# already fixes for RSS feeds, applied here to every trafilatura.fetch_url
# call (GDELT, SerpAPI, and the RSS content:encoded-fallback body fetches)
# so a WAF blocking Trafilatura specifically doesn't read as "this source
# has no body text". Does not fix sites that block scraping regardless of
# UA (confirmed live: reuters.com, bloomberg.com return 401/403 even with
# this same browser UA) - those still fail open to a null body, same as
# before.

# One ConfigParser instance PER THREAD, not a single shared module-level
# object - every call site above runs inside a ThreadPoolExecutor(max_workers=10)
# (RSS body fetch, SerpAPI body fetch, GDELT body fetch), so a shared
# instance means up to 10 threads read the same ConfigParser concurrently
# on every fetch. A real crash was observed in production the first time
# this shared-instance version ran at GDELT's larger batch scale
# (glibc "corrupted size vs. prev_size while consolidating" - a heap
# corruption abort) - root cause not confirmed (a comparable-scale local
# repro with the shared instance did not reproduce it, and nothing in
# Trafilatura's own downloads.py appears to mutate the config object), but
# building one instance per thread removes shared mutable state from this
# code path entirely regardless of whether it was the actual cause, at
# negligible cost (ConfigParser construction is cheap, done once per
# thread's lifetime via threading.local, not once per URL).
_trafilatura_config_local = threading.local()


def _get_trafilatura_config():
    config = getattr(_trafilatura_config_local, "config", None)
    if config is None:
        config = use_config()
        config.set("DEFAULT", "USER_AGENTS", _RSS_USER_AGENT)
        _trafilatura_config_local.config = config
    return config


def _parse_feed(source: dict, cutoff: datetime) -> list[dict]:
    """Parse a single RSS feed and return articles published after cutoff.

    Args:
        source: Source dict with ``url`` and ``no_fetch`` keys.
        cutoff: Exclude entries published before this datetime.

    Returns:
        List of article dicts with a ``_pub_date`` key for sorting.
    """
    url: str = source["url"]
    no_fetch: bool = source["no_fetch"]
    # Optional post-fetch title filter (config.title_filter, a regex
    # pattern string) - for a general/mixed-topic feed with no
    # business/economy section feed of its own (e.g. Yonhap's only
    # confirmed-working feed is its general wire, ~85% off-topic for this
    # pipeline's purposes: politics, diplomacy, sports). None/absent means
    # no filtering at all, unchanged behavior for every other RSS source.
    title_filter = (source.get("config") or {}).get("title_filter")
    title_filter_re = re.compile(title_filter, re.IGNORECASE) if title_filter else None
    t0 = time.perf_counter()
    # feedparser's own default User-Agent (UniversalFeedParser/...)
    # identifies it as a bot - confirmed live 2026-09-23 that at least one
    # real, on-target Korean feed (hankyung.com/feed/economy) 403s that
    # default outright while returning clean 200s + full content for a
    # normal browser User-Agent. Same underlying risk class as the
    # DART/document-fetch WAF issue found earlier - a source can look
    # "broken" (bozo=True, 0 entries) when it's actually just bot-blocked,
    # not genuinely dead the way ETNews's frozen feed was.
    feed = feedparser.parse(url, request_headers={"User-Agent": _RSS_USER_AGENT})
    results = []
    filtered_out = 0
    for entry in feed.entries:
        pub_date = None
        if (
            hasattr(entry, "published_parsed")
            and entry.published_parsed
        ):
            pub_date = datetime(
                *entry.published_parsed[:6], tzinfo=timezone.utc
            )
            if pub_date < cutoff:
                continue
        title = entry.get("title", "")
        if title_filter_re and not title_filter_re.search(title):
            filtered_out += 1
            continue
        article_url = entry.get("link", "")
        results.append({
            "title": title,
            "url": article_url,
            "published": entry.get("published", ""),
            "source": feed.feed.get("title", url),
            "summary": _clean_summary(entry.get("summary", "")),
            "body": _extract_body(entry, article_url, no_fetch),
            "_pub_date": pub_date,
        })
    logger.info(
        "[TIMER] feed=%s articles=%d filtered_out=%d elapsed=%.2fs",
        url, len(results), filtered_out, time.perf_counter() - t0,
    )
    return results


def _fetch_rss(sources: list[dict], days_back: int) -> list[dict]:
    """Fetch articles from RSS feeds in parallel.

    Args:
        sources: List of RSS source dicts with a ``url`` key.
        days_back: Exclude articles older than this many days.

    Returns:
        List of article dicts with a ``_pub_date`` key for sorting.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    articles: list[dict] = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=10) as executor:
        for feed_articles in executor.map(
            partial(_parse_feed, cutoff=cutoff),
            sources,
        ):
            articles.extend(feed_articles)
    logger.info(
        "[TIMER] rss total: feeds=%d articles=%d elapsed=%.2fs",
        len(sources), len(articles), time.perf_counter() - t0,
    )
    return articles


def _days_to_tbs(days_back: int) -> str | None:
    """Map days_back to a SerpAPI tbs date-range parameter."""
    if days_back <= 1:
        return "qdr:d"
    if days_back <= 7:
        return "qdr:w"
    if days_back <= 30:
        return "qdr:m"
    return None


def _parse_serpapi_date(date_str: str) -> datetime | None:
    """Parse a SerpAPI date string to a UTC datetime, or None on failure."""
    m = _SERPAPI_DATE_RE.match(date_str)
    if not m:
        return None
    try:
        return datetime.strptime(
            f"{m.group(1)} {m.group(2)}", "%m/%d/%Y %I:%M %p"
        ).replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _fetch_one_serpapi(
    source: dict, days_back: int, api_key: str, domain_slug: str = "",
) -> list[dict]:
    """Fetch Google News results for a SerpAPI source.

    If ``config`` contains a ``queries`` list, all queries are fetched in
    parallel and results are deduplicated by URL. Falls back to
    ``source["url"]`` as a single query when ``queries`` is absent.

    Args:
        source: Source dict; ``config.queries`` is the preferred query list;
            ``url`` is used as a single query when ``queries`` is absent.
        days_back: Used to set the SerpAPI tbs date-range filter.
        api_key: SerpAPI API key.

    Returns:
        List of article dicts with ``_pub_date`` set.
    """
    config = dict(source.get("config") or {})
    queries: list[str] = config.pop("queries", None) or [source["url"]]
    tbs = _days_to_tbs(days_back)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    def _fetch_query(query: str) -> list[dict]:
        params: dict = {"engine": "google_news", "q": query, "api_key": api_key, "num": _SERPAPI_RESULTS_PER_QUERY, **config}
        if tbs:
            params["tbs"] = tbs
        try:
            resp = httpx.get(
                "https://serpapi.com/search",
                params=params,
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("[SERPAPI] query=%r failed: %s", query, exc)
            return []
        results = []
        for r in data.get("news_results", []):
            pub_date = _parse_serpapi_date(r.get("date", ""))
            if pub_date and pub_date < cutoff:
                continue
            results.append({
                "title": r.get("title", ""),
                "url": r.get("link", ""),
                "published": r.get("date", ""),
                "source": r.get("source", {}).get("name", ""),
                "summary": _clean_summary(r.get("snippet", "")),
                "_pub_date": pub_date,
            })
        return results

    t0 = time.perf_counter()
    seen_urls: set[str] = set()
    candidates: list[dict] = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        for query_results in executor.map(_fetch_query, queries):
            for a in query_results:
                if a["url"] and a["url"] not in seen_urls:
                    seen_urls.add(a["url"])
                    candidates.append(a)

    def _fetch_body_if_url(url: str) -> str | None:
        # Same body-fetch allowlist the GDELT path applies, for the same
        # reason - a SerpAPI result from a domain the consumer drops at
        # Stage A is not worth a Trafilatura fetch. The article is still
        # stored; only the body is skipped.
        if not url or not _should_fetch_body(url, domain_slug):
            return None
        return _fetch_body(
            url, impersonate_fallback=_impersonate_for(domain_slug),
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        bodies = list(executor.map(_fetch_body_if_url, [a["url"] for a in candidates]))

    for article, body in zip(candidates, bodies):
        article["body"] = body

    logger.info(
        "[TIMER] serpapi source=%r queries=%d articles=%d elapsed=%.2fs",
        source["url"], len(queries), len(candidates), time.perf_counter() - t0,
    )
    return candidates


def _fetch_serpapi(
    sources: list[dict],
    days_back: int,
    api_key: str,
    domain_slug: str = "",
) -> list[dict]:
    """Fetch Google News articles from SerpAPI for multiple queries in parallel.

    Args:
        sources: List of SerpAPI source dicts; ``url`` is the search query.
        days_back: Used to set the SerpAPI tbs date-range filter.
        api_key: SerpAPI API key.

    Returns:
        List of article dicts with ``_pub_date`` set to None.
    """
    articles: list[dict] = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as executor:
        for source_articles in executor.map(
            partial(
                _fetch_one_serpapi, days_back=days_back, api_key=api_key,
                domain_slug=domain_slug,
            ),
            sources,
        ):
            articles.extend(source_articles)
    logger.info(
        "[TIMER] serpapi total: sources=%d articles=%d elapsed=%.2fs",
        len(sources), len(articles), time.perf_counter() - t0,
    )
    return articles


def _fetch_one_newsapi(source: dict, days_back: int, api_key: str) -> list[dict]:
    """Fetch top-headlines articles for a single NewsAPI source.

    If ``config`` contains a ``categories`` list, one HTTP request is made per
    category and results are deduplicated by URL before body enrichment.

    Args:
        source: Source dict; ``config`` carries NewsAPI params (``endpoint``,
            ``categories``, ``language``, etc.).
        days_back: Exclude articles older than this many days.
        api_key: NewsAPI API key.

    Returns:
        List of article dicts with ``_pub_date`` set.
    """
    config = dict(source.get("config") or {})
    endpoint = config.pop("endpoint", "top-headlines")
    categories: list[str | None] = config.pop("categories", [None])
    from_date = (
        datetime.now(timezone.utc) - timedelta(days=days_back)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    base_url = f"https://newsapi.org/v2/{endpoint}"

    t0 = time.perf_counter()
    seen_urls: set[str] = set()
    candidates: list[dict] = []

    for category in categories:
        params: dict = {**config, "pageSize": _NEWSAPI_PAGE_SIZE, "apiKey": api_key}
        if endpoint == "everything":
            params["from"] = from_date
        if category is not None:
            params["category"] = category
        try:
            resp = httpx.get(base_url, params=params, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning(
                "[NEWSAPI] source=%r category=%r failed: %s",
                source["url"], category, exc,
            )
            continue
        for r in data.get("articles", []):
            url = r.get("url", "")
            if not url or url in seen_urls:
                continue
            pub_date = _parse_newsapi_date(r.get("publishedAt", ""))
            if pub_date and pub_date < cutoff:
                continue
            seen_urls.add(url)
            raw_content = r.get("content", "") or ""
            candidates.append({
                "title": r.get("title", ""),
                "url": url,
                "published": r.get("publishedAt", ""),
                "source": (r.get("source") or {}).get("name", ""),
                "summary": _clean_summary(r.get("description", "") or ""),
                "body": _clean_summary(raw_content) or None,
                "_pub_date": pub_date,
            })

    logger.info(
        "[TIMER] newsapi source=%r articles=%d elapsed=%.2fs",
        source["url"], len(candidates), time.perf_counter() - t0,
    )
    return candidates


def _fetch_newsapi(
    sources: list[dict],
    days_back: int,
    api_key: str,
) -> list[dict]:
    """Fetch articles from NewsAPI for multiple sources in parallel.

    Args:
        sources: List of NewsAPI source dicts with ``config`` carrying API params.
        days_back: Exclude articles older than this many days.
        api_key: NewsAPI API key.

    Returns:
        List of article dicts with ``_pub_date`` set.
    """
    articles: list[dict] = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as executor:
        for source_articles in executor.map(
            partial(_fetch_one_newsapi, days_back=days_back, api_key=api_key),
            sources,
        ):
            articles.extend(source_articles)
    logger.info(
        "[TIMER] newsapi total: sources=%d articles=%d elapsed=%.2fs",
        len(sources), len(articles), time.perf_counter() - t0,
    )
    return articles


# research-universe tickers use dot notation for both US share classes
# (BRK.B) and foreign exchange suffixes (6503.JP, 000660.KS, ATCO-A.ST).
# Alpha Vantage only covers US-listed symbols and expects a dash for share
# classes (BRK-B). A true US share class is ALL-LETTERS.SINGLE-LETTER with
# no dash; anything else containing a dot (numeric prefixes, 2+ letter
# suffixes, or a dash elsewhere in the ticker) is a foreign exchange
# suffix and gets dropped.
_AV_SHARE_CLASS_RE = re.compile(r"^([A-Z]+)\.([A-Z])$")


def _normalize_av_ticker(ticker: str) -> str | None:
    """Convert a research-universe ticker to Alpha Vantage format, or None if unsupported."""
    ticker = ticker.strip().upper()
    share_class = _AV_SHARE_CLASS_RE.match(ticker)
    if share_class:
        return f"{share_class.group(1)}-{share_class.group(2)}"
    if "." in ticker:
        return None
    return ticker


def _fetch_universe_tickers(base_url: str, api_key: str | None = None) -> list[str]:
    """Fetch US-listed ticker symbols from the research-universe API.

    Calls GET /companies?country=United States&has_ticker=true (both verified
    and pending_review companies), normalizes each ticker to Alpha Vantage
    format (see _normalize_av_ticker), and drops any still-foreign tickers
    that slip through the country filter (the data has some mislabeled rows,
    e.g. Japan/HK/Korea listings tagged "United States"). Falls back to []
    on any error so the caller can continue with config-only tickers.

    Args:
        base_url: research-universe service base URL, e.g.
            "http://research-universe.staging.ocn.internal:8007"
        api_key: service API key (ru_ prefix) for Authorization header
    """
    try:
        headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
        resp = httpx.get(
            f"{base_url}/companies",
            params={
                "country": "United States",
                "has_ticker": "true",
                "limit": 10000,
            },
            headers=headers,
            timeout=30.0,
        )
        resp.raise_for_status()
        companies = resp.json()
        tickers = []
        for c in companies:
            raw = c.get("ticker", "").strip()
            if not raw:
                continue
            normalized = _normalize_av_ticker(raw)
            if normalized:
                tickers.append(normalized)
        logger.info("[ALPHA_VANTAGE] fetched %d tickers from universe API", len(tickers))
        return tickers
    except Exception as exc:
        logger.warning(
            "[ALPHA_VANTAGE] universe API unavailable, falling back to config tickers: %s", exc
        )
        return []


# Alpha Vantage API limits (premium key):
# - 75 calls/minute, no daily limit
# - 1 ticker per call (multi-ticker requests return far fewer articles)
_AV_CALLS_PER_MINUTE = 75
_AV_MIN_INTERVAL = 60.0 / _AV_CALLS_PER_MINUTE  # ~0.8 seconds between calls
 

def get_tracked_ticker_universe(
    universe_url: str | None, universe_api_key: str | None = None,
) -> list[str]:
    """Single source of truth for the tracked ticker universe: US-listed
    tickers fetched live from research-universe, normalized and deduped.
    Used by both the Alpha Vantage fetch (this module) and the
    /market/tracked-tickers route, so the two never drift apart.

    Returns [] if universe_url is not set or research-universe is
    unreachable - callers treat an empty list as "skip this poll run"
    rather than falling back to a hardcoded list.
    """
    if not universe_url:
        return []
    return list(dict.fromkeys(_fetch_universe_tickers(universe_url, universe_api_key)))


class UniverseUnavailable(RuntimeError):
    """A tracked company universe could not be loaded.

    Raised rather than falling back to anything local. A fetch run
    that quietly stores zero articles looks exactly like a quiet news
    day and goes unnoticed for as long as the market stays calm; a run
    that exits non-zero is visible the same hour.
    """


def get_tracked_company_universe(
    country: str,
    universe_url: str | None,
    universe_api_key: str | None = None,
) -> list[dict[str, Any]]:
    """The companies one country's signal pipeline follows, live from
    research-universe.

    ``tracked=true`` is what narrows the catalogue to the pipeline's own
    universe, and it is not the same question as country: Japan holds 62
    catalogue companies but only 19 tracked ones, so filtering on country
    alone would have this service fetching filings for Keyence and Daikin.

    Field names are translated to the ones the fetchers already use -
    research-universe calls them code/native_name because it holds
    Taiwan and Korea too, while the fetchers here have always said
    code/native_name. Translating in one place keeps that rename out of
    six call sites.

    Supersedes the per-source ``config.companies`` list seed.py bakes in,
    which remains as the fallback when this service cannot be reached.
    Preferring the live read fixes a confirmed failure mode: seeding is
    ``ON CONFLICT DO NOTHING``, so a company added or corrected in code
    never reached an already-seeded database - an alias added for Resonac
    and Renesas silently kept matching 62 articles instead of 74 until
    the stored config was updated by hand.

    Raises UniverseUnavailable if the URL is unset or the service
    cannot be reached. There is no local copy to fall back to, by
    design: the one that existed had already drifted from the live
    data it was standing in for.
    """
    if not universe_url:
        raise UniverseUnavailable(
            f"RESEARCH_UNIVERSE_URL is not set; the {country} company "
            f"universe has no other source")
    try:
        headers = ({"Authorization": f"Bearer {universe_api_key}"}
                   if universe_api_key else {})
        resp = httpx.get(
            f"{universe_url}/companies",
            params={"country": country, "tracked": "true", "limit": 10000},
            headers=headers,
            timeout=30.0,
        )
        resp.raise_for_status()
        rows = resp.json()
    except Exception as exc:
        raise UniverseUnavailable(
            f"research-universe unreachable for {country}: {exc}") from exc

    companies: list[dict[str, Any]] = []
    for r in rows:
        code = (r.get("code") or "").strip()
        if not code:
            continue
        company = (r.get("company_name") or "").strip()
        entry: dict[str, Any] = {"code": code, "company": company}
        if r.get("native_name"):
            entry["native_name"] = r["native_name"]
        # Every other written form, carried as the list it is rather
        # than collapsed to one `short_name`. Japan has at most one per
        # company today, so the two would look alike - but Korea needs
        # several (a group prefix plus variants), and picking aliases[0]
        # would quietly make array order decide which form gets matched.
        entry["aliases"] = [a for a in (r.get("aliases") or []) if a]
        # Names that must NOT count as a mention of this company -
        # Inspur's 浪潮软件 and 浪潮数字 are separate listed entities
        # sharing its 浪潮 prefix. Carried through because the China
        # press matcher reads it; absent upstream means "no exclusions",
        # which is the common case.
        excludes = [t for t in (r.get("exclude_terms") or []) if t]
        if excludes:
            entry["exclude_terms"] = excludes
        if r.get("fiscal_year_end"):
            entry["fiscal_year_end"] = r["fiscal_year_end"]
        # Only two of the nineteen Japanese companies carry one: "Disco"
        # and "Towa" are ordinary English words and a bare-name search
        # returns nightclubs and unrelated firms. Everyone else searches
        # on their own name, so NULL upstream means exactly that rather
        # than a missing value to fill in.
        entry["search_query"] = r.get("search_query") or company
        companies.append(entry)

    if not companies:
        raise UniverseUnavailable(
            f"research-universe returned no tracked companies for {country}")
    logger.info("[UNIVERSE] %s: %d tracked companies from research-universe",
                country, len(companies))
    return companies


# Resolved once per fetch run and shared by every Japan source_type, so
# six fetchers in one run make one HTTP call rather than six. Set by
# _fetch_articles before dispatch and cleared after, rather than cached
# across runs: a scheduled fetch should see a company added this morning.
_JAPAN_UNIVERSE: list[dict[str, Any]] | None = None


def _japan_companies(sources: list[dict]) -> list[dict[str, Any]]:
    """The tracked Japanese companies for one fetcher.

    Resolved once per run from research-universe, which owns this data,
    and raised on rather than approximated when that fails. ``sources``
    is taken for signature compatibility with the fetchers that call
    this and is no longer read: the per-source ``config.companies``
    these rows used to carry is dead weight, because `sources` seeds
    ON CONFLICT DO NOTHING and so a company added or corrected in code
    never reaches an already-seeded database. That is not a fallback,
    it is a second copy guaranteed to go stale - confirmed when an
    alias added for Resonac and Renesas silently kept matching 62
    articles instead of 74 until the stored config was updated by hand.
    """
    if not _JAPAN_UNIVERSE:
        raise UniverseUnavailable(
            "the Japan company universe could not be loaded from "
            "research-universe; see the earlier [UNIVERSE] log line")
    return _JAPAN_UNIVERSE


def _fetch_alpha_vantage(
    sources: list[dict],
    alpha_vantage_key: str,
    universe_url: str | None = None,
    universe_api_key: str | None = None,
) -> list[dict]:
    """Fetch company-specific news from Alpha Vantage News & Sentiments API.

    One API call per ticker — multi-ticker requests return far fewer articles.
    Premium key has no daily call cap, only the per-minute rate limit above.

    Args:
        sources: List of alpha_vantage source dicts with ``config.tickers``.
        alpha_vantage_key: Alpha Vantage API key.
        universe_url: research-universe base URL; tickers are fetched
            dynamically from here (see get_tracked_ticker_universe). If
            unset or unreachable, no tickers are fetched and this run
            is skipped.
        universe_api_key: Service API key (ru_ prefix) for research-universe auth.

    Returns:
        List of article dicts with ``_pub_date`` set.
    """
    # Always fetch last 24 hours only — AV is a daily source and fetching
    # wider windows re-fetches articles already stored from previous runs.
    _av_cutoff_days = 1
    cutoff = datetime.now(timezone.utc) - timedelta(days=_av_cutoff_days)
    time_from = cutoff.strftime("%Y%m%dT%H%M")

    tickers = get_tracked_ticker_universe(universe_url, universe_api_key)

    if not tickers:
        logger.warning("[ALPHA_VANTAGE] no tickers configured, skipping")
        return []

    logger.info("[ALPHA_VANTAGE] tickers=%d", len(tickers))

    # Fixed interval: 1 call/sec to stay within 60/min steady rate
    last_call_time: list[float] = [0.0]

    def _rate_limited_get(ticker: str) -> list[dict]:
        now = time.monotonic()
        elapsed = now - last_call_time[0]
        if elapsed < _AV_MIN_INTERVAL:
            time.sleep(_AV_MIN_INTERVAL - elapsed)
        last_call_time[0] = time.monotonic()

        try:
            resp = httpx.get(
                "https://www.alphavantage.co/query",
                params={
                    "function": "NEWS_SENTIMENT",
                    "tickers": ticker,
                    "time_from": time_from,
                    "limit": 50,
                    "apikey": alpha_vantage_key,
                },
                timeout=30.0,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("[ALPHA_VANTAGE] ticker=%r failed: %s", ticker, exc)
            return []

        results = []
        for item in data.get("feed", []):
            raw_date = item.get("time_published", "")
            try:
                pub_date = datetime.strptime(raw_date, "%Y%m%dT%H%M%S").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                pub_date = None
            if pub_date and pub_date < cutoff:
                continue
            results.append({
                "title": item.get("title", ""),
                "url": item.get("url", ""),
                "published": pub_date.isoformat() if pub_date else raw_date,
                "source": item.get("source", ""),
                "summary": _clean_summary(item.get("summary", "")),
                "body": None,
                "_pub_date": pub_date,
                # metadata: source-specific enrichment from Alpha Vantage.
                # overall_sentiment_score: float (-1 bearish to +1 bullish)
                # overall_sentiment_label: e.g. "Somewhat-Bullish"
                # ticker_sentiment: [{ticker, relevance_score, ticker_sentiment_score, ticker_sentiment_label}]
                # topics: [{topic, relevance_score}]
                "metadata": {
                    "overall_sentiment_score": item.get("overall_sentiment_score"),
                    "overall_sentiment_label": item.get("overall_sentiment_label"),
                    "ticker_sentiment": item.get("ticker_sentiment", []),
                    "topics": item.get("topics", []),
                },
            })
        logger.info("[ALPHA_VANTAGE] ticker=%r articles=%d", ticker, len(results))
        return results

    t0 = time.perf_counter()
    seen_urls: set[str] = set()
    articles: list[dict] = []

    for ticker in tickers:
        for article in _rate_limited_get(ticker):
            if article["url"] and article["url"] not in seen_urls:
                seen_urls.add(article["url"])
                articles.append(article)

    logger.info(
        "[TIMER] alpha_vantage tickers=%d articles=%d elapsed=%.2fs",
        len(tickers), len(articles), time.perf_counter() - t0,
    )
    return articles


_FEDERAL_REGISTER_URL = "https://www.federalregister.gov/api/v1/documents.json"
_FEDERAL_REGISTER_PAGE_SIZE = 100

# The documents.json list response carries only 10 fields by default, none of
# them the document text - `abstract` is the longest thing in it. Asking for
# these explicitly is what makes the full text reachable at all; `fields[]`
# REPLACES the default set rather than adding to it, so every field this
# fetcher reads has to be named here, including the ones the default response
# would have given for free.
_FEDERAL_REGISTER_FIELDS = [
    "title",
    "html_url",
    "publication_date",
    "abstract",
    "type",
    "document_number",
    "agencies",
    "executive_order_number",
    "raw_text_url",
    # When a rule takes effect - the M1 alert rule in the Middle East
    # spec needs a stated start date, and it can differ from publication
    # (the UAE A:5 rule was published 14 Jul 2026, effective 10 Jul).
    "effective_on",
]

# Measured over the 14 real stored documents: the abstract carries 1.5-20% of
# the document where it exists at all, and is EMPTY on 5 of 14 - including
# both Presidential Documents, which is the category Stage A's own Federal
# Register rule exists to catch (a proclamation restricting polysilicon
# imports has no abstract, so the row reached the classifier as a bare title).
# raw_text_url returned HTTP 200 on all 14, so the body is fetched from there
# rather than left to the abstract.
#
# NOT trafilatura, unlike every other body-fetch call site in this file: the
# response is already plain text inside a single <pre> block, so there is no
# article to extract from a page of navigation and boilerplate. That also
# keeps this path off _EXTRACT_LOCK - see that constant's comment for the
# segfaults concurrent extract() caused on this very domain's 02:00 run.
_FEDERAL_REGISTER_BODY_MAX_CHARS = 20000

# The GPO text wraps the document in a minimal HTML shell and prefixes it with
# a citation block (volume/issue/page/FR Doc No) that repeats what metadata
# already carries. Stripped because downstream truncates to 2,000 chars -
# boilerplate eats that budget before the measure itself is reached.
_FR_TEXT_LEAD_RE = re.compile(
    r"^.*?\[FR Doc No:\s*[^\]]+\]\s*", re.DOTALL,
)

# Presidential Documents carry a SECOND lead after the citation block - the
# issue masthead the proclamation is printed under ("[[Page 58569]] Vol. 91
# Tuesday, No. 177 September 15, 2026 Part II The President"), which the
# citation regex above leaves behind because it sits after the [FR Doc No:]
# marker rather than before it. Confirmed live on 13 of 45 real documents;
# every one of those is a Presidential Document, and they are precisely the
# documents with NO abstract, so the body is the only text they have.
#
# Anchored on the page marker and cut at "The President" rather than matched
# as a whole: the volume/issue/date wording between the two varies per issue,
# and a looser pattern risks eating the proclamation's own opening line.
_FR_TEXT_MASTHEAD_RE = re.compile(
    r"^\s*\[\[Page\s+\w+\]\]\s*Vol\.\s*\d+.{0,120}?\bThe President\b\s*",
    re.DOTALL,
)


def _fetch_federal_register_body(
    raw_text_url: str,
    document_number: str | None = None,
    publication_date: str | None = None,
    max_chars: int | None = _FEDERAL_REGISTER_BODY_MAX_CHARS,
) -> str | None:
    """Return the document's full text, or None when it cannot be read.

    None, never "" - a null reads as "not fetched", an empty string would read
    as "this document says nothing", and the two must not be conflated (same
    rule the China and Japan sources follow for unreadable PDFs).

    govinfo.gov is tried FIRST, with federalregister.gov as the fallback.
    Both serve the same document - govinfo is the Government Publishing
    Office's own system and the Register's API links to it - but they
    behave very differently under load:

      federalregister.gov  rate-limits per IP and answers 302 ->
                           unblock.federalregister.gov on EVERY full-text
                           format (txt, xml, html) and every user agent,
                           while its JSON API keeps answering 200.
      govinfo.gov          12 parallel requests with no delay returned
                           12/12 200s in 1.7 seconds (measured).

    So the throttle below applies only to the fallback leg; the primary
    needs none, which is what lets a backfill of hundreds of documents
    run in seconds rather than minutes.
    """
    body = _fetch_federal_register_body_from_govinfo(
        document_number, publication_date, max_chars,
    )
    if body:
        return body

    # Fallback: the Register's own full-text URL. Reached when govinfo
    # has no copy (very recent documents can lag) or the document number
    # is missing. Throttled, because this is the host that rate-limits.
    _throttle_domain(raw_text_url)
    try:
        resp = httpx.get(raw_text_url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning(
            "[FEDERAL_REGISTER] body fetch failed url=%r: %s", raw_text_url, exc,
        )
        return None

    # These URLs serve plain text. A redirect to the access-request
    # interstitial (unblock.federalregister.gov) or any HTML response is
    # the site rate-limiting this IP, not the document - confirmed live:
    # every full-text URL returned 302 -> "Federal Register :: Request
    # Access" after a burst of fetches, on every user agent, while the
    # JSON API kept answering 200. Without this the HTML would be
    # tag-stripped and stored as the document's body, which is worse
    # than storing nothing: a boilerplate "request access" page is
    # indistinguishable downstream from a real notice that says little.
    final_host = resp.url.host or ""
    content_type = resp.headers.get("content-type", "")
    if "unblock" in final_host or "html" in content_type.lower():
        logger.warning(
            "[FEDERAL_REGISTER] body fetch blocked (redirected to %s, "
            "content-type %r) url=%r - govinfo had no copy either, "
            "storing no body rather than the block page",
            final_host, content_type, raw_text_url,
        )
        return None
    text = _clean_summary(resp.text)
    if not text:
        return None
    # Each strip falls back to its own input rather than to "" - a document
    # whose lead does not match the expected shape keeps its full text, which
    # is the safe direction: a little boilerplate beats a blanked body.
    text = _FR_TEXT_LEAD_RE.sub("", text) or text
    text = _FR_TEXT_MASTHEAD_RE.sub("", text) or text
    return text[:max_chars] or None


def _fetch_federal_register_body_from_govinfo(
    document_number: str | None, publication_date: str | None,
    max_chars: int | None = _FEDERAL_REGISTER_BODY_MAX_CHARS,
) -> str | None:
    """Fetch the same document from govinfo.gov, the GPO's own site.

    Not a proxy or a workaround: govinfo.gov IS the Government
    Publishing Office's official publication system, and the Federal
    Register's own API links to it. federalregister.gov rate-limits its
    full-text URLs per IP (302 -> unblock.federalregister.gov, on every
    format - txt, xml and html alike - and on every user agent, while
    its JSON API keeps answering 200). govinfo serves the identical
    document from different infrastructure with no such limit -
    confirmed live on 4 documents returning 2.2k-22k characters each
    while federalregister.gov was returning the block page.

    The URL is deterministic from two fields the API already gives us,
    so this costs no extra lookup:
        https://www.govinfo.gov/content/pkg/FR-<pub-date>/html/<doc-no>.htm
    """
    if not document_number or not publication_date:
        return None
    url = (
        f"https://www.govinfo.gov/content/pkg/FR-{publication_date}"
        f"/html/{document_number}.htm"
    )
    # No throttle, deliberately. Measured: 12 parallel requests with no
    # delay returned 12/12 200s in 1.7s. This is a government CDN, not a
    # publisher defending its paywall, and pacing it would cost a daily
    # run real time for no benefit. If it ever starts 429ing, add
    # "govinfo.gov" to _DOMAIN_FETCH_INTERVALS and call _throttle_domain
    # here - the machinery is already there.
    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("[FEDERAL_REGISTER] govinfo fallback failed url=%r: %s", url, exc)
        return None

    # govinfo wraps the document in a <pre> block inside a minimal page,
    # so tag-stripping yields the text rather than a layout. The same
    # masthead/lead strips as the primary path then apply, since it is
    # the same document with the same preamble.
    # NUL bytes are stripped before anything else: Postgres rejects them
    # outright ("A string literal cannot contain NUL (0x00) characters"),
    # and at least one real govinfo document carries them, so without
    # this the whole insert fails rather than that one body.
    text = _clean_summary(re.sub(r"<[^>]+>", " ", resp.text).replace("\x00", ""))
    if not text:
        return None
    text = _FR_TEXT_LEAD_RE.sub("", text) or text
    text = _FR_TEXT_MASTHEAD_RE.sub("", text) or text
    text = text[:max_chars] or None
    if text:
        logger.info(
            "[FEDERAL_REGISTER] govinfo fallback recovered %d chars for %s",
            len(text), document_number,
        )
    return text


def _fetch_one_federal_register(source: dict, days_back: int) -> list[dict]:
    """Fetch Federal Register documents for a single source's agency/type filter.

    No API key required. Paginates via ``next_page_url`` until exhausted.

    Args:
        source: Source dict; ``config.agencies`` and ``config.type`` scope
            the query to specific agencies and document types.
        days_back: Exclude documents published before this many days ago.

    Returns:
        List of article dicts with ``_pub_date`` set.
    """
    config = dict(source.get("config") or {})
    agencies: list[str] = config.get("agencies", [])
    doc_types: list[str] = config.get("type", [])
    # Optional full-text filter (the API's own `|` OR syntax) and url
    # scheme. A second domain fetching documents geopolitical_news already
    # stored would lose every one of them to the global url index, so a
    # source with `url_scheme` stores {scheme}://{document_number} and
    # keeps the real link in metadata.html_url.
    terms: str | None = config.get("terms")
    url_scheme: str | None = config.get("url_scheme")
    # The 20,000-character default exists because geopolitical_news reads
    # only the first 2,000. A source may set body_max_chars to null to
    # store the whole document - the UAE A:5 rule is 33,187 characters.
    body_max_chars = config.get("body_max_chars", _FEDERAL_REGISTER_BODY_MAX_CHARS)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    params: dict = {
        "per_page": _FEDERAL_REGISTER_PAGE_SIZE,
        "order": "newest",
        "conditions[publication_date][gte]": cutoff.strftime("%Y-%m-%d"),
        "fields[]": _FEDERAL_REGISTER_FIELDS,
    }
    if agencies:
        params["conditions[agencies][]"] = agencies
    if doc_types:
        params["conditions[type][]"] = doc_types
    if terms:
        params["conditions[term]"] = terms

    t0 = time.perf_counter()
    results: list[dict] = []
    url: str | None = _FEDERAL_REGISTER_URL
    request_params: dict | None = params

    while url:
        try:
            resp = httpx.get(url, params=request_params, timeout=30.0)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("[FEDERAL_REGISTER] source=%r failed: %s", source["url"], exc)
            break

        for doc in data.get("results", []):
            pub_date = None
            try:
                pub_date = datetime.strptime(
                    doc.get("publication_date", ""), "%Y-%m-%d"
                ).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
            if pub_date and pub_date < cutoff:
                continue
            agency_names = [a.get("name", "") for a in doc.get("agencies", [])]
            # Both fields are stored, and they are not the same thing: the
            # abstract is the filing desk's own lead (often absent), the body
            # is the document. Never a slice of the other - this service keeps
            # summary a real lead or NULL everywhere else too.
            raw_text_url = doc.get("raw_text_url")
            html_url = doc.get("html_url", "")
            results.append({
                "title": doc.get("title", ""),
                "url": (f"{url_scheme}://{doc.get('document_number')}"
                        if url_scheme else html_url),
                "published": doc.get("publication_date", ""),
                "source": ", ".join(filter(None, agency_names)) or "Federal Register",
                "summary": _clean_summary(doc.get("abstract") or ""),
                # Filled in parallel after the whole listing is walked -
                # see the ThreadPoolExecutor below. Fetching inline here
                # made the loop one blocking request per document.
                "body": None,
                "_pub_date": pub_date,
                "metadata": {
                    "type": doc.get("type"),
                    "document_number": doc.get("document_number"),
                    "agencies": agency_names,
                    "executive_order_number": doc.get("executive_order_number"),
                    "raw_text_url": raw_text_url,
                    "effective_on": doc.get("effective_on"),
                    "html_url": html_url,
                    # Optional, from config - lets a domain exclude these
                    # rows from its title-similarity dedup by category.
                    **({"source_category": config["source_category"]}
                       if config.get("source_category") else {}),
                },
            })

        # next_page_url already carries the query string - drop params on
        # subsequent requests to avoid duplicating conditions.
        url = data.get("next_page_url")
        request_params = None

    # Bodies in parallel, after the listing is fully walked. govinfo.gov
    # is the primary source and takes 12 concurrent requests without
    # complaint (measured: 12/12 200s in 1.7s), so this turns a loop that
    # was one blocking fetch per document into a single wide batch - on a
    # real day's ~9 documents it is the difference between ~20 seconds
    # and ~2.
    #
    # Width matches the other body-fetch paths in this module. The
    # federalregister.gov fallback leg still throttles itself per host,
    # so a wide pool cannot stampede the host that actually rate-limits.
    to_fetch = [r for r in results if (r["metadata"] or {}).get("raw_text_url")]
    if to_fetch:
        with ThreadPoolExecutor(max_workers=10) as executor:
            bodies = list(executor.map(
                lambda r: _fetch_federal_register_body(
                    r["metadata"]["raw_text_url"],
                    r["metadata"].get("document_number"),
                    r["published"] or None,
                    body_max_chars,
                ),
                to_fetch,
            ))
        for row, body in zip(to_fetch, bodies):
            row["body"] = body
        logger.info(
            "[FEDERAL_REGISTER] fetched %d/%d bodies",
            sum(1 for b in bodies if b), len(to_fetch),
        )

    logger.info(
        "[TIMER] federal_register source=%r articles=%d elapsed=%.2fs",
        source["url"], len(results), time.perf_counter() - t0,
    )
    return results


_EVENT_REGISTRY_ENDPOINT = "https://eventregistry.org/api/v1/article/getArticles"
# One request returns at most this many; the free tier bills 1 token per
# request regardless, so asking for the maximum is strictly cheaper than
# paging.
_EVENT_REGISTRY_PAGE = 100
# Free-tier ceiling, enforced by the API: a query carrying more is
# rejected outright ("Too many keywords specified"). A MULTI-WORD phrase
# counts per word - "export controls" is 2, not 1 - so a 4-phrase query
# plus lang and two dates sits just under it.
_EVENT_REGISTRY_MAX_KEYWORDS = 15

# The 15 commonest English function words. Real prose is built out of
# them; text whose columns have been interleaved during extraction keeps
# its letters and loses its words, so this rate collapses while every
# other cheap measure (length, average token size) looks normal.
_ER_STOPWORDS = frozenset({
    "the", "and", "of", "to", "in", "for", "that", "with", "on", "as",
    "is", "are", "was", "by", "from",
})
_ER_STOPWORD_SPLIT_RE = re.compile(r"[^a-z]+")
# Below this share of stopwords the body is not prose and is dropped.
#
# Measured over 304 real stored bodies from this domain: mean 0.222,
# 5th percentile 0.168, and the ONE corrupt row 0.004 - a 40x gap with
# nothing between 0.10 and 0.168, so the floor has wide margin in both
# directions. The corrupt row was ibj.com served through Event Registry
# as 5,220 characters reading "i tgr c a taoPufitoboadarylsmdaaibuit a
# sryww ecnavm rutRey aggSoCn" - letters from adjacent columns of a
# multi-column page, interleaved.
#
# Scoped to this fetcher deliberately. Event Registry is the one source
# here whose body text is a THIRD PARTY's crawl of an arbitrary
# publisher's rendered page, so it is the only one that can hand us
# someone else's extraction failure. Federal Register bodies come from
# a government API's raw_text_url and cannot take this shape.
#
# ibj.com now answers our own fetch with a 403 JavaScript challenge,
# which is the likely cause: a bot-protected page scraped anyway. That
# is also why this is a validator and not a fix - the text arrives
# broken from the supplier and there is no extraction step of ours to
# correct.
_ER_MIN_STOPWORD_RATE = 0.10
# Short bodies are exempt: a 20-word body can legitimately contain few
# function words, and the rate is too noisy to judge on. Every real
# corrupt sample seen has been thousands of characters.
_ER_STOPWORD_MIN_CHARS = 400


def _is_readable_prose(body: str) -> bool:
    """True when ``body`` reads as English prose rather than scrambled text.

    Returns True for anything shorter than _ER_STOPWORD_MIN_CHARS - the
    measure needs enough words to be stable, and fail-open is the right
    direction: a body wrongly dropped is evidence lost, while a body
    wrongly kept is what the caller already had.
    """
    if len(body) < _ER_STOPWORD_MIN_CHARS:
        return True
    tokens = body.split()
    if not tokens:
        return False
    words = _ER_STOPWORD_SPLIT_RE.split(body.lower())
    hits = sum(1 for w in words if w in _ER_STOPWORDS)
    return (hits / len(tokens)) >= _ER_MIN_STOPWORD_RATE


def _er_body_or_none(body: str | None, url: str | None) -> str | None:
    """Return ``body``, or None when it is not readable prose.

    Logged at WARNING rather than dropped quietly: a body silently
    becoming NULL looks exactly like a publisher that served nothing,
    and the two call for different responses - the first is a supplier
    defect worth watching, the second is normal.
    """
    if not body:
        return None
    if _is_readable_prose(body):
        return body
    logger.warning(
        "[EVENT_REGISTRY] dropping unreadable body (%d chars, not prose)"
        " for %s", len(body), url or "(no url)",
    )
    return None


def _fetch_one_event_registry(
    source: dict, days_back: int, api_key: str,
) -> list[dict]:
    """Fetch one Event Registry (newsapi.ai) query.

    Unlike every other news source here, the article BODY arrives with
    the response - so none of the fetch tiers (Trafilatura, curl_cffi,
    archive.ph, the reader proxy) runs for these rows. Measured over one
    real day: 245 articles, 245 with body, average 3,501 characters.

    `keywordLoc: "title"` is deliberate and was measured: the same terms
    matched against the body returned 11,020 results for a single day,
    including Formula 1 and local gas prices, against 565 title-matched
    with usable precision.

    Every enrichment field is opt-in and silently absent without its
    flag - a first version stored 245 rows with concepts=[] because one
    flag was missing - so each is requested explicitly below.
    """
    config = source.get("config") or {}
    terms = config.get("keywords") or []
    # A source may instead carry a whole Event Registry condition in
    # `query` - concepts, outlets and keyword locations combined - which
    # the Middle East queries need: an entity alone was measured to match
    # astronomy ("helium") and IPO anchor books (the Gulf funds), so each
    # pairs an entity with a topic term. Language and date are still added
    # here, so no query can drift outside the run's window.
    condition = config.get("query") or (
        {"$or": [{"keyword": t, "keywordLoc": "title"} for t in terms]}
        if terms else None)
    if not condition:
        return []
    # See _fetch_one_federal_register: a second domain storing articles
    # another domain's query also matches needs its own url scheme, or the
    # global url index drops them. The real link moves to
    # metadata.article_url.
    url_scheme: str | None = config.get("url_scheme")
    end = datetime.now(timezone.utc).date()
    start = end - timedelta(days=max(days_back, 1))
    payload = {
        "query": {"$query": {"$and": [
            condition,
            {"lang": "eng"},
            {"dateStart": str(start), "dateEnd": str(end)},
        ]},
        # Drop near-identical reprints at the API, before they cost a
        # row, a Stage A insert and a Stage B model call. Measured on
        # one real day: 187 matches without it, 142 with - 45 syndicated
        # copies of stories already in the set.
        #
        # The url dedup further down cannot catch these: a reprint
        # carries a DIFFERENT url, so it reads as a new article. This is
        # also distinct from event_uri grouping, which links separate
        # outlets genuinely reporting one event (7 different headlines
        # on one UN Haiti vote) rather than copies of one wire story.
        "$filter": {"isDuplicate": "skipDuplicates"}},
        "articlesCount": _EVENT_REGISTRY_PAGE,
        "articleBodyLen": -1,          # full body; the default truncates
        "articlesSortBy": "date",
        "includeArticleConcepts": True,
        "includeArticleCategories": True,
        "includeArticleLocation": True,
        "includeArticleImage": True,
        "includeArticleSentiment": True,
        "includeArticleEventUri": True,
        "includeArticleAuthors": True,
        "includeArticleOriginalArticle": True,
        "includeArticleSocialScore": True,
        "includeArticleExtractedDates": True,
        "includeArticleDuplicateList": True,
        "includeArticleStoryUri": True,
        "includeSourceTitle": True,
        "includeSourceDescription": True,
        "includeSourceLocation": True,
        "includeSourceRanking": True,
        "apiKey": api_key,
    }
    try:
        resp = httpx.post(
            _EVENT_REGISTRY_ENDPOINT, json=payload, timeout=90.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning(
            "[EVENT_REGISTRY] source=%r failed: %s", source.get("url"), exc,
        )
        return []
    if "articles" not in data:
        # The API reports quota and query errors as a 200 with an
        # "error" key, so a bare status check would read a rejected
        # query as an empty day.
        logger.warning(
            "[EVENT_REGISTRY] source=%r rejected: %s",
            source.get("url"), str(data.get("error") or data)[:200],
        )
        return []

    results = data["articles"]["results"]
    total = data["articles"].get("totalResults") or 0
    # Further pages only when the source asks for them (`max_pages`,
    # default 1): each page costs a token. The Middle East shipping query
    # matched 108 in one day against the 100-article page.
    for page in range(2, int(config.get("max_pages") or 1) + 1):
        if len(results) >= total:
            break
        try:
            more = httpx.post(_EVENT_REGISTRY_ENDPOINT,
                              json=dict(payload, articlesPage=page), timeout=90.0)
            more.raise_for_status()
            batch = (more.json().get("articles") or {}).get("results") or []
        except Exception as exc:
            logger.warning("[EVENT_REGISTRY] source=%r page %d failed: %s",
                           source.get("url"), page, exc)
            break
        if not batch:
            break
        results.extend(batch)
    logger.info(
        "[EVENT_REGISTRY] source=%r matched=%s fetched=%d",
        source.get("url"), total, len(results),
    )

    articles: list[dict] = []
    for raw in results:
        published = raw.get("dateTimePub") or raw.get("dateTime")
        pub_date = None
        if published:
            try:
                pub_date = datetime.fromisoformat(published.replace("Z", "+00:00"))
            except ValueError:
                pass
        articles.append({
            # --- COLUMNS on `articles`, shared by every domain -----
            # These are this service's own schema, not Event Registry's,
            # so they keep their existing names and are NOT duplicated
            # into metadata below under the supplier's spelling.

            # title: the headline, verbatim. Never rewritten - it is
            # also the dedup and also_reported_by matching key.
            "title": raw.get("title", ""),
            # url: the article's own address at the publisher.
            "url": raw.get("url", ""),
            # published: WHEN IT WAS PUBLISHED - their dateTimePub,
            # deliberately not dateTime (which is when Event Registry
            # crawled it, and is kept separately in metadata). A crawl
            # time would silently shift every article forward by
            # minutes to hours.
            "published": published,
            # source: the outlet's DISPLAY name ("The Hindu"). The
            # matchable domain is metadata.source_uri - this field is
            # for a reader, that one is for code.
            "source": (raw.get("source") or {}).get("title"),
            # summary: deliberately NULL. Event Registry returns the
            # body and never an editorial lead, and this service keeps
            # summary a real lead or NULL rather than a slice of the
            # body - the same rule the China and Japan sources follow.
            # Stage C generates one downstream where it needs it.
            "summary": None,
            # body: THE FULL ARTICLE TEXT, which is the reason this
            # source is worth having. It arrives WITH the response, so
            # none of the fetch tiers (Trafilatura, curl_cffi,
            # archive.ph, the reader proxy) runs for these rows -
            # measured 245/245 with body, average 3,501 characters,
            # against 12/114 on the fetched-RSS path.
            #
            # Validated before storing, because it is THEIR crawl of
            # someone else's page: one row in 325 arrived as 5,220
            # characters of column-interleaved letters (see
            # _is_readable_prose). NULL rather than the text, so the
            # row keeps its headline and downstream judges on that -
            # dropping the article would discard a real event over a
            # supplier-side extraction fault, and keeping the text
            # would present noise as evidence.
            "body": _er_body_or_none(raw.get("body"), raw.get("url")),
            # _pub_date: the parsed form of `published`, which
            # create_articles uses for the days_back window. Internal
            # to this service's fetchers; renamed to `published` before
            # storage.
            "_pub_date": pub_date,
            "metadata": {
                # source_type: OURS, not theirs - which fetcher wrote
                # this row. Every domain in this service stamps it, and
                # it is what makes "where did this article come from"
                # answerable without inspecting the url.
                "source_type": "eventregistry",

                # --- Event Registry's own fields -------------------
                # Stored under THEIR names, snake_cased (every key in
                # this service is snake_case, and Postgres folds
                # unquoted identifiers to lowercase, so a camelCase key
                # is a query trap). The word is theirs; only the
                # spelling convention is ours.
                #
                # url / title / published / body / source are NOT here:
                # they are first-class columns on `articles`, shared by
                # every domain, and storing them twice would be the
                # same value under two names.

                # uri: WHICH ARTICLE this is, in Event Registry's own
                # numbering ("9506780276"). Stable across requests, so
                # it is the natural dedup key if two fetches disagree
                # about a url (tracking parameters, http vs https).
                # Distinct from story_uri and event_uri below, which
                # identify GROUPS rather than this one article.
                "uri": raw.get("uri"),
                # event_uri (their eventUri): cluster id for a
                # real-world event. Articles sharing one report the SAME
                # underlying action - measured: 7 outlets with different
                # headlines on one UN Haiti sanctions vote. Absent on
                # single-source stories (59 of 100 had one).
                "event_uri": raw.get("eventUri"),
                # story_uri (their storyUri): WHICH ONGOING STORY this
                # belongs to - a coarser grouping than event_uri, meant
                # to link related coverage over time rather than one
                # discrete action. Measured 66/100, the same rows that
                # carry an event_uri, and on this sample the two values
                # were identical - so it adds nothing here yet, and is
                # kept only because a longer-running story would be
                # where the difference appears.
                "story_uri": raw.get("storyUri"),
                # is_duplicate (their isDuplicate): this article is a
                # near-identical reprint of another. The query already
                # asks the API to skip these ($filter skipDuplicates),
                # so it should be false on everything fetched.
                "is_duplicate": raw.get("isDuplicate"),
                # duplicate_list (their duplicateList): the uris of
                # those reprints, populated only on the CANONICAL
                # article. Empty in practice once skipDuplicates is on.
                "duplicate_list": [
                    d.get("uri") for d in (raw.get("duplicateList") or [])
                    if isinstance(d, dict) and d.get("uri")
                ],
                # original_article (their originalArticle): where this
                # one was copied from, when it is itself a copy.
                "original_article": raw.get("originalArticle"),

                # concepts: WHO AND WHAT the article is about, as
                # Event Registry extracted it - named entities with a
                # score (5 = the article's subject, 1 = a passing
                # mention) and a type (wiki / org / loc / person).
                #
                # An INDEPENDENT read of the question Stage C's own
                # entity extraction answers, which is the point of
                # keeping it: the two can be compared rather than one
                # trusted blindly. Measured against Stage C on the same
                # articles, Stage C wins on the part that matters - on a
                # Belarus sanctions fine ER returned "Euro, Belarus"
                # while Stage C named Dage Capital and Alfa-Bank, the
                # actually-sanctioned firms - because concepts are
                # topical tags with no reason text, no actor/target
                # distinction, and no named-in-text check. Useful as a
                # RECALL check (a company ER names that Stage C missed
                # is a prompt bug worth finding), not as a source.
                "concepts": [
                    {"label": c["label"]["eng"], "score": c.get("score"),
                     "type": c.get("type")}
                    for c in (raw.get("concepts") or [])
                    if c.get("label", {}).get("eng")
                ],
                # categories: WHAT SUBJECT AREA is this, on two
                # published taxonomies - DMOZ (the old web directory)
                # and IPTC (the news industry's own). Each carries a
                # weight, 0-100, for how strongly it applies:
                #   {"label":"iptc/economy, business and finance/
                #             products and services/energy and resource",
                #    "wgt":100}
                # 100% coverage, but these are BROAD subject buckets,
                # not the AI-economy transmission question
                # impacted_categories answers downstream. A story can be
                # "dmoz/Business/Energy" and touch no category in this
                # pipeline's taxonomy at all.
                "categories": [
                    {"label": c.get("label"), "wgt": c.get("wgt")}
                    for c in (raw.get("categories") or [])
                ],
                # extracted_dates (their extractedDates): dates found IN
                # the text. `implicit` marks one resolved from a relative
                # phrase ("today", "Wednesday") rather than stated.
                # Measured: on a real sanctions day EVERY extracted date
                # was implicit and equal to the publication date, so this
                # cannot supply event_date - but it can CHECK one: a
                # Stage C event_date far from publication, on an article
                # whose only dates are implicit, is the shape of the
                # fabrications caught live (two rows claiming 2023-10-09).
                "extracted_dates": [
                    {"date": d.get("date"), "implicit": d.get("imp"),
                     "ambiguous": d.get("amb")}
                    for d in (raw.get("extractedDates") or [])
                    if isinstance(d, dict) and d.get("date")
                ],
                # authors: WHO WROTE IT. Their `authors` is a list of
                # objects (uri, name, type, isAgency); only the name is
                # kept, since the rest is Event Registry's own author
                # identity graph and nothing here resolves against it.
                # 50/100 coverage - wires and aggregators often carry no
                # byline.
                "authors": [
                    a.get("name") for a in (raw.get("authors") or []) if a.get("name")
                ],
                # location: WHERE THE EVENT HAPPENED - their docs say
                # "the geographic location that the event mentioned in
                # the article is about". It is the place the story is
                # about, NOT where the outlet sits; source_country and
                # source_location below answer that separate question.
                #
                # Shape varies: either a bare country
                #   {"type":"country","label":{"eng":"Canada"}}
                # or a place nested under one
                #   {"type":"place","label":{"eng":"Ontario"},
                #    "country":{"label":{"eng":"Canada"}}}
                # so a reader must handle both rather than assuming
                # `location.country` exists.
                #
                # Measured 7/100 on a real geopolitical day, and the
                # ones it DOES fill are the least useful here - Canada,
                # Ontario, Maharashtra, i.e. local stories that matched
                # a keyword ("Maharashtra Govt sanctions Rs 17 crore").
                # Event Registry geolocates a provincial budget
                # confidently and a Treasury determination not at all,
                # because a policy action has no place attached the way
                # a protest or an earthquake does. Stored raw because it
                # costs nothing; do not build on it.
                "location": raw.get("location"),
                # image: the article's lead image url. Stored because
                # it arrives free; nothing in this pipeline reads it.
                "image": raw.get("image"),
                # lang: WHAT LANGUAGE the article is in, ISO 639-3
                # ("eng"). Constant for this domain - every query asks
                # for {"lang":"eng"} - so it is an assertion that the
                # filter held, not a variable.
                "lang": raw.get("lang"),
                # data_type (their dataType): WHAT KIND of content -
                # "news", "pr" (a press release) or "blogs". Worth
                # keeping distinct: a PR wire item is a company talking
                # about itself, which Stage B should read as a statement
                # rather than an action. Every row here is "news"
                # because the query does not ask for the others.
                "data_type": raw.get("dataType"),
                # date_time (their dateTime): when Event Registry
                # CRAWLED it. The `published` column carries
                # dateTimePub, the publication time, which is the one
                # that matters.
                "date_time": raw.get("dateTime"),

                # --- source sub-object, flattened ------------------
                # source_uri (their source.uri): WHICH OUTLET
                # published it, as a bare domain ("livemint.com"). This
                # is the dedup and allowlist key everywhere else in this
                # service, and it is more reliable than the `source`
                # COLUMN, which carries a display title ("The Hindu")
                # that cannot be matched against a domain list.
                "source_uri": (raw.get("source") or {}).get("uri"),
                # source_country / source_location (their
                # source.location.country.label.eng and .label.eng):
                # WHERE THE OUTLET IS BASED - "India" / "Chennai". The
                # counterpart to `location` above, which is where the
                # EVENT happened: a Jamaican paper reporting a UN vote
                # in New York has source_country "Jamaica" and an event
                # location of Haiti or the US, and the two must not be
                # read as the same fact.
                #
                # Bears on corroboration: one Russian state outlet and
                # one US wire are not two independent reports, the
                # distinction Stage D's allowlist filter makes by hand.
                # ~50% coverage, against 7% for the event location.
                "source_country": (
                    ((raw.get("source") or {}).get("location") or {})
                    .get("country", {}).get("label", {}).get("eng")
                ),
                "source_location": (
                    ((raw.get("source") or {}).get("location") or {})
                    .get("label", {}).get("eng")
                ),
                # source_importance_rank (their
                # source.ranking.importanceRank): LOWER is better -
                # New York Post 1,581, The Telegraph 2,712. 1,000,000 is
                # a sentinel for "unranked", NOT "worst": The Kyiv
                # Independent and Chosun Ilbo both sit there, so
                # filtering on this naively would discard real outlets.
                "source_importance_rank": (
                    ((raw.get("source") or {}).get("ranking") or {})
                    .get("importanceRank")
                ),

                # --- scores: stored as returned, NOT acted on ------
                # sentiment: tone of the prose, -1..+1. Measured to
                # track the OUTLET'S WRITING, not the direction of a
                # measure - two outlets covering one EU sanctions
                # package scored +0.271 and -0.075. Event Registry's own
                # docs define no bullish/bearish reading; their README
                # example treats >= 0.4 as "positive" for celebrity
                # coverage.
                "sentiment": raw.get("sentiment"),
                # relevance: HOW WELL this matched the query - but a
                # constant 1 in practice, because it only varies when
                # articlesSortBy="rel", and this fetcher sorts by date
                # so the newest articles arrive rather than the ones an
                # opaque scorer liked most. Stored so the constancy is
                # visible rather than assumed.
                "relevance": raw.get("relevance"),
                # sim: HOW SIMILAR this article is to the first one in
                # its event cluster, 0-1. The trap: 0.00 marks the
                # cluster HEAD itself, not "unrelated" - so a naive
                # "drop anything below 0.3" would drop exactly the
                # canonical article of every event.
                "sim": raw.get("sim"),
                # wgt: Event Registry's internal sort weight, an
                # opaque integer (~529,787,429) with no documented
                # meaning or scale. Stored because it is returned;
                # nothing can legitimately be inferred from it.
                "wgt": raw.get("wgt"),
                # shares: HOW WIDELY SHARED on social platforms -
                # Facebook, LinkedIn and others. Returns {} on this
                # plan, so the field exists and the data does not.
                "shares": raw.get("shares"),
            },
        })
    if url_scheme:
        for a, raw in zip(articles, results):
            a["metadata"]["article_url"] = a["url"]
            a["metadata"]["er_query"] = source.get("url")
            a["url"] = f"{url_scheme}://{raw.get('uri')}"
    if config.get("require_body"):
        # geopolitical_news keeps a row whose body failed the prose check
        # (the headline still carries the event); a domain whose consumer
        # judges only on text asks for those rows to be dropped instead -
        # and also for the ones whose body is only PART of the article.
        kept = [a for a in articles
                if a["body"] and not _er_body_is_partial(a)]
        if len(kept) < len(articles):
            logger.info("[EVENT_REGISTRY] source=%r dropped %d without a full body",
                        source.get("url"), len(articles) - len(kept))
        articles = kept
    return articles


# Outlets whose Event Registry body is only ever the opening of the
# article, measured over 871 stored rows: Bloomberg 3/3, Telegraph 3/3 and
# Bloomberg Law 2/2 end in "..." (paywall teaser); Reuters is the first
# paragraph plus reporting credits (4 of 5 under 700 characters); The
# Africa Report is a paywall teaser (checked by hand). The repo's page
# fetch tiers do not recover them either: Reuters returned nothing,
# Telegraph only its navigation, Bloomberg one more paragraph inside page
# chrome. Their stories reach this domain through the outlets that
# syndicate or re-report them.
_ER_PARTIAL_BODY_OUTLETS: frozenset[str] = frozenset({
    "bloomberg.com", "telegraph.co.uk", "news.bloomberglaw.com",
    "reuters.com", "theafricareport.com",
})
# "..." / "…" and the bracketed "[...]" a syndicated teaser ends on.
_ER_TEASER_END_RE = re.compile(r"(\.\.\.|…|\[\.\.\.\]|\[…\])\s*$")
# A video or podcast page has a description, not an article - 8 of 871.
_ER_MEDIA_PAGE_RE = re.compile(r"/(videos?|podcasts?|watch)/", re.IGNORECASE)
# Every one of 851 stored bodies under this length was partial - a video
# series' standing blurb (The National's "Manus on Markets", 263), a
# teaser (cryptonews, 269), Bloomberg/Telegraph teasers. The shortest
# complete item was a 310-character liveblog entry, which stays.
_ER_MIN_FULL_BODY_CHARS = 300


def _er_body_is_partial(article: dict) -> bool:
    """True when an Event Registry body is a teaser, a media-page blurb,
    from an outlet whose body is only ever an opening, or shorter than any
    complete body measured."""
    meta = article.get("metadata") or {}
    body = article.get("body") or ""
    return bool(
        meta.get("source_uri") in _ER_PARTIAL_BODY_OUTLETS
        or len(body) < _ER_MIN_FULL_BODY_CHARS
        or _ER_TEASER_END_RE.search(body)
        or _ER_MEDIA_PAGE_RE.search(meta.get("article_url") or article.get("url") or ""))


def _fetch_event_registry(
    sources: list[dict], days_back: int, api_key: str,
) -> list[dict]:
    """Fetch every Event Registry query in parallel, dedup by url.

    One token per query on the free tier, so the five seeded queries
    cost five tokens a day against a 2,000-token allowance.
    """
    started = time.perf_counter()
    articles: list[dict] = []
    with ThreadPoolExecutor(max_workers=min(len(sources), 5)) as executor:
        for batch in executor.map(
            lambda s: _fetch_one_event_registry(s, days_back, api_key), sources,
        ):
            articles.extend(batch)

    seen: set[str] = set()
    unique: list[dict] = []
    for article in articles:
        url = article.get("url")
        if url and url not in seen:
            seen.add(url)
            unique.append(article)

    logger.info(
        "[TIMER] event_registry sources=%d articles=%d (%d after url dedup)"
        " elapsed=%.2fs",
        len(sources), len(articles), len(unique), time.perf_counter() - started,
    )
    return unique


def _fetch_federal_register(sources: list[dict], days_back: int) -> list[dict]:
    """Fetch Federal Register documents for multiple sources in parallel.

    Args:
        sources: List of federal_register source dicts with ``config``
            carrying ``agencies`` and ``type`` filters.
        days_back: Exclude documents published before this many days ago.

    Returns:
        List of article dicts with ``_pub_date`` set.
    """
    articles: list[dict] = []
    t0 = time.perf_counter()
    with ThreadPoolExecutor(max_workers=5) as executor:
        for source_articles in executor.map(
            partial(_fetch_one_federal_register, days_back=days_back),
            sources,
        ):
            articles.extend(source_articles)
    logger.info(
        "[TIMER] federal_register total: sources=%d articles=%d elapsed=%.2fs",
        len(sources), len(articles), time.perf_counter() - t0,
    )
    return articles


_GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
_GDELT_MIN_INTERVAL = 15.0  # GDELT's documented floor is 5s; observed to tighten under load -
# bumped from 10.0 after a live run showed 17/29 queries failing (429s, SSL handshake
# timeouts, connection resets) even at 10s spacing for the Taiwan per-company-name query
# pattern specifically (vs. geopolitical_news's broader theme queries, which succeed
# reliably at the same interval) - this alone won't fix the separate, larger loss from
# the company-name-in-title filter (Path 3 Stage A) dropping most surviving results.
_GDELT_MAXRECORDS = 250
_GDELT_MAX_ROUNDS = 2  # round-robin passes over rate-limited queries before giving up -
# lowered from 3 after a real run (2026-09-24) showed GDELT rate-limiting every single
# query through round 2, with round 3 recovering nothing - a third round only adds
# runtime (up to 27 queries * _GDELT_MIN_INTERVAL each) with no evidence it typically
# helps; if a future run shows round 3 reliably recovering a meaningful number of
# queries, revisit this rather than assuming today's outlier is representative.
_GDELT_USER_AGENT = (
    "Mozilla/5.0 (compatible; ocn-news-retrieval/1.0;"
    " +https://opengrowthventures.com)"
)


_GDELT_RATE_LIMITED = object()  # sentinel: distinguishes 429 from "no results"

# Title-similarity dedup for Taiwan GDELT articles: two outlets covering the
# same underlying fact will use different wording, so exact URL/title
# matching can't catch it - only ticker-scoped Taiwan GDELT rows carry a
# "ticker" in metadata (set via source config's query_ticker map), so this
# check is a no-op for the geopolitical_news GDELT source, which has no
# ticker concept. Small model (1536 dims) since only short titles are
# compared, not full article bodies, within a narrow same-ticker window.
_TITLE_EMBEDDING_MODEL = "openai/text-embedding-3-small"
_TITLE_DEDUP_WINDOW_HOURS = 24
_TITLE_DEDUP_SIMILARITY_THRESHOLD = 0.90


def _cosine_similarity(a: list[float], b: list[float]) -> float:
    """Cosine similarity between two equal-length vectors."""
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = sum(x * x for x in a) ** 0.5
    norm_b = sum(y * y for y in b) ** 0.5
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def _publisher_url(article: dict) -> str:
    """The article's address at its publisher. A source stored under its
    own url scheme (er-me://{uri}, see _fetch_one_event_registry) keeps
    the real link in metadata.article_url - reading `url` there would name
    the outlet "9509743911"."""
    return ((article.get("metadata") or {}).get("article_url")
            or article.get("url") or "")


def _extract_domain_for_also_reported_by(article: dict) -> str | None:
    """Return the registrable domain for an also_reported_by entry, derived
    from the article's own url - never from article["source"].

    Confirmed live: article["source"] is a display name ("PBS", "Bloomberg",
    "Bloomberg Law News", "ABC News - Breaking News, Latest News and
    Videos"), not a domain - both dedup call sites in this module were
    storing that raw display name into also_reported_by, so any downstream
    consumer comparing against a domain allowlist (signal-detection-agent's
    Stage D corroboration check) could never match it, silently making
    corroboration structurally unreachable. Same "www." stripping as
    signal-detection-agent's Stage A _extract_domain, so a value written
    here is directly comparable against that service's ALLOWED_DOMAINS
    without further normalization.
    """
    url = _publisher_url(article)
    host = urlparse(url).hostname or ""
    host = host.lower()
    if host.startswith("www."):
        host = host[len("www."):]
    return host or None


# How much of a discarded duplicate's headline is kept. Long enough for
# every real headline measured in this domain (longest 118 characters),
# short enough that one pathological title cannot bloat the row.
_ALSO_REPORTED_TITLE_CHARS = 200


def _also_reported_entry(article: dict) -> dict | None:
    """Return {outlet, title, url} for an article being DISCARDED as a
    near-duplicate, or None when it carries no usable domain.

    The outlet alone used to be stored, which told a reader that
    somebody else had the story but never what they called it. The
    headline is the part worth having: seven outlets on one Strait of
    Hormuz attack wrote "projectile attacks", "naval mine explosion"
    and "Iran expands attacks on shipping", and that disagreement is
    invisible from a list of domains.

    This is the only moment the discarded article exists - it is
    dropped on the next line and never stored - so the fields are
    copied here or lost.

    Shape matches the siblings signal-detection-agent's Stage D builds
    from Event Registry's event_uri clusters, so one consumer renders
    both without caring which mechanism found the duplicate.
    """
    outlet = _extract_domain_for_also_reported_by(article)
    if not outlet:
        return None
    title = " ".join((article.get("title") or "").split())
    return {
        "outlet": outlet,
        "title": title[:_ALSO_REPORTED_TITLE_CHARS],
        "url": _publisher_url(article),
    }


def _is_self_reference(
    entry: dict, batch_article: dict | None, article: dict,
) -> bool:
    """True when the entry names the SAME article it would be attached to.

    Two Federal Register notices filed the same day ("Notice of OFAC
    Sanctions Action" and "...Actions") matched on title similarity, and
    each recorded the other, leaving rows whose also_reported_by read
    ["federalregister.gov"] - the Federal Register corroborating itself.
    A publisher is not independent confirmation of its own document, and
    a reader counting outlets would have counted that one twice.

    Matched on url first, since that identifies the document; the
    surviving row's own domain is the fallback when the kept article is
    a stored row rather than a batch member.
    """
    url = (entry.get("url") or "").strip().lower()
    if batch_article is not None:
        kept_url = _publisher_url(batch_article).strip().lower()
        if url and url == kept_url:
            return True
        kept_outlet = _extract_domain_for_also_reported_by(batch_article)
        return bool(kept_outlet and kept_outlet == entry.get("outlet"))
    # Cross-run match: the kept row is in the database, not in hand, so
    # its domain cannot be compared here. append_also_reported_by does
    # that check against the stored row instead.
    return False


_EMBED_TITLES_MAX_ATTEMPTS = 3
_EMBED_TITLES_RETRY_BACKOFF_SECS = 2.0


def _embed_titles(titles: list[str], api_key: str | None) -> list[list[float] | None]:
    """Embed a batch of titles via OpenRouter, same model class already used
    by signal-detection for lightweight claim-level comparison (as opposed
    to the larger text-embedding-3-large used there for full-body
    clustering — titles are short, a smaller model is enough and cheaper).

    Returns one embedding per title, in order; a title's slot is None if
    it was blank or every attempt failed, so callers must treat missing
    embeddings as "cannot compare" rather than "definitely not a duplicate"
    (fail-open - never silently drop an article because embedding failed).

    Blank/whitespace-only titles are filtered out before the API call and
    always map to None, never sent. Confirmed live: OpenRouter's embeddings
    endpoint rejects the ENTIRE batch with one 400 ("too_small": a string
    must have >=1 characters) if even one input is blank - and since the
    request is unchanged, every retry fails identically, so one blank title
    among 1362 real ones silently zeroed out embedding coverage (and so
    dedup) for the whole run. 6 of the 11 most recent geopolitical_news
    daily runs hit this exact failure before the filter was added here -
    not a rare blip, roughly half of all runs. Confirmed same-story
    duplicates across up to 4 different outlets (e.g. "EU lifts sanctions
    on Russian oligarchs Usmanov/Fridman", reuters.com/kyivpost.com/ft.com/
    france24.com) went unmerged as a direct result.

    Retries up to _EMBED_TITLES_MAX_ATTEMPTS times with a short fixed
    backoff before falling back to all-None for whatever's left in the
    batch (now only real, non-blank titles - a transient network/API error
    remains the only retryable failure mode). Confirmed live, separately:
    a single transient connection error on this call (no retry, at the
    time) caused an entire 369-article geopolitical_news fetch to skip
    dedup completely. A one-off network blip failing open for a handful of
    unresolvable titles is an acceptable, rare cost; failing open for an
    entire day's fetch on the first retry-free error is not.
    """
    if not titles:
        return []
    results: list[list[float] | None] = [None] * len(titles)
    embeddable_indices = [i for i, t in enumerate(titles) if t and t.strip()]
    if not embeddable_indices:
        return results
    embeddable_titles = [titles[i] for i in embeddable_indices]

    last_exc: Exception | None = None
    for attempt in range(1, _EMBED_TITLES_MAX_ATTEMPTS + 1):
        try:
            client = _make_client(api_key)
            response = client.embeddings.create(
                model=_TITLE_EMBEDDING_MODEL, input=embeddable_titles,
            )
            for i, item in zip(embeddable_indices, response.data):
                results[i] = item.embedding
            return results
        except Exception as exc:
            last_exc = exc
            if attempt < _EMBED_TITLES_MAX_ATTEMPTS:
                logger.warning(
                    "[GDELT] title embedding attempt %d/%d failed for batch"
                    " of %d, retrying: %s",
                    attempt, _EMBED_TITLES_MAX_ATTEMPTS, len(embeddable_titles), exc,
                )
                time.sleep(_EMBED_TITLES_RETRY_BACKOFF_SECS)
    logger.warning(
        "[GDELT] title embedding failed for batch of %d after %d attempts: %s",
        len(embeddable_titles), _EMBED_TITLES_MAX_ATTEMPTS, last_exc,
    )
    return results


# Below _SAME_EVENT_SIMILARITY_FLOOR, two titles are treated as definitely
# not the same story - no LLM call. Set from real geopolitical_news data:
# the lowest similarity measured between two confirmed-same-story headlines
# ("EU Delists 2 Oligarchs, Extends Russia Sanctions for 3 Years..." vs
# "EU to Extend Russia Sanctions After Latvia Drops Opposition to Oligarch
# Delistings", both about the same September 2026 EU Council decision) was
# 0.5951 - the floor is set just below that, not at an arbitrary round
# number. At or above _DOMAIN_TITLE_DEDUP_SIMILARITY_THRESHOLD (0.82 for
# geopolitical_news), two titles are treated as definitely the same story -
# also no LLM call, matching existing behavior exactly. Only the band
# between the two calls _titles_describe_same_event - see that function's
# docstring for why title similarity alone can't resolve this band.
_SAME_EVENT_SIMILARITY_FLOOR = 0.50

_SAME_EVENT_SYSTEM_PROMPT = (
    "You judge whether two news headlines report the SAME underlying event"
    " - the same specific government/institutional action, decision,"
    " ruling, signing, or announcement - even if the two headlines lead"
    " with different details, emphasize different consequences, or use"
    " different wording for the same outcome. Different outlets covering"
    " one real-world action commonly emphasize different angles of it and"
    " use different phrasing for the same outcome - this is still the SAME"
    " event.\n\n"
    "ASK TWO QUESTIONS, IN THIS ORDER.\n"
    "1. WHAT STAGE is each headline at? Proposal, offer, talks, agreement"
    " in principle, signing, implementation, withdrawal, collapse. If the"
    " two are at DIFFERENT stages, they are DIFFERENT events - stop there,"
    " however identical the parties, the amount and the sector. 'X"
    " proposes a $21bn deal' and 'X and Y strike a $21bn deal' are two"
    " events: one is a proposal and one is a conclusion.\n"
    "2. Only if the stage matches, ask WHOSE SIDE each is told from. Two"
    " headlines at the same stage, told from different participants'"
    " sides, are ONE event.\n\n"
    "Watch for these specific traps:\n"
    "- 'Lifting sanctions on X', 'removing X from the sanctions list', and"
    " 'delisting X' all mean the SAME thing (X is no longer sanctioned) -"
    " never treat these as opposite or different actions.\n"
    "- A single vote or decision that both extends/renews a sanctions"
    " package AND delists specific individuals as part of that same vote"
    " is ONE event, not two - a headline emphasizing only the extension and"
    " another emphasizing only the delisting can both be reporting the"
    " exact same Council/legislative action.\n"
    "- ONE agreement between two or more parties is ONE event however many"
    " sides it is told from. Each participant's press operation leads with"
    " its own actor and its own verb, so the same signing appears as 'A"
    " announces investment', 'B to provide financial support', 'A and B"
    " strike deal' and 'A, B build plant'. A DIFFERENT NAMED ACTOR IS NOT"
    " EVIDENCE OF A DIFFERENT EVENT when the deal, the amount, the sector"
    " and the location match. The same applies to one party's contribution"
    " to a joint action: funding, hosting, approving or signing are parts"
    " of the one deal, not separate deals.\n"
    "  This clause is about WHO is telling it, never about WHEN. It does"
    " not merge different stages of the same deal: a proposal, an offer,"
    " talks, an agreement in principle, the signing, and a later"
    " withdrawal are separate events even when the parties, the amount"
    " and the sector are identical. Check the stage first; only then ask"
    " whose side it is told from.\n"
    "  Nor does it merge an action with a reaction to it. One party"
    " acting and another condemning, welcoming or responding are two"
    " events, even where both name the same measure.\n"
    "- A shared figure, project or location is strong evidence of one"
    " event. Two headlines naming the same amount ('$21bn', '$21 Billion'),"
    " the same named project, or the same specific site are almost always"
    " the same action reported twice. Only ONE of the two needs to carry"
    " the figure: 'A announces $21bn investment in New York plants' and 'B"
    " to provide financial support for New York plant' are the same deal,"
    " since the second is B's half of the first. Do not require both"
    " headlines to repeat the same details before calling them one event -"
    " that is precisely what different outlets do not do.\n\n"
    "Treat two headlines as DIFFERENT events only when they describe a"
    " genuinely different action, decision, or moment in time: e.g. a"
    " proposal, attempt, or failed vote vs. its later actual approval; an"
    " announcement vs. a separate, later reaction to it; or two distinct"
    " developments on different days in a multi-day story. A running story"
    " does produce genuinely separate events - one vessel struck, a weekly"
    " casualty count, a warning about consequences and a statement of"
    " intent are four events even when all four name the same place. Ask"
    " what HAPPENED in each: different happenings are different events,"
    " but one happening told from two sides is one event."
    ' Return strict JSON only: {"same_event": true or false}'
)


_SAME_EVENT_LLM_CONCURRENCY = 10

# How many borderline candidates one article may be compared against
# before giving up. Without a cap, a batch of N near-identical headlines
# costs O(N^2) model calls; with it, the worst case is N*4. Four is
# generous against the real data - the EU Council cluster that motivated
# this needed its SECOND-ranked candidate, never worse than that - and
# the pair cache means a comparison already made is free.
_SAME_EVENT_MAX_CANDIDATES = 4


def _resolve_db_borderline_matches_parallel(
    pending: list[tuple[int, str, str, Any]], api_key: str | None,
    cache: dict[tuple[str, str], bool | None] | None = None,
) -> dict[int, bool | None]:
    """Resolve a batch of DB-history borderline same-event checks concurrently.

    ``pending`` is a list of (article_index, article_title, candidate_title,
    candidate_db_id) - one entry per article whose best borderline
    candidate came from DB history (never same-batch; see the docstring on
    the two-phase split in _dedup_by_title_similarity_for_domain for why
    same-batch borderline candidates are excluded from this fast path and
    resolved sequentially instead).

    Each check only depends on its own (article_title, candidate_title)
    pair, fixed before any of these calls run - none of them can change
    another's outcome, so running them concurrently produces the exact
    same per-pair results as running them one at a time, just faster.
    Bounded by _SAME_EVENT_LLM_CONCURRENCY workers, each with its own
    OpenAI client (httpx.Client instances aren't meant to be shared across
    threads).

    Returns {article_index: same_event_result}, same True/False/None
    semantics as _titles_describe_same_event itself.
    """
    if not pending:
        return {}

    # Real runs show the exact same (article_title, candidate_title) pair
    # recurring dozens of times in one batch - GDELT/SerpAPI/RSS pulling
    # the same wire story from many outlets, several of which produce
    # identical or near-identical raw titles that each land their own
    # "pending" entry against the same DB candidate. The LLM verdict for
    # a given pair is a pure function of the two title strings, so an
    # exact-pair cache is always correct and cuts real, measured repeat
    # calls (confirmed live 2026-09-24: one pair alone repeated 30x in a
    # single run) without changing any dedup outcome. Caller may pass a
    # cache shared with phase 3's sequential same-batch checks too, since
    # the same (article_title, candidate_title) pair can recur there.
    if cache is None:
        cache = {}
    cache_lock = threading.Lock()

    def _check_one(item):
        idx, article_title, candidate_title, _candidate_db_id = item
        key = (article_title, candidate_title)
        with cache_lock:
            if key in cache:
                return idx, cache[key]
        same_event = _titles_describe_same_event(article_title, candidate_title, api_key)
        with cache_lock:
            cache[key] = same_event
        return idx, same_event

    results: dict[int, bool | None] = {}
    with ThreadPoolExecutor(max_workers=_SAME_EVENT_LLM_CONCURRENCY) as executor:
        futures = [executor.submit(_check_one, item) for item in pending]
        for future in as_completed(futures):
            idx, same_event = future.result()
            results[idx] = same_event
    return results


def _titles_describe_same_event(
    title_a: str, title_b: str, api_key: str | None,
) -> bool | None:
    """Ask an LLM whether two headlines report the same underlying event.

    Exists because title-embedding similarity alone cannot separate real
    same-story duplicates from real different-story near-misses in this
    domain - confirmed live on geopolitical_news: a 9-headline cluster
    confirmed (by a human reviewing the actual stored rows) to be the same
    EU Council sanctions-extension story scored pairwise similarity
    0.5951-0.8177, which fully overlaps the similarity range (0.61-0.72)
    of a separately-confirmed pair of genuinely DIFFERENT stories (two
    distinct Canada-tariff headlines) that motivated raising the threshold
    to 0.82 in the first place - i.e. there is no single similarity cutoff
    that includes the real duplicate cluster while excluding the real
    false-positive case; they occupy the same band. This resolves that
    band with real semantic judgment instead of a threshold that cannot
    exist.

    Prompt history (tested live against an 8-case set: reaction-vs-event,
    multi-day story development, a confirmed false-positive pair, and 5
    EU sanctions-delisting headlines confirmed by a human to be one real
    event): the original wording scored 4/8, missing 4 of 5 EU cases -
    root cause was two specific reading failures, not vague instructions:
    the model read "lifting sanctions on X" and "removing X from the
    sanctions list" as OPPOSITE actions (they mean the same thing), and
    treated "delist individuals" and "extend the sanctions package" as
    two events even when reported as one Council vote. Two more general
    rewrites (a broader "same source article" framing, with and without
    also naming the sanctions-terminology trap) scored 5/8 each - worse
    than naming the specific traps directly, so this prompt keeps the
    explicit trap callouts rather than relying on general phrasing.
    Current wording scores 6/8 - fixed 3 of the 4 EU misses; the
    remaining miss is a headline that mentions delisting only in a
    subordinate clause explaining WHY an extension happened, which the
    model reads as background rather than as itself reporting the
    delisting - and introduced one new miss (conflates a failed vote with
    the later successful one), which fails toward extra merging rather
    than extra dropping, same direction other failures already accept.

    Returns True/False, or None on any failure (network, non-JSON, wrong
    shape) - callers must treat None as "cannot determine" and fail open
    (not a duplicate), same discipline as _embed_titles: a missed dedup is
    far cheaper than wrongly merging two real, distinct stories.
    """
    try:
        client = _make_client(api_key)
        response = client.chat.completions.create(
            # Hardcoded, not env-configurable - chosen for speed/cost on a
            # one-word-equivalent classification call; avoid reasoning
            # models here, they add real per-call latency for no accuracy
            # gain on this task.
            model="openai/gpt-4o-mini",
            temperature=0,
            messages=[
                {"role": "system", "content": _SAME_EVENT_SYSTEM_PROMPT},
                {"role": "user", "content": f"Headline A: {title_a}\nHeadline B: {title_b}"},
            ],
        )
        content = response.choices[0].message.content or ""
        parsed = json.loads(content)
        same_event = parsed.get("same_event")
        if not isinstance(same_event, bool):
            raise ValueError(f"same_event field missing or not boolean: {parsed!r}")
        return same_event
    except Exception as exc:
        logger.warning(
            "[GDELT] same-event check failed, treating as not-duplicate"
            " (fail-open): %r vs %r: %s",
            title_a, title_b, exc,
        )
        return None


# ---------------------------------------------------------------------------
# Path 3, Stage A - free NOISE removal for GDELT (no model, runs before
# translation and before Stage B's model call - see module docstring notes
# on ordering: the company-name check below must run on the native-language
# title, since translating first would make this check depend on
# translation quality for no benefit).
# ---------------------------------------------------------------------------

# Domains confirmed live, in this project, to be genuine Taiwan financial
# or general-news press (not an exhaustive list - a starter allowlist,
# same spirit as the clause-code table above). Extend as more legitimate
# outlets are observed in real GDELT results.
_TAIWAN_PRESS_ALLOWLIST: frozenset[str] = frozenset({
    "setn.com",
    "digitimes.com.tw",
    "taipeitimes.com",
    "focustaiwan.tw",
    "udn.com",
    "money.udn.com",
    "ctee.com.tw",       # Commercial Times
    "cna.com.tw",        # Central News Agency
    # Added after a real live run against GDELT (2026-08-24) surfaced these
    # 4 domains carrying genuine, on-topic Taiwan company coverage
    # (Quanta, Wistron) that the original 8-domain starter list rejected -
    # each verified as a legitimate Taiwan outlet before adding.
    "n.yam.com",           # Yam News (蕃新聞) - general Taiwan news portal/aggregator
    "finance.ettoday.net", # ETtoday Finance Cloud - Taiwan financial-news vertical
    "news.ustv.com.tw",    # Unique Business News (非凡新聞) - Taiwan financial/business broadcaster
    # China Times (中國時報) - mainstream, Taiwan-headquartered, and it
    # carried real Wistron coverage in the same live test. Included, but
    # flagged: since its 2008 acquisition by Want Want China Times Group,
    # multiple credible sources (Freedom House, Taiwan's Mainland Affairs
    # Council, FT reporting) document PRC editorial-influence concerns on
    # cross-strait political topics. Judged acceptable for routine company
    # revenue/business reporting (this allowlist's actual purpose), not a
    # blanket endorsement of the outlet's political coverage - revisit if
    # this ever needs to expand beyond financial/business news.
    "chinatimes.com",
    # Added after a second real live run against GDELT (2026-08-24) surfaced
    # these domains carrying genuine, on-topic Taiwan company coverage
    # (TSMC, King Slide, Wistron) that the 12-domain list at that point
    # still rejected. Two OTHER domains from the same run - woman.udn.com
    # (a lifestyle/beauty vertical) and blog.udn.com (an unedited,
    # user-generated blogging platform) - were deliberately NOT added
    # despite being subdomains of the already-allowlisted udn.com: neither
    # is financial/business news, and udn.com's own allowlisting doesn't
    # extend to every subdomain under it.
    "ec.ltn.com.tw",  # Liberty Times (自由時報) dedicated finance/economics vertical
    "newtalk.tw",     # Newtalk News (新頭殼) - general Taiwan news outlet, not finance-dedicated but carries real company coverage
    "nownews.com",    # NOWnews (今日新聞) - general Taiwan news outlet, same reasoning
})

# Same purpose as _TAIWAN_PRESS_ALLOWLIST, scoped to Korea instead - a
# starter list, not exhaustive by design (per the Korea Signals spec's own
# "trusted Korean list" framing and explicit user instruction not to make
# this comprehensive), of domains this project has directly, live-verified
# as real Korean trade/business press: the five RSS feeds already seeded in
# KOREA_RSS_SOURCES below (thelec.kr, feedburner.com/zdkorea resolves to
# zdnet.co.kr bylines, businesspost.co.kr, ddaily.co.kr, and Yonhap's
# yna.co.kr), plus hankyung.com (Korea Economic Daily - confirmed reachable
# during the RSS User-Agent fix earlier in this build, even though it isn't
# one of the seeded RSS sources itself). GDELT can surface other genuine
# Korean outlets beyond this starter set the same way real Taiwan runs
# surfaced n.yam.com/finance.ettoday.net/etc. after the fact - extend as
# more legitimate Korean outlets are observed in real GDELT results, same
# process as Taiwan's own list above, not by trying to enumerate every
# Korean publication upfront.
_KOREA_PRESS_ALLOWLIST: frozenset[str] = frozenset({
    "thelec.kr",          # THE ELEC (디일렉)
    "zdnet.co.kr",        # ZDNet Korea (지디넷코리아)
    "businesspost.co.kr", # BusinessPost (비즈니스포스트)
    "ddaily.co.kr",       # DigitalDaily (디지털데일리)
    "yna.co.kr",          # Yonhap (연합뉴스)
    "hankyung.com",       # Korea Economic Daily (한국경제)
})

# Bug fixed 2026-09-23, live-confirmed against the test DB: BOTH
# KOREA_GDELT_SOURCE and KOREA_GDELT_ENGLISH_SOURCE (seed.py) populate
# query_ticker, so both were routed through _filter_by_domain_allowlist -
# which, before this map existed, only ever checked against
# _TAIWAN_PRESS_ALLOWLIST regardless of which domain's GDELT source called
# it. Result: every real Korea GDELT article (Korean-language AND the
# English-coverage-check source) was being silently dropped, since no
# Korean or English-wire domain is on the Taiwan-specific list - confirmed
# by a direct query against the local test DB returning zero
# source_category='gdelt' rows ever stored under korea_market_signal,
# despite the source being seeded and scheduled since early in this build.
_DOMAIN_PRESS_ALLOWLIST: dict[str, frozenset[str]] = {
    "taiwan_market_signal": _TAIWAN_PRESS_ALLOWLIST,
    "korea_market_signal": _KOREA_PRESS_ALLOWLIST,
}


def _filter_by_domain_allowlist(
    articles: list[dict], query_ticker: dict[str, str], domain_slug: str,
) -> list[dict]:
    """Drop GDELT articles whose domain is not on ``domain_slug``'s press
    allowlist (see ``_DOMAIN_PRESS_ALLOWLIST``).

    Only touches ticker-scoped GDELT articles (those whose ``_query`` is in
    ``query_ticker`` - the same test used by dedup) - other sources pass
    through untouched. Keyed on ``_query`` presence rather than
    ``metadata.source_category``, since this runs BEFORE
    _dedup_by_title_similarity, which is what actually writes
    source_category - at this point in the pipeline that field doesn't
    exist yet.

    A ticker key ending in ``-en`` (KOREA_GDELT_ENGLISH_SOURCE's convention
    - see seed.py) marks an English-coverage-CHECK query, not a primary
    signal source: its whole purpose is "does English coverage exist at
    all", so allowlisting it against a trusted-press list is the wrong
    concept (Reuters/Bloomberg/etc. covering a story is exactly the
    positive result this check wants to detect, not something to filter
    out for being untrusted) - passed through unconditionally instead.

    GDELT's ``domain`` field (stored as the article's ``source``) is
    compared case-insensitively; articles with no ``source`` set are
    dropped (fail-closed here, unlike the embedding fail-open logic below,
    since a domain we can't identify at all can't be verified as
    legitimate press for this market).

    Bug fixed 2026-09-23: this used to check unconditionally against
    _TAIWAN_PRESS_ALLOWLIST regardless of which domain's GDELT source
    called it, which silently dropped every real Korea GDELT article
    (Korean-language AND English-coverage-check) - confirmed live against
    the test DB (zero source_category='gdelt' rows ever stored under
    korea_market_signal despite the source being seeded and scheduled).
    """
    allowlist = _DOMAIN_PRESS_ALLOWLIST.get(domain_slug, frozenset())
    kept = []
    dropped = 0
    for a in articles:
        ticker_key = query_ticker.get(a.get("_query", ""))
        if not ticker_key:
            kept.append(a)
            continue
        if ticker_key.endswith("-en"):
            kept.append(a)
            continue
        domain = (a.get("source") or "").lower()
        if domain in allowlist:
            kept.append(a)
        else:
            dropped += 1
            logger.info("[PATH3-A] dropped (domain not allowlisted): %r", domain)
    if dropped:
        logger.info("[PATH3-A] domain allowlist: dropped %d article(s)", dropped)
    return kept


def _filter_by_company_name_in_title(
    articles: list[dict],
    query_ticker: dict[str, str],
    query_english_name: dict[str, str] | None = None,
) -> list[dict]:
    """Drop GDELT articles whose title doesn't actually contain the company
    name that was queried for.

    GDELT matches on free-text company name, not a guaranteed ticker field
    - this is a common failure mode of keyword search generally (a query
    for a company name can surface an article that only mentions that
    company in passing, inside a story primarily about something else).
    Checking here, on the native-language title, catches the case where the
    queried name doesn't appear in the returned headline at all.

    Must run BEFORE translation - the query string itself (and therefore
    the name to check for) is in whichever language was used to search
    (native Chinese or English name), and checking against the original,
    untranslated title avoids making this check depend on translation
    quality. Case-insensitive for English names; exact substring match for
    Chinese names (no case concept).

    Also accepts the ticker's English name as a fallback match, even for a
    native-name query - GDELT's sourcelang:chinese scope still surfaces some
    English-language syndication (Taipei Times, Focus Taiwan), which was
    being dropped by a native-name-only check despite genuinely naming the
    company. ``query_english_name`` is optional so other sources (e.g.
    geopolitical_news's theme queries, which have no ticker mapping at all)
    are unaffected.

    Keyed on ``_query`` presence, same reasoning as the allowlist filter
    above - source_category isn't set yet at this point in the pipeline.
    """
    query_english_name = query_english_name or {}
    kept = []
    dropped = 0
    for a in articles:
        query = a.get("_query", "")
        if not query_ticker.get(query):
            kept.append(a)
            continue
        # The query string is "{name} sourcelang:chinese sourcecountry:TW" -
        # the name is everything before the first operator.
        name = query.split(" sourcelang:")[0].strip()
        english_name = query_english_name.get(query, "")
        title = a.get("title") or ""
        title_lower = title.lower()
        if (name and name.lower() in title_lower) or (
            english_name and english_name.lower() in title_lower
        ):
            kept.append(a)
        else:
            dropped += 1
            logger.info(
                "[PATH3-A] dropped (queried name %r / english name %r not"
                " found in title %r)",
                name, english_name, title,
            )
    if dropped:
        logger.info(
            "[PATH3-A] company-name-in-title: dropped %d article(s)", dropped,
        )
    return kept


def _dedup_by_title_similarity(
    articles: list[dict], query_ticker: dict[str, str],
) -> list[dict]:
    """Drop Taiwan GDELT articles whose title is near-identical in meaning
    to one already stored for the same ticker in the last
    ``_TITLE_DEDUP_WINDOW_HOURS`` hours (rolling window relative to now,
    not calendar-day, so a duplicate that lands just after midnight isn't
    missed).

    Each article dict must carry ``_query`` (the GDELT query string that
    produced it, used to look up its ticker via ``query_ticker`` — GDELT has
    no ticker field itself, only a company-name query). Articles whose
    query has no ticker mapping (e.g. non-Taiwan GDELT sources) pass through
    unchanged - this function is a no-op unless ticker-scoped queries are
    present.

    Embedding failures fail open: an article whose title couldn't be
    embedded, or whose only comparison candidates lack a stored embedding,
    is kept rather than silently dropped or silently deduped.
    """
    ticker_scoped = [a for a in articles if query_ticker.get(a.get("_query", ""))]
    if not ticker_scoped:
        return articles

    api_key = os.environ.get("OPENROUTER_API_KEY")
    new_embeddings = _embed_titles([a["title"] for a in ticker_scoped], api_key)

    kept: list[dict] = []
    dropped = 0
    # Cache recent-article lookups per ticker within this call - multiple
    # surviving articles for the same ticker in one run shouldn't each
    # trigger their own DB query.
    recent_cache: dict[str, list[dict]] = {}
    # Candidates kept so far *within this same batch*, per ticker - required
    # in addition to the DB-backed recent_cache above, since two outlets can
    # both surface the same story within one poll run, before either has
    # been stored yet. Each entry also carries the in-memory article dict
    # itself (unlike DB candidates, which carry a row id instead) so a later
    # duplicate in the same batch can be merged into it directly rather
    # than needing a DB round-trip.
    kept_this_batch: dict[str, list[dict]] = {}

    for article, embedding in zip(ticker_scoped, new_embeddings):
        ticker = query_ticker[article["_query"]]
        if embedding is None:
            # No embedding to compare with, so this can't be checked for
            # duplicates against future polls either - but it still needs
            # ticker/source_category set, same as the normal path below,
            # so downstream consumers (ranking, filters) can rely on these
            # fields being present on every taiwan_market_signal GDELT row.
            article["metadata"]["ticker"] = ticker
            article["metadata"]["source_type"] = "gdelt"
            article["metadata"]["source_category"] = "gdelt"
            article["metadata"]["customers"] = _taiwan_customers(ticker)
            kept.append(article)
            continue

        if ticker not in recent_cache:
            recent_cache[ticker] = get_recent_gdelt_articles_for_ticker(
                ticker, hours=_TITLE_DEDUP_WINDOW_HOURS,
            )

        # Each candidate: (title, embedding, db_id_or_None, batch_article_or_None).
        # Exactly one of the last two is set - db_id for a DB-backed
        # candidate (needs an UPDATE), batch_article for a same-batch one
        # (can be merged in-memory, not yet written).
        candidates = [
            (c.get("title"), (c.get("metadata") or {}).get("title_embedding"),
             c.get("id"), None)
            for c in recent_cache[ticker]
        ] + [
            (c["title"], c["metadata"]["title_embedding"], None, c)
            for c in kept_this_batch.get(ticker, [])
        ]

        match = None
        for candidate_title, candidate_embedding, db_id, batch_article in candidates:
            if not candidate_embedding:
                continue
            similarity = _cosine_similarity(embedding, candidate_embedding)
            if similarity >= _TITLE_DEDUP_SIMILARITY_THRESHOLD:
                logger.info(
                    "[GDELT] near-duplicate title (similarity=%.3f)"
                    " ticker=%s new=%r existing=%r",
                    similarity, ticker, article["title"], candidate_title,
                )
                match = (db_id, batch_article)
                break

        if match is not None:
            db_id, batch_article = match
            domain = _extract_domain_for_also_reported_by(article)
            if batch_article is not None:
                # Same-batch match: merge in-memory, nothing written yet.
                # Same {outlet, title, url} shape the per-domain dedup
                # below writes, so a consumer reads one shape whichever
                # path found the duplicate.
                entry = _also_reported_entry(article)
                also_reported_by = batch_article["metadata"].setdefault(
                    "also_reported_by", []
                )
                if entry is not None and not any(
                    e.get("url") == entry["url"] for e in also_reported_by
                    if isinstance(e, dict)
                ):
                    also_reported_by.append(entry)
            elif db_id is not None:
                # Cross-run match: the existing row is already stored, so
                # this needs a real UPDATE rather than an in-memory merge.
                entry = _also_reported_entry(article)
                if entry is not None:
                    try:
                        append_also_reported_by(db_id, entry)
                    except Exception as exc:
                        logger.warning(
                            "[GDELT] failed to record also_reported_by for"
                            " article_id=%s entry=%r: %s", db_id, entry, exc,
                        )
            dropped += 1
            continue

        # ticker + source_type must be written here, not left implicit -
        # get_recent_gdelt_articles_for_ticker()'s query filters on both
        # (metadata->>'ticker', metadata->>'source_type') to find candidates
        # for the NEXT poll run's dedup check. Without these, that lookup
        # always returns empty and cross-run dedup silently never fires.
        article["metadata"]["ticker"] = ticker
        article["metadata"]["source_type"] = "gdelt"
        article["metadata"]["source_category"] = "gdelt"
        article["metadata"]["customers"] = _taiwan_customers(ticker)
        article["metadata"]["title_embedding"] = embedding
        kept.append(article)
        kept_this_batch.setdefault(ticker, []).append(article)

    # Articles whose query has no ticker mapping (non-Taiwan GDELT sources)
    # were excluded from ticker_scoped above and never touched - add back.
    untouched = [a for a in articles if not query_ticker.get(a.get("_query", ""))]
    logger.info(
        "[GDELT] title-similarity dedup: %d ticker-scoped article(s),"
        " %d dropped as near-duplicates", len(ticker_scoped), dropped,
    )
    return untouched + kept


# Domains with no ticker/agency/curated-source pre-scoping (see the skip
# list in run()) - RSS/SerpAPI/NewsAPI sources for these routinely surface
# the same underlying story from multiple outlets with differently-worded
# titles, same problem _dedup_by_title_similarity solves for Taiwan GDELT,
# just scoped by domain instead of ticker.
_TITLE_DEDUP_DOMAINS: frozenset[str] = frozenset({
    "ai_news", "smart_money", "geopolitical_news", "korea_market_signal",
    "middle_east_market_signal",
})

# ai_news/smart_money: wider than Taiwan GDELT's 24h (_TITLE_DEDUP_WINDOW_HOURS)
# - RSS/SerpAPI/NewsAPI sources publish re-coverage of the same story across a
# longer tail than same-day Taiwan company filings/news, so a same-day-only
# window was missing duplicates that appear the next day.
# geopolitical_news: 168h (7 days) - matches this domain's own full article
# retention (see Article Retention in CLAUDE.md), so a story that resurfaces
# anywhere within its retained lifetime still gets caught as a duplicate of
# the original, not just same-news-cycle re-reporting. Deliberately wider
# than Taiwan's 24h: geopolitical stories are corroborated by wire services
# over a longer tail than a same-day window would catch. Trade-off accepted:
# more embedding-comparison candidates per run than a 24h window, and a
# genuine multi-day story development (e.g. a conflict escalating days
# later) risks being merged into the original as a "duplicate" rather than
# treated as new - both weighed against catching more real duplicates.
_DOMAIN_TITLE_DEDUP_WINDOW_HOURS: dict[str, int] = {
    "ai_news": 48,
    "smart_money": 48,
    "geopolitical_news": 168,
    # Korea Signals spec Section 5.7, Filter 3: "drop anything that repeats
    # a story already seen in the last day" - literally 24h, unlike the
    # other domains here (which widened past 24h after missing real
    # duplicates - see comment below). No such finding yet for Korea; start
    # at the spec's literal number rather than importing another domain's
    # correction pre-emptively.
    "korea_market_signal": 24,
    # Same settings as geopolitical_news (window, threshold, same-event LLM
    # check below): its press is the same kind - wire stories on one war
    # and one set of deals re-reported across outlets over days. Measured
    # on its first run: six outlets carrying "Iran urges US to accept
    # proposal to reopen Strait of Hormuz" verbatim, and more reworded.
    "middle_east_market_signal": 168,
}

# Per-domain override of _TITLE_DEDUP_SIMILARITY_THRESHOLD (0.90 default,
# used by ai_news/smart_money and Taiwan GDELT). geopolitical_news lowered
# twice after real same-story duplicates were confirmed missed just under
# each prior threshold:
#   0.90 -> 0.85: "Canada Sanctions Streit Group Over Armored Vehicle
#   Supplies to Russia" (militarnyi.com) vs "Canada Hits Streit Group With
#   Sanctions Over Armored Vehicles Used by Russia's National Guard"
#   (kyivpost.com), measured at 0.8691.
#   0.85 -> 0.82: "US Congress passes Russia sanctions bill" (dw.com) vs
#   "Congress passes sweeping US sanctions bill targeting Russia"
#   (aljazeera.com), measured at 0.8488 - both allowlisted wire sources,
#   so this specific miss was directly suppressing real corroboration
#   downstream (Stage D's corroborated check).
# Kept domain-specific rather than lowering the shared default globally:
# geopolitical wire coverage of the same event varies headline wording
# more than ai_news/smart_money's typical re-reporting, and 0.82 keeps a
# real margin above the ~0.61-0.72 range three genuinely different
# Canada-tariff headlines (different specifics: general tariffs vs. named
# products) scored in the same real dataset - so this still catches both
# confirmed-missed duplicates without pulling those distinct stories
# together. If a third real miss turns up close to 0.82, that's a signal
# this domain's headline variance may need a different approach entirely
# (e.g. entity/actor overlap in addition to title similarity) rather than
# a third threshold nudge.
_DOMAIN_TITLE_DEDUP_SIMILARITY_THRESHOLD: dict[str, float] = {
    "geopolitical_news": 0.82,
    "middle_east_market_signal": 0.82,
}

# korea_market_signal mixes DART filings, Customs export data, and RSS/GDELT
# news in one domain - unlike ai_news/smart_money/geopolitical_news, which
# are news-only. The Korea Signals spec's S7 "drop anything that repeats a
# story already seen" (Filter 3) is a NEWS-story rule; a DART filing header
# ("SK Hynix: 단일판매·공급계약체결") is never a near-duplicate of a real
# headline by embedding similarity in practice, but running the comparison
# over filing/customs rows at all is still wasted work and the wrong
# candidate pool in principle - so korea_market_signal excludes any article
# whose stored/incoming metadata.source_category is one of these from BOTH
# sides of the comparison (new batch and DB-side recent candidates), rather
# than trying to enumerate "is a news source_type" (RSS articles set no
# source_category at all at this point in the pipeline - see _parse_feed -
# so absence of source_category means "news", not "unknown").
_DOMAIN_TITLE_DEDUP_EXCLUDED_CATEGORIES: dict[str, frozenset[str]] = {
    "korea_market_signal": frozenset({"dart_filing", "kr_customs_export"}),
    # Official and filing rows are kept out of both sides, same reasoning
    # as Korea plus one more: whichever copy of a story is stored first
    # survives, so a WAM release that landed after press coverage of it
    # would otherwise be dropped as the duplicate - losing the official
    # source and keeping the retelling. Press copies still dedup against
    # each other.
    "middle_east_market_signal": frozenset({
        "me_official_wire", "me_us_policy", "me_fund_stake"}),
}

# Domains where a borderline-similarity pair (see _SAME_EVENT_SIMILARITY_FLOOR)
# gets resolved by _titles_describe_same_event instead of staying unmerged.
# geopolitical_news only for now - confirmed live there that title similarity
# alone can't separate real duplicates from real different-story near-misses
# (see that function's docstring); ai_news/smart_money haven't shown the same
# confirmed failure, so they keep the cheaper threshold-only comparison
# rather than paying for LLM calls speculatively.
_SAME_EVENT_LLM_DOMAINS: frozenset[str] = frozenset({
    "geopolitical_news", "middle_east_market_signal"})


def _dedup_by_title_similarity_for_domain(
    articles: list[dict], domain_slug: str,
) -> list[dict]:
    """Drop articles whose title is near-identical in meaning to one already
    stored for this domain in the last ``_DOMAIN_TITLE_DEDUP_WINDOW_HOURS[domain_slug]``
    hours.

    Same algorithm as ``_dedup_by_title_similarity`` (Taiwan GDELT), scoped
    by domain instead of ticker - ai_news/smart_money/geopolitical_news sources
    aren't ticker-scoped, so "same domain" is the natural comparison boundary
    instead.

    Embedding failures fail open: an article whose title couldn't be
    embedded, or whose only comparison candidates lack a stored embedding,
    is kept rather than silently dropped or silently deduped.

    Domains in ``_DOMAIN_TITLE_DEDUP_EXCLUDED_CATEGORIES`` mix news articles
    with non-news rows (e.g. korea_market_signal's DART filings/Customs
    export data) - rows whose ``metadata.source_category`` is in that set
    are excluded from both the incoming batch and the DB-side candidate
    pool, since this check is a news-story rule, not a general dedup.
    """
    if not articles:
        return articles

    excluded_categories = _DOMAIN_TITLE_DEDUP_EXCLUDED_CATEGORIES.get(domain_slug)
    if excluded_categories:
        non_news = [
            a for a in articles
            if (a.get("metadata") or {}).get("source_category") in excluded_categories
        ]
        articles = [
            a for a in articles
            if (a.get("metadata") or {}).get("source_category") not in excluded_categories
        ]
        if not articles:
            return non_news
    else:
        non_news = []

    api_key = os.environ.get("OPENROUTER_API_KEY")
    new_embeddings = _embed_titles([a["title"] for a in articles], api_key)

    threshold = _DOMAIN_TITLE_DEDUP_SIMILARITY_THRESHOLD.get(
        domain_slug, _TITLE_DEDUP_SIMILARITY_THRESHOLD,
    )
    same_event_domain = domain_slug in _SAME_EVENT_LLM_DOMAINS

    # Phase 1 (sequential, no I/O beyond the paginated fetch itself):
    # resolve each article's DB-history-only hard match and best
    # DB-history-only borderline candidate. DB history is fixed before
    # this loop starts - unlike same-batch candidates (which grow as
    # earlier articles in THIS batch survive), comparing against it never
    # depends on what order articles are processed in, so this pass is
    # safe to resolve out of order (which phase 2 then does, concurrently).
    #
    # Candidates are streamed in bounded-size pages via
    # iter_recent_articles_for_domain rather than loaded all at once via
    # get_recent_articles_for_domain - confirmed live 2026-09-24 that
    # holding the full window (2127+ rows, ~30KB each, dominated by the
    # stored title_embedding) in memory for the whole dedup pass was a
    # real cost during the exact phase a production run died with no
    # traceback. Chunking trades that for more DB round-trips, with zero
    # loss of dedup coverage - every candidate in the window is still
    # compared, just one page at a time. best_borderline_by_article
    # persists across pages so a later page can still beat an earlier
    # page's borderline candidate; articles already hard-matched are
    # skipped on subsequent pages instead of re-scanned.
    db_hard_match: dict[int, Any] = {}
    best_borderline_by_article: dict[int, tuple[float, str, Any]] = {}
    for article in articles:
        article.setdefault("metadata", {})

    pending_indices = {
        i for i, embedding in enumerate(new_embeddings) if embedding is not None
    }
    for chunk in iter_recent_articles_for_domain(
        domain_slug, hours=_DOMAIN_TITLE_DEDUP_WINDOW_HOURS[domain_slug],
    ):
        if not pending_indices:
            break
        if excluded_categories:
            chunk = [
                c for c in chunk
                if (c.get("metadata") or {}).get("source_category") not in excluded_categories
            ]
        chunk_candidates = [
            (c.get("title"), (c.get("metadata") or {}).get("title_embedding"), c.get("id"))
            for c in chunk
        ]
        for i in list(pending_indices):
            embedding = new_embeddings[i]
            best_borderline = best_borderline_by_article.get(i)
            for candidate_title, candidate_embedding, db_id in chunk_candidates:
                if not candidate_embedding:
                    continue
                similarity = _cosine_similarity(embedding, candidate_embedding)
                if similarity >= threshold:
                    db_hard_match[i] = db_id
                    best_borderline = None
                    break
                if (
                    same_event_domain
                    and similarity >= _SAME_EVENT_SIMILARITY_FLOOR
                    and (best_borderline is None or similarity > best_borderline[0])
                ):
                    best_borderline = (similarity, candidate_title, db_id)
            if i in db_hard_match:
                pending_indices.discard(i)
                best_borderline_by_article.pop(i, None)
            elif best_borderline is not None:
                best_borderline_by_article[i] = best_borderline

    db_borderline: dict[int, tuple[float, str, Any]] = best_borderline_by_article

    # Phase 2 (parallel): resolve every DB-history borderline check
    # concurrently - each pair is independent (see
    # _resolve_db_borderline_matches_parallel's docstring), so this is
    # the one part of the whole function safe to run out of order and
    # concurrently. Same-batch borderline checks are deliberately NOT
    # included here - they're resolved sequentially in phase 3 below,
    # since which same-batch candidates even EXIST depends on batch
    # order (an earlier article being dropped means it's never added to
    # kept_this_batch for a later article to match against).
    pending = [
        (i, articles[i]["title"], title, db_id)
        for i, (sim, title, db_id) in db_borderline.items()
    ]
    # Shared with phase 3 below - the same (new title, candidate title)
    # pair can recur there too (e.g. two same-batch outlets both compared
    # against a title already resolved in phase 2).
    same_event_cache: dict[tuple[str, str], bool | None] = {}
    db_same_event_results = _resolve_db_borderline_matches_parallel(
        pending, api_key, cache=same_event_cache,
    )

    # Phase 3 (sequential): walk the batch in order exactly as before,
    # but hard matches and DB-borderline same-event checks are already
    # known from phases 1-2 (no LLM call needed here for those) - only a
    # same-batch borderline candidate (rare: two outlets in one fetch)
    # still triggers an inline LLM call, same as the original single-pass
    # version, preserving its exact behavior for that case.
    kept_this_batch: list[dict] = []
    kept: list[dict] = []
    dropped = 0

    for i, (article, embedding) in enumerate(zip(articles, new_embeddings)):
        if embedding is None:
            kept.append(article)
            continue

        match: tuple[Any, dict | None] | None = None

        if i in db_hard_match:
            db_id = db_hard_match[i]
            logger.info(
                "[%s] near-duplicate title (threshold=%.2f) new=%r db_id=%s",
                domain_slug, threshold, article["title"], db_id,
            )
            match = (db_id, None)
        elif i in db_borderline and db_same_event_results.get(i):
            similarity, candidate_title, db_id = db_borderline[i]
            logger.info(
                "[%s] near-duplicate title via same-event check"
                " (similarity=%.3f, below threshold=%.2f) new=%r existing=%r",
                domain_slug, similarity, threshold, article["title"], candidate_title,
            )
            match = (db_id, None)
        else:
            # No DB-history match - only same-batch candidates kept so
            # far can still produce a match, checked sequentially exactly
            # as the original single-pass loop did.
            # EVERY borderline candidate, best-first - not just the single
            # most similar one. The old version kept only the highest
            # scorer and asked about that one alone, which silently
            # assumed similarity ranking predicts the model's verdict.
            # It does not, and that assumption is the very thing this
            # check exists to work around.
            #
            # Measured on one real EU Council sitting that produced four
            # surviving clusters: "EU: Fresh Russia sanctions over 'sham'
            # elections" scored highest against "EU adopts nearly 1,600
            # new sanctions listings" (0.5872), the model said NOT the
            # same event, and the article was kept as new - while "EU
            # Approves 22nd Package" at 0.5326 WOULD have been judged the
            # same event, but was never asked because it ranked second.
            # 4 of the 6 pairs among those headlines already return true;
            # the cluster failed to form only because one comparison per
            # article was allowed.
            borderline: list[tuple[float, str, dict]] = []
            for batch_article in kept_this_batch:
                candidate_title = batch_article["title"]
                candidate_embedding = batch_article["metadata"]["title_embedding"]
                similarity = _cosine_similarity(embedding, candidate_embedding)
                if similarity >= threshold:
                    logger.info(
                        "[%s] near-duplicate title (similarity=%.3f, threshold=%.2f)"
                        " new=%r existing=%r",
                        domain_slug, similarity, threshold, article["title"], candidate_title,
                    )
                    match = (None, batch_article)
                    break
                if same_event_domain and similarity >= _SAME_EVENT_SIMILARITY_FLOOR:
                    borderline.append((similarity, candidate_title, batch_article))
            if match is None and borderline:
                # Best-first so the likeliest match is usually the first
                # call, and capped so one batch of near-identical
                # headlines cannot turn into a quadratic pile of LLM
                # calls. The cache below means a repeated pair costs
                # nothing.
                borderline.sort(key=lambda b: -b[0])
                for similarity, candidate_title, batch_article in (
                    borderline[:_SAME_EVENT_MAX_CANDIDATES]
                ):
                    cache_key = (article["title"], candidate_title)
                    if cache_key in same_event_cache:
                        same_event = same_event_cache[cache_key]
                    else:
                        same_event = _titles_describe_same_event(
                            article["title"], candidate_title, api_key,
                        )
                        same_event_cache[cache_key] = same_event
                    if same_event:
                        logger.info(
                            "[%s] near-duplicate title via same-event check"
                            " (similarity=%.3f, below threshold=%.2f)"
                            " new=%r existing=%r",
                            domain_slug, similarity, threshold,
                            article["title"], candidate_title,
                        )
                        match = (None, batch_article)
                        break

        if match is not None:
            db_id, batch_article = match
            # The DISCARDED article's own outlet, title and url - not
            # just its domain. This is the only moment that article
            # exists: it is dropped as a near-duplicate on the next
            # line and never stored, so a consumer wanting to show
            # "who else reported this" could otherwise show a bare
            # domain and nothing to read.
            #
            # Same shape as the siblings signal-detection-agent's
            # Stage D builds from Event Registry's event_uri clusters,
            # deliberately: one consumer renders both without caring
            # which mechanism found the duplicate. Event Registry
            # clusters 121 of 325 rows; this dedup catches another 28
            # it misses, and those rows previously reported no other
            # coverage at all.
            entry = _also_reported_entry(article)
            if entry is not None and not _is_self_reference(entry, batch_article, article):
                if batch_article is not None:
                    also_reported_by = batch_article["metadata"].setdefault(
                        "also_reported_by", []
                    )
                    if not any(
                        e.get("url") == entry["url"] for e in also_reported_by
                        if isinstance(e, dict)
                    ):
                        also_reported_by.append(entry)
                elif db_id is not None:
                    try:
                        append_also_reported_by(db_id, entry)
                    except Exception as exc:
                        logger.warning(
                            "[%s] failed to record also_reported_by for"
                            " article_id=%s entry=%r: %s",
                            domain_slug, db_id, entry, exc,
                        )
            dropped += 1
            continue

        article["metadata"]["title_embedding"] = embedding
        kept.append(article)
        kept_this_batch.append(article)

    logger.info(
        "[%s] title-similarity dedup: %d article(s), %d dropped as"
        " near-duplicates", domain_slug, len(articles), dropped,
    )
    return kept + non_news


def _fetch_one_gdelt(query: str, days_back: int):
    """Fetch GDELT DOC 2.0 API results for a single theme query, one attempt.

    English-only. DOC API returns headline + URL metadata only, no body
    text — the Context 2.0 API returns snippet text but rejects bare
    ``theme:`` filters ("keywords too common"), so DOC is used here for
    theme-filter support; ``body`` is left None for a later fetch step.

    Makes exactly one HTTP request — no internal retry. GDELT's rate limit
    has been observed to persist well beyond its documented 5-second floor,
    so retrying a single query immediately just stalls every other query
    behind it; the caller (_fetch_gdelt) instead moves on to the next query
    and retries rate-limited ones in a later round-robin pass, once more
    real time has actually elapsed.

    Args:
        query: GDELT query string, e.g. "theme:ARMEDCONFLICT sourcelang:english".
        days_back: Exclude articles older than this many days (clamped to
            90 - DOC API only covers a rolling 3-month window).

    Returns:
        List of article dicts with ``_pub_date`` set, or the
        ``_GDELT_RATE_LIMITED`` sentinel if GDELT returned 429.
    """
    timespan_days = min(days_back, 90)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    params = {
        "query": query,
        "mode": "artlist",
        "maxrecords": _GDELT_MAXRECORDS,
        "timespan": f"{timespan_days}d",
        "sort": "datedesc",
        "format": "json",
    }

    try:
        resp = httpx.get(
            _GDELT_DOC_URL,
            params=params,
            headers={"User-Agent": _GDELT_USER_AGENT},
            timeout=30.0,
        )
        if resp.status_code == 429:
            logger.warning("[GDELT] query=%r rate-limited", query)
            return _GDELT_RATE_LIMITED
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[GDELT] query=%r failed: %s", query, exc)
        return []

    results = []
    for item in data.get("articles", []):
        try:
            pub_date = datetime.strptime(
                item.get("seendate", ""), "%Y%m%dT%H%M%SZ"
            ).replace(tzinfo=timezone.utc)
        except ValueError:
            pub_date = None
        if pub_date and pub_date < cutoff:
            continue
        results.append({
            "title": item.get("title", ""),
            "url": item.get("url", ""),
            "published": item.get("seendate", ""),
            "source": item.get("domain", ""),
            "summary": None,
            "body": None,
            "_pub_date": pub_date,
            # transient - which query produced this; consumed by
            # _dedup_by_title_similarity's ticker lookup, stripped before
            # storage same as _pub_date (never enters the DB write path).
            "_query": query,
            "metadata": {
                "sourcecountry": item.get("sourcecountry"),
                "language": item.get("language"),
            },
        })
    logger.info(
        "[GDELT] query=%r articles=%d", query, len(results),
    )
    return results


def _fetch_gdelt(sources: list[dict], days_back: int, domain_slug: str = "") -> list[dict]:
    """Fetch articles from GDELT for multiple theme queries, round-robin.

    Runs every query once, in order, spaced by ``_GDELT_MIN_INTERVAL`` to
    respect GDELT's rate limit. Queries that come back 429'd are not
    retried immediately (that would stall every later query behind them) —
    they're collected and retried in a second pass, up to
    ``_GDELT_MAX_ROUNDS`` total passes, by which point real time has
    actually elapsed and the rate limit is more likely to have cleared.
    Queries still failing after the last pass are skipped for this run
    (fail-open per query, not per run).

    Once all queries are fetched and deduplicated, articles already stored
    globally (any domain, any prior run) are dropped *before* the
    body-fetch step — GDELT's theme queries return heavily overlapping
    results day-to-day, so fetching bodies for already-seen URLs would be
    wasted work. Remaining articles get their body fetched in parallel via
    Trafilatura, since DOC API returns no body/summary text itself.

    Args:
        sources: List of gdelt source dicts; ``config.queries`` is a list
            of GDELT query strings (one per theme/category).
        days_back: Exclude articles older than this many days.
        domain_slug: The domain these sources belong to - used for the
            domain-scoped press allowlist (_DOMAIN_PRESS_ALLOWLIST) below.
            Passed explicitly by the caller (run() via _fetch_articles),
            not read off a source dict - a source dict here (as returned
            by load_sources) never carries its own domain_slug field
            (unlike list_sources, an unrelated admin/UI-facing query which
            does join it in). Bug fixed 2026-09-23, confirmed live: this
            function used to try `sources[0].get("domain_slug", "")`,
            which always silently returned "" (never matching any real
            key in _DOMAIN_PRESS_ALLOWLIST) - a real staging run showed
            every GDELT article for korea_market_signal being dropped by
            the allowlist check, including ones from domains that ARE on
            _KOREA_PRESS_ALLOWLIST (zdnet.co.kr, ddaily.co.kr), proving
            the lookup itself was never reaching the right allowlist, not
            that those domains were missing from it.

    Returns:
        List of article dicts with ``_pub_date`` set, deduplicated by URL.
    """
    pending: list[str] = []
    for source in sources:
        config = source.get("config") or {}
        pending.extend(config.get("queries", []))
    total_queries = len(pending)

    t0 = time.perf_counter()
    seen_urls: set[str] = set()
    articles: list[dict] = []
    last_call_time = 0.0

    for round_num in range(1, _GDELT_MAX_ROUNDS + 1):
        retry_queue: list[str] = []
        for query in pending:
            elapsed = time.monotonic() - last_call_time
            if last_call_time and elapsed < _GDELT_MIN_INTERVAL:
                time.sleep(_GDELT_MIN_INTERVAL - elapsed)
            last_call_time = time.monotonic()

            result = _fetch_one_gdelt(query, days_back)
            if result is _GDELT_RATE_LIMITED:
                retry_queue.append(query)
                continue
            for article in result:
                if article["url"] and article["url"] not in seen_urls:
                    seen_urls.add(article["url"])
                    articles.append(article)

        if not retry_queue:
            break
        logger.info(
            "[GDELT] round %d/%d: %d quer(y/ies) rate-limited, retrying"
            " in next round",
            round_num, _GDELT_MAX_ROUNDS, len(retry_queue),
        )
        pending = retry_queue
    else:
        logger.warning(
            "[GDELT] %d/%d quer(y/ies) still rate-limited after %d"
            " round(s), skipping for this run",
            len(pending), total_queries, _GDELT_MAX_ROUNDS,
        )

    already_stored = get_already_stored_urls([a["url"] for a in articles])
    before = len(articles)
    articles = [a for a in articles if a["url"] not in already_stored]
    logger.info(
        "[GDELT] dropped %d already-stored article(s) before body fetch",
        before - len(articles),
    )

    query_ticker: dict[str, str] = {}
    query_english_name: dict[str, str] = {}
    for source in sources:
        config = source.get("config") or {}
        query_ticker.update(config.get("query_ticker", {}))
        query_english_name.update(config.get("query_english_name", {}))

    # Path 3, Stage A (spec order: allowlist -> name-in-title -> dedup).
    # All three are no-ops for sources with no ticker mapping (e.g.
    # geopolitical_news's theme queries) - only ticker-scoped Taiwan
    # queries are affected. Run before translation and before the body
    # fetch below, so a dropped article never wastes translation cost or a
    # Trafilatura fetch.
    if query_ticker:
        before_stage_a = len(articles)
        articles = _filter_by_domain_allowlist(articles, query_ticker, domain_slug)
        articles = _filter_by_company_name_in_title(
            articles, query_ticker, query_english_name,
        )
        logger.info(
            "[GDELT] Path 3 Stage A (allowlist + name-check): %d -> %d article(s)",
            before_stage_a, len(articles),
        )

        before_title_dedup = len(articles)
        articles = _dedup_by_title_similarity(articles, query_ticker)
        logger.info(
            "[GDELT] title-similarity dedup: %d -> %d article(s)",
            before_title_dedup, len(articles),
        )

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Only articles whose source domain can ever become a signal get
        # a body fetch - see _BODY_FETCH_DOMAINS. The row is still
        # stored either way; this skips the expensive part, not the
        # article.
        body_urls = [
            a["url"] if _should_fetch_body(a["url"], domain_slug) else None
            for a in articles
        ]
        skipped = sum(1 for u in body_urls if u is None)
        bodies = list(executor.map(
            partial(
                _fetch_body_or_none,
                impersonate_fallback=_impersonate_for(domain_slug),
            ),
            body_urls,
        ))
    if skipped:
        logger.info(
            "[GDELT] body fetch skipped for %d/%d article(s) whose domain is"
            " not body-fetch allowlisted for %s",
            skipped, len(articles), domain_slug or "(unset)",
        )
    for article, body in zip(articles, bodies):
        article["body"] = body

    logger.info(
        "[TIMER] gdelt total: queries=%d articles=%d elapsed=%.2fs",
        total_queries, len(articles), time.perf_counter() - t0,
    )
    return articles


_TWSE_REVENUE_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"
_TPEX_REVENUE_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap05_O"
_TWSE_MATERIAL_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap04_L"
_TPEX_MATERIAL_URL = "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap04_O"

# TPEx has been observed to reset the connection on a bare httpx request;
# a browser User-Agent avoids it. Applied to all four TWSE/TPEx calls for
# consistency even though only TPEx showed the issue.
_TWSE_TPEX_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        " AppleWebKit/537.36 (KHTML, like Gecko)"
        " Chrome/124.0.0.0 Safari/537.36"
    ),
}

# TPEx's site is served through Cloudflare (a clean, stable 2-IP answer
# set), plus a separate origin IP that serves a broken TWCA-issued cert
# directly (intermediate missing a Subject Key Identifier extension -
# OpenSSL 3.x's stricter chain builder rejects it; curl's looser
# chain-building tolerates it). This was originally suspected to be
# Cloudflare edge-routing flakiness, observed to fail in bursts (e.g. 8/8
# retries failing together in one run) - but root-caused via live DNS
# testing to be LOCAL macOS resolver cache pollution on the dev machine
# used to build this, intermittently returning that bare origin IP instead
# of Cloudflare's. 20/20 real HTTPS calls from a Linux container (the
# ECS/Fargate production environment's actual OS) succeeded with zero SSL
# errors - this is not expected to reproduce in production. The retry loop
# below is kept as a harmless safety net (costs nothing on a clean
# resolver, and would still help if a transient real Cloudflare-side issue
# ever occurs) but is not itself the fix for the dev-machine symptom that
# motivated it - don't re-investigate this as a live Cloudflare edge issue
# without first checking the calling host's DNS resolution.
_TPEX_SSL_RETRY_ATTEMPTS = 8
_TPEX_SSL_RETRY_BACKOFF_SECONDS = 2.0


def _get_with_ssl_retry(url: str, *, headers: dict, timeout: float) -> httpx.Response:
    """GET with retry on transient SSL verification failure.

    Retries the exact same request (full verification, never relaxed) up
    to ``_TPEX_SSL_RETRY_ATTEMPTS`` times, waiting
    ``_TPEX_SSL_RETRY_BACKOFF_SECONDS`` between attempts — used for TPEx,
    whose CDN intermittently serves a cert chain that fails strict
    validation on one edge node but succeeds on the next. The backoff
    matters as much as the attempt count: retrying instantly tends to hit
    the same anycast route and thus the same bad edge repeatedly.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _TPEX_SSL_RETRY_ATTEMPTS + 1):
        try:
            resp = httpx.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except httpx.ConnectError as exc:
            if "CERTIFICATE_VERIFY_FAILED" not in str(exc):
                raise
            last_exc = exc
            logger.warning(
                "[TPEX] SSL verification failed (attempt %d/%d), retrying"
                " in %.0fs: %s",
                attempt, _TPEX_SSL_RETRY_ATTEMPTS,
                _TPEX_SSL_RETRY_BACKOFF_SECONDS, exc,
            )
            if attempt < _TPEX_SSL_RETRY_ATTEMPTS:
                time.sleep(_TPEX_SSL_RETRY_BACKOFF_SECONDS)
    raise last_exc


def _roc_to_gregorian_year(roc_year: int) -> int:
    """Convert an ROC (Minguo) calendar year to Gregorian."""
    return roc_year + 1911


def _parse_roc_date(value: str) -> datetime | None:
    """Parse a TWSE/TPEx ROC-calendar date string (YYYMMDD) to UTC midnight.

    Returns None if the value isn't a well-formed 7-digit ROC date.
    """
    if not value or not value.isdigit() or len(value) != 7:
        return None
    try:
        roc_year, month, day = int(value[:3]), int(value[3:5]), int(value[5:7])
        return datetime(
            _roc_to_gregorian_year(roc_year), month, day, tzinfo=timezone.utc
        )
    except ValueError:
        return None


def _parse_roc_period(value: str) -> str | None:
    """Convert an ROC-calendar year-month string (YYYMM) to 'YYYY-MM'.

    Returns None if the value isn't a well-formed 5-digit ROC period.
    """
    if not value or not value.isdigit() or len(value) != 5:
        return None
    roc_year, month = int(value[:3]), int(value[3:5])
    return f"{_roc_to_gregorian_year(roc_year)}-{month:02d}"


def _parse_roc_time(value: str) -> str | None:
    """Parse an up-to-6-digit HHMMSS time string (not zero-padded) to 'HH:MM:SS'."""
    if not value or not value.isdigit():
        return None
    value = value.zfill(6)
    return f"{value[0:2]}:{value[2:4]}:{value[4:6]}"


def _to_float(value: str) -> float | None:
    """Cast a TWSE/TPEx numeric-string field to float, or None if blank/invalid."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _today_roc_date() -> str:
    """Return today's date (UTC) as a 7-digit ROC-calendar string (YYYMMDD)."""
    now = datetime.now(timezone.utc)
    return f"{now.year - 1911}{now.month:02d}{now.day:02d}"


def _check_freshness(output_date: str, source_label: str) -> bool:
    """Compare a TWSE/TPEx response's 出表日期 (report/output date) against
    today's actual date and log a warning if they don't match.

    TWSE/TPEx's revenue and material-announcement endpoints always return
    only the single latest snapshot (no date/period query param exists —
    verified against the live Swagger spec), so 出表日期 is the only signal
    that today's poll actually got a fresh response rather than a stale
    cache or a delayed publish. Confirmed live that 出表日期 equals the
    actual current date under normal operation.

    Returns:
        True if the response is fresh (matches today), False otherwise.
        Never raises — a malformed/missing output_date is treated as stale
        rather than crashing the fetch.
    """
    today = _today_roc_date()
    if output_date == today:
        return True
    logger.warning(
        "[%s] response output_date=%r does not match today=%r —"
        " data may be stale (TWSE/TPEx delayed publish, or a cached"
        " response)",
        source_label, output_date, today,
    )
    return False


def _to_int(value: str) -> int | None:
    """Cast a TWSE/TPEx numeric-string field to int, or None if blank/invalid."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _taiwan_customers(ticker: str | None) -> list[dict[str, str]] | None:
    """The company's verified customers ({name, relationship}) from
    seed.TAIWAN_TICKER_UNIVERSE - the one source of truth - read from code at
    fetch time, not from the seeded sources.config, so editing the list takes
    effect on the next run without a DB update. None for a ticker outside the
    universe; [] means checked and none verified."""
    from seed import TAIWAN_TICKER_UNIVERSE  # lazy: seed imports db
    for t in TAIWAN_TICKER_UNIVERSE:
        if t["ticker"] == str(ticker or ""):
            return [{"name": c["name"], "relationship": c["relationship"]} for c in t.get("customers", [])]
    return None


def _fetch_one_revenue_dump(
    url: str, exchange: str, tickers: list[str], ticker_names: dict[str, str] | None = None,
) -> list[dict]:
    """Fetch one TWSE/TPEx monthly-revenue full-dump endpoint, filtered by ticker.

    Both TWSE (t187ap05_L) and TPEx (mopsfin_t187ap05_O) return an identical
    14-key Chinese-language schema, all values as strings, and no ticker
    query param — the full company roster is pulled every time and filtered
    client-side against ``tickers``.

    Args:
        url: Full-dump JSON endpoint URL.
        exchange: "TWSE" or "TPEx", stored in metadata and used to build a
            synthesized dedup URL (revenue rows have no natural article URL).
        tickers: Ticker codes to keep; rows for other companies are dropped.
        ticker_names: Known-correct English company name per ticker, from
            TAIWAN_TICKER_UNIVERSE (seed.py). Set directly as
            metadata.translated_company_name here rather than asking an
            LLM to translate 公司名稱 downstream - confirmed live that
            LLM translation of bare 2-4 character Taiwan company names
            produces serious errors (e.g. 2383 Elite Material
            mistranslated as "Taiwan Semiconductor Manufacturing
            Company"; 8210 Chenbro translated literally as "Diligence
            and sincerity"). We have the ground truth here; no reason to
            let a model guess it.

    Returns:
        List of article dicts with ``_pub_date`` set (last day of the
        reported period) and a synthesized ``url`` for dedup.
    """
    t0 = time.perf_counter()
    ticker_set = set(tickers)
    ticker_names = ticker_names or {}
    try:
        if exchange == "TPEx":
            resp = _get_with_ssl_retry(url, headers=_TWSE_TPEX_HEADERS, timeout=30.0)
        else:
            resp = httpx.get(url, headers=_TWSE_TPEX_HEADERS, timeout=30.0)
            resp.raise_for_status()
        rows = resp.json()
    except Exception as exc:
        logger.warning("[%s_REVENUE] failed: %s", exchange.upper(), exc)
        return []

    # 出表日期 (report/output date) is identical across every row in a
    # response — TWSE/TPEx always return a single current snapshot with no
    # date/period query param, so this is the only signal that today's poll
    # actually got fresh data rather than a stale cache or delayed publish.
    is_fresh = (
        _check_freshness(rows[0].get("出表日期", ""), f"{exchange.upper()}_REVENUE")
        if rows else True
    )

    results = []
    for row in rows:
        ticker = row.get("公司代號", "")
        if ticker not in ticker_set:
            continue
        period = _parse_roc_period(row.get("資料年月", ""))
        if not period:
            continue
        mom_pct = _to_float(row.get("營業收入-上月比較增減(%)", ""))
        yoy_pct = _to_float(row.get("營業收入-去年同月增減(%)", ""))
        remarks = row.get("備註", "")
        results.append({
            "title": (
                f"{row.get('公司名稱', ticker)} ({ticker}) {period} revenue:"
                f" {yoy_pct:+.1f}% YoY" if yoy_pct is not None
                else f"{row.get('公司名稱', ticker)} ({ticker}) {period} revenue"
            ),
            "url": f"{exchange.lower()}-revenue://{ticker}/{period}",
            "published": period,
            "source": exchange,
            "summary": None,
            "body": None,
            "_pub_date": datetime.strptime(period, "%Y-%m").replace(
                tzinfo=timezone.utc
            ),
            "metadata": {
                "ticker": ticker,
                "company_name": row.get("公司名稱"),
                "industry": row.get("產業別"),
                "period_gregorian": period,
                "revenue_current_month": _to_int(row.get("營業收入-當月營收", "")),
                "revenue_prior_month": _to_int(row.get("營業收入-上月營收", "")),
                "revenue_prior_year_month": _to_int(
                    row.get("營業收入-去年當月營收", "")
                ),
                "mom_pct": mom_pct,
                "yoy_pct": yoy_pct,
                "revenue_ytd": _to_int(row.get("累計營業收入-當月累計營收", "")),
                "revenue_ytd_prior_year": _to_int(
                    row.get("累計營業收入-去年累計營收", "")
                ),
                "ytd_yoy_pct": _to_float(
                    row.get("累計營業收入-前期比較增減(%)", "")
                ),
                "remarks": remarks if remarks and remarks != "-" else None,
                "exchange": exchange,
                "is_stale": not is_fresh,
                "source_category": "mops_revenue",
                "translated_company_name": ticker_names.get(ticker),
                "customers": _taiwan_customers(ticker),
            },
        })
    logger.info(
        "[TIMER] %s_revenue url=%r matched=%d/%d elapsed=%.2fs fresh=%s",
        exchange.lower(), url, len(results), len(rows), time.perf_counter() - t0,
        is_fresh,
    )
    return results


def _fetch_twse_revenue(sources: list[dict]) -> list[dict]:
    """Fetch TWSE monthly revenue for tickers across all twse_revenue sources."""
    tickers: list[str] = []
    ticker_names: dict[str, str] = {}
    for source in sources:
        config = source.get("config") or {}
        tickers.extend(config.get("tickers", []))
        ticker_names.update(config.get("ticker_names", {}))
    if not tickers:
        return []
    return _fetch_one_revenue_dump(_TWSE_REVENUE_URL, "TWSE", tickers, ticker_names)


def _fetch_tpex_revenue(sources: list[dict]) -> list[dict]:
    """Fetch TPEx monthly revenue for tickers across all tpex_revenue sources."""
    tickers: list[str] = []
    ticker_names: dict[str, str] = {}
    for source in sources:
        config = source.get("config") or {}
        tickers.extend(config.get("tickers", []))
        ticker_names.update(config.get("ticker_names", {}))
    if not tickers:
        return []
    return _fetch_one_revenue_dump(_TPEX_REVENUE_URL, "TPEx", tickers, ticker_names)


# TWSE and TPEx material-announcement feeds use different key names for the
# same fields (TWSE: Chinese keys throughout; TPEx: English keys for
# ticker/company/date). This maps both onto one common field name.
_TWSE_MATERIAL_KEYS = {
    "output_date": "出表日期",
    "ticker": "公司代號",
    "company_name": "公司名稱",
    "subject": "主旨 ",  # trailing space is part of TWSE's actual key
}
_TPEX_MATERIAL_KEYS = {
    "output_date": "Date",
    "ticker": "SecuritiesCompanyCode",
    "company_name": "CompanyName",
    "subject": "主旨",
}


def _fetch_one_material_dump(
    url: str,
    exchange: str,
    tickers: list[str],
    key_map: dict[str, str],
    ticker_names: dict[str, str] | None = None,
) -> list[dict]:
    """Fetch one TWSE/TPEx material-announcements full-dump endpoint, filtered by ticker.

    Args:
        url: Full-dump JSON endpoint URL.
        exchange: "TWSE" or "TPEx".
        tickers: Ticker codes to keep; rows for other companies are dropped.
        key_map: Maps common field names to this exchange's actual JSON keys
            (TWSE and TPEx disagree on key names — see module-level constants).
        ticker_names: Known-correct English company name per ticker - see
            _fetch_one_revenue_dump's docstring for why this is set
            directly rather than left to an LLM translation step.

    Returns:
        List of article dicts with ``_pub_date`` set (statement date/time)
        and a synthesized ``url`` for dedup.
    """
    t0 = time.perf_counter()
    ticker_set = set(tickers)
    ticker_names = ticker_names or {}
    try:
        if exchange == "TPEx":
            resp = _get_with_ssl_retry(url, headers=_TWSE_TPEX_HEADERS, timeout=30.0)
        else:
            resp = httpx.get(url, headers=_TWSE_TPEX_HEADERS, timeout=30.0)
            resp.raise_for_status()
        rows = resp.json()
    except Exception as exc:
        logger.warning("[%s_MATERIAL] failed: %s", exchange.upper(), exc)
        return []

    # See _fetch_one_revenue_dump for why this check exists — same "always
    # latest snapshot, no date param" API shape applies here.
    is_fresh = (
        _check_freshness(
            rows[0].get(key_map["output_date"], ""), f"{exchange.upper()}_MATERIAL"
        )
        if rows else True
    )

    results = []
    for row in rows:
        ticker = row.get(key_map["ticker"], "")
        if ticker not in ticker_set:
            continue
        statement_date = _parse_roc_date(row.get("發言日期", ""))
        statement_time = _parse_roc_time(row.get("發言時間", ""))
        fact_date = _parse_roc_date(row.get("事實發生日", ""))
        company_name = row.get(key_map["company_name"], ticker)
        subject = row.get(key_map["subject"], "")
        date_key = (
            statement_date.strftime("%Y-%m-%d") if statement_date else "unknown"
        )
        results.append({
            "title": f"{company_name} ({ticker}): {subject}",
            "url": (
                f"{exchange.lower()}-material://{ticker}/{date_key}"
                f"/{row.get('發言時間', '0')}"
            ),
            "published": statement_date.isoformat() if statement_date else "",
            "source": exchange,
            "summary": _clean_summary(subject),
            "body": _clean_summary(row.get("說明", "") or ""),
            "_pub_date": statement_date,
            "metadata": {
                "ticker": ticker,
                "company_name": company_name,
                "disclosure_clause_code": row.get("符合條款"),
                "statement_date": (
                    statement_date.strftime("%Y-%m-%d") if statement_date else None
                ),
                "statement_time": statement_time,
                "fact_occurred_date": (
                    fact_date.strftime("%Y-%m-%d") if fact_date else None
                ),
                "exchange": exchange,
                "is_stale": not is_fresh,
                "source_category": "mops_material",
                "translated_company_name": ticker_names.get(ticker),
                "customers": _taiwan_customers(ticker),
            },
        })
    logger.info(
        "[TIMER] %s_material url=%r matched=%d/%d elapsed=%.2fs fresh=%s",
        exchange.lower(), url, len(results), len(rows), time.perf_counter() - t0,
        is_fresh,
    )
    return results


def _fetch_twse_material(sources: list[dict]) -> list[dict]:
    """Fetch TWSE material announcements for tickers across all twse_material sources."""
    tickers: list[str] = []
    ticker_names: dict[str, str] = {}
    for source in sources:
        config = source.get("config") or {}
        tickers.extend(config.get("tickers", []))
        ticker_names.update(config.get("ticker_names", {}))
    if not tickers:
        return []
    return _fetch_one_material_dump(
        _TWSE_MATERIAL_URL, "TWSE", tickers, _TWSE_MATERIAL_KEYS, ticker_names,
    )


def _fetch_tpex_material(sources: list[dict]) -> list[dict]:
    """Fetch TPEx material announcements for tickers across all tpex_material sources."""
    tickers: list[str] = []
    ticker_names: dict[str, str] = {}
    for source in sources:
        config = source.get("config") or {}
        tickers.extend(config.get("tickers", []))
        ticker_names.update(config.get("ticker_names", {}))
    if not tickers:
        return []
    return _fetch_one_material_dump(
        _TPEX_MATERIAL_URL, "TPEx", tickers, _TPEX_MATERIAL_KEYS, ticker_names,
    )


# DART (Financial Supervisory Service) filing list + document body fetch.
# Covers Korea Signals spec Section 4's S2 (supply contract), S3 (capacity
# commitment), S4 (preliminary earnings), S5 (guidance disclosure), and S6
# (rumour adjudication) - all five live under DART's "I" (거래소공시/exchange
# disclosure) pblntf_ty category, since DART mirrors KRX-mandated
# disclosures rather than having a distinct structured endpoint per report
# type (confirmed 2026-09-22 against DART's own official pblntf_detail_ty
# taxonomy - no dedicated code exists for e.g. "supply contract"
# specifically). Report-type identification is therefore by matching
# report_nm text, not a query parameter.
#
# S6 was an open question (does DART carry the exchange's rumour-demand
# separately from the company's answer?) - resolved live 2026-09-22 against
# a real SK Hynix example (corp_code 00164779, rcept_no 20260821800214 /
# 20260821800524): YES, DART carries both as distinct filings, and flr_nm
# distinguishes them (exchange = "유가증권시장본부", company = the ticker's
# own name). The precision rule 5.6 needs ("hours between the two") is NOT
# available from list.json's rcept_dt (date-only, no time component) - both
# filings showed the same date. It IS recoverable, but only from inside the
# ANSWER filing's document body, which restates the original demand time as
# free text, e.g. "거래소의 조회요구(2026년 08월 21일 11:35)에 따른
# 공시사항임". Extracting that timestamp from body text is a downstream
# parsing task (signal-detection-agent), not done here - this fetcher's job
# is only to ensure both filings, bodies included, reach storage (see
# "조회공시" in _DART_TARGET_REPORT_PATTERNS below).
#
# S1 (export data) is NOT covered here - separate source (Korea Customs).
_DART_LIST_URL = "https://opendart.fss.or.kr/api/list.json"
_DART_DOCUMENT_URL = "https://opendart.fss.or.kr/api/document.xml"

# Substrings matched against report_nm (DART's filing title) to decide which
# filings are worth a full document-body fetch. Matched as "contains", not
# exact-equals, since real report_nm values append suffixes (e.g. a
# correction: "...체결정정"). Left un-narrowed beyond this for now - false
# positives here cost one extra document fetch, not a wrong classification
# (classification itself is signal-detection-agent's job, not fetched here).
_DART_TARGET_REPORT_PATTERNS = [
    "단일판매",       # S2 - single-sale/supply contract conclusion
    "공급계약",       # S2 - supply contract (alternate phrasing)
    "시설투자",       # S3 - facility/capacity investment
    "유형자산",       # S3 - tangible asset acquisition (equipment purchases)
    "잠정",           # S4 - preliminary earnings. Confirmed live 2026-09-23
                      # the real report_nm is
                      # "연결재무제표기준영업(잠정)실적(공정공시)" - "(잠정)"
                      # sits as a bracketed qualifier INSIDE the compound
                      # word, not concatenated as a single
                      # "잠정실적"/"잠정영업실적" substring. The two
                      # previous patterns here never matched a single real
                      # filing (verified: 14 real S4 filings in stored
                      # data, all fetched only because "공정공시" below
                      # also matched their same title) - this fixes the
                      # dead patterns rather than leaving them as
                      # harmless-looking but non-functional dead code.
    "공정공시",       # S5 - fair disclosure (voluntary guidance)
    "조회공시",       # S6 - rumour adjudication (both the exchange's demand
                      # and the company's answer use this term; confirmed
                      # live 2026-09-22 both filings are needed - the
                      # precise demand timestamp is NOT in list.json's
                      # rcept_dt (date-only), it's embedded as text inside
                      # the ANSWER filing's body, e.g. "거래소의 조회요구
                      # (2026년 08월 21일 11:35)" - so both filings' bodies
                      # must be fetched, not just the answer's.
    "해명",           # S6-adjacent - 자율적 해명공시 (voluntary clarification
                      # disclosure), report_nm "풍문또는보도에대한해명" - a
                      # DISTINCT mechanism from 조회공시 confirmed via
                      # research 2026-09-23: introduced by KRX effective
                      # 2015-09-07, this is COMPANY-initiated (a company
                      # clarifying a rumor on its own, without the
                      # exchange demanding it), unlike 조회공시 which is
                      # exchange-compelled. Real examples found live
                      # (SK Hynix 2026-07-22 and 2026-09-04, Samsung
                      # Electronics 2026-07-23) had NO body fetched before
                      # this pattern was added, since "해명" shares no
                      # substring with "조회공시" - classified separately
                      # in signal-detection-agent
                      # (classify_voluntary_clarification), not folded
                      # into the exchange-demand rule.
]

_dart_rate_lock = threading.Lock()
_dart_last_call = [0.0]
_DART_MIN_INTERVAL = 0.25  # conservative pending confirmation of DART's actual rate limit


def _dart_rate_sleep() -> None:
    with _dart_rate_lock:
        elapsed = time.monotonic() - _dart_last_call[0]
        if elapsed < _DART_MIN_INTERVAL:
            time.sleep(_DART_MIN_INTERVAL - elapsed)
        _dart_last_call[0] = time.monotonic()


def _dart_matches_target_report(report_nm: str) -> bool:
    return any(pattern in report_nm for pattern in _DART_TARGET_REPORT_PATTERNS)


def _fetch_dart_document_body(corp_code: str, rcept_no: str, api_key: str) -> str | None:
    """Fetch and extract plain text from a DART filing's document.xml.

    Despite the .xml filename/extension, the member DART actually zips is
    XForms-flavored HTML (confirmed live 2026-09-22: content starts with
    "<html><head>...charset=euc-kr"), not well-formed XML - a strict XML
    parser fails on it (unescaped "&", mismatched/unclosed tags are normal
    in real filings). Stripped the same way sec_edgar.py's
    fetch_filing_text() handles SEC's HTML filings: drop script/style
    blocks, strip tags, unescape entities.

    The declared "charset=euc-kr" meta tag is WRONG - confirmed live by
    decoding the raw bytes both ways: euc-kr produces mojibake on every
    Korean character, utf-8 produces correct readable Korean (e.g. the
    font-name string decodes cleanly as "돋움체" under utf-8, garbage under
    euc-kr). This is a stale/incorrect header on DART's side, not
    something to infer from - decode as utf-8 regardless of what the
    document claims.

    Returns None on any failure - callers treat a missing body as "not yet
    fetched", not an error, same fail-open convention as the rest of this
    pipeline.
    """
    import zipfile
    from io import BytesIO

    _dart_rate_sleep()
    try:
        resp = httpx.get(
            _DART_DOCUMENT_URL,
            params={"crtfc_key": api_key, "rcept_no": rcept_no},
            timeout=30.0,
        )
        resp.raise_for_status()
        # On failure DART returns a small XML error envelope instead of a
        # zip - confirmed live 2026-09-23 (status "014", message "파일이
        # 존재하지 않습니다"/file does not exist) for a [첨부정정]
        # (attachment-correction) filing, which apparently has no document
        # body of its own to fetch. Checked explicitly before attempting
        # to unzip so this fails with the real reason, not a misleading
        # "not a zip file" error that gives no clue what actually happened.
        if resp.content[:5] != b"PK\x03\x04" and b"<status>" in resp.content[:200]:
            logger.warning(
                "[DART] document.xml returned an error envelope for"
                " rcept_no=%s (not a zip) - body: %r",
                rcept_no, resp.content[:200],
            )
            return None
        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            # DART zips a single member per filing; name varies (usually
            # "{rcept_no}.xml" but not guaranteed), so take whichever
            # member is present rather than assuming a fixed filename
            # (unlike corpCode.xml's fixed CORPCODE.xml).
            names = zf.namelist()
            if not names:
                return None
            raw = zf.read(names[0])
        decoded = raw.decode("utf-8", errors="replace")
        stripped = re.sub(r"<(script|style)[^>]*>.*?</\1>", " ", decoded, flags=re.S | re.I)
        stripped = re.sub(r"<[^>]+>", " ", stripped)
        stripped = html.unescape(stripped)
        return re.sub(r"\s+", " ", stripped).strip() or None
    except Exception as exc:
        logger.warning(
            "[DART] document fetch failed corp_code=%s rcept_no=%s error=%s",
            corp_code, rcept_no, exc,
        )
        return None


def _fetch_one_dart_filing_list(
    corp_code: str,
    stock_code: str,
    company_name: str,
    api_key: str,
    bgn_de: str,
    end_de: str,
) -> list[dict]:
    """Fetch DART's filing list for one company over a date range.

    Returns article dicts with a synthetic url (dart-filing://corp_code/
    rcept_no) so the DB's existing global-unique-url dedup applies without
    any DART-specific dedup logic. Full document body is fetched only for
    report_nm values matching _DART_TARGET_REPORT_PATTERNS.
    """
    _dart_rate_sleep()
    try:
        resp = httpx.get(
            _DART_LIST_URL,
            params={
                "crtfc_key": api_key,
                "corp_code": corp_code,
                "bgn_de": bgn_de,
                "end_de": end_de,
                "pblntf_ty": "I",
                "page_count": 100,
            },
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[DART] list fetch failed corp_code=%s error=%s", corp_code, exc)
        return []

    # DART returns status "013" (no data) as a normal empty result, not an
    # error - only log/skip other non-success codes.
    status = data.get("status")
    if status not in ("000", "013"):
        logger.warning(
            "[DART] list fetch corp_code=%s status=%s message=%s",
            corp_code, status, data.get("message"),
        )
    filings = data.get("list") or []

    results = []
    for filing in filings:
        report_nm = filing.get("report_nm", "")
        rcept_no = filing.get("rcept_no", "")
        rcept_dt = filing.get("rcept_dt", "")
        pub_date = (
            datetime.strptime(rcept_dt, "%Y%m%d").replace(tzinfo=timezone.utc)
            if rcept_dt else None
        )
        body = (
            _fetch_dart_document_body(corp_code, rcept_no, api_key)
            if _dart_matches_target_report(report_nm) else None
        )
        results.append({
            "title": f"{company_name} ({stock_code}): {report_nm}",
            "url": f"dart-filing://{corp_code}/{rcept_no}",
            "published": rcept_dt,
            "source": filing.get("flr_nm") or "DART",
            "summary": None,
            "body": body,
            "_pub_date": pub_date,
            "metadata": {
                "corp_cls": filing.get("corp_cls"),
                "corp_code": corp_code,
                "stock_code": stock_code,
                "rcept_no": rcept_no,
                "rm": filing.get("rm"),
                "pblntf_ty": "I",
                "source_category": "dart_filing",
            },
        })
    return results


def _fetch_dart_filing(sources: list[dict], days_back: int, api_key: str) -> list[dict]:
    """Fetch DART filings for all companies across all dart_filing sources.

    Resolves stock_code -> corp_code once (cached process-lifetime by
    dart_corp_code), then queries the filing list per company. Companies
    with no resolvable corp_code (e.g. an unlisted entity with no
    stock_code at all) are skipped with a warning, not an error - matches
    KOREA_TICKER_UNIVERSE's Hanwha Semitech entry, which is intentionally
    absent from every dart_filing source's ticker config for this reason.
    """
    tickers: list[dict[str, str]] = []
    for source in sources:
        config = source.get("config") or {}
        tickers.extend(config.get("companies", []))
    if not tickers:
        return []

    stock_codes = [t["ticker"] for t in tickers]
    corp_code_map = resolve_corp_codes(stock_codes, api_key)

    bgn_de = (datetime.now(timezone.utc) - timedelta(days=days_back)).strftime("%Y%m%d")
    end_de = datetime.now(timezone.utc).strftime("%Y%m%d")

    articles: list[dict] = []
    for t in tickers:
        stock_code = t["ticker"]
        corp_code = corp_code_map.get(stock_code)
        if not corp_code:
            logger.warning("[DART] skipping stock_code=%s - no corp_code resolved", stock_code)
            continue
        articles.extend(
            _fetch_one_dart_filing_list(
                corp_code, stock_code, t["company"], api_key, bgn_de, end_de,
            )
        )
    return articles


# Korea Customs Service export-figure press-release scraper (S1 - export
# surprise). Fallback for the official data.go.kr 10-day provisional
# export-statistics API (dataset 15157908), which requires a personal
# account gated behind Korea's phone/ARC-linked identity verification - not
# obtainable without a Korean-resident proxy. This scrapes the same
# underlying figure from Korea Customs' own public press-release board
# instead, which needs no login at all.
#
# Confirmed live 2026-09-23: no RSS feed exists for this board, and post
# IDs (nttSn) are a global auto-increment across ~6,600+ unrelated posts
# (drug busts, personnel notices, etc.) - not usable as a date-based
# pattern. The only reliable way to find the newest release is the
# board's own server-side search, which is a POST (not GET query params -
# a GET-param guess 404'd first), with the recurring report family
# isolated by searching the title (searchType=sj) for "수출입 현황".
_CUSTOMS_LIST_URL = "https://www.customs.go.kr/kcs/na/ntt/selectNttList.do"
_CUSTOMS_INFO_URL = "https://www.customs.go.kr/kcs/na/ntt/selectNttInfo.do"
_CUSTOMS_BBS_ID = "1362"
_CUSTOMS_MI = "2891"
_CUSTOMS_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    " (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_CUSTOMS_LIST_PAGE_MAX_ATTEMPTS = 3
_CUSTOMS_LIST_PAGE_RETRY_BACKOFF_SECS = 2.0

# Confirmed live 2026-09-23 against real posts: the 10-day/20-day
# preliminary releases only ever state the semiconductor figure as a
# dollar value plus a superlative record/streak claim - e.g.
# "반도체(341억 달러) 수출 동기간 역대최대" (semiconductor exports of
# $34.1bn, a period record) - never a year-over-year percentage or a
# share-of-total-exports percentage in the page's own HTML text (checked
# every occurrence of "반도체" on a real 20-day release page, found
# exactly one, matching this pattern; also searched for "차지" - the verb
# used for "share of" - and found zero matches). The YoY% Section 5.1
# needs is therefore NOT available from the source at 10-day granularity
# and must be computed by this pipeline itself, from its own stored
# history of these dollar figures - matching the doc's own build note
# that this rule needs two years of history before it means anything.
#
# "억" is a Korean numeral unit = 100,000,000 (one hundred million) - "341
# 억" is not a typo/OCR artifact, it is standard Korean notation for large
# won/dollar figures and must be multiplied out, not read digit-by-digit.
# Confirmed live 2026-09-23 against real historical posts (backfill
# verification): the bracketed semiconductor figure is stated as either
# "반도체(341억 달러)" (10-day/20-day releases) or "반도체 수출(468억 달러)"
# (monthly confirmed releases - an extra "수출"/export word inserted
# between 반도체 and the parenthesis) - both real phrasings, not a typo in
# one of them. The optional "(?:수출)?" covers both without a second
# pattern.
_CUSTOMS_SEMICONDUCTOR_PATTERN = re.compile(
    r"반도체\s*(?:수출)?\s*\(\s*([\d,]+)\s*억\s*달러\s*\)"
)
_CUSTOMS_PERIOD_PATTERN = re.compile(
    r"(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(?:~|-)\s*(?:(\d{4})년\s*)?(\d{1,2})월\s*(\d{1,2})일\s*수출입\s*현황"
)
# Monthly CONFIRMED release title, e.g. "2026년 8월 월간 수출입 현황
# [확정치]" - distinct from the day-range pattern above (no day numbers,
# has "월간"/monthly) and from the monthly PROVISIONAL title (same month
# number but no "월간" word, e.g. "2026년 8월 수출입 현황 [잠정치]" -
# confirmed live this provisional monthly variant has no bracketed
# semiconductor figure in extractable form, so it is deliberately NOT
# matched by this pattern nor produced by classify_export_surprise's
# period grouping - see _classify_customs_period_type's own docstring).
_CUSTOMS_MONTHLY_CONFIRMED_PERIOD_PATTERN = re.compile(
    r"(\d{4})년\s*(\d{1,2})월\s*월간\s*수출입\s*현황\s*\[\s*확정치\s*\]"
)
# Confirmed live: only the monthly CONFIRMED release states a real YoY%
# in its own body text ("전년 동월 대비 수출은 68.7% 증가") - 10-day/20-day
# releases never do (checked live, see module comment on
# _CUSTOMS_SEMICONDUCTOR_PATTERN's own history). This is the TOTAL export
# YoY%, not semiconductor-specific - used only as a cross-check value,
# never as classify_export_surprise's own computed semiconductor YoY
# (which is always computed by this pipeline from its own stored history,
# per spec Section 5.1's own design intent).
_CUSTOMS_YOY_PATTERN = re.compile(
    r"전년\s*동월\s*대비\s*수출\s*은?\s*([\d.]+)\s*%\s*증가"
)
# Confirmed live 2026-09-23 against a real May 2026 monthly CONFIRMED
# post that had NO bracketed dollar figure at all (unlike June/July/
# August's same-type posts, which do - real inconsistency between
# individual releases, not a parsing gap): this same post instead states
# a real, SEMICONDUCTOR-SPECIFIC (not total-export) YoY% series, e.g.
# "반도체 전년동월대비 증감률(%): ['25.10월] 25.2 → [11월] 38.7 → ... →
# [5월] 167.7" - a genuinely better data point than the dollar figure for
# S1's own purposes (it's already the exact YoY comparison Section 5.1
# needs, semiconductor-specific, and it comes with several trailing
# months embedded in the same sentence). Captures everything after the
# label up to the next "*"-prefixed series (the next line is always a
# different product's own series, e.g. "승용차"/passenger cars) or end of
# string.
_CUSTOMS_SEMI_YOY_SERIES_HEADER_PATTERN = re.compile(
    r"반도체\s*전년동월대비\s*증감률\s*\(%\)\s*:\s*(.+?)(?:\*|$)"
)
# One entry in that series: "['25. 10월] 25.2" or "[11월] 38.7" (year
# omitted when unchanged from the previous entry - real behavior
# confirmed live, not every entry restates it) with an optional "△"
# prefix meaning negative (a real minus sign glyph used throughout this
# board's tables, confirmed live on the same page's "승용차" series
# showing "△12.6" for an actual year-on-year decline). The apostrophe
# before the 2-digit year is a RIGHT SINGLE QUOTATION MARK (U+2019, "'"),
# not an ASCII apostrophe (confirmed live via direct byte inspection of a
# real fetched page - a plain "'?" silently matched nothing here and made
# every entry in a real series fail to parse, since the year group never
# matched at all). Both accepted, in case a straight apostrophe appears
# in some other real post never checked.
_CUSTOMS_SEMI_YOY_SERIES_ENTRY_PATTERN = re.compile(
    r"\[\s*(?:['’]?(\d{2})\.\s*)?(\d{1,2})\s*월\s*\]\s*(△?)\s*([\d.]+)"
)


def _fetch_customs_list_page(page: int) -> list[dict]:
    """Return every '수출입 현황' post on one page of the board's title
    search results (confirmed live 2026-09-23: ~10 posts/page, 47 total
    pages at the time of checking - real history reaching back through at
    least mid-2025, likely further).

    Post links are JS-driven (href="javascript:"), not real <a href>
    URLs - the real post identifiers live in data-id (nttSn) and
    data-url (nttSnUrl) attributes on the same anchor tag, alongside its
    title attribute, filtered to the nttInfoBtn class used for this
    board's title links specifically. Returns [] after
    ``_CUSTOMS_LIST_PAGE_MAX_ATTEMPTS`` failed attempts - fail-open, same
    convention as every other fetcher in this module, but retried first:
    confirmed live this board can return a transient "server disconnected"
    error on an otherwise-real, non-empty page (seen during backfill
    verification) - without a retry, that transient error is
    indistinguishable from "this page is genuinely past the end of
    results" to fetch_customs_export_backfill's own stopping condition,
    which would silently truncate a real backfill run short.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _CUSTOMS_LIST_PAGE_MAX_ATTEMPTS + 1):
        try:
            resp = httpx.post(
                _CUSTOMS_LIST_URL,
                data={
                    "bbsId": _CUSTOMS_BBS_ID,
                    "mi": _CUSTOMS_MI,
                    "searchType": "sj",
                    "searchValue": "수출입 현황",
                    "currPage": str(page),
                },
                headers={"User-Agent": _CUSTOMS_USER_AGENT},
                timeout=30.0,
            )
            resp.raise_for_status()
            break
        except Exception as exc:
            last_exc = exc
            if attempt < _CUSTOMS_LIST_PAGE_MAX_ATTEMPTS:
                logger.warning(
                    "[CUSTOMS] list fetch attempt %d/%d failed page=%d, retrying: %s",
                    attempt, _CUSTOMS_LIST_PAGE_MAX_ATTEMPTS, page, exc,
                )
                time.sleep(_CUSTOMS_LIST_PAGE_RETRY_BACKOFF_SECS)
    else:
        logger.warning(
            "[CUSTOMS] list fetch failed page=%d after %d attempts: %s",
            page, _CUSTOMS_LIST_PAGE_MAX_ATTEMPTS, last_exc,
        )
        return []

    return [
        {"ntt_sn": ntt_sn, "ntt_url": ntt_url, "title": html.unescape(title)}
        for ntt_sn, ntt_url, title in re.findall(
            r'data-id="(\d+)"\s+data-url="([a-f0-9]+)"\s+class="nttInfoBtn"'
            r'\s+title="([^"]+)"',
            resp.text,
        )
    ]


def _fetch_customs_newest_post(days_back: int) -> dict | None:
    """Find the newest '수출입 현황' post via the board's title search
    (page 1's first result - confirmed live the board lists newest-first).

    Returns {"ntt_sn": str, "ntt_url": str, "title": str} for the newest
    matching post, or None on any failure - fail-open, same convention as
    every other fetcher in this module.
    """
    posts = _fetch_customs_list_page(1)
    if not posts:
        logger.warning("[CUSTOMS] no matching post found in search results")
        return None
    return posts[0]


def _classify_customs_period_type(title: str) -> tuple[str, dict[str, Any]] | None:
    """Classify a real Customs board title into one of the THREE period
    types Korea Signals spec Section 5.1 actually asks for ("days 1-10,
    days 1-20, or full month") - returns (period_type, fields) or None if
    the title doesn't match any of the three.

    Confirmed live 2026-09-23 against real historical titles: at least 5
    distinct title shapes exist on this board, not 3 - a monthly
    PROVISIONAL release (e.g. "2026년 8월 수출입 현황 [잠정치]", no "월간"
    word) that has no bracketed semiconductor figure in extractable plain
    text, and a "기업규모별" (by company size) report that doesn't mention
    semiconductors at all. Both are deliberately NOT matched here -
    returning None for them, not a guessed period_type - since forcing
    them into one of the three real types would either silently produce
    a NULL semiconductor value or double-count a period already covered
    by the monthly CONFIRMED release for the same month.

    period_type is one of "10day", "20day", "monthly" - "monthly" always
    refers to the CONFIRMED release specifically (the only monthly
    variant this function ever returns), never the provisional one.
    """
    day_range = _CUSTOMS_PERIOD_PATTERN.search(title)
    if day_range:
        y1, m1, d1, y2, m2, d2 = day_range.groups()
        y2 = y2 or y1
        period_start = f"{y1}-{int(m1):02d}-{int(d1):02d}"
        period_end = f"{y2}-{int(m2):02d}-{int(d2):02d}"
        period_type = "10day" if int(d2) <= 10 else "20day"
        return period_type, {"period_start": period_start, "period_end": period_end}

    monthly = _CUSTOMS_MONTHLY_CONFIRMED_PERIOD_PATTERN.search(title)
    if monthly:
        y, m = monthly.groups()
        # A full-month period's "end" is the month's own last day - not
        # computed exactly here (would need real per-month day counts,
        # leap years); the first day of the NEXT month minus one day is
        # unnecessary precision for what published/period_end are used
        # for downstream (grouping/sorting by month, not exact-day math),
        # so period_end is left as the month's first day too, distinguished
        # from period_start only by period_type="monthly" - a caller
        # needing the real last day should derive it from period_type,
        # not assume period_end is that day.
        period_start = f"{y}-{int(m):02d}-01"
        return "monthly", {"period_start": period_start, "period_end": period_start}

    return None


def _parse_customs_semi_yoy_series(text: str) -> dict[str, float]:
    """Parse the semiconductor-specific YoY% trailing series (see
    _CUSTOMS_SEMI_YOY_SERIES_HEADER_PATTERN's own docstring for a real
    example) into {"YYYY-MM": pct, ...}.

    A year digit is only restated in the source when it changes from the
    previous entry (confirmed live: "['25.10월] 25.2 → [11월] 38.7" - Nov
    doesn't repeat '25) - carried forward here from the last entry that
    DID state one. An entry appearing before any year has been stated at
    all is skipped (can't anchor it to a real year) rather than guessed.
    """
    header = _CUSTOMS_SEMI_YOY_SERIES_HEADER_PATTERN.search(text)
    if not header:
        return {}
    series: dict[str, float] = {}
    current_year: int | None = None
    for year_suffix, month, sign, value in _CUSTOMS_SEMI_YOY_SERIES_ENTRY_PATTERN.findall(
        header.group(1)
    ):
        if year_suffix:
            current_year = 2000 + int(year_suffix)
        if current_year is None:
            continue
        try:
            pct = float(value)
        except ValueError:
            continue
        if sign == "△":
            pct = -pct
        series[f"{current_year}-{int(month):02d}"] = pct
    return series


def _extract_customs_figures(text: str) -> dict[str, Any]:
    """Pull the semiconductor export figure, the semiconductor-specific
    YoY% series (if present - monthly CONFIRMED releases only, and not
    even every one of those, see _CUSTOMS_SEMI_YOY_SERIES_HEADER_PATTERN's
    own docstring), and the total-export YoY% cross-check value (see
    _CUSTOMS_YOY_PATTERN's own docstring for why this is never used as
    classify_export_surprise's own computed YoY) out of a Customs post's
    plain-text body.

    Returns {} if NEITHER the dollar figure NOR the YoY series was found -
    a real, expected outcome for some post shapes (see
    _classify_customs_period_type's own docstring), not treated as an
    error by any caller. A post with the series but no dollar figure (the
    real May/April/March/Feb/Jan 2026 monthly examples found during
    backfill verification) still returns real data via the series alone -
    _build_customs_article's own "no semiconductor figure" check looks
    for either field, not just the dollar one.
    """
    fields: dict[str, Any] = {}
    semi_match = _CUSTOMS_SEMICONDUCTOR_PATTERN.search(text)
    if semi_match:
        fields["semiconductor_export_usd_billion"] = (
            int(semi_match.group(1).replace(",", "")) / 10.0
        )
    semi_yoy_series = _parse_customs_semi_yoy_series(text)
    if semi_yoy_series:
        fields["semiconductor_yoy_pct_series"] = semi_yoy_series
        # The series' own last entry is this release's own period - a
        # direct, stated semiconductor YoY%, not the total-export one
        # _CUSTOMS_YOY_PATTERN captures.
        fields["semiconductor_yoy_pct_stated"] = semi_yoy_series[max(semi_yoy_series)]
    yoy_match = _CUSTOMS_YOY_PATTERN.search(text)
    if yoy_match:
        try:
            fields["total_export_yoy_pct_stated"] = float(yoy_match.group(1))
        except ValueError:
            pass
    return fields


def _fetch_customs_post_body(ntt_sn: str, ntt_url: str) -> str | None:
    """Fetch and clean one Customs board post's body text. Returns None on
    any failure - fail-open, same convention as every other fetcher here.

    Strips the per-word <span> fragments the HWP-to-HTML export wraps
    every few characters in - confirmed live these break a direct regex
    against the raw HTML, so tags must be stripped and whitespace
    collapsed first, same pattern as sec_edgar.py's HTML handling.
    """
    try:
        resp = httpx.get(
            _CUSTOMS_INFO_URL,
            params={"mi": _CUSTOMS_MI, "bbsId": _CUSTOMS_BBS_ID, "nttSn": ntt_sn, "nttSnUrl": ntt_url},
            headers={"User-Agent": _CUSTOMS_USER_AGENT},
            timeout=30.0,
        )
        resp.raise_for_status()
    except Exception as exc:
        logger.warning("[CUSTOMS] post fetch failed ntt_sn=%s: %s", ntt_sn, exc)
        return None
    text = re.sub(r"<[^>]+>", " ", resp.text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _build_customs_article(post: dict, text: str) -> dict | None:
    """Build one article dict from a Customs post's title + cleaned body,
    or None if the title isn't one of the three period types
    classify_export_surprise needs (_classify_customs_period_type) or
    NEITHER a semiconductor dollar figure NOR a semiconductor YoY% series
    was found in the body (_extract_customs_figures) - both real, expected
    outcomes for some post shapes, not errors.

    Shared by both the newest-only fetch (_fetch_customs_export_data) and
    the historical backfill (_fetch_customs_export_backfill) - one place
    defining what makes a post into a storable article, so the two
    fetchers can never silently diverge on that definition.
    """
    period_info = _classify_customs_period_type(post["title"])
    if period_info is None:
        return None
    period_type, period_fields = period_info

    figures = _extract_customs_figures(text)
    if not ({"semiconductor_export_usd_billion", "semiconductor_yoy_pct_stated"} & figures.keys()):
        logger.warning(
            "[CUSTOMS] no semiconductor figure or YoY series found in post ntt_sn=%s title=%r",
            post["ntt_sn"], post["title"],
        )
        return None

    period_end = period_fields["period_end"]
    pub_date = None
    try:
        pub_date = datetime.strptime(period_end, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        pub_date = None

    return {
        "title": post["title"],
        "url": f"kr-customs-export://semiconductor/{post['ntt_sn']}",
        "published": period_end,
        "source": "Korea Customs Service",
        "summary": None,
        "body": text[:5000],
        "_pub_date": pub_date,
        "metadata": {
            "ntt_sn": post["ntt_sn"],
            "period_type": period_type,
            **period_fields,
            **figures,
            "source_category": "kr_customs_export",
        },
    }


def _fetch_customs_export_data(sources: list[dict], days_back: int) -> list[dict]:
    """Fetch the newest Korea Customs semiconductor export figure.

    Only produces an article for the newest 10-day/20-day/monthly release
    found - this is the going-forward path the 4-hourly schedule runs
    every poll; historical backfill is a SEPARATE, one-time fetcher
    (_fetch_customs_export_backfill) - see that function's own docstring
    for why a real 2-year backfill turned out to be reachable through
    this same no-login board after all (confirmed live 2026-09-23,
    reversing this comment's own earlier claim that no backfill existed).

    url is deliberately keyed on ntt_sn (the post's own unique board ID),
    not on period alone - a distinct real post exists per release, so
    this is a natural, already-unique dedup key without needing to invent
    a period+revision scheme.
    """
    post = _fetch_customs_newest_post(days_back)
    if post is None:
        return []
    text = _fetch_customs_post_body(post["ntt_sn"], post["ntt_url"])
    if text is None:
        return []
    article = _build_customs_article(post, text)
    return [article] if article else []


# ~2 years of history at 3 releases/month (10-day + 20-day + monthly) plus
# the 2 non-target report shapes each month (monthly provisional,
# 기업규모별) that also appear in this same title search - confirmed live
# 2026-09-23 that ~5 posts/month appear across all shapes, so 2 years
# needs roughly 120 posts / ~10 posts-per-page = ~12 pages minimum;
# doubled for margin since real page density varies (some months had more
# report variants than others in the pages actually checked).
_CUSTOMS_BACKFILL_MAX_PAGES = 30
# Confirmed live 2026-09-23: this board's detail pages genuinely stop
# including inline breakdown text at some point in its history (March
# 2026 in the specific run checked) - everything older is a stub page
# linking to a .hwpx/.pdf attachment this scraper never opens. A real,
# permanent content-format boundary, not a transient gap - once a real
# run crosses it, every earlier post will also fail the same way, so
# there's no reason to keep walking further pages after a solid run of
# consecutive misses among otherwise-target-type posts. 4 is a small
# margin above 1 (a single miss could still be one of the OTHER real
# gaps this module already accepts - a genuinely missing figure on one
# real post, a transient fetch failure already retried and still failed)
# without walking dozens of pointless pages once the real boundary is
# crossed.
_CUSTOMS_BACKFILL_STOP_AFTER_CONSECUTIVE_MISSES = 4


def fetch_customs_export_backfill(max_pages: int = _CUSTOMS_BACKFILL_MAX_PAGES) -> list[dict]:
    """One-time historical backfill for korea_market_signal's
    kr_customs_export source - walks the SAME title-search board
    _fetch_customs_export_data already polls going forward, but paginates
    through it instead of only ever reading page 1's first match.

    Confirmed live 2026-09-23: this board's own title search already
    returns full pagination (~10 posts/page, 47 total pages at the time of
    checking) with no login or Korean identity verification required at
    all - directly reversing the earlier finding (recorded in
    KOREA_CUSTOMS_EXPORT_SOURCE's own seed.py comment, now stale) that
    "this scraper only ever sees whatever's newest right now" and that S1
    was therefore permanently out of scope. HOWEVER, also confirmed live:
    the board's own detail-page FORMAT changed at some point in its own
    history (March 2026, in the specific run checked) - posts from that
    point forward embed the full breakdown (including, for monthly
    CONFIRMED releases, a real semiconductor-specific YoY% series - see
    _extract_customs_figures) directly in the page's own HTML; everything
    older is a stub page linking to a .hwpx/.pdf attachment this scraper
    never opens. This means REAL backfillable depth today is roughly 7
    months (March-September 2026), not the full 2 years Section 5.1's own
    build note asks for - genuinely short of the 12 EQUIVALENT periods
    (12 prior same-period-type readings) that rule needs before it's
    computable, so a real first classification still needs several more
    months of the existing 4-hourly poll's own organic growth on top of
    this backfill, not an immediate fix. This function stops itself once
    it detects that older-format boundary (see
    _CUSTOMS_BACKFILL_STOP_AFTER_CONSECUTIVE_MISSES) rather than walking
    dozens of pages that can only ever return stub pages past that point.

    The existing 4-hourly scheduled fetch (_fetch_customs_export_data)
    still only reads page 1 - this function is a SEPARATE, one-time (or
    manually re-run) call, not part of that regular poll.

    Stops after ``max_pages`` (a real page limit, not a date-based one -
    the board has no date filter on this search, only pagination), as
    soon as a page returns zero posts (end of results), or once the
    older-format boundary is detected (see above) - whichever comes
    first. Not stopped by encountering an already-stored URL - unlike the
    regular fetch, a one-time backfill run is expected to see mostly-new
    URLs throughout, and stopping early on the first repeat would risk
    missing genuine gaps if pages are ever returned out of strict
    chronological order (not confirmed either way - safer to walk every
    page up to whichever real stopping condition fires first).

    Returns article dicts in the same shape _fetch_customs_export_data
    produces (via the same shared _build_customs_article) - posts that
    aren't one of the three target period types, or have no extractable
    semiconductor figure, are silently skipped (see
    _classify_customs_period_type / _extract_customs_figures's own
    docstrings for why that's a real, expected outcome, not an error).
    """
    articles: list[dict] = []
    skipped_period_type = 0
    skipped_no_figure = 0
    skipped_fetch_failed = 0
    consecutive_no_figure = 0

    for page in range(1, max_pages + 1):
        posts = _fetch_customs_list_page(page)
        if not posts:
            logger.info("[CUSTOMS_BACKFILL] page=%d empty, stopping", page)
            break
        for post in posts:
            period_info = _classify_customs_period_type(post["title"])
            if period_info is None:
                skipped_period_type += 1
                continue
            text = _fetch_customs_post_body(post["ntt_sn"], post["ntt_url"])
            if text is None:
                skipped_fetch_failed += 1
                continue
            article = _build_customs_article(post, text)
            if article is None:
                skipped_no_figure += 1
                consecutive_no_figure += 1
                if consecutive_no_figure >= _CUSTOMS_BACKFILL_STOP_AFTER_CONSECUTIVE_MISSES:
                    logger.info(
                        "[CUSTOMS_BACKFILL] %d consecutive target-type posts with no"
                        " extractable figure - stopping (this board's detail-page"
                        " format changed at some point in its history; confirmed"
                        " live 2026-09-23 that posts before March 2026 are stub"
                        " pages linking to a .hwpx/.pdf attachment this scraper"
                        " never opens, not a parsing bug)",
                        consecutive_no_figure,
                    )
                    return articles
                continue
            consecutive_no_figure = 0
            articles.append(article)

    logger.info(
        "[CUSTOMS_BACKFILL] pages_walked<=%d articles=%d skipped_period_type=%d"
        " skipped_no_figure=%d skipped_fetch_failed=%d",
        max_pages, len(articles), skipped_period_type, skipped_no_figure,
        skipped_fetch_failed,
    )
    return articles


# ---------------------------------------------------------------------------
# Japan market signal - IRBANK (irbank.net)
# ---------------------------------------------------------------------------
#
# No API key, no official rate-limit documentation - same conservative,
# undocumented-limit posture as _dart_rate_sleep above (a small fixed delay
# between requests, not tuned against a confirmed number).
_irbank_rate_lock = threading.Lock()
_irbank_last_call = [0.0]
_IRBANK_MIN_INTERVAL = 1.0

# IRBANK returns a plain 403 to a bare httpx client (no User-Agent) -
# confirmed live 2026-09-27. A common desktop-browser UA is sufficient.
_IRBANK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                  " (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
}

# /{ticker}/tdnet 301-redirects to /{edinet_code}/tdnet (e.g. /6857/tdnet ->
# /E01950/tdnet) - confirmed live; httpx does not follow redirects by
# default (unlike curl -L, used during manual verification, which silently
# masked this). Every httpx.get() call against irbank.net below passes
# follow_redirects=True for this reason - a plain httpx.get() here would
# raise on the 301 rather than error clearly, so this is not optional.

# Matches a "備考" (notes) block on a /{code}/tdnet page - each block groups
# disclosures under a label ("修正"=revision, "説明"=presentation materials,
# "配当"=dividend, etc.). Filtering on the BLOCK's label (rather than on
# individual title text) is more robust: confirmed live a Disco (6146)
# filing titled as a variance-vs-actual + forward-guidance combo notice
# ("...業績予想値と実績値との差異ならびに...予想...に関するお知らせ") still
# correctly lands in a 修正-labeled block despite not matching the simpler
# "...の修正に関するお知らせ" title pattern used in an earlier version of
# this fetcher.
_IRBANK_NOTE_BLOCK_RE = re.compile(
    r'<dd class="weaken" id="note_\d+"><sup>#\d+</sup> (?P<label>[^<]+)'
    r'<ul class="ic">(?P<items>.*?)</ul></dd>',
    re.S,
)
_IRBANK_NOTE_ITEM_RE = re.compile(
    r'<li><a title="(?P<title>[^"]+)" href="(?P<href>/[^"]+)">[^<]*'
    r'<span class="weaken">（(?P<date>[^）]+)）</span></a></li>',
)

# The PDF link is the one part of a /{code}/{doc_id} detail page confirmed
# reliably present across every filing tested (Advantest, Tokyo Electron,
# Disco, Shin-Etsu) - an earlier design depended on an opportunistic
# /news/{id} HTML summary instead, confirmed via live testing NOT to be
# reliably paired with every revision (see this function's own docstring
# below for the full story) - dropped in favor of this PDF path, which also
# recovers the company's own stated reason text that the /news/ page never
# had at all.
_IRBANK_PDF_LINK_RE = re.compile(r'href="(?P<pdf_url>https://f\.irbank\.net/pdf/[^"]+\.pdf)"')

# Matches the start of a reason paragraph inside a filing PDF's extracted
# text (e.g. "修正の理由", "業績予想の修正の理由", or a bare "理由" heading -
# confirmed live across Advantest's, Disco's, and Fujikura's real filings,
# the last one prefixed with a "※" bullet marker this pattern does not
# anchor against, deliberately - so it still matches) up to the
# forward-looking-statement disclaimer boilerplate that reliably follows
# it. Confirmed live 3 distinct disclaimer openers occur: "※ 将来の事象"
# (Advantest), "（注）上記の予想は" (Disco), and "※上記の予想は" (Fujikura -
# no space after "※", and no "（注）" prefix at all; an earlier version of
# this pattern only matched the first two openers and silently let a
# Fujikura reason run on into its own disclaimer text).
_IRBANK_REASON_RE = re.compile(
    r"(?:修正の理由|理由)\s*\n(?P<reason>.*)",
    re.S,
)

# The forward-looking-statement boilerplate disclaimer present on every
# real filing (in some wording) is NOT reliably introduced by any single
# marker character - confirmed live 2026-09-28 across all 154 real
# revisions in the 20-company/5-year universe, collecting every real
# variant found: some start with "※", some with "（注）", one filer uses a
# bare "注．" (full-width period, no parens), and at least one filer's
# disclaimer has NO leading marker at all, just starting mid-paragraph as
# plain prose ("当資料に記載の業績見通し等の将来に関する記述は..."). A
# marker-character-based stop rule (the earlier version of this function)
# structurally cannot catch that last case, and kept needing new marker
# variants added as more were found - confirmed live this pattern of
# "one more variant" repeated at least 3 times before this rewrite.
#
# Every real variant found DOES share one content signature regardless of
# marker: a lead-in word naming what the disclaimer covers - "業績見通し"/
# "業績予想" (earnings outlook/forecast), or (confirmed live as a further,
# narrower real variant - Tokyo Electron's own filing) a bare "上記の予想"/
# "上記予想"/"本資料"/"当資料" (a plainer "the above forecast"/"this
# document" opener with no "業績" prefix at all) - followed, within a short
# window, by "入手可能な情報"/"入手している情報" (information [that was]
# available/obtained). That second phrase is the standard Japanese forward-
# looking-statement disclaimer's own core claim ("this forecast is based on
# information available as of the announcement date"), confirmed live to
# appear in EVERY real disclaimer variant collected regardless of lead-in
# wording. A bare "contains 入手可能な情報" check with no lead-in
# requirement at all was tried and rejected: confirmed live one real
# filing (TDK) has genuine, substantive reason prose that happens to use
# the same "当社が現在入手している情報に基づきますと" phrasing mid-sentence
# while describing an actual production-volume outlook, not a disclaimer -
# requiring one of these specific lead-in words immediately before it is
# what correctly excludes that real content while still catching every
# actual disclaimer (every disclaimer variant found opens with a "the
# above forecast/report" framing word; no genuine reason sentence in the
# dataset pairs the information-availability phrase with one of these
# specific lead-ins).
_IRBANK_REASON_BOILERPLATE_RE = re.compile(
    r"(?:業績見通し|業績予想|上記の?予想|上記業績予想|本資料|当資料).{0,60}?(?:入手可能な情報|入手している情報)"
    # A second, independent real boilerplate FAMILY - confirmed live
    # 2026-09-28 as Advantest's own consistent house style across all 6 of
    # its real revisions - uses none of the "入手可能な情報" family's
    # vocabulary at all, instead opening with a dedicated heading
    # ("※ 将来の事象に係る記述に関する注意" / "将来の事象についての... 記述
    # に関する注意", note-optional leading space after ※) followed by
    # safe-harbor language about "期待、見積り" (expectations, estimates)
    # and "既知及び未知のリスク、不確実性" (known and unknown risks,
    # uncertainties) - a genuinely different disclaimer template, not a
    # wording variant of the first family. Matched as its own alternative
    # rather than folded into the first pattern's lead-in list, since its
    # anchor phrase ("将来の事象に係る記述に関する注意") is structurally
    # different (a heading, not a lead-in immediately followed by the
    # information-availability phrase).
    r"|将来の事象(?:についての|に係る記述に関する注意)"
    # A third real family - Hitachi's house style, confirmed live
    # 2026-10-01 on 3 of its revisions (140120190115459780,
    # 140120191210435490, 140120200727467442). The disclaimer opens
    # with its own bracketed heading, "＜将来の見通しに関するリスク情報＞"
    # (risk information on forward-looking statements), and is followed
    # by ~32 "・" risk bullets. Neither existing alternative matches it:
    # it carries none of the "入手可能な情報" vocabulary, and its heading
    # is 将来の見通し, not 将来の事象. Left unmatched, the whole bullet
    # list was stored as the revision's reason, burying the real one
    # sentence that precedes it.
    r"|[＜<]?将来の見通しに関するリスク情報",
    re.S,
)


def _trim_irbank_reason_at_boilerplate(raw_reason: str) -> str:
    """Cut ``raw_reason`` (everything after the "理由" heading the regex
    above anchors on) at the start of the first genuine boilerplate
    disclaimer sentence found anywhere in it - see
    _IRBANK_REASON_BOILERPLATE_RE's own comment for why this is detected
    by content rather than by a leading marker character, and why a
    "※...理由"-style reason sub-heading (e.g. a filing that states both an
    earnings reason and a separate dividend reason) is naturally never
    matched by this pattern and stays intact.

    Confirmed live 2026-09-28: real boilerplate disclaimers come in
    genuinely different, unrelated wording FAMILIES (see
    _IRBANK_REASON_BOILERPLATE_RE's own comment on the two found so far) -
    rather than keep discovering and enumerating every family's own exact
    phrasing indefinitely, this function is deliberately written to make
    adding a new family (a new alternative in that one regex) the only
    change needed, with the actual trimming logic below staying generic
    over whichever family matched.
    """
    match = _IRBANK_REASON_BOILERPLATE_RE.search(raw_reason)
    if not match:
        return raw_reason.strip()
    # The regex anchors on "業績見通し"/"業績予想"/etc - the disclaimer
    # SENTENCE (or, for the "将来の事象" family, its own heading LINE)
    # itself starts earlier on the same line (e.g. "（注）業績見通し...",
    # "注．本資料に掲載されている業績予想等...", "※ 将来の事象に係る記述に
    # 関する注意") - trimming at match.start() would keep that marker/
    # lead-in fragment dangling at the end of the real reason text instead
    # of removing the whole disclaimer. Backing up to the start of the
    # match's own line (or the very start of raw_reason if the disclaimer
    # begins on the first line) removes the complete sentence/heading,
    # marker included, regardless of which real family or marker variant
    # (or none at all) precedes it.
    line_start = raw_reason.rfind("\n", 0, match.start()) + 1
    return raw_reason[:line_start].strip()

# Row labels inside a filing PDF's extracted table always carry a bare
# "(A)" marker for the previous-forecast row and a bare "(B)" marker for
# the revised/actual row - but the prefix word varies far more than
# expected across real filers (confirmed live, in order of discovery:
# 前回発表予想(A)/今回発表予想(B); 前回発表予想(A)/今回修正予想(B);
# 前回発表予想(A)/今回実績(B) - Disco's variance-vs-actual notice;
# 前回発表予想(Ａ)/今回発表予想(Ｂ) using FULL-WIDTH Unicode Ａ/Ｂ, not ASCII
# A/B - Fujikura; 前回発表予想(A)\n(2026年２月13日発表) - Resonac/Murata,
# a trailing date-annotation line inside the SAME cell, ruling out an
# end-anchored match; 前回発表予想 (Ａ) - Tokyo Ohka Kogyo, ASCII parens
# around a full-width letter, a THIRD width combination; and finally
# 実績値（Ｂ） - SUMCO's variance notice, which doesn't start with 前 or 今
# at all, ruling out a start-anchored "must begin with 前/今" version of
# this matcher that worked for every earlier variant but not this one).
#
# Given how many prefix variants exist and keep appearing, this matches on
# nothing but the marker itself: a row is "previous" if it contains an
# (A)/（Ａ） marker and NOT a (B)/（Ｂ） one, and vice versa for "revised" -
# this correctly EXCLUDES the one row that contains both
# ("増減額(B)－(A)"/"増減額（Ｂ－Ａ）", the increment/decrement row) without
# needing to know anything about its own label text, and does not depend on
# where in the label string the marker falls. `\s*` inside the brackets
# tolerates a further variant confirmed live in Shin-Etsu Chemical's filing:
# fully character-spaced labels like "前 期 実 績 （ Ａ ）"/"当 期 予 想
# （ Ｂ ）" (Japanese typesetting convention, space between EVERY character
# including around the letter itself, not just between kanji) - an earlier
# version with no `\s*` matched every other variant seen but not this one.
_IRBANK_PDF_A_MARK_RE = re.compile(r"[\(（]\s*[AＡ]\s*[\)）]")
_IRBANK_PDF_B_MARK_RE = re.compile(r"[\(（]\s*[BＢ]\s*[\)）]")


def _irbank_pdf_row_kind(label: str) -> str | None:
    has_a = bool(_IRBANK_PDF_A_MARK_RE.search(label))
    has_b = bool(_IRBANK_PDF_B_MARK_RE.search(label))
    if has_a and not has_b:
        return "previous"
    if has_b and not has_a:
        return "revised"
    return None


# Confirmed live 2026-09-28 (Fujikura, 140120221108559389): a filing whose
# title contains "差異" (variance) genuinely combines TWO distinct table
# kinds, not one ambiguous kind - a "results vs. the old forecast" table
# ("前回発表予想(A)" vs "実績(B)" - actual results replace the (B) marker's
# usual meaning of a NEW forecast) immediately followed by a separate,
# genuine "old forecast vs. new forecast" revision table ("前回発表予想
# (A)" vs "今回発表予想(B)"). Both tables use the identical (A)/(B) marker
# convention _irbank_pdf_row_kind matches on - the only way to tell them
# apart is the (B) row's OWN label text: "実績" (actual/results) marks the
# first kind, any other (B) label ("発表予想"/"修正予想"/etc.) marks a
# genuine revision. This distinction did not matter for J1 (forecast
# revision), which already excludes any table where "previous" doesn't
# resolve to a real number (an actual-vs-forecast table's own (A) row is a
# real previous FORECAST value, not null, for a filing that only carries
# actuals) - but IS the exact signal J2 (results against forecast) needs
# to find and use this same table type deliberately, not stumble on it as
# a J1 near-miss.
_IRBANK_PDF_ACTUAL_MARK_RE = re.compile(r"実\s*績")


def _irbank_pdf_table_kind(revised_row_label: str) -> str:
    """Classify a matched previous/revised table pair by what its (B) row
    actually represents - "actual_vs_forecast" (J2's data: quarterly
    results measured against the OLD forecast) if the (B) row's own label
    contains "実績" (actual/results), else "revision" (J1's data: a
    genuine old-forecast-vs-new-forecast change) - see this module's own
    comment on _irbank_pdf_row_kind for the real filing that surfaces
    both kinds in one PDF and why the (A)/(B) marker alone can't tell them
    apart.
    """
    return "actual_vs_forecast" if _IRBANK_PDF_ACTUAL_MARK_RE.search(revised_row_label) else "revision"


def _irbank_rate_sleep() -> None:
    with _irbank_rate_lock:
        elapsed = time.monotonic() - _irbank_last_call[0]
        if elapsed < _IRBANK_MIN_INTERVAL:
            time.sleep(_IRBANK_MIN_INTERVAL - elapsed)
        _irbank_last_call[0] = time.monotonic()


def _parse_irbank_pdf_number(value: str | None) -> float | None:
    """Parse one table-cell value from an IRBANK filing PDF, e.g.
    "百万円\\n1,714,000" or "20.7%" or "―" (no data placeholder).
    Returns None for a "―" placeholder (used for not-yet-forecast line
    items, e.g. Disco's Q2 dividend table before any dividend is set) - and
    for a genuinely empty/None cell (confirmed live: a real Lasertec (6920)
    filing's table has a short row that pdfplumber returns with a None
    cell, not an empty string, which crashed an earlier version of this
    function that assumed every cell was always a string).

    Some filers (confirmed live 2026-09-28: TDK) issue a RANGE forecast
    ("180,000〜225,000") rather than a single point figure - the cell's
    raw text renders across THREE lines: unit ("百万円"), the low bound
    ("180,000"), then a full-width wave dash (U+FF5E) immediately
    followed by the high bound ("～225,000") on its own final line, no
    space. The old code took only the single last line
    (split("\n")[-1]), which for a range cell is just "～225,000" - the
    low bound one line up was discarded before it was ever seen, so this
    silently returned None for the whole cell rather than a number: a
    real forecast value was being read as missing. When the last line
    starts with the wave dash, the low bound is recovered from the
    SECOND-to-last line instead, and the midpoint of the two is returned -
    the standard, defensible single-number reading of a min-max range
    (the same way a trader reads "180,000〜225,000" as "about 202,500
    expected"), not an arbitrary pick of one bound over the other.

    A negative figure (an operating LOSS forecast, not just a smaller
    profit) is marked with a leading "△" (triangle), the standard
    Japanese accounting convention for a negative number (the equivalent
    of parentheses in Western accounting) - confirmed live 2026-09-28:
    Resonac Holdings' real filings forecast an operating loss this way
    ("△20,000" = -20,000 million yen). The old code had no handling for
    this character at all - float("△20,000") raises ValueError, so a
    real loss figure was silently returned as None (missing), not
    misread as a wrong number, but a genuine loss forecast disappearing
    entirely is exactly the kind of habit-computation-corrupting gap this
    whole audit exists to catch. Stripped and negated before the normal
    float conversion, checked on both a plain value and each bound of a
    range (a range can itself have a negative bound, e.g. a
    loss-narrowing-to-a-smaller-loss forecast).
    """
    if value is None:
        return None
    lines = value.strip().split("\n")
    last_line = lines[-1].strip()

    # A range's low/high bounds can render two different ways - confirmed
    # live 2026-09-28 against two different real filers: TDK splits them
    # across lines ("180,000" then, on its own final line, "～225,000"),
    # while Kioxia renders both bounds on the SAME line ("4,316～4,536",
    # no line break at all). Checking for "～" anywhere in the last line
    # (not just as its leading character) covers both shapes with one
    # code path - when the wave dash sits at the very start of the last
    # line, the low bound is on the line above instead of being part of
    # this line at all.
    if "～" in last_line:
        if last_line.startswith("～") and len(lines) >= 2:
            low_str = lines[-2].strip()
            high_str = last_line[1:].strip()
        else:
            low_str, high_str = last_line.split("～", 1)
        low = _parse_irbank_pdf_signed_number(low_str)
        high = _parse_irbank_pdf_signed_number(high_str)
        if low is None or high is None:
            return None
        return (low + high) / 2

    value = last_line
    if not value or value in ("―", "-"):
        return None
    # Some real filers (confirmed live: Renesas Electronics 6723's own
    # Non-GAAP margin columns, e.g. "58.2％") render the percent sign as
    # the full-width Japanese character U+FF05, not the ASCII "%"
    # (U+0025) - rstrip("%") silently leaves it in place, which then made
    # float() raise and the whole cell return None, discarding a real,
    # present percentage figure rather than a genuinely absent one. Both
    # forms are stripped, not just one - a real mix of both across
    # different filers/columns in the same document has not been ruled
    # out, and stripping the ASCII form when it's absent is a no-op.
    value = value.rstrip("%％")
    return _parse_irbank_pdf_signed_number(value)


def _parse_irbank_pdf_signed_number(value: str) -> float | None:
    """Parse a single (non-range) numeric token, honoring a leading "△"
    (Japanese accounting negative marker) - see _parse_irbank_pdf_number's
    own docstring for why this exists. Shared by both the plain-value and
    range-bound parsing paths in that function.
    """
    value = value.strip()
    is_negative = value.startswith("△")
    if is_negative:
        value = value[1:].strip()
    value = value.replace(",", "")
    try:
        number = float(value)
    except ValueError:
        return None
    return -number if is_negative else number


# pdfplumber's default table-detection settings are not consistently
# reliable across real filings - confirmed live: the default (equivalent to
# "lines") finds Advantest's and Disco's tables correctly but finds ZERO
# tables in Fujikura's filing despite the same table genuinely being
# present and readable via extract_text(); explicitly forcing
# "lines_strict" finds Fujikura's 2 tables correctly but then finds ZERO in
# Advantest's/Disco's. There is no single strategy that works for all three
# - _extract_irbank_pdf_tables tries each in turn per page and keeps
# whichever one actually yields a recognizable previous/revised-forecast
# table, rather than assuming one upfront.
_IRBANK_PDF_TABLE_STRATEGIES = ["lines", "lines_strict", "text"]


def _irbank_pdf_header_row(raw_table: list[list[str | None]], previous_row_index: int) -> list[str | None] | None:
    """Find the real header row for a previous/revised pair at
    ``previous_row_index`` within ``raw_table``.

    Confirmed live 2026-09-28 against a real Fujikura filing
    (140120250806532652): pdfplumber's own extract_tables() can merge
    MULTIPLE distinct forecast blocks (e.g. a Q2/half-year block and a
    separate full-year block) into ONE raw_table object when they sit on
    the same page with no detected separator - always using raw_table[0]
    as the header (the previous version's assumption) is only correct for
    the FIRST such block; every later block's real header (line-item
    names like 売上高/営業利益) sits a few rows above ITS OWN previous row,
    not at the very top of the merged table. Walking backward from the
    previous row to the nearest row with 2+ non-empty cells finds each
    block's own header correctly regardless of how many blocks got merged
    into one raw_table - confirmed against the real Fujikura filing this
    correctly recovers both blocks' headers (its own row 5 for the Q2
    block, row 14 for the full-year block) instead of picking row 0's
    garbage cells for both.
    """
    for i in range(previous_row_index - 1, -1, -1):
        candidate = raw_table[i]
        if sum(1 for cell in candidate if cell and cell.strip()) >= 2:
            return candidate
    return None


def _extract_tables_matching(raw_tables: list[list[list[str | None]]]) -> list[dict[str, Any]]:
    """From a list of raw pdfplumber tables, keep only the ones that look
    like a forecast-revision table (has both a previous- and a
    revised-forecast row) and reshape each into {line_item: {previous,
    revised}}.

    A single raw_table can contain more than one previous/revised pair
    (see _irbank_pdf_header_row's own docstring for the confirmed-live
    Fujikura case where two forecast blocks merge into one pdfplumber
    table) - every previous/revised pair found is processed with its own
    nearest header, not just the first one, so a merged multi-block table
    yields multiple result dicts instead of silently only ever reading the
    first block.
    """
    tables: list[dict[str, Any]] = []
    for raw_table in raw_tables:
        if len(raw_table) < 2:
            continue

        for idx, row in enumerate(raw_table):
            # The previous/revised marker usually sits in column 0, but
            # confirmed live 2026-09-28 NOT always: some real filings
            # (Fujikura consistently; one real Disco filing too - 5 of 154
            # real revisions across the 20-company universe) render an
            # empty leading cell, pushing every label one column to the
            # right (e.g. ['', '前回発表予想(Ａ)', '481,000', ...] instead
            # of ['前回発表予想(Ａ)', '481,000', ...]). Scanning the first
            # 2 cells for the marker (rather than assuming column 0)
            # covers both real layouts seen so far without needing a
            # per-filer special case; marker_col is then reused for the
            # revised-row lookup and the header/data column alignment
            # below so the whole row stays internally consistent.
            marker_col = next(
                (c for c in (0, 1) if c < len(row) and row[c] and _irbank_pdf_row_kind(row[c]) == "previous"),
                None,
            )
            if marker_col is None:
                continue
            previous_row = row

            # Some real filings (confirmed live 2026-09-28: 2 of Resonac
            # Holdings' 15 real revisions) render the previous-forecast
            # row's own VALUES wrapped onto a second physical row that
            # pdfplumber splits off as its own table row - the marker row
            # itself then has only a bare unit string ("百万円"/"円 銭",
            # no digits) in place of each real figure, e.g.:
            #   ['前回発表予想(A)\n...', '百万円\n1,270,000', '百万円', '百万円', ...]
            #   [None, None, '△20,000', '△26,000', '△37,000', '△204.27']
            # The continuation row's own leading cells (through marker_col)
            # are None/empty, distinguishing it from a genuine new labelled
            # row (revised/increase-decrease/etc.), which always has real
            # text in its own marker_col. Where the marker row's cell has
            # no digits at all, the continuation row's same-index cell
            # (if it has one) is used instead - this only ever fills a gap,
            # never overwrites a real value the marker row already had.
            if idx + 1 < len(raw_table):
                next_row = raw_table[idx + 1]
                looks_like_continuation = (
                    len(next_row) > marker_col
                    and all(not (c and c.strip()) for c in next_row[: marker_col + 1])
                    and any(c and re.search(r"\d", c) for c in next_row[marker_col + 1:])
                )
                if looks_like_continuation:
                    merged = list(previous_row)
                    for i in range(marker_col + 1, min(len(merged), len(next_row))):
                        cell = merged[i]
                        if (not cell or not re.search(r"\d", cell)) and next_row[i] and re.search(r"\d", next_row[i]):
                            merged[i] = next_row[i]
                    previous_row = merged

            revised_row = next(
                (
                    r for r in raw_table[idx + 1:]
                    if marker_col < len(r) and r[marker_col] and _irbank_pdf_row_kind(r[marker_col]) == "revised"
                ),
                None,
            )
            if not revised_row:
                continue
            header_row = _irbank_pdf_header_row(raw_table, idx)
            if not header_row:
                continue
            header = header_row[marker_col + 1:]

            # Some real filers (confirmed live: Renesas Electronics 6723,
            # every one of its 20 real filings checked) never state a
            # previous FORECAST at all - the (A) row is a literal dash
            # ("－") in every column, every time, not a parsing failure.
            # The only other real figure in these filings is a separate
            # "reference" row - labelled with both "前期" (previous
            # period) and "実績" (actual results), e.g. "（ご参考）前期
            # （2025年12月期）実績" - giving the SAME period last YEAR's
            # real actual result, not this year's earlier forecast. This
            # is a genuinely different concept from (A) (a real result vs.
            # a stated forecast), so it is captured under its own key
            # (reference_prior_year_actual), never substituted for
            # "previous" - a consumer that wants "did the forecast change"
            # still correctly sees previous=None for a company that never
            # discloses one, rather than being fed a same-value-different-
            # meaning number silently mislabeled as a forecast.
            reference_row = next(
                (
                    r for r in raw_table
                    if marker_col < len(r) and r[marker_col]
                    and "前期" in r[marker_col] and "実績" in r[marker_col]
                ),
                None,
            )

            line_items = {}
            for i, label in enumerate(header, start=marker_col + 1):
                if not label:
                    continue
                # Some filers' PDFs render a header label with a space
                # injected between every character (confirmed live
                # 2026-09-28: Murata Manufacturing's own tables
                # consistently extract "営 業 利 益" instead of "営業利益"
                # for the exact same line item other filers render
                # without spacing - a PDF kerning/layout artifact, not a
                # different label) - stripping ALL whitespace (not just
                # "\n") merges these back to the same key a normally-
                # rendered filing already uses. Confirmed safe via the
                # real label set collected across all 20 companies'
                # 5-year history: every case two labels collapse to the
                # same whitespace-stripped form is a genuine duplicate
                # (e.g. "税 引 前 利 益"/"税引前利益"), never two distinct
                # concepts merged incorrectly.
                label = re.sub(r"\s+", "", label)
                line_items[label] = {
                    "previous": _parse_irbank_pdf_number(previous_row[i]) if i < len(previous_row) else None,
                    "revised": _parse_irbank_pdf_number(revised_row[i]) if i < len(revised_row) else None,
                    "reference_prior_year_actual": (
                        _parse_irbank_pdf_number(reference_row[i])
                        if reference_row and i < len(reference_row) else None
                    ),
                }
            if line_items:
                # "_kind" is a synthetic key, not a real Japanese line-item
                # label (every real label is Japanese text) - safe to mix
                # into the same dict without risking a collision, and lets
                # every existing consumer that only ever reads real
                # line-item keys keep working unchanged (json.dumps/reads
                # untouched, dict shape untouched) while new J2 code can
                # check table["_kind"] to find the actual-vs-forecast
                # tables it needs. See _irbank_pdf_table_kind's own
                # docstring for what the two possible values mean.
                line_items["_kind"] = _irbank_pdf_table_kind(revised_row[marker_col] or "")
                tables.append(line_items)
    return tables


def _irbank_pdf_table_looks_headerless(raw_table: list[list[str | None]]) -> bool:
    """True if raw_table's very first row is already a previous/revised
    marker row (no header row above it) - the shape of a table whose
    header was left behind on the PREVIOUS page (see
    _extract_irbank_pdf_tables's own docstring on the confirmed-live TDK
    cross-page split).
    """
    if not raw_table:
        return False
    first_row = raw_table[0]
    return any(
        first_row[c] and _irbank_pdf_row_kind(first_row[c]) == "previous"
        for c in (0, 1) if c < len(first_row)
    )


def _irbank_pdf_table_looks_headeronly(raw_table: list[list[str | None]]) -> bool:
    """True if raw_table has no previous/revised marker row anywhere but
    its own final row has 2+ non-empty cells (looks like a real header,
    e.g. 売上高/営業利益/...) - the shape of a table whose data rows
    haven't arrived yet because they spilled onto the NEXT page (see
    _extract_irbank_pdf_tables's own docstring).
    """
    if not raw_table:
        return False
    has_marker = any(
        row[c] and _irbank_pdf_row_kind(row[c]) in ("previous", "revised")
        for row in raw_table for c in (0, 1) if c < len(row)
    )
    if has_marker:
        return False
    last_row = raw_table[-1]
    return sum(1 for cell in last_row if cell and cell.strip()) >= 2


def _extract_irbank_pdf_tables(pdf_bytes: bytes) -> list[dict[str, Any]]:
    """Extract every forecast-revision table from a filing PDF's pages.

    A single PDF can contain multiple such tables - confirmed live for
    Disco's variance-vs-actual notice, which carried 4 (Q1 consolidated
    variance, Q1 standalone variance, Q2 consolidated forecast, Q2
    standalone forecast) plus a separate dividend table (skipped - no
    previous/revised row labels match, so it is naturally excluded rather
    than needing an explicit skip rule).

    A table can also split ACROSS a page boundary - confirmed live
    2026-09-28 against a real TDK filing (140120220418523467): the
    header row (売上高/営業利益/...) lands as the very last row extracted
    on page 0, with its own previous/revised data rows appearing as the
    FIRST rows of a separate raw_table on page 1 - pdfplumber's own
    per-page extraction has no way to know these are one logical table,
    so processing each page independently (the old code) silently
    produced a header-only table with no data on page 0 and a markerless,
    header-less table on page 1, neither of which _extract_tables_matching
    could turn into a result. Detected here (not inside
    _extract_tables_matching, which has no cross-page visibility) by
    checking whether the PREVIOUS page's last raw table looks header-only
    (_irbank_pdf_table_looks_headeronly) and the CURRENT page's first raw
    table looks headerless (_irbank_pdf_table_looks_headerless) - when
    both hold, the two are spliced into one combined raw_table before
    matching, exactly as if pdfplumber had extracted them as a single
    table in the first place.
    """
    import pdfplumber
    from io import BytesIO

    tables: list[dict[str, Any]] = []
    pending_header_only: list[list[str | None]] | None = None
    with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            for strategy in _IRBANK_PDF_TABLE_STRATEGIES:
                settings = {"vertical_strategy": strategy, "horizontal_strategy": strategy}
                try:
                    raw_tables = page.extract_tables(table_settings=settings)
                except Exception:
                    continue

                if raw_tables and pending_header_only is not None and _irbank_pdf_table_looks_headerless(raw_tables[0]):
                    raw_tables = [pending_header_only + raw_tables[0]] + raw_tables[1:]
                pending_header_only = None

                matched = _extract_tables_matching(raw_tables)
                if matched:
                    tables.extend(matched)
                if raw_tables and _irbank_pdf_table_looks_headeronly(raw_tables[-1]):
                    pending_header_only = raw_tables[-1]
                if matched or raw_tables:
                    break
    return tables


# IRBANK occasionally renders a stale date-folder prefix in a detail page's
# PDF link (confirmed live: Screen Holdings 7735's 2021/10/26 filing showed
# "/pdf/20211026/{doc_id}.pdf" - a 403 AccessDenied S3-style error, not a
# PDF - while re-fetching the SAME detail page moments later, and every
# time since, correctly shows "/pdf/20211027/{doc_id}.pdf", a real PDF; the
# filing's own title text confirms 2021/10/27 as the true TDnet submission
# time, one day after the doc_id's embedded drafting date). This self-
# corrects on IRBANK's side - retrying the SAME (wrong) pdf_url would just
# repeat the same 403, so the retry re-fetches and re-parses the detail
# page each attempt, unlike _get_with_ssl_retry above (which retries one
# fixed URL for a different, transient-connection failure mode).
_IRBANK_PDF_RETRY_ATTEMPTS = 2
_IRBANK_PDF_RETRY_BACKOFF_SECONDS = 3.0

# Some IRBANK-hosted filing PDFs embed Type3 fonts with no ToUnicode CMap -
# confirmed live for Towa (6315) and Tokyo Ohka Kogyo (4186), 3 of 154 real
# filings (~2%) in a 20-company/5-year sample: pdfplumber, pdfminer,
# poppler's pdftotext, and PyMuPDF all extract identical (cid:N) glyph-code
# garbage, not readable text, because Type3 glyphs are raw drawing
# procedures with no character-identity table to recover from at all (not
# the more common "subsetted font missing its map" case, which sometimes
# has a recoverable encoding). Recurring across 2 different filers (not a
# one-off) is why an OCR fallback is worth the dependency cost here -
# tesseract-ocr + tesseract-ocr-jpn are this Dockerfile's first apt-get
# system dependency (everything else is pip-only).
_CID_GARBAGE_RE = re.compile(r"\(cid:\d+\)")


def _looks_like_cid_garbage(text: str) -> bool:
    if not text or not text.strip():
        return True
    cid_chars = sum(len(m.group()) for m in _CID_GARBAGE_RE.finditer(text))
    real_chars = len(re.sub(r"\s", "", text))
    if real_chars == 0:
        return True
    return cid_chars / real_chars > 0.3


# Confirmed live via manual testing against Towa's real CID-garbage PDF:
# dpi=400 + "--psm 6" (Tesseract's "assume a single uniform block of text"
# page-segmentation mode) produces materially cleaner output than the
# default psm or a lower DPI - other combinations tried (dpi=300 psm=4,
# dpi=400 psm=4) left visible garbled fragments in the reason paragraph
# that this combination does not. OCR quality is still noticeably rougher
# than the PDF text layer (occasional misread characters, stray line-break
# artifacts) - "readable and substantially correct," not "verbatim." Table
# numbers are NOT attempted here at all: the same manual test found numeric
# OCR meaningfully less reliable than prose OCR, and a silently-wrong
# figure feeding a classification rule downstream is worse than a missing
# one - so this only ever returns reason text, tables stay unset for any
# filing that reaches this fallback.
_IRBANK_OCR_DPI = 400
_IRBANK_OCR_CONFIG = "--psm 6"


def _ocr_extract_irbank_reason(pdf_bytes: bytes) -> str | None:
    """OCR fallback for a Type3-font PDF with no extractable text layer -
    renders each page to an image and runs Tesseract (Japanese) over it,
    then applies the same reason-paragraph pattern used for normal
    PDF-text-layer extraction. Returns None if OCR itself fails or no
    reason paragraph is found in the OCR'd text.
    """
    import pymupdf
    import pytesseract
    from PIL import Image
    from io import BytesIO

    try:
        doc = pymupdf.open(stream=pdf_bytes, filetype="pdf")
        page_texts = []
        for page in doc:
            pix = page.get_pixmap(dpi=_IRBANK_OCR_DPI)
            img = Image.open(BytesIO(pix.tobytes("png")))
            page_texts.append(pytesseract.image_to_string(img, lang="jpn", config=_IRBANK_OCR_CONFIG))
        ocr_text = "\n".join(page_texts)
    except Exception as exc:
        logger.warning("[IRBANK] OCR fallback failed: %s", exc)
        return None

    reason_match = _IRBANK_REASON_RE.search(ocr_text)
    return _trim_irbank_reason_at_boilerplate(reason_match.group("reason")) if reason_match else None


def _fetch_irbank_filing_pdf(doc_href: str) -> dict[str, Any] | None:
    """Fetch one irbank.net/{code}/{doc_id} detail page, find its PDF link,
    download the PDF, and extract forecast tables plus the company's own
    stated reason text.

    Returns None on any failure (detail page unreachable, no PDF link found,
    PDF unparseable) - callers treat a missing result as "figures not yet
    available", not an error, same fail-open convention _fetch_dart_document_
    body uses.
    """
    pdf_bytes: bytes | None = None
    pdf_url: str | None = None
    last_exc: Exception | None = None

    for attempt in range(1, _IRBANK_PDF_RETRY_ATTEMPTS + 1):
        _irbank_rate_sleep()
        try:
            resp = httpx.get(
                f"https://irbank.net{doc_href}", headers=_IRBANK_HEADERS, timeout=30.0,
                follow_redirects=True,
            )
            resp.raise_for_status()
            detail_html = resp.text
        except Exception as exc:
            last_exc = exc
            continue

        pdf_match = _IRBANK_PDF_LINK_RE.search(detail_html)
        if not pdf_match:
            last_exc = ValueError("no PDF link found on detail page")
            continue

        _irbank_rate_sleep()
        try:
            pdf_resp = httpx.get(
                pdf_match.group("pdf_url"), headers=_IRBANK_HEADERS, timeout=30.0,
                follow_redirects=True,
            )
            pdf_resp.raise_for_status()
            pdf_bytes = pdf_resp.content
            pdf_url = pdf_match.group("pdf_url")
            break
        except Exception as exc:
            last_exc = exc
            logger.warning(
                "[IRBANK] PDF fetch failed (attempt %d/%d) href=%s url=%s error=%s"
                " - re-fetching detail page for a possibly-corrected link",
                attempt, _IRBANK_PDF_RETRY_ATTEMPTS, doc_href, pdf_match.group("pdf_url"), exc,
            )
            if attempt < _IRBANK_PDF_RETRY_ATTEMPTS:
                time.sleep(_IRBANK_PDF_RETRY_BACKOFF_SECONDS)

    if pdf_bytes is None:
        logger.warning("[IRBANK] PDF fetch exhausted retries href=%s error=%s", doc_href, last_exc)
        return None

    try:
        import pdfplumber
        from io import BytesIO

        with pdfplumber.open(BytesIO(pdf_bytes)) as pdf:
            full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        tables = _extract_irbank_pdf_tables(pdf_bytes)
    except Exception as exc:
        logger.warning("[IRBANK] PDF parse failed url=%s error=%s", pdf_url, exc)
        return None

    if _looks_like_cid_garbage(full_text):
        logger.warning(
            "[IRBANK] PDF text looks like CID-only garbage (likely a Type3"
            " font with no ToUnicode map) - falling back to OCR for reason"
            " text; tables stay unset (numeric OCR is unreliable): url=%s",
            pdf_url,
        )
        reason = _ocr_extract_irbank_reason(pdf_bytes)
        return {"tables": [], "reason": reason, "pdf_url": pdf_url}

    reason_match = _IRBANK_REASON_RE.search(full_text)
    reason = _trim_irbank_reason_at_boilerplate(reason_match.group("reason")) if reason_match else None

    return {"tables": tables, "reason": reason, "pdf_url": pdf_url}


# Once this many consecutive already-stored revisions are seen while
# walking a company's note_N history newest-first, stop walking further
# back rather than keep checking (cheaply) or fetching (expensively) items
# almost certain to already be stored too. A small buffer (not 1) tolerates
# a single out-of-order edge case without losing much of the speed benefit -
# see _fetch_one_irbank_financials's docstring for why this is safe to rely
# on at all (confirmed live: both note_N block order and each block's own
# item order are consistently newest-first, no interleaving observed across
# 19 real revisions for Advantest).
_IRBANK_STOP_AFTER_CONSECUTIVE_SEEN = 3


def _fetch_one_irbank_financials(code: str, company: str, days_back: int) -> list[dict]:
    """Fetch forecast-revision notices for one company from its IRBANK
    /{code}/tdnet disclosure-history page.

    Confirmed live 2026-09-27 against Advantest (6857) and Disco (6146): the
    page holds the company's full disclosure history (13+ years deep for
    Advantest), covering far more than revisions (results, buybacks,
    dividends, administrative notices) - filtering to "備考" blocks labelled
    "修正" isolates forecast-revision notices only; every other disclosure
    type on this same page is a distinct source_type's job (see
    kabutan_tdnet_mirror, irbank_buyback, etc.), not fetched again from here.

    Only fetches (and PDF-parses) revisions not already stored - the first
    run for a company naturally becomes a full backfill (nothing stored yet,
    so every revision is fetched), and every run after that is naturally
    incremental (get_already_stored_urls skips the PDF fetch entirely for
    anything already in the DB, and _IRBANK_STOP_AFTER_CONSECUTIVE_SEEN
    stops walking the list once enough already-stored items are seen in a
    row) - one code path serves both purposes, no separate backfill mode or
    CLI flag needed. This matters in practice: a company's full history can
    be 19-50+ revisions, each requiring its own PDF download and parse -
    re-doing that every day for years of unchanged history would make daily
    runs slow for no benefit, given every filing here is immutable once
    published.
    """
    _irbank_rate_sleep()
    try:
        resp = httpx.get(
            f"https://irbank.net/{code}/tdnet", headers=_IRBANK_HEADERS, timeout=30.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        text = resp.text
    except Exception as exc:
        logger.warning("[IRBANK] tdnet fetch failed code=%s error=%s", code, exc)
        return []

    revision_blocks = [
        b for b in _IRBANK_NOTE_BLOCK_RE.finditer(text) if b.group("label").strip() == "修正"
    ]
    # Flatten to one newest-first list of (doc_href, doc_id, url, title,
    # date, pub_date) before doing anything expensive - candidate_urls below
    # needs the full list up front for one batched get_already_stored_urls()
    # call rather than a query per item, and pub_date is parsed here (rather
    # than only for items that survive filtering) so the days_back cutoff
    # below can use it as an early-stop condition too.
    candidates = []
    for block in revision_blocks:
        for item in _IRBANK_NOTE_ITEM_RE.finditer(block.group("items")):
            # date group e.g. "2026年7月29日 15:30" - parse the date portion
            # only (time is kept in the title/metadata, not needed for
            # _pub_date's day-level sort).
            date_match = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", item.group("date"))
            pub_date = (
                datetime(int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)), tzinfo=timezone.utc)
                if date_match else None
            )
            doc_id = item.group("href").rsplit("/", 1)[-1]
            candidates.append({
                "doc_href": item.group("href"),
                "doc_id": doc_id,
                "url": f"irbank-financials://{code}/{doc_id}",
                "title": item.group("title"),
                "date": item.group("date"),
                "pub_date": pub_date,
            })
    if not candidates:
        return []

    # days_back is a client-side cutoff (not a query param - IRBANK's
    # /{code}/tdnet page has no date-range filter, confirmed live it always
    # returns the full history) - combined with the newest-first ordering,
    # once an item older than the cutoff is reached every remaining item is
    # too, so this doubles as an early-stop, same rationale as the
    # already-stored check below.
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    stored_urls = get_already_stored_urls([c["url"] for c in candidates])

    results: list[dict] = []
    consecutive_seen = 0
    for c in candidates:
        if c["pub_date"] and c["pub_date"] < cutoff:
            break
        if c["url"] in stored_urls:
            consecutive_seen += 1
            if consecutive_seen >= _IRBANK_STOP_AFTER_CONSECUTIVE_SEEN:
                break
            continue
        consecutive_seen = 0

        detail = _fetch_irbank_filing_pdf(c["doc_href"])
        pub_date = c["pub_date"]

        results.append({
            # IRBANK genuinely reuses generic title text across many
            # distinct filings for the same company - confirmed live
            # 2026-09-28: Disco alone has 5 title strings (e.g. "業績予想
            # のお知らせ") repeated across 49 of its 50 real revisions,
            # each a real, separate filing with its own date and doc_id.
            # articles' own uq_articles_run_title unique index is
            # (run_id, lower(title)) - without the date folded in here,
            # a single run inserting more than one revision sharing a
            # title silently drops every collision after the first via
            # ON CONFLICT DO NOTHING (confirmed live: a 20-company/5-year
            # backfill inserted only 78 of 154 real fetched revisions
            # before this fix). c["date"]'s own minute-precision text is
            # unique per filing in practice (two distinct TDnet filings
            # for the same company at the exact same minute has never
            # been observed), so prefixing it makes every title distinct
            # without changing what a reader sees the filing as.
            "title": f"{company} ({code}) [{c['date']}]: {c['title']}",
            "url": c["url"],
            "published": c["date"],
            "source": "IRBANK",
            # Not duplicated into "summary" - metadata.reason below is the
            # only field any real consumer (classify_forecast_revision)
            # reads; nothing in either service reads this article's own
            # "summary" column for jp_forecast, confirmed by a full grep
            # of both services before this was changed.
            "summary": None,
            "body": None,
            "_pub_date": pub_date,
            "metadata": {
                "code": code,
                "company": company,
                "figures": detail["tables"] if detail else None,
                "reason": detail["reason"] if detail else None,
                "pdf_url": detail["pdf_url"] if detail else None,
                    "source_category": "jp_forecast",
                    "doc_href": c["doc_href"],
                },
            })
    return results


def _fetch_irbank_financials(sources: list[dict], days_back: int) -> list[dict]:
    """Fetch forecast-revision notices for all companies across all
    irbank_financials sources.
    """
    companies = _japan_companies(sources)
    if not companies:
        return []

    articles: list[dict] = []
    for c in companies:
        articles.extend(_fetch_one_irbank_financials(c["code"], c["company"], days_back))
    return articles


# ---------------------------------------------------------------------------
# Japan market signal - IRBANK buyback status (irbank.net/{code}/buyback)
# ---------------------------------------------------------------------------
#
# Confirmed live 2026-09-28 against Advantest (6857): /{code}/buyback
# 301-redirects to /{edinet_code}/buyback (same pattern as /tdnet above -
# follow_redirects=True required). The page groups entries by year under
# an <h2 id="cYYYY"> heading, each year one board-resolution PROGRAM (its
# own share/yen upper limit and acquisition window), with one <dl class=
# "gdl"> holding one dated entry per monthly status report: date, this
# month's share-count CHANGE, cumulative yen amount, and cumulative %
# of the program's authorized upper limit. This is genuinely clean,
# already-structured per-report data - unlike EDINET's own buyback-status
# filing (docTypeCode 220/230, see edinet_buyback_status), which has NO
# discrete numeric field at all for the same information (confirmed live
# it is only a free-text XBRL block) - this source is the intended PRIMARY
# source for buyback figures; EDINET's is same-day corroboration only.
_IRBANK_BUYBACK_PROGRAM_RE = re.compile(
    r'<h2 id="c(?P<year>\d{4})">\d{4}年</h2>'
    r'<h3>(?P<resolution>[^<]+)<br><small>(?P<limits>[^<]+)</small><br>'
    r'<small>取得期間：(?P<period>[^<]+)</small></h3>'
    r'<dl class="gdl">(?P<entries>.*?)</dl>',
    re.S,
)
_IRBANK_BUYBACK_ENTRY_RE = re.compile(
    r'<dt><a title="[^"]*" href="(?P<href>/[^"]+)">(?P<date>\d{4}年\d{1,2}月\d{1,2}日)</a>'
    r'<br>&thinsp;<span class="co_red">(?P<change>[^<]*)</span></dt>'
    r'<dd><span class="ratio"[^>]*></span><span class="text">(?P<cumulative>[^<]*)<br>'
    r'<span class="weaken">\((?P<pct>[^)]*)\)</span></span></dd>',
)


def _parse_japanese_date(date_str: str) -> datetime | None:
    m = re.match(r"(\d{4})年(\d{1,2})月(\d{1,2})日", date_str)
    if not m:
        return None
    return datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)


def _fetch_one_irbank_buyback(code: str, company: str) -> list[dict]:
    """Fetch a company's full buyback-program history from its IRBANK
    /{code}/buyback page - confirmed live this page holds every board-
    resolution program the company has run (Advantest: 4 programs back to
    2020), each with its own monthly status-report entries.

    Not days_back-scoped, unlike irbank_financials - confirmed live this
    page is far smaller (a handful of programs, a few entries each) than a
    company's full disclosure history, so walking the whole thing and
    relying on dedup (rather than a client-side date cutoff) is simpler and
    still cheap. No PDF fetch needed either - every figure needed is
    already in this page's own HTML, unlike irbank_financials.
    """
    _irbank_rate_sleep()
    try:
        resp = httpx.get(
            f"https://irbank.net/{code}/buyback", headers=_IRBANK_HEADERS, timeout=30.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        text = resp.text
    except Exception as exc:
        logger.warning("[IRBANK] buyback page fetch failed code=%s error=%s", code, exc)
        return []

    results: list[dict] = []
    for program in _IRBANK_BUYBACK_PROGRAM_RE.finditer(text):
        for entry in _IRBANK_BUYBACK_ENTRY_RE.finditer(program.group("entries")):
            href = entry.group("href")
            # href e.g. "/E01950/offer?f=S100X9OX" - the query value is the
            # underlying EDINET docID, globally unique, used as dedup key.
            doc_id_match = re.search(r"f=([^&]+)", href)
            doc_id = doc_id_match.group(1) if doc_id_match else href

            results.append({
                # Two distinct buyback-status entries for the same company
                # can share the identical date+change text (confirmed live
                # 2026-09-28: Renesas Electronics has 2 real, separate
                # entries both dated 2020年7月14日 with "+0株" - two
                # different programs both reporting zero activity that
                # day) - same uq_articles_run_title collision risk as
                # every other Japan fetcher's generic-title case (see
                # _fetch_one_irbank_financials's comment on this bug
                # class). doc_id is already this row's own unique dedup
                # key (used in its url), so appending it here guarantees a
                # distinct title too.
                "title": f"{company} ({code}) buyback status [{doc_id}]: {entry.group('date')}, {entry.group('change')}",
                "url": f"irbank-buyback://{code}/{doc_id}",
                "published": entry.group("date"),
                "source": "IRBANK",
                "summary": None,
                "body": None,
                "_pub_date": _parse_japanese_date(entry.group("date")),
                "metadata": {
                    "code": code,
                    "company": company,
                    "program_year": program.group("year"),
                    "program_resolution": program.group("resolution"),
                    "program_limits": program.group("limits"),
                    "program_period": program.group("period"),
                    "share_change": entry.group("change"),
                    "cumulative_amount": entry.group("cumulative"),
                    "cumulative_pct_of_limit": entry.group("pct"),
                    "source_category": "jp_buyback",
                },
            })
    return results


def _fetch_irbank_buyback(sources: list[dict]) -> list[dict]:
    """Fetch buyback-program history for all companies across all
    irbank_buyback sources.
    """
    companies = _japan_companies(sources)
    if not companies:
        return []

    articles: list[dict] = []
    for c in companies:
        articles.extend(_fetch_one_irbank_buyback(c["code"], c["company"]))
    return articles


# ---------------------------------------------------------------------------
# Japan market signal - IRBANK company reference facts (total assets,
# shares outstanding)
# ---------------------------------------------------------------------------
#
# Two slow-changing per-company REFERENCE facts - not news, not events -
# needed as denominators by signal-detection-agent's J5/J6 classifiers
# (see japan_signal_classifier.py):
#   - Total assets: J5's "investment >= 10% of total assets" rule.
#   - Shares outstanding: J6's "buyback >= 5% of shares outstanding" rule.
# Folded into ONE source_type/fetcher rather than two, since both are
# fetched from the same IRBANK domain in the same per-company loop, both
# change at the same slow (quarterly-at-most) cadence, and both exist
# purely to serve the same pair of downstream classifiers - splitting them
# would mean two near-identical fetchers and two redundant DB rows per
# company per poll for no benefit (unlike irbank_financials vs.
# irbank_buyback, which really are two independently-scoped feeds).
#
# Total assets: confirmed live 2026-09-29 on IRBANK's own /{code}/bs
# ("financial condition") page - reachable via the same
# follow_redirects=True /{code}/... pattern irbank_financials/
# irbank_buyback already use - a clean per-fiscal-period history table
# (JPY millions) with 総資産 (total assets) as its own labelled column,
# one row per fiscal period back many years (confirmed live: 23 real
# quarterly/annual rows for Disco, back to 2007/03). Only the single
# newest row is kept per run (no habit computation needed here, unlike
# irbank_financials - J5 only ever needs the LATEST known figure).
#
# Shares outstanding: NOT published directly anywhere on IRBANK's pages
# checked (confirmed live: no 発行済株式/上場株式数 field on the main
# /{code} page or the /dividend page) - derived instead as
# market_cap / previous_close_price, both of which ARE present on the
# main /{code} page (confirmed live for Advantest: 時価総額 24兆8880億 /
# 前日終値 33,060 -> ~752M shares). Confirmed live three distinct real
# market-cap label formats exist depending on company size (trillion+oku,
# e.g. "24兆8880億"; oku+man only for smaller caps, e.g. "1660億2262万";
# oku alone with no man remainder, e.g. "1兆681億") - _parse_irbank_
# market_cap_yen handles all three.
#
# Dedup: synthetic url "irbank-company-reference://{code}/{period}" -
# period is the /bs page's own newest fiscal-period label (e.g.
# "2026/06"), so a company's reference row is only re-stored once its
# next real fiscal-period row actually appears on IRBANK - the same
# reasoning irbank_buyback already uses (small page, dedup handles
# staleness, no days_back scoping needed). Note shares-outstanding is
# tagged with the SAME period as total-assets for one combined dedup key,
# even though it technically comes from a live market-cap snapshot (which
# changes daily with the share price) rather than a fiscal filing - this
# is deliberate: J6's 5%-of-shares-outstanding rule only needs a
# reasonably current share count, not a point-in-time-exact one, and
# tying it to the same slow refresh cadence as total assets avoids a
# second independent dedup key for what is, for this rule's purposes, the
# same "company reference snapshot" concept.
_IRBANK_BS_TABLE_RE = re.compile(
    r"<caption[^>]*>財務履歴（百万円）.*?</caption>.*?<tbody>(?P<tbody>.*?)</tbody>", re.S,
)
_IRBANK_BS_ROW_RE = re.compile(r"<tr[^>]*>(?P<row>.*?)</tr>", re.S)
_IRBANK_BS_CELL_RE = re.compile(r"<t[dh][^>]*>(?P<cell>.*?)</t[dh]>", re.S)


def _parse_irbank_yen_millions(cell_text: str) -> int | None:
    """Parse a bs-table cell like "751,159" into an int (JPY millions).
    Returns None for a blank/placeholder cell ("-", empty string) -
    confirmed live IRBANK uses a bare "-" for periods before a company
    started reporting a given line item (see Disco's own 2007/03 row,
    which has a real 総資産 value but a blank 有利子負債).
    """
    cleaned = re.sub(r"<[^>]+>", "", cell_text).strip().replace(",", "")
    if not cleaned or cleaned == "-":
        return None
    try:
        return int(cleaned)
    except ValueError:
        return None


def _parse_irbank_bs_period(period: str) -> datetime | None:
    """Parse a bs-table period label like "2026/06" (fiscal year/month,
    NOT year/month/day - _parse_japanese_date expects a full Y-M-D string
    and does not apply here) into a UTC datetime on that month's last day.
    The exact day within the month is never used for anything beyond
    sort order (there is no finer-grained date on this page), so using
    the reported month's first day is a fine, simple stand-in - never
    compared against a real day-level date elsewhere.
    """
    m = re.match(r"(\d{4})/(\d{1,2})$", period)
    if not m:
        return None
    return datetime(int(m.group(1)), int(m.group(2)), 1, tzinfo=timezone.utc)


_IRBANK_MARKET_CAP_RE = re.compile(
    r"時価総額</dt><dd>.*?<span class=\"text\">(?P<label>[^<]+)</span>", re.S,
)
_IRBANK_PRICE_RE = re.compile(
    r"前日終値.*?<span class=\"text\">(?P<price>[\d,]+)</span>", re.S,
)


def _parse_irbank_market_cap_yen(label: str) -> int | None:
    """Parse an IRBANK market-cap label into a plain yen int. Confirmed
    live three real formats exist depending on company size: trillion+oku
    ("24兆8880億"), oku+man ("1660億2262万"), and oku alone with no man
    remainder ("1兆681億") - all three are handled here since which one a
    given universe company gets is a function of its own market cap, not
    something to special-case per company.
    """
    trillion_match = re.search(r"(\d+)兆", label)
    oku_match = re.search(r"(\d+)億", label)
    man_match = re.search(r"億(\d+)万", label)
    if not trillion_match and not oku_match:
        return None
    trillion = int(trillion_match.group(1)) if trillion_match else 0
    oku = int(oku_match.group(1)) if oku_match else 0
    man = int(man_match.group(1)) if man_match else 0
    return trillion * 1_0000_0000_0000 + oku * 1_0000_0000 + man * 1_0000


def _fetch_one_irbank_company_reference(code: str, company: str) -> list[dict]:
    """Fetch one company's latest known total-assets figure (from its
    IRBANK /{code}/bs page) and shares-outstanding estimate (derived from
    market cap / previous-close price on its main /{code} page). Returns a
    single-item list (one synthetic "article" combining both reference
    facts) or an empty list if the total-assets half can't be parsed -
    fail-open, same contract as every other Japan fetcher in this file, so
    one company's malformed page never blocks the other 19 in the same
    run. shares_outstanding is allowed to be independently None (kept as
    a separate optional field, not a reason to drop the whole row) since
    total assets alone is still useful to J5 even if the market-cap page
    format changes unexpectedly for one company.
    """
    _irbank_rate_sleep()
    try:
        resp = httpx.get(
            f"https://irbank.net/{code}/bs", headers=_IRBANK_HEADERS, timeout=30.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        bs_text = resp.text
    except Exception as exc:
        logger.warning("[IRBANK] bs page fetch failed code=%s error=%s", code, exc)
        return []

    table_match = _IRBANK_BS_TABLE_RE.search(bs_text)
    if not table_match:
        logger.warning("[IRBANK] bs table not found code=%s", code)
        return []

    rows = _IRBANK_BS_ROW_RE.findall(table_match.group("tbody"))
    if not rows:
        return []

    # Confirmed live (Disco 6146, 2026-09-29): rows are oldest-first, so
    # the newest fiscal period is the LAST row, not the first.
    newest_row = rows[-1]
    cells = _IRBANK_BS_CELL_RE.findall(newest_row)
    if len(cells) < 2:
        return []

    period = re.sub(r"<[^>]+>", "", cells[0]).strip()
    total_assets_jpy_millions = _parse_irbank_yen_millions(cells[1])
    if not period or total_assets_jpy_millions is None:
        return []

    shares_outstanding: int | None = None
    try:
        _irbank_rate_sleep()
        resp = httpx.get(
            f"https://irbank.net/{code}", headers=_IRBANK_HEADERS, timeout=30.0,
            follow_redirects=True,
        )
        resp.raise_for_status()
        main_text = resp.text
        mc_match = _IRBANK_MARKET_CAP_RE.search(main_text)
        price_match = _IRBANK_PRICE_RE.search(main_text)
        if mc_match and price_match:
            market_cap_yen = _parse_irbank_market_cap_yen(mc_match.group("label"))
            price = int(price_match.group("price").replace(",", ""))
            if market_cap_yen and price:
                shares_outstanding = round(market_cap_yen / price)
    except Exception as exc:
        logger.warning("[IRBANK] main page fetch failed code=%s error=%s", code, exc)

    return [{
        "title": (
            f"{company} ({code}) company reference [{period}]:"
            f" total assets {total_assets_jpy_millions:,} JPY millions,"
            f" shares outstanding {shares_outstanding:,}" if shares_outstanding
            else f"{company} ({code}) company reference [{period}]:"
            f" total assets {total_assets_jpy_millions:,} JPY millions"
        ),
        "url": f"irbank-company-reference://{code}/{period}",
        "published": period,
        "source": "IRBANK",
        "summary": None,
        "body": None,
        "_pub_date": _parse_irbank_bs_period(period),
        "metadata": {
            "code": code,
            "company": company,
            "fiscal_period": period,
            "total_assets_jpy_millions": total_assets_jpy_millions,
            "shares_outstanding": shares_outstanding,
            "source_category": "jp_company_reference",
        },
    }]


def _fetch_irbank_company_reference(sources: list[dict]) -> list[dict]:
    """Fetch the latest total-assets + shares-outstanding reference facts
    for all companies across all irbank_company_reference sources.
    """
    companies = _japan_companies(sources)
    if not companies:
        return []

    articles: list[dict] = []
    for c in companies:
        articles.extend(_fetch_one_irbank_company_reference(c["code"], c["company"]))
    return articles


# ---------------------------------------------------------------------------
# Japan market signal - Kabutan TDnet disclosure mirror (kabutan.jp)
# ---------------------------------------------------------------------------
#
# Same no-key, undocumented-rate-limit posture as IRBANK above - reuses the
# identical browser User-Agent (confirmed live both sites need one) and a
# conservative fixed inter-request delay.
_kabutan_rate_lock = threading.Lock()
_kabutan_last_call = [0.0]
_KABUTAN_MIN_INTERVAL = 1.0

_KABUTAN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                  " (KHTML, like Gecko) Chrome/128.0 Safari/537.36",
}

# Matches one row of kabutan.jp/disclosures/'s disclosure table
# (class="stock_table") - confirmed live against real fetched HTML. Deliber-
# ately does NOT match the page's separate market-index widget (a
# different, unrelated <table> earlier in the same page, which has its own
# <tbody> with no <a href="/stock/?code=..."> links at all - excluded
# simply by requiring that link shape, not by any special-casing).
# company/market can be legitimately blank for some smaller listings
# (confirmed live, e.g. code 171A) - not a parse failure, just missing
# metadata on Kabutan's own side.
_KABUTAN_ROW_RE = re.compile(
    r'<tr>\s*'
    r'<td class="tac"><a href="/stock/\?code=(?P<code>[^"]+)">[^<]*</a></td>\s*'
    r'<th scope="row" class="tal">(?P<company>[^<]*)</th>\s*'
    r'<td class="tac">(?P<market>[^<]*)</td>\s*'
    r'<td class="tal">(?P<category>[^<]*)</td>\s*'
    r'<td class="tal wsnormal"[^>]*><a href="(?P<pdf_url>[^"]+)"[^>]*>(?P<title>[^<]*)[^<]*<[^>]*/?>?</a></td>\s*'
    r'<td><time datetime="(?P<datetime>[^"]+)">[^<]*</time></td>',
    re.S,
)


def _kabutan_rate_sleep() -> None:
    with _kabutan_rate_lock:
        elapsed = time.monotonic() - _kabutan_last_call[0]
        if elapsed < _KABUTAN_MIN_INTERVAL:
            time.sleep(_KABUTAN_MIN_INTERVAL - elapsed)
        _kabutan_last_call[0] = time.monotonic()


def _fetch_kabutan_disclosures(sources: list[dict], kubun: str | None, url_scheme: str, source_category: str) -> list[dict]:
    """Walk kabutan.jp/disclosures/'s paginated, newest-first disclosure
    feed page by page, stopping once a page's dates roll over to a prior
    trading day, and keep only rows for tickers in JAPAN_TICKER_UNIVERSE.

    kubun optionally scopes the feed server-side to one category (e.g. "j"
    = 自社株取得/buyback, confirmed live to return ONLY that category and to
    span far fewer pages per day than the unfiltered feed - one real page
    covered 3 trading days of buyback-only filings, vs. ~30 pages/day for
    the unfiltered feed) - shared by _fetch_kabutan_tdnet_mirror (kubun=None)
    and _fetch_kabutan_buyback (kubun="j") rather than two near-duplicate
    functions, same reuse pattern as _fetch_edinet_by_mode above.

    Deliberately daily-only (no days_back parameter, unlike
    _fetch_irbank_financials) for either kubun value - irbank_financials/
    irbank_buyback already own historical data; this source's job is
    same-day corroboration only.
    """
    codes: set[str] = set()
    for source in sources:
        config = source.get("config") or {}
        codes.update(config.get("codes", []))
    if not codes:
        return []

    articles: list[dict] = []
    seen_date: str | None = None
    page = 1
    # Bounded generously above the ~30 pages/day confirmed live for the
    # unfiltered feed (a kubun-scoped feed needs far fewer), so a freak
    # high-volume day cannot spin this into an unbounded crawl.
    max_pages = 60
    while page <= max_pages:
        params = []
        if kubun:
            params.append(f"kubun={kubun}")
        if page > 1:
            params.append(f"page={page}")
        url = "https://kabutan.jp/disclosures/" + (f"?{'&'.join(params)}" if params else "")
        _kabutan_rate_sleep()
        try:
            resp = httpx.get(url, headers=_KABUTAN_HEADERS, timeout=30.0, follow_redirects=True)
            resp.raise_for_status()
            text = resp.text
        except Exception as exc:
            logger.warning("[KABUTAN] disclosures fetch failed page=%d kubun=%s error=%s", page, kubun, exc)
            break

        rows = list(_KABUTAN_ROW_RE.finditer(text))
        if not rows:
            break

        stop = False
        for row in rows:
            row_date = row.group("datetime")[:10]
            if seen_date is None:
                seen_date = row_date
            elif row_date != seen_date:
                stop = True
                break

            code = row.group("code")
            if code not in codes:
                continue

            pdf_url = row.group("pdf_url")
            doc_id = pdf_url.rstrip("/").rsplit("/", 1)[-1]
            pub_date = datetime.fromisoformat(row.group("datetime"))

            articles.append({
                # Kabutan reuses generic disclosure-title text across
                # different companies/times routinely (e.g. plain "自己株式
                # の取得状況に関するお知らせ" with no distinguishing detail) -
                # same collision shape confirmed live for IRBANK's own
                # title text (see _fetch_one_irbank_financials's comment on
                # this exact bug class). articles' uq_articles_run_title
                # unique index is (run_id, lower(title)), so two same-day
                # same-title rows in one run would silently drop the
                # second via ON CONFLICT DO NOTHING. The minute-precision
                # datetime is unique per disclosure in practice (confirmed
                # live: Kabutan's own feed has never shown two different
                # disclosures at the identical minute), so folding it in
                # here prevents that without changing what a reader sees.
                "title": f"{row.group('company').strip() or code} ({code}) [{row.group('datetime')}]: {row.group('title').strip()}",
                "url": f"{url_scheme}://{code}/{doc_id}",
                "published": row.group("datetime"),
                "source": "Kabutan",
                "summary": None,
                "body": None,
                "_pub_date": pub_date,
                "metadata": {
                    "code": code,
                    "company": row.group("company").strip() or None,
                    "market": row.group("market").strip() or None,
                    "category": row.group("category").strip(),
                    "source_category": source_category,
                    "pdf_url": pdf_url,
                },
            })

        if stop:
            break
        page += 1
    else:
        # Loop exhausted max_pages without the date ever rolling over -
        # confirmed live this is a real gap (an earlier version had no
        # warning here at all): a genuinely massive single-day volume
        # (e.g. results season) could silently truncate mid-day with zero
        # signal that anything was cut off, unlike every other
        # truncation/failure path in this fetcher, which logs a warning.
        logger.warning(
            "[KABUTAN] hit max_pages=%d for kubun=%s without the date rolling"
            " over (date=%s) - today's results may be incomplete",
            max_pages, kubun, seen_date,
        )

    return articles


def _fetch_kabutan_tdnet_mirror(sources: list[dict]) -> list[dict]:
    return _fetch_kabutan_disclosures(sources, kubun=None, url_scheme="kabutan-tdnet", source_category="jp_disclosure")


def _fetch_kabutan_buyback(sources: list[dict]) -> list[dict]:
    return _fetch_kabutan_disclosures(sources, kubun="j", url_scheme="kabutan-buyback", source_category="jp_buyback_announce")


# ---------------------------------------------------------------------------
# Japan market signal - EDINET (Financial Services Agency filing API)
# ---------------------------------------------------------------------------
#
# Free, official REST API, but requires a registered Subscription-Key
# (EDINET_API_KEY) - confirmed live 2026-09-27/28 with a real key: the
# v2 API returns a hard 401 without one, no anonymous access exists at all
# (unlike IRBANK/Kabutan, which need no key).
# Japan Standard Time. Japan observes no daylight saving, so a fixed
# offset is correct year-round - EDINET and TDnet both report
# submission times in local time with no offset on the string.
_JST = timezone(timedelta(hours=9))

_EDINET_DOCUMENTS_URL = "https://api.edinet-fsa.go.jp/api/v2/documents.json"
_EDINET_DOCUMENT_URL_TMPL = "https://api.edinet-fsa.go.jp/api/v2/documents/{doc_id}"

# docTypeCode 350 = 大量保有報告書 (large shareholding report, the 5%-rule
# filing) - confirmed live via a real date's documents.json response. The
# metadata list NEVER carries the actual percentage held (secCode is null
# on every 350-type row, and no ratio field exists in documents.json at
# all) - that value is only inside the filing's own CSV export (type=5),
# fetched per-filing below, same two-step shape as irbank_financials'
# list-then-detail design.
_EDINET_DOC_TYPE_SHAREHOLDING = "350"

# docTypeCode 220 = 自己株券買付状況報告書 (report on status of share
# repurchase) - confirmed live via a real filing (Godo Steel, 2026-09-15).
# UNLIKE the 350-type filing above, this one's list-metadata carries
# secCode/edinetCode DIRECTLY (issuerEdinetCode is null instead - a company
# reporting its OWN buyback has no separate "issuer," it IS the issuer),
# so the ticker join uses a different field. Also unlike 350, the CSV
# export here has NO discrete numeric field for shares/amount/progress at
# all - every figure lives inside free-text [TextBlock] XBRL elements
# (confirmed live: AcquisitionsByResolutionOfBoardOfDirectorsMeetingText
# Block contains a long unstructured Japanese paragraph with dates, share
# counts, and yen amounts run together, not a table pdfplumber-style
# extraction could parse cleanly). Per explicit decision: this fetcher
# stores the filing's existence/dates/raw text block as-is and does NOT
# attempt to parse it into numeric fields - IRBANK's own dedicated
# /{code}/buyback page (a separate, not-yet-built source_type,
# irbank_buyback) already has this data as genuinely clean structured
# fields (date, shares, cumulative %, program size - confirmed live in
# earlier research), so it is the intended primary source for buyback
# NUMBERS; this source's job is same-day EDINET corroboration only.
# 230 (an amendment to 220) was never observed live in this session's
# testing but is included on the same assumption, given it is documented
# as the analogous amendment code, same pattern as SEC 10-K/A.
_EDINET_DOC_TYPE_BUYBACK = ("220", "230")
_EDINET_BUYBACK_TEXT_ELEMENT_ID = "jpcrp-sbr_cor:AcquisitionsByResolutionOfBoardOfDirectorsMeetingTextBlock"

# docTypeCode 180 = 臨時報告書 (extraordinary report), 190 = its amendment -
# the design spec's Step 3 explicitly asks for "5% shareholding filings
# AND extraordinary reports" and this docType was missed in the initial
# build (only 350/220/230 were implemented) - confirmed live 2026-09-28
# via a real filing search across several cached dates: Shin-Etsu Chemical
# (4063) filed one on 2026-09-15 (docID S100Z2DU) - a stock-option/warrant
# issuance to directors and employees, joined via edinetCode DIRECTLY
# (issuerEdinetCode was null, same as buyback status - a company reporting
# its OWN extraordinary event has no separate "issuer" field either).
#
# currentReportReason (a legal-clause code, e.g. "第19条第2項第2号の2" for
# the stock-option case found) is present directly in documents.json's
# list metadata for this doctype - unlike shareholding/buyback, no
# per-filing CSV/PDF fetch is needed just to know WHAT KIND of
# extraordinary event this is, though the specific numeric details (in
# the Shin-Etsu case: warrant count, strike price, share count) still
# only live in the filing's own PDF/CSV body. "Extraordinary report" is a
# broad, varied category (confirmed live it can mean anything from a
# stock-option grant to M&A to a disaster disclosure, distinguished only
# by the reason-code) - same reasoning as edinet_buyback_status applies:
# store the filing's existence, reason code, and (when the CSV export
# succeeds) its raw text, rather than attempt a single numeric-field
# extraction that would not generalize across such a varied doctype.
_EDINET_DOC_TYPE_EXTRAORDINARY = ("180", "190")

# The exact XBRL element ID for the holding-percentage value inside a
# type=5 CSV export - confirmed live against a real filing (E06477's
# 2026-09-15 report on issuer E27585): "jplvh_cor:
# HoldingRatioOfShareCertificatesEtc", value "0.0504" (i.e. 5.04%). A
# filing can report this MULTIPLE times (once per named holder/context, and
# again as a filing-level total) - confirmed live 3 occurrences in one real
# CSV, all carrying the same final value - so this takes the LAST matching
# row (the filing-level total, confirmed live to always appear last),
# rather than the first (a per-holder breakdown, which undercounts a joint
# filing).
_EDINET_HOLDING_RATIO_ELEMENT_ID = "jplvh_cor:HoldingRatioOfShareCertificatesEtc"
# The ratio stated in the holder's PREVIOUS report, which a Japanese
# change report (変更報告書) must carry alongside the current one -
# confirmed live across six real filings. Without it a card can only
# say "12.8% stake changed" and not whether that is a buy or a sell;
# Toshiba's real Kioxia filing is 14.06% -> 12.84%, a sell-down.
# Already inside the CSV this function downloads, so reading it costs
# no extra request.
_EDINET_HOLDING_RATIO_PREVIOUS_ELEMENT_ID = (
    "jplvh_cor:HoldingRatioOfShareCertificatesEtcPerLastReport"
)
# The filing's own stated reason, e.g. 株券等保有割合の1%以上の減少.
_EDINET_CHANGE_REPORT_REASON_ELEMENT_ID = (
    "jplvh_cor:ReasonForFilingChangeReportCoverPage"
)

_edinet_rate_lock = threading.Lock()
_edinet_last_call = [0.0]
_EDINET_MIN_INTERVAL = 3.0  # no official rate limit published - confirmed live no 429 seen at this pace


def _edinet_rate_sleep() -> None:
    with _edinet_rate_lock:
        elapsed = time.monotonic() - _edinet_last_call[0]
        if elapsed < _EDINET_MIN_INTERVAL:
            time.sleep(_EDINET_MIN_INTERVAL - elapsed)
        _edinet_last_call[0] = time.monotonic()


# The 臨時報告書 block a 第19条第2項第4号 filing carries. Unlike a
# docTypeCode-350 report, which the HOLDER files on crossing 5%, this
# one is filed by the ISSUER when its own 主要株主 (≥10% of voting
# rights) composition changes - different filer, different trigger,
# and confirmed live that no 350 exists for the same event.
_EDINET_MAJOR_SHAREHOLDER_ELEMENT_ID = (
    "jpcrp-esr_cor:ChangesInMajorShareholderTextBlock"
)

# The block is prose with the figures embedded, not discrete fields:
#   ...異動前181,572個9.59％異動後209,566個11.06％...
# 異動前 = before, 異動後 = after, 個 = the vote count, ％ the share of
# total voting rights. The percentage is what the signal reads; the
# vote count is matched only so the two numbers cannot be confused
# with each other.
_MAJOR_SHAREHOLDER_MOVE_RE = re.compile(
    r"異動前[^0-9]*[\d,]+個\s*([\d.]+)\s*[％%]"
    r".*?異動後[^0-9]*[\d,]+個\s*([\d.]+)\s*[％%]",
    re.S,
)
# 主要株主となるもの ("the party BECOMING a major shareholder") and
# 主要株主でなくなるもの ("ceasing to be one"). The filing states which
# of the two happened, in those words, and the holder's name follows.
#
# Capturing the marker is what makes the status change a FILED FACT
# rather than something derived from a threshold. The filing never
# prints the threshold - it cites the statute and leaves the number
# to FIEA Article 163 - so comparing the percentage against a
# hardcoded 10% would be grading on an assumption the document does
# not support. What the document does support, in its own words, is
# that this holder became a major shareholder at 11.06% and was not
# one at 9.59%.
_MAJOR_SHAREHOLDER_NAME_RE = re.compile(
    r"主要株主(となるもの|でなくなるもの)\s*([^\n(（]{2,80})"
)
_MAJOR_SHAREHOLDER_BECAME = "となるもの"


def _is_major_shareholder_clause(reason: str | None) -> bool:
    """True for 第19条第2項第4号 - a change in major shareholders.

    Matched on the final 第N号 exactly, never as a suffix: a filing
    citing 第19条第2項第14号 ends with "4号" and would otherwise be
    fetched as a major-shareholder change it is not.
    """
    if not reason:
        return False
    normalised = reason.translate(str.maketrans("０１２３４５６７８９", "0123456789"))
    found = re.findall(r"第(\d+)号(?:の(\d+))?", normalised)
    return bool(found) and found[-1] == ("4", "")


def _fetch_edinet_major_shareholder_change(
    doc_id: str, api_key: str,
) -> dict[str, Any] | None:
    """The holder and the before/after voting-rights share from a
    第19条第2項第4号 extraordinary report.

    Returns None when the filing is not one of these, or the block
    cannot be parsed - fail-open, like every other Japan fetcher.

    This is the only source of the figure. The clause tells a reader
    that a major shareholder changed; it takes the document to say by
    how much, and without that the event cannot be graded against the
    same thresholds an ownership filing is.
    """
    _edinet_rate_sleep()
    try:
        resp = httpx.get(
            _EDINET_DOCUMENT_URL_TMPL.format(doc_id=doc_id),
            params={"type": "5", "Subscription-Key": api_key},
            timeout=30.0,
        )
        resp.raise_for_status()
        import zipfile
        from io import BytesIO

        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not names:
                return None
            text = zf.read(names[0]).decode("utf-16")
    except Exception as exc:                                # noqa: BLE001
        logger.warning("[EDINET] major-shareholder export fetch failed "
                       "doc_id=%s error=%s", doc_id, exc)
        return None

    block = ""
    for line in text.split("\n"):
        fields = line.strip().split("\t")
        if len(fields) >= 9 and fields[0].strip('"') == _EDINET_MAJOR_SHAREHOLDER_ELEMENT_ID:
            block = fields[8].strip('"')
            break
    if not block:
        return None

    move = _MAJOR_SHAREHOLDER_MOVE_RE.search(block)
    if not move:
        logger.warning("[EDINET] major-shareholder block found but no "
                       "before/after pair doc_id=%s", doc_id)
        return None
    try:
        before, after = float(move.group(1)), float(move.group(2))
    except ValueError:
        return None

    name = _MAJOR_SHAREHOLDER_NAME_RE.search(block)
    # became / ceased / None where the filing words it some other way.
    status = None
    if name:
        status = ("became" if name.group(1) == _MAJOR_SHAREHOLDER_BECAME
                  else "ceased")
    return {
        "major_shareholder_name": name.group(2).strip() if name else None,
        "major_shareholder_status_change": status,
        "major_shareholder_pct_previous": before,
        "major_shareholder_pct": after,
    }


def _fetch_edinet_holding_ratio(
    doc_id: str, api_key: str,
) -> tuple[float | None, float | None]:
    """Fetch a docTypeCode-350 filing's CSV export (type=5) and return
    (holding_ratio, previous_holding_ratio) as fractions.

    The previous ratio is what makes a change report readable as a buy
    or a sell. It is None for an initial 大量保有報告書, which has no
    prior position, and for any filing that omits the element.

    Returns (None, None) on any failure (fail-open, same convention as
    every other Japan fetcher).
    """
    _edinet_rate_sleep()
    try:
        resp = httpx.get(
            _EDINET_DOCUMENT_URL_TMPL.format(doc_id=doc_id),
            params={"type": "5", "Subscription-Key": api_key},
            timeout=30.0,
        )
        resp.raise_for_status()
        import zipfile
        from io import BytesIO

        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not names:
                return (None, None)
            raw = zf.read(names[0])
        text = raw.decode("utf-16")
    except Exception as exc:
        logger.warning("[EDINET] CSV export fetch failed doc_id=%s error=%s", doc_id, exc)
        return (None, None)

    # Walk the rows in order, keeping each holding ratio next to the
    # prior-report ratio that follows it. A filing with co-filers
    # (joint holders) states one pair per filer and THEN repeats every
    # current value again - confirmed across six real filings. So the
    # last current value is the last co-filer's, not the aggregate:
    # taking it is how this function used to report 5.9% for a filing
    # whose real total was 8.24%. The last COMPLETE pair is the
    # aggregate, which is the figure the card means by "the stake".
    pairs: list[tuple[float, float | None]] = []
    pending: float | None = None
    for line in text.split("\n"):
        fields = line.strip().split("\t")
        if len(fields) < 9:
            continue
        element = fields[0].strip('"')
        raw = fields[8].strip('"')
        if element == _EDINET_HOLDING_RATIO_ELEMENT_ID:
            try:
                pending = float(raw)
            except ValueError:
                pending = None
        elif element == _EDINET_HOLDING_RATIO_PREVIOUS_ELEMENT_ID and pending is not None:
            # An initial report writes "－" here - a real "no prior
            # holding", not a parse failure, so the pair is kept with
            # a None previous rather than dropped.
            try:
                previous: float | None = float(raw)
            except ValueError:
                previous = None
            pairs.append((pending, previous))
            pending = None

    if pairs:
        return pairs[-1]
    # No pair at all: fall back to the last bare current value, which
    # is what a filing with no prior-report line looks like.
    return (pending, None) if pending is not None else (None, None)


# jpcrp-esr_cor:ReasonForFilingTextBlock ("提出理由") - confirmed live in a
# real extraordinary report (Shin-Etsu Chemical, docID S100Z2DU, a
# stock-option issuance) - the free-text explanation of why the report was
# filed. Unlike the buyback text block, this is present alongside a
# consistent currentReportReason legal-clause code already in
# documents.json's own list metadata, so the WHAT-KIND-OF-EVENT signal
# doesn't require this fetch at all - only the free-text detail does.
_EDINET_EXTRAORDINARY_TEXT_ELEMENT_ID = "jpcrp-esr_cor:ReasonForFilingTextBlock"


def _fetch_edinet_csv_text_element(doc_id: str, api_key: str, element_id: str) -> str | None:
    """Fetch a filing's CSV export (type=5) and return one XBRL element's
    raw text value, unparsed. Shared by edinet_buyback_status
    (_EDINET_BUYBACK_TEXT_ELEMENT_ID) and edinet_extraordinary_report
    (_EDINET_EXTRAORDINARY_TEXT_ELEMENT_ID) - both doctypes carry their
    real content as a single free-text XBRL block rather than discrete
    numeric fields, so both are stored as-is rather than parsed further -
    see each doctype's own comment above for why.
    """
    _edinet_rate_sleep()
    try:
        resp = httpx.get(
            _EDINET_DOCUMENT_URL_TMPL.format(doc_id=doc_id),
            params={"type": "5", "Subscription-Key": api_key},
            timeout=30.0,
        )
        resp.raise_for_status()
        import zipfile
        from io import BytesIO

        with zipfile.ZipFile(BytesIO(resp.content)) as zf:
            names = [n for n in zf.namelist() if n.endswith(".csv")]
            if not names:
                return None
            raw = zf.read(names[0])
        text = raw.decode("utf-16")
    except Exception as exc:
        logger.warning("[EDINET] CSV export fetch failed doc_id=%s element=%s error=%s", doc_id, element_id, exc)
        return None

    for line in text.split("\n"):
        if not line.startswith(f'"{element_id}"'):
            continue
        fields = line.strip().split("\t")
        if len(fields) < 9:
            continue
        return fields[8].strip('"').strip()
    return None


def _fetch_one_edinet_date(
    date: str,
    edinet_code_to_ticker: dict[str, str],
    api_key: str,
    mode: str,
) -> list[dict]:
    """Fetch EDINET's documents list for one date, filtered by mode:

    - "shareholding": docTypeCode 350, joined via issuerEdinetCode (the
      company whose shares were bought - NOT the filer), holding-ratio
      percentage fetched per-filing from its CSV export.
    - "buyback": docTypeCode 220/230, joined via edinetCode/secCode
      DIRECTLY (a buyback filer IS the issuer, there is no separate issuer
      field for this doctype - confirmed live), raw acquisition text block
      fetched per-filing from its CSV export and stored unparsed.
    - "extraordinary": docTypeCode 180/190, joined via edinetCode DIRECTLY
      (same as buyback - confirmed live via Shin-Etsu Chemical's real
      stock-option filing, issuerEdinetCode was null there too). The
      currentReportReason legal-clause code is already present in
      documents.json's own list metadata for this doctype (no per-filing
      fetch needed just to know what KIND of extraordinary event this is);
      the free-text detail is fetched per-filing and stored unparsed, same
      as buyback status.

    One shared list-fetch + dispatch, not three separate near-duplicate
    functions, mirroring how Taiwan's _fetch_one_material_dump handles
    TWSE vs TPEx via a key_map rather than being two functions.
    """
    _edinet_rate_sleep()
    try:
        resp = httpx.get(
            _EDINET_DOCUMENTS_URL,
            params={"date": date, "type": "2", "Subscription-Key": api_key},
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[EDINET] documents list fetch failed date=%s error=%s", date, exc)
        return []

    articles: list[dict] = []
    for doc in data.get("results") or []:
        doc_type = doc.get("docTypeCode")

        if mode == "shareholding":
            if doc_type != _EDINET_DOC_TYPE_SHAREHOLDING:
                continue
            join_code = doc.get("issuerEdinetCode")
        elif mode == "buyback":
            if doc_type not in _EDINET_DOC_TYPE_BUYBACK:
                continue
            join_code = doc.get("edinetCode")
        elif mode == "extraordinary":
            if doc_type not in _EDINET_DOC_TYPE_EXTRAORDINARY:
                continue
            join_code = doc.get("edinetCode")
        else:
            raise ValueError(f"unknown mode: {mode}")

        ticker = edinet_code_to_ticker.get(join_code) if join_code else None
        if not ticker:
            continue

        doc_id = doc["docID"]
        try:
            # EDINET reports submission times in Japan time with no
            # offset on the string. Stamping them UTC without
            # converting moved every filing 9 hours: a 16:50 JST
            # disclosure read as 16:50Z, which is 01:50 the next
            # morning in Tokyo - not a time any company files.
            pub_date = (
                datetime.strptime(doc["submitDateTime"], "%Y-%m-%d %H:%M")
                .replace(tzinfo=_JST)
                .astimezone(timezone.utc)
            )
        except (KeyError, ValueError):
            pub_date = None

        base_metadata = {
            "code": ticker,
            "filer_edinet_code": doc.get("edinetCode"),
            "filer_name": doc.get("filerName"),
            "doc_id": doc_id,
            "doc_type_code": doc_type,
        }

        if mode == "shareholding":
            ratio, ratio_previous = _fetch_edinet_holding_ratio(doc_id, api_key)
            articles.append({
                # EDINET's docDescription is a generic report-type label
                # (e.g. "大量保有報告書"), not distinguished by filer/issuer -
                # same title-reuse collision risk confirmed live for
                # IRBANK's own generic titles (see _fetch_one_irbank_
                # financials's comment on this bug class). articles'
                # uq_articles_run_title index is (run_id, lower(title)),
                # so two same-day filings sharing this label in one run
                # would silently drop the second via ON CONFLICT DO
                # NOTHING. doc_id is EDINET's own globally-unique document
                # ID (already the dedup key in this row's own url), so
                # appending it here guarantees a distinct title too.
                "title": f"{doc.get('filerName', 'Unknown filer')} -> {ticker} [{doc_id}]: {doc.get('docDescription', '')}",
                "url": f"edinet-filing://{join_code}/{doc_id}",
                # Real UTC, not EDINET's own Japan-time string - see
                # pub_date above for why the raw value is 9 hours off.
                "published": pub_date.isoformat() if pub_date else None,
                "source": "EDINET",
                "summary": None,
                "body": None,
                "_pub_date": pub_date,
                "metadata": {
                    **base_metadata,
                    "issuer_edinet_code": join_code,
                    "holding_ratio": ratio,
                    # The ratio the holder's PREVIOUS report stated, so
                    # a change report reads as a buy or a sell rather
                    # than a bare new number. None for an initial
                    # report, which has no prior position.
                    "holding_ratio_previous": ratio_previous,
                    # EDINET's own report-type label, kept as a real field
                    # rather than only folded into the title above. It
                    # carries two facts nothing else on the row does:
                    # 大量保有報告書 (an initial report, a holder newly
                    # crossing 5%) vs 変更報告書 (a change report against a
                    # position already disclosed), and the 特例対象株券等
                    # suffix marking the relaxed periodic-filing regime for
                    # passive institutional holders. Downstream
                    # classification distinguishes those cases; parsing
                    # them back out of a formatted title would be fragile.
                    "doc_description": doc.get("docDescription"),
                    "source_category": "jp_ownership",
                },
            })
        elif mode == "buyback":
            raw_text = _fetch_edinet_csv_text_element(doc_id, api_key, _EDINET_BUYBACK_TEXT_ELEMENT_ID)
            articles.append({
                # Same generic-docDescription title-collision risk as the
                # shareholding branch above (e.g. two same-day buyback
                # status filings from different tickers, or the same
                # ticker's own program+status pair, sharing the identical
                # label text) - doc_id folded in for the same reason.
                "title": f"{ticker} [{doc_id}]: {doc.get('docDescription', '')}",
                "url": f"edinet-buyback://{join_code}/{doc_id}",
                # Real UTC, not EDINET's own Japan-time string - see
                # pub_date above for why the raw value is 9 hours off.
                "published": pub_date.isoformat() if pub_date else None,
                "source": "EDINET",
                "summary": raw_text,
                "body": None,
                "_pub_date": pub_date,
                "metadata": {
                    **base_metadata,
                    "raw_acquisition_text": raw_text,
                    "source_category": "jp_buyback_status",
                },
            })
        else:
            raw_text = _fetch_edinet_csv_text_element(doc_id, api_key, _EDINET_EXTRAORDINARY_TEXT_ELEMENT_ID)
            articles.append({
                # Same generic-docDescription title-collision risk as the
                # shareholding/buyback branches above - doc_id folded in
                # for the same reason.
                "title": f"{ticker} [{doc_id}]: {doc.get('docDescription', '')}",
                "url": f"edinet-extraordinary://{join_code}/{doc_id}",
                # Real UTC, not EDINET's own Japan-time string - see
                # pub_date above for why the raw value is 9 hours off.
                "published": pub_date.isoformat() if pub_date else None,
                "source": "EDINET",
                "summary": raw_text,
                "body": None,
                "_pub_date": pub_date,
                "metadata": {
                    **base_metadata,
                    "current_report_reason": doc.get("currentReportReason"),
                    "raw_reason_text": raw_text,
                    "source_category": "jp_extraordinary",
                    # Only for 第19条第2項第4号, a major-shareholder
                    # change. That clause names the event but not its
                    # size, and no docTypeCode-350 filing covers the
                    # same event (different filer, different trigger -
                    # confirmed live against EDINET for the one real
                    # case stored). One extra export fetch per such
                    # filing is what turns "a major shareholder
                    # changed" into "9.59% -> 11.06%".
                    **(_fetch_edinet_major_shareholder_change(doc_id, api_key) or {}
                       if _is_major_shareholder_clause(doc.get("currentReportReason"))
                       else {}),
                },
            })
    return articles


def _fetch_edinet_by_mode(sources: list[dict], days_back: int, api_key: str, mode: str) -> list[dict]:
    """Shared driver for both EDINET fetchers below: resolves tickers to
    EDINET codes once, then walks days_back calendar days (EDINET's
    documents.json takes exactly ONE date per request - no date-range query
    param, confirmed live - so this issues one request per day in the
    window, same shape as this project's SEC EDGAR/DART per-day or
    per-company query patterns elsewhere in this file).
    """
    from edinet_code import resolve_edinet_codes

    tickers: list[str] = []
    for source in sources:
        config = source.get("config") or {}
        tickers.extend(config.get("codes", []))
    if not tickers:
        return []

    ticker_to_edinet = resolve_edinet_codes(tickers)
    edinet_code_to_ticker = {v: k for k, v in ticker_to_edinet.items()}
    if not edinet_code_to_ticker:
        return []

    articles: list[dict] = []
    today = datetime.now(timezone.utc).date()
    for offset in range(days_back):
        date_str = (today - timedelta(days=offset)).strftime("%Y-%m-%d")
        articles.extend(_fetch_one_edinet_date(date_str, edinet_code_to_ticker, api_key, mode))
    return articles


def _fetch_edinet_filing(sources: list[dict], days_back: int, api_key: str) -> list[dict]:
    """Fetch docTypeCode-350 (large shareholding) filings for all companies
    across all edinet_filing sources, over the last days_back days.
    """
    return _fetch_edinet_by_mode(sources, days_back, api_key, mode="shareholding")


def _fetch_edinet_buyback_status(sources: list[dict], days_back: int, api_key: str) -> list[dict]:
    """Fetch docTypeCode-220/230 (buyback status report) filings for all
    companies across all edinet_buyback_status sources, over the last
    days_back days.
    """
    return _fetch_edinet_by_mode(sources, days_back, api_key, mode="buyback")


def _fetch_edinet_extraordinary_report(sources: list[dict], days_back: int, api_key: str) -> list[dict]:
    """Fetch docTypeCode-180/190 (extraordinary report) filings for all
    companies across all edinet_extraordinary_report sources, over the
    last days_back days.
    """
    return _fetch_edinet_by_mode(sources, days_back, api_key, mode="extraordinary")


# ---------------------------------------------------------------------------
# Japan market signal - SEAJ (Semiconductor Equipment Association of Japan)
# ---------------------------------------------------------------------------
#
# Free, no API key, no per-company scoping at all - this is a single
# industry-wide monthly figure, not scoped to JAPAN_TICKER_UNIVERSE (unlike
# every other Japan source_type). Confirmed live 2026-09-28: the free
# public release is a 3-MONTH MOVING AVERAGE, never a true single-month
# figure - SEAJ's own PDF states this explicitly ("Note : All data is
# based on three month average numbers") and this is confirmed to be the
# ceiling of what SEAJ (and, per earlier research, every other
# free/third-party source, including SEMI's own WWSEMS report) publishes
# for free - there is no free path to the underlying true monthly number.
# The design's own rule for this signal is deliberately built around this
# limitation (compare the 3-month-average series against its own 12-month
# average and use SEAJ's own published YoY%, not a derived single-month
# spike detector) - see this codebase's Japan design notes; not
# implemented here, since classification is signal-detection-agent's job.
_SEAJ_STATISTICS_URL = "https://www.seaj.or.jp/english/statistics/index.html"
_SEAJ_BASE_URL = "https://www.seaj.or.jp/english/statistics/"

# Isolates the semiconductor-equipment press release row from the FPD
# (flat panel display) and World Wide SEMS Report rows also present on the
# same index page - confirmed live via the exact substring SEAJ's own page
# uses, including its own typo ("Equipement", not "Equipment") - a
# corrected-spelling regex would silently never match.
_SEAJ_INDEX_LINK_RE = re.compile(
    r'<tr><td class="lft">Sales Express Report\s?\(3Month Average\)\s*'
    r'<br>Semiconductor Manufacturing Equipment\(Japanese Equipement\)</td>'
    r'<td class="cntr"><a href="(?P<pdf_href>[^"]+)"[^>]*>.*?</a></td>'
    r'<td class="cntr">(?P<release_date>[^<]+)</td></tr>',
)

# One row of the PDF's billings table, confirmed live against a real
# release (August 2026): "August 2026(prelim) 597,915 7.5% 47.4%" or a
# bare "March 2026 480,182 13.5% 11.1%" (no qualifier - only the two most
# recent months in any given release carry one). qualifier matters for
# dedup: the SAME calendar month appears in two consecutive releases,
# first as "(prelim)" then finalized as "(final)" the following month -
# these are genuinely different, non-duplicate records (a revision from
# preliminary to final is itself meaningful), not the same fact twice.
_SEAJ_MONTH_ROW_RE = re.compile(
    r"^(?P<month>[A-Za-z]+ \d{4})(?:\((?P<qualifier>prelim|final)\))?\s+"
    r"(?P<billings>[\d,]+)\s+(?P<mom>-?[\d.]+)%\s+(?P<yoy>-?[\d.]+)%$",
)


def _fetch_seaj_billings(sources: list[dict]) -> list[dict]:
    """Fetch SEAJ's current monthly billings press release: find the
    semiconductor-equipment PDF link on the statistics index page, download
    it, and extract every month's row from its billings table (confirmed
    live: each release includes ~6 trailing months, not just the newest).

    sources is accepted for interface consistency with every other
    _fetch_* function (all take a `sources: list[dict]` even when, as
    here, there is nothing per-source to iterate - SEAJ has exactly one
    seed row and no per-company config at all).
    """
    if not sources:
        return []

    try:
        resp = httpx.get(_SEAJ_STATISTICS_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        # Confirmed live: this page is Shift-JIS (cp932), not UTF-8 -
        # decoding as UTF-8 raises rather than silently mojibake-ing.
        text = resp.content.decode("cp932", errors="replace")
    except Exception as exc:
        logger.warning("[SEAJ] statistics index fetch failed error=%s", exc)
        return []

    link_match = _SEAJ_INDEX_LINK_RE.search(text)
    if not link_match:
        logger.warning("[SEAJ] no semiconductor-equipment press release link found on index page")
        return []

    pdf_url = _SEAJ_BASE_URL + link_match.group("pdf_href")
    release_date = link_match.group("release_date")

    try:
        pdf_resp = httpx.get(pdf_url, timeout=30.0, follow_redirects=True)
        pdf_resp.raise_for_status()
        import pdfplumber
        from io import BytesIO

        with pdfplumber.open(BytesIO(pdf_resp.content)) as pdf:
            full_text = "\n".join(p.extract_text() or "" for p in pdf.pages)
    except Exception as exc:
        logger.warning("[SEAJ] PDF fetch/parse failed url=%s error=%s", pdf_url, exc)
        return []

    articles: list[dict] = []
    for line in full_text.split("\n"):
        row_match = _SEAJ_MONTH_ROW_RE.match(line.strip())
        if not row_match:
            continue
        month = row_match.group("month")
        qualifier = row_match.group("qualifier") or "final"
        period_key = f"{month.replace(' ', '-')}-{qualifier}"

        pub_date = None
        month_match = re.match(r"([A-Za-z]+) (\d{4})", month)
        if month_match:
            try:
                pub_date = datetime.strptime(f"{month_match.group(1)} {month_match.group(2)}", "%B %Y").replace(tzinfo=timezone.utc)
            except ValueError:
                pass

        articles.append({
            "title": f"SEAJ Japan semiconductor equipment billings, {month} ({qualifier}): "
                     f"¥{row_match.group('billings')}M ({row_match.group('yoy')}% YoY)",
            "url": f"seaj-billings://{period_key}",
            "published": release_date,
            "source": "SEAJ",
            "summary": None,
            "body": None,
            "_pub_date": pub_date,
            "metadata": {
                "period": month,
                "qualifier": qualifier,
                "billings_3mo_avg_millions_jpy": int(row_match.group("billings").replace(",", "")),
                "mom_pct": float(row_match.group("mom")),
                "yoy_pct": float(row_match.group("yoy")),
                # A real publication date, read from the index page's own
                # release column - unlike the historical archive, which
                # records none (see fetch_seaj_billings_backfill).
                "release_date": release_date,
                "date_confirmed": bool(release_date),
                "pdf_url": pdf_url,
                "source_category": "jp_industry",
            },
        })
    return articles


# Isolates the same Excel-download row from the index page's other links
# (the PDF release, the FPD equipment rows, the World Wide SEMS Report) -
# confirmed live 2026-09-29 this row's own label text is SHORTER than the
# PDF row's ("Semiconductor Manufacturing Equipment", no "(Japanese
# Equipement)" suffix, and no separate release-date cell) - a real,
# distinct HTML shape, not reusable via _SEAJ_INDEX_LINK_RE.
_SEAJ_XLS_INDEX_LINK_RE = re.compile(
    r'<tr><td class="lft">Sales Express Report\s?\(3Month Average\)\s*'
    r'<br>\s*Semiconductor Manufacturing Equipment</td>'
    r'<td class="cntr"><a href="(?P<xls_href>[^"]+\.xls)"[^>]*>.*?</a></td></tr>',
)

# Japan Signals spec Section 9 Step 4's own "two years of history on the
# first run" - doubled to 5, matching this codebase's own established
# precedent for "full real history" (IRBANK's own 1825-day/5-year
# backfill) rather than the spec's bare minimum: SEAJ's real equipment-
# billings cycle is famously cyclical (semiconductor capex up-cycles and
# down-cycles), and a 12-month rolling average/spread computed from a
# window entirely inside one phase of that cycle would misjudge "normal"
# the moment the cycle turns - 5 years of real history costs nothing
# extra to fetch (confirmed live: it is all in the same one Excel file,
# one HTTP request, no per-month pagination) versus the spec's own
# 2-year minimum, so there is no reason to take the smaller number.
_SEAJ_BACKFILL_YEARS = 5

# The live PDF fetcher (_fetch_seaj_billings) already owns the most
# recent months with a real prelim/final qualifier distinction - the
# Excel file has NO qualifier column at all (it is a flat, continuously-
# revised time series, not a versioned release). A fixed "skip the most
# recent N months" margin was tried first and REJECTED after live testing
# 2026-09-29 found a real gap: with N=8, the backfill correctly stopped at
# 2026-01, but this test DB's own already-stored live-fetch data only
# reached back to 2026-03 - January/February 2026 fell into a genuine gap
# between the two, neither backfilled nor live-fetched. A fixed month
# count cannot know in advance exactly which months the live fetcher has
# actually already captured (that depends on real scheduling history, not
# a guessable constant) - checking the real stored URLs directly (see
# below) is the correct fix, not a wider guessed margin.
def _seaj_backfill_candidate_urls(month_label: str) -> list[str]:
    """Both possible urls _fetch_seaj_billings could have already stored
    for ``month_label`` (the live fetcher tags a month "prelim" the first
    time it's reported, then "final" once SEAJ settles it - see
    _SEAJ_MONTH_ROW_RE's own comment) - checked together so this backfill
    correctly skips a month regardless of which qualifier the live fetcher
    happened to store it under.
    """
    key = month_label.replace(" ", "-")
    return [f"seaj-billings://{key}-prelim", f"seaj-billings://{key}-final"]


def fetch_seaj_billings_backfill(sources: list[dict]) -> list[dict]:
    """One-time backfill: fetch SEAJ's own historical Excel archive (real
    monthly billings + YoY%, back to 2005 - confirmed live 2026-09-29) and
    return every real month within the last _SEAJ_BACKFILL_YEARS years
    that isn't ALREADY stored under either the "prelim" or "final" url the
    live fetcher (_fetch_seaj_billings) would use for it - see
    _seaj_backfill_candidate_urls's own comment for why both are checked,
    and this function's own module-level comment for why a fixed
    skip-month-count was tried and rejected in favor of this direct check.

    Deliberately a SEPARATE function from _fetch_seaj_billings (the
    regular incremental fetch), not folded into it or auto-run every
    time - a one-time historical load, same "separate CLI-triggerable
    entry point" shape as news-retrieval's other explicit backfill
    functions (e.g. fetch_customs_export_backfill) - called once (or
    re-run only if the stored history needs rebuilding), not on every
    scheduled poll.

    Same url scheme as _fetch_seaj_billings
    ("seaj-billings://{Month}-{Year}-final") so a month this function
    backfills and a month the live fetcher later reports as "final" refer
    to the same real fact under the same key - articles' own global
    unique-url dedup naturally treats them as one record, not two, even if
    this function is ever re-run after the live fetcher has since covered
    some of the same months.
    """
    if not sources:
        return []

    try:
        resp = httpx.get(_SEAJ_STATISTICS_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        text = resp.content.decode("cp932", errors="replace")
    except Exception as exc:
        logger.warning("[SEAJ_BACKFILL] statistics index fetch failed error=%s", exc)
        return []

    link_match = _SEAJ_XLS_INDEX_LINK_RE.search(text)
    if not link_match:
        logger.warning("[SEAJ_BACKFILL] no semiconductor-equipment Excel link found on index page")
        return []

    xls_url = _SEAJ_BASE_URL + link_match.group("xls_href")

    try:
        xls_resp = httpx.get(xls_url, timeout=30.0, follow_redirects=True)
        xls_resp.raise_for_status()
        import xlrd
        from io import BytesIO

        wb = xlrd.open_workbook(file_contents=xls_resp.content)
        sheet = wb.sheet_by_index(0)
    except Exception as exc:
        logger.warning("[SEAJ_BACKFILL] Excel fetch/parse failed url=%s error=%s", xls_url, exc)
        return []

    now = datetime.now(timezone.utc)
    cutoff_old = now - timedelta(days=365 * _SEAJ_BACKFILL_YEARS)

    articles: list[dict] = []
    current_year: int | None = None
    previous_billings: float | None = None
    # Confirmed live: only the year column (index 0) is populated on a
    # row's own January entry - every other month in that year leaves it
    # blank, so the current year must be carried forward across rows
    # rather than read fresh from each one.
    for row_idx in range(6, sheet.nrows):
        row = sheet.row_values(row_idx)
        if row[0] != "":
            current_year = int(row[0])
        if current_year is None or row[1] == "" or row[4] == "":
            continue  # header/blank/not-yet-published-this-month row

        # The month column is USUALLY a plain xlrd float (e.g. 8.0), but
        # confirmed live 2026-09-29 not always: SEAJ marks a since-revised
        # month with an "R" suffix in several real, inconsistent STRING
        # forms across the file's own history ("8R", "1R", "7(R)",
        # "10（R）" - full-width parens too) - the month itself is still
        # real and correctly sequenced (each one sits exactly where its
        # plain number would between its real neighbors, e.g. "8R" between
        # 7 and 9), "R" only marks that this particular month's own figure
        # was later revised, not a different or extra month. Handled as
        # two genuinely different cases rather than one shared string-
        # digit-stripping regex: an earlier version of this code stripped
        # non-digit characters from str(row[1]) for EVERY row, which
        # silently corrupted the common float case too (str(2.0) is
        # "2.0" - stripping the "." concatenates the digits into "20", a
        # real out-of-range month that crashed date() construction).
        if isinstance(row[1], str):
            month_digits = re.sub(r"\D", "", row[1])
            if not month_digits:
                continue
            month_num = int(month_digits)
        else:
            month_num = int(row[1])
        billings = row[4]
        yoy_fraction = row[5]
        if billings == "" or yoy_fraction == "":
            continue

        # Month-over-month % is not a column in this file, but IS directly
        # derivable from two consecutive rows' own billings values -
        # confirmed live 2026-09-29 this reproduces the live PDF
        # fetcher's own independently-confirmed April 2026 MoM figure
        # (6.2%) exactly. previous_billings is updated from EVERY row
        # seen (even one later excluded by the date cutoffs below), not
        # just included ones, so the first row actually included in the
        # output still gets a real MoM value computed against its true
        # immediately-preceding calendar month.
        mom_pct = (
            round((billings - previous_billings) / previous_billings * 100.0, 1)
            if previous_billings else None
        )
        previous_billings = billings

        month_date = date(current_year, month_num, 1)
        if month_date < cutoff_old.date() or month_date > now.date():
            continue

        month_label = f"{month_date.strftime('%B')} {current_year}"
        pub_date = datetime(current_year, month_num, 1, tzinfo=timezone.utc)
        billings_int = round(billings)
        yoy_pct = round(yoy_fraction * 100.0, 1)

        articles.append({
            "title": f"SEAJ Japan semiconductor equipment billings, {month_label} (final): "
                     f"¥{billings_int:,}M ({yoy_pct}% YoY)",
            "url": f"seaj-billings://{month_label.replace(' ', '-')}-final",
            "published": month_date.isoformat(),
            "source": "SEAJ",
            "summary": None,
            "body": None,
            "_pub_date": pub_date,
            "metadata": {
                "period": month_label,
                "qualifier": "final",
                "billings_3mo_avg_millions_jpy": billings_int,
                "mom_pct": mom_pct,
                "yoy_pct": yoy_pct,
                # The historical archive is a spreadsheet of year, month
                # and billings - it records no publication date for any
                # month, so there is none to read here. The live PDF
                # fetcher does read a real release date from the index
                # page (see _fetch_seaj_billings), which is why a month
                # fetched live carries one and a backfilled month cannot.
                # `published` on this row is the first of the reporting
                # month, a stand-in for ordering, not the date SEAJ
                # actually published the figure - date_confirmed says so
                # explicitly rather than leaving a consumer to infer it.
                "release_date": None,
                "date_confirmed": False,
                "pdf_url": xls_url,
                "source_category": "jp_industry",
            },
        })

    # Drop any month already stored under EITHER qualifier the live
    # fetcher could have used for it - see _seaj_backfill_candidate_urls's
    # own comment for why both "prelim" and "final" are checked, and this
    # function's own module-level comment for the real gap a fixed
    # skip-month-count left (confirmed live 2026-09-29) that this direct
    # check is designed to close. One batched get_already_stored_urls call
    # for every candidate this function built, not a query per month.
    candidate_urls = [
        url for a in articles for url in _seaj_backfill_candidate_urls(a["metadata"]["period"])
    ]
    already_stored = get_already_stored_urls(candidate_urls)
    articles = [
        a for a in articles
        if not (set(_seaj_backfill_candidate_urls(a["metadata"]["period"])) & already_stored)
    ]

    logger.info(
        "[SEAJ_BACKFILL] fetched %d historical month(s) from %s (already-stored months excluded)",
        len(articles), xls_url,
    )
    return articles


# ---------------------------------------------------------------------------
# Japan market signal - MONOist Factory News (monoist.itmedia.co.jp)
# ---------------------------------------------------------------------------
#
# Free, no API key, no per-company scoping server-side - this is a general
# manufacturing-industry news feed (all Japanese manufacturers, not just
# our 20-company universe), filtered CLIENT-SIDE by native-name substring
# match against JAPAN_TICKER_UNIVERSE's own native_name field, same
# approach as Taiwan's GDELT title-filter. Confirmed live 2026-09-28: a
# single page load of the series listing (monoist.itmedia.co.jp/mn/
# series/1464/) already returns ~950 real "工場ニュース" (Factory News)
# articles spanning back several months - enough real history that no
# separate backfill/pagination logic is needed for this source, unlike
# Kabutan's global feed.
#
# Confirmed live this listing mixes two subtitle spellings for the same
# "Factory News" tag - "工場ニュース：" (standard katakana long vowel mark,
# U+30FC) and "工場ニュ―ス：" (a full-width horizontal bar, U+2015, in the
# same visual position) - both are matched, an earlier version checking
# only the first form would have silently missed roughly half of real
# articles seen live.
_MONOIST_SERIES_URL = "https://monoist.itmedia.co.jp/mn/series/1464/"

_MONOIST_ARTICLE_BLOCK_RE = re.compile(
    r'<div class="colBoxIndex[^"]*"[^>]*>(?P<block>.*?)</div>\s*<div class="colBoxClear',
    re.S,
)
_MONOIST_SUBTITLE_RE = re.compile(r'<div class="colBoxSubTitle"><h5>工場ニュ[ー―]ス：</h5></div>')
_MONOIST_TITLE_RE = re.compile(r'<div class="colBoxTitle"><h3><a href="([^"]+)">([^<]+)</a></h3></div>')
_MONOIST_DESC_RE = re.compile(r'<div class="colBoxDescription"><p>([^<]*)</p></div>')
_MONOIST_DATE_RE = re.compile(r'<time class="date" datatime="([\d/]+) ([\d:]+)"')


def _fetch_monoist_article_body(url: str) -> str | None:
    """Fetch one MONOist article's own page and extract its full body text.

    NOT a plain trafilatura.fetch_url() call - confirmed live the article
    pages are Shift-JIS (cp932), same as the series listing page
    (_fetch_monoist_capex's own fetch above), and trafilatura.fetch_url()
    has no way to know that; feeding it the raw undecoded bytes would
    garble every non-ASCII character. Fetch via httpx (matching the
    listing fetch's own pattern), decode as cp932, then hand the decoded
    text straight to trafilatura.extract() rather than trafilatura's own
    fetch path.

    J5 (capacity commitment - see japan_signal_classifier.py in
    signal-detection-agent) needs the real investment yen figure, which
    confirmed live is NOT reliably present in the series listing's own
    title/description snippet (only ~20% of real articles carried any
    figure there) but IS reliably in the full article body when one
    exists (confirmed live for both a real investment announcement and a
    real factory-closure announcement with no figure at all - the body
    text correctly reflects that difference rather than the fetcher
    guessing one).

    Returns None on any failure (network error, non-200, extraction
    failure) - fail-open, same contract as every other trafilatura-backed
    body fetch in this file (_fetch_body etc.) - a missing
    body just means J5's own investment-figure regex has nothing to
    search, not a pipeline failure.

    Confirmed live 2026-09-29 (found via a real J5 false positive: Disco's
    real 2021 wafer-facility article states "投資金額は約140億円" - ~JPY14bn -
    but a naive max-figure regex over the raw extracted body picked up
    "200億円" instead) that trafilatura.extract() does not always stop at
    the real article - some MONOist pages carry a "≫「工場ニュース」の
    バックナンバー" (this series' own back-numbers/related-articles list)
    block AFTER the real article text, and trafilatura's own boundary
    detection sometimes includes it (confirmed live: 5 of 74 real articles
    in the current universe carry this trailing block). That block quotes
    OTHER, unrelated articles' own headlines and blurbs verbatim,
    including THEIR OWN yen figures - which is exactly the kind of text a
    "find the largest 億円 figure in the body" rule (J5's own
    _extract_capex_investment_jpy) will silently misread as this article's
    real investment amount. Trimmed here, at the source, rather than left
    for every downstream consumer of this body text to work around
    separately.
    """
    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        text = resp.content.decode("cp932", errors="replace")
        # Held even though this fetcher's own loop is sequential: the lock
        # guards a process-global C library (see _EXTRACT_LOCK), so the
        # invariant worth keeping is "every trafilatura.extract() call in
        # this file holds it", not "the ones that look concurrent today".
        with _EXTRACT_LOCK:
            body = trafilatura.extract(text, include_comments=False, include_tables=False)
        if body:
            body = body.split("バックナンバー")[0].rstrip()
        return body or None
    except Exception:
        logger.warning("[MONOIST] article body fetch failed url=%r", url, exc_info=True)
        return None


def _match_japan_company(
    companies: list[dict[str, Any]], text: str,
) -> dict[str, Any] | None:
    """Find the first company whose native_name OR any of its aliases
    appears in text - used by both monoist_capex and press_jp, the two
    source_types that filter a general news feed client-side rather than
    querying by ticker/company.

    Aliases matter because real press articles frequently use an
    abbreviated name a full native_name substring match would miss
    entirely: Resonac Holdings (レゾナック・ホールディングス) matched 0
    real MONOist articles by its full name vs. 5 by レゾナック, and
    Renesas Electronics (ルネサスエレクトロニクス) matched 0 vs. 7.

    Every alias is tried, not just the first. Japanese companies carry at
    most one today, so a single value would behave identically - but
    Korea needs several per company (a group prefix plus variants), and
    reading only one would leave the rest silently unmatched.

    Both sources of companies use the same `aliases` key: the live
    research-universe read and the seeded config fallback.
    """
    for c in companies:
        if c["native_name"] in text:
            return c
        if any(a and a in text for a in (c.get("aliases") or [])):
            return c
    return None


def _fetch_monoist_capex(sources: list[dict]) -> list[dict]:
    """Fetch MONOist's Factory News series listing, keep only articles
    mentioning a company in JAPAN_TICKER_UNIVERSE (matched by native_name
    substring, same as Taiwan's GDELT title-filter approach - this feed has
    no per-company query capability at all, unlike GDELT's own query
    param, so the filter is applied after a single full-listing fetch
    rather than one query per company).

    Also fetches each surviving article's own full body text (see
    _fetch_monoist_article_body) - one extra request per matched article,
    not per listing row, since the client-side company filter above
    already runs first. Confirmed live this stays cheap in practice: a
    31-day window matched ~15-20 articles, not the full ~950-row listing.
    """
    companies = _japan_companies(sources)
    if not companies:
        return []

    try:
        resp = httpx.get(_MONOIST_SERIES_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        # Confirmed live: this page is Shift-JIS (cp932), not UTF-8.
        text = resp.content.decode("cp932", errors="replace")
    except Exception as exc:
        logger.warning("[MONOIST] series listing fetch failed error=%s", exc)
        return []

    articles: list[dict] = []
    for block_match in _MONOIST_ARTICLE_BLOCK_RE.finditer(text):
        content = block_match.group("block")
        if not _MONOIST_SUBTITLE_RE.search(content):
            continue
        title_match = _MONOIST_TITLE_RE.search(content)
        date_match = _MONOIST_DATE_RE.search(content)
        if not title_match or not date_match:
            continue
        desc_match = _MONOIST_DESC_RE.search(content)

        title = title_match.group(2)
        description = desc_match.group(1) if desc_match else ""
        full_text = title + description

        matched_company = _match_japan_company(companies, full_text)
        if not matched_company:
            continue

        try:
            # MONOist timestamps its listing in Japan time with no
            # offset, the same as EDINET - stamping them UTC without
            # converting put every article 9 hours early, so a 16:00
            # JST publication read as 01:00 the next morning in Tokyo.
            pub_date = datetime.strptime(
                f"{date_match.group(1)} {date_match.group(2)}", "%Y/%m/%d %H:%M",
            ).replace(tzinfo=_JST).astimezone(timezone.utc)
        except ValueError:
            pub_date = None

        article_url = title_match.group(1)
        body = _fetch_monoist_article_body(article_url)

        articles.append({
            "title": f"{matched_company['company']} ({matched_company['code']}): {title}",
            "url": article_url,
            "published": f"{date_match.group(1)} {date_match.group(2)}",
            "source": "MONOist",
            "summary": description or None,
            "body": body,
            "_pub_date": pub_date,
            "metadata": {
                "code": matched_company["code"],
                "company": matched_company["company"],
                "source_category": "jp_capex",
            },
        })
    return articles


# ---------------------------------------------------------------------------
# Japan market signal - press (Nikkei-equivalent free wire coverage)
# ---------------------------------------------------------------------------
#
# The design spec calls for Nikkei-style scoop coverage but Nikkei itself
# is paywalled by design (headline+timestamp only would still require
# scraping a paywalled site's teaser, not attempted here). Free wire-
# service substitutes were checked live 2026-09-28. Two sub-sources are
# usable, merged into one source_type (same "multiple feeds, one
# source_type" pattern as edinet_filing's shareholding+buyback modes):
#   - Reuters Japan (jp.reuters.com): blocked outright - every path tried
#     (article pages, RSS guesses) returned 401 behind an active anti-bot
#     challenge (DataDome, confirmed via the "dd" cookie/script signature
#     in the block-page response) - no path around this without a
#     headless browser, out of scope here.
#   - Kyodo News English (english.kyodonews.net): reachable, but confirmed
#     live it has NO business/economy/tech category at all - its full nav
#     is limited to Japan/World/Sports/Arts/Feature/Travel-Tourism/Sumo/
#     Asian Games/Podcast. Not usable for this signal.
#   - Jiji Press (jiji.com/jc/list?g=eco): works - a real, free,
#     unauthenticated economy-category listing, confirmed live to return
#     ~50 real headline+timestamp entries per page load. This is a BROAD
#     general-economy feed, not semiconductor-specific - confirmed live it
#     can go an entire page with zero matches against
#     JAPAN_TICKER_UNIVERSE (same as kabutan_buyback's legitimate
#     zero-match days).
#   - newswitch.jp (ニュースイッチ, Nikkan Kogyo Shimbun's free consumer
#     site): confirmed live 2026-09-28 to have a dedicated 半導体
#     (semiconductor) keyword-tag page (newswitch.jp/keyword/detail/573),
#     free, static HTML, semiconductor-SPECIFIC by construction rather
#     than by luck - genuinely more targeted than Jiji's broad economy
#     feed. One real match confirmed live on this exact test (Resonac's
#     12-inch SiC substrate development). The listing page itself has NO
#     timestamp, though (confirmed live) - only individual article pages
#     carry one (a "published" date plus, separately, the original Nikkan
#     Kogyo print-edition date) - so this is a two-step fetch, same
#     "cheap discovery, expensive fetch only for matches" shape as
#     irbank_financials: walk the listing cheaply, only fetch an
#     individual article page's date for entries that already match our
#     company universe by title.
#
# Per the design's own rule (headline + timestamp only, never full article
# text - this is a copyright/scope boundary, not a technical one): body is
# deliberately always None for both sub-sources, matching Nikkei's own
# treatment in every other Japan source_type's design.
# No official rate limit published for either sub-source - same
# conservative, undocumented-limit posture as every other free scrape
# source in this file (IRBANK, Kabutan, MONOist, SEAJ).
_newswitch_rate_lock = threading.Lock()
_newswitch_last_call = [0.0]
_NEWSWITCH_MIN_INTERVAL = 1.0


def _newswitch_rate_sleep() -> None:
    with _newswitch_rate_lock:
        elapsed = time.monotonic() - _newswitch_last_call[0]
        if elapsed < _NEWSWITCH_MIN_INTERVAL:
            time.sleep(_NEWSWITCH_MIN_INTERVAL - elapsed)
        _newswitch_last_call[0] = time.monotonic()


_JIJI_ECO_URL = "https://www.jiji.com/jc/list?g=eco"
_JIJI_ARTICLE_RE = re.compile(
    r'<li><a href="(?P<href>/jc/article\?k=[^"]+)"><p>(?P<title>[^<]+)</p><span>\((?P<time>[\d/]+ [\d:]+)\)</span>',
)

_NEWSWITCH_SEMICONDUCTOR_URL = "https://newswitch.jp/keyword/detail/573"
_NEWSWITCH_ARTICLE_RE = re.compile(
    r'<div class="title">\s*<a href="(?P<url>https://newswitch\.jp/p/\d+)">\s*(?P<title>[^<]+?)\s*</a>',
)
# Confirmed live on a real article page: `<span class="published">2026年
# 09月28日</span>` - the newswitch.jp posting date. A separate `<div
# class="source">日刊工業新聞 2026年09月25日</div>` line also carries the
# original Nikkan Kogyo print-edition date (often a few days earlier) -
# not used here since the posting date is what determines when this
# pipeline could plausibly have seen it.
_NEWSWITCH_DATE_RE = re.compile(r'<span class="published">(\d{4})年(\d{1,2})月(\d{1,2})日</span>')


def _fetch_press_jp_jiji(companies: list[dict[str, str]]) -> list[dict]:
    try:
        resp = httpx.get(_JIJI_ECO_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        text = resp.text
    except Exception as exc:
        logger.warning("[PRESS_JP] Jiji economy listing fetch failed error=%s", exc)
        return []

    articles: list[dict] = []
    for match in _JIJI_ARTICLE_RE.finditer(text):
        title = match.group("title")
        matched_company = _match_japan_company(companies, title)
        if not matched_company:
            continue

        # time e.g. "09/28 19:02" - no year in the source; the listing is
        # always current-day/recent, so the current UTC year is assumed
        # (same convention news-retrieval already uses elsewhere for
        # sources that omit a year in their own date format).
        try:
            month_day, hm = match.group("time").split(" ")
            month, day = month_day.split("/")
            hour, minute = hm.split(":")
            # Jiji's listing shows Japan time with no offset, same as
            # EDINET and MONOist. The year is absent too, so the
            # current one is assumed - the listing only ever carries
            # recent items.
            now = datetime.now(_JST)
            pub_date = datetime(
                now.year, int(month), int(day), int(hour), int(minute), tzinfo=_JST,
            ).astimezone(timezone.utc)
        except (ValueError, IndexError):
            pub_date = None

        articles.append({
            "title": f"{matched_company['company']} ({matched_company['code']}): {title}",
            "url": f"https://www.jiji.com{match.group('href')}",
            "published": match.group("time"),
            "source": "Jiji Press",
            "summary": None,
            "body": None,
            "_pub_date": pub_date,
            "metadata": {
                "code": matched_company["code"],
                "company": matched_company["company"],
                "source_category": "jp_press",
                "unconfirmed": True,
            },
        })
    return articles


def _fetch_press_jp_newswitch(companies: list[dict[str, str]]) -> list[dict]:
    try:
        resp = httpx.get(_NEWSWITCH_SEMICONDUCTOR_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        text = resp.text
    except Exception as exc:
        logger.warning("[PRESS_JP] newswitch.jp semiconductor listing fetch failed error=%s", exc)
        return []

    articles: list[dict] = []
    for match in _NEWSWITCH_ARTICLE_RE.finditer(text):
        title = match.group("title")
        matched_company = _match_japan_company(companies, title)
        if not matched_company:
            continue

        _newswitch_rate_sleep()
        try:
            article_resp = httpx.get(match.group("url"), timeout=30.0, follow_redirects=True)
            article_resp.raise_for_status()
            date_match = _NEWSWITCH_DATE_RE.search(article_resp.text)
        except Exception as exc:
            logger.warning("[PRESS_JP] newswitch.jp article date fetch failed url=%s error=%s", match.group("url"), exc)
            date_match = None

        pub_date = None
        published_str = None
        if date_match:
            pub_date = datetime(
                int(date_match.group(1)), int(date_match.group(2)), int(date_match.group(3)),
                tzinfo=timezone.utc,
            )
            published_str = f"{date_match.group(1)}-{date_match.group(2)}-{date_match.group(3)}"

        articles.append({
            "title": f"{matched_company['company']} ({matched_company['code']}): {title}",
            "url": match.group("url"),
            "published": published_str,
            "source": "Newswitch",
            "summary": None,
            "body": None,
            "_pub_date": pub_date,
            "metadata": {
                "code": matched_company["code"],
                "company": matched_company["company"],
                "source_category": "jp_press",
                "unconfirmed": True,
            },
        })
    return articles


def _fetch_press_jp(sources: list[dict]) -> list[dict]:
    """Fetch Nikkei-equivalent free wire coverage from all usable
    sub-sources (Jiji Press economy feed + newswitch.jp semiconductor
    feed), keeping only articles mentioning a company in
    JAPAN_TICKER_UNIVERSE (matched by native_name or short_name - see
    _match_japan_company).

    Headline + timestamp only, deliberately - no article body is fetched
    or stored, matching the design's own treatment of Nikkei (and, by
    extension, every free wire substitute for it): the free-to-view
    surface is the headline/timestamp, not the article text itself.
    """
    companies = _japan_companies(sources)
    if not companies:
        return []

    return _fetch_press_jp_jiji(companies) + _fetch_press_jp_newswitch(companies)


# ---------------------------------------------------------------------------
# Japan market signal - English-coverage mirror check (DuckDuckGo)
# ---------------------------------------------------------------------------
#
# The design spec's Step 5 explicitly asks to "mirror every search in
# English recording whether anything was found and when" - "the English
# mirror makes the timing claim testable rather than assumed." Not built
# in the initial pass (out-of-scope decision at the time, since
# signal-detection-agent's web_search.py already does a similar per-entity
# English search for a DIFFERENT purpose - classification-time context for
# already-scored signals). Added here as its own source_type once it was
# confirmed that job is genuinely distinct: this runs at FETCH time, once
# per company, independent of any specific already-found Japanese article
# (same shape as Korea's KOREA_GDELT_ENGLISH_SOURCE - an independent
# per-company search, not a per-article cross-reference) - not a
# duplication of web_search.py's job.
#
# DuckDuckGo (via the free `ddgs` package, no API key) rather than GDELT -
# explicit choice for this source. Confirmed live 2026-09-28 real,
# relevant results: e.g. a real Reuters article ("Advantest raises
# full-year operating profit forecast by 24% on strong AI demand", dated
# 2025-07-29) for the same company/event type irbank_financials
# separately confirms via real forecast-revision filings - exactly the
# kind of timing comparison this source exists to make possible
# downstream (signal-detection-agent's job, not computed here).
_PRESS_JP_ENGLISH_CHECK_MAX_RESULTS = 5

# DuckDuckGo rate-limits aggressively on burst traffic (confirmed by
# signal-detection-agent's own web_search.py, which applies the same 1.5s
# delay between consecutive queries for this exact reason).
_ddg_rate_lock = threading.Lock()
_ddg_last_call = [0.0]
_DDG_MIN_INTERVAL = 1.5


def _ddg_rate_sleep() -> None:
    with _ddg_rate_lock:
        elapsed = time.monotonic() - _ddg_last_call[0]
        if elapsed < _DDG_MIN_INTERVAL:
            time.sleep(_DDG_MIN_INTERVAL - elapsed)
        _ddg_last_call[0] = time.monotonic()


def _ddg_timelimit_for_days_back(days_back: int) -> str:
    """Map a days_back window onto ddgs's own timelimit buckets (d/w/m/y).

    ddgs.news() has no arbitrary-day-count parameter - confirmed live
    (2026-09-28) by reading ddgs/ddgs.py's _search_sync signature, which
    forwards `timelimit` straight through to each backend engine's own
    search() call as one of exactly these four values (or a custom date
    range string, unused here). Rounds UP to the nearest bucket that still
    covers the requested window, so results are never scoped narrower than
    days_back asks for - the client-side cutoff below then trims anything
    the chosen bucket over-includes.
    """
    if days_back <= 1:
        return "d"
    if days_back <= 7:
        return "w"
    if days_back <= 31:
        return "m"
    return "y"


def _fetch_press_jp_english_check(companies: list[dict[str, str]], days_back: int) -> list[dict]:
    """For each company, search DuckDuckGo News once (English) and store
    whatever is found - headline + timestamp only, matching every other
    press source_type's own body=None convention.

    Returns an empty list (not a partial one) for the whole fetch if the
    `ddgs` package is unavailable - confirmed this fails open the same way
    a missing API key does for other optional sources elsewhere in this
    file, rather than raising and aborting sibling source_types in the
    same pipeline run.

    Recency is enforced twice, confirmed live (2026-09-28): ddgs's own
    `timelimit` kwarg scopes the search server-side (confirmed against the
    real DuckDuckGo News backend - timelimit="w" returned only results
    dated within the prior 7 days, vs. an unscoped call spanning further
    back), then the same client-side days_back cutoff every other fetcher
    in this file applies is used here too, since timelimit only offers
    coarse d/w/m/y buckets rather than an exact day count.
    """
    try:
        from ddgs import DDGS
    except ImportError:
        logger.warning("[PRESS_JP_ENGLISH_CHECK] ddgs package not installed - skipping")
        return []

    timelimit = _ddg_timelimit_for_days_back(days_back)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    articles: list[dict] = []
    for c in companies:
        _ddg_rate_sleep()
        query = c.get("search_query") or c["company"]
        try:
            with DDGS(timeout=10) as ddgs:
                results = list(
                    ddgs.news(
                        query,
                        max_results=_PRESS_JP_ENGLISH_CHECK_MAX_RESULTS,
                        timelimit=timelimit,
                    )
                )
        except Exception as exc:
            logger.warning("[PRESS_JP_ENGLISH_CHECK] search failed company=%s error=%s", c["company"], exc)
            continue

        for r in results:
            url = r.get("url")
            if not url:
                continue
            date_str = r.get("date")
            try:
                pub_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")) if date_str else None
            except ValueError:
                pub_date = None

            if pub_date and pub_date < cutoff:
                continue

            articles.append({
                "title": f"{c['company']} ({c['code']}) [English coverage check]: {r.get('title', '')}",
                "url": url,
                # The PARSED datetime, not the raw string. ddgs does not
                # always return ISO: some results carry display text
                # ("Opinion7 days ago"), which reached Postgres as a
                # timestamp and raised InvalidDatetimeFormat, aborting
                # the whole insert batch. _pub_date below was already
                # parsed-or-None and is the value that survives into
                # `published` via _fetch_articles anyway, so the raw
                # string was only ever a second, unvalidated path to the
                # same column.
                "published": pub_date,
                "source": r.get("url", "").split("/")[2] if url else "Unknown",
                # The search result's own snippet. Previously discarded,
                # which left this source_type headline-only and made it
                # useless to any downstream rule needing a figure or a
                # named customer - confirmed against China's C4
                # (accelerator milestones), which found no usable input
                # anywhere until this was kept. The snippets carry real
                # substance: "Chinese chipmaker Cambricon Technologies,
                # which is seen as a potential domestic alternative..."
                #
                # Stored as `summary`, not `body`: it IS a lead, one or
                # two sentences written by the publisher, which is what
                # summary means everywhere else in this service. The
                # article page itself is still not fetched, so `body`
                # stays None and the source remains a coverage CHECK
                # rather than a press fetcher.
                "summary": (r.get("body") or "").strip() or None,
                "body": None,
                "_pub_date": pub_date,
                "metadata": {
                    "code": c["code"],
                    "company": c["company"],
                    "source_category": "jp_english_coverage_check",
                    "unconfirmed": True,
                },
            })
    return articles


def _fetch_press_jp_english_check_source(sources: list[dict], days_back: int) -> list[dict]:
    companies = _japan_companies(sources)
    if not companies:
        return []
    return _fetch_press_jp_english_check(companies, days_back)


# ---------------------------------------------------------------------------
# China market signal
# ---------------------------------------------------------------------------
#
# Eight source_types. Two more were scoped and dropped after the access
# probe (2026-10-05), rather than built and left failing:
#   gacc_trade  - customs.gov.cn answers 412 with a JSluid cookie challenge
#                 and obfuscated JS. Full browser headers do not help; it
#                 needs a JS-executing client, which this service has no
#                 dependency for. C6 (trade data) is therefore not covered.
#   nbs_output  - stats.gov.cn is blocked at the NETWORK layer from here
#                 ("The URL has been blocked as per the instructions of the
#                 Competent Government Authority"), not by the site itself.
#                 May work from ECS; untested there.
#
# China time. Every source here timestamps in Beijing time (UTC+8) with no
# offset in the markup, the same trap EDINET and MONOist set for Japan -
# stamping them UTC directly would put every article 8 hours early.
_CST = timezone(timedelta(hours=8))

# Government and press sites here are slow and occasionally intermittent
# from outside the mainland (MOFCOM's own index took 19.5s on a cold first
# hit during the probe, then under 1s). Generous timeout plus a retry,
# rather than treating one slow response as an outage.
_CHINA_TIMEOUT = 40.0
_CHINA_MAX_ATTEMPTS = 3
_CHINA_RETRY_BACKOFF_SECS = 2.0

# english.mofcom.gov.cn refuses a bare httpx request outright
# (RemoteProtocolError, server disconnects without responding) but serves
# normally with a full browser header set. The Chinese site does not need
# this. Sent on every China request since it costs nothing and removes a
# whole class of intermittent failure.
_CHINA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
    ),
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}


# Filing bodies are capped before storage. A Chinese annual report runs to
# hundreds of pages; the classifier needs the announcement's substance, not
# its appendices, and an unbounded body would bloat every row that carries
# one. 20k characters covers a full announcement comfortably - the real
# samples checked ran 2.5-6k.
_CHINA_BODY_MAX_CHARS = 20000
# Only the first pages are read. Page 1 carries the title, the issuer and
# the substance of an announcement; later pages are signatures, appendices
# and audit boilerplate.
_CHINA_PDF_MAX_PAGES = 5

# RESULTS announcements are the exception, and the exception is measured.
# Tencent's "ANNOUNCEMENT OF THE RESULTS FOR THE THREE AND SIX MONTHS
# ENDED..." is 50 pages / 82,418 characters, and its capital-expenditure
# line sits at character 19,113 - inside page ~12, far past the 5-page
# cut that stored only 7,153 characters of it. Downstream this made
# Tencent and Baidu look as though they never report capex at all
# (0 mentions across 54 stored bodies), when in fact the figure was
# simply never fetched.
#
# Raised only for this filing type, not globally: a results announcement
# front-loads its financial tables, while a prospectus or bond programme
# of the same length is appendix the whole way down. The character cap
# above still applies, so this buys depth, not unbounded size.
_CHINA_PDF_MAX_PAGES_RESULTS = 25
# The \b applies to the ENGLISH alternatives only. Python's \b sits
# between a word and a non-word character, and a CJK character counts
# as a word character - so "\b半年度报告" never matches inside
# "中芯国际2026年半年度报告", because the preceding 年 is also a word
# character. Confirmed live; the Chinese terms are matched without an
# anchor, which is correct since they are already specific phrases.
_CHINA_RESULTS_TITLE_RE = re.compile(
    r"(?i)(?:\b(?:results|interim report|annual report)\b"
    r"|业绩|中期报告|年度报告|季度报告|半年度报告)")


def _fetch_china_filing_body(url: str, title: str = "") -> str | None:
    """Body text of one filing document, PDF or HTML.

    Returns None on any failure - a filing without a readable body is
    still a real filing, and its title, date and company are already
    stored. Fail-open, same convention as every other fetcher here.

    Image-only PDFs return None rather than an empty string: a null body
    reads as "not extracted", while an empty one would read as "this
    filing says nothing", and the two must not be conflated.

    ``title`` decides how deep into the PDF to read - a results
    announcement gets a larger page budget than an ordinary filing. See
    _CHINA_PDF_MAX_PAGES_RESULTS for the measurement behind that.
    """
    try:
        resp = httpx.get(url, headers=_CHINA_HEADERS,
                         timeout=_CHINA_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        logger.info("[CHINA] filing body fetch failed url=%s: %s", url, exc)
        return None

    ctype = (resp.headers.get("content-type") or "").lower()
    if "pdf" in ctype or url.lower().endswith(".pdf"):
        max_pages = (_CHINA_PDF_MAX_PAGES_RESULTS
                     if _CHINA_RESULTS_TITLE_RE.search(title or "")
                     else _CHINA_PDF_MAX_PAGES)
        try:
            from io import BytesIO

            import pdfplumber
            with pdfplumber.open(BytesIO(resp.content)) as pdf:
                parts = [
                    (page.extract_text() or "")
                    for page in pdf.pages[:max_pages]
                ]
            text = re.sub(r"\s+", " ", " ".join(parts)).strip()
        except Exception as exc:
            logger.info("[CHINA] pdf parse failed url=%s: %s", url, exc)
            return None
    else:
        text = _china_strip_html(resp.text)

    if not text:
        return None
    # NUL bytes reach here from PDF extraction and Postgres cannot store
    # them in a text column - the driver raises "A string literal cannot
    # contain NUL (0x00) characters" and the whole batch insert aborts,
    # losing every filing in the run rather than the one bad document.
    # Confirmed live: a four-year HKEX backfill fetched ~2,000 filings
    # and stored none for this reason.
    text = text.replace("\x00", "")
    return text[:_CHINA_BODY_MAX_CHARS]


# Per-outlet article containers, used when trafilatura cannot find the
# body on its own. Confirmed live 2026-10-05: trafilatura returns a real
# article for ITHome and EEFocus but falls back to the site navigation
# menu on Jiemian's newsflash pages, whose body is a single short <p>
# inside class="article-content" - too short for trafilatura's own
# heuristics to prefer it over the much longer nav block.
_CHINA_ARTICLE_CONTAINERS: list[tuple[str, re.Pattern[str]]] = [
    ("jiemian.com", re.compile(
        r'class="article-content"[^>]*>(.*?)</div>', re.S)),
]

# Outlets whose article pages are gated: the LISTING is free and carries
# a real dated headline, but the article body behind it is not served to
# an anonymous reader. Confirmed live for ijiwei, whose /n/<id> pages
# return a 1.9KB stub reading 该文章未发布 ("this article is not
# published", error 70002) regardless of the item.
#
# Their headlines are still worth collecting - 盛美上海在手订单突破170亿元
# 同比大增88.2% is a complete C2 signal on its own - but the stub must
# never be stored as a body, or a classifier would read an error page as
# the article's content.
_CHINA_GATED_ARTICLE_HOSTS = ("ijiwei.com",)
_CHINA_GATE_MARKERS = ("该文章未发布", "错误码", "请登录", "登录后查看")


def _fetch_china_article_body(url: str) -> str | None:
    """Body text of one Chinese press article.

    Uses trafilatura rather than _china_strip_html: a news page is mostly
    navigation, related-article lists and comment furniture, and tag
    stripping would return all of it. Government announcement pages are
    the opposite case - short, mostly the notice itself - which is why
    they keep the simpler path.

    Where trafilatura fails, a per-outlet container pattern takes over
    rather than the article being dropped - see
    _CHINA_ARTICLE_CONTAINERS.
    """
    # Not even requested for an outlet known to gate its articles - the
    # fetch would only ever return the "not published" stub.
    if any(host in url for host in _CHINA_GATED_ARTICLE_HOSTS):
        return None
    try:
        resp = httpx.get(url, headers=_CHINA_HEADERS,
                         timeout=_CHINA_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
    except Exception as exc:
        logger.info("[CHINA] article body fetch failed url=%s: %s", url, exc)
        return None
    try:
        text = trafilatura.extract(
            resp.text, config=_get_trafilatura_config()) or ""
    except Exception as exc:
        logger.info("[CHINA] article extract failed url=%s: %s", url, exc)
        return None
    text = re.sub(r"\s+", " ", text).strip()

    # A login wall or "not published" stub from any outlet, not just the
    # known-gated ones above - checked on the extracted text because the
    # stub is short enough that trafilatura returns it as the article.
    if text and any(m in text[:200] for m in _CHINA_GATE_MARKERS):
        logger.info("[CHINA] article body is a gate stub, dropping url=%s",
                    url)
        return None

    # Where trafilatura cannot find the article body it falls back to
    # whatever block of text it judges longest, which on these sites is
    # the site-wide navigation menu - confirmed live on one real Jiemian
    # article, which returned "首页 科技 金融 证券 地产 汽车 健康 ..." as its
    # body. A nav menu is many short space-separated labels with no
    # sentence punctuation, so it is recognisable.
    head = text[:120]
    looks_like_nav = bool(
        text and "首页" in head and not re.search(r"[。！？，]", head))

    if not text or looks_like_nav:
        for host, pattern in _CHINA_ARTICLE_CONTAINERS:
            if host not in url:
                continue
            m = pattern.search(resp.text)
            if not m:
                continue
            fallback = _china_strip_html(m.group(1))
            if fallback:
                logger.info("[CHINA] used %s container fallback url=%s",
                            host, url)
                return fallback[:_CHINA_BODY_MAX_CHARS]
        if looks_like_nav:
            logger.info("[CHINA] article body looks like navigation and no "
                        "container matched, dropping url=%s", url)
        return None
    return text[:_CHINA_BODY_MAX_CHARS]


def _china_companies(sources: list[dict]) -> list[dict[str, Any]]:
    """The tracked Chinese companies for one fetcher, from seeded config.

    Deliberately NOT read live from research-universe, unlike Japan.
    The catalogue cannot yet express two facts this domain needs:

      - Alibaba and Tencent are Chinese companies filed under
        `country='United States'`, because that is what the SEC EDGAR
        and Alpha Vantage pollers select on and moving them would drop
        real data (38 stored Alibaba 6-K filings, and both tickers'
        market data). A `country='China'` read cannot see them, and
        they are two of the three C5 names.
      - A dual-listed issuer has two codes (SMIC: 688981 and 00981)
        and the catalogue holds one, so an HK code currently survives
        only as an alias.

    Both are schema questions - a `listing_country`, or a listings
    table - deferred rather than worked around here. Until then this
    reads the seeded config, which carries all 20 companies with the
    fields each fetcher needs.

    The exchange-internal identifiers those fetchers also need
    (cninfo's org_id, HKEX's stockId) are NOT part of this config: they
    are resolved per run by china_code.py, so a company added here
    needs only its code.
    """
    companies: list[dict[str, Any]] = []
    for s_ in sources:
        companies.extend((s_.get("config") or {}).get("companies", []))
    return companies


def _china_get(url: str) -> str | None:
    """GET one China URL, retrying transient failures.

    Returns the decoded body, or None after ``_CHINA_MAX_ATTEMPTS`` -
    fail-open, the same convention every other fetcher in this module
    uses. A failed source never fails the run.
    """
    last_exc: Exception | None = None
    for attempt in range(1, _CHINA_MAX_ATTEMPTS + 1):
        try:
            resp = httpx.get(url, headers=_CHINA_HEADERS,
                             timeout=_CHINA_TIMEOUT, follow_redirects=True)
            resp.raise_for_status()
            return resp.text
        except Exception as exc:
            last_exc = exc
            if attempt < _CHINA_MAX_ATTEMPTS:
                time.sleep(_CHINA_RETRY_BACKOFF_SECS * attempt)
    logger.warning("[CHINA] fetch failed url=%s after %d attempts: %s",
                   url, _CHINA_MAX_ATTEMPTS, last_exc)
    return None


def _china_day(year: int, month: int, day: int) -> datetime:
    """A date-only Chinese publication date, as UTC.

    Stamped at 12:00 Beijing rather than 00:00. A date with no time is
    all these sources give, and converting a bare midnight to UTC moves
    it back 8 hours into the PREVIOUS day - confirmed in testing, where
    a filing dated 2026-09-18 came back as 2026-09-17T16:00Z. Midday is
    the only choice that survives the conversion in either direction.
    """
    return datetime(year, month, day, 12, 0,
                    tzinfo=_CST).astimezone(timezone.utc)


# _HTML_TAG_RE is defined at module top and reused here.
_SCRIPT_STYLE_RE = re.compile(
    r"<(script|style)\b.*?</\1>", re.S | re.I)


def _china_strip_html(raw: str) -> str:
    """Plain text from a China government article page.

    trafilatura is used elsewhere in this module for press articles, but
    these CMS pages are simple enough (confirmed live against a real
    MOFCOM announcement: 989 characters of clean text) that stripping
    tags directly avoids trafilatura's own failure mode of discarding a
    short official notice as boilerplate.
    """
    text = _SCRIPT_STYLE_RE.sub(" ", raw)
    text = _HTML_TAG_RE.sub(" ", text)
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


# A government announcement's own metadata, printed in the article body
# rather than the listing. Confirmed live against a real MOFCOM 公告:
# "【发布单位】安全与管制局 【发布文号】商务部公告2026年第40号
#  【发文日期】2026年09月22日". The document number is what makes an
# announcement citable and is how a later amendment refers back to it, so
# it is extracted rather than left in the prose.
_CN_DOC_NO_RE = re.compile(r"【发布文号】\s*([^\s【]{4,60})")
_CN_ISSUER_RE = re.compile(r"【发布单位】\s*([^\s【]{2,40})")
_CN_DOC_DATE_RE = re.compile(r"【发文日期】\s*(\d{4})年(\d{1,2})月(\d{1,2})日")
# SAMR (and the same CMS family) prints the publication date in the
# article body as 发布时间：2026-07-27, and puts NO date in the listing at
# all - confirmed live, where all 59 SAMR rows came back with a null
# published date. An undated row can never be excluded by days_back, so
# it would be re-fetched on every run forever.
_CN_PUBLISH_TIME_RE = re.compile(
    r"发布(?:时间|日期)[：:]\s*(\d{4})[-/年](\d{1,2})[-/月](\d{1,2})")
# MOFCOM's 政策解读 pages carry no labelled date at all - the header runs
# "来源：商务部新闻办公室 类型：原创 分类：新闻 2026-09-28 09:00" with the
# date bare. Confirmed live: 9 of 48 rows in a clean fetch had a null
# published for exactly this reason, which would have excluded them
# from every days_back window forever. Anchored to the masthead fields
# so a date appearing later in prose is not mistaken for the
# publication date.
_CN_HEADER_DATE_RE = re.compile(
    r"(?:来源|类型|分类)[：:][^0-9]{0,40}?"
    r"(20\d{2})-(\d{1,2})-(\d{1,2})")
# The listing's own date, next to each row.
_CN_LISTING_DATE_RE = re.compile(r"(20\d{2})-(\d{1,2})-(\d{1,2})")
# Article links on a government listing. Three distinct shapes, all
# confirmed live 2026-10-05 - these are different CMSs, not one:
#   MOFCOM, SAMR   .../art/2026/art_<32 hex>.html
#   CAC            .../2026-09/30/c_<digits>.htm
#   Xinhua English .../20261005/<32 hex>/c.html
# Quotes are matched as a backreference, not hardcoded to ": Xinhua
# writes href='...' with SINGLE quotes while the government CMSs use
# double, and a double-only pattern silently matched nothing on Xinhua
# but its two footer links.
_CN_ARTICLE_HREF_RE = re.compile(
    r'<a\s[^>]*href=(?P<q>["\'])(?P<href>(?:(?!(?P=q))[^#])*?(?:'
    r'art_[0-9a-f]{32}\.html?'
    r'|/20\d{2}-\d{2}/\d{2}/c_\d+\.html?'
    r'|/20\d{6}/[0-9a-f]{16,40}/c\.html'
    r'))(?P=q)[^>]*>\s*(?P<title>[^<]{6,160}?)\s*</a>',
    re.I)
# A date in the URL itself, which all three shapes above carry. More
# reliable than the markup around the anchor: CAC and Xinhua place no
# date next to the link at all, and MOFCOM's is only sometimes within
# reach. Groups: (YYYY, MM, DD) for CAC, (YYYYMMDD) for Xinhua.
_CN_HREF_DATE_RE = re.compile(
    r"/(20\d{2})-(\d{2})/(\d{2})/|/(20\d{2})(\d{2})(\d{2})/")


def _china_policy_rows(listing_html: str, base_url: str) -> list[dict]:
    """Announcement rows from one government listing page.

    Each row is {href, title, listing_date or None}. The date is taken
    from the markup FOLLOWING the anchor, which is where these CMS
    templates put it - confirmed live on MOFCOM (8 of 16 article anchors
    carried a date within 220 characters). A row whose date cannot be
    found is still returned: the article page carries its own 【发文日期】,
    which is more authoritative anyway.
    """
    rows: list[dict] = []
    seen: set[str] = set()
    for m in _CN_ARTICLE_HREF_RE.finditer(listing_html):
        href, title = m.group("href"), html.unescape(m.group("title").strip())
        # Policy-interpretation stubs ([政策解读]) link to a commentary
        # page about an announcement, not the announcement - they carry no
        # measure of their own and would double-count the real notice.
        if not title or title.startswith("[") or href in seen:
            continue
        seen.add(href)
        # The URL's own date first - CAC and Xinhua carry no date beside
        # the anchor at all, and a date in the path cannot drift the way
        # a nearby-markup match can.
        listing_date = None
        hm = _CN_HREF_DATE_RE.search(href)
        if hm:
            parts = [g for g in hm.groups() if g]
            try:
                listing_date = _china_day(
                    int(parts[0]), int(parts[1]), int(parts[2]))
            except (ValueError, IndexError):
                listing_date = None
        if listing_date is None:
            tail = listing_html[m.end():m.end() + 260]
            dm = _CN_LISTING_DATE_RE.search(tail)
            if dm:
                try:
                    listing_date = _china_day(
                        int(dm.group(1)), int(dm.group(2)), int(dm.group(3)))
                except ValueError:
                    listing_date = None
        if href.startswith("//"):
            url = "https:" + href
        elif href.startswith("http"):
            url = href
        else:
            url = base_url.rsplit("/", 1)[0] + "/" + href.lstrip("/") \
                if not href.startswith("/") \
                else re.match(r"https?://[^/]+", base_url).group(0) + href
        rows.append({"url": url, "title": title,
                     "listing_date": listing_date})
    return rows


def _fetch_one_china_policy(source: dict, days_back: int) -> list[dict]:
    """One policy source_type (one ministry/regulator), all its listings."""
    config = source.get("config") or {}
    outlet_type = config.get("source_outlet_type", "official")
    issuing_body = config.get("issuing_body")
    source_type = source.get("source_type", "")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    articles: list[dict] = []
    # Across listings, not just within one: MOFCOM's section pages repeat
    # the same announcement on its own index (confirmed - 5 duplicate
    # URLs in a real run), and the same title also reaches the listing
    # under both an http:// and an https:// href, which the URL-keyed
    # dedup below would otherwise treat as two articles.
    seen_urls: set[str] = set()
    for listing_url in config.get("listing_urls", []):
        listing = _china_get(listing_url)
        if not listing:
            continue
        rows = _china_policy_rows(listing, listing_url)
        logger.info("[CHINA] %s listing=%s rows=%d",
                    source_type, listing_url, len(rows))
        for row in rows:
            # Scheme-insensitive: the same announcement is linked as both
            # http:// and https:// from different sections of the site.
            dedup_key = re.sub(r"^https?://", "", row["url"])
            if dedup_key in seen_urls:
                continue
            seen_urls.add(dedup_key)
            # Cheap cutoff first, on the listing's own date, so an old
            # announcement is skipped without fetching its body at all.
            if row["listing_date"] and row["listing_date"] < cutoff:
                continue
            body_html = _china_get(row["url"])
            if not body_html:
                continue
            body = _china_strip_html(body_html)

            published = row["listing_date"]
            # The document's own stated date wins over the listing's,
            # then the CMS publication timestamp. Both are more
            # authoritative than where a link happened to sit on a page.
            # Most specific first: a stated document date beats a CMS
            # publication timestamp, which beats a bare masthead date.
            for pattern in (_CN_DOC_DATE_RE, _CN_PUBLISH_TIME_RE,
                            _CN_HEADER_DATE_RE):
                dm = pattern.search(body)
                if not dm:
                    continue
                try:
                    published = _china_day(
                        int(dm.group(1)), int(dm.group(2)), int(dm.group(3)))
                    break
                except ValueError:
                    continue
            if published and published < cutoff:
                continue
            # A page with no date at all after all three patterns is not
            # an announcement. Confirmed live: the three that survive on
            # SAMR are data-download catalogue entries
            # (计量标准器具核准数据, 标准物质定级鉴定数据) describing a dataset's
            # update cadence, not a measure. An undated row can never be
            # excluded by days_back either, so it would be re-fetched
            # every run forever - the same failure SAMR's listing had
            # before its article-body date was read.
            if published is None:
                logger.info("[CHINA] %s: no date found, skipping url=%s",
                            source_type, row["url"][:90])
                continue

            doc_no = _CN_DOC_NO_RE.search(body)
            issuer = _CN_ISSUER_RE.search(body)
            metadata = {
                "source_category": "cn_policy",
                "source_outlet_type": outlet_type,
                "source_type": source_type,
                "issuing_body": issuing_body,
            }
            # Only ever set when the document actually states it. An
            # absent document number means "not stated", and must never
            # be inferred - a measure's citation is the thing a later
            # amendment refers back to.
            if doc_no:
                metadata["document_number"] = doc_no.group(1)
            if issuer:
                metadata["issuing_department"] = issuer.group(1)

            articles.append({
                "url": row["url"],
                "title": row["title"],
                # No separate lead exists in a government announcement -
                # it opens straight into the measure. Left NULL rather
                # than storing body[:500], which would duplicate a
                # prefix of `body` and cut mid-sentence. Every other
                # domain treats summary as a real lead, not a slice.
                "summary": None,
                "body": body or None,
                "source": source.get("name", source_type),
                "_pub_date": published,
                "metadata": metadata,
            })
    return articles


def _fetch_china_policy(sources: list[dict], days_back: int) -> list[dict]:
    articles: list[dict] = []
    for source in sources:
        try:
            articles.extend(_fetch_one_china_policy(source, days_back))
        except Exception as exc:
            logger.warning("[CHINA] policy source failed name=%s error=%s",
                           source.get("name"), exc)
    logger.info("[CHINA] policy total articles=%d", len(articles))
    return articles


_CNINFO_QUERY_URL = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
_CNINFO_PDF_BASE = "http://static.cninfo.com.cn/"
_CNINFO_PAGE_SIZE = 30
# A ceiling on pagination, not an expected depth. The deepest real
# company over four years is Luxshare at 785 announcements, which is 27
# pages; 40 leaves headroom while stopping a malformed response from
# walking forever.
_CNINFO_MAX_PAGES = 40
# Titles whose filing is worth downloading. Everything else is stored
# with its title and date but no body - see the call site for why.
#   periodic reports and pre-announcements -> C2's revenue YoY
#   investment / transaction / fundraising -> C3's commitment amount
#
# 业绩预增 / 预减 / 预盈 / 预亏 / 变动 are the OTHER pre-announcement
# wordings and were missing. A company does not always head the filing
# 业绩预告: 拓荆科技's own "2023年年度业绩预增公告" states its full-year
# revenue growth and was skipped, so its PDF was never fetched and C2
# never saw the figure. Measured across the stored history, 30 filings
# that state results were being skipped this way - 15 of them 业绩预增.
# The bare 营业收入 catches the few that name the line item in the
# title instead of the filing type.
_CNINFO_BODY_RE = re.compile(
    r"(半年度报告|年度报告|季度报告|业绩快报"
    r"|业绩(?:预告|预增|预减|预盈|预亏|变动|修正)"
    r"|营业收入"
    r"|对外投资|投资公告|重大资产|收购|股权转让|增资"
    r"|募集资金|募投|签订|中标|合同)")
# cninfo rejects an unreferred POST, and needs the XHR marker its own
# front-end sends - confirmed live, a request without them returns a 500
# rather than an auth error.
_CNINFO_HEADERS = {
    **_CHINA_HEADERS,
    "Content-Type": "application/x-www-form-urlencoded",
    "X-Requested-With": "XMLHttpRequest",
    "Referer": ("http://www.cninfo.com.cn/new/commonUrl?url="
                "disclosure/list/notice"),
}


def _cninfo_column(code: str) -> str:
    """Which exchange a mainland code belongs to.

    Confirmed live: sending the wrong column returns zero rows with
    totalAnnouncement=0 rather than an error, so this is worth getting
    right explicitly rather than trying both.
    """
    return "szse" if code.startswith(("0", "3")) else "sse"


def _fetch_one_cninfo_company(
    company: dict, days_back: int, source_name: str, outlet_type: str,
) -> list[dict]:
    code, org_id = company.get("code"), company.get("org_id")
    if not code or not org_id:
        return []
    today = datetime.now(_CST).date()
    start = today - timedelta(days=days_back)

    def _page(num: int) -> dict:
        return {
            "pageNum": str(num),
            "pageSize": str(_CNINFO_PAGE_SIZE),
            "column": _cninfo_column(code),
            "tabName": "fulltext",
            "plate": "",
            # Both halves are required. A bare code returns zero rows and
            # totalAnnouncement=0 - it does NOT error, which is what makes
            # this failure mode worth naming: it looks exactly like a
            # company that filed nothing.
            "stock": f"{code},{org_id}",
            "searchkey": "",
            "secid": "",
            "category": "",
            "trade": "",
            "seDate": f"{start:%Y-%m-%d}~{today:%Y-%m-%d}",
            "sortName": "",
            "sortType": "",
            "isHLtitle": "true",
        }

    # cninfo returns 30 announcements per page and the window is walked
    # to its end, not sampled. This used to request page 1 only, which
    # silently capped every company at 30 regardless of days_back - a
    # year's real volume is 103-219 announcements per company and four
    # years is 396-785, so the stored history was a recent slice wearing
    # the date range's clothes. Any per-company baseline built on it
    # measured the wrong thing.
    announcements: list[dict] = []
    for page in range(1, _CNINFO_MAX_PAGES + 1):
        try:
            resp = httpx.post(_CNINFO_QUERY_URL, data=_page(page),
                              headers=_CNINFO_HEADERS, timeout=_CHINA_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
        except Exception as exc:
            logger.warning("[CHINA] cninfo query failed code=%s page=%d "
                           "error=%s", code, page, exc)
            break
        batch = data.get("announcements") or []
        announcements.extend(batch)
        # A short page is the last page. Checked on the batch rather than
        # against totalAnnouncement, which cninfo reports inconsistently
        # once a query spans several years.
        if len(batch) < _CNINFO_PAGE_SIZE:
            break
    else:
        logger.warning("[CHINA] cninfo hit the %d-page cap for code=%s - "
                       "older announcements in this window were not read",
                       _CNINFO_MAX_PAGES, code)

    articles: list[dict] = []
    for ann in announcements:
        ann_id = ann.get("announcementId")
        if not ann_id:
            continue
        ts = ann.get("announcementTime")
        published = None
        if ts:
            # Epoch milliseconds at Beijing MIDNIGHT - cninfo carries the
            # announcement's date, never its time of day. Re-stamped at
            # midday so the UTC conversion keeps the date it was filed
            # on; see _china_day.
            local = datetime.fromtimestamp(ts / 1000, tz=_CST)
            published = _china_day(local.year, local.month, local.day)
        adjunct = ann.get("adjunctUrl") or ""
        filing_url = (_CNINFO_PDF_BASE + adjunct) if adjunct else None
        # The announcement's own text. Without it a downstream classifier
        # sees only a title - and a title like "关于在手订单情况的自愿性披露
        # 公告" (voluntary disclosure of orders on hand) names the subject
        # but carries none of the figures that make it a signal.
        title = html.unescape(ann.get("announcementTitle") or "")
        # Only filings whose TITLE says they carry figures get their PDF
        # fetched. Walking a company's full window returns 100-800
        # announcements, the large majority of them routine - board
        # resignations, articles of association, shareholding changes -
        # that no classifier reads. Downloading every one costs a PDF
        # fetch each and fills the disk for nothing (it did: a four-year
        # run exhausted the database volume mid-fetch).
        #
        # The two classifiers that need a body both key on a title
        # pattern anyway: C2 reads the YoY revenue a periodic report or
        # earnings pre-announcement states about itself, and C3 reads
        # the amount in an investment or transaction announcement.
        # Titles are always present, so a filing excluded here is still
        # STORED with its title and date - it is the body that is
        # skipped, not the row, and a later pass can fill it in.
        body = (_fetch_china_filing_body(filing_url, title)
                if filing_url and _CNINFO_BODY_RE.search(title) else None)
        articles.append({
            # Synthetic, and globally unique: announcementId is cninfo's
            # own permanent key, so the existing uq_articles_url index
            # does all dedup, same convention as irbank-financials:// and
            # twse-revenue://.
            "url": f"cninfo-filing://{code}/{ann_id}",
            "title": html.unescape(ann.get("announcementTitle") or ""),
            # A filing has no editorial lead - it opens with its own
            # issuer/document header, which is already captured as
            # structured metadata below. NULL rather than a truncated
            # copy of `body`.
            "summary": None,
            "body": body,
            "source": source_name,
            "_pub_date": published,
            "metadata": {
                "source_category": "cn_disclosure",
                "source_outlet_type": outlet_type,
                "source_type": "cninfo_filing",
                "code": code,
                "company": company.get("company"),
                "native_name": company.get("native_name"),
                # The exchange's own name on the filing. Kept because it
                # differs from the stored one often enough to matter
                # (京东方A vs BOE Technology) and is the string a reader
                # would see on the document itself.
                "sec_name": ann.get("secName"),
                "announcement_id": str(ann_id),
                "filing_url": filing_url,
                "adjunct_type": ann.get("adjunctType"),
            },
        })
    return articles


def _fetch_cninfo_filing(sources: list[dict], days_back: int) -> list[dict]:
    companies = _china_companies(sources)
    if not companies:
        logger.warning("[CHINA] cninfo_filing: no companies configured")
        return []
    source_name = sources[0].get("name", "CNINFO Filings (China)")
    outlet_type = (sources[0].get("config") or {}).get(
        "source_outlet_type", "official")
    # One bulk request covers the whole universe. Resolved live rather
    # than read from the seeded config for the same reason Japan's
    # EDINET codes and Korea's DART corp_codes are: a stored identifier
    # goes stale silently here, because a wrong orgId returns
    # totalAnnouncement=0 rather than an error. A company whose orgId
    # cannot be resolved keeps whatever the config carried, so a
    # cninfo outage degrades to the old behaviour instead of dropping
    # the whole source.
    #
    # Asked for every code, NOT only the ones lacking an hk_code: SMIC
    # and Hua Hong are dual-listed and file on BOTH venues, so they
    # carry a mainland code, an hk_code and a real orgId at once.
    # Filtering on hk_code would have silently dropped the two largest
    # foundries from cninfo - the same shape of bug as the stockId
    # confusion this module exists to prevent. HK-only names (Lenovo,
    # Alibaba, Tencent, Baidu) have no mainland listing, so they simply
    # miss the map and are logged.
    codes = [c["code"] for c in companies if c.get("code")]
    org_ids = resolve_cninfo_org_ids(codes) if codes else {}
    articles: list[dict] = []
    for company in companies:
        resolved = org_ids.get(company.get("code") or "")
        if resolved:
            company = {**company, "org_id": resolved}
        articles.extend(_fetch_one_cninfo_company(
            company, days_back, source_name, outlet_type))
    logger.info("[CHINA] cninfo_filing companies=%d articles=%d",
                len(companies), len(articles))
    return articles


# HKEX's own disclosure search is a JSF form carrying a server-side
# ViewState, which an automated client would have to replay. Its
# "Latest Listed Company Information" JSON endpoint answers the same
# question without that handshake, keyed by the 5-digit stock code.
_HKEX_SEARCH_URL = (
    "https://www1.hkexnews.hk/ncms/script/eds/"
    "newsearchresult_c.json"
)
_HKEX_TITLE_SEARCH = "https://www1.hkexnews.hk/search/titleSearchServlet.do"


def _fetch_one_hkex_company(
    company: dict, days_back: int, source_name: str, outlet_type: str,
) -> list[dict]:
    hk_code = company.get("hk_code")
    stock_id = company.get("hkex_stock_id")
    if not hk_code or not stock_id:
        return []
    today = datetime.now(_CST).date()
    start = today - timedelta(days=days_back)
    params = {
        "sortDir": "0",
        "sortByOptions": "DateTime",
        "category": "0",
        "market": "SEHK",
        # HKEX's internal issuer id, NOT the stock code - see seed.py's
        # own note. Passing the code returns an empty 200, not an error.
        "stockId": str(stock_id),
        "documentType": "-1",
        "fromDate": f"{start:%Y%m%d}",
        "toDate": f"{today:%Y%m%d}",
        "title": "",
        "searchType": "1",
        "t1code": "-2",
        "t2Gcode": "-2",
        "t2code": "-2",
        # A result LIMIT, not a page size - HKEX returns everything up
        # to this number in one response and has no pagination. 100
        # silently truncated every company: SMIC's four-year history is
        # 380 filings and came back as exactly 100 at every window,
        # which looks like a complete answer rather than a capped one.
        # 1000 is above the deepest real issuer over four years.
        "rowRange": "1000",
        "lang": "EN",
    }
    try:
        resp = httpx.get(_HKEX_TITLE_SEARCH, params=params,
                         headers=_CHINA_HEADERS, timeout=_CHINA_TIMEOUT,
                         follow_redirects=True)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:
        logger.warning("[CHINA] hkex query failed code=%s error=%s",
                       hk_code, exc)
        return []

    rows = payload if isinstance(payload, list) else (
        payload.get("result") or payload.get("results") or [])
    if isinstance(rows, str):
        try:
            rows = json.loads(rows)
        except Exception:
            logger.warning("[CHINA] hkex unparseable result code=%s", hk_code)
            return []

    articles: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        file_link = row.get("FILE_LINK") or row.get("fileLink") or ""
        news_id = row.get("NEWS_ID") or row.get("newsId")
        title = html.unescape(
            row.get("TITLE") or row.get("title") or "").strip()
        if not title or not (news_id or file_link):
            continue
        published = None
        raw_dt = row.get("DATE_TIME") or row.get("dateTime") or ""
        for fmt in ("%d/%m/%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%d/%m/%Y"):
            try:
                published = datetime.strptime(raw_dt, fmt).replace(
                    tzinfo=_CST).astimezone(timezone.utc)
                break
            except (ValueError, TypeError):
                continue
        doc_url = (file_link if file_link.startswith("http")
                   else f"https://www1.hkexnews.hk{file_link}")
        # English filing text, no translation needed - the one company
        # source in this domain where the body is directly readable.
        body = _fetch_china_filing_body(doc_url, title) if file_link else None
        articles.append({
            "url": f"hkex-filing://{hk_code}/{news_id or file_link}",
            "title": title,
            # HKEX filings open with the exchange's standard liability
            # disclaimer, so the first 500 characters are boilerplate
            # identical across every filing - actively worse than NULL.
            "summary": None,
            "body": body,
            "source": source_name,
            "_pub_date": published,
            "metadata": {
                "source_category": "cn_disclosure",
                "source_outlet_type": outlet_type,
                "source_type": "hkex_filing",
                "code": company.get("code"),
                "hk_code": hk_code,
                "company": company.get("company"),
                "native_name": company.get("native_name"),
                "filing_url": doc_url,
                # English by Hong Kong listing requirement - recorded so a
                # downstream consumer never sends this to a translator.
                "filing_language": "en",
            },
        })
    return articles


def _fetch_hkex_filing(sources: list[dict], days_back: int) -> list[dict]:
    companies = _china_companies(sources)
    if not companies:
        logger.warning("[CHINA] hkex_filing: no companies configured")
        return []
    source_name = sources[0].get("name", "HKEX Filings (China)")
    outlet_type = (sources[0].get("config") or {}).get(
        "source_outlet_type", "official")
    # One request per HK code (HKEX publishes no bulk list), cached for
    # the process lifetime - 6 companies, so the cost is trivial next to
    # the filing fetches themselves. Same degrade-to-config rule as
    # cninfo above: an unresolved code keeps whatever the seed carried.
    hk_codes = [c["hk_code"] for c in companies if c.get("hk_code")]
    stock_ids = resolve_hkex_stock_ids(hk_codes) if hk_codes else {}
    articles: list[dict] = []
    for company in companies:
        resolved = stock_ids.get(company.get("hk_code") or "")
        if resolved:
            company = {**company, "hkex_stock_id": resolved}
        articles.extend(_fetch_one_hkex_company(
            company, days_back, source_name, outlet_type))
    logger.info("[CHINA] hkex_filing companies=%d articles=%d",
                len(companies), len(articles))
    return articles


def _match_china_company(
    companies: list[dict[str, Any]], text: str,
) -> dict[str, Any] | None:
    """First company whose native_name or any alias appears in text.

    Same approach as _match_japan_company, with one addition China needs:
    `exclude_terms`. Several of these names are substrings of a DIFFERENT
    listed company - 浪潮 (Inspur Electronic Information) also matches
    浪潮软件 and 浪潮数字, which are separate issuers - so a term that
    marks a known false match suppresses the row rather than attributing
    one company's news to another.
    """
    for c in companies:
        excludes = c.get("exclude_terms") or []
        if any(x and x in text for x in excludes):
            continue
        native = c.get("native_name")
        if native and native in text:
            return c
        if any(a and a in text for a in (c.get("aliases") or [])):
            return c
    return None


# ITHome stamps each listing row with an ISO timestamp in Beijing time.
_ITHOME_ROW_TS_RE = re.compile(r'data-ot="(20\d{2}-\d{2}-\d{2}T[\d:.]+)')

_IJIWEI_ABSOLUTE_RE = re.compile(r"^(\d{1,2})-(\d{1,2})\s+(\d{1,2}):(\d{2})$")
_IJIWEI_RELATIVE_RE = re.compile(r"^(\d{1,3})\s*(分钟|小时|天)前$")
_IJIWEI_RELATIVE_UNIT = {"分钟": "minutes", "小时": "hours", "天": "days"}


def _parse_ijiwei_time(raw: str) -> datetime | None:
    """ijiwei's listing timestamp, in either form it publishes.

    Recent rows carry a RELATIVE time ("14小时前", "3天前") and older ones
    an absolute "10-04 18:33" with no year. Confirmed live that both
    appear on the same page, which is why this handles both rather than
    the absolute form alone - a first version matched only the absolute
    shape and silently dropped every recent item, which is most of the
    feed.

    The absolute form assumes the current Beijing year, rolling back one
    year if that would place the item in the future - the same
    convention Japan's Jiji fetcher uses for its year-less timestamps.
    """
    raw = (raw or "").strip()
    now_cst = datetime.now(_CST)

    if m := _IJIWEI_RELATIVE_RE.match(raw):
        unit = _IJIWEI_RELATIVE_UNIT.get(m.group(2))
        if not unit:
            return None
        delta = timedelta(**{unit: int(m.group(1))})
        return (now_cst - delta).astimezone(timezone.utc)

    if m := _IJIWEI_ABSOLUTE_RE.match(raw):
        try:
            stamp = datetime(now_cst.year, int(m.group(1)), int(m.group(2)),
                             int(m.group(3)), int(m.group(4)), tzinfo=_CST)
        except ValueError:
            return None
        if stamp > now_cst + timedelta(days=1):
            stamp = stamp.replace(year=now_cst.year - 1)
        return stamp.astimezone(timezone.utc)

    return None

# Each press outlet needs its own row pattern - these are three unrelated
# sites, not one CMS. Confirmed live 2026-10-05 against each listing.
_PRESS_CN_PATTERNS = [
    # ITHome: dated rows, the timestamp in a data-ot attribute.
    (re.compile(
        r'<a[^>]+href="(?P<url>(?://|https?://)[^"]*ithome\.com/[^"]*\d+\.htm)"'
        r'[^>]*>(?P<title>[^<]{6,160}?)</a>', re.I), "ithome"),
    # Jiemian: article links with the headline as anchor text.
    (re.compile(
        r'<a\s+href="(?P<url>https://www\.jiemian\.com/article/\d+\.html)"'
        r'[^>]*>(?P<title>[^<]{6,160}?)</a>', re.I), "jiemian"),
    # EEFocus: semiconductor trade coverage.
    (re.compile(
        r'<a[^>]+href="(?P<url>(?://|https?://)[^"]*eefocus\.com/[^"]+)"'
        r'[^>]*>(?P<title>[^<]{6,160}?)</a>', re.I), "eefocus"),
    # ijiwei (集微网): semiconductor trade newsflash. Its headline sits in
    # a nested <p> rather than as the anchor's own text, so the title is
    # matched inside the tag rather than between > and <.
    #
    # The anchor contains TWO <p> elements and the first is the
    # timestamp: <p class="time">10-04 18:33</p> then
    # <p class="text-hover ell_two">headline</p>. A naive "first <p>"
    # match captured "10-04 18:33" AS the title - confirmed live, every
    # ijiwei row stored its own timestamp as its headline. The time is
    # now captured deliberately (it is the only per-row date any of
    # these outlets expose) and the headline taken from the <p> that
    # follows it.
    # The time cell carries TWO forms: an absolute "10-04 18:33" for
    # older items and a relative "14小时前" / "3天前" for recent ones.
    # Both are captured raw and resolved in the fetcher.
    (re.compile(
        r'<a[^>]+href="(?P<path>/n/\d+)"[^>]*>\s*'
        r'<p[^>]*class="time"[^>]*>\s*(?P<time>[^<]{1,24}?)'
        r'\s*</p>\s*<p[^>]*>\s*(?P<title>[^<]{6,160}?)\s*</p>', re.I),
     "ijiwei"),
]
# Outlets whose links are site-relative and need a host prefix.
_PRESS_CN_HOSTS = {"ijiwei": "https://www.ijiwei.com"}


def _fetch_press_cn_rss(
    feed_url: str, outlet: str, companies: list[dict[str, Any]],
    seen: set[str], source_name: str, outlet_type: str,
) -> list[dict]:
    """One Chinese press RSS feed, filtered to the universe.

    Matching is on the TITLE ONLY, deliberately - not title+summary.
    See the comment at the match itself for the live measurement: the
    summary roughly doubles the hit count and roughly half of what it
    adds is a company named in passing.

    The feed's own summary IS stored - as `summary`, carried through to
    the classifier in `metadata.summary`, where it can be weighed in
    context - just not used to decide whether the article is about the
    company.
    """
    raw = _china_get(feed_url)
    if not raw:
        return []
    parsed = feedparser.parse(raw)
    if not parsed.entries:
        logger.info("[CHINA] press_cn rss empty feed=%s", feed_url)
        return []

    articles: list[dict] = []
    for entry in parsed.entries:
        title = html.unescape(entry.get("title") or "").strip()
        url = (entry.get("link") or "").strip()
        if not title or not url or url in seen:
            continue
        # The feed's summary is HTML, not plain text.
        summary = _china_strip_html(entry.get("summary") or "") or None
        # MATCHED ON THE TITLE ONLY, never title+summary. Re-measured
        # live on a 60-entry feed: title-only matched 3 articles and
        # all 3 were genuinely about their company; adding the summary
        # matched 5, and both extras were wrong - an Omdia panel-
        # shipment forecast that names Lenovo in passing, and a Hang
        # Seng index-movement story that names SMIC the same way. A
        # 40% false-positive rate, reproducing the same result this
        # was first measured at on different articles.
        #
        # A company named in passing is what the China Signals spec's
        # own press classifier calls WEAK, so widening the match here
        # manufactures exactly the noise the classifier then has to
        # reject.
        company = _match_china_company(companies, title)
        if not company:
            continue
        seen.add(url)

        pub = None
        if entry.get("published_parsed"):
            try:
                pub = datetime(*entry.published_parsed[:6],
                               tzinfo=timezone.utc)
            except (TypeError, ValueError):
                pub = None
        articles.append({
            "url": url,
            "title": title,
            "summary": summary[:_CHINA_BODY_MAX_CHARS] if summary else None,
            # No body fetch: the feed's summary already carries the
            # article's substance (~1,200 characters on ITHome), so
            # fetching the page again would spend a request to restate
            # it.
            "body": None,
            "source": source_name,
            "_pub_date": pub,
            "metadata": {
                "source_category": "cn_press",
                "source_outlet_type": outlet_type,
                "source_type": "press_cn",
                "outlet": outlet,
                "via": "rss",
                "code": company.get("code"),
                "company": company.get("company"),
                "native_name": company.get("native_name"),
            },
        })
    logger.info("[CHINA] press_cn rss feed=%s entries=%d matched=%d",
                feed_url, len(parsed.entries), len(articles))
    return articles


def _fetch_press_cn(sources: list[dict], days_back: int) -> list[dict]:
    """Chinese commercial trade press, filtered to the China universe.

    None of these outlets offers a per-company query, so the whole
    listing is fetched once and filtered client-side by native_name or
    alias - the same approach Japan's monoist_capex and press_jp take,
    and Taiwan's GDELT title filter before them.

    ITHome stamps each row with a real ISO timestamp in a `data-ot`
    attribute, which is used where present. Jiemian and EEFocus carry no
    per-row timestamp, and those rows are left with `_pub_date` None
    rather than guessed - a wrong date is worse than an absent one, and
    the articles table accepts a null published (news-retrieval's own
    expiry rule never deletes a null-dated row, which is the safe
    direction).
    """
    companies = _china_companies(sources)
    if not companies:
        logger.warning("[CHINA] press_cn: no companies configured")
        return []
    source = sources[0]
    config = source.get("config") or {}
    outlet_type = config.get("source_outlet_type", "commercial_press")
    source_name = source.get("name", "China Trade and Financial Press")

    articles: list[dict] = []
    seen: set[str] = set()

    # RSS first, where an outlet publishes one. Confirmed live
    # 2026-10-05: ITHome's feed carries 60 dated entries with real
    # editorial summaries, against 50 undated headlines from its HTML
    # listing - strictly more, and the summary removes a per-article
    # body fetch. Every other Chinese outlet tried (Jiemian, EEFocus,
    # Yicai, STCN, CLS, 36kr, Sina, Huxiu, laoyaoba, eet-china,
    # semiinsights) returns 404, an empty feed, or is unreachable.
    for feed_url, outlet in config.get("rss_urls", {}).items():
        articles.extend(_fetch_press_cn_rss(
            feed_url, outlet, companies, seen,
            source_name, outlet_type))

    for listing_url in config.get("listing_urls", []):
        listing = _china_get(listing_url)
        if not listing:
            continue
        matched_here = 0
        for pattern, outlet in _PRESS_CN_PATTERNS:
            for m in pattern.finditer(listing):
                title = html.unescape(m.group("title")).strip()
                groups = m.groupdict()
                url = groups.get("url") or (
                    _PRESS_CN_HOSTS.get(outlet, "") + groups.get("path", ""))
                if url.startswith("//"):
                    url = "https:" + url
                if not title or url in seen:
                    continue
                company = _match_china_company(companies, title)
                if not company:
                    continue
                seen.add(url)
                matched_here += 1
                # ITHome carries a real timestamp per row
                # (data-ot="2026-10-05T17:22:46.9100000+08:00"); the
                # other two outlets carry none. Looked for just behind
                # the anchor, which is where the attribute sits.
                pub = None
                ot = _ITHOME_ROW_TS_RE.search(
                    listing[max(0, m.start() - 200):m.end() + 200])
                if ot:
                    try:
                        pub = datetime.fromisoformat(
                            ot.group(1)[:19]).replace(
                                tzinfo=_CST).astimezone(timezone.utc)
                    except ValueError:
                        pub = None
                # ijiwei prints MM-DD HH:MM with no year - the feed only
                # ever carries recent items, so the current Beijing year
                # is assumed, and a January row read in December is
                # rolled back rather than dated eleven months ahead.
                # Same convention Japan's Jiji fetcher already uses.
                if pub is None and groups.get("time"):
                    pub = _parse_ijiwei_time(groups["time"])
                # The article's own page text. Fetched per surviving
                # article, after the company filter - the same order
                # Japan's monoist_capex uses, so a listing of 50
                # headlines costs one body fetch rather than fifty.
                body = _fetch_china_article_body(url)
                articles.append({
                    "url": url,
                    "title": title,
                    # These three outlets' listings carry a headline
                    # only - no lead, no standfirst. NULL rather than a
                    # truncated copy of `body`; the RSS path above does
                    # have a real summary and stores it.
                    "summary": None,
                    "body": body,
                    "source": source_name,
                    "_pub_date": pub,
                    "metadata": {
                        "source_category": "cn_press",
                        "source_outlet_type": outlet_type,
                        "source_type": "press_cn",
                        "outlet": outlet,
                        # Which path fetched this, stated on both. The
                        # RSS path has always set `via`; this one did
                        # not, so a null meant either "came via HTML"
                        # or "the fetcher forgot" and a reader could
                        # not tell. It matters because only the RSS
                        # path carries a summary - "which outlets give
                        # us a lead?" is answerable from this field
                        # alone once both paths fill it.
                        "via": "html",
                        "code": company.get("code"),
                        "company": company.get("company"),
                        "native_name": company.get("native_name"),
                    },
                })
        logger.info("[CHINA] press_cn listing=%s matched=%d",
                    listing_url, matched_here)
    logger.info("[CHINA] press_cn total articles=%d", len(articles))
    return articles


_COMTRADE_URL = "https://comtradeapi.un.org/public/v1/preview/C/M/HS"
# The public preview tier answers 429 "Rate limit is exceeded. Try again
# in 3 seconds." when called back-to-back - observed live. 4s between
# calls clears it; with 4 reporters x 2 commodities x N months this is
# the dominant cost of the fetch, which is why it is seeded monthly.
_COMTRADE_MIN_INTERVAL = 4.0
_comtrade_last_call = [0.0]


def _comtrade_rate_sleep() -> None:
    elapsed = time.monotonic() - _comtrade_last_call[0]
    if elapsed < _COMTRADE_MIN_INTERVAL:
        time.sleep(_COMTRADE_MIN_INTERVAL - elapsed)
    _comtrade_last_call[0] = time.monotonic()


def _fetch_one_comtrade_period(
    reporter: dict, commodity: dict, period: str, partner_code: str,
    source_name: str, outlet_type: str,
) -> dict | None:
    """One reporter's exports of one commodity to China, for one month.

    Returns None when the period is not yet published - which is the
    normal case for the most recent months and must not be logged as a
    failure. Comtrade answers 200 with count=0 for an unpublished
    period, not an error.
    """
    _comtrade_rate_sleep()
    try:
        resp = httpx.get(_COMTRADE_URL, params={
            "reporterCode": reporter["code"],
            "period": period,
            "cmdCode": commodity["code"],
            "flowCode": "X",
        }, headers={"Accept": "application/json"},
            timeout=_CHINA_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("[CHINA] comtrade failed reporter=%s cmd=%s "
                       "period=%s: %s",
                       reporter["name"], commodity["code"], period, exc)
        return None

    rows = [r for r in (data.get("data") or [])
            if str(r.get("partnerCode")) == partner_code]
    if not rows:
        return None
    row = rows[0]
    value = row.get("primaryValue")
    if value is None:
        return None

    year, month = int(period[:4]), int(period[4:])
    label = commodity["label"]
    # Whether the figure is the reporter's own filed number or Comtrade's
    # estimate standing in for one not yet filed. Confirmed live: the
    # 2026-07 US equipment figure comes back isReported=False with
    # legacyEstimationFlag=6, i.e. an estimate that WILL be revised once
    # the real return lands.
    is_reported = bool(row.get("isReported"))
    est_flag = row.get("legacyEstimationFlag")
    title = (f"{reporter['name']} {label} exports to China, "
             f"{year}-{month:02d}: ${value / 1e9:.3f}bn"
             f"{'' if is_reported else ' (estimate)'}")
    return {
        # Unique per (reporter, commodity, period, value) - NOT per
        # period alone. Comtrade back-revises: an early month is served
        # as an estimate and replaced by the reporter's filed figure
        # weeks later. Keying on the period alone meant the revision hit
        # the global uq_articles_url index and was silently dropped,
        # leaving the estimate stored forever as if it were final
        # (confirmed by re-inserting a changed row - it did not land).
        # Including the value in the key makes a revised figure a NEW
        # row, so the series carries both and a consumer can see the
        # correction. A re-run that fetches the SAME figure still
        # dedups, which is the behaviour that mattered originally.
        "url": (f"comtrade-china://{reporter['code']}/"
                f"{commodity['code']}/{period}/{int(value)}"),
        "title": title,
        "summary": None,
        "body": None,
        "source": source_name,
        # Dated to the period it measures, not when it was published.
        # A trailing-pattern comparison downstream keys on which MONTH
        # the figure describes.
        "_pub_date": _china_day(year, month, 15),
        "metadata": {
            "source_category": "cn_trade",
            "source_outlet_type": outlet_type,
            "source_type": "comtrade_china_trade",
            "reporter": reporter["name"],
            "reporter_code": reporter["code"],
            "partner": "China",
            "commodity_code": commodity["code"],
            "commodity_label": label,
            "flow": "export",
            "period": f"{year}-{month:02d}",
            "value_usd": value,
            # Weight travels with value because a value move alone
            # cannot separate a price change from a volume change -
            # the distinction C6 needs to read export controls.
            "net_weight_kg": row.get("netWgt"),
            # False means Comtrade estimated this figure because the
            # reporter has not filed yet; it will be revised. A
            # consumer comparing a month against its own trailing
            # pattern must not treat an estimate as a confirmed move.
            "is_reported": is_reported,
            "estimation_flag": est_flag,
        },
    }


def _fetch_comtrade_china_trade(sources: list[dict], days_back: int) -> list[dict]:
    """IC and equipment exports TO China, as reported by the exporters.

    China's own customs data is unreachable (see seed.py), and
    Comtrade's China-reported monthly series stops at 2024-12. The
    partner side is current to within ~2-3 months and measures export
    controls from the side that imposes them.

    Walks back month by month from the current one. The newest months
    are usually unpublished and return nothing, which is expected - the
    walk continues rather than stopping at the first gap, since
    reporters publish on different schedules.
    """
    if not sources:
        return []
    source = sources[0]
    config = source.get("config") or {}
    reporters = config.get("reporters") or []
    commodities = config.get("commodities") or []
    partner_code = config.get("partner_code", "156")
    outlet_type = config.get("source_outlet_type", "official")
    source_name = source.get("name", "China Semiconductor Trade")
    if not reporters or not commodities:
        logger.warning("[CHINA] comtrade: no reporters/commodities configured")
        return []

    # Months to attempt, newest first. days_back is a window over the
    # DATA's own period, so a 30-day run still has to reach back past
    # the publication lag to find anything at all - hence the floor of
    # four months rather than days_back/30.
    months = max(4, (days_back // 30) + 3)
    now = datetime.now(_CST)
    periods: list[str] = []
    year, month = now.year, now.month
    for _ in range(months):
        periods.append(f"{year}{month:02d}")
        month -= 1
        if month == 0:
            year, month = year - 1, 12

    articles: list[dict] = []
    for reporter in reporters:
        for commodity in commodities:
            for period in periods:
                row = _fetch_one_comtrade_period(
                    reporter, commodity, period, partner_code,
                    source_name, outlet_type)
                if row:
                    articles.append(row)
    logger.info("[CHINA] comtrade_china_trade reporters=%d periods=%d "
                "articles=%d", len(reporters), len(periods), len(articles))
    return articles


_NBS_RELEASE_INDEX = "http://www.stats.gov.cn/sj/zxfb/"
# Article links on the NBS release index: ./202609/t20260915_1965308.html
_NBS_ARTICLE_RE = re.compile(
    r'href="(?P<href>\.?/?(?P<ym>20\d{4})/t(?P<date>20\d{6})_\d+\.html)"'
    r'[^>]*>\s*(?P<title>[^<]{8,80}?)\s*</a>', re.I)
# The monthly industrial-output release is the one carrying IC production.
_NBS_OUTPUT_TITLE_RE = re.compile(r"工业增加值")
# The DATA's own month, from the release title ("2026年8月份规模以上工业
# 增加值增长5.2%"). Distinct from the publication date: NBS publishes the
# August figure on 15 September, so keying the period off the URL date
# would label August's output as September's and shift every month by one.
_NBS_PERIOD_RE = re.compile(r"(20\d{2})年\s*(\d{1,2})月")
# 集成电路（亿块） followed by its two table cells: absolute output for the
# month, then year-on-year percent change. Confirmed live 2026-10-05
# against the August 2026 release: 529 亿块, +20.6%.
#
# The figures are not adjacent to the label - each sits inside a <span>
# within its own <td>, separated by ~250 characters of inline style
# attributes. So the row is captured greedily to a fixed width and the
# numbers pulled from the spans within it, rather than matched
# positionally.
_NBS_IC_ROW_RE = re.compile(
    r"集成电路[（(]亿块[)）](?P<row>.{0,1200})", re.S)
# A table cell's value: the text inside a <span>, which is where this
# CMS puts every number.
_NBS_CELL_RE = re.compile(r"<span[^>]*>([^<>]{1,18})</span>")


def _fetch_nbs_ic_output(sources: list[dict], days_back: int) -> list[dict]:
    """China's own monthly integrated-circuit PRODUCTION volume, from the
    National Bureau of Statistics.

    The other half of C6. Comtrade's partner-reported series measures what
    the world ships INTO China; this measures what China itself makes -
    the substitution read, in units rather than dollars. Together they
    answer "are controls biting, and is domestic output replacing what
    was restricted".

    NBS is unreachable from a local dev machine (a network-level block
    returns "The URL has been blocked as per the instructions of the
    Competent Government Authority") but answers normally from ECS -
    confirmed live 2026-10-05, 127KB of real HTML. Its structured
    easyquery API is WAF-blocked even there (`reason:UrlACL`), so the
    figure is read from the monthly industrial-output press release,
    which carries it as a table row.
    """
    if not sources:
        return []
    source = sources[0]
    config = source.get("config") or {}
    outlet_type = config.get("source_outlet_type", "official")
    source_name = source.get("name", "NBS IC Production (China)")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    index = _china_get(config.get("index_url") or _NBS_RELEASE_INDEX)
    if not index:
        return []

    articles: list[dict] = []
    seen: set[str] = set()
    for m in _NBS_ARTICLE_RE.finditer(index):
        title = html.unescape(m.group("title")).strip()
        if not _NBS_OUTPUT_TITLE_RE.search(title):
            continue
        href = m.group("href").lstrip(".")
        url = _NBS_RELEASE_INDEX.rstrip("/") + "/" + href.lstrip("/")
        if url in seen:
            continue
        seen.add(url)

        raw = m.group("date")
        try:
            published = _china_day(
                int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
        except ValueError:
            continue
        if published < cutoff:
            continue

        page = _china_get(url)
        if not page:
            continue
        ic = _NBS_IC_ROW_RE.search(page)
        if not ic:
            logger.info("[CHINA] nbs_ic_output: no IC row in %s", url)
            continue
        cells = [c.strip() for c in _NBS_CELL_RE.findall(ic.group("row"))
                 if c.strip() and c.strip() != "&nbsp;"]
        # First two numeric cells are the month's output and its
        # year-on-year percent change, in that order.
        nums: list[float] = []
        for c in cells:
            try:
                nums.append(float(c.replace(",", "")))
            except ValueError:
                continue
            if len(nums) == 2:
                break
        if len(nums) < 2:
            logger.info("[CHINA] nbs_ic_output: no figures in %s", url)
            continue
        output, yoy = nums[0], nums[1]

        # The month the FIGURE describes, read from the release title -
        # not the publication date. NBS publishes August's output on 15
        # September, so using the URL date would label every month as
        # the following one.
        pm = _NBS_PERIOD_RE.search(title)
        if pm:
            period = f"{pm.group(1)}-{int(pm.group(2)):02d}"
        else:
            logger.info("[CHINA] nbs_ic_output: no period in title %r", title)
            continue

        articles.append({
            # Keyed on the period AND the figure, for the same reason
            # comtrade-china:// is - NBS revises its monthly series, and
            # a period-only key would send the revision into the global
            # uq_articles_url index and silently drop it, leaving the
            # first-published number stored as if it were final. Both
            # now land, so a consumer can see the correction; a re-run
            # that reads the same figure still dedups.
            "url": f"nbs-ic-output://{period}/{output:g}",
            "title": (f"China integrated circuit production, "
                      f"{period}: {output:g} 亿块 ({yoy:+g}% YoY)"),
            "summary": title,
            "body": None,
            "source": source_name,
            "_pub_date": published,
            "metadata": {
                "source_category": "cn_trade",
                "source_outlet_type": outlet_type,
                "source_type": "nbs_ic_output",
                "issuing_body": "NBS",
                "indicator": "integrated_circuit_production",
                # 亿块 = 100 million units. Kept in the source's own unit
                # with the unit named, rather than silently converted -
                # a converted figure cannot be checked against the
                # release it came from.
                "value": output,
                "unit": "100m_units",
                "yoy_pct": yoy,
                # The month measured, not the month published.
                "period": period,
                "published_date": f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}",
                "release_url": url,
            },
        })
    logger.info("[CHINA] nbs_ic_output articles=%d", len(articles))
    return articles


def _fetch_press_cn_english_check(sources: list[dict], days_back: int) -> list[dict]:
    """Per-company English-coverage check for the China universe.

    Reuses _fetch_press_jp_english_check, which is not Japan-specific
    despite its name: it takes {code, company, search_query} and runs a
    DuckDuckGo News search per company. Duplicating it for China would
    mean two copies of the same rate-limited search loop.
    """
    companies = _china_companies(sources)
    if not companies:
        logger.warning("[CHINA] press_cn_english_check: no companies")
        return []
    config = (sources[0].get("config") or {})
    body_codes = set(config.get("fetch_body_for_codes") or [])

    articles = _fetch_press_jp_english_check(companies, days_back)
    fetched = 0
    for a in articles:
        meta = a.setdefault("metadata", {})
        meta["source_type"] = "press_cn_english_check"
        meta["source_category"] = "cn_english_coverage_check"
        meta["source_outlet_type"] = "commercial_press"
        # Article bodies, for the companies that need them. The search
        # snippet is one truncated sentence ("... according to"), which
        # is enough to know an article EXISTS but not to read a
        # production volume or a named customer out of it - the two
        # things signal-detection-agent's C4 rule requires before an
        # accelerator announcement counts as displacement.
        #
        # Scoped to specific codes rather than applied to every company,
        # because this costs one page fetch per surviving article and
        # the coverage check's own job - measuring how fast English
        # coverage appears - never needed a body. Confirmed live that
        # the bodies are reachable and substantial (627-6,096
        # characters across Yahoo Finance, SCMP, Zacks and others).
        if meta.get("code") in body_codes and a.get("url"):
            body = _fetch_china_article_body(a["url"])
            if body:
                a["body"] = body
                fetched += 1
    if body_codes:
        logger.info("[CHINA] press_cn_english_check bodies fetched=%d "
                    "for %d scoped codes", fetched, len(body_codes))
    return articles


# ---------------------------------------------------------------------------
# Middle East market signal
# ---------------------------------------------------------------------------
# Fetch-and-store only, same boundary as the four Asian market domains.
# Every source here is an API or structured JSON - no PDF extraction - and
# a row with no readable body is dropped rather than stored headline-only:
# the domain's consumer judges on the text, and every source below has a
# route to it (see news-retrieval/CLAUDE.md for the probe record).

_ME_HEADERS = {"User-Agent": _RSS_USER_AGENT, "Accept-Language": "en"}
# A ceiling on listing pagination, not an expected depth: both wires
# publish ~30-40 English items a day at 20 per page, so a 1-day window is
# 2-3 pages. 15 stops a malformed "hasNext" from walking forever.
_ME_MAX_PAGES = 15
# Below this, a BIS press-release body is a link stub ("<a href=...pdf>"),
# not text - measured: the short bodies were all a single link line.
_BIS_MIN_BODY_CHARS = 200


@lru_cache(maxsize=8)
def _me_match_regex(terms: tuple[str, ...]) -> re.Pattern[str]:
    """One case-insensitive alternation over a source's match_terms.

    Boundaries are "not a word character or &" rather than \\b, so "e&"
    matches on its own and "AI" does not match inside "Dubai" or "said".
    Longest terms first, so "data centres" wins over "data centre".
    """
    alternation = "|".join(
        re.escape(t) for t in sorted(terms, key=len, reverse=True))
    return re.compile(rf"(?<![\w&])(?:{alternation})(?![\w&])", re.IGNORECASE)


def _me_matches(source: dict, text: str) -> bool:
    terms = (source.get("config") or {}).get("match_terms") or []
    return not terms or bool(_me_match_regex(tuple(terms)).search(text or ""))


def _me_names(items: Any, key: str = "name") -> list[str]:
    """Display names from a list of API objects (tags, categories)."""
    return [i.get(key) or i.get("title") for i in (items or [])
            if isinstance(i, dict) and (i.get(key) or i.get("title"))]


_WAM_BASE = "https://www.wam.ae"


def _fetch_wam_article(url_slug: str) -> dict | None:
    """One WAM article by its urlSlug. 204 (empty) means "no such slug" -
    the bare `slug` field returns 204 for every article; `urlSlug`
    ("{shortCode}-{slug}") is what the endpoint actually keys on."""
    try:
        resp = httpx.get(f"{_WAM_BASE}/api/app/articles/GetArticleBySlug",
                         params={"slug": url_slug}, headers=_ME_HEADERS,
                         timeout=30.0)
        resp.raise_for_status()
        return resp.json() if resp.content else None
    except Exception as exc:
        logger.warning("[WAM] article fetch failed slug=%s: %s", url_slug, exc)
        return None


def _fetch_one_wam_wire(source: dict, days_back: int) -> list[dict]:
    """WAM English via the JSON API its own Angular site calls.

    The listing is the "latest-news" view; further pages are a POST of the
    section's own paging object with pageNumber + 1 (the endpoint rejects
    the object wrapped in any envelope). Bodies come from
    GetArticleBySlug, after the match filter - WAM publishes weather,
    sport and condolence cables alongside the AI and energy news.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    resp = httpx.get(f"{_WAM_BASE}/api/app/views/GetViewByUrl",
                     params={"url": "en/list/latest-news"},
                     headers=_ME_HEADERS, timeout=30.0)
    resp.raise_for_status()
    section = next((s["articlesResult"] for s in resp.json().get("sections", [])
                    if (s.get("articlesResult") or {}).get("items")), None)
    if not section:
        logger.warning("[WAM] latest-news view returned no article section")
        return []

    def _date(item: dict) -> datetime | None:
        try:
            return datetime.fromisoformat(item["articleDate"]).astimezone(timezone.utc)
        except (KeyError, TypeError, ValueError):
            return None

    items, paging, pages = list(section["items"]), section["paging"], 1
    while (paging.get("hasNext") and pages < _ME_MAX_PAGES
           and (_date(items[-1]) or cutoff) >= cutoff):
        # A failed later page stops paging; the pages already read are
        # kept rather than lost with the whole source.
        try:
            page = httpx.post(
                f"{_WAM_BASE}/api/app/views/GetSectionArticlesFDto",
                json=dict(paging, pageNumber=paging["pageNumber"] + 1),
                headers=_ME_HEADERS, timeout=30.0)
            page.raise_for_status()
            data = page.json()
        except Exception as exc:
            logger.warning("[WAM] page %d failed, keeping %d pages: %s",
                           pages + 1, pages, exc)
            break
        if not data.get("items"):
            break
        items.extend(data["items"])
        paging, pages = data["paging"], pages + 1

    listed = [i for i in items if (_date(i) or cutoff) >= cutoff]
    kept = [i for i in listed
            if _me_matches(source, f"{i.get('title', '')} {i.get('subTitle', '')}")]
    with ThreadPoolExecutor(max_workers=5) as executor:
        details = list(executor.map(
            lambda i: _fetch_wam_article(i["urlSlug"]), kept))

    articles: list[dict] = []
    for item, detail in zip(kept, details):
        body = _clean_summary((detail or {}).get("body") or "") or None
        if not body:
            logger.warning("[WAM] no body, dropped: %s", item.get("urlSlug"))
            continue
        articles.append({
            "title": item.get("title", ""),
            "url": f"{_WAM_BASE}/en/article/{item['urlSlug']}",
            "published": item.get("articleDate"),
            "source": "WAM - Emirates News Agency",
            # NULL: the listing's subTitle is the body's first ~200
            # characters cut mid-sentence, not a lead.
            "summary": None,
            "body": body,
            "_pub_date": _date(item),
            "metadata": {
                "source_type": "wam_wire",
                "source_category": "me_official_wire",
                "source_outlet_type": "state_press",
            },
        })
    logger.info("[WAM] pages=%d listed=%d matched=%d stored=%d",
                pages, len(listed), len(kept), len(articles))
    return articles


_SPA_API = "https://portalapi.spa.gov.sa/api/v1"


def _fetch_spa_article(uuid: str) -> dict | None:
    try:
        resp = httpx.get(f"{_SPA_API}/news/{uuid}", headers=_ME_HEADERS,
                         timeout=30.0)
        resp.raise_for_status()
        return resp.json().get("data")
    except Exception as exc:
        logger.warning("[SPA] article fetch failed uuid=%s: %s", uuid, exc)
        return None


def _fetch_one_spa_wire(source: dict, days_back: int) -> list[dict]:
    """Saudi Press Agency English via its portal API.

    The site's own homepage data carries only 7 items; the list behind it
    is GET /news?page=N (20 per page, newest first, English when asked in
    English). The list's `content` is always null, so bodies come from
    /news/{uuid} after the match filter.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    items: list[dict] = []
    for page in range(1, _ME_MAX_PAGES + 1):
        try:
            resp = httpx.get(f"{_SPA_API}/news", params={"page": page},
                             headers=_ME_HEADERS, timeout=30.0)
            resp.raise_for_status()
            batch = resp.json().get("data") or []
        except Exception as exc:
            # Page 1 failing is a real outage and is raised; a later page
            # failing keeps the pages already read.
            if page == 1:
                raise
            logger.warning("[SPA] page %d failed, keeping %d pages: %s",
                           page, page - 1, exc)
            break
        items.extend(batch)
        if not batch or int(batch[-1].get("published_at") or 0) < cutoff.timestamp():
            break

    def _date(item: dict) -> datetime | None:
        try:
            return datetime.fromtimestamp(int(item["published_at"]), timezone.utc)
        except (KeyError, TypeError, ValueError):
            return None

    listed = [i for i in items if i.get("locale") == "en"
              and (_date(i) or cutoff) >= cutoff]
    kept = [i for i in listed
            if _me_matches(source, f"{i.get('title', '')} {i.get('subtitle') or ''}")]
    with ThreadPoolExecutor(max_workers=5) as executor:
        details = list(executor.map(lambda i: _fetch_spa_article(i["uuid"]), kept))

    articles: list[dict] = []
    for item, detail in zip(kept, details):
        detail = detail or {}
        body = _clean_summary(detail.get("content") or "") or None
        if not body:
            logger.warning("[SPA] no body, dropped: %s", item.get("uuid"))
            continue
        pub = _date(item)
        articles.append({
            "title": item.get("title", ""),
            "url": f"https://www.spa.gov.sa/en/{item['uuid']}",
            "published": pub.isoformat() if pub else None,
            "source": "SPA - Saudi Press Agency",
            "summary": _clean_summary(detail.get("subtitle") or "") or None,
            "body": body,
            "_pub_date": pub,
            "metadata": {
                "source_type": "spa_wire",
                "source_category": "me_official_wire",
                "source_outlet_type": "state_press",
            },
        })
    logger.info("[SPA] listed=%d matched=%d stored=%d",
                len(listed), len(kept), len(articles))
    return articles


_SUBSTACK_MAX_ATTEMPTS = 4
# Seconds between post requests. A daily run fetches ~3-5 posts, so this
# costs seconds; a 30-day backfill (~70 posts) about half a minute.
_SUBSTACK_REQUEST_INTERVAL = 0.5


def _fetch_one_substack_post(source: dict, days_back: int) -> list[dict]:
    """A Substack publication's free articles via Substack's own API.

    Replaces the publication's RSS feed, whose body comes from the feed's
    content field and - when that is short - from a page fetch that can
    fail and leave the row without a body. Here the listing is
    /api/v1/archive (newest first, paged by offset) and every body is
    /api/v1/posts/{slug} -> body_html, so no row depends on scraping.

    Kept: type "newsletter" with audience "everyone". Podcast posts are
    audio episodes whose text is show notes repeating a written post
    under another title (measured: "Only one in ten UAE firms Agentic AI
    ready" the episode vs "Only one in 10 UAE organisations feels ready
    for agentic AI" the article); a paid post's body_html is truncated.
    """
    base = (source.get("config") or {})["base_url"].rstrip("/")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    def _date(post: dict) -> datetime | None:
        try:
            return datetime.fromisoformat(post["post_date"].replace("Z", "+00:00"))
        except (KeyError, AttributeError, ValueError):
            return None

    posts: list[dict] = []
    for page in range(_ME_MAX_PAGES):
        try:
            resp = httpx.get(f"{base}/api/v1/archive",
                             params={"sort": "new", "limit": 25, "offset": len(posts)},
                             headers=_ME_HEADERS, timeout=30.0)
            resp.raise_for_status()
            batch = resp.json() or []
        except Exception as exc:
            if page == 0:
                raise
            logger.warning("[SUBSTACK] page %d failed, keeping %d posts: %s",
                           page + 1, len(posts), exc)
            break
        posts.extend(batch)
        if not batch or (_date(batch[-1]) or cutoff) < cutoff:
            break

    kept = [p for p in posts
            if p.get("type") == "newsletter" and p.get("audience") == "everyone"
            and (_date(p) or cutoff) >= cutoff]

    def _body(slug: str) -> str | None:
        # Substack answers 429 to bursts - measured: 19 of 68 posts lost
        # with 5 parallel requests. One at a time, and a 429 is retried
        # after its Retry-After (or a doubling wait) rather than dropped.
        for attempt in range(_SUBSTACK_MAX_ATTEMPTS):
            try:
                resp = httpx.get(f"{base}/api/v1/posts/{slug}", headers=_ME_HEADERS,
                                 timeout=30.0)
                if resp.status_code == 429 and attempt + 1 < _SUBSTACK_MAX_ATTEMPTS:
                    wait = resp.headers.get("Retry-After", "")
                    time.sleep(float(wait) if wait.isdigit() else 2.0 * 2 ** attempt)
                    continue
                resp.raise_for_status()
                return _clean_summary(resp.json().get("body_html") or "") or None
            except Exception as exc:
                logger.warning("[SUBSTACK] post fetch failed slug=%s: %s", slug, exc)
                return None
        return None

    bodies = []
    for post in kept:
        bodies.append(_body(post["slug"]))
        time.sleep(_SUBSTACK_REQUEST_INTERVAL)

    articles: list[dict] = []
    for post, body in zip(kept, bodies):
        if not body:
            continue
        articles.append({
            "title": post.get("title", ""),
            "url": post.get("canonical_url") or f"{base}/p/{post['slug']}",
            "published": post.get("post_date"),
            "source": source.get("name") or base,
            # The author's own dek, written as a lead - not a body slice.
            "summary": _clean_summary(post.get("subtitle") or "") or None,
            "body": body,
            "_pub_date": _date(post),
            "metadata": {
                "source_type": "substack_post",
                "source_category": "me_ai_press",
                "source_outlet_type": "commercial_press",
            },
        })
    logger.info("[SUBSTACK] %s listed=%d kept=%d stored=%d",
                base, len(posts), len(kept), len(articles))
    return articles


_BIS_NEWS_URL = "https://www.bis.gov/news-updates"
_DOJ_PRESS_API = "https://www.justice.gov/api/v1/press_releases.json"
_NEXT_DATA_RE = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.DOTALL)


def _norm_title(title: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", (title or "").lower()).strip()


def _fetch_doj_body(title: str, published: datetime) -> tuple[str | None, str | None]:
    """The DOJ press release BIS links to, as (body, url).

    Matched by normalized title and a 14-day date window, never by the
    url BIS stores: DOJ has moved pages since BIS linked them (one link
    404s while the API serves the same release under a different slug).
    Measured: url matching found 77 of 130 BIS-linked releases, title +
    date found 122.
    """
    try:
        resp = httpx.get(_DOJ_PRESS_API, params={
            "pagesize": 20, "parameters[title]": " ".join(title.split()[:8]),
        }, headers=_ME_HEADERS, timeout=30.0)
        resp.raise_for_status()
        results = resp.json().get("results") or []
    except Exception as exc:
        logger.warning("[BIS] DOJ lookup failed title=%r: %s", title[:60], exc)
        return None, None
    want = _norm_title(title)
    for r in results:
        try:
            close = abs(int(r["date"]) - published.timestamp()) < 14 * 86400
        except (KeyError, TypeError, ValueError):
            close = False
        body = _clean_summary(r.get("body") or "")
        if _norm_title(r.get("title", "")) == want and close and body:
            return body, r.get("url")
    return None, None


def _fetch_one_bis_press(source: dict, days_back: int) -> list[dict]:
    """BIS press releases from the JSON embedded in bis.gov/news-updates.

    One request carries the whole archive (435 releases when built), each
    with its body, publication date, topic and `countryTag`. Kept when
    tagged with a configured country or when title/body name a tracked
    company - most BIS releases are Russia/China/Iran enforcement that has
    nothing to do with the Gulf.
    """
    cfg = source.get("config") or {}
    countries = set(cfg.get("countries") or [])
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    resp = httpx.get(_BIS_NEWS_URL, headers=_ME_HEADERS, timeout=60.0)
    resp.raise_for_status()
    match = _NEXT_DATA_RE.search(resp.text)
    if not match:
        logger.warning("[BIS] news-updates page carried no __NEXT_DATA__")
        return []
    teasers = (json.loads(match.group(1)).get("props", {}).get("pageProps", {})
               .get("pressReleaseTeasers") or [])

    articles: list[dict] = []
    skipped_no_body = 0
    for t in teasers:
        stamp = (t.get("publishedOn") or t.get("created") or {}).get("timestamp")
        if not stamp:
            continue
        published = datetime.fromtimestamp(int(stamp), timezone.utc)
        if published < cutoff:
            continue
        title = t.get("title") or ""
        tags = _me_names(t.get("countryTag"))
        text = _clean_summary((t.get("body") or {}).get("processed") or "")
        external = t.get("externalLink")
        external = external.get("url") if isinstance(external, dict) else external
        body, body_source, doj_url = (
            (text, "bis", None) if len(text) >= _BIS_MIN_BODY_CHARS
            else (None, None, None))
        if body is None and external and "justice.gov" in external:
            body, doj_url = _fetch_doj_body(title, published)
            body_source = "doj" if body else None
        if not (countries & set(tags) or _me_matches(source, f"{title} {body or ''}")):
            continue
        if not body:
            # Releases whose only text is a scanned PDF have no API route
            # to it - skipped, since every row in this domain needs a body.
            skipped_no_body += 1
            continue
        articles.append({
            "title": title,
            "url": f"https://www.bis.gov{t.get('path', '')}",
            "published": published.isoformat(),
            "source": "Bureau of Industry and Security",
            "summary": None,
            "body": body,
            "_pub_date": published,
            "metadata": {
                "source_type": "bis_press",
                "source_category": "me_us_policy",
                "source_outlet_type": "official",
                # Why the release was kept, and which Gulf country it names.
                "country_tags": tags,
                # "Enforcement" vs "Regulations": an M8 case vs an M1 rule.
                "topics": _me_names(t.get("newsUpdatesTopic")),
                # Where the body text came from - "bis" (the release itself)
                # or "doj" (the DOJ release it links to) - as its own field
                # rather than inferred from whether doj_url is set.
                "body_source": body_source,
                # The link BIS itself published. Kept separate from doj_url:
                # BIS's link can be stale (one 404s while DOJ serves the same
                # release elsewhere), doj_url is the release actually read.
                "external_link": external,
                "doj_url": doj_url,
            },
        })
    logger.info("[BIS] teasers=%d stored=%d skipped_no_body=%d",
                len(teasers), len(articles), skipped_no_body)
    return articles


_SEC_ARCHIVE = "https://www.sec.gov/Archives/edgar/data"
_SEC_STAKE_FORM_RE = re.compile(r"13D|13G")
# The 13D and 13G XML schemas name the same facts differently
# (percentOfClass vs classPercent, ...); each tuple lists both spellings.
_SEC_STAKE_FIELDS = {
    "issuer_name": ("issuerName",),
    "issuer_cik": ("issuerCik", "issuerCIK"),
    "issuer_cusip": ("issuerCUSIP", "issuerCusipNumber", "issuerCusip"),
    "event_date": ("dateOfEvent", "eventDateRequiresFilingThisStatement"),
}
# Narrative items: 13D items 3-7 ("purpose of transaction" is item 4) and
# the filer's own comments. Other long leaf fields (addresses, officer
# lists) are not narrative and are left out.
_SEC_NARRATIVE_TAG_RE = re.compile(r"^(item\d+|comments|commentContent)$")
_SEC_LEAF_RE = re.compile(r"<(\w+)>([^<]+)</\1>")


def _parse_sec_stake_xml(xml: str) -> dict[str, Any]:
    leaves = [(tag, html.unescape(val).strip()) for tag, val in _SEC_LEAF_RE.findall(xml)]
    first = lambda names: next((v for t, v in leaves if t in names and v), None)

    def _numbers(names: tuple[str, ...]) -> list[float]:
        out = []
        for t, v in leaves:
            if t in names:
                try:
                    out.append(float(v.replace(",", "")))
                except ValueError:
                    pass
        return out

    percents = _numbers(("percentOfClass", "classPercent"))
    shares = _numbers(("aggregateAmountOwned",
                       "reportingPersonBeneficiallyOwnedAggregateNumberOfShares"))
    narrative = list(dict.fromkeys(
        v for t, v in leaves if _SEC_NARRATIVE_TAG_RE.match(t) and len(v) >= 40))
    return {
        **{k: first(v) for k, v in _SEC_STAKE_FIELDS.items()},
        "percent_of_class": max(percents) if percents else None,
        "shares_owned": max(shares) if shares else None,
        "reporting_persons": list(dict.fromkeys(
            v for t, v in leaves if t == "reportingPersonName")),
        "narrative": narrative,
    }


def _fetch_one_sec_fund_stake(source: dict, days_back: int) -> list[dict]:
    """Schedule 13D/13G filings by the configured Gulf funds.

    Read from each filing's primary_doc.xml - SEC's structured form of the
    schedule since December 2024 - so issuer, percent and share count are
    fields, not text to extract. The body is the filing's own words: the
    cover facts in one line, then its narrative items (a 13D's item 4,
    "purpose of transaction", ran 1,700 characters on Mubadala's Evotec
    filing).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days_back)).date().isoformat()
    headers = {"User-Agent": _SEC_USER_AGENT}
    articles: list[dict] = []
    for fund in (source.get("config") or {}).get("funds") or []:
        cik = int(fund["cik"])
        try:
            resp = httpx.get(f"https://data.sec.gov/submissions/CIK{cik:010d}.json",
                             headers=headers, timeout=30.0)
            resp.raise_for_status()
            recent = resp.json()["filings"]["recent"]
        except Exception as exc:
            logger.warning("[SEC_STAKE] submissions failed fund=%s: %s", fund["fund"], exc)
            continue
        for i, form in enumerate(recent["form"]):
            filed = recent["filingDate"][i]
            if not _SEC_STAKE_FORM_RE.search(form) or filed < cutoff:
                continue
            accession = recent["accessionNumber"][i]
            folder = f"{_SEC_ARCHIVE}/{cik}/{accession.replace('-', '')}"
            time.sleep(0.15)  # SEC's fair-access limit is 10 requests/second
            try:
                doc = httpx.get(f"{folder}/primary_doc.xml", headers=headers, timeout=30.0)
                doc.raise_for_status()
            except Exception as exc:
                # Filings before the December 2024 XML mandate have no
                # primary_doc.xml; none fall inside a daily window.
                logger.warning("[SEC_STAKE] no primary_doc.xml %s: %s", accession, exc)
                continue
            p = _parse_sec_stake_xml(doc.text)
            issuer = p["issuer_name"] or "unknown issuer"
            pct = p["percent_of_class"]
            facts = (
                f"{fund['fund']} filed {form} on {filed} for {issuer}"
                + (f" (CUSIP {p['issuer_cusip']})" if p["issuer_cusip"] else "")
                + (f". Event date: {p['event_date']}" if p["event_date"] else "")
                + (f". Reporting persons: {'; '.join(p['reporting_persons'])}"
                   if p["reporting_persons"] else "")
                + (f". Beneficially owned: {p['shares_owned']:,.0f} shares"
                   if p["shares_owned"] is not None else "")
                + (f", {pct}% of class" if pct is not None else "")
                + "."
            )
            pub = datetime.fromisoformat(filed).replace(tzinfo=timezone.utc)
            articles.append({
                "title": (f"{fund['fund']}: {form} - {issuer}"
                          + (f" ({pct}% of class)" if pct is not None else "")),
                "url": f"sec-stake://{cik}/{accession}",
                "published": pub.isoformat(),
                "source": "SEC EDGAR",
                "summary": None,
                "body": "\n\n".join([facts, *p["narrative"]]),
                "_pub_date": pub,
                "metadata": {
                    "source_type": "sec_fund_stake",
                    "source_category": "me_fund_stake",
                    "source_outlet_type": "official",
                    # The row's url (sec-stake://{cik}/{accession}) exists
                    # only as the dedup key. Every value it encodes is also
                    # stored here as its own field, so a consumer reads
                    # fields instead of parsing the url - the same goes for
                    # filing_date, which `published` also carries.
                    "fund": fund["fund"],
                    "fund_cik": fund["cik"],
                    "accession_number": accession,
                    "filing_date": filed,
                    # 13D = active stake (intends to influence), 13G = passive.
                    "form": form,
                    # The filing itself on EDGAR.
                    "filing_url": f"{folder}/{accession}-index.htm",
                    "issuer_name": p["issuer_name"],
                    # Maps to a ticker via SEC company_tickers.json.
                    "issuer_cik": p["issuer_cik"],
                    # The security's own identifier, as filed.
                    "issuer_cusip": p["issuer_cusip"],
                    # When the stake changed - can be weeks before filing.
                    "event_date": p["event_date"],
                    # The spec's M3 rule compares these against the last filing.
                    "percent_of_class": p["percent_of_class"],
                    "shares_owned": p["shares_owned"],
                    # Also named in the body; kept as a list so a consumer
                    # can match entities without parsing prose.
                    "reporting_persons": p["reporting_persons"],
                },
            })
    logger.info("[SEC_STAKE] stored=%d", len(articles))
    return articles


def _fetch_middle_east(fetch_one, sources: list[dict], days_back: int) -> list[dict]:
    """Run one Middle East fetcher per source; one failing source never
    costs the others their rows (same isolation as _fetch_china_policy)."""
    articles: list[dict] = []
    for source in sources:
        try:
            articles.extend(fetch_one(source, days_back))
        except Exception as exc:
            logger.warning("[MIDDLE_EAST] source failed name=%s error=%s",
                           source.get("name"), exc)
    return articles


def _fetch_articles(
    sources: list[dict],
    days_back: int,
    max_articles: int,
    serpapi_key: str | None = None,
    newsapi_key: str | None = None,
    alpha_vantage_key: str | None = None,
    universe_url: str | None = None,
    universe_api_key: str | None = None,
    dart_api_key: str | None = None,
    edinet_api_key: str | None = None,
    domain_slug: str = "",
    event_registry_key: str | None = None,
) -> list[dict]:
    """Fetch articles from all sources, routing by source_type.

    Args:
        sources: List of source dicts (RSS, SerpAPI, NewsAPI, Alpha Vantage,
            Federal Register, and/or GDELT).
        days_back: Exclude articles older than this many days.
        max_articles: Cap on total articles; 0 means no limit.
        serpapi_key: SerpAPI API key; SerpAPI sources are skipped if None.
        newsapi_key: NewsAPI API key; NewsAPI sources are skipped if None.
        alpha_vantage_key: Alpha Vantage API key; AV sources are skipped if None.
        universe_url: research-universe base URL for dynamic ticker fetching.
        universe_api_key: Service API key (ru_ prefix) for research-universe auth.
        dart_api_key: DART (Korea FSS) API key; dart_filing sources are
            skipped if None.
        domain_slug: The domain this fetch is for - passed through to
            _fetch_gdelt for its own domain-scoped press allowlist (see
            that function's own docstring for why sources' own dicts
            can't carry this: load_sources' SELECT never includes
            domain_slug, unlike list_sources - a source dict read via the
            normal run() path has no such field to read it off of).

    Returns:
        List of article dicts sorted newest-first.
    """
    rss_sources = [s for s in sources if s.get("source_type", "rss") == "rss"]
    serpapi_sources = [s for s in sources if s.get("source_type") == "google_news"]
    newsapi_sources = [s for s in sources if s.get("source_type") == "newsapi"]
    alpha_vantage_sources = [s for s in sources if s.get("source_type") == "alpha_vantage"]
    federal_register_sources = [s for s in sources if s.get("source_type") == "federal_register"]
    event_registry_sources = [s for s in sources if s.get("source_type") == "eventregistry"]
    gdelt_sources = [s for s in sources if s.get("source_type") == "gdelt"]
    twse_revenue_sources = [s for s in sources if s.get("source_type") == "twse_revenue"]
    tpex_revenue_sources = [s for s in sources if s.get("source_type") == "tpex_revenue"]
    twse_material_sources = [s for s in sources if s.get("source_type") == "twse_material"]
    tpex_material_sources = [s for s in sources if s.get("source_type") == "tpex_material"]
    dart_filing_sources = [s for s in sources if s.get("source_type") == "dart_filing"]
    kr_customs_export_sources = [s for s in sources if s.get("source_type") == "kr_customs_export"]
    irbank_financials_sources = [s for s in sources if s.get("source_type") == "irbank_financials"]
    irbank_buyback_sources = [s for s in sources if s.get("source_type") == "irbank_buyback"]
    irbank_company_reference_sources = [s for s in sources if s.get("source_type") == "irbank_company_reference"]
    kabutan_tdnet_mirror_sources = [s for s in sources if s.get("source_type") == "kabutan_tdnet_mirror"]
    kabutan_buyback_sources = [s for s in sources if s.get("source_type") == "kabutan_buyback"]
    edinet_filing_sources = [s for s in sources if s.get("source_type") == "edinet_filing"]
    edinet_buyback_status_sources = [s for s in sources if s.get("source_type") == "edinet_buyback_status"]
    edinet_extraordinary_report_sources = [s for s in sources if s.get("source_type") == "edinet_extraordinary_report"]
    seaj_billings_sources = [s for s in sources if s.get("source_type") == "seaj_billings"]
    monoist_capex_sources = [s for s in sources if s.get("source_type") == "monoist_capex"]
    press_jp_sources = [s for s in sources if s.get("source_type") == "press_jp"]
    press_jp_english_check_sources = [s for s in sources if s.get("source_type") == "press_jp_english_check"]
    # China. The four policy source_types share one fetcher - they differ
    # by which body issues the measure, not by how the page is parsed -
    # so they are collected into one list rather than four.
    china_policy_sources = [s for s in sources if s.get("source_type") in (
        "mofcom_policy", "miit_policy", "samr_action", "cac_review",
        "cn_state_press")]
    cninfo_filing_sources = [s for s in sources if s.get("source_type") == "cninfo_filing"]
    hkex_filing_sources = [s for s in sources if s.get("source_type") == "hkex_filing"]
    comtrade_china_sources = [s for s in sources if s.get("source_type") == "comtrade_china_trade"]
    nbs_ic_output_sources = [s for s in sources if s.get("source_type") == "nbs_ic_output"]
    press_cn_sources = [s for s in sources if s.get("source_type") == "press_cn"]
    press_cn_english_check_sources = [s for s in sources if s.get("source_type") == "press_cn_english_check"]
    # Middle East. Each source_type has its own fetcher; _fetch_middle_east
    # isolates a failing source from the rest.
    middle_east_fetchers = {
        "wam_wire": _fetch_one_wam_wire,
        "spa_wire": _fetch_one_spa_wire,
        "bis_press": _fetch_one_bis_press,
        "sec_fund_stake": _fetch_one_sec_fund_stake,
        "substack_post": _fetch_one_substack_post,
    }
    middle_east_sources = {
        st: [s for s in sources if s.get("source_type") == st]
        for st in middle_east_fetchers
    }

    # Resolve the Japanese universe once for the whole run, before any
    # Japan fetcher dispatches - six source_types share it, and each
    # would otherwise call research-universe itself. Empty on failure,
    # which is what makes _japan_companies fall back to seeded config.
    global _JAPAN_UNIVERSE
    _JAPAN_UNIVERSE = (
        get_tracked_company_universe("Japan", universe_url, universe_api_key)
        if domain_slug == "japan_market_signal" else None)

    articles: list[dict] = []
    t0 = time.perf_counter()

    if rss_sources:
        articles.extend(_fetch_rss(rss_sources, days_back))

    if serpapi_sources:
        if serpapi_key:
            articles.extend(
                _fetch_serpapi(serpapi_sources, days_back, serpapi_key, domain_slug)
            )
        else:
            logger.warning(
                "SERPAPI_KEY not set - skipping %d serpapi source(s)",
                len(serpapi_sources),
            )

    if newsapi_sources:
        if newsapi_key:
            articles.extend(_fetch_newsapi(newsapi_sources, days_back, newsapi_key))
        else:
            logger.warning(
                "NEWSAPI_KEY not set - skipping %d newsapi source(s)",
                len(newsapi_sources),
            )

    if alpha_vantage_sources:
        if alpha_vantage_key:
            articles.extend(_fetch_alpha_vantage(alpha_vantage_sources, alpha_vantage_key, universe_url, universe_api_key))
        else:
            logger.warning(
                "ALPHA_VANTAGE_API_KEY not set - skipping %d alpha_vantage source(s)",
                len(alpha_vantage_sources),
            )

    if federal_register_sources:
        articles.extend(_fetch_federal_register(federal_register_sources, days_back))

    if event_registry_sources:
        if event_registry_key:
            articles.extend(_fetch_event_registry(
                event_registry_sources, days_back, event_registry_key,
            ))
        else:
            logger.warning(
                "EVENT_REGISTRY_API_KEY not set - skipping %d eventregistry"
                " source(s)", len(event_registry_sources),
            )

    if gdelt_sources:
        articles.extend(_fetch_gdelt(gdelt_sources, days_back, domain_slug))

    if twse_revenue_sources:
        articles.extend(_fetch_twse_revenue(twse_revenue_sources))

    if tpex_revenue_sources:
        articles.extend(_fetch_tpex_revenue(tpex_revenue_sources))

    if twse_material_sources:
        articles.extend(_fetch_twse_material(twse_material_sources))

    if tpex_material_sources:
        articles.extend(_fetch_tpex_material(tpex_material_sources))

    if dart_filing_sources:
        if dart_api_key:
            articles.extend(_fetch_dart_filing(dart_filing_sources, days_back, dart_api_key))
        else:
            logger.warning(
                "DART_API_KEY not set - skipping %d dart_filing source(s)",
                len(dart_filing_sources),
            )

    if kr_customs_export_sources:
        articles.extend(_fetch_customs_export_data(kr_customs_export_sources, days_back))

    if irbank_financials_sources:
        articles.extend(_fetch_irbank_financials(irbank_financials_sources, days_back))

    if irbank_buyback_sources:
        articles.extend(_fetch_irbank_buyback(irbank_buyback_sources))

    if irbank_company_reference_sources:
        articles.extend(_fetch_irbank_company_reference(irbank_company_reference_sources))

    if kabutan_tdnet_mirror_sources:
        articles.extend(_fetch_kabutan_tdnet_mirror(kabutan_tdnet_mirror_sources))

    if kabutan_buyback_sources:
        articles.extend(_fetch_kabutan_buyback(kabutan_buyback_sources))

    if edinet_filing_sources:
        if edinet_api_key:
            articles.extend(_fetch_edinet_filing(edinet_filing_sources, days_back, edinet_api_key))
        else:
            logger.warning(
                "EDINET_API_KEY not set - skipping %d edinet_filing source(s)",
                len(edinet_filing_sources),
            )

    if edinet_buyback_status_sources:
        if edinet_api_key:
            articles.extend(_fetch_edinet_buyback_status(edinet_buyback_status_sources, days_back, edinet_api_key))
        else:
            logger.warning(
                "EDINET_API_KEY not set - skipping %d edinet_buyback_status source(s)",
                len(edinet_buyback_status_sources),
            )

    if edinet_extraordinary_report_sources:
        if edinet_api_key:
            articles.extend(_fetch_edinet_extraordinary_report(edinet_extraordinary_report_sources, days_back, edinet_api_key))
        else:
            logger.warning(
                "EDINET_API_KEY not set - skipping %d edinet_extraordinary_report source(s)",
                len(edinet_extraordinary_report_sources),
            )

    if seaj_billings_sources:
        articles.extend(_fetch_seaj_billings(seaj_billings_sources))

    if monoist_capex_sources:
        articles.extend(_fetch_monoist_capex(monoist_capex_sources))

    if press_jp_sources:
        articles.extend(_fetch_press_jp(press_jp_sources))

    if press_jp_english_check_sources:
        articles.extend(_fetch_press_jp_english_check_source(press_jp_english_check_sources, days_back))

    if china_policy_sources:
        articles.extend(_fetch_china_policy(china_policy_sources, days_back))

    if cninfo_filing_sources:
        articles.extend(_fetch_cninfo_filing(cninfo_filing_sources, days_back))

    if hkex_filing_sources:
        articles.extend(_fetch_hkex_filing(hkex_filing_sources, days_back))

    if comtrade_china_sources:
        articles.extend(_fetch_comtrade_china_trade(comtrade_china_sources, days_back))

    if nbs_ic_output_sources:
        articles.extend(_fetch_nbs_ic_output(nbs_ic_output_sources, days_back))

    if press_cn_sources:
        articles.extend(_fetch_press_cn(press_cn_sources, days_back))

    if press_cn_english_check_sources:
        articles.extend(_fetch_press_cn_english_check(press_cn_english_check_sources, days_back))

    for source_type, fetch_one in middle_east_fetchers.items():
        if middle_east_sources[source_type]:
            articles.extend(_fetch_middle_east(
                fetch_one, middle_east_sources[source_type], days_back))

    articles.sort(
        key=lambda a: (
            a["_pub_date"] or datetime.min.replace(tzinfo=timezone.utc)
        ),
        reverse=True,
    )
    if max_articles:
        articles = articles[:max_articles]
    for a in articles:
        a["published"] = a.pop("_pub_date")  # normalised UTC datetime; None if source omitted date

    logger.info(
        "[TIMER] fetch total: sources=%d articles=%d elapsed=%.2fs",
        len(sources), len(articles), time.perf_counter() - t0,
    )
    return articles


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

_ARTICLE_KEYS = ("url", "title", "summary", "source", "published", "body", "metadata")

# Domains whose rows must carry a body AND be published inside the run's
# window (see run()). Their consumer judges only on current text.
_BODY_REQUIRED_DOMAINS: frozenset[str] = frozenset({"middle_east_market_signal"})


def run(
    domain_slug: str,
    days_back: int = 7,
    max_articles: int = 0,
) -> dict[str, Any]:
    """Fetch articles for the given domain.

    Args:
        domain_slug: Domain identifier used to query sources from DB.
        days_back: Exclude articles older than this many days.
        max_articles: Cap on total articles fetched; 0 means no limit.

    Returns:
        Dict with ``"articles"`` (list of article dicts).
    """
    t0 = time.perf_counter()
    serpapi_key = (
        os.environ.get("SERPAPI_KEY_GEOPOLITICAL")
        if domain_slug == "geopolitical_news"
        else os.environ.get("SERPAPI_KEY")
    )
    newsapi_key = os.environ.get("NEWSAPI_KEY")
    # newsapi.ai / Event Registry - a different service from
    # newsapi.org above, with its own key format and endpoint.
    event_registry_key = os.environ.get("EVENT_REGISTRY_API_KEY")
    alpha_vantage_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
    universe_url = os.environ.get("RESEARCH_UNIVERSE_URL")
    universe_api_key = os.environ.get("RESEARCH_UNIVERSE_API_KEY")
    dart_api_key = os.environ.get("DART_API_KEY")
    edinet_api_key = os.environ.get("EDINET_API_KEY")

    sources = load_sources(domain_slug, days_back)
    if not sources:
        return {"articles": []}

    articles = _fetch_articles(
        sources, days_back, max_articles, serpapi_key, newsapi_key, alpha_vantage_key,
        universe_url, universe_api_key, dart_api_key, edinet_api_key, domain_slug,
        event_registry_key,
    )
    if not articles:
        return {"articles": []}

    # Domains whose consumer judges only on text drop body-less rows here,
    # once, whatever the source. Every Middle East source already reads its
    # body from an API and drops a row without one; this is the backstop
    # for any source added later that does not.
    if domain_slug in _BODY_REQUIRED_DOMAINS:
        before = len(articles)
        articles = [a for a in articles if a.get("body")]
        if len(articles) < before:
            logger.warning("[%s] dropped %d article(s) without a body",
                           domain_slug, before - len(articles))
        if not articles:
            return {"articles": []}

    # Same domains: drop rows PUBLISHED before the run's window. Event
    # Registry's date filter selects on when it crawled an article, not
    # when it was published, so a late crawl brings old stories in -
    # measured: a 1-day run returned a story published 6 days earlier,
    # a 30-day run one from July. One day of margin absorbs timezones and
    # crawl lag. A row with no publication date is kept (fail-open).
    if domain_slug in _BODY_REQUIRED_DOMAINS:
        oldest = datetime.now(timezone.utc) - timedelta(days=days_back + 1)
        before = len(articles)
        articles = [a for a in articles
                    if a.get("published") is None or a["published"] >= oldest]
        if len(articles) < before:
            logger.info("[%s] dropped %d article(s) published before the window",
                        domain_slug, before - len(articles))
        if not articles:
            return {"articles": []}

    # Global cross-domain, cross-run dedup: drop articles already stored
    # under ANY domain's prior run. Bounded to this run's candidate URLs,
    # not a full history pull.
    seen_urls = get_already_stored_urls([a["url"] for a in articles])
    articles = [a for a in articles if a["url"] not in seen_urls]
    if not articles:
        return {"articles": []}

    # ai_news/smart_money: RSS/SerpAPI/NewsAPI sources aren't pre-scoped to
    # one story the way ticker/agency-filtered domains are, so the same
    # story from multiple outlets survives the URL-only dedup above with
    # different URLs and differently-worded titles. Same title-similarity
    # embedding check as Taiwan GDELT (_dedup_by_title_similarity), scoped
    # by domain instead of ticker.
    if domain_slug in _TITLE_DEDUP_DOMAINS:
        before_title_dedup = len(articles)
        articles = _dedup_by_title_similarity_for_domain(articles, domain_slug)
        logger.info(
            "[%s] title-similarity dedup: %d -> %d article(s)",
            domain_slug, before_title_dedup, len(articles),
        )

    # taiwan_market_signal: news-retrieval's job stops at fetch, Stage A
    # filtering (allowlist + name-in-title, already run inside
    # _fetch_gdelt), and exact/title-similarity dedup — matching every
    # other domain's fetch/store-raw boundary. Revenue ranking, material-
    # announcement clause classification, and translation all moved to
    # signal-detection-agent (see insert_taiwan_signal_classification in
    # that service's models/jobs.py) - keeping this service uniformly
    # fetch-only rather than the sole domain doing classification/
    # translation work here.

    logger.info(
        "[TIMER] domain=%s total=%.2fs articles=%d",
        domain_slug, time.perf_counter() - t0, len(articles),
    )
    return {
        "articles": [
           {k: a.get(k) for k in _ARTICLE_KEYS} for a in articles
        ],
    }
