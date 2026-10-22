"""middle_east_market_signal classifier.

Built from "Middle East Market Signals for the AI Economy" (Ayushi Shah,
2026-10-15), classification only - trader cards, "confirmed if / wrong
if" lines and the daily summary are later work.

Labels are this service's own, used directly: `signal` (the spec's
"Alert"), `weak_signal` ("Store"), `noise` ("Ignore"). One difference from
the spec, deliberate: "Ignore" there means discarded; here a `noise` row
is STORED with its reason, like every other domain, so an article we
judged stays distinguishable from one we never saw.

The pipeline, one step per section below:
  1. pool today's articles and skip any already judged   (no model)
  2. load the reference tables                           (no model)
  3. event type from the source: final or a hint         (no model)
  4. headline rules: market wraps, weekly digests        (no model)
  5. SEC 13D/G: labelled from the filing's fields        (no model)
  6. one model call per other article, full body, read once (facts only)
  7. code checks the facts against the text, decides the label from the
     spec's section 7 rules, and adds the read-through   (no model)
  8. store
The model extracts facts; code decides labels, so every label traces to a
spec rule, and a rule change can re-label stored rows from
`metadata.extracted` without calling the model again.

Not built (deliberately): same-story grouping. news-retrieval already
merges same-event articles into `also_reported_by`; a separate step is
only worth building if the stored rows show a need.
"""
from __future__ import annotations

import json
import logging
import re
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import config
from pipeline.geopolitical_signal_stage_d import build_corroboration
from pipeline.geopolitical_signal_stage_e import _entity_keys, _is_specific

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Step 1 - which source an article came from
# ---------------------------------------------------------------------------
# Every later step routes on this. news-retrieval stamps
# metadata.source_type on all but one source: the Federal Register copy,
# whose fetcher is shared with geopolitical_news and writes no source_type
# - its rows are recognised by their own url scheme instead. The five
# newsapi.ai queries share one source_type, and WHICH query found the
# article (shipping, helium, ...) is the part routing needs, so they are
# told apart by metadata.er_query.

_FEDERAL_REGISTER_SCHEME = "me-federal-register://"


def source_name(article: dict[str, Any]) -> str:
    """The source an article came from, as one stable name:
    `wam_wire`, `spa_wire`, `substack_post`, `bis_press`, `sec_fund_stake`,
    `federal_register`, or `eventregistry:me_<query>`. `unknown` when the
    article carries none of these - logged and counted by the caller,
    never silently routed."""
    metadata = article.get("metadata") or {}
    source_type = metadata.get("source_type")
    if source_type == "eventregistry":
        return metadata.get("er_query") or "eventregistry"
    if source_type:
        return source_type
    if (article.get("url") or "").startswith(_FEDERAL_REGISTER_SCHEME):
        return "federal_register"
    return "unknown"


def describe_pool(articles: list[dict[str, Any]]) -> dict[str, int]:
    """Article count per source - what a dry run reports, and what each
    run logs, so a source that stops delivering shows up as a missing
    line rather than going unnoticed."""
    return dict(Counter(source_name(a) for a in articles).most_common())


# ---------------------------------------------------------------------------
# Step 2 - reference tables
# ---------------------------------------------------------------------------
# Two tables, one copy each:
#  - the COMPANY table lives in news-retrieval (seed.MIDDLE_EAST_TICKER_
#    UNIVERSE, read live from GET /market/middle-east/universe each run):
#    each Gulf/Israeli company and the US companies its news moves
#    (`counterparties`, with `share_price_on_good_news`).
#  - the EVENTS table lives here: the spec's "At a glance" rows for events
#    that name no company (Hormuz closed, oil prices spike, Qatar's helium
#    plant offline). Deciding "this article is a Hormuz closure" is
#    classification, so the table sits with the classifier that uses it.

# Spec section 3, in its own words.
MIDDLE_EAST_EVENT_TYPES: dict[str, str] = {
    "M1": "US decisions on Gulf chip sales",
    "M2": "Gulf data centres switching on",
    "M3": "Gulf money into US AI",
    "M4": "War and shipping",
    "M5": "Helium and gas supply",
    "M6": "Gulf government rules and supplier choices",
    "M7": "Israel",
    "M8": "Enforcement against a named Gulf company",
}


def _affected(names: dict[str, str], share_price: str) -> list[dict[str, str]]:
    """{ticker: name} -> affected-company rows sharing one direction."""
    return [{"company_name": name, "ticker": ticker, "share_price": share_price}
            for ticker, name in names.items()]


_GULF_DATA_CENTRE_SUPPLIERS = {
    "NVDA": "Nvidia", "AMD": "AMD", "CSCO": "Cisco", "ORCL": "Oracle",
    "MSFT": "Microsoft", "VRT": "Vertiv", "GEV": "GE Vernova", "ETN": "Eaton"}
_GULF_FUND_BENEFICIARIES = {"NVDA": "Nvidia", "AVGO": "Broadcom", "MU": "Micron"}

# Spec "At a glance", row by row. `news` is whether the row is the good or
# the bad side of its event type - how the read-through finds the row for an
# article. `share_price` is the move on THIS event, already signed (unlike the company table's share_price_on_good_news,
# which depends on whether the news is good or bad). Where the spec names
# a group rather than companies, its wording is kept in
# `affected_companies_described` and no ticker is invented.
MIDDLE_EAST_EVENTS: list[dict[str, Any]] = [
    {"event_type": "M2", "news": "good", "event": "A Gulf data centre switches on, or receives its chips",
     "why": "More AI hardware is actually sold",
     "affected_companies": _affected(_GULF_DATA_CENTRE_SUPPLIERS, "rise")},
    {"event_type": "M2", "news": "bad", "event": "A Gulf data centre is delayed, damaged or cancelled",
     "why": "Expected sales disappear",
     "affected_companies": _affected(_GULF_DATA_CENTRE_SUPPLIERS, "fall")},
    {"event_type": "M6", "news": "bad", "event": "A Gulf buyer picks a Chinese chip or cloud supplier",
     "why": "A lost sale, and a reason for Washington to tighten",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD", "MSFT": "Microsoft",
                                      "ORCL": "Oracle", "AMZN": "Amazon"}, "fall")},
    {"event_type": "M3", "news": "good", "event": "A Gulf sovereign fund invests in a US AI company",
     "why": "More cash for AI spending, which becomes chip and server orders",
     "affected_companies": _affected(_GULF_FUND_BENEFICIARIES, "rise"),
     "affected_companies_described": "Nvidia, Broadcom, Micron, data-centre builders"},
    {"event_type": "M3", "news": "bad", "event": "A Gulf fund pulls back or misses a commitment",
     "why": "The AI spending cycle has less fuel",
     "affected_companies": _affected(_GULF_FUND_BENEFICIARIES, "fall"),
     "affected_companies_described": "Nvidia, Broadcom, Micron, data-centre builders"},
    {"event_type": "M1", "news": "good", "event": "The US approves chip sales to a Gulf country",
     "why": "Signed deals can now ship and become revenue",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD"}, "rise")},
    {"event_type": "M1", "news": "bad", "event": "Congress blocks a deal, or a smuggling case names a Gulf firm",
     "why": "Shipments freeze",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD"}, "fall")},
    {"event_type": "M4", "news": "bad", "event": "Hormuz or the Red Sea is closed, or ships are attacked",
     "why": "Oil, gas and freight costs rise; chips and equipment ship more slowly",
     "affected_companies": _affected({"MU": "Micron", "INTC": "Intel"}, "fall"),
     "affected_companies_described": "Chipmakers (Micron, Intel), Apple's supply chain, data-centre operators"},
    {"event_type": "M5", "news": "bad", "event": "Qatar's helium plant stays offline",
     "why": "Chip factories run short of a gas they cannot replace",
     "affected_companies": _affected({"MU": "Micron", "INTC": "Intel", "NVDA": "Nvidia",
                                      "AAPL": "Apple"}, "fall")},
    {"event_type": "M5", "news": "bad", "event": "Qatar's helium plant stays offline (other side)",
     "why": "Helium becomes scarce, so sellers charge more",
     "affected_companies": _affected({"XOM": "ExxonMobil", "LIN": "Linde",
                                      "APD": "Air Products"}, "rise")},
    {"event_type": "M4", "news": "bad", "event": "Oil prices spike",
     "why": "US inflation rises, interest rates stay higher",
     "affected_companies": [],
     "affected_companies_described": "Highly valued tech stocks broadly"},
    {"event_type": "M4", "news": "good", "event": "A ceasefire holds and ships move through Hormuz again",
     "why": "Energy, helium and shipping recover",
     "affected_companies": [],
     "affected_companies_described": "Chipmakers, memory makers, the wider tech market"},
    {"event_type": "M7", "news": "bad", "event": "Missile strikes or shutdowns in Israel",
     "why": "Key design centres and factories are disrupted",
     "affected_companies": _affected({"NVDA": "Nvidia", "INTC": "Intel",
                                      "TSEM": "Tower Semiconductor"}, "fall")},
]

_DIRECTIONS = {"rise", "fall"}
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_COMPANY_FIELDS = ("ticker", "company", "exchange", "country", "aliases", "counterparties")
_COUNTERPARTY_FIELDS = ("counterparty_name", "counterparty_ticker", "relationship",
                        "share_price_on_good_news")


def company_table_problems(companies: list[dict[str, Any]]) -> list[str]:
    """Everything wrong with the company table as served, one line each.

    Checked on every load because the table is edited by hand in another
    service: a typo ("rsie"), a lower-case ticker or a name claimed by two
    companies would otherwise produce wrong rows silently - and rows are
    written once and never revised.
    """
    problems: list[str] = []
    if not companies:
        return ["the company table is empty"]
    owner_of: dict[str, str] = {}
    for c in companies:
        label = c.get("company") or "<no name>"
        missing = [f for f in _COMPANY_FIELDS if f not in c]
        if missing:
            problems.append(f"{label}: missing {', '.join(missing)}")
            continue
        for name in [c["company"], *c["aliases"]]:
            key = name.casefold()
            if key in owner_of and owner_of[key] != label:
                problems.append(f"{label}: name {name!r} also belongs to {owner_of[key]}")
            owner_of[key] = label
        for cp in c["counterparties"]:
            cp_label = f"{label} -> {cp.get('counterparty_name') or '<no name>'}"
            missing = [f for f in _COUNTERPARTY_FIELDS if not cp.get(f)]
            if missing:
                problems.append(f"{cp_label}: missing {', '.join(missing)}")
                continue
            if cp["share_price_on_good_news"] not in _DIRECTIONS:
                problems.append(f"{cp_label}: share_price_on_good_news is "
                                f"{cp['share_price_on_good_news']!r}, not rise/fall")
            if not _TICKER_RE.match(cp["counterparty_ticker"]):
                problems.append(f"{cp_label}: ticker {cp['counterparty_ticker']!r} is malformed")
    return problems


def company_name_index(companies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Every company name and alias, case-folded -> its company row. What
    later steps use to check that a company the model says an article is
    about is really one of ours, and to find its counterparties."""
    return {name.casefold(): c
            for c in companies for name in [c["company"], *c["aliases"]]}


# ---------------------------------------------------------------------------
# Event type from the source
# ---------------------------------------------------------------------------
# Only the SEC 13D/G has a FINAL event type: what it is, is certain from
# where it came, and its facts are structured fields. Everything else only
# has a HINT - the query that found it is a good guess (the shipping query
# is usually M4) but not proof, so the model, which reads the article,
# states the final type. Federal Register and BIS are hinted M1 but read by
# the model too: they are free text, an enforcement order naming a Gulf
# company is M8, and the Federal Register filter also catches notices that
# mention the Gulf only in passing (a Russian airline's denial order).
SOURCE_EVENT_TYPES: dict[str, dict[str, Any]] = {
    "sec_fund_stake": {"event_type": "M3", "final": True},
    "federal_register": {"event_type": "M1", "final": False},
    "bis_press": {"event_type": "M1", "final": False},
    "eventregistry:me_shipping": {"event_type": "M4", "final": False},
    "eventregistry:me_helium": {"event_type": "M5", "final": False},
    "eventregistry:me_qatarenergy": {"event_type": "M5", "final": False},
    "eventregistry:me_israel_sites": {"event_type": "M7", "final": False},
    # Gulf outlets and the wires carry every type (deals, money, rules,
    # war), so they get no hint at all.
    "eventregistry:me_gulf_outlets": {"event_type": None, "final": False},
    "wam_wire": {"event_type": None, "final": False},
    "spa_wire": {"event_type": None, "final": False},
    "substack_post": {"event_type": None, "final": False},
}

# Sources code labels alone (no model): their facts are already structured
# fields. Everything else goes through the headline rules, then the model.
RULE_DECIDED_SOURCES = frozenset({"sec_fund_stake"})


def event_type_from_source(source: str) -> dict[str, Any]:
    """{"event_type": "M1".."M8" | None, "final": bool} for a source name;
    an unknown source gets no type and is never final."""
    return SOURCE_EVENT_TYPES.get(source, {"event_type": None, "final": False})




# ---------------------------------------------------------------------------
# Step 4 - headline rules: the only press articles skipped before the model
# ---------------------------------------------------------------------------
# Two shapes are certain from the headline alone and never carry an event of
# their own. Everything else - however off-topic it looks - goes to the
# model: the 50-article test (2026-10-21) showed keyword rules on the
# opening throw out items the spec stores (partnerships, plans, threats).

# Price or market reports, analyst calls and predictions: a market move is
# not a Middle East event - the event behind it, if any, arrives in its own
# article. "Why X stock fell" headlines are NOT skipped: one ("Why Intel
# Stock Hit the Skids on Wednesday") was the report of the Kiryat Gat fab
# explosion, so they go to the model.
_MARKET_WRAP_RE = re.compile(
    r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun):"                      # Globes' daily TASE wrap
    r"|\b(?:US|European|Asian|Gulf|Israel) (?:stocks|shares)\b"
    r"|\bstock market news\b|\bstock prediction\b|\bprice prediction\b"
    r"|\bmarket cap stock movers\b|\banalyst calls\b|\btop picks\b"
    r"|\b(?:starts|initiates|reiterates|keeps)\b[^.]{0,60}\b(?:buy|sell|hold|overweight|outperform)\b"
    r"|\bdollar (?:heads|retreats|firms|slips)\b|\bTASE\b|\bindices\b",
    re.IGNORECASE)

# Middle East AI News' weekly roundup repeats posts already judged one by one.
_WEEKLY_DIGEST_RE = re.compile(r"^this week[’']s top stories", re.IGNORECASE)


def headline_rule(article: dict[str, Any], source: str) -> tuple[str, str] | None:
    """(rule, reason) when the headline alone shows the article carries no
    event of its own, else None. Official sources are never skipped."""
    if source in RULE_DECIDED_SOURCES or source in ("federal_register", "bis_press"):
        return None
    title = (article.get("title") or "").strip()
    if source == "substack_post" and _WEEKLY_DIGEST_RE.search(title):
        return ("weekly_digest",
                "This is Middle East AI News' weekly roundup; each of its stories is"
                " covered on its own.")
    if _MARKET_WRAP_RE.search(title):
        return ("market_wrap",
                f'"{title}" is a report on share-price moves, not a news event.')
    return None


# ---------------------------------------------------------------------------
# Step 5 - SEC 13D/G: labelled from the filing's own fields
# ---------------------------------------------------------------------------
# Spec 7.4 (M3): a completed investment by a Gulf fund in a US AI company is
# an alert; so is a fund selling or reducing a stake. "US AI company" means
# a US company in research-universe's AI Economy Universe - the company list
# every other domain in this service already uses. A 13D/G is filed at 5%:
#  - a NEW filing (no "/A") is a new holding of 5% or more  -> signal, rise
#  - an amendment below 5% means the fund sold down           -> signal, fall
#  - any other amendment states the new holding but not the old one, so the
#    size of the change is unknown                            -> weak_signal
_SEC_FILING_THRESHOLD_PCT = 5.0
_COMPANY_SUFFIX_RE = re.compile(
    r"\b(?:inc|incorporated|corp|corporation|co|company|ltd|limited|plc|llc|lp|"
    r"holdings?|group|sa|se|nv|ag|the|class [abc])\b")


def company_key(name: str) -> str:
    """A company name reduced for matching across sources: "NVIDIA Corp."
    and "Nvidia Corporation" -> "nvidia"."""
    key = unicodedata.normalize("NFKC", name or "").casefold().replace("&", " and ")
    key = re.sub(r"[^\w\s]", " ", key)
    key = _COMPANY_SUFFIX_RE.sub(" ", key)
    return re.sub(r"\s+", " ", key).strip()


def us_universe_index(companies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """research-universe's US companies by company_key of name and aliases."""
    index: dict[str, dict[str, Any]] = {}
    for c in companies:
        for name in [c.get("company_name") or "", *(c.get("aliases") or [])]:
            key = company_key(name)
            if key:
                index.setdefault(key, c)
    return index


def _pct(value: float) -> str:
    return f"{value:g}%"


def judge_sec_stake(article: dict[str, Any], universe: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The result for one 13D/G filing - no model call. The summary is
    built from the filing's own fields, so it needs no check."""
    md = article.get("metadata") or {}
    fund = md.get("fund") or "A Gulf fund"
    issuer = md.get("issuer_name") or "an unnamed issuer"
    filed = md.get("filing_date") or "an unstated date"
    pct = md.get("percent_of_class")
    pct = float(pct) if isinstance(pct, (int, float)) or (isinstance(pct, str) and _is_number(pct)) else None
    shares = md.get("shares_owned")
    shares = float(shares) if isinstance(shares, (int, float)) or (isinstance(shares, str) and _is_number(shares)) else None
    amended = (md.get("form") or "").upper().endswith("/A")
    listed = universe.get(company_key(issuer))
    ticker = listed.get("ticker") if listed else None
    entities = [{"name": fund, "ticker": None, "country": None, "role": "subject"},
                {"name": issuer, "ticker": ticker, "country": None, "role": "subject"}]
    holding = (f"{shares:,.0f} shares ({_pct(pct)} of the class)" if shares is not None and pct is not None
               else f"{_pct(pct)} of the class" if pct is not None else "an undisclosed number of shares")
    summary = (f"{fund} {'updated its' if amended else 'filed a new'} US SEC ownership filing on {filed}"
               f" for {issuer}: it holds {holding}.")
    name = f"{issuer} ({ticker})" if ticker else issuer

    if not listed:
        return {"signal": "noise",
                "reason": f"{issuer} is not one of the US AI companies this signal follows.",
                "entities": entities,
                "metadata": {"summary": summary, "rule": "sec_issuer_not_ai_universe"}}
    if not amended:
        label, rule, code, share_price = "signal", "sec_new_stake", "m3_investment", "rise"
        reason = f"{fund} disclosed a new stake in {name}: Gulf money going into a US AI company."
    elif pct is not None and pct < _SEC_FILING_THRESHOLD_PCT:
        label, rule, code, share_price = "signal", "sec_stake_cut_below_5pct", "m3_pullback", "fall"
        reason = f"{fund} now holds {_pct(pct)} of {name}: it has sold most or all of its stake."
    else:
        label, rule, code, share_price = "weak_signal", "sec_stake_amended", "m3_investment", None
        reason = (f"{fund} updated its holding in {name}. The filing does not say what it held"
                  " before, so the size of the change is unknown.")
    if share_price:
        entities.append({"name": issuer, "ticker": ticker, "role": "affected",
                         "share_price": share_price,
                         "relationship": f"{fund} holds its shares"})
    return {"signal": label, "reason": reason, "entities": entities,
            "metadata": {"event": {"type": "M3", "code": code}, "summary": summary, "rule": rule}}


def _is_number(text: str) -> bool:
    try:
        float(text)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Step 6 - one model call per article: facts only
# ---------------------------------------------------------------------------
PROMPT_VERSION = "middle_east_signal_extract_v1"

# The whole body is sent - the 50-article test showed shorter cuts change
# the answer on 2-5 articles in 50. This cap only guards against a freak
# page (a Federal Register rule can run to hundreds of thousands of
# characters); a cut body is recorded in metadata.body_truncated.
_BODY_MAX_CHARS = 60_000

_SOURCE_DESCRIPTIONS = {
    "wam_wire": "WAM, the UAE government's news agency",
    "spa_wire": "SPA, the Saudi government's news agency",
    "substack_post": "Middle East AI News, a specialist newsletter",
    "bis_press": "a US Bureau of Industry and Security press release",
    "federal_register": "a US Federal Register notice",
}


def _source_line(article: dict[str, Any], source: str) -> str:
    if source in _SOURCE_DESCRIPTIONS:
        return _SOURCE_DESCRIPTIONS[source]
    outlet = (article.get("metadata") or {}).get("source_uri")
    return f"a news article from {outlet}" if outlet else "a news article"


def _published(article: dict[str, Any]) -> str | None:
    md = article.get("metadata") or {}
    return article.get("published") or md.get("date_time") or md.get("filing_date")


def _whole_word(name: str, text_folded: str) -> bool:
    return re.search(rf"(?<![\w&]){re.escape(name)}(?![\w&])", text_folded) is not None


def tracked_names_in(text: str, name_index: dict[str, dict[str, Any]]) -> list[str]:
    """The table companies the text names (by name or alias), canonical
    names, in table order - given to the model so it writes them the same
    way, and so it is not left to guess which names are ours."""
    folded = text.casefold()
    found: dict[str, None] = {}
    for name, company in name_index.items():
        if _whole_word(name, folded):
            found[company["company"]] = None
    return list(found)


def _user_message(article: dict[str, Any], source: str, body: str,
                  tracked: list[str]) -> str:
    hint = event_type_from_source(source)["event_type"]
    lines = [
        "<article>",
        f"Source: {_source_line(article, source)}",
        f"Published: {_published(article) or 'not stated'}",
    ]
    if hint:
        lines.append(f"Found by a search for {hint} ({MIDDLE_EAST_EVENT_TYPES[hint]}) - a hint only.")
    effective = (article.get("metadata") or {}).get("effective_on")
    if effective:
        lines.append(f"Effective date stated by the Federal Register: {effective}")
    if tracked:
        lines.append(f"Tracked companies named in the text: {', '.join(tracked)}")
    lines += [f"Headline: {article.get('title') or ''}", "Text:", body, "</article>"]
    return "\n".join(lines)


# Spend of the current run. The instructions are identical for every
# article and sent first, so the provider caches them; keep anything that
# varies per article OUT of the system prompt or that cache is lost.
_usage_lock = threading.Lock()
_usage = {"calls": 0, "prompt": 0, "cached": 0, "completion": 0, "cost": 0.0}


def _record_usage(u: dict[str, Any]) -> None:
    with _usage_lock:
        _usage["calls"] += 1
        _usage["prompt"] += u.get("prompt_tokens") or 0
        _usage["cached"] += (u.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
        _usage["completion"] += u.get("completion_tokens") or 0
        _usage["cost"] += float(u.get("cost") or 0)


def spend() -> dict[str, Any]:
    with _usage_lock:
        return dict(_usage)


def _reset_spend() -> None:
    with _usage_lock:
        _usage.update(calls=0, prompt=0, cached=0, completion=0, cost=0.0)


_RETRY_STATUS = {408, 429, 500, 502, 503, 504}


def _call_model(system: str, user: str) -> str | None:
    """One chat completion. Retried only when the request itself failed
    (timeout, rate limit, server error) - never to get a different answer.
    None when every attempt failed."""
    payload = {
        "model": config.MIDDLE_EAST_SIGNAL_MODEL,
        "temperature": 0,
        "max_tokens": 900,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": user}],
        "usage": {"include": True},
    }
    attempts = max(1, config.OPENAI_MAX_ATTEMPTS)
    for attempt in range(1, attempts + 1):
        try:
            req = Request(
                f"{config.OPENAI_BASE_URL}/chat/completions",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Authorization": f"Bearer {config.OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                method="POST")
            with urlopen(req, timeout=config.OPENAI_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            _record_usage(data.get("usage") or {})
            return (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
        except HTTPError as exc:
            if exc.code not in _RETRY_STATUS or attempt == attempts:
                logger.warning("[MIDDLE_EAST_SIGNAL] model call failed: HTTP %s", exc.code)
                return None
        except Exception as exc:  # timeout, connection reset, bad JSON envelope
            if attempt == attempts:
                logger.warning("[MIDDLE_EAST_SIGNAL] model call failed: %s", exc)
                return None
        time.sleep(2 * attempt)
    return None


# ---------------------------------------------------------------------------
# Step 7a - check the model's facts against the text
# ---------------------------------------------------------------------------
# Nothing the model writes is kept unless the text supports it. A fact that
# fails is removed - never repaired with a second call - and the label rules
# then see less, so a failed check can only LOWER a label, never raise it.

EVENT_CODES: dict[str, str] = {
    code: code[:2].upper() for code in (
        "m1_rule_or_licence", "m1_congress_action", "m1_export_category_change",
        "m1_draft_or_hearing", "m1_repeat_or_speech",
        "m2_switched_on", "m2_chips_delivered", "m2_setback", "m2_deal_or_construction",
        "m2_visit_or_ceremony",
        "m3_investment", "m3_pullback", "m3_talks",
        "m4_strike_on_site", "m4_other_strike", "m4_interception", "m4_waterway_status", "m4_shipping_or_insurance",
        "m4_ship_incident", "m4_threat_or_statement", "m4_commentary",
        "m5_qatar_force_majeure", "m5_supplier_rationing", "m5_asia_shortfall", "m5_commentary",
        "m6_binding_rule", "m6_chinese_supplier", "m6_strategy", "m6_speech",
        "m7_site_disruption", "m7_site_incident", "m7_home_front_restriction", "m7_acquisition", "m7_commentary",
        "m8_enforcement", "m8_investigation_unnamed",
    )}
_CONFIRMATIONS = ("official", "named_source", "unnamed")
_NEWS = ("good", "bad", "mixed")
_COMPANIES_MAX = 8

_PUNCT_MAP = str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-", "—": "-",
                            " ": " ", "‑": "-"})


def normalize_text(text: str) -> str:
    """Case, quote, dash and whitespace differences removed, so a quote the
    model copied is found however the page typeset it."""
    text = unicodedata.normalize("NFKC", text or "").translate(_PUNCT_MAP).casefold()
    return re.sub(r"\s+", " ", text).strip()


def _in_text(value: Any, text_norm: str) -> bool:
    return isinstance(value, str) and bool(value.strip()) and normalize_text(value) in text_norm


# A number with any letters stuck to its front ("GB300"), so "300MW" is not
# accepted because the text mentions Nvidia GB300 systems.
# A number, optionally after a currency code stuck to it ("aed2.5m") and
# before a scale word ("2.5 million", "680k"). A number with other letters
# stuck to its front ("gb300") is never a quantity: it is matched as a
# whole token instead, so "300MW" is not accepted because the text names
# Nvidia GB300 systems.
_CURRENCY = r"(?:aed|sar|qar|usd|eur|gbp|inr|rs|kwd|bhd|omr|ils|nis|us\$|\$|€|£)"
_SCALES = {"k": 1e3, "thousand": 1e3, "m": 1e6, "mn": 1e6, "million": 1e6,
           "b": 1e9, "bn": 1e9, "billion": 1e9, "tn": 1e12, "trillion": 1e12}
_QUANTITY_RE = re.compile(
    rf"(?<![a-z0-9.]){_CURRENCY}?\s?(\d[\d,]*(?:\.\d+)?)(?![\d])"
    rf"(?:\s?({'|'.join(sorted(_SCALES, key=len, reverse=True))})(?![a-z]))?")
_CODE_NUMBER_RE = re.compile(r"(?<![a-z0-9])[a-z]+(?<!aed)(?<!sar)(?<!qar)(?<!usd)\d[\d,.]*")


def _quantities(text: str) -> set[float]:
    """Every quantity in the text, both as written and scaled
    ("2.5m" -> 2.5 and 2500000)."""
    values: set[float] = set()
    for number, scale in _QUANTITY_RE.findall(text):
        value = float(number.replace(",", ""))
        values.add(value)
        if scale:
            values.add(round(value * _SCALES[scale], 6))
    return values


def _numbers_supported(sentence: str, text_norm: str) -> bool:
    """Every quantity in the sentence is in the text (as written or by
    value: "AED 2.5 million" matches "AED2.5M"), and every letter-number
    code ("GB300") appears as a whole token."""
    sentence = normalize_text(sentence)
    text_values = _quantities(text_norm)
    for number, scale in _QUANTITY_RE.findall(sentence):
        value = float(number.replace(",", ""))
        wanted = round(value * _SCALES[scale], 6) if scale else value
        if wanted not in text_values and value not in text_values:
            return False
    for code in _CODE_NUMBER_RE.findall(sentence):
        if not re.search(rf"(?<![a-z0-9]){re.escape(code)}(?![0-9])", text_norm):
            return False
    return True


_GULF_COUNTRIES = {
    "uae": "UAE", "united arab emirates": "UAE", "abu dhabi": "UAE", "dubai": "UAE",
    "saudi arabia": "Saudi Arabia", "saudi": "Saudi Arabia", "ksa": "Saudi Arabia",
    "qatar": "Qatar", "kuwait": "Kuwait", "bahrain": "Bahrain", "oman": "Oman",
}
_STRIKE_ALERT_COUNTRIES = {"UAE", "Saudi Arabia", "Qatar", "Israel"}
_HOME_FRONT_ALERT_PLACES = ("haifa", "tel aviv", "herzliya", "kiryat gat")


def country_name(country: Any) -> str | None:
    """The model's country, in the table's spelling for Gulf countries and
    Israel; other countries kept as written."""
    if not isinstance(country, str) or not country.strip():
        return None
    key = country.strip().casefold()
    if key in ("null", "none", "unknown", "n/a"):
        return None
    if key in _GULF_COUNTRIES:
        return _GULF_COUNTRIES[key]
    if key == "israel":
        return "Israel"
    return country.strip()


def _match_tracked(name: str, name_index: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    """The table company a name refers to: exact name or alias first, else
    the longest table name inside it ("Khazna Data Centers" -> G42)."""
    folded = name.casefold().strip()
    if folded in name_index:
        return name_index[folded]
    for key in sorted(name_index, key=len, reverse=True):
        if _whole_word(key, folded):
            return name_index[key]
    return None


_NAME_STOPWORDS = {"the", "of", "and", "for", "via", "its", "a", "an", "in", "on", "s"}


def _name_in_text(name: str, text_norm: str) -> bool:
    """A source name the text supports: the name as written, or every
    significant word of it as a whole word ("IRGC Navy" when the text says
    "the IRGC's navy"). An invented name fails both."""
    if normalize_text(name) in text_norm:
        return True
    words = [w for w in re.findall(r"[\w-]+", normalize_text(name)) if w not in _NAME_STOPWORDS]
    return bool(words) and all(_whole_word(w, text_norm) for w in words)


_ANONYMOUS_RE = re.compile(
    r"\b(?:people|persons?|sources?|insiders?|familiar|aware|anonymity|unnamed|"
    r"officials? (?:said|say)|according to reports)\b", re.IGNORECASE)


def outlet_name(article: dict[str, Any], source: str) -> str | None:
    """The publisher's own name, case-folded: what can never confirm its
    own story ("tokenpost" for tokenpost.com, "wam" for the UAE wire)."""
    if source in ("wam_wire", "spa_wire"):
        return source.split("_")[0]
    uri = (article.get("metadata") or {}).get("source_uri") or ""
    stem = uri.split(".")[-2] if uri.count(".") >= 1 else uri
    return stem.casefold() or None


def check_extraction(raw: str | None, title: str, body: str,
                     name_index: dict[str, dict[str, Any]],
                     counterparty_tickers: dict[str, str],
                     outlet: str | None = None) -> tuple[dict[str, Any] | None, list[str]]:
    """The model's answer, kept only where the text supports it.

    Returns (extracted, dropped): `extracted` is None when the answer is not
    usable at all (not JSON, or an event code we do not have); `dropped`
    names every item removed, for the run log.
    """
    try:
        data = json.loads(raw or "")
    except (TypeError, ValueError):
        return None, ["not_json"]
    if not isinstance(data, dict):
        return None, ["not_an_object"]
    # event: {"code", "place", "effective_date", "lasts_over_24_hours"};
    # deal: {"level", "amount", "up_to", "capacity"}. A bare code string is
    # accepted too, with the other fields read from the top level.
    raw_event = data.get("event")
    raw_event = raw_event if isinstance(raw_event, dict) else {"code": raw_event, **data}
    raw_deal = data.get("deal")
    raw_deal = raw_deal if isinstance(raw_deal, dict) else {}
    event = raw_event.get("code")
    if event in (None, "", "none"):
        event = None
    elif event not in EVENT_CODES:
        return None, [f"unknown_event:{event}"]

    text_norm = normalize_text(f"{title}\n{body}")
    dropped: list[str] = []

    def sentences(field: str) -> str | None:
        """The model's prose for `field`, or None when it states a number
        the text does not contain."""
        value = data.get(field) if isinstance(data.get(field), str) else ""
        value = re.sub(r"\s+", " ", value).strip()
        if not value or not _numbers_supported(value, text_norm):
            dropped.append(field)
            return None
        return value if value.endswith((".", "!", "?")) else value + "."

    headline = f'"{title.strip()}".'
    summary = sentences("summary") or f"The article is headlined {headline}"
    # reason_facts opens signal_reason; if it fails its check, the
    # summary's first sentence (already checked) stands in, then the headline.
    reason_facts = (sentences("reason_facts")
                    or (re.split(r"(?<=[.!?])\s", summary, maxsplit=1)[0]
                        if "summary" not in dropped else f"Headline: {headline}"))

    if event is None:
        return {"reason_facts": reason_facts, "summary": summary}, dropped

    companies: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in data.get("companies") or []:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            continue
        name = item["name"].strip()
        if not _in_text(name, text_norm):
            dropped.append(f"company:{name}")
            continue
        tracked = _match_tracked(name, name_index)
        canonical = tracked["company"] if tracked else name
        if canonical.casefold() in seen:
            continue
        seen.add(canonical.casefold())
        companies.append({
            "name": canonical,
            "ticker": tracked["ticker"] if tracked else counterparty_tickers.get(name.casefold()),
            "country": tracked["country"] if tracked else country_name(item.get("country")),
            "tracked": tracked is not None,
        })
        if len(companies) == _COMPANIES_MAX:
            break

    def copied(value: Any, name: str) -> str | None:
        """`value` if the text contains it, else None (and logged)."""
        if value is None:
            return None
        if _in_text(value, text_norm):
            return value.strip()
        dropped.append(name)
        return None

    # confirmation: {"level", "by"}. A level above "unnamed" needs a `by`
    # the text contains - a confirmation nobody in the article gave is not
    # one, so it falls to "unnamed" (which can only lower a label).
    conf = data.get("confirmation")
    conf = conf if isinstance(conf, dict) else {"level": conf}
    level_c = conf.get("level")
    if level_c not in _CONFIRMATIONS:
        dropped.append("confirmation")
        level_c = "unnamed"
    by = conf.get("by") if isinstance(conf.get("by"), str) and conf.get("by").strip() else None
    # Checked against the BODY: a headline often ends "| <outlet>".
    if by and not _name_in_text(by, normalize_text(body)):
        dropped.append(f"confirmation.by:{by}")
        by = None
    # "two people aware of the plans" names nobody.
    if by and _ANONYMOUS_RE.search(by):
        dropped.append(f"confirmation.by_anonymous:{by}")
        by = None
    if level_c != "unnamed" and not by:
        dropped.append("confirmation.level")
        level_c = "unnamed"
    # The outlet reporting a story does not confirm it: at most a named source.
    if by and level_c == "official" and outlet and _whole_word(outlet, normalize_text(by)):
        dropped.append("confirmation.outlet_not_official")
        level_c = "named_source"
    confirmation = {"level": level_c, **({"by": by} if by else {})}

    news_effect = data.get("news_effect", data.get("news"))
    if news_effect not in _NEWS:
        dropped.append("news_effect")
        news_effect = "mixed"               # no direction rather than a guessed one
    deal_level = raw_deal.get("level", data.get("deal_level"))
    if not (isinstance(deal_level, int) and not isinstance(deal_level, bool) and 1 <= deal_level <= 6):
        deal_level = None
    # Spec 9.1: "If the only source is unnamed, the highest level is 3."
    if deal_level and level_c == "unnamed" and deal_level > 3:
        deal_level = 3
    new_development = data.get("new_development")
    if not isinstance(new_development, bool):
        new_development = True              # unknown: judged as new, not hidden as a repeat
    lasts = raw_event.get("lasts_over_24_hours", raw_event.get("over_24_hours"))
    if not isinstance(lasts, bool):
        lasts = None

    raw_amount = raw_deal.get("amount", data.get("amount"))
    up_to = raw_deal.get("up_to")
    if isinstance(raw_amount, dict):            # the older {"value", "up_to"} shape
        raw_amount, up_to = raw_amount.get("value"), raw_amount.get("up_to")
    amount = copied(raw_amount, "deal.amount")

    def present(d: dict[str, Any]) -> dict[str, Any]:
        """Absent means not stated: nothing is stored as null."""
        return {k: v for k, v in d.items() if v is not None}

    event_obj = present({
        "type": EVENT_CODES[event], "code": event,
        "place": copied(raw_event.get("place"), "event.place"),
        "effective_date": copied(raw_event.get("effective_date"), "event.effective_date"),
        "lasts_over_24_hours": lasts,
    })
    # up_to means nothing without the amount it qualifies.
    deal = present({
        "level": deal_level, "amount": amount,
        "up_to": (up_to is True) if amount else None,
        "capacity": copied(raw_deal.get("capacity", data.get("capacity")), "deal.capacity"),
    })
    extracted = {
        "event": event_obj,
        "companies": companies,
        "deal": deal or None,
        "confirmation": confirmation,
        "new_development": new_development,
        "news_effect": news_effect,
        "reason_facts": reason_facts,
        "summary": summary,
    }
    return {k: v for k, v in extracted.items() if v is not None}, dropped


# ---------------------------------------------------------------------------
# Step 7b - the label, from spec section 7
# ---------------------------------------------------------------------------
# One branch per spec row. Each returns (label, rule, sentence): `rule` is
# stored so every row says which branch decided it; the sentence is the
# second half of signal_reason.

_EVENT_TYPE_LIST = ("US chip-sale decisions, Gulf data centres, Gulf money into US AI,"
                    " war and shipping, helium and gas, Gulf rules and supplier choices,"
                    " Israel's chip and AI sites, and enforcement against Gulf companies")


def event_code(ex: dict[str, Any]) -> str | None:
    return (ex.get("event") or {}).get("code")


def event_type(ex: dict[str, Any]) -> str | None:
    return (ex.get("event") or {}).get("type")


def _event(ex: dict[str, Any], key: str) -> Any:
    return (ex.get("event") or {}).get(key)


def _deal(ex: dict[str, Any], key: str) -> Any:
    return (ex.get("deal") or {}).get(key)


def _amount(ex: dict[str, Any]) -> str | None:
    return _deal(ex, "amount")


def _up_to(ex: dict[str, Any]) -> bool:
    return bool(_deal(ex, "up_to"))


def _gulf_company(ex: dict[str, Any]) -> dict[str, Any] | None:
    return next((c for c in ex.get("companies", [])
                 if c["country"] in set(_GULF_COUNTRIES.values())), None)


def _committed_amount(ex: dict[str, Any]) -> bool:
    return bool(_amount(ex)) and not _up_to(ex)


def _place_country(ex: dict[str, Any]) -> str | None:
    place = (_event(ex, "place") or "").casefold()
    for key, country in {**_GULF_COUNTRIES, "israel": "Israel", "haifa": "Israel",
                         "tel aviv": "Israel", "eilat": "Israel", "ashdod": "Israel",
                         "ras laffan": "Qatar", "fujairah": "UAE", "jebel ali": "UAE",
                         "ras tanura": "Saudi Arabia", "abqaiq": "Saudi Arabia",
                         "jeddah": "Saudi Arabia", "riyadh": "Saudi Arabia",
                         "doha": "Qatar", "ruwais": "UAE"}.items():
        if _whole_word(key, place):
            return country
    return None


_QATAR_RE = re.compile(r"\b(?:Qatar|QatarEnergy|Ras Laffan)\b", re.IGNORECASE)
# QatarEnergy's own action, in the headline: declares / extends / lifts /
# ends a force majeure, or restarts. Measured on the 403-article run: every
# wrong "signal" headline named Qatar but reported a consequence ("exports
# plunge 96%", "Pakistan ... amid Qatar force majeure"); the one real
# announcement was "Qatar extends Force Majeure".
_QATAR_ACTION_RE = re.compile(
    r"\b(?:declar|extend|lift|end)\w*\s+(?:its\s+|the\s+)?force majeure"
    r"|force majeure\s+(?:is\s+|was\s+|has been\s+)?(?:declar|extend|lift|end)\w*"
    r"|\b(?:restart|resum|reopen)\w*", re.IGNORECASE)


def _qatar_announcement_headline(title: str) -> bool:
    return bool(_QATAR_RE.search(title) and _QATAR_ACTION_RE.search(title))


# What makes a Gulf rule an M6 rule: AI, data or chips. Added after the
# Saudi Red Sea Authority's recreational boat-chartering rules were labelled
# a binding Gulf AI rule.
_AI_DATA_RE = re.compile(
    r"\bAI\b|artificial intelligence|personal data|data (?:protection|centres?|centers?|"
    r"locali[sz]ation|residency|privacy|governance|sovereignty)|\bcloud\b|algorithm|"
    r"semiconductor|\bchips?\b", re.IGNORECASE)


_HARDWARE_RE = re.compile(
    r"data[ -]?cent(?:re|er)s?|\bgpus?\b|\bchips?\b|semiconductor|\bservers?\b|megawatt|"
    r"gigawatt|\d\s?(?:mw|gw)\b|compute capacity|computing capacity|\bcampus\b|"
    r"accelerators?|supercomputer", re.IGNORECASE)


def _is_hardware_deal(ex: dict[str, Any], body: str) -> bool:
    """A data-centre deal names a figure or at least talks about hardware:
    capacity or an amount, or chips / servers / data centres in the body."""
    return bool(_deal(ex, "capacity") or _amount(ex) or _HARDWARE_RE.search(body or ""))


# A headline that reacts to an event ("UN Security Council Members Strongly
# Condemn Houthi Attacks...") is never the first report of it: the model
# coded such a statement as a confirmed strike on the 403-article run.
_REACTION_HEADLINE_RE = re.compile(
    r"\b(?:condemn\w*|denounc\w*|welcom\w*|condolence\w*|solidarity|deplor\w*|"
    r"urges? restraint|expresses? concern)\b", re.IGNORECASE)


def decide_label(ex: dict[str, Any], title: str = "", body: str | None = None) -> tuple[str, str, str]:
    """(label, rule, why) - see _decide_label. A reaction headline is capped
    at weak_signal: the event itself is alerted from its own report."""
    label, rule, why = _decide_label(ex, title, body)
    if label == "signal" and _REACTION_HEADLINE_RE.search(title or ""):
        return ("weak_signal", f"{rule}_reaction_headline",
                "The article is a statement reacting to the event, not the first report of it.")
    return label, rule, why


# Who controls each waterway: Iran (Hormuz), the Houthis (Red Sea, Bab el-Mandeb).
_CONTROLLING_PARTY_RE = re.compile(
    r"\b(?:iran\w*|irgc|islamic revolutionary guard\w*|revolutionary guard\w*|tehran|"
    r"houthi\w*|ansar allah|ansarallah)\b", re.IGNORECASE)
# A named ship tracker with a count of ships or transits.
_SHIP_COUNT_RE = re.compile(
    r"\b(?:portwatch|kpler|vortexa|marinetraffic|windward|lloyd's list|ukmto)\b"
    r"[^.]{0,200}?\b\d[\d,]*\s+(?:ships|vessels|transits|tankers|crossings)\b"
    r"|\b\d[\d,]*\s+(?:ships|vessels|transits|tankers|crossings)\b[^.]{0,200}?"
    r"\b(?:portwatch|kpler|vortexa|marinetraffic|windward|lloyd's list|ukmto)\b",
    re.IGNORECASE)


def _decide_label(ex: dict[str, Any], title: str = "", body: str | None = None) -> tuple[str, str, str]:
    """(label, rule, why) from the checked facts.

    `why` is the second half of signal_reason, shown to readers in the UI,
    so it is plain language: no spec section numbers, no deal levels, no
    rule names. The first half - what happened, with names and figures -
    is the model's `reason_facts`. `rule` is the internal name of the
    branch, stored in metadata for us, never shown.

    `title` is the article's headline - one rule needs to know the event
    is the article's main subject, not background."""
    event = event_code(ex)
    if event is None:
        # The model's reason_facts already says, for THIS article, why it
        # changes nothing - a fixed sentence here contradicted articles that
        # are about Gulf data centres or helium but report no event.
        return ("noise", "not_an_event_type", "")
    confirmed_by = (ex.get("confirmation") or {}).get("level", "unnamed")
    official = confirmed_by == "official"
    named = confirmed_by in ("official", "named_source")
    level = _deal(ex, "level") or 0

    if not ex.get("new_development", True):
        if event_type(ex) == "M8":   # spec 7.9: an action naming a Gulf company is never ignored
            return ("weak_signal", "m8_repeat",
                    "This repeats an earlier US action against a Gulf company; such actions are always kept.")
        return ("noise", "repeat", "This repeats earlier news; nothing new has happened.")

    unconfirmed = "Only unnamed sources report it so far."

    # --- M1 US decisions on Gulf chip sales (spec 7.1)
    if event == "m1_rule_or_licence":
        if official and _event(ex, "effective_date"):
            return ("signal", "m1_final_with_date",
                    f"It is a final US decision and takes effect {_event(ex, 'effective_date')}.")
        if not named:
            return ("weak_signal", "m1_unnamed_officials", unconfirmed)
        return ("weak_signal", "m1_no_effective_date",
                "No date is given for when it takes effect, so shipments cannot be planned on it yet.")
    if event == "m1_congress_action":
        if named and (_gulf_company(ex) or _amount(ex)):
            return ("signal", "m1_congress_named_deal",
                    "It is formal action in Congress aimed at a specific Gulf deal or company.")
        return ("weak_signal", "m1_congress_no_named_deal",
                "It names no specific Gulf deal or company, so no shipment is held up yet.")
    if event == "m1_export_category_change":
        if official:
            return ("signal", "m1_export_category_change",
                    "Changing a country's US export category changes how easily US chips can ship there.")
        return ("weak_signal", "m1_export_category_unconfirmed", unconfirmed)
    if event == "m1_draft_or_hearing":
        return ("weak_signal", "m1_draft_or_hearing",
                "It is a draft, a hearing or an unconfirmed report, not a final US decision.")
    if event == "m1_repeat_or_speech":
        return ("noise", "m1_repeat_or_speech",
                "It is a speech or a rule already in force; no US decision has changed.")

    # --- M2 Gulf data centres (spec 7.3, deal levels 7.2)
    if event == "m2_switched_on":
        if _deal(ex, "capacity") and named:
            return ("signal", "m2_switched_on_with_capacity",
                    "The capacity is running now, which shows Gulf AI orders turning into real hardware sales.")
        return ("weak_signal", "m2_no_capacity_figure",
                "No figure is given for how much capacity is running.")
    if event == "m2_chips_delivered":
        if named and _gulf_company(ex):
            return ("signal", "m2_chips_delivered",
                    "The chips have arrived, which shows a Gulf order turning into real sales.")
        return ("weak_signal", "m2_delivery_unconfirmed",
                "The delivery is not confirmed for a named Gulf buyer.")
    if event == "m2_setback":
        if named:
            return ("signal", "m2_setback",
                    "A delay, cut or cancellation pushes back hardware sales that were expected.")
        return ("weak_signal", "m2_setback_unconfirmed", unconfirmed)
    if event == "m2_deal_or_construction":
        if body is not None and not _is_hardware_deal(ex, body):
            # The model kept calling software / agentic-AI partnerships
            # (OrbitronAI-Aramco Digital) data-centre deals despite the prompt.
            return ("noise", "m2_not_hardware",
                    "It is a software or services partnership; it buys no chips, servers or"
                    " data-centre capacity.")
        if level == 1:
            return ("noise", "m2_pledge",
                    "It is a headline spending figure with no named contract behind it.")
        stage = ("The order is signed, but the capacity is not built or running yet." if level >= 4 else
                 "It is a partnership or plan, not a signed order." if level else
                 "It is a plan or construction update; nothing is running yet.")
        return ("weak_signal", "m2_deal_below_delivery", stage)
    if event == "m2_visit_or_ceremony":
        return ("noise", "m2_visit_or_ceremony",
                "It is a visit, a ceremony or a repeated target; no capacity has changed.")

    # --- M3 Gulf money into US AI (spec 7.4)
    if event == "m3_investment":
        if level == 1:
            return ("noise", "m3_pledge", "It is a headline pledge with no named deal behind it.")
        if level >= 4 and _committed_amount(ex) and named:
            return ("signal", "m3_completed_with_amount",
                    "It is a completed investment with the amount stated: more money for AI spending.")
        why = ("the amount is only an \"up to\" figure" if _up_to(ex)
               else "no amount is stated" if not _amount(ex)
               else "it is not signed or completed yet" if level < 4
               else "only unnamed sources report it")
        return ("weak_signal", "m3_investment_not_alertable",
                f"It is Gulf money for US AI, but {why}.")
    if event == "m3_pullback":
        if named:
            return ("signal", "m3_pullback",
                    "A Gulf fund pulling money out means less fuel for AI spending.")
        return ("weak_signal", "m3_pullback_unconfirmed", unconfirmed)
    if event == "m3_talks":
        return ("weak_signal", "m3_talks", "These are talks; no money has been committed yet.")

    # --- M4 War and shipping (spec 7.5)
    if event == "m4_strike_on_site":
        country = _place_country(ex)
        if official and country in _STRIKE_ALERT_COUNTRIES:
            return ("signal", "m4_strike_confirmed",
                    f"A government or the site's operator confirms the strike on energy, port or"
                    f" data-centre infrastructure in {country}.")
        if official:
            return ("weak_signal", "m4_strike_outside_alert_countries",
                    "The strike was not in the UAE, Saudi Arabia, Qatar or Israel, where the"
                    " infrastructure US AI companies depend on sits.")
        return ("weak_signal", "m4_strike_claimed",
                "Only one side or unnamed sources say it happened; no government or site operator"
                " has confirmed it.")
    if event == "m4_interception":
        return ("noise", "m4_interception", "The missiles or drones were intercepted; nothing was hit.")
    if event == "m4_other_strike":
        if named:
            return ("weak_signal", "m4_other_strike",
                    "No energy plant, port or data centre was hit.")
        return ("noise", "m4_other_strike_unnamed", "No named source confirms it.")
    if event == "m4_waterway_status":
        # Departs from spec 7.5 (user decision 2026-10-22): the spec alerts a
        # closure or reopening only once ship counts confirm it within 72h,
        # and this module has no ship-count feed - so the IRGC's declaration
        # that Hormuz was closed to all shipping was held at weak_signal.
        # Now: ship counts quoted in the article from a named tracker
        # confirm it; otherwise the party that CONTROLS the waterway
        # declaring it alerts too, marked as not yet confirmed by traffic.
        by = (ex.get("confirmation") or {}).get("by") or ""
        if named and body is not None and _SHIP_COUNT_RE.search(body):
            return ("signal", "m4_waterway_confirmed_by_ship_counts",
                    "Ship-tracking figures in the article show the change in traffic.")
        if official and _CONTROLLING_PARTY_RE.search(by):
            return ("signal", "m4_waterway_declared_by_controlling_party",
                    "Declared by the side that controls the waterway; ship traffic has not yet"
                    " confirmed it.")
        return ("weak_signal", "m4_waterway_unconfirmed_by_ship_counts",
                "The statement does not come from the side that controls the waterway, and no"
                " ship-tracking figures confirm it.")
    if event == "m4_shipping_or_insurance":
        if named:
            return ("signal", "m4_shipping_line_or_insurer",
                    "Shipping lines and insurers changing course moves freight costs and delivery times"
                    " for oil, gas and chip supplies.")
        return ("weak_signal", "m4_shipping_unconfirmed", unconfirmed)
    if event == "m4_ship_incident":
        if named:
            return ("weak_signal", "m4_ship_incident",
                    "Ships were hit, but no port or energy site was, and no shipping line has"
                    " stopped the route.")
        return ("noise", "m4_ship_incident_unnamed", "No named source confirms it.")
    if event == "m4_threat_or_statement":
        if named:
            return ("weak_signal", "m4_threat_or_statement", "It is a threat or statement; nothing has happened yet.")
        return ("noise", "m4_threat_or_statement_unnamed", "No named source stands behind it.")
    if event == "m4_commentary":
        return ("noise", "m4_commentary", "It is commentary on the war, not a new event.")

    # --- M5 Helium and gas supply (spec 7.6)
    if event == "m5_qatar_force_majeure" and not _qatar_announcement_headline(title):
        # The model repeatedly took a force majeure mentioned as background
        # (in a story about Oman's LNG) for the article's event.
        return ("weak_signal", "m5_force_majeure_in_background",
                "Qatar's force majeure appears only as background to this story.")
    if event == "m5_qatar_force_majeure":
        if official:
            return ("signal", event,
                    "QatarEnergy announced it itself; Ras Laffan also produces the helium chip factories use.")
        return ("weak_signal", f"{event}_unconfirmed",
                "QatarEnergy has not announced it itself.")
    if event == "m5_supplier_rationing":
        if named:
            return ("signal", event,
                    "Helium rationing or surcharges raise costs for chip factories.")
        return ("weak_signal", f"{event}_unconfirmed", unconfirmed)
    if event == "m5_asia_shortfall":
        if official:
            return ("signal", "m5_asia_shortfall",
                    "Chip factories cannot run without helium.")
        return ("weak_signal", "m5_asia_shortfall_unconfirmed",
                "The shortfall has not been announced by the company or government itself.")
    if event == "m5_commentary":
        return ("weak_signal", "m5_commentary",
                "It is commentary on helium or gas supply, not a change in supply.")

    # --- M6 Gulf rules and supplier choices (spec 7.7)
    if event == "m6_binding_rule":
        if body is not None and not _AI_DATA_RE.search(f"{title}\n{body}"):
            return ("noise", "m6_not_ai_or_data",
                    "It is a Gulf rule, but not about AI, data or chips.")
        if official and _event(ex, "effective_date"):
            return ("signal", "m6_binding_rule_with_date",
                    f"It is a binding rule that takes effect {_event(ex, 'effective_date')}.")
        return ("weak_signal", "m6_rule_no_date", "No start date is given for the rule.")
    if event == "m6_chinese_supplier":
        if level >= 4 and named:
            return ("signal", "m6_chinese_supplier_signed",
                    "A signed deal with a Chinese supplier is a sale US chip and cloud companies did not win.")
        return ("weak_signal", "m6_chinese_supplier_not_signed",
                "The deal with the Chinese supplier is not signed yet.")
    if event == "m6_strategy":
        return ("weak_signal", "m6_strategy", "It is a strategy or plan; no binding rule has been set.")
    if event == "m6_speech":
        return ("noise", "m6_speech", "It is a speech; nothing has changed.")

    # --- M7 Israel (spec 7.8)
    if event == "m7_site_disruption":
        # A tracked Israeli company, or a listed one with Israeli sites
        # (Intel's Kiryat Gat fab, Nvidia's Israeli design centres).
        if official and any(c["tracked"] or c["ticker"] for c in ex.get("companies", [])):
            return ("signal", "m7_site_disruption",
                    "The company itself reports the disruption at a site it depends on.")
        return ("weak_signal", "m7_disruption_unconfirmed",
                "The disruption is not confirmed by a listed chip or AI company.")
    if event == "m7_site_incident":
        # Departs from spec 7.8 (user decision 2026-10-22), which wants the
        # company itself to announce a stop: police, fire and ambulance
        # services confirmed the Intel Kiryat Gat fab explosion the same day
        # and Intel's shares fell ~4.5%, while Intel never announced a stop.
        if official and any(c["tracked"] or c["ticker"] for c in ex.get("companies", [])):
            return ("signal", "m7_site_incident_confirmed",
                    "Officials confirm the incident at a listed chip company's site in Israel.")
        return ("weak_signal", "m7_site_incident", "No official body has confirmed it yet.")
    if event == "m7_home_front_restriction":
        place = (_event(ex, "place") or "").casefold()
        if _event(ex, "lasts_over_24_hours") and any(p in place for p in _HOME_FRONT_ALERT_PLACES):
            return ("signal", "m7_home_front_over_24h",
                    "The restrictions last more than a day in an area with major chip and AI sites.")
        return ("weak_signal", "m7_home_front_short_or_elsewhere",
                "The restrictions are short or outside Haifa, Tel Aviv-Herzliya and Kiryat Gat,"
                " where the main chip and AI sites are.")
    if event == "m7_acquisition":
        if level >= 4 and _committed_amount(ex) and named:
            return ("signal", "m7_acquisition_with_price",
                    "The deal is agreed and the price is stated.")
        return ("weak_signal", "m7_acquisition_talks",
                "The deal is not agreed yet, or no price is stated.")
    if event == "m7_commentary":
        return ("noise", "m7_commentary", "It is commentary on the war, not a new event.")

    # --- M8 Enforcement against a named Gulf company (spec 7.9)
    if event == "m8_enforcement":
        gulf = _gulf_company(ex)
        if gulf:
            return ("signal", "m8_enforcement",
                    f"US action against {gulf['name']} decides whether US chips can ship to it.")
        # The model once called a Russian airline's denial order M8.
        return ("noise", "m8_no_gulf_company", "It involves no Gulf company.")
    return ("weak_signal", "m8_investigation_unnamed",
            "No Gulf company is named in the investigation yet.")


# ---------------------------------------------------------------------------
# Step 7c - read-through: which US companies, which way
# ---------------------------------------------------------------------------
# Two sources, company-specific first:
#  1. each tracked company the article is about -> its counterparties from
#     the company table (share_price_on_good_news, reversed on bad news);
#  2. the spec's events table row for this event type and side.
# A ticker already given by (1) is not repeated by (2). Mixed news gets no
# direction at all - a guessed direction is worse than none.
_FLIP = {"rise": "fall", "fall": "rise"}

# Which spec "At a glance" row(s) each event code moves, per side of the
# news. A code not listed (a construction contract, a ship incident with no
# route change, a threat, a software partnership) moves no row: the spec
# ties those stock moves to specific events, and a copied template
# ("more AI hardware is actually sold" on a construction contract) told
# readers something the article did not.
_ROWS_BY_CODE: dict[tuple[str, str], list[str]] = {
    ("m1_rule_or_licence", "good"): ["The US approves chip sales to a Gulf country"],
    ("m1_export_category_change", "good"): ["The US approves chip sales to a Gulf country"],
    ("m1_rule_or_licence", "bad"): ["Congress blocks a deal, or a smuggling case names a Gulf firm"],
    ("m1_congress_action", "bad"): ["Congress blocks a deal, or a smuggling case names a Gulf firm"],
    ("m8_enforcement", "bad"): ["Congress blocks a deal, or a smuggling case names a Gulf firm"],
    ("m2_switched_on", "good"): ["A Gulf data centre switches on, or receives its chips"],
    ("m2_chips_delivered", "good"): ["A Gulf data centre switches on, or receives its chips"],
    ("m2_setback", "bad"): ["A Gulf data centre is delayed, damaged or cancelled"],
    ("m3_investment", "good"): ["A Gulf sovereign fund invests in a US AI company"],
    ("m3_pullback", "bad"): ["A Gulf fund pulls back or misses a commitment"],
    ("m4_strike_on_site", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m4_waterway_status", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m7_site_incident", "bad"): ["Missile strikes or shutdowns in Israel"],
    ("m4_shipping_or_insurance", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m4_waterway_status", "good"): ["A ceasefire holds and ships move through Hormuz again"],
    ("m4_shipping_or_insurance", "good"): ["A ceasefire holds and ships move through Hormuz again"],
    ("m5_qatar_force_majeure", "bad"): ["Qatar's helium plant stays offline",
                                        "Qatar's helium plant stays offline (other side)"],
    ("m5_supplier_rationing", "bad"): ["Qatar's helium plant stays offline",
                                       "Qatar's helium plant stays offline (other side)"],
    ("m5_asia_shortfall", "bad"): ["Qatar's helium plant stays offline"],
    ("m6_chinese_supplier", "bad"): ["A Gulf buyer picks a Chinese chip or cloud supplier"],
    ("m7_site_disruption", "bad"): ["Missile strikes or shutdowns in Israel"],
    ("m7_home_front_restriction", "bad"): ["Missile strikes or shutdowns in Israel"],
}
# The good side of the helium rows (the plant restarts), which the spec's
# table does not list but its shipping card (8.1) words: directions
# reversed, and the "why" rewritten to match - a reversed direction under
# the original wording read as nonsense.
_HELIUM_RESTART_WHY = {
    "Qatar's helium plant stays offline": "Helium supply to chip factories is recovering",
    "Qatar's helium plant stays offline (other side)": "Helium shortage surcharges are likely to end",
}
_SPEC_REF_RE = re.compile(r"\s*\((?:spec|section)[^)]*\)", re.IGNORECASE)


def _event_rows(code: str, news: str) -> list[dict[str, Any]]:
    by_name = {r["event"]: r for r in MIDDLE_EAST_EVENTS}
    if code == "m5_qatar_force_majeure" and news == "good":
        return [{**by_name[name], "why": why,
                 "affected_companies": [{**c, "share_price": _FLIP[c["share_price"]]}
                                        for c in by_name[name]["affected_companies"]]}
                for name, why in _HELIUM_RESTART_WHY.items()]
    return [by_name[name] for name in _ROWS_BY_CODE.get((code, news), [])]


def plain_relationship(text: str) -> str:
    """A counterparty relationship as shown to readers: the table's
    "(spec 4.3)" citations are for us, not them."""
    return _SPEC_REF_RE.sub("", text or "").strip()


def read_through(ex: dict[str, Any], name_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if event_code(ex) is None or ex.get("news_effect") not in ("good", "bad"):
        return []
    affected: dict[str, dict[str, Any]] = {}
    # A fund's table counterparties (Mubadala -> GlobalFoundries) describe
    # what it already owns, not what a NEW investment moves: for M3 only the
    # events table row applies.
    companies = [] if event_type(ex) == "M3" else ex.get("companies", [])
    for c in companies:
        row = name_index.get(c["name"].casefold()) if c["tracked"] else None
        for cp in (row or {}).get("counterparties", []):
            share = cp["share_price_on_good_news"]
            affected.setdefault(cp["counterparty_ticker"], {
                "name": cp["counterparty_name"], "ticker": cp["counterparty_ticker"],
                "role": "affected",
                "share_price": share if ex["news_effect"] == "good" else _FLIP[share],
                "relationship": plain_relationship(cp["relationship"])})
    for row in _event_rows(event_code(ex), ex["news_effect"]):
        for c in row["affected_companies"]:
            affected.setdefault(c["ticker"], {
                "name": c["company_name"], "ticker": c["ticker"], "role": "affected",
                "share_price": c["share_price"], "relationship": row["why"]})
    return list(affected.values())


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------
_FALLBACK_REASON = ("This article could not be assessed automatically: the model's answer"
                    " could not be read. It is kept as possibly relevant rather than hidden.")


def _model_result(article: dict[str, Any], source: str, system: str,
                  name_index: dict[str, dict[str, Any]],
                  counterparty_tickers: dict[str, str]) -> dict[str, Any] | None:
    title = article.get("title") or ""
    body = article.get("body") or ""
    truncated = len(body) > _BODY_MAX_CHARS
    body = body[:_BODY_MAX_CHARS]
    tracked = tracked_names_in(f"{title}\n{body}", name_index)
    raw = _call_model(system, _user_message(article, source, body, tracked))
    if raw is None:
        return None                       # not stored: the next run retries it
    base: dict[str, Any] = {"extracted_by": {"model": config.MIDDLE_EAST_SIGNAL_MODEL,
                                             "prompt": PROMPT_VERSION}}
    if truncated:
        base["body_truncated"] = True
    ex, dropped = check_extraction(raw, title, body, name_index, counterparty_tickers,
                                   outlet_name(article, source))
    if dropped:
        logger.info("[MIDDLE_EAST_SIGNAL] dropped from model answer url=%s: %s",
                    article.get("url"), ", ".join(dropped))
    if ex is None:
        logger.warning("[MIDDLE_EAST_SIGNAL] unusable model answer url=%s: %s",
                       article.get("url"), dropped)
        return {"signal": "weak_signal", "reason": _FALLBACK_REASON, "entities": [],
                "metadata": {**base, "rule": "model_output_unusable"}}

    label, rule, sentence = decide_label(ex, title, body)
    # Each model field is stored ONCE, in the column made for it:
    #  - companies -> entities_json (role "subject"), with the read-through
    #    companies (role "affected") - so entity_names_normalized finds a row
    #    by "g42" or by "nvidia";
    #  - summary (what happened) -> metadata.summary. It is NOT the reason:
    #    signal_reason says why the row got its label - the rule's sentence,
    #    then which US companies are likely to rise or fall;
    #  - every other field the label rules read -> metadata, in the shape
    #    the model returned it (event, amount and confirmation are objects),
    #    absent when not stated - nothing is stored as null. With these, a
    #    rule change re-labels a stored row without a model call.
    entities = [{"name": c["name"], "ticker": c["ticker"], "country": c["country"],
                 "role": "subject"} for c in ex.get("companies", [])]
    reason = " ".join(part for part in (ex.pop("reason_facts"), sentence) if part)
    if label != "noise":
        entities += read_through(ex, name_index)
    stored = {k: v for k, v in ex.items() if k != "companies"}
    return {"signal": label, "reason": reason, "entities": entities,
            "metadata": {**base, **stored, "rule": rule}}


def counterparty_ticker_index(companies: list[dict[str, Any]]) -> dict[str, str]:
    """US counterparty names (case-folded) -> ticker, so a subject the model
    names as "Nvidia" carries NVDA."""
    return {cp["counterparty_name"].casefold(): cp["counterparty_ticker"]
            for c in companies for cp in c["counterparties"]}


def classify_middle_east_signal_batch(
    articles: list[dict[str, Any]],
    companies: list[dict[str, Any]],
    us_universe: list[dict[str, Any]] | None = None,
    stored_event_members: dict[str, list[dict[str, Any]]] | None = None,
) -> list[dict[str, Any]]:
    """Classify new articles against the company table (already checked by
    company_table_problems). Returns one {"article", "result"} per article
    judged; an article left out is NOT stored and is retried next run:
      - the model call failed (timeout, outage);
      - a 13D/G arrived while research-universe (`us_universe`) was
        unavailable - its label depends on that list.
    `stored_event_members` is {event_uri: rows already stored} for this
    batch's newsapi.ai event clusters - see corroboration_for().
    """
    _reset_spend()
    name_index = company_name_index(companies)
    cp_tickers = counterparty_ticker_index(companies)
    universe = us_universe_index(us_universe) if us_universe is not None else None
    system = config.MIDDLE_EAST_SIGNAL_PROMPT.read_text(encoding="utf-8")

    out: list[dict[str, Any] | None] = [None] * len(articles)
    to_model: list[int] = []
    paths: Counter = Counter()
    for i, article in enumerate(articles):
        source = source_name(article)
        if source == "sec_fund_stake":
            if universe is None:
                paths["sec_waiting_for_universe"] += 1
                continue
            out[i] = {"article": article, "result": judge_sec_stake(article, universe)}
            paths["sec_rule"] += 1
            continue
        skipped = headline_rule(article, source)
        if skipped:
            rule, reason = skipped
            out[i] = {"article": article, "result": {
                "signal": "noise", "reason": reason, "entities": [],
                "metadata": {"rule": rule}}}
            paths[f"headline_{rule}"] += 1
            continue
        to_model.append(i)

    def judge(i: int) -> dict[str, Any] | None:
        article = articles[i]
        try:
            return _model_result(article, source_name(article), system, name_index, cp_tickers)
        except Exception:
            logger.exception("[MIDDLE_EAST_SIGNAL] classify failed url=%s", article.get("url"))
            return None

    # The first call goes alone so the provider has cached the instructions
    # before the parallel calls start; sent together, they all miss the
    # cache (measured: 3% cached on an 18-article run).
    results = [judge(i) for i in to_model[:1]]
    with ThreadPoolExecutor(max_workers=max(1, config.MIDDLE_EAST_CLASSIFY_CONCURRENCY)) as pool:
        results += list(pool.map(judge, to_model[1:]))
    for i, result in zip(to_model, results):
        if result is None:
            paths["model_failed"] += 1
            continue
        out[i] = {"article": articles[i], "result": result}
        paths["model_" + result["signal"]] += 1

    judged = [r for r in out if r is not None]
    for r in judged:
        r["result"]["metadata"]["source"] = source_fields(r["article"])
    batch_members = event_members(judged)
    for r in judged:
        if r["result"]["signal"] != "noise" and source_name(r["article"]) != "sec_fund_stake":
            r["result"]["metadata"]["corroboration"] = corroboration_for(
                r["article"], batch_members, stored_event_members or {})

    used = spend()
    logger.info("[MIDDLE_EAST_SIGNAL] paths: %s", dict(sorted(paths.items())))
    logger.info(
        "[MIDDLE_EAST_SIGNAL] spend: %d calls, %d input tokens (%d%% cached), %d output tokens, $%.4f",
        used["calls"], used["prompt"],
        round(100 * used["cached"] / used["prompt"]) if used["prompt"] else 0,
        used["completion"], used["cost"])
    return judged


# ---------------------------------------------------------------------------
# Corroboration - who else reported this same event
# ---------------------------------------------------------------------------
# The geopolitical pipeline's Stage D, computed when a row is classified:
# no model, no network. Two independent sources of other coverage:
#  - news-retrieval's title-similarity dedup (`also_reported_by` on the
#    article): near-identical headlines it discarded as the same event;
#  - newsapi.ai's own event cluster (`event_uri`): articles it grouped as
#    one event even when the headlines share no words - in this batch, and
#    in rows already stored.
# Stored as {count, siblings} on every signal and weak_signal row from a
# press or wire source; an explicit count 0 means "looked, none found".
# Not stored on noise (nobody reads its coverage) or SEC filings (a filing
# has no other outlets). It is shown to the reader, never used to raise a
# label: seven outlets repeating unnamed sources are still unnamed sources.

def source_fields(article: dict[str, Any]) -> dict[str, str]:
    """metadata.source - where the article came from:
      name       the source or newsapi.ai search (`wam_wire`, `eventregistry:me_shipping`)
      url        newsapi.ai only: the publisher's link - the row's own url is
                 the synthetic er-me:// id, which a reader cannot open
      outlet     newsapi.ai only: who published it (`reuters.com`)
      event_uri  newsapi.ai only, when it has one: its event cluster - how
                 later articles about the same event find this row
    Other sources keep only `name`: their row url is already the real link."""
    md = article.get("metadata") or {}
    fields = {"name": source_name(article)}
    if md.get("source_type") == "eventregistry":
        fields.update(url=md.get("article_url"), outlet=md.get("source_uri"),
                      event_uri=md.get("event_uri"))
    return {k: v for k, v in fields.items() if v and v != "None"}


def _member(row_id: Any, title: str | None, src: dict[str, Any], url: str | None) -> dict[str, Any]:
    return {"id": row_id, "title": title or "", "url": src.get("url") or url or "",
            "outlet": src.get("outlet") or ""}


def event_members(judged: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """{event_uri: [cluster member]} for the articles in this batch."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in judged:
        src = r["result"]["metadata"]["source"]
        if src.get("event_uri"):
            grouped.setdefault(src["event_uri"], []).append(
                _member(r["article"].get("url"), r["article"].get("title"), src, r["article"].get("url")))
    return grouped


def stored_member(row: dict[str, Any]) -> dict[str, Any]:
    """A stored agent_classifications row as a cluster member."""
    md = row.get("metadata") or {}
    if isinstance(md, str):
        md = json.loads(md)
    return _member(row.get("source_id") or row.get("url"), row.get("title"),
                   md.get("source") or {}, row.get("url"))


def corroboration_for(article: dict[str, Any],
                      batch_members: dict[str, list[dict[str, Any]]],
                      stored_members: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    md = article.get("metadata") or {}
    event_uri = md.get("event_uri") if md.get("event_uri") not in (None, "None") else None
    cluster = []
    if event_uri:
        cluster = stored_members.get(event_uri, []) + batch_members.get(event_uri, [])
    also = md.get("also_reported_by")
    if isinstance(also, str):
        try:
            also = json.loads(also)
        except ValueError:
            also = []
    return build_corroboration(
        event_uri=event_uri, row_id=article.get("url"), cluster=cluster,
        also_reported_by=also if isinstance(also, list) else [],
        self_url=md.get("article_url") or article.get("url"))


# ---------------------------------------------------------------------------
# Stories - which rows are one developing situation
# ---------------------------------------------------------------------------
# The geopolitical pipeline's Stage E, adapted. Corroboration asks "who
# else reported this EVENT"; a story asks "is this the same SITUATION":
# an IRGC closure of Hormuz, a tanker hitting a mine there and a shipping
# line pausing the route are three events and one story.
#
# Two rows are one story when, within 72 hours, they have the SAME event
# type (M1-M8) and share at least one SPECIFIC name - a company the
# article is about, or the place (Strait of Hormuz, Ras Laffan, Kiryat
# Gat). Countries, armed forces, issuing agencies and commodities never
# count on their own (geopolitical Stage E's lists, each from a measured
# wrong grouping). Same event type is required because a name alone
# joins different situations: G42 switching on capacity (M2) and G42 on a
# blacklist (M8) are two stories. The read-through companies (Nvidia,
# Micron) are never keys - they are on almost every row.
#
# Seeded, not transitive (a row joins by matching the story's SEED), and
# nothing is deleted: the seed carries metadata.story_development, the
# others metadata.in_story = seed id. One row per story is the filter
#     signal_detection IN ('signal','weak_signal') AND NOT (metadata ? 'in_story')
# Regrouped from scratch on every run, so a later arrival can re-seed.
_STORY_WINDOW_HOURS = 72
_STORY_LABEL_RANK = {"signal": 0, "weak_signal": 1}


# Names that never identify a story on their own here, on top of
# geopolitical Stage E's lists: the Gulf countries (two unrelated UAE AI
# governance pieces were grouped on "uae") and the Saudi-led coalition,
# which reports every Houthi interception - the actor, not the story.
_ME_GENERIC_KEYS = frozenset({
    "uae", "united arab emirates", "emirates", "saudi", "saudi arabia", "ksa", "qatar",
    "kuwait", "bahrain", "oman", "gcc", "gulf", "middle east", "uae government",
    "coalition restore legitimacy", "crly", "coalition", "saudi-led coalition",
})
# Direction and position words around a place: "eastern Strait of Hormuz"
# and "east of the Strait of Hormuz" are one place.
_PLACE_FILLER = frozenset({
    "the", "of", "off", "near", "in", "at", "and", "around", "surrounding", "waters", "waterways",
    "coast", "area", "region", "east", "eastern", "west", "western", "north", "northern",
    "south", "southern", "central", "outside", "inside"})


def place_key(place: str) -> str:
    """A place as a grouping key - the place name only, never initials
    (initials of "eastern Strait of Hormuz" gave the junk key "esh")."""
    words = [w for w in re.findall(r"[a-z0-9]+", normalize_text(place)) if w not in _PLACE_FILLER]
    return " ".join(words)


def story_keys(row: dict[str, Any]) -> set[str]:
    """The SPECIFIC names a row can be grouped on: the companies it is
    about and the place of its event."""
    keys: set[str] = set()
    for e in row.get("entities") or []:
        if e.get("role") == "subject":
            keys |= _entity_keys(e.get("name") or "")
    place = _event(row.get("metadata") or {}, "place")
    if place:
        keys.add(place_key(place))
    return {k for k in keys if k and _is_specific(k) and k not in _ME_GENERIC_KEYS}


def _hours_apart(a: Any, b: Any) -> float | None:
    try:
        return abs((b - a).total_seconds()) / 3600.0
    except (TypeError, AttributeError):
        return None


def build_middle_east_stories(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group signal/weak_signal rows into stories of two or more. Each row
    needs id, title, url, published (datetime or None), signal_detection,
    signal_reason, entities (parsed) and metadata.

    The seed - the row a reader lands on - is a signal before a weak
    signal, then the best-corroborated, then the earliest."""
    for r in rows:
        r["_keys"] = story_keys(r)
    ordered = sorted(rows, key=lambda r: (
        _STORY_LABEL_RANK.get(r.get("signal_detection"), 2),
        -int(((r.get("metadata") or {}).get("corroboration") or {}).get("count") or 0),
        r.get("published").timestamp() if r.get("published") else float("inf")))
    stories: list[dict[str, Any]] = []
    for row in ordered:
        for story in stories:
            seed = story["seed"]
            if (event_type(row["metadata"]) != event_type(seed["metadata"])
                    or not event_type(row["metadata"])):
                continue
            shared = seed["_keys"] & row["_keys"]
            gap = _hours_apart(seed.get("published"), row.get("published"))
            # A row with no published time cannot be placed in the 72h
            # window, so it never joins a story.
            if shared and gap is not None and gap <= _STORY_WINDOW_HOURS:
                story["members"].append({"row": row, "shared": shared})
                break
        else:
            stories.append({"seed": row, "members": []})
    return [s for s in stories if s["members"]]


# ---------------------------------------------------------------------------
# Same-event duplicates - folded into one row (tombstones, never deleted)
# ---------------------------------------------------------------------------
# Several outlets report one event (the IRGC closing Hormuz, the Intel fab
# blast) and each gets its own row and label. A reader must see the event
# once, so - as in geopolitical Stage B - the duplicates are folded into one
# row: their outlet, headline and link are added to that row's
# corroboration.siblings (via "same_event"), and the duplicate rows are
# TOMBSTONED: signal_detection = noise, metadata.collapsed_into = the kept
# row's id. Never deleted: the already-judged skip reads their url, and a
# wrong fold stays auditable and reversible.
#
# newsapi.ai's event_uri is NOT used to decide "same event" here: on the
# 403-article run one event_uri held 15 different rows (the closure,
# several different tanker hits, a US statement). Two rows are one event
# only when they have the same event code, are within 48 hours, and share
# either a company the article is about or - for events tied to one place
# (a closure, a site incident, a strike on a site) - the place. Ship
# attacks fold only on a shared ship or company name: several tankers hit
# near Hormuz on one day are separate events.
_SAME_EVENT_WINDOW_HOURS = 48
_PLACE_BOUND_CODES = frozenset({
    "m4_waterway_status", "m4_strike_on_site", "m4_other_strike",
    "m5_qatar_force_majeure", "m7_site_incident", "m7_site_disruption",
    "m7_home_front_restriction"})
_CONFIRMATION_RANK = {"official": 0, "named_source": 1, "unnamed": 2}


def _subject_keys(row: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for e in row.get("entities") or []:
        if e.get("role") == "subject":
            keys |= _entity_keys(e.get("name") or "")
    return {k for k in keys if k and _is_specific(k) and k not in _ME_GENERIC_KEYS}


def _same_event(kept: dict[str, Any], row: dict[str, Any]) -> bool:
    code = event_code(row["metadata"])
    if not code or code != event_code(kept["metadata"]):
        return False
    gap = _hours_apart(kept.get("published"), row.get("published"))
    if gap is None or gap > _SAME_EVENT_WINDOW_HOURS:
        return False
    if _subject_keys(kept) & _subject_keys(row):
        return True
    place_a, place_b = (_event(r["metadata"], "place") for r in (kept, row))
    return (code in _PLACE_BOUND_CODES and bool(place_a) and bool(place_b)
            and place_key(place_a) == place_key(place_b))


def find_same_event_groups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """[{"kept": row, "duplicates": [rows]}] for every event reported more
    than once. The kept row is the one a reader should see: a signal before
    a weak signal, then the best-confirmed, then the most corroborated,
    then the earliest. Seeded, not transitive - a row joins by matching the
    kept row itself. Each row needs id, url, title, published,
    signal_detection, entities (parsed) and metadata."""
    ordered = sorted(rows, key=lambda r: (
        _STORY_LABEL_RANK.get(r.get("signal_detection"), 2),
        _CONFIRMATION_RANK.get((r["metadata"].get("confirmation") or {}).get("level"), 3),
        -int((r["metadata"].get("corroboration") or {}).get("count") or 0),
        r.get("published").timestamp() if r.get("published") else float("inf")))
    groups: list[dict[str, Any]] = []
    for row in ordered:
        for group in groups:
            if _same_event(group["kept"], row):
                group["duplicates"].append(row)
                break
        else:
            groups.append({"kept": row, "duplicates": []})
    return [g for g in groups if g["duplicates"]]


def folded_corroboration(kept: dict[str, Any], duplicates: list[dict[str, Any]]) -> dict[str, Any]:
    """The kept row's corroboration with each duplicate added as a
    sibling (deduped by url) - the duplicate's own headline stays visible."""
    current = kept["metadata"].get("corroboration") or {"count": 0, "siblings": []}
    siblings = list(current.get("siblings") or [])
    seen = {s.get("url") for s in siblings if s.get("url")}
    for d in duplicates:
        src = d["metadata"].get("source") or {}
        url = src.get("url") or d.get("url") or ""
        if url in seen:
            continue
        seen.add(url)
        siblings.append({"outlet": src.get("outlet") or src.get("name") or "",
                         "title": (d.get("title") or "").strip(), "url": url, "via": "same_event"})
    return {"count": len(siblings), "siblings": siblings}


def story_development(story: dict[str, Any]) -> dict[str, Any]:
    """The field written to the seed row: each member's own event, so a
    reader opening the story sees what each development was. Copies of the
    member rows' fields - the member row stays the source of truth (join
    on `id`). `shared` says which names put each member here."""
    members = []
    for m in sorted(story["members"], key=lambda m: (m["row"].get("published") is None,
                                                      m["row"].get("published") or 0)):
        r = m["row"]
        md = r.get("metadata") or {}
        members.append({
            "id": r["id"],
            "title": (r.get("title") or "").strip(),
            "url": (md.get("source") or {}).get("url") or r.get("url") or "",
            "published": r["published"].isoformat() if r.get("published") else None,
            "signal_detection": r.get("signal_detection"),
            "reason": r.get("signal_reason"),
            "summary": md.get("summary"),
            "shared": sorted(m["shared"]),
        })
    return {"count": len(members), "members": members}
