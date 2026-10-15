"""Deterministic classification for taiwan_market_signal items (TWSE/TPEx
revenue and material announcements read from news-retrieval).

Moved here from news-retrieval per the decision that news-retrieval stays
fetch/dedup-only, uniformly across every domain - no source type computes a
signal or translates text there. Ranking and clause lookup are pure
arithmetic/lookup (no model call, matching the original design intent);
translation reuses this service's own OpenAI-compatible client rather than
introducing a second LLM provider.
"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.request import Request, urlopen

import config

logger = logging.getLogger(__name__)

_REVENUE_RANK_TOP_N = 3

# Starter table built from clause codes observed live against the TWSE/TPEx
# material-announcements feed. A real table should be built from ~2 weeks
# of historical data across all tracked tickers (expect ~20-30 distinct
# codes) - this is intentionally a small starting point, not a claim of
# completeness. Unrecognized codes default to WEAK rather than guessing a
# category for a code never observed.
_CLAUSE_CODE_TABLE: dict[str, str] = {
    "第51款": "HIGH",   # observed live: treasury-stock buyback execution - capital action
    "第14款": "WEAK",   # observed live: subsidiary dividend distribution - substantive but once-removed
    "第12款": "NOISE",  # observed live: investor-conference attendance notice - admin
    "第53款": "NOISE",  # observed live: company name change - admin
}
_CLAUSE_CODE_DEFAULT = "WEAK"

# Which metadata/top-level fields need translation, per source_category.
# company_name is deliberately NOT translated here - news-retrieval already
# sets metadata.translated_company_name directly from its own known-correct
# TAIWAN_TICKER_UNIVERSE list (seed.py), because LLM translation of bare
# 2-4 character Taiwan company names was confirmed live to produce serious
# errors (e.g. 2383 Elite Material mistranslated as "Taiwan Semiconductor
# Manufacturing Company"; 8210 Chenbro translated literally as "Diligence
# and sincerity"). Numeric fields never need translation.
#
# GDELT's own API response (mode=artlist) has no body/snippet field - but
# news-retrieval's _fetch_gdelt does NOT rely on GDELT for body text: it
# separately fetches each surviving article's real webpage via Trafilatura
# and sets article["body"] from the extracted page text (confirmed live -
# a real SETN article yielded 1000+ characters of genuine body text this
# way). So GDELT articles that survive Stage A DO have a real body to
# translate, same as material announcements - both title and body are
# translated here.
_TRANSLATION_FIELDS: dict[str, dict[str, str]] = {
    "mops_revenue": {
        "metadata.industry": "translated_industry",
        "metadata.remarks": "translated_remarks",
    },
    "mops_material": {
        "summary": "translated_summary",
        "body": "translated_body",
    },
    "gdelt": {
        "title": "translated_title",
        "body": "translated_body",
    },
}


def _get_nested(article: dict, path: str):
    """Read 'metadata.company_name' or a plain top-level key like 'title'."""
    if path.startswith("metadata."):
        return (article.get("metadata") or {}).get(path.split(".", 1)[1])
    return article.get(path)


def _translate_one(
    text: str, model: str, api_key: str, base_url: str, timeout: int,
) -> str | None:
    """Translate one short native-language string to English via a raw
    HTTP call to the OpenAI-compatible endpoint - matching this service's
    existing pattern in pipeline/classifier.py (urllib.request, not the
    openai SDK, which isn't a dependency here). Returns None on failure or
    blank input - callers must fall back to the native text, never treat
    None as an empty translation.
    """
    if not text or not text.strip():
        return None
    payload = {
        "model": model,
        "temperature": 0,
        "messages": [
            {
                "role": "system",
                "content": (
                    "Translate the following text to English. Translate"
                    " exactly what is written - do not add, infer,"
                    " summarize, embellish, or explain anything that is"
                    " not present in the source text. Do not correct,"
                    " complete, or interpret the source text; if it is"
                    " incomplete or ambiguous, translate it as literally"
                    " and faithfully as possible rather than guessing"
                    " the intended meaning."
                    " Output ONLY the translation itself, with no notes,"
                    " no quotation marks, and no explanation."
                ),
            },
            {"role": "user", "content": text},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        req = Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        translated = (content or "").strip()
        return translated or None
    except Exception as exc:
        logger.warning("[TAIWAN_TRANSLATE] failed for text=%r: %s", text[:60], exc)
        return None


def rank_revenue_by_yoy(
    articles: list[dict[str, Any]],
    context_articles: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Rank revenue articles by YoY% within their reporting period and tag
    each with a HIGH/WEAK classification, in place.

    A fixed percentage cutoff doesn't work for this universe - these
    companies routinely post 50-300% YoY as their normal run rate, so a
    flat threshold would flag nearly everyone, nearly every month. Ranking
    is self-calibrating for the top end and guarantees a stable number of
    top-N HIGH signals per period. Never NOISE - a monthly statutory filing
    is never noise by definition.

    The bottom end is NOT rank-based: in a month where every tracked
    company grew, the slowest grower is still just a normal month, not a
    signal. HIGH at the bottom only fires on actual negative YoY - a real
    contraction, not merely "smallest gain in a good month." This means
    the number of HIGH signals per period varies (some months more, some
    fewer, some none at the bottom) - that's correct, not a bug to force
    back to a fixed count.

    Ranks ALL revenue articles passed in together, per period - callers
    are responsible for passing in every article for a given day's
    classification pass so "top 3 of 20" is meaningful, not "top 3 of
    whatever happened to be in this one batch."

    ``context_articles`` (optional): already-classified revenue rows for
    the same periods, read back from storage by the caller. A company's
    filing can land days after the rest of the period's field has already
    been ranked and inserted - without this, that straggler would be
    ranked against only itself ("rank 1 of 1") instead of the true field
    size. Included in the ranking computation but never mutated in place
    (they're plain dicts built fresh by the caller from DB rows, not the
    live news-retrieval article objects) - the function instead returns
    the subset of context_articles whose rank/signal changed as a result
    of adding today's new articles to the field, so the caller can decide
    whether/how to persist that (this module has no DB access itself).

    A filing present on both sides - re-fetched today AND already stored -
    is counted once, as the fresh article. Ranks within a period are
    therefore always 1..n over distinct (ticker, period) filings, with no
    repeated or skipped rank and no inflation of ``n``.

    Adds to each qualifying article's ``metadata``, in place:
        - ``revenue_rank_signal``: "HIGH" or "WEAK"
        - ``revenue_yoy_rank``: 1-indexed rank by YoY% within its period
        - ``revenue_rank_reason``: e.g. "rank_2_of_20_yoy"

    Returns:
        The subset of context_articles whose revenue_rank_signal,
        revenue_yoy_rank, or revenue_rank_reason changed - empty if no
        context_articles were passed, or if none of them changed.
    """
    # One filing = one entry in the field, keyed by (ticker, period) - the
    # same identity insert_taiwan_signal_classification's source_id uses,
    # rebuilt here because that source_id is only derived later, in
    # classify_taiwan_signal_batch, after ranking has already run.
    #
    # Both sides of the field can carry the same filing twice. The caller
    # pools every completed news-retrieval run in the window and dedups
    # those by URL, but one filing can be re-fetched under a new URL across
    # polls, so it survives URL-dedup into `articles`; and the stored rows
    # read back as context are selected by period alone, so they include
    # the filings today's batch just re-delivered. Concatenating the two
    # ranked one company twice - two consecutive ranks for one filing,
    # every company below it shifted down, and the field size `n` inflated
    # past the real universe. The duplicate row was then correctly blocked
    # at insert by source_id, so only the ranks were wrong, invisibly.
    def _collect(source: list[dict], into: dict[str, list[dict]], seen: set) -> None:
        for a in source:
            meta = a.get("metadata") or {}
            if meta.get("source_category") != "mops_revenue":
                continue
            if meta.get("yoy_pct") is None:
                continue
            # A row with no ticker has no filing identity to dedupe on -
            # every such row would collide on (None, period) and all but
            # the first would vanish from the field, understating n. The
            # fetcher filters to a fixed ticker universe so this should
            # never happen; if it ever does, keep the row (a duplicated
            # rank is recoverable, a silently dropped company is not) and
            # say so, rather than deduping on a key that isn't an identity.
            ticker = meta.get("ticker")
            if ticker is None:
                logger.warning(
                    "[TAIWAN_RANK] mops_revenue row with no ticker in period=%s"
                    " - ranked without dedup", meta["period_gregorian"],
                )
            else:
                key = (ticker, meta["period_gregorian"])
                if key in seen:
                    continue
                seen.add(key)
            into.setdefault(meta["period_gregorian"], []).append(a)

    # CONTEXT rows claim their key first, so when a filing is both stored
    # and re-delivered today it is the STORED row that gets ranked and the
    # fresh duplicate that is dropped. This precedence is load-bearing, not
    # cosmetic: the caller filters already-stored source_ids out of its
    # insert list, so a rank computed onto the fresh copy of an existing
    # filing is never written anywhere. Ranking the stored row instead
    # routes a genuine change through changed_context ->
    # update_taiwan_revenue_rank, which is the only path that can correct a
    # row after insert. Ranking the fresh copy would silently discard the
    # correction and leave the stale rank in the DB forever.
    #
    # This does not reintroduce per-run revision churn: the loop below only
    # appends to rank_revision_history when the recomputed rank/signal
    # actually differs from what is stored, so a re-delivery that changes
    # nothing is a true no-op.
    seen_keys: set = set()
    context_by_period: dict[str, list[dict]] = {}
    if context_articles:
        _collect(context_articles, context_by_period, seen_keys)

    by_period: dict[str, list[dict]] = {}
    _collect(articles, by_period, seen_keys)

    changed_context: list[dict[str, Any]] = []

    all_periods = set(by_period) | set(context_by_period)
    for period in all_periods:
        period_articles = by_period.get(period, []) + context_by_period.get(period, [])
        ranked = sorted(
            period_articles, key=lambda a: a["metadata"]["yoy_pct"], reverse=True,
        )
        n = len(ranked)
        for i, article in enumerate(ranked):
            rank = i + 1
            yoy_pct = article["metadata"]["yoy_pct"]
            is_top = rank <= _REVENUE_RANK_TOP_N
            is_shrinking = yoy_pct < 0
            signal = "HIGH" if (is_top or is_shrinking) else "WEAK"
            reason = f"rank_{rank}_of_{n}_yoy"

            meta = article["metadata"]
            is_context_row = "id" in article  # DB-sourced rows carry their row id; fresh articles don't
            if is_context_row:
                prev_signal = meta.get("revenue_rank_signal")
                prev_rank = meta.get("revenue_yoy_rank")
                if prev_signal != signal or prev_rank != rank:
                    meta.setdefault("rank_revision_history", []).append({
                        "previous_rank": prev_rank,
                        "previous_signal": prev_signal,
                        "previous_reason": meta.get("revenue_rank_reason"),
                    })
                    meta["revenue_rank_signal"] = signal
                    meta["revenue_yoy_rank"] = rank
                    meta["revenue_rank_reason"] = reason
                    changed_context.append(article)
                # else: rank/signal unchanged - leave the row exactly as
                # stored, don't touch rank_revision_history for a no-op.
            else:
                meta["revenue_rank_signal"] = signal
                meta["revenue_yoy_rank"] = rank
                meta["revenue_rank_reason"] = reason

        logger.info(
            "[TAIWAN_RANK] period=%s ranked=%d high=%d changed_context=%d",
            period, n,
            sum(1 for a in ranked if a["metadata"]["revenue_rank_signal"] == "HIGH"),
            sum(1 for a in changed_context if a["metadata"]["period_gregorian"] == period),
        )

    return changed_context


def classify_material_announcements(articles: list[dict[str, Any]]) -> None:
    """Classify material-announcement articles by disclosure clause code, in
    place. Only touches articles with metadata.source_category ==
    "mops_material". Adds ``announcement_materiality_signal`` (HIGH/WEAK/
    NOISE), ``announcement_materiality_reason``, and ``period_gregorian``
    to metadata.

    period_gregorian is derived from statement_date (e.g. "2026-08-24" ->
    "2026-08") so material announcements can be filtered by the same
    period= query param revenue rows already use - one filtering concept
    across both source_category values instead of a second, date-range-
    shaped mechanism just for announcements. Distinct from mops_revenue's
    period_gregorian, which is the reporting period a filing is ABOUT
    (e.g. July's revenue, filed in August) - here it's simply the calendar
    month the announcement was filed, since material announcements don't
    have a separate "reporting period" concept the way monthly revenue
    does.
    """
    counts = {"HIGH": 0, "WEAK": 0, "NOISE": 0}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "mops_material":
            continue
        code = meta.get("disclosure_clause_code")
        signal = _CLAUSE_CODE_TABLE.get(code, _CLAUSE_CODE_DEFAULT)
        a["metadata"]["announcement_materiality_signal"] = signal
        a["metadata"]["announcement_materiality_reason"] = (
            f"clause_{code}" if code in _CLAUSE_CODE_TABLE else "unrecognized_clause"
        )
        statement_date = meta.get("statement_date")
        if statement_date and len(statement_date) >= 7:
            a["metadata"]["period_gregorian"] = statement_date[:7]
        counts[signal] += 1
    if any(counts.values()):
        logger.info(
            "[TAIWAN_MATERIALITY] classified high=%d weak=%d noise=%d",
            counts["HIGH"], counts["WEAK"], counts["NOISE"],
        )


def translate_taiwan_articles(
    articles: list[dict[str, Any]],
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: int | None = None,
) -> None:
    """Translate the fields that need it, per source_category, in place.

    Only touches articles whose metadata.source_category has an entry in
    _TRANSLATION_FIELDS. Native-language fields are never overwritten;
    translated values are added as new metadata fields (e.g.
    metadata.translated_title). Failed translations leave the
    translated_* field absent rather than set to None/empty.

    Defaults to OPENAI_MODEL_V2 (the cheaper refine-pass model, e.g.
    gpt-4o-mini per infra/CLAUDE.md), not the base OPENAI_MODEL - this is a
    mechanical translation task, not a judgment call, so it doesn't need
    the stronger/pricier base classification model.
    """
    model = model or config.OPENAI_MODEL_V2
    api_key = api_key or config.OPENAI_API_KEY
    base_url = base_url or config.OPENAI_BASE_URL
    timeout = timeout or config.OPENAI_TIMEOUT

    to_translate: list[tuple[dict, str, str]] = []  # (article, source_path, dest_field)
    for a in articles:
        category = (a.get("metadata") or {}).get("source_category")
        field_map = _TRANSLATION_FIELDS.get(category)
        if not field_map:
            continue
        for source_path, dest_field in field_map.items():
            if _get_nested(a, source_path):
                to_translate.append((a, source_path, dest_field))

    def _translate_and_apply(item: tuple[dict, str, str]) -> bool:
        article, source_path, dest_field = item
        text = _get_nested(article, source_path)
        result = _translate_one(text, model, api_key, base_url, timeout)
        if result is not None:
            article["metadata"][dest_field] = result
            return True
        return False

    translated_count = 0
    if to_translate:
        with ThreadPoolExecutor(max_workers=config.TAIWAN_CLASSIFY_CONCURRENCY) as executor:
            translated_count = sum(executor.map(_translate_and_apply, to_translate))

    if to_translate:
        logger.info(
            "[TAIWAN_TRANSLATE] %d/%d field(s) translated successfully",
            translated_count, len(to_translate),
        )


# Path 3, Stage B - the one genuine model call in this whole module.
# Returns HIGH/WEAK plus, for HIGH, which of the prompt's own criteria
# triggered it - so signal_reason carries real information instead of a
# fixed "gdelt_relevance_high" string. Still one call, no retry - per the
# original spec's own cost design. Runs only on GDELT articles that already
# survived news-retrieval's Stage A (domain allowlist + company-name-in-title
# check) and title-similarity dedup, so this is judging genuine candidates,
# not raw noise.
_GDELT_RELEVANCE_SYSTEM_PROMPT = """You classify Taiwan stock headlines as HIGH or WEAK.

HIGH means the headline states a specific, checkable fact:
- numeric_fact: a number (revenue, shipments, capacity, price, percentage)
- named_customer_or_contract: a named customer, partner, or contract
- capacity_or_pricing_change: a capacity, production, or pricing change
- company_guidance_change: a guidance or forecast change from the company itself
- regulatory_action: a regulatory or export-control action naming the company

WEAK means anything else:
- analyst opinion, ratings, price targets
- outlook, sentiment, or expectation with no figure
- the company mentioned in passing in a market roundup
- a story mainly about a different company

The source matters. A fact stated by the company or a
regulator is HIGH. The same fact predicted by an analyst
is WEAK.

If unsure, answer WEAK.

Answer with exactly one line, two words separated by a space:
- if HIGH, the category then HIGH, e.g. "numeric_fact HIGH"
- if WEAK, just "WEAK"
"""

# No numeric confidence - the model is forced into a single-word HIGH/WEAK
# call and never asked for (or gives) a real confidence value, so a fixed
# number here would be fabricated precision. NULL means "not measured",
# distinct from the deterministic paths' 1.0 (which reflects certainty in
# an actual computation, not a guess).
_GDELT_RELEVANCE_SCORE = None
_GDELT_HIGH_CATEGORIES = frozenset({
    "numeric_fact",
    "named_customer_or_contract",
    "capacity_or_pricing_change",
    "company_guidance_change",
    "regulatory_action",
})


def _classify_gdelt_relevance(
    company_name: str, title: str, model: str, api_key: str, base_url: str, timeout: int,
) -> tuple[str, str]:
    """HIGH/WEAK classification for a single GDELT article, plus a reason.

    Returns (signal, reason). For HIGH, reason is one of
    _GDELT_HIGH_CATEGORIES; for WEAK, reason is always "gdelt_relevance_weak".

    No retry - per the spec's own reasoning, a repair attempt costs more
    than the value of one extra try on a call this cheap, and failing
    toward WEAK (hiding a possible signal) is the safer direction than
    failing toward HIGH (surfacing noise as if it were checked).

    Any response that isn't a recognized "<category> HIGH" pair is treated
    as WEAK - a model that hedges, explains, or returns something else has
    not given a usable answer, and guessing which way it leaned is worse
    than the safe default.
    """
    user_prompt = f"Company: {company_name}\nHeadline: {title}"
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": 10,
        "messages": [
            {"role": "system", "content": _GDELT_RELEVANCE_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        req = Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        parts = (content or "").strip().split()
        if len(parts) == 2 and parts[1].upper() == "HIGH" and parts[0].lower() in _GDELT_HIGH_CATEGORIES:
            return "HIGH", parts[0].lower()
        return "WEAK", "gdelt_relevance_weak"
    except Exception as exc:
        logger.warning("[GDELT_RELEVANCE] classification failed, defaulting to WEAK: %s", exc)
        return "WEAK", "gdelt_relevance_weak"


def classify_gdelt_articles(
    articles: list[dict[str, Any]], model: str | None = None,
) -> list[dict[str, Any]]:
    """Classify GDELT articles (source_category == "gdelt"), one model call
    per article, bounded by config.TAIWAN_CLASSIFY_CONCURRENCY (same
    semaphore-free thread-pool pattern translate_taiwan_articles uses -
    each call is independent, no batch context needed).

    Only touches articles that already have a translated_title (i.e.
    survived Stage A + dedup + translation) - an article with no
    translated_title has nothing usable to classify.

    source_id is the article's own URL - GDELT articles have no natural
    ticker+period/ticker+timestamp key the way TWSE/TPEx rows do (each is
    a distinct story, not a recurring filing), and news-retrieval has
    already deduplicated by URL (exact match) and by title similarity
    before this point, so the URL is a reliable identifier by the time it
    reaches this function.
    """
    model = model or config.OPENAI_MODEL_V2
    api_key = config.OPENAI_API_KEY
    base_url = config.OPENAI_BASE_URL
    timeout = config.OPENAI_TIMEOUT

    candidates = []
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "gdelt":
            continue
        translated_title = meta.get("translated_title")
        if not translated_title:
            continue  # nothing usable to classify without a translated title
        candidates.append((a, meta, translated_title))

    def _classify_one(item: tuple[dict, dict, str]) -> dict[str, Any]:
        a, meta, translated_title = item
        company_name = meta.get("translated_company_name") or meta.get("ticker") or ""
        signal, reason = _classify_gdelt_relevance(
            company_name, translated_title, model, api_key, base_url, timeout,
        )
        return {
            "article": a,
            "result": {
                "signal": signal,
                "signal_score": _GDELT_RELEVANCE_SCORE,
                "source_id": a.get("url"),
                "reason": reason,
                "metadata": meta,
            },
        }

    if not candidates:
        return []
    with ThreadPoolExecutor(max_workers=config.TAIWAN_CLASSIFY_CONCURRENCY) as executor:
        return list(executor.map(_classify_one, candidates))


def classify_taiwan_signal_batch(
    articles: list[dict[str, Any]],
    model: str | None = None,
    revenue_context: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Run the full taiwan_market_signal classification pass on a batch of
    articles read from news-retrieval: rank revenue, classify material
    announcements, translate whatever needs it - then build one result dict
    per article for insert_taiwan_signal_classification.

    Must be called with ALL of a period's/day's articles together, not
    per-article - ranking needs the full set to be meaningful. This is why
    this is a batch function, unlike the per-article classifiers in
    dispatch.py's registry (rank_revenue_by_yoy's "top 3 of 20" requirement
    doesn't fit a one-item-at-a-time interface). GDELT articles don't need
    batch context (each is classified independently, per Stage B), but are
    included here too so one call handles the whole domain's classification.

    ``revenue_context`` (optional): already-stored mops_revenue rows for
    periods present in ``articles``, passed straight through to
    rank_revenue_by_yoy so a late-arriving filing is ranked against the
    true known field, not just today's batch. See that function's
    docstring for why these are never mutated by INSERT - the second
    element of the return tuple carries whichever of them changed.

    Returns:
        (results, changed_context_rows) - results is the usual list of
        {"article", "result"} dicts for insert_taiwan_signal_classification,
        same as before this parameter was added. changed_context_rows is
        the subset of revenue_context whose rank/signal changed - the
        caller persists these via update_taiwan_revenue_rank, not insert.
    """
    changed_context = rank_revenue_by_yoy(articles, revenue_context)
    classify_material_announcements(articles)
    translate_taiwan_articles(articles, model=model)

    results = []
    for a in articles:
        meta = a.get("metadata") or {}
        category = meta.get("source_category")
        if category == "mops_revenue":
            signal = meta.get("revenue_rank_signal")
            reason = meta.get("revenue_rank_reason")
            source_id = f"{meta.get('ticker')}-{meta.get('period_gregorian')}"
        elif category == "mops_material":
            signal = meta.get("announcement_materiality_signal")
            reason = meta.get("announcement_materiality_reason")
            source_id = (
                f"{meta.get('ticker')}-{meta.get('statement_date')}"
                f"-{meta.get('statement_time')}"
            )
        else:
            continue  # gdelt handled separately below (needs a model call
            # per article, not a metadata lookup) - and any other/unknown
            # source_category is correctly skipped here too.

        if signal is None:
            continue  # e.g. revenue row with no yoy_pct, never ranked

        results.append({
            "article": a,
            "result": {
                # Both mops_revenue (rank) and mops_material (clause-code
                # lookup) are deterministic rules, not scored judgments -
                # null is more honest than a fabricated 1.0, and each
                # already has a self-describing reason/rank field to sort
                # or filter on instead. signal_score is reserved for a
                # path where a model genuinely produces one (gdelt).
                "signal": signal,
                "signal_score": None,
                "source_id": source_id,
                "reason": reason,
                "metadata": meta,
            },
        })

    results.extend(classify_gdelt_articles(articles, model=model))
    return results, changed_context
