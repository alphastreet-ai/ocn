"""Deterministic classification for taiwan_market_signal items (TWSE/TPEx
revenue and material announcements read from news-retrieval).

Moved here from news-retrieval per the decision that news-retrieval stays
fetch/dedup-only, uniformly across every domain - no source type computes a
signal or translates text there. Revenue scoring and clause lookup are pure
arithmetic/lookup (no model call, matching the original design intent);
translation reuses this service's own OpenAI-compatible client rather than
introducing a second LLM provider.
"""
from __future__ import annotations

import json
import logging
import math
import re
import statistics
from concurrent.futures import ThreadPoolExecutor
from typing import Any
from urllib.request import Request, urlopen

import config
from pipeline.taiwan_announcement_classifier import apply_known_names, classify_announcements

logger = logging.getLogger(__name__)

# Revenue signal - each filing judged against the company's OWN history.
# Every constant below was checked in a point-in-time backtest over
# 2021-07..2026-06 (20 companies, MOPS history since 2019), see
# score_revenue_filing's docstring for what each one does and the result.
_REVENUE_WINDOW_MONTHS = 36
_REVENUE_MIN_HISTORY = 24
_MAD_TO_SD = 1.4826
# Averaging two scores that correlate at 0.52 narrows the spread to
# sqrt((1 + 0.52) / 2) = 0.87 of either; dividing restores the scale so the
# 2.5 / 1.5 cutoffs mean the same as for a single score.
_COMBINED_SCALE = 0.87
_REVENUE_HIGH = 2.5
_REVENUE_WEAK = 1.5
# Lunar New Year moves between January and February, and in those months
# the score predicted nothing (next-quarter growth improved 43% of the time
# after a big "drop" vs 55% normally).
_REVENUE_UNSCORED_MONTHS = frozenset({1, 2})


# What each material-announcement clause means, from the official lists.
# TWSE: 對有價證券上市公司重大訊息之查證暨公開處理程序 Art. 4 para 1, 51
# clauses (twse-regulation.twse.com.tw FL007111, version 2026-09-30).
# TPEx: its own procedure has 53 - the same as TWSE through 46, plus two
# TPEx-only clauses (47, 50) that shift the rest (TPEx checklist, Jan 2025
# revision; FAQ, Apr 2026). TPEx 39, 42 and 44 are absent from both TPEx
# documents and are left out rather than assumed equal to TWSE's - an
# unlisted clause gets no label and a warning, never a guess.
_TWSE_CLAUSE_LABELS: dict[int, str] = {
    1: "Bounced cheque, credit loss or trading status change",
    2: "Litigation or regulatory action with major impact",
    3: "Production cut, shutdown or asset pledge",
    4: "Lease or transfer of the whole business",
    5: "Reorganization or bankruptcy",
    6: "Chairman, CEO or director change",
    7: "Change of auditor",
    8: "Spokesperson or key manager change",
    9: "Accounting year or policy change",
    10: "Major contract, partnership or product milestone",
    11: "Capital change, merger or securities issue",
    12: "Investor conference",
    13: "Financial forecast published or revised",
    14: "Dividend decision",
    15: "Major investment plan",
    16: "Change to a capital-raising plan",
    17: "Shareholder meeting called",
    18: "Shareholder meeting resolutions",
    19: "Fraud, irregular transaction or search by authorities",
    20: "Asset purchase or sale",
    21: "Manager or director non-compete approval",
    22: "Endorsement or guarantee",
    23: "Loan of funds to others",
    24: "Purchase or sale of privately placed securities",
    25: "Main customer or supplier relationship ended",
    26: "Disaster, strike, pollution or cybersecurity incident",
    27: "Creditor bank negotiation result",
    28: "Default by a related party or debtor",
    29: "Internal control statement change",
    30: "Financial report late, misstated or qualified",
    31: "Financial report board approval",
    32: "Custodied shares withdrawn below the required level",
    33: "Cross-shareholding change notice",
    34: "Director or supervisor suspended by court",
    35: "Share buyback",
    36: "Capital reduction or par value change",
    37: "Listing commitment not met",
    38: "Tender offer",
    # 39 deleted from the TWSE procedure
    40: "Trading halted or resumed by the exchange",
    41: "Holding company's subsidiary count changed",
    42: "Delisting or listing transfer decision",
    43: "Donation to a related party or major donation",
    44: "Committee or independent director objection",
    45: "Directors waive half or more of a rights issue",
    46: "Listed-subsidiary holding threshold reached",
    47: "Overseas financial report reconciliation",
    48: "Material change after a demerger share acquisition",
    49: "Control of an important subsidiary lost or stake cut",
    50: "Subsidiary spin-off or overseas listing matter",
    51: "Other material decision or event",
}
_TPEX_CLAUSE_LABELS: dict[int, str] = {
    **{n: label for n, label in _TWSE_CLAUSE_LABELS.items() if n <= 46 and n not in (39, 42, 44)},
    47: "Withdrawn application to move to TWSE",
    48: _TWSE_CLAUSE_LABELS[47],
    49: _TWSE_CLAUSE_LABELS[48],
    50: "Annual report correction required by the exchange",
    51: _TWSE_CLAUSE_LABELS[49],
    52: _TWSE_CLAUSE_LABELS[50],
    53: _TWSE_CLAUSE_LABELS[51],
}
_CLAUSE_LABELS = {"TWSE": _TWSE_CLAUSE_LABELS, "TPEx": _TPEX_CLAUSE_LABELS}


def clause_label(exchange: str | None, code: str | None) -> str | None:
    """Plain-English meaning of a clause code such as '第20款', for the
    exchange that filed it. None when the code or exchange is not in the
    official tables."""
    match = re.search(r"\d+", code or "")
    if not match:
        return None
    return _CLAUSE_LABELS.get(exchange or "", {}).get(int(match.group()))


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
    # summary here is news-retrieval's field name for the filing's SUBJECT
    # line, so its translation is the title. The English summary itself is
    # written by taiwan_announcement_classifier (translated_summary); the
    # whole body is translated literally only when no summary passed.
    "mops_material": {
        "summary": "translated_title",
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
                    " Translate the name of every company, organisation,"
                    " exchange, place or person written in Chinese into"
                    " English and put the original Chinese name in brackets"
                    " right after it, e.g. Suzhou Qunce Technology Co., Ltd."
                    " (蘇州群策科技股份有限公司). Write no other Chinese."
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


def taiwan_month_index(year: int, month: int) -> int:
    """A month as one integer, so 'the previous month' is index - 1."""
    return year * 12 + (month - 1)


def build_revenue_history(rows: list[dict[str, Any]]) -> dict[str, dict[int, tuple[float, float | None]]]:
    """news-retrieval's /market/taiwan/revenue-history rows -> per ticker,
    {month index: (revenue, prior-year revenue as printed in that filing)}."""
    history: dict[str, dict[int, tuple[float, float | None]]] = {}
    for r in rows:
        try:
            idx = taiwan_month_index(int(r["year"]), int(str(r["period_type"]).lstrip("M")))
        except (KeyError, TypeError, ValueError):
            continue
        if r.get("revenue") is None:
            continue
        history.setdefault(str(r["code"]), {})[idx] = (float(r["revenue"]), r.get("prior_revenue"))
    return history


def _log_ratio(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or a <= 0 or b <= 0:
        return None
    return math.log(a / b)


def _own_score(series: dict[int, float], idx: int) -> tuple[float | None, str | None]:
    """(value at idx - company's median) / its usual swing, using only the
    _REVENUE_WINDOW_MONTHS before idx. Returns (score, None), or
    (None, reason) when it cannot be judged."""
    past = [series[j] for j in range(idx - _REVENUE_WINDOW_MONTHS, idx) if j in series]
    if len(past) < _REVENUE_MIN_HISTORY:
        return None, "not_scored_short_history"
    median = statistics.median(past)
    swing = _MAD_TO_SD * statistics.median([abs(x - median) for x in past])
    if swing == 0:
        return None, "not_scored_flat_history"
    return (series[idx] - median) / swing, None


def score_revenue_filing(
    company_history: dict[int, tuple[float, float | None]],
    idx: int,
    revenue: float | None,
    prior_revenue: float | None,
) -> dict[str, Any]:
    """Judge one monthly revenue filing against the company's own past.

    Two scores, each "how unusual is this month for THIS company":
      mom_deviation         this month vs last month
      yoy_change_deviation  this month's YoY minus last month's YoY
    each = (value - median of past 36 months) / (MAD * 1.4826), on log
    changes so +50% and -33% are the same size. revenue_deviation is their
    average / 0.87. |score| >= 2.5 HIGH, >= 1.5 WEAK, else NOISE; the sign
    is the direction.

    Why these, from the backtest (1,000 scored company-months):
      - top-3-by-YoY (the old rule) was worse than random; YoY level alone
        predicted nothing (rank correlation +0.08)
      - the combined score predicted both the price move by the 11th
        (HIGH up +3.9%, HIGH down -4.4% vs the other tracked companies,
        t = +2.5 / -3.8) and next-quarter growth (82% / 24% improved vs 54%
        normally), stable in 2021-23 and 2024-26
      - MAD rather than standard deviation: a company's 2 most extreme
        months carry ~22% of its variance; SD fired 65% fewer signals and
        lost the downside entirely
      - seasonal adjustment and 3-month acceleration were tested and
        rejected (no noise reduction; not stable across periods)

    YoY uses the prior-year figure printed in each month's own filing, so a
    restatement (KYEC 2023-24, ~-28%) never shows as a false jump.

    Returns {"signal", "reason", "fields"}; `fields` holds only what could be
    computed - an unscorable filing carries no score keys at all, so "not
    measured" never looks like a measured zero.
    """
    revenue_by_month = {i: v[0] for i, v in company_history.items()}
    prior_by_month = {i: v[1] for i, v in company_history.items()}
    revenue_by_month[idx] = revenue
    prior_by_month[idx] = prior_revenue

    fields: dict[str, Any] = {}
    prev, prev_prior = revenue_by_month.get(idx - 1), prior_by_month.get(idx - 1)
    if prev and prev_prior and prev > 0 and prev_prior > 0:
        fields["prior_yoy_pct"] = round((prev / prev_prior - 1) * 100, 2)
    last_12 = [revenue_by_month.get(j) for j in range(idx - 12, idx)]
    if revenue is not None and all(v is not None for v in last_12):
        fields["is_12m_high"] = revenue > max(last_12)

    def _not_scored(reason: str) -> dict[str, Any]:
        return {"signal": "NOISE", "reason": reason, "fields": fields}

    if revenue is None or prior_revenue is None or revenue <= 0 or prior_revenue <= 0:
        return _not_scored("not_scored_bad_value")
    if idx % 12 + 1 in _REVENUE_UNSCORED_MONTHS:
        return _not_scored("not_scored_lunar_new_year")

    mom = {i: r for i in revenue_by_month
           if (r := _log_ratio(revenue_by_month[i], revenue_by_month.get(i - 1))) is not None}
    yoy = {i: r for i in revenue_by_month
           if (r := _log_ratio(revenue_by_month[i], prior_by_month.get(i))) is not None}
    yoy_change = {i: yoy[i] - yoy[i - 1] for i in yoy if i - 1 in yoy}
    if idx not in mom or idx not in yoy_change:
        return _not_scored("not_scored_short_history")

    mom_score, why = _own_score(mom, idx)
    if why:
        return _not_scored(why)
    yoy_change_score, why = _own_score(yoy_change, idx)
    if why:
        return _not_scored(why)

    signal_score = (mom_score + yoy_change_score) / 2 / _COMBINED_SCALE
    strength = abs(signal_score)
    fields.update({
        "revenue_deviation": round(signal_score, 2),
        "mom_deviation": round(mom_score, 2),
        "yoy_change_deviation": round(yoy_change_score, 2),
    })
    signal = "HIGH" if strength >= _REVENUE_HIGH else "WEAK" if strength >= _REVENUE_WEAK else "NOISE"
    return {"signal": signal, "reason": "scored", "fields": fields}


def _period_before(period: str) -> str:
    year, month = (int(x) for x in period.split("-"))
    return f"{year - 1}-12" if month == 1 else f"{year}-{month - 1:02d}"


def _pct_change(now: Any, before: Any) -> float | None:
    try:
        return (float(now) / float(before) - 1) * 100 if now and before and float(before) > 0 else None
    except (TypeError, ValueError):
        return None


def revenue_reason(meta: dict[str, Any], signal: str, code: str) -> str:
    """signal_reason for a revenue row: one or two plain sentences built from
    the stored numbers by template - no model call, so it can never state
    anything the row's own fields do not. Dates as YYYY-MM, like every other
    Taiwan reason."""
    company = meta.get("translated_company_name") or meta.get("ticker") or "The company"
    owner = f"{company}'" if company.endswith("s") else f"{company}'s"  # "Delta Electronics'"
    period = str(meta.get("period_gregorian") or "")
    if code == "not_scored_lunar_new_year":
        return (f"Not scored: January and February revenue shifts with the Lunar New Year holiday, "
                f"so {period} is not compared with {owner} history.")
    if code == "not_scored_short_history":
        return (f"Not scored: {company} has fewer than {_REVENUE_MIN_HISTORY} months of revenue history "
                f"before {period}, too few to judge what is unusual.")
    if code == "not_scored_flat_history":
        return f"Not scored: {owner} past monthly changes are too uniform to measure {period} against."
    if code == "not_scored_bad_value":
        return (f"Not scored: the {period} revenue figure or its prior-year figure is missing or not a "
                f"positive number.")

    before = _period_before(period)
    mom = meta.get("mom_pct")
    if mom is None:
        mom = _pct_change(meta.get("revenue_current_month"), meta.get("revenue_prior_month"))
    yoy = meta.get("yoy_pct")
    if yoy is None:
        yoy = _pct_change(meta.get("revenue_current_month"), meta.get("revenue_prior_year_month"))
    prior_yoy = meta.get("prior_yoy_pct")
    facts = []
    if mom is not None:
        facts.append(f"{period} revenue {'rose' if mom >= 0 else 'fell'} {abs(mom):.1f}% from {before}")
    if yoy is not None:
        trend = (f", {'up' if yoy > prior_yoy else 'down'} from {prior_yoy:+.1f}% in {before}"
                 if prior_yoy is not None and round(yoy, 1) != round(prior_yoy, 1) else "")
        facts.append(f"year on year it was {yoy:+.1f}%{trend}")
    first = ("; ".join(facts) + ".") if facts else f"{period} revenue was filed."
    first = first[0].upper() + first[1:]

    deviation = meta.get("revenue_deviation") or 0
    mom_dev, yoy_dev = abs(meta.get("mom_deviation") or 0), abs(meta.get("yoy_change_deviation") or 0)
    driver = "month-to-month swings" if mom_dev >= yoy_dev else "changes in its growth rate"
    direction = "upward" if deviation > 0 else "downward"
    if signal == "HIGH":
        second = f"That is far outside {owner} usual {driver}: a strong {direction} break from its own pattern."
    elif signal == "WEAK":
        second = f"That is outside {owner} usual {driver}: a moderate {direction} break from its own pattern."
    else:
        second = f"That is within {owner} usual range."
    if meta.get("is_12m_high"):
        second += " It is also the highest monthly revenue of the past 12 months."
    return f"{first} {second}"


def score_revenue_articles(
    articles: list[dict[str, Any]],
    history: dict[str, dict[int, tuple[float, float | None]]],
) -> None:
    """Score every mops_revenue article in place, each against its own
    company's history - no article depends on any other company's filing,
    so a filing is labelled the run it arrives in. Adds the score fields to
    metadata plus revenue_signal / revenue_signal_reason for the batch."""
    counts: dict[str, int] = {}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "mops_revenue":
            continue
        try:
            year, month = (int(x) for x in str(meta.get("period_gregorian")).split("-"))
        except ValueError:
            continue
        out = score_revenue_filing(
            history.get(str(meta.get("ticker")), {}),
            taiwan_month_index(year, month),
            meta.get("revenue_current_month"),
            meta.get("revenue_prior_year_month"),
        )
        meta.update(out["fields"])
        a["_revenue_signal"] = (out["signal"], revenue_reason(meta, out["signal"], out["reason"]))
        key = out["signal"] if out["reason"] == "scored" else out["reason"]
        counts[key] = counts.get(key, 0) + 1
    if counts:
        logger.info("[TAIWAN_REVENUE] %s", " ".join(f"{k}={v}" for k, v in sorted(counts.items())))


# Remarks that carry no explanation at all - no repeat flag is set for these.
_EMPTY_REMARKS = frozenset({"", "-", "無", "無。"})
_REMARK_REPEAT_MONTHS = 12


def _normalize_remark(text: str | None) -> str:
    return re.sub(r"\s+", "", text or "")


def mark_repeated_remarks(
    articles: list[dict[str, Any]],
    past_remarks: list[dict[str, Any]] | None = None,
) -> None:
    """Set metadata.is_repeated_remark_12m on each mops_revenue article with
    an explanation: True when the same company gave the same text (ignoring
    whitespace) in any of the previous 12 months, so the page can hide it.

    Measured on 2019-26 MOPS remarks: 58% repeat LAST month's text, 66%
    repeat something from the last 12 months, 67% ever - comparing only to
    last month would miss e.g. TSMC reusing its April 2024 sentence in
    December 2024. 12 months catches nearly all of it without hiding a
    sentence a company reuses years later, when it may be news again.
    Same company only: another company's identical sentence is still news
    for this one.

    `past_remarks`: already-stored rows as {"ticker", "period_gregorian",
    "remarks"}. Earlier months in this same batch count too.
    """
    seen: dict[str, dict[int, str]] = {}

    def _remember(ticker: Any, period: Any, text: Any) -> tuple[int, str] | None:
        try:
            year, month = (int(x) for x in str(period).split("-"))
        except ValueError:
            return None
        norm = _normalize_remark(text)
        idx = taiwan_month_index(year, month)
        if norm not in _EMPTY_REMARKS:
            seen.setdefault(str(ticker), {})[idx] = norm
        return idx, norm

    for r in past_remarks or []:
        _remember(r.get("ticker"), r.get("period_gregorian"), r.get("remarks"))
    current = []
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "mops_revenue":
            continue
        got = _remember(meta.get("ticker"), meta.get("period_gregorian"), meta.get("remarks"))
        if got:
            current.append((meta, *got))
    for meta, idx, norm in current:
        if norm in _EMPTY_REMARKS:
            continue
        earlier = seen.get(str(meta.get("ticker")), {})
        meta["is_repeated_remark_12m"] = any(
            earlier.get(j) == norm for j in range(idx - _REMARK_REPEAT_MONTHS, idx))


def classify_material_announcements(articles: list[dict[str, Any]]) -> None:
    """Attach each material announcement's clause meaning (clause_label) and
    period_gregorian, in place. The HIGH/WEAK/NOISE rating is set afterwards
    by taiwan_announcement_classifier.classify_announcements.

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
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "mops_material":
            continue
        code = meta.get("disclosure_clause_code")
        label = clause_label(meta.get("exchange"), code)
        if label:
            a["metadata"]["clause_label"] = label
        else:
            logger.warning("[TAIWAN_MATERIALITY] no clause label for exchange=%r code=%r",
                           meta.get("exchange"), code)
        statement_date = meta.get("statement_date")
        if statement_date and len(statement_date) >= 7:
            a["metadata"]["period_gregorian"] = statement_date[:7]


def translate_taiwan_articles(
    articles: list[dict[str, Any]],
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: int | None = None,
    known_remark_translations: dict[str, str] | None = None,
) -> None:
    """Translate the fields that need it, per source_category, in place.

    Only touches articles whose metadata.source_category has an entry in
    _TRANSLATION_FIELDS. Native-language fields are never overwritten;
    translated values are added as new metadata fields (e.g.
    metadata.translated_title). Failed translations leave the
    translated_* field absent rather than set to None/empty.

    Revenue remarks (the company's own explanation of the month) repeat
    heavily - 471 remarks in 2019-26 held only 147 distinct texts, one
    sentence 32 times from the same company. So each distinct remark is
    translated once: `known_remark_translations` (Chinese text -> English,
    from already-stored rows) is reused without a model call, and identical
    texts within this batch share one call. Same text, same English, every
    time.

    Defaults to OPENAI_MODEL_V2 (the cheaper refine-pass model, e.g.
    gpt-4o-mini per infra/CLAUDE.md), not the base OPENAI_MODEL - this is a
    mechanical translation task, not a judgment call, so it doesn't need
    the stronger/pricier base classification model.
    """
    model = model or config.OPENAI_MODEL_V2
    api_key = api_key or config.OPENAI_API_KEY
    base_url = base_url or config.OPENAI_BASE_URL
    timeout = timeout or config.OPENAI_TIMEOUT

    known = known_remark_translations or {}
    to_translate: list[tuple[dict, str, str]] = []  # (article, source_path, dest_field)
    # Remark text -> every other article in this batch carrying the same text
    # (the first is in to_translate and gets the one model call).
    remark_followers: dict[str, list[dict]] = {}
    reused = 0
    for a in articles:
        category = (a.get("metadata") or {}).get("source_category")
        field_map = _TRANSLATION_FIELDS.get(category)
        if not field_map:
            continue
        for source_path, dest_field in field_map.items():
            text = _get_nested(a, source_path)
            if not text or a["metadata"].get(dest_field):
                continue  # nothing to translate, or already written
            if dest_field == "translated_body" and a["metadata"].get("translated_summary"):
                continue  # a checked English summary covers the body
            if source_path == "metadata.remarks":
                if text in known:
                    a["metadata"][dest_field] = known[text]
                    reused += 1
                    continue
                if text in remark_followers:
                    remark_followers[text].append(a)
                    continue
                remark_followers[text] = []
            to_translate.append((a, source_path, dest_field))

    def _translate_and_apply(item: tuple[dict, str, str]) -> bool:
        article, source_path, dest_field = item
        text = _get_nested(article, source_path)
        result = _translate_one(text, model, api_key, base_url, timeout)
        if result is not None and source_path in ("summary", "body") and \
                (article.get("metadata") or {}).get("source_category") == "mops_material":
            result = apply_known_names(result)  # verified group-company names (Cyntec, SPIL...)
        if result is not None:
            article["metadata"][dest_field] = result
            if source_path == "metadata.remarks":
                for follower in remark_followers.get(text, []):
                    follower["metadata"][dest_field] = result
            return True
        return False

    translated_count = 0
    if to_translate:
        with ThreadPoolExecutor(max_workers=config.TAIWAN_CLASSIFY_CONCURRENCY) as executor:
            translated_count = sum(executor.map(_translate_and_apply, to_translate))
    if reused:
        logger.info("[TAIWAN_TRANSLATE] %d remark(s) reused an existing translation", reused)

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


def taiwan_source_id(article: dict[str, Any]) -> str | None:
    """The dedup key a classified row is stored under - knowable before
    classifying, so the caller can skip already-stored items first.

    Revenue: ticker + reporting period. Material announcement: ticker +
    statement date + time. GDELT: the article url (each is a distinct
    story, already url- and title-deduplicated by news-retrieval).
    """
    meta = article.get("metadata") or {}
    category = meta.get("source_category")
    if category == "mops_revenue":
        return f"{meta.get('ticker')}-{meta.get('period_gregorian')}"
    if category == "mops_material":
        return f"{meta.get('ticker')}-{meta.get('statement_date')}-{meta.get('statement_time')}"
    if category == "gdelt":
        return article.get("url")
    return None


def classify_taiwan_signal_batch(
    articles: list[dict[str, Any]],
    model: str | None = None,
    revenue_history: dict[str, dict[int, tuple[float, float | None]]] | None = None,
    known_remark_translations: dict[str, str] | None = None,
    past_remarks: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Run the full taiwan_market_signal classification pass on a batch of
    articles read from news-retrieval: score revenue, classify material
    announcements, translate whatever needs it - then build one result dict
    per article for insert_taiwan_signal_classification.

    Every item is judged on its own (revenue against its own company's
    history, announcements by clause, GDELT per headline), so a batch can
    be any subset - callers pass only what is not yet stored.

    ``revenue_history``: build_revenue_history() over news-retrieval's
    /market/taiwan/revenue-history. Without it every revenue filing would
    come back not_scored_short_history, so callers should not classify
    revenue at all when the history could not be read.

    ``past_remarks``: this batch's companies' stored revenue remarks from
    the last 12 months, for is_repeated_remark_12m.
    """
    score_revenue_articles(articles, revenue_history or {})
    mark_repeated_remarks(articles, past_remarks)
    classify_material_announcements(articles)
    classify_announcements(articles)
    translate_taiwan_articles(articles, model=model,
                              known_remark_translations=known_remark_translations)

    results = []
    for a in articles:
        meta = a.get("metadata") or {}
        category = meta.get("source_category")
        if category == "mops_revenue":
            signal, reason = a.pop("_revenue_signal", (None, None))
        elif category == "mops_material":
            # Carried here from the announcement classifier and not stored:
            # they are the signal_detection / signal_reason columns.
            signal = meta.pop("announcement_materiality_signal", None)
            reason = meta.pop("announcement_materiality_reason", None)
        else:
            continue  # gdelt handled separately below (needs a model call
            # per article, not a metadata lookup) - and any other/unknown
            # source_category is correctly skipped here too.

        if signal is None:
            continue  # e.g. revenue row with an unparseable period

        results.append({
            "article": a,
            "result": {
                # Deterministic rules, not a model's confidence - the
                # column stays null (revenue's own score is in
                # metadata.revenue_deviation). signal_score the column is
                # reserved for a path where a model produces one (gdelt).
                "signal": signal,
                "signal_score": None,
                "source_id": taiwan_source_id(a),
                "reason": reason,
                "metadata": meta,
                "entities": a.pop("_entities", []),
            },
        })

    results.extend(classify_gdelt_articles(articles, model=model))
    return results
