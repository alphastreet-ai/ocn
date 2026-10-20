"""Deterministic classification for korea_market_signal items (DART
filings and Korea Customs export data read from news-retrieval).

Follows the same design as taiwan_signal_classifier.py: news-retrieval
stays fetch/dedup-only, uniformly across every domain - no source type
computes a signal or translates text there. Arithmetic/lookup rules live
here; a real LLM call is reserved for the news layer (S7) only, per the
Korea Signals spec's own design intent ("five of the seven signal types
are decided by arithmetic or by looking up a code").

Signal types and their status here:
  S1 export surprise      - REOPENED 2026-09-23 (was briefly marked
                             permanently out of scope - see below for why
                             that reversed) and IMPLEMENTED:
                             classify_export_surprise(). The two API-key
                             backfill paths (data.go.kr, Bank of Korea
                             ECOS) both genuinely require Korea's mobile-
                             carrier identity verification (PASS, run by
                             NICE) - confirmed live against both real
                             signup flows, no foreigner path exists for
                             either. BUT: news-retrieval's own Customs
                             board scraper turned out to already expose
                             full pagination on its title-search endpoint
                             (no login at all) reaching back through real
                             historical posts - a code limitation (only
                             ever reading page 1's first match), not a
                             source limitation. news-retrieval's
                             fetch_customs_export_backfill() (pipeline.py)
                             now walks that pagination for real historical
                             semiconductor figures. Honest remaining gap:
                             the board's own detail-page FORMAT changes
                             further back in its history (confirmed live:
                             March 2026 in the specific backfill run
                             checked) - posts older than that are stub
                             pages linking to an unopenable .hwpx/.pdf
                             attachment, so real backfillable depth is
                             ~7 months, not the full 2 years Section 5.1's
                             own build note asks for. This function's own
                             per-period-type precondition check (needs a
                             same-period-one-year-ago reading AND 12 prior
                             equivalent periods) means it correctly
                             produces ZERO real classifications today -
                             not a bug, an honest reflection of real data
                             not existing yet - and will start producing
                             real output as the existing 4-hourly poll's
                             own organic growth (plus this backfill's head
                             start) crosses that threshold over the
                             following months.
  S2 supply contract       - IMPLEMENTED: classify_supply_contract() +
                             translation via _TRANSLATION_FIELDS["supply_contract"]
  S3 capacity commitment   - IMPLEMENTED: classify_capacity_commitment() +
                             translation via _TRANSLATION_FIELDS["capacity_commitment"].
                             NOISE (routine replacement/maintenance spending)
                             is not yet detected - no real example seen in
                             filings checked so far to design the rule
                             against; every real filing so far is at least
                             WEAK, never NOISE.
  S4 preliminary earnings  - IMPLEMENTED: classify_preliminary_earnings() +
                             translation via
                             _TRANSLATION_FIELDS["preliminary_earnings"]
                             (title only - the results table itself is
                             numeric, not free text)
  S5 guidance disclosure   - IMPLEMENTED: classify_guidance_disclosure() +
                             translation via
                             _TRANSLATION_FIELDS["guidance_disclosure"].
                             NOISE (no view at all) is not yet detected -
                             same honest gap as S3, no real example seen
                             so far (every real filing checked had a real
                             number, classifying as SIGNAL).
  S6 rumour adjudication   - IMPLEMENTED across three functions:
                             classify_rumour_adjudication() (exchange
                             demand + undecided/denial answers, via
                             report_nm's dedicated 조회공시 patterns),
                             classify_voluntary_clarification() (해명 -
                             a DISTINCT company-initiated mechanism,
                             confirmed via research NOT to carry the same
                             "always Signal" exchange-compulsion
                             justification), and classify_rumour_confirmation()
                             (the CONFIRMED outcome, which per KRX's own
                             disclosure manual has NO dedicated report_nm
                             at all - detected by scanning every
                             dart_filing body for a boilerplate sentence,
                             not a title lookup). Translation via
                             _TRANSLATION_FIELDS["rumour_adjudication"] /
                             ["voluntary_clarification"].
  S7 qualification news    - IMPLEMENTED: classify_qualification_news() +
                             translation via _TRANSLATION_FIELDS["qualification_news"].
                             Three free filters (trusted publication,
                             company name in headline - extended to also
                             check body, live-confirmed 2026-09-23 to
                             surface real candidates a headline-only check
                             was silently dropping - repeat-story count)
                             then one HIGH/WEAK model call (spec 8.1-8.3),
                             mirroring Taiwan's classify_gdelt_articles -
                             the one real LLM call in this module. Known
                             gap: model call doesn't yet receive
                             publications_carrying_this/english_coverage_found/
                             price_move_today as context (computed but not
                             sent - see the function's own docstring).
                             Section 5.8's two override rules (already-
                             moved price action, index-reweighting
                             suppression) aren't implemented anywhere -
                             checked live 2026-09-23, Alpha Vantage has no
                             real KRX quote for this universe (see the
                             note further down), a genuine data-source
                             gap rather than a code gap.

English-coverage comparison (spec Section 7 Step 5 / Section 6.4's
"English press" line): IMPLEMENTED via _attach_english_coverage(), called
at the end of classify_korea_signal_batch() against the full pooled
batch. Not yet live-verified end-to-end against a real English GDELT row
(KOREA_GDELT_ENGLISH_SOURCE has never actually landed a stored row yet -
GDELT's own rate limit has blocked every live fetch attempt this session,
left untouched per explicit instruction) - verified instead with a
synthetic row shaped exactly like a real one (ticker suffixed "-en",
matching seed.py's own construction) reproducing the spec's own Section
6.5 worked example (Hanmi Semiconductor, "English coverage appeared 6h
after the Korean") to confirm the join/delta arithmetic is correct. Needs
a real live check once GDELT's rate limit clears and a real
korea_market_signal_english row actually lands in storage.

Section 5.8 (the two rules that override everything above - "already
moved" price-action downgrade, index-reweighting suppression) - NOT
IMPLEMENTED ANYWHERE. Both need real-time KRX stock price data. Checked
live 2026-09-23: Alpha Vantage (the only market-data source already wired
into this codebase - see news-retrieval's poller.py) has no real KRX quote
for these companies - GLOBAL_QUOTE for 000660.KS/005930.KS returns an
empty object, and SYMBOL_SEARCH for "SK Hynix" surfaces only US OTC ADR
proxies (HXSCL, SKHY) and a Frankfurt GDR (HY9H.FRK), none of which track
real KRX price action (different market, different hours, different
liquidity - using one as a stand-in would produce a misleading "already
moved" signal, not a correct one). Same shape as the S1 blocker (a real
external data source is missing, not a code gap) - not pursued further per
explicit decision, pending a real KRX-quote source being found or added.

Orchestration: classify_korea_signal_batch() runs S2-S7 (S1 excluded, not
yet implemented) against one pooled batch and translates whatever
survived - the function controllers/run.py's run_korea_signal_classification
calls, same role as classify_taiwan_signal_batch for Taiwan.

Translation (Korea Signals spec Section 7 Step 8 / Section 2's "Plain
English, with the Korean original attached" want): reuses
taiwan_signal_classifier's _translate_one/_get_nested directly rather
than re-implementing the same raw HTTP-to-OpenAI-compatible-endpoint call
a second time - that machinery has no Taiwan-specific logic in it (it's a
generic "translate this string" helper), so importing it here is
deliberate reuse, not an accidental cross-domain dependency. Only
_TRANSLATION_FIELDS (which fields need translation, per source_category)
and translate_korea_articles() (the orchestration loop) are Korea's own -
same split Taiwan itself has between its generic helpers and its own
_TRANSLATION_FIELDS map/translate_taiwan_articles() loop.
"""
from __future__ import annotations

import json
import logging
import re
import statistics
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

import config
from pipeline.taiwan_signal_classifier import _get_nested, _translate_one

logger = logging.getLogger(__name__)

# Korea Signals spec Section 5.2. All three fields are stated by the
# company itself in a standardized KRX disclosure form - not free-text
# guessing, a fixed template field DART's own document body renders as
# plain "label(unit) value" text after HTML stripping. Percentage is
# pre-calculated by the company under a legal obligation (percent-of-
# revenue), matching the spec's own point that "we do not have to guess
# whether a contract is big."
#
# TWO real form templates exist, confirmed live 2026-09-23 against 8 real
# 단일판매ㆍ공급계약체결/[기재정정] filings from LS Electric, Samsung
# Electro-Mechanics, HD Hyundai Electric, Hanmi Semiconductor, Techwing -
# the mandatory-disclosure template (most companies) and Techwing's own
# 자율공시 (voluntary disclosure) template, which uses genuinely different
# field labels, not just a formatting quirk:
#   mandatory: 계약금액(원) / 최근매출액(원) / 매출액대비(%) / 계약상대
#   자율공시:  계약금액 총액(원) / 최근 매출액(원) (note the space) /
#              매출액 대비(%) (note the space) / 계약상대방 (note the 방)
# An earlier version of this module only handled the mandatory template
# and silently produced "fields_not_extracted" on both real Techwing
# filings, plus a genuine extraction bug on 계약상대방 (matching starting
# right after 계약상대 left a stray "방" character glued onto the front of
# the real company name, e.g. "방 삼성전자 주식회사" instead of "삼성전자
# 주식회사") - caught by inspecting raw body text against real filings,
# not assumed fixed from the first three examples matching cleanly.
_CONTRACT_AMOUNT_PATTERN = re.compile(r"계약금액(?:\s*총액)?\(원\)\s*([\d,]+)")
_PRIOR_YEAR_REVENUE_PATTERN = re.compile(r"최근\s?매출액\(원\)\s*([\d,]+)")
_PCT_OF_REVENUE_PATTERN = re.compile(r"매출액\s?대비\(%\)\s*([\d.]+)")
_COUNTERPARTY_PATTERN = re.compile(r"계약상대방?\s*([^\-]+?)\s*-")

# Confirmed NOT present on every S2 filing template - handle as a
# missing-field case, not an extraction failure - the counterparty name
# check still applies even when the percentage fields are absent.
_PCT_SIGNAL_THRESHOLD = 20.0


def _parse_krw(raw: str) -> int | None:
    try:
        return int(raw.replace(",", ""))
    except ValueError:
        return None


def _extract_contract_fields(body: str) -> dict[str, Any]:
    """Pull the label-value fields Korea Signals spec Section 5.2 needs
    out of a DART supply-contract filing's plain-text body.

    Returns a dict with whichever fields were actually found - callers
    must check for None/missing keys rather than assume all four are
    always present (see Techwing counter-example in module docstring).
    """
    fields: dict[str, Any] = {}

    amount_match = _CONTRACT_AMOUNT_PATTERN.search(body)
    if amount_match:
        fields["contract_amount_krw"] = _parse_krw(amount_match.group(1))

    revenue_match = _PRIOR_YEAR_REVENUE_PATTERN.search(body)
    if revenue_match:
        fields["prior_year_revenue_krw"] = _parse_krw(revenue_match.group(1))

    pct_match = _PCT_OF_REVENUE_PATTERN.search(body)
    if pct_match:
        try:
            fields["pct_of_revenue"] = float(pct_match.group(1))
        except ValueError:
            pass

    counterparty_match = _COUNTERPARTY_PATTERN.search(body)
    if counterparty_match:
        counterparty = counterparty_match.group(1).strip()
        # Confirmed live: withheld counterparties render as a placeholder
        # phrase ("글로벌 대형기업" - "global large enterprise") rather than
        # an empty field or a dash - treat this specific phrase as "not
        # named", not as a literal company name to match against
        # KOREA_TICKER_UNIVERSE.
        fields["counterparty"] = counterparty
        fields["counterparty_withheld"] = counterparty in ("글로벌 대형기업", "", "-")

    return fields


# Korea Signals spec Section 5's own vocabulary is SIGNAL/WEAK/NOISE, not
# Taiwan's HIGH/WEAK/NOISE (models/jobs.py's _TAIWAN_SIGNAL_MAP) - same
# 3-value shape and same target column (agent_classifications.
# signal_detection), but the source keys differ, so this is its own table
# rather than reusing Taiwan's dict with the wrong key names.
_KOREA_SIGNAL_MAP = {"SIGNAL": "signal", "WEAK": "weak_signal", "NOISE": "noise"}


def classify_supply_contract(
    articles: list[dict[str, Any]],
    tracked_companies: dict[str, str] | None = None,
) -> list[dict[str, Any]]:
    """Classify DART supply-contract filings (금액/판매 form report types).

    Only touches articles whose title contains 단일판매 or 공급계약
    (matching news-retrieval's own DART target-pattern list) and that
    have a fetched body - a header-only row (body not fetched) has
    nothing to extract and is skipped, same as classify_gdelt_articles
    skipping articles with no translated_title.

    Korea Signals spec Section 5.2 rule:
      SIGNAL - contract is >=20% of last year's revenue, OR the named
               counterparty is a company we track
      WEAK   - above the legal reporting threshold (this filing exists at
               all) but below 20% and no tracked counterparty
      NOISE  - an amendment/correction ([기재정정]) that commits no new
               money - confirmed live this is a real, distinct case
               (HD Hyundai Electric's [기재정정] filing was a genuine
               correction to an already-reported contract, not new money)

    tracked_companies: {english_or_korean_name: ticker} - used to check
    whether an extracted counterparty name matches a company we already
    track (the spec's "the customer is named and is one we track" OR
    condition). Optional - if omitted, only the percentage threshold is
    checked.

    Returns a list of {"article": ..., "result": {...}} dicts, same shape
    classify_gdelt_articles returns - result["source_id"] is the filing's
    own DART rcept_no (a permanent, unique key per filing, same role as
    Taiwan's ticker+period/ticker+timestamp), so the caller's existing
    dedup-before-insert pattern (get_existing_taiwan_source_ids's
    Korea-equivalent) works unchanged.
    """
    tracked_companies = tracked_companies or {}
    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0, "NOISE": 0}

    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "dart_filing":
            continue
        title = a.get("title", "")
        if "단일판매" not in title and "공급계약" not in title:
            continue
        body = a.get("body")
        if not body:
            continue  # header-only row, no fields to extract
        rcept_no = meta.get("rcept_no")
        if not rcept_no:
            continue  # no stable key to dedup/insert against

        fields = _extract_contract_fields(body)

        # An amendment/correction filing that restates an existing
        # contract commits no new money - confirmed live this report_nm
        # pattern exists ([기재정정] = "content correction"). Not every
        # [기재정정] filing is automatically noise in general (DART uses
        # this tag broadly), but for a supply-contract filing specifically
        # it means "correcting details of an already-disclosed contract,"
        # which is the spec's own NOISE case.
        is_correction = "기재정정" in title

        pct = fields.get("pct_of_revenue")
        counterparty = fields.get("counterparty")
        counterparty_withheld = fields.get("counterparty_withheld", True)
        counterparty_tracked = (
            not counterparty_withheld
            and counterparty is not None
            and any(name in counterparty for name in tracked_companies)
        )

        if is_correction:
            signal = "NOISE"
            reason = "correction_no_new_money"
        elif counterparty_tracked:
            signal = "SIGNAL"
            reason = f"named_tracked_counterparty:{counterparty}"
        elif pct is not None and pct >= _PCT_SIGNAL_THRESHOLD:
            signal = "SIGNAL"
            reason = f"pct_of_revenue_{pct}_gte_{_PCT_SIGNAL_THRESHOLD}"
        elif pct is not None:
            signal = "WEAK"
            reason = f"pct_of_revenue_{pct}_below_threshold"
        else:
            # Filing exists (legal reporting threshold already met by the
            # fact it was filed at all) but neither field extracted -
            # different form template (e.g. Techwing's 자율공시 case) or a
            # genuinely missing field. Weak, not noise or a dropped row -
            # a real disclosure with unextracted numbers is still more
            # than nothing, and silently discarding it would understate
            # real filing volume to signal-detection-agent's consumers.
            signal = "WEAK"
            reason = "fields_not_extracted"

        counts[signal] += 1
        # Written back into the live article's metadata in place - not a
        # separate copy - so a later translate_korea_articles() pass (run
        # on this same `articles` list, same sequencing as Taiwan's
        # classify-then-translate-in-place-then-build-results order in
        # classify_taiwan_signal_batch) can add translated_* fields onto
        # the same dict this function already populated, and the final
        # result["metadata"] naturally picks up both.
        meta.update(fields)
        meta["source_category"] = "supply_contract"
        a["metadata"] = meta
        results.append({
            "article": a,
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                # Deterministic rule, not a scored judgment - null is more
                # honest than a fabricated confidence value, same
                # reasoning taiwan_signal_classifier.py uses for its own
                # rank/clause-lookup paths (signal_score reserved for a
                # path where a model genuinely produces one).
                "signal_score": None,
                "source_id": rcept_no,
                "reason": reason,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[KOREA_SUPPLY_CONTRACT] classified signal=%d weak=%d noise=%d",
            counts["SIGNAL"], counts["WEAK"], counts["NOISE"],
        )
    return results


# Korea Signals spec Section 5.3. TWO real, genuinely different DART form
# templates cover this signal type - confirmed live 2026-09-23 against 6
# real filings (Simmtech, SK Hynix, SK Square, Daeduck Electronics,
# Hanmi Semiconductor x2):
#
#   "신규시설투자등" (new facility investment) - 투자금액(원) / 자기자본(원)
#   / 자기자본대비(%) / 투자목적 / 투자기간 시작일·종료일. The percentage
#   here is against EQUITY (자기자본/capital), not total assets - a
#   materially different, larger-looking number than a %-of-total-assets
#   figure for the same investment (equity is always <= total assets).
#   4 of 6 real filings used this template (Simmtech 47.6%, SK Hynix
#   29.19%, SK Square 29.19% [see dedup note below], Daeduck 55.39%).
#
#   "유형자산취득결정" (tangible asset acquisition decision) -
#   취득가액(원) / 자산총액(원) / 자산총액대비(%) / 거래상대 / 취득목적.
#   This one genuinely does match the spec's literal wording ("percentage
#   of total assets") - Hanmi Semiconductor's two real examples (6.54%,
#   6.89%) both used this template.
#
# The spec's own rule text ("Amount as a percentage of total assets") is
# only exactly correct for the second template - stored as two distinct
# metadata fields (pct_of_equity / pct_of_total_assets) rather than
# force-unified into one "pct" field that would misrepresent which base a
# given filing's percentage is actually measured against.
_INVESTMENT_AMOUNT_PATTERN = re.compile(r"투자금액\(원\)\s*([\d,]+)")
_EQUITY_PATTERN = re.compile(r"자기자본\(원\)\s*([\d,]+)")
_PCT_OF_EQUITY_PATTERN = re.compile(r"자기자본대비\(%\)\s*([\d.]+)")
_INVESTMENT_PURPOSE_PATTERN = re.compile(r"투자목적\s*(.+?)\s*4\.\s*투자기간")

_ACQUISITION_AMOUNT_PATTERN = re.compile(r"취득가액\(원\)\s*([\d,]+)")
_TOTAL_ASSETS_PATTERN = re.compile(r"자산총액\(원\)\s*([\d,]+)")
_PCT_OF_TOTAL_ASSETS_PATTERN = re.compile(r"자산총액대비\(%\)\s*([\d.]+)")
_ACQUISITION_PURPOSE_PATTERN = re.compile(r"취득목적\s*(.+?)\s*5\.\s*취득예정일자")

_PCT_SIGNAL_THRESHOLD_S3 = 10.0
# Korea Signals spec Section 5.3's own reasoning for this window: "A
# company approving a new factory in the same fortnight that it reported
# a large contract is a much stronger statement than either on its own."
_LINK_WINDOW_DAYS = 14


def _extract_capacity_fields(body: str, title: str) -> dict[str, Any]:
    """Pull the label-value fields Korea Signals spec Section 5.3 needs,
    trying both real DART form templates (see module comment above) -
    title decides which template's field labels to look for, since the
    two templates' labels don't overlap and guessing wrong template first
    would just fail to match, not silently extract the wrong field.
    """
    fields: dict[str, Any] = {}
    if "유형자산" in title:
        amount_match = _ACQUISITION_AMOUNT_PATTERN.search(body)
        base_match = _TOTAL_ASSETS_PATTERN.search(body)
        pct_match = _PCT_OF_TOTAL_ASSETS_PATTERN.search(body)
        purpose_match = _ACQUISITION_PURPOSE_PATTERN.search(body)
        if amount_match:
            fields["investment_amount_krw"] = _parse_krw(amount_match.group(1))
        if base_match:
            fields["total_assets_krw"] = _parse_krw(base_match.group(1))
        if pct_match:
            fields["pct_of_total_assets"] = float(pct_match.group(1))
        if purpose_match:
            fields["investment_purpose"] = purpose_match.group(1).strip()
    else:
        amount_match = _INVESTMENT_AMOUNT_PATTERN.search(body)
        base_match = _EQUITY_PATTERN.search(body)
        pct_match = _PCT_OF_EQUITY_PATTERN.search(body)
        purpose_match = _INVESTMENT_PURPOSE_PATTERN.search(body)
        if amount_match:
            fields["investment_amount_krw"] = _parse_krw(amount_match.group(1))
        if base_match:
            fields["equity_krw"] = _parse_krw(base_match.group(1))
        if pct_match:
            fields["pct_of_equity"] = float(pct_match.group(1))
        if purpose_match:
            fields["investment_purpose"] = purpose_match.group(1).strip()
    return fields


def _article_published_dt(article: dict[str, Any]) -> datetime | None:
    published = article.get("published")
    if isinstance(published, datetime):
        return published if published.tzinfo else published.replace(tzinfo=timezone.utc)
    if isinstance(published, str) and published:
        try:
            return datetime.fromisoformat(published).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def classify_capacity_commitment(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Classify DART capacity-commitment filings (신규시설투자등 /
    유형자산취득결정 form types).

    Only touches articles whose title contains 시설투자 or 유형자산
    (matching news-retrieval's own DART target-pattern list) and that
    have a fetched body.

    Korea Signals spec Section 5.3 rule:
      SIGNAL - investment is >=10% of its stated base (total assets for
               유형자산 filings, equity for 신규시설투자 filings - see
               module comment on why these are tracked separately), OR
               another dart_filing-sourced signal from the SAME company
               (by stock_code) landed within the previous 14 days
      WEAK   - a disclosed investment below that size with nothing else
               nearby
      NOISE  - routine replacement/maintenance spending - NOT
               automatically detected here (no real example of this in
               the filings checked so far to confirm what marks a filing
               as "routine" vs "new capacity" - 취득목적 values seen so
               far were all substantive, e.g. "AI향 SOCAMM모듈 기판...
               생산능력 확대" - so every real filing checked classifies as
               at least WEAK, never NOISE, until a real routine-spending
               example is found to design the actual rule against)

    The 14-day link needs every OTHER dart_filing-sourced result already
    classified in this same batch as context (not just supply-contract
    results specifically - the spec's own wording is "another signal from
    the same company," not "another supply contract") - pass
    other_company_signals (a list of {stock_code, published} for anything
    already classified as SIGNAL this run) to check against. Callers
    build this from whatever other classify_* calls already ran in the
    same batch (see classify_korea_signal_batch).

    A real, confirmed duplication case exists in this data: a parent
    holding company (SK Square) files its own "(자회사의 주요경영사항)"
    echo of a subsidiary's (SK Hynix) capacity-commitment filing - same
    investment, same amount, same dates, different rcept_no/corp_code.
    NOT deduplicated here (each is a real, independent DART filing with
    its own source_id/rcept_no, and the spec doesn't say these should be
    merged) - flagged in metadata via is_subsidiary_echo so a downstream
    consumer (the daily summary, Section 6.5's "group items covering the
    same story") can choose to collapse them, rather than silently
    dropping one filing news-retrieval genuinely fetched.
    """
    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0, "NOISE": 0}

    # First pass: extract fields and compute the percentage-threshold
    # signal for every candidate, since the 14-day-link check (second
    # pass below) needs to know which OTHER filings in this same batch
    # already qualify as SIGNAL - a single-pass approach would miss a
    # same-batch link depending on article order.
    candidates: list[dict[str, Any]] = []
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "dart_filing":
            continue
        title = a.get("title", "")
        if "시설투자" not in title and "유형자산" not in title:
            continue
        body = a.get("body")
        if not body:
            continue
        rcept_no = meta.get("rcept_no")
        if not rcept_no:
            continue

        fields = _extract_capacity_fields(body, title)
        pct = fields.get("pct_of_total_assets") or fields.get("pct_of_equity")
        pub_dt = _article_published_dt(a)
        candidates.append({
            "article": a,
            "meta": meta,
            "title": title,
            "rcept_no": rcept_no,
            "fields": fields,
            "pct": pct,
            "pub_dt": pub_dt,
            "stock_code": meta.get("stock_code"),
            "is_subsidiary_echo": "자회사" in title,
        })

    for c in candidates:
        pct_signal = c["pct"] is not None and c["pct"] >= _PCT_SIGNAL_THRESHOLD_S3

        # 14-day link: another SIGNAL-worthy filing (percentage-threshold
        # OR itself already link-qualified) from the same stock_code,
        # strictly within the window, excluding this filing itself.
        linked_rcept_no = None
        if not pct_signal and c["pub_dt"] and c["stock_code"]:
            for other in candidates:
                if other["rcept_no"] == c["rcept_no"]:
                    continue
                if other["stock_code"] != c["stock_code"]:
                    continue
                other_pct_signal = (
                    other["pct"] is not None and other["pct"] >= _PCT_SIGNAL_THRESHOLD_S3
                )
                if not other_pct_signal or not other["pub_dt"]:
                    continue
                if abs((c["pub_dt"] - other["pub_dt"]).days) <= _LINK_WINDOW_DAYS:
                    linked_rcept_no = other["rcept_no"]
                    break

        if pct_signal:
            base = "total_assets" if "pct_of_total_assets" in c["fields"] else "equity"
            signal = "SIGNAL"
            reason = f"pct_of_{base}_{c['pct']}_gte_{_PCT_SIGNAL_THRESHOLD_S3}"
        elif linked_rcept_no:
            signal = "SIGNAL"
            reason = f"linked_signal_within_{_LINK_WINDOW_DAYS}d:{linked_rcept_no}"
        elif c["pct"] is not None:
            signal = "WEAK"
            reason = "below_pct_threshold_no_link"
        else:
            # Same reasoning as classify_supply_contract's
            # "fields_not_extracted" fallback - a real disclosure with
            # unextracted numbers is still more than nothing.
            signal = "WEAK"
            reason = "fields_not_extracted"

        counts[signal] += 1
        meta = c["meta"]
        meta.update(c["fields"])
        meta["source_category"] = "capacity_commitment"
        meta["is_subsidiary_echo"] = c["is_subsidiary_echo"]
        c["article"]["metadata"] = meta
        results.append({
            "article": c["article"],
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": c["rcept_no"],
                "reason": reason,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[KOREA_CAPACITY_COMMITMENT] classified signal=%d weak=%d noise=%d",
            counts["SIGNAL"], counts["WEAK"], counts["NOISE"],
        )
    return results


# Korea Signals spec Section 5.4. Confirmed live 2026-09-23 against 3 real
# 연결재무제표기준영업(잠정)실적(공정공시) filings (Daeduck Electronics, DB
# HiTek, Hanmi Semiconductor) - a single flattened results table, not
# separate label-value fields like S2/S3. After HTML stripping, one row
# per financial line item reads as:
#   {label} 당해실적 {current_qtr} {prior_qtr} {qoq_pct_or_dash}
#   {qoq_transition_flag_or_dash} {yoy_qtr} {yoy_pct_or_dash}
#   {yoy_transition_flag_or_dash}
# in KRW millions (단위: 백만원 - stated once near the top of the table,
# not repeated per row). Two real quirks a naive percentage regex misses:
#   1. YoY%/QoQ% CAN exceed 999% and Korean number formatting still
#      thousands-commas it (confirmed live: Daeduck's real operating
#      profit YoY change was "3,666.0" - a genuine 3,666% swing off a
#      near-zero prior-year base, not a formatting artifact) - a percent
#      pattern of [\d.]+ alone silently fails to match these rows at all.
#   2. When one side of the comparison is a genuine swing between profit
#      and loss, DART reports a literal "흑자전환"/"적자전환" (swung to
#      profit / swung to loss) flag INSTEAD of a percentage, not alongside
#      one - confirmed live on Daeduck's and Hanmi's operating-profit
#      rows. A swing is always Section 5.4 SIGNAL-worthy (a profit/loss
#      swing is definitionally a >20% change; there is no finite
#      percentage to compute from a near-zero or negative base) - handled
#      as its own case, not coerced into a fake percentage number.
_QUARTERLY_TABLE_PATTERN = re.compile(
    r"(매출액|영업이익)\s+당해실적\s+"
    r"(-?[\d,.]+)\s+(-?[\d,.]+)\s+(-?[\d,.]+|-)\s+(흑자전환|적자전환|-)\s+"
    r"(-?[\d,.]+)\s+(-?[\d,.]+|-)\s+(흑자전환|적자전환|-)"
)

_OPERATING_MARGIN_PCT_POINTS_THRESHOLD = 5.0
_QOQ_YOY_PCT_SIGNAL_THRESHOLD = 20.0


# DART's own unit declaration ("단위 : 백만원, %" / million won, or
# "단위 : 조원, %" / trillion won for at least one real filer - confirmed
# live 2026-09-23: Samsung Electronics reports its preliminary-earnings
# table in 조원/trillion won with decimal values like "171.50", while
# every other company checked (Daeduck, DB HiTek, Hanmi, SK Hynix, etc.)
# reports in 백만원/million won with large comma-separated integers like
# "400,975" - both for the exact same table structure/labels). Reading
# this declared unit and normalizing every filer's figures to a single
# consistent scale (KRW millions) is required - storing Samsung's
# "171.50" and Daeduck's "400,975" under the same *_krw_mn field name
# without normalizing would silently mix a value ~40,000x too small into
# comparisons/aggregates against every other filer.
_UNIT_DECLARATION_PATTERN = re.compile(r"단위\s*:\s*([^,]+),")
_KRW_UNIT_TO_MILLIONS = {"백만원": 1, "조원": 1_000_000}


def _parse_krw_scaled(raw: str, unit_multiplier: int) -> int | None:
    if raw in ("-", ""):
        return None
    try:
        return round(float(raw.replace(",", "")) * unit_multiplier)
    except ValueError:
        return None


def _parse_pct(raw: str) -> float | None:
    if raw in ("-", ""):
        return None
    try:
        return float(raw.replace(",", ""))
    except ValueError:
        return None


def _extract_earnings_fields(body: str) -> dict[str, Any]:
    """Pull revenue and operating-profit rows out of a DART preliminary-
    earnings filing's flattened results table (see module comment above),
    normalized to KRW millions regardless of the filing's own declared
    unit (see _KRW_UNIT_TO_MILLIONS comment - defaults to 백만원/1x if no
    unit declaration is found, matching the more common case rather than
    silently guessing 조원 for an unrecognized/missing declaration).

    Returns whichever of revenue_current_krw_mn / revenue_prior_qtr_krw_mn
    / revenue_qoq_pct / revenue_yoy_pct / operating_profit_* were actually
    found - a row DART reports via a 흑자전환/적자전환 transition flag
    instead of a percentage sets the corresponding *_qoq_swing /
    *_yoy_swing field to that flag string and leaves the percentage field
    absent, rather than a fabricated number.
    """
    unit_match = _UNIT_DECLARATION_PATTERN.search(body)
    unit_multiplier = _KRW_UNIT_TO_MILLIONS.get(
        unit_match.group(1).strip() if unit_match else "", 1
    )

    fields: dict[str, Any] = {}
    for match in _QUARTERLY_TABLE_PATTERN.finditer(body):
        label, current, prior, qoq_pct, qoq_flag, yoy_val, yoy_pct, yoy_flag = match.groups()
        prefix = "revenue" if label == "매출액" else "operating_profit"

        fields[f"{prefix}_current_krw_mn"] = _parse_krw_scaled(current, unit_multiplier)
        fields[f"{prefix}_prior_qtr_krw_mn"] = _parse_krw_scaled(prior, unit_multiplier)
        fields[f"{prefix}_prior_year_qtr_krw_mn"] = _parse_krw_scaled(yoy_val, unit_multiplier)

        if qoq_flag in ("흑자전환", "적자전환"):
            fields[f"{prefix}_qoq_swing"] = qoq_flag
        else:
            fields[f"{prefix}_qoq_pct"] = _parse_pct(qoq_pct)

        if yoy_flag in ("흑자전환", "적자전환"):
            fields[f"{prefix}_yoy_swing"] = yoy_flag
        else:
            fields[f"{prefix}_yoy_pct"] = _parse_pct(yoy_pct)

    return fields


def classify_preliminary_earnings(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Classify DART preliminary-earnings filings
    (연결재무제표기준영업(잠정)실적(공정공시)).

    Only touches articles whose title contains 잠정실적 or 잠정영업실적
    (matching news-retrieval's own DART target-pattern list) and that
    have a fetched body.

    Korea Signals spec Section 5.4 rule:
      SIGNAL - operating profit differs from the previous quarter by more
               than 20%, OR operating margin moved by more than 5
               percentage points. A profit/loss swing (흑자전환/적자전환)
               on operating profit always counts as this condition (a
               swing has no finite QoQ% to compare against 20%, but is
               definitionally more extreme than any bounded percentage
               change - see module comment).
      WEAK   - anything smaller. Never NOISE - "a preliminary earnings
               announcement is always at least Weak" (spec's own words).

    Operating margin (= operating profit / revenue) is computed here for
    both the current and prior quarter when both figures are available,
    and compared as a raw percentage-point difference - not read from
    DART's own table, which reports profit/revenue growth rates, not a
    margin ratio at all.
    """
    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0}

    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "dart_filing":
            continue
        title = a.get("title", "")
        # Confirmed live 2026-09-23: the real report_nm is
        # "연결재무제표기준영업(잠정)실적(공정공시)" - "(잠정)" sits as a
        # bracketed qualifier INSIDE "영업...실적", not concatenated as a
        # single "잠정실적"/"잠정영업실적" substring the way an earlier
        # version of this check assumed (that check silently matched zero
        # of 14 real filings). "잠정" alone (provisional/preliminary) is
        # specific enough here - it's not used in any other DART report
        # type this pipeline's target patterns already watch for (S2/S3's
        # patterns are 단일판매/공급계약/시설투자/유형자산, none of which
        # contain 잠정).
        if "잠정" not in title:
            continue
        body = a.get("body")
        if not body:
            continue
        rcept_no = meta.get("rcept_no")
        if not rcept_no:
            continue

        fields = _extract_earnings_fields(body)

        op_qoq_swing = fields.get("operating_profit_qoq_swing")
        op_qoq_pct = fields.get("operating_profit_qoq_pct")

        current_margin = None
        prior_margin = None
        margin_delta_points = None
        rev_current = fields.get("revenue_current_krw_mn")
        rev_prior = fields.get("revenue_prior_qtr_krw_mn")
        op_current = fields.get("operating_profit_current_krw_mn")
        op_prior = fields.get("operating_profit_prior_qtr_krw_mn")
        if rev_current and op_current is not None:
            current_margin = op_current / rev_current * 100
        if rev_prior and op_prior is not None:
            prior_margin = op_prior / rev_prior * 100
        if current_margin is not None and prior_margin is not None:
            margin_delta_points = current_margin - prior_margin

        if op_qoq_swing:
            signal = "SIGNAL"
            reason = f"operating_profit_qoq_{op_qoq_swing}"
        elif op_qoq_pct is not None and abs(op_qoq_pct) > _QOQ_YOY_PCT_SIGNAL_THRESHOLD:
            signal = "SIGNAL"
            reason = f"operating_profit_qoq_pct_{op_qoq_pct}_exceeds_{_QOQ_YOY_PCT_SIGNAL_THRESHOLD}"
        elif (
            margin_delta_points is not None
            and abs(margin_delta_points) > _OPERATING_MARGIN_PCT_POINTS_THRESHOLD
        ):
            signal = "SIGNAL"
            reason = f"operating_margin_delta_{margin_delta_points:.1f}pts_exceeds_{_OPERATING_MARGIN_PCT_POINTS_THRESHOLD}"
        else:
            # Never NOISE for this signal type - spec's own explicit rule.
            signal = "WEAK"
            reason = "below_qoq_and_margin_thresholds"

        counts[signal] += 1
        meta.update(fields)
        meta["operating_margin_current_pct"] = current_margin
        meta["operating_margin_prior_qtr_pct"] = prior_margin
        meta["operating_margin_delta_points"] = margin_delta_points
        meta["source_category"] = "preliminary_earnings"
        a["metadata"] = meta
        results.append({
            "article": a,
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": rcept_no,
                "reason": reason,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[KOREA_PRELIMINARY_EARNINGS] classified signal=%d weak=%d",
            counts["SIGNAL"], counts["WEAK"],
        )
    return results


# Korea Signals spec Section 5.5. TWO real DART form templates cover this
# signal type - confirmed live 2026-09-23 against 6 real filings (Simmtech,
# HD Hyundai Electric, Samsung Electro-Mechanics, SK Hynix, Samsung
# Electronics x2):
#
#   "영업실적등에대한전망" (earnings outlook/forecast) - a real numeric
#   forecast table (매출액/영업이익 for the target period) plus a prose
#   reasoning section restating the same figures as a range, e.g.
#   "매출액 : 약 10,719억원 (범위 : 10,619억원 ~ 10,819억원)".
#
#   "장래사업ㆍ경영계획" (future business/management plan) - a real
#   투자금액/예상투자금액 (expected investment amount) figure, e.g. SK
#   Hynix "약 1,100조원", Samsung Electronics "약 2,450조".
#
# Unlike S2-S4, this signal type's rule is NOT itself an arithmetic
# threshold - Section 5.5 only asks whether a forward-looking NUMBER is
# present at all (SIGNAL) vs. a statement with no number (WEAK) vs. no
# view at all (NOISE, never seen in real data checked so far - all 6 real
# examples had at least one real number). "억" and "조" are real Korean
# large-number units (100 million and 1 trillion respectively) that must
# be recognized as units, not stripped as noise - a plain digit-extraction
# regex that ignores them would silently produce a technically-non-null
# but meaningless number.
_FORECAST_FIGURE_PATTERN = re.compile(
    r"(매출액|영업이익)\s*[:：]\s*약?\s*([\d,]+(?:\.[\d]+)?)\s*(억원|조원|백만불|억달러)"
)
# "원" is dropped inconsistently across real filers - confirmed live
# 2026-09-23: SK Hynix wrote "약 1,100조원" (with 원) while Samsung
# Electronics wrote "약 2,450조" (bare, no 원) for the exact same
# 예상투자금액 field in the exact same form template - both are the same
# real unit (1 trillion won), just with/without the trailing currency
# character, not two different units.
_INVESTMENT_FIGURE_PATTERN = re.compile(
    r"(?:예상)?투자금액\s+약?\s*([\d,]+(?:\.[\d]+)?)\s*(조원?|억원?)"
)
# HD Hyundai Electric's real [기재정정] example: 정정사유 states the
# revision was to a forecast figure ("수주 전망치 수정" - order forecast
# revised), and 4. 정정사항 lists a real before/after value pair
# ("4,222 5,185" for 수주). Detecting a revision needs BOTH the
# correction-filing marker AND a real numeric before/after pair in the
# 정정사항 table - a bare [기재정정] tag alone (e.g. correcting a contact
# phone number) would not be a genuine number revision.
_REVISION_BEFORE_AFTER_PATTERN = re.compile(r"([\d,]+(?:\.[\d]+)?)\s+([\d,]+(?:\.[\d]+)?)\s*-\s*연결재무제표")

_KOREAN_LARGE_UNIT_TO_WON = {
    "억원": 100_000_000,
    "조원": 1_000_000_000_000,
    # Bare 억/조 (no 원 suffix) - same real unit, just without the
    # trailing currency character, confirmed inconsistent across real
    # filers (see _INVESTMENT_FIGURE_PATTERN comment above).
    "억": 100_000_000,
    "조": 1_000_000_000_000,
    "억달러": None,  # foreign-currency figure, not converted - see below
    "백만불": None,
}


def _parse_korean_large_number(raw: str, unit: str) -> float | None:
    """Parse a Korean large-number figure (e.g. "10,719" + "억원") into a
    plain KRW value. Foreign-currency units (억달러/백만불) are left
    unconverted (returns None for the won-equivalent field, the raw
    figure and unit are still stored separately) - converting USD/KRW
    would need a live exchange rate this module has no access to, and a
    stale/hardcoded rate would silently misstate the number worse than
    admitting the conversion wasn't done.
    """
    won_per_unit = _KOREAN_LARGE_UNIT_TO_WON.get(unit)
    if won_per_unit is None:
        return None
    try:
        return float(raw.replace(",", "")) * won_per_unit
    except ValueError:
        return None


def _extract_guidance_fields(body: str, title: str) -> dict[str, Any]:
    """Pull whichever forward-looking figures Korea Signals spec Section
    5.5 needs out of a DART guidance-disclosure filing - tries both real
    form templates (see module comment above), since a filing's title
    alone doesn't reliably distinguish which fields will be present (both
    templates use 공정공시 in the title).
    """
    fields: dict[str, Any] = {}

    forecast_matches = list(_FORECAST_FIGURE_PATTERN.finditer(body))
    for m in forecast_matches:
        label, raw_value, unit = m.groups()
        prefix = "revenue_forecast" if label == "매출액" else "operating_profit_forecast"
        fields[f"{prefix}_raw"] = f"{raw_value}{unit}"
        won_value = _parse_korean_large_number(raw_value, unit)
        if won_value is not None:
            fields[f"{prefix}_krw"] = won_value

    investment_match = _INVESTMENT_FIGURE_PATTERN.search(body)
    if investment_match:
        raw_value, unit = investment_match.groups()
        fields["investment_amount_raw"] = f"{raw_value}{unit}"
        won_value = _parse_korean_large_number(raw_value, unit)
        if won_value is not None:
            fields["investment_amount_krw"] = won_value

    if "기재정정" in title:
        revision_match = _REVISION_BEFORE_AFTER_PATTERN.search(body)
        if revision_match:
            fields["revision_before"] = revision_match.group(1)
            fields["revision_after"] = revision_match.group(2)

    return fields


def classify_guidance_disclosure(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Classify DART guidance-disclosure filings (장래사업ㆍ경영계획 /
    영업실적등에대한전망 form types).

    Only touches articles whose title contains 공정공시 AND (전망 or
    장래사업 or 경영계획) - narrower than just "공정공시" alone, since that
    substring is shared with S4's preliminary-earnings filings (both are
    a 공정공시/fair-disclosure category, but genuinely different report
    types - see classify_preliminary_earnings, which is scoped by 잠정
    instead). An article already classified by classify_preliminary_earnings
    is skipped here via this title check, not a shared "already
    processed" flag - the two functions' title patterns are mutually
    exclusive by construction (잠정 filings don't also say 전망/장래사업/
    경영계획, confirmed against all real filings checked).

    Korea Signals spec Section 5.5 rule:
      SIGNAL - a forward-looking NUMBER is present (either a revenue/
               operating-profit forecast, or an investment amount), OR
               a [기재정정] filing revises a previously-given number
      WEAK   - a forward-looking statement with no number found
      NOISE  - administrative disclosures carrying no view - NOT
               automatically detected here, same honest gap as S3's
               NOISE case: no real example exists in the filings checked
               so far to design the rule against (all 6 real examples had
               at least one real number, i.e. all classify as SIGNAL)
    """
    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0, "NOISE": 0}

    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "dart_filing":
            continue
        title = a.get("title", "")
        if "공정공시" not in title:
            continue
        if not any(marker in title for marker in ("전망", "장래사업", "경영계획")):
            continue
        body = a.get("body")
        if not body:
            continue
        rcept_no = meta.get("rcept_no")
        if not rcept_no:
            continue

        fields = _extract_guidance_fields(body, title)
        has_forecast_number = any(
            k.endswith("_raw") for k in fields if k != "revision_before" and k != "revision_after"
        )
        is_revision = "revision_before" in fields and "revision_after" in fields

        if is_revision:
            signal = "SIGNAL"
            reason = f"revised_figure:{fields['revision_before']}->{fields['revision_after']}"
        elif has_forecast_number:
            signal = "SIGNAL"
            reason = "forward_looking_number_present"
        else:
            signal = "WEAK"
            reason = "no_forward_looking_number_found"

        counts[signal] += 1
        meta.update(fields)
        meta["source_category"] = "guidance_disclosure"
        a["metadata"] = meta
        results.append({
            "article": a,
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": rcept_no,
                "reason": reason,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[KOREA_GUIDANCE_DISCLOSURE] classified signal=%d weak=%d noise=%d",
            counts["SIGNAL"], counts["WEAK"], counts["NOISE"],
        )
    return results


# Korea Signals spec Section 5.6. Confirmed live 2026-09-23 against 5 real
# filings (all SK Hynix + Samsung Electronics): the DEMAND and the
# UNDECIDED-answer cases both have dedicated, reliable report_nm patterns
# ("조회공시요구(풍문또는보도)" for the demand, filer=거래소 per KRX's own
# taxonomy; "...에대한답변(미확정)" for an undecided answer). The CONFIRMED
# case does NOT - per KRX's own official 코스닥시장 공시·상장관리 해설서
# (fetched directly during an earlier research pass), a confirmed rumour
# is disclosed as an ordinary SUBSTANTIVE filing under its own normal form
# name (e.g. a real 유상증자결정/단일판매공급계약체결 filing), with only
# one boilerplate sentence embedded in the body identifying it as also
# answering a prior demand: "이 공시는 YYYY년 MM월 DD일 조회공시(풍문 또는
# 보도) 요구에 대한 답변임" - confirmed live on SK Hynix's real 2026-09-18
# answer filing, which carries this exact sentence even though ITS OWN
# title uses the 답변(미확정) pattern (i.e. the sentence appears on BOTH
# the dedicated-title case and, per the KRX manual, would be the ONLY
# marker on a confirmed case with no dedicated title at all). Detecting a
# confirmed rumour therefore means scanning EVERY dart_filing-sourced
# article's body for this sentence, not filtering by title first the way
# S2-S5 do - classify_rumour_confirmation below does this as a separate
# pass over the full batch, not folded into classify_rumour_adjudication.
_RUMOUR_DEMAND_TIMESTAMP_PATTERN = re.compile(
    r"거래소의\s*조회요구\((\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*(\d{1,2}):(\d{2})\)"
)
_RUMOUR_CONFIRMATION_SENTENCE_PATTERN = re.compile(
    r"이\s*공시는\s*(\d{4})년\s*(\d{1,2})월\s*(\d{1,2})일\s*조회공시\s*"
    r"\(?풍문\s*(?:또는|또)\s*보도\)?\s*요구에?\s*따른\s*공시사항"
)


def _parse_kst_datetime(year: str, month: str, day: str, hour: str = "0", minute: str = "0") -> datetime:
    """KST is UTC+9 - stored as an explicit-offset datetime, not converted
    to UTC, since every downstream consumer of these timestamps (the
    doc's own trader-facing screen mockups) wants to see them in KST, the
    market's own local time, not silently shifted.
    """
    return datetime(
        int(year), int(month), int(day), int(hour), int(minute),
        tzinfo=timezone(timedelta(hours=9)),
    )


def classify_rumour_adjudication(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Classify DART rumour-adjudication filings (조회공시요구(풍문또는보도)
    demands and their 답변 answers).

    Only touches articles whose title contains 조회공시 (matching
    news-retrieval's own DART target-pattern list) and that have a
    fetched body. Does NOT cover the CONFIRMED case - see
    classify_rumour_confirmation for why that needs a separate,
    title-independent pass over the whole batch.

    Korea Signals spec Section 5.6 rule:
      SIGNAL - always, for the demand itself (filer=거래소, the exchange -
               distinguished from a company's own answer via
               metadata.flr_nm, confirmed live this field reliably
               differs: "유가증권시장본부" for a demand, the company's own
               name for an answer)
      SIGNAL - when the answer denies (부인) - CONFIRMED is handled
               separately, see classify_rumour_confirmation
      WEAK   - when the answer is undecided (미확정) - "the most common
               response, and says less" (spec's own words)
      NOISE  - never

    The demand's own timestamp (to the minute, not just rcept_dt's date)
    is embedded as free text inside a later ANSWER filing's body, not a
    structured field on the demand filing itself - confirmed live this is
    the only place it appears with minute precision. So an ANSWER
    filing's metadata carries demand_datetime_kst (parsed from that
    embedded sentence) and answer_datetime_kst (this filing's own
    rcept_dt, date-only - see module-level caveat that DART's list.json
    has no time-of-day field), plus hours_between_demand_and_answer when
    both are available. A bare DEMAND filing (no matching answer yet
    fetched in this same batch) has none of these - the gap can only be
    computed once the answer exists.
    """
    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0}

    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "dart_filing":
            continue
        title = a.get("title", "")
        if "조회공시" not in title:
            continue
        body = a.get("body")
        if not body:
            continue
        rcept_no = meta.get("rcept_no")
        if not rcept_no:
            continue

        # Confirmed live 2026-09-23: a demand's filer is the exchange
        # itself, e.g. "유가증권시장본부" (Securities Market Division) - an
        # answer's filer is always the company's own name instead. This
        # value is news-retrieval's article["source"] field, NOT
        # metadata.flr_nm - _fetch_dart_filing (pipeline.py) stores
        # filing.get("flr_nm") into "source", never copies it into
        # metadata at all. An earlier version of this check read
        # meta.get("flr_nm"), which is always None/absent (confirmed live
        # against the real DB row for this exact filing - metadata has no
        # flr_nm key), so is_demand was always False regardless of the
        # real filer - caught by testing against real data, not assumed
        # correct from the field name looking plausible.
        source = a.get("source") or ""
        is_demand = "거래소" in source or "시장본부" in source
        is_undecided = "미확정" in title
        is_denial = "부인" in title or "중요정보 없음" in title

        fields: dict[str, Any] = {}
        demand_match = _RUMOUR_DEMAND_TIMESTAMP_PATTERN.search(body)
        if demand_match:
            y, mo, d, h, mi = demand_match.groups()
            demand_dt = _parse_kst_datetime(y, mo, d, h, mi)
            fields["demand_datetime_kst"] = demand_dt.isoformat()
            answer_dt = _article_published_dt(a)
            if answer_dt:
                fields["answer_datetime_kst"] = answer_dt.isoformat()
                fields["hours_between_demand_and_answer"] = round(
                    (answer_dt - demand_dt).total_seconds() / 3600, 1
                )

        if is_demand:
            signal = "SIGNAL"
            reason = "exchange_demand_issued"
        elif is_denial:
            signal = "SIGNAL"
            reason = "answer_denied"
        elif is_undecided:
            signal = "WEAK"
            reason = "answer_undecided"
        else:
            # A 조회공시-titled filing that's neither the demand nor
            # matches 미확정/부인 - not yet seen in real data, treated as
            # WEAK (same fail-open-toward-WEAK reasoning the spec uses
            # elsewhere) rather than guessed at.
            signal = "WEAK"
            reason = "unrecognized_rumour_filing_pattern"

        counts[signal] += 1
        meta.update(fields)
        meta["source_category"] = "rumour_adjudication"
        meta["is_exchange_demand"] = is_demand
        a["metadata"] = meta
        results.append({
            "article": a,
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": rcept_no,
                "reason": reason,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[KOREA_RUMOUR_ADJUDICATION] classified signal=%d weak=%d",
            counts["SIGNAL"], counts["WEAK"],
        )
    return results


_CLARIFICATION_RUMOUR_CONTENT_PATTERN = re.compile(r"풍문\s*또는\s*보도의\s*내용\s+(.+?)\s*2\.\s*풍문")
# Confirmed live 2026-09-23 against 3 real 해명 filings: a genuine denial
# reads as "~한 사실은 없습니다"/"~한 바 없습니다" (there is no fact
# that.../we have not...) - a specific, substantive rejection of the named
# claim (SK Hynix's real 2026-07-22 example: "인텔의 오하이오 부지와 Fab
# 인수를 추진하거나 결정한 사실은 없습니다" - explicitly denying the
# specific acquisition claimed in the press report). This is genuinely
# stronger than the vague "검토 중이나 확정된 사항 없음" (under review,
# nothing decided) pattern the other 2 real examples used - conflating
# the two would understate a real, checkable denial as merely WEAK.
_CLARIFICATION_DENIAL_PATTERN = re.compile(r"(?:추진|결정)한?\s*사실은?\s*없습니다|바\s*없습니다")


def classify_voluntary_clarification(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Classify DART 자율적 해명공시 (voluntary clarification disclosure)
    filings - report_nm "풍문또는보도에대한해명" (with or without a trailing
    (미확정) qualifier).

    This is a DISTINCT mechanism from classify_rumour_adjudication's
    조회공시요구/답변 pair, confirmed via research 2026-09-23: introduced
    by KRX effective 2015-09-07, this is issued by the COMPANY on its own
    initiative, "without an exchange inquiry disclosure demand preceding
    it" - unlike 조회공시, which only exists because the exchange judged
    a rumour serious enough to compel a response. Korea Signals spec
    Section 5.6's "always SIGNAL" rule is justified specifically by that
    exchange compulsion ("The exchange itself judged the rumour serious
    enough to force an answer") - a justification that does not transfer
    to a company's own voluntary clarification, so this function
    deliberately does NOT reuse that rule wholesale rather than silently
    applying it to a mechanism the spec never described.

    Rule used here (not directly from the spec, since the spec never
    describes this mechanism - see reasoning above): a REAL, substantive
    denial of the specific claim is SIGNAL (real information the market
    didn't have before, even without exchange compulsion behind it) - a
    vague "under review, nothing decided yet" response is WEAK, matching
    the spirit of the spec's own 조회공시 rule ("nothing has been decided
    - which is the most common response, and says less") even though
    this isn't literally that rule. Confirmed live: 2 of 3 real examples
    are the vague case, 1 is a genuine denial (SK Hynix's Intel-Ohio-Fab
    denial) - both patterns are real, not hypothetical.

    Not yet confirmed (see research notes): whether any of the real
    해명 filings found actually had a preceding 조회공시요구 anyway (which
    would make them functionally answers in substance, filed under the
    voluntary form instead) - research could not verify this either way
    for the real examples checked.
    """
    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0}

    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "dart_filing":
            continue
        title = a.get("title", "")
        if "해명" not in title:
            continue
        body = a.get("body")
        if not body:
            continue
        rcept_no = meta.get("rcept_no")
        if not rcept_no:
            continue

        fields: dict[str, Any] = {}
        rumour_match = _CLARIFICATION_RUMOUR_CONTENT_PATTERN.search(body)
        if rumour_match:
            fields["rumour_claim"] = rumour_match.group(1).strip()

        is_denial = bool(_CLARIFICATION_DENIAL_PATTERN.search(body))
        if is_denial:
            signal = "SIGNAL"
            reason = "voluntary_clarification_denial"
        else:
            signal = "WEAK"
            reason = "voluntary_clarification_under_review"

        counts[signal] += 1
        meta.update(fields)
        meta["source_category"] = "voluntary_clarification"
        meta["is_exchange_demand"] = False
        meta["is_denial"] = is_denial
        a["metadata"] = meta
        results.append({
            "article": a,
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": rcept_no,
                "reason": reason,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[KOREA_VOLUNTARY_CLARIFICATION] classified signal=%d weak=%d",
            counts["SIGNAL"], counts["WEAK"],
        )
    return results


def classify_rumour_confirmation(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Detect the CONFIRMED case of Korea Signals spec Section 5.6 - a
    rumour that turned out true, disclosed via an ORDINARY substantive
    filing (its own real form name, e.g. 단일판매ㆍ공급계약체결) rather
    than a dedicated 조회공시 title, per KRX's own disclosure manual (see
    module comment above classify_rumour_adjudication).

    Scans EVERY dart_filing-sourced article's body (not filtered by title
    first - that's the whole point, a confirmed rumour's title gives no
    hint) for the boilerplate sentence
    "이 공시는 YYYY년 MM월 DD일 조회공시(풍문 또는 보도) 요구에 따른
    공시사항임". A filing already classified by classify_rumour_adjudication
    (i.e. one that already has this sentence AND a 조회공시 title, like
    the real SK Hynix 2026-09-18 example) is intentionally NOT excluded
    here - callers dedup by source_id (rcept_no) downstream the same way
    every other Korea signal type does, so a filing matching in both
    passes just gets classified twice under two different source_ids that
    happen to be the same value, and get_existing_korea_signal_source_ids
    prevents the second insert as a no-op, not a double-count.

    Korea Signals spec Section 5.6 rule: this outcome is always SIGNAL -
    "when the answer confirms or denies." No NOISE/WEAK case exists for a
    confirmed rumour by definition (if the sentence is present, the
    rumour was confirmed true).
    """
    results: list[dict[str, Any]] = []
    count = 0

    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "dart_filing":
            continue
        body = a.get("body")
        if not body:
            continue
        rcept_no = meta.get("rcept_no")
        if not rcept_no:
            continue

        confirm_match = _RUMOUR_CONFIRMATION_SENTENCE_PATTERN.search(body)
        if not confirm_match:
            continue

        y, mo, d = confirm_match.groups()
        # No time-of-day in this specific sentence (unlike the answer-side
        # embedded timestamp classify_rumour_adjudication reads, which DOES
        # carry minutes) - confirmed live the confirmation boilerplate
        # itself is date-only. demand_datetime_kst here is therefore
        # date-precision, not minute-precision like the undecided/denial
        # case's - stored as a distinct, less-precise field rather than
        # padding in a fake time.
        demand_date = f"{y}-{int(mo):02d}-{int(d):02d}"

        count += 1
        meta["confirmed_rumour_demand_date"] = demand_date
        meta["source_category"] = "rumour_confirmation"
        a["metadata"] = meta
        results.append({
            "article": a,
            "result": {
                "signal": "signal",
                "signal_score": None,
                "source_id": rcept_no,
                "reason": f"rumour_confirmed_via_substantive_filing:{a.get('title', '')[:60]}",
                "metadata": meta,
            },
        })

    if count:
        logger.info("[KOREA_RUMOUR_CONFIRMATION] found %d confirmed rumour filing(s)", count)
    return results


# Which metadata/top-level fields need translation, per source_category -
# same shape as taiwan_signal_classifier.py's own _TRANSLATION_FIELDS, one
# entry added per Korea signal type as each is implemented (not written
# speculatively ahead of the classifier that would populate its fields -
# an untested field-name guess here would silently no-op forever, since
# _get_nested just returns None for a key that was never set, which
# translate_korea_articles below correctly treats as "nothing to
# translate" rather than an error).
#
#   S1 export surprise     - title only (a press-release headline
#                             sentence, no other free text); not yet added,
#                             S1 itself isn't classified yet (see module
#                             docstring - no 2yr history backfill)
#   S2 supply contract     - IMPLEMENTED below. title (the raw report_nm,
#                             e.g. "단일판매ㆍ공급계약체결") and
#                             metadata.counterparty (sometimes already
#                             bilingual when DART's own filing states an
#                             English name in parenthesis, as confirmed
#                             live on Hanmi Semiconductor's SK Hynix
#                             counterparty - translating it again in that
#                             case is harmless, just a no-op-shaped
#                             re-translation, not a correctness bug)
#   S3 capacity commitment - IMPLEMENTED below. title and
#                             metadata.investment_purpose (투자목적/취득목적
#                             - a real free-text sentence, e.g. "AI향
#                             SOCAMM모듈 기판... 생산능력 확대", not a
#                             label-value field, genuinely needs
#                             translation rather than being self-evident
#                             the way a counterparty company name often is)
#   S4 preliminary earnings - IMPLEMENTED below. title only - the results
#                             table itself is numeric, not free text
#   S5 guidance disclosure  - IMPLEMENTED below. title only - same reason
#                             as S4 (the forecast/investment-plan table is
#                             numeric, not free text)
#   S6 rumour adjudication  - IMPLEMENTED below, across three
#                             source_categories (rumour_adjudication,
#                             voluntary_clarification, rumour_confirmation).
#                             voluntary_clarification's rumour_claim field
#                             is real extracted prose and is translated;
#                             the others are title-only until the demand/
#                             answer free-text is extracted (see each
#                             entry's own comment below)
#   S7 qualification news   - IMPLEMENTED below (classify_qualification_news).
#                             Mirrors Taiwan's gdelt entry (title only -
#                             translated_company_name comes from
#                             KOREA_TICKER_UNIVERSE directly, same as
#                             Taiwan's own known-correct-name policy, never
#                             LLM-translated)
_TRANSLATION_FIELDS: dict[str, dict[str, str]] = {
    "supply_contract": {
        "title": "translated_title",
        "metadata.counterparty": "translated_counterparty",
    },
    "capacity_commitment": {
        "title": "translated_title",
        "metadata.investment_purpose": "translated_investment_purpose",
    },
    "preliminary_earnings": {
        "title": "translated_title",
    },
    "guidance_disclosure": {
        "title": "translated_title",
    },
    # rumour_adjudication/rumour_confirmation: title only. The doc's own
    # mockup (Section 6.3) shows the rumour claim text and the company's
    # answer text translated too (e.g. "Reports of an expanded supply
    # agreement...") - not yet extracted into a metadata field by
    # classify_rumour_adjudication (extracts only timestamps, not the
    # free-text claim/answer body) or classify_rumour_confirmation, so not
    # listed here yet either - adding a field-map entry for a field the
    # classifier never populates would silently no-op forever (see
    # _TRANSLATION_FIELDS's own top-of-map comment on this exact failure
    # mode). voluntary_clarification is different - see below.
    "rumour_adjudication": {
        "title": "translated_title",
    },
    # classify_voluntary_clarification DOES extract the real rumour claim
    # sentence (metadata.rumour_claim, via _CLARIFICATION_RUMOUR_CONTENT_PATTERN) -
    # unlike rumour_adjudication above, this is a populated field, not a
    # future placeholder, so it belongs in the map now.
    "voluntary_clarification": {
        "title": "translated_title",
        "metadata.rumour_claim": "translated_rumour_claim",
    },
    "rumour_confirmation": {
        "title": "translated_title",
    },
    "qualification_news": {
        "title": "translated_title",
    },
}


def translate_korea_articles(
    articles: list[dict[str, Any]],
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: int | None = None,
) -> None:
    """Translate the fields that need it, per source_category, in place -
    identical shape to taiwan_signal_classifier.py's
    translate_taiwan_articles (same _get_nested/_translate_one helpers,
    imported directly rather than re-implemented; see module docstring).

    Only touches articles whose metadata.source_category has an entry in
    _TRANSLATION_FIELDS - i.e. only articles a classify_* function in this
    module has already run on (source_category is set by that function,
    not present on a raw news-retrieval article). Native-language fields
    are never overwritten; translated values are added as new metadata
    fields (e.g. metadata.translated_title). Failed translations leave the
    translated_* field absent rather than set to None/empty.

    Must run AFTER classification, not before - Korea Signals spec Section
    7 Step 8 ("Translate only what survived classification") - and the
    caller decides what "survived" means (e.g. skip NOISE rows) by
    filtering the articles list passed in here, same as Taiwan's caller
    does implicitly by translating the whole classified batch regardless
    of label (translation cost is the same either way in this codebase
    today - Taiwan doesn't currently skip NOISE rows either).
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
            "[KOREA_TRANSLATE] %d/%d field(s) translated successfully",
            translated_count, len(to_translate),
        )


# S7 qualification news (Korea Signals spec Section 5.7 / 8.1-8.3): the one
# path that touches a language model, and only after three free filters.
# Runs against RSS + GDELT articles read from news-retrieval's
# korea_market_signal domain - the ones with NO source_category set yet
# (RSS: _parse_feed never sets metadata at all) or source_category=="gdelt"
# (KOREA_GDELT_SOURCE/KOREA_GDELT_ENGLISH_SOURCE - see news-retrieval
# seed.py). DART filings/Customs export rows are a different signal type
# entirely (S2-S6, S1) and are never routed through this function - callers
# must not pass them in (see classify_qualification_news's own filter,
# which only touches source_category in (None, "gdelt") as a defensive
# check, not a trust boundary).

# Same narrow, curated-starter-list spirit as news-retrieval's own
# _KOREA_PRESS_ALLOWLIST (pipeline.py) - duplicated here as its own literal
# list rather than a cross-service import, same pattern Taiwan's clause-code
# table already uses (each service owns its own copy of a small lookup
# table rather than adding a shared-code dependency for it). Kept in sync
# manually if either list changes - deliberately NOT comprehensive, per
# explicit instruction: a small, high-confidence set the spec's own
# "trusted Korean list" phrasing calls for, extended only as more outlets
# are directly confirmed, not expanded pre-emptively to look thorough.
_KOREA_TRUSTED_PUBLICATIONS: frozenset[str] = frozenset({
    "thelec.kr",          # THE ELEC (디일렉)
    "zdnet.co.kr",        # ZDNet Korea (지디넷코리아)
    "businesspost.co.kr", # BusinessPost (비즈니스포스트)
    "ddaily.co.kr",       # DigitalDaily (디지털데일리)
    "yna.co.kr",          # Yonhap (연합뉴스)
    "hankyung.com",       # Korea Economic Daily (한국경제)
})

# Spec Section 5.7's rule distinguishes "trusted first-tier" from "second-
# tier" for the Signal-vs-Weak split on a HIGH model answer - not just a
# binary trusted/not-trusted gate the way Filter 1 (which drops untrusted
# publications entirely, before this) is. THE ELEC is the dedicated
# semiconductor-equipment trade press this market's edge is built on (see
# spec Section 1: "closest to the bottleneck", the DIGITIMES-equivalent);
# Yonhap is Korea's national wire service, the closest analogue to a
# paper-of-record. The rest are general business/tech dailies where
# semiconductor coverage is one recurring topic, not a dedicated beat - real
# outlets, correctly on the Filter-1 trusted list, but not first-tier by
# this specific rule.
_KOREA_FIRST_TIER_PUBLICATIONS: frozenset[str] = frozenset({
    "thelec.kr",
    "yna.co.kr",
})


def _extract_domain(url: str | None) -> str:
    """Same domain-derivation rule as news-retrieval's own
    _extract_domain_for_also_reported_by / geopolitical_signal_stage_d's
    _extract_domain - strip "www.", lowercase, keep any other subdomain
    as-is. Kept as its own copy here (not imported) for the same reason
    _KOREA_TRUSTED_PUBLICATIONS is its own copy - no cross-service import,
    matching this codebase's existing pattern for small shared logic.
    """
    host = urlparse(url or "").hostname or ""
    host = host.lower()
    if host.startswith("www."):
        host = host[len("www."):]
    return host


def _publications_carrying_story(article: dict[str, Any]) -> int:
    """Count of distinct publications carrying this story: the article's
    own outlet plus each also_reported_by entry - matches Filter 3's "count
    how many publications carried it" literally (unfiltered by trust tier,
    unlike geopolitical_signal_stage_d's _corroborating_domains, which
    deliberately filters to allowlisted-only for a different purpose - see
    that function's own docstring). also_reported_by is only populated once
    news-retrieval's title-similarity dedup has actually run for this
    domain (see _TITLE_DEDUP_DOMAINS in news-retrieval/pipeline.py, which
    korea_market_signal was added to specifically to make this field real).
    """
    also_reported_by = (article.get("metadata") or {}).get("also_reported_by") or []
    domains = {_extract_domain(article.get("url"))}
    for entry in also_reported_by:
        # Two shapes. Rows written before geopolitical_news needed the
        # headline carry a bare domain string; rows written after carry
        # {outlet, title, url}, so the discarded duplicate's own
        # headline survives rather than only its domain. Both are read
        # here - this counts outlets, and the outlet is in both shapes.
        if isinstance(entry, dict):
            domain = (entry.get("outlet") or "").strip().lower()
        else:
            domain = (entry or "").strip().lower()
        if domain.startswith("www."):
            domain = domain[len("www."):]
        if domain:
            domains.add(domain)
    return len(domains)


def _company_name_in_headline(
    title: str, body: str | None, tracked_companies: list[dict[str, Any]],
) -> tuple[dict[str, Any], str] | None:
    """Filter 2: does a tracked company's name actually appear in the
    headline, OR (checked second, only if the headline check fails) in the
    body. Returns (matching KOREA_TICKER_UNIVERSE-shaped entry, "headline"
    or "body") or None.

    The spec's own wording (Section 5.7, Filter 2) is headline-only - "drop
    anything where no tracked company name actually appears in the
    headline" - and the model prompt (8.1) classifies headlines, not
    bodies. Extended to also check body live 2026-09-23: a real story can
    name a tracked company (or a named product only that company makes)
    solely in the body, with a generic-sounding headline that would
    otherwise be dropped here despite being genuinely relevant - e.g. "AI
    accelerator demand pushes Korean chipmakers to raise 2027 capex" never
    says "SK Hynix" or "Samsung" in the headline itself. A body-only match
    is real information Filter 2 would otherwise silently lose - body text
    is already fetched for RSS/GDELT articles at no extra cost (Trafilatura
    for GDELT, feedparser+_extract_body for RSS), so checking it costs
    nothing new. The match location is returned (not just a bool) so the
    caller can tell a body-only match apart from a title match - a
    headline hit is exactly what the spec's own Filter 2 describes; a
    body-only hit is this function's own extension of it, and worth being
    able to tell apart downstream (see classify_qualification_news's own
    metadata.company_match_location field).

    Checks both korean_name and company (English name) in each text,
    same dual-name fallback _filter_by_company_name_in_title uses for
    GDELT's native-vs-English query surfacing - a Korean headline is
    checked against korean_name first (the common case), but an
    English-named mention (e.g. quoting an English-language exchange
    filing) still matches via company.

    Group-prefix exclusion (KOREA_TICKER_UNIVERSE's own exclude_terms,
    e.g. "Samsung Life"/"Samsung SDI" for 삼성전자) matters here exactly
    the way the spec's own Step 0 warns: Korean business groups share a
    name across many unrelated listed companies, so a bare group-prefix
    substring match would silently pull in the wrong firm. Checked against
    BOTH title and body, for the same reason the name check itself now
    covers both - an exclude_terms hit in the body is just as real a
    false-positive risk as one in the headline.
    """
    body = body or ""
    for entry in tracked_companies:
        korean_name = entry.get("korean_name", "")
        company_name = entry.get("company", "")
        exclude_terms = entry.get("exclude_terms") or []
        if any(term and (term in title or term in body) for term in exclude_terms):
            continue
        if (korean_name and korean_name in title) or (
            company_name and company_name.lower() in title.lower()
        ):
            return entry, "headline"
        if (korean_name and korean_name in body) or (
            company_name and company_name.lower() in body.lower()
        ):
            return entry, "body"
    return None


# Spec Section 8.1, verbatim (the doc's own instructions text, not a
# paraphrase) - deliberately not reworded, since the doc's own examples
# (8.3) and edge-case guidance ("who is speaking matters", the stacked-
# memory-approval carve-out) are calibrated to this exact wording.
_QUALIFICATION_NEWS_SYSTEM_PROMPT = """You classify Korean market headlines about semiconductor and
electronics companies as HIGH or WEAK.

HIGH means the headline states a specific, checkable fact:
- a number (revenue, shipments, capacity, price, percentage)
- a named customer, partner, supplier or contract
- a result or stage change in an approval process
- a change in capacity, production, pricing or investment
- a forecast change stated by the company itself
- a government or regulatory action naming the company

WEAK means anything else:
- analyst opinion, ratings or price targets
- outlook or sentiment with no figure attached
- the company mentioned in passing in a market round-up
- a story mainly about a different company
- speculation attributed to unnamed sources

Who is speaking matters. A fact stated by the company, the
exchange or a regulator is HIGH. The same fact predicted by an
analyst is WEAK.

Approval of stacked memory at a customer is the most important
topic in this market. A headline stating an approval outcome, a
supply agreement or a volume for it is always HIGH.

If you are unsure, answer WEAK.

Example 1 - HIGH
  "SK Hynix begins volume supply of next-generation stacked
   memory to a major accelerator customer"
  Reason: a stage change in supply, plus a named customer type.

Example 2 - HIGH
  "Hanmi Semiconductor wins 48.5 billion won bonding equipment
   order"
  Reason: a specific contract value.

Example 3 - WEAK
  "Analysts expect the memory upcycle to continue into 2027"
  Reason: opinion, no figure, no company action.

Example 4 - WEAK
  "Korean chip shares rise on artificial intelligence optimism"
  Reason: sector sentiment, companies mentioned in passing.

Answer with one word only: HIGH or WEAK"""

_QUALIFICATION_NEWS_SCORE = None  # forced one-word call, no real confidence value - see Taiwan's own _GDELT_RELEVANCE_SCORE for the identical reasoning


def _classify_qualification_news_relevance(
    company_name: str, korean_name: str, source: str, headline: str,
    model: str, api_key: str, base_url: str, timeout: int,
    publications_carrying_this: int | None = None,
    english_coverage_found: bool | None = None,
) -> str:
    """One model call per surviving headline - spec Section 8.2's field
    layout (company/code/source/headline/original/published, plus the
    "already worked out, do not recalculate" pre-computed facts).

    publications_carrying_this/english_coverage_found are sent when known
    (see classify_qualification_news's own call site, which now computes
    both before this call - previously not wired at all, see this
    function's own history). Genuinely optional, not just permissive
    typing: publications_carrying_this is always available (computed
    per-article regardless of English-coverage data), but
    english_coverage_found depends on KOREA_GDELT_ENGLISH_SOURCE rows
    existing in the SAME batch - not guaranteed on every call (GDELT's own
    rate limit has blocked every live fetch this session, so this is
    currently never populated in practice, but the wiring is real and
    correct once real rows exist - not a placeholder).

    price_move_today is NOT sent and has no parameter here at all - a
    structural gap, not a missing-wiring one: checked live 2026-09-23,
    Alpha Vantage (the only market-data source in this codebase) has no
    real KRX quote for this universe (see the module docstring's own note
    on Section 5.8) - there is no real value to send, and a fabricated
    placeholder would be worse than omitting the field, per this
    function's own stated principle below.

    Per spec 8.1's own instructions text, none of these three fields
    actually change the model's HIGH/WEAK decision rule (8.1 never
    references them - the rule is purely "does this headline state a
    specific checkable fact") - they're sent as informational context per
    spec 8.2's literal input table, not because withholding them would
    make the model's own answer wrong. The SIGNAL/WEAK/NOISE table in
    spec 5.7 depends on the model's HIGH/WEAK answer, publication tier,
    and Filter 2 - not on these three fields - so their absence here has
    never affected classify_qualification_news's own final signal, only
    the model's context.

    Returns "HIGH" or "WEAK" - never anything else (unparseable/failed
    calls fail toward WEAK, same fail-safe direction as Taiwan's
    equivalent: hiding a possible signal costs less than surfacing noise
    as if it were checked).
    """
    prompt_lines = [
        f"company: {company_name} / {korean_name}",
        f"source: {source}",
        f"headline: {headline}",
    ]
    if publications_carrying_this is not None:
        prompt_lines.append(f"publications_carrying_this: {publications_carrying_this}")
    if english_coverage_found is not None:
        prompt_lines.append(f"english_coverage_found: {'true' if english_coverage_found else 'false'}")
    user_prompt = "\n".join(prompt_lines)
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": 10,
        "messages": [
            {"role": "system", "content": _QUALIFICATION_NEWS_SYSTEM_PROMPT},
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
        answer = (content or "").strip().upper()
        return "HIGH" if answer == "HIGH" else "WEAK"
    except Exception as exc:
        logger.warning("[KOREA_QUALIFICATION_NEWS] classification failed, defaulting to WEAK: %s", exc)
        return "WEAK"


def classify_qualification_news(
    articles: list[dict[str, Any]],
    tracked_companies: list[dict[str, Any]],
    model: str | None = None,
) -> list[dict[str, Any]]:
    """S7: classify Korean-language (and English-coverage-check) news
    articles per Korea Signals spec Section 5.7 - three free filters, then
    one model call for whatever survives.

    ``tracked_companies``: caller's own KOREA_TICKER_UNIVERSE-shaped list
    (this module has no news-retrieval import - same boundary as every
    other classify_* function here, which read only what's already on the
    article's own metadata).

    Filter 1 (trusted publication): drop unless the article's own domain
    (derived from its url) is in _KOREA_TRUSTED_PUBLICATIONS.
    Filter 2 (company name in headline, extended to body): drop unless a
    tracked company's name appears in the title OR (as a live-confirmed
    extension past the spec's own headline-only wording, since a real
    story can name a tracked company/product only in the body while the
    headline stays generic) the body - _company_name_in_headline, whose
    own docstring explains why body is checked and how a body-only match
    is distinguished from a title match.
    Filter 3 (repeat story): count distinct publications carrying this
    story (own domain + also_reported_by) - not itself a drop condition
    per the spec's own wording ("keep the earliest") - since
    news-retrieval's title-similarity dedup (_dedup_by_title_similarity_for_domain,
    scoped to korea_market_signal) already drops the later near-duplicate
    articles upstream and folds their outlet into the survivor's
    also_reported_by list. This function's job is to read that count back
    out, not re-run the same dedup a second time.
    Model: one HIGH/WEAK call (spec 8.1-8.3) on whatever survives all
    three filters.

    Rule (spec Section 5.7's table):
      SIGNAL: model says HIGH, publication is first-tier, company-name
              check passed.
      WEAK:   model says HIGH but publication is second-tier, OR model
              says WEAK.
      NOISE:  removed by Filter 1, 2, or an empty/unparseable title before
              ever reaching the model - not returned as a classified
              result at all (mirrors Taiwan's classify_gdelt_articles,
              which likewise only returns rows for candidates that survive
              to the model call - a dropped-by-filter article was never a
              candidate, so there's nothing to insert a NOISE row for).

    Model-input wiring (spec Section 5.7's input fields / Section 8.2):
    publications_carrying_this is always computed and sent.
    english_coverage_found is computed from KOREA_GDELT_ENGLISH_SOURCE rows
    present in this SAME ``articles`` batch (via _english_coverage_pool -
    the same pool classify_korea_signal_batch's own _attach_english_coverage
    pass builds separately for the full result set) and sent when a match
    exists for the article's own ticker; None (omitted from the prompt) if
    no English-coverage data is available at all for this ticker in this
    batch - genuinely "not known", not "known to be false" (only
    classify_korea_signal_batch's later _attach_english_coverage pass, over
    the full result set, decides the stored false vs true - this function
    only ever sends what it can compute from the SAME batch, before that
    pass runs). price_move_today is never sent - see
    _classify_qualification_news_relevance's own docstring for why
    (a structural data-source gap, not a wiring gap). Neither field changes
    the Signal/Weak/Noise rule below - see that same function's docstring.
    Section 5.8's two override rules (already-moved price action, index-
    reweighting suppression) are also not implemented here - no
    price-move or index-membership data reaches this module today.
    """
    model = model or config.OPENAI_MODEL_V2
    api_key = config.OPENAI_API_KEY
    base_url = config.OPENAI_BASE_URL
    timeout = config.OPENAI_TIMEOUT
    english_coverage_pool = _english_coverage_pool(articles)

    candidates = []
    for a in articles:
        meta = a.get("metadata") or {}
        category = meta.get("source_category")
        if category not in (None, "gdelt"):
            continue
        title = a.get("title") or ""
        if not title.strip():
            continue

        domain = _extract_domain(a.get("url"))
        if domain not in _KOREA_TRUSTED_PUBLICATIONS:
            continue

        match = _company_name_in_headline(title, a.get("body"), tracked_companies)
        if match is None:
            continue
        matched_company, match_location = match

        candidates.append((a, domain, matched_company, match_location))

    def _classify_one(item: tuple[dict, str, dict, str]) -> dict[str, Any]:
        a, domain, matched_company, match_location = item
        meta = a.get("metadata") or {}
        title = a.get("title") or ""
        publications_carrying_this = _publications_carrying_story(a)
        ticker = matched_company.get("ticker")
        # None (omit from the prompt) when this batch has NO English-
        # coverage data at all (english_coverage_pool empty - e.g. GDELT's
        # English source was rate-limited or returned nothing this run) -
        # "not known" for every ticker alike, not specifically "false" for
        # this one. Once the pool is non-empty, a ticker simply absent
        # from it genuinely means "no English coverage found among what
        # WAS fetched this batch" - a real, computable False, not a gap.
        english_coverage_found = (
            bool(english_coverage_pool.get(ticker)) if english_coverage_pool
            else None
        )

        model_answer = _classify_qualification_news_relevance(
            matched_company.get("company", ""),
            matched_company.get("korean_name", ""),
            domain, title, model, api_key, base_url, timeout,
            publications_carrying_this=publications_carrying_this,
            english_coverage_found=english_coverage_found,
        )

        if model_answer == "HIGH":
            signal = "SIGNAL" if domain in _KOREA_FIRST_TIER_PUBLICATIONS else "WEAK"
            reason = (
                "qualification_news_high_first_tier" if signal == "SIGNAL"
                else "qualification_news_high_second_tier"
            )
        else:
            signal = "WEAK"
            reason = "qualification_news_weak"

        meta["source_category"] = "qualification_news"
        meta["ticker"] = matched_company.get("ticker") or matched_company.get("company")
        meta["publication_domain"] = domain
        meta["publication_tier"] = "first" if domain in _KOREA_FIRST_TIER_PUBLICATIONS else "second"
        meta["publications_carrying_this"] = publications_carrying_this
        # "headline" matches the spec's own Filter 2 exactly; "body" is
        # this function's live-confirmed extension past it (see
        # _company_name_in_headline's docstring) - kept visible on the
        # stored row rather than silently folded into the same case, since
        # a body-only match is real but weaker evidence than a headline
        # naming the company directly (the model itself still only ever
        # sees the headline - spec 8.2 - so a body-only match's HIGH/WEAK
        # answer is judged the same way either case's is, but a reader
        # looking at why this row exists at all should be able to tell the
        # two apart).
        meta["company_match_location"] = match_location
        a["metadata"] = meta

        return {
            "article": a,
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                "signal_score": _QUALIFICATION_NEWS_SCORE,
                "source_id": a.get("url"),
                "reason": reason,
                "metadata": meta,
            },
        }

    if not candidates:
        return []
    with ThreadPoolExecutor(max_workers=config.TAIWAN_CLASSIFY_CONCURRENCY) as executor:
        results = list(executor.map(_classify_one, candidates))
    logger.info(
        "[KOREA_QUALIFICATION_NEWS] %d article(s) survived filters, classified",
        len(results),
    )
    return results


def _english_coverage_pool(articles: list[dict[str, Any]]) -> dict[str, list[datetime]]:
    """Build {ticker: [published_dt, ...]} from KOREA_GDELT_ENGLISH_SOURCE
    rows in this batch - news-retrieval's own English-coverage-check
    source (seed.py), ticker-keyed with a "-en" suffix
    (e.g. "000660-en") specifically so its title-similarity dedup pool
    never collides with the Korean-language GDELT source's own
    "000660" key (see KOREA_GDELT_ENGLISH_SOURCE's own comment in seed.py
    for why that suffix exists). Stripping it back off here is what lets
    this function join an English row back to its Korean-language
    company.

    Only rows with BOTH a recognizable "-en"-suffixed ticker AND a
    parseable published date are included - an English row with no
    ticker (a source config bug, not expected in practice given the
    seed.py source's own construction) or no timestamp can't be compared
    against anything, so it's silently excluded rather than crashing the
    whole batch's classification over one bad row.
    """
    pool: dict[str, list[datetime]] = {}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "gdelt":
            continue
        ticker = meta.get("ticker") or ""
        if not ticker.endswith("-en"):
            continue
        base_ticker = ticker[: -len("-en")]
        pub_dt = _article_published_dt(a)
        if not base_ticker or pub_dt is None:
            continue
        pool.setdefault(base_ticker, []).append(pub_dt)
    return pool


def _attach_english_coverage(
    results: list[dict[str, Any]], articles: list[dict[str, Any]],
) -> None:
    """Korea Signals spec Section 7 Step 5 / Section 6.4's "English press"
    line: for every classified item, record whether ANY English-language
    GDELT article about the same tracked company exists in this batch,
    and if so, how it compares in time to when the Korean item was
    published - "None found at time of retrieval" (spec's own wording) if
    not, "appeared Nh after/before the Korean" if so. Mutates each
    result's own metadata dict in place, same pattern
    translate_korea_articles uses.

    Applies to EVERY classified item, not just S7's own qualification_news
    rows - spec Section 6.4's own worked example shows this line on an S2
    supply-contract item (Hanmi Semiconductor), not only on news items, so
    this runs as its own pass over the full result set rather than being
    folded into classify_qualification_news, which only ever sees S7's own
    candidates.

    Ticker lookup checks metadata.ticker first (S7's own field - see
    classify_qualification_news), falling back to metadata.stock_code
    (DART filings' field, set by news-retrieval's _fetch_dart_filing) -
    these are two genuinely different field names for the same underlying
    company identifier across this module's own S2-S6 (stock_code) vs S7
    (ticker) code paths, not a typo to unify away (each is set by its own
    upstream fetcher/classifier and read elsewhere in this file under its
    own name already).

    A missing/unmatched ticker is NOT an error - not every classified item
    is guaranteed to belong to a company with a ticker (Hanwha Semitech is
    unlisted, ticker=None in KOREA_TICKER_UNIVERSE) - such items simply get
    no english_coverage_found field at all, same "field absent means not
    computed" convention _get_nested/translate_korea_articles already use
    for translation.
    """
    pool = _english_coverage_pool(articles)
    if not pool:
        return

    for r in results:
        meta = r["result"].get("metadata") or {}
        ticker = meta.get("ticker") or meta.get("stock_code")
        if not ticker:
            continue
        english_dts = pool.get(ticker)
        if not english_dts:
            meta["english_coverage_found"] = False
            r["result"]["metadata"] = meta
            continue

        korean_dt = _article_published_dt(r["article"])
        earliest_english_dt = min(english_dts)
        meta["english_coverage_found"] = True
        meta["english_coverage_published"] = earliest_english_dt.isoformat()
        if korean_dt is not None:
            delta_hours = (earliest_english_dt - korean_dt).total_seconds() / 3600.0
            meta["english_coverage_hours_after_korean"] = round(delta_hours, 1)
        r["result"]["metadata"] = meta


_EXPORT_SURPRISE_SIGNAL_SPREADS = 2.0
_EXPORT_SURPRISE_WEAK_SPREADS = 1.0
# Spec Section 5.1's own rule needs 12 EQUIVALENT periods (12 prior
# readings of the SAME period_type - e.g. 12 prior "1st-10th" readings,
# one per month) before the average/spread mean anything at all - not 12
# rows of any type. See this module's own docstring for the real,
# confirmed depth of history actually available today (~7 months as of
# 2026-09-23), genuinely short of this threshold for every period_type -
# this constant is the real rule, not tuned down to match what exists.
_EXPORT_SURPRISE_MIN_HISTORY_PERIODS = 12


def _customs_period_key(meta: dict[str, Any]) -> tuple[str, str] | None:
    """Return (period_type, period_start) for one kr_customs_export
    article's metadata, or None if either field is missing (a malformed
    row - not expected in practice given news-retrieval's own
    _build_customs_article, but this module never trusts a field is
    present without checking, same discipline as every other classify_*
    function here).
    """
    period_type = meta.get("period_type")
    period_start = meta.get("period_start")
    if not period_type or not period_start:
        return None
    return period_type, period_start


def _customs_same_period_last_year(period_start: str) -> str:
    """Return the period_start string for the SAME period one year
    earlier - e.g. "2026-07-01" -> "2025-07-01". Only the year changes;
    month/day are identical by construction (period_start's own day is
    always 1 for both 10-day/20-day periods - see news-retrieval's
    _classify_customs_period_type, which always encodes the period's
    START date - and for monthly periods, which use the 1st as a
    placeholder). Matching on the exact string is correct and simpler
    than parsing/re-formatting a date object, since the day component
    never needs to change.
    """
    year, rest = period_start.split("-", 1)
    return f"{int(year) - 1}-{rest}"


def classify_export_surprise(
    articles: list[dict[str, Any]],
    context_articles: list[dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Classify kr_customs_export articles per Korea Signals spec Section
    5.1 - the one signal type here that needs real historical context,
    not just the current batch.

    ``context_articles``: already-stored kr_customs_export rows (as
    {"metadata": {...}} dicts - caller's own DB read shape) covering
    at least the last ~13 periods per period_type, so a genuine YoY
    comparison and 12-period average/spread can be computed. Optional -
    omitting it means every article in this batch is classified against
    an empty history, which will correctly find no match for "same period
    one year ago" and produce nothing (see below) - not an error, just
    the same "not enough real data yet" outcome context_articles exists
    to eventually resolve.

    Per spec Section 5.1's own data-points table:
      - semiconductor export value for the period: metadata.
        semiconductor_export_usd_billion (10-day/20-day releases) OR
        derived from metadata.semiconductor_yoy_pct_stated directly
        (monthly CONFIRMED releases - see below, this is a genuinely
        different, BETTER data path for that one period_type)
      - period type: metadata.period_type ("10day"/"20day"/"monthly")
      - change against the same period one year earlier: computed here
      - average/spread over the last 12 equivalent periods: computed here
        from context_articles, grouped by period_type
      - how many spreads the current reading sits from the average:
        computed here

    Monthly releases are handled differently from 10-day/20-day: when a
    monthly post's own body already states a real semiconductor-specific
    YoY% (metadata.semiconductor_yoy_pct_stated - see news-retrieval's
    _extract_customs_figures), that stated value IS this function's own
    "change against the same period one year earlier" for that period,
    used directly rather than recomputed from two raw dollar figures -
    the source's own YoY% is a real, stated fact under the same
    "we do not have to guess" principle Section 5.2 states for supply
    contracts, not something to discard in favor of a self-computed
    figure when a genuine one already exists. A monthly post with a
    dollar figure but no stated YoY% (real, confirmed live - some monthly
    posts have one field but not the other) still needs a same-period-
    last-year dollar reading to compute YoY the normal way; if neither
    path has what it needs, that period contributes nothing (see below).

    The rule (spec's own table):
      SIGNAL - the reading sits more than 2 spreads from its 12-period
               average, in either direction, OR the YoY change is
               negative (regardless of spread distance - shrinking
               exports are rare enough in this cycle to matter on their
               own, per the spec's own reasoning)
      WEAK   - between 1 and 2 spreads away
      NOISE  - within 1 spread (the series behaving normally)

    A period contributes NO result at all (not NOISE, not anything) when
    either precondition is unmet: no same-period-one-year-ago reading
    exists in context_articles, or fewer than
    _EXPORT_SURPRISE_MIN_HISTORY_PERIODS prior equivalent periods exist to
    compute a real average/spread from. Confirmed live 2026-09-23: with
    real history reaching back only to ~2026-02, BOTH preconditions
    currently fail for every real period_type that exists - this function
    is expected to return [] against real data today, not a bug, an
    honest reflection of Section 5.1's own build note ("this rule needs
    two years of stored history before it means anything") - see this
    module's own docstring for the real, current depth.
    """
    by_period: dict[tuple[str, str], float] = {}

    def _collect(source: list[dict[str, Any]], into: dict[tuple[str, str], float]) -> None:
        for a in source:
            meta = a.get("metadata") or {}
            if meta.get("source_category") != "kr_customs_export":
                continue
            key = _customs_period_key(meta)
            if key is None:
                continue
            # Prefer the source's own stated semiconductor YoY% (monthly
            # CONFIRMED releases only - see this function's own docstring)
            # over the raw dollar figure when both exist for the SAME
            # period, since that's a directly-usable YoY reading already,
            # not something needing a year-ago comparison at all. Stored
            # keyed by (period_type, period_start) either way so the two
            # paths merge into one lookup table.
            usd = meta.get("semiconductor_export_usd_billion")
            if usd is not None:
                into[key] = float(usd)

    _collect(context_articles or [], by_period)
    _collect(articles, by_period)

    # Monthly releases' own stated YoY%, kept separate from the dollar-
    # figure table above - used directly as this period's "change" value
    # instead of computing one from two dollar readings (see docstring).
    stated_yoy: dict[tuple[str, str], float] = {}
    for source in (context_articles or []), articles:
        for a in source:
            meta = a.get("metadata") or {}
            if meta.get("source_category") != "kr_customs_export":
                continue
            key = _customs_period_key(meta)
            if key is None:
                continue
            yoy = meta.get("semiconductor_yoy_pct_stated")
            if yoy is not None:
                stated_yoy[key] = float(yoy)

    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0, "NOISE": 0, "no_data": 0}

    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "kr_customs_export":
            continue
        key = _customs_period_key(meta)
        if key is None:
            continue
        period_type, period_start = key

        # This period's own "change" value: prefer the source's own
        # stated YoY% (monthly only); otherwise compute from this
        # period's dollar figure vs the same period one year ago.
        this_yoy = stated_yoy.get(key)
        if this_yoy is None:
            this_usd = by_period.get(key)
            year_ago_key = (period_type, _customs_same_period_last_year(period_start))
            year_ago_usd = by_period.get(year_ago_key)
            if this_usd is None or year_ago_usd is None or year_ago_usd == 0:
                counts["no_data"] += 1
                continue
            this_yoy = (this_usd - year_ago_usd) / year_ago_usd * 100.0

        # 12 equivalent prior periods' own YoY values, for the average/
        # spread - each prior period's YoY is itself computed the same
        # way (stated YoY if that period was a monthly release with one,
        # else its own dollar-figure-vs-year-ago computation), so the
        # history this function judges the current reading against is
        # internally consistent with how the current reading itself was
        # derived.
        history_yoys: list[float] = []
        for candidate_key, candidate_usd in by_period.items():
            if candidate_key == key or candidate_key[0] != period_type:
                continue
            candidate_yoy = stated_yoy.get(candidate_key)
            if candidate_yoy is None:
                candidate_year_ago = by_period.get(
                    (period_type, _customs_same_period_last_year(candidate_key[1]))
                )
                if candidate_year_ago is None or candidate_year_ago == 0:
                    continue
                candidate_yoy = (candidate_usd - candidate_year_ago) / candidate_year_ago * 100.0
            history_yoys.append(candidate_yoy)

        if len(history_yoys) < _EXPORT_SURPRISE_MIN_HISTORY_PERIODS:
            counts["no_data"] += 1
            continue

        average = statistics.mean(history_yoys)
        spread = statistics.stdev(history_yoys)
        if spread == 0:
            counts["no_data"] += 1
            continue
        spreads_away = abs(this_yoy - average) / spread

        if this_yoy < 0 or spreads_away > _EXPORT_SURPRISE_SIGNAL_SPREADS:
            signal = "SIGNAL"
            reason = (
                "export_surprise_yoy_negative" if this_yoy < 0
                else "export_surprise_spreads_away"
            )
        elif spreads_away > _EXPORT_SURPRISE_WEAK_SPREADS:
            signal = "WEAK"
            reason = "export_surprise_spreads_away"
        else:
            signal = "NOISE"
            reason = "export_surprise_within_normal_range"

        counts[signal] += 1
        meta["source_category"] = "export_surprise"
        meta["yoy_pct"] = round(this_yoy, 1)
        meta["twelve_period_average_yoy_pct"] = round(average, 1)
        meta["twelve_period_spread"] = round(spread, 2)
        meta["spreads_from_average"] = round(spreads_away, 2)
        a["metadata"] = meta
        results.append({
            "article": a,
            "result": {
                "signal": _KOREA_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": f"kr_customs_export-{period_type}-{period_start}",
                "reason": reason,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[KOREA_EXPORT_SURPRISE] signal=%d weak=%d noise=%d no_data=%d",
            counts["SIGNAL"], counts["WEAK"], counts["NOISE"], counts["no_data"],
        )
    return results


def classify_korea_signal_batch(
    articles: list[dict[str, Any]],
    tracked_companies: list[dict[str, Any]] | None = None,
    model: str | None = None,
) -> list[dict[str, Any]]:
    """Run every implemented korea_market_signal classifier (S2-S7) on one
    pooled batch of articles read from news-retrieval, translate whatever
    survived, and return one {"article", "result"} dict per classified item
    for insert_korea_signal_classification - same top-level shape as
    Taiwan's classify_taiwan_signal_batch. Every Korea classifier judges
    each article/filing against fixed thresholds or its own filing body,
    never against a field of peers, so this returns a plain list.

    ``tracked_companies``: caller's own KOREA_TICKER_UNIVERSE-shaped list
    ({"ticker", "company", "korean_name", "group_prefix", "exclude_terms",
    ...} per entry, news-retrieval seed.py's own shape) - this module has
    no news-retrieval import, so the caller (controllers/run.py) is
    responsible for fetching/passing this. Used directly by
    classify_qualification_news (S7, needs the full entry - exclude_terms,
    korean_name, company together); classify_supply_contract (S2) wants a
    flatter {name: ticker}-shaped map instead (see its own docstring), so
    this function derives that shape here rather than changing S2's
    existing, already-verified signature. Optional - omitting it just
    means S2's tracked-counterparty check and all of S7 are skipped
    (S7 structurally cannot run without it - Filter 2 has nothing to match
    against), matching classify_supply_contract's own "omit for percentage-
    only" fallback.

    Runs classifiers in report_nm-title order, not calling every function
    on every article: each classify_* function already narrows to its own
    title pattern internally (see each one's own docstring - "only touches
    articles whose title contains ..."), so calling all of them against
    the full pooled batch is correct and cheap, not wasteful - an article
    that doesn't match a given classifier's pattern is a no-op call, same
    as Taiwan's classify_material_announcements being called against
    mops_revenue rows it will never touch.

    S1 (export surprise) is NOT called here - not yet implemented (see
    module docstring: needs a 2yr history backfill this module doesn't
    have yet). kr_customs_export articles pooled into ``articles`` simply
    pass through this function unclassified.
    """
    name_to_ticker: dict[str, str] = {}
    if tracked_companies:
        for entry in tracked_companies:
            ticker = entry.get("ticker") or entry.get("company")
            if entry.get("korean_name"):
                name_to_ticker[entry["korean_name"]] = ticker
            if entry.get("company"):
                name_to_ticker[entry["company"]] = ticker

    results: list[dict[str, Any]] = []
    results.extend(classify_supply_contract(articles, tracked_companies=name_to_ticker or None))
    results.extend(classify_capacity_commitment(articles))
    results.extend(classify_preliminary_earnings(articles))
    results.extend(classify_guidance_disclosure(articles))
    results.extend(classify_rumour_adjudication(articles))
    results.extend(classify_voluntary_clarification(articles))
    results.extend(classify_rumour_confirmation(articles))
    if tracked_companies:
        results.extend(classify_qualification_news(articles, tracked_companies, model=model))

    classified_articles = [r["article"] for r in results]
    translate_korea_articles(classified_articles, model=model)
    # translate_korea_articles writes translated_* fields onto each
    # article's own metadata dict in place - re-point each result's
    # "metadata" at the same (now-updated) dict, since classify_* built
    # result["metadata"] as a reference at classification time, before
    # translation ran; without this, result["metadata"] would still be the
    # right object in practice (same dict, mutated in place) - this line
    # exists for clarity/defensiveness, not because the reference would
    # otherwise be stale.
    for r in results:
        r["result"]["metadata"] = r["article"].get("metadata") or {}

    # Spec Section 7 Step 5 / Section 6.4's "English press" line - runs
    # last, against the full pooled ``articles`` (which still carries
    # KOREA_GDELT_ENGLISH_SOURCE's own rows even though they're never
    # classified into a result themselves - see _english_coverage_pool),
    # not just ``classified_articles``.
    _attach_english_coverage(results, articles)

    logger.info(
        "[KOREA_SIGNAL_BATCH] %d article(s) pooled, %d classified across S2-S7",
        len(articles), len(results),
    )
    return results
