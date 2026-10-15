"""GET /jobs - job listing and results endpoints."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query

import config
from adapters.news_client import fetch_macro_series_total
from auth import require_auth
from controllers.run import generate_korea_signal_summary_for_date
from models.jobs import get_job, get_results_summary, list_all_results, list_jobs, list_results, list_taiwan_periods
from pipeline.china_companies import china_company_universe
from pipeline.japan_companies import company_for, valuation_for
from pipeline.japan_signal_view import to_jp_signal
from pipeline.japan_companies import japan_ticker_universe

router = APIRouter()


@router.get("/results")
async def get_all_results(
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = Query(default=None),
    signal_detection: str | None = Query(default=None),
    source_type: str | None = Query(default=None, description="Filter by source_type, e.g. 'news' or 'sec_filing'"),
    ticker: str | None = Query(default=None, description="Filter by ticker (only populated for source_type='sec_filing' today)"),
    code: str | None = Query(default=None, description="Filter by metadata.code - the exchange code a market-signal domain files a company under, NOT the same field as ticker. japan_market_signal stores a TSE code ('6857', or Kioxia\'s '285A'); china_market_signal stores a mainland or HK code ('688981' SMIC, '09988' Alibaba). Case-insensitive."),
    period: str | None = Query(default=None, description="Filter by metadata.period_gregorian, e.g. '2026-07' (only populated for taiwan_market_signal mops_revenue rows)"),
    source_category: str | None = Query(default=None, description="Filter by metadata.source_category, e.g. 'mops_revenue', 'mops_material', 'gdelt' (only populated for taiwan_market_signal rows)"),
    grade: str | None = Query(default=None, description="Filter by metadata.grade: 'TOP', 'STRONG', or 'STANDARD' (only populated for geopolitical_signal rows Stage D has graded)"),
    impacted_category: str | None = Query(default=None, description="Filter to rows whose metadata.impacted_categories includes this exact category name, e.g. 'Semiconductor Manufacturing' (only populated for geopolitical_signal rows Stage C has tagged)"),
    impacted_ticker: str | None = Query(default=None, description="Filter to rows whose entities_json includes this ticker, e.g. 'NVDA' - the companies the article itself names, each stored with the reason the text gives (only populated for geopolitical_signal rows Stage C has tagged; case-insensitive)"),
    channel: str | None = Query(default=None, description="Filter by metadata.channel: 'energy', 'trade', 'sanctions', 'shipping', or 'conflict' (only populated for geopolitical_signal rows Stage C has tagged)"),
    corroborated: bool | None = Query(default=None, description="Filter by metadata.corroborated (only populated for geopolitical_signal rows Stage D has graded)"),
    published_from: str | None = Query(default=None, description="Filter to rows with published >= this date (YYYY-MM-DD), inclusive"),
    published_to: str | None = Query(default=None, description="Filter to rows with published <= this date (YYYY-MM-DD), inclusive"),
    macro_series: str | None = Query(default=None, description="Filter to macro_signal rows involving this series_id, e.g. 'DGS10' - matches either an interpreted event's member_series or a suppressed/audit row's own series_id"),
    macro_suspect: bool | None = Query(default=None, description="Filter by metadata.suspect (only populated for source_type='macro_signal' interpreted-event rows - the model's calendar-mechanics override flag from Appendix A)"),
    macro_interpreted_only: bool | None = Query(default=None, description="true: only macro_signal rows that reached and passed INTERPRET (a real LLM call, real channel/entry_point/assets/transmission fields) - excludes suppressed/NOISE audit rows kept only for the 'nothing is ever deleted' audit trail, which otherwise mix in with real signal_detection='signal'/'weak_signal' results. false: only the suppressed/audit rows. Use plain signal_detection ('signal'/'weak_signal'/'noise') for macro_signal's HIGH/WEAK/NOISE tier - same convention as every other domain, no separate macro-specific tier filter."),
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Return paginated classification results across all jobs, newest first."""
    return list_all_results(limit=limit, cursor=cursor, signal_detection=signal_detection, source_type=source_type, ticker=ticker, code=code, period=period, source_category=source_category, grade=grade, impacted_category=impacted_category, impacted_ticker=impacted_ticker, channel=channel, corroborated=corroborated, published_from=published_from, published_to=published_to, macro_series=macro_series, macro_suspect=macro_suspect, macro_interpreted_only=macro_interpreted_only)


@router.get("/korea-signals/summary")
async def get_korea_signal_summary(
    date: str | None = Query(default=None, description="YYYY-MM-DD (UTC). Defaults to today (UTC)."),
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Spec Section 8.4's twice-daily trader summary, generated on demand
    for one day's already-classified korea_market_signal rows (reads
    agent_classifications directly - no news-retrieval fetch, no
    re-classification; see generate_korea_signal_summary_for_date's own
    docstring).

    Not paginated/streamed - this returns one finished text block per
    call, same shape the CLI's summarize-korea-signals command logs.
    Calling this twice for the same date before new rows are inserted is
    expected to return the same or near-identical text - the row
    selection/ordering is fully deterministic (see
    generate_korea_signal_summary_for_date), and the model call itself
    uses temperature=0 (see korea_signal_summary.py's own payload), same
    as every other LLM call in this module - not a hard guarantee of
    byte-identical output across calls, but not free-running temperature
    either.
    """
    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    summary = generate_korea_signal_summary_for_date(date)
    return {"date": date, "summary": summary}


@router.get("/japan-signals/universe")
async def get_japan_signal_universe(
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Return the static 19-company japan_market_signal tracked universe
    - reference data, not classification results, so this reads from
    memory rather than agent_classifications.

    Exists so a frontend can fetch this once and join it client-side by
    metadata.code against GET /results rows, rather than every
    japan_market_signal classification row repeating the same company
    facts.

    Each company carries its market weight (TSE Prime share, Japan
    rank), its sales inside Japan, and its customers with the share of
    sales the company itself discloses. Those replace the bare customer
    name list this endpoint used to return: the old list had no
    citation trail, while every percentage here is a filed fact from an
    annual securities report, carrying the period it was filed for and
    the confidence in its source. A customer with no percentage is one
    the company does not disclose at 10% or above, which is itself
    information rather than a gap.

    Valuation figures are a dated snapshot, not a live feed - `asOf`
    and `isStale` travel with them so a consumer cannot present a
    months-old market cap as current.
    """
    companies = []
    for record in japan_ticker_universe():
        code = record["code"]
        # Built field by field rather than spread from the record.
        # Spreading leaked whatever the table happened to hold - the
        # market figures went out twice, once snake_case at the top
        # level and again camelCase inside marketWeight, and internal
        # fields like fiscal_year_end rode along unasked. Naming each
        # field keeps the response a decision rather than a side
        # effect of the storage shape.
        entry: dict[str, Any] = {
            "code": code,
            "company": record["company"],
            "native_name": record["native_name"],
            # The market figures carry asOf and isStale with them, so
            # a consumer cannot show a months-old market cap as
            # current without having seen the date.
            "marketWeight": valuation_for(code),
            "customers": record["customers"],
        }
        companies.append(entry)
    return {"companies": companies}


@router.get("/china-signals/universe")
async def get_china_signal_universe(
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Return the static 20-company china_market_signal tracked
    universe - reference data, not classification results, so this
    reads from memory rather than agent_classifications.

    Exists so a frontend can fetch this once and join it client-side by
    metadata.code against GET /results, rather than paging ~9k
    classification rows to recover a 20-row company list. Same role
    and shape as /japan-signals/universe.

    Each company carries its codes (mainland and, where dual-listed,
    Hong Kong), its native name, which classifiers apply to it
    (`signal_roles`), and every US name its signals read to - with
    `direction` (which way that name moves on this company's good
    news) and `relationship` (why the two are linked at all).

    The read-through links are ANALYST JUDGMENTS transcribed from the
    China Signals spec, not filed facts - no issuer discloses "our
    gains come out of Applied Materials". A consumer presenting them
    should not imply the issuer stated them. See china_companies.py's
    own docstring, which also records that one ticker (HPQ) has no
    match in research-universe.
    """
    return {"companies": china_company_universe()}


@router.get("/japan-signals/results")
async def get_japan_signal_results(
    limit: int = Query(default=50, ge=1, le=500),
    cursor: str | None = Query(default=None),
    signal_detection: str | None = Query(default=None, description="Filter by signal_detection: 'signal', 'weak_signal', or 'noise'"),
    code: str | None = Query(default=None, description="Filter to one tracked company's TSE code, e.g. '6857' (Advantest) or '285A' (Kioxia) - case-insensitive, matches metadata.code"),
    source_category: str | None = Query(default=None, description="Filter by metadata.source_category: 'jp_forecast' (J1/J2), 'jp_missing_revision' (J3), 'jp_industry' (J4), 'jp_capex' (J5), 'jp_buyback'/'jp_ownership' (J6), 'jp_press' (J7), or 'jp_watching'"),
    published_from: str | None = Query(default=None, description="Filter to rows with published >= this date (YYYY-MM-DD), inclusive"),
    published_to: str | None = Query(default=None, description="Filter to rows with published <= this date (YYYY-MM-DD), inclusive"),
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Return paginated, filterable japan_market_signal classification
    rows - the Japan-specific counterpart to the generic GET /results
    above, for the two real filters that endpoint cannot express for this
    domain: ticker there only matches metadata.ticker (sec_filing rows
    only - Japan rows store their code under metadata.code instead, a
    genuinely different field - see list_all_results' own docstring on
    `code`), and source_type there requires spelling out the full
    domain string ('japan_market_signal') on every call.

    Pins source_type=config.JAPAN_SIGNAL_DOMAIN internally and delegates
    straight to list_all_results (same cross-job, cursor-paginated
    listing GET /results itself uses) - no separate query path, no risk
    of drifting from that function's own ordering/pagination guarantees.

    Rows come back shaped for display (see pipeline.japan_signal_view):
    a figure appears once, in the field that presents it, rather than
    both raw and formatted. `published` becomes `date`, the stored
    `metadata` blob becomes the typed fields that read from it, and the
    habit statistics become the usual range and the threshold a reader
    sees. A caller wanting the stored rows exactly as classified -
    every metadata key, no formatting - can still call GET /results
    directly with source_type=japan_market_signal.

    `asOf` is the clock the consumer counts relative windows back from,
    sent so a "last 30 days" view does not depend on the reader's own
    clock agreeing with the server's.
    """
    page = list_all_results(
        limit=limit,
        cursor=cursor,
        signal_detection=signal_detection,
        source_type=config.JAPAN_SIGNAL_DOMAIN,
        code=code,
        source_category=source_category,
        published_from=published_from,
        published_to=published_to,
    )
    rows = page.get("results") or []
    dates = sorted(r["published"] for r in rows if r.get("published"))
    return {
        # Tokyo's date, not UTC's. The market this covers runs on it,
        # and a UTC date reads a day behind for the first nine hours of
        # every Japanese trading day - exactly when a reader is most
        # likely to be looking.
        "asOf": datetime.now(timezone(timedelta(hours=9))).date().isoformat(),
        "window": {
            "start": dates[0] if dates else None,
            "end": dates[-1] if dates else None,
        },
        "signals": [to_jp_signal(r) for r in rows],
        "next_cursor": page.get("next_cursor"),
    }


@router.get("/results/periods")
async def get_taiwan_periods(
    source_category: str | None = Query(default=None, description="Scope to one source_category, e.g. 'mops_revenue' or 'mops_material'. Omit for periods across both."),
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Return every distinct period (metadata.period_gregorian) that exists
    for taiwan_market_signal, newest first - backs a period filter/picker
    without a client-side scan over a fetched page. Always scoped to
    source_type='taiwan_market_signal'; that's the only source_type with
    period_gregorian populated today.
    """
    return {"periods": list_taiwan_periods(source_category=source_category)}


@router.get("/results/summary")
async def get_results_summary_route(
    source_type: str = Query(..., description="Required. e.g. 'macro_signal' - counted over exactly this source_type's rows"),
    published_from: str | None = Query(default=None, description="Filter to rows with published >= this date (YYYY-MM-DD), inclusive - same field as /results' own published_from"),
    published_to: str | None = Query(default=None, description="Filter to rows with published <= this date (YYYY-MM-DD), inclusive - same field as /results' own published_to"),
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Real ask (frontend ticket, 2026-09-25): counts for a date window,
    so the frontend doesn't have to download every audit row just to
    count them. Guaranteed to match /results for the same filters -
    see get_results_summary's own docstring for the exact counting
    rules (observations are per-series, expanded from member_series on
    interpreted events; events are per-row, not expanded).

    series_total is fetched from news-retrieval's /macro/series (the
    real tracked-series-universe count) - best-effort, None if
    news-retrieval is unreachable, since a missing series_total
    shouldn't turn an otherwise-good summary into a 500. Only
    meaningful for source_type='macro_signal' today (the only
    source_type with a real "total tracked series" concept) - None for
    every other source_type.
    """
    summary = get_results_summary(
        source_type=source_type, published_from=published_from, published_to=published_to,
    )
    summary["series_total"] = await fetch_macro_series_total() if source_type == "macro_signal" else None
    return summary


@router.get("/jobs")
async def get_jobs(
    limit: int = Query(default=20, ge=1, le=100),
    cursor: str | None = Query(default=None),
    status: str | None = Query(default="completed"),
    domain: str | None = Query(default=None, description="Filter by agent_jobs.domain, e.g. 'ai_news' or 'sec_filing'"),
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    return list_jobs(limit=limit, cursor=cursor, status=status, domain=domain)


@router.get("/jobs/{job_id}")
async def get_job_by_id(
    job_id: int,
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return job


@router.get("/jobs/{job_id}/results")
async def get_job_results(
    job_id: int,
    limit: int = Query(default=100, ge=1, le=500),
    cursor: str | None = Query(default=None),
    signal_detection: str | None = Query(default=None),
    source_type: str | None = Query(default=None, description="Filter by source_type, e.g. 'news' or 'sec_filing'"),
    ticker: str | None = Query(default=None, description="Filter by ticker (only populated for source_type='sec_filing' today)"),
    period: str | None = Query(default=None, description="Filter by metadata.period_gregorian, e.g. '2026-07' (only populated for taiwan_market_signal mops_revenue rows)"),
    source_category: str | None = Query(default=None, description="Filter by metadata.source_category, e.g. 'mops_revenue', 'mops_material', 'gdelt' (only populated for taiwan_market_signal rows)"),
    grade: str | None = Query(default=None, description="Filter by metadata.grade: 'TOP', 'STRONG', or 'STANDARD' (only populated for geopolitical_signal rows Stage D has graded)"),
    impacted_category: str | None = Query(default=None, description="Filter to rows whose metadata.impacted_categories includes this exact category name, e.g. 'Semiconductor Manufacturing' (only populated for geopolitical_signal rows Stage C has tagged)"),
    impacted_ticker: str | None = Query(default=None, description="Filter to rows whose entities_json includes this ticker, e.g. 'NVDA' - the companies the article itself names, each stored with the reason the text gives (only populated for geopolitical_signal rows Stage C has tagged; case-insensitive)"),
    channel: str | None = Query(default=None, description="Filter by metadata.channel: 'energy', 'trade', 'sanctions', 'shipping', or 'conflict' (only populated for geopolitical_signal rows Stage C has tagged)"),
    corroborated: bool | None = Query(default=None, description="Filter by metadata.corroborated (only populated for geopolitical_signal rows Stage D has graded)"),
    published_from: str | None = Query(default=None, description="Filter to rows with published >= this date (YYYY-MM-DD), inclusive"),
    published_to: str | None = Query(default=None, description="Filter to rows with published <= this date (YYYY-MM-DD), inclusive"),
    macro_series: str | None = Query(default=None, description="Filter to macro_signal rows involving this series_id, e.g. 'DGS10' - matches either an interpreted event's member_series or a suppressed/audit row's own series_id"),
    macro_suspect: bool | None = Query(default=None, description="Filter by metadata.suspect (only populated for source_type='macro_signal' interpreted-event rows - the model's calendar-mechanics override flag from Appendix A)"),
    macro_interpreted_only: bool | None = Query(default=None, description="true: only macro_signal rows that reached and passed INTERPRET (a real LLM call, real channel/entry_point/assets/transmission fields) - excludes suppressed/NOISE audit rows kept only for the 'nothing is ever deleted' audit trail, which otherwise mix in with real signal_detection='signal'/'weak_signal' results. false: only the suppressed/audit rows. Use plain signal_detection ('signal'/'weak_signal'/'noise') for macro_signal's HIGH/WEAK/NOISE tier - same convention as every other domain, no separate macro-specific tier filter."),
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")
    return list_results(job_id=job_id, limit=limit, cursor=cursor, signal_detection=signal_detection, source_type=source_type, ticker=ticker, period=period, source_category=source_category, grade=grade, impacted_category=impacted_category, impacted_ticker=impacted_ticker, channel=channel, corroborated=corroborated, published_from=published_from, published_to=published_to, macro_series=macro_series, macro_suspect=macro_suspect, macro_interpreted_only=macro_interpreted_only)
