"""Orchestration - daily SEC filing classification job.

Mirrors controllers/run.py's shape (create job -> classify concurrently ->
persist -> mark complete) but for SEC filings: read each tracked ticker's
recent filing metadata from news-retrieval (adapters/news_client.get_sec_filings
-> GET /market/sec-filings/{ticker}), which news-retrieval's own daily poller
fetches from SEC EDGAR and stores in DynamoDB. This service never calls
EDGAR's submissions API directly - only the filing body text (fetch_filing_text
below) is fetched here, since news-retrieval stores metadata + a link only.

One CloudWatch schedule triggers this directly (see infra/modules/ecs_cluster),
timed to run after news-retrieval's own sec_filings poller (01:00 UTC daily) -
there is no push/fire-and-forget step between the two services. The ticker
universe (get_tracked_tickers) is likewise read live from news-retrieval
rather than duplicated, since that list is its single source of truth.
"""
from __future__ import annotations

import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import config
from adapters import sec_edgar
from adapters.news_client import get_sec_filings, get_tracked_tickers
from models.jobs import (
    create_job,
    get_existing_filing_source_ids,
    insert_filing_classification,
    update_job_status,
)
from pipeline.filing_classifier import (
    classify_filing_two_stage,
    load_sec_filing_prompt,
    load_sec_filing_summary_prompt,
)

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=config.FILING_CLASSIFY_CONCURRENCY)

# Every filing's text is still cut at _FILING_TEXT_MAX_CHARS, as before.
# 10-K/10-Q/20-F/40-F keep up to _FULL_TEXT_MAX_CHARS in memory so section
# extraction can retry on the whole document when the cut-off text has no
# results window - measured on 59 extraction failures (2026-10-24): 18 were
# 1.0-2.1M-char 10-K/20-F documents whose results section sat past the
# 600K cut. The retry only runs when the first pass found nothing, so every
# filing that already extracts gets the identical window as before.
_FILING_TEXT_MAX_CHARS = 600_000
_FULL_TEXT_MAX_CHARS = 3_000_000


async def _fetch_all_filings(tickers: list[str]) -> list[dict[str, Any]]:
    """Fetch stored filing metadata for every tracked ticker from news-retrieval.
    One HTTP call per ticker, bounded by FILING_CLASSIFY_CONCURRENCY - this is
    a read against news-retrieval's own DynamoDB-backed endpoint, not a call to
    SEC EDGAR, so there's no external rate limit to share across tickers here.
    """
    semaphore = asyncio.Semaphore(config.FILING_CLASSIFY_CONCURRENCY)

    async def fetch_one(ticker: str) -> list[dict[str, Any]]:
        async with semaphore:
            try:
                return await get_sec_filings(ticker)
            except Exception:
                logger.exception("Failed to fetch filings for ticker=%s", ticker)
                return []

    results = await asyncio.gather(*[fetch_one(t) for t in tickers])
    return [filing for filings in results for filing in filings]


def _fetch_filing_text_and_extract(filing: dict[str, Any], last_call: list[float]) -> tuple[str, bool, str]:
    """Fetch one filing's text and, for 10-K/10-Q, extract the relevant sections.

    Returns (text, extraction_found, exhibit_fetch_status). exhibit_fetch_status
    is only meaningful for 8-Ks carrying an exhibit-fetch-eligible item code
    (see sec_edgar._EXHIBIT_FETCH_ITEM_CODES): "full" (every referenced EX-99*
    exhibit fetched), "partial" (index fetch or an exhibit fetch failed, or no
    EX-99* exhibit was listed at all despite an eligible item code). Every
    other filing (10-K/10-Q, or an 8-K with no eligible item code) gets
    "not_applicable" - passed straight through to the prompt so the model
    knows whether "not_applicable" means "no fetch was warranted" versus
    "full"/"partial" meaning a fetch was attempted (10-K/10-Q's own
    extraction_found already covers whether ITS OWN text was usable, tracked
    separately as the returned bool).

    10-K/10-Q use extract_filing_sections (results-of-operations + qualitative
    notes on income taxes/contingencies/debt), not the full ~100K-500K char
    document. History here: first tried extract_results_section alone (just
    the numbers window) for 10-Q - real ~85% cost cut, $0.13 -> $0.02/call -
    but reverted after confirming on a real AAPL 10-Q that window dropped a
    genuinely material EU State Aid legal matter living in a different note,
    and the model's own signal_reason flipped from "not a routine in-line
    quarter" to "routine periodic filing" on the identical filing once that
    context was cut. extract_filing_sections adds the qualitative notes back
    (found via "Note N - Title" headers, which generalize across filers even
    though note numbering doesn't) - confirmed to restore the State Aid
    content while still cutting the same AAPL 10-Q by 82.5% (170K -> 30K
    chars), since the numeric tables that pad out the rest of the document
    are no longer needed in prose - see xbrl_facts below for why.

    8-K: the cover-page primary document rarely carries the actual figures
    for 2.02 (earnings)/7.01 (Reg FD)/8.01 (other events) - those live in an
    attached EX-99* exhibit instead. Confirmed empirically this session:
    every real 2.02 filing classified so far was scored noise with a reason
    like "figures are in Exhibit 99.1 which is not included", because only
    the primary doc was ever fetched. For those item codes, fetch the
    exhibit(s) too and append to the primary doc's text; other item codes
    (e.g. 5.02, 5.07) are typically self-contained on the cover page and
    don't warrant the extra EDGAR round-trip.
    """
    full_text = sec_edgar.fetch_filing_text(
        filing["primary_doc_url"], last_call, max_chars=_FULL_TEXT_MAX_CHARS,
    )
    text = full_text[:_FILING_TEXT_MAX_CHARS]
    if not text:
        return "", False, "not_applicable"
    if filing["form_type"] in ("10-K", "10-Q", "40-F", "20-F"):
        # 40-F (Canadian MJDS annual report) / 20-F (other foreign private
        # issuer annual report) are the foreign-issuer equivalents of 10-K -
        # same "periodic annual/interim report" shape, same risk that the
        # primary document is a thin cover-page/XBRL shell with the real
        # content furnished as a separate exhibit (confirmed empirically:
        # AEM's 40-F primary doc uses IFRS XBRL tags almost exclusively,
        # with the actual annual report living in EX-99.1/EX-99.2 - a
        # different exhibit-numbering convention than a domestic 10-K's
        # EX-13, but the same underlying pattern). 20-F, by contrast, was
        # confirmed to carry its real narrative directly in the primary
        # document on a real TSM filing (131 genuine "revenue" mentions,
        # not XBRL-tag noise) - so the EX-13/EX-99 fallback below is a
        # no-op for filers like TSM (extract_filing_sections finds real
        # content on the first attempt) and only actually fires for
        # filers structured like AEM.
        section_text, found = sec_edgar.extract_filing_sections(text)
        if not found and len(full_text) > len(text):
            section_text, found = sec_edgar.extract_filing_sections(full_text)
        if not found and filing.get("cik"):
            # Some 10-K/40-F filers (confirmed empirically this session:
            # IBM's 10-K via EX-13, AEM's 40-F via EX-99.1/EX-99.2) file
            # primary_doc_url as a thin cover-page/XBRL-cover shell with no
            # real MD&A narrative - the actual annual report is furnished
            # as a separate exhibit instead. found=False on the primary doc
            # is exactly the signal that happened, so retry section
            # extraction against whichever fallback exhibit is actually
            # present before giving up - most filers have neither, so this
            # fallback fetch only fires on the rare filing that needs it
            # (see fetch_annual_report_exhibit_text's docstring).
            annual_report_text = sec_edgar.fetch_annual_report_exhibit_text(
                filing.get("cik", ""), filing.get("accession_number", ""), last_call,
            )
            if not annual_report_text:
                # EX-13 (domestic 10-K convention) came back empty - try
                # the EX-99* fallback (40-F/20-F convention, confirmed on
                # AEM) before giving up entirely. Needs a much higher
                # max_chars than fetch_filing_exhibits_text's 13K default
                # (tuned for a single small 8-K press release) - confirmed
                # empirically on a real AEM 40-F: EX-99.1 alone is a
                # 3.3MB Annual Information Form, so a 13K combined cap
                # across ALL EX-99* exhibits never reaches past its front
                # matter/table of contents into either its own financial
                # content or EX-99.2's, silently returning the wrong
                # section instead of failing loudly. 300K matches
                # fetch_annual_report_exhibit_text's own default for the
                # same "large annual report exhibit" case.
                annual_report_text, _ = sec_edgar.fetch_filing_exhibits_text(
                    filing.get("cik", ""), filing.get("accession_number", ""), [], last_call,
                    max_chars=300_000,
                )
            if annual_report_text:
                section_text, found = sec_edgar.extract_filing_sections(annual_report_text)
        mdna_text = ""
        if not found and filing["form_type"] in ("40-F", "20-F") and filing.get("cik"):
            # The combined EX-99* text above is cut at 300K, so a large
            # Annual Information Form in EX-99.1 can push the MD&A exhibit
            # out of it - read the MD&A exhibit on its own instead.
            mdna_text = sec_edgar.fetch_mdna_exhibit_text(
                filing.get("cik", ""), filing.get("accession_number", ""), last_call,
            )
            if mdna_text:
                section_text, found = sec_edgar.extract_filing_sections(mdna_text)
        # Looser tiers only once the precise ones have failed on every text
        # above (see sec_edgar._find_results_window's last_resort).
        if not found:
            section_text, found = sec_edgar.extract_filing_sections(full_text, last_resort=True)
        if not found and mdna_text:
            section_text, found = sec_edgar.extract_filing_sections(mdna_text, last_resort=True)
        return section_text, found, "not_applicable"

    if filing["form_type"] == "6-K":
        # 6-K (foreign private issuer current report) is the closest
        # analog to an 8-K, but never carries item_codes (that's an
        # 8-K-only SEC concept) - the item-code-based exhibit-fetch gate
        # below would never fire for one, even though a 6-K's primary
        # document is confirmed (on a real AEM 6-K) to be JUST a cover
        # page, with the actual press-release content living in an
        # EX-99.1 exhibit every time, the same way a 2.02/7.01/8.01 8-K's
        # substance lives in its exhibit. Always attempt the exhibit
        # fetch unconditionally rather than gating on item_codes, which a
        # 6-K will never have.
        exhibit_text, quality = sec_edgar.fetch_filing_exhibits_text(
            filing.get("cik", ""), filing.get("accession_number", ""), [], last_call,
        )
        if exhibit_text:
            text = text + "\n\n--- Attached exhibit(s) ---\n\n" + exhibit_text
        return text, True, ("full" if quality == "full" else "partial")

    item_codes = filing.get("item_codes") or []
    if any(code in sec_edgar._EXHIBIT_FETCH_ITEM_CODES for code in item_codes):
        # Skip the EDGAR index-page round-trip when the primary doc itself
        # gives no sign an exhibit exists - confirmed empirically this
        # session against 46 real exhibit-eligible 8-Ks: every 2.02 filing
        # references "Exhibit 99" in its cover page (2.02's substance is
        # ALWAYS in the exhibit, never skip for it), but ~22% of 7.01/8.01-
        # only filings describe the event directly in the primary doc with
        # no exhibit at all - fetching the index page for those always came
        # back with no EX-99* row. This is a pure EDGAR-request-count
        # reduction, not a token/cost saving: when no exhibit exists,
        # exhibit_text is already "" today and contributes nothing to the
        # prompt either way - skipping the fetch just avoids the wasted
        # round-trip, it does not change what the model sees.
        skip_fetch = "2.02" not in item_codes and "exhibit 99" not in text.lower()
        if skip_fetch:
            return text, True, "not_applicable"

        exhibit_text, quality = sec_edgar.fetch_filing_exhibits_text(
            filing.get("cik", ""), filing.get("accession_number", ""), item_codes, last_call,
        )
        if exhibit_text:
            text = text + "\n\n--- Attached exhibit(s) ---\n\n" + exhibit_text
        # fetch_filing_exhibits_text's "none" (index/exhibit fetch failed, or
        # no EX-99* exhibit was listed) maps to "partial" here, not
        # "not_applicable" - an eligible item code means a fetch WAS
        # warranted and attempted, so the model must be told the exhibit is
        # genuinely absent from what it's been given, not that no fetch was
        # ever due. "full" (all fetched) passes through unchanged.
        return text, True, ("full" if quality == "full" else "partial")

    return text, True, "not_applicable"


async def run_filing_classification_job(job_id: int, tickers: list[str] | None = None) -> None:
    """Background task: fetch tracked tickers' filings -> dedup -> classify -> persist.

    tickers overrides the fetched universe when provided - for scoped manual
    test runs (e.g. one ticker) without touching the scheduled job's normal
    all-tracked-tickers behavior, which always passes None.
    """
    update_job_status(job_id, "running")
    if tickers is None:
        try:
            tickers = await get_tracked_tickers()
        except Exception:
            logger.exception("Failed to fetch tracked ticker list for job %d", job_id)
            update_job_status(job_id, "failed", set_completed_at=True)
            return

    loop = asyncio.get_event_loop()
    all_filings = await _fetch_all_filings(tickers)

    accession_numbers = [f["accession_number"] for f in all_filings if f.get("accession_number")]
    existing = get_existing_filing_source_ids(accession_numbers)
    new_filings = [f for f in all_filings if f.get("accession_number") not in existing]
    update_job_status(job_id, "running", article_count=len(new_filings))

    if not new_filings:
        logger.info("No new filings for job %d (fetched=%d, all already classified)", job_id, len(all_filings))
        update_job_status(job_id, "completed", set_completed_at=True)
        return

    system_prompt = load_sec_filing_prompt()
    summary_system_prompt = load_sec_filing_summary_prompt()
    models = [config.SEC_FILING_MODEL]
    semaphore = asyncio.Semaphore(config.FILING_CLASSIFY_CONCURRENCY)
    # One shared clock for ALL sec.gov/data.sec.gov calls (filing text AND
    # XBRL) - two separate clocks would each individually respect
    # _MIN_INTERVAL but let the combined rate across concurrent filings
    # exceed SEC's ~10 req/sec guidance, since nothing would coordinate
    # between them. Same fix as _fetch_all_filings's shared last_call this
    # session, just applied here too.
    edgar_last_call = [0.0]
    company_facts_cache: dict[str, dict] = {}
    company_facts_locks: dict[str, asyncio.Lock] = {}

    async def _get_company_facts(cik: str) -> dict:
        """Fetch+cache one ticker's XBRL companyfacts document per job run -
        same "fetch once per ticker, reuse across its filings" pattern as
        company_overview, since one CIK's companyfacts covers every filing.
        Locked per-CIK so two filings for the same ticker classified
        concurrently can't both miss the cache and fire duplicate ~4-5MB
        fetches - confirmed this is a real race, not hypothetical, given
        FILING_CLASSIFY_CONCURRENCY allows several filings in flight at once.
        """
        lock = company_facts_locks.setdefault(cik, asyncio.Lock())
        async with lock:
            if cik not in company_facts_cache:
                company_facts_cache[cik] = await loop.run_in_executor(
                    _executor, sec_edgar.fetch_company_facts, cik, edgar_last_call,
                )
            return company_facts_cache[cik]

    async def classify_one(filing: dict[str, Any]) -> bool:
        """Classify a single filing; returns True if skipped (failed)."""
        async with semaphore:
            try:
                filing_text, extraction_found, exhibit_fetch_status = await loop.run_in_executor(
                    _executor, _fetch_filing_text_and_extract, filing, edgar_last_call,
                )
                xbrl_facts = None
                trailing_quarterly_revenue = None
                if filing["form_type"] in ("10-K", "10-Q") and filing.get("cik"):
                    company_facts = await _get_company_facts(filing["cik"])
                    xbrl_facts = sec_edgar.extract_xbrl_facts_for_filing(
                        company_facts, filing["accession_number"],
                        filing.get("period_of_report", ""), filing["form_type"],
                    )
                    trailing_quarterly_revenue = sec_edgar.get_trailing_quarterly_revenue(
                        company_facts, filing.get("period_of_report", ""), filing["form_type"],
                    )
                result = await loop.run_in_executor(
                    _executor,
                    lambda f=filing, t=filing_text, found=extraction_found, xf=xbrl_facts, efs=exhibit_fetch_status, tqr=trailing_quarterly_revenue: classify_filing_two_stage(
                        f, t,
                        summary_system_prompt=summary_system_prompt,
                        classify_system_prompt=system_prompt,
                        models=models,
                        api_key=config.OPENAI_API_KEY,
                        base_url=config.OPENAI_BASE_URL,
                        timeout=config.OPENAI_TIMEOUT,
                        max_attempts=config.OPENAI_MAX_ATTEMPTS,
                        extraction_found=found,
                        cache_system_prompt=True,
                        xbrl_facts=xf,
                        exhibit_fetch_status=efs,
                        trailing_quarterly_revenue=tqr,
                    ),
                )
                insert_filing_classification(job_id, filing, result)
                return False
            except Exception:
                logger.exception(
                    "Filing classification failed for accession=%s (job %d)",
                    filing.get("accession_number"), job_id,
                )
                return True

    outcomes = await asyncio.gather(*[classify_one(f) for f in new_filings])
    skipped = sum(outcomes)

    if skipped == len(new_filings) and new_filings:
        update_job_status(job_id, "failed", set_completed_at=True)
    else:
        update_job_status(job_id, "completed", set_completed_at=True)


def submit_filing_run() -> int:
    """Create a new agent_jobs row for the SEC filing domain; return its id.
    Caller schedules run_filing_classification_job(job_id) as a background task.
    """
    return create_job(domain=config.SEC_FILING_DOMAIN)


async def run_ticker_backfill() -> None:
    """One-time migration: add metadata.ticker to sec_filing rows written
    before insert_filing_classification() started storing it. Never guesses
    ticker from the display-only title string - rebuilds the same
    accession_number -> ticker mapping insert_filing_classification()
    originally had, straight from news-retrieval's GET
    /market/sec-filings/{ticker} (the same call classify_filings makes),
    keyed by the SAME tracked-ticker universe so accession numbers from
    this run resolve against fresh data rather than a stale local guess.

    Read-only against EDGAR (no fetches at all, just news-retrieval reads)
    and cheap - only the metadata endpoint is called, no filing text/LLM
    calls, so this is safe to run independently of the classification job.
    """
    from models.jobs import backfill_filing_tickers, get_filing_source_ids_missing_ticker

    missing = get_filing_source_ids_missing_ticker()
    logger.info("Ticker backfill: %d rows missing metadata.ticker", len(missing))
    if not missing:
        return
    missing_set = set(missing)

    tickers = await get_tracked_tickers()
    logger.info("Ticker backfill: %d tracked tickers to check", len(tickers))

    semaphore = asyncio.Semaphore(10)
    accession_to_ticker: dict[str, str] = {}

    async def fetch_one(ticker: str) -> None:
        async with semaphore:
            try:
                filings = await get_sec_filings(ticker)
            except Exception:
                logger.exception("Ticker backfill: failed to fetch filings for ticker=%s", ticker)
                return
            for f in filings:
                acc = f.get("accession_number")
                if acc and acc in missing_set:
                    accession_to_ticker[acc] = ticker

    await asyncio.gather(*[fetch_one(t) for t in tickers])
    logger.info("Ticker backfill: matched %d/%d rows", len(accession_to_ticker), len(missing))

    updated = backfill_filing_tickers(accession_to_ticker)
    logger.info("Ticker backfill: updated %d rows", updated)
