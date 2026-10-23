"""HTTP client for the news-retrieval service."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

import config

logger = logging.getLogger(__name__)

_POLL_INTERVAL_SECS = 10
_FETCH_PAGE_SIZE = 100


class NewsRetrievalError(Exception):
    """Raised when news-retrieval returns an unexpected response."""


def _headers() -> dict[str, str]:
    return {"x-ocn-caller": config.NEWS_RETRIEVAL_SERVICE_CALLER}


async def fetch_china_revenue_history(
    refresh: bool = False, codes: list[str] | None = None,
) -> list[dict[str, Any]]:
    """The China universe's revenue YoY series, per company and period.

    Fetched from news-retrieval rather than from cninfo and Alpha
    Vantage directly: reaching external sources is that service's job,
    and this one should not hold two more sets of API conventions.

    Reads STORED figures by default - a baseline refresh should not
    trigger twenty upstream calls. `refresh=True` asks news-retrieval
    to re-fetch first, which takes around ninety seconds across the
    whole universe against about five seconds for the stored read.

    `codes` narrows that re-fetch to named companies, which is what
    makes it affordable daily: a company's figures change only when it
    files, so the daily pass re-fetches just the companies that filed
    in its window (usually none) while the monthly job refreshes
    everything. Without `codes` a refresh covers the whole universe,
    which is still what the monthly job wants.

    Narrowing applies to the FETCH only - the response is always every
    stored company's series, because per-company baselines are built
    for the whole universe regardless of who filed today.

    Returns [] on any failure - C2 then builds what it can from filing
    text, which is the behaviour before this source existed.
    """
    params: dict[str, str] = {}
    if refresh:
        params["refresh"] = "true"
        if codes:
            params["codes"] = ",".join(sorted(set(codes)))
    try:
        async with httpx.AsyncClient(timeout=300.0) as client:
            resp = await client.get(
                f"{config.NEWS_RETRIEVAL_URL}/market/china/revenue-history",
                params=params or None)
            resp.raise_for_status()
            payload = resp.json()
    except Exception:
        logger.exception("Failed to fetch China revenue history")
        return []
    rows = payload.get("rows") or []
    missing = payload.get("no_structured_source") or []
    logger.info(
        "[CHINA_FIN] %d revenue observations; %d company/companies have no "
        "structured source and fall back to filing text: %s",
        len(rows), len(missing), ", ".join(missing) or "none")
    return rows


async def fetch_taiwan_revenue_history() -> list[dict[str, Any]] | None:
    """Every stored Taiwan monthly revenue row (company, month, revenue,
    prior-year revenue as printed), from news-retrieval's company_financials.

    Returns None on failure - distinct from [] - because a revenue filing
    classified without history would be stored permanently as
    not_scored_short_history; the caller skips revenue for that run instead
    and the next run retries.
    """
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(
                f"{config.NEWS_RETRIEVAL_URL}/market/taiwan/revenue-history")
            resp.raise_for_status()
            return resp.json().get("rows") or []
    except Exception:
        logger.exception("Failed to fetch Taiwan revenue history")
        return None


async def fetch_taiwan_universe() -> list[dict[str, Any]] | None:
    """The Taiwan company table from news-retrieval
    (seed.TAIWAN_TICKER_UNIVERSE): ticker, company_name, translated_company_name
    and verified customers per company.

    Returns None on failure - distinct from [] - so the route can answer 503
    rather than an empty universe that reads as "no companies".
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(
                f"{config.NEWS_RETRIEVAL_URL}/market/taiwan/universe")
            resp.raise_for_status()
            return resp.json().get("companies") or []
    except Exception:
        logger.exception("Failed to fetch the Taiwan company table")
        return None


async def fetch_middle_east_universe() -> list[dict[str, Any]] | None:
    """The Middle East company table from news-retrieval
    (seed.MIDDLE_EAST_TICKER_UNIVERSE, served unchanged) - each company's
    own listing, aliases and US counterparties.

    Returns None on failure - distinct from [] - because a row classified
    without the table would be stored permanently without its read-through;
    the caller fails the run instead and the next pass retries.
    """
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            resp = await client.get(
                f"{config.NEWS_RETRIEVAL_URL}/market/middle-east/universe")
            resp.raise_for_status()
            return resp.json().get("companies") or []
    except Exception:
        logger.exception("Failed to fetch the Middle East company table")
        return None


async def trigger_run(domain: str, days_back: int = 7) -> int:
    """POST /run to news-retrieval; return run_id.

    Handles 202 (new run), 200 (cache hit), and 409 (conflict).
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{config.NEWS_RETRIEVAL_URL}/run",
                json={"domain": domain, "days_back": days_back},
                headers=_headers(),
            )
    except httpx.HTTPError as exc:
        raise NewsRetrievalError(f"news-retrieval unreachable: {exc}") from exc

    if resp.status_code == 200:
        return int(resp.json()["id"])
    if resp.status_code == 202:
        return int(resp.json()["run_id"])
    if resp.status_code == 409:
        return int(resp.json()["detail"]["run_id"])
    raise NewsRetrievalError(
        f"POST /run returned {resp.status_code}: {resp.text}"
    )


async def trigger_macro_fetch(*, incremental: bool) -> int:
    """POST /macro/fetch to news-retrieval; return run_id.

    Separate from trigger_run (POST /run) because macro_signal isn't an
    article-shaped domain - news-retrieval's own /macro/fetch takes
    `incremental` (not `domain`/`days_back`) and is tracked in the same
    `runs` table via controllers.macro_run rather than the generic
    create_run_record/run_pipeline path (see that module's own
    docstring). poll_run_until_done works unchanged against the
    returned run_id - GET /runs/{id} doesn't care which path created
    the row.
    """
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.post(
                f"{config.NEWS_RETRIEVAL_URL}/macro/fetch",
                json={"incremental": incremental},
                headers=_headers(),
            )
    except httpx.HTTPError as exc:
        raise NewsRetrievalError(f"news-retrieval unreachable: {exc}") from exc

    if resp.status_code == 202:
        return int(resp.json()["run_id"])
    raise NewsRetrievalError(
        f"POST /macro/fetch returned {resp.status_code}: {resp.text}"
    )


async def fetch_macro_series_total() -> int | None:
    """GET /macro/series from news-retrieval; return the tracked series
    count. Backs GET /results/summary's series_total field (real ask,
    frontend ticket, 2026-09-25) - news-retrieval owns the series
    universe (MACRO_SERIES_UNIVERSE), not this service, so the count is
    fetched, not recomputed here. Returns None (not raise) on any
    failure - a summary with series_total missing is still useful; a
    summary that 500s because a sibling service hiccuped is not, same
    fail-soft posture as the rest of this module's read paths."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{config.NEWS_RETRIEVAL_URL}/macro/series",
                headers=_headers(),
            )
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("[MACRO_SIGNAL_SUMMARY] failed to fetch series_total from news-retrieval: %s", exc)
        return None
    return resp.json().get("total")


async def fetch_latest_run(domain: str) -> int | None:
    """GET /runs?domain=<slug>&status=completed&limit=1; return run_id or None."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{config.NEWS_RETRIEVAL_URL}/runs",
                params={"domain": domain, "status": "completed", "limit": 1},
                headers=_headers(),
            )
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        raise NewsRetrievalError(f"news-retrieval unreachable: {exc}") from exc
    runs = resp.json().get("runs") or []
    return int(runs[0]["id"]) if runs else None


async def poll_run_until_done(run_id: int) -> None:
    """Poll GET /runs/{run_id} until status is completed or failed."""
    elapsed = 0
    async with httpx.AsyncClient(timeout=10.0) as client:
        while True:
            try:
                resp = await client.get(
                    f"{config.NEWS_RETRIEVAL_URL}/runs/{run_id}",
                    headers=_headers(),
                )
                resp.raise_for_status()
                status = resp.json().get("status")
            except Exception as exc:
                raise NewsRetrievalError(
                    f"Failed to poll news-retrieval run {run_id}: {exc}"
                ) from exc

            if status == "completed":
                return
            if status == "failed":
                raise NewsRetrievalError(f"news-retrieval run {run_id} failed")
            if elapsed >= config.PIPELINE_POLL_TIMEOUT_SECS:
                raise NewsRetrievalError(
                    f"Timed out waiting for news-retrieval run {run_id}"
                )
            await asyncio.sleep(_POLL_INTERVAL_SECS)
            elapsed += _POLL_INTERVAL_SECS


async def get_company_overview(ticker: str) -> dict[str, Any] | None:
    """GET /market/overview/{ticker} from news-retrieval - fundamentals strip
    (market_cap, revenue_ttm, shares_outstanding, sector, etc). No auth header
    required, /market/* routes are unauthenticated.

    Returns None on 503 (poller hasn't run for this ticker yet) or any other
    failure - callers should treat missing overview data as "unavailable",
    not as an error to abort classification over.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{config.NEWS_RETRIEVAL_URL}/market/overview/{ticker}")
        if resp.status_code == 503:
            return None
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch overview for %s: %s", ticker, exc)
        return None


async def get_price_history(ticker: str, days: int = 10) -> list[dict[str, Any]]:
    """GET /market/price-history/{ticker}?days= - the last `days` trading
    days of adjusted closes, newest first. [] when unavailable: market
    prices are a reference for a run's log, never an input a run should
    fail over."""
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{config.NEWS_RETRIEVAL_URL}/market/price-history/{ticker}",
                                    params={"days": days})
        if resp.status_code == 503:
            return []
        resp.raise_for_status()
        return resp.json().get("history", [])
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch price history for %s: %s", ticker, exc)
        return []


async def get_sec_filings(ticker: str) -> list[dict[str, Any]]:
    """GET /market/sec-filings/{ticker} from news-retrieval - filing metadata
    (form_type, filed_at, accession_number, primary_doc_url) fetched from EDGAR
    and stored by news-retrieval's own daily poller. signal-detection-agent
    does not call EDGAR's submissions API itself; this is the only source of
    filing metadata for the classification job.

    Returns [] on 503 (no filings stored yet for this ticker) or any failure -
    callers should treat a missing/failed ticker as "nothing new", not abort
    the whole run over one ticker.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{config.NEWS_RETRIEVAL_URL}/market/sec-filings/{ticker}")
        if resp.status_code == 503:
            return []
        resp.raise_for_status()
        return resp.json().get("filings", [])
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch SEC filings for %s: %s", ticker, exc)
        return []


async def get_tracked_tickers() -> list[str]:
    """GET /market/tracked-tickers from news-retrieval - the single source of
    truth ticker universe (fetched live from research-universe by
    news-retrieval's get_tracked_ticker_universe()). Read live rather than
    duplicating the list here, so it never drifts out of sync when
    news-retrieval's list is updated.

    Raises NewsRetrievalError on failure - unlike get_company_overview, there
    is no sensible per-ticker fallback if this fails entirely; the caller has
    nothing to classify without it.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(f"{config.NEWS_RETRIEVAL_URL}/market/tracked-tickers")
        resp.raise_for_status()
        return resp.json().get("tickers", [])
    except httpx.HTTPError as exc:
        raise NewsRetrievalError(f"Failed to fetch tracked tickers: {exc}") from exc


async def list_completed_runs(
    domain: str, from_date: str, to_date: str,
) -> list[int]:
    """GET /runs?domain=&status=completed&from_date=&to_date=, paginated;
    return ALL matching run ids, not just the latest one.

    news-retrieval polls taiwan_market_signal every 30 minutes, so a single
    day can have up to ~30 separate completed runs - a twice-daily
    classification pass needs to read across all of them, not just
    fetch_latest_run's single most-recent run, or it would silently miss
    everything from earlier runs that same day.
    """
    run_ids: list[int] = []
    cursor: str | None = None
    async with httpx.AsyncClient(timeout=15.0) as client:
        while True:
            params: dict[str, Any] = {
                "domain": domain, "status": "completed",
                "from_date": from_date, "to_date": to_date,
                "limit": 100,
            }
            if cursor:
                params["cursor"] = cursor
            try:
                resp = await client.get(
                    f"{config.NEWS_RETRIEVAL_URL}/runs",
                    params=params,
                    headers=_headers(),
                )
                resp.raise_for_status()
            except httpx.HTTPError as exc:
                raise NewsRetrievalError(
                    f"Failed to list runs for domain={domain!r}: {exc}"
                ) from exc
            data = resp.json()
            run_ids.extend(int(r["id"]) for r in data.get("runs", []))
            cursor = data.get("next_cursor")
            if not cursor:
                break
    return run_ids


async def get_article(article_id: int) -> dict[str, Any] | None:
    """GET /articles/{article_id} from news-retrieval; return the article
    (including its metadata, e.g. also_reported_by), or None if it no
    longer exists (e.g. expired by news-retrieval's retention job).

    Used by Stage D to read also_reported_by for corroboration grading -
    that field lives only in news-retrieval's own article metadata, never
    copied into this service's agent_classifications rows.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(
                f"{config.NEWS_RETRIEVAL_URL}/articles/{article_id}",
                headers=_headers(),
            )
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        return resp.json()
    except httpx.HTTPError as exc:
        logger.warning("Failed to fetch article %s: %s", article_id, exc)
        return None


async def get_run_articles(run_id: int) -> list[dict[str, Any]]:
    """Paginate GET /runs/{run_id}/articles; return all articles with body."""
    articles: list[dict[str, Any]] = []
    cursor: str | None = None
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            params: dict[str, Any] = {"include_body": "true", "limit": _FETCH_PAGE_SIZE}
            if cursor:
                params["cursor"] = cursor
            try:
                resp = await client.get(
                    f"{config.NEWS_RETRIEVAL_URL}/runs/{run_id}/articles",
                    params=params,
                    headers=_headers(),
                )
                resp.raise_for_status()
            except Exception as exc:
                raise NewsRetrievalError(
                    f"Failed to fetch articles for run {run_id}: {exc}"
                ) from exc
            data = resp.json()
            articles.extend(data.get("articles", []))
            cursor = data.get("next_cursor")
            if not cursor:
                break
    return articles


async def get_macro_observations(
    series_ids: list[str] | None = None,
    from_date: str | None = None,
    to_date: str | None = None,
) -> list[dict[str, Any]]:
    """Paginate GET /macro/observations; return ALL matching rows. Uses
    _headers() (x-ocn-caller), not the no-auth /market/* pattern - this
    route is not under /market/* (see news-retrieval's routes/macro.py
    docstring for why: avoids api-gateway's forced-auth-for-market/*
    special case while still being gateway-proxied normally)."""
    observations: list[dict[str, Any]] = []
    cursor: str | None = None
    async with httpx.AsyncClient(timeout=30.0) as client:
        while True:
            params: dict[str, Any] = {"limit": 2000}
            if series_ids:
                params["series"] = series_ids
            if from_date:
                params["from_date"] = from_date
            if to_date:
                params["to_date"] = to_date
            if cursor:
                params["cursor"] = cursor
            try:
                resp = await client.get(
                    f"{config.NEWS_RETRIEVAL_URL}/macro/observations",
                    params=params,
                    headers=_headers(),
                )
                resp.raise_for_status()
            except Exception as exc:
                raise NewsRetrievalError(
                    f"Failed to fetch macro observations: {exc}"
                ) from exc
            data = resp.json()
            observations.extend(data.get("observations", []))
            cursor = data.get("next_cursor")
            if not cursor:
                break
    return observations
