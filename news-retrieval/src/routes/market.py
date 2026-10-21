"""Routes for /market — read market data from DynamoDB."""
import logging
import os
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.conditions import Key
from fastapi import APIRouter, HTTPException

from pipeline import _normalize_av_ticker

router = APIRouter()
logger = logging.getLogger(__name__)

_AWS_REGION = os.environ.get("AWS_REGION", "eu-north-1")
_INDEX_TICKERS = ["SPY", "QQQ", "SOXX"]

_TABLES = {
    "quote": os.environ.get("DYNAMODB_TABLE_QUOTE", "ocn-market-quote"),
    "overview": os.environ.get("DYNAMODB_TABLE_OVERVIEW", "ocn-market-overview"),
    "price_history": os.environ.get("DYNAMODB_TABLE_PRICE_HISTORY", "ocn-market-price-history"),
    "earnings": os.environ.get("DYNAMODB_TABLE_EARNINGS", "ocn-market-earnings"),
    "indices": os.environ.get("DYNAMODB_TABLE_INDICES", "ocn-market-indices"),
    "market_status": os.environ.get("DYNAMODB_TABLE_MARKET_STATUS", "ocn-market-status"),
    "sec_filings": os.environ.get("DYNAMODB_TABLE_SEC_FILINGS", "ocn-sec-filings"),
    "macro": os.environ.get("DYNAMODB_TABLE_MACRO", "ocn-market-macro"),
}

_MACRO_INDICATORS = [
    "fed_funds_rate", "cpi", "treasury_yield_10y", "unemployment",
    "nonfarm_payroll", "real_gdp", "retail_sales", "durables", "top_movers",
]


def _table(name: str):
    return boto3.resource("dynamodb", region_name=_AWS_REGION).Table(_TABLES[name])


def _deserialize(item: dict) -> dict:
    """Convert Decimal values to float for JSON serialisation."""
    return {
        k: float(v) if isinstance(v, Decimal) else v
        for k, v in item.items()
        if k != "ttl"
    }


def _latest(table_name: str, pk_name: str, pk_value: str) -> dict | None:
    """Return the most recent item for a partition key, or None."""
    result = _table(table_name).query(
        KeyConditionExpression=Key(pk_name).eq(pk_value),
        ScanIndexForward=False,
        Limit=1,
    )
    items = result.get("Items", [])
    return _deserialize(items[0]) if items else None


def _not_found(ticker: str) -> HTTPException:
    return HTTPException(
        status_code=503,
        detail=f"No data available for {ticker} yet. Poller may not have run.",
    )


def _dynamo_key(ticker: str) -> str:
    """Normalize a ticker to the same form the poller stores it under
    (e.g. BRK.B -> BRK-B), so share-class tickers can be looked up in
    whichever form the caller passes. Falls back to a plain upper() if
    normalization doesn't apply (e.g. an already-dashed or plain ticker)."""
    return _normalize_av_ticker(ticker) or ticker.strip().upper()


@router.get("/market/quote/{ticker}")
def get_quote(ticker: str) -> dict:
    """Current price, change, volume, previous close for a ticker."""
    item = _latest("quote", "ticker", _dynamo_key(ticker))
    if not item:
        raise _not_found(ticker)
    return item


@router.get("/market/overview/{ticker}")
def get_overview(ticker: str) -> dict:
    """Fundamentals strip: market cap, P/E, 52-week range, analyst target, beta, sector,
    momentum_roc, momentum_mom, 50/200-day moving averages, EPS/forward PE/price-to-book/
    EV-to-EBITDA, margins, YoY earnings/revenue growth, dividend yield/per-share."""
    item = _latest("overview", "ticker", _dynamo_key(ticker))
    if not item:
        raise _not_found(ticker)
    return item


@router.get("/market/price-history/{ticker}")
def get_price_history(ticker: str) -> dict:
    """Last 10 trading days of adjusted close prices."""
    key = _dynamo_key(ticker)
    result = _table("price_history").query(
        KeyConditionExpression=Key("ticker").eq(key),
        ScanIndexForward=False,
        Limit=10,
    )
    items = result.get("Items", [])
    if not items:
        raise _not_found(ticker)
    return {"ticker": key, "history": [_deserialize(i) for i in items]}


@router.get("/market/earnings/{ticker}")
def get_earnings(ticker: str) -> dict:
    """Next earnings date, estimated EPS, last quarter surprise %."""
    item = _latest("earnings", "ticker", _dynamo_key(ticker))
    if not item:
        raise _not_found(ticker)
    return item


@router.get("/market/indices")
def get_indices() -> dict:
    """Latest price and change% for SPY, QQQ, SOXX."""
    indices = {}
    for ticker in _INDEX_TICKERS:
        item = _latest("indices", "ticker", ticker)
        if item:
            indices[ticker] = item
    if not indices:
        raise HTTPException(status_code=503, detail="No index data available yet.")
    return {"indices": indices}


@router.get("/market/status")
def get_market_status() -> dict:
    """US market open/closed status."""
    item = _latest("market_status", "market", "US")
    if not item:
        raise HTTPException(status_code=503, detail="No market status data available yet.")
    return item


@router.get("/market/macro")
def get_macro() -> dict:
    """Latest Fed funds rate, CPI, and 10-year Treasury yield. Not ticker-keyed —
    one shared reading per indicator, updated once per daily poll."""
    macro = {}
    for indicator in _MACRO_INDICATORS:
        item = _latest("macro", "indicator", indicator)
        if item:
            macro[indicator] = item
    if not macro:
        raise HTTPException(status_code=503, detail="No macro data available yet.")
    return {"macro": macro}


@router.get("/market/sec-filings/{ticker}")
def get_sec_filings(ticker: str) -> dict:
    """Recent 8-K/10-Q/10-K filings for a ticker, newest first: form_type, filed_at,
    accession_number, primary_doc_url, cik, accepted_at, period_of_report,
    item_codes, filer_category. Metadata + link only - filing body text is
    fetched live by consumers (e.g. signal-detection-agent), never stored here.
    """
    key = _dynamo_key(ticker)
    result = _table("sec_filings").query(
        KeyConditionExpression=Key("ticker").eq(key),
        ScanIndexForward=False,
    )
    items = result.get("Items", [])
    if not items:
        raise _not_found(ticker)
    return {"ticker": key, "filings": [_deserialize(i) for i in items]}


@router.get("/market/tracked-tickers")
def get_tracked_tickers() -> dict:
    """The tracked ticker universe - single source of truth for which
    companies this system polls market/filing data for. Delegates to
    pipeline.get_tracked_ticker_universe(), the same function the Alpha
    Vantage fetch itself uses, so this route and the AV poller never
    drift apart. Other services (e.g. signal-detection-agent's SEC filing
    classification job) read this instead of duplicating the list.
    """
    from pipeline import get_tracked_ticker_universe

    universe_url = os.environ.get("RESEARCH_UNIVERSE_URL")
    universe_api_key = os.environ.get("RESEARCH_UNIVERSE_API_KEY")
    tickers = get_tracked_ticker_universe(universe_url, universe_api_key)

    return {"tickers": tickers}


@router.get("/market/taiwan/revenue-history")
def taiwan_revenue_history() -> dict:
    """Monthly revenue for the Taiwan universe, one row per company and
    month, from storage (taiwan_financials.py writes it).

    Same row shape as the China endpoint: code, company, period_type
    ("M01".."M12"), year, revenue, prior_revenue (same month last year as
    printed in that month's filing), yoy_pct (always null here - it is
    revenue / prior_revenue), source. Values are NT$ thousands. Always the
    full stored history: signal-detection-agent judges each filing against
    the company's own past 36 months, whoever filed today.
    """
    from models.company_financials import get_financials
    from taiwan_financials import DOMAIN

    return {"rows": get_financials(DOMAIN)}


@router.get("/market/middle-east/universe")
def middle_east_universe() -> dict:
    """The Middle East company table (seed.MIDDLE_EAST_TICKER_UNIVERSE),
    unchanged: each company's own listing, aliases and the US
    `counterparties` its news moves, with `share_price_on_good_news`.

    Served rather than copied onto articles at fetch time, unlike Taiwan's
    customers: a Taiwan source is scoped to one company, but a Middle East
    article is a wire or press story that can name a company in passing,
    so which companies it is ABOUT is the classifier's decision.
    signal-detection-agent reads this once per run and attaches
    counterparties only for the companies it has verified the article is
    about. Read live, so a correction to the table reaches the next run
    rather than being frozen onto stored articles.
    """
    from seed import MIDDLE_EAST_TICKER_UNIVERSE

    return {"companies": MIDDLE_EAST_TICKER_UNIVERSE}


@router.get("/market/china/revenue-history")
def china_revenue_history(
    refresh: bool = False, codes: str | None = None,
) -> dict:
    """Revenue year-on-year history for the China universe, per company
    and reporting period.

    Served from storage by default. `refresh=true` re-fetches from the
    upstream sources and replaces what is stored - that is the expensive
    path (one request per company, ~90s for the whole universe).

    `codes` narrows that refresh to a comma-separated subset, which is
    what makes a daily refresh affordable. A company's revenue figures
    change only when it files, so re-fetching twenty companies to pick
    up the one that filed this morning is twenty requests to learn one
    fact. The daily classify pass passes the codes that actually filed
    in its window - usually none, occasionally two or three - and the
    monthly job still refreshes everything as a backstop against a
    filing whose code was missed or a figure restated without a new
    filing.

    Ignored unless `refresh` is set: it narrows what is FETCHED, never
    what is returned. The response is always the full stored history,
    because a classifier building per-company baselines needs every
    company's series regardless of who filed today.

    Every row carries the same seven fields whichever source produced
    it - code, period_type, year, revenue, prior_revenue, yoy_pct,
    source - so a consumer cannot accidentally depend on one, and a
    stored row is indistinguishable from a freshly fetched one.
    """
    from models.company_financials import get_financials, upsert_financials

    if refresh:
        from china_financials import fetch_revenue_history
        from seed import CHINA_TICKER_UNIVERSE

        wanted = {c.strip() for c in (codes or "").split(",") if c.strip()}
        universe = [c for c in CHINA_TICKER_UNIVERSE
                    if not wanted or c["code"] in wanted]
        # An explicit `codes` list matching nothing in the universe is a
        # caller error - a typo'd or renamed code - and refreshing all
        # twenty in response would hide it behind a slow but successful
        # run. Fetch nothing and say so; the stored history is still
        # returned below.
        if wanted and not universe:
            logger.warning(
                "[CHINA_FIN] refresh requested for %s, none of which are in "
                "the China universe - nothing fetched", ", ".join(sorted(wanted)))

        api_key = os.environ.get("ALPHA_VANTAGE_API_KEY")
        fetched: list[dict[str, Any]] = []
        missing: list[str] = []
        for company in universe:
            series = fetch_revenue_history(company["code"], api_key)
            if not series:
                missing.append(company["code"])
                continue
            fetched.extend(
                {**row, "company": company["company"], "metric": "revenue"}
                for row in series
            )
        written = upsert_financials("china_market_signal", fetched)
        if missing:
            logger.info(
                "[CHINA_FIN] no structured revenue for %s - these have no "
                "mainland listing and no SEC filing, so their figures exist "
                "only in filing text", ", ".join(missing))
        logger.info(
            "[CHINA_FIN] refreshed %d revenue observations over %d of %d "
            "companies", written, len(universe), len(CHINA_TICKER_UNIVERSE))

    rows = get_financials("china_market_signal", "revenue")
    return {"rows": rows, "total": len(rows)}
