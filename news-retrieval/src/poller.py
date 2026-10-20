"""Background poller: fetches market data from Alpha Vantage and writes to DynamoDB."""
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
import httpx
from boto3.dynamodb.conditions import Key

from sec_edgar import fetch_recent_filings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

_AV_BASE_URL = "https://www.alphavantage.co/query"
_AV_MIN_INTERVAL = 1.0  # 1 call/sec → 60/min, safely under 75/min premium limit

_INDEX_TICKERS = ["SPY", "QQQ", "SOXX"]

# quotes mode: 1 call/ticker + 3 indices + 1 market_status
# At 1 call/sec, 1000 tickers = ~1004 sec (~17 min) — fits the hourly schedule and
# stays under the 20-min lock TTL. Tickers past the cap are dropped from the end
# of the company-name order; increase if the universe grows past it.
_MAX_QUOTES_TICKERS = 1000

_AWS_REGION = os.environ.get("AWS_REGION", "eu-north-1")

# Self URL — used to check if company_news pipeline is currently running
# On ECS this is the internal service discovery URL; locally it's localhost
_NEWS_RETRIEVAL_URL = os.environ.get("NEWS_RETRIEVAL_URL", "http://localhost:8000")

_LOCK_TABLE = os.environ.get("DYNAMODB_TABLE_LOCK", "ocn-market-lock")
_LOCK_TTL_SECONDS = 20 * 60  # 20 min — auto-expires if poller crashes mid-run

# DynamoDB table names
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


# ---------------------------------------------------------------------------
# DynamoDB helpers
# ---------------------------------------------------------------------------

def _dynamodb() -> "boto3.resource":
    return boto3.resource("dynamodb", region_name=_AWS_REGION)


def _table(name: str):
    return _dynamodb().Table(_TABLES[name])


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _last_trading_day() -> str:
    """Most recent US trading date (weekday) as of now, UTC. Ignores market holidays."""
    day = datetime.now(timezone.utc).date()
    while day.weekday() >= 5:  # Sat=5, Sun=6
        day -= timedelta(days=1)
    return day.isoformat()


def _ttl(days: int) -> int:
    return int(time.time()) + (days * 86400)


def _to_decimal(value: str) -> Decimal:
    """Convert string number to Decimal for DynamoDB (no floats allowed)."""
    try:
        return Decimal(str(value).rstrip("%").strip() or "0")
    except Exception:
        return Decimal("0")


# ---------------------------------------------------------------------------
# Distributed lock — prevents overlapping poller runs
# ---------------------------------------------------------------------------

def _acquire_lock(mode: str) -> bool:
    """Try to acquire the poll lock for the given mode.

    Uses DynamoDB conditional write — atomic across all ECS tasks.
    Returns True if lock acquired, False if another run is already active.
    """
    lock_key = f"poll-{mode}"
    client = boto3.client("dynamodb", region_name=_AWS_REGION)
    try:
        client.put_item(
            TableName=_LOCK_TABLE,
            Item={
                "lock_key": {"S": lock_key},
                "acquired_at": {"S": _now_iso()},
                "ttl": {"N": str(int(time.time()) + _LOCK_TTL_SECONDS)},
            },
            ConditionExpression="attribute_not_exists(lock_key)",
        )
        return True
    except client.exceptions.ConditionalCheckFailedException:
        return False
    except Exception as exc:
        logger.warning("[LOCK] acquire failed, proceeding without lock: %s", exc)
        return True  # fail open — better to double-poll than to permanently skip


def _release_lock(mode: str) -> None:
    """Delete the lock item after a successful poll run."""
    try:
        boto3.client("dynamodb", region_name=_AWS_REGION).delete_item(
            TableName=_LOCK_TABLE,
            Key={"lock_key": {"S": f"poll-{mode}"}},
        )
    except Exception as exc:
        logger.warning("[LOCK] release failed (TTL will expire it): %s", exc)


# ---------------------------------------------------------------------------
# Alpha Vantage fetch helpers
# ---------------------------------------------------------------------------

def _av_get(params: dict, av_key: str) -> dict:
    try:
        resp = httpx.get(
            _AV_BASE_URL,
            params={**params, "apikey": av_key},
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("[AV] request failed params=%s error=%s", params, exc)
        return {}


def _rate_sleep(last_call: list[float]) -> None:
    elapsed = time.monotonic() - last_call[0]
    if elapsed < _AV_MIN_INTERVAL:
        time.sleep(_AV_MIN_INTERVAL - elapsed)
    last_call[0] = time.monotonic()


# ---------------------------------------------------------------------------
# Company news guard — skip AV polling if company_news pipeline is running
# ---------------------------------------------------------------------------

def _company_news_running() -> bool:
    """Return True if a company_news pipeline run is currently active.

    Checks the runs API on this same service. Fails open (returns False)
    so a transient error never permanently blocks the poller.
    """
    try:
        resp = httpx.get(
            f"{_NEWS_RETRIEVAL_URL}/runs",
            params={"domain": "company_news", "limit": 1},
            timeout=5.0,
        )
        runs = resp.json().get("runs", [])
        if runs and runs[0].get("status") == "running":
            logger.info("[POLLER] company_news pipeline is running — skipping AV calls to avoid rate limit")
            return True
        return False
    except Exception as exc:
        logger.warning("[POLLER] could not check company_news status, proceeding: %s", exc)
        return False  # fail open


# ---------------------------------------------------------------------------
# Daily mode: OVERVIEW, EARNINGS, TIME_SERIES_DAILY_ADJUSTED
# ---------------------------------------------------------------------------

_ROC_TIME_PERIOD = 10  # trading days — short-term window, more reactive to recent news
_MOM_TIME_PERIOD = 10  # same window as ROC, so the two are directly comparable


def _fetch_technical_indicator(
    ticker: str, av_key: str, last_call: list[float], function: str, time_period: int,
) -> Decimal:
    """Latest value of a single-field AV technical indicator (ROC, MOM, ...). 0 on any failure."""
    _rate_sleep(last_call)
    data = _av_get(
        {
            "function": function,
            "symbol": ticker,
            "interval": "daily",
            "time_period": time_period,
            "series_type": "close",
        },
        av_key,
    )
    series = data.get(f"Technical Analysis: {function}", {})
    if not series:
        logger.warning("[AV] %s empty for %s", function, ticker)
        return Decimal("0")

    latest_date = max(series.keys())
    return _to_decimal(series[latest_date].get(function, "0"))


def _poll_overview(ticker: str, av_key: str, last_call: list[float]) -> None:
    _rate_sleep(last_call)
    data = _av_get({"function": "OVERVIEW", "symbol": ticker}, av_key)
    if not data or "MarketCapitalization" not in data:
        logger.warning("[AV] OVERVIEW empty for %s", ticker)
        return

    roc = _fetch_technical_indicator(ticker, av_key, last_call, "ROC", _ROC_TIME_PERIOD)
    mom = _fetch_technical_indicator(ticker, av_key, last_call, "MOM", _MOM_TIME_PERIOD)

    _table("overview").put_item(Item={
        "ticker": ticker,
        "recorded_at": _now_iso(),
        "name": data.get("Name", ""),
        "exchange": data.get("Exchange", ""),
        "market_cap": _to_decimal(data.get("MarketCapitalization", "0")),
        "pe_ratio": _to_decimal(data.get("PERatio", "0")),
        "week_52_high": _to_decimal(data.get("52WeekHigh", "0")),
        "week_52_low": _to_decimal(data.get("52WeekLow", "0")),
        "analyst_target": _to_decimal(data.get("AnalystTargetPrice", "0")),
        "beta": _to_decimal(data.get("Beta", "0")),
        "sector": data.get("Sector", ""),
        "revenue_ttm": _to_decimal(data.get("RevenueTTM", "0")),
        "shares_outstanding": _to_decimal(data.get("SharesOutstanding", "0")),
        "momentum_roc": roc,
        "momentum_mom": mom,
        "moving_avg_50day": _to_decimal(data.get("50DayMovingAverage", "0")),
        "moving_avg_200day": _to_decimal(data.get("200DayMovingAverage", "0")),
        "eps": _to_decimal(data.get("EPS", "0")),
        "forward_pe": _to_decimal(data.get("ForwardPE", "0")),
        "price_to_book": _to_decimal(data.get("PriceToBookRatio", "0")),
        "ev_to_ebitda": _to_decimal(data.get("EVToEBITDA", "0")),
        "profit_margin": _to_decimal(data.get("ProfitMargin", "0")),
        "operating_margin_ttm": _to_decimal(data.get("OperatingMarginTTM", "0")),
        "return_on_equity_ttm": _to_decimal(data.get("ReturnOnEquityTTM", "0")),
        "quarterly_earnings_growth_yoy": _to_decimal(data.get("QuarterlyEarningsGrowthYOY", "0")),
        "quarterly_revenue_growth_yoy": _to_decimal(data.get("QuarterlyRevenueGrowthYOY", "0")),
        "dividend_yield": _to_decimal(data.get("DividendYield", "0")),
        "dividend_per_share": _to_decimal(data.get("DividendPerShare", "0")),
        "ttl": _ttl(30),
    })
    logger.info(
        "[DYNAMODB] overview written ticker=%s momentum_roc=%s momentum_mom=%s",
        ticker, roc, mom,
    )


def _poll_earnings(ticker: str, av_key: str, last_call: list[float]) -> None:
    _rate_sleep(last_call)
    data = _av_get({"function": "EARNINGS", "symbol": ticker}, av_key)
    quarterly = data.get("quarterlyEarnings", [])
    if not quarterly:
        logger.warning("[AV] EARNINGS empty for %s", ticker)
        return

    def _safe_float(val) -> float:
        try:
            return float(val or 0)
        except (ValueError, TypeError):
            return 0.0

    last = quarterly[0]
    estimated = _safe_float(last.get("estimatedEPS"))
    reported = _safe_float(last.get("reportedEPS"))
    surprise_pct = (
        round((reported - estimated) / abs(estimated) * 100, 2)
        if estimated != 0 else 0.0
    )

    next_report = ""
    estimated_next = "0"
    for q in quarterly:
        if not q.get("reportedEPS"):
            next_report = q.get("fiscalDateEnding", "")
            estimated_next = q.get("estimatedEPS") or "0"
            break

    _table("earnings").put_item(Item={
        "ticker": ticker,
        "recorded_at": _now_iso(),
        "next_report_date": next_report or "unknown",
        "estimated_eps": _to_decimal(estimated_next),
        "last_surprise_pct": _to_decimal(str(surprise_pct)),
        "ttl": _ttl(30),
    })
    logger.info("[DYNAMODB] earnings written ticker=%s", ticker)


def _poll_price_history(ticker: str, av_key: str, last_call: list[float]) -> None:
    _rate_sleep(last_call)
    data = _av_get(
        {"function": "TIME_SERIES_DAILY_ADJUSTED", "symbol": ticker, "outputsize": "compact"},
        av_key,
    )
    series = data.get("Time Series (Daily)", {})
    if not series:
        logger.warning("[AV] TIME_SERIES_DAILY_ADJUSTED empty for %s", ticker)
        return

    dates = sorted(series.keys(), reverse=True)[:10]
    table = _table("price_history")
    with table.batch_writer() as batch:
        for date_str in dates:
            adj_close = series[date_str].get("5. adjusted close", "0")
            batch.put_item(Item={
                "ticker": ticker,
                "date": date_str,
                "adjusted_close": _to_decimal(adj_close),
                "ttl": _ttl(365),
            })
    logger.info("[DYNAMODB] price_history written ticker=%s days=%d", ticker, len(dates))


# ---------------------------------------------------------------------------
# Macro indicators — not ticker-keyed, fetched once per daily run
# ---------------------------------------------------------------------------

_MACRO_INDICATORS = [
    ("fed_funds_rate", "FEDERAL_FUNDS_RATE", {"interval": "monthly"}),
    ("cpi", "CPI", {"interval": "monthly"}),
    ("treasury_yield_10y", "TREASURY_YIELD", {"interval": "monthly", "maturity": "10year"}),
    ("unemployment", "UNEMPLOYMENT", {}),
    ("nonfarm_payroll", "NONFARM_PAYROLL", {}),
    ("real_gdp", "REAL_GDP", {"interval": "quarterly"}),
    ("retail_sales", "RETAIL_SALES", {}),
    ("durables", "DURABLES", {}),
]


def _poll_macro(av_key: str, last_call: list[float]) -> None:
    """Fetch economic indicators — one call each, written under their own
    `indicator` partition in ocn-market-macro. These update monthly/quarterly/
    at FOMC meetings, not per-ticker, so this runs once per daily poll, not
    once per ticker."""
    for indicator, function, params in _MACRO_INDICATORS:
        try:
            _rate_sleep(last_call)
            data = _av_get({"function": function, **params}, av_key)
            points = data.get("data", [])
            if not points:
                logger.warning("[AV] %s empty", function)
                continue
            latest = points[0]
            _table("macro").put_item(Item={
                "indicator": indicator,
                "recorded_at": _now_iso(),
                "date": latest.get("date", ""),
                "value": _to_decimal(latest.get("value", "0")),
                "unit": data.get("unit", ""),
                "ttl": _ttl(90),
            })
            logger.info("[DYNAMODB] macro written indicator=%s value=%s", indicator, latest.get("value"))
        except Exception as exc:
            logger.error("[POLLER] macro indicator=%s failed, skipping: %s", indicator, exc)

    try:
        _poll_top_movers(av_key, last_call)
    except Exception as exc:
        logger.error("[POLLER] top_movers failed, skipping: %s", exc)


def _poll_top_movers(av_key: str, last_call: list[float]) -> None:
    """Fetch top gainers/losers/most-actively-traded US tickers — one call,
    written as a single item (indicator=top_movers) since it's a snapshot
    list, not a single scalar reading like the other macro indicators."""
    _rate_sleep(last_call)
    data = _av_get({"function": "TOP_GAINERS_LOSERS"}, av_key)
    gainers = data.get("top_gainers", [])
    if not gainers:
        logger.warning("[AV] TOP_GAINERS_LOSERS empty")
        return

    def _shape(rows: list[dict]) -> list[dict]:
        return [
            {
                "ticker": row.get("ticker", ""),
                "price": _to_decimal(row.get("price", "0")),
                "change_amount": _to_decimal(row.get("change_amount", "0")),
                "change_percentage": _to_decimal(row.get("change_percentage", "0%")),
                "volume": _to_decimal(row.get("volume", "0")),
            }
            for row in rows
        ]

    _table("macro").put_item(Item={
        "indicator": "top_movers",
        "recorded_at": _now_iso(),
        "date": data.get("last_updated", ""),
        "top_gainers": _shape(gainers),
        "top_losers": _shape(data.get("top_losers", [])),
        "most_actively_traded": _shape(data.get("most_actively_traded", [])),
        "ttl": _ttl(90),
    })
    logger.info("[DYNAMODB] macro written indicator=top_movers")


def run_daily(tickers: list[str], av_key: str) -> None:
    """Poll macro indicators once, then OVERVIEW, EARNINGS, TIME_SERIES_DAILY_ADJUSTED per ticker."""
    if _company_news_running():
        return
    if not _acquire_lock("daily"):
        logger.info("[POLLER] daily already running — skipping this trigger")
        return
    try:
        logger.info("[POLLER] daily mode tickers=%d", len(tickers))
        last_call: list[float] = [0.0]
        try:
            _poll_macro(av_key, last_call)
        except Exception as exc:
            logger.error("[POLLER] macro poll failed, continuing to tickers: %s", exc)
        for ticker in tickers:
            logger.info("[POLLER] daily ticker=%s", ticker)
            try:
                _poll_overview(ticker, av_key, last_call)
            except Exception as exc:
                logger.error("[POLLER] daily ticker=%s overview failed, skipping: %s", ticker, exc)
            try:
                _poll_earnings(ticker, av_key, last_call)
            except Exception as exc:
                logger.error("[POLLER] daily ticker=%s earnings failed, skipping: %s", ticker, exc)
            try:
                _poll_price_history(ticker, av_key, last_call)
            except Exception as exc:
                logger.error("[POLLER] daily ticker=%s price_history failed, skipping: %s", ticker, exc)
        logger.info("[POLLER] daily complete")
    finally:
        _release_lock("daily")


# ---------------------------------------------------------------------------
# Quotes mode: GLOBAL_QUOTE per ticker + indices + MARKET_STATUS
# ---------------------------------------------------------------------------

def _poll_quote(ticker: str, av_key: str, last_call: list[float]) -> None:
    _rate_sleep(last_call)
    data = _av_get({"function": "GLOBAL_QUOTE", "symbol": ticker}, av_key)
    quote = data.get("Global Quote", {})
    if not quote:
        logger.warning("[AV] GLOBAL_QUOTE empty for %s", ticker)
        return

    trading_day = quote.get("07. latest trading day", "")
    stale = trading_day < _last_trading_day() if trading_day else False
    if stale:
        logger.warning(
            "[AV] GLOBAL_QUOTE stale for %s trading_day=%s expected=%s",
            ticker, trading_day, _last_trading_day(),
        )

    _table("quote").put_item(Item={
        "ticker": ticker,
        "recorded_at": _now_iso(),
        "price": _to_decimal(quote.get("05. price", "0")),
        "change": _to_decimal(quote.get("09. change", "0")),
        "change_percent": _to_decimal(quote.get("10. change percent", "0%")),
        "volume": _to_decimal(quote.get("06. volume", "0")),
        "previous_close": _to_decimal(quote.get("08. previous close", "0")),
        "latest_trading_day": trading_day,
        "stale": stale,
        "ttl": _ttl(4),
    })
    logger.info("[DYNAMODB] quote written ticker=%s stale=%s", ticker, stale)


def _poll_indices(av_key: str, last_call: list[float]) -> None:
    for ticker in _INDEX_TICKERS:
        try:
            _rate_sleep(last_call)
            data = _av_get({"function": "GLOBAL_QUOTE", "symbol": ticker}, av_key)
            quote = data.get("Global Quote", {})
            if not quote:
                logger.warning("[AV] GLOBAL_QUOTE empty for index %s", ticker)
                continue
            trading_day = quote.get("07. latest trading day", "")
            stale = trading_day < _last_trading_day() if trading_day else False
            _table("indices").put_item(Item={
                "ticker": ticker,
                "recorded_at": _now_iso(),
                "price": _to_decimal(quote.get("05. price", "0")),
                "change_percent": _to_decimal(quote.get("10. change percent", "0%")),
                "latest_trading_day": trading_day,
                "stale": stale,
                "ttl": _ttl(4),
            })
            logger.info("[DYNAMODB] indices written ticker=%s stale=%s", ticker, stale)
        except Exception as exc:
            logger.error("[POLLER] indices ticker=%s failed, skipping: %s", ticker, exc)


def _poll_market_status(av_key: str, last_call: list[float]) -> None:
    _rate_sleep(last_call)
    data = _av_get({"function": "MARKET_STATUS"}, av_key)
    markets = data.get("markets", [])
    us = next((m for m in markets if m.get("region") == "United States"), None)
    if not us:
        logger.warning("[AV] MARKET_STATUS no US market found")
        return
    _table("market_status").put_item(Item={
        "market": "US",
        "recorded_at": _now_iso(),
        "current_status": us.get("current_status", "unknown"),
        "local_open": us.get("local_open", ""),
        "local_close": us.get("local_close", ""),
        "ttl": _ttl(4),
    })
    logger.info("[DYNAMODB] market_status written")


def run_quotes(tickers: list[str], av_key: str) -> None:
    """Poll GLOBAL_QUOTE for each ticker + indices + MARKET_STATUS."""
    if _company_news_running():
        return
    if not _acquire_lock("quotes"):
        logger.info("[POLLER] quotes already running — skipping this trigger")
        return

    try:
        if len(tickers) > _MAX_QUOTES_TICKERS:
            logger.warning(
                "[POLLER] ticker list truncated %d → %d to stay within the lock TTL",
                len(tickers), _MAX_QUOTES_TICKERS,
            )
            tickers = tickers[:_MAX_QUOTES_TICKERS]

        logger.info("[POLLER] quotes mode tickers=%d", len(tickers))
        t0 = time.perf_counter()
        last_call: list[float] = [0.0]

        for ticker in tickers:
            try:
                _poll_quote(ticker, av_key, last_call)
            except Exception as exc:
                logger.error("[POLLER] quotes ticker=%s failed, skipping: %s", ticker, exc)

        try:
            _poll_indices(av_key, last_call)
        except Exception as exc:
            logger.error("[POLLER] indices poll failed, continuing: %s", exc)

        try:
            _poll_market_status(av_key, last_call)
        except Exception as exc:
            logger.error("[POLLER] market_status poll failed: %s", exc)

        elapsed = time.perf_counter() - t0
        logger.info("[POLLER] quotes complete elapsed=%.1fs", elapsed)
        if elapsed > _LOCK_TTL_SECONDS - 120:
            logger.warning(
                "[POLLER] quotes took %.1fs — approaching the 20-min lock TTL. "
                "Consider reducing ticker count or increasing poll interval.",
                elapsed,
            )
    finally:
        _release_lock("quotes")


# ---------------------------------------------------------------------------
# SEC filings mode: 8-K/10-Q/10-K/6-K/40-F/20-F per ticker, link + metadata only
# ---------------------------------------------------------------------------

_SEC_FILING_TTL_DAYS = 180


def _existing_accessions(ticker: str) -> set[str]:
    """Return the set of accession_numbers already stored for this ticker."""
    try:
        result = _table("sec_filings").query(
            KeyConditionExpression=Key("ticker").eq(ticker),
            ProjectionExpression="accession_number",
        )
        return {item["accession_number"] for item in result.get("Items", [])}
    except Exception as exc:
        logger.warning("[SEC_EDGAR] failed to read existing accessions ticker=%s: %s", ticker, exc)
        return set()


def run_sec_filings(tickers: list[str]) -> None:
    """Fetch new 8-K/10-Q/10-K/6-K/40-F/20-F filings per ticker and write metadata to DynamoDB.

    Only filings whose filed_at falls in the current calendar year are
    written - fetch_recent_filings() returns EDGAR's full recent-filings
    history per ticker (years back), but we only want a rolling current-year
    window in DynamoDB, not an ever-growing multi-year backlog. This check
    reads the year at call time, so it re-anchors automatically each January
    without any config or code change - a filing fetched in December stays
    filtered to that year; the same ticker's filings fetched in January are
    checked against the new year.
    """
    if not _acquire_lock("sec_filings"):
        logger.info("[POLLER] sec_filings already running — skipping this trigger")
        return

    try:
        logger.info("[POLLER] sec_filings mode tickers=%d", len(tickers))
        last_call: list[float] = [0.0]
        table = _table("sec_filings")
        written = 0
        skipped_prior_year = 0
        current_year = str(datetime.now(timezone.utc).year)

        for ticker in tickers:
            try:
                seen = _existing_accessions(ticker)
                filings = fetch_recent_filings(ticker, last_call)
                new_filings = [
                    f for f in filings
                    if f["accession_number"] not in seen and f.get("filed_at", "").startswith(current_year)
                ]
                skipped_prior_year += sum(
                    1 for f in filings
                    if f["accession_number"] not in seen and not f.get("filed_at", "").startswith(current_year)
                )
                for filing in new_filings:
                    table.put_item(Item={
                        "ticker": filing["ticker"],
                        "accession_number": filing["accession_number"],
                        "form_type": filing["form_type"],
                        "filed_at": filing["filed_at"],
                        "primary_doc_url": filing["primary_doc_url"],
                        "cik": filing.get("cik", ""),
                        "accepted_at": filing.get("accepted_at", ""),
                        "period_of_report": filing.get("period_of_report", ""),
                        "item_codes": filing.get("item_codes", []),
                        "filer_category": filing.get("filer_category", ""),
                        "recorded_at": _now_iso(),
                        "ttl": _ttl(_SEC_FILING_TTL_DAYS),
                    })
                    written += 1
                if new_filings:
                    logger.info(
                        "[DYNAMODB] sec_filings written ticker=%s new=%d", ticker, len(new_filings),
                    )
            except Exception as exc:
                logger.error("[POLLER] sec_filings ticker=%s failed, skipping: %s", ticker, exc)

        logger.info(
            "[POLLER] sec_filings complete written=%d skipped_prior_year=%d", written, skipped_prior_year,
        )
    finally:
        _release_lock("sec_filings")
