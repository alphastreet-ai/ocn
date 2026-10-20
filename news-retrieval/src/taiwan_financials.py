"""Monthly revenue history for the Taiwan universe.

signal-detection-agent judges each monthly revenue filing against the
company's OWN past 36 months, so it needs a stored series. The TWSE/TPEx
OpenAPI feeds the daily fetch reads only ever return the latest month,
with no period parameter - on their own they can never build a history,
and a missed poll is lost for good.

MOPS (the exchanges' official disclosure site) publishes every past month
as one CSV per market, holding every listed company. The CSV carries the
same columns as the OpenAPI feeds (confirmed against Quanta 2026-09 from
ECS: 325,299,509 / 184,108,513 in both), values in NT$ thousands.

TWO WRITERS, ONE TABLE
  daily fetch     revenue_rows_from_articles() - the OpenAPI rows the
                  taiwan_market_signal run already fetched
  MOPS CSV        fetch_month() - the one-time backfill, and a monthly
                  re-read of recent months that recovers any filing the
                  daily poll missed and picks up restatements

Both produce identical rows for company_financials. `prior_revenue` is
the same-month-last-year figure AS PRINTED IN THAT FILING, never our own
row from twelve months earlier: companies restate (KYEC restated a full
year by about -28% in 2023-24), and only the filing's own comparative is
like-for-like. Last month's revenue is not stored - it is the previous
month's row (0 of 1,840 prior-month figures differed from what was filed).
"""
import csv
import io
import logging
import time
from typing import Any

import httpx

logger = logging.getLogger(__name__)

DOMAIN = "taiwan_market_signal"

_MOPS_CSV_URL = "https://mopsov.twse.com.tw/server-java/FileDownLoad"
# sii = TWSE-listed, otc = TPEx-listed. Each file includes both domestic
# and foreign (-KY) companies.
_MOPS_MARKETS = {"TWSE": "sii", "TPEx": "otc"}
_MOPS_HEADERS = {"User-Agent": "Mozilla/5.0"}
# MOPS throttles bursts; one request per ~0.7s held for 186 files with
# no refusal when the backfill was first run.
_MOPS_PACING_SECS = 0.7
_MOPS_RETRIES = 3


def _to_int(value: str | None) -> int | None:
    if value is None:
        return None
    value = value.strip().replace(",", "")
    if not value or value == "-":
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _row(code: str, company: str | None, year: int, month: int,
         revenue: int | None, prior_revenue: int | None, source: str) -> dict[str, Any]:
    """One company_financials row, in the shape upsert_financials takes."""
    return {
        "code": code,
        "company": company,
        "metric": "revenue",
        "period_type": f"M{month:02d}",
        "year": year,
        "revenue": revenue,
        "prior_revenue": prior_revenue,
        "yoy_pct": None,  # derivable from revenue / prior_revenue - not stored
        "source": source,
    }


def parse_mops_csv(text: str, names: dict[str, str]) -> list[dict[str, Any]]:
    """Rows for the tracked tickers in one MOPS monthly revenue CSV.

    `names` maps ticker -> English company name; tickers absent from it
    are skipped. The period comes from each row's own 資料年月 (ROC
    year/month, e.g. "115/9"), not from the request, so a file served for
    the wrong month can never be stored under the requested one.
    """
    rows: list[dict[str, Any]] = []
    for r in csv.DictReader(io.StringIO(text.lstrip("﻿"))):
        code = (r.get("公司代號") or "").strip()
        if code not in names:
            continue
        try:
            roc_year, month = (int(x) for x in (r.get("資料年月") or "").split("/"))
        except ValueError:
            logger.warning("[TW_FIN] unparseable period %r for %s", r.get("資料年月"), code)
            continue
        rows.append(_row(
            code, names[code], roc_year + 1911, month,
            _to_int(r.get("營業收入-當月營收")),
            _to_int(r.get("營業收入-去年當月營收")),
            "mops_csv",
        ))
    return rows


def fetch_month(exchange: str, year: int, month: int,
                names: dict[str, str]) -> list[dict[str, Any]] | None:
    """Download and parse one MOPS monthly CSV. None if it could not be
    fetched (network failure, or a throttle/error page instead of a CSV) -
    distinct from [] (fetched, but no tracked company in it)."""
    market = _MOPS_MARKETS[exchange]
    data = {
        "step": "9",
        "functionName": "show_file2",
        "filePath": f"/t21/{market}/",
        "fileName": f"t21sc03_{year - 1911}_{month}.csv",
    }
    for attempt in range(1, _MOPS_RETRIES + 1):
        try:
            resp = httpx.post(_MOPS_CSV_URL, data=data, headers=_MOPS_HEADERS, timeout=40.0)
            resp.raise_for_status()
            text = resp.content.decode("utf-8-sig", errors="replace")
            # A throttled or missing file comes back as a 200 HTML page,
            # so the header row is the only reliable success check.
            if "公司代號" in text and "資料年月" in text:
                return parse_mops_csv(text, names)
            logger.warning("[TW_FIN] %s %d-%02d: response is not a revenue CSV (attempt %d)",
                           exchange, year, month, attempt)
        except Exception as exc:
            logger.warning("[TW_FIN] %s %d-%02d failed (attempt %d): %s",
                           exchange, year, month, attempt, exc)
        time.sleep(_MOPS_PACING_SECS * 5 * attempt)
    return None


def fetch_range(start: tuple[int, int], end: tuple[int, int],
                universe: list[dict[str, str]]) -> tuple[list[dict[str, Any]], list[str]]:
    """Fetch every month from `start` to `end` inclusive ((year, month)),
    for each exchange that has a tracked company.

    Returns (rows, failed) - `failed` names each exchange-month that could
    not be fetched, so a caller can say so rather than store a silent gap.
    """
    by_exchange: dict[str, dict[str, str]] = {}
    for c in universe:
        by_exchange.setdefault(c["exchange"], {})[c["ticker"]] = c["company"]

    rows: list[dict[str, Any]] = []
    failed: list[str] = []
    y, m = start
    while (y, m) <= end:
        for exchange, names in by_exchange.items():
            got = fetch_month(exchange, y, m, names)
            if got is None:
                failed.append(f"{exchange} {y}-{m:02d}")
            else:
                rows.extend(got)
            time.sleep(_MOPS_PACING_SECS)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return rows, failed


def revenue_rows_from_articles(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """company_financials rows from the daily fetch's mops_revenue articles
    (TWSE/TPEx OpenAPI), so the stored history stays current between
    MOPS re-reads without a separate request."""
    rows: list[dict[str, Any]] = []
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "mops_revenue":
            continue
        period = meta.get("period_gregorian") or ""
        try:
            year, month = (int(x) for x in period.split("-"))
        except ValueError:
            continue
        rows.append(_row(
            meta.get("ticker"), meta.get("translated_company_name"), year, month,
            meta.get("revenue_current_month"), meta.get("revenue_prior_year_month"),
            f"{(meta.get('exchange') or '').lower()}_openapi",
        ))
    return rows
