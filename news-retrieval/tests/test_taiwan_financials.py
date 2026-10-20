"""Taiwan monthly revenue history: MOPS CSV parsing, daily-feed rows,
storage round trip and the read endpoint."""
from httpx import AsyncClient

from db import get_db
from models.company_financials import get_financials, upsert_financials
from taiwan_financials import DOMAIN, parse_mops_csv, revenue_rows_from_articles

# Real rows from MOPS t21sc03_115_9.csv (2026-09) plus one untracked company.
_CSV = (
    "﻿出表日期,資料年月,公司代號,公司名稱,產業別,營業收入-當月營收,營業收入-上月營收,"
    "營業收入-去年當月營收,營業收入-上月比較增減(%),營業收入-去年同月增減(%),"
    "累計營業收入-當月累計營收,累計營業收入-去年累計營收,累計營業收入-前期比較增減(%),備註\n"
    '"115/10/17","115/9","2382","廣達","電腦及週邊設備業","325299509","423970932","184108513",'
    '"-23.27","76.68","2961358288","1485052023","99.41","產品組合變更"\n'
    '"115/10/17","115/9","1101","台泥","水泥工業","8735157","9379408","10404901",'
    '"-6.86","-16.04","80942003","79341961","2.01","-"\n'
    '"115/10/17","115/9","2449","京元電子","半導體業","3000000","2900000","-",'
    '"3.4","","1","1","","-"\n'
)

_NAMES = {"2382": "Quanta", "2449": "King Yuan Electronics"}


def test_parse_mops_csv_keeps_tracked_tickers_with_filing_own_prior_year() -> None:
    rows = parse_mops_csv(_CSV, _NAMES)
    assert [r["code"] for r in rows] == ["2382", "2449"]
    quanta = rows[0]
    assert quanta == {
        "code": "2382", "company": "Quanta", "metric": "revenue",
        "period_type": "M09", "year": 2026,
        "revenue": 325299509, "prior_revenue": 184108513,
        "yoy_pct": None, "source": "mops_csv",
    }
    # A blank/"-" prior-year figure (e.g. newly listed) is None, not 0.
    assert rows[1]["prior_revenue"] is None


def test_parse_mops_csv_skips_unparseable_period() -> None:
    bad = _CSV.replace('"115/9","2382"', '"bad","2382"')
    assert [r["code"] for r in parse_mops_csv(bad, _NAMES)] == ["2449"]


def test_revenue_rows_from_articles_maps_openapi_rows_only() -> None:
    articles = [
        {"metadata": {"source_category": "mops_revenue", "ticker": "2382",
                      "translated_company_name": "Quanta", "period_gregorian": "2026-09",
                      "revenue_current_month": 325299509,
                      "revenue_prior_year_month": 184108513, "exchange": "TWSE"}},
        {"metadata": {"source_category": "mops_material", "ticker": "2382"}},
    ]
    assert revenue_rows_from_articles(articles) == [{
        "code": "2382", "company": "Quanta", "metric": "revenue",
        "period_type": "M09", "year": 2026,
        "revenue": 325299509, "prior_revenue": 184108513,
        "yoy_pct": None, "source": "twse_openapi",
    }]


def _clear() -> None:
    with get_db() as conn:
        conn.execute("DELETE FROM company_financials WHERE domain = %s", (DOMAIN,))


def test_upsert_replaces_a_restated_month() -> None:
    _clear()
    rows = parse_mops_csv(_CSV, _NAMES)
    upsert_financials(DOMAIN, rows)
    restated = [{**rows[0], "prior_revenue": 150000000, "source": "twse_openapi"}]
    upsert_financials(DOMAIN, restated)
    stored = {r["code"]: r for r in get_financials(DOMAIN)}
    assert len(stored) == 2
    assert stored["2382"]["prior_revenue"] == 150000000.0
    assert stored["2382"]["source"] == "twse_openapi"
    _clear()


async def test_taiwan_revenue_history_endpoint(client: AsyncClient) -> None:
    _clear()
    upsert_financials(DOMAIN, parse_mops_csv(_CSV, _NAMES))
    resp = await client.get("/market/taiwan/revenue-history")
    assert resp.status_code == 200
    rows = resp.json()["rows"]
    quanta = next(r for r in rows if r["code"] == "2382")
    assert (quanta["period_type"], quanta["year"], quanta["revenue"], quanta["prior_revenue"]) == (
        "M09", "2026", 325299509.0, 184108513.0)
    assert quanta["yoy_pct"] is None
    _clear()
