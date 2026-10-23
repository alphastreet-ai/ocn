"""Customers in TAIWAN_TICKER_UNIVERSE and how they are served
(GET /market/taiwan/universe, not article metadata)."""
from routes.market import taiwan_universe
from seed import TAIWAN_TICKER_UNIVERSE

_CUSTOMER_KEYS = {"name", "relationship", "status", "source_type", "source", "source_date", "source_url"}
_STATUSES = {"verified", "partial", "unverified"}
_SOURCE_TYPES = {
    "sec_filing", "supplier_list", "press_release", "company_filing", "annual_report", "earnings_call",
    "code_commit", "product_datasheet", "company_website", "company_statement", "customs_record", "news", "none",
}
_SEGMENTS = {
    "Foundry", "Packaging & test", "Server ODM / EMS", "Power & components", "PCB & materials",
    "Thermal", "Connectors", "Server rails", "Server chassis", "ASIC design", "BMC chips",
}
_PREFIXES = ("Customer: ", "Customer (distributor): ", "Customer and investor: ", "Partner: ", "Reported: ")


def test_every_customer_carries_its_evidence() -> None:
    assert len(TAIWAN_TICKER_UNIVERSE) == 20
    for t in TAIWAN_TICKER_UNIVERSE:
        assert t["segment"] in _SEGMENTS, t["ticker"]
        names = [c["name"] for c in t["customers"]]
        assert len(names) == len(set(names)), t["ticker"]
        for c in t["customers"]:
            where = (t["ticker"], c["name"])
            assert set(c) == _CUSTOMER_KEYS, where
            assert c["status"] in _STATUSES and c["source_type"] in _SOURCE_TYPES, where
            assert c["name"].strip() and c["relationship"].startswith(_PREFIXES), where
            if c["status"] == "verified":
                # a verified customer must be checkable: a real source, a link and a date
                assert c["source_type"] not in ("news", "none", "customs_record"), where
                assert c["source_url"].startswith("https://") and c["source_date"], where


def test_universe_serves_every_company_with_its_customers() -> None:
    companies = {c["ticker"]: c for c in taiwan_universe()["companies"]}
    assert len(companies) == 20
    for c in companies.values():
        assert set(c) == {"ticker", "company_name", "translated_company_name", "segment", "customers"}
    kyec = companies["2449"]
    assert kyec["company_name"] == "京元電子" and kyec["translated_company_name"] == "King Yuan Electronics"
    assert kyec["segment"] == "Packaging & test"
    nvidia = next(x for x in kyec["customers"] if x["name"] == "NVIDIA")
    assert nvidia["status"] == "verified" and nvidia["source_url"].startswith("https://")
