"""Verified customers in TAIWAN_TICKER_UNIVERSE and how they are served
(GET /market/taiwan/universe, not article metadata)."""
from routes.market import taiwan_universe
from seed import TAIWAN_TICKER_UNIVERSE


def test_every_company_has_a_customers_list_of_name_and_relationship() -> None:
    assert len(TAIWAN_TICKER_UNIVERSE) == 20
    for t in TAIWAN_TICKER_UNIVERSE:
        assert isinstance(t["customers"], list), t["ticker"]       # [] = checked, none verified
        names = [c["name"] for c in t["customers"]]
        assert len(names) == len(set(names)), t["ticker"]
        for c in t["customers"]:
            assert set(c) == {"name", "relationship"}
            assert c["name"].strip() and c["relationship"].startswith(("Customer: ", "Customer (distributor): "))


def test_universe_serves_every_company_with_its_customers() -> None:
    companies = {c["ticker"]: c for c in taiwan_universe()["companies"]}
    assert len(companies) == 20
    for c in companies.values():
        assert set(c) == {"ticker", "company_name", "translated_company_name", "customers"}
    kyec = companies["2449"]
    assert kyec["company_name"] == "京元電子" and kyec["translated_company_name"] == "King Yuan Electronics"
    assert kyec["customers"][0] == {"name": "NVIDIA", "relationship":
                                    "Customer: prepaid for US test capacity the company reserves for it (MOPS filing 2026-10-13)"}
    assert companies["3017"]["customers"] == []                    # Asia Vital Components: none verified
