"""Verified customers in TAIWAN_TICKER_UNIVERSE and how they reach Taiwan
article metadata."""
from pipeline import _taiwan_customers
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


def test_customers_are_read_from_the_universe() -> None:
    kyec = _taiwan_customers("2449")
    assert kyec[0] == {"name": "NVIDIA", "relationship":
                       "Customer: prepaid for US test capacity the company reserves for it (MOPS filing 2026-10-13)"}
    assert _taiwan_customers("3017") == []                         # Asia Vital Components: none verified
    assert _taiwan_customers("1101") is None                       # not tracked
