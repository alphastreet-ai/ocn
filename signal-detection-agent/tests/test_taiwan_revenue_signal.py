"""Taiwan monthly revenue signal: each filing judged against the company's
own history.

The fixture is real MOPS history (2023-01..2026-09), not invented numbers:
the September 2026 expectations below are the values the validated
backtest produced for the same filings, so a test passing here means the
production scorer reproduces the backtest.
"""
from __future__ import annotations

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))

import pipeline.taiwan_signal_classifier as tsc  # noqa: E402
from pipeline.taiwan_signal_classifier import (  # noqa: E402
    build_revenue_history,
    mark_repeated_remarks,
    score_revenue_filing,
    taiwan_month_index,
    taiwan_source_id,
    translate_taiwan_articles,
)

_FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "taiwan_revenue_history.json")
HISTORY = build_revenue_history(json.load(open(_FIXTURE))["rows"])
SEP_2026 = taiwan_month_index(2026, 9)


def _score(code: str, idx: int, history=None) -> dict:
    company = (history or HISTORY)[code]
    revenue, prior = company[idx]
    return score_revenue_filing(company, idx, revenue, prior)


@pytest.mark.parametrize("code,signal,score,mom,yoy_change", [
    ("2059", "HIGH", -5.85, -5.82, -4.36),   # King Slide: -31% MoM, YoY +344% -> +196%
    ("2382", "HIGH", -3.19, -1.94, -3.61),   # Quanta: YoY +177% -> +77%
    ("3231", "WEAK", -1.96, -1.31, -2.11),   # Wistron: YoY +166% -> +87%
    ("5274", "NOISE", -0.63, -0.35, -0.75),  # Aspeed: in line (was "High" under top-3 ranking)
])
def test_september_2026_matches_backtest(code, signal, score, mom, yoy_change) -> None:
    out = _score(code, SEP_2026)
    assert out["signal"] == signal
    assert out["reason"] == "scored"
    f = out["fields"]
    assert (f["revenue_deviation"], f["mom_deviation"], f["yoy_change_deviation"]) == (score, mom, yoy_change)


def test_display_fields_for_quanta_september() -> None:
    f = _score("2382", SEP_2026)["fields"]
    # August's YoY as filed: 423,970,932 vs 152,807,846 a year earlier.
    assert f["prior_yoy_pct"] == round((423970932 / 152807846 - 1) * 100, 2)
    assert f["is_12m_high"] is False  # August was higher


def test_direction_is_the_sign_of_the_score() -> None:
    assert _score("2059", SEP_2026)["fields"]["revenue_deviation"] < 0


def test_january_and_february_are_not_scored() -> None:
    for month in (1, 2):
        out = _score("2382", taiwan_month_index(2026, month))
        assert (out["signal"], out["reason"]) == ("NOISE", "not_scored_lunar_new_year")
        assert "revenue_deviation" not in out["fields"]


def test_short_history_is_not_scored_and_carries_no_score_keys() -> None:
    company = {i: v for i, v in HISTORY["2382"].items() if i > SEP_2026 - 10}
    revenue, prior = company[SEP_2026]
    out = score_revenue_filing(company, SEP_2026, revenue, prior)
    assert (out["signal"], out["reason"]) == ("NOISE", "not_scored_short_history")
    assert not {"revenue_deviation", "mom_deviation", "yoy_change_deviation"} & out["fields"].keys()


def test_flat_history_is_not_scored() -> None:
    company = {i: (100.0, 100.0) for i in range(SEP_2026 - 40, SEP_2026)}
    out = score_revenue_filing(company, SEP_2026, 130.0, 100.0)
    assert out["reason"] == "not_scored_flat_history"


@pytest.mark.parametrize("revenue,prior", [(None, 100.0), (0.0, 100.0), (100.0, None), (100.0, -5.0)])
def test_bad_values_are_not_scored(revenue, prior) -> None:
    out = score_revenue_filing(HISTORY["2382"], SEP_2026, revenue, prior)
    assert out["reason"] == "not_scored_bad_value"


def test_no_hindsight_later_months_do_not_change_a_score() -> None:
    company = dict(HISTORY["2059"])
    before = _score("2059", taiwan_month_index(2026, 6))["fields"]
    company[SEP_2026 + 1] = (1.0, 1.0)  # an extreme month AFTER the one judged
    company[SEP_2026 + 2] = (9e9, 1.0)
    revenue, prior = company[taiwan_month_index(2026, 6)]
    after = score_revenue_filing(company, taiwan_month_index(2026, 6), revenue, prior)["fields"]
    assert before == after


def test_yoy_uses_the_filings_own_prior_year_figure_not_our_older_row() -> None:
    # KYEC restated 2023-04..2024-03 by about -28%: the prior-year figure in
    # its 2024-04 filing is far below what it originally filed for 2023-04.
    kyec = HISTORY["2449"]
    apr_2024, apr_2023 = taiwan_month_index(2024, 4), taiwan_month_index(2023, 4)
    assert kyec[apr_2024][1] < kyec[apr_2023][0] * 0.8
    # Scoring May 2024 uses April's filed comparative, so prior_yoy_pct is
    # April's like-for-like YoY - not the false jump against the old figure.
    f = _score("2449", taiwan_month_index(2024, 5))["fields"]
    filed = (kyec[apr_2024][0] / kyec[apr_2024][1] - 1) * 100
    assert f["prior_yoy_pct"] == pytest.approx(filed, abs=0.01)


def _rev(ticker: str, period: str, remark: str | None) -> dict:
    return {"metadata": {"source_category": "mops_revenue", "ticker": ticker,
                         "period_gregorian": period, "remarks": remark}}


def test_repeated_remark_within_12_months_not_just_last_month() -> None:
    # Real case: TSMC used this sentence in April 2024 and again in December 2024.
    text = "因先進製程產品需求增加所致。"
    past = [{"ticker": "2330", "period_gregorian": "2024-04", "remarks": text},
            {"ticker": "2330", "period_gregorian": "2024-11", "remarks": "其他說明"}]
    dec = _rev("2330", "2024-12", text)
    mark_repeated_remarks([dec], past)
    assert dec["metadata"]["is_repeated_remark_12m"] is True


def test_remark_older_than_12_months_or_from_another_company_is_new() -> None:
    text = "接單暢旺"
    past = [{"ticker": "2382", "period_gregorian": "2025-09", "remarks": text},   # 13 months back
            {"ticker": "3231", "period_gregorian": "2026-08", "remarks": text}]   # other company
    a = _rev("2382", "2026-10", text)
    mark_repeated_remarks([a], past)
    assert a["metadata"]["is_repeated_remark_12m"] is False


def test_repeat_ignores_whitespace_and_counts_earlier_months_in_the_batch() -> None:
    aug, sep = _rev("2382", "2026-08", "產品 組合變更"), _rev("2382", "2026-09", "產品組合變更\n")
    mark_repeated_remarks([sep, aug])  # order in the batch must not matter
    assert aug["metadata"]["is_repeated_remark_12m"] is False
    assert sep["metadata"]["is_repeated_remark_12m"] is True


@pytest.mark.parametrize("remark", [None, "", "-", "無", "無。"])
def test_no_explanation_carries_no_repeat_flag(remark) -> None:
    a = _rev("2382", "2026-09", remark)
    mark_repeated_remarks([a], [{"ticker": "2382", "period_gregorian": "2026-08", "remarks": remark}])
    assert "is_repeated_remark_12m" not in a["metadata"]


def test_taiwan_source_id_is_known_before_classifying() -> None:
    assert taiwan_source_id({"metadata": {"source_category": "mops_revenue", "ticker": "2382",
                                          "period_gregorian": "2026-09"}}) == "2382-2026-09"
    assert taiwan_source_id({"url": "https://x", "metadata": {"source_category": "gdelt"}}) == "https://x"


def test_each_distinct_remark_is_translated_once(monkeypatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr(tsc, "_translate_one", lambda text, *a: calls.append(text) or f"EN:{text}")
    rev = lambda remark: {"metadata": {"source_category": "mops_revenue", "remarks": remark}}
    articles = [rev("主要係客戶需求強勁"), rev("主要係客戶需求強勁"), rev("產品組合變更"), rev("接單量增加")]
    translate_taiwan_articles(articles, known_remark_translations={"產品組合變更": "Product mix change"},
                              model="m", api_key="k", base_url="u", timeout=1)
    assert sorted(calls) == ["主要係客戶需求強勁", "接單量增加"]  # one call per new text
    assert [a["metadata"]["translated_remarks"] for a in articles] == [
        "EN:主要係客戶需求強勁", "EN:主要係客戶需求強勁", "Product mix change", "EN:接單量增加"]



# --- material announcements: clause meaning per exchange ---------------------

from pipeline.taiwan_signal_classifier import classify_material_announcements, clause_label  # noqa: E402


@pytest.mark.parametrize("exchange,code,label", [
    ("TWSE", "第20款", "Asset purchase or sale"),
    ("TWSE", "第51款", "Other material decision or event"),   # TWSE catch-all
    ("TWSE", "第35款", "Share buyback"),
    ("TWSE", "第22款", "Endorsement or guarantee"),
    ("TWSE", "第23款", "Loan of funds to others"),
    ("TPEx", "第53款", "Other material decision or event"),   # TPEx catch-all is 53, not 51
    ("TPEx", "第48款", "Overseas financial report reconciliation"),  # = TWSE 47
    ("TPEx", "第47款", "Withdrawn application to move to TWSE"),  # TPEx only
    ("TPEx", "第51款", "Control of an important subsidiary lost or stake cut"),  # = TWSE 49
])
def test_clause_label_uses_each_exchanges_own_numbering(exchange, code, label) -> None:
    assert clause_label(exchange, code) == label


@pytest.mark.parametrize("exchange,code", [
    ("TWSE", "第39款"),   # deleted clause
    ("TWSE", "第53款"),   # TWSE has only 51
    ("TPEx", "第42款"),   # not confirmed for TPEx yet
    ("TWSE", None), (None, "第20款"),
])
def test_unknown_clause_gets_no_label(exchange, code) -> None:
    assert clause_label(exchange, code) is None


def test_material_announcement_stores_clause_label_only_when_known() -> None:
    known = {"metadata": {"source_category": "mops_material", "exchange": "TPEx",
                          "disclosure_clause_code": "第53款", "statement_date": "2026-10-15"}}
    unknown = {"metadata": {"source_category": "mops_material", "exchange": "TPEx",
                            "disclosure_clause_code": "第42款", "statement_date": "2026-10-15"}}
    classify_material_announcements([known, unknown])
    assert known["metadata"]["clause_label"] == "Other material decision or event"
    assert "clause_label" not in unknown["metadata"]


# --- signal_reason: a plain sentence from the stored numbers -------------------

from pipeline.taiwan_signal_classifier import revenue_reason  # noqa: E402

_KING_SLIDE = {"translated_company_name": "King Slide", "period_gregorian": "2026-09", "mom_pct": -30.957,
               "yoy_pct": 196.22, "prior_yoy_pct": 344.42, "revenue_deviation": -5.85, "mom_deviation": -5.82,
               "yoy_change_deviation": -4.36, "is_12m_high": False}


def test_revenue_reason_signal_down() -> None:
    assert revenue_reason(_KING_SLIDE, "HIGH", "scored") == (
        "2026-09 revenue fell 31.0% from 2026-08; year on year it was +196.2%, down from +344.4% in 2026-08. "
        "That is far outside King Slide's usual month-to-month swings: a strong downward break from its own pattern.")


def test_revenue_reason_weak_up_driven_by_growth_rate_and_12m_high() -> None:
    meta = {"translated_company_name": "Wiwynn", "period_gregorian": "2026-01", "mom_pct": 4.0, "yoy_pct": 100.0,
            "prior_yoy_pct": 50.4, "revenue_deviation": 1.73, "mom_deviation": 1.15, "yoy_change_deviation": 1.86,
            "is_12m_high": True}
    assert revenue_reason(meta, "WEAK", "scored") == (
        "2026-01 revenue rose 4.0% from 2025-12; year on year it was +100.0%, up from +50.4% in 2025-12. "
        "That is outside Wiwynn's usual changes in its growth rate: a moderate upward break from its own pattern. "
        "It is also the highest monthly revenue of the past 12 months.")


def test_revenue_reason_noise_and_not_scored() -> None:
    noise = {**_KING_SLIDE, "translated_company_name": "TSMC", "revenue_deviation": -0.13}
    assert revenue_reason(noise, "NOISE", "scored").endswith("That is within TSMC's usual range.")
    for code, start in (("not_scored_lunar_new_year", "Not scored: January and February revenue shifts"),
                        ("not_scored_short_history", "Not scored: King Slide has fewer than 24 months"),
                        ("not_scored_flat_history", "Not scored: King Slide's past monthly changes are too uniform"),
                        ("not_scored_bad_value", "Not scored: the 2026-09 revenue figure")):
        assert revenue_reason(_KING_SLIDE, "NOISE", code).startswith(start)


def test_revenue_reason_possessive_for_names_ending_in_s() -> None:
    meta = {**_KING_SLIDE, "translated_company_name": "Delta Electronics", "revenue_deviation": 0.1}
    assert revenue_reason(meta, "NOISE", "scored").endswith("That is within Delta Electronics' usual range.")


def test_customers_from_news_retrieval_are_stored_unchanged(monkeypatch) -> None:
    # metadata.customers is set by news-retrieval from TAIWAN_TICKER_UNIVERSE
    # (the one source of truth); the agent stores it as received.
    monkeypatch.setattr(tsc, "translate_taiwan_articles", lambda *a, **k: None)
    monkeypatch.setattr(tsc, "classify_gdelt_articles", lambda articles, model=None: [])
    customers = [{"name": "NVIDIA", "relationship": "US test capacity reserved for NVIDIA"}]
    rev = {"url": "twse-revenue://2449/2026-09", "metadata": {
        "source_category": "mops_revenue", "ticker": "2449", "period_gregorian": "2026-09",
        "revenue_current_month": 100, "revenue_prior_year_month": 90, "customers": customers}}
    results = tsc.classify_taiwan_signal_batch([rev], model="m", revenue_history={})
    assert results[0]["result"]["metadata"]["customers"] == customers
