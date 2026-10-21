"""middle_east_market_signal classifier, tested one build step at a time.

Step 1: pooling, the already-judged skip, and the insert. Unit tests fake
news-retrieval and the DB lookups; the round-trip test at the bottom uses
a real Postgres when one is reachable (skipped otherwise, like
test_results_filters_integration)."""
from __future__ import annotations

import asyncio
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))

import controllers.run as run  # noqa: E402
from pipeline.middle_east_signal_classifier import (  # noqa: E402
    describe_pool,
    source_name,
)


def _article(url, source_type=None, er_query=None, **extra):
    metadata = {}
    if source_type:
        metadata["source_type"] = source_type
    if er_query:
        metadata["er_query"] = er_query
    return {"url": url, "title": f"t {url}", "metadata": metadata, **extra}


# --- Step 1: source naming ---------------------------------------------------

@pytest.mark.parametrize("article, expected", [
    (_article("https://www.wam.ae/en/article/x", "wam_wire"), "wam_wire"),
    (_article("https://www.spa.gov.sa/en/N1", "spa_wire"), "spa_wire"),
    (_article("https://www.middleeastainews.com/p/x", "substack_post"), "substack_post"),
    (_article("https://www.bis.gov/press-release/x", "bis_press"), "bis_press"),
    (_article("sec-stake://1704268/0001-26-1", "sec_fund_stake"), "sec_fund_stake"),
    # Each newsapi.ai query is its own source - routing needs WHICH query.
    (_article("er-me://1", "eventregistry", "eventregistry:me_shipping"), "eventregistry:me_shipping"),
    (_article("er-me://2", "eventregistry", "eventregistry:me_helium"), "eventregistry:me_helium"),
    # The Federal Register copy carries no source_type: told by its url scheme.
    (_article("me-federal-register://2026-14132"), "federal_register"),
    (_article("https://example.com/x"), "unknown"),
    ({"url": "x", "metadata": None}, "unknown"),
])
def test_source_name(article, expected) -> None:
    assert source_name(article) == expected


def test_describe_pool_counts_per_source() -> None:
    pool = [_article("er-me://1", "eventregistry", "eventregistry:me_shipping"),
            _article("er-me://2", "eventregistry", "eventregistry:me_shipping"),
            _article("https://www.wam.ae/en/article/x", "wam_wire")]
    assert describe_pool(pool) == {"eventregistry:me_shipping": 2, "wam_wire": 1}


# --- Step 1: pooling and the already-judged skip -------------------------------

def _fake_news(monkeypatch, runs, judged=frozenset()):
    async def list_completed_runs(domain, from_date, to_date):
        assert domain == "middle_east_market_signal"
        return list(runs)

    async def get_run_articles(run_id):
        return [dict(a) for a in runs[run_id]]

    monkeypatch.setattr(run, "list_completed_runs", list_completed_runs)
    monkeypatch.setattr(run, "get_run_articles", get_run_articles)
    monkeypatch.setattr(run, "get_existing_middle_east_signal_source_ids",
                        lambda urls: {u for u in urls if u in judged})


def test_pool_dedups_across_runs_counts_missing_url_and_skips_judged(monkeypatch) -> None:
    runs = {
        # The 05:00 and 13:00 fetches overlap: er-me://1 is in both.
        10: [_article("er-me://1", "eventregistry", "eventregistry:me_shipping"),
             _article("https://www.wam.ae/en/article/a", "wam_wire")],
        11: [_article("er-me://1", "eventregistry", "eventregistry:me_shipping"),
             _article("sec-stake://1/acc", "sec_fund_stake"),
             {"url": None, "title": "no url", "metadata": {}}],
    }
    _fake_news(monkeypatch, runs, judged={"https://www.wam.ae/en/article/a"})
    pool = asyncio.run(run.pool_middle_east_articles("2026-10-20", "2026-10-21"))
    assert pool["run_ids"] == [10, 11]
    assert [a["url"] for a in pool["pooled"]] == [
        "er-me://1", "https://www.wam.ae/en/article/a", "sec-stake://1/acc"]
    assert pool["already_judged"] == 1
    assert [a["url"] for a in pool["new"]] == ["er-me://1", "sec-stake://1/acc"]
    assert pool["no_url"] == 1


def _fake_table(monkeypatch):
    async def table():
        return _news_retrieval_company_table()
    monkeypatch.setattr(run, "fetch_middle_east_universe", table)


def test_run_stores_nothing_until_the_classifier_judges(monkeypatch) -> None:
    """Step 1 has no classifier yet: nothing may be stored, or the
    already-judged skip would hide those articles from every later run."""
    _fake_news(monkeypatch, {10: [_article("er-me://1", "eventregistry", "eventregistry:me_shipping")]})
    _fake_table(monkeypatch)
    statuses, inserted = [], []
    monkeypatch.setattr(run, "update_job_status", lambda *a, **k: statuses.append((a, k)))
    monkeypatch.setattr(run, "insert_middle_east_signal_classification",
                        lambda *a: inserted.append(a))
    asyncio.run(run.run_middle_east_signal_classification(1, "2026-10-20", "2026-10-21"))
    assert inserted == []
    assert statuses[-1][0][1] == "completed" and statuses[-1][1]["article_count"] == 0


def test_run_drops_rows_an_overlapping_pass_stored_meanwhile(monkeypatch) -> None:
    _fake_news(monkeypatch, {10: [_article("er-me://1", "eventregistry", "eventregistry:me_shipping"),
                                  _article("er-me://2", "eventregistry", "eventregistry:me_shipping")]})
    # The pre-filter saw neither as judged; by insert time the other pass
    # has stored er-me://2.
    calls = []

    def existing(urls):
        calls.append(list(urls))
        return set() if len(calls) == 1 else {"er-me://2"}

    monkeypatch.setattr(run, "get_existing_middle_east_signal_source_ids", existing)
    _fake_table(monkeypatch)
    monkeypatch.setattr(run, "classify_middle_east_signal_batch", lambda arts, companies: [
        {"article": a, "result": {"signal": "noise", "reason": "r"}} for a in arts])
    monkeypatch.setattr(run, "update_job_status", lambda *a, **k: None)
    inserted = []
    monkeypatch.setattr(run, "insert_middle_east_signal_classification",
                        lambda job_id, article, result: inserted.append(article["url"]))
    asyncio.run(run.run_middle_east_signal_classification(1, "2026-10-20", "2026-10-21"))
    assert inserted == ["er-me://1"]


def test_news_retrieval_down_fails_the_job_and_stores_nothing(monkeypatch) -> None:
    async def broken(*a):
        raise run.NewsRetrievalError("down")
    monkeypatch.setattr(run, "list_completed_runs", broken)
    statuses = []
    monkeypatch.setattr(run, "update_job_status", lambda *a, **k: statuses.append(a[1]))
    asyncio.run(run.run_middle_east_signal_classification(1, "2026-10-20", "2026-10-21"))
    assert statuses == ["running", "failed"]


# --- Step 1: the insert against a real Postgres --------------------------------

try:
    from db import get_db, init_db
    from models.jobs import (
        create_job,
        get_existing_middle_east_signal_source_ids,
        insert_middle_east_signal_classification,
    )
    with get_db() as _conn:
        _conn.execute("SELECT 1")
    _DB = True
except Exception:
    _DB = False


@pytest.mark.skipif(not _DB, reason="requires a reachable Postgres")
def test_insert_round_trip_and_unique_source_id() -> None:
    init_db()
    url = "er-me://test_middle_east_signal_round_trip"
    with get_db() as conn:
        conn.execute("DELETE FROM agent_classifications WHERE source_id = %s", (url,))
    job_id = create_job(domain="middle_east_market_signal")
    article = {"url": url, "title": "Tanker hits mine in Hormuz",
               "published": "2026-10-19T08:31:00+00:00"}
    result = {"signal": "signal", "reason": "Confirmed strike on a ship.",
              "entities": [{"name": "QatarEnergy", "type": "company"}],
              "metadata": {"m_type": "M4"}}
    try:
        insert_middle_east_signal_classification(job_id, article, result)
        # An overlapping pass inserting the same url stores nothing twice.
        insert_middle_east_signal_classification(job_id, article, result)
        assert get_existing_middle_east_signal_source_ids([url, "er-me://other"]) == {url}
        with get_db() as conn:
            rows = conn.execute(
                "SELECT source_type, signal_detection, signal_score, signal_reason,"
                " entity_names_normalized, metadata FROM agent_classifications"
                " WHERE source_id = %s", (url,)).fetchall()
        assert len(rows) == 1
        row = rows[0]
        assert row["source_type"] == "middle_east_market_signal"
        assert row["signal_detection"] == "signal" and row["signal_score"] is None
        assert row["entity_names_normalized"] == ["qatarenergy"]
        metadata = row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"])
        assert metadata == {"m_type": "M4"}
    finally:
        with get_db() as conn:
            conn.execute("DELETE FROM agent_classifications WHERE source_id = %s", (url,))


# --- Step 2: reference tables --------------------------------------------------

import importlib.util  # noqa: E402
import types  # noqa: E402

from pipeline.middle_east_signal_classifier import (  # noqa: E402
    MIDDLE_EAST_EVENT_TYPES,
    MIDDLE_EAST_EVENTS,
    company_name_index,
    company_table_problems,
)


def _news_retrieval_company_table():
    """The REAL table, loaded from news-retrieval's seed.py. Its `from db
    import ...` would pick up this service's own db module, so a stand-in
    is put in place only while it loads."""
    path = os.path.join(os.path.dirname(__file__), "..", "..", "news-retrieval", "src", "seed.py")
    real_db = sys.modules.get("db")
    sys.modules["db"] = types.SimpleNamespace(get_db=None, init_db=None, transaction=None)
    try:
        spec = importlib.util.spec_from_file_location("news_retrieval_seed", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    finally:
        if real_db is not None:
            sys.modules["db"] = real_db
        else:
            del sys.modules["db"]
    return module.MIDDLE_EAST_TICKER_UNIVERSE


def test_the_real_company_table_passes_the_load_check() -> None:
    companies = _news_retrieval_company_table()
    assert company_table_problems(companies) == []
    assert len(companies) == 25


def test_company_table_problems_catch_hand_edit_mistakes() -> None:
    good = {"ticker": None, "company": "G42", "exchange": None, "country": "UAE",
            "aliases": ["Core42"],
            "counterparties": [{"counterparty_name": "Nvidia", "counterparty_ticker": "NVDA",
                                "relationship": "Partner: ...", "share_price_on_good_news": "rise"}]}
    assert company_table_problems([good]) == []
    assert company_table_problems([]) == ["the company table is empty"]
    typo = {**good, "counterparties": [{**good["counterparties"][0], "share_price_on_good_news": "rsie"}]}
    assert "not rise/fall" in company_table_problems([typo])[0]
    lower = {**good, "counterparties": [{**good["counterparties"][0], "counterparty_ticker": "nvda"}]}
    assert "malformed" in company_table_problems([lower])[0]
    no_reason = {**good, "counterparties": [{**good["counterparties"][0], "relationship": ""}]}
    assert "missing relationship" in company_table_problems([no_reason])[0]
    missing_field = {k: v for k, v in good.items() if k != "aliases"}
    assert "missing aliases" in company_table_problems([missing_field])[0]
    # One alias claimed by two companies would make "about G42" ambiguous.
    clash = {**good, "company": "MGX", "aliases": ["core42"], "counterparties": []}
    assert "also belongs to G42" in company_table_problems([good, clash])[0]


def test_company_name_index_is_case_insensitive_over_names_and_aliases() -> None:
    companies = _news_retrieval_company_table()
    index = company_name_index(companies)
    assert index["core42"]["company"] == "G42"
    assert index["ras laffan"]["company"] == "QatarEnergy"
    assert "elm" not in index  # stored in full, never as the common word


def test_events_table_matches_the_spec() -> None:
    assert len(MIDDLE_EAST_EVENTS) == 13  # the spec's "At a glance" rows
    for event in MIDDLE_EAST_EVENTS:
        assert event["event_type"] in MIDDLE_EAST_EVENT_TYPES
        assert event["affected_companies"] or event.get("affected_companies_described")
        for c in event["affected_companies"]:
            assert c["share_price"] in {"rise", "fall"}
            assert c["ticker"].isupper()
    helium = [e for e in MIDDLE_EAST_EVENTS if e["event"].startswith("Qatar's helium plant")]
    assert {c["share_price"] for e in helium for c in e["affected_companies"]} == {"rise", "fall"}


def test_run_fails_and_stores_nothing_when_the_company_table_is_unavailable(monkeypatch) -> None:
    _fake_news(monkeypatch, {10: [_article("er-me://1", "eventregistry", "eventregistry:me_shipping")]})

    async def unavailable():
        return None
    monkeypatch.setattr(run, "fetch_middle_east_universe", unavailable)
    statuses, inserted = [], []
    monkeypatch.setattr(run, "update_job_status", lambda *a, **k: statuses.append(a[1]))
    monkeypatch.setattr(run, "insert_middle_east_signal_classification", lambda *a: inserted.append(a))
    classify_called = []
    monkeypatch.setattr(run, "classify_middle_east_signal_batch",
                        lambda *a: classify_called.append(a) or [])
    asyncio.run(run.run_middle_east_signal_classification(1, "2026-10-20", "2026-10-21"))
    assert statuses == ["running", "failed"]
    assert inserted == [] and classify_called == []
