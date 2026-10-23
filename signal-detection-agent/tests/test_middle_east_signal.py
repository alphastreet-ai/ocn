"""middle_east_market_signal classifier, tested one build step at a time.

Step 1: pooling, the already-judged skip, and the insert. Unit tests fake
news-retrieval and the DB lookups; the round-trip test at the bottom uses
a real Postgres when one is reachable (skipped otherwise, like
test_results_filters_integration)."""
from __future__ import annotations

import asyncio
import re
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))

import controllers.run as run  # noqa: E402
import pipeline.middle_east_signal_classifier as mec  # noqa: E402
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

async def _no_market_reference(job_id, companies):
    return None


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
    monkeypatch.setattr(run, "get_middle_east_event_members", lambda uris: {})
    monkeypatch.setattr(run, "regroup_middle_east_stories", lambda job_id, companies=None: None)
    monkeypatch.setattr(run, "log_middle_east_market_reference", _no_market_reference)


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
    monkeypatch.setattr(run, "get_middle_east_story_candidates", lambda: [])


def test_run_stores_nothing_when_the_model_call_fails(monkeypatch) -> None:
    """A failed model call stores nothing, or the already-judged skip would
    hide the article from every later run."""
    _fake_news(monkeypatch, {10: [_article("er-me://1", "eventregistry", "eventregistry:me_shipping",
                                           body="Ships were attacked near Hormuz.")]})
    _fake_table(monkeypatch)
    monkeypatch.setattr(mec, "_call_model", lambda system, user: None)
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
    monkeypatch.setattr(run, "classify_middle_east_signal_batch",
                        lambda arts, companies, universe, stored, waterways: [
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
    # 26: Intel Kiryat Gat and Nvidia Israel are separate rows (review point 22).
    assert len(companies) == 26


def test_company_table_problems_catch_hand_edit_mistakes() -> None:
    good = {"ticker": None, "company": "G42", "kind": "company", "exchange": None, "country": "UAE",
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


# --- Step 3: event type from the source -----------------------------------------

from pipeline.middle_east_signal_classifier import (  # noqa: E402
    EVENT_CODES,
    RULE_DECIDED_SOURCES,
    SOURCE_EVENT_TYPES,
    check_extraction,
    company_key,
    counterparty_ticker_index,
    decide_label,
    event_type_from_source,
    headline_rule,
    judge_sec_stake,
    read_through,
    tracked_names_in,
    us_universe_index,
)


def test_only_the_sec_filing_is_final_and_rule_decided() -> None:
    assert event_type_from_source("sec_fund_stake") == {"event_type": "M3", "final": True}
    assert RULE_DECIDED_SOURCES == {"sec_fund_stake"}
    # Federal Register and BIS are free text: hinted, read by the model.
    assert event_type_from_source("federal_register") == {"event_type": "M1", "final": False}
    assert event_type_from_source("bis_press")["final"] is False
    assert event_type_from_source("eventregistry:me_shipping")["event_type"] == "M4"
    assert event_type_from_source("wam_wire")["event_type"] is None
    assert event_type_from_source("something_new") == {"event_type": None, "final": False}


def test_every_source_news_retrieval_sends_has_an_entry() -> None:
    sources = {"wam_wire", "spa_wire", "substack_post", "bis_press", "federal_register",
               "sec_fund_stake", "eventregistry:me_shipping", "eventregistry:me_helium",
               "eventregistry:me_qatarenergy", "eventregistry:me_israel_sites",
               "eventregistry:me_gulf_outlets", "eventregistry:me_energy_sites",
               "eventregistry:me_strike_plans", "eventregistry:me_gulf_money"}
    assert set(SOURCE_EVENT_TYPES) == sources


# --- Step 4: headline rules ----------------------------------------------------

@pytest.mark.parametrize("title, source, rule", [
    ("US stocks close higher on AI rally", "eventregistry:me_gulf_outlets", "market_wrap"),
    # A stock-move headline goes to the model: one reported the Intel fab blast.
    ("Why Intel Stock Hit the Skids on Wednesday", "eventregistry:me_israel_sites", None),
    ("Tue: TASE closes higher", "eventregistry:me_israel_sites", "market_wrap"),
    ("This week's top stories: G42, Humain", "substack_post", "weekly_digest"),
    # A digest headline from another source is not the newsletter's digest.
    ("This week's top stories: G42, Humain", "wam_wire", None),
    # An event headline goes to the model, whatever it mentions.
    ("Houthis strike tanker in Red Sea", "eventregistry:me_shipping", None),
    # Official sources are never skipped by a headline.
    ("US stocks and the new BIS rule", "bis_press", None),
])
def test_headline_rule(title, source, rule) -> None:
    got = headline_rule({"title": title}, source)
    assert (got[0] if got else None) == rule
    if got:
        assert got[1].endswith(".")


# --- Step 5: SEC 13D/G ---------------------------------------------------------

_UNIVERSE = us_universe_index([
    {"company_name": "NVIDIA Corporation", "ticker": "NVDA", "aliases": []},
    {"company_name": "Global Foundries", "ticker": "GFS", "aliases": ["GlobalFoundries Inc."]},
])


def _sec(form, issuer, pct):
    return {"url": "sec-stake://1/a", "title": "t", "metadata": {
        "source_type": "sec_fund_stake", "fund": "Mubadala Investment Company", "form": form,
        "issuer_name": issuer, "percent_of_class": pct, "filing_date": "2026-10-14"}}


def test_company_key_matches_across_suffixes() -> None:
    assert company_key("NVIDIA Corp.") == company_key("Nvidia Corporation") == "nvidia"
    assert company_key("GlobalFoundries Inc.") == "globalfoundries"


@pytest.mark.parametrize("form, issuer, pct, label, rule, share_price", [
    ("SCHEDULE 13D/A", "Evotec SE", 0.0, "noise", "sec_issuer_not_ai_universe", None),
    ("SCHEDULE 13G", "NVIDIA Corp", 5.2, "signal", "sec_new_stake", "rise"),
    ("SCHEDULE 13D/A", "GlobalFoundries Inc.", 0.0, "signal", "sec_stake_cut_below_5pct", "fall"),
    ("SCHEDULE 13G/A", "GlobalFoundries Inc.", 4.9, "signal", "sec_stake_cut_below_5pct", "fall"),
    ("SCHEDULE 13D/A", "GlobalFoundries Inc.", 81.0, "weak_signal", "sec_stake_amended", None),
    # percent missing from the filing data: not called a sale.
    ("SCHEDULE 13D/A", "GlobalFoundries Inc.", None, "weak_signal", "sec_stake_amended", None),
])
def test_sec_rules(form, issuer, pct, label, rule, share_price) -> None:
    result = judge_sec_stake(_sec(form, issuer, pct), _UNIVERSE)
    assert result["signal"] == label and result["metadata"]["rule"] == rule
    affected = [e for e in result["entities"] if e["role"] == "affected"]
    assert [a["share_price"] for a in affected] == ([share_price] if share_price else [])
    assert result["reason"].endswith(".") and "None" not in result["reason"]


def test_batch_holds_back_sec_filings_when_research_universe_is_unavailable(monkeypatch) -> None:
    monkeypatch.setattr(mec, "_call_model", lambda s, u: pytest.fail("SEC needs no model"))
    out = mec.classify_middle_east_signal_batch(
        [_sec("SCHEDULE 13G", "NVIDIA Corp", 5.2)], _news_retrieval_company_table(), None)
    assert out == []
    out = mec.classify_middle_east_signal_batch(
        [_sec("SCHEDULE 13G", "NVIDIA Corp", 5.2)], _news_retrieval_company_table(),
        [{"company_name": "NVIDIA Corporation", "ticker": "NVDA", "aliases": []}])
    assert out[0]["result"]["signal"] == "signal"


# --- Step 7a: checking the model's answer against the text -----------------------

_BODY = ("ABU DHABI, 19 Oct 2026 (WAM) - G42 said on Sunday that the first 200MW phase of"
         " Stargate UAE is now operational, running Nvidia GB300 systems. “The campus is"
         " live,” the company said. Khazna Data Centers built the site.")


def _index():
    companies = _news_retrieval_company_table()
    return company_name_index(companies), counterparty_ticker_index(companies)


_EVENT_KEYS = ("place", "date", "country", "effective_date", "lasts_over_24_hours")
_DEAL_KEYS = {"deal_level": "level", "amount": "amount", "up_to": "up_to", "capacity": "capacity"}


def _answer(**overrides):
    """A model answer in the prompt's output shape. Overrides may name a
    nested field flat (place=, capacity=, deal_level=, amount=)."""
    answer = {"event": {"code": "m2_switched_on", "place": "Abu Dhabi", "date": "2026-10-19",
                        "country": "UAE", "effective_date": None, "lasts_over_24_hours": None},
              "companies": [{"name": "G42", "country": "UAE"},
                            {"name": "Nvidia", "country": "United States"}],
              "deal": {"level": 6, "amount": None, "up_to": False, "capacity": "200MW"},
              "confirmation": {"level": "official", "by": "G42"},
              "news_effect": "good",
              "reason_facts": "G42 switched on the first 200MW phase of Stargate UAE, running Nvidia GB300 systems.",
              "summary": "G42 said the first 200MW phase of Stargate UAE is now operational."}
    for key, value in overrides.items():
        if key in _EVENT_KEYS:
            answer["event"][key] = value
        elif key in _DEAL_KEYS:
            answer["deal"][_DEAL_KEYS[key]] = value
        elif key == "event":
            answer["event"]["code"] = value
        else:
            answer[key] = value
    return json.dumps(answer)


def _check(raw, title="G42 switches on Stargate UAE", body=_BODY):
    names, tickers = _index()
    return check_extraction(raw, title, body, names, tickers)


def test_check_keeps_supported_facts_and_maps_tracked_companies() -> None:
    ex, dropped = _check(_answer())
    assert dropped == []
    assert ex["event"] == {"type": "M2", "code": "m2_switched_on", "place": "Abu Dhabi",
                           "date": "2026-10-19", "country": "UAE"}
    assert ex["deal"] == {"level": 6, "capacity": "200MW"}
    assert ex["confirmation"] == {"level": "official", "by": "G42"}
    # Not stated -> absent, never stored as null.
    assert "amount" not in ex["deal"] and "effective_date" not in ex["event"]
    assert "lasts_over_24_hours" not in ex["event"]
    g42, nvidia = ex["companies"]
    assert (g42["name"], g42["tracked"], g42["country"]) == ("G42", True, "UAE")
    assert (nvidia["ticker"], nvidia["tracked"]) == ("NVDA", False)
    assert ex["reason_facts"].startswith("G42 switched on the first 200MW phase")


def test_check_maps_an_alias_inside_a_longer_name() -> None:
    ex, _ = _check(_answer(companies=[{"name": "Khazna Data Centers", "country": "UAE"}]))
    assert ex["companies"][0]["name"] == "G42"


@pytest.mark.parametrize("field, value, where", [
    ("capacity", "500MW", "deal"),               # not in the text
    ("amount", "$10 billion", "deal"),
    ("place", "Riyadh", "event"),
])
def test_check_drops_a_copied_value_the_text_does_not_contain(field, value, where) -> None:
    ex, dropped = _check(_answer(**{field: value}))
    assert field not in ex.get(where, {}) and f"{where}.{field}" in dropped


def test_check_amount_keeps_up_to_with_its_value() -> None:
    body = _BODY + " MGX will invest up to $2 billion."
    ex, _ = _check(_answer(amount="$2 billion", up_to=True), body=body)
    assert ex["deal"]["amount"] == "$2 billion" and ex["deal"]["up_to"] is True


@pytest.mark.parametrize("confirmation, expected", [
    ({"level": "official", "by": "G42"}, {"level": "official", "by": "G42"}),
    # Nobody named, or a name the article does not contain: not confirmed.
    ({"level": "official"}, {"level": "unnamed"}),
    ({"level": "official", "by": "the UAE government"}, {"level": "unnamed"}),
    # Every word of it is in the text, not in that order: accepted.
    ({"level": "official", "by": "Khazna Data Centers, G42"},
     {"level": "official", "by": "Khazna Data Centers, G42"}),
    ({"level": "unnamed"}, {"level": "unnamed"}),
    ("official", {"level": "unnamed"}),         # the old flat shape, no `by`
])
def test_confirmation_needs_a_by_the_text_contains(confirmation, expected) -> None:
    ex, _ = _check(_answer(confirmation=confirmation))
    assert ex["confirmation"] == expected


def test_check_drops_invented_companies_and_unsupported_numbers() -> None:
    ex, dropped = _check(_answer(
        companies=[{"name": "Microsoft", "country": "United States"}],
        reason_facts="G42 spent $5 billion.",
        summary="G42 switched on 300MW."))
    assert ex["companies"] == []
    assert ex["summary"] == 'The article is headlined "G42 switches on Stargate UAE".'
    assert ex["reason_facts"] == 'Headline: "G42 switches on Stargate UAE".'
    assert {"company:Microsoft", "reason_facts", "summary"} <= set(dropped)


def test_failed_reason_facts_falls_back_to_the_summarys_first_sentence() -> None:
    ex, _ = _check(_answer(reason_facts="G42 spent $5 billion.",
                           summary="G42 switched on the first 200MW phase. Khazna built it."))
    assert ex["reason_facts"] == "G42 switched on the first 200MW phase."


def test_check_falls_back_to_the_weakest_value_for_bad_enums() -> None:
    ex, dropped = _check(_answer(confirmation={"level": "certain", "by": "G42"}, news_effect="great",
                                 date="next week", deal_level=9))
    assert ex["confirmation"]["level"] == "unnamed" and ex["news_effect"] == "mixed"
    assert "date" not in ex["event"] and "level" not in ex["deal"]
    assert {"confirmation", "news_effect", "event.date"} <= set(dropped)


def test_check_caps_deal_level_at_3_when_only_unnamed_sources() -> None:
    ex, _ = _check(_answer(confirmation={"level": "unnamed"}, deal_level=6))
    assert ex["deal"]["level"] == 3


def test_check_rejects_unusable_answers() -> None:
    assert _check("not json")[0] is None
    assert _check("[1, 2]")[0] is None
    assert _check(_answer(event="m9_invented"))[0] is None


def test_check_none_event_keeps_only_the_summary() -> None:
    ex, _ = _check(json.dumps({"event": "none", "reason_facts": "A survey of firms.",
                               "summary": "A cyber-security survey."}))
    assert ex == {"reason_facts": "A survey of firms.", "summary": "A cyber-security survey."}


def test_every_event_code_belongs_to_its_type() -> None:
    assert set(EVENT_CODES.values()) == set(MIDDLE_EAST_EVENT_TYPES)
    for code, event_type in EVENT_CODES.items():
        assert code.startswith(event_type.lower() + "_")


def test_every_event_code_is_in_the_prompt() -> None:
    import config
    prompt = config.MIDDLE_EAST_SIGNAL_PROMPT.read_text(encoding="utf-8")
    for code in EVENT_CODES:
        assert f"  {code} " in prompt, code


# --- Step 7b: label rules (spec section 7) -------------------------------------

def _ex(event, confirmation="official", amount=None, amount_up_to=False, deal_level=None,
        capacity=None, place=None, effective_date=None, over_24_hours=None, news="bad",
        country=None, day=None, by="x", **kw):
    """A checked extraction in its stored shape; None values are left out
    exactly as check_extraction leaves them out."""
    ex = {"companies": [], "news_effect": news, "summary": "s.",
          "confirmation": {"level": confirmation, **({"by": by} if confirmation != "unnamed" else {})}}
    if event:
        ex["event"] = {k: v for k, v in {
            "type": EVENT_CODES[event], "code": event, "place": place, "date": day,
            "country": country, "effective_date": effective_date,
            "lasts_over_24_hours": over_24_hours}.items()
            if v is not None}
    deal = {k: v for k, v in {"level": deal_level, "amount": amount,
                              "up_to": amount_up_to if amount else None,
                              "capacity": capacity}.items() if v is not None}
    if deal:
        ex["deal"] = deal
    ex.update({k: v for k, v in kw.items() if v is not None})
    return ex


_G42 = {"name": "G42", "ticker": None, "country": "UAE", "tracked": True}
_TOWER = {"name": "Tower Semiconductor", "ticker": "TSEM", "country": "Israel", "tracked": True}
_OPENAI = {"name": "OpenAI", "ticker": None, "country": "United States", "tracked": False}
_MAERSK = {"name": "Maersk", "ticker": None, "country": "Denmark", "tracked": False}


@pytest.mark.parametrize("ex, label, rule", [
    (_ex(None), "noise", "not_an_event_type"),
    # M1
    (_ex("m1_rule_or_licence", effective_date="15 December 2026"), "signal", "m1_final_decision"),
    # An official approval is final without a start date (review point 42).
    (_ex("m1_rule_or_licence"), "signal", "m1_final_decision"),
    (_ex("m1_rule_or_licence", confirmation="named_source"), "weak_signal", "m1_not_official"),
    (_ex("m1_rule_or_licence", confirmation="unnamed"), "weak_signal", "m1_unnamed_officials"),
    (_ex("m1_congress_action", companies=[_G42]), "signal", "m1_congress_named_deal"),
    (_ex("m1_congress_action"), "weak_signal", "m1_congress_no_named_deal"),
    (_ex("m1_export_category_change"), "signal", "m1_export_category_change"),
    (_ex("m1_draft_or_hearing"), "weak_signal", "m1_draft_or_hearing"),
    (_ex("m1_repeat_or_speech"), "noise", "m1_repeat_or_speech"),
    # M2
    (_ex("m2_switched_on", capacity="200MW"), "signal", "m2_switched_on_with_capacity"),
    (_ex("m2_switched_on"), "weak_signal", "m2_no_capacity_figure"),
    (_ex("m2_chips_delivered", companies=[_G42]), "signal", "m2_chips_delivered"),
    (_ex("m2_chips_delivered"), "weak_signal", "m2_delivery_unconfirmed"),
    (_ex("m2_setback"), "signal", "m2_setback"),
    # A memorandum with no amount and no capacity commits nothing (review point 21)...
    (_ex("m2_deal_or_construction", deal_level=2), "noise", "m2_memorandum_without_figures"),
    # ...one with a figure, or a partnership with an amount, is still stored.
    (_ex("m2_deal_or_construction", companies=[_G42], deal_level=2, capacity="100MW"), "weak_signal", "m2_deal_below_signed_amount"),
    (_ex("m2_deal_or_construction", companies=[_G42], deal_level=3, amount="$2 billion", amount_up_to=True),
     "weak_signal", "m2_deal_below_signed_amount"),
    # A signed order with its value (review point 6: MIS's $500M order for Humain).
    (_ex("m2_deal_or_construction", companies=[_G42], deal_level=4, amount="$500 million"), "signal", "m2_signed_with_amount"),
    (_ex("m2_deal_or_construction", companies=[_G42], deal_level=4, amount="$500 million", amount_up_to=True),
     "weak_signal", "m2_deal_below_signed_amount"),
    (_ex("m2_deal_or_construction", companies=[_G42], deal_level=4), "weak_signal", "m2_deal_below_signed_amount"),
    (_ex("m2_deal_or_construction", deal_level=1), "noise", "m2_pledge"),
    (_ex("m2_company_update", confirmation="named_source"), "weak_signal", "m2_company_update"),
    (_ex("m2_company_update", confirmation="unnamed"), "noise", "m2_company_update_unnamed"),
    (_ex("m2_visit_or_ceremony"), "noise", "m2_visit_or_ceremony"),
    # M3
    (_ex("m3_investment", deal_level=4, amount="$2 billion", companies=[_G42, _OPENAI]), "signal",
     "m3_completed_with_amount"),
    (_ex("m3_investment", deal_level=4, amount="$2 billion", amount_up_to=True),
     "weak_signal", "m3_investment_not_alertable"),
    (_ex("m3_investment", deal_level=4), "weak_signal", "m3_investment_not_alertable"),
    (_ex("m3_investment", deal_level=2, amount="$2 billion"), "weak_signal", "m3_investment_not_alertable"),
    (_ex("m3_investment", deal_level=1), "noise", "m3_pledge"),
    (_ex("m3_pullback"), "signal", "m3_pullback"),
    (_ex("m3_talks", amount="$30 billion"), "weak_signal", "m3_talks"),
    # "Exploring investment opportunities" with no amount commits nothing.
    (_ex("m3_talks"), "noise", "m3_talks_without_amount"),
    # M4
    (_ex("m4_strike_on_site", place="Ras Laffan", country="Qatar", day="2026-10-14"), "signal", "m4_strike_confirmed"),
    (_ex("m4_strike_on_site", place="Haifa port", country="Israel", day="2026-10-14"), "signal", "m4_strike_confirmed"),
    (_ex("m4_strike_on_site", place="Basra", country="Iraq"), "weak_signal",
     "m4_strike_outside_alert_countries"),
    (_ex("m4_strike_on_site", place="Ras Laffan", country="Qatar", confirmation="unnamed"),
     "weak_signal", "m4_strike_claimed"),
    # A waterway with no known status and no controlling-party confirmer.
    (_ex("m4_waterway_closed"), "weak_signal", "m4_waterway_closed_unconfirmed"),
    # Homes hit, nothing economic (review point 18).
    (_ex("m4_other_strike", place="Najran"), "noise", "m4_no_economic_target"),
    (_ex("m4_other_strike", confirmation="unnamed"), "noise", "m4_no_economic_target"),
    (_ex("m4_shipping_or_insurance", by="Maersk", companies=[_MAERSK]), "signal", "m4_shipping_line_or_insurer"),
    (_ex("m4_ship_incident"), "weak_signal", "m4_ship_incident"),
    (_ex("m4_ship_incident", confirmation="unnamed"), "noise", "m4_ship_incident_unnamed"),
    # Ships at sea (review points 11-12).
    (_ex("m4_ship_sunk_or_crew_killed", confirmation="named_source", day="2026-10-13"), "signal",
     "m4_ship_sunk_or_crew_killed"),
    (_ex("m4_ship_sunk_or_crew_killed", confirmation="unnamed"), "weak_signal", "m4_ship_sunk_unconfirmed"),
    (_ex("m4_attack_tally", confirmation="named_source", day="2026-10-16"), "signal", "m4_attack_tally"),
    (_ex("m4_attack_tally", confirmation="unnamed"), "weak_signal", "m4_attack_tally_unnamed"),
    (_ex("m4_ship_traffic", confirmation="named_source"), "weak_signal", "m4_ship_traffic"),
    (_ex("m4_ship_traffic", confirmation="unnamed"), "noise", "m4_ship_traffic_unnamed"),
    # Planned strikes (review point 14).
    (_ex("m4_strike_plan"), "signal", "m4_strike_plan_announced"),
    (_ex("m4_strike_plan", confirmation="unnamed"), "weak_signal", "m4_strike_plan_reported"),
    # The Black Sea is not the Middle East (review point 16).
    (_ex("m4_ship_incident", place="Black Sea"), "noise", "m4_outside_middle_east"),
    (_ex("m4_ship_incident", place="off Constanta", country="Romania"), "noise", "m4_outside_middle_east"),
    (_ex("m4_threat_or_statement", confirmation="named_source"), "weak_signal", "m4_threat_or_statement"),
    (_ex("m4_commentary"), "noise", "m4_commentary"),
    # M5
    (_ex("m5_qatar_force_majeure", by="QatarEnergy", day="2026-10-12"), "signal", "m5_qatar_force_majeure"),
    # Official, but not QatarEnergy itself.
    (_ex("m5_qatar_force_majeure", by="Pakistan LNG Ltd", day="2026-10-12"), "weak_signal",
     "m5_qatar_force_majeure_unconfirmed"),
    # Only QatarEnergy itself can declare or end its force majeure.
    (_ex("m5_qatar_force_majeure", confirmation="named_source", day="2026-10-12"), "weak_signal",
     "m5_qatar_force_majeure_unconfirmed"),
    (_ex("m5_supplier_rationing", confirmation="unnamed"), "weak_signal", "m5_supplier_rationing_unconfirmed"),
    (_ex("m5_asia_shortfall"), "signal", "m5_asia_shortfall"),
    (_ex("m5_asia_shortfall", confirmation="named_source"), "weak_signal", "m5_asia_shortfall_unconfirmed"),
    (_ex("m5_commentary"), "weak_signal", "m5_commentary"),
    # M6
    (_ex("m6_binding_rule", effective_date="1 January 2027"), "signal", "m6_binding_rule_with_date"),
    (_ex("m6_binding_rule"), "weak_signal", "m6_rule_no_date"),
    (_ex("m6_chinese_supplier", deal_level=4), "signal", "m6_chinese_supplier_signed"),
    (_ex("m6_chinese_supplier", deal_level=2), "weak_signal", "m6_chinese_supplier_not_signed"),
    (_ex("m6_strategy"), "weak_signal", "m6_strategy"),
    (_ex("m6_speech"), "noise", "m6_speech"),
    # M7
    (_ex("m7_site_disruption", companies=[_TOWER]), "signal", "m7_site_disruption"),
    (_ex("m7_site_disruption"), "weak_signal", "m7_disruption_unconfirmed"),
    # Intel is not in the Israeli table by that name, but is listed (INTC).
    (_ex("m7_site_disruption", companies=[{"name": "Intel", "ticker": "INTC", "country": None,
                                           "tracked": False}]), "signal", "m7_site_disruption"),
    # No production stop announced -> one weak signal (review point 3).
    (_ex("m7_site_incident", companies=[_TOWER]), "weak_signal", "m7_site_incident_confirmed"),
    (_ex("m7_company_deal", deal_level=4, amount="$300 million", companies=[_TOWER]),
     "signal", "m7_company_deal_signed_with_amount"),
    (_ex("m7_company_deal", deal_level=2, companies=[_TOWER]), "weak_signal", "m7_company_deal"),
    (_ex("m7_site_incident", companies=[_TOWER], confirmation="named_source"), "weak_signal", "m7_site_incident"),
    (_ex("m4_interception"), "noise", "m4_interception"),
    (_ex("m7_home_front_restriction", place="Haifa", over_24_hours=True), "signal", "m7_home_front_over_24h"),
    (_ex("m7_home_front_restriction", place="Haifa", over_24_hours=None),
     "weak_signal", "m7_home_front_short_or_elsewhere"),
    (_ex("m7_home_front_restriction", place="Eilat", over_24_hours=True),
     "weak_signal", "m7_home_front_short_or_elsewhere"),
    (_ex("m7_acquisition", deal_level=4, amount="$900 million"), "signal", "m7_acquisition_with_price"),
    (_ex("m7_acquisition", deal_level=3, amount="$900 million"), "weak_signal", "m7_acquisition_talks"),
    (_ex("m7_commentary"), "noise", "m7_commentary"),
    # M8
    (_ex("m8_enforcement", confirmation="unnamed", companies=[_G42]), "signal", "m8_enforcement"),
    (_ex("m8_enforcement", companies=[{"name": "URAL Airlines JSC", "ticker": None,
                                       "country": "Russia", "tracked": False}]),
     "noise", "m8_no_gulf_company"),
    (_ex("m8_investigation_unnamed"), "weak_signal", "m8_investigation_unnamed"),
])
def test_decide_label(ex, label, rule) -> None:
    got_label, got_rule, sentence = decide_label(
        ex, "Qatar extends force majeure" if ex.get("event", {}).get("type") == "M5" else "")
    assert (got_label, got_rule) == (label, rule)
    if ex.get("event"):     # "none": the model's own sentence is the whole reason
        assert sentence[0].isupper() and sentence.endswith(".") and "None" not in sentence
    else:
        assert sentence == ""


def test_every_event_code_has_a_rule() -> None:
    """No code may fall through to the M8 default by accident."""
    for code in EVENT_CODES:
        if code.startswith("m8_"):
            continue
        _, rule, _ = decide_label(_ex(code))
        assert not rule.startswith("m8_"), code


# --- Step 7c: read-through -----------------------------------------------------

def test_read_through_uses_the_company_table_then_the_events_table() -> None:
    names, _ = _index()
    affected = read_through(_ex("m2_switched_on", news="good", companies=[_G42]), names)
    by_ticker = {a["ticker"]: a["share_price"] for a in affected}
    # G42's counterparties, then the M2 row's suppliers not already listed.
    assert by_ticker["NVDA"] == "rise" and by_ticker["CBRS"] == "rise" and by_ticker["VRT"] == "rise"
    assert len(by_ticker) == len(affected)   # no ticker twice


def test_read_through_reverses_on_bad_news_and_keeps_inverse_counterparties() -> None:
    names, _ = _index()
    qatar = {"name": "QatarEnergy", "ticker": None, "country": "Qatar", "tracked": True}
    bad = {a["ticker"]: a["share_price"]
           for a in read_through(_ex("m5_qatar_force_majeure", companies=[qatar]), names)}
    assert bad["MU"] == "fall" and bad["LIN"] == "rise" and bad["XOM"] == "rise"
    good = {a["ticker"]: a["share_price"]
            for a in read_through(_ex("m5_qatar_force_majeure", companies=[qatar], news="good"), names)}
    assert good["MU"] == "rise" and good["LIN"] == "fall" and good["AAPL"] == "rise"


def test_read_through_gives_no_direction_on_mixed_news_or_no_event() -> None:
    names, _ = _index()
    assert read_through(_ex("m2_switched_on", news="mixed", companies=[_G42]), names) == []
    assert read_through(_ex(None), names) == []


def test_read_through_for_events_without_companies() -> None:
    names, _ = _index()
    m8 = {a["ticker"]: a["share_price"] for a in read_through(_ex("m8_enforcement"), names)}
    assert m8 == {"NVDA": "fall", "AMD": "fall"}
    # An acquisition's buyer is not in any table: no invented direction.
    assert read_through(_ex("m7_acquisition", news="good"), names) == []


def test_reasons_shown_to_readers_carry_no_internal_terms() -> None:
    """signal_reason is shown in the UI: no spec, levels, codes or labels."""
    banned = re.compile(r"\bspec\b|\blevel \d|\bM[1-8]\b|signal|noise|\w+_\w+|\bmay\b|\bmight\b|\bcould\b",
                        re.IGNORECASE)
    for code in EVENT_CODES:
        for conf in ("official", "named_source", "unnamed"):
            for level in (None, 1, 3, 4):
                for day in ("2026-10-19", "2026-08-01"):      # new, and a repeat
                    ex = _ex(code, confirmation=conf, deal_level=level, day=day,
                             amount="$1 billion", capacity="50MW", effective_date="1 Jan",
                             place="Haifa", country="Israel", over_24_hours=True, companies=[_G42])
                    _, _, why = decide_label(ex, "Qatar", None, None, "2026-10-20")
                    assert not banned.search(why), (code, why)


# --- Batch, end to end with a fake model ----------------------------------------

def test_batch_end_to_end_with_a_fake_model(monkeypatch) -> None:
    sent = []

    def fake_model(system, user):
        sent.append(user)
        return _answer()
    monkeypatch.setattr(mec, "_call_model", fake_model)
    article = {"url": "https://www.wam.ae/en/article/x", "title": "G42 switches on Stargate UAE",
               "body": _BODY, "metadata": {"source_type": "wam_wire"}}
    wrap = {"url": "er-me://9", "title": "US stocks close higher on AI rally", "body": "x",
            "metadata": {"source_type": "eventregistry", "er_query": "eventregistry:me_gulf_outlets"}}
    out = mec.classify_middle_east_signal_batch([article, wrap], _news_retrieval_company_table())
    assert len(sent) == 1                                  # the market wrap needed no call
    assert "Tracked companies named in the text: G42" in sent[0]
    assert _BODY in sent[0]                               # the whole body, once
    result = out[0]["result"]
    assert result["signal"] == "signal"
    # The reason: what happened (the model's reason_facts), then why (code).
    assert result["reason"] == (
        "G42 switched on the first 200MW phase of Stargate UAE, running Nvidia GB300 systems."
        " The capacity is running now, which shows Gulf AI orders turning into real hardware sales.")
    # The affected companies are in entities_json, not repeated in the text.
    assert result["metadata"]["summary"].startswith("G42 said the first 200MW phase")
    md = result["metadata"]
    assert md["rule"] == "m2_switched_on_with_capacity"
    assert md["confirmation"] == {"level": "official", "by": "G42"}
    assert md["extracted_by"]["prompt"] == "middle_east_signal_extract_v1"
    assert md["source"] == {"name": "wam_wire"}
    # Each field stored once: companies in entities_json, not in metadata.
    assert "companies" not in md
    assert "effective_date" not in md and "amount" not in md      # nulls not stored
    assert set(md) == {"extracted_by", "event", "deal", "confirmation",
                       "news_effect", "summary", "rule", "source", "corroboration"}
    assert md["event"] == {"type": "M2", "code": "m2_switched_on", "place": "Abu Dhabi",
                           "date": "2026-10-19", "country": "UAE"}
    assert md["deal"] == {"level": 6, "capacity": "200MW"}
    roles = {(e["name"], e["role"]) for e in result["entities"]}
    assert ("G42", "subject") in roles and ("Nvidia", "affected") in roles
    assert out[1]["result"]["metadata"] == {"source": {"name": "eventregistry:me_gulf_outlets"},
                                            "rule": "market_wrap"}


def test_batch_stores_an_unusable_answer_as_weak_not_hidden(monkeypatch) -> None:
    monkeypatch.setattr(mec, "_call_model", lambda s, u: "I think this is about G42.")
    article = {"url": "u", "title": "t", "body": "b", "metadata": {"source_type": "wam_wire"}}
    out = mec.classify_middle_east_signal_batch([article], _news_retrieval_company_table())
    assert out[0]["result"]["signal"] == "weak_signal"
    assert out[0]["result"]["metadata"]["rule"] == "model_output_unusable"


def test_batch_cuts_a_freak_body_and_records_it(monkeypatch) -> None:
    sent = []
    monkeypatch.setattr(mec, "_call_model",
                        lambda s, u: sent.append(u) or json.dumps({"event": "none", "reason_facts": "x.", "summary": "x."}))
    article = {"url": "me-federal-register://1", "title": "t", "body": "a" * 70_000, "metadata": {}}
    out = mec.classify_middle_east_signal_batch([article], _news_retrieval_company_table())
    assert len(sent[0]) < 61_000
    assert out[0]["result"]["metadata"]["body_truncated"] is True
    assert out[0]["result"]["signal"] == "noise"


def test_tracked_names_need_whole_words() -> None:
    names, _ = _index()
    assert tracked_names_in("Ooredoo and Humain", names) == ["Humain", "Ooredoo"]
    assert tracked_names_in("Telecom element for stcx", names) == []


@pytest.mark.parametrize("sentence, ok", [
    ("G42 switched on 200MW.", True),
    ("G42 switched on 300MW.", False),          # "300" only inside "GB300"
    ("It runs Nvidia GB300 systems.", True),
    ("The deal is worth $1,000.", False),
    ("On 19 Oct 2026 it opened.", True),
    ("It opened on 2026-10-19.", False),        # a date the text writes differently
])
def test_summary_numbers_must_appear_as_whole_numbers(sentence, ok) -> None:
    from pipeline.middle_east_signal_classifier import _numbers_supported, normalize_text
    assert _numbers_supported(sentence, normalize_text(_BODY)) is ok


def test_m3_read_through_ignores_a_funds_existing_holdings() -> None:
    """Mubadala's table counterparty is GlobalFoundries (it owns most of it);
    a NEW Mubadala investment moves the spec's M3 row, not GFS."""
    names, _ = _index()
    mubadala = {"name": "Mubadala", "ticker": None, "country": "UAE", "tracked": True}
    got = {a["ticker"] for a in read_through(_ex("m3_investment", news="good", companies=[mubadala]), names)}
    assert got == {"NVDA", "AVGO", "MU"}


def test_batch_warms_the_cache_with_one_call_before_the_parallel_ones(monkeypatch) -> None:
    import threading
    active, peak = [0], [0]
    lock = threading.Lock()
    order = []

    def fake(system, user):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
            order.append(active[0])
        import time as _t
        _t.sleep(0.05)
        with lock:
            active[0] -= 1
        return json.dumps({"event": "none", "summary": "x."})
    monkeypatch.setattr(mec, "_call_model", fake)
    arts = [{"url": f"u{i}", "title": "t", "body": "b", "metadata": {"source_type": "wam_wire"}}
            for i in range(6)]
    out = mec.classify_middle_east_signal_batch(arts, _news_retrieval_company_table())
    assert len(out) == 6
    assert order[0] == 1          # the first call ran alone
    assert peak[0] > 1            # the rest ran in parallel


def test_qatar_force_majeure_in_the_background_is_not_an_alert() -> None:
    """A force majeure mentioned as background carries the day it was
    declared - weeks before the article - so it is a repeat, whatever the
    headline says."""
    fresh = _ex("m5_qatar_force_majeure", by="QatarEnergy", day="2026-10-12")
    assert decide_label(fresh, "any headline", None, None, "2026-10-13")[0] == "signal"
    background = _ex("m5_qatar_force_majeure", by="QatarEnergy", day="2026-09-30")
    assert decide_label(background, "any headline", None, None, "2026-10-16")[:2] == ("noise", "repeat")



# --- Corroboration: who else reported this same event -----------------------------

from datetime import date, datetime, timedelta, timezone  # noqa: E402

from pipeline.middle_east_signal_classifier import (  # noqa: E402
    build_middle_east_stories,
    source_fields,
    story_development,
    story_keys,
)


def _er(uri, title, event_uri=None, also=None, outlet="reuters.com"):
    md = {"source_type": "eventregistry", "er_query": "eventregistry:me_shipping",
          "source_uri": outlet, "article_url": f"https://{outlet}/{uri}",
          "event_uri": event_uri if event_uri else "None"}
    if also is not None:
        md["also_reported_by"] = also
    return {"url": f"er-me://{uri}", "title": title, "body": "Tankers hit near Hormuz.", "metadata": md}


def test_source_fields_keep_the_clickable_link_and_cluster_only_for_newsapi() -> None:
    assert source_fields(_er("1", "t", event_uri="eng-1")) == {
        "name": "eventregistry:me_shipping", "url": "https://reuters.com/1",
        "outlet": "reuters.com", "event_uri": "eng-1"}
    # newsapi.ai's literal "None" is not an event id.
    assert "event_uri" not in source_fields(_er("1", "t"))
    assert source_fields({"url": "https://www.wam.ae/x", "metadata": {"source_type": "wam_wire"}}) == {
        "name": "wam_wire"}


def test_batch_attaches_corroboration_from_both_sources(monkeypatch) -> None:
    monkeypatch.setattr(mec, "_call_model", lambda s, u: json.dumps({
        "event": {"code": "m4_ship_incident"}, "companies": [],
        "confirmation": {"level": "named_source", "by": "Tankers"},
        "new_development": True, "news_effect": "bad",
        "reason_facts": "Tankers were hit near Hormuz.", "summary": "Tankers were hit near Hormuz."}))
    also = [{"url": "https://www.theguardian.com/x/", "title": "Tankers hit", "outlet": "theguardian.com"},
            # the article itself, listed by news-retrieval: never counted
            {"url": "https://reuters.com/1", "title": "same", "outlet": "reuters.com"}]
    a = _er("1", "Tankers hit near Hormuz", event_uri="eng-1", also=also)
    b = _er("2", "Ships attacked in Gulf", event_uri="eng-1", outlet="apnews.com")
    stored = {"eng-1": [{"id": "er-me://0", "title": "Mine strike", "url": "https://ft.com/0",
                         "outlet": "ft.com"},
                        # the Guardian again, found by the cluster too: counted once
                        {"id": "er-me://9", "title": "Tankers hit", "url": "https://theguardian.com/x",
                         "outlet": "theguardian.com"}]}
    out = mec.classify_middle_east_signal_batch([a, b], _news_retrieval_company_table(),
                                                stored_event_members=stored)
    corr = out[0]["result"]["metadata"]["corroboration"]
    assert sorted(x["outlet"] for x in corr["siblings"]) == ["apnews.com", "ft.com", "theguardian.com"]
    assert corr["count"] == 3
    # The other article sees the first one as its sibling.
    assert "reuters.com" in [x["outlet"] for x in out[1]["result"]["metadata"]["corroboration"]["siblings"]]


def test_corroboration_is_an_explicit_zero_and_absent_on_noise_and_sec(monkeypatch) -> None:
    monkeypatch.setattr(mec, "_call_model", lambda s, u: json.dumps({
        "event": {"code": "m4_ship_incident"}, "companies": [],
        "confirmation": {"level": "named_source", "by": "Tankers"},
        "new_development": True, "news_effect": "bad", "reason_facts": "Ships hit.", "summary": "Ships hit."}))
    alone = mec.classify_middle_east_signal_batch([_er("1", "Ships hit")], _news_retrieval_company_table())
    assert alone[0]["result"]["metadata"]["corroboration"] == {"count": 0, "siblings": []}
    wrap = _er("2", "US stocks close higher on AI rally")
    sec = _sec("SCHEDULE 13G", "NVIDIA Corp", 5.2)
    out = mec.classify_middle_east_signal_batch(
        [wrap, sec], _news_retrieval_company_table(),
        [{"company_name": "NVIDIA Corporation", "ticker": "NVDA", "aliases": []}])
    assert all("corroboration" not in o["result"]["metadata"] for o in out)


# --- Stories: which rows are one developing situation ---------------------------------

_T0 = datetime(2026, 10, 17, 6, tzinfo=timezone.utc)


def _row(id_, event_type, place=None, subjects=(), hours=0, label="weak_signal", count=0):
    md = {"event": {"type": event_type}, "summary": f"summary {id_}",
          "corroboration": {"count": count, "siblings": []}}
    if place:
        md["event"]["place"] = place
    entities = [{"name": n, "role": "subject"} for n in subjects]
    entities += [{"name": "Micron", "ticker": "MU", "role": "affected", "share_price": "fall"}]
    return {"id": id_, "title": f"title {id_}", "url": f"er-me://{id_}",
            "published": _T0 + timedelta(hours=hours), "signal_detection": label,
            "signal_reason": f"reason {id_}", "entities": entities, "metadata": md}


def test_hormuz_events_of_one_type_form_one_story_seeded_by_the_signal() -> None:
    rows = [_row(1, "M4", "Strait of Hormuz", hours=0),
            _row(2, "M4", "Strait of Hormuz", hours=5, label="signal"),
            _row(3, "M4", "the Strait of Hormuz", hours=30, count=4)]
    stories = build_middle_east_stories(rows)
    assert len(stories) == 1
    assert stories[0]["seed"]["id"] == 2                # a signal seeds over a weak one
    dev = story_development(stories[0])
    assert [m["id"] for m in dev["members"]] == [1, 3]  # in time order
    assert dev["count"] == 2 and dev["members"][0]["summary"] == "summary 1"
    assert dev["members"][0]["shared"] == ["strait hormuz"]


@pytest.mark.parametrize("a, b", [
    # Same place, different event types: two situations.
    (_row(1, "M4", "Ras Laffan"), _row(2, "M5", "Ras Laffan")),
    # Same type, a country is all they share.
    (_row(1, "M4", "Iran"), _row(2, "M4", "Iran")),
    # Same type and place, more than 72 hours apart.
    (_row(1, "M4", "Strait of Hormuz"), _row(2, "M4", "Strait of Hormuz", hours=80)),
    # The read-through company (Micron, on both) is never a key.
    (_row(1, "M4"), _row(2, "M4")),
    # The IRGC is the actor in every Hormuz story, not its identity.
    (_row(1, "M4", subjects=["IRGC"]), _row(2, "M4", subjects=["IRGC"])),
])
def test_rows_that_must_not_be_grouped(a, b) -> None:
    assert build_middle_east_stories([a, b]) == []


def test_a_company_groups_its_own_events_of_one_type() -> None:
    stories = build_middle_east_stories([_row(1, "M2", subjects=["G42"], label="signal"),
                                         _row(2, "M2", subjects=["Khazna"], hours=2),
                                         _row(3, "M2", subjects=["G42"], hours=3)])
    assert [m["row"]["id"] for m in stories[0]["members"]] == [3]
    assert story_keys(_row(9, "M2", subjects=["G42"])) == {"g42"}


# --- Corroboration and stories against a real Postgres -------------------------------

@pytest.mark.skipif(not _DB, reason="requires a reachable Postgres")
def test_event_members_and_story_round_trip() -> None:
    from models.jobs import (
        apply_middle_east_story, clear_middle_east_story_fields,
        get_middle_east_event_members, get_middle_east_story_candidates,
    )
    init_db()
    urls = ["er-me://test_story_a", "er-me://test_story_b"]
    with get_db() as conn:
        conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))
    job_id = create_job(domain="middle_east_market_signal")
    try:
        for i, url in enumerate(urls):
            insert_middle_east_signal_classification(
                job_id, {"url": url, "title": f"t{i}", "published": "2026-10-17T06:00:00+00:00"},
                {"signal": "weak_signal", "reason": "r.", "entities": [],
                 "metadata": {"event": {"type": "M4"},
                              "source": {"name": "eventregistry:me_shipping",
                                         "url": f"https://ft.com/{i}", "outlet": "ft.com",
                                         "event_uri": "eng-test-story"}}})
        members = get_middle_east_event_members(["eng-test-story"])["eng-test-story"]
        assert {m["source_id"] for m in members} == set(urls)
        rows = [r for r in get_middle_east_story_candidates() if r["url"] in urls]
        seed, member = rows
        apply_middle_east_story(seed["id"], development={"count": 1, "members": []},
                                member_ids=[member["id"]])
        rows = {r["url"]: r for r in get_middle_east_story_candidates() if r["url"] in urls}
        assert "story_development" in rows[seed["url"]]["metadata"]
        assert rows[member["url"]]["metadata"]["in_story"] == seed["id"]
        assert clear_middle_east_story_fields() >= 2
        rows = [r for r in get_middle_east_story_candidates() if r["url"] in urls]
        assert all("in_story" not in r["metadata"] and "story_development" not in r["metadata"]
                   for r in rows)
    finally:
        with get_db() as conn:
            conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))




# --- Fixes from the four-reviewer test (2026-10-22) -----------------------------------

@pytest.mark.parametrize("sentence, text, ok", [
    ("Dubai launched an AED 2.5 million championship.", "launches AED2.5M Create AI Agents", True),
    ("The prize is $680,000.", "a prize of $680k for the winner", True),
    ("The prize is $690,000.", "a prize of $680k for the winner", False),
    ("It won a $137 million contract.", "a contract worth more than $137 million", True),
    ("It runs 300 chips.", "It runs GB300 chips.", False),
])
def test_quantities_match_by_value(sentence, text, ok) -> None:
    from pipeline.middle_east_signal_classifier import _numbers_supported, normalize_text
    assert _numbers_supported(sentence, normalize_text(text)) is ok


@pytest.mark.parametrize("by", ["two people aware of the company's plans",
                                "sources familiar with the matter", "unnamed officials"])
def test_anonymous_confirmers_are_unnamed(by) -> None:
    body = _BODY + f" {by} said the campus is live."
    ex, _ = _check(_answer(confirmation={"level": "named_source", "by": by}), body=body)
    assert ex["confirmation"] == {"level": "unnamed"}


def test_the_publishing_outlet_never_confirms_its_own_story() -> None:
    names, tickers = _index()
    body = _BODY + " TokenPost reports the campus is live."
    ex, _ = check_extraction(_answer(confirmation={"level": "official", "by": "TokenPost"}),
                             "G42 | TokenPost", body, names, tickers, outlet="tokenpost")
    assert ex["confirmation"] == {"level": "named_source", "by": "TokenPost"}
    # A name only in the headline ("... | TokenPost") is not in the body.
    ex, _ = check_extraction(_answer(confirmation={"level": "official", "by": "TokenPost"}),
                             "G42 | TokenPost", _BODY, names, tickers, outlet="tokenpost")
    assert ex["confirmation"] == {"level": "unnamed"}


def test_outlet_name() -> None:
    from pipeline.middle_east_signal_classifier import outlet_name
    assert outlet_name(_er("1", "t", outlet="tokenpost.com"), "eventregistry:me_shipping") == "tokenpost"
    assert outlet_name({"metadata": {}}, "wam_wire") == "wam"


def test_country_null_string_is_absent() -> None:
    from pipeline.middle_east_signal_classifier import country_name
    assert country_name("null") is None and country_name("UAE") == "UAE"


def test_read_through_only_for_codes_the_spec_ties_to_stock_moves() -> None:
    names, _ = _index()
    # A construction contract moves no spec row...
    assert read_through(_ex("m2_deal_or_construction", news="good", deal_level=4), names) == []
    # ...ships attacked do: the spec row's own words (review point 24).
    assert {a["ticker"] for a in read_through(_ex("m4_ship_incident"), names)} == {"MU", "INTC"}
    # A helium restart reverses the direction AND the wording.
    good = {a["ticker"]: a for a in read_through(_ex("m5_qatar_force_majeure", news="good"), names)}
    assert good["MU"]["share_price"] == "rise"
    assert good["MU"]["relationship"] == "Helium supply to chip factories is recovering"
    assert good["LIN"]["share_price"] == "fall"


def test_relationships_shown_to_readers_carry_no_spec_references() -> None:
    names, _ = _index()
    qatar = {"name": "QatarEnergy", "ticker": None, "country": "Qatar", "tracked": True}
    for a in read_through(_ex("m5_qatar_force_majeure", companies=[qatar]), names):
        assert "spec" not in a["relationship"].lower(), a


def test_a_row_without_a_published_time_never_joins_a_story() -> None:
    a = _row(1, "M4", "Strait of Hormuz", label="signal")
    b = _row(2, "M4", "Strait of Hormuz")
    b["published"] = None
    assert build_middle_east_stories([a, b]) == []


def test_sec_rows_carry_event_code_issuer_and_summary() -> None:
    r = judge_sec_stake(_sec("SCHEDULE 13G", "NVIDIA Corp", 5.2), _UNIVERSE)
    assert r["metadata"]["event"] == {"type": "M3", "code": "m3_investment"}
    assert r["metadata"]["summary"].startswith("Mubadala Investment Company filed a new US SEC")
    noise = judge_sec_stake(_sec("SCHEDULE 13D/A", "Evotec SE", 0.0), _UNIVERSE)
    assert {"name": "Evotec SE", "ticker": None, "country": None, "role": "subject"} in noise["entities"]
    assert "event" not in noise["metadata"]


@pytest.mark.parametrize("body, label", [
    ("OrbitronAI and Aramco Digital will commercialise agentic AI for industry.", "noise"),
    ("Estithmar will build MEEZA's new data centre in two phases.", "weak_signal"),
])
def test_a_data_centre_deal_must_involve_hardware(body, label) -> None:
    # A construction update (no deal level): only the hardware test decides.
    got, _, _ = decide_label(_ex("m2_deal_or_construction", companies=[_G42]), "", body)
    assert got == label



# --- Field naming and the 403-article run fixes (2026-10-22) --------------------------

def test_an_old_event_is_a_repeat_by_its_own_date() -> None:
    """New = happened within 7 days of the article - from the event's date,
    not a yes/no from the model. No date is judged new."""
    old = _ex("m4_strike_on_site", place="Ras Laffan", country="Qatar", day="2026-09-01")
    assert decide_label(old, "", None, None, "2026-10-17")[:2] == ("noise", "repeat")
    m8 = _ex("m8_enforcement", day="2026-09-01", companies=[_G42])
    assert decide_label(m8, "", None, None, "2026-10-17")[:2] == ("weak_signal", "m8_repeat")
    recent = _ex("m4_strike_on_site", place="Ras Laffan", country="Qatar", day="2026-10-12")
    assert decide_label(recent, "", None, None, "2026-10-17")[0] == "signal"
    # Undated: not a repeat, but an attack alert needs its day - stored as weak.
    undated = _ex("m4_strike_on_site", place="Ras Laffan", country="Qatar")
    assert decide_label(undated, "", None, None, "2026-10-17")[0] == "weak_signal"


def test_an_event_date_after_the_article_is_dropped() -> None:
    from pipeline.middle_east_signal_classifier import event_date_checked
    assert event_date_checked("2026-10-17", "2026-10-17T10:00:00+00:00") == "2026-10-17"
    assert event_date_checked("2026-10-25", "2026-10-17T10:00:00+00:00") is None
    assert event_date_checked("17 October", "2026-10-17") is None
    # A month alone ("last month") is kept; a month after the article is not.
    assert event_date_checked("2026-09", "2026-10-13") == "2026-09"
    assert event_date_checked("2026-11", "2026-10-13") is None


def test_an_event_dated_only_last_month_is_old() -> None:
    """Point 40: a force majeure "extended last month" mentioned as background."""
    bg = _ex("m5_qatar_force_majeure", by="QatarEnergy", day="2026-09")
    assert decide_label(bg, "", None, None, "2026-10-13")[:2] == ("noise", "repeat")
    # This month is not old, but not shown new either: kept, not alerted
    # (Edison's 28 Sep extension re-alerted as "2026-10", 2026-10-24).
    this_month = _ex("m5_qatar_force_majeure", by="QatarEnergy", day="2026-10")
    assert decide_label(this_month, "", None, None, "2026-10-13")[0] == "weak_signal"


def test_a_month_alone_never_joins_two_reports() -> None:
    a = _ev(1, "m4_strike_on_site", "Ras Laffan", label="signal")
    b = _ev(2, "m4_strike_on_site", "Ras Laffan", hours=24 * 5)
    a["metadata"]["event"]["date"] = b["metadata"]["event"]["date"] = "2026-10"
    assert find_same_event_groups([a, b]) == []          # 5 days apart, no exact day


def test_a_gulf_rule_must_be_about_ai_data_or_chips() -> None:
    ex = _ex("m6_binding_rule", effective_date="1 January 2027")
    boats = "The Red Sea Authority issued requirements for recreational boat chartering."
    assert decide_label(ex, "Boat chartering rules", boats)[:2] == ("noise", "m6_not_ai_or_data")
    data = "The rule requires personal data of Saudi residents to be stored in the Kingdom."
    assert decide_label(ex, "Data rule", data)[0] == "signal"


@pytest.mark.parametrize("a, b", [
    ("eastern Strait of Hormuz", "east of the Strait of Hormuz"),
    ("Strait of Hormuz and surrounding waterways", "south of the Strait of Hormuz"),
])
def test_place_keys_ignore_direction_words(a, b) -> None:
    from pipeline.middle_east_signal_classifier import place_key
    assert place_key(a) == place_key(b) == "strait hormuz"


def test_gulf_countries_and_the_coalition_never_make_a_story_alone() -> None:
    a = _row(1, "M6", "UAE", subjects=["UAE Government"], label="signal")
    b = _row(2, "M6", "UAE", hours=2)
    assert build_middle_east_stories([a, b]) == []
    c = _row(3, "M4", subjects=["Coalition to Restore Legitimacy in Yemen"], label="signal")
    d = _row(4, "M4", subjects=["Coalition to Restore Legitimacy in Yemen"], hours=2)
    assert build_middle_east_stories([c, d]) == []



def test_a_reaction_headline_is_never_a_signal() -> None:
    ex = _ex("m4_strike_on_site", place="Ras Laffan", country="Qatar", day="2026-10-14",
             summary="A Houthi missile hit the Ras Laffan LNG plant.")
    title = "UN Security Council Members Strongly Condemn Houthi Attacks Against Saudi Arabia"
    label, rule, why = decide_label(ex, title)
    assert (label, rule) == ("weak_signal", "m4_strike_confirmed_reaction_headline")
    assert decide_label(ex, "Missile hits Ras Laffan gas plant")[0] == "signal"



@pytest.mark.parametrize("code, place, by, body, status, label, rule", [
    # A closure already alerted: the IRGC saying it again restates it (review point 1).
    ("m4_waterway_closed", "Strait of Hormuz", "IRGC Navy", "The strait remains closed.",
     {"hormuz": {"closed": True, "as_of": "since 12 October 2026", "said": {}}},
     "noise", "m4_waterway_status_unchanged"),
    # The Red Sea has no known status: the Houthis closing it is news.
    ("m4_waterway_closed", "Bab el-Mandeb", "Houthi military spokesman", "The Bab el-Mandeb is closed.",
     {}, "signal", "m4_waterway_closed_declared_by_controlling_party"),
    # The US saying Hormuz is open is the other side of a contested status
    # (review point 2) - shown, not an alert...
    ("m4_waterway_reopened", "Strait of Hormuz", "US Navy Captain Tim Hawkins",
     "The strait is completely open.",
     {"hormuz": {"said": {True: {"by": "IRGC Navy", "date": date(2026, 10, 17)}}}},
     "weak_signal", "m4_waterway_reopened_contested"),
    # ...unless ship-tracking figures in the article back it.
    ("m4_waterway_reopened", "Strait of Hormuz", "US Navy Captain Tim Hawkins",
     "The strait is open: Kpler data showed 400 vessels crossed in the past week.", {},
     "signal", "m4_waterway_reopened_confirmed_by_ship_counts"),
    # Iran reopening the strait is a change.
    ("m4_waterway_reopened", "Strait of Hormuz", "IRGC Navy", "Ships may pass from today.", {},
     "signal", "m4_waterway_reopened_declared_by_controlling_party"),
    # After a stored reopening, a new closure is a change again.
    ("m4_waterway_closed", "Strait of Hormuz", "IRGC Navy", "The strait is closed.",
     {"hormuz": {"closed": False, "as_of": "since 30 October 2026"}},
     "signal", "m4_waterway_closed_declared_by_controlling_party"),
])
def test_waterway_alerts_need_a_change_of_status(code, place, by, body, status, label, rule) -> None:
    ex = _ex(code, place=place, summary=body)
    ex["confirmation"] = {"level": "official", "by": by}
    got_label, got_rule, why = decide_label(ex, "Hormuz", body, status)
    assert (got_label, got_rule) == (label, rule)
    if rule.endswith("_contested"):
        assert why == "Contested: IRGC Navy said on 17 October that the Strait of Hormuz is closed."


def test_remains_closed_with_no_history_is_judged_by_its_date() -> None:
    """With nothing stored, "closed since March" is old by its own date -
    no hand-set starting status."""
    ex = _ex("m4_waterway_closed", place="Strait of Hormuz", day="2026-03-02", by="IRGC Navy")
    assert decide_label(ex, "", "The strait remains closed.", {}, "2026-10-17")[:2] == ("noise", "repeat")


def test_current_waterway_status_comes_from_the_latest_alerted_change() -> None:
    from pipeline.middle_east_signal_classifier import current_waterway_status
    closed = _ev(1, "m4_waterway_closed", "Strait of Hormuz", label="signal")
    reopened = _ev(2, "m4_waterway_reopened", "the Strait of Hormuz", hours=48, label="signal")
    weak = _ev(3, "m4_waterway_closed", "Strait of Hormuz", hours=72)          # not alerted
    folded = _ev(4, "m4_waterway_closed", "Strait of Hormuz", hours=80, label="signal")
    folded["metadata"]["collapsed_into"] = 2
    status = current_waterway_status([weak, reopened, closed, folded])
    assert status["hormuz"]["closed"] is False and status["hormuz"]["as_of"].startswith("since ")
    assert current_waterway_status([]) == {}


# --- Review of 2026-10-23 -------------------------------------------------------------

def test_the_alert_country_comes_from_the_event_country_not_a_city_list() -> None:
    """Dhahran is in no list: its country, from the extraction, decides."""
    ex = _ex("m4_strike_on_site", place="Dhahran", country="Saudi Arabia", day="2026-10-14")
    assert decide_label(ex)[:2] == ("signal", "m4_strike_confirmed")
    no_country = _ex("m4_strike_on_site", place="Dhahran")
    assert decide_label(no_country)[:2] == ("weak_signal", "m4_strike_outside_alert_countries")


def test_an_outlet_written_with_spaces_still_cannot_confirm() -> None:
    names, tickers = _index()
    body = _BODY + " NDTV Profit reported the switch-on."
    ex, _ = check_extraction(_answer(confirmation={"level": "official", "by": "NDTV Profit"}),
                             "G42 switches on", body, names, tickers, outlet="ndtvprofit")
    assert ex["confirmation"]["level"] == "named_source"


@pytest.mark.parametrize("place, country, outside", [
    ("Khamis Mushait", "Saudi Arabia", False),
    ("Ras Laffan", None, False),                 # no country given is not evidence
    ("Strait of Hormuz", None, False),           # the region's sea routes
    ("Gulf of Oman", None, False),
    ("Black Sea", None, True),                   # another sea
    ("Sulina Anchorage", "Romania", True),       # another country
])
def test_region_is_judged_by_country_or_sea_route(place, country, outside) -> None:
    rule = decide_label(_ex("m4_ship_incident", place=place, country=country))[1]
    assert (rule == "m4_outside_middle_east") is outside


def test_place_is_kept_when_the_text_names_it_without_its_kind_word() -> None:
    names, tickers = _index()
    body = "Iran's navy said a tanker hit a mine in Hormuz on Saturday."
    ex, dropped = check_extraction(
        _answer(event="m4_ship_incident", place="Strait of Hormuz", summary="A tanker hit a mine.",
                reason_facts="A tanker hit a mine."),
        "Tanker hit", body, names, tickers)
    assert ex["event"]["place"] == "Strait of Hormuz" and "event.place" not in dropped
    ex, dropped = check_extraction(
        _answer(event="m4_ship_incident", place="Strait of Malacca", summary="A tanker hit a mine.",
                reason_facts="A tanker hit a mine."),
        "Tanker hit", body, names, tickers)
    assert "place" not in ex["event"] and "event.place" in dropped


def test_abbreviation_dots_do_not_hide_a_confirmer() -> None:
    from pipeline.middle_east_signal_classifier import normalize_text
    body = normalize_text("The U.K. Maritime Trade Operations Agency (UKMTO) said 12 ships were hit.")
    assert "uk maritime trade operations agency" in body
    # Decimals and multi-letter abbreviations keep their dots.
    assert normalize_text("$1.5 billion, Acme Inc. said") == "$1.5 billion, acme inc. said"


def test_an_intel_site_event_written_as_intel_names_intel_only() -> None:
    names, _ = _index()
    intel = {"name": "Intel", "ticker": "INTC", "country": None, "tracked": False}
    got = read_through(_ex("m7_site_incident", companies=[intel]), names)
    assert [a["ticker"] for a in got] == ["INTC"]
    # With no company named, the spec's Israel row stands.
    assert {a["ticker"] for a in read_through(_ex("m7_site_incident"), names)} == {"NVDA", "INTC", "TSEM"}


def test_a_funds_holdings_are_never_its_read_through() -> None:
    """Together AI's Abu Dhabi office named GlobalFoundries through
    Mubadala (review point 23)."""
    names, _ = _index()
    mubadala = {"name": "Mubadala", "ticker": None, "country": "UAE", "tracked": True}
    deal = _ex("m2_deal_or_construction", news="good", deal_level=2, companies=[mubadala])
    assert "GFS" not in {a["ticker"] for a in read_through(deal, names)}


def test_an_intel_fab_event_names_intel_only() -> None:
    """Review point 22: every Intel fab row named Nvidia, Intel and Tower."""
    names, _ = _index()
    fab = {"name": "Intel Kiryat Gat", "ticker": None, "country": "Israel", "tracked": True}
    got = read_through(_ex("m7_site_incident", companies=[fab]), names)
    assert [(a["ticker"], a["share_price"]) for a in got] == [("INTC", "fall")]


def test_a_site_incident_and_its_follow_up_days_later_are_one_story() -> None:
    blast = _ev(1, "m7_site_incident", "Kiryat Gat", subjects=["Intel Kiryat Gat"], label="signal")
    leak = _ev(2, "m7_site_incident", "Kiryat Gat", subjects=["Intel Kiryat Gat"], hours=5 * 24)
    [story] = build_middle_east_stories([blast, leak])
    assert story["seed"]["id"] == 1
    # Other event types keep the 72-hour window.
    a = _row(3, "M4", "Strait of Hormuz", label="signal")
    b = _row(4, "M4", "Strait of Hormuz", hours=5 * 24)
    assert build_middle_east_stories([a, b]) == []


def test_the_better_confirmed_report_leads_a_story() -> None:
    first = _ev(1, "m4_ship_incident", "Strait of Hormuz", conf="named_source")
    official = _ev(2, "m4_ship_incident", "Strait of Hormuz", hours=6, conf="official")
    [story] = build_middle_east_stories([first, official])
    assert story["seed"]["id"] == 2


def _history(ticker_moves):
    """{ticker: [daily % moves]} -> price-history rows, oldest move first."""
    out = {}
    for t, moves in ticker_moves.items():
        price, rows = 100.0, []
        for i, m in enumerate([0.0] + moves):
            price *= 1 + m / 100
            rows.append({"date": f"2026-{8 + (i // 28):02d}-{1 + i % 28:02d}", "adjusted_close": price})
        out[t] = rows
    return out


def test_an_unusual_day_is_oil_or_several_stocks_far_beyond_normal() -> None:
    from pipeline.middle_east_signal_classifier import missed_move_lines, unusual_market_days
    quiet = [0.5, -0.5] * 15                              # normal move 0.5%
    hist = _history({"BNO": quiet + [2.0, 0.4],           # oil 4x normal on day 31
                     "INTC": quiet + [0.2, -2.0], "MU": quiet + [0.1, -1.9], "NVDA": quiet + [0.3, -1.8],
                     "AMD": quiet + [0.2, 2.5]})          # 4 stocks unusual on day 32; one alone earlier
    days = unusual_market_days(hist, ["BNO", "INTC", "MU", "NVDA", "AMD"], since="2026-09-01")
    assert [(d["day"], bool(d["oil"]), len(d["stocks"])) for d in days] == [
        ("2026-09-04", True, 0), ("2026-09-05", False, 4)]
    lines = missed_move_lines(days, "BNO", {"2026-09-04": 2})
    assert lines[0].startswith("COVERED 2026-09-04") and lines[1].startswith("COVERED 2026-09-05")
    assert missed_move_lines(days, "BNO", {})[-1] == "missed-move score: 2 of 2 unusual days had no Middle East signal"


def test_a_single_stock_move_is_company_news_not_an_unusual_day() -> None:
    from pipeline.middle_east_signal_classifier import unusual_market_days
    quiet = [0.5, -0.5] * 15
    hist = _history({"BNO": quiet + [0.4], "AMZN": quiet + [5.0], "MU": quiet + [0.3]})
    assert unusual_market_days(hist, ["BNO", "AMZN", "MU"], since="2026-09-01") == []


def test_market_reference_tickers_come_from_the_tables() -> None:
    from pipeline.middle_east_signal_classifier import market_reference_tickers
    tickers = market_reference_tickers(_news_retrieval_company_table())
    assert tickers[0] == "BNO" and {"NVDA", "INTC", "MU", "GFS"} <= set(tickers)
    assert len(tickers) == len(set(tickers))


# --- Same-event duplicates: one row per event (tombstones) ---------------------------

from pipeline.middle_east_signal_classifier import (  # noqa: E402
    find_same_event_groups,
    folded_corroboration,
)


def _ev(id_, code, place=None, subjects=(), hours=0, label="weak_signal", conf="official", count=0):
    row = _row(id_, code[:2].upper(), place, subjects, hours, label, count)
    row["metadata"]["event"]["code"] = code
    row["metadata"]["confirmation"] = {"level": conf}
    row["metadata"]["source"] = {"name": "eventregistry:me_shipping", "url": f"https://o{id_}.com/a",
                                 "outlet": f"o{id_}.com"}
    return row


def test_three_reports_of_the_hormuz_closure_fold_into_the_best_one() -> None:
    rows = [_ev(1, "m4_waterway_closed", "Strait of Hormuz", hours=0, conf="unnamed"),
            _ev(2, "m4_waterway_closed", "the Strait of Hormuz", hours=3, label="signal"),
            _ev(3, "m4_waterway_closed", "Strait of Hormuz", hours=20, label="signal", count=4)]
    [group] = find_same_event_groups(rows)
    # A signal before a weak one; among signals, the better corroborated.
    assert group["kept"]["id"] == 3
    assert sorted(d["id"] for d in group["duplicates"]) == [1, 2]
    corr = folded_corroboration(group["kept"], group["duplicates"])
    assert corr["count"] == 2 and {s["via"] for s in corr["siblings"]} == {"same_event"}
    assert {s["outlet"] for s in corr["siblings"]} == {"o1.com", "o2.com"}


@pytest.mark.parametrize("a, b", [
    # Different tankers hit near Hormuz on one day are separate events.
    (_ev(1, "m4_ship_incident", "Strait of Hormuz"), _ev(2, "m4_ship_incident", "Strait of Hormuz", hours=2)),
    # Two statements about one place, by different speakers.
    (_ev(1, "m4_threat_or_statement", "Strait of Hormuz", subjects=["Iran"]),
     _ev(2, "m4_threat_or_statement", "Strait of Hormuz", subjects=["IMO"], hours=3)),
    # Same place, different event code.
    (_ev(1, "m4_waterway_closed", "Strait of Hormuz"), _ev(2, "m4_threat_or_statement", "Strait of Hormuz")),
    # More than 48 hours apart.
    (_ev(1, "m7_site_incident", "Kiryat Gat"), _ev(2, "m7_site_incident", "Kiryat Gat", hours=60)),
])
def test_rows_that_are_not_the_same_event(a, b) -> None:
    assert find_same_event_groups([a, b]) == []


def test_ship_attacks_fold_on_a_shared_ship_or_company() -> None:
    a = _ev(1, "m4_ship_incident", "Strait of Hormuz", subjects=["Safeen Principal"], label="signal")
    b = _ev(2, "m4_ship_incident", "off Jubail", subjects=["Safeen Principal"], hours=5)
    [group] = find_same_event_groups([a, b])
    assert group["kept"]["id"] == 1


def test_a_company_event_folds_on_the_company() -> None:
    a = _ev(1, "m2_deal_or_construction", subjects=["Humain", "MIS"])
    b = _ev(2, "m2_deal_or_construction", subjects=["Humain"], hours=30)
    assert len(find_same_event_groups([a, b])) == 1


@pytest.mark.skipif(not _DB, reason="requires a reachable Postgres")
def test_collapse_tombstones_duplicates_and_keeps_them() -> None:
    from models.jobs import collapse_middle_east_same_event
    init_db()
    urls = ["er-me://test_fold_kept", "er-me://test_fold_dup"]
    with get_db() as conn:
        conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))
    job_id = create_job(domain="middle_east_market_signal")
    try:
        for url in urls:
            insert_middle_east_signal_classification(
                job_id, {"url": url, "title": url, "published": "2026-10-17T06:00:00+00:00"},
                {"signal": "signal", "reason": "r.", "entities": [], "metadata": {"rule": "x"}})
        with get_db() as conn:
            ids = {r["source_id"]: r["id"] for r in conn.execute(
                "SELECT id, source_id FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))}
        corr = {"count": 1, "siblings": [{"outlet": "o.com", "title": "t", "url": "u", "via": "same_event"}]}
        assert collapse_middle_east_same_event(ids[urls[0]], corr, [ids[urls[1]]]) == 1
        with get_db() as conn:
            rows = {r["source_id"]: r for r in conn.execute(
                "SELECT source_id, signal_detection, signal_reason, metadata FROM agent_classifications"
                " WHERE source_id = ANY(%s)", (urls,))}
        kept, dup = rows[urls[0]], rows[urls[1]]
        assert kept["signal_detection"] == "signal" and kept["metadata"]["corroboration"] == corr
        assert dup["signal_detection"] == "noise" and dup["metadata"]["collapsed_into"] == ids[urls[0]]
        assert "folded into row" in dup["signal_reason"]
        # Still there: the already-judged skip keeps seeing it.
        assert get_existing_middle_east_signal_source_ids(urls) == set(urls)
    finally:
        with get_db() as conn:
            conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))


# --- /results "events" view: one row per event or story --------------------------------

@pytest.mark.skipif(not _DB, reason="requires a reachable Postgres")
def test_results_events_view_hides_story_members_and_folded_duplicates() -> None:
    from models.jobs import get_results_summary, list_all_results
    init_db()
    urls = ["er-me://test_view_lead", "er-me://test_view_member", "er-me://test_view_dup",
            "test://view_null_metadata"]
    with get_db() as conn:
        conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))
    job_id = create_job(domain="middle_east_market_signal")
    day = "2031-01-15"            # a date no real row has, so the window holds only these
    try:
        for url, label in zip(urls[:3], ("signal", "weak_signal", "noise")):
            insert_middle_east_signal_classification(
                job_id, {"url": url, "title": url, "published": f"{day}T06:00:00+00:00"},
                {"signal": label, "reason": "r.", "entities": [], "metadata": {"rule": "x"}})
        with get_db() as conn:
            ids = {r["source_id"]: r["id"] for r in conn.execute(
                "SELECT id, source_id FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))}
            conn.execute("UPDATE agent_classifications SET metadata = metadata || jsonb_build_object('in_story', %s::int) WHERE id = %s",
                         (ids[urls[0]], ids[urls[1]]))
            conn.execute("UPDATE agent_classifications SET metadata = metadata || jsonb_build_object('collapsed_into', %s::int) WHERE id = %s",
                         (ids[urls[0]], ids[urls[2]]))
            # A row of another domain with NULL metadata must not be dropped by the view.
            conn.execute("INSERT INTO agent_classifications (job_id, source_type, source_id, url, title,"
                         " signal_detection, published) VALUES (%s, 'news', %s, %s, 't', 'noise', %s)",
                         (job_id, urls[3], urls[3], f"{day}T06:00:00+00:00"))
        window = {"published_from": day, "published_to": day}
        events = list_all_results(source_type="middle_east_market_signal", **window)["results"]
        assert [r["source_id"] for r in events] == [urls[0]]
        rows = list_all_results(source_type="middle_east_market_signal", view="rows", **window)["results"]
        assert {r["source_id"] for r in rows} == set(urls[:3])
        news = list_all_results(source_type="news", **window)["results"]
        assert [r["source_id"] for r in news] == [urls[3]]
        # The counts follow the same view as the rows.
        events_counts = get_results_summary("middle_east_market_signal", day, day)["observations"]
        assert (events_counts["total"], events_counts["high"], events_counts["weak"]) == (1, 1, 0)
        row_counts = get_results_summary("middle_east_market_signal", day, day, view="rows")["observations"]
        assert (row_counts["total"], row_counts["high"], row_counts["weak"], row_counts["noise"]) == (3, 1, 1, 1)
    finally:
        with get_db() as conn:
            conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))


@pytest.mark.skipif(not _DB, reason="requires a reachable Postgres")
def test_results_events_view_paginates_over_visible_rows_only() -> None:
    """limit=1 pages walk every visible row once, in order, skipping hidden
    rows - including a hidden row that sorts BETWEEN two visible ones."""
    from models.jobs import list_all_results
    init_db()
    urls = [f"er-me://test_page_{i}" for i in range(5)]
    with get_db() as conn:
        conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))
    job_id = create_job(domain="middle_east_market_signal")
    day = "2031-02-15"
    try:
        for i, url in enumerate(urls):          # newest first: 4, 3, 2, 1, 0
            insert_middle_east_signal_classification(
                job_id, {"url": url, "title": url, "published": f"{day}T0{i}:00:00+00:00"},
                {"signal": "weak_signal", "reason": "r.", "entities": [], "metadata": {"rule": "x"}})
        with get_db() as conn:
            ids = {r["source_id"]: r["id"] for r in conn.execute(
                "SELECT id, source_id FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))}
            conn.execute("UPDATE agent_classifications SET metadata = metadata || jsonb_build_object('in_story', %s::int) WHERE id = %s",
                         (ids[urls[4]], ids[urls[3]]))
            conn.execute("UPDATE agent_classifications SET metadata = metadata || jsonb_build_object('collapsed_into', %s::int) WHERE id = %s",
                         (ids[urls[4]], ids[urls[1]]))

        def walk(view):
            seen, cursor = [], None
            while True:
                page = list_all_results(source_type="middle_east_market_signal", limit=1, cursor=cursor,
                                        published_from=day, published_to=day, view=view)
                seen += [r["source_id"] for r in page["results"]]
                cursor = page["next_cursor"]
                if not cursor:
                    return seen
        assert walk("events") == [urls[4], urls[2], urls[0]]
        assert walk("rows") == [urls[4], urls[3], urls[2], urls[1], urls[0]]
    finally:
        with get_db() as conn:
            conn.execute("DELETE FROM agent_classifications WHERE source_id = ANY(%s)", (urls,))


# --- Generalised fixes (2026-10-23): event date, combined confirmation ----------------

def test_reports_of_one_event_fold_by_its_own_date_days_apart() -> None:
    """A sinking on the 13th re-reported on the 19th is one event."""
    a = _ev(1, "m4_ship_sunk_or_crew_killed", "Strait of Hormuz", subjects=["GFS Pride"], label="signal")
    b = _ev(2, "m4_ship_sunk_or_crew_killed", "Strait of Hormuz", subjects=["GFS Pride"], hours=6 * 24)
    for r in (a, b):
        r["metadata"]["event"]["date"] = "2026-10-13"
    assert len(find_same_event_groups([a, b])) == 1
    c = _ev(3, "m4_ship_sunk_or_crew_killed", "Strait of Hormuz", subjects=["GFS Pride"], hours=6 * 24)
    c["metadata"]["event"]["date"] = "2026-10-19"           # a different day: another event
    assert find_same_event_groups([a, c]) == []


def test_the_same_attack_count_repeated_for_days_is_one_event() -> None:
    rows = [_ev(i, "m4_attack_tally", "Strait of Hormuz", hours=24 * i, label="signal") for i in range(3)]
    for r in rows:
        r["metadata"]["event"]["date"] = "2026-10-16"
    [group] = find_same_event_groups(rows)
    assert len(group["duplicates"]) == 2


@pytest.mark.parametrize("confs, expected", [
    # Lead unnamed, another report official -> official.
    ([{"level": "unnamed"}, {"level": "official", "by": "UKMTO"}], ["UKMTO"]),
    # Two different named sources -> confirmed.
    ([{"level": "named_source", "by": "IRGC Navy"},
      {"level": "named_source", "by": "Maritime and Port Authority of Singapore"}],
     ["IRGC Navy", "Maritime and Port Authority of Singapore"]),
    # One source repeated by many outlets -> no change.
    ([{"level": "named_source", "by": "IRGC Navy"}, {"level": "named_source", "by": "IRGC"}], None),
    # Unnamed only -> no change.
    ([{"level": "unnamed"}, {"level": "unnamed"}], None),
    # The lead is already official -> nothing to raise.
    ([{"level": "official", "by": "UKMTO"}, {"level": "named_source", "by": "Reuters"}], None),
])
def test_confirmed_across_reports(confs, expected) -> None:
    from pipeline.middle_east_signal_classifier import confirmed_across_reports
    reports = [{"metadata": {"confirmation": c}} for c in confs]
    got = confirmed_across_reports(reports)
    assert (got["by"] if got else None) == expected


def test_combined_confirmation_raises_the_lead_rows_label_and_says_who() -> None:
    from pipeline.middle_east_signal_classifier import relabel_confirmed
    names, _ = _index()
    kept = _ev(1, "m4_ship_sunk_or_crew_killed", "Strait of Hormuz", conf="unnamed")
    kept["metadata"]["confirmation"] = {"level": "unnamed"}
    kept["metadata"]["event"]["date"] = "2026-10-13"
    kept["signal_reason"] = "Nine ships were attacked and one sank. Only unnamed sources report it so far."
    got = relabel_confirmed(kept, {"level": "official", "by": ["UKMTO"]}, names)
    assert got[0] == "signal" and got[1].endswith("_confirmed_across_reports")
    assert got[2].startswith("Nine ships were attacked and one sank. A ship was sunk")
    assert got[2].endswith("Confirmed across reports by UKMTO.")
    # Already a signal: nothing to raise.
    kept["signal_detection"] = "signal"
    assert relabel_confirmed(kept, {"level": "official", "by": ["UKMTO"]}, names) is None


def test_one_attack_coded_two_ways_is_one_event() -> None:
    a = _ev(1, "m4_ship_incident", "Strait of Hormuz", subjects=["IRGC Navy"], label="signal")
    b = _ev(2, "m4_strike_on_site", "Strait of Hormuz", subjects=["IRGC Navy"], hours=5)
    for r in (a, b):
        r["metadata"]["event"]["date"] = "2026-10-22"
    assert len(find_same_event_groups([a, b])) == 1
    # A threat about the same place is not the attack.
    c = _ev(3, "m4_threat_or_statement", "Strait of Hormuz", subjects=["IRGC Navy"], hours=5)
    assert find_same_event_groups([a, c]) == []


def test_a_tracked_company_in_both_headlines_joins_one_strike() -> None:
    names, _ = _index()
    a = _ev(1, "m4_strike_on_site", "Aramco compound, Eastern Province", label="signal")
    b = _ev(2, "m4_strike_on_site", "Dhahran", hours=20)
    a["title"] = "Saudi Ministry of Energy reports limited fire in building at Aramco compound"
    b["title"] = "Houthis vow more attacks after Aramco headquarters strike"
    a["metadata"]["event"]["date"], b["metadata"]["event"]["date"] = "2026-10-13", "2026-10-14"
    assert find_same_event_groups([a, b]) == []                 # without the company table
    assert len(find_same_event_groups([a, b], names)) == 1


@pytest.mark.parametrize("day_b, by_b, same", [
    ("2026-10-17", "IRGC Navy", True),      # same day, place and source
    ("2026-10-18", "IRGC Navy", False),     # the next day's mine strike is another event
    ("2026-10-17", "UKMTO", False),         # a different source may be a different tanker
])
def test_unnamed_ship_attacks_fold_only_on_same_day_place_and_source(day_b, by_b, same) -> None:
    a = _ev(1, "m4_ship_incident", "Strait of Hormuz", label="signal")
    b = _ev(2, "m4_ship_incident", "southern Strait of Hormuz", hours=10)
    a["metadata"]["event"]["date"], b["metadata"]["event"]["date"] = "2026-10-17", day_b
    a["metadata"]["confirmation"] = {"level": "official", "by": "IRGC Navy"}
    b["metadata"]["confirmation"] = {"level": "official", "by": by_b}
    assert (len(find_same_event_groups([a, b])) == 1) is same


def test_a_place_inside_another_is_the_same_place_for_a_count() -> None:
    a = _ev(1, "m4_attack_tally", "Persian Gulf and Strait of Hormuz", label="signal")
    b = _ev(2, "m4_attack_tally", "Strait of Hormuz", hours=12, label="signal")
    a["metadata"]["event"]["date"], b["metadata"]["event"]["date"] = "2026-10-21", "2026-10-22"
    assert len(find_same_event_groups([a, b])) == 1
    c = _ev(3, "m4_attack_tally", "Red Sea", hours=12, label="signal")
    c["metadata"]["event"]["date"] = "2026-10-22"
    assert find_same_event_groups([a, c]) == []


def test_a_country_alone_never_joins_two_strikes() -> None:
    a = _ev(1, "m4_strike_on_site", "Shaybah oil field, Saudi Arabia", label="signal")
    b = _ev(2, "m4_strike_on_site", "Saudi Arabia", hours=3)
    for r in (a, b):
        r["metadata"]["event"]["date"] = "2026-10-21"
    assert find_same_event_groups([a, b]) == []


def test_separate_weak_ship_attacks_add_up_to_one_wave_alert() -> None:
    """Review point 13: a week of tanker attacks stayed weak, row by row."""
    from pipeline.middle_east_signal_classifier import attack_wave_raises
    rows = []
    for i, day in enumerate(["2026-10-14", "2026-10-16", "2026-10-17", "2026-10-18", "2026-10-19"]):
        r = _ev(i, "m4_ship_incident", "Strait of Hormuz", hours=24 * i)
        r["metadata"]["event"]["date"] = day
        rows.append(r)
    raises = attack_wave_raises(rows)
    # The third attack in 7 days completes the wave; the 4th and 5th are the
    # same week - one alert, not one per ship.
    assert [r[0] for r in raises] == [2]
    assert raises[0][1] == "signal" and "attack number 3" in raises[0][3]
    # Spread out over weeks: no wave.
    spread = []
    for i, day in enumerate(["2026-09-01", "2026-09-12", "2026-09-25"]):
        r = _ev(10 + i, "m4_ship_incident", "Strait of Hormuz")
        r["metadata"]["event"]["date"] = day
        spread.append(r)
    assert attack_wave_raises(spread) == []
    # Other waterways are counted apart.
    red = []
    for i in range(2):
        r = _ev(20 + i, "m4_ship_incident", "Red Sea")
        r["metadata"]["event"]["date"] = "2026-10-17"
        red.append(r)
    assert all(rid not in (20, 21) for rid, *_ in attack_wave_raises(rows[:2] + red))


@pytest.mark.parametrize("place, outside", [
    ("off Saudi Arabia's Gulf coast", False), ("Gulf", False), ("Gulf of Mexico", True)])
def test_the_gulf_and_a_named_country_are_in_the_region(place, outside) -> None:
    rule = decide_label(_ex("m4_ship_incident", place=place))[1]
    assert (rule == "m4_outside_middle_east") is outside


def test_a_source_and_its_adjective_are_one_source() -> None:
    from pipeline.middle_east_signal_classifier import _same_source
    assert _same_source("Iran's navy", "Iranian Navy")
    assert _same_source("IRGC Navy", "IRGC")
    assert not _same_source("UKMTO", "Maritime and Port Authority of Singapore")
    assert not _same_source("Saudi Ministry of Energy", "Saudi Ministry of Defence")


def test_a_named_ship_is_one_event_across_a_few_days() -> None:
    a = _ev(1, "m4_ship_sunk_or_crew_killed", "Strait of Hormuz", subjects=["GFS Pride"], label="signal")
    b = _ev(2, "m4_ship_sunk_or_crew_killed", "Strait of Hormuz", subjects=["GFS Pride"], hours=24)
    a["metadata"]["event"]["date"], b["metadata"]["event"]["date"] = "2026-10-13", "2026-10-15"
    assert len(find_same_event_groups([a, b])) == 1


def test_no_wave_alert_in_a_week_that_already_has_a_signal() -> None:
    from pipeline.middle_east_signal_classifier import attack_wave_raises
    rows = []
    for i, (day, label) in enumerate([("2026-10-14", "signal"), ("2026-10-16", "weak_signal"),
                                      ("2026-10-17", "weak_signal")]):
        r = _ev(i, "m4_ship_incident", "Strait of Hormuz", label=label)
        r["metadata"]["event"]["date"] = day
        rows.append(r)
    assert attack_wave_raises(rows) == []


def test_a_tracked_site_as_the_place_names_its_owner() -> None:
    """The model listed no company for an incident at Kiryat Gat; the
    company table knows it as Intel's fab."""
    names, tickers = _index()
    body = "Fire and rescue services responded to hazardous materials detected at the factory in Kiryat Gat."
    ex, _ = check_extraction(
        _answer(event="m7_site_incident", place="Kiryat Gat", companies=[],
                confirmation={"level": "official", "by": "Fire and rescue services"},
                summary="Hazardous materials were detected at the factory.",
                reason_facts="Hazardous materials were detected at the factory."),
        "Rescue services on site", body, names, tickers)
    assert [c["name"] for c in ex["companies"]] == ["Intel Kiryat Gat"]
    assert [a["ticker"] for a in read_through({**ex, "news_effect": "bad"}, names)] == ["INTC"]


def test_other_events_are_checked_like_the_main_event() -> None:
    names, tickers = _index()
    body = ("The IRGC said the Strait of Hormuz is closed to all shipping from today. "
            "UKMTO said three tankers were struck near Hormuz on 17 October.")
    raw = _answer(event="m4_attack_tally", place="Hormuz", date="2026-10-17", country=None,
                  confirmation={"level": "official", "by": "UKMTO"},
                  summary="Three tankers were struck near Hormuz.", reason_facts="Three tankers were struck.",
                  other_events=[
                      {"code": "m4_waterway_closed", "place": "Strait of Hormuz", "date": "2026-10-17",
                       "confirmation": {"level": "official", "by": "IRGC"}, "facts": "The IRGC closed the strait."},
                      {"code": "m4_commentary", "place": "Hormuz"},                    # never an event
                      {"code": "m4_attack_tally", "place": "Hormuz"},                  # same code as main
                      {"code": "m4_threat_or_statement", "place": "Strait of Malacca",  # place not in text
                       "confirmation": {"level": "official", "by": "Someone Absent"}}])
    ex, dropped = check_extraction(raw, "Hormuz closed", body, names, tickers, published="2026-10-17")
    codes = [o["event"]["code"] for o in ex["other_events"]]
    assert codes == ["m4_waterway_closed", "m4_threat_or_statement"]
    assert ex["other_events"][0]["confirmation"] == {"level": "official", "by": "IRGC"}
    assert "place" not in ex["other_events"][1]["event"]
    assert ex["other_events"][1]["confirmation"] == {"level": "unnamed"}
    assert "other_events[3].place" in dropped


def test_the_strongest_event_in_an_article_becomes_its_label(monkeypatch) -> None:
    """Main event a threat (weak); the article also reports a sinking
    confirmed by an authority (signal) - the row is the sinking."""
    answer = json.loads(_answer(
        event="m4_threat_or_statement", place="Strait of Hormuz", date="2026-10-17", country=None,
        companies=[], deal=None, news_effect="bad",
        confirmation={"level": "official", "by": "IRGC"},
        reason_facts="Iran warned ships to stay away.", summary="Iran warned ships to stay away."))
    answer["other_events"] = [{"code": "m4_ship_sunk_or_crew_killed", "place": "Strait of Hormuz",
                               "date": "2026-10-17", "confirmation": {"level": "official", "by": "UKMTO"},
                               "facts": "A tanker sank after a mine strike, UKMTO said."}]
    monkeypatch.setattr(mec, "_call_model", lambda system, user: json.dumps(answer))
    article = {"url": "er-me://77", "title": "Iran warns ships", "published": "2026-10-17T12:00:00+00:00",
               "body": "Iran warned ships to stay away from Hormuz. UKMTO said a tanker sank after a mine strike in the Strait of Hormuz.",
               "metadata": {"source_type": "eventregistry", "er_query": "eventregistry:me_shipping"}}
    [out] = mec.classify_middle_east_signal_batch([article], _news_retrieval_company_table())
    r = out["result"]
    assert r["signal"] == "signal" and r["metadata"]["event"]["code"] == "m4_ship_sunk_or_crew_killed"
    assert r["reason"].startswith("A tanker sank after a mine strike, UKMTO said.")
    assert [o["event"]["code"] for o in r["metadata"]["other_events"]] == ["m4_threat_or_statement"]


# --- Full-run fixes (2026-10-23) -------------------------------------------------------

@pytest.mark.parametrize("companies, by, label", [
    ([{"name": "Maersk", "ticker": None, "country": "Denmark", "tracked": False}], "Maersk", "signal"),
    ([], "Baltic Exchange", "weak_signal"),                                     # a rate report
    ([{"name": "South Korea's Ministry of Oceans and Fisheries", "ticker": None,
       "country": "South Korea", "tracked": False}], "Ministry of Transport", "weak_signal"),
])
def test_a_route_change_needs_the_line_or_insurer_itself(companies, by, label) -> None:
    ex = _ex("m4_shipping_or_insurance", companies=companies, by=by)
    assert decide_label(ex)[0] == label


def test_an_amount_alone_is_not_a_data_centre_deal_and_a_gulf_party_is_needed() -> None:
    port = _ex("m2_deal_or_construction", deal_level=4, amount="$100 million",
               companies=[{"name": "CMA CGM", "ticker": None, "country": "France", "tracked": False}])
    assert decide_label(port, "", "CMA CGM will expand the container terminal at Beirut Port.")[0] == "noise"
    no_gulf = _ex("m2_deal_or_construction", deal_level=4, amount="$100 million", capacity="20MW",
                  companies=[{"name": "Acme", "ticker": None, "country": "France", "tracked": False}])
    assert decide_label(no_gulf)[:2] == ("noise", "m2_no_gulf_company")


def test_a_completed_investment_names_who_was_invested_in() -> None:
    mgx = {"name": "MGX", "ticker": None, "country": "UAE", "tracked": True}
    alone = _ex("m3_investment", deal_level=4, amount="$65 billion", companies=[mgx])
    assert decide_label(alone)[0] == "weak_signal"
    both = _ex("m3_investment", deal_level=4, amount="$65 billion",
               companies=[mgx, {"name": "Anthropic", "ticker": None, "country": "United States", "tracked": False}])
    assert decide_label(both)[0] == "signal"


def test_sanctions_over_oil_shipping_are_not_chip_enforcement() -> None:
    ex = _ex("m8_enforcement", companies=[{"name": "Whales Shipping Line LLC", "ticker": None,
                                           "country": "UAE", "tracked": False}])
    oil = "The Treasury sanctioned shipping agents and nine vessels in Iran's shadow fleet carrying oil."
    assert decide_label(ex, "", oil)[:2] == ("noise", "m8_not_chips_or_technology")
    chips = "Commerce added the UAE firm to the Entity List for diverting Nvidia chips to China."
    assert decide_label(ex, "", chips)[0] == "signal"


def test_an_undated_force_majeure_is_background_and_rationing_needs_the_supplier() -> None:
    fm = _ex("m5_qatar_force_majeure", by="QatarEnergy")
    assert decide_label(fm)[:2] == ("weak_signal", "m5_force_majeure_undated")
    assert decide_label(_ex("m5_supplier_rationing", confirmation="named_source"))[0] == "weak_signal"
    assert decide_label(_ex("m5_supplier_rationing"))[0] == "signal"


def test_a_deal_confirmed_by_a_non_party_is_a_named_source() -> None:
    names, tickers = _index()
    body = _BODY + " S&P Global Ratings said in a report."
    ex, dropped = check_extraction(_answer(confirmation={"level": "official", "by": "S&P Global Ratings"}),
                                   "G42 switches on", body, names, tickers)
    assert ex["confirmation"]["level"] == "named_source" and "confirmation.not_a_party" in dropped


def test_the_target_confirming_leads_over_the_attackers_claim() -> None:
    claim = _ev(1, "m4_strike_on_site", "Dhahran", subjects=["Saudi Aramco"], label="signal", hours=0)
    target = _ev(2, "m4_strike_on_site", "Dhahran", subjects=["Saudi Aramco"], label="signal", hours=5)
    claim["metadata"]["confirmation"] = {"level": "official", "by": "Houthi spokesman Yahya Saree"}
    target["metadata"]["confirmation"] = {"level": "official", "by": "Saudi Energy Ministry"}
    for r in (claim, target):
        r["metadata"]["event"]["date"] = "2026-10-14"
    [group] = find_same_event_groups([claim, target])
    assert group["kept"]["id"] == 2


@pytest.mark.parametrize("code, title, summary, place, want_rule_prefix", [
    # A tanker that exploded but did not sink is a ship attack, not a sinking.
    ("m4_ship_sunk_or_crew_killed", "Tanker explodes after hitting mine",
     "An oil tanker exploded and caught fire after hitting a mine.", "Strait of Hormuz", "m4_ship_incident"),
    # A helicopter shot down is not a ship.
    ("m4_ship_sunk_or_crew_killed", "Helicopter shot down",
     "Forces shot down a helicopter, killing both pilots.", "Saudi border", "m4_no_economic_target"),
    # A real sinking stands.
    ("m4_ship_sunk_or_crew_killed", "Ship sinks after strike",
     "The container ship sank after being hit by a projectile.", "Strait of Hormuz", "m4_ship_sunk"),
    # An oil spill is no attack.
    ("m4_strike_on_site", "Beaches shut after oil spill",
     "Authorities closed beaches after an oil spill from a tanker.", "Umm Al Quwain", "guard_no_attack"),
    # An attack with no site named is another strike - noise.
    ("m4_strike_on_site", "Launcher destroyed",
     "The coalition destroyed a missile launcher used in an attack on Riyadh.", "Riyadh", "m4_no_economic"),
    # A strike on a named site stands.
    ("m4_strike_on_site", "Drone hits Aramco oilfield",
     "A drone struck the Shaybah oil field, killing two.", "Shaybah oil field", "m4_strike_confirmed"),
])
def test_guardrails_need_the_words_that_make_a_code_true(code, title, summary, place, want_rule_prefix) -> None:
    ex = _ex(code, place=place, country="Saudi Arabia", summary=summary, reason_facts=summary)
    assert decide_label(ex, title, summary)[1].startswith(want_rule_prefix)


def test_a_list_of_investments_is_not_one_new_investment() -> None:
    mgx = {"name": "MGX", "ticker": None, "country": "UAE", "tracked": True}
    us = [{"name": n, "ticker": None, "country": "United States", "tracked": False}
          for n in ("Anthropic", "Databricks")]
    ex = _ex("m3_investment", deal_level=4, amount="$65 billion", companies=[mgx, *us])
    assert decide_label(ex)[:2] == ("weak_signal", "m3_several_investments_listed")



@pytest.mark.parametrize("code, summary, place, want", [
    ("m4_strike_on_site", "Authorities closed beaches after a tanker attacked offshore spilled oil.",
     "Umm Al Quwain coast", "noise"),                                   # "oil" is no site
    ("m4_waterway_closed", "Unverified claims of mine threats to two oil tankers near Hormuz.",
     "Strait of Hormuz", "m4_threat"),                                  # no closure in the text
    ("m5_asia_shortfall", "South Korea will burn more coal to cut winter LNG use.", "South Korea",
     "m5_commentary"),                                                  # gas, not helium
])
def test_more_guardrails_on_real_misreadings(code, summary, place, want) -> None:
    ex = _ex(code, place=place, summary=summary, reason_facts=summary, by="IRGC Navy")
    label, rule, _ = decide_label(ex, "headline", summary)
    assert rule.startswith(want) or label == want


def test_an_undated_attack_count_is_weak() -> None:
    assert decide_label(_ex("m4_attack_tally", confirmation="named_source"))[:2] == (
        "weak_signal", "m4_attack_tally_undated")
    assert decide_label(_ex("m4_attack_tally", confirmation="named_source", day="2026-10-16"))[0] == "signal"


def test_an_attack_without_its_day_is_not_an_alert() -> None:
    sunk = _ex("m4_ship_sunk_or_crew_killed", place="Strait of Hormuz", confirmation="named_source",
               summary="A container ship sank after a strike earlier.", reason_facts="A ship sank.")
    assert decide_label(sunk)[0] == "weak_signal"
    month = {**sunk, "event": {**sunk["event"], "date": "2026-10"}}
    assert decide_label(month)[0] == "weak_signal"
    dated = {**sunk, "event": {**sunk["event"], "date": "2026-10-13"}}
    assert decide_label(dated)[0] == "signal"


def test_reports_of_one_attack_never_make_a_wave() -> None:
    """Three unfolded reports of one tanker attack, same source, a day
    apart, are one attack - no wave."""
    from pipeline.middle_east_signal_classifier import attack_wave_raises
    rows = []
    for i, (day, place) in enumerate([("2026-10-07", "off Qatar"), ("2026-10-08", "north coast of Qatar"),
                                      ("2026-10-07", "off the coast of Qatar")]):
        r = _ev(i, "m4_ship_incident", place)
        r["metadata"]["event"]["date"] = day
        r["metadata"]["confirmation"] = {"level": "official", "by": "UKMTO"}
        rows.append(r)
    assert attack_wave_raises(rows) == []


def test_several_counts_of_one_escalation_alert_once() -> None:
    from pipeline.middle_east_signal_classifier import repeated_count_lowers
    rows = []
    for i, day in enumerate(["2026-10-13", "2026-10-16", "2026-10-17", "2026-10-25"]):
        r = _ev(i, "m4_attack_tally", "Strait of Hormuz", label="signal")
        r["metadata"]["event"]["date"] = day
        rows.append(r)
    lowered = [rid for rid, *_ in repeated_count_lowers(rows)]
    assert lowered == [1, 2]            # the 13th alerts; the 25th starts a new window


# --- Fixes from reviewing every visible row of the full run (2026-10-24) ------------

@pytest.mark.parametrize("by, anonymous", [
    ("an Arab diplomat posted to a major Middle Eastern country", True),
    ("a senior Iranian official", True),
    ("two people familiar with the plans", True),
    ("a spokesperson for the Houthi armed forces", False),
    ("a Pentagon spokesperson", False),
    ("Kpler", False),
])
def test_an_indefinite_person_described_only_by_nationality_names_nobody(by, anonymous) -> None:
    assert mec.is_anonymous_source(by) is anonymous


def test_another_governments_export_rule_is_not_a_us_decision() -> None:
    got = decide_label(_ex("m1_draft_or_hearing", place="China", country="China"))
    assert got[:2] == ("noise", "m1_not_a_us_decision")
    us = decide_label(_ex("m1_rule_or_licence", country="United States"))
    assert us[:2] == ("signal", "m1_final_decision")


_MEEZA = {"name": "MEEZA", "ticker": None, "country": "Qatar", "tracked": False}


def test_a_signed_order_mentioned_by_a_third_party_is_weak() -> None:
    kw = dict(deal_level=4, amount="over QR1bn", capacity="8 MW", companies=[_MEEZA])
    got = decide_label(_ex("m2_deal_or_construction", confirmation="named_source", by="S&P Global Ratings", **kw))
    assert got[:2] == ("weak_signal", "m2_signed_reported_by_third_party")
    got = decide_label(_ex("m2_deal_or_construction", confirmation="named_source", by="MEEZA", **kw))
    assert got[:2] == ("signal", "m2_signed_with_amount")


def test_a_capacity_that_is_not_compute_is_not_a_data_centre_deal() -> None:
    ex = _ex("m2_deal_or_construction", deal_level=4, capacity="up to 4 million tonnes per annum",
             companies=[{"name": "QatarEnergy", "ticker": None, "country": "Qatar", "tracked": True}])
    ex["summary"] = ("QatarEnergy signed two long-term agreements with Cheniere Energy to buy LNG"
                     " from Sabine Pass and Corpus Christi through 2054.")
    assert decide_label(ex, "QatarEnergy to buy up to 4 MTPA of U.S. LNG from Cheniere")[:2] == \
        ("noise", "m2_not_hardware")


def test_a_canal_authority_relaying_a_shipping_line_is_not_the_line() -> None:
    hapag = {"name": "Hapag-Lloyd", "ticker": None, "country": "Germany", "tracked": False}
    sca = {"name": "Suez Canal Authority", "ticker": None, "country": "Egypt", "tracked": False}
    got = decide_label(_ex("m4_shipping_or_insurance", place="Suez Canal", by="Suez Canal Authority",
                           companies=[hapag, sca]))
    assert got[:2] == ("weak_signal", "m4_shipping_not_announced_by_line")
    got = decide_label(_ex("m4_shipping_or_insurance", place="Suez Canal", by="Hapag-Lloyd",
                           companies=[hapag, sca]))
    assert got[:2] == ("signal", "m4_shipping_line_or_insurer")


def _dated(row, day, by=None, country=None):
    row["metadata"]["event"]["date"] = day
    if by:
        row["metadata"]["confirmation"]["by"] = by
    if country:
        row["metadata"]["event"]["country"] = country
    return row


def test_one_sides_claim_reported_with_different_places_and_names_is_one_count() -> None:
    rows = [_dated(_ev(1, "m4_attack_tally", "wider Gulf", label="signal"), "2026-10-22", "Iranian forces"),
            _dated(_ev(2, "m4_attack_tally", "northern Arabian Gulf", hours=15, label="signal"), "2026-10-22", "IRGC"),
            _dated(_ev(3, "m4_attack_tally", "northern Persian Gulf", hours=19), "2026-10-22", "IRGC Navy")]
    [group] = find_same_event_groups(rows)
    assert len(group["duplicates"]) == 2
    # A different source on other waters the same day is another count.
    other = _dated(_ev(4, "m4_attack_tally", "Red Sea", hours=2, label="signal"), "2026-10-22", "UKMTO")
    assert len(find_same_event_groups([rows[0], other])) == 0


def test_an_attackers_new_claim_is_not_lowered_as_a_recount() -> None:
    from pipeline.middle_east_signal_classifier import repeated_count_lowers
    kpler = _dated(_ev(1, "m4_attack_tally", "Strait of Hormuz", label="signal"), "2026-10-19", "Kpler")
    irgc = _dated(_ev(2, "m4_attack_tally", "Strait of Hormuz", label="signal"), "2026-10-22", "IRGC Navy")
    ukmto = _dated(_ev(3, "m4_attack_tally", "Strait of Hormuz", label="signal"), "2026-10-21", "UKMTO")
    assert [rid for rid, *_ in repeated_count_lowers([kpler, irgc, ukmto])] == [3]


def test_reports_of_one_strike_plan_fold_within_three_days() -> None:
    a = _dated(_ev(1, "m4_strike_plan", "Iran", label="signal"), "2026-10-20", country="Iran")
    b = _dated(_ev(2, "m4_strike_plan", "Iran", hours=53, label="signal"), "2026-10-22", country="Iran")
    assert len(find_same_event_groups([a, b])) == 1
    late = _dated(_ev(3, "m4_strike_plan", "Iran", hours=24 * 5, label="signal"), "2026-10-25", country="Iran")
    assert find_same_event_groups([a, late]) == []


def test_an_interception_report_folds_into_the_strike_only_on_the_same_day_and_place() -> None:
    strike = _dated(_ev(1, "m4_strike_on_site", "Shaybah oil field", label="signal"), "2026-10-21")
    same = _dated(_ev(2, "m4_interception", "Shaybah", hours=-11, label="noise"), "2026-10-21")
    next_day = _dated(_ev(3, "m4_interception", "Shaybah", hours=20, label="noise"), "2026-10-22")
    [group] = find_same_event_groups([strike, same, next_day])
    assert group["kept"]["id"] == 1 and [d["id"] for d in group["duplicates"]] == [2]


def test_noise_never_leads_and_old_background_never_folds() -> None:
    a = _dated(_ev(1, "m4_interception", "Shaybah", label="noise"), "2026-10-21")
    b = _dated(_ev(2, "m4_interception", "Shaybah", hours=2, label="noise"), "2026-10-21")
    assert find_same_event_groups([a, b]) == []
    fm = _ev(3, "m5_qatar_force_majeure", "Ras Laffan", hours=0)
    background = _dated(_ev(4, "m5_qatar_force_majeure", "Ras Laffan", hours=10, label="noise"), "2026-03")
    assert find_same_event_groups([fm, background]) == []


def test_two_deals_by_one_gulf_company_are_two_deals() -> None:
    nvidia = _ev(1, "m2_deal_or_construction", "Saudi Arabia", subjects=("Humain", "Nvidia"))
    nebius = _ev(2, "m2_deal_or_construction", "NEOM", subjects=("Humain", "DataVolt", "Nebius"), hours=5)
    assert find_same_event_groups([nvidia, nebius]) == []
    again = _ev(3, "m2_deal_or_construction", "Saudi Arabia", subjects=("Humain", "Nvidia"), hours=8)
    assert len(find_same_event_groups([nvidia, again])) == 1


def test_a_force_majeure_alert_needs_the_day() -> None:
    qatar = {"name": "QatarEnergy", "ticker": None, "country": "Qatar", "tracked": True}
    kw = dict(place="Ras Laffan", country="Qatar", by="QatarEnergy", companies=[qatar])
    assert decide_label(_ex("m5_qatar_force_majeure", day="2026-10-05", **kw),
                        published="2026-10-06")[0] == "signal"
    got = decide_label(_ex("m5_qatar_force_majeure", day="2026-10", **kw), published="2026-10-13")
    assert got[:2] == ("weak_signal", "m5_qatar_force_majeure_undated")
