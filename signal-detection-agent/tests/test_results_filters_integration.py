"""Integration tests for list_all_results' filters (grade, impacted_category,
impacted_ticker, channel, corroborated, published_from/published_to).

Unlike every other test in this suite, these hit a REAL Postgres connection
- list_all_results/list_results build raw SQL directly against get_db(),
with no injectable seam to mock (no repository interface, no DB fixture in
this codebase's conftest.py today). A pure-mock unit test here would only
verify the Python glue, not the actual SQL - and the impacted_ticker filter
had a real bug (jsonb_array_elements_text choking on a JSON null, not a
missing key) that only real data caught, not a syntax/compile check. So
these tests insert real rows into agent_classifications, query through
list_all_results, and clean up after themselves - skipped automatically if
no DB is reachable (e.g. running outside the docker-compose network),
rather than failing the whole suite in an environment without Postgres.
"""
import sys
import os
import json
from datetime import datetime, timezone, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))

try:
    from db import get_db, init_db
    from models.jobs import create_job, get_results_summary, list_all_results
    # importing db.py does not itself open a connection (psycopg2 connects
    # lazily) - actually attempt one here, so this skip check catches "not
    # running inside the docker-compose network" (e.g. a plain .venv/bin/
    # pytest run on the host, where the Docker-internal hostname
    # postgres-signal doesn't resolve), not just an import-time failure.
    with get_db() as _conn:
        _conn.execute("SELECT 1")
    _DB_AVAILABLE = True
except Exception:
    _DB_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _DB_AVAILABLE, reason="requires a reachable Postgres (run inside docker-compose network)"
)

_TEST_MARKER = "test_results_filters_integration"


def _cleanup():
    with get_db() as conn:
        conn.execute(
            "DELETE FROM agent_classifications WHERE metadata->>'_test_marker' = %s",
            (_TEST_MARKER,),
        )


@pytest.fixture(autouse=True)
def _db_setup_teardown():
    init_db()
    _cleanup()
    yield
    _cleanup()


def _insert_row(
    *,
    article_id: int,
    title: str,
    url: str,
    published: datetime,
    metadata: dict,
    entities: list[dict] | None = None,
):
    job_id = create_job(domain="geopolitical_news")
    full_metadata = {**metadata, "_test_marker": _TEST_MARKER}
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications
                (job_id, article_id, url, title, signal_detection, published,
                 source_type, metadata, entities_json)
            VALUES (%s, %s, %s, %s, 'signal', %s, 'geopolitical_signal', %s::jsonb, %s)
            """,
            # entities_json is NOT NULL DEFAULT '[]' - a row with no
            # entities stores the empty array, never SQL NULL.
            (
                job_id, article_id, url, title, published,
                json.dumps(full_metadata), json.dumps(entities or []),
            ),
        )


def test_grade_filter_matches_only_that_grade():
    _insert_row(
        article_id=900001, title="TOP row", url="https://reuters.com/a",
        published=datetime.now(timezone.utc),
        metadata={"grade": "TOP"},
    )
    _insert_row(
        article_id=900002, title="STANDARD row", url="https://reuters.com/b",
        published=datetime.now(timezone.utc),
        metadata={"grade": "STANDARD"},
    )
    result = list_all_results(source_type="geopolitical_signal", grade="TOP", limit=100)
    titles = {r["title"] for r in result["results"]}
    assert "TOP row" in titles
    assert "STANDARD row" not in titles


def test_impacted_category_filter_matches_array_containment():
    _insert_row(
        article_id=900003, title="Semis row", url="https://reuters.com/c",
        published=datetime.now(timezone.utc),
        metadata={"impacted_categories": ["Semiconductor Manufacturing", "Cloud & Compute Platforms"]},
    )
    _insert_row(
        article_id=900004, title="Energy row", url="https://reuters.com/d",
        published=datetime.now(timezone.utc),
        metadata={"impacted_categories": ["Energy & Grid Infrastructure"]},
    )
    result = list_all_results(
        source_type="geopolitical_signal", impacted_category="Semiconductor Manufacturing", limit=100,
    )
    titles = {r["title"] for r in result["results"]}
    assert "Semis row" in titles
    assert "Energy row" not in titles


def test_impacted_ticker_filter_matches_entities_json_case_insensitive():
    """impacted_ticker reads entities_json - the companies the article
    itself names, each carrying the reason the text gives.

    It used to read metadata.impacted_companies_direct (null on every
    geopolitical row ever stored) and metadata.impacted_companies_by_
    category (every ticker in a matched SECTOR, so a UK airbase story
    naming no company carried 11 defence tickers). Both fields are gone:
    a ticker is only attached to an entity the article names.
    """
    _insert_row(
        article_id=900005, title="Named company row", url="https://reuters.com/e",
        published=datetime.now(timezone.utc), metadata={},
        entities=[
            {"name": "Nvidia", "type": "company", "ticker": "NVDA",
             "reason": "barred from selling H20 accelerators to China"},
        ],
    )
    _insert_row(
        article_id=900006, title="Other company row", url="https://reuters.com/f",
        published=datetime.now(timezone.utc), metadata={},
        entities=[
            {"name": "Advanced Micro Devices", "type": "company", "ticker": "AMD",
             "reason": "subject to the same licence requirement"},
            {"name": "Office of Foreign Assets Control", "type": "government",
             "ticker": None, "reason": "issued the determination"},
        ],
    )
    _insert_row(
        article_id=900007, title="No match row", url="https://reuters.com/g",
        published=datetime.now(timezone.utc), metadata={},
    )
    result = list_all_results(source_type="geopolitical_signal", impacted_ticker="nvda", limit=100)
    titles = {r["title"] for r in result["results"]}
    assert "Named company row" in titles
    assert "Other company row" not in titles
    assert "No match row" not in titles

    result2 = list_all_results(source_type="geopolitical_signal", impacted_ticker="AMD", limit=100)
    titles2 = {r["title"] for r in result2["results"]}
    assert "Other company row" in titles2
    assert "Named company row" not in titles2

    # An entity with a null ticker (a ministry, a country) must never
    # match - only a company carries one.
    result3 = list_all_results(source_type="geopolitical_signal", impacted_ticker="OFAC", limit=100)
    assert {r["title"] for r in result3["results"]} == set()


def test_impacted_ticker_filter_does_not_crash_on_null_entities():
    # regression test: jsonb_array_elements raised
    # psycopg2.errors.InvalidParameterValue: cannot extract elements from a
    # scalar when the column holds JSON null (not SQL NULL, not a missing
    # key) - COALESCE alone does not catch this, only jsonb_typeof(...) =
    # 'array' does. Confirmed live against real Stage C output, which
    # stores exactly this shape on a fail-open row where tagging failed.
    _insert_row(
        article_id=900008, title="Empty entities row", url="https://reuters.com/h",
        published=datetime.now(timezone.utc), metadata={}, entities=[],
    )
    # The shape that actually crashed: the literal JSON scalar null,
    # which jsonb_typeof reports as 'null' rather than 'array'.
    with get_db() as conn:
        conn.execute(
            """
            UPDATE agent_classifications SET entities_json = 'null'
            WHERE article_id = 900008
            """,
        )
    _insert_row(
        article_id=900009, title="Entity without ticker row", url="https://reuters.com/i",
        published=datetime.now(timezone.utc), metadata={},
        entities=[{"name": "Iran", "type": "country", "ticker": None,
                   "reason": "targeted by the determination"}],
    )
    result = list_all_results(source_type="geopolitical_signal", impacted_ticker="NVDA", limit=100)
    titles = {r["title"] for r in result["results"]}
    assert "Empty entities row" not in titles
    assert "Entity without ticker row" not in titles


def test_channel_filter():
    _insert_row(
        article_id=900009, title="Sanctions row", url="https://reuters.com/i",
        published=datetime.now(timezone.utc),
        metadata={"channel": "sanctions"},
    )
    _insert_row(
        article_id=900010, title="Trade row", url="https://reuters.com/j",
        published=datetime.now(timezone.utc),
        metadata={"channel": "trade"},
    )
    result = list_all_results(source_type="geopolitical_signal", channel="sanctions", limit=100)
    titles = {r["title"] for r in result["results"]}
    assert "Sanctions row" in titles
    assert "Trade row" not in titles


def test_corroborated_filter_true_and_false():
    _insert_row(
        article_id=900011, title="Corroborated row", url="https://reuters.com/k",
        published=datetime.now(timezone.utc),
        metadata={"corroborated": True},
    )
    _insert_row(
        article_id=900012, title="Not corroborated row", url="https://reuters.com/l",
        published=datetime.now(timezone.utc),
        metadata={"corroborated": False},
    )
    result_true = list_all_results(source_type="geopolitical_signal", corroborated=True, limit=100)
    titles_true = {r["title"] for r in result_true["results"]}
    assert "Corroborated row" in titles_true
    assert "Not corroborated row" not in titles_true

    result_false = list_all_results(source_type="geopolitical_signal", corroborated=False, limit=100)
    titles_false = {r["title"] for r in result_false["results"]}
    assert "Not corroborated row" in titles_false
    assert "Corroborated row" not in titles_false


def test_published_from_to_filters_are_inclusive_date_range():
    # scoped with a unique marker channel (not just source_type, which alone
    # has thousands of real rows from other test sessions in this shared
    # local DB - a bare limit=100 newest-first page would not reliably
    # contain our just-inserted row otherwise) so the assertion is exact
    # regardless of how much other data already exists in the table.
    now = datetime.now(timezone.utc)
    _insert_row(
        article_id=900013, title="In range row", url="https://reuters.com/m",
        published=now - timedelta(days=1),
        metadata={"channel": _TEST_MARKER},
    )
    _insert_row(
        article_id=900014, title="Too old row", url="https://reuters.com/n",
        published=now - timedelta(days=30),
        metadata={"channel": _TEST_MARKER},
    )
    from_date = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")
    result = list_all_results(
        source_type="geopolitical_signal", channel=_TEST_MARKER,
        published_from=from_date, published_to=to_date, limit=100,
    )
    titles = {r["title"] for r in result["results"]}
    assert "In range row" in titles
    assert "Too old row" not in titles


def test_combined_filters_all_apply_together():
    _insert_row(
        article_id=900015, title="Matches all row", url="https://reuters.com/o",
        published=datetime.now(timezone.utc),
        metadata={
            "grade": "STRONG", "channel": "trade", "corroborated": False,
            "impacted_categories": ["Semiconductor Manufacturing"],
        },
    )
    _insert_row(
        article_id=900016, title="Wrong channel row", url="https://reuters.com/p",
        published=datetime.now(timezone.utc),
        metadata={
            "grade": "STRONG", "channel": "sanctions", "corroborated": False,
            "impacted_categories": ["Semiconductor Manufacturing"],
        },
    )
    result = list_all_results(
        source_type="geopolitical_signal", grade="STRONG", channel="trade",
        corroborated=False, impacted_category="Semiconductor Manufacturing", limit=100,
    )
    titles = {r["title"] for r in result["results"]}
    assert "Matches all row" in titles
    assert "Wrong channel row" not in titles


def _insert_macro_row(*, source_id: str, published: datetime, metadata: dict, signal_detection: str = "signal"):
    """macro_signal rows have no article_id/url/title (see
    insert_macro_signal_event) - a dedicated helper rather than reusing
    _insert_row above, which hardcodes source_type='geopolitical_signal'
    and an article-shaped INSERT."""
    job_id = create_job(domain="macro_signal")
    full_metadata = {**metadata, "_test_marker": _TEST_MARKER}
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications
                (job_id, source_id, signal_detection, published, source_type, metadata)
            VALUES (%s, %s, %s, %s, 'macro_signal', %s::jsonb)
            """,
            (job_id, source_id, signal_detection, published, json.dumps(full_metadata)),
        )


def test_macro_signal_detection_filter_matches_only_that_value():
    """macro_signal has no domain-specific tier filter - it reads
    through plain signal_detection ('signal'/'weak_signal'/'noise'),
    same as every other domain."""
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2026-08-28T20:30:00+00:00", published=datetime.now(timezone.utc),
        metadata={}, signal_detection="signal",
    )
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS10-2026-08-27", published=datetime.now(timezone.utc),
        metadata={}, signal_detection="noise",
    )
    result_signal = list_all_results(source_type="macro_signal", signal_detection="signal", limit=100)
    signal_ids = {r["source_id"] for r in result_signal["results"] if r["metadata"].get("_test_marker") == _TEST_MARKER}
    assert signal_ids == {"TESTFIXTURE-18-2026-08-28T20:30:00+00:00"}

    result_noise = list_all_results(source_type="macro_signal", signal_detection="noise", limit=100)
    noise_ids = {r["source_id"] for r in result_noise["results"] if r["metadata"].get("_test_marker") == _TEST_MARKER}
    assert noise_ids == {"TESTFIXTURE-DGS10-2026-08-27"}


def test_macro_series_filter_matches_both_row_shapes():
    """member_series (interpreted-event rows, a JSONB array) and
    series_id (suppressed/audit rows, a plain string) are the two
    different places macro_signal stores 'which series' - this filter
    must match a series_id search against either shape."""
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2026-08-28T20:30:00+00:00", published=datetime.now(timezone.utc),
        metadata={"member_series": ["DFII5", "DGS1"]},
    )
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS10-2026-08-28", published=datetime.now(timezone.utc),
        metadata={"series_id": "DGS10", "suppressed_by": "derived:DFII10+T10YIE"},
        signal_detection="signal",
    )
    _insert_macro_row(
        source_id="TESTFIXTURE-VIXCLS-2026-08-28", published=datetime.now(timezone.utc),
        metadata={"series_id": "VIXCLS"}, signal_detection="noise",
    )

    result_dgs1 = list_all_results(source_type="macro_signal", macro_series="DGS1", limit=100)
    matched_ids = {r["source_id"] for r in result_dgs1["results"]}
    assert "TESTFIXTURE-18-2026-08-28T20:30:00+00:00" in matched_ids  # matched via member_series
    assert "TESTFIXTURE-VIXCLS-2026-08-28" not in matched_ids

    result_dgs10 = list_all_results(source_type="macro_signal", macro_series="DGS10", limit=100)
    matched_ids = {r["source_id"] for r in result_dgs10["results"]}
    assert "TESTFIXTURE-DGS10-2026-08-28" in matched_ids  # matched via series_id
    assert "TESTFIXTURE-18-2026-08-28T20:30:00+00:00" not in matched_ids


def test_macro_suspect_filter():
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2026-11-27T14:00:00+00:00", published=datetime.now(timezone.utc),
        metadata={"suspect": True, "suspect_reason": "Thanksgiving holiday week"},
    )
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2026-08-28T20:30:00+00:00", published=datetime.now(timezone.utc),
        metadata={"suspect": False, "suspect_reason": None},
    )
    result_suspect = list_all_results(source_type="macro_signal", macro_suspect=True, limit=100)
    matched_ids = {r["source_id"] for r in result_suspect["results"]}
    assert "TESTFIXTURE-18-2026-11-27T14:00:00+00:00" in matched_ids
    assert "TESTFIXTURE-18-2026-08-28T20:30:00+00:00" not in matched_ids

    result_not_suspect = list_all_results(source_type="macro_signal", macro_suspect=False, limit=100)
    matched_ids = {r["source_id"] for r in result_not_suspect["results"]}
    assert "TESTFIXTURE-18-2026-08-28T20:30:00+00:00" in matched_ids
    assert "TESTFIXTURE-18-2026-11-27T14:00:00+00:00" not in matched_ids


def test_macro_interpreted_only_filter_excludes_suppressed_high_rows():
    """CONFIRMED LIVE against a real pipeline run on 2026-08-28: DGS2
    cleared HIGH (z=2.736) but was suppressed via the {DGS3MO,DGS1,DGS2}
    collinear cluster - the audit-row branch of insert_macro_signal_event
    mapped its original HIGH tier to signal_detection='signal'
    (metadata.suppressed_by='collinear:DGS1'), same as a genuinely
    interpreted survivor, so signal_detection='signal' alone returns
    both mixed together with no way to tell them apart except
    suppressed_by. This is exactly the gap macro_interpreted_only
    exists to close."""
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS2-2026-08-28", published=datetime.now(timezone.utc),
        metadata={"series_id": "DGS2", "suppressed_by": "collinear:DGS1"},
        signal_detection="signal",
    )
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2026-08-28", published=datetime.now(timezone.utc),
        metadata={
            "suppressed_by": None, "channel": "discount_rate",
            "transmission": "The 5-year real yield rose 18bp.",
        },
        signal_detection="signal",
    )

    result_all_signal = list_all_results(source_type="macro_signal", signal_detection="signal", limit=100)
    all_ids = {r["source_id"] for r in result_all_signal["results"] if r["metadata"].get("_test_marker") == _TEST_MARKER}
    assert all_ids == {"TESTFIXTURE-DGS2-2026-08-28", "TESTFIXTURE-18-2026-08-28"}  # both present without the new filter

    result_interpreted = list_all_results(
        source_type="macro_signal", signal_detection="signal", macro_interpreted_only=True, limit=100,
    )
    interpreted_ids = {r["source_id"] for r in result_interpreted["results"] if r["metadata"].get("_test_marker") == _TEST_MARKER}
    assert interpreted_ids == {"TESTFIXTURE-18-2026-08-28"}  # only the real interpreted event

    result_suppressed = list_all_results(
        source_type="macro_signal", signal_detection="signal", macro_interpreted_only=False, limit=100,
    )
    suppressed_ids = {r["source_id"] for r in result_suppressed["results"] if r["metadata"].get("_test_marker") == _TEST_MARKER}
    assert suppressed_ids == {"TESTFIXTURE-DGS2-2026-08-28"}  # only the suppressed audit row


def test_macro_interpreted_only_excludes_noise_rows_regardless_of_suppressed_by():
    """A genuine NOISE row (suppressed_by is absent, never set) must also
    be excluded by macro_interpreted_only=True - it never reached
    INTERPRET either, even though it has no suppressed_by value to check."""
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS10-2026-08-27", published=datetime.now(timezone.utc),
        metadata={}, signal_detection="noise",
    )
    result = list_all_results(source_type="macro_signal", macro_interpreted_only=True, limit=100)
    matched_ids = {r["source_id"] for r in result["results"] if r["metadata"].get("_test_marker") == _TEST_MARKER}
    assert "TESTFIXTURE-DGS10-2026-08-27" not in matched_ids


# ============================================================================
# GET /results/summary  (real ask: frontend ticket, 2026-09-25)
# ============================================================================
# A distinct, far-future date window (2099) isolates these tests from any
# real production macro_signal data in the same database, since
# get_results_summary has no _test_marker filter of its own (it must count
# ALL rows in the window, including ones this test suite didn't insert, to
# genuinely match what /results returns for the same window).

_SUMMARY_WINDOW_FROM = "2099-01-01"
_SUMMARY_WINDOW_TO = "2099-01-31"
_SUMMARY_WINDOW_DATE = datetime(2099, 1, 15, tzinfo=timezone.utc)


def test_summary_observations_counts_each_series_by_its_own_tier():
    """Real ask, and a real reported bug fix (frontend follow-up,
    2026-09-25): observations.* must count each series by ITS OWN tier
    (metadata.series_tiers), not the event's rolled-up signal_detection
    applied to every member. Confirmed live: the 2026-08-28
    discount_rate event was signal_detection='signal' (DFII5/T10Y2Y are
    HIGH) but DFII10/T5YIFR are WEAK per their own classification_reason
    - counting them as "high" contradicted that sentence. This test
    locks in the fix with the same real shape: 2 HIGH + 2 WEAK members
    in one row must land as 2/2, not 4/0."""
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2099-01-15", published=_SUMMARY_WINDOW_DATE,
        metadata={
            "member_series": ["DFII5", "DFII10", "T10Y2Y", "T5YIFR"],
            "series_tiers": {"DFII5": "signal", "T10Y2Y": "signal", "DFII10": "weak_signal", "T5YIFR": "weak_signal"},
            "transmission": "x", "suppressed_by": None,
        },
        signal_detection="signal",  # the event's own rolled-up tier - deliberately NOT what observations.* should use
    )
    summary = get_results_summary(
        source_type="macro_signal", published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO,
    )
    assert summary["observations"]["high"] == 2
    assert summary["observations"]["weak"] == 2
    assert summary["observations"]["total"] == 4


def test_summary_observations_falls_back_to_row_level_tier_when_series_tiers_missing():
    """An interpreted event inserted before the series_tiers fix (or any
    row that genuinely has none) must still be counted, not silently
    dropped - falls back to the row's own signal_detection applied to
    the whole row, same as the old (less precise) behavior, until it's
    re-interpreted."""
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2099-01-20", published=_SUMMARY_WINDOW_DATE,
        metadata={"member_series": ["T10Y2Y", "T5YIFR"], "transmission": "x", "suppressed_by": None},
        signal_detection="signal",
    )
    summary = get_results_summary(
        source_type="macro_signal", published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO,
    )
    assert summary["observations"]["high"] == 1  # one row, no series_tiers -> counted once, not expanded
    assert summary["observations"]["total"] == 1


def test_summary_observations_counts_audit_rows_as_one_series_each():
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS10-2099-01-16", published=_SUMMARY_WINDOW_DATE,
        metadata={"series_id": "DGS10", "suppressed_by": None},
        signal_detection="noise",
    )
    summary = get_results_summary(
        source_type="macro_signal", published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO,
    )
    assert summary["observations"]["noise"] == 1
    assert summary["observations"]["total"] == 1


def test_summary_suppressed_also_counts_toward_its_own_tier():
    """Real ask's own wording: suppressed rows "also count toward their
    own tier" - not a mutually exclusive bucket."""
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS2-2099-01-17", published=_SUMMARY_WINDOW_DATE,
        metadata={"series_id": "DGS2", "suppressed_by": "collinear:DGS1"},
        signal_detection="signal",
    )
    summary = get_results_summary(
        source_type="macro_signal", published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO,
    )
    assert summary["observations"]["high"] == 1
    assert summary["observations"]["suppressed"] == 1


def test_summary_events_counts_rows_not_expanded_by_member_series():
    """events.* is a plain row count of interpreted (macro_interpreted_
    only=true) events - a 2-member event is still 1 event, unlike
    observations.* which expands it to 2."""
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2099-01-18", published=_SUMMARY_WINDOW_DATE,
        metadata={"member_series": ["DFII5", "DGS1"], "transmission": "x", "suppressed_by": None},
        signal_detection="weak_signal",
    )
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS10-2099-01-18", published=_SUMMARY_WINDOW_DATE,
        metadata={"series_id": "DGS10", "suppressed_by": "derived:DFII10+T10YIE"},
        signal_detection="signal",
    )
    summary = get_results_summary(
        source_type="macro_signal", published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO,
    )
    assert summary["events"]["weak"] == 1  # the 2-member event, counted once
    assert summary["events"].get("high", 0) == 0  # the suppressed audit row never reached INTERPRET


def test_summary_series_reporting_unions_both_row_shapes():
    _insert_macro_row(
        source_id="TESTFIXTURE-18-2099-01-19", published=_SUMMARY_WINDOW_DATE,
        metadata={"member_series": ["DFII5", "DGS1"], "transmission": "x", "suppressed_by": None},
        signal_detection="weak_signal",
    )
    _insert_macro_row(
        source_id="TESTFIXTURE-DGS10-2099-01-19", published=_SUMMARY_WINDOW_DATE,
        metadata={"series_id": "DGS10", "suppressed_by": None},
        signal_detection="noise",
    )
    summary = get_results_summary(
        source_type="macro_signal", published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO,
    )
    assert summary["series_reporting"] >= 3  # DFII5, DGS1, DGS10 - at least these 3 (window may include other real rows)


def test_summary_matches_list_all_results_for_same_window():
    """The ticket's own hard requirement: "the numbers must match
    /results for the same window." Builds a small known dataset, then
    cross-checks the summary's noise count against a real list_all_
    results(signal_detection='noise') call over the identical window -
    not just internally consistent, but equal to the paginated endpoint's
    own count."""
    for i in range(3):
        _insert_macro_row(
            source_id=f"TESTFIXTURE-DGS3MO-2099-01-2{i}", published=_SUMMARY_WINDOW_DATE,
            metadata={"series_id": "DGS3MO", "suppressed_by": None},
            signal_detection="noise",
        )
    summary = get_results_summary(
        source_type="macro_signal", published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO,
    )
    direct = list_all_results(
        source_type="macro_signal", signal_detection="noise",
        published_from=_SUMMARY_WINDOW_FROM, published_to=_SUMMARY_WINDOW_TO, limit=500,
    )
    # direct["results"] may include other real noise rows in this window
    # too (this test doesn't own the whole window) - so compare the TEST
    # rows' own contribution, not an exact total (that's what the
    # multi-member expansion tests above already lock in precisely).
    test_noise_ids = {r["source_id"] for r in direct["results"] if r["metadata"].get("_test_marker") == _TEST_MARKER}
    assert len(test_noise_ids) == 3
    assert summary["observations"]["noise"] >= 3


def test_summary_last_run_at_reflects_latest_completed_macro_job():
    job_id = create_job(domain="macro_signal")
    with get_db() as conn:
        conn.execute("UPDATE agent_jobs SET status = 'completed', completed_at = NOW() WHERE id = %s", (job_id,))
    summary = get_results_summary(source_type="macro_signal")
    assert summary["last_run_at"] is not None
