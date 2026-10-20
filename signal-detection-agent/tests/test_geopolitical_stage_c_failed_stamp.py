"""A Stage C row that failed must not be stamped as classified.

The bug this covers, measured live on one 504 in a 45-row run: Layer 2
failed, the row was stamped stage_c_classified_at anyway, and that one
key is read by two opposite worklists -

    Stage D   selects   jsonb_exists(metadata, 'stage_c_classified_at')
    Stage C   selects   NOT jsonb_exists(... same key ...)

so the empty row was GRADED by Stage D with a null channel, and was
simultaneously invisible to every future Stage C run. It could not heal;
un-sticking it took manual SQL.

The fix stamps stage_c_failed_at instead. These tests pin both halves:
the stamp chosen for a failed row, and the two queries that read it.
Testing the SQL text is deliberate - the whole bug lived in which key a
WHERE clause named, and nothing else in the suite would catch that key
changing back.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from models import jobs  # noqa: E402


def _capture_update(*, tags, entities=None):
    """Run the stamp writer against a fake DB and return the SQL it ran."""
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch.object(jobs, "get_db", return_value=ctx):
        jobs.update_geopolitical_signal_stage_c_tags(
            1, tags=tags, entities=entities)
    return conn.execute.call_args[0][0]


# --------------------------------------------------------------------
# which stamp gets written
# --------------------------------------------------------------------

def test_failed_row_is_stamped_failed_not_classified() -> None:
    """Every tag null and nothing resolved = the model call failed."""
    sql = _capture_update(tags={
        "channel": None, "assets": None, "impacted_categories": None,
        "summary": None, "event_date": None, "event_location": None,
    })
    assert "stage_c_failed_at" in sql
    assert "'stage_c_classified_at', NOW()" not in sql


def test_tagged_row_is_stamped_classified() -> None:
    """A real verdict always carries a channel."""
    sql = _capture_update(tags={
        "channel": "sanctions", "assets": [], "impacted_categories": None,
        "summary": "x", "event_date": "2026-10-15", "event_location": None,
    })
    assert "stage_c_classified_at" in sql
    assert "stage_c_failed_at', NOW()" not in sql


def test_row_with_entities_but_no_channel_is_not_treated_as_failed() -> None:
    """Layer 2 returned something usable, so it did not fail - the probe
    must not be channel alone."""
    sql = _capture_update(
        tags={"channel": None, "impacted_categories": None},
        entities=[{"name": "MP Materials", "ticker": "MP"}])
    assert "stage_c_classified_at" in sql


def test_row_with_categories_but_no_channel_is_not_treated_as_failed() -> None:
    sql = _capture_update(
        tags={"channel": None, "impacted_categories": ["critical_minerals"]})
    assert "stage_c_classified_at" in sql


def test_successful_retag_clears_a_previous_failure_marker() -> None:
    """A row that failed once and succeeds later must not keep both
    keys - stage_c_failed_at lingering would re-select it forever."""
    sql = _capture_update(tags={"channel": "trade", "impacted_categories": None})
    assert "metadata - 'stage_c_failed_at'" in sql


# --------------------------------------------------------------------
# the two worklists that read the stamp
# --------------------------------------------------------------------

def _query_of(fn) -> str:
    """Capture the SQL a worklist function runs."""
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch.object(jobs, "get_db", return_value=ctx):
        fn()
    return conn.execute.call_args[0][0]


def test_stage_c_worklist_picks_failed_rows_back_up() -> None:
    """So a failure heals on the next run instead of needing SQL."""
    sql = _query_of(jobs.get_untagged_geopolitical_signal_high_articles)
    assert "stage_c_failed_at" in sql
    assert "NOT jsonb_exists(metadata, 'stage_c_classified_at')" in sql


def test_stage_d_worklist_still_requires_a_successful_stage_c() -> None:
    """The point of the whole change: nothing empty flows into Stage D.

    Stage D's gate is unchanged - it did not need to change, because a
    failed row no longer carries the key it selects on.
    """
    sql = _query_of(jobs.get_ungraded_geopolitical_signal_tagged_articles)
    assert "jsonb_exists(metadata, 'stage_c_classified_at')" in sql
    assert "stage_c_failed_at" not in sql


@pytest.mark.parametrize("fn", [
    jobs.get_untagged_geopolitical_signal_high_articles,
    jobs.get_ungraded_geopolitical_signal_tagged_articles,
])
def test_worklists_never_use_the_bare_jsonb_question_mark_operator(fn) -> None:
    """psycopg2 scans the whole query for ? and % placeholders, so
    `metadata ? 'key'` breaks the call. This has bitten this pipeline
    before.

    Comments are NOT exempt and are deliberately not stripped here.
    Confirmed against a real Postgres: a parameterised query carrying a
    `?` only inside an SQL comment still raises IndexError, because
    psycopg2 scans the raw string before the server ever sees it and
    never parses comments. So documenting the trap by quoting the broken
    form is itself the trap.
    """
    assert "metadata ? " not in _query_of(fn)
