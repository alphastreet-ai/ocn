"""Stage B's event collapse must not destroy the fact that an article was seen.

The bug this pins, measured over two consecutive days of real runs:

  Stage B folded N near-duplicate rows into one judged row and DELETED
  them. Stage A's "already classified: skip" check reads article_id
  (get_existing_geopolitical_signal_article_ids). A deleted row has no
  article_id to find, so the next Stage A pass called those articles
  new and re-inserted them - resurrecting the exact duplicates the
  collapse had just folded away. 10 rows came back on one day, 14 on
  the next, and 24 articles ended up BOTH recorded as a sibling on the
  judged row AND present again as standalone rows, three of them as
  `signal`.

The fix keeps the row and marks it. These tests assert the two
properties that closes the loop on:

  1. the row survives, so article_id still resolves
  2. its verdict is terminal, so no later stage and no reader sees it

A test that only checked "collapse does not delete" would pass against
code that left the row as `signal`, which would be worse than the bug.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from models import jobs  # noqa: E402

_SIBLINGS = [
    {"id": 11, "outlet": "a.com", "title": "A", "url": "http://a.com/1"},
    {"id": 12, "outlet": "b.com", "title": "B", "url": "http://b.com/2"},
]


def _run_collapse(siblings=_SIBLINGS, judged=99):
    """Run the collapse against a fake DB; return the SQL it executed."""
    conn = MagicMock()
    conn.execute.return_value.rowcount = len(siblings)
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch.object(jobs, "get_db", return_value=ctx):
        n = jobs.collapse_geopolitical_signal_event_group(judged, siblings)
    return n, [c[0][0] for c in conn.execute.call_args_list], conn


def test_collapse_does_not_delete_the_folded_rows() -> None:
    """A deleted row loses its article_id, which is the only thing
    stopping Stage A re-inserting the article next run."""
    _, sqls, _ = _run_collapse()
    assert not any("DELETE" in s.upper() for s in sqls), \
        "collapse must tombstone, not delete - deletion is what let rows resurrect"


def test_collapse_marks_the_folded_rows_noise() -> None:
    """Terminal verdict: Stage C, D and E all select signal_detection
    = 'signal', so `noise` keeps the row out of every later stage and
    out of the feed, which is what deletion was for."""
    _, sqls, _ = _run_collapse()
    update = next(s for s in sqls if "signal_detection" in s)
    assert "'noise'" in update


def test_collapse_records_which_row_absorbed_it() -> None:
    """So a fold can be audited, or undone by hand."""
    _, sqls, conn = _run_collapse(judged=99)
    update = next(s for s in sqls if "collapsed_into" in s)
    assert "collapsed_at" in update
    params = conn.execute.call_args_list[-1][0][1]
    assert 99 in params, "the judged row id must be bound into the update"


def test_collapse_writes_a_reason_naming_the_judged_row() -> None:
    """Every row in this domain carries a signal_reason; a tombstoned
    row must say what happened to it rather than inherit a stale one."""
    _, _, conn = _run_collapse(judged=99)
    params = conn.execute.call_args_list[-1][0][1]
    reason = next(p for p in params if isinstance(p, str))
    assert "99" in reason and "also_reported_by" in reason


def test_collapse_still_appends_coverage_before_tombstoning() -> None:
    """Order matters: the sibling's outlet/title/url must be recorded
    on the judged row first, or a half-applied collapse hides rows that
    were never recorded as coverage."""
    _, sqls, _ = _run_collapse()
    assert "also_reported_by" in sqls[0]
    assert "signal_detection" in sqls[1]


def test_collapse_is_a_no_op_with_no_siblings() -> None:
    """Nothing to fold, nothing to touch."""
    conn = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch.object(jobs, "get_db", return_value=ctx):
        assert jobs.collapse_geopolitical_signal_event_group(99, []) == 0
    conn.execute.assert_not_called()


def test_collapse_returns_how_many_were_folded() -> None:
    n, _, _ = _run_collapse()
    assert n == len(_SIBLINGS)
