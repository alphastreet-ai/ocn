"""`last_run_at` must describe the domain the caller asked about.

The bug this pins: the agent_jobs lookup inside get_results_summary
bound a hardcoded 'macro_signal' instead of the requested source_type,
so every caller got macro_signal's last run. Confirmed live - three
different source_types all returned 2026-10-20T23:00:53, which was
macro_signal's job, while geopolitical_signal's own latest job had
completed thirteen hours later. The counts in the same response were
always filtered correctly, which is what made it hard to notice: the
payload looked domain-specific apart from this one field.

agent_jobs.domain is the RETRIEVAL domain and source_type is what the
resulting rows are stored as. Most domains use the same string for
both; two deliberately do not, and those two are the whole reason this
needs a mapping rather than passing source_type straight through.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from models import jobs  # noqa: E402


def _domain_queried(source_type: str) -> str:
    """Run the summary against a fake DB; return the domain it looked up."""
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    conn.execute.return_value.fetchone.return_value = None
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch.object(jobs, "get_db", return_value=ctx):
        jobs.get_results_summary(source_type=source_type)
    for call in conn.execute.call_args_list:
        sql = call[0][0]
        if "FROM agent_jobs" in sql:
            return call[0][1][0]
    raise AssertionError("no agent_jobs query was issued")


# --------------------------------------------------------------------
# the two domains whose names genuinely differ
# --------------------------------------------------------------------

def test_geopolitical_signal_maps_to_its_retrieval_domain() -> None:
    """Rows are stored as geopolitical_signal; the jobs that made them
    run under geopolitical_news."""
    assert _domain_queried("geopolitical_signal") == "geopolitical_news"


def test_news_maps_to_its_retrieval_domain() -> None:
    """Rows are stored as news; the jobs run under ai_news."""
    assert _domain_queried("news") == "ai_news"


# --------------------------------------------------------------------
# every domain whose names match passes straight through
# --------------------------------------------------------------------

@pytest.mark.parametrize("source_type", [
    "macro_signal",
    "sec_filing",
    "taiwan_market_signal",
    "japan_market_signal",
    "china_market_signal",
    "korea_market_signal",
])
def test_matching_names_pass_through(source_type: str) -> None:
    assert _domain_queried(source_type) == source_type


def test_an_unknown_source_type_is_not_rewritten() -> None:
    """A domain added later under the matching-name convention needs no
    entry in the map, so the fallthrough must be source_type itself -
    never a default that silently reports another domain's run."""
    assert _domain_queried("some_future_domain") == "some_future_domain"


# --------------------------------------------------------------------
# the specific regression
# --------------------------------------------------------------------

def test_no_source_type_is_answered_with_macro_signals_job() -> None:
    """The original bug in one assertion: nothing but macro_signal
    itself may look up macro_signal's jobs."""
    for st in ("geopolitical_signal", "news", "sec_filing",
               "taiwan_market_signal", "japan_market_signal",
               "china_market_signal", "korea_market_signal"):
        assert _domain_queried(st) != "macro_signal", \
            f"{st} must not report macro_signal's last run"


def test_counts_and_last_run_use_the_same_source_type() -> None:
    """The count queries bind source_type directly. If the jobs lookup
    ever diverges again, the response mixes one domain's counts with
    another's timestamp - which is exactly how this shipped."""
    conn = MagicMock()
    conn.execute.return_value.fetchall.return_value = []
    conn.execute.return_value.fetchone.return_value = None
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=conn)
    ctx.__exit__ = MagicMock(return_value=False)
    with patch.object(jobs, "get_db", return_value=ctx):
        jobs.get_results_summary(source_type="geopolitical_signal")

    counted = [c[0][1][0] for c in conn.execute.call_args_list
               if "FROM agent_classifications" in c[0][0] and c[0][1]]
    assert counted, "expected at least one filtered count query"
    assert all(p == "geopolitical_signal" for p in counted)
