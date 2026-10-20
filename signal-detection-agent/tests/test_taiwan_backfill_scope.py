"""One-off Taiwan backfill scope: only the chosen revenue period and the
chosen month's material announcements are classified; gdelt never is."""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))

from controllers.run import _in_taiwan_backfill_scope  # noqa: E402


def _rev(period):
    return {"metadata": {"source_category": "mops_revenue", "period_gregorian": period}}


def _mat(date):
    return {"metadata": {"source_category": "mops_material", "statement_date": date}}


def test_keeps_only_chosen_revenue_period():
    assert _in_taiwan_backfill_scope(_rev("2026-09"), "2026-09", "2026-10")
    assert not _in_taiwan_backfill_scope(_rev("2026-08"), "2026-09", "2026-10")


def test_keeps_only_chosen_month_announcements():
    assert _in_taiwan_backfill_scope(_mat("2026-10-01"), "2026-09", "2026-10")
    assert not _in_taiwan_backfill_scope(_mat("2026-09-30"), "2026-09", "2026-10")
    assert not _in_taiwan_backfill_scope(_mat(None), "2026-09", "2026-10")


def test_gdelt_excluded():
    gdelt = {"url": "https://x", "metadata": {"source_category": "gdelt"}}
    assert not _in_taiwan_backfill_scope(gdelt, "2026-09", "2026-10")


def test_unset_option_excludes_that_category():
    assert not _in_taiwan_backfill_scope(_rev("2026-09"), None, "2026-10")
    assert not _in_taiwan_backfill_scope(_mat("2026-10-05"), "2026-09", None)
