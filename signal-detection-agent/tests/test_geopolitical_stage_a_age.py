"""Stage A drops archive pages, and never drops anything for being undated.

The fetch runs --days-back 1, so nothing legitimate arrives weeks old.
Measured over 690 stored rows: 676 were under 3.4 days, 8 at ~6.5 days,
and the handful beyond that were a 2017 Tesla feature, three 2018-19
renewable round-ups and a "Top 5 Uranium Stocks" listicle 82 days old.
None of them was a signal.

The cutoff is 14 days rather than 7 because the age measured is the
ARTICLE's own publication date, and a legitimate item can lag: a
Federal Register notice records a measure taken earlier, and Stage A
pools several runs. 14 clears every real row observed by a factor of
two while the archive pages miss it by years.

The tests that matter most here are the ones asserting what is NOT
dropped. A date guard that fails closed would silently discard real
signals, which is worse than the archive pages it was added to catch.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pipeline.geopolitical_signal_classifier import (  # noqa: E402
    _MAX_ARTICLE_AGE_DAYS, _age_in_days,
    classify_geopolitical_signal_stage_a)


def _article(**kw):
    base = {"title": "EU adopts new sanctions package", "url": "http://x.com/a",
            "metadata": {"lang": "eng"}}
    base.update(kw)
    return base


def _iso(days_ago: float) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days_ago)).isoformat()


# --------------------------------------------------------------------
# dropped
# --------------------------------------------------------------------

def test_an_article_older_than_the_cutoff_is_dropped() -> None:
    out = classify_geopolitical_signal_stage_a(_article(published=_iso(90)))
    assert out["outcome"] == "noise"
    assert out["drop_reason"] == "too_old"


def test_the_real_archive_pages_observed_in_production_are_dropped() -> None:
    """2017-2021 electrek/siliconcanals archive pages - the rows this
    guard exists for."""
    for year in (2017, 2018, 2019, 2021):
        out = classify_geopolitical_signal_stage_a(
            _article(published=f"{year}-07-07T00:00:00+00:00"))
        assert out["drop_reason"] == "too_old", year


# --------------------------------------------------------------------
# NOT dropped - the cases a careless guard would break
# --------------------------------------------------------------------

def test_an_article_with_no_date_is_never_dropped_for_age() -> None:
    """Fail-open, same rule as the language check: "we don't know when
    this was published" must not read as "it is old"."""
    for missing in (None, "", "   "):
        out = classify_geopolitical_signal_stage_a(_article(published=missing))
        assert out.get("drop_reason") != "too_old", repr(missing)


def test_an_unparseable_date_is_not_dropped_for_age() -> None:
    out = classify_geopolitical_signal_stage_a(_article(published="last Tuesday"))
    assert out.get("drop_reason") != "too_old"


def test_a_future_dated_article_is_not_dropped() -> None:
    """Clock skew, or a publisher post-dating. A negative age is below
    any positive cutoff and must stay that way."""
    out = classify_geopolitical_signal_stage_a(_article(published=_iso(-3)))
    assert out.get("drop_reason") != "too_old"


@pytest.mark.parametrize("days", [0, 1, 3.4, 6.5, 13.9])
def test_every_age_seen_in_real_data_survives(days: float) -> None:
    """676 rows under 3.4 days, 8 at ~6.5 - none may be dropped."""
    out = classify_geopolitical_signal_stage_a(_article(published=_iso(days)))
    assert out.get("drop_reason") != "too_old", days


def test_the_cutoff_leaves_headroom_over_the_oldest_real_row() -> None:
    """6.5 days was the oldest legitimate row observed. A cutoff that
    did not at least double it would be one slow news cycle from
    dropping real signals."""
    assert _MAX_ARTICLE_AGE_DAYS >= 13


# --------------------------------------------------------------------
# the helper
# --------------------------------------------------------------------

def test_age_handles_datetime_naive_and_aware_and_z_suffix() -> None:
    assert _age_in_days(datetime.now(timezone.utc) - timedelta(days=5)) == pytest.approx(5, abs=0.01)
    naive = (datetime.now(timezone.utc) - timedelta(days=5)).replace(tzinfo=None)
    assert _age_in_days(naive) == pytest.approx(5, abs=0.01)
    z = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat().replace("+00:00", "Z")
    assert _age_in_days(z) == pytest.approx(2, abs=0.01)


@pytest.mark.parametrize("bad", [None, "", "   ", "not a date", 12345, [], {}])
def test_age_returns_none_rather_than_raising(bad) -> None:
    assert _age_in_days(bad) is None
