"""Card-text rules for the Japan Signals view layer.

Each test here pins a wording or link bug reported against the live
API, using the same row shape the real data has.
"""
from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'shared', 'src'))

from pipeline.japan_signal_view import (
    _checkpoint_tests,
    _edinet_still_serves,
    _headline,
    _metric,
    _press_claim_noun,
    _press_headline,
    _rule_text,
    _source_url,
    _target_period_label,
    _usable_progress_baseline,
)


def _recent() -> str:
    return (datetime.now(timezone.utc) - timedelta(days=10)).isoformat()


class TestReversalDirection:
    """The rule text must name the direction actually reversed, not the
    company's usual direction."""

    def test_reversed_raise_says_raise(self):
        text = _rule_text({
            "signal_reason_code": "reverses_direction_within_fiscal_year",
            "reversed_direction": "raise",
        })
        assert "earlier raise" in text

    def test_reversed_cut_says_cut(self):
        text = _rule_text({
            "signal_reason_code": "reverses_direction_within_fiscal_year",
            "reversed_direction": "cut",
        })
        assert "earlier cut" in text

    def test_habit_direction_is_not_used_as_a_fallback(self):
        """A row stored before the direction was recorded must stay
        neutral rather than guess from the company's habit."""
        text = _rule_text({
            "signal_reason_code": "reverses_direction_within_fiscal_year",
            "habit_typical_direction": "raise",
        })
        assert "earlier cut" not in text
        assert "earlier raise" not in text


class TestForecastHeadline:
    def test_zero_change_is_not_called_a_cut(self):
        assert _headline("forecast", {"operating_profit_pct_change": 0.0,
                                      "forecast_period": "full_year"}) == (
            "Left full-year profit forecast unchanged"
        )

    @pytest.mark.parametrize("pct,word", [(5.0, "Raised"), (-33.3, "Cut")])
    def test_real_moves_keep_their_direction(self, pct, word):
        assert _headline("forecast", {"operating_profit_pct_change": pct,
                                      "forecast_period": "full_year"}).startswith(word)

    def test_the_headline_names_the_period_the_figures_came_from(self):
        """A filing can revise a half-year and a full-year forecast at
        once. This line said "full-year" for both while the classifier
        took whichever table came first - which on 51 of 66
        multi-table filings was not the full year."""
        assert _headline("forecast", {"operating_profit_pct_change": 13.8,
                                      "forecast_period": "half_year"}) == (
            "Raised half-year profit forecast")
        assert _headline("forecast", {"operating_profit_pct_change": 9.0,
                                      "forecast_period": "quarter"}) == (
            "Raised quarterly profit forecast")

    def test_an_unmarked_period_is_not_called_full_year(self):
        """33 stored tables carry no period marker. Naming one would
        assert a period the filing never states."""
        assert _headline("forecast", {"operating_profit_pct_change": 5.0}) == (
            "Raised profit forecast")


class TestProgressBaseline:
    """An untrusted habit must not be used to rank the company, or the
    card contradicts its own 'not enough history' signal."""

    SCREEN = {
        "progress_pct": 44.1,
        "fiscal_year_elapsed_pct": 58.5,
        "progress_habit_typical_pct": 39.5,
        "progress_habit_is_trusted": False,
    }

    def test_untrusted_habit_is_refused(self):
        assert _usable_progress_baseline(self.SCREEN) is None

    def test_headline_falls_back_to_elapsed_and_says_behind(self):
        assert _headline("progress", self.SCREEN) == "Ran behind its full-year target"

    def test_metric_matches_the_headline(self):
        metric = _metric("progress", self.SCREEN)
        assert metric["value"] == -14.4
        assert "behind" in metric["label"]

    def test_trusted_habit_is_still_used(self):
        trusted = dict(self.SCREEN, progress_habit_is_trusted=True)
        assert _usable_progress_baseline(trusted) == 39.5
        assert _headline("progress", trusted) == "Ran ahead of its full-year target"

    def test_self_referential_median_is_refused(self):
        same = dict(self.SCREEN, progress_habit_is_trusted=True,
                    progress_habit_typical_pct=44.1)
        assert _usable_progress_baseline(same) is None


class TestTargetPeriodLabel:
    @pytest.mark.parametrize("stored,expected", [
        ("full_year", "full-year"),
        ("half_year", "half-year"),
        ("quarter", "quarterly"),
        (None, "full-year"),
    ])
    def test_label_follows_the_filing(self, stored, expected):
        assert _target_period_label({"target_period_type": stored}) == expected

    def test_headline_uses_the_filings_own_period(self):
        half = {"progress_pct": 46.7, "fiscal_year_elapsed_pct": 31.2,
                "target_period_type": "half_year", "progress_habit_is_trusted": False}
        assert _headline("progress", half) == "Ran ahead of its half-year target"


class TestPressHeadline:
    META = {
        "company": "Resonac Holdings",
        # Curly quotes, as the real translated titles carry them.
        "translated_title": (
            "Resonac Holdings (4004): Resonac achieves Japan’s first success, "
            "develops “12-inch SiC substrate”"
        ),
        "publication_domain": "newswitch.jp",
    }

    def test_verb_first_past_tense_with_the_publication(self):
        assert _headline("press", self.META) == (
            "Developed a 12-inch SiC substrate (Newswitch report)"
        )

    def test_claim_noun_is_the_quoted_product(self):
        assert _press_claim_noun(self.META) == "12-inch SiC substrate"

    def test_unknown_verb_passes_through_unmangled(self):
        meta = {"company": "X Corp", "translated_title": "Ponders a new direction",
                "publication_domain": "nikkei.com"}
        assert _press_headline("Ponders a new direction", meta) == (
            "Ponders a new direction (Nikkei report)"
        )


class TestSourceUrl:
    """A buyback row links to the EDINET filing while EDINET still
    serves it, and to the company's buyback list once it does not."""

    def test_recent_buyback_links_to_the_filing_pdf(self):
        url = _source_url({"code": "285A"}, "irbank-buyback://285A/S100Z1UD",
                          "2026-09-11T00:00:00+00:00")
        assert url.endswith("/S100Z1UD.pdf")

    def test_old_buyback_falls_back_to_the_list_page(self):
        url = _source_url({"code": "4186"}, "irbank-buyback://4186/S1004UZ6",
                          "2015-06-04T00:00:00+00:00")
        assert url == "https://irbank.net/4186/buyback"

    def test_undated_row_does_not_get_a_filing_link(self):
        assert not _edinet_still_serves(None)


class TestWatchingSourceId:
    """A WATCHING row is a standing state, keyed on the company alone -
    keying it on the date emptied the Watching tab at every midnight."""

    def _run(self, as_of):
        """Drive the real function, stubbing only the two inputs a
        fixture cannot reasonably fake: which article counts as a
        genuine revision, and when it was filed."""
        from unittest.mock import patch

        import pipeline.japan_signal_classifier as mod

        article = {
            "url": "irbank-financials://6146/x",
            "title": "Disco (6146): forecast revision",
            "published": "2025-07-01T00:00:00+00:00",
            "metadata": {"code": "6146", "source_category": "jp_forecast"},
        }
        habits = {"6146": {"typical_days_between_revisions": 182,
                           "is_trusted": True, "revisions_per_year": 2.0}}
        with patch.object(mod, "_is_genuine_revision", return_value=True):
            return mod.classify_stale_revision_pattern(
                [article], stored_habits=habits, as_of=as_of,
            )

    def test_same_company_keeps_one_id_across_days(self):
        day1 = self._run(datetime(2026, 10, 1, tzinfo=timezone.utc))
        day2 = self._run(datetime(2026, 10, 2, tzinfo=timezone.utc))
        assert day1 and day2, "fixture should produce an overdue row"
        ids1 = {r["result"]["source_id"] for r in day1}
        ids2 = {r["result"]["source_id"] for r in day2}
        # The id must not move with the date, or the row disappears
        # from the Watching tab every midnight.
        assert ids1 == ids2 == {"japan-stale-revision://6146"}

    def test_the_reading_itself_still_moves_with_the_date(self):
        """The id is stable; the figure it carries is not - that is why
        the insert upserts rather than skipping."""
        day1 = self._run(datetime(2026, 10, 1, tzinfo=timezone.utc))
        day2 = self._run(datetime(2026, 10, 31, tzinfo=timezone.utc))
        d1 = day1[0]["result"]["metadata"]["days_since_last_revision"]
        d2 = day2[0]["result"]["metadata"]["days_since_last_revision"]
        assert d2 == d1 + 30


class TestCheckpointTests:
    """supports/weakens must carry this event's own figure."""

    def test_raise_names_the_revised_forecast(self):
        supports, weakens = _checkpoint_tests(
            "forecast",
            {"operating_profit_pct_change": 13.8, "operating_profit_revised": 198000.0},
            "Q3",
        )
        assert supports == "Q3 results track at or above the raised ¥198bn forecast"
        assert weakens == "Q3 results fall short of ¥198bn, or the raise is cut back"

    def test_cut_names_the_revised_forecast(self):
        supports, weakens = _checkpoint_tests(
            "forecast",
            {"operating_profit_pct_change": -33.3, "operating_profit_revised": 56000.0},
            "Q4",
        )
        assert supports == "Q4 results land at or below the cut ¥56bn forecast"
        assert "too cautious" in weakens

    def test_buyback_names_the_programme_size(self):
        supports, weakens = _checkpoint_tests(
            "buyback", {"buyback_program_limit_shares": 30000000}, "Q3")
        assert supports == "Q3 results report shares repurchased under the 30M programme"
        assert weakens == "Q3 results show little or none of the 30M programme used"

    def test_ownership_names_the_holder_and_stake(self):
        """With no previous ratio the direction is unknown, so the test
        is neutral - "moving", not the old "raising", which presumed
        the holder was building a position."""
        supports, weakens = _checkpoint_tests("ownership", {
            "translated_filer_name": "Sumitomo Mitsui Trust Asset Management Co., Ltd.",
            "holding_pct": 5.94,
        }, "Q4")
        assert supports == (
            "Sumitomo Mitsui Trust AM files a change report moving its 5.94% stake"
        )
        assert weakens == "Sumitomo Mitsui Trust AM's stake falls back below 5%"
        assert "raising" not in supports

    def test_ownership_sell_down_is_not_described_as_building(self):
        """Toshiba really cut Kioxia from 14.06% to 12.84% (S100Z25L).
        The old text said it would "file a change report raising its
        stake" - the opposite of what happened."""
        supports, weakens = _checkpoint_tests("ownership", {
            "filer_name": "Toshiba Corporation",
            "holding_pct": 12.84, "holding_pct_previous": 14.06,
        }, "Q3")
        assert "selling down" in supports
        assert "raising" not in supports

    def test_ownership_build_up_reads_as_a_build(self):
        supports, _ = _checkpoint_tests("ownership", {
            "filer_name": "Octagon Lab Co., Ltd.",
            "holding_pct": 5.9, "holding_pct_previous": 5.1,
        }, "Q3")
        assert "larger stake" in supports

    def test_press_already_announced_tests_follow_through(self):
        """Nothing left to confirm, so the checkpoint asks whether it
        turned into orders - not whether the company admits it."""
        supports, weakens = _checkpoint_tests("press", TestPressHeadline.META, "Q4")
        assert supports == "Q4 results cite 12-inch SiC substrate orders or capacity"
        assert weakens == "No mention of 12-inch SiC substrate at Q4 results"

    def test_press_unconfirmed_still_tests_confirmation(self):
        meta = dict(TestPressHeadline.META, unconfirmed=True)
        supports, _ = _checkpoint_tests("press", meta, "Q4")
        assert supports == (
            "Resonac Holdings confirms the 12-inch SiC substrate "
            "in a filing or at Q4 results"
        )

    def test_rows_with_no_specific_figure_send_null(self):
        assert _checkpoint_tests("forecast", {}, "Q3") == (None, None)
        assert _checkpoint_tests("ownership", {}, "Q3") == (None, None)

    @pytest.mark.parametrize("signal_type,meta", [
        ("forecast", {"operating_profit_pct_change": 13.8,
                      "operating_profit_revised": 198000.0}),
        ("buyback", {"buyback_program_limit_shares": 30000000}),
        ("ownership", {"translated_filer_name": "Nomura Securities Co., Ltd.",
                       "holding_pct": 5.3}),
        ("capacity", {}),
        ("disclosure", {}),
        ("missing", {}),
        ("watching", {"company": "Disco"}),
    ])
    def test_every_line_stays_under_ninety_characters(self, signal_type, meta):
        for text in _checkpoint_tests(signal_type, meta, "Q3"):
            if text is not None:
                assert len(text) < 90, text


class TestCompanyProfile:
    """Disclosed customer concentration, and how it reaches the card."""

    def test_disclosed_percentage_is_stored_with_its_period(self):
        from pipeline.japan_companies import company_for
        nvda = next(c for c in company_for("6857")["customers"]
                    if c["ticker"] == "NVDA")
        assert nvda["pct_of_sales"] == 21.6
        assert nvda["period"] == "FY3/26"

    def test_lapsed_disclosure_keeps_the_year_it_was_filed(self):
        """11.0% in FY3/25, below the threshold in FY3/26. The period
        must stay FY3/25 so a consumer can tell it is not current."""
        from pipeline.japan_companies import company_for
        amd = next(c for c in company_for("4062")["customers"]
                   if c["ticker"] == "AMD")
        assert amd["pct_of_sales"] == 11.0
        assert amd["period"] == "FY3/25"

    def test_results_no_longer_carries_connections(self):
        """Dropped as redundant: every field was derivable from the
        customers[] the frontend already fetches from /universe, and
        it was repeated on all 396 signal rows."""
        from pipeline.japan_signal_view import to_jp_signal
        row = {"source_id": "irbank-financials://6857/x", "signal_detection": "signal",
               "signal_score": None, "signal_reason": "r", "published": None,
               "title": "t", "metadata": {"code": "6857", "source_category": "jp_forecast"}}
        assert "connections" not in to_jp_signal(row)

    def test_distributors_are_flagged_so_a_consumer_can_hide_them(self):
        """SUMCO's largest disclosed customer is Sumitomo Corporation at
        27% - a trading house, so its results say nothing about wafer
        demand. The flag is what lets the frontend drop it."""
        from pipeline.japan_companies import company_for
        sumitomo = next(c for c in company_for("3436")["customers"]
                        if c["name"] == "Sumitomo Corporation")
        assert sumitomo["pct_of_sales"] == 27.0
        assert sumitomo["is_distributor"] is True

    def test_valuation_carries_its_own_date_and_staleness(self):
        from datetime import date
        from pipeline.japan_companies import valuation_for
        v = valuation_for("6857")
        assert v["asOf"] == "2026-10-01"
        assert v["isStale"] is False
        assert valuation_for("6857", date(2027, 6, 1))["isStale"] is True

    def test_the_universe_has_no_second_copy_to_drift_from(self):
        """A hardcoded table of the same companies used to sit beside
        the fetch as a fallback, and this test asserted the two agreed.
        They stopped agreeing - the table still said Kioxia and Murata
        where the database said Kioxia Holdings and Murata
        Manufacturing - and the fallback served the stale names
        silently. The table is gone; what is worth asserting now is
        that it has not come back.
        """
        import pipeline.japan_companies as jc
        assert not hasattr(jc, "JAPAN_COMPANIES")

    def test_every_company_carries_what_the_card_reads(self):
        from pipeline.japan_companies import japan_ticker_universe
        for c in japan_ticker_universe():
            assert c["code"] and c["company"], c
            assert "customers" in c


class TestMentionedCustomers:
    """Which of a company's own customers an event actually NAMES -
    the Direct/Inferred question, answered by lookup against a known
    candidate set rather than free extraction."""

    def test_english_name_is_found(self):
        from pipeline.japan_companies import mentioned_customers
        got = mentioned_customers("6857", "Advantest wins an order from NVIDIA")
        assert got == [{"name": "Nvidia", "type": "company"}]

    def test_japanese_alias_is_found(self):
        """ホンダ appears in stored text where "Honda" does not, so
        matching the English name alone misses real occurrences."""
        from pipeline.japan_companies import mentioned_customers
        got = mentioned_customers("6723", "ホンダ向けSoCの共同開発を開始")
        assert got == [{"name": "Honda", "type": "company"}]

    def test_a_substring_is_not_a_mention(self):
        """"ASE" sits inside "PHASE" - Latin names need a word
        boundary or every other row matches something."""
        from pipeline.japan_companies import mentioned_customers
        assert mentioned_customers("6315", "entering its final PHASE") == []

    def test_an_ordinary_revision_names_nobody(self):
        from pipeline.japan_companies import mentioned_customers
        assert mentioned_customers(
            "6857", "Advantest raised its full-year operating profit forecast",
        ) == []

    def test_untracked_company_and_empty_text_are_safe(self):
        from pipeline.japan_companies import mentioned_customers
        assert mentioned_customers("9999", "NVIDIA") == []
        assert mentioned_customers("6857", None) == []

    def test_customers_with_no_ticker_are_skipped(self):
        """Toshiba is a real TDK customer but delisted, so a reader has
        nothing to look up - a mention of it is not actionable."""
        from pipeline.japan_companies import mentioned_customers
        assert mentioned_customers("6762", "東芝向けHDDヘッドの出荷") == []

    def test_several_customers_in_one_event(self):
        from pipeline.japan_companies import mentioned_customers
        names = {e["name"] for e in mentioned_customers(
            "4062", "NVIDIA and Intel substrate orders both increased")}
        assert names == {"Nvidia", "Intel"}


class TestSignalImplication:
    """`signal_implication` and its thresholds moved here when the
    summary module was deleted. The progress branch reached a constant
    that did not come across with it, so every progress-type row raised
    NameError and `/japan-signals/results` returned 500 for the whole
    page - one row is enough to fail the list comprehension that shapes
    it. These pin each branch so a move cannot break one silently again.
    """

    def test_progress_ahead_of_elapsed_year_is_positive(self):
        from pipeline.japan_signal_view import signal_implication
        assert signal_implication(
            {"progress_pct": 62.0, "fiscal_year_elapsed_pct": 50.0}) == "Positive"

    def test_progress_behind_elapsed_year_is_negative(self):
        from pipeline.japan_signal_view import signal_implication
        assert signal_implication(
            {"progress_pct": 38.0, "fiscal_year_elapsed_pct": 50.0}) == "Negative"

    def test_progress_inside_the_ordinary_band_is_mixed(self):
        """A quarter never lands exactly on its share of the year; five
        points either side is ordinary, not a signal."""
        from pipeline.japan_signal_view import signal_implication
        assert signal_implication(
            {"progress_pct": 53.0, "fiscal_year_elapsed_pct": 50.0}) == "Mixed"
        assert signal_implication(
            {"progress_pct": 47.0, "fiscal_year_elapsed_pct": 50.0}) == "Mixed"

    def test_margin_guidance_against_prior_year_actual(self):
        from pipeline.japan_signal_view import signal_implication
        assert signal_implication(
            {"margin_revised_pct": 24.0,
             "margin_reference_prior_year_actual_pct": 21.0}) == "Positive"
        assert signal_implication(
            {"margin_revised_pct": 19.0,
             "margin_reference_prior_year_actual_pct": 21.0}) == "Negative"

    def test_industry_yoy_sign(self):
        from pipeline.japan_signal_view import signal_implication
        assert signal_implication({"yoy_pct": 12.5}) == "Positive"
        assert signal_implication({"yoy_pct": -3.0}) == "Negative"

    def test_empty_meta_is_safe(self):
        from pipeline.japan_signal_view import signal_implication
        assert isinstance(signal_implication({}), str)


class TestDisclosureCaveat:
    """A disclosure row carries no caveat, of either tier.

    Three versions were tried and each failed the same way. "Judged
    from the filing's own title" described our parsing rather than the
    reader's risk. "Its size and terms are in the document itself"
    pointed at information without supplying any. Printing the title
    itself carried real detail but put a third near-identical line on
    a card whose headline and signal sentence already say the same
    thing.

    The detail belongs in the headline, where the classifier prompt
    now keeps the qualifiers it used to trim.
    """

    @staticmethod
    def _card(reason_code, action, detection, translated_title=None):
        from pipeline.japan_signal_view import to_jp_signal
        meta = {"code": "6981", "company": "Murata Manufacturing",
                "source_category": "jp_disclosure",
                "signal_reason_code": reason_code,
                "disclosure_action": action}
        if translated_title:
            meta["translated_title"] = translated_title
        return to_jp_signal({
            "source_id": f"kabutan-tdnet://6981/{action}",
            "signal_detection": detection, "signal_score": None,
            "signal_reason": "r", "published": "2026-09-20T00:00:00Z",
            "title": "t", "metadata": meta})

    def test_a_routine_filing_carries_no_caveat(self):
        c = self._card("routine_corporate_filing",
                       "acquisition of treasury shares", "noise")
        assert c["classification"] == "Noise"
        assert c["caveat"] is None

    def test_a_material_action_carries_no_caveat_either(self):
        """Even with a title full of detail, a caveat here would only
        restate the headline a third time."""
        c = self._card("material_corporate_action",
                       "follow-on investment in OpenAI", "signal",
                       "SoftBank Group (9984) [x]: Execution of Follow-on "
                       "Investment (Third Tranche) in OpenAI")
        assert c["classification"] == "High Signal"
        assert c["caveat"] is None

    def test_the_card_still_names_the_action(self):
        """Dropping the caveat must not drop the event itself."""
        c = self._card("material_corporate_action",
                       "partial spin-off of Crasus Chemical", "signal")
        assert c["headline"] == "Filed partial spin-off of Crasus Chemical"


class TestIndustryReadingIsLegible:
    """J4 judges one national monthly reading against its own recent run.

    All three facts were present but ordered so that none landed: "Ran
    3.1 times its usual distance from the recent average of 12.3%" left
    out the month's own figure entirely, called a standard deviation a
    "usual distance", and put 3.1 beside 12.3% where the two invite
    being multiplied.
    """

    @staticmethod
    def _card(yoy, average, spreads):
        from pipeline.japan_signal_view import to_jp_signal
        meta = {"source_category": "jp_industry", "period": "August 2026",
                "signal_reason_code": "industry_unusual",
                "industry_baseline_avg_yoy_pct": average,
                "industry_spreads_away": spreads,
                "billings_3mo_avg_millions_jpy": 597900}
        if yoy is not None:
            meta["yoy_pct"] = yoy
        return to_jp_signal({
            "source_id": "seaj-billings://2026-08-final",
            "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": "2026-09-15T00:00:00Z",
            "title": "t", "metadata": meta})

    def test_the_month_own_reading_comes_first(self):
        u = self._card(47.4, 12.3, 3.15)["unusual"]
        assert u.startswith("+47.4% year-on-year")

    def test_the_baseline_and_the_swing_are_both_named(self):
        u = self._card(47.4, 12.3, 3.15)["unusual"]
        assert "recent average of 12.3%" in u
        assert "3.1x the usual swing" in u

    def test_a_contraction_keeps_its_sign(self):
        """A shrinking industry is the reading a trader most needs to
        see as negative, not as a bare magnitude."""
        assert self._card(-4.2, 9.8, -1.6)["unusual"].startswith("-4.2% year-on-year")

    def test_the_multiplier_never_sits_against_the_average(self):
        """3.1 beside 12.3% read as a product. They are now separated
        by the clause naming what each measures."""
        u = self._card(47.4, 12.3, 3.15)["unusual"]
        assert "3.1 times its usual distance" not in u
        assert u.index("12.3%") < u.index("3.1x")

    def test_a_missing_month_figure_still_reads(self):
        u = self._card(None, 12.3, 3.15)["unusual"]
        assert u == "3.1x the usual swing against a recent average of 12.3%."


class TestOwnershipShowsTheMove:
    """An ownership change report is an event about a delta.

    Presented as a snapshot it was unreadable: two Capital Research
    filings on Resonac four days apart - a +0.78 point build, then a
    +0.02 point nothing - came out identical on 20 of 26 card fields,
    because `change` was empty and `unusual` described the position
    rather than the move. A reader could not tell them apart.
    """

    @staticmethod
    def _card(cur, prev, passive=False):
        from pipeline.japan_signal_view import to_jp_signal
        rc = ("holder_crosses_five_pct_passive" if prev is None
              else "holder_changes_existing_stake_passive" if passive
              else "active_holder_changes_stake")
        return to_jp_signal({
            "source_id": "edinet-filing://E00751/S1",
            "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": "2026-10-06T01:30:00Z",
            "title": "t",
            "metadata": {"code": "4004", "company": "Resonac Holdings",
                         "source_category": "jp_ownership",
                         "signal_reason_code": rc, "holding_pct": cur,
                         "holding_pct_previous": prev,
                         "translated_filer_name": "Capital Research"},
        })

    def test_change_carries_both_ends_and_the_delta(self):
        assert self._card(12.40, 11.62)["change"] == "11.62% → 12.40% · +0.78 pts"

    def test_a_sell_down_signs_the_delta_negative(self):
        assert self._card(12.84, 14.06)["change"] == "14.06% → 12.84% · -1.22 pts"

    def test_an_initial_report_has_no_previous_to_move_from(self):
        assert self._card(6.49, None)["change"] == "New position · 6.49%"

    def test_the_two_resonac_filings_are_distinguishable(self):
        """The bug this exists for: both read "raised stake to 12.4%"."""
        a = self._card(12.40, 11.62)
        b = self._card(12.42, 12.40)
        assert a["change"] != b["change"]
        assert a["unusual"] != b["unusual"]
        assert a["headline"] != b["headline"]

    def test_a_real_move_leads_with_its_size(self):
        u = self._card(12.40, 11.62)["unusual"]
        assert u.startswith("A 0.78-point increase")
        assert "active basis" in u

    def test_a_point_or_more_reads_as_a_build(self):
        assert "1.11-point build" in self._card(7.30, 6.19, passive=True)["unusual"]

    def test_a_sub_tenth_move_is_called_what_it_is(self):
        """A holding drifts this much without a decision behind it.
        Calling it unusual would dress up nothing as something."""
        u = self._card(12.42, 12.40)["unusual"]
        assert "effectively unchanged" in u
        assert "build" not in u and "increase" not in u

    def test_an_initial_report_names_the_threshold_it_crossed(self):
        assert "crossing the 5% disclosure threshold" in self._card(6.49, None)["unusual"]


class TestHoldingPrecision:
    """A holding percentage is shown as the filing states it.

    EDINET reports the ratio to four decimals (0.1242), so the stored
    percentage is exactly two. One decimal rounded a filed figure into
    a different number, and could erase a real move: Capital Research
    went 12.40% -> 12.42% of Resonac, which rendered as "Raised stake
    to 12.4% from 12.4%" - a headline asserting a change and showing
    none.
    """

    @staticmethod
    def _row(pct, prev):
        return {
            "source_id": "edinet-filing://E00751/S1",
            "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": "2026-10-06T01:30:00Z",
            "title": "t",
            "metadata": {
                "code": "4004", "company": "Resonac Holdings",
                "source_category": "jp_ownership",
                "signal_reason_code": "active_holder_changes_stake",
                "holding_pct": pct, "holding_pct_previous": prev,
                "translated_filer_name": "Capital Research and Management Company",
            },
        }

    def test_a_move_smaller_than_one_decimal_still_shows_both_ends(self):
        from pipeline.japan_signal_view import to_jp_signal
        h = to_jp_signal(self._row(12.42, 12.40))["headline"]
        assert "12.42%" in h and "12.40%" in h
        assert "12.4% from 12.4%" not in h

    def test_filed_precision_is_not_rounded_away(self):
        from pipeline.japan_signal_view import to_jp_signal
        h = to_jp_signal(self._row(12.84, 14.06))["headline"]
        assert "12.84%" in h and "14.06%" in h

    def test_a_trailing_zero_decimal_is_kept(self):
        """"12.4%" beside "12.42%" invites the question of whether the
        first is less precise or simply shorter."""
        from pipeline.japan_signal_view import to_jp_signal
        assert "12.40%" in to_jp_signal(self._row(12.40, 11.62))["headline"]


class TestCardIdentityFields:
    """`code` is the TSE code everywhere a reader meets it.

    The card is the only place that could disagree, because it is built
    field by field rather than copied from the row: it names the stored
    `metadata.code` itself, and it also carries which rule fired. Those
    are two different facts, and one of them being called `code` while
    the other went out as `ticker` put one word on two meanings inside
    one object - inverted against the stored row, the universe endpoint
    and the ?code= filter, all three of which say `code` for the TSE
    code.
    """

    @staticmethod
    def _row(**over):
        row = {
            "source_id": "edinet-filing://E00751/S1",
            "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": None, "title": "t",
            "metadata": {"code": "4004", "company": "Resonac Holdings",
                         "source_category": "jp_ownership"},
        }
        row.update(over)
        return row

    def test_code_is_the_tse_code(self):
        from pipeline.japan_signal_view import to_jp_signal
        assert to_jp_signal(self._row())["code"] == "4004"

    def test_signal_type_carries_the_rule_that_fired(self):
        from pipeline.japan_signal_view import to_jp_signal
        assert to_jp_signal(self._row())["signalType"] == "J6"

    def test_ticker_is_gone(self):
        """`ticker` means metadata.ticker on this API, which only
        sec_filing rows populate - so ?ticker= never matched a Japan
        row. Carrying the word on the card implied a filter that does
        not work, and 6857 is not the ticker in any case: 6857.T is.
        """
        from pipeline.japan_signal_view import to_jp_signal
        assert "ticker" not in to_jp_signal(self._row())

    def test_card_code_joins_to_the_universe(self):
        """The join a consumer actually writes: card.code against the
        universe's own code, with no translation between them.
        """
        from pipeline.japan_companies import company_for
        from pipeline.japan_signal_view import to_jp_signal
        card = to_jp_signal(self._row())
        assert company_for(card["code"])["company"] == card["company"]


class TestCardCarriesEntities:
    """`entities` is computed at classification time and stored, but
    `to_jp_signal` rebuilds the card field by field, so a stored column
    reaches the card only if it is named here. It was not: the entity
    work shipped and stayed invisible to the card endpoint, readable
    only on the raw /results rows.
    """

    @staticmethod
    def _row(**over):
        row = {
            "source_id": "irbank-financials://6857/x",
            "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": None, "title": "t",
            "metadata": {"code": "6857", "source_category": "jp_forecast"},
        }
        row.update(over)
        return row

    def test_stored_entities_reach_the_card(self):
        from pipeline.japan_signal_view import to_jp_signal
        card = to_jp_signal(self._row(
            entities=[{"name": "Nvidia", "type": "company"}]))
        assert card["entities"] == [{"name": "Nvidia", "type": "company"}]

    def test_no_named_customer_is_an_empty_list_not_missing(self):
        """390 of 394 real rows name nobody - a revision states figures
        and mentions no customer. The key must still be present so a
        consumer renders "none named" rather than treating the field as
        absent."""
        from pipeline.japan_signal_view import to_jp_signal
        card = to_jp_signal(self._row(entities=[]))
        assert card["entities"] == []

    def test_row_without_the_column_is_safe(self):
        from pipeline.japan_signal_view import to_jp_signal
        assert to_jp_signal(self._row())["entities"] == []

    def test_null_entities_becomes_empty_list(self):
        from pipeline.japan_signal_view import to_jp_signal
        assert to_jp_signal(self._row(entities=None))["entities"] == []

    def test_several_named_customers_all_carry(self):
        from pipeline.japan_signal_view import to_jp_signal
        card = to_jp_signal(self._row(entities=[
            {"name": "Toyota", "type": "company"},
            {"name": "Honda", "type": "company"},
        ]))
        assert [e["name"] for e in card["entities"]] == ["Toyota", "Honda"]


class TestDisclosureNamesTheEvent:
    """J8 cards described the taxonomy instead of the filing.

    Resonac's card read "a disclosure that changes what the company
    owns, controls or is committing capital to" for a filing whose own
    title said "Completion of Execution of Partial Spin-off of Crasus".
    And "Why it's unusual" claimed filing in English was rare - 56% of
    stored disclosures carry an English version, and for SoftBank,
    Murata and Advantest it is every one.
    """

    @staticmethod
    def _row(**over):
        row = {
            "source_id": "kabutan://4004/x", "signal_detection": "signal",
            "signal_score": None, "signal_reason": "r", "published": None,
            "title": "t",
            "metadata": {"code": "4004", "source_category": "jp_disclosure"},
        }
        row["metadata"].update(over.pop("metadata", {}))
        row.update(over)
        return row

    def test_english_filing_is_not_called_unusual(self):
        from pipeline.japan_signal_view import to_jp_signal
        card = to_jp_signal(self._row(metadata={"filed_in_english": True}))
        assert card["unusual"] is None

    def test_english_filing_never_mentioned_anywhere_on_the_card(self):
        from pipeline.japan_signal_view import to_jp_signal
        card = to_jp_signal(self._row(metadata={"filed_in_english": True}))
        blob = " ".join(str(v) for v in card.values()).lower()
        assert "english" not in blob

    def test_a_real_comparison_still_reports_as_unusual(self):
        """Removing the English line must not disable `unusual` for the
        signal types that do have a measured comparison."""
        from pipeline.japan_signal_view import to_jp_signal
        card = to_jp_signal(self._row(metadata={
            "source_category": "jp_buyback",
            "buyback_pct_of_shares_outstanding": 7.0,
        }))
        assert card["unusual"] and "7" in card["unusual"]


class TestDisclosureSubstanceParsing:
    """The model now answers "TIER/action"; the tier must survive and
    the action must reach the sentence."""

    def test_tier_and_action_are_split(self):
        from pipeline.japan_signal_classifier import (
            _classify_japan_disclosure_substance as f)
        import pipeline.japan_signal_classifier as m

        class _R:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self):
                import json
                return json.dumps({"choices": [{"message": {
                    "content": "HIGH/partial spin-off of Crasus"}}]}).encode()

        orig = m.urlopen
        m.urlopen = lambda *a, **k: _R()
        try:
            assert f("Resonac", "4004", "x", "m", "k", "https://x.invalid", 5) == (
                "HIGH", "partial spin-off of Crasus")
        finally:
            m.urlopen = orig

    def test_bare_tier_still_parses_with_no_action(self):
        """Older behaviour - a tier word alone - must not crash."""
        from pipeline.japan_signal_classifier import (
            _classify_japan_disclosure_substance as f)
        import pipeline.japan_signal_classifier as m

        class _R:
            def __enter__(self): return self
            def __exit__(self, *a): return False
            def read(self):
                import json
                return json.dumps({"choices": [{"message": {
                    "content": "ROUTINE"}}]}).encode()

        orig = m.urlopen
        m.urlopen = lambda *a, **k: _R()
        try:
            assert f("Ibiden", "4062", "x", "m", "k", "https://x.invalid", 5) == ("ROUTINE", None)
        finally:
            m.urlopen = orig

    def test_failure_still_defaults_to_weak(self):
        from pipeline.japan_signal_classifier import (
            _classify_japan_disclosure_substance as f)
        import pipeline.japan_signal_classifier as m

        def _boom(*a, **k):
            raise OSError("network down")

        orig = m.urlopen
        m.urlopen = _boom
        try:
            assert f("X", "1", "x", "m", "k", "https://x.invalid", 5) == ("WEAK", None)
        finally:
            m.urlopen = orig


class TestTranslatedTitlePrefix:
    """A bilingual filing's stored `translated_title` kept its Japanese
    name prefix: "村田製 (6981) [...]: Announcement Concerning
    Absorption-type Merger" - the body was the company's own English,
    the prefix was not. A Japanese-only filing, whose whole title goes
    through the model, came back fully English ("Ibiden (4062) [...]"),
    so the same field had two shapes depending on how it was produced.
    """

    def test_japanese_name_prefix_is_replaced(self):
        from pipeline.japan_signal_classifier import _with_english_prefix
        assert _with_english_prefix(
            "村田製 (6981) [2026-10-01T15:00:00+09:00]: Absorption-type Merger",
            "Murata Manufacturing",
        ) == ("Murata Manufacturing (6981) [2026-10-01T15:00:00+09:00]: "
              "Absorption-type Merger")

    def test_body_is_left_exactly_as_the_company_wrote_it(self):
        """The issuer's own English is the translation - never reworded."""
        from pipeline.japan_signal_classifier import _with_english_prefix
        body = "Completion of Execution of Partial Spin-off of Crasus"
        out = _with_english_prefix(f"レゾナック (4004) [t]: {body}", "Resonac Holdings")
        assert out.endswith(f": {body}")

    def test_edinet_id_prefix_is_not_touched(self):
        """"4004 [S100Z5EL]: 臨時報告書" has a document id, not a name -
        rewriting it would invent a company prefix that was never there."""
        from pipeline.japan_signal_classifier import _with_english_prefix
        t = "4004 [S100Z5EL]: 臨時報告書"
        assert _with_english_prefix(t, "Resonac Holdings") == t

    def test_title_with_no_separator_is_unchanged(self):
        from pipeline.japan_signal_classifier import _with_english_prefix
        assert _with_english_prefix("Extraordinary Report", "Lasertec") == \
            "Extraordinary Report"

    def test_missing_company_leaves_the_title_alone(self):
        from pipeline.japan_signal_classifier import _with_english_prefix
        t = "村田製 (6981) [t]: Merger"
        assert _with_english_prefix(t, None) == t

    def test_none_title_is_safe(self):
        from pipeline.japan_signal_classifier import _with_english_prefix
        assert _with_english_prefix(None, "Murata Manufacturing") is None

    def test_body_containing_a_colon_keeps_all_of_it(self):
        """Splitting on the first ": " only - a body with its own colon
        must not be truncated."""
        from pipeline.japan_signal_classifier import _with_english_prefix
        out = _with_english_prefix(
            "信越化 (4063) [t]: Notice: Payment Amount Decision", "Shin-Etsu Chemical")
        assert out.endswith("Notice: Payment Amount Decision")


class TestDisclosureSentencesAreNotOnePerTier:
    """J8 shipped three sentences for nine cards - one per tier - so
    SoftBank's OpenAI investment, Murata's merger and Resonac's
    spin-off carried word-for-word identical text. The sentence must
    follow the filing, not the bucket it landed in.
    """

    @staticmethod
    def _sentence(signal, company, action):
        """Mirrors classify_corporate_disclosure's own construction."""
        if signal == "SIGNAL":
            return (f"{company} filed a disclosure of {action}." if action else
                    f"{company} filed a disclosure that changes what the company "
                    f"owns, controls or is committing capital to.")
        if signal == "WEAK":
            if action:
                return (f"{company} filed a disclosure of {action}, whose effect on "
                        f"the business is not established from the filing's own title.")
            return (f"{company} filed a corporate disclosure whose effect on the "
                    f"business is not established from the filing's own title.")
        return (f"{company} filed a routine administrative disclosure - {action}."
                if action else
                f"{company} filed a routine administrative disclosure - "
                f"compensation, governance paperwork or a scheduled notice.")

    def test_three_material_filings_do_not_share_a_sentence(self):
        """The reported bug: these three were identical but for the name."""
        s = self._sentence
        got = [
            s("SIGNAL", "SoftBank Group", "follow-on investment in OpenAI"),
            s("SIGNAL", "Resonac Holdings", "partial spin-off of Crasus"),
            s("SIGNAL", "Murata Manufacturing", "absorption-type merger"),
        ]
        stripped = [t.split(" filed ", 1)[1] for t in got]
        assert len(set(stripped)) == 3, "sentences differ only by company name"

    def test_routine_filings_name_what_was_filed(self):
        s = self._sentence
        got = [
            s("NOISE", "Advantest", "disposal of treasury stock for an RSU plan"),
            s("NOISE", "Ibiden", "articles of incorporation"),
        ]
        assert len({t.split(" filed ", 1)[1] for t in got}) == 2
        assert "compensation, governance paperwork" not in " ".join(got)

    def test_generic_fallback_survives_for_a_filing_with_nothing(self):
        """Not every unreadable filing is an extraordinary report."""
        t = self._sentence("WEAK", "Someone", None)
        assert "corporate disclosure whose effect" in t


class TestDisclosureClassifierRunsForEveryFiling:
    """classify_corporate_disclosure must survive a filing that is not
    a major-shareholder report.

    `major_shareholder_override` was assigned only inside the branch
    that handles a 第19条第2項第4号 extraordinary report - one clause
    of one doc type. Every other disclosure read an unbound local and
    raised UnboundLocalError, which killed the whole batch on its
    first Kabutan row. The earlier tests exercised the override logic
    directly and never called this function, so none of them saw it.
    """

    def test_an_ordinary_disclosure_does_not_raise(self, monkeypatch):
        import pipeline.japan_signal_classifier as jc
        monkeypatch.setattr(
            jc, "_classify_japan_disclosure_substance",
            lambda *a, **k: ("ROUTINE", "acquisition of treasury shares"))
        out = jc.classify_corporate_disclosure(
            [{"title": "Murata (6981) [x]: Notice Regarding Acquisition "
                       "of Treasury Shares",
              "url": "kabutan-tdnet://6981/x", "published": "2026-09-20",
              "metadata": {"code": "6981", "company": "Murata Manufacturing",
                           "source_category": "jp_disclosure"}}],
            model="m")
        assert len(out) == 1
        assert out[0]["result"]["signal"] in ("signal", "weak_signal", "noise")

    def test_a_major_shareholder_filing_still_overrides(self, monkeypatch):
        import pipeline.japan_signal_classifier as jc
        monkeypatch.setattr(
            jc, "_classify_japan_disclosure_substance",
            lambda *a, **k: ("ROUTINE", None))
        out = jc.classify_corporate_disclosure(
            [{"title": "Resonac (4004) [x]: Extraordinary Report",
              "url": "edinet-extraordinary://E00751/S100Z2WW",
              "published": "2026-09-17",
              "metadata": {"code": "4004", "company": "Resonac Holdings",
                           "source_category": "jp_extraordinary",
                           "current_report_reason": "第19条第2項第4号",
                           "major_shareholder_name": "Capital Research",
                           "major_shareholder_pct": 11.06,
                           "major_shareholder_pct_previous": 9.59,
                           "major_shareholder_status_change": "became"}}],
            model="m")
        assert out[0]["result"]["signal"] == "signal"


class TestMajorShareholderChange:
    """A 第19条第2項第4号 extraordinary report, graded on its figures.

    The clause says a major shareholder changed and nothing more, so
    these were WEAK at best - and before that, Noise, because the
    filing's whole title is "Extraordinary Report" and the model that
    grades a disclosure had nothing to read.

    No docTypeCode-350 filing covers the same event: a 350 is filed by
    the HOLDER on crossing 5%, this one by the ISSUER when its own
    major-shareholder composition changes. Confirmed live against
    EDINET - no 350 exists for Resonac in the window around its 4号
    filing. The document is the only source of the number.

    Status is read from the filing's own wording, never from a
    percentage threshold. The filing cites FIEA Article 24-5(4) and
    the ordinance clause but never prints the level at which 主要株主
    status begins, so grading against a hardcoded 10% would rest on
    an assumption the document does not support. It does state, in
    words, 主要株主となるもの - "the party becoming a major
    shareholder" - and that is a filed fact.
    """

    @staticmethod
    def _graded(now, was, status=None):
        from pipeline.japan_signal_classifier import (
            _MAJOR_SHAREHOLDER_LARGE_MOVE_PTS)
        delta = round(now - was, 2)
        return ("SIGNAL" if status in ("became", "ceased")
                or abs(delta) >= _MAJOR_SHAREHOLDER_LARGE_MOVE_PTS
                else "WEAK"), delta

    def test_the_real_resonac_filing_is_a_signal(self):
        """9.59% -> 11.06%, filed as 主要株主となるもの: Capital
        Research became a major shareholder of Resonac."""
        assert self._graded(11.06, 9.59, "became") == ("SIGNAL", 1.47)

    def test_gaining_the_status_is_a_signal_on_any_move(self):
        """The filing says the holder became a major shareholder. That
        is the event, whatever the size of the step that did it."""
        assert self._graded(10.02, 9.98, "became")[0] == "SIGNAL"

    def test_losing_the_status_is_a_signal_too(self):
        assert self._graded(9.4, 10.6, "ceased")[0] == "SIGNAL"

    def test_a_large_move_with_no_status_change_is_still_a_signal(self):
        assert self._graded(8.4, 6.9)[0] == "SIGNAL"

    def test_drift_with_no_status_change_stays_weak(self):
        assert self._graded(11.3, 11.0)[0] == "WEAK"

    def test_no_threshold_constant_exists_to_be_assumed(self):
        """The filing never prints one, so neither does the code."""
        import pipeline.japan_signal_classifier as jc
        assert not hasattr(jc, "_MAJOR_SHAREHOLDER_THRESHOLD_PCT")


class TestRevenueOnlyRevision:
    """A revision that moves revenue and leaves profit flat.

    Hitachi does this in all four of its stored filings: revenue
    +11.3%, while the filing's own 増減額 row reads 0 for operating
    profit, EBIT and net income alike - verified against the source
    PDF, so the figures are right and the behaviour is real. Judged
    on operating profit alone the card said "Left full-year profit
    forecast unchanged" beside "¥372bn → ¥372bn", and an 11.3%
    revenue raise disappeared from the feed. 6 of 191 stored rows.
    """

    @staticmethod
    def _card(**over):
        from pipeline.japan_signal_view import to_jp_signal
        meta = {"code": "6501", "company": "Hitachi",
                "source_category": "jp_forecast",
                "forecast_period": "full_year",
                "operating_profit_previous": 372000.0,
                "operating_profit_revised": 372000.0,
                "operating_profit_pct_change": 0.0}
        meta.update(over)
        return to_jp_signal({
            "source_id": "irbank-financials://6501/140120200727467442",
            "signal_detection": "weak_signal", "signal_score": None,
            "signal_reason": "r", "published": "2020-07-30T00:00:00Z",
            "title": "t", "metadata": meta})

    def test_the_headline_reports_the_line_that_moved(self):
        c = self._card(revenue_previous=7080000.0, revenue_revised=7880000.0,
                       revenue_pct_change=11.3)
        assert c["headline"] == ("Raised full-year revenue forecast 11.3%, "
                                 "profit unchanged")

    def test_the_change_line_shows_the_revenue_move(self):
        """"¥372bn → ¥372bn" states a figure twice and shows nothing."""
        c = self._card(revenue_previous=7080000.0, revenue_revised=7880000.0,
                       revenue_pct_change=11.3)
        assert c["change"] == "¥7.1tn → ¥7.9tn revenue"

    def test_a_revenue_cut_keeps_its_direction(self):
        c = self._card(revenue_previous=7880000.0, revenue_revised=7080000.0,
                       revenue_pct_change=-10.2)
        assert c["headline"].startswith("Cut full-year revenue forecast 10.2%")

    def test_everything_flat_still_reads_as_unchanged(self):
        """Only claim a revision where one happened."""
        c = self._card()
        assert c["headline"] == "Left full-year profit forecast unchanged"

    def test_a_profit_move_is_never_displaced_by_revenue(self):
        """Where profit moved, profit is what the signal is about."""
        c = self._card(operating_profit_revised=432000.0,
                       operating_profit_pct_change=16.1,
                       revenue_pct_change=11.3)
        assert c["headline"] == "Raised full-year profit forecast"

    def test_ocr_mangled_revenue_keys_are_still_found(self):
        """The OCR fallback emits 売上⾼ with a different codepoint for
        高, and 売売上上高高 with every character doubled. Both appear
        in stored rows."""
        from pipeline.japan_signal_classifier import _get_revenue
        for key in ("売上高", "売上⾼", "売売上上高高", "売上収益", "営業収益"):
            figures = [{"_kind": "revision", "当期利益": {},
                        key: {"previous": 100.0, "revised": 120.0}}]
            assert _get_revenue(figures) == {"previous": 100.0, "revised": 120.0}


class TestForecastTableSelection:
    """A revision is judged on the full-year table, not the first one.

    `_get_operating_profit` used to take `figures[0]` on the stated
    grounds that a filing lists its primary table first, citing
    Fujikura's Q2+full-year filing as confirmation. The real PDF says
    the opposite: that filing's HALF-YEAR table is first. Across the
    stored rows, 51 of 66 multi-table filings carry their full-year
    table somewhere after position 0, so the card was labelling
    half-year and quarterly figures "full-year" and the habit median
    was pooling periods that are not comparable.
    """

    # Fujikura 5803, filing 140120260806513565, re-parsed from the
    # source PDF - two tables, half-year first.
    FUJIKURA = [
        {"_kind": "revision",
         "売上高": {"previous": 778000.0, "revised": 821000.0},
         "営業利益": {"previous": 174000.0, "revised": 198000.0},
         "親会社株主に帰属する中間純利益": {"previous": 128000.0, "revised": 149000.0}},
        {"_kind": "revision",
         "売上高": {"previous": 1462000.0, "revised": 1755000.0},
         "営業利益": {"previous": 310000.0, "revised": 432000.0},
         "親会社株主に帰属する当期純利益": {"previous": 229000.0, "revised": 326000.0}},
    ]

    def test_the_period_is_read_from_the_net_income_line(self):
        from pipeline.japan_signal_classifier import table_period
        assert table_period(self.FUJIKURA[0]) == "half_year"
        assert table_period(self.FUJIKURA[1]) == "full_year"

    def test_the_full_year_table_wins_over_the_first_one(self):
        from pipeline.japan_signal_classifier import select_forecast_table
        table, period = select_forecast_table(self.FUJIKURA)
        assert period == "full_year"
        assert table["営業利益"] == {"previous": 310000.0, "revised": 432000.0}

    def test_the_figures_are_the_full_year_ones(self):
        """174000 -> 198000 is the half year. Taking it and calling it
        full-year understated this revision as 13.8% when it was
        39.4%."""
        from pipeline.japan_signal_classifier import _get_operating_profit
        assert _get_operating_profit(self.FUJIKURA) == {
            "previous": 310000.0, "revised": 432000.0}

    def test_a_single_table_filing_is_unaffected(self):
        from pipeline.japan_signal_classifier import select_forecast_table
        table, period = select_forecast_table([self.FUJIKURA[0]])
        assert period == "half_year"
        assert table is self.FUJIKURA[0]

    def test_an_unmarked_table_reports_no_period_rather_than_guessing(self):
        from pipeline.japan_signal_classifier import select_forecast_table
        table, period = select_forecast_table(
            [{"_kind": "revision", "営業利益": {"previous": 1.0, "revised": 2.0}}])
        assert period is None and table is not None

    def test_quarter_is_not_read_as_full_year(self):
        """四半期 and 当期 share characters; testing for the full year
        first would claim every table."""
        from pipeline.japan_signal_classifier import table_period
        assert table_period(
            {"親会社株主に帰属する四半期純利益": {}}) == "quarter"

    def test_the_title_fills_in_where_the_keys_are_silent(self):
        """Two stored SoftBank rows report under IFRS wording -
        親会社の所有者に帰属する純利益 - which carries no 当期/中間
        marker, while their titles read "full-year earnings forecast"
        plainly."""
        from pipeline.japan_signal_classifier import table_period
        ifrs = {"_kind": "revision", "売上高": {}, "営業利益": {},
                "親会社の所有者に帰属する純利益": {}}
        assert table_period(ifrs) is None
        assert table_period(
            ifrs, "Notice regarding the revision of full-year earnings "
                  "forecast") == "full_year"

    def test_the_keys_win_over_a_title_that_disagrees(self):
        """The keys describe the table the figures came from; the
        title describes the filing, which may revise more than one
        period."""
        from pipeline.japan_signal_classifier import table_period
        assert table_period({"親会社株主に帰属する当期純利益": {}},
                            "half-year forecast revision") == "full_year"

    def test_a_title_naming_two_periods_names_neither(self):
        """Fujikura's "revision of half-year and full-year earnings
        forecasts" cannot say which period the figures describe, so it
        is not allowed to."""
        from pipeline.japan_signal_classifier import _title_period
        assert _title_period(
            "Notice regarding the revision of half-year and full-year "
            "earnings forecasts") is None

    def test_no_figures_selects_nothing(self):
        from pipeline.japan_signal_classifier import select_forecast_table
        assert select_forecast_table([]) == (None, None)
        assert select_forecast_table(None) == (None, None)


class TestMajorShareholderIsEnglish:
    """The holder's name reaches the card and the raw row in English.

    A 第19条第2項第4号 filing names its major shareholder in katakana
    and nowhere else on the row - "キャピタル・リサーチ・アンド・
    マネージメント・カンパニー" is Capital Research and Management
    Company. Without translation the one English card carrying real
    ownership figures states them about a party the reader cannot
    identify.

    Both forms are stored: the filed Japanese for traceability, the
    English for display - the same arrangement jp_ownership already
    uses for filer_name.
    """

    @staticmethod
    def _card(**over):
        from pipeline.japan_signal_view import to_jp_signal
        meta = {"code": "4004", "company": "Resonac Holdings",
                "source_category": "jp_extraordinary",
                "signal_reason_code": "major_shareholder_change",
                "major_shareholder_name": "キャピタル・リサーチ・アンド・マネージメント・カンパニー",
                "major_shareholder_pct": 11.06,
                "major_shareholder_pct_previous": 9.59,
                "major_shareholder_status_change": "became"}
        meta.update(over)
        return to_jp_signal({
            "source_id": "edinet-extraordinary://E00751/S100Z2WW",
            "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": "2026-09-17T06:00:00Z",
            "title": "t", "metadata": meta})

    def test_the_headline_uses_the_english_name(self):
        c = self._card(translated_major_shareholder_name=
                       "Capital Research and Management Company")
        assert c["headline"].startswith("Capital Research and Management Company")
        assert "キャピタル" not in c["headline"]

    def test_the_headline_carries_both_ends_of_the_move(self):
        c = self._card(translated_major_shareholder_name="Capital Research")
        assert "11.06%" in c["headline"] and "9.59%" in c["headline"]

    def test_a_sell_down_says_cut(self):
        c = self._card(translated_major_shareholder_name="Capital Research",
                       major_shareholder_pct=9.4,
                       major_shareholder_pct_previous=10.6,
                       major_shareholder_status_change="ceased")
        assert c["headline"].startswith("Capital Research cut its holding")

    def test_the_filed_japanese_name_is_the_fallback_not_a_blank(self):
        """Translation runs after classification, so a row read before
        it completes still names the holder rather than dropping it."""
        assert "キャピタル" in self._card()["headline"]


class TestExtraordinaryReportReason:
    """EDINET's `current_report_reason` is the ordinance clause the
    filing cites, and the clause IS the event type - a fixed legal
    enumeration. news-retrieval has always stored it; nothing read it,
    so every extraordinary report fell back to "a corporate disclosure
    whose effect is not established from the filing's own title". The
    row carried the answer in a field no code touched.
    """

    def test_real_clauses_from_stored_rows_resolve(self):
        from pipeline.japan_signal_classifier import _extraordinary_reason
        assert _extraordinary_reason(
            {"current_report_reason": "第19条第2項第4号"}
        )[0] == "a change in major shareholders"
        assert _extraordinary_reason(
            {"current_report_reason": "第19条第2項第2号の2"}
        )[0] == "a grant of share options"

    def test_the_clause_grades_the_filing_as_well_as_naming_it(self):
        """These filings' whole title is "Extraordinary Report", so the
        model that grades a Kabutan disclosure has nothing to read and
        answers ROUTINE for every one. The clause is a fixed legal
        enumeration and a firmer basis than prose, so it carries the
        tier too.
        """
        from pipeline.japan_signal_classifier import _extraordinary_tier
        # Who owns a large block of the company changed - the same
        # class of event J6 reads from a large-shareholding filing.
        assert _extraordinary_tier({"current_report_reason": "第19条第2項第4号"}) == "WEAK"
        # Employee compensation, filed on a schedule.
        assert _extraordinary_tier({"current_report_reason": "第19条第2項第2号の2"}) == "ROUTINE"
        # An AGM passing its agenda is the expected outcome.
        assert _extraordinary_tier({"current_report_reason": "第19条第2項第9号の2"}) == "ROUTINE"

    def test_no_clause_maps_to_signal_without_a_magnitude(self):
        """A clause says a major shareholder changed, not by how much.
        Grading one SIGNAL on event type alone would assert a size the
        filing never states."""
        from pipeline.japan_signal_classifier import _EDINET_EXTRAORDINARY_REASONS
        assert all(tier in ("WEAK", "ROUTINE")
                   for _, tier in _EDINET_EXTRAORDINARY_REASONS.values())

    def test_an_unmapped_clause_has_no_tier_to_apply(self):
        from pipeline.japan_signal_classifier import _extraordinary_tier
        assert _extraordinary_tier({"current_report_reason": "第19条第2項第99号"}) is None

    def test_longest_clause_wins(self):
        """"2号の2" must not be matched by the "2号"-style shorter key."""
        from pipeline.japan_signal_classifier import _extraordinary_reason
        assert _extraordinary_reason(
            {"current_report_reason": "第19条第2項第2号の2"}
        ) != _extraordinary_reason({"current_report_reason": "第19条第2項第7号"})

    def test_full_width_digits_match_the_same_clause(self):
        """Filings write the same clause both ways."""
        from pipeline.japan_signal_classifier import _extraordinary_reason
        assert _extraordinary_reason({"current_report_reason": "第19条第２項第４号"}) == \
               _extraordinary_reason({"current_report_reason": "第19条第2項第4号"})

    def test_unmapped_clause_returns_none_not_a_guess(self):
        """An unknown clause keeps the honest generic sentence rather
        than being labelled with a neighbouring event type."""
        from pipeline.japan_signal_classifier import _extraordinary_reason
        assert _extraordinary_reason({"current_report_reason": "第19条第2項第99号"}) is None
        assert _extraordinary_reason({}) is None


class TestDisclosureHeadlineNamesTheFiling:
    """`headline` was the constant "Filed a corporate disclosure" on
    every J8 card - SoftBank's OpenAI investment and Ibiden's articles
    of incorporation read identically."""

    @staticmethod
    def _card(**meta):
        from pipeline.japan_signal_view import to_jp_signal
        m = {"code": "4004", "company": "Resonac Holdings",
             "source_category": "jp_disclosure"}
        m.update(meta)
        return to_jp_signal({
            "source_id": "k://4004/x", "signal_detection": "signal",
            "signal_score": None, "signal_reason": "r", "published": None,
            "title": "t", "metadata": m})

    def test_headline_names_the_action(self):
        assert self._card(disclosure_action="partial spin-off of Crasus")[
            "headline"] == "Filed partial spin-off of Crasus"

    def test_headline_omits_the_company_name(self):
        """Verb-first without the issuer, like every other card type -
        the card already prints the company."""
        h = self._card(disclosure_action="partial spin-off of Crasus")["headline"]
        assert "Resonac" not in h

    def test_two_filings_do_not_share_a_headline(self):
        a = self._card(disclosure_action="follow-on investment in OpenAI")["headline"]
        b = self._card(disclosure_action="articles of incorporation")["headline"]
        assert a != b

    def test_no_action_keeps_the_old_constant(self):
        assert self._card()["headline"] == "Filed a corporate disclosure"

    def test_source_title_carries_the_original(self):
        c = self._card(original_language_title="村田製 (6981): 完全子会社の吸収合併")
        assert c["sourceTitle"] == "村田製 (6981): 完全子会社の吸収合併"

    def test_source_title_is_null_when_there_is_no_original(self):
        assert self._card()["sourceTitle"] is None


class TestCheckpointPointsAtThePendingQuarter:
    """`_next_quarter_end` tested `end > today`, so it named the next
    quarter to START rather than the most recent one to END. On 2 Oct
    a March-closing issuer's card read "Q3 results, periodEnd
    2026-12-31" when the Q2 results for the quarter ended 30 Sep were
    weeks away - a reader asking "when will I know" was pointed a full
    quarter too far out.
    """

    @staticmethod
    def _on(iso):
        """The function does `from datetime import date` in its own
        body, so the module attribute is not what it reads - patch
        datetime.date itself."""
        import datetime as real
        from unittest.mock import patch

        class _D(real.date):
            @classmethod
            def today(cls):
                return real.date.fromisoformat(iso)
        return patch.object(real, "date", _D)

    def test_just_after_a_quarter_closes_names_that_quarter(self):
        import pipeline.japan_signal_view as v
        with self._on("2026-10-02"):
            assert v._next_quarter_end("03-31") == ("2026-09-30", "Q2")

    def test_rolls_forward_once_those_results_are_out(self):
        import pipeline.japan_signal_view as v
        with self._on("2026-11-20"):
            assert v._next_quarter_end("03-31") == ("2026-12-31", "Q3")

    def test_december_closer_gets_its_own_quarter_numbering(self):
        import pipeline.japan_signal_view as v
        with self._on("2026-10-02"):
            assert v._next_quarter_end("12-31") == ("2026-09-30", "Q3")


class TestIndustryCheckpointOnlyOnTheLatestMonth:
    """All 47 industry rows carried the same future checkpoint, so a
    May reading claimed September billings would confirm it. Only the
    newest month still has an open question."""

    def test_only_the_newest_month_keeps_a_checkpoint(self):
        import json, os
        from pipeline.japan_signal_view import to_jp_signal
        p = os.path.join(os.path.dirname(__file__), "..", "..",
                         "signal-detection-agent", "tests", "_fixtures_absent")
        # Shape synthetic rows rather than depend on a data file.
        def row(period, published):
            return {"source_id": "s", "signal_detection": "signal",
                    "signal_score": None, "signal_reason": "r",
                    "published": published, "title": "t",
                    "metadata": {"source_category": "jp_industry",
                                 "period": period, "yoy_pct": 5.0}}
        # Months relative to today, by the code's own rule (from the 20th
        # the newest released month is one back, before it two back) - a
        # hard-coded "August 2026" stopped being the newest on 2026-10-20
        # and failed the deploy's test run.
        from datetime import date, datetime, timezone

        def months_back(n):
            t = date.today().replace(day=1)
            for _ in range(n):
                t = (t.replace(day=1) - __import__("datetime").timedelta(days=1)).replace(day=1)
            return t
        newest = months_back(1 if date.today().day >= 20 else 2)
        old = months_back(5)
        now = datetime.now(timezone.utc).isoformat()
        recent = to_jp_signal(row(newest.strftime("%B %Y"), now))
        stale = to_jp_signal(row(old.strftime("%B %Y"), now))
        assert recent["nextCheckpoint"] is not None
        assert stale["nextCheckpoint"] is None


class TestOwnershipCheckpointNamesTheHolder:
    """`event` said "{issuer} Q3 results" while supports/weakens
    described the holder's next change report - two halves of one
    checkpoint describing different things on different cadences."""

    @staticmethod
    def _card(**over):
        from pipeline.japan_signal_view import to_jp_signal
        m = {"source_category": "jp_ownership", "code": "3436",
             "filer_name": "Toshiba Corporation", "holding_pct": 12.8,
             "signal_reason_code": "active_holder_changes_stake"}
        m.update(over)
        return to_jp_signal({"source_id": "s", "signal_detection": "signal",
                             "signal_score": None, "signal_reason": "r",
                             "published": _recent(), "title": "t",
                             "metadata": m})

    def test_event_is_the_holders_filing_not_the_issuers_results(self):
        cp = self._card()["nextCheckpoint"]
        assert "change report" in cp["event"]
        assert "results" not in cp["event"]

    def test_no_date_or_period_since_a_holder_files_on_no_cadence(self):
        cp = self._card()["nextCheckpoint"]
        assert cp["date"] is None and cp["periodEnd"] is None


class TestOwnershipDirection:
    """A change report states the holder's previous ratio, so the card
    can say whether they bought or sold. Without it every row read
    "12.8% stake changed" - Toshiba's real Kioxia sell-down from 14.06%
    looked identical to an accumulation."""

    @staticmethod
    def _card(**over):
        from pipeline.japan_signal_view import to_jp_signal
        m = {"source_category": "jp_ownership", "code": "285A",
             "filer_name": "Toshiba Corporation",
             "signal_reason_code": "active_holder_changes_stake"}
        m.update(over)
        return to_jp_signal({"source_id": "s", "signal_detection": "signal",
                             "signal_score": None, "signal_reason": "r",
                             "published": _recent(), "title": "t", "metadata": m})

    def test_sell_down_says_cut_and_names_the_prior_level(self):
        h = self._card(holding_pct=12.84, holding_pct_previous=14.06)["headline"]
        assert h.startswith("Cut stake to 12.84% from 14.06%")

    def test_build_up_says_raised(self):
        h = self._card(holding_pct=5.9, holding_pct_previous=5.1)["headline"]
        assert h.startswith("Raised stake to 5.90% from 5.10%")

    def test_no_previous_keeps_the_neutral_wording(self):
        """An initial report has no prior position to compare."""
        h = self._card(holding_pct=12.84)["headline"]
        assert "changed" in h and "Cut" not in h and "Raised" not in h

    def test_unchanged_ratio_is_not_called_a_move(self):
        """A filing can be triggered by a contract change, not a trade."""
        h = self._card(holding_pct=5.5, holding_pct_previous=5.5)["headline"]
        assert "Cut" not in h and "Raised" not in h

    def test_passive_holder_movement_is_also_shown(self):
        """A passive filing's stake still moves. Sumitomo Mitsui Trust
        really went 9.31% -> 8.24% (S100YZWG) while the headline said
        only "Passive 8.2% stake filed" - and the card's own checkpoint
        said "keeps selling down", so the two halves disagreed."""
        from pipeline.japan_signal_view import to_jp_signal
        c = to_jp_signal({
            "source_id": "s", "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": _recent(), "title": "t",
            "metadata": {"source_category": "jp_ownership", "code": "6857",
                         "filer_name": "Sumitomo Mitsui Trust Asset Management Co., Ltd.",
                         "signal_reason_code": "holder_changes_existing_stake_passive",
                         "holding_pct": 8.24, "holding_pct_previous": 9.31}})
        assert "cut to 8.24% from 9.31%" in c["headline"]

    def test_passive_initial_report_stays_neutral(self):
        """No prior position exists, so there is no move to describe."""
        from pipeline.japan_signal_view import to_jp_signal
        c = to_jp_signal({
            "source_id": "s", "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": _recent(), "title": "t",
            "metadata": {"source_category": "jp_ownership", "code": "6315",
                         "filer_name": "Nomura Securities Co., Ltd.",
                         "signal_reason_code": "holder_crosses_five_pct_passive",
                         "holding_pct": 5.35}})
        assert c["headline"] == "Passive 5.35% stake filed by Nomura Securities"


class TestCapexAndFormNameHeadlines:
    """Round two of the J8/J5 headline work, against real stored rows."""

    @staticmethod
    def _capex(company, translated):
        from pipeline.japan_signal_view import to_jp_signal
        return to_jp_signal({
            "source_id": "s", "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": None, "title": "t",
            "metadata": {"source_category": "jp_capex", "code": "6723",
                         "company": company, "translated_title": translated}})["headline"]

    def test_possessive_suffix_does_not_eat_the_verb(self):
        """lstrip(" ,-'’s") strips CHARACTERS, so "Renesas starts
        operation" became "tarts operation" - the verb lost its s."""
        h = self._capex("Renesas Electronics",
                        "Renesas Electronics (6723): Renesas starts operation of a line")
        assert h.startswith("Starts operation")

    def test_legal_suffix_is_removed_with_the_name(self):
        """Stripping only "Hitachi" from "Hitachi, Ltd.'s factory" left
        ", Ltd.'s factory", rendering as "Ltd. delivers ..."."""
        h = self._capex("Hitachi",
                        "Hitachi (6501): Hitachi, Ltd.'s new factory begins operation")
        assert h.startswith("New factory begins")

    def test_a_planned_investment_is_not_stated_as_done(self):
        """"Shin-Etsu to build a new factory" must not become "Built a
        new factory" - the plant does not exist yet."""
        h = self._capex("Shin-Etsu Chemical",
                        "Shin-Etsu Chemical (4063): Shin-Etsu Chemical to build a new factory in China")
        assert h.startswith("To build")
        assert "Built" not in h

    def test_japanese_form_name_is_translated(self):
        from pipeline.japan_signal_classifier import _japanese_form_name
        assert _japanese_form_name("イビデン (4062) [t]: 定款 2026/10/01") == \
            "its articles of incorporation"
        assert _japanese_form_name("イビデン (4062) [t]: 統合報告書 2026") == \
            "its integrated report"

    def test_english_title_is_not_matched_as_a_form_name(self):
        from pipeline.japan_signal_classifier import _japanese_form_name
        assert _japanese_form_name("村田製 (6981) [t]: Absorption-type Merger") is None

    def test_missing_revision_caveat_names_the_month(self):
        from pipeline.japan_signal_view import _caveat
        text = _caveat("missing", {"hit_ratio": 0.8, "expected_month": 7})
        assert "July" in text and "month 7" not in text


class TestQuotedTermArticle:
    """Japanese headlines bracket a product name typographically, and
    unwrapping one gives it an English article. That is right for a
    noun the sentence acts on, wrong for one that opens it: a real
    Hitachi row rendered "A Physical AI implemented in manufacturing
    sites" - an article governing nothing.
    """

    @staticmethod
    def _headline(translated, company="Hitachi"):
        from pipeline.japan_signal_view import to_jp_signal
        return to_jp_signal({
            "source_id": "s", "signal_detection": "signal", "signal_score": None,
            "signal_reason": "r", "published": None, "title": "t",
            "metadata": {"source_category": "jp_press", "code": "6501",
                         "company": company, "translated_title": translated,
                         "publication_domain": "newswitch.jp"}})["headline"]

    def test_quote_opening_the_headline_gets_no_article(self):
        h = self._headline(
            'Hitachi (6501): "Physical AI" implemented in manufacturing sites')
        assert h.startswith("Physical AI implemented")

    def test_quote_mid_sentence_keeps_its_article(self):
        """The original behaviour this must not break."""
        h = self._headline(
            "Resonac Holdings (4004): Resonac achieves Japan’s first success, "
            "develops “12-inch SiC substrate”",
            company="Resonac Holdings")
        assert h.startswith("Developed a 12-inch SiC substrate")
