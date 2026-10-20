"""Deterministic + habit-aware classification for japan_market_signal
items (news-retrieval's japan_market_signal domain: IRBANK forecast
revisions, buybacks, Kabutan/EDINET filings, SEAJ billings, press).

Follows the same design as korea_signal_classifier.py/
taiwan_signal_classifier.py: news-retrieval stays fetch/dedup-only - no
source_type computes a signal or translates text there. Arithmetic/lookup
rules live here; a real LLM call is reserved for the press layer (J7)
only, per the Japan Signals spec's own explicit design intent ("Five of
the seven signal types are arithmetic or a lookup. A model is used only
to judge whether a news headline states a fact or an opinion").

J1 (forecast revision) status here: IMPLEMENTED - classify_forecast_
revision(), the module's first signal type built. See that function's own
docstring for the full habit-computation design and the real data-quality
findings (from a live 20-company/5-year backfill against news-retrieval's
test DB, 2026-09-28) that shaped it:

  - Not every irbank_financials row is a genuine forward-looking revision.
    ~21% of the real 154-row sample were pure actuals-vs-forecast VARIANCE
    notices (title contains 差異, no accompanying 修正) with no "previous"
    figure to compare against at all - these carry no habit-relevant
    signal and are excluded from both the habit computation AND from
    being classified as a revision themselves (they are Japan Signals
    spec Section 5's J2 "results against forecast" material instead, not
    yet built here).
  - The spec's own Section 7.1 worked example (Advantest "raises 2x/yr",
    illustrative ~8-12% typical size) does NOT match Advantest's real
    filed history over the confirmed live window: real data shows ~3
    revisions/year with a real median absolute change near 30% - the
    spec's numbers are illustrative scaffolding, not literally sourced
    from Advantest's actual filings, so this module's own habit baseline
    is computed from each company's REAL stored history, never from the
    spec's own worked numbers.
  - Renesas Electronics (6723) reports quarterly range-based guidance in
    Non-GAAP margin PERCENTAGES (revenue/gross-margin%/operating-margin%),
    with no absolute yen operating-profit figure in any real filing
    checked - structurally incompatible with every other company's
    absolute-yen full-year-revision model, not a label variant to map
    around. Deliberately excluded from classify_forecast_revision
    entirely (see _JAPAN_OPERATING_PROFIT_EXCLUDED_CODES) rather than
    silently misreading a margin percentage as a yen amount.
  - The operating-profit line-item key varies across real filers -
    confirmed live: "営業利益" (the common case) and "営業利益（△損失）"
    (Kioxia's own label, "Operating Profit (or Loss)") are the same
    concept under two different literal keys - both are checked, not just
    the first.

J2 (results against forecast), J3 (missing revision), J4 (industry
equipment sales), and J5+J6 (capacity commitment / ownership and capital
policy - one combined function, classify_capacity_and_ownership, since
the spec's own Section 6.5 is one shared rule block for both) status:
IMPLEMENTED - classify_results_against_forecast(),
classify_missing_revision(), classify_industry_equipment_sales(),
classify_capacity_and_ownership(). See each function's own docstring for
its design and real-data findings - classify_capacity_and_ownership's own
docstring documents a real finding worth calling out here too: the
spec's literal "investment >= 10% of total assets" clause never fires on
real data (largest real case checked: 9.2%), so the 14-day
same-company-co-occurrence clause is the one that actually promotes real
J5 items to Signal in practice - confirmed live, not assumed.

J7 (press) status: IMPLEMENTED - classify_press(). See that function's
own docstring for its design and real-data findings, most importantly:
news-retrieval's own jp_press source has exactly TWO real sub-sources
wired (Jiji Press, Newswitch - see news-retrieval's CLAUDE.md for why
Nikkei itself, Reuters Japan and Kyodo News English were all checked and
found unusable), both already first-tier by this module's own tiering
(no second-tier source exists in this pipeline at all, unlike Korea's
mixed trusted-publication list) - and confirmed live 2026-09-29 this is a
genuinely thin real feed (1 real Newswitch article, 0 real Jiji articles,
in the current test window), matched by the spec's own Section 8's
observation that a market like this produces "few but heavy" signals.
All seven (J1-J7) are wired into classify_japan_signal_batch(), the
single entry point controllers/run.py calls.
"""
from __future__ import annotations

import json
import logging
import re
import statistics
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from pipeline.japan_companies import (
    japan_ticker_universe,
    company_for,
    mentioned_customers,
)
from pipeline.taiwan_signal_classifier import _get_nested, _translate_one

logger = logging.getLogger(__name__)

# Japan Signals spec Section 5/6's own vocabulary is Signal/Weak/Noise,
# matching Korea's shape and target column (agent_classifications.
# signal_detection) - same _KOREA_SIGNAL_MAP-style table, kept as its own
# copy since a future Japan-specific signal type could plausibly need a
# 4th key this table would then need to NOT silently support.
_JAPAN_SIGNAL_MAP = {"SIGNAL": "signal", "WEAK": "weak_signal", "NOISE": "noise"}

# The line-item label(s) that mean "operating profit" - see module
# docstring for why more than one literal key exists across real filers.
# "調整後営業利益" (adjusted operating profit) is Hitachi's own real,
# official reporting metric - confirmed live its first table carries NO
# standard "営業利益" key at all, only this adjusted one (a genuine large
# industrial conglomerate reporting choice, not a labeling quirk) - a
# company whose first table uses only this key had zero genuine revisions
# ever computed before this was added (all filtered out by _get_
# operating_profit finding nothing, not by _is_genuine_revision itself).
_OPERATING_PROFIT_KEYS = ("営業利益", "営業利益（△損失）", "調整後営業利益")

# The revenue line, read only when operating profit did not move.
# A company can revise revenue and leave every profit line untouched -
# Hitachi does it in all four of its stored filings, raising revenue
# 11.3% while 増減額 reads 0 for operating profit, EBIT and net income
# alike. Judged on operating profit alone those filings report
# "unchanged", so a real revision disappears from the feed.
#
# 売上⾼ and 売売上上高高 are the same label as 売上高, arriving through
# the OCR fallback: the first uses a different codepoint for 高, the
# second has every character doubled. Both appear in stored rows, so
# both are matched rather than left to read as an absent line.
_REVENUE_KEYS = ("売上高", "売上⾼", "売売上上高高", "売上収益", "営業収益")

# Renesas Electronics reports Non-GAAP margin PERCENTAGES only, no
# absolute yen operating-profit figure in any real filing checked - see
# module docstring. Excluded from J1 entirely rather than silently
# misread.
_JAPAN_OPERATING_PROFIT_EXCLUDED_CODES = frozenset({"6723"})

# Japan Signals spec Section 6.1's own explicit numeric rules.
_REVERSAL_OR_LOSS_SWING = "SIGNAL"  # rules 2/3 below are unconditional
_RARE_REVISER_THRESHOLD_PER_YEAR = 1.0  # "fewer than one revision a year on average"
_TYPICAL_SIZE_MULTIPLE_THRESHOLD = 2.0  # "more than twice this company's typical revision size"
_ABSOLUTE_SIGNAL_THRESHOLD_PCT = 20.0  # "changes by twenty percent or more, whatever the habit"
# The 20%-floor rule below (rule 6) only fires for a trusted-habit company
# once its move ALSO clears a real bar above that company's own typical
# size - see rule 6's own comment for why a flat 20% floor alone is wrong
# once a company's typical size is itself large. Real, confirmed-live
# audit trail 2026-09-30 (see compute_forecast_habit's own comment on
# typical_size_mad_pct for the full statistical reasoning): the ORIGINAL
# version of this rule required only "> typical_size_pct" (bare median),
# which is a very weak bar - by definition roughly half of any company's
# own real revisions exceed their own median - so this rule was flagging
# routine, unremarkable activity as SIGNAL for the many real companies in
# this sector whose typical size is already 15-38% (Advantest 27.6%,
# Shinko Electric 37.8%, Ibiden 33.2%, Screen Holdings 33.3%, confirmed
# live from stored habits). It also had a real rounding-boundary bug: a
# company's own true median revision (e.g. Advantest's real median IS
# 27.62%, rounding to display as the same "27.6%" its own habit shows)
# could trip ">" against its own rounded self by pure rounding noise.
#
# A FIRST fix tried a flat multiple of the median (1.5x) instead - real
# testing found this was itself an arbitrary number, chosen by trying a
# few round values and picking one that reduced false-positive volume by
# a plausible amount, not derived from any real statistical property of
# the data. THE REAL FIX (see _ABSOLUTE_SIGNAL_MAD_MULTIPLE below): the
# bar is now median + N*MAD (median absolute deviation, this module's own
# established robust-outlier statistic - see typical_size_mad_pct's own
# docstring), which self-scales to each company's own real spread instead
# of a company-blind multiplier. _ABSOLUTE_SIGNAL_TYPICAL_SIZE_MULTIPLE
# below is kept ONLY as the fallback for the rare case a company's habit
# has typical_size_pct but no typical_size_mad_pct (fewer than 2 real
# pct_changes - see compute_forecast_habit's own docstring on why MAD
# needs at least 2 values to mean anything) - not the primary bar
# anymore.
_ABSOLUTE_SIGNAL_TYPICAL_SIZE_MULTIPLE = 1.5
# The real, principled bar: median + this many MADs. Tested live against
# the full real 991-article/158-genuine-revision dataset using the actual
# production compute_forecast_habit/_is_genuine_revision functions (never
# reimplemented): 1*MAD (36 fires), 2*MAD (25 fires, EACH one confirmed
# genuinely far outside that company's own normal spread - e.g. Advantest
# only fires on its real 50-53% moves, never its routine ~27% median),
# 3*MAD (12 fires, arguably too strict - real large moves risk being
# missed). 2 is the standard "moderately unusual" convention for a
# MAD-based bar (comparable in spirit to roughly 2 robust-standard-
# deviations under a normal approximation) - not tuned to hit a target
# count the way the old flat multiplier was; the real result (25 fires
# with correct examples, not the count itself) is what confirmed it's
# the right choice, not the reverse.
_ABSOLUTE_SIGNAL_MAD_MULTIPLE = 2.0

# A company's habit is only trusted once it has this many genuine
# (non-variance) revisions in its stored history - below this, rules that
# depend on "typical size"/"revisions per year" cannot mean anything yet
# (the same "not enough real data yet" principle Korea's own
# classify_export_surprise uses for its 12-period minimum - see that
# function's own docstring). Below the floor, only the two habit-
# independent rules (reversal, profit/loss swing) and the flat >=20%
# absolute rule can fire; everything else falls through to WEAK rather
# than a fabricated habit-based judgment.
_MIN_REVISIONS_FOR_TRUSTED_HABIT = 3

# See compute_forecast_habit's own comment on why sample_size alone is not
# sufficient evidence of a real pattern: revisions_per_year's own formula
# (revisions / distinct fiscal years WITH a revision) reads a single
# isolated revision identically to a company that revises once a year,
# every year - both produce 1.0. Real data confirmed this live: Ibiden (4
# genuine revisions across 4 consecutive real fiscal years, a genuine
# reliable pattern) and Disco (1 genuine revision, alone, in an otherwise
# empty 5-year window) need to be told apart, and sample_size/ratio alone
# cannot do it. A company's real fiscal-year SPAN (its earliest to latest
# genuine revision) must cover at least this many real years before
# is_trusted can be true - not stored anywhere, computed fresh from
# already-loaded fiscal_years each time compute_forecast_habit runs.
_MIN_FISCAL_YEAR_SPAN_FOR_TRUSTED_HABIT = 3



# Which period a forecast table covers, read from its own line-item
# keys. A filing states the period in the net-income line rather than
# anywhere structural: 当期純利益 is the full year, 中間純利益 the
# half, 四半期純利益 a quarter.
_TABLE_PERIOD_MARKERS = (("中間", "half_year"),
                         ("四半期", "quarter"),
                         ("当期", "full_year"))


# The same three periods as named in a filing's TITLE. Used only
# where the line-item keys are silent - the title is a weaker source
# because a filing that revises two periods names both in it, so
# scanning it first reads Fujikura's "revision of half-year and
# full-year earnings forecasts" as half-year when the table selected
# is the full year.
_TITLE_PERIOD_MARKERS = (
    ("full_year", ("通期", "full-year", "full year")),
    ("half_year", ("中間", "半期", "half-year", "interim")),
    ("quarter", ("四半期", "quarter")),
)


def _title_period(title: str | None) -> str | None:
    """The period a filing's title names, or None if it names none.

    Only meaningful when the title names exactly one. A filing that
    revises a half-year and a full-year forecast says both, and which
    of the two the figures describe is then a question only the table
    can answer.
    """
    if not title:
        return None
    low = title.lower()
    found = [period for period, markers in _TITLE_PERIOD_MARKERS
             if any(m in title or m in low for m in markers)]
    return found[0] if len(found) == 1 else None


def table_period(table: dict[str, Any] | None,
                 title: str | None = None) -> str | None:
    """The period a forecast table covers, or None if unmarked.

    The line-item keys come first and are authoritative: 当期純利益 is
    the full year, 中間純利益 the half, 四半期純利益 a quarter, and
    they describe the table the figures were actually read from.
    Checked longest-marker-first, since 中間 and 四半期 both contain
    characters that also appear in 当期-style keys.

    ``title`` is consulted only when the keys say nothing. Two stored
    SoftBank rows report under IFRS wording - 親会社の所有者に帰属する
    純利益 - which carries no period marker at all, while their titles
    read "full-year earnings forecast" plainly. (The other 30 unmarked
    rows are Renesas, excluded from this signal type entirely for
    reporting Non-GAAP margins with no yen figure.)
    """
    if not table:
        return None
    keys = " ".join(k for k in table if k != "_kind")
    for marker, period in _TABLE_PERIOD_MARKERS:
        if marker in keys:
            return period
    return _title_period(title)


def select_forecast_table(
    figures: list[dict[str, Any]] | None,
    title: str | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """The table a revision should be judged on, and its period.

    Prefers the full year. One filing routinely revises two periods at
    once - a half-year forecast and the full year it sits inside - and
    the full year is the one a reader means by "the forecast".

    This used to take ``figures[0]`` on the stated grounds that a
    filing lists its primary table first. The real PDFs say otherwise:
    Fujikura's own Q2+full-year filing, cited as the confirming
    example, puts the HALF-YEAR table first. Across the stored rows,
    51 of 66 multi-table filings carry a full-year table somewhere
    after position 0, so the card was labelling half-year and
    quarterly figures "full-year" and the habit median was pooling
    periods that are not comparable.

    Falls back to the first table when no table is marked full-year -
    a single-table filing, or one whose keys carry no period marker at
    all (33 stored tables). Its period comes back as whatever that
    table is, so a caller can still say which it got.

    The habit median stays pooled across periods rather than split by
    one, and this selection is why. Measured over the stored rows,
    choosing the full-year table leaves 145 of 154 revisions on the
    full year, 6 on a quarter, 1 on a half and 2 unmarked. A habit
    split per period would compute medians over one and six
    observations - below the three-sample floor that gates a trusted
    habit at all - so it would add a column and withhold the
    comparison, where pooling keeps a 145-sample median whose handful
    of other-period members cannot move it meaningfully.
    """
    if not figures:
        return None, None
    for table in figures:
        if table_period(table) == "full_year":
            return table, "full_year"
    # Title only for the fallback table. Asking it per-table above
    # would let one title's word stand in for every table's period.
    return figures[0], table_period(figures[0], title)


def _get_revenue(figures: list[dict[str, Any]] | None) -> dict[str, float | None] | None:
    """The {"previous", "revised"} revenue pair for the table this
    revision is judged on, or None if the table carries no revenue
    line. Same table selection as operating profit, so the two are
    always read from the same period.
    """
    table, _ = select_forecast_table(figures)
    if not table:
        return None
    for key in _REVENUE_KEYS:
        if key in table:
            return table[key]
    return None


def _get_operating_profit(figures: list[dict[str, Any]] | None) -> dict[str, float | None] | None:
    """The {"previous", "revised"} operating-profit pair for the table
    this revision should be judged on - see select_forecast_table for
    which that is. Returns None if no table carries either key.
    """
    table, _ = select_forecast_table(figures)
    if not table:
        return None
    for key in _OPERATING_PROFIT_KEYS:
        if key in table:
            return table[key]
    return None


def _find_table_of_kind(figures: list[dict[str, Any]] | None, kind: str) -> dict[str, Any] | None:
    """Return the FIRST table in ``figures`` whose "_kind" tag matches
    ``kind``, or None if none does.

    Confirmed live 2026-09-29 this is necessary, not optional: an earlier
    version of classify_results_against_forecast assumed the accompanying
    revision table always sits at figures[1] (immediately after the
    actual_vs_forecast table at figures[0]) - true for most real filings,
    but a real Disco filing (140120260721597097) has FOUR tables (Q1
    consolidated actual_vs_forecast, Q1 STANDALONE actual_vs_forecast,
    then the H1 consolidated revision, then H1 standalone revision) -
    figures[1] there is a SECOND actual_vs_forecast table, not the
    revision target at all, which produced a nonsensical 120.3%
    "progress" reading (comparing Q1 consolidated actuals against Q1
    standalone's own unrelated figures, not a real full-year target).
    Searching by _kind directly, rather than assuming a fixed index,
    is the correct general fix - the same "read the real tag, don't infer
    from position/absence" lesson news-retrieval's own _kind tag itself
    was added to teach (see pipeline.py's _irbank_pdf_table_kind).
    """
    if not figures:
        return None
    for table in figures:
        if table.get("_kind") == kind:
            return table
    return None


def _is_genuine_revision(article: dict[str, Any]) -> bool:
    """True if this jp_forecast article's FIRST table is a genuine
    forward-looking forecast REVISION (news-retrieval's own "_kind":
    "revision" tag - old-forecast-vs-new-forecast) rather than a pure
    actuals-vs-forecast variance notice ("_kind": "actual_vs_forecast" -
    old-forecast-vs-REAL-RESULTS, J2's data, not J1's) or a first-time
    forecast announcement with nothing to compare against.

    An earlier version of this check inferred "genuine revision" from
    "previous and revised are both non-null" - confirmed live 2026-09-29
    this is WRONG for a real, non-trivial fraction of rows: an
    actual_vs_forecast table's own (A) row is a real PRIOR FORECAST value
    (not null), so a pure actuals-vs-forecast notice with no accompanying
    revision (confirmed live: ALL 5 of SUMCO's stored "業績予想値と決算値
    との差異に関するお知らせ" filings, plus real examples from Disco,
    Fujikura, Shin-Etsu Chemical, Screen Holdings) was silently counted as
    a genuine J1 revision and polluted the habit computation with actuals
    data - not the illustrative-vs-real-habit numbers Japan Signals spec
    Section 4.1 asks for. Checking figures[0]'s own "_kind" tag directly
    (added to news-retrieval's own extraction specifically to resolve
    this ambiguity - see pipeline.py's _irbank_pdf_table_kind) is the
    correct, direct signal; title-text pattern matching (差異 without 修正)
    was considered and rejected as a workaround for the same reason every
    other title-text heuristic in this codebase gets replaced once a
    direct signal exists - real title wording combines both concepts too
    often to reliably split by text alone (e.g. "...業績予想と実績との
    差異、通期業績予想の修正...に関するお知らせ" states both in one title).
    """
    meta = article.get("metadata") or {}
    if meta.get("code") in _JAPAN_OPERATING_PROFIT_EXCLUDED_CODES:
        return False
    figures = meta.get("figures")
    if not figures or figures[0].get("_kind") != "revision":
        return False
    op_profit = _get_operating_profit(figures)
    if not op_profit:
        return False
    return op_profit.get("previous") is not None and op_profit.get("revised") is not None


def _pct_change(previous: float, revised: float) -> float:
    """Percentage change from previous to revised - signed (negative for
    a cut). Division-by-zero (previous == 0, a company forecasting
    breakeven) is not handled by this function - see
    classify_forecast_revision's own profit/loss-swing check, which
    handles the previous==0 or sign-crossing cases before any percentage
    is computed from them, since a percentage off a zero or near-zero
    base is not a meaningful "how big was this change" figure at all.
    """
    return (revised - previous) / abs(previous) * 100.0


def _fiscal_year_label(pub_dt: datetime, fiscal_year_end: str) -> int:
    """Return the fiscal year a filing published at ``pub_dt`` belongs to,
    per Japan Signals spec Section 2.4 ("a company's stated year label
    refers to the year it ENDS in... never assume December").

    ``fiscal_year_end``: "MM-DD" (japan_ticker_universe()'s own field). A
    filing published on or before the fiscal year's own end-of-year date
    in a given calendar year belongs to that calendar year's fiscal year;
    a filing published AFTER that date belongs to the NEXT calendar year's
    fiscal year (e.g. Advantest's fiscal_year_end is "03-31" - a July 2024
    filing is published after 2024-03-31, so it belongs to fiscal year
    2025, the year that "2025年3月期" label itself names - confirmed live
    this matches IRBANK's own filing titles, e.g. "通期連結業績予想の修正"
    filed 2024-07-31 is titled under the company's FY2025 in its own real
    disclosure history).
    """
    month, day = (int(x) for x in fiscal_year_end.split("-"))
    fiscal_end_this_year = date(pub_dt.year, month, day)
    return pub_dt.year if pub_dt.date() <= fiscal_end_this_year else pub_dt.year + 1


def _article_published_dt(article: dict[str, Any]) -> datetime | None:
    published = article.get("published")
    if isinstance(published, datetime):
        return published if published.tzinfo else published.replace(tzinfo=timezone.utc)
    if isinstance(published, str) and published:
        try:
            return datetime.fromisoformat(published).replace(tzinfo=timezone.utc)
        except ValueError:
            return None
    return None


def compute_forecast_habit(revisions: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute one company's real forecast-revision habit from its own
    stored history, per Japan Signals spec Section 4.1 ("for each company
    we store its pattern: how many times it typically revises in a year,
    in which direction, by roughly how much").

    ``revisions``: this company's own genuine revisions only (already
    filtered via _is_genuine_revision) - each a news-retrieval article
    dict with metadata.figures/metadata.code/published populated.

    Returns:
      revisions_per_year: count of genuine revisions / number of distinct
        fiscal years they span (at least 1, so a single-fiscal-year
        sample doesn't divide by a fraction and inflate the rate) -
        confirmed live this real per-fiscal-year grouping is necessary,
        not calendar-year: Advantest's real revisions cluster in Jan/Jul/
        Oct windows that straddle a calendar-year boundary but stay
        within the SAME fiscal year (FY ends 03-31).
      typical_size_pct: median ABSOLUTE percentage change across all
        revisions with a computable percentage (previous != 0) - median,
        not mean, per this module's own design decision (a single outlier
        revision, e.g. a COVID-year swing, should not set the bar for
        "normal" going forward - confirmed live 2026-09-28 this matters in
        practice: Advantest's own 6 real revisions range from +21.4% to
        +53.3%, a wide enough spread that a mean vs. median choice
        produces a materially different baseline).
      typical_size_mad_pct: the median absolute deviation (MAD) of the
        same pct_changes sample, scaled by 1.4826 (the standard constant
        that makes MAD comparable to a normal distribution's stdev for a
        roughly-normal sample - conventional, not this module's own
        invention). Used by classify_forecast_revision's own rule 6 as
        the real, principled bar for "unusually large even for this
        volatile company" (median + 2*MAD) - replacing an earlier, real,
        confirmed-live finding that a flat multiple of the median alone
        (e.g. 1.5x) was an arbitrary number tuned to hit a target
        reduction in signal volume, not derived from any real statistical
        property of the data. MAD, not stdev, for the same median-over-
        mean reasoning typical_size_pct itself already uses: this
        module's own real data has fat-tailed per-company outliers
        (confirmed live: Resonac's own real revisions range up to 380%,
        Shinko Electric's up to 575%) that would otherwise distort a
        mean+stdev bar upward by one freak historical event, hiding a
        genuinely large-but-not-record-setting future move. None if
        pct_changes is empty (nothing to compute a spread from) or has
        fewer than 2 values (MAD of a single value is trivially 0,
        indistinguishable from "no real variation" - not stored as a
        false 0.0, left None so callers can tell "no real spread data"
        from "this company's revisions genuinely never vary").
      typical_direction: "raise" or "cut", whichever is more common across
        this company's own real revisions (ties broken toward "raise" -
        arbitrary but stated, since the spec itself never resolves this
        case and it only matters for descriptive display, not for any
        numeric threshold below).
      typical_months: up to 3 calendar months (1-12) this company's real
        revisions most commonly land in - the 4th stored number the user
        explicitly asked for ("what point in the year does it usually
        happen"). A SET of months, not one, since a real filer revises at
        multiple points across its fiscal year (confirmed live: Advantest
        clusters at Jan/Jul/Oct - three distinct months, not one) - the up
        to 3 most frequent months by real count, ties broken by earliest
        calendar month for determinism. Empty list if revisions is empty.
      sample_size: how many revisions this habit was computed from - see
        _MIN_REVISIONS_FOR_TRUSTED_HABIT for how callers must use this.
      is_trusted: sample_size >= _MIN_REVISIONS_FOR_TRUSTED_HABIT.

    Returns None for every field except sample_size/is_trusted (False) if
    ``revisions`` is empty - a company with zero genuine revisions in its
    stored history (not yet seen live, but not excluded by construction)
    has no habit to compute at all, not a habit of "never revises" (that
    is itself a real, computable rate of 0/year once at least one fiscal
    year of history exists with zero revisions in it - a genuinely
    different, not-yet-built case from "no data exists at all").
    """
    if not revisions:
        return {
            "revisions_per_year": None,
            "typical_size_pct": None,
            "typical_size_mad_pct": None,
            "typical_direction": None,
            "typical_months": [],
            "sample_size": 0,
            "is_trusted": False,
        }

    fiscal_years: set[int] = set()
    pct_changes: list[float] = []
    directions: list[str] = []
    months: list[int] = []

    for r in revisions:
        meta = r.get("metadata") or {}
        code = meta.get("code")
        ticker = company_for(code)
        pub_dt = _article_published_dt(r)
        if ticker and pub_dt:
            fiscal_years.add(_fiscal_year_label(pub_dt, ticker["fiscal_year_end"]))
        if pub_dt:
            months.append(pub_dt.month)

        op_profit = _get_operating_profit(meta.get("figures"))
        previous = op_profit.get("previous")
        revised = op_profit.get("revised")
        if previous and previous != 0:
            pct = _pct_change(previous, revised)
            # A genuinely UNCHANGED figure (pct == 0.0 exactly - confirmed
            # live 2026-09-29 not yet seen in the real dataset, but a real
            # possible case: a filing that restates the same operating-
            # profit number while revising a different line item) is
            # neither a raise nor a cut - "raise if pct > 0 else cut" (the
            # earlier version of this check) silently miscounted it as a
            # cut, which could then wrongly feed a false reversal
            # detection in classify_forecast_revision (a real prior raise
            # followed by a truly-unchanged filing would look like a
            # raise-then-cut reversal that never actually happened).
            # Excluded from both the size sample (0% is not informative
            # about "how big is a typical change") and the direction tally
            # (it is neither direction) rather than silently attributed to
            # one side.
            if pct != 0:
                pct_changes.append(abs(pct))
                directions.append("raise" if pct > 0 else "cut")
        elif revised is not None and previous == 0:
            # A previous of exactly 0 (breakeven forecast) makes any
            # percentage change undefined/infinite - direction is still
            # knowable (revised > 0 is a raise into profit, < 0 a cut into
            # loss) even though no percentage is - not yet seen live in
            # the confirmed dataset, handled defensively rather than
            # crashing on a real future filing that hits this case.
            directions.append("raise" if revised > 0 else "cut")

    typical_size_pct = statistics.median(pct_changes) if pct_changes else None
    typical_size_mad_pct = None
    if len(pct_changes) >= 2:
        typical_size_mad_pct = statistics.median(
            [abs(x - typical_size_pct) for x in pct_changes]
        ) * 1.4826
    raise_count = directions.count("raise")
    cut_count = directions.count("cut")
    typical_direction = "raise" if raise_count >= cut_count else "cut"
    revisions_per_year = len(revisions) / max(len(fiscal_years), 1)
    sample_size = len(revisions)

    month_counts: dict[int, int] = {}
    for m in months:
        month_counts[m] = month_counts.get(m, 0) + 1
    typical_months = [
        m for m, _ in sorted(month_counts.items(), key=lambda item: (-item[1], item[0]))
    ][:3]

    # sample_size alone is not sufficient evidence of a real pattern - a
    # real cross-check against this company's own dates found revisions_
    # per_year (revisions / distinct fiscal years WITH a revision) reads a
    # single isolated revision the same way it reads a company that
    # genuinely revises once a year, every year: both produce exactly 1.0,
    # since the denominator only counts years that had a revision at all,
    # never the years that didn't. A real company with 4 genuine revisions
    # spread across 4 consecutive real fiscal years (a real, reliable
    # once-a-year pattern) and a different real company with a single
    # isolated revision sitting alone in an otherwise-empty 5-year window
    # both compute the same 1.0/year - the ratio alone cannot tell a real
    # recurring pattern apart from one sparse data point.
    #
    # What actually distinguishes them is REAL ELAPSED TIME: the reliable
    # reviser's revisions genuinely span multiple real years; the sparse
    # one's single revision spans none. is_trusted now requires both the
    # existing sample-size floor AND that the company's own real fiscal-
    # year span (max - min + 1) covers at least _MIN_FISCAL_YEAR_SPAN_
    # FOR_TRUSTED_HABIT real years - not just that enough revisions exist,
    # but that they were actually observed over enough real time to mean
    # something. A company whose real revisions are unlucky enough to
    # cluster inside one or two fiscal years (even if there happen to be
    # 3+ of them) is not yet trusted either, for the same reason a company
    # with too few revisions isn't - not enough real elapsed time has
    # passed to say this is a genuine, repeatable pattern rather than a
    # temporary cluster.
    fiscal_year_span = (max(fiscal_years) - min(fiscal_years) + 1) if fiscal_years else 0
    is_trusted = (
        sample_size >= _MIN_REVISIONS_FOR_TRUSTED_HABIT
        and fiscal_year_span >= _MIN_FISCAL_YEAR_SPAN_FOR_TRUSTED_HABIT
    )

    return {
        "revisions_per_year": round(revisions_per_year, 2),
        "typical_size_pct": round(typical_size_pct, 1) if typical_size_pct is not None else None,
        "typical_size_mad_pct": round(typical_size_mad_pct, 1) if typical_size_mad_pct is not None else None,
        "typical_direction": typical_direction if directions else None,
        "typical_months": typical_months,
        "sample_size": sample_size,
        "is_trusted": is_trusted,
    }


# Japan Signals spec Section 6.1, rule 1: "The company attributes the
# change only to currency or an accounting change and states the
# underlying business is unchanged." Confirmed live 2026-09-28 (20-
# company/5-year audited sample, 154 real revisions): NOT ONE real
# reason text matches this case - every real reason found cites a genuine
# business driver (AI/semiconductor demand, segment performance, cost
# items, impairments), and several ALSO mention 為替/円安/円高 (currency)
# as one contributing factor among others, never as the sole cause.
#
# An earlier version of this function inferred "currency-only" from the
# ABSENCE of a small fixed whitelist of business-driver keywords (需要/
# 受注/生産/etc.) - confirmed live this produces real false positives: a
# genuine SUMCO reason citing lower-than-expected depreciation cost
# ("減価償却費を含むコストの発生が前回公表時の予想を下回った") and a
# genuine Resonac reason citing real segment demand ("半導体・電子材料
# セグメントにおいて需要が引き続き好調に推移") were both misclassified as
# NOISE purely because they also mentioned 為替 and didn't happen to use
# one of the whitelisted words - the same "keep discovering one more
# real-vocabulary variant" failure mode already hit and fixed twice for
# the reason-text boilerplate trim (see _IRBANK_REASON_BOILERPLATE_RE's
# own module-level comment in news-retrieval's pipeline.py).
#
# Rewritten to require POSITIVE, explicit evidence instead of inferring
# from absence: the reason must contain an explicit "solely due to
# currency" / "the underlying business is unaffected" phrasing pattern -
# a genuine business-driver-absent case would say something like "為替
# の影響のみにより" (due to currency effects ONLY) or "事業の状況に変化は
# ありません" (no change in business conditions) - not just happen to
# omit a whitelisted word. Since no real filing has ever used this
# phrasing in the audited sample, this rule is expected to correctly
# never fire against real data today, which is the honest, safe default
# given the false-positive risk just confirmed - it exists so a real
# future filing using this exact framing is still caught, rather than
# guessing at inclusion/exclusion word lists that keep needing new
# entries.
_CURRENCY_ONLY_EXPLICIT_RE_PARTS = ("為替.{0,10}のみ", "為替.{0,15}影響のみ", "事業.{0,10}変化はありません")


def _is_currency_or_accounting_only(reason: str | None) -> bool:
    """True only if ``reason`` explicitly states the change is due to
    currency/accounting ALONE - see module comment above on why this
    requires positive phrasing evidence rather than inferring from the
    absence of business-driver keywords (confirmed live to false-positive
    on real mixed-cause reasons).
    """
    if not reason:
        return False
    import re

    return any(re.search(pattern, reason) for pattern in _CURRENCY_ONLY_EXPLICIT_RE_PARTS)


def classify_forecast_revision(
    articles: list[dict[str, Any]],
    stored_habits: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Classify jp_forecast (irbank_financials) articles per Japan Signals
    spec Section 6.1 - J1, "the best single item in this market" (spec
    Section 5's own words).

    Only touches articles whose metadata.source_category == "jp_forecast"
    and that pass _is_genuine_revision (see that function's own docstring
    for why ~21% of real rows are pure variance notices, excluded here
    entirely - they contribute nothing to this signal type and are not
    classified as anything by this function).

    ``stored_habits``: {code: habit_dict}, normally
    models.japan_company_habits.get_all_habits()'s own return value - the
    PRE-COMPUTED per-company habit (revisions/year, typical size,
    direction, typical months), refreshed periodically by
    refresh_japan_habits() from the company's full stored history, not
    recomputed from THIS batch's own (possibly much smaller/more recent)
    articles - explicit user instruction 2026-09-28: "save those against
    each company... then when a new revision comes in, compare it to that
    company's own four numbers instead of a fixed threshold." A habit
    computed from only the few revisions in one classification batch
    would silently drift from - and eventually contradict - the company's
    own REAL 5-year pattern (e.g. a batch containing only this company's 2
    most recent revisions could never detect a 3-revisions/year real habit
    at all). Falls back to computing the habit from ``articles`` alone
    (the pre-refresh-table behavior) ONLY when no stored habit exists yet
    for a given code (e.g. before refresh_japan_habits has ever run) -
    this keeps the function safely callable standalone/in tests without
    requiring the DB-backed cache to exist first, while production callers
    (controllers/run.py) always pass the real stored_habits.

    Processes each company's own revisions together (grouped by
    metadata.code, sorted oldest-first) since Section 6.1's reversal rule
    is inherently relational - a same-fiscal-year reversal check against
    the immediately preceding revision - not evaluable from a single
    article in isolation the way Korea's own classify_supply_contract/
    classify_capacity_commitment rules are.

    Japan Signals spec Section 6.1 rules, applied in this exact order
    (first match wins, matching the spec's own "applied in order" table):
      1. NOISE - currency/accounting-only change, business unchanged.
         See _is_currency_or_accounting_only's own docstring: no real
         trigger confirmed yet.
      2. SIGNAL - reverses the direction of an earlier revision in the
         SAME fiscal year (a company that raised earlier this fiscal year
         now cuts, or vice versa).
      3. SIGNAL - moves between forecasting a profit and forecasting a
         loss (previous and revised operating profit have different
         signs, or previous == 0 crossing into a negative revised value).
      4. SIGNAL - this company's own habit shows fewer than one revision
         a year on average (rarely revises) and it has revised. Only
         evaluated when the habit is trusted (see
         _MIN_REVISIONS_FOR_TRUSTED_HABIT) - an untrusted low sample count
         cannot yet distinguish "genuinely rare" from "not enough history
         collected yet".
      5. SIGNAL - the revision is more than twice this company's own
         typical (median) revision size. Only evaluated when the habit is
         trusted, same reasoning as rule 4.
      6. SIGNAL - operating profit forecast changes by 20% or more. For an
         UNTRUSTED-habit company this alone is sufficient (the spec's own
         explicit habit-independent backstop - no trusted habit at all
         needed). For a TRUSTED-habit company, the move must ALSO exceed
         median + _ABSOLUTE_SIGNAL_MAD_MULTIPLE * MAD (median absolute
         deviation, this company's own real spread - see that constant's
         own comment for the full statistical testing behind this bar,
         and why a bare ">" against the median, or an earlier flat
         multiplier of it, both let routine above-average activity for
         volatile companies flood this rule with false positives).
      7. WEAK - anything else (a routine revision from a company that
         revises routinely, within its own normal range).

    Returns a list of {"article", "result"} dicts, same shape every other
    classify_* function in this codebase returns - result["source_id"] is
    the filing's own synthetic irbank-financials:// URL (already globally
    unique per filing, set by news-retrieval), so the caller's existing
    dedup-before-insert pattern works unchanged.
    """
    stored_habits = stored_habits or {}
    by_code: dict[str, list[dict[str, Any]]] = {}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_forecast":
            continue
        if not _is_genuine_revision(a):
            continue
        by_code.setdefault(meta["code"], []).append(a)

    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0, "NOISE": 0}

    for code, company_revisions in by_code.items():
        ticker = company_for(code)
        company_revisions.sort(key=lambda a: _article_published_dt(a) or datetime.min.replace(tzinfo=timezone.utc))
        # Prefer the pre-computed, full-history habit (see this function's
        # own docstring on why) - only fall back to computing it from this
        # batch's own (possibly small/recent-only) revisions when no
        # stored habit exists yet for this code at all.
        habit = stored_habits.get(code) or compute_forecast_habit(company_revisions)

        prior_fiscal_year_direction: dict[int, str] = {}

        for r in company_revisions:
            meta = r["metadata"]
            _table, _period = select_forecast_table(
                meta["figures"],
                meta.get("translated_title") or a.get("title"))
            op_profit = _get_operating_profit(meta["figures"])
            previous = op_profit["previous"]
            revised = op_profit["revised"]
            pub_dt = _article_published_dt(r)
            fiscal_year = (
                _fiscal_year_label(pub_dt, ticker["fiscal_year_end"])
                if ticker and pub_dt else None
            )

            is_profit_loss_swing = (previous > 0 > revised) or (previous < 0 < revised) or (
                previous == 0 and revised != 0
            )
            pct = _pct_change(previous, revised) if previous != 0 else None
            # A genuinely unchanged figure (pct == 0.0 exactly) is neither
            # a raise nor a cut - direction stays None for it, same fix
            # and same reasoning as compute_forecast_habit's own identical
            # correction above (see that function's own comment) -
            # without this, an unchanged revision could wrongly register
            # as a "cut" and trigger a false reversal against a real prior
            # raise, or vice versa.
            direction = None
            if pct is not None and pct != 0:
                direction = "raise" if pct > 0 else "cut"
            elif pct is None and revised is not None:
                direction = "raise" if revised > 0 else "cut"

            is_reversal = (
                fiscal_year is not None
                and fiscal_year in prior_fiscal_year_direction
                and direction is not None
                and direction != prior_fiscal_year_direction[fiscal_year]
            )

            company_name = meta.get("company", "This company")
            # One decimal, matching metadata.operating_profit_pct_change's
            # own stored precision and the summary's rendering of it, with
            # a trailing zero dropped (53.3% stays 53.3%, 40.0% reads
            # 40%). Two decimals cost reading speed for digits the filing
            # itself does not emphasise; rounding to whole points loses a
            # real figure the reader may check against the filing.
            pct_rounded = round(abs(pct), 1) if pct is not None else None
            pct_abs_str = (
                f"{pct_rounded:.1f}".rstrip("0").rstrip(".") + "%"
                if pct_rounded is not None else "an unquantifiable amount"
            )
            # The article follows how the number is SPOKEN, not how it is
            # spelled: "an 8% raise" (eight), "an 11% raise" (eleven),
            # "an 18% raise" (eighteen) - but "a 13.8% raise", because
            # thirteen starts with a consonant. Only a leading 8 and the
            # whole-number 11 and 18 take "an"; 12-17 do not, and neither
            # does 13.8 merely for sitting between them. "0.8%" is spoken
            # "zero point eight" and takes "a", so the test is the
            # whole-number part rather than the first digit printed.
            pct_article = "A"
            if pct_rounded is not None:
                whole = int(abs(pct_rounded))
                if str(whole).startswith("8") or whole in (11, 18):
                    pct_article = "An"

            if _is_currency_or_accounting_only(meta.get("reason")):
                signal = "NOISE"
                reason_code = "currency_or_accounting_only"
                reason_text = (
                    f"{company_name} attributes this change only to currency or an "
                    f"accounting effect, with the underlying business unchanged."
                )
            elif is_reversal:
                signal = "SIGNAL"
                reason_code = "reverses_direction_within_fiscal_year"
                prior_direction = prior_fiscal_year_direction.get(fiscal_year)
                meta["reversed_direction"] = prior_direction
                reason_text = (
                    f"{company_name} reversed direction within the same fiscal year - "
                    f"it previously {'raised' if prior_direction == 'raise' else 'cut'} its "
                    f"forecast and has now {'raised' if direction == 'raise' else 'cut'} it "
                    f"({pct_abs_str} {'increase' if direction == 'raise' else 'decrease'})."
                )
            elif is_profit_loss_swing:
                signal = "SIGNAL"
                reason_code = "profit_loss_swing"
                reason_text = (
                    f"{company_name} moved between forecasting a profit and a loss "
                    f"(operating profit forecast changed from {previous:,.0f} to {revised:,.0f})."
                )
            elif habit["is_trusted"] and habit["revisions_per_year"] < _RARE_REVISER_THRESHOLD_PER_YEAR:
                signal = "SIGNAL"
                reason_code = f"rare_reviser_{habit['revisions_per_year']}_per_year"
                reason_text = (
                    f"{company_name} rarely revises its forecast (about "
                    f"{habit['revisions_per_year']:.1f} times a year on average) and has "
                    f"revised now - a {pct_abs_str} {'raise' if direction == 'raise' else 'cut'}."
                )
            elif (
                habit["is_trusted"]
                and habit["typical_size_pct"] is not None
                and pct is not None
                and abs(pct) > habit["typical_size_pct"] * _TYPICAL_SIZE_MULTIPLE_THRESHOLD
                # A multiple of the median and the median plus a multiple
                # of the MAD answer the same question - "is this move
                # unusual for this company" - and a move should clear
                # both before it is reported as unusual. Which of the two
                # is the higher bar depends on the company's own spread:
                # for a tight distribution 2x the median is the stricter
                # test, while for a wide one (Fujikura's median 19.2% with
                # a 17.5 MAD) median+2MAD is stricter, and a move can pass
                # 2x the median while still sitting inside the company's
                # ordinary range. Requiring both makes this rule and the
                # absolute-floor rule below agree on what "unusual" means
                # rather than each company's spread deciding which
                # definition applies.
                and (
                    habit["typical_size_mad_pct"] is None
                    or abs(pct) > habit["typical_size_pct"]
                    + _ABSOLUTE_SIGNAL_MAD_MULTIPLE * habit["typical_size_mad_pct"]
                )
            ):
                signal = "SIGNAL"
                reason_code = f"exceeds_2x_typical_size_{habit['typical_size_pct']}pct"
                reason_text = (
                    f"This {pct_abs_str} {'raise' if direction == 'raise' else 'cut'} is more "
                    f"than twice {company_name}'s own typical revision size "
                    f"(usually about {habit['typical_size_pct']:.1f}%)."
                )
            elif (
                pct is not None
                and abs(pct) >= _ABSOLUTE_SIGNAL_THRESHOLD_PCT
                and (
                    not habit["is_trusted"]
                    or habit["typical_size_pct"] is None
                    or (
                        habit["typical_size_mad_pct"] is not None
                        and abs(pct) > habit["typical_size_pct"]
                        + _ABSOLUTE_SIGNAL_MAD_MULTIPLE * habit["typical_size_mad_pct"]
                    )
                    or (
                        habit["typical_size_mad_pct"] is None
                        and abs(pct) > habit["typical_size_pct"] * _ABSOLUTE_SIGNAL_TYPICAL_SIZE_MULTIPLE
                    )
                )
            ):
                # The fixed 20% floor alone is not sufficient once a
                # company's own typical size is itself large - a company
                # that routinely moves 35%+ making a real, large 22% move
                # would otherwise trigger this rule despite the move being
                # SMALLER than what's normal for that company. Requiring
                # the move to exceed median + _ABSOLUTE_SIGNAL_MAD_MULTIPLE
                # * MAD (see that constant's own comment for the full,
                # real statistical testing behind it) closes that gap with
                # a bar that self-scales to each company's own real
                # spread, while staying a materially weaker/wider bar than
                # the 2x-typical-size rule above (this rule only reaches
                # once that one has already failed to fire). A company
                # with no real MAD yet (fewer than 2 pct_changes) falls
                # back to the older flat _ABSOLUTE_SIGNAL_TYPICAL_SIZE_
                # MULTIPLE, and an untrusted company (is_trusted=False)
                # falls back to the flat 20% floor alone - with too few
                # real samples to trust typical_size_pct/MAD as genuine
                # statistics, comparing against either would be trusting a
                # number this module itself has already decided not to
                # trust anywhere else.
                signal = "SIGNAL"
                reason_code = f"absolute_change_{round(pct, 1)}pct_gte_{_ABSOLUTE_SIGNAL_THRESHOLD_PCT}"
                reason_text = (
                    f"{company_name} {'raised' if direction == 'raise' else 'cut'} its "
                    f"operating profit forecast by {pct_abs_str} - "
                    f"at least {_ABSOLUTE_SIGNAL_THRESHOLD_PCT:.0f}%, which counts as a "
                    f"signal regardless of this company's own habit."
                )
            else:
                signal = "WEAK"
                reason_code = "routine_revision_within_habit"
                typical_note = (
                    f" (its own typical size is about {habit['typical_size_pct']:.1f}%)"
                    if habit["is_trusted"] and habit["typical_size_pct"] is not None else ""
                )
                if direction is None:
                    # A genuinely unchanged figure (pct == 0.0 exactly) -
                    # confirmed live 2026-09-29 not yet seen in the real
                    # dataset, but a real reachable case: this is the one
                    # branch a zero-change revision can fall through to
                    # (every Signal rule above needs a non-zero pct to
                    # fire at all). Worded as "unchanged", not defaulted to
                    # "cut" (the pre-fix behavior) or "raise".
                    reason_text = (
                        f"{company_name}'s operating profit forecast is unchanged at this "
                        f"revision - routine for this company{typical_note}."
                    )
                else:
                    reason_text = (
                        f"{pct_article} {pct_abs_str} {'raise' if direction == 'raise' else 'cut'} from "
                        f"{company_name} - routine for this company{typical_note}."
                    )

            if fiscal_year is not None and direction is not None:
                prior_fiscal_year_direction[fiscal_year] = direction

            counts[signal] += 1
            meta["operating_profit_previous"] = previous
            meta["operating_profit_revised"] = revised
            meta["operating_profit_pct_change"] = round(pct, 1) if pct is not None else None
            # Which period these three figures describe. A filing can
            # revise a half-year and a full-year forecast in one
            # document, so a percentage with no period attached cannot
            # be read, compared with another revision, or labelled on a
            # card. None where the table carries no period marker -
            # unknown, rather than assumed to be the full year.
            meta["forecast_period"] = _period
            # Where operating profit did not move, the revenue line is
            # what the filing actually revised. Stored only in that
            # case: when profit moved, profit is the figure the signal
            # is about and a second number beside it competes with the
            # one that matters.
            if pct == 0:
                _rev = _get_revenue(meta["figures"])
                if _rev and _rev.get("previous") and _rev.get("revised"):
                    _rp, _rr = _rev["previous"], _rev["revised"]
                    if _rp != _rr:
                        meta["revenue_previous"] = _rp
                        meta["revenue_revised"] = _rr
                        meta["revenue_pct_change"] = round(
                            _pct_change(_rp, _rr), 1)
            meta["habit_revisions_per_year"] = habit["revisions_per_year"]
            meta["habit_typical_size_pct"] = habit["typical_size_pct"]
            # The spread the rule-6 bar is built from. Stored alongside
            # the median because a consumer showing "unusual for this
            # company" needs both numbers to draw the range and the bar
            # - the median alone says where the middle is, not how wide
            # normal reaches.
            meta["habit_typical_size_mad_pct"] = habit.get("typical_size_mad_pct")
            meta["habit_typical_direction"] = habit["typical_direction"]
            meta["habit_sample_size"] = habit["sample_size"]
            meta["habit_is_trusted"] = habit["is_trusted"]
            meta["fiscal_year"] = fiscal_year
            # native_name: the company's real Japanese name, from the same
            # static japan_ticker_universe() list fiscal_year_end already
            # comes from (via the ticker dict already in scope above) -
            # not fetched/computed per article. None when ticker itself is
            # None (a code outside japan_ticker_universe() - not expected in
            # practice, but not assumed away either).
            meta["native_name"] = ticker["native_name"] if ticker else None
            # The machine-readable rule slug (e.g. "exceeds_2x_typical_
            # size_8.5pct") stays here in metadata for grouping/filtering
            # by rule type - signal_reason (the top-level column, see
            # below) is the plain-English sentence a human reads; both
            # are kept since they answer different questions ("which rule
            # fired" vs. "what does that mean in plain terms").
            meta["signal_reason_code"] = reason_code
            r["metadata"] = meta

            results.append({
                "article": r,
                "result": {
                    "signal": _JAPAN_SIGNAL_MAP[signal],
                    # Deterministic rule, not a scored judgment - null is
                    # more honest than a fabricated confidence value, same
                    # reasoning every other rule/lookup-based classify_*
                    # function in this codebase uses (Korea's own
                    # classify_supply_contract, Taiwan's clause-lookup
                    # path).
                    "signal_score": None,
                    "source_id": r.get("url"),
                    "reason": reason_text,
                    "metadata": meta,
                },
            })

    if any(counts.values()):
        logger.info(
            "[JAPAN_FORECAST_REVISION] classified signal=%d weak=%d noise=%d",
            counts["SIGNAL"], counts["WEAK"], counts["NOISE"],
        )
    return results


# Renesas Electronics (6723) is the one tracked company that reports
# Non-GAAP operating-margin PERCENTAGES rather than an absolute yen
# operating-profit figure - see module docstring and
# _JAPAN_OPERATING_PROFIT_EXCLUDED_CODES for why it cannot use
# classify_forecast_revision's own yen-based rule chain at all. Confirmed
# live against all 20 of its real stored filings: its own "previous
# forecast" (期初予想/A) column is a literal dash EVERY time - not a
# parsing gap, this company's own real disclosure format never restates
# an earlier forecast in this comparison table at all - so a J1-style
# "change from previous forecast" is not computable for it from any real
# data, ever, not just today's stored sample.
#
# What IS real and consistently present: a "reference" row in the same
# table (labelled "前期...実績", "same period last year's actual") -
# giving the new margin figure something real to be compared against,
# just a genuinely different basis than every other company's J1
# comparison (a stated real actual for the same period last year, not
# this company's own earlier stated guidance for the SAME period).
# Confirmed live: 19 of Renesas's 20 real filings have both a revised
# margin and this reference figure, real gaps ranging -8.3 to +10.7
# points, median absolute gap 4.9 points - a real, computable pattern
# worth its own dedicated rule, not silently excluded from every signal
# type as the only alternative.
_MARGIN_BASED_OPERATING_PROFIT_KEY = "Non-GAAP営業利益率"
_MARGIN_BASED_CODES = frozenset({"6723"})
_MARGIN_GAP_SIGNAL_THRESHOLD_POINTS = 10.0  # see docstring below for how this was chosen from real data
_MIN_MARGIN_SAMPLES_FOR_TRUSTED_HABIT = 3


def _is_margin_based_revision(article: dict[str, Any]) -> bool:
    """True if this jp_forecast article is a genuine margin-percentage
    revision for a _MARGIN_BASED_CODES company - the parallel check to
    _is_genuine_revision, for the one real company that structurally
    cannot use that function's own yen-based logic. Requires a real
    revised margin figure (the new guidance) - the reference figure
    (last year's actual) is used for the gap calculation below but is not
    itself required to call this "genuine", since a company's very first
    tracked filing would have a revised margin with no reference figure
    yet (not observed live in the current 5-year window, but a real
    possible future case for a newly-added company reported this way).
    """
    meta = article.get("metadata") or {}
    if meta.get("code") not in _MARGIN_BASED_CODES:
        return False
    figures = meta.get("figures")
    if not figures or figures[0].get("_kind") != "revision":
        return False
    margin = figures[0].get(_MARGIN_BASED_OPERATING_PROFIT_KEY)
    return bool(margin) and margin.get("revised") is not None


def compute_margin_revision_habit(revisions: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute a margin-based company's real habit for its gap between a
    new margin figure and the same real filing's own reference (prior-
    year-actual) figure - the parallel calculation to compute_forecast_
    habit, for the one company (see _MARGIN_BASED_CODES) whose real
    disclosures never restate an earlier forecast to measure a normal
    yen-based revision size against.

    Deliberately NOT reusing compute_forecast_habit's own revisions_per_
    year/typical_direction/typical_months fields - the reversal/rare-
    reviser/typical-months rules in classify_forecast_revision all depend
    on comparing one revision against an immediately preceding one in the
    SAME fiscal year, which is a different question from this company's
    only real comparable pair (new guidance vs. a different fixed
    reference point, last year's actual for the same period). Returns
    only what this company's own real data can actually support:
    typical_gap_pts (median absolute gap, same median-over-mean reasoning
    as every other habit in this module), sample_size, is_trusted.
    """
    gaps: list[float] = []
    for r in revisions:
        meta = r.get("metadata") or {}
        figures = meta.get("figures") or []
        if not figures:
            continue
        margin = figures[0].get(_MARGIN_BASED_OPERATING_PROFIT_KEY) or {}
        revised = margin.get("revised")
        reference = margin.get("reference_prior_year_actual")
        if revised is not None and reference is not None:
            gaps.append(abs(revised - reference))

    if not gaps:
        return {"typical_gap_pts": None, "sample_size": 0, "is_trusted": False}
    return {
        "typical_gap_pts": round(statistics.median(gaps), 1),
        "sample_size": len(gaps),
        "is_trusted": len(gaps) >= _MIN_MARGIN_SAMPLES_FOR_TRUSTED_HABIT,
    }


def classify_margin_forecast_revision(
    articles: list[dict[str, Any]],
    stored_habits: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Classify jp_forecast articles for _MARGIN_BASED_CODES companies -
    the parallel path to classify_forecast_revision, for the one real
    company (Renesas Electronics) whose own disclosures cannot support
    that function's yen-based "change from previous forecast" logic at
    all (see module comment above _MARGIN_BASED_CODES for the full real
    finding). Same Signal/Weak/Noise philosophy, same "measure against
    this company's own real pattern, not a company-blind constant"
    design principle as every other rule in this module - just measuring
    a different real quantity (points of margin gap vs. last year's own
    actual, since that is the only real comparable pair this company's
    filings ever provide, not a change from an earlier stated forecast).

    ``stored_habits``: the SAME {code: habit_dict} lookup classify_
    forecast_revision reads (models.japan_company_habits.get_all_habits())
    - not a separate table/parameter. A margin-based company's row in
    that same table stores its typical gap (points) in the existing
    typical_size_pct column - a genuine, deliberate reuse, not a type
    mismatch: that column's real role in this table has always been "this
    company's typical revision-size measure," and nothing about it is
    yen-percent-specific at the storage layer - only classify_forecast_
    revision's own interpretation of it is. revisions_per_year/
    typical_direction/typical_month_1-3 simply stay NULL for this
    company's row, since no rule in this function reads them (this
    function's own simpler 2-rule chain, see below, needs only a typical
    gap size and a trust flag - the same two fields every other habit-
    based rule in this module already needs). Avoids a second new DB
    table/refresh path for what is, underneath, the exact same real
    concept (a per-company typical-deviation-size cache) computed a
    different way for the one company that needs it computed differently.
    Falls back to computing inline from ``articles`` alone when no stored
    habit exists yet for a code, same fallback contract as classify_
    forecast_revision.

    Rule (deliberately simpler than classify_forecast_revision's 6-rule
    chain - the reversal/profit-loss-swing/rare-reviser rules all depend
    on a same-fiscal-year comparison to an immediately preceding
    revision, which this company's own data cannot support; only the
    two rules that make sense for a fixed-reference-point gap are kept):
      SIGNAL - this filing's gap (new margin vs. last year's actual for
               the same period) is more than this company's own typical
               gap, once trusted (median-based, same _TYPICAL_SIZE_
               MULTIPLE_THRESHOLD-equivalent reasoning as J1's own 2x
               rule - but here the multiplier is folded into a single
               points-based floor, see below).
      SIGNAL - the gap is at least 10.0 points, regardless of habit - the
               margin-equivalent of _ABSOLUTE_SIGNAL_THRESHOLD_PCT's own
               fixed floor, chosen from this company's own real data: the
               real median absolute gap across its full stored history is
               4.9 points, so a floor of double that (10.0) marks a real
               gap large enough to be unusual by this company's own
               long-run standard, not an arbitrary round number - same
               "validate against real data, don't just assert a number"
               standard J1's own 20% floor was held to.
      WEAK   - anything else with a real, computable gap.

    Untrusted (fewer than 3 real samples): falls back to the flat 10.0-
    point floor alone, same reasoning as classify_forecast_revision's own
    untrusted fallback for its 20% floor.
    """
    stored_habits = stored_habits or {}
    by_code: dict[str, list[dict[str, Any]]] = {}
    for a in articles:
        if _is_margin_based_revision(a):
            code = a["metadata"]["code"]
            by_code.setdefault(code, []).append(a)

    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0}

    for code, company_revisions in by_code.items():
        # stored_habits is japan_company_habits's own shape (see this
        # function's own docstring on why no separate table exists) -
        # its "typical_size_pct" column holds this company's typical GAP
        # in points, not a yen percentage; remapped to compute_margin_
        # revision_habit's own field name here so the rest of this
        # function reads one consistent key regardless of which source
        # (stored table vs. inline fallback) the habit came from.
        stored = stored_habits.get(code)
        if stored is not None:
            habit = {
                "typical_gap_pts": stored.get("typical_size_pct"),
                "sample_size": stored.get("sample_size", 0),
                "is_trusted": stored.get("is_trusted", False),
            }
        else:
            habit = compute_margin_revision_habit(company_revisions)

        for r in company_revisions:
            meta = r["metadata"]
            margin = meta["figures"][0][_MARGIN_BASED_OPERATING_PROFIT_KEY]
            revised = margin["revised"]
            reference = margin.get("reference_prior_year_actual")
            company_name = meta.get("company", "This company")

            if reference is None:
                # No reference figure on this specific filing (real,
                # possible case: this company's very first tracked
                # filing) - a real revised margin exists, but nothing yet
                # to compute a gap against. Weak, not Noise - a genuine
                # forecast filing is always at least Weak, same
                # "never Noise" floor classify_results_against_forecast
                # already applies for the analogous case.
                signal = "WEAK"
                reason_code = "margin_no_reference_yet"
                reason_text = (
                    f"{company_name}'s operating margin guidance is now {revised:.1f}% - "
                    f"no prior-year reference figure available yet to judge whether this "
                    f"is unusual."
                )
            else:
                gap = revised - reference
                if habit["is_trusted"] and habit["typical_gap_pts"] is not None and abs(gap) > habit["typical_gap_pts"]:
                    signal = "SIGNAL"
                    reason_code = f"margin_exceeds_typical_gap_{habit['typical_gap_pts']}pts"
                    reason_text = (
                        f"{company_name}'s operating margin guidance of {revised:.1f}% is "
                        f"{abs(round(gap, 1))} points {'above' if gap > 0 else 'below'} its "
                        f"actual margin for the same period last year ({reference:.1f}%) - "
                        f"more than this company's own typical gap of "
                        f"{habit['typical_gap_pts']:.1f} points."
                    )
                elif abs(gap) >= _MARGIN_GAP_SIGNAL_THRESHOLD_POINTS:
                    signal = "SIGNAL"
                    reason_code = f"margin_gap_{round(gap, 1)}pts_gte_{_MARGIN_GAP_SIGNAL_THRESHOLD_POINTS}"
                    reason_text = (
                        f"{company_name}'s operating margin guidance of {revised:.1f}% is "
                        f"{abs(round(gap, 1))} points {'above' if gap > 0 else 'below'} its "
                        f"actual margin for the same period last year ({reference:.1f}%) - "
                        f"at least {_MARGIN_GAP_SIGNAL_THRESHOLD_POINTS:.0f} points, which "
                        f"counts as a signal regardless of this company's own habit."
                    )
                else:
                    signal = "WEAK"
                    reason_code = "margin_gap_within_habit"
                    reason_text = (
                        f"{company_name}'s operating margin guidance of {revised:.1f}% is "
                        f"{abs(round(gap, 1))} points {'above' if gap > 0 else 'below'} its "
                        f"actual margin for the same period last year ({reference:.1f}%) - "
                        f"within its own usual range."
                    )

            counts[signal] += 1
            meta["margin_revised_pct"] = revised
            meta["margin_reference_prior_year_actual_pct"] = reference
            meta["margin_habit_typical_gap_pts"] = habit["typical_gap_pts"]
            meta["margin_habit_sample_size"] = habit["sample_size"]
            meta["margin_habit_is_trusted"] = habit["is_trusted"]
            meta["signal_reason_code"] = reason_code
            r["metadata"] = meta

            results.append({
                "article": r,
                "result": {
                    "signal": _JAPAN_SIGNAL_MAP[signal],
                    "signal_score": None,
                    "source_id": r.get("url"),
                    "reason": reason_text,
                    "metadata": meta,
                },
            })

    if any(counts.values()):
        logger.info(
            "[JAPAN_MARGIN_FORECAST_REVISION] classified signal=%d weak=%d",
            counts["SIGNAL"], counts["WEAK"],
        )
    return results


# Which metadata/top-level fields need translation, per source_category -
# same shape as taiwan_signal_classifier.py's/korea_signal_classifier.py's
# own _TRANSLATION_FIELDS. Only jp_forecast is implemented so far (J1) -
# entries for other Japan source_types are added once each one's own
# classify_* function is built, not written speculatively ahead of it
# (see korea_signal_classifier.py's own _TRANSLATION_FIELDS top comment
# for why an untested field-name guess here would silently no-op forever).
_TRANSLATION_FIELDS: dict[str, dict[str, str]] = {
    "jp_forecast": {
        "title": "translated_title",
        "metadata.reason": "translated_reason",
    },
    "jp_capex": {
        "title": "translated_title",
    },
    "jp_press": {
        "title": "translated_title",
    },
    "jp_disclosure": {
        # Only reached for a filing published in Japanese alone. Where
        # the company also filed in English, _pair_bilingual_disclosures
        # has already set translated_title to the issuer's own wording
        # and the loop above skips it.
        "title": "translated_title",
    },
    "jp_extraordinary": {
        "title": "translated_title",
        # A 第19条第2項第4号 filing names its major shareholder in
        # Japanese and nowhere else on the row - "キャピタル・リサーチ・
        # アンド・マネージメント・カンパニー", which is Capital Research
        # and Management Company written in katakana. The signal
        # sentence is built around that name, so without this the one
        # English card carrying real ownership figures states them
        # about a party the reader cannot identify. Same treatment
        # jp_ownership already gives its own filer_name.
        "metadata.major_shareholder_name": "translated_major_shareholder_name",
    },
    "jp_ownership": {
        # title itself is structural/numeric (ticker, doc_id, docDescription
        # label - see classify_japan_signal_batch's own comment on why
        # jp_ownership was originally left out of this table entirely), but
        # metadata.filer_name is real Japanese prose - the reporting
        # institution's own name (e.g. "三井住友トラスト・アセットマネジメント
        # 株式会社") - confirmed live 2026-09-30 every real jp_ownership row
        # has an unreadable-to-non-Japanese-speakers filer_name and no
        # English equivalent anywhere else on the row. Translating only
        # this one field (not the whole title) avoids re-translating the
        # already-structural ticker/doc_id/docDescription portion.
        "metadata.filer_name": "translated_filer_name",
    },
    "jp_buyback": {
        # title itself is structural/numeric (company name already
        # English, a date, a share-count delta) - confirmed no translation
        # needed there. But three real metadata fields ARE genuine
        # Japanese prose, straight from IRBANK's own page text:
        # program_resolution (e.g. "取締役会（平成26年7月31日）での決議状況"),
        # program_limits, and program_period - all three left untranslated
        # until now despite carrying real information a non-Japanese
        # reader cannot use.
        "metadata.program_resolution": "translated_program_resolution",
        "metadata.program_limits": "translated_program_limits",
        "metadata.program_period": "translated_program_period",
    },
}


def translate_japan_articles(
    articles: list[dict[str, Any]],
    *,
    model: str | None = None,
    api_key: str | None = None,
    base_url: str | None = None,
    timeout: int | None = None,
) -> None:
    """Translate the fields that need it, per source_category, in place -
    identical shape to taiwan_signal_classifier.py's/korea_signal_
    classifier.py's own translate_*_articles (same _get_nested/
    _translate_one helpers, imported directly rather than
    reimplemented).

    Only touches articles whose metadata.source_category has an entry in
    _TRANSLATION_FIELDS. Native-language fields are never overwritten;
    translated values are added as new metadata fields. Failed
    translations leave the translated_* field absent rather than set to
    None/empty.
    """
    import config

    model = model or config.JAPAN_SIGNAL_MODEL
    api_key = api_key or config.OPENAI_API_KEY
    base_url = base_url or config.OPENAI_BASE_URL
    timeout = timeout or config.OPENAI_TIMEOUT

    to_translate: list[tuple[dict, str, str]] = []
    for a in articles:
        category = (a.get("metadata") or {}).get("source_category")
        field_map = _TRANSLATION_FIELDS.get(category)
        if not field_map:
            continue
        for source_path, dest_field in field_map.items():
            # An already-populated destination is authoritative and is
            # left alone: a disclosure the company itself published in
            # English carries that wording here (see
            # _pair_bilingual_disclosures), and the issuer's own English
            # is both more accurate than a translation of it and the
            # version they are accountable for. Re-translating would
            # spend a model call to replace it with something worse.
            if (a.get("metadata") or {}).get(dest_field):
                continue
            if _get_nested(a, source_path):
                to_translate.append((a, source_path, dest_field))

    def _translate_and_apply(item: tuple[dict, str, str]) -> bool:
        article, source_path, dest_field = item
        text = _get_nested(article, source_path)
        result = _translate_one(text, model, api_key, base_url, timeout)
        if result is not None:
            article["metadata"][dest_field] = result
            return True
        return False

    translated_count = 0
    if to_translate:
        import config as _config
        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor(max_workers=_config.TAIWAN_CLASSIFY_CONCURRENCY) as executor:
            translated_count = sum(executor.map(_translate_and_apply, to_translate))

    if to_translate:
        logger.info(
            "[JAPAN_TRANSLATE] %d/%d field(s) translated successfully",
            translated_count, len(to_translate),
        )


# Japan Signals spec Section 5/6.2 - J2, results against forecast.
#
# Data source: the SAME irbank_financials filing that carries a genuine J1
# revision very often ALSO carries a distinct "actual_vs_forecast" table
# (news-retrieval's own _kind tag, added specifically to resolve this -
# see pipeline.py's _irbank_pdf_table_kind) - confirmed live 2026-09-29
# across the real 20-company/5-year dataset: every filing with an
# actual_vs_forecast table in figures[0] also has a genuine revision table
# immediately after it in figures[1], both from the SAME filing/timestamp.
# This means J2 needs no separate fetch or join - both halves of Section
# 6.2's own data-points table ("operating profit for the year so far" /
# "full-year forecast") arrive together in one filing, unlike a naive
# reading of the spec that might assume a separate quarterly-results
# filing has to be matched up with a separate forecast filing after the
# fact.
#
# Period detection: the per-share net-income line item's own label
# reliably names which period the actual covers - "中間" (half-year/H1),
# "四半期" (a quarter, not otherwise numbered in the label itself), or
# "当期" (full fiscal year) appear as SUBSTRINGS of the real label text
# across every filer checked (e.g. "基本的１株当たり中間利益",
# "１株当たり四半期純利益", "1株当たり当期純利益") - confirmed live this
# varies in exact wording (以下 the "基本的"/"1"　vs "１" digit-width
# differences already documented for J1's own labels) but the 中間/四半期/
# 当期 substring itself is consistent. A "当期" (full-year) actual is NOT
# a progress reading at all (the year is essentially over) - only 中間/
# 四半期 rows are progress candidates.
_PERIOD_HALF_YEAR_MARKER = "中間"
_PERIOD_QUARTER_MARKER = "四半期"
_PERIOD_FULL_YEAR_MARKER = "当期"

# Japan Signals spec Section 6.2's own explicit numeric rule.
_PROGRESS_SIGNAL_THRESHOLD_POINTS = 15.0
# Same "not enough real data yet" principle as J1's own
# _MIN_REVISIONS_FOR_TRUSTED_HABIT and Korea's classify_export_surprise -
# confirmed live 2026-09-29: most company/period-type combinations in the
# real 5-year dataset have only 1-3 samples (e.g. SUMCO's own quarterly
# actuals: 3 real readings total across the whole window) - nowhere near
# enough to compute a trustworthy "usual progress at this point" baseline.
# This is expected to produce few or no real Signal classifications today
# via the habit-comparison rule specifically (same honest status as J1's
# own currency-only-Noise rule) - the fallback rule below (WEAK, never
# fabricating a baseline from too little data) is expected to be the
# common real outcome until more years of history accumulate.
_MIN_PROGRESS_SAMPLES_FOR_TRUSTED_HABIT = 3


def _detect_period_type(table: dict[str, Any]) -> str | None:
    """Return "half_year", "quarter", "full_year", or None (unrecognized)
    for an actual_vs_forecast table, by checking its own per-share
    net-income-style label for the 中間/四半期/当期 substring - see this
    module's own comment above classify_results_against_forecast for why
    substring matching (not an exact key list) is required.
    """
    for label in table:
        if label == "_kind":
            continue
        if _PERIOD_HALF_YEAR_MARKER in label:
            return "half_year"
        if _PERIOD_QUARTER_MARKER in label:
            return "quarter"
        if _PERIOD_FULL_YEAR_MARKER in label:
            return "full_year"
    return None


def _fiscal_year_elapsed_fraction(pub_dt: datetime, fiscal_year_end: str) -> float:
    """Fraction (0.0-1.0) of the company's own fiscal year elapsed as of
    ``pub_dt`` - Japan Signals spec Section 2.4's own "never assume
    December" rule applied to a progress calculation, not just a revision
    date label (see _fiscal_year_label's own docstring for the labelling
    case this mirrors).

    Computed from the two most recent fiscal-year-end dates bracketing
    pub_dt (the year-end just passed, and the one still ahead) rather than
    a fixed 365-day assumption, so a leap year or an exact fiscal-year-end
    date itself doesn't skew the fraction.
    """
    month, day = (int(x) for x in fiscal_year_end.split("-"))
    fiscal_end_this_year = date(pub_dt.year, month, day)
    if pub_dt.date() <= fiscal_end_this_year:
        fiscal_start = date(pub_dt.year - 1, month, day) + timedelta(days=1)
        fiscal_end = fiscal_end_this_year
    else:
        fiscal_start = fiscal_end_this_year + timedelta(days=1)
        fiscal_end = date(pub_dt.year + 1, month, day)
    total_days = (fiscal_end - fiscal_start).days + 1
    elapsed_days = (pub_dt.date() - fiscal_start).days + 1
    return max(0.0, min(1.0, elapsed_days / total_days))


def _progress_pct(actual: float, target: float) -> float | None:
    """actual/target as a percentage, or None if the two don't share a
    sign - see this function's one call site's own comment (in both
    compute_progress_habit and classify_results_against_forecast, kept as
    one shared helper rather than duplicated) for why a sign mismatch
    (e.g. a small actual profit against a full-year LOSS target) is
    excluded rather than reported as a mathematically-valid but
    not-intuitively-meaningful negative percentage.
    """
    if target == 0 or (actual > 0) != (target > 0):
        return None
    return actual / target * 100.0


def compute_progress_habit(actuals: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute one company's typical progress-vs-target at a given point
    in its fiscal year, per period_type ("half_year"/"quarter") - Japan
    Signals spec Section 6.2's own "compare to this company's typical
    progress at the same point in previous years."

    ``actuals``: this company's own actual_vs_forecast-tagged articles
    (already filtered to a single period_type by the caller - see
    classify_results_against_forecast, which computes one habit per
    (code, period_type) pair, not one per company, since a company's
    typical H1 progress and typical Q1 progress are genuinely different
    numbers, not interchangeable).

    Returns {typical_progress_pct, sample_size, is_trusted} - median (not
    mean) progress percentage across the sample, same reasoning as
    compute_forecast_habit's own median choice (one outlier year - e.g. a
    COVID-year swing - should not set the bar for "normal"). is_trusted
    requires _MIN_PROGRESS_SAMPLES_FOR_TRUSTED_HABIT samples - see that
    constant's own comment on why this is expected to rarely be met today
    against real, current data depth.
    """
    progress_values: list[float] = []
    for a in actuals:
        meta = a.get("metadata") or {}
        figures = meta.get("figures") or []
        actual_table = _find_table_of_kind(figures, "actual_vs_forecast")
        target_table = _find_table_of_kind(figures, "revision")
        if not actual_table or not target_table:
            continue
        actual_op_profit = _get_operating_profit([actual_table])
        target_op_profit = _get_operating_profit([target_table])
        if not actual_op_profit or not target_op_profit:
            continue
        actual = actual_op_profit.get("revised")
        target = target_op_profit.get("revised")
        if actual is None or not target:
            continue
        pct = _progress_pct(actual, target)
        if pct is not None:
            progress_values.append(pct)

    if not progress_values:
        return {"typical_progress_pct": None, "sample_size": 0, "is_trusted": False}
    return {
        "typical_progress_pct": round(statistics.median(progress_values), 1),
        "sample_size": len(progress_values),
        "is_trusted": len(progress_values) >= _MIN_PROGRESS_SAMPLES_FOR_TRUSTED_HABIT,
    }


def classify_results_against_forecast(
    articles: list[dict[str, Any]],
    stored_progress_habits: dict[tuple[str, str], dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Classify jp_forecast (irbank_financials) articles carrying an
    actual_vs_forecast table per Japan Signals spec Section 6.2 - J2,
    "lets us calculate whether the company is ahead of its own target."

    Only touches articles whose figures[0] has "_kind" ==
    "actual_vs_forecast" AND whose period_type is "half_year" or
    "quarter" (a "full_year" actual is not a progress reading - see
    module comment above). The accompanying full-year TARGET is read from
    figures[1] (the same filing's own revision table, immediately
    following the actual_vs_forecast table - see this function's own
    module-level comment on why both arrive together in one filing) - an
    article with no second table (a bare actuals notice with no
    accompanying revision) is skipped, not classified, since Section 6.2
    needs both halves.

    ``stored_progress_habits``: {(code, period_type): habit_dict}, the
    pre-computed equivalent of stored_habits for J1 - see
    compute_progress_habit's own docstring. Same fallback behavior as
    classify_forecast_revision: computes inline from ``articles`` alone
    only when no stored habit exists for a given (code, period_type) pair.

    Japan Signals spec Section 6.2 rule:
      SIGNAL - progress is more than 15 percentage points ahead of, or
               behind, this company's usual pattern at this point in its
               year. Only evaluated when the (code, period_type) habit is
               trusted (see _MIN_PROGRESS_SAMPLES_FOR_TRUSTED_HABIT) -
               confirmed live this is rarely met today given real current
               data depth (see that constant's own comment), so this rule
               is expected to fire rarely until more years accumulate.
      WEAK   - progress within 15 points of the usual pattern, OR no
               trusted habit exists yet to compare against (a real result
               with nothing established to judge it against is still
               real information, not nothing - same "weak, not dropped"
               principle _extract_contract_fields's own "fields_not_
               extracted" fallback and Korea's own S2/S3 use).
      NOISE  - never. "A results filing is always at least Weak" (spec's
               own explicit words) - matches Korea's own classify_
               preliminary_earnings, which has the identical rule.
    """
    stored_progress_habits = stored_progress_habits or {}
    by_code: dict[str, list[dict[str, Any]]] = {}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_forecast":
            continue
        figures = meta.get("figures") or []
        if not figures or figures[0].get("_kind") != "actual_vs_forecast":
            continue
        # The accompanying full-year TARGET must exist somewhere in this
        # filing's own tables, but NOT necessarily at figures[1] - see
        # _find_table_of_kind's own docstring for the real, confirmed-live
        # Disco filing (4 tables: Q1 consolidated actual, Q1 STANDALONE
        # actual, then the two revision tables) where figures[1] is a
        # second actual_vs_forecast table, not the target at all.
        if _find_table_of_kind(figures, "revision") is None:
            continue
        period_type = _detect_period_type(figures[0])
        if period_type not in ("half_year", "quarter"):
            continue
        by_code.setdefault(meta["code"], []).append(a)

    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0}

    for code, company_actuals in by_code.items():
        ticker = company_for(code)
        for a in company_actuals:
            meta = a["metadata"]
            figures = meta["figures"]
            period_type = _detect_period_type(figures[0])
            actual_table = _find_table_of_kind(figures, "actual_vs_forecast")
            target_table = _find_table_of_kind(figures, "revision")
            actual_op_profit = _get_operating_profit([actual_table]) if actual_table else None
            target_op_profit = _get_operating_profit([target_table]) if target_table else None
            if not actual_op_profit or not target_op_profit:
                continue
            actual = actual_op_profit.get("revised")
            target = target_op_profit.get("revised")
            if actual is None or not target:
                continue
            # See _progress_pct's own docstring for why a sign mismatch
            # (e.g. a small actual profit against a full-year LOSS target)
            # is excluded rather than reported as a confusing negative
            # percentage - confirmed live 2026-09-29 no real case in the
            # current dataset hits this, but a company swinging from a
            # profit to a loss forecast mid-year is a real, plausible
            # future filing. That swing itself is already J1's own job
            # (classify_forecast_revision's is_profit_loss_swing rule),
            # not something J2 needs to additionally characterize via a
            # percentage that doesn't mean what "progress" normally means.
            progress_pct = _progress_pct(actual, target)
            if progress_pct is None:
                continue

            # The accompanying target is not always full-year - confirmed
            # live 2026-09-29 (Disco, 140120260721597097): a real filing
            # can measure Q1 actuals against an H1 (half-year) target, not
            # a full-year one, when the company is issuing fresh H1
            # guidance in the same filing as its Q1 results. Recorded
            # separately from the actual's own period_type so a reader
            # can tell "Q1 actual vs H1 target" apart from "Q1 actual vs
            # full-year target" - conflating the two (an earlier version
            # of this function always said "full-year target" regardless)
            # would misstate what progress_pct is actually measuring.
            target_period_type = _detect_period_type(target_table)
            # _detect_period_type also returns "quarter" and None, and
            # calling either of those "half-year" misstates the target
            # the same way always saying "full-year" did. Unrecognised
            # falls back to full-year: it is the common case and the
            # label the rest of the card uses.
            target_description = {
                "half_year": "half-year",
                "quarter": "quarterly",
                "full_year": "full-year",
            }.get(target_period_type or "", "full-year")

            # How much of the year the REPORTED PERIOD covers - not how
            # much had passed when the company filed. Results are
            # published weeks after the period closes, so the filing
            # date overstates it: Ibiden's 2025-10-30 half-year filing
            # measured 58.4% (30 October's share of an April-March
            # year) against a period that is exactly 50% of it. The
            # 8.4-point gap is subtracted from progress everywhere
            # downstream, and flipped that card from "ahead 3.4" to
            # "behind 5.0" - the verdict, not just the number.
            #
            # A half-year is half a year and a full year is all of it,
            # by definition, so those need no date arithmetic. Only a
            # quarter is ambiguous from the type alone (Q1/Q2/Q3 all
            # say 四半期), so it still falls back to the filing date,
            # which remains the best available estimate there.
            pub_dt = _article_published_dt(a)
            if period_type == "half_year":
                elapsed_pct = 50.0
            elif period_type == "full_year":
                elapsed_pct = 100.0
            elif ticker and pub_dt:
                elapsed_pct = _fiscal_year_elapsed_fraction(
                    pub_dt, ticker["fiscal_year_end"]) * 100.0
            else:
                elapsed_pct = None

            habit_key = (code, period_type)
            if habit_key in stored_progress_habits:
                habit = stored_progress_habits[habit_key]
            else:
                # Fallback (no stored habit for this exact (code,
                # period_type) pair yet) must be computed from THIS
                # period_type's own actuals only, not company_actuals as a
                # whole - confirmed live 2026-09-29 a company can have
                # BOTH half_year and quarter actual_vs_forecast rows (e.g.
                # Ibiden: 2 half_year + 3 quarter), and mixing them here
                # would contaminate the fallback habit with a different
                # period type's own progress percentages, exactly what the
                # (code, period_type) keying elsewhere in this module
                # exists to prevent (see compute_all_japan_progress_
                # habits, which already gets this right by construction).
                same_period_actuals = [
                    other for other in company_actuals
                    if _detect_period_type(other["metadata"]["figures"][0]) == period_type
                ]
                habit = compute_progress_habit(same_period_actuals)
            company_name = meta.get("company", "This company")

            if habit["is_trusted"] and habit["typical_progress_pct"] is not None:
                gap = progress_pct - habit["typical_progress_pct"]
                if abs(gap) > _PROGRESS_SIGNAL_THRESHOLD_POINTS:
                    signal = "SIGNAL"
                    reason_code = f"progress_gap_{round(gap, 1)}pts"
                    reason_text = (
                        f"{company_name} is {abs(round(gap, 1))} points "
                        f"{'ahead of' if gap > 0 else 'behind'} its own usual progress "
                        f"at this point in the year ({progress_pct:.1f}% of target reached, "
                        f"usually about {habit['typical_progress_pct']:.1f}% by now)."
                    )
                else:
                    signal = "WEAK"
                    reason_code = "progress_within_usual_range"
                    reason_text = (
                        f"{company_name} has reached {progress_pct:.1f}% of its {target_description} "
                        f"target - close to its own usual pace at this point "
                        f"(usually about {habit['typical_progress_pct']:.1f}%)."
                    )
            else:
                signal = "WEAK"
                reason_code = "no_trusted_progress_habit_yet"
                reason_text = (
                    f"{company_name} has reached {progress_pct:.1f}% of its {target_description} "
                    f"target - not enough stored history yet to judge whether this is "
                    f"unusual for this company at this point in the year."
                )

            counts[signal] += 1
            meta["period_type"] = period_type
            meta["target_period_type"] = target_period_type
            meta["progress_pct"] = round(progress_pct, 1)
            meta["fiscal_year_elapsed_pct"] = round(elapsed_pct, 1) if elapsed_pct is not None else None
            meta["progress_habit_typical_pct"] = habit["typical_progress_pct"]
            meta["progress_habit_sample_size"] = habit["sample_size"]
            meta["progress_habit_is_trusted"] = habit["is_trusted"]
            meta["signal_reason_code"] = reason_code
            meta["native_name"] = ticker["native_name"] if ticker else None
            a["metadata"] = meta

            results.append({
                "article": a,
                "result": {
                    "signal": _JAPAN_SIGNAL_MAP[signal],
                    "signal_score": None,
                    "source_id": a.get("url"),
                    "reason": reason_text,
                    "metadata": meta,
                },
            })

    if any(counts.values()):
        logger.info(
            "[JAPAN_RESULTS_AGAINST_FORECAST] classified signal=%d weak=%d",
            counts["SIGNAL"], counts["WEAK"],
        )
    return results


def compute_all_japan_progress_habits(
    articles: list[dict[str, Any]],
) -> dict[tuple[str, str], dict[str, Any]]:
    """Compute every tracked company's progress habit, per (code,
    period_type) pair - the J2 equivalent of compute_all_japan_habits.

    Pure function (no DB access) - a future refresh-japan-habits extension
    (or its own CLI command) is responsible for pooling articles and
    persisting this via a models.japan_company_habits-equivalent table for
    progress habits, not yet built (this function exists so
    classify_results_against_forecast has a real, tested computation to
    fall back to today, matching how compute_forecast_habit existed and
    was tested before its own storage layer was added).
    """
    by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_forecast":
            continue
        figures = meta.get("figures") or []
        if not figures or figures[0].get("_kind") != "actual_vs_forecast":
            continue
        period_type = _detect_period_type(figures[0])
        if period_type not in ("half_year", "quarter"):
            continue
        code = meta.get("code")
        if code:
            by_key.setdefault((code, period_type), []).append(a)

    return {key: compute_progress_habit(actuals) for key, actuals in by_key.items()}


# Japan Signals spec Section 5/6.3 - J3, missing revision ("the expected
# thing not happening").
#
# Redefined from the spec's own literal wording after confirming live
# 2026-09-29 against the real 20-company/5-year dataset: a GENUINE
# revision (a real change to an already-existing forecast number) does
# NOT recur on a predictable per-company calendar - confirmed live no
# company/month combination has genuine revisions (figures[0]["_kind"] ==
# "revision" AND a non-null previous value) in 3+ distinct years anywhere
# in the dataset, so "this company normally revises in month X" has no
# real support to check "did it happen again this year" against.
#
# What DOES recur reliably is the FIRST-TIME quarterly/half-year/full-year
# FORECAST ANNOUNCEMENT (figures[0]["_kind"] == "revision" with a NULL
# previous value - no revision, a fresh guidance issuance) - confirmed
# live: Disco's real history shows an announcement in every one of its 19
# real quarterly slots from 2021-10 through 2026-04 with zero gaps, and
# critically, its July 2026 slot (confirmed overdue by 160+ real days as
# of 2026-09-29, checked against BOTH the stored backfill AND IRBANK's own
# live page directly - not a backfill-lag artifact) is a real, live,
# currently-ongoing missing case - the exact "expected thing not
# happening" the spec's own Section 4.1 describes, just for the
# announcement mechanism rather than the revision mechanism specifically.
# This module tracks announcement slots, not literal "revisions", as the
# thing whose absence is detected - the spec's own underlying intent (spot
# an expected filing that didn't show up) is served correctly this way;
# its literal wording is not, given what 5 years of real data actually
# supports.
#
# A company's real slot calendar is NOT a generic "every 3 months"
# template - confirmed live Disco's own slots are asymmetric by period
# type (January = full-year 当期 guidance for the ending year, April = Q1
# 四半期, July = half-year 中間, October = Q3 四半期) - learned per company
# from its own real historical months, never assumed from a fixed
# template.
_MIN_YEARS_FOR_TRUSTED_SLOT_CALENDAR = 3
# Spec Section 6.3's own two-tier rule ("4 of last 5 years" / "3 of last 5
# years") assumes 5 years of slot history exists per slot - real data
# depth varies a lot per company (see this module's own docstring on the
# confirmed real per-company span), so the ratio is computed against
# however many years of real history exist for THIS slot, not hardcoded
# to a literal /5.
_SIGNAL_HIT_RATIO = 0.8  # "4 of the last 5 years" == 80%
_WEAK_HIT_RATIO = 0.6  # "3 of the last 5 years" == 60%
# A slot is only "overdue" once meaningfully past its usual date, not the
# instant the calendar date passes - real filing dates vary by a few days
# year to year (confirmed live: Disco's own October slot ranges from the
# 17th to the 29th) - this grace window absorbs that normal jitter so a
# slot isn't flagged the day after its most literal historical date.
_SLOT_OVERDUE_GRACE_DAYS = 30
#
# A "company has gone quiet entirely" check (Japan Signals spec Step 0's
# own delisting/take-private concern) was designed, built, and then
# REMOVED after live verification 2026-09-29 found it unreliable: inferred
# from "no jp_forecast filing in N days", it produced a real false
# positive for Tokyo Electron (no forecast-revision filing since 2024-08,
# but confirmed live on IRBANK's own site to have filed disclosures
# continuously through 2026-08 - the company simply had no need to revise
# its forecast in that window, which is normal, not silence) and the same
# risk was confirmed to apply to Screen Holdings. "No forecast revision"
# and "the company has gone quiet" are not the same fact, and jp_forecast
# alone cannot tell them apart - a reliable version of this check would
# need to cross-reference a company's OTHER disclosure types (e.g. EDINET
# filings, already fetched for other source_types) before concluding real
# silence, which is real future work, not something to ship as a
# confidently-labeled "prolonged_silence" result today. Deliberately not
# rebuilt as a heuristic here - a wrong "this company may be delisting"
# signal is worse than not having the feature at all.


def _is_first_time_announcement(article: dict[str, Any]) -> bool:
    """True if this jp_forecast article is a first-time forecast
    announcement (figures[0]["_kind"] == "revision", but with NO prior
    figure to compare against) - see this module's own comment above for
    why this, not a genuine revision, is what J3 tracks the recurrence of.
    """
    meta = article.get("metadata") or {}
    figures = meta.get("figures") or []
    if not figures or figures[0].get("_kind") != "revision":
        return False
    op_profit = _get_operating_profit([figures[0]])
    if not op_profit:
        return False
    return op_profit.get("revised") is not None and op_profit.get("previous") is None


def _build_slot_calendar(announcements: list[dict[str, Any]]) -> dict[int, list[datetime]]:
    """Group a company's own first-time announcements by calendar month,
    returning {month: [published_dt, ...]} - the real, learned slot
    calendar this company files under (see module comment on why this is
    never assumed from a fixed template).
    """
    by_month: dict[int, list[datetime]] = {}
    for a in announcements:
        pub_dt = _article_published_dt(a)
        if pub_dt:
            by_month.setdefault(pub_dt.month, []).append(pub_dt)
    return by_month


def classify_missing_revision(
    articles: list[dict[str, Any]],
    as_of: datetime | None = None,
) -> list[dict[str, Any]]:
    """Detect a missing expected forecast-announcement slot per Japan
    Signals spec Section 6.3 ("the expected thing not happening") - J3.

    See this module's own comment above for the real-data-driven
    redefinition (tracks first-time announcement slots, not literal
    revisions) and every edge case this function accounts for:

      - Not enough years of history: a slot needs
        _MIN_YEARS_FOR_TRUSTED_SLOT_CALENDAR real prior occurrences before
        "missing" means anything - a company with only 1-2 years of
        stored history has no established pattern to judge an absence
        against (same "not enough data yet" principle as every other
        habit-based rule in this module).
      - The slot's window hasn't passed yet: only a slot whose usual month
        (plus _SLOT_OVERDUE_GRACE_DAYS grace for normal date jitter) has
        already passed this fiscal year, with nothing filed, counts as
        missing - a slot due next month is not evidence of anything yet.
      - Normal date jitter: a company's real filing date for the "same"
        slot varies by some days year to year - the grace window absorbs
        this rather than flagging the day after the single earliest
        historical date.
      - Multiple filings in one slot: a real slot can have more than one
        announcement in the same month (confirmed live: Disco filed twice
        in July 2025) - _build_slot_calendar records every occurrence, not
        just one, so this doesn't distort the hit-count.

    ``as_of``: the "today" this function judges overdue-ness against -
    defaults to real UTC now. Exposed as a parameter (not hardcoded to
    datetime.now()) so this function is deterministically testable against
    a fixed point in time, not whatever day it happens to run.

    Japan Signals spec Section 6.3 rule (ratio-based - see
    _SIGNAL_HIT_RATIO/_WEAK_HIT_RATIO's own comment on why a ratio, not a
    literal "4 of 5"/"3 of 5", is used against real variable-depth data):
      SIGNAL - this slot's real historical hit ratio is >= 80% (the
               spec's own "4 of the last 5 years" threshold) and the slot
               is now overdue with nothing filed.
      WEAK   - hit ratio is >= 60% but < 80% (the spec's own "3 of the
               last 5 years", a weaker but still real pattern) and overdue.
      (no result) - hit ratio below 60% (spec: "no consistent pattern
               exists"), OR the slot isn't overdue yet, OR not enough
               years of history exist to compute a ratio at all. Per the
               spec's own words ("Handle this carefully... present it as
               an observation... never let it lead the summary"), an
               absence this function can't confidently characterize
               produces NO row, not a fabricated WEAK - unlike J1/J2,
               which always produce a WEAK fallback for a real filing
               that exists but is merely unremarkable, there is no real
               filing here to fall back to describing.

    Deliberately does NOT attempt to detect a company "going quiet"
    entirely (Japan Signals spec Step 0's own delisting/take-private
    concern) - see this module's own comment above _SLOT_OVERDUE_GRACE_DAYS
    for why that check was built, found unreliable against real data
    (a confirmed Tokyo Electron false positive), and removed rather than
    shipped as a confidently-labeled result.
    """
    as_of = as_of or datetime.now(timezone.utc)

    announcements_by_code: dict[str, list[dict[str, Any]]] = {}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_forecast":
            continue
        code = meta.get("code")
        if not code:
            continue
        if _is_first_time_announcement(a):
            announcements_by_code.setdefault(code, []).append(a)

    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0}

    for ticker in japan_ticker_universe():
        code = ticker["code"]
        company_name = ticker["company"]
        announcements = announcements_by_code.get(code, [])
        if not announcements:
            continue
        slot_calendar = _build_slot_calendar(announcements)
        for month, occurrences in slot_calendar.items():
            distinct_years = {dt.year for dt in occurrences}
            if len(distinct_years) < _MIN_YEARS_FOR_TRUSTED_SLOT_CALENDAR:
                continue

            # This slot's own real historical date-of-month, used to judge
            # whether THIS year's occurrence is now overdue - the LATEST
            # real historical day-of-month for this slot (not the
            # earliest), so normal jitter never makes an on-time filing
            # look overdue.
            latest_day_of_month = max(dt.day for dt in occurrences)

            # Has this slot's window (this calendar year's own occurrence
            # of `month`) already passed relative to as_of, with grace
            # days added for normal jitter? Capped at day 28 so this never
            # raises on a month with fewer days (e.g. a historical slot
            # that happened to fall on the 29th-31st of a longer month).
            slot_date_this_year = date(as_of.year, month, min(latest_day_of_month, 28))
            slot_date_with_grace = slot_date_this_year + timedelta(days=_SLOT_OVERDUE_GRACE_DAYS)
            if as_of.date() < slot_date_with_grace:
                continue  # not overdue yet this year

            filed_this_year = any(dt.year == as_of.year and dt.month == month for dt in occurrences)
            if filed_this_year:
                continue  # already filed on time this year, nothing missing

            hit_ratio = len(distinct_years) / max(as_of.year - min(distinct_years) + 1, 1)
            if hit_ratio >= _SIGNAL_HIT_RATIO:
                signal = "SIGNAL"
            elif hit_ratio >= _WEAK_HIT_RATIO:
                signal = "WEAK"
            else:
                continue  # no consistent pattern - spec's own "present nothing" case

            counts[signal] += 1
            # A month NAME, not its number. "in month 7" leaks the
            # stored integer onto the card; the view layer's own
            # caveat was fixed for this and this second site was
            # missed, so the number still reached a reader.
            month_name = date(2000, month, 1).strftime("%B")
            reason_text = (
                f"{company_name} normally files a forecast announcement in "
                f"{month_name} ({len(distinct_years)} of the last "
                f"{as_of.year - min(distinct_years) + 1} years) and has not done so "
                f"this year - the usual window has now passed."
            )
            results.append({
                # Real fix 2026-09-30 (same reasoning as classify_stale_
                # revision_pattern's own result-shape comment): this is
                # NOT a timeless absence - "the usual window has now
                # passed with nothing filed" is only true AS OF as_of,
                # not forever, so a synthetic dict with a real published
                # timestamp (as_of itself) is passed instead of None. This
                # is what makes a J3 row findable by GET /japan-signals/
                # results and the date-windowed results query's own
                # published_from/published_to day-window query (see
                # models.jobs.list_all_results, which filters on the
                # literal `published` column - a NULL published can never
                # satisfy a >=/< range, confirmed live this was a real,
                # previously-documented gap). title is a real descriptive
                # label, not fabricated content; there is no url (no real
                # fetched page backs an absence), so url stays unset, same
                # "field absent means not applicable" convention this
                # module already uses elsewhere.
                "article": {
                    "title": f"{company_name}: expected forecast announcement overdue",
                    "published": as_of.isoformat(),
                },
                "result": {
                    "signal": _JAPAN_SIGNAL_MAP[signal],
                    "signal_score": None,
                    "source_id": f"japan-missing-revision://{code}/{as_of.year}-{month:02d}",
                    "reason": reason_text,
                    "metadata": {
                        "code": code,
                        "company": company_name,
                        "native_name": ticker.get("native_name"),
                        "source_category": "jp_missing_revision",
                        "signal_reason_code": f"missing_slot_month_{month}",
                        "expected_month": month,
                        "historical_years_with_hit": sorted(distinct_years),
                        "hit_ratio": round(hit_ratio, 2),
                    },
                },
            })

    if any(counts.values()):
        logger.info(
            "[JAPAN_MISSING_REVISION] signal=%d weak=%d",
            counts["SIGNAL"], counts["WEAK"],
        )
    return results


# Japan Signals spec Section 10.3's own WATCHING section ("companies
# diverging from their own pattern that have not announced anything") -
# this was a real, documented gap (see the date-window note in
# former top-docstring note) until this function: no existing classify_*
# function scanned a company's CURRENT standing against its own habit
# with no new filing to trigger it. classify_missing_revision (J3) is the
# closest real precedent for scanning-without-a-filing, but it answers a
# different question ("did an EXPECTED FILING fail to arrive in its usual
# calendar slot") - this function answers "has more real time passed
# since this company's last genuine revision than its own established
# cadence would predict", using only data already computed and stored:
# the real dates of past genuine revisions (already fetched/stored by
# news-retrieval, filtered the same way compute_forecast_habit's own
# caller filters them) and revisions_per_year from japan_company_habits
# (already computed by refresh-japan-habits). No new data source, no new
# LLM call - pure date arithmetic against real stored history, same
# "arithmetic, not judgment" design every other rule in this module uses.
#
# Deliberately NOT the same thing as the removed "company has gone quiet
# entirely" check documented above (see the comment above
# _SLOT_OVERDUE_GRACE_DAYS) - that check inferred "may be delisting" from
# silence alone and produced a confirmed live false positive (Tokyo
# Electron, Screen Holdings: no forecast revision in a long stretch
# because nothing warranted one, not because the company had gone quiet).
# This function makes a narrower, more defensible claim: not "something
# is wrong", only "this specific company's real elapsed silence is now
# longer, relative to ITS OWN established cadence, than is typical for
# it" - an observation about pace, stated as a fact with the real numbers
# attached, never a prediction of what happens next (matching the spec's
# own explicit instruction: "Do not predict whether a revision will
# come"). For exactly that reason this function only ever emits WEAK
# ("weak_signal"), never SIGNAL - "overdue relative to habit" is
# real and worth surfacing, but it is not itself a confirmed event the
# way an actual filed revision is.
_STALE_SIGNAL_RATIO = 2.0  # elapsed / typical_gap_days at or above this: weak_signal
# WATCHING reports a company diverging from its own pattern, which
# presumes the pattern still describes the company. Past some age the
# habit's own evidence is too old to carry that claim, and the honest
# reading becomes "this baseline is out of date" rather than "this
# company is overdue".
#
# The test is the age of the habit's most recent observation, not how
# many multiples of the cadence have elapsed. A ratio cannot separate the
# two real situations that produce a large value, because both a densely
# observed company that is genuinely late and a company whose cadence was
# measured from a brief cluster years ago present as the same number -
# confirmed against the stored universe, where Tokyo Electron (14
# revisions, most recent 2024-08) and Screen Holdings (3 revisions, most
# recent 2021-10) sit at a comparable 4.3x and 4.9x while meaning
# entirely different things.
#
# 3 years is the boundary because _MIN_FISCAL_YEAR_SPAN_FOR_TRUSTED_HABIT
# already requires a habit to span at least 3 fiscal years before it is
# trusted at all. A company silent for longer than the minimum span used
# to establish a pattern has now been silent longer than the evidence the
# pattern rests on, so the cadence no longer describes current behaviour.
# Using the same span for both keeps one definition of how much history
# makes a pattern real, rather than introducing a second, unrelated one.
_HABIT_EVIDENCE_STALE_DAYS = 365 * _MIN_FISCAL_YEAR_SPAN_FOR_TRUSTED_HABIT
# Below _STALE_SIGNAL_RATIO's own threshold, normal per-company jitter
# (a company whose typical cadence is "twice a year" does not revise
# every exactly-182-days) must not itself be misread as drift - confirmed
# live reasoning from compute_forecast_habit's own docstring: even a
# reliable habit's real spacing varies year to year. 2x the company's own
# typical gap is a deliberately conservative bar - meaningfully overdue by
# the company's own measure, not merely "a bit longer than usual".


def classify_stale_revision_pattern(
    articles: list[dict[str, Any]],
    stored_habits: dict[str, dict[str, Any]] | None = None,
    as_of: datetime | None = None,
) -> list[dict[str, Any]]:
    """Japan Signals spec Section 10.3's WATCHING section - detect a
    tracked company whose real elapsed time since its last genuine
    forecast revision now exceeds its own established cadence, with
    nothing new filed to explain the gap. See this module's own comment
    above for the full design rationale and why this is answers a
    different question from classify_missing_revision (J3).

    ``articles``: the same full pooled jp_forecast batch every other
    forecast-revision rule in this module reads from - this function
    finds each tracked company's own most recent genuine revision date
    directly from real article history, not from a stored "last seen"
    column (none exists - see japan_company_habits.py's own schema,
    which stores only aggregate stats, never a last-revision date). This
    is exactly why the caller (controllers.run.refresh_japan_habits) MUST
    pass the same full wide-history pool it already assembles for
    compute_all_japan_habits, not a narrower window: passing a narrower
    ``articles`` window than a company's TRUE last real revision would
    make that revision invisible here, either wrongly treating a company
    with a real recent revision (just outside the passed window) as
    having none at all (silently skipped, see below) or, worse, finding
    an OLDER real revision still inside the window and wrongly reporting
    it as the company's most recent one - overstating elapsed_days. Not
    a risk today (refresh-japan-habits's own default from_date is the
    full 10-year backfill window), but a real constraint on how this
    function may ever be called, stated here rather than left implicit.
    ``stored_habits``: {code: habit_dict} from japan_company_habits (see
    classify_forecast_revision's own docstring) - this function reads
    each company's own real revisions_per_year and is_trusted.
    ``as_of``: the real "today" this function judges elapsed time
    against - defaults to real UTC now, exposed as a parameter for the
    same deterministic-testing reason classify_missing_revision's own
    ``as_of`` parameter is.

    Only ever produces weak_signal rows (see _STALE_SIGNAL_RATIO's own
    comment on why never a full signal), and only for a company whose
    habit is_trusted (same standard J1 itself requires before comparing
    anything against a company's habit - an untrusted, sparse habit's own
    revisions_per_year is not a reliable cadence to measure lateness
    against). A company with NO real revision anywhere in ``articles`` at
    all is skipped entirely (nothing to measure elapsed time from) rather
    than treated as maximally overdue - a real gap in what news-retrieval
    has fetched/stored is a data-completeness question, not evidence this
    company itself is behaving unusually.

    result["article"] is None for every row here, same as J3 - there is
    no real article behind an absence, described the same way J3's own
    result shape already establishes for that case.
    """
    as_of = as_of or datetime.now(timezone.utc)
    stored_habits = stored_habits or {}

    last_revision_by_code: dict[str, datetime] = {}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_forecast":
            continue
        code = meta.get("code")
        # _MARGIN_BASED_CODES (Renesas) is excluded here explicitly, not
        # just left to fall out of the revisions_per_year check below -
        # compute_margin_revision_habit's own docstring confirms that
        # company's stored habit row always has revisions_per_year=NULL
        # (a genuinely different quantity, typical_gap_pts, is stored
        # instead), so it would be filtered out downstream regardless.
        # Stated explicitly anyway, matching this module's own convention
        # of naming a real gap rather than relying on it falling out of
        # an unrelated check by coincidence: WATCHING is simply not built
        # for a margin-based company today (a real gap - a margin-based
        # "cadence" would need its own definition of what counts as
        # elapsed-time overdue, not built or verified here).
        if not code or code in _MARGIN_BASED_CODES:
            continue
        if not _is_genuine_revision(a):
            continue
        pub_dt = _article_published_dt(a)
        if not pub_dt:
            continue
        existing = last_revision_by_code.get(code)
        if existing is None or pub_dt > existing:
            last_revision_by_code[code] = pub_dt

    results: list[dict[str, Any]] = []
    for ticker in japan_ticker_universe():
        code = ticker["code"]
        habit = stored_habits.get(code)
        if not habit or not habit.get("is_trusted"):
            continue
        revisions_per_year = habit.get("revisions_per_year")
        if not revisions_per_year:
            continue
        last_revision = last_revision_by_code.get(code)
        if not last_revision:
            continue

        typical_gap_days = 365.0 / revisions_per_year
        elapsed_days = (as_of - last_revision).total_seconds() / 86400.0
        ratio = elapsed_days / typical_gap_days
        if ratio < _STALE_SIGNAL_RATIO:
            continue
        if elapsed_days >= _HABIT_EVIDENCE_STALE_DAYS:
            # The habit's own most recent observation is older than the
            # span required to establish a habit in the first place (see
            # _HABIT_EVIDENCE_STALE_DAYS), so the stored cadence no longer
            # describes how this company behaves and there is nothing
            # meaningful to report it as diverging from. The company
            # returns to WATCHING once a fresh revision re-establishes a
            # real cadence.
            logger.info(
                "[JAPAN_WATCHING] %s (%s) habit evidence stale - last revision %d days"
                " ago, older than the %d-day habit-establishing span; dropped from"
                " WATCHING",
                ticker["company"], code, round(elapsed_days), _HABIT_EVIDENCE_STALE_DAYS,
            )
            continue

        company_name = ticker["company"]
        # "its own habit is raises about twice a year" reads as the
        # module's own vocabulary rather than a sentence about the
        # company. The cadence is the point; the habit phrase just
        # names how often.
        reason_text = (
            f"{company_name} last filed a forecast revision "
            f"{round(elapsed_days)} days ago, against a usual gap of about "
            f"{round(typical_gap_days)} days "
            f"({_format_company_revision_habit(habit)})."
        )
        results.append({
            # Unlike J3 (a real absence with no meaningful date at all), a
            # WATCHING reading genuinely IS "as of" a specific real moment
            # - the elapsed-time comparison above is only true measured at
            # as_of, not timeless the way "this slot did not occur" is.
            # So, unlike J3's own "article": None, this passes a small
            # synthetic dict with a REAL published timestamp (as_of
            # itself, not a fabricated one) - this is what lets
            # the results query's own published_from/
            # published_to day-window query actually find these rows (see
            # models.jobs.list_all_results, which filters on the literal
            # `published` column - a NULL published, as J3 has, can never
            # satisfy a >=/< range). title is a real descriptive label,
            # not fabricated content - there is no url to point to (no
            # real fetched page backs this row), so url stays unset,
            # same "field absent means not applicable" convention
            # insert_japan_signal_classification's own docstring
            # describes for J3.
            "article": {
                "title": f"{company_name}: forecast revision overdue vs. own habit",
                "published": as_of.isoformat(),
            },
            "result": {
                "signal": "weak_signal",
                "signal_score": None,
                # One row per company, not per company per day. A
                # WATCHING reading is a STANDING STATE ("Disco is
                # overdue"), not a dated event the way every other
                # Japan row is - those key on a filing's own immutable
                # doc id, while this used to key on "today". Dating it
                # meant the row stopped matching the current date the
                # moment midnight passed, emptying the Watching tab
                # until the next monthly refresh, and accumulating a
                # fresh row per company per run for the same fact.
                # Keyed on the company alone, the row persists and is
                # updated in place - see insert_japan_signal_
                # classification's jp_watching upsert, which this
                # stable id depends on to keep days_since_last_revision
                # current rather than frozen at first insert.
                "source_id": f"japan-stale-revision://{code}",
                "reason": reason_text,
                "metadata": {
                    "code": code,
                    "company": company_name,
                    "native_name": ticker.get("native_name"),
                    "source_category": "jp_watching",
                    "signal_reason_code": "stale_revision_pattern",
                    "days_since_last_revision": round(elapsed_days),
                    "typical_days_between_revisions": round(typical_gap_days),
                    "stale_ratio": round(ratio, 2),
                    "company_revision_habit": _format_company_revision_habit(habit),
                    "last_revision_published": last_revision.isoformat(),
                },
            },
        })

    if results:
        logger.info("[JAPAN_STALE_REVISION] weak_signal=%d", len(results))
    return results


# Japan Signals spec Section 5/6.4 - J4, industry equipment sales (SEAJ).
#
# The one signal type in this module with no per-company habit at all -
# a single national/industry-wide monthly reading, judged against ITS
# OWN trailing 12-month average and spread (standard deviation), same
# statistical shape as Korea's own classify_export_surprise (this
# codebase's closest real precedent - confirmed live 2026-09-29 the
# spec's own thresholds match Korea's exactly: >2 spreads for Signal, 1-2
# for Weak, <=1 for Noise).
#
# Confirmed live against the real, now-complete 59-month SEAJ backfill
# (Oct 2021 - Aug 2026, see news-retrieval's fetch_seaj_billings_backfill):
# this data spans a genuine full semiconductor equipment cycle - a real
# boom (Oct 2021-Feb 2022, 50-70% YoY), a real downturn with several
# months of negative YoY (mid-to-late 2023), and a real recovery back to
# 47.4% YoY by Aug 2026. August 2026's own real reading sits 3.15 spreads
# above its own trailing-12-month baseline - a genuine, live-verified
# Signal trigger, not a synthetic test case.
_INDUSTRY_SIGNAL_SPREADS = 2.0
_INDUSTRY_WEAK_SPREADS = 1.0
# A trailing 12-month baseline is exactly what "12-month average" means
# (Japan Signals spec's own literal wording, Section 6.4) - not a minimum-
# sample floor to grow into like J1/J2/J3's own habit floors, but a fixed
# window size: a month needs exactly its own 12 real predecessors to be
# judged at all, no fewer and no more (using fewer would silently produce
# a smaller, less representative "12-month" average; using more would
# stop being what the spec itself defines).
_INDUSTRY_BASELINE_MONTHS = 12


def classify_industry_equipment_sales(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Classify jp_industry (SEAJ billings) articles per Japan Signals
    spec Section 6.4 - J4, "an industry-wide monthly reading on how much
    chipmaking equipment Japanese makers are actually selling."

    Only touches articles whose metadata.source_category == "jp_industry"
    (SEAJ's own monthly billings articles - see news-retrieval's
    _fetch_seaj_billings/fetch_seaj_billings_backfill). Sorts by month and
    judges each month (from the 13th real month onward - see
    _INDUSTRY_BASELINE_MONTHS's own comment) against the mean/stdev of its
    own real trailing 12 months, not a company habit (this signal type has
    no per-company concept at all - it is one national reading per month).

    Japan Signals spec Section 6.4 rule (exact boundaries confirmed
    against the spec's own literal wording - "more than"/"between"/
    "within" - and Korea's own classify_export_surprise, the closest real
    precedent in this codebase, which implements the identical thresholds
    the same way):
      SIGNAL - the reading sits MORE than 2 spreads from its trailing
               12-month average (either direction), OR the YoY reading
               itself is negative (regardless of spread distance - the
               spec's own explicit "shrinking exports are rare enough in
               this cycle to matter on their own" reasoning, mirrored
               exactly from Korea's own identical rule for its analogous
               signal type).
      WEAK   - between 1 and 2 spreads away.
      NOISE  - within 1 spread (the series behaving normally).

    A month with fewer than _INDUSTRY_BASELINE_MONTHS real predecessors
    produces NO result at all (not NOISE, not WEAK) - same "not enough
    real data yet" principle as every habit-based rule in this module,
    and the literal reason no such gap exists in the real, now-complete
    59-month SEAJ dataset (every month from the 13th real month onward has
    a genuine trailing-12 baseline to judge against).
    """
    seaj_articles_raw = [
        a for a in articles
        if (a.get("metadata") or {}).get("source_category") == "jp_industry"
    ]

    # The SAME real calendar month can legitimately be stored under TWO
    # different articles over time - news-retrieval's own live SEAJ
    # fetcher reports a month "prelim" the first time, then "final" once
    # SEAJ settles it (a genuine, deliberate revision, not a duplicate -
    # see news-retrieval's own _SEAJ_MONTH_ROW_RE comment). Left
    # unresolved, this module would see the same month twice - both
    # skewing this function's own classified output (one calendar month
    # counted as two results) and corrupting every LATER month's trailing-
    # 12-month baseline (the same real month's YoY value counted twice in
    # the average/spread). Deduped here by period, preferring "final" over
    # "prelim" for the same month (the more authoritative, settled
    # reading) - not yet triggered by the current real dataset (confirmed
    # live 2026-09-29: no duplicate month exists in the stored 59-month
    # SEAJ history), but a real, inevitable future case once the live
    # fetcher's own prelim-then-final cycle actually completes for a
    # month it originally reported as prelim.
    by_period: dict[str, dict[str, Any]] = {}
    for a in seaj_articles_raw:
        period = (a.get("metadata") or {}).get("period")
        if not period:
            continue
        existing = by_period.get(period)
        if existing is None or (a["metadata"].get("qualifier") == "final" and existing["metadata"].get("qualifier") != "final"):
            by_period[period] = a
    seaj_articles = list(by_period.values())
    seaj_articles.sort(key=lambda a: _article_published_dt(a) or datetime.min.replace(tzinfo=timezone.utc))

    results: list[dict[str, Any]] = []
    counts = {"SIGNAL": 0, "WEAK": 0, "NOISE": 0}

    for idx, a in enumerate(seaj_articles):
        if idx < _INDUSTRY_BASELINE_MONTHS:
            continue  # not enough real trailing history yet for this month

        meta = a.get("metadata") or {}
        yoy_pct = meta.get("yoy_pct")
        if yoy_pct is None:
            continue

        trailing = [
            seaj_articles[j]["metadata"].get("yoy_pct")
            for j in range(idx - _INDUSTRY_BASELINE_MONTHS, idx)
        ]
        if any(v is None for v in trailing):
            continue

        average = statistics.mean(trailing)
        spread = statistics.stdev(trailing)
        period = meta.get("period", "This month")

        if spread == 0:
            # Every one of the trailing 12 real readings was identical -
            # not yet seen live (real SEAJ data always shows some real
            # month-to-month variation), but a real division-by-zero risk
            # if it ever happened - skipped rather than crashing or
            # fabricating a spreads-away value that has no meaning when
            # the baseline itself has zero variance.
            continue

        spreads_away = (yoy_pct - average) / spread

        # A contracting industry is a sustained condition, not a single
        # reading below zero. Requiring an adjacent negative month is
        # what separates a real downturn from a lone soft month or a
        # figure that is negative only by rounding - the stored series
        # carries both kinds, with a genuine multi-month contraction
        # (-16.7%, -10.5% consecutively) alongside isolated readings of
        # -0.1% and -4.5% that a bare "below zero" test ranks equally.
        # Adjacency is the test rather than a magnitude floor because the
        # real series offers no natural break to put a floor at, and
        # persistence is what the word "contraction" actually claims.
        prev_yoy = (
            seaj_articles[idx - 1]["metadata"].get("yoy_pct") if idx >= 1 else None
        )
        next_yoy = (
            seaj_articles[idx + 1]["metadata"].get("yoy_pct")
            if idx + 1 < len(seaj_articles) else None
        )
        sustained_contraction = yoy_pct < 0 and (
            (prev_yoy is not None and prev_yoy < 0)
            or (next_yoy is not None and next_yoy < 0)
        )

        if sustained_contraction or abs(spreads_away) > _INDUSTRY_SIGNAL_SPREADS:
            signal = "SIGNAL"
            reason_code = (
                "sustained_negative_yoy" if sustained_contraction
                else f"spreads_away_{round(spreads_away, 2)}"
            )
            if sustained_contraction:
                reason_text = (
                    f"{period}'s Japan semiconductor equipment billings fell {abs(yoy_pct):.1f}% "
                    f"year-on-year, alongside another month of contraction - a sustained "
                    f"decline, which counts as a signal regardless of how far it sits from "
                    f"the recent average."
                )
            else:
                # A reading far below a high trailing average is a sharp
                # deceleration in growth, which is a real change in
                # trajectory, but it is not a decline while the reading
                # itself is still positive - said plainly so the line
                # cannot be read as the industry shrinking.
                if spreads_away > 0:
                    movement = "an acceleration"
                    qualifier = ""
                elif yoy_pct > 0:
                    movement = "a sharp deceleration"
                    qualifier = ", with growth still positive"
                else:
                    movement = "a sharp drop"
                    qualifier = ""
                # "spreads" is the statistic's own vocabulary, not a
                # reader's - the sentence says how far from normal this
                # reading sits, and the figure stays in evidence for
                # anyone who wants it.
                reason_text = (
                    f"{period}'s Japan semiconductor equipment billings ran {yoy_pct:.1f}% "
                    f"year-on-year against a recent average of {average:.1f}% - "
                    f"{movement}, well {'above' if spreads_away > 0 else 'below'} this "
                    f"industry's usual range{qualifier}."
                )
        elif abs(spreads_away) > _INDUSTRY_WEAK_SPREADS:
            signal = "WEAK"
            reason_code = f"spreads_away_{round(spreads_away, 2)}"
            reason_text = (
                f"{period}'s Japan semiconductor equipment billings ran {yoy_pct:.1f}% "
                f"year-on-year against a recent average of {average:.1f}% - somewhat "
                f"{'above' if spreads_away > 0 else 'below'} this industry's usual range."
            )
        else:
            signal = "NOISE"
            reason_code = "within_normal_range"
            reason_text = (
                f"{period}'s Japan semiconductor equipment billings ({yoy_pct:.1f}% YoY) are within "
                f"the usual range of the trailing 12-month average ({average:.1f}%)."
            )

        counts[signal] += 1
        meta["industry_baseline_avg_yoy_pct"] = round(average, 1)
        meta["industry_baseline_spread"] = round(spread, 2)
        meta["industry_spreads_away"] = round(spreads_away, 2)
        meta["signal_reason_code"] = reason_code
        a["metadata"] = meta

        results.append({
            "article": a,
            "result": {
                "signal": _JAPAN_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": a.get("url"),
                "reason": reason_text,
                "metadata": meta,
            },
        })

    if any(counts.values()):
        logger.info(
            "[JAPAN_INDUSTRY_EQUIPMENT_SALES] signal=%d weak=%d noise=%d",
            counts["SIGNAL"], counts["WEAK"], counts["NOISE"],
        )
    return results


# Japan Signals spec Section 6.5's own explicit numeric rules (shared by
# J5 "capacity commitment" and J6 "ownership and capital policy" - one
# rule block covers both signal types).
_CAPACITY_INVESTMENT_PCT_OF_ASSETS_THRESHOLD = 10.0  # "10 percent or more of total assets"
_OWNERSHIP_HOLDING_THRESHOLD_PCT = 5.0  # "a holder crosses five percent"
# What separates a real 5%-crossing event from routine filing traffic is
# the report TYPE, not the size of the stake. Two labels in EDINET's own
# docDescription carry that distinction, and both were confirmed against
# the real filings stored for this universe:
#
#   大量保有報告書 - an INITIAL report: a holder newly crossing 5%, which
#       is the event the spec's own rule describes.
#   変更報告書     - a CHANGE report: an existing holder adjusting a
#       position already disclosed. Filed on any 1%+ move, so it
#       describes a position that crossed 5% at some earlier point,
#       possibly years earlier.
#
#   特例対象株券等 - the relaxed periodic-filing regime available to
#       passive institutional holders (asset managers, brokers, trust
#       banks). These filings report custody and index positions on a
#       schedule rather than a control decision taken on a date.
#
# Stake size does not substitute for either: the largest real holding in
# the stored set is a legacy position reported on a change report, while
# a genuine new crossing sits near the statutory floor. A threshold on
# the percentage promotes the former and demotes the latter.
#
# Of the two, the FILING REGIME is the stronger signal, because it says
# something about the holder's intent that neither the report type nor
# the stake does. The passive regime is only available to a holder that
# has declared it is not seeking control - so an active-basis filing is
# an ordinary company or fund taking a deliberate position, while a
# passive one is an index or custody position moving with its mandate.
# Report type then separates a new position from a change to an existing
# one within each of those.
_OWNERSHIP_INITIAL_REPORT_MARKER = "大量保有報告書"
_OWNERSHIP_PASSIVE_REGIME_MARKER = "特例対象株券等"


def _format_holding_pct(value: float) -> str:
    """A holding percentage at the precision the filing states it.

    EDINET reports the ratio to four decimal places (0.1242), so the
    percentage is exactly two (12.42%) and both are stored. Rendering
    one decimal rounded a filed figure into a different number -
    14.06% became 14.1% - and could erase a real move outright: a
    12.40% to 12.42% increase read "raised its stake to 12.4%, from
    12.4%".

    A trailing zero decimal is kept rather than trimmed. These are two
    figures a reader compares digit by digit, and "12.4%" beside
    "12.42%" invites the question of whether the first is less precise
    or simply shorter.
    """
    return f"{value:.2f}%"
_BUYBACK_PCT_OF_SHARES_THRESHOLD = 5.0  # "a buyback of five percent or more of shares outstanding"
_CO_OCCURRENCE_WINDOW_DAYS = 14  # "another signal from the same company landed within fourteen days"

# Real MONOist article bodies (jp_capex) carry the investment yen figure
# as "XXX億円" (X hundred-million yen) - confirmed live 2026-09-29 across
# 74 real articles this is the only real format used, never a bare 万円
# or 兆円 figure for a single investment announcement at this company
# scale. Matches the LARGEST such figure in the body (a real announcement
# sometimes mentions a smaller PRIOR investment for context alongside the
# new one - see Shin-Etsu's 24億円/650億円 double-figure case found live -
# and the new investment being announced is consistently the larger of
# the two, never the smaller).
_CAPEX_YEN_OKU_RE = re.compile(r"([\d,]+)億円")


def _extract_capex_investment_jpy(article: dict[str, Any]) -> int | None:
    """Extract the largest 億円 (100-million-yen) investment figure found
    in a jp_capex article's title+body, converted to plain yen. Returns
    None if no such figure appears - confirmed live only ~62% of real
    MONOist articles carry one at all (the rest are announcements with no
    stated yen amount, e.g. capacity-doubling or factory-closure articles
    - see news-retrieval's own CLAUDE.md for the exact recovery rate).
    """
    combined = f"{article.get('title') or ''} {article.get('body') or ''}"
    matches = _CAPEX_YEN_OKU_RE.findall(combined)
    if not matches:
        return None
    oku_values = [int(m.replace(",", "")) for m in matches]
    return max(oku_values) * 100_000_000


def _company_total_assets_jpy(code: str, stored_company_reference: dict[str, dict[str, Any]] | None) -> int | None:
    """Look up a company's latest known total assets (plain yen, not JPY
    millions) from the stored irbank_company_reference habit-style cache.
    Returns None if the company has no stored reference row yet (e.g. a
    brand new universe addition before the next refresh) - same fail-open
    "not enough real data yet" contract as every other habit lookup in
    this module, rather than fabricating a denominator.
    """
    if not stored_company_reference:
        return None
    ref = stored_company_reference.get(code)
    if not ref or ref.get("total_assets_jpy_millions") is None:
        return None
    return int(ref["total_assets_jpy_millions"]) * 1_000_000


def _company_shares_outstanding(code: str, stored_company_reference: dict[str, dict[str, Any]] | None) -> int | None:
    """Look up a company's latest known shares-outstanding estimate from
    the same stored reference cache as _company_total_assets_jpy. Returns
    None if unavailable - see news-retrieval's own CLAUDE.md for why this
    field is independently nullable even when total assets succeeded
    (derived from a market-cap page fetch that can fail separately).
    """
    if not stored_company_reference:
        return None
    ref = stored_company_reference.get(code)
    if not ref or ref.get("shares_outstanding") is None:
        return None
    return int(ref["shares_outstanding"])


def _parse_buyback_program_limit_shares(program_limits: str | None) -> int | None:
    """Parse a jp_buyback program_limits string like "上限1800万株、1500億円"
    (upper limit 18 million shares, JPY150bn) into a plain share count.
    Confirmed live real values use 万株 (10-thousands of shares) or 億株
    (100-millions of shares) depending on program size - both handled.
    Returns None if no share-count figure is present at all (not observed
    live, but every real program_limits string carries one).
    """
    if not program_limits:
        return None
    oku_match = re.search(r"上限([\d,]+)億株", program_limits)
    man_match = re.search(r"上限([\d,]+)万株", program_limits)
    if oku_match:
        return int(oku_match.group(1).replace(",", "")) * 100_000_000
    if man_match:
        return int(man_match.group(1).replace(",", "")) * 10_000
    return None


def classify_capacity_and_ownership(
    articles: list[dict[str, Any]],
    stored_company_reference: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Classify jp_capex (J5, capacity commitment), jp_ownership and
    jp_buyback (J6, ownership and capital policy) articles per Japan
    Signals spec Section 6.5 - ONE shared rule block for both signal
    types, per the spec's own table structure (unlike every other signal
    type, which gets its own dedicated rule table).

    ``stored_company_reference``: {code: {total_assets_jpy_millions,
    shares_outstanding, ...}} - see models.japan_company_reference (new,
    mirrors japan_company_habits/japan_progress_habits's own shape) and
    news-retrieval's irbank_company_reference source. Like every other
    stored_* parameter in this module, loading/supplying this is
    controllers/run.py's responsibility; this module has no DB access.

    Real-data findings that shaped this function (confirmed live
    2026-09-29 against news-retrieval's test DB, see PR history for the
    exact figures):
      - The spec's own literal "investment >= 10% of total assets" clause
        NEVER fires on real data: all 15 real jp_capex articles carrying
        an extractable investment figure, checked against each company's
        real total assets, came in well under 10% (largest: Shin-Etsu's
        real ~JPY530bn US expansion at 9.2%). This is a genuine finding
        about the market, not a bug - large Japanese industrials'
        balance sheets are simply big relative to any single plant
        announcement - so the clause is kept exactly as spec'd (never
        loosened to force it to fire) and the 14-day co-occurrence clause
        is expected to be the one that actually promotes real J5 items to
        Signal in practice, same as J1's currency-only-Noise rule and
        J3's missing-slot rule are kept exactly as spec'd despite rarely
        firing on real data.
      - jp_ownership's own docTypeCode 350 ("large shareholding report")
        is BY LEGAL DEFINITION only filed once a holder's stake crosses
        5% - every real row already IS a 5%-crossing event, so no
        before/after delta computation is needed or possible (there is
        no "below 5%" row to compare against - the filing simply does not
        exist below that threshold). Every real jp_ownership article is
        therefore SIGNAL under this rule, unconditionally.
      - jp_buyback is a MONTHLY RUNNING-STATUS feed, not a one-time
        announcement (confirmed live: Advantest has 28 real status rows
        across 4 distinct board-resolution programs since 2020, e.g.
        "+124万株" one month, "+84万株" the next, against the SAME
        program's fixed cap). The spec's own wording is "a buyback... is
        ANNOUNCED" - matched here to the FIRST status row seen for a
        given program_resolution (a real, distinct board-resolution date
        embedded in that field), not to every monthly progress update,
        since only the announcement itself is the trigger, not routine
        progress reporting against an already-known cap.
    """
    capex_articles = [
        a for a in articles if (a.get("metadata") or {}).get("source_category") == "jp_capex"
    ]
    ownership_articles = [
        a for a in articles if (a.get("metadata") or {}).get("source_category") == "jp_ownership"
    ]
    buyback_articles_raw = [
        a for a in articles if (a.get("metadata") or {}).get("source_category") == "jp_buyback"
    ]

    # First real status row per (code, program_resolution) is the
    # announcement; every later row for the same program is routine
    # progress reporting, not a fresh trigger - see docstring.
    buyback_articles_raw.sort(key=lambda a: _article_published_dt(a) or datetime.min.replace(tzinfo=timezone.utc))
    seen_programs: set[tuple[str, str]] = set()
    buyback_announcements: list[dict[str, Any]] = []
    for a in buyback_articles_raw:
        meta = a.get("metadata") or {}
        key = (meta.get("code"), meta.get("program_resolution"))
        if key in seen_programs or not meta.get("program_resolution"):
            continue
        seen_programs.add(key)
        buyback_announcements.append(a)

    # Build one combined, time-ordered list of every company-level event
    # across J1 (jp_forecast revisions only, not results-vs-forecast -
    # the spec's own "another SIGNAL" wording means a genuine event, not
    # every results filing which is always at least Weak per J2's own
    # rule), J4 has no per-company concept so is excluded, and this
    # function's own J5/J6 candidates - so the 14-day co-occurrence
    # clause can check across signal types, not just within J5/J6 itself.
    other_signal_events: list[tuple[str, datetime]] = []
    for a in articles:
        meta = a.get("metadata") or {}
        category = meta.get("source_category")
        code = meta.get("code")
        pub_dt = _article_published_dt(a)
        if not code or not pub_dt:
            continue
        if category == "jp_forecast" and _is_genuine_revision(a):
            other_signal_events.append((code, pub_dt))

    def _has_co_occurring_signal(code: str, pub_dt: datetime) -> bool:
        window = timedelta(days=_CO_OCCURRENCE_WINDOW_DAYS)
        return any(
            other_code == code and other_dt != pub_dt and abs(other_dt - pub_dt) <= window
            for other_code, other_dt in other_signal_events
        )

    results: list[dict[str, Any]] = []

    for a in capex_articles:
        meta = a.get("metadata") or {}
        code = meta.get("code")
        company = meta.get("company") or code
        pub_dt = _article_published_dt(a)
        investment_jpy = _extract_capex_investment_jpy(a)
        total_assets_jpy = _company_total_assets_jpy(code, stored_company_reference)

        pct_of_assets = None
        if investment_jpy is not None and total_assets_jpy:
            pct_of_assets = investment_jpy / total_assets_jpy * 100.0

        co_occurring = _has_co_occurring_signal(code, pub_dt) if pub_dt else False

        if (pct_of_assets is not None and pct_of_assets >= _CAPACITY_INVESTMENT_PCT_OF_ASSETS_THRESHOLD) or co_occurring:
            signal = "SIGNAL"
            if co_occurring and (pct_of_assets is None or pct_of_assets < _CAPACITY_INVESTMENT_PCT_OF_ASSETS_THRESHOLD):
                reason_code = "co_occurring_signal"
                reason_text = (
                    f"{company} announced a capacity investment, and another signal from this "
                    f"company landed within {_CO_OCCURRENCE_WINDOW_DAYS} days - two related "
                    f"items from the same company close together outrank either one alone."
                )
            else:
                reason_code = "large_vs_total_assets"
                reason_text = (
                    f"{company}'s announced investment is {pct_of_assets:.1f}% of its total "
                    f"assets - at or above the {_CAPACITY_INVESTMENT_PCT_OF_ASSETS_THRESHOLD:.0f}% "
                    f"threshold for a capacity commitment large enough to matter on its own."
                )
        elif investment_jpy is not None:
            signal = "WEAK"
            reason_code = "capacity_investment"
            if pct_of_assets is not None:
                reason_text = (
                    f"{company} announced a capacity investment of approximately "
                    f"{investment_jpy / 100_000_000:,.0f} oku yen ({pct_of_assets:.1f}% of total "
                    f"assets)."
                )
            else:
                reason_text = (
                    f"{company} announced a capacity investment of approximately "
                    f"{investment_jpy / 100_000_000:,.0f} oku yen - this company's total assets "
                    f"are not yet known, so it cannot be measured against the "
                    f"{_CAPACITY_INVESTMENT_PCT_OF_ASSETS_THRESHOLD:.0f}%-of-assets threshold."
                )
        else:
            signal = "WEAK"
            reason_code = "capacity_news_no_figure"
            reason_text = (
                f"{company} published capacity/investment news with no stated yen figure - real "
                f"information; the announcement states no yen figure."
            )

        meta["capex_investment_jpy"] = investment_jpy
        meta["capex_pct_of_total_assets"] = round(pct_of_assets, 2) if pct_of_assets is not None else None
        meta["signal_reason_code"] = reason_code
        meta["native_name"] = (company_for(code) or {}).get("native_name")
        a["metadata"] = meta

        results.append({
            "article": a,
            "result": {
                "signal": _JAPAN_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": a.get("url"),
                "reason": reason_text,
                "metadata": meta,
            },
        })

    for a in ownership_articles:
        meta = a.get("metadata") or {}
        # `or`, not .get(key, default) - filer_name is EDINET's own
        # externally-sourced field (doc.get("filerName") in
        # news-retrieval's own fetcher) and can genuinely be a
        # present-but-None key if EDINET's own API response omits it for
        # a given filing, which .get(key, default) would not fall back
        # on (same real bug class this module's own _japan_press
        # source_label fix documents).
        company = meta.get("filer_name") or meta.get("code")
        issuer_code = meta.get("code")
        holding_ratio = meta.get("holding_ratio")
        holding_pct = holding_ratio * 100.0 if holding_ratio is not None else None
        # The holder's previous ratio, where the filing states one. A
        # change report without it reads "12.8% stake changed", which
        # hides whether the holder bought or sold - the one thing a
        # reader wants from an ownership row.
        previous_ratio = meta.get("holding_ratio_previous")
        previous_pct = previous_ratio * 100.0 if previous_ratio is not None else None

        # docTypeCode 350 only exists once a holder has already crossed
        # 5%, so the filing's existence is a genuine crossing event. That
        # establishes the event is real; it does not make every such
        # event equally material. A holder sitting just over the
        # threshold and one holding a fifth of the company file the same
        # document, so the stake itself is what separates them.
        # doc_description is EDINET's own report-type label. Fall back to
        # the title, which the fetcher builds ending in that same label,
        # for rows stored before it was kept as its own field.
        doc_description = meta.get("doc_description") or a.get("title") or ""
        is_initial = _OWNERSHIP_INITIAL_REPORT_MARKER in doc_description
        is_passive = _OWNERSHIP_PASSIVE_REGIME_MARKER in doc_description
        # Two decimals, because that is the precision EDINET filings
        # state and this field stores: the XBRL ratio is 4dp (0.1242),
        # so the percentage is exactly 2dp. Rounding to one threw away
        # filed precision on every row - 14.06% was shown as 14.1% -
        # and collapsed real moves entirely: Capital Research went from
        # 12.40% to 12.42% of Resonac, which read "raised its stake to
        # 12.4%, from 12.4%", a claim of a change with no change shown.
        stake = (_format_holding_pct(holding_pct) if holding_pct is not None
                 else "an unstated stake")
        # The issuer's English name, not its bare TSE code. These
        # sentences read "in the company (code 3436)" because they were
        # written before the issuer lookup twelve lines below, and never
        # updated to use it - the resolved name was already being stored
        # on the row the whole time.
        _issuer = company_for(issuer_code) or {}
        issuer_label = _issuer.get("company") or f"the company (code {issuer_code})"

        # "raised to 12.8% from 14.1%" is wrong even when the numbers
        # are right, so the verb is taken from the comparison, not
        # assumed. Equal ratios do happen (a filing triggered by a
        # contract change, not a trade), and read as "held at".
        if previous_pct is None or holding_pct is None:
            move_verb, from_clause = "changed its stake in", ""
        elif holding_pct > previous_pct:
            move_verb = "raised its stake in"
            from_clause = f", from {_format_holding_pct(previous_pct)}"
        elif holding_pct < previous_pct:
            move_verb = "cut its stake in"
            from_clause = f", from {_format_holding_pct(previous_pct)}"
        else:
            move_verb, from_clause = "held its stake in", ""

        if is_passive:
            # Declared non-controlling. An index or custody position
            # moving with its mandate, whichever report type carries it.
            signal = "WEAK"
            reason_code = (
                "holder_crosses_five_pct_passive" if is_initial
                else "holder_changes_existing_stake_passive"
            )
            if is_initial:
                opening = (
                    f"{company} has newly crossed 5% in {issuer_label}, "
                    f"now holding {stake}"
                )
            else:
                opening = (f"{company} holds {stake} of {issuer_label}{from_clause}")
            reason_text = (
                f"{opening} - filed under the passive-investor regime, which is only "
                f"available to a holder not seeking control."
            )
        elif is_initial:
            signal = "SIGNAL"
            reason_code = "holder_crosses_five_pct"
            reason_text = (
                f"{company} has newly crossed 5% in {issuer_label}, "
                f"now holding {stake} - an initial large-shareholding report filed on an "
                f"active basis, not under the passive-investor regime."
            )
        else:
            signal = "SIGNAL"
            reason_code = "active_holder_changes_stake"
            reason_text = (
                f"{company} has {move_verb} {issuer_label} to {stake}{from_clause} - "
                f"a change report filed on an active basis, not under the "
                f"passive-investor regime."
            )

        meta["holding_pct"] = round(holding_pct, 2) if holding_pct is not None else None
        meta["holding_pct_previous"] = (
            round(previous_pct, 2) if previous_pct is not None else None)
        meta["signal_reason_code"] = reason_code
        # "company" here is the TARGET company being reported on (issuer_code),
        # not the filer (see "company" local var above, which is
        # filer_name/filer's own code - a real, different concept: WHO
        # crossed 5%, vs WHOSE stock they crossed it in). Every other
        # signal type's "company"/"native_name" fields describe the
        # tracked company itself, so jp_ownership rows should too, kept
        # consistent rather than left as the one real exception.
        issuer_ticker = company_for(issuer_code)
        meta["company"] = issuer_ticker["company"] if issuer_ticker else None
        meta["native_name"] = issuer_ticker["native_name"] if issuer_ticker else None
        a["metadata"] = meta

        results.append({
            "article": a,
            "result": {
                "signal": _JAPAN_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": a.get("url"),
                "reason": reason_text,
                "metadata": meta,
            },
        })

    for a in buyback_announcements:
        meta = a.get("metadata") or {}
        code = meta.get("code")
        company = meta.get("company") or code
        program_limit_shares = _parse_buyback_program_limit_shares(meta.get("program_limits"))
        shares_outstanding = _company_shares_outstanding(code, stored_company_reference)

        pct_of_shares = None
        if program_limit_shares is not None and shares_outstanding:
            pct_of_shares = program_limit_shares / shares_outstanding * 100.0

        if pct_of_shares is not None and pct_of_shares >= _BUYBACK_PCT_OF_SHARES_THRESHOLD:
            signal = "SIGNAL"
            reason_code = "large_buyback_vs_shares_outstanding"
            reason_text = (
                f"{company} announced a buyback program authorizing up to {program_limit_shares:,} "
                f"shares - {pct_of_shares:.1f}% of shares outstanding, at or above the "
                f"{_BUYBACK_PCT_OF_SHARES_THRESHOLD:.0f}% threshold."
            )
        elif program_limit_shares is not None:
            signal = "WEAK"
            reason_code = "buyback_announced"
            if pct_of_shares is not None:
                reason_text = (
                    f"{company} announced a buyback program authorizing up to "
                    f"{program_limit_shares:,} shares ({pct_of_shares:.1f}% of shares "
                    f"outstanding)."
                )
            else:
                reason_text = (
                    f"{company} announced a buyback program authorizing up to "
                    f"{program_limit_shares:,} shares - this company's shares outstanding are "
                    f"not yet known, so it cannot be measured against the "
                    f"{_BUYBACK_PCT_OF_SHARES_THRESHOLD:.0f}%-of-shares threshold."
                )
        else:
            signal = "WEAK"
            reason_code = "buyback_announced_no_figure"
            reason_text = f"{company} announced a buyback program; it states no share-count cap."

        meta["buyback_program_limit_shares"] = program_limit_shares
        meta["buyback_pct_of_shares_outstanding"] = round(pct_of_shares, 2) if pct_of_shares is not None else None
        meta["signal_reason_code"] = reason_code
        meta["native_name"] = (company_for(code) or {}).get("native_name")
        a["metadata"] = meta

        results.append({
            "article": a,
            "result": {
                "signal": _JAPAN_SIGNAL_MAP[signal],
                "signal_score": None,
                "source_id": a.get("url"),
                "reason": reason_text,
                "metadata": meta,
            },
        })

    return results


# Japan Signals spec Section 6.6/8.1 - press classification. Confirmed
# live 2026-09-29: news-retrieval's own jp_press source has exactly TWO
# real sub-sources wired (see news-retrieval's CLAUDE.md) - Jiji Press
# (jiji.com, Japan's national wire service - the closest real analogue to
# a paper-of-record, same role Yonhap plays for Korea's own
# _KOREA_FIRST_TIER_PUBLICATIONS) and Newswitch (newswitch.jp, a
# dedicated semiconductor/industry trade press site - the closest real
# analogue to THE ELEC). Nikkei itself, Reuters Japan, and Kyodo News
# English were all checked and confirmed unusable (paywalled/anti-bot/no
# matching category respectively) - there is no second-tier source in
# this pipeline at all, unlike Korea's mixed trusted-publication list, so
# BOTH real sources are first-tier here.
_JAPAN_PRESS_FIRST_TIER_PUBLICATIONS: frozenset[str] = frozenset({
    "jiji.com",
    "newswitch.jp",
})

# Same set as _JAPAN_PRESS_FIRST_TIER_PUBLICATIONS today (Filter 1's
# trusted-publication gate and the first/second-tier split happen to
# coincide, since every wired jp_press source is first-tier) - kept as
# its own separate name/constant rather than reusing one for both, so a
# future second-tier addition (a general business daily, say) only needs
# to be added to the trusted set, without silently also promoting it to
# first-tier.
_JAPAN_PRESS_TRUSTED_PUBLICATIONS: frozenset[str] = _JAPAN_PRESS_FIRST_TIER_PUBLICATIONS

# Jiji Press is this pipeline's own real substitute for the spec's
# "Nikkei reports something the company has not confirmed" carve-out
# (Section 6.6: "Signal, marked unconfirmed" - Nikkei's own scoops are
# NOT downgraded, just clearly labelled, because Nikkei is reliably
# accurate ahead of formal disclosure). Newswitch does NOT get the same
# treatment - confirmed live its real coverage (e.g. the one genuine
# jp_press row in this test window: Resonac's 12-inch SiC substrate
# announcement) is post-hoc reporting on an already-public technical
# achievement, not a scoop ahead of a company's own disclosure - treating
# it as "unconfirmed, watch for the company to confirm" would be a
# category error, since there is usually nothing further for the company
# to confirm. news-retrieval's own fetcher currently stamps
# metadata.unconfirmed=True unconditionally for BOTH sources (a real gap,
# not fixed here - this module has no write access to news-retrieval's
# own fetch code, and changing this field's meaning there is a separate,
# out-of-scope task) - this function derives its OWN unconfirmed
# decision from the publication domain instead of trusting that
# blanket-True field, so the wrong signal from that gap never reaches a
# stored result.
_JAPAN_PRESS_NIKKEI_SUBSTITUTE_PUBLICATIONS: frozenset[str] = frozenset({
    "jiji.com",
})

# Spec Section 10.1, verbatim (the doc's own instructions text, not a
# paraphrase) - same "kept exact, not reworded" discipline
# _QUALIFICATION_NEWS_SYSTEM_PROMPT already follows for Korea's own press
# prompt, since the doc's own HIGH/WEAK examples are calibrated to this
# exact wording. "stacked memory" carve-out (Korea's own prompt) has no
# Japan equivalent in the spec's own 10.1 text, so none is added here.
_JAPAN_PRESS_SYSTEM_PROMPT = """You classify Japanese market headlines about semiconductor,
equipment, materials and electronics companies as HIGH or WEAK.

HIGH means the headline states a specific, checkable fact:
- a number (revenue, profit, orders, capacity, price)
- a change to a company forecast
- a named customer, partner, supplier or contract
- an order, a capacity change or an investment decision
- a government or regulatory action naming the company

WEAK means anything else:
- analyst opinion, ratings or price targets
- outlook or sentiment with no figure
- the company mentioned in passing in a market round-up
- a story mainly about a different company

Who is speaking matters. A fact stated by the company, the
exchange or a regulator is HIGH. The same fact predicted by an
analyst is WEAK.

A Nikkei report of something the company has not yet confirmed
is HIGH, but set unconfirmed to true. The Nikkei reports
accurately ahead of formal disclosure often enough that
downgrading it loses real information.

If unsure, answer WEAK.

Answer in this exact form and nothing else:
HIGH unconfirmed=false
HIGH unconfirmed=true
WEAK"""

_JAPAN_PRESS_SCORE = None  # forced fixed-form call, no real confidence value - same reasoning as Korea's own _QUALIFICATION_NEWS_SCORE


def _japan_press_extract_domain(url: str | None) -> str:
    """Same domain-derivation rule as Korea's own _extract_domain - kept
    as its own copy, not imported, matching this codebase's existing
    no-cross-classifier-import convention for small shared logic.
    """
    host = urlparse(url or "").hostname or ""
    host = host.lower()
    if host.startswith("www."):
        host = host[len("www."):]
    return host


def _japan_press_company_name_in_headline(
    title: str, code: str | None, company: str | None,
) -> bool:
    """Filter 2 (spec Section 6.6's own "no company name in the headline"
    free filter): does the tracked company's own name (or code) appear in
    the headline. Unlike Korea's _company_name_in_headline, jp_press
    articles have NO body text at all (news-retrieval's own fetcher
    deliberately stores body=None for this source - headline+timestamp
    only, matching the spec's own treatment of Nikkei), so this is
    headline-only by necessity, not by choice to follow the spec's
    literal wording narrowly.

    In practice this filter never actually drops anything real: every
    jp_press article's own title is ALREADY built by news-retrieval's
    fetcher as "{company} ({code}): {original headline}" (see
    _fetch_press_jp_jiji/_fetch_press_jp_newswitch), i.e. the matched
    company's name is baked into the stored title by construction, before
    this classifier ever sees it - the underlying client-side company
    match (_match_japan_company) already did the real filtering work at
    fetch time. Kept as an explicit, real check anyway (not assumed/
    skipped) so a future change to how jp_press articles are titled does
    not silently bypass this filter without a test catching it.
    """
    if not title:
        return False
    if company and company in title:
        return True
    if code and code in title:
        return True
    return False


def _format_company_revision_habit(habit: dict[str, Any] | None) -> str | None:
    """Format a stored J1 habit dict (see compute_forecast_habit's own
    return shape) into spec Section 10.2's own example string,
    "raises twice a year" - the same real revisions_per_year/
    typical_direction fields J1's own classify_forecast_revision already
    reads, just rendered as prose instead of used numerically.

    Returns None (not a fabricated string) when there is no habit at all,
    or when it has no real revisions_per_year to describe - same "send
    what's real, omit what would be fabricated" principle this module
    already applies throughout (see classify_press's own docstring).
    """
    if not habit or habit.get("revisions_per_year") is None:
        return None
    per_year = habit["revisions_per_year"]
    direction = habit.get("typical_direction")
    verb = "raises" if direction == "raise" else "cuts" if direction == "cut" else "revises"
    if per_year < 0.75:
        frequency = "less than once a year on average"
    elif per_year < 1.5:
        frequency = "about once a year"
    elif per_year < 2.5:
        frequency = "about twice a year"
    else:
        frequency = f"about {round(per_year)} times a year"
    return f"{verb} {frequency}"


def _format_progress_vs_usual(progress_pct: float | None, typical_progress_pct: float | None) -> str | None:
    """Format a real progress-vs-habit gap into spec Section 10.2's own
    example string, "+17 points ahead" - the same real gap
    classify_results_against_forecast already computes numerically
    (gap = progress_pct - habit['typical_progress_pct']), rendered as
    prose here instead.

    Returns None (not a fabricated string) when either real value is
    missing - same "send what's real, omit what would be fabricated"
    principle as _format_company_revision_habit above.
    """
    if progress_pct is None or typical_progress_pct is None:
        return None
    gap = round(progress_pct - typical_progress_pct, 1)
    if gap == 0:
        return "exactly on its usual pace"
    direction = "ahead" if gap > 0 else "behind"
    return f"{abs(gap):.0f} points {direction}"


def _classify_japan_press_relevance(
    company_name: str, code: str | None, source: str, headline: str,
    published: str | None,
    company_revision_habit: str | None, progress_vs_usual: str | None,
    model: str, api_key: str, base_url: str, timeout: int,
) -> tuple[str, bool]:
    """One model call per surviving headline - spec Section 10.1's exact
    prompt, 10.2's input field layout. Every one of 10.2's fields this
    module can genuinely compute is now sent - see classify_press's own
    docstring for the two remaining, real, structural exceptions (an
    English "headline" field and 10.2's two "already worked out" fields
    this function has no real basis for, since a repeat-story/English-
    coverage-check aren't computed here).

    ``headline`` is jp_press's own stored ``title`` field, which IS the
    native Japanese headline (confirmed live: news-retrieval's
    _fetch_press_jp_jiji/_fetch_press_jp_newswitch never populate an
    English title at fetch time - _TRANSLATION_FIELDS's own jp_press
    entry only adds translated_title AFTER classification, via
    translate_japan_articles, which classify_press's caller runs
    afterward). Sent as ``original`` per spec 10.2's own field name for
    the Japanese text - a separate English ``headline`` field is NOT
    sent, since no real English headline exists yet at this point in the
    pipeline; fabricating one here would violate this module's own
    "send what's real" principle worse than omitting it.

    ``code`` and ``published`` are genuinely available on the stored
    article and are sent, closing the earlier gap where the model had no
    way to see them despite the spec listing them as part of every
    classification call's input.

    ``company_revision_habit``/``progress_vs_usual`` are real, already
    formatted by _format_company_revision_habit/_format_progress_vs_usual
    from the SAME stored J1/J2 habit caches classify_forecast_revision/
    classify_results_against_forecast already read - see
    classify_press's own docstring for how classify_japan_signal_batch
    now threads stored_habits/stored_progress_habits into this function
    to make that real (previously classify_press received neither cache
    at all, so these two fields were correctly omitted rather than
    fabricated - not true anymore now that the caches are threaded
    through). Either can still legitimately be None (no trusted habit
    yet for this company) - sent as "not established" rather than
    omitting the line entirely, so the model can tell "we checked and
    there's no pattern yet" apart from "we didn't check."
    """
    code_line = f"\ncode: {code}" if code else ""
    published_line = f"\npublished: {published}" if published else ""
    habit_line = f"\ncompany_revision_habit: {company_revision_habit or 'not established'}"
    progress_line = f"\nprogress_vs_usual: {progress_vs_usual or 'not established'}"
    user_prompt = (
        f"company: {company_name}{code_line}\nsource: {source}\n"
        f"original: {headline}{published_line}{habit_line}{progress_line}"
    )
    payload = {
        "model": model,
        "temperature": 0,
        "max_tokens": 10,
        "messages": [
            {"role": "system", "content": _JAPAN_PRESS_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        req = Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        answer = (content or "").strip().upper()
        if answer.startswith("HIGH"):
            return "HIGH", "UNCONFIRMED=TRUE" in answer.replace(" ", "")
        return "WEAK", False
    except Exception as exc:
        logger.warning("[JAPAN_PRESS] classification failed, defaulting to WEAK: %s", exc)
        return "WEAK", False


def _press_story_fragment(title: str | None) -> str:
    """The story out of a stored press title.

    Titles are stored as "{company} ({code}): {headline}", so the part
    after the first ": " is what was actually reported. Shared by the
    reason sentence and the post-translation swap that replaces it, so
    both split the Japanese and the English title the same way - a
    mismatch there would leave the Japanese text in place.
    """
    if not title:
        return ""
    return (title.split(": ", 1)[-1] if ": " in title else title).strip()


def classify_press(
    articles: list[dict[str, Any]],
    model: str | None = None,
    stored_habits: dict[str, dict[str, Any]] | None = None,
    latest_progress_by_code: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """J7: classify jp_press articles per Japan Signals spec Section 6.6 -
    three free filters, then one model call for whatever survives.

    Filter 1 (trusted publication): drop unless the article's own domain
    is in _JAPAN_PRESS_TRUSTED_PUBLICATIONS. In practice this never drops
    anything real today, since news-retrieval's own jp_press fetcher only
    ever wires Jiji/Newswitch in the first place (see that source's own
    CLAUDE.md entry) - kept as an explicit, real gate anyway (same
    reasoning _japan_press_company_name_in_headline's own docstring gives
    for Filter 2), not skipped just because it currently never fires.

    Filter 2 (company name in headline): see
    _japan_press_company_name_in_headline's own docstring.

    Filter 3 (repeat story, spec's own "already seen"): counts distinct
    publications carrying this story via metadata.also_reported_by, same
    field/convention Korea's own _publications_carrying_story reads.
    CONFIRMED LIVE 2026-09-29 this is currently always 1 for every real
    jp_press row: japan_market_signal is NOT in news-retrieval's own
    _TITLE_DEDUP_DOMAINS (unlike korea_market_signal), so also_reported_by
    is never populated for this domain - a real, documented gap (adding
    japan_market_signal to that set is a news-retrieval change, out of
    scope for this classifier). Computed and sent anyway (not hardcoded
    to 1) so the wiring is correct and starts reflecting real repeat
    coverage the moment that gap is closed, without this function needing
    a second change then.

    Model: one HIGH/WEAK(+unconfirmed) call (spec 10.1) on whatever
    survives all three filters.

    Rule (spec Section 6.6's own table):
      SIGNAL (unconfirmed=True):  model says HIGH unconfirmed=true AND
                                  the publication is Jiji (this
                                  pipeline's own Nikkei substitute - see
                                  _JAPAN_PRESS_NIKKEI_SUBSTITUTE_
                                  PUBLICATIONS). Not downgraded to Weak
                                  even though unconfirmed - the spec's
                                  own explicit instruction.
      SIGNAL:                     model says HIGH (not marked
                                  unconfirmed, or unconfirmed from a
                                  non-Jiji source), publication is
                                  first-tier, name check passed - all
                                  true simultaneously here, since every
                                  wired jp_press source is first-tier.
      WEAK:                       model says WEAK.
      NOISE:                      removed by Filter 1, 2, or an empty/
                                  unparseable title before ever reaching
                                  the model - not returned as a
                                  classified result at all (mirrors
                                  Korea's own classify_qualification_
                                  news and Taiwan's classify_gdelt_
                                  articles: a dropped-by-filter article
                                  was never a candidate).

    Model-input wiring (spec Section 10.2's field layout) - company/code/
    source/original/published/company_revision_habit/progress_vs_usual
    are all sent to the model now (see _classify_japan_press_relevance's
    own docstring for why "original" carries the real Japanese title and
    no separate English "headline" field is sent - no real English
    headline exists yet at this point in the pipeline).
    company_revision_habit is read from ``stored_habits`` (the SAME real
    J1 cache classify_forecast_revision already receives - see
    classify_japan_signal_batch, which now threads it into this function
    too) and formatted via _format_company_revision_habit. May be
    None/omitted (falls back to an empty dict, same as J1's own
    stored_habits fallback) - every real jp_press article then simply
    sends "not established" when no habit is found for its company,
    rather than this function failing or fabricating a plausible-looking
    value.

    progress_vs_usual describes how a company's LATEST real filed result
    compares to its own usual pace (progress_pct - typical_progress_pct),
    same real gap classify_results_against_forecast (J2) already computes
    per-filing - but a jp_press headline is not itself a filing, so it has
    no progress reading of its own. Re-read against the spec's own
    Section 10.3 framing ("items arrive already classified... carries...
    the company's revision habit... use them exactly as given"),
    progress_vs_usual is COMPANY-level context attached to any item about
    that company, same as company_revision_habit already is - not
    something that must be derived from the specific item's own content.
    Resolved via ``latest_progress_by_code`` ({code: {progress_pct,
    typical_progress_pct, published}}), built by
    classify_japan_signal_batch from this SAME batch's own real J2
    results before calling classify_press (see that function's own
    comment on how "most recent" is picked deterministically when a
    company has more than one real J2 result in the batch). This is a
    real, stored-as-of-last-filing snapshot - the same "as of last known
    data" spirit company_revision_habit already has (that field is also
    only as fresh as japan_company_habits' last refresh, not computed
    live from the press headline either) - not a live computation from
    the press article's own text, and not a fabricated/guessed number
    when no real J2 result exists for this company in the batch (falls
    back to "not established", same principle as company_revision_habit's
    own missing-habit case).

    The remaining three of Section 10.2's "already worked out" fields
    (publications_carrying_this, english_coverage_found, price_move_today)
    stay correctly unsent here - publications_carrying_this is already
    computed elsewhere in this function but is a metadata field on the
    RESULT, not a model input per spec 10.2 (rereading 10.2's own layout:
    it lists it as an input too, but this function's own Filter 3 already
    established it is always 1 today, real but uninformative - no real
    basis is skipped by leaving it as a stored result field only, not a
    second copy sent to the model); english_coverage_found and
    price_move_today remain Section 10.3 SUMMARY WRITER inputs this
    function has no real basis for at all (no English-coverage check or
    market price feed reaches this classifier).
    """
    import config as _config
    model = model or _config.JAPAN_SIGNAL_MODEL
    api_key = _config.OPENAI_API_KEY
    base_url = _config.OPENAI_BASE_URL
    timeout = _config.OPENAI_TIMEOUT
    stored_habits = stored_habits or {}
    latest_progress_by_code = latest_progress_by_code or {}

    candidates = []
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_press":
            continue
        title = a.get("title") or ""
        if not title.strip():
            continue

        domain = _japan_press_extract_domain(a.get("url"))
        if domain not in _JAPAN_PRESS_TRUSTED_PUBLICATIONS:
            continue

        if not _japan_press_company_name_in_headline(title, meta.get("code"), meta.get("company")):
            continue

        candidates.append((a, domain))

    results: list[dict[str, Any]] = []
    for a, domain in candidates:
        meta = a.get("metadata") or {}
        title = a.get("title") or ""
        company = meta.get("company") or meta.get("code") or ""
        also_reported_by = meta.get("also_reported_by") or []
        # Two shapes: a bare domain string on rows written before this
        # field carried headlines, and {outlet, title, url} after. Only
        # the outlet is counted here, and unpacking the list directly
        # would raise on the dict form (unhashable in a set).
        other_outlets = {
            (e.get("outlet") if isinstance(e, dict) else e) or ""
            for e in also_reported_by
        }
        publications_carrying_this = len({domain, *other_outlets} - {""})
        # a.get("source", domain) would NOT fall back here if "source" is
        # a present-but-None key (a dict default only applies when the key
        # is MISSING, not when its value is None) - confirmed live this is
        # a real case, not a hypothetical: news-retrieval's own generic
        # article-shape default is source=None for any article a caller
        # builds without setting it explicitly (see e.g. test fixtures /
        # any future jp_press sub-source that omits it) - a bare
        # a.get("source", domain) silently produced the reason text
        # "None reports a specific, checkable fact" in exactly this case.
        source_label = a.get("source") or domain

        code = meta.get("code")
        revision_habit = stored_habits.get(code) if code else None
        company_revision_habit = _format_company_revision_habit(revision_habit)

        # progress_vs_usual: company-level context (like company_revision_
        # habit above), from this company's LATEST real J2 result in this
        # same batch - see classify_japan_signal_batch's own comment on how
        # latest_progress_by_code is built. Genuinely None when this
        # company has no real J2 result in the current batch (falls back
        # to "not established" via _classify_japan_press_relevance), not a
        # fabricated/guessed comparison.
        latest_progress = latest_progress_by_code.get(code) if code else None
        progress_vs_usual = (
            _format_progress_vs_usual(
                latest_progress["progress_pct"], latest_progress["typical_progress_pct"],
            )
            if latest_progress else None
        )

        model_answer, model_unconfirmed = _classify_japan_press_relevance(
            company, code, source_label, title, a.get("published"),
            company_revision_habit, progress_vs_usual,
            model, api_key, base_url, timeout,
        )

        is_nikkei_substitute = domain in _JAPAN_PRESS_NIKKEI_SUBSTITUTE_PUBLICATIONS
        unconfirmed = model_unconfirmed and is_nikkei_substitute

        # Lead with what was reported, not with the verdict on it. The
        # headline is the story; whether it states a checkable fact is
        # why it is here, which belongs after.
        story = _press_story_fragment(title)

        if model_answer == "HIGH":
            signal = "SIGNAL"
            if unconfirmed:
                reason_code = "press_high_unconfirmed_nikkei_substitute"
                reason_text = (
                    f"{source_label} reports: {story} - a specific, checkable claim "
                    f"the company has not yet confirmed, carried by a publication that "
                    f"reports reliably ahead of formal disclosure."
                )
            else:
                # The company has already announced this - that is what
                # "not unconfirmed" means on a press row, and the card's
                # own caveat says so ("{company} announced this itself;
                # {source} is reporting it after the fact"). A High
                # badge beside that caveat contradicts it: a re-report
                # carries no information the company's own disclosure
                # did not already carry, and the disclosure is the
                # better source. Weak, with the re-report stated.
                signal = "WEAK"
                reason_code = "press_report_of_company_announcement"
                reason_text = (
                    f"{source_label} reports: {story} - the company had already "
                    f"announced this."
                )
        else:
            signal = "WEAK"
            reason_code = "press_weak"
            # WEAK is the catch-all arm of the prompt: analyst opinion,
            # an outlook with no figure, a passing mention, a story
            # mainly about someone else, or simple uncertainty. The
            # model returns the verdict only, not which of those fired,
            # so name the bar that was missed rather than asserting a
            # cause - calling a real technical achievement "opinion or
            # sentiment" is wrong whenever the miss was just a figure.
            reason_text = (
                f"{source_label} reports: {story} - no figure, named counterparty "
                f"or company confirmation in the headline to check it against."
            )

        meta["publication_domain"] = domain
        meta["publication_tier"] = "first" if domain in _JAPAN_PRESS_FIRST_TIER_PUBLICATIONS else "second"
        meta["publications_carrying_this"] = publications_carrying_this
        meta["unconfirmed"] = unconfirmed
        meta["signal_reason_code"] = reason_code
        meta["company_revision_habit"] = company_revision_habit
        meta["progress_vs_usual"] = progress_vs_usual
        meta["native_name"] = (company_for(code) or {}).get("native_name") if code else None
        a["metadata"] = meta

        results.append({
            "article": a,
            "result": {
                "signal": _JAPAN_SIGNAL_MAP[signal],
                "signal_score": _JAPAN_PRESS_SCORE,
                "source_id": a.get("url"),
                "reason": reason_text,
                "metadata": meta,
            },
        })

    logger.info(
        "[JAPAN_PRESS] %d article(s) survived filters, classified",
        len(results),
    )
    return results


_JAPAN_DISCLOSURE_SYSTEM_PROMPT = """You classify corporate disclosures filed by Japanese
semiconductor, equipment, materials and electronics companies as
HIGH, WEAK or ROUTINE.

HIGH means the filing changes what the company owns, controls or
is committing capital to:
- an acquisition, merger, spin-off, divestiture or joint venture
- an investment in, or sale of, a stake in another company
- a plant, site or business being opened, closed or transferred
- a capital raise, bond issue or change to the share structure
- a change of control, or a tender offer

WEAK means a real decision that does not change the above:
- a change of representative director or senior management
- a dividend policy change
- an organisational or reporting-structure change
- a disclosure whose substance you cannot determine

ROUTINE means administrative filings that recur as a matter of
course:
- stock options, restricted stock, or treasury shares issued for
  employee or director compensation
- articles of incorporation, internal regulations, or similar
  filings
- a notice of a scheduled meeting, or its results
- a correction or re-filing of an earlier document

Judge the filing itself, not whether the company is important.
A compensation filing from a large company is still ROUTINE.
If unsure, answer WEAK.

Answer with the tier, then a slash, then the action the title names,
as a noun phrase taken from the title. Do not add detail the title
does not state.

Keep any qualifier the title gives that changes what the action
means - which business, which subsidiary, which tranche, whether it
is planned or completed. Drop only words that carry nothing:
"Announcement Concerning", "Notice Regarding", "Execution of".

"Execution of Follow-on Investment (Third Tranche) in OpenAI"
  -> follow-on investment in OpenAI, third tranche
"Completion of Execution of Partial Spin-off of Crasus Chemical
 (Petrochemical Business)"
  -> completed partial spin-off of Crasus Chemical's petrochemical business
"Announcement Concerning Absorption-type Merger with a Wholly Owned
 Subsidiary and Closure of its Business Site"
  -> absorption-type merger with a wholly owned subsidiary, and closure
     of its business site

HIGH/spin-off of its display-materials business
WEAK/change of representative director
ROUTINE/articles of incorporation

Answer with that single line and nothing else."""

_JAPAN_DISCLOSURE_MAP = {"HIGH": "SIGNAL", "WEAK": "WEAK", "ROUTINE": "NOISE"}

# Disclosure categories a J8 result can come from. jp_extraordinary is
# EDINET's 臨時報告書 (extraordinary report), which a company files for
# the same class of event Kabutan carries as a disclosure - the two are
# often the same event seen through two sources.
_JAPAN_DISCLOSURE_CATEGORIES = ("jp_disclosure", "jp_extraordinary")


# What an extraordinary report was filed FOR. EDINET's own
# `currentReportReason` is the FSA disclosure ordinance clause the
# filing cites, and the clause IS the event type - a fixed legal
# enumeration, not free text, so it maps exactly rather than being
# guessed at.
#
# This is why an extraordinary report needs no PDF fetch to be
# described: news-retrieval already stores the clause from
# documents.json (see its own comment on _EDINET_DOC_TYPE_
# EXTRAORDINARY). Before this map, every such row fell back to "a
# corporate disclosure whose effect is not established from the
# filing's own title" - true of the title, but the row carried the
# answer in a field nothing read.
#
# Keys are matched on the clause's numbered tail, since filings write
# the same clause with full-width and half-width digits
# interchangeably ("第2項第4号" / "第２項第４号").
# Only clauses CONFIRMED against a real stored filing are listed.
# An unlisted clause returns None and keeps the plain sentence, which
# is why a half-remembered entry is worse than a missing one: it
# labels a filing with the wrong event and nothing flags it. The first
# draft of this map carried five clauses written from memory of the
# ordinance, including "9号" sitting beside the real "9号の2" - close
# enough to look right and wrong in exactly the way a reader cannot
# check. Add a clause here when a filing citing it has been read.
# Japanese-only filing titles that are a standard document NAME rather
# than a description of an event. Asked to summarise one, the model
# returns the name verbatim, which puts untranslated Japanese on an
# English card - confirmed on real Ibiden rows ("定款", "統合報告書").
# These are ordinary recurring documents, so naming them in English is
# the whole answer; anything not listed keeps the generic sentence
# rather than being guessed at.
_JAPANESE_FORM_NAMES: dict[str, str] = {
    "定款": "its articles of incorporation",
    "統合報告書": "its integrated report",
    "臨時報告書": "an extraordinary report",
    "訂正臨時報告書": "a corrected extraordinary report",
    "有価証券報告書": "its annual securities report",
    "四半期報告書": "its quarterly report",
    "半期報告書": "its half-year report",
}


def _japanese_form_name(title: str | None) -> str | None:
    """English for a filing title that is just a document name.

    Matches the title's own text after the fetcher's
    "{company} ({code}) [{timestamp}]: " prefix, ignoring a trailing
    date or year the real titles carry ("定款 2026/10/01",
    "統合報告書 2026").
    """
    if not title:
        return None
    body = (title.split(": ", 1)[-1] if ": " in title else title).strip()
    for form, english in _JAPANESE_FORM_NAMES.items():
        if body.startswith(form):
            return english
    return None


# An extraordinary report's whole title is its form name - 臨時報告書,
# nothing to read - so the model that grades Kabutan disclosures has
# no input here and answers ROUTINE for all of them. The clause the
# filing cites is the only thing that names the event, so it carries
# the tier as well as the phrase.
#
# Grading by clause rather than by title is not a workaround: the
# ordinance defines each clause as a specific triggering event, which
# is a firmer basis than a model reading prose. What it cannot give
# is magnitude - the clause says a major shareholder changed, not by
# how much - so no clause maps to SIGNAL on its own.
# A move this large is material whether or not the holder's status
# changed. Matches the point at which J6 treats an ownership change
# as a build or sell-down rather than drift, so the two signal types
# grade the same movement the same way.
_MAJOR_SHAREHOLDER_LARGE_MOVE_PTS = 1.0

_EDINET_EXTRAORDINARY_REASONS: dict[str, tuple[str, str]] = {
    # Shin-Etsu S100Z2DU: ストックオプションとして新株予約権を発行.
    # Employee and director compensation, filed on a schedule.
    "2号の2": ("a grant of share options", "ROUTINE"),
    # Resonac S100Z2WW: 当社の主要株主に異動がありました. Who owns a
    # large block of the company changed. Not routine: it is the same
    # class of event J6 reads from a large-shareholding filing, and
    # the clause is only triggered above a threshold.
    "4号": ("a change in major shareholders", "WEAK"),
    # Lasertec S100Z4BX: 定時株主総会において決議事項が決議されました.
    # An AGM passing its resolutions is the expected outcome; the
    # clause does not say which resolutions, so there is nothing here
    # to grade above routine.
    "9号の2": ("a resolution passed at a shareholder meeting", "ROUTINE"),
}


def _extraordinary_reason(meta: dict[str, Any]) -> tuple[str, str] | None:
    """The event an extraordinary report was filed for, if known.

    Reads EDINET's clause code; returns None for a clause not in the
    map rather than a vague stand-in, so an unmapped filing keeps the
    honest generic sentence instead of being mislabelled.
    """
    clause = meta.get("current_report_reason")
    if not clause:
        return None
    # Normalise full-width digits so one key matches both spellings.
    normalised = clause.translate(str.maketrans("０１２３４５６７８９",
                                                "0123456789"))
    # Match the final 第N号[のM] exactly. A suffix test cannot be used:
    # "第19条第2項第99号" ends with "9号" and would be read as clause 9,
    # labelling an unknown filing "a business transfer".
    found = re.findall(r"第(\d+)号(?:の(\d+))?", normalised)
    if not found:
        return None
    number, sub = found[-1]
    key = f"{number}号の{sub}" if sub else f"{number}号"
    return _EDINET_EXTRAORDINARY_REASONS.get(key)


def _extraordinary_tier(meta: dict[str, Any]) -> str | None:
    """The tier the cited clause implies, if the clause is known."""
    entry = _extraordinary_reason(meta)
    return entry[1] if entry else None


def _is_english_disclosure(title: str) -> bool:
    """True when a disclosure's title is the company's own English
    filing rather than its Japanese one.

    Japanese issuers routinely file the same disclosure twice, once in
    each language, within the same minute. Measured on the stored set:
    the ASCII share of an English title runs well above half, while a
    Japanese title's is near zero once the "{company} ({code})
    [{timestamp}]: " prefix the fetcher adds is discounted. Half is the
    boundary because no real title sits near it - they cluster at the
    two ends.
    """
    body = title.split(": ", 1)[-1] if ": " in title else title
    if not body:
        return False
    ascii_letters = sum(1 for ch in body if ch.isascii() and ch.isalpha())
    return ascii_letters > len(body) / 2


def _with_english_prefix(title: str | None, company: str | None) -> str | None:
    """Swap a stored title's Japanese name prefix for the English one.

    Stored titles are "{native_name} ({code}) [{timestamp}]: {body}".
    Where the body is already the company's own English, only the
    prefix is left in Japanese - so the English name replaces it and
    the body is untouched.

    Leaves the title alone when there is no "{prefix}: {body}" split
    or no English name to use: a title that is all body (EDINET's
    "4004 [S100Z5EL]: 臨時報告書" has an id prefix, not a name) must
    not lose or gain text here.
    """
    if not title or not company or ": " not in title:
        return title
    prefix, body = title.split(": ", 1)
    # Only rewrite a prefix that really is the name-and-code form the
    # fetcher writes. An id prefix ("4004 [S100Z5EL]") has no "(code)"
    # and is left as it stands.
    if "(" not in prefix:
        return title
    _, _, tail = prefix.partition("(")
    return f"{company} ({tail}: {body}" if tail else title


def _pair_bilingual_disclosures(
    articles: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Return one entry per filing event, carrying whether the company
    also filed it in English.

    A Japanese issuer files most disclosures in Japanese only. Choosing
    to publish an English version is a decision about who the filing is
    meant to reach, so the fact that one exists is itself worth
    reporting - it is the same awareness gap jp_english_coverage_check
    measures for press, available here as a hard fact from the filing
    rather than inferred from a search.

    The pair is identified by company and the filing's own
    minute-precision publication time: two copies of one event share
    them exactly, and two genuinely different filings do not share a
    minute. The English copy leads where it exists, so the card reads
    without translation, with the Japanese title kept alongside it.

    Keyed on the stored `published` column rather than the timestamp
    the fetcher folds into the title, because the same event also
    arrives from two different SOURCES - TDnet via Kabutan and the
    issuer's own extraordinary report via EDINET - and only one of
    them writes a timestamp into its title; EDINET's bracket holds a
    document id. Both carry the same `published`, so that is the field
    the two have in common.
    """
    by_event: dict[tuple[str, str], list[dict[str, Any]]] = {}
    unpaired: list[dict[str, Any]] = []
    for a in articles:
        meta = a.get("metadata") or {}
        code = meta.get("code")
        stamp = str(a.get("published") or "")
        if not code or not stamp:
            unpaired.append(a)
            continue
        by_event.setdefault((code, stamp), []).append(a)

    out: list[dict[str, Any]] = []
    for a in unpaired:
        meta = a.get("metadata") or {}
        meta["filed_in_english"] = _is_english_disclosure(a.get("title") or "")
        a["metadata"] = meta
        out.append(a)

    for copies in by_event.values():
        english = [c for c in copies if _is_english_disclosure(c.get("title") or "")]
        japanese = [c for c in copies if c not in english]
        lead = (english or japanese)[0]
        meta = lead.get("metadata") or {}
        meta["filed_in_english"] = bool(english)

        # One event reported through two sources. TDnet names what the
        # filing is ("Completion of Execution of Partial Spin-off");
        # EDINET's own title for the same event is just its form name
        # ("臨時報告書"), but EDINET is where the document itself lives.
        # Keep the informative title and borrow the other's document
        # id, so the merged row reads well AND links to the filing.
        others = [c for c in copies if c is not lead]
        if others:
            meta["also_filed_via"] = sorted({
                (c.get("metadata") or {}).get("source_category")
                for c in others
                if (c.get("metadata") or {}).get("source_category")
            })
            if not meta.get("doc_id"):
                for c in others:
                    doc_id = (c.get("metadata") or {}).get("doc_id")
                    if doc_id:
                        meta["doc_id"] = doc_id
                        break
        if english and japanese:
            # Both exist: keep the other language's title rather than
            # discarding it, so a reader can check the original wording.
            meta["original_language_title"] = japanese[0].get("title")
            # The company's own English filing IS the translation, and a
            # better one than this pipeline could produce - it is the
            # wording the issuer chose for international holders and the
            # version they are accountable for. Setting it here means
            # translate_japan_articles finds the field already populated
            # and leaves it alone, so no model call is spent restating
            # text the company already published.
            #
            # Only the BODY of that title is the company's English,
            # though. The fetcher prepends "{native_name} ({code})
            # [{timestamp}]: ", which stays Japanese, so the stored
            # field read "村田製 (6981) [...]: Announcement Concerning
            # Absorption-type Merger" - half translated, and
            # inconsistent with a Japanese-only filing, whose whole
            # title goes through the model and comes back as "Ibiden
            # (4062) [...]". Rewrite the prefix with the English name
            # so the field reads as English however it was produced.
            # No model call - the name is already in the universe table.
            # Resolved from the universe table, not meta["company"] -
            # that is not written until classify_corporate_disclosure
            # runs, well after this, so reading it here would silently
            # pass None and leave the prefix Japanese.
            _rec = company_for(meta.get("code"))
            meta["translated_title"] = _with_english_prefix(
                english[0].get("title"), (_rec or {}).get("company"),
            )
        lead["metadata"] = meta
        out.append(lead)
    return out


def classify_corporate_disclosure(
    articles: list[dict[str, Any]],
    model: str | None = None,
) -> list[dict[str, Any]]:
    """J8: classify corporate-action disclosures.

    The seven rules before this one each read a specific, numeric filing
    - a forecast revision, a buyback programme, a shareholding ratio.
    A corporate action has no such figure in its title: a spin-off, a
    merger or an investment in another company is material because of
    what it does, not because of a number it reports. So unlike J1-J6,
    there is nothing here to threshold, and unlike them this reads the
    filing's own words.

    Kabutan's own category field cannot stand in for that judgment:
    measured on the stored set it reads その他 ("other") on most rows,
    including a follow-on investment in OpenAI and a petrochemical
    spin-off. A keyword list over Japanese titles was the other option
    and was rejected - these filings are formulaic enough that a list
    would work on the examples in hand and silently miss the phrasings
    nobody thought of, which is the failure mode a fixed list always
    has.

    So this follows J7: let the model judge the filing's substance, with
    the deterministic work - pairing the bilingual copies, mapping the
    answer - done here. Three tiers rather than J7's two, because a
    disclosure feed carries a large routine floor (compensation filings,
    articles of incorporation) that is genuinely noise rather than a
    weak signal.
    """
    candidates = [
        a for a in articles
        if (a.get("metadata") or {}).get("source_category")
        in _JAPAN_DISCLOSURE_CATEGORIES
    ]
    if not candidates:
        return []

    import config

    model = model or config.JAPAN_SIGNAL_MODEL
    api_key = config.OPENAI_API_KEY
    base_url = config.OPENAI_BASE_URL
    timeout = config.OPENAI_TIMEOUT

    results: list[dict[str, Any]] = []
    for a in _pair_bilingual_disclosures(candidates):
        meta = a.get("metadata") or {}
        code = meta.get("code")
        ticker = company_for(code or "")
        company_name = (ticker or {}).get("company") or meta.get("company") or code
        title = a.get("title") or ""
        headline = title.split(": ", 1)[-1] if ": " in title else title
        # Whether the company also published in English says who the
        # filing is meant to reach, not what it does - the two are
        # independent, and a management change is the same event in
        # either language. So `filed_in_english` is stored on the row
        # and never alters the tier or the sentence: letting a
        # publishing decision speak to substance would manufacture
        # materiality the filing does not have. It is not surfaced as
        # "unusual" either - 56% of stored disclosures carry an English
        # version, so it is the norm, not an exception.
        # Initialised per filing, not only inside the branch that sets
        # it: that branch is reached by one clause of one doc type, so
        # every other disclosure read an unbound local and the whole
        # batch died on the first Kabutan row.
        major_shareholder_override: str | None = None
        answer, action = _classify_japan_disclosure_substance(
            company_name, code, headline, model, api_key, base_url, timeout,
        )
        signal = _JAPAN_DISCLOSURE_MAP.get(answer, "WEAK")

        # An EDINET-only filing's whole title is its form name
        # ("臨時報告書"), so the model has nothing to summarise and
        # returns no action. The clause the filing cites names the
        # event type exactly - use it rather than falling back to a
        # sentence about what we could not establish. Only when the
        # model gave nothing, so a real summary always wins.
        # Asked to summarise a title that is only Japanese, the model
        # echoes it back verbatim - confirmed on real rows: "臨時報告書",
        # "統合報告書", "定款". That is the form name, not a summary,
        # and it put untranslated Japanese on an English card. Treat an
        # action with no Latin letters as nothing returned, so the
        # clause lookup below gets its turn.
        if action and not any(ch.isascii() and ch.isalpha() for ch in action):
            action = None
        if not action:
            # The ordinance clause first - it names the actual event
            # ("a change in major shareholders"), where the form name
            # only says which document was filed.
            # A major-shareholder change whose figures news-retrieval
            # parsed out of the filing. The clause alone could only
            # say THAT one changed, which is why these were graded no
            # higher than WEAK; with the before/after share in hand
            # the event is gradeable on the same basis an ownership
            # filing is.
            msh_now = meta.get("major_shareholder_pct")
            msh_was = meta.get("major_shareholder_pct_previous")
            if msh_now is not None and msh_was is not None:
                holder = meta.get("major_shareholder_name")
                delta = round(msh_now - msh_was, 2)
                meta["major_shareholder_pct_change"] = delta
                # Whether the holder gained or lost 主要株主 status,
                # taken from the filing's own wording - 主要株主と
                # なるもの ("the party becoming a major shareholder")
                # or 主要株主でなくなるもの. That is a filed fact.
                #
                # Not computed from a percentage threshold: the filing
                # cites the statute and never prints the number, so
                # comparing against a hardcoded 10% would grade on an
                # assumption the document does not support. What the
                # document does support is that this holder was not a
                # major shareholder at 9.59% and is one at 11.06%.
                status = meta.get("major_shareholder_status_change")
                major_shareholder_override = (
                    "SIGNAL"
                    if status in ("became", "ceased")
                    or abs(delta) >= _MAJOR_SHAREHOLDER_LARGE_MOVE_PTS
                    else "WEAK")
                # The name is left out of the action phrase entirely.
                # Translation runs AFTER classification, so `holder` is
                # still katakana here and would be baked into a stored
                # string the later swap cannot reach - the card would
                # carry Japanese in the one field a reader scans. The
                # card composes the name itself from the translated
                # field; this phrase carries only the figures.
                action = (f"a major shareholder's holding to {msh_now:.2f}% "
                          f"of voting rights, from {msh_was:.2f}%")

            clause = _extraordinary_reason(meta)
            if clause:
                # The clause grades the filing too. The model saw only
                # "Extraordinary Report" and answered ROUTINE for every
                # one of these, which read a major-shareholder change
                # the same as an AGM passing its agenda. Applied only
                # where the model had nothing to go on, so a filing
                # with a real title keeps the tier its title earned.
                action, signal = clause[0], _JAPAN_DISCLOSURE_MAP[clause[1]]
            else:
                action = _japanese_form_name(title)

        meta["disclosure_action"] = action

        # An EDINET-only filing's whole title is its form name -
        # "臨時報告書", nothing to summarise - so the model returns no
        # action and all such rows fell back to one identical
        # sentence. The form name IS the fact available: an
        # extraordinary report is filed for a specific triggering
        # event, which is more than "a corporate disclosure" says.
        # Used only when the model gave nothing, so a real summary
        # always wins.
        # Name the event, not the category. These sentences used to
        # restate the tier's definition - "a disclosure that changes
        # what the company owns, controls or is committing capital to"
        # - on a filing whose own title read "Partial Spin-off of
        # Crasus". The action phrase comes from that title, so the
        # reader learns what happened; the category wording survives
        # only as the fallback for a filing with no form name either.
        # Where the action is known the sentence is the same for every
        # tier - "{company} filed a disclosure of {action}." The tier
        # is already its own field on the card, so repeating it in
        # prose said nothing extra, and each tier's wording carried a
        # trailing clause about what this pipeline could not establish
        # ("whose effect on the business is not established from the
        # filing's own title"). That describes our processing, not the
        # filing, so it is gone: the sentence states the fact and
        # stops. Only the no-action fallbacks still differ, because
        # there the tier is the only thing left to say.
        # A parsed major-shareholder change outranks whatever the
        # model made of a title reading only "Extraordinary Report".
        if major_shareholder_override:
            signal = major_shareholder_override
            reason_code = "major_shareholder_change"
        else:
            reason_code = {
                "SIGNAL": "material_corporate_action",
                "WEAK": "corporate_disclosure_unclear",
            }.get(signal, "routine_corporate_filing")

        if reason_code == "major_shareholder_change":
            # Stated as a movement, not as a filing that happened. The
            # figures are the point - every other disclosure sentence
            # has none to give.
            verb = "raised" if (meta.get("major_shareholder_pct_change") or 0) > 0 else "cut"
            status = meta.get("major_shareholder_status_change")
            crossing = (" - becoming a major shareholder" if status == "became"
                        else " - ceasing to be a major shareholder"
                        if status == "ceased" else "")
            reason_text = (
                f"{company_name} reported that "
                f"{meta.get('major_shareholder_name') or 'a major shareholder'} "
                f"{verb} its holding to "
                f"{meta['major_shareholder_pct']:.2f}% of voting rights, from "
                f"{meta['major_shareholder_pct_previous']:.2f}%{crossing}.")
        elif action:
            reason_text = (
                f"{company_name} filed a routine administrative disclosure - {action}."
                if signal == "NOISE" else
                f"{company_name} filed a disclosure of {action}."
            )
        elif signal == "SIGNAL":
            reason_text = (
                f"{company_name} filed a disclosure that changes what the company owns, "
                f"controls or is committing capital to."
            )
        elif signal == "WEAK":
            reason_text = f"{company_name} filed a corporate disclosure."
        else:
            reason_text = (
                f"{company_name} filed a routine administrative disclosure - "
                f"compensation, governance paperwork or a scheduled notice."
            )

        meta["signal_reason_code"] = reason_code
        meta["company"] = company_name
        meta["native_name"] = (ticker or {}).get("native_name")
        a["metadata"] = meta

        results.append({
            "article": a,
            "result": {
                "signal": _JAPAN_SIGNAL_MAP[signal],
                "signal_score": _JAPAN_PRESS_SCORE,
                "source_id": a.get("url"),
                "reason": reason_text,
                "metadata": meta,
            },
        })

    logger.info(
        "[JAPAN_DISCLOSURE] %d disclosure(s) pooled, %d classified after pairing"
        " bilingual filings",
        len(candidates), len(results),
    )
    return results


def _classify_japan_disclosure_substance(
    company_name: str, code: str | None, headline: str,
    model: str, api_key: str, base_url: str, timeout: int,
) -> tuple[str, str | None]:
    """One model call per disclosure, returning (tier, action).

    `tier` is HIGH, WEAK or ROUTINE. `action` is the short noun phrase
    naming what the filing does ("spin-off of its display-materials
    business"), or None when the model returned only a tier.

    The action exists because the reason sentence used to restate the
    category - "a disclosure that changes what the company owns,
    controls or is committing capital to" - for a filing whose own
    title said "Partial Spin-off of Crasus". The reader learned the
    taxonomy, not the event.

    Defaults to WEAK on any failure, the same fail-safe J7 uses: a
    disclosure this module could not read is reported as unestablished
    rather than silently dropped or asserted to be material.
    """
    user_prompt = (
        f"company: {company_name}\n"
        f"{f'code: {code}' if code else ''}\n"
        f"filing: {headline}"
    )
    payload = {
        "model": model,
        "temperature": 0,
        # Was 5 - enough for the tier word alone. The action phrase
        # needs room, and a truncated one would read as a sentence
        # cut off mid-word on the card.
        "max_tokens": 40,
        "messages": [
            {"role": "system", "content": _JAPAN_DISCLOSURE_SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
    }
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
    try:
        req = Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        content = (data.get("choices", [{}])[0]
                   .get("message", {}).get("content", "") or "").strip()
        # "HIGH/spin-off of its display-materials business" - the tier
        # is matched case-insensitively, the action kept as written.
        head, _, tail = content.partition("/")
        action = tail.strip().rstrip(".") or None
        for tier in ("HIGH", "ROUTINE", "WEAK"):
            if head.strip().upper().startswith(tier):
                return tier, action
        return "WEAK", None
    except Exception as exc:
        logger.warning("[JAPAN_DISCLOSURE] classification failed, defaulting to WEAK: %s", exc)
        return "WEAK", None


# Hosts whose pages for a ticker exist whether or not anyone has written
# about the company. A quote, profile or historical-data page is always
# there, so finding one says nothing about coverage - confirmed against
# the stored set, where searches returned "Advantest Corp. ADR"
# (a Barron's market-data page), "Hitachi Ltd ADR (HTHIY)" and
# "Tokyo Ohka Kogyo (4186)" as if they were articles.
_ENGLISH_COVERAGE_QUOTE_PAGE_MARKERS = (
    "/market-data/", "/quote", "/equities/", "/stock/", "/stocks/",
    "/profile", "/historical-data",
)

# A path segment that marks real written content, which overrides the
# markers above: finance.yahoo.com/markets/stocks/articles/... is an
# article that happens to sit under a stocks path, and dropping it
# would discard genuine coverage.
_ENGLISH_COVERAGE_ARTICLE_MARKERS = ("/article", "/news/", "/press-release")

# A Latin-script headline is not necessarily English - the search
# returns German, French and Spanish coverage too, which an
# English-reading desk can no more act on than Japanese. These are
# function words common in those languages and rare in English
# headlines, checked as whole words.
_ENGLISH_COVERAGE_NON_ENGLISH_WORDS = frozenset({
    "aktie", "nach", "und", "der", "die", "das", "von", "für", "mit",
    "les", "des", "une", "pour", "aux", "sur",
    "acciones", "empresa", "mercado", "para", "con",
    "azioni", "società", "mercato",
})

# A headline is only evidence of English coverage if it is in English.
# The search returns Japanese, Chinese and Korean results too - real
# articles, but not ones an English-reading desk can act on, which is
# the whole question this check exists to answer.
_ENGLISH_COVERAGE_MIN_ASCII_RATIO = 0.6

# How long after a Japanese filing an English article can appear and
# still plausibly be about it.
#
# The coverage search runs today and returns today's articles, so
# matching it against any filing by the same company measures nothing:
# a 2014 buyback paired with a 2026 article produced "appeared 111299h
# after", which is 12.7 years and describes two unrelated events that
# share a ticker. Company identity alone cannot establish that an
# article covers a filing.
#
# A time bound is the honest approximation available: an English desk
# that picks up a Japanese disclosure does so within days, not years.
# 7 days is the window because it spans a full reporting cycle - a
# filing released after Friday's close can be written up the following
# week - while excluding anything far enough away to be a separate
# story. Outside it, no claim is made rather than a wrong one.
_ENGLISH_COVERAGE_MAX_LAG_HOURS = 24 * 7

# Coverage must follow the filing. An article published before a
# filing cannot be reporting it, and this check has no way to tell
# otherwise: the coverage search is a generic company query, so an
# earlier article is about some other event involving the same company
# - a price move, an analyst note - not an early account of this
# filing. An earlier version allowed a 24h lead on the grounds that a
# wire can run ahead of a formal disclosure, which is a real pattern
# but not one a company-wide search can identify; it only produced
# "appeared Nh before the filing", which reads as nonsense because it
# is.
_ENGLISH_COVERAGE_MAX_LEAD_HOURS = 0


def _is_english_coverage_headline(title: str) -> bool:
    """True when a coverage-check hit is genuinely English prose.

    Measured on the stored set: an English headline is almost entirely
    ASCII letters and spaces, while a Japanese, Chinese or Korean one is
    almost entirely not - the two cluster at opposite ends with nothing
    near the middle, so the exact ratio matters far less than having
    one. 0.6 sits in that empty band rather than being tuned.
    """
    body = title.split(": ", 1)[-1] if ": " in title else title
    body = body.strip()
    if not body:
        return False
    letters = [ch for ch in body if ch.isalpha()]
    if not letters:
        return False
    if sum(1 for ch in letters if ch.isascii()) / len(letters) < _ENGLISH_COVERAGE_MIN_ASCII_RATIO:
        return False
    # Latin script alone is not English: a German or Spanish headline
    # passes the ratio above and is no more readable to the desk this
    # check is asking about than a Japanese one.
    words = {w.strip(".,:;!?()[]\"'").lower() for w in body.split()}
    return not (words & _ENGLISH_COVERAGE_NON_ENGLISH_WORDS)


def _english_coverage_mentions_company(title: str, company: str | None) -> bool:
    """True when the headline actually names the company searched for.

    A keyword search returns neighbours: a search for Towa returned a
    Lattice Semiconductor page, and one for Tokyo Ohka Kogyo returned
    Kaname Kogyo. Those are real pages about real companies, just not
    this one, and counting them would report the awareness gap as
    closed when nobody has written about this company at all.

    Matches on the first word of the company name, which is the part a
    headline reliably carries ("Advantest Corp. ADR" for Advantest,
    "TDK Corp's Dividend Analysis" for TDK) - requiring the full
    registered name would reject those real hits.
    """
    if not company:
        return False
    body = title.split(": ", 1)[-1] if ": " in title else title
    head = company.split()[0].lower()
    return len(head) >= 3 and head in body.lower()


def _english_coverage_pool(articles: list[dict[str, Any]]) -> dict[str, list[datetime]]:
    """Build {code: [published_dt, ...]} from this batch's English
    coverage-check rows.

    news-retrieval runs one English news search per tracked company
    (press_jp_english_check) and stores the hits under
    source_category='jp_english_coverage_check', keyed by the same
    metadata.code every other Japanese row uses - unlike Korea, which
    needs a "-en" ticker suffix to keep two GDELT pools apart, Japan's
    English rows live in their own source_category and need no such
    marker to join back.

    A row without a code or a parseable date is skipped rather than
    failing the batch: the search is a scrape, and a hit occasionally
    comes back as a page fragment with neither.
    """
    pool: dict[str, list[datetime]] = {}
    dropped = 0
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_english_coverage_check":
            continue
        code = meta.get("code")
        pub_dt = _article_published_dt(a)
        if not code or pub_dt is None:
            continue

        title = a.get("title") or ""
        url = (a.get("url") or "").lower()
        ticker = company_for(code)
        company = (ticker or {}).get("company") or meta.get("company")

        # Three ways a hit looks like coverage without being any: it is
        # not in English, it is a quote page that exists regardless, or
        # it is about a different company the search surfaced nearby.
        # Each would close the awareness gap on paper while leaving it
        # open in fact, which is the one error this check cannot afford.
        if not _is_english_coverage_headline(title):
            dropped += 1
            continue
        is_article = any(m in url for m in _ENGLISH_COVERAGE_ARTICLE_MARKERS)
        if not is_article and any(
            m in url for m in _ENGLISH_COVERAGE_QUOTE_PAGE_MARKERS
        ):
            dropped += 1
            continue
        if not _english_coverage_mentions_company(title, company):
            dropped += 1
            continue

        pool.setdefault(code, []).append(pub_dt)

    if dropped:
        logger.info(
            "[JAPAN_ENGLISH_COVERAGE] %d hit(s) dropped as non-English, a quote"
            " page, or about another company; %d company/companies have real"
            " coverage",
            dropped, len(pool),
        )
    return pool


def _attach_english_coverage(
    results: list[dict[str, Any]], articles: list[dict[str, Any]],
) -> None:
    """Record, on every classified item, whether English coverage of the
    same company exists and how its timing compares.

    A Japanese filing or headline is public the moment it is released,
    but an English-reading desk only learns of it once someone writes it
    up. That lag is the window in which the information is unevenly
    held, which is what makes it worth reporting: "no English coverage
    yet" says the gap is still open, and "appeared 6h after" says it has
    closed.

    Applies to every classified item rather than only press rows. A
    forecast revision nobody has covered in English carries the same
    advantage as an uncovered headline - the gap is a property of the
    company's news flow, not of which rule classified the row.

    Mutates each result's metadata in place, the same pass-over-results
    pattern translate_japan_articles uses. An item whose company has no
    code is left untouched, so an absent field means "not computed"
    rather than "no coverage".
    """
    pool = _english_coverage_pool(articles)
    if not pool:
        return

    for r in results:
        meta = r["result"].get("metadata") or {}
        code = meta.get("code")
        if not code:
            continue
        english_dts = pool.get(code)
        japanese_dt = _article_published_dt(r.get("article") or {})

        # Without a date on the Japanese item there is nothing to
        # measure a lag against, and company identity alone does not
        # establish that an article covers a filing - so no claim is
        # made either way.
        if not english_dts or japanese_dt is None:
            if english_dts and japanese_dt is None:
                r["result"]["metadata"] = meta
                continue
            meta["english_coverage_found"] = False
            r["result"]["metadata"] = meta
            continue

        # Only coverage close enough in time to plausibly be about this
        # filing counts - see _ENGLISH_COVERAGE_MAX_LAG_HOURS.
        related = [
            d for d in english_dts
            if -_ENGLISH_COVERAGE_MAX_LEAD_HOURS
            <= (d - japanese_dt).total_seconds() / 3600.0
            <= _ENGLISH_COVERAGE_MAX_LAG_HOURS
        ]
        if not related:
            meta["english_coverage_found"] = False
            r["result"]["metadata"] = meta
            continue

        earliest = min(related)
        meta["english_coverage_found"] = True
        meta["english_coverage_published"] = earliest.isoformat()
        meta["english_coverage_hours_after_japanese"] = round(
            (earliest - japanese_dt).total_seconds() / 3600.0, 1
        )
        r["result"]["metadata"] = meta


def _attach_mentioned_customers(results: list[dict[str, Any]]) -> None:
    """Record which of the company's own customers each event NAMES.

    Runs over the finished result set rather than inside any one
    classifier, for the same reason _attach_english_coverage does:
    the question belongs to the event, not to the rule that produced
    it, so every row gets the same treatment.

    Stored in entities_json, the column every other domain already
    uses for extracted entities - a Japan row left it at its '[]'
    default until now. Deliberately runs AFTER translate_japan_
    articles, so an originally-Japanese reason has an English form to
    match too.

    Most rows get an empty list and that is the honest answer: a
    forecast revision states figures, a buyback states share counts,
    an ownership filing names a shareholder. Only press and
    disclosures have reason to name a customer.
    """
    for r in results:
        meta = r["result"].get("metadata") or {}
        article = r.get("article") or {}
        # The article BODY matters as much as the metadata here. Only
        # MONOist capex articles carry one (72 of 1,111), but they are
        # prose rather than figures, so they are where a counterparty
        # actually gets named - measured: including the body takes
        # matches from 2 rows to 5. The body is never persisted on the
        # classification (news-retrieval owns article content), so this
        # is the one point in the pipeline where it is in hand.
        text = " ".join(filter(None, (
            meta.get("translated_title"),
            meta.get("translated_reason"),
            meta.get("reason"),
            article.get("title"),
            article.get("body"),
        )))
        entities = mentioned_customers(meta.get("code"), text)
        if entities:
            r["result"]["entities"] = entities


def classify_japan_signal_batch(
    articles: list[dict[str, Any]],
    stored_habits: dict[str, dict[str, Any]] | None = None,
    stored_progress_habits: dict[tuple[str, str], dict[str, Any]] | None = None,
    as_of: datetime | None = None,
    stored_company_reference: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Run every implemented japan_market_signal classifier (J1-J6 so far)
    against one pooled batch of articles read from news-retrieval,
    translate whatever survived, and return one {"article", "result"}
    dict per classified item - same top-level shape as
    classify_korea_signal_batch/classify_taiwan_signal_batch.

    ``stored_habits``: see classify_forecast_revision's own docstring.
    ``stored_progress_habits``: see classify_results_against_forecast's
    own docstring. ``as_of``: see classify_missing_revision's own
    docstring. ``stored_company_reference``: see
    classify_capacity_and_ownership's own docstring. All are the
    caller's (controllers/run.py) responsibility to load/supply and pass
    through; this module has no DB access itself, same boundary every
    other pipeline/*.py module in this service already follows.

    J1 and J2 read from the SAME jp_forecast articles but are mutually
    exclusive per article (an article's figures[0]["_kind"] is either
    "revision", in which case only J1 touches it, or
    "actual_vs_forecast", in which case only J2 does - see each
    function's own filtering) - passing the full pooled ``articles`` list
    to both is correct and not wasteful, matching how Korea's own
    classify_korea_signal_batch calls every classify_* function against
    the full batch regardless of which rows each one will actually touch.
    J4 reads from a disjoint jp_industry slice, J5/J6 (one combined
    function, classify_capacity_and_ownership) read from disjoint
    jp_capex/jp_ownership/jp_buyback slices, and J7 reads from a disjoint
    jp_press slice, of the same pooled ``articles`` list - all grouped
    with J1/J2 below since, like them, each of their results carries a
    real article (unlike J3 - see below). Passing jp_industry through
    translate_japan_articles is a safe no-op (no entry in
    _TRANSLATION_FIELDS - SEAJ's press release is already English).
    jp_ownership and jp_buyback both have real entries now, even though
    each one's own TITLE is structural/numeric (ticker, doc_id,
    docDescription label for jp_ownership; company/date/share-count for
    jp_buyback) - the genuine Japanese prose lives in specific METADATA
    fields instead (jp_ownership's filer_name; jp_buyback's
    program_resolution/program_limits/program_period - see
    _TRANSLATION_FIELDS's own comments on each for why, added after
    confirming live every real row of both had unreadable-to-
    non-Japanese-speakers text with no English equivalent anywhere else
    on the row); jp_capex and jp_press DO have entries (their titles are real Japanese
    prose, same as jp_forecast's), so this costs nothing extra to include
    and keeps one single translate/metadata-refresh pass for every result
    backed by a real article.

    J3 is genuinely different from J1/J2/J4/J5/J6/J7: its results
    describe an ABSENCE (no filing exists to point to), so
    result["article"] is a small synthetic dict (title + a real `as_of`
    published timestamp - see classify_missing_revision's own result-
    shape comment for why not None), never a real news-retrieval article
    dict - handled separately from the translate/metadata-refresh step
    below, which only makes sense for a result backed by a real article
    (J3's own metadata/reason are already final, in English, with no
    native-language field to translate).

    classify_stale_revision_pattern (the spec's own WATCHING section) is
    NOT called from here, unlike J3 - see that function's own docstring
    for why: it needs each company's real FULL revision history to find a
    genuine "last revision date", but this function is normally called
    with only a narrow recent window (classify-japan-signals defaults to
    a single day - see __main__.py). Wired instead into
    controllers.run.refresh_japan_habits, which already pools the full
    wide-history window this function needs and runs on the same
    occasional (e.g. monthly) cadence a stale-revision reading calls for.
    """
    with_article_results: list[dict[str, Any]] = []
    with_article_results.extend(classify_forecast_revision(articles, stored_habits=stored_habits))
    with_article_results.extend(classify_margin_forecast_revision(articles, stored_habits=stored_habits))
    j2_results = classify_results_against_forecast(articles, stored_progress_habits=stored_progress_habits)
    with_article_results.extend(j2_results)
    with_article_results.extend(classify_industry_equipment_sales(articles))
    with_article_results.extend(classify_capacity_and_ownership(articles, stored_company_reference=stored_company_reference))
    with_article_results.extend(classify_corporate_disclosure(articles))

    # Most recent REAL progress_pct/typical_progress_pct per company, from
    # this same batch's own real J2 results - the "current reading" half
    # of progress_vs_usual that only exists tied to an actual filing (see
    # classify_press's own docstring on why this can't be looked up from
    # stored_progress_habits alone, which only has the TYPICAL half).
    # "Most recent" = latest real published date among this company's own
    # J2 results in this batch - a real, deterministic tie-break, not an
    # arbitrary pick when a company has more than one.
    latest_progress_by_code: dict[str, dict[str, Any]] = {}
    for r in j2_results:
        meta = r["result"]["metadata"]
        code = meta.get("code")
        progress_pct = meta.get("progress_pct")
        typical_pct = meta.get("progress_habit_typical_pct")
        if not code or progress_pct is None or typical_pct is None:
            continue
        published = (r["article"] or {}).get("published") or ""
        existing = latest_progress_by_code.get(code)
        if existing is None or published > existing["published"]:
            latest_progress_by_code[code] = {
                "published": published,
                "progress_pct": progress_pct,
                "typical_progress_pct": typical_pct,
            }

    with_article_results.extend(classify_press(
        articles, stored_habits=stored_habits, latest_progress_by_code=latest_progress_by_code,
    ))

    classified_articles = [r["article"] for r in with_article_results]
    translate_japan_articles(classified_articles)
    for r in with_article_results:
        meta = r["article"].get("metadata") or {}
        r["result"]["metadata"] = meta
        # Reason sentences are built before translation runs, so an
        # ownership row names its filer in Japanese - the only place
        # that name appears on the row at all. Now that the English
        # form exists, swap it in: a reader of the English card should
        # not have to parse 株式会社 to learn who filed.
        english = meta.get("translated_filer_name")
        japanese = meta.get("filer_name")
        if english and japanese and r["result"].get("reason"):
            r["result"]["reason"] = r["result"]["reason"].replace(japanese, english)

        # The same swap for an extraordinary report's major
        # shareholder, whose name reaches the sentence in katakana.
        msh_en = meta.get("translated_major_shareholder_name")
        msh_ja = meta.get("major_shareholder_name")
        if msh_en and msh_ja and r["result"].get("reason"):
            r["result"]["reason"] = r["result"]["reason"].replace(msh_ja, msh_en)

        # Same ordering problem for a press row: its reason quotes the
        # story straight from the article's own title, which is still
        # Japanese when classify_press builds the sentence. Swap in the
        # translated headline now that it exists, so the card's reason
        # is readable rather than the one field on an English card a
        # non-Japanese reader cannot use.
        translated = meta.get("translated_title")
        original = r["article"].get("title")
        if translated and original and r["result"].get("reason"):
            story = _press_story_fragment(original)
            english_story = _press_story_fragment(translated)
            if story and english_story and story != english_story:
                r["result"]["reason"] = r["result"]["reason"].replace(
                    story, english_story,
                )

    j3_results = classify_missing_revision(articles, as_of=as_of)
    results = with_article_results + j3_results

    # Runs over the finished result set rather than inside any one
    # classifier: the English-coverage gap belongs to the company's news
    # flow, so every classified item gets it, whichever rule produced
    # the row.
    _attach_english_coverage(results, articles)
    _attach_mentioned_customers(results)

    logger.info(
        "[JAPAN_SIGNAL_BATCH] %d article(s) pooled, %d classified (J1/J2/J4/J5/J6/J7: %d, J3: %d)",
        len(articles), len(results), len(with_article_results), len(j3_results),
    )
    return results


def compute_all_japan_habits(
    articles: list[dict[str, Any]],
    computed_from_years: int,
) -> list[dict[str, Any]]:
    """Compute every tracked company's forecast-revision habit from a
    pooled batch of jp_forecast articles (normally that company's FULL
    stored history, e.g. a 5-year pool - see refresh-japan-habits's own
    CLI docstring for how the caller assembles this).

    Pure function (no DB access, same boundary as every other pipeline/
    *.py module here) - controllers/run.py's refresh_japan_habits() is
    responsible for pooling the articles (via news-retrieval's own
    list_completed_runs/get_run_articles, a wide date window) and writing
    this function's return value via models.japan_company_habits.
    replace_habits().

    Returns one dict per japan_ticker_universe() company (code, company,
    plus every compute_forecast_habit field) - including companies with
    ZERO genuine revisions in the pooled articles (habit fields None,
    sample_size 0, is_trusted False) rather than omitting them, so
    replace_habits's delete-then-insert always produces one row per
    tracked company, never a silently-missing row for a company that
    happens to have no revisions in this particular pool.

    _MARGIN_BASED_CODES companies (Renesas Electronics) are a real
    exception to the above: compute_margin_revision_habit runs instead of
    compute_forecast_habit for these, and its own typical_gap_pts value
    is stored in this same row's typical_size_pct slot - see
    classify_margin_forecast_revision's own docstring for why this reuses
    japan_company_habits rather than a second table. revisions_per_year/
    typical_direction/typical_months are left None for this company's
    row, since no rule reads them for a margin-based company.
    """
    genuine_by_code: dict[str, list[dict[str, Any]]] = {t["code"]: [] for t in japan_ticker_universe()}
    margin_by_code: dict[str, list[dict[str, Any]]] = {t["code"]: [] for t in japan_ticker_universe()}
    for a in articles:
        meta = a.get("metadata") or {}
        if meta.get("source_category") != "jp_forecast":
            continue
        code = meta.get("code")
        if code in _MARGIN_BASED_CODES:
            if _is_margin_based_revision(a) and code in margin_by_code:
                margin_by_code[code].append(a)
            continue
        if not _is_genuine_revision(a):
            continue
        if code in genuine_by_code:
            genuine_by_code[code].append(a)

    habits: list[dict[str, Any]] = []
    for ticker in japan_ticker_universe():
        code = ticker["code"]
        if code in _MARGIN_BASED_CODES:
            margin_habit = compute_margin_revision_habit(margin_by_code[code])
            habit = {
                "revisions_per_year": None,
                "typical_size_pct": margin_habit["typical_gap_pts"],
                # No MAD computed for a margin-based company -
                # classify_margin_forecast_revision's own 2-rule chain
                # doesn't have a rule 6 equivalent to use it for (see
                # that function's own docstring) - explicit None, not
                # omitted, same "state the real gap" convention every
                # other margin-based field on this row already follows.
                "typical_size_mad_pct": None,
                "typical_direction": None,
                "typical_months": [],
                "sample_size": margin_habit["sample_size"],
                "is_trusted": margin_habit["is_trusted"],
            }
        else:
            habit = compute_forecast_habit(genuine_by_code[code])
        habits.append({
            "code": code,
            "company": ticker["company"],
            "computed_from_years": computed_from_years,
            **habit,
        })
    return habits
