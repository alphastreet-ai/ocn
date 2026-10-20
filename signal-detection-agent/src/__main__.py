"""Entry point for the signal-detection-agent service."""
import logging

import click
import uvicorn

from db import init_db
from seed import seed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    """signal-detection-agent service CLI."""


@cli.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8003)
def serve(host: str, port: int) -> None:
    """Start the uvicorn server."""
    from app import app

    logger.info("Initialising database...")
    init_db()
    logger.info("Seeding database...")
    seed()
    logger.info("Startup complete.")
    uvicorn.run(app, host=host, port=port)


@cli.command("classify-filings")
@click.option(
    "--tickers",
    default=None,
    help="Comma-separated ticker subset for a scoped manual test run "
    "(e.g. AAPL,MSFT). Omit for the normal full tracked-universe run "
    "used by the daily CloudWatch schedule.",
)
def classify_filings(tickers: str | None) -> None:
    """One-shot: fetch tracked tickers' SEC filings, classify what's new, persist.

    Entry point for the daily CloudWatch-triggered scheduled task - runs to
    completion and exits (not a server), mirroring the shape of
    news-retrieval's `poll-market --mode sec_filings` command.
    """
    import asyncio

    from controllers.filing_run import run_filing_classification_job, submit_filing_run

    logger.info("Initialising database...")
    init_db()
    seed()

    ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()] if tickers else None
    job_id = submit_filing_run()
    logger.info("Created SEC filing job_id=%s tickers=%s", job_id, ticker_list or "all-tracked")
    asyncio.run(run_filing_classification_job(job_id, tickers=ticker_list))
    logger.info("SEC filing job_id=%s finished", job_id)


@cli.command("backfill-filing-tickers")
def backfill_filing_tickers_cmd() -> None:
    """One-time migration: add metadata.ticker to sec_filing rows written
    before this field existed. Read-only against news-retrieval and EDGAR
    (no filing text/LLM calls) - safe to run independently of classify-filings.
    """
    import asyncio

    from controllers.filing_run import run_ticker_backfill

    logger.info("Initialising database...")
    init_db()
    seed()

    asyncio.run(run_ticker_backfill())
    logger.info("Ticker backfill finished")


@cli.command("classify-taiwan-signals")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC) - i.e. classify all of today's completed "
    "taiwan_market_signal runs so far.",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC).",
)
@click.option(
    "--revenue-period",
    default=None,
    help="Backfill only: classify revenue for this period (YYYY-MM) only. "
    "With either backfill option set, gdelt and out-of-scope items are skipped.",
)
@click.option(
    "--material-month",
    default=None,
    help="Backfill only: classify material announcements filed in this month (YYYY-MM) only.",
)
def classify_taiwan_signals(
    from_date: str | None,
    to_date: str | None,
    revenue_period: str | None,
    material_month: str | None,
) -> None:
    """One-shot: pool today's completed taiwan_market_signal news-retrieval
    runs, rank/classify/translate, persist. Entry point for the twice-daily
    CloudWatch-triggered scheduled task (post-Asia-close and pre-US-open) -
    runs to completion and exits (not a server).
    """
    import asyncio
    from datetime import datetime, timezone

    import config
    from controllers.run import run_taiwan_signal_classification
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = from_date or today
    to_date = to_date or today

    job_id = create_job(domain=config.TAIWAN_SIGNAL_DOMAIN)
    logger.info(
        "Created taiwan_market_signal job_id=%s from_date=%s to_date=%s",
        job_id, from_date, to_date,
    )
    asyncio.run(run_taiwan_signal_classification(
        job_id, from_date, to_date,
        revenue_period=revenue_period, material_month=material_month,
    ))
    logger.info("taiwan_market_signal job_id=%s finished", job_id)


@cli.command("run-macro-signal-pipeline")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of observation dates to tier/collapse/"
    "interpret this run. Defaults to yesterday (UTC), or today (UTC) if "
    "--today is passed.",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of observation dates to tier/collapse/"
    "interpret this run. Defaults to yesterday (UTC), or today (UTC) if "
    "--today is passed.",
)
@click.option(
    "--today",
    is_flag=True,
    default=False,
    help="Evaluate today's UTC date instead of yesterday's (the plain "
    "default) when --from-date/--to-date are omitted. Ignored if "
    "--from-date/--to-date are given explicitly. For the twice-daily "
    "schedule's after-close pass (~22:00 UTC) - by then a market-close "
    "series' TODAY observation already exists (16:30 ET stamp), so "
    "evaluating yesterday only would miss a same-day move until the "
    "next day's run. The plain default (no flag) stays correct for the "
    "next-morning catch-up pass, which still wants yesterday's tail-end "
    "data, not the day that's only just started in UTC.",
)
def run_macro_signal_pipeline_cmd(from_date: str | None, to_date: str | None, today: bool) -> None:
    """One-shot: pull macro_observations from news-retrieval (full
    z-score window, not just [from_date, to_date] - see
    run_macro_signal_pipeline's own docstring), run
    Z-SCORE->TIER->SUPPRESS->COLLAPSE->INTERPRET for observation dates in
    [from_date, to_date], persist. Entry point for the CloudWatch-triggered
    scheduled task (twice daily - see infra's own schedule comments) -
    runs to completion and exits.
    """
    import asyncio
    from datetime import datetime, timedelta, timezone

    import config
    from controllers.run import run_macro_signal_pipeline
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    default_offset_days = 0 if today else 1
    default_date = (datetime.now(timezone.utc) - timedelta(days=default_offset_days)).strftime("%Y-%m-%d")
    from_date = from_date or default_date
    to_date = to_date or default_date

    job_id = create_job(domain=config.MACRO_SIGNAL_DOMAIN)
    logger.info(
        "Created macro_signal job_id=%s from_date=%s to_date=%s",
        job_id, from_date, to_date,
    )
    asyncio.run(run_macro_signal_pipeline(job_id, from_date, to_date))
    logger.info("macro_signal job_id=%s finished", job_id)


@cli.command("recheck-macro-signal-confirmations")
@click.option(
    "--since-date",
    default=None,
    help="Recheck already-stored macro_signal rows published on or after "
    "this date (YYYY-MM-DD) for confirmation reversal. Defaults to 60 "
    "days ago - generous enough to cover even a monthly series' full "
    "confirmation window (its next print can land up to ~4 weeks after "
    "the original event) with margin.",
)
def recheck_macro_signal_confirmations_cmd(since_date: str | None) -> None:
    """One-shot: re-examines already-stored macro_signal events/audit
    rows for weekly/monthly series against newly-arrived next-
    observations, retroactively marks confirmation-reversed events as
    suppressed (mechanism C - see pipeline/macro_signal_confirmation.py
    and pipeline/macro_signal_frequency.py). Separate schedule from the
    main daily pipeline since this is retroactive - a monthly series'
    reversal can only be confirmed up to ~4 weeks after the original
    event was stored. Runs to completion and exits.
    """
    import asyncio
    from datetime import datetime, timedelta, timezone

    import config
    from controllers.run import run_macro_signal_confirmation_recheck
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    since_date = since_date or (datetime.now(timezone.utc) - timedelta(days=60)).strftime("%Y-%m-%d")

    job_id = create_job(domain=config.MACRO_SIGNAL_DOMAIN)
    logger.info("Created macro_signal confirmation-recheck job_id=%s since_date=%s", job_id, since_date)
    asyncio.run(run_macro_signal_confirmation_recheck(job_id, since_date))
    logger.info("macro_signal confirmation-recheck job_id=%s finished", job_id)


@cli.command("refresh-fomc-calendar")
def refresh_fomc_calendar_cmd() -> None:
    """One-shot: fetches the Fed's own published FOMC meeting calendar
    (federalreserve.gov/json/calendar.json) and upserts it into
    fomc_meeting_dates, so DFF's inter-meeting move detection (see
    pipeline/macro_signal_fomc_calendar.py) always has a current, real
    calendar to check against. Meant to run on its own recurring
    schedule (CloudWatch), independent of the daily pipeline run -
    the calendar itself only needs refreshing when the Fed publishes
    new dates, not every pipeline run.
    """
    from pipeline.macro_signal_fomc_calendar import refresh_fomc_meeting_dates

    logger.info("Initialising database...")
    init_db()
    seed()

    count = refresh_fomc_meeting_dates()
    logger.info("refresh-fomc-calendar: upserted %d FOMC meeting dates", count)


@cli.command("classify-korea-signals")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC) - i.e. classify all of today's completed "
    "korea_market_signal runs so far.",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC).",
)
def classify_korea_signals(from_date: str | None, to_date: str | None) -> None:
    """One-shot: pool today's completed korea_market_signal news-retrieval
    runs, classify (S2-S7)/translate, persist. Entry point for the scheduled
    task - runs to completion and exits (not a server).
    """
    import asyncio
    from datetime import datetime, timezone

    import config
    from controllers.run import run_korea_signal_classification
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = from_date or today
    to_date = to_date or today

    job_id = create_job(domain=config.KOREA_SIGNAL_DOMAIN)
    logger.info(
        "Created korea_market_signal job_id=%s from_date=%s to_date=%s",
        job_id, from_date, to_date,
    )
    asyncio.run(run_korea_signal_classification(job_id, from_date, to_date))
    logger.info("korea_market_signal job_id=%s finished", job_id)


@cli.command("refresh-china-baselines")
@click.option(
    "--years",
    default=5,
    show_default=True,
    help="How many years of stored history to pool. Five, matching the "
    "window classify-china-signals rebuilds from, so a manual refresh "
    "and a scheduled one produce the same baselines. Asking for more "
    "than exists is free - the window selects completed runs, and a "
    "year with none contributes nothing and costs nothing. Most of "
    "this universe cannot fill it either way: five of the twenty "
    "companies listed in 2022 or later, so their entire filing history "
    "is four years or less (an eight-year Hygon fetch returned exactly "
    "what a four-year fetch already had).",
)
@click.option(
    "--refetch",
    is_flag=True,
    default=False,
    help="Re-fetch revenue figures from cninfo and Alpha Vantage before "
    "recomputing, instead of using what news-retrieval has stored. Slow "
    "(one request per company) and only worth doing when companies have "
    "filed since the last fetch - monthly is more than enough.",
)
def refresh_china_baselines_cmd(years: int, refetch: bool) -> None:
    """Recompute every company's C2 baseline from the full stored
    history and overwrite the cache.

    Occasional, not per-classification-pass. The daily
    classify-china-signals run pools a single day of filings, which
    cannot rebuild a company's own trailing distribution - without this
    cache it issues no C2 verdict at all, while the same code over a
    multi-year backfill produces signals. Same split Japan already uses
    between refresh-japan-habits and classify-japan-signals.
    """
    import asyncio
    from datetime import datetime, timedelta, timezone

    from controllers.run import refresh_china_baselines

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc)
    from_date = (today - timedelta(days=365 * years)).strftime("%Y-%m-%d")
    to_date = today.strftime("%Y-%m-%d")
    logger.info("Refreshing china baselines from %s to %s", from_date, to_date)
    written = asyncio.run(
        refresh_china_baselines(from_date, to_date, refetch=refetch))
    logger.info("china baselines refreshed: %d series written", written)


@cli.command("classify-china-signals")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC) - i.e. classify all of today's completed "
    "china_market_signal runs so far.",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC).",
)
def classify_china_signals(from_date: str | None, to_date: str | None) -> None:
    """One-shot: pool today's completed china_market_signal
    news-retrieval runs, classify, persist. Entry point for the
    scheduled task - runs to completion and exits (not a server).

    Runs Gate 1 filing triage, all seven signal types (C1-C7), the
    read-through to US tickers, the WOULD CONFIRM / WOULD CONTRADICT
    pass and translation. C2-C6 were unimplemented while their
    thresholds had no stored distribution to derive from; they are now
    built on a MAD baseline computed from real accumulated data, with
    trust floors (_C2_MIN_OBSERVATIONS, _C6_MIN_PERIODS) that make the
    classifier decline to judge rather than invent a baseline. See
    pipeline/china_signal_classifier.py's own docstring.

    Scheduled twice daily (10:00 and 14:00 UTC, MON-FRI). Defaults to
    today (UTC) and skips already-classified source_ids, so the two
    passes are additive rather than duplicative.

    REBUILDS THE BASELINES FIRST, in the same process, immediately
    before classifying. A company filing its annual report on 1 March
    must be judged against a baseline that includes everything it had
    filed up to that morning - not against one last rebuilt weeks
    earlier. The two halves cannot be separate scheduled rules: a
    verdict is written once and the next pass skips that source_id, so
    a filing judged while the cache was stale keeps that verdict
    permanently, and nothing revisits it when the cache catches up.
    Ordering them in one process is what makes that impossible.

    Cheap enough to do every pass - measured at ~5 seconds over the
    9,176 articles currently stored, writing 83 series. The expensive
    half is the upstream revenue re-fetch (~90s, one request per
    company), which is why that stays on its own monthly rule and this
    reads the figures news-retrieval has already stored.
    """
    import asyncio
    from datetime import datetime, timedelta, timezone

    import config
    from controllers.run import (
        refresh_china_baselines, run_china_signal_classification,
    )
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = from_date or today
    to_date = to_date or today

    # The baseline window is deliberately NOT the classify window. C2
    # and C3 judge a figure against that company's own past figures, so
    # the baseline needs years of history where the classify pass wants
    # only today's unjudged filings.
    #
    # FIVE years, matching the Japan spec's own Section 9 Step 1
    # history requirement. The binding constraint on these baselines is
    # sample size, not the band: 63 of 65 C2 baselines have n <= 6 and
    # most have n = 4, which is why a single dropped observation moves
    # Hygon's own threshold between +87% and +176%. A wider window is
    # the only thing that genuinely improves that, so it is set as wide
    # as the stored runs allow rather than trimmed to what is currently
    # populated.
    #
    # It costs nothing to ask for more than exists. The window selects
    # completed news-retrieval runs, so a year with no stored runs
    # contributes no articles and no time - the measured ~5s is over
    # everything currently stored, and widening the request does not
    # re-fetch anything.
    _BASELINE_HISTORY_YEARS = 5
    baseline_from = (
        datetime.now(timezone.utc)
        - timedelta(days=365 * _BASELINE_HISTORY_YEARS)
    ).strftime("%Y-%m-%d")
    try:
        written = asyncio.run(refresh_china_baselines(
            baseline_from, to_date, refetch=False,
            # Re-pull upstream revenue only for companies that filed a
            # periodic report inside the CLASSIFY window. The baseline
            # window is five years, in which everyone has filed
            # something; what makes a company's stored figures stale is
            # having filed since they were last pulled. On a normal day
            # this is nobody and nothing is fetched.
            refetch_filed_since=from_date))
        logger.info("china baselines refreshed: %d series written", written)
    except Exception:
        # A failed refresh must not stop the classify pass. The cache
        # from the previous run is still there and still better than
        # nothing - load_baselines reads whatever is stored, and C2
        # falls back to its own trust floor where a company has no row
        # at all. Classifying against a slightly stale baseline beats
        # not classifying the day's filings.
        logger.exception(
            "china baseline refresh failed - classifying against the "
            "previously cached baselines instead")

    job_id = create_job(domain=config.CHINA_SIGNAL_DOMAIN)
    logger.info(
        "Created china_market_signal job_id=%s from_date=%s to_date=%s",
        job_id, from_date, to_date,
    )
    asyncio.run(run_china_signal_classification(job_id, from_date, to_date))
    logger.info("china_market_signal job_id=%s finished", job_id)


@cli.command("summarize-korea-signals")
@click.option(
    "--date",
    default=None,
    help="Date (YYYY-MM-DD, UTC) of classified korea_market_signal rows to "
    "summarize. Defaults to today (UTC).",
)
def summarize_korea_signals(date: str | None) -> None:
    """One-shot: read today's already-classified korea_market_signal rows
    from this service's own DB and generate the spec Section 8.4 trader
    summary text. No news-retrieval fetch, no classification - reads only
    (see generate_korea_signal_summary_for_date's own docstring). Runs to
    completion and exits (not a server) - same shape as classify-korea-signals.

    Output goes to the log only (INFO level) - there is no email/delivery
    mechanism wired up yet (see korea_signal_summary.py's own module
    docstring on scope). GET /korea-signals/summary (routes/jobs.py) is
    the on-demand equivalent of this same call, for a caller that wants
    the text back directly rather than reading CloudWatch logs.
    """
    from datetime import datetime, timezone

    from controllers.run import generate_korea_signal_summary_for_date

    logger.info("Initialising database...")
    init_db()
    seed()

    date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")
    summary = generate_korea_signal_summary_for_date(date)
    logger.info("[KOREA_SIGNAL_SUMMARY] date=%s\n%s", date, summary)


@cli.command("classify-japan-signals")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC) - i.e. classify all of today's completed "
    "japan_market_signal runs so far.",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC).",
)
def classify_japan_signals(from_date: str | None, to_date: str | None) -> None:
    """One-shot: pool today's completed japan_market_signal news-retrieval
    runs, classify (J1 only, so far)/translate, persist. Entry point for
    the scheduled task - runs to completion and exits (not a server).

    Compares each new revision against the company's own PRE-COMPUTED
    habit (models.japan_company_habits, refreshed separately by
    refresh-japan-habits below) - not a fixed threshold, and not
    recomputed from this narrow window's own articles.
    """
    import asyncio
    from datetime import datetime, timezone

    import config
    from controllers.run import run_japan_signal_classification
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = from_date or today
    to_date = to_date or today

    job_id = create_job(domain=config.JAPAN_SIGNAL_DOMAIN)
    logger.info(
        "Created japan_market_signal job_id=%s from_date=%s to_date=%s",
        job_id, from_date, to_date,
    )
    asyncio.run(run_japan_signal_classification(job_id, from_date, to_date))
    logger.info("japan_market_signal job_id=%s finished", job_id)


@cli.command("refresh-japan-habits")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of the news-retrieval run window to pool "
    "for habit computation - should span the FULL stored history (e.g. 10 "
    "years back), not a narrow recent window. Defaults to 3650 days "
    "(10 years) before today (UTC).",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC).",
)
def refresh_japan_habits_command(from_date: str | None, to_date: str | None) -> None:
    """One-shot: recompute all THREE stored reference caches from the full
    stored history in one pooled pass and overwrite them - J1's
    forecast-revision habit (revisions/year, typical size, direction,
    typical months; japan_company_habits table), J2's progress-vs-target
    habit (typical progress per (code, period_type) pair;
    japan_progress_habits table), and J5/J6's company reference facts
    (total assets, shares outstanding; japan_company_reference table).
    The first two read from the exact same pooled jp_forecast articles;
    the third reads from the same pooled window's jp_company_reference
    articles instead (see controllers.run.refresh_japan_habits's own
    docstring on why all three are refreshed together, not as separate
    CLI commands/schedules) - explicit user instruction 2026-09-28: these
    numbers are computed once and stored, not recomputed inline on every
    classification pass.

    Intended to run occasionally (e.g. monthly via a separate CloudWatch
    schedule), not on every classify-japan-signals pass - a company's real
    multi-year habit does not meaningfully change day to day. Runs to
    completion and exits (not a server), same shape as
    classify-japan-signals.
    """
    import asyncio
    from datetime import datetime, timedelta, timezone

    from controllers.run import refresh_japan_habits

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc)
    to_date = to_date or today.strftime("%Y-%m-%d")
    from_date = from_date or (today - timedelta(days=3650)).strftime("%Y-%m-%d")
    computed_from_years = round((today - datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)).days / 365)

    logger.info(
        "Refreshing japan_company_habits from_date=%s to_date=%s (~%d years)",
        from_date, to_date, computed_from_years,
    )
    count = asyncio.run(refresh_japan_habits(from_date, to_date, computed_from_years))
    logger.info("refresh-japan-habits: wrote %d company habit(s)", count)


@cli.command("classify-geopolitical-signals")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC).",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of the news-retrieval run window to pool. "
    "Defaults to today (UTC).",
)
def classify_geopolitical_signals(from_date: str | None, to_date: str | None) -> None:
    """One-shot: pool completed geopolitical_news news-retrieval runs in the
    given window, apply Stage A's free rule-based filters, persist. Stage A
    only - no model call. Runs to completion and exits (not a server).
    """
    import asyncio
    from datetime import datetime, timezone

    import config
    from controllers.run import run_geopolitical_signal_stage_a
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = from_date or today
    to_date = to_date or today

    job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info(
        "Created geopolitical_signal job_id=%s from_date=%s to_date=%s",
        job_id, from_date, to_date,
    )
    asyncio.run(run_geopolitical_signal_stage_a(job_id, from_date, to_date))
    logger.info("geopolitical_signal job_id=%s finished", job_id)


@cli.command("classify-geopolitical-signals-stage-b")
def classify_geopolitical_signals_stage_b() -> None:
    """One-shot: classify every geopolitical_signal row currently
    signal_detection='waiting' as HIGH/WEAK via one model call each,
    update in place. No news-retrieval fetch - reads only from this
    service's own DB. Runs to completion and exits (not a server).
    """
    import asyncio

    import config
    from controllers.run import run_geopolitical_signal_stage_b
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage B job_id=%s", job_id)
    asyncio.run(run_geopolitical_signal_stage_b(job_id))
    logger.info("geopolitical_signal Stage B job_id=%s finished", job_id)


@cli.command("refresh-geopolitical-category-map")
def refresh_geopolitical_category_map() -> None:
    """One-shot: fetch the current US-listed company universe from
    research-universe, rebuild Stage C's category->ticker cache. Runs on
    its own schedule (independent of Stage C's own runs) - Stage C only
    ever reads this cache, never calls research-universe directly.
    """
    import asyncio

    from controllers.category_map_refresh import refresh_category_ticker_map

    logger.info("Initialising database...")
    init_db()
    seed()

    counts = asyncio.run(refresh_category_ticker_map())
    logger.info("Category map refreshed: %s", counts)


@cli.command("classify-geopolitical-signals-stage-c")
def classify_geopolitical_signals_stage_c() -> None:
    """One-shot: tag every untagged HIGH geopolitical_signal row - channel,
    actors, assets, impacted_categories, impacted companies, summary.
    No news-retrieval fetch - reads only from this service's own DB and
    its cached company data. Runs to completion and exits (not a server).
    """
    import asyncio

    import config
    from controllers.run import run_geopolitical_signal_stage_c
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage C job_id=%s", job_id)
    asyncio.run(run_geopolitical_signal_stage_c(job_id))
    logger.info("geopolitical_signal Stage C job_id=%s finished", job_id)


@cli.command("classify-geopolitical-signals-stage-d")
def classify_geopolitical_signals_stage_d() -> None:
    """One-shot: grade every Stage-C-tagged HIGH geopolitical_signal row
    into TOP/STRONG/STANDARD (corroboration, primary-source, specificity -
    see pipeline/geopolitical_signal_stage_d.py). No model call; one
    news-retrieval call per row (GET /articles/{id}) for
    also_reported_by. Runs to completion and exits (not a server).
    """
    import asyncio

    import config
    from controllers.run import run_geopolitical_signal_stage_d
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage D job_id=%s", job_id)
    asyncio.run(run_geopolitical_signal_stage_d(job_id))
    logger.info("geopolitical_signal Stage D job_id=%s finished", job_id)


@cli.command("classify-geopolitical-signals-stage-e")
def classify_geopolitical_signals_stage_e() -> None:
    """One-shot: group Stage-D-graded signals into developing STORIES.

    No model call, no network call - groups on the entities Stage C
    already verified against the article text.

    Writes metadata.story_development on each story's seed row and
    metadata.in_story on the others. Nothing is deleted: one row per
    story is a filter the caller applies -

        signal_detection = 'signal' AND NOT (metadata ? 'in_story')

    Safe to re-run: it clears its own keys and regroups from scratch,
    because which story a row belongs to can change when a later row
    arrives. Runs to completion and exits (not a server).
    """
    import asyncio

    import config
    from controllers.run import run_geopolitical_signal_stage_e
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage E job_id=%s", job_id)
    asyncio.run(run_geopolitical_signal_stage_e(job_id))
    logger.info("geopolitical_signal Stage E job_id=%s finished", job_id)


@cli.command("run-geopolitical-signal-pipeline")
@click.option(
    "--from-date",
    default=None,
    help="Start date (YYYY-MM-DD) of the news-retrieval run window Stage A "
    "pools. Defaults to today (UTC).",
)
@click.option(
    "--to-date",
    default=None,
    help="End date (YYYY-MM-DD) of the news-retrieval run window Stage A "
    "pools. Defaults to today (UTC).",
)
def run_geopolitical_signal_pipeline(from_date: str | None, to_date: str | None) -> None:
    """One-shot: Stage A, then B, then C, then D, then E, in sequence in
    one process. Stages A-D each only ever act on rows the previous stage
    already wrote (A creates rows -> B classifies A's 'waiting' rows -> C
    tags B's 'signal'/HIGH rows -> D grades C's tagged rows), so running
    them as separately-scheduled triggers risked a later stage firing
    before the prior stage's run had finished. A single combined command
    removes that race entirely - the stages are inherently sequential,
    not independent jobs.

    Stage E runs last and is NOT skippable here, for a different reason:
    every API read of this domain is expected to be post-Stage-E, since
    the default signals view filters on NOT (metadata ? 'in_story'). If E
    never runs, that filter matches every row and a reader sees one story
    repeated under several headlines. E is also not scoped to the rows D
    just wrote - grouping is a property of the whole signal set, so it
    re-reads every graded signal and regroups from scratch, clearing its
    own two keys first. Runs to completion and
    exits (not a server).
    """
    import asyncio
    from datetime import datetime, timezone

    import config
    from controllers.run import (
        run_geopolitical_signal_stage_a,
        run_geopolitical_signal_stage_b,
        run_geopolitical_signal_stage_c,
        run_geopolitical_signal_stage_d,
        run_geopolitical_signal_stage_e,
    )
    from models.jobs import create_job

    logger.info("Initialising database...")
    init_db()
    seed()

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    from_date = from_date or today
    to_date = to_date or today

    stage_a_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info(
        "Created geopolitical_signal Stage A job_id=%s from_date=%s to_date=%s",
        stage_a_job_id, from_date, to_date,
    )
    asyncio.run(run_geopolitical_signal_stage_a(stage_a_job_id, from_date, to_date))
    logger.info("geopolitical_signal Stage A job_id=%s finished", stage_a_job_id)

    stage_b_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage B job_id=%s", stage_b_job_id)
    asyncio.run(run_geopolitical_signal_stage_b(stage_b_job_id))
    logger.info("geopolitical_signal Stage B job_id=%s finished", stage_b_job_id)

    stage_c_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage C job_id=%s", stage_c_job_id)
    asyncio.run(run_geopolitical_signal_stage_c(stage_c_job_id))
    logger.info("geopolitical_signal Stage C job_id=%s finished", stage_c_job_id)

    stage_d_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage D job_id=%s", stage_d_job_id)
    asyncio.run(run_geopolitical_signal_stage_d(stage_d_job_id))
    logger.info("geopolitical_signal Stage D job_id=%s finished", stage_d_job_id)

    stage_e_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
    logger.info("Created geopolitical_signal Stage E job_id=%s", stage_e_job_id)
    asyncio.run(run_geopolitical_signal_stage_e(stage_e_job_id))
    logger.info("geopolitical_signal Stage E job_id=%s finished", stage_e_job_id)


@cli.command("expire-classifications")
@click.option("--source-type", required=True, help="source_type to expire, e.g. news or sec_filing.")
@click.option("--days", default=30, show_default=True, help="Delete classifications published more than this many days ago.")
def expire_classifications(source_type: str, days: int) -> None:
    """Delete old classifications for one source_type. Runs to completion and exits."""
    from models.jobs import expire_classifications_for_source_type

    init_db()
    deleted = expire_classifications_for_source_type(source_type, days)
    logger.info("[EXPIRE] source_type=%s days=%d deleted=%d", source_type, days, deleted)


if __name__ == "__main__":
    cli()
