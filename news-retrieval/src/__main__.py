"""Entry point for the News Aggregator service."""
import logging
import sys

import click
import uvicorn

from app import app
from db import init_db
from models.runs import fail_orphaned_runs
from seed import seed

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@click.group()
def cli() -> None:
    pass


@cli.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8000)
def serve(host: str, port: int) -> None:
    """Start the uvicorn server."""
    logger.info("Initialising database...")
    init_db()
    logger.info("Seeding database...")
    seed()
    cleaned = fail_orphaned_runs()
    if cleaned:
        logger.info("Cleaned up %d orphaned run(s).", cleaned)
    logger.info("Startup complete.")
    uvicorn.run(app, host=host, port=port)


@cli.command()
@click.option("--domain", default="company_news", show_default=True)
@click.option("--days-back", default=1, show_default=True)
@click.option(
    "--callback-url",
    default=None,
    help="If set, POST {run_id, status, domain, summary} to this URL when the "
    "run completes or fails (see controllers/run.py's _fire_webhook - "
    "fire-and-forget, no retry, no auth header). Lets a scheduled trigger "
    "notify a downstream consumer the moment the fetch is actually done, "
    "instead of the consumer guessing a fixed delay on its own schedule.",
)
def trigger(domain: str, days_back: int, callback_url: str | None) -> None:
    """Run the fetch pipeline for a domain and exit."""
    from controllers.run import RunRequest, create_run_record, run_pipeline

    init_db()
    seed()

    request = RunRequest(domain=domain, days_back=days_back, force=True, callback_url=callback_url)
    caller = {"role": "admin", "id": None}

    try:
        result = create_run_record(request, caller)
    except Exception as exc:
        logger.error("Failed to create run record: %s", exc)
        sys.exit(1)

    if result["cache_hit"]:
        logger.info("Cache hit — run_id=%d already completed today.", result["run_id"])
        sys.exit(0)

    logger.info("Starting pipeline — domain=%s run_id=%d", domain, result["run_id"])
    run_pipeline(result["run_id"], request)
    logger.info("Pipeline complete — run_id=%d", result["run_id"])


@cli.command("backfill-korea-customs")
@click.option(
    "--max-pages",
    default=None,
    type=int,
    help="Cap on how many pages of the Customs board's title-search "
    "results to walk. Defaults to pipeline.py's own "
    "_CUSTOMS_BACKFILL_MAX_PAGES (30) if not given - in practice the "
    "walk stops itself well before that once it detects the board's "
    "older stub-page format (see fetch_customs_export_backfill's own "
    "docstring), so this is a safety ceiling, not the expected page count.",
)
def backfill_korea_customs(max_pages: int | None) -> None:
    """One-time historical backfill for korea_market_signal's
    kr_customs_export source - walks the Customs board's own pagination
    to recover real historical semiconductor export figures, going
    beyond the regular 4-hourly poll's "newest release only" scope.

    Read/write against this service's own DB only (no other service
    involved) - safe to run independently of the regular trigger command,
    and safe to re-run (article storage is deduped by url, same as every
    other domain). See pipeline.fetch_customs_export_backfill's own
    docstring for what depth of real history this can actually recover
    (confirmed live 2026-09-23: roughly 7 months, not the full 2 years
    Korea Signals spec Section 5.1 asks for - the board's own detail-page
    format changes further back in its history, past which this scraper
    cannot extract anything).
    """
    import pipeline
    from models.articles import create_articles
    from models.runs import complete_run, create_run, fail_run

    init_db()
    seed()

    kwargs = {"max_pages": max_pages} if max_pages is not None else {}
    run_id = create_run(
        name="backfill-korea-customs",
        domain="korea_market_signal",
        days_back=0,
        max_articles=None,
        focus=None,
        model="none",
    )
    logger.info("Starting Korea Customs backfill — run_id=%d", run_id)
    try:
        articles = pipeline.fetch_customs_export_backfill(**kwargs)
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        fail_run(run_id, str(exc))
        sys.exit(1)

    all_articles = [{**art, "run_id": run_id} for art in articles]
    if all_articles:
        create_articles(all_articles)
    complete_run(run_id, len(articles))
    logger.info(
        "Korea Customs backfill complete — run_id=%d articles=%d",
        run_id, len(articles),
    )


@cli.command("backfill-japan-seaj")
def backfill_japan_seaj() -> None:
    """One-time historical backfill for japan_market_signal's
    seaj_billings source - reads SEAJ's own historical Excel archive
    (real monthly billings + YoY%, confirmed live 2026-09-29 to go back to
    2005) to recover real history beyond the regular monthly poll's
    "current PDF release only" scope (~6 trailing months).

    Deliberately NOT wired into the regular `trigger --domain
    japan_market_signal` path at all, and never should be - see
    pipeline.fetch_seaj_billings_backfill's own docstring for why running
    this repeatedly on a schedule would risk inserting a duplicate for any
    month the live PDF fetcher already owns (the Excel file has no
    prelim/final qualifier of its own, so a naive re-run could produce a
    second url for a month the live fetcher is still actively tracking).
    Safe to run once, and safe to RE-run after that one time (article
    storage is deduped by url, same as every other domain, and this
    function's own _SEAJ_BACKFILL_SKIP_RECENT_MONTHS margin keeps it away
    from the live fetcher's own active window on every run) - but it is
    not meant to be scheduled, only invoked deliberately when the stored
    seaj_billings history needs to be (re)built.
    """
    import pipeline
    from models.articles import create_articles
    from models.runs import complete_run, create_run, fail_run

    init_db()
    seed()

    run_id = create_run(
        name="backfill-japan-seaj",
        domain="japan_market_signal",
        days_back=0,
        max_articles=None,
        focus=None,
        model="none",
    )
    logger.info("Starting SEAJ billings backfill — run_id=%d", run_id)
    try:
        articles = pipeline.fetch_seaj_billings_backfill(sources=[{"placeholder": True}])
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        fail_run(run_id, str(exc))
        sys.exit(1)

    all_articles = [{**art, "run_id": run_id} for art in articles]
    if all_articles:
        create_articles(all_articles)
    complete_run(run_id, len(articles))
    logger.info(
        "SEAJ billings backfill complete — run_id=%d articles=%d",
        run_id, len(articles),
    )


@cli.command("backfill-taiwan-revenue")
@click.option("--from-month", default="2019-01", show_default=True,
              help="First month to read, YYYY-MM. Ignored if --months-back is given.")
@click.option("--to-month", default=None,
              help="Last month to read, YYYY-MM. Defaults to last calendar month (UTC).")
@click.option("--months-back", default=None, type=int,
              help="Read only the last N months up to --to-month - the scheduled "
              "monthly re-read uses this to recover missed filings and restatements.")
def backfill_taiwan_revenue(from_month: str, to_month: str | None, months_back: int | None) -> None:
    """Read Taiwan monthly revenue from MOPS's per-month CSVs into
    company_financials (taiwan_financials.py).

    One command for both jobs: the one-time history backfill (default
    2019-01 onward, ~186 requests, ~5 minutes) and the monthly re-read of
    recent months. Safe to re-run - rows UPSERT on (company, month), so a
    re-read replaces rather than duplicates. Exits non-zero if any
    exchange-month could not be fetched, so a gap is never silent.
    """
    from datetime import datetime, timezone

    from models.company_financials import upsert_financials
    from seed import TAIWAN_TICKER_UNIVERSE
    from taiwan_financials import DOMAIN, fetch_range

    init_db()

    def _ym(value: str) -> tuple[int, int]:
        year, month = (int(x) for x in value.split("-"))
        return year, month

    if to_month:
        end = _ym(to_month)
    else:
        now = datetime.now(timezone.utc)
        end = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
    if months_back:
        idx = end[0] * 12 + end[1] - 1 - (months_back - 1)
        start = (idx // 12, idx % 12 + 1)
    else:
        start = _ym(from_month)

    logger.info("[TW_FIN] reading MOPS revenue %d-%02d .. %d-%02d", *start, *end)
    rows, failed = fetch_range(start, end, TAIWAN_TICKER_UNIVERSE)
    written = upsert_financials(DOMAIN, rows)
    logger.info("[TW_FIN] stored %d monthly revenue row(s)", written)
    if failed:
        logger.error("[TW_FIN] could not fetch: %s", ", ".join(failed))
        sys.exit(1)


@cli.command("fetch-macro-signals")
@click.option(
    "--incremental",
    is_flag=True,
    default=False,
    help="Fetch only the last macro_signal_fetch.INCREMENTAL_LOOKBACK_DAYS "
    "(1) day per series, instead of the full 3-year backfill window. "
    "The mode for a scheduled run once the initial full backfill "
    "has been run once - stateless, no watermark lookup, just a fixed "
    "small window every run, upserted safely over whatever's stored.",
)
def fetch_macro_signals(incremental: bool) -> None:
    """Fetch macro_signal series (FRED + Treasury Fiscal Data), construct
    knowledge_time, upsert into macro_observations. Runs to completion
    and exits - same shape as backfill-korea-customs.

    Default (no --incremental): full 3-year lookback re-pull per series
    (macro_signal_fetch.FETCH_LOOKBACK_DAYS) - run this once for the
    initial backfill. --incremental: fixed 1-day fetch per series - run
    this on the twice-daily schedule after the initial backfill is done.

    Tracked in the same `runs` table every other domain uses (GET
    /runs?domain=macro_signal) via controllers.macro_run - NOT routed
    through POST /run's own create_run_record/run_pipeline (that path
    assumes an article-shaped domain registered in the `domains` table,
    which macro_signal is not), but the run lifecycle (create/complete/
    fail) is the same as every other domain's, for consistency. Requires
    FRED_API_KEY. See also: POST /macro/fetch, the HTTP-triggerable
    equivalent of this command (routes/macro.py)."""
    import os

    from controllers.macro_run import create_macro_fetch_run, run_macro_fetch
    from models.runs import get_run

    init_db()

    fred_api_key = os.environ.get("FRED_API_KEY")
    if not fred_api_key:
        logger.error("FRED_API_KEY is not set")
        sys.exit(1)

    run_id = create_macro_fetch_run(incremental=incremental)
    logger.info("Starting macro_signal fetch run_id=%d (46 series, incremental=%s)", run_id, incremental)
    run_macro_fetch(run_id, fred_api_key, incremental=incremental)

    # run_macro_fetch itself never raises (fail_run absorbs the error so
    # the run row always reaches a terminal status) - check that status
    # here so this CLI invocation still exits non-zero on failure, same
    # as every other one-shot fetch command in this file (CloudWatch/CI
    # failure detection depends on the exit code, not just the DB row).
    final_run = get_run(run_id)
    if final_run and final_run["status"] == "failed":
        logger.error("Macro signal fetch failed: %s", final_run.get("summary"))
        sys.exit(1)


@cli.command("expire-articles")
@click.option("--domain", required=True, help="Domain slug to expire articles for, e.g. geopolitical_news.")
@click.option("--days", default=7, show_default=True, help="Delete articles published more than this many days ago.")
def expire_articles(domain: str, days: int) -> None:
    """Delete old articles for one domain. Runs to completion and exits."""
    from models.articles import expire_articles_for_domain

    init_db()
    deleted = expire_articles_for_domain(domain, days)
    logger.info("[EXPIRE] domain=%s days=%d deleted=%d", domain, days, deleted)


@cli.command("fail-stuck-runs")
@click.option(
    "--max-hours", default=3, show_default=True,
    help="Mark any run still 'running' after this many hours as failed. "
    "Meant to run on its own schedule (independent of server restarts) - "
    "confirmed live (2026-09-25, run_id=472) that a scheduled fetch can "
    "die silently with no exception/traceback and leave its run row stuck "
    "in 'running' indefinitely, since nothing else revisits it once the "
    "process is gone.",
)
def fail_stuck_runs_cmd(max_hours: int) -> None:
    """Clean up any run stuck in 'running' past the given age. Runs to
    completion and exits."""
    from models.runs import fail_stuck_runs

    init_db()
    failed_ids = fail_stuck_runs(max_hours)
    logger.info("[WATCHDOG] max_hours=%d marked_failed=%s", max_hours, failed_ids)


@cli.command("poll-market")
@click.option(
    "--mode",
    type=click.Choice(["quotes", "daily", "sec_filings"]),
    required=True,
    help=(
        "quotes: price/indices/status every 15 min. daily: overview/earnings/history once a day. "
        "sec_filings: 8-K/10-Q/10-K metadata + link once a day."
    ),
)
@click.option(
    "--tickers",
    default=None,
    help="Comma-separated ticker list. Falls back to MARKET_POLL_TICKERS env var.",
)
def poll_market(mode: str, tickers: str | None) -> None:
    """Fetch market data from Alpha Vantage / SEC EDGAR and write to DynamoDB."""
    import os
    from poller import run_daily, run_quotes, run_sec_filings

    from pipeline import get_tracked_ticker_universe
    if tickers:
        ticker_list = [t.strip().upper() for t in tickers.split(",") if t.strip()]
    else:
        universe_url = os.environ.get("RESEARCH_UNIVERSE_URL")
        universe_api_key = os.environ.get("RESEARCH_UNIVERSE_API_KEY")
        ticker_list = get_tracked_ticker_universe(universe_url, universe_api_key)
    logger.info("[POLLER] tickers=%d", len(ticker_list))

    if mode == "sec_filings":
        run_sec_filings(ticker_list)
        return

    av_key = os.environ.get("ALPHA_VANTAGE_API_KEY", "")
    if not av_key:
        logger.error("ALPHA_VANTAGE_API_KEY is not set")
        sys.exit(1)

    if mode == "daily":
        run_daily(ticker_list, av_key)
    else:
        run_quotes(ticker_list, av_key)


if __name__ == "__main__":
    cli()
