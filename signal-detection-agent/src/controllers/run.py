"""Orchestration - fetch articles from news-retrieval, classify via LLM, persist."""
from __future__ import annotations

import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any

import config
from adapters.research_universe_client import ResearchUniverseError, fetch_us_listed_companies
from adapters.news_client import (
    NewsRetrievalError,
    fetch_latest_run,
    get_article,
    get_macro_observations,
    get_run_articles,
    list_completed_runs,
    poll_run_until_done,
    trigger_macro_fetch,
    trigger_run,
    fetch_china_revenue_history,
    fetch_middle_east_universe,
    fetch_taiwan_revenue_history,
)
from models.geopolitical_signal_companies import get_companies_for_name_matching
from models.jobs import (
    count_geopolitical_signal_stage_b_calls_today,
    count_geopolitical_signal_stage_c_calls_today,
    create_job,
    get_completed_job_for_run,
    get_existing_geopolitical_signal_article_ids,
    get_existing_japan_signal_source_ids,
    get_existing_china_signal_source_ids,
    get_existing_korea_signal_source_ids,
    get_existing_macro_signal_source_ids,
    get_existing_middle_east_signal_source_ids,
    get_middle_east_event_members,
    get_middle_east_story_candidates,
    clear_middle_east_story_fields,
    apply_middle_east_story,
    collapse_middle_east_same_event,
    get_existing_taiwan_source_ids,
    get_taiwan_recent_remarks,
    get_taiwan_remark_translations,
    get_geopolitical_signal_event_members,
    get_ungraded_geopolitical_signal_tagged_articles,
    get_untagged_geopolitical_signal_high_articles,
    get_waiting_geopolitical_signal_articles,
    insert_geopolitical_signal_classification,
    insert_japan_signal_classification,
    insert_china_signal_classification,
    insert_korea_signal_classification,
    insert_macro_signal_event,
    insert_middle_east_signal_classification,
    insert_taiwan_signal_classification,
    list_all_results,
    list_macro_signal_events_for_confirmation_recheck,
    mark_macro_signal_event_suppressed,
    apply_geopolitical_signal_story,
    clear_geopolitical_signal_story_fields,
    collapse_geopolitical_signal_event_group,
    get_geopolitical_signal_story_candidates,
    update_geopolitical_signal_classification,
    update_geopolitical_signal_stage_c_tags,
    update_geopolitical_signal_stage_d_grade,
    update_job_status,
)
from models.japan_company_habits import get_all_habits, replace_habits
from models.japan_company_reference import get_all_company_reference, replace_company_reference
from models.japan_progress_habits import get_all_progress_habits, replace_progress_habits
from pipeline.classifier import has_usable_body
from pipeline.dispatch import get_domain_config, known_domains
from pipeline.geopolitical_signal_classifier import classify_geopolitical_signal_stage_a
from pipeline.geopolitical_signal_layer1 import find_direct_company_matches
from pipeline.geopolitical_signal_layer2 import (
    classify_geopolitical_signal_tags,
    resolve_against_universe,
)
from pipeline.geopolitical_signal_overrides import apply_overrides
from pipeline.geopolitical_signal_stage_b import (
    classify_geopolitical_signal_headline,
    load_stage_b_prompt,
)
from pipeline.geopolitical_signal_stage_d import build_corroboration
from pipeline.geopolitical_signal_stage_e import (
    build_stories,
    story_development,
)
from pipeline.japan_signal_classifier import (
    _article_published_dt,
    classify_japan_signal_batch,
    classify_stale_revision_pattern,
    compute_all_japan_habits,
    compute_all_japan_progress_habits,
)
from pipeline.japan_companies import japan_ticker_universe
from pipeline.china_signal_classifier import (
    FILING_RANK_PERIODIC, classify_china_signal_batch, compute_c2_baselines,
    compute_c3_baselines, compute_c5_baselines, filing_authority,
    filing_period)
from models.market_baselines import load_baselines, upsert_baselines
from pipeline.middle_east_signal_classifier import (
    build_middle_east_stories,
    classify_middle_east_signal_batch,
    find_same_event_groups,
    folded_corroboration,
    company_table_problems,
    describe_pool,
    source_name,
    stored_member,
)
from pipeline.middle_east_signal_classifier import (
    story_development as middle_east_story_development,
)
from pipeline.korea_signal_classifier import classify_korea_signal_batch
from pipeline.korea_signal_summary import generate_korea_signal_summary
from pipeline.korea_ticker_universe import KOREA_TICKER_UNIVERSE
from pipeline.macro_signal_collapse import collapse_events
from pipeline.macro_signal_confirmation import check_confirmation_reversal
from pipeline.macro_signal_fedtarmd import compute_sep_median_shift_bp
from pipeline.macro_signal_fomc_calendar import (
    calendar_staleness_warning,
    is_scheduled_fomc_date,
    is_within_known_calendar_horizon,
    refresh_fomc_meeting_dates,
)
from pipeline.macro_signal_frequency import CONFIRMATION_REVERSAL_ELIGIBLE
from pipeline.macro_signal_fetch_universe import MACRO_SERIES_TO_RELEASE_ID
from pipeline.macro_signal_interpret import interpret_events_batch
from pipeline.macro_signal_suppress import (
    apply_collinear_suppression,
    apply_derived_suppression,
    from_tier_result,
)
from pipeline.macro_signal_thresholds import (
    Channel,
    EXCLUDED_FROM_TIERING,
    SeriesMove,
    THRESHOLDS,
    Tier,
    tier_series_move,
)
from pipeline.macro_signal_zscore import compute_daily_changes, compute_series_zscores
from pipeline.taiwan_signal_classifier import (
    build_revenue_history,
    classify_taiwan_signal_batch,
    taiwan_source_id,
)

logger = logging.getLogger(__name__)

_executor = ThreadPoolExecutor(max_workers=config.CLASSIFY_CONCURRENCY)

# Real ask (frontend ticket, 2026-09-25): metadata.series_tiers per-member
# vocabulary - same HIGH->signal/WEAK->weak_signal/NOISE->noise mapping
# models/jobs.py's own _AUDIT_TIER_TO_SIGNAL_DETECTION and
# _TAIWAN_SIGNAL_MAP already use, kept as its own module-level constant
# here rather than importing either of those (both are private to
# models/jobs.py and shaped around a different call site).
_TIER_TO_SIGNAL_DETECTION = {"HIGH": "signal", "WEAK": "weak_signal", "NOISE": "noise"}


async def resolve_news_run_id(
    domain: str,
    run_id: int | None,
    days_back: int,
    use_latest_run: bool,
) -> int:
    """Resolve which news-retrieval run to classify against."""
    if run_id is not None:
        await poll_run_until_done(run_id)
        return run_id
    if use_latest_run:
        existing = await fetch_latest_run(domain)
        if existing is not None:
            return existing
    resolved = await trigger_run(domain, days_back)
    await poll_run_until_done(resolved)
    return resolved


async def submit_run(
    domain: str,
    run_id: int | None,
    days_back: int,
    use_latest_run: bool,
    force: bool,
) -> tuple[int, int, bool]:
    """Resolve news_run_id, check cache, create job if needed.

    Returns (job_id, news_run_id, cached) where cached=True means an existing
    completed job was found and no new classification run is needed.
    """
    news_run_id = await resolve_news_run_id(domain, run_id, days_back, use_latest_run)
    if not force:
        existing = get_completed_job_for_run(news_run_id)
        if existing:
            return int(existing["id"]), news_run_id, True
    job_id = create_job(domain=domain, news_run_id=news_run_id)
    return job_id, news_run_id, False


def _in_taiwan_backfill_scope(
    article: dict[str, Any], revenue_period: str | None, material_month: str | None,
) -> bool:
    """Backfill scope: revenue for exactly `revenue_period` (YYYY-MM) and
    announcements filed in `material_month` (YYYY-MM); gdelt is excluded.
    A category whose option is None is excluded too."""
    meta = article.get("metadata") or {}
    category = meta.get("source_category")
    if category == "mops_revenue":
        return revenue_period is not None and meta.get("period_gregorian") == revenue_period
    if category == "mops_material":
        return material_month is not None and (meta.get("statement_date") or "")[:7] == material_month
    return False


async def run_taiwan_signal_classification(
    job_id: int,
    from_date: str,
    to_date: str,
    revenue_period: str | None = None,
    material_month: str | None = None,
) -> None:
    """Classify taiwan_market_signal items across ALL of news-retrieval's
    completed runs in [from_date, to_date] - not just the latest run.

    news-retrieval polls this domain every 30 minutes, so a single day can
    have ~30 separate completed runs; fetch_latest_run/get_run_articles
    (built for the single-run news/sec_filing paths) would
    silently see only the most recent one. This function instead lists
    every completed run in the window and pools their articles before
    classifying, so a twice-daily pass sees the full day's fetched data.

    Only classifies items not already classified - checked via
    get_existing_taiwan_source_ids against the (ticker+period or
    ticker+timestamp) source_id classify_taiwan_signal_batch derives, not
    against article id, since the same underlying fact can legitimately
    appear in multiple news-retrieval runs (re-fetched, not yet superseded)
    and must still only be classified once.

    Every item is judged on its own (revenue against its own company's
    stored history, not against the other companies), so already-stored
    items are filtered out BEFORE classifying - nothing is re-translated or
    re-scored, and a filing is labelled in the run it arrives in. Stored
    rows are never changed after insert.

    Revenue needs the company history from news-retrieval. If it cannot
    be read, revenue items are skipped for this run (not stored as
    unscorable) and the next run retries them.

    revenue_period / material_month (YYYY-MM) restrict a one-off backfill
    to that revenue period and that month's announcements, dropping
    everything else (gdelt included) before classifying. Both None (the
    scheduled run) means no restriction.
    """
    update_job_status(job_id, "running")
    try:
        run_ids = await list_completed_runs(
            config.TAIWAN_SIGNAL_DOMAIN, from_date, to_date,
        )
        all_articles: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for run_id in run_ids:
            for article in await get_run_articles(run_id):
                url = article.get("url")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(article)
    except NewsRetrievalError:
        logger.exception(
            "Failed to fetch taiwan_market_signal articles for job %d", job_id,
        )
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    if revenue_period or material_month:
        pooled = len(all_articles)
        all_articles = [a for a in all_articles
                        if _in_taiwan_backfill_scope(a, revenue_period, material_month)]
        logger.info(
            "[TAIWAN_SIGNAL] job=%d backfill scope revenue_period=%s material_month=%s:"
            " kept %d of %d pooled articles",
            job_id, revenue_period, material_month, len(all_articles), pooled,
        )

    if not all_articles:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    already_done = get_existing_taiwan_source_ids(
        [sid for a in all_articles if (sid := taiwan_source_id(a))])
    pending = [a for a in all_articles if taiwan_source_id(a) not in already_done]

    def _is_revenue(a: dict[str, Any]) -> bool:
        return (a.get("metadata") or {}).get("source_category") == "mops_revenue"

    revenue_history: dict[str, Any] = {}
    remark_translations: dict[str, str] = {}
    past_remarks: list[dict[str, Any]] = []
    revenue_pending = [a for a in pending if _is_revenue(a)]
    if revenue_pending:
        rows = await fetch_taiwan_revenue_history()
        if rows is None:
            logger.error(
                "[TAIWAN_SIGNAL] job=%d revenue history unavailable - skipping"
                " %d revenue filing(s) this run", job_id, len(revenue_pending))
            pending = [a for a in pending if not _is_revenue(a)]
        else:
            revenue_history = build_revenue_history(rows)
            remark_translations = get_taiwan_remark_translations(
                [r for a in revenue_pending if (r := a["metadata"].get("remarks"))])
            periods = sorted(p for a in revenue_pending if (p := a["metadata"].get("period_gregorian")))
            if periods:
                year, month = periods[0].split("-")  # 12 months before the earliest filing
                past_remarks = get_taiwan_recent_remarks(
                    [a["metadata"].get("ticker") for a in revenue_pending],
                    f"{int(year) - 1}-{month}")

    to_insert = classify_taiwan_signal_batch(
        pending,
        revenue_history=revenue_history,
        known_remark_translations=remark_translations,
        past_remarks=past_remarks,
    )

    update_job_status(job_id, "running", article_count=len(to_insert))

    inserted = 0
    for c in to_insert:
        try:
            insert_taiwan_signal_classification(job_id, c["article"], c["result"])
            inserted += 1
        except Exception:
            logger.exception(
                "Failed to insert taiwan_market_signal classification for"
                " source_id=%s (job %d)",
                c["result"]["source_id"], job_id,
            )

    logger.info(
        "[TAIWAN_SIGNAL] job=%d runs=%d pooled_articles=%d already_done=%d"
        " classified=%d inserted=%d",
        job_id, len(run_ids), len(all_articles), len(already_done),
        len(to_insert), inserted,
    )
    update_job_status(job_id, "completed", article_count=inserted, set_completed_at=True)


# DFF and FEDTARMD need inputs (inter-meeting date detection, SEP median
# shift) that aren't derivable via the generic z-score/tier path every
# other series uses - the spec documents these as genuinely special
# cases (an emergency Fed move date-matched against the FOMC calendar; a
# quarterly dot-plot comparison against the PRIOR SEP release, not a
# rolling window). Both are handled by their own tiering branches below
# (_tier_dff / _tier_fedtarmd), driven by the real FOMC calendar
# (fomc_meeting_dates table) and news-retrieval's per-(release,
# target_year) FEDTARMD rows, respectively - not skipped.
_GENERIC_TIER_SKIP_SERIES = frozenset({"DFF", "FEDTARMD"})


async def run_macro_signal_pipeline(job_id: int, from_date: str, to_date: str) -> None:
    """Runs the Macro Signal Backbone's deterministic core (Z-SCORE ->
    TIER -> SUPPRESS -> COLLAPSE) plus INTERPRET, end to end, for every
    tracked series with data in news-retrieval's macro_observations table.

    Pulls the FULL z-score window (config.MACRO_SIGNAL_ZSCORE_WINDOW_DAYS,
    default 730 days) per series on every run rather than maintaining an
    incremental watermark - simpler, and 46 series x ~2 years is a small
    enough pull that re-fetching daily isn't a real cost (see project
    plan history for this tradeoff).

    from_date/to_date bound which DATES' events get evaluated and
    persisted this run (typically "yesterday" for a daily scheduled
    run) - they do NOT bound how much history is pulled for z-score
    computation, which always goes back the full window regardless.

    DFEDTARU is fetched (news-retrieval stores it) but never tiered here
    - it's excluded from tiering per the spec, used only as a context
    field (dfedtaru_ctx_bp) attached to same-date events. Not yet wired
    into the interpret payload in this first pass - a real but narrow
    gap, tracked separately from the DFF/FEDTARMD wiring below.
    """
    update_job_status(job_id, "running")

    from datetime import date as _date, timedelta as _timedelta

    # Surfaces in ordinary operational logs on every run, well ahead of
    # the horizon guard actually dropping rows - see
    # macro_signal_fomc_calendar.calendar_staleness_warning's own
    # docstring for why this can't just be a one-time manual check.
    staleness_warning = calendar_staleness_warning(_date.today())
    if staleness_warning:
        logger.warning(staleness_warning)

    try:
        to_dt = _date.fromisoformat(to_date)
        window_start = to_dt - _timedelta(days=config.MACRO_SIGNAL_ZSCORE_WINDOW_DAYS + 30)
        all_observations = await get_macro_observations(
            from_date=window_start.isoformat(), to_date=to_date,
        )
        # FEDTARMD's observation_date is the PROJECTED TARGET YEAR (e.g.
        # 2029-01-01), not the real event date (the SEP release itself,
        # stored separately as vintage) - see macro_signal_fedtarmd.py's
        # own docstring. A target year can be several years past
        # `to_date`, so the bounded pull above silently excludes rows
        # for a release that itself happened well within window - e.g.
        # the 2026-09-16 release's own 2029 projection falls outside any
        # to_date<2029 bound despite the release being current. Fetched
        # separately, unbounded by observation_date (bounded only by
        # series), and merged in below.
        fedtarmd_observations = await get_macro_observations(series_ids=["FEDTARMD"])
    except NewsRetrievalError:
        logger.exception("Failed to fetch macro observations for job %d", job_id)
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    all_observations = [o for o in all_observations if o["series_id"] != "FEDTARMD"] + fedtarmd_observations

    if not all_observations:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    # CONFIRMED LIVE: news-retrieval's /macro/observations JSON response
    # serializes observation_date as an ISO string (FastAPI's default
    # date->JSON encoding) - it does NOT come back as a Python date
    # object the way it is inside news-retrieval's own Postgres reads.
    # Re-parse here, once, at the HTTP boundary, so every downstream
    # module (macro_signal_zscore's date arithmetic, tiered_by_date's
    # dict keys, etc.) can rely on a real date object without each
    # having to guard against a string.
    for obs in all_observations:
        if isinstance(obs.get("observation_date"), str):
            obs["observation_date"] = _date.fromisoformat(obs["observation_date"])
        if isinstance(obs.get("vintage"), str):
            obs["vintage"] = _date.fromisoformat(obs["vintage"])

    by_series: dict[str, list[dict[str, Any]]] = {}
    for obs in all_observations:
        by_series.setdefault(obs["series_id"], []).append(obs)

    from_dt = _date.fromisoformat(from_date)

    # --- Z-SCORE + TIER ---
    tiered_by_date: dict[Any, dict[str, Any]] = {}  # date -> {series_id: SuppressibleResult}
    for series_id, observations in by_series.items():
        if series_id in EXCLUDED_FROM_TIERING or series_id in _GENERIC_TIER_SKIP_SERIES:
            continue
        if series_id not in THRESHOLDS:
            continue  # tracked in news-retrieval's universe but not yet wired into THRESHOLDS
        zscores = compute_series_zscores(observations)
        sorted_obs_dates = sorted(o["observation_date"] for o in observations if o.get("value") is not None)
        for obs_date, (change_bp, z) in zscores.items():
            if not (from_dt <= obs_date <= to_dt):
                continue
            move = SeriesMove(series_id, d1d_bp=change_bp, z=z)
            tier_result = tier_series_move(move)
            obs_for_date = next((o for o in observations if o["observation_date"] == obs_date), None)
            # Real ask (frontend ticket, 2026-09-25, job 262): prior_value
            # populated for every series, not derived by the caller from
            # value - move_bp/100 - the immediately preceding real
            # observation in this series' own history, same day used to
            # compute change_bp above (compute_daily_changes.py).
            prior_idx = sorted_obs_dates.index(obs_date) - 1 if obs_date in sorted_obs_dates else -1
            prior_obs_date = sorted_obs_dates[prior_idx] if prior_idx >= 0 else None
            prior_obs = next((o for o in observations if o["observation_date"] == prior_obs_date), None) if prior_obs_date else None
            tiered_by_date.setdefault(obs_date, {})[series_id] = from_tier_result(
                tier_result,
                value=obs_for_date["value"] if obs_for_date else None,
                move_bp=change_bp,
                prior_value=prior_obs["value"] if prior_obs else None,
                source=obs_for_date.get("source") if obs_for_date else None,
                knowledge_time_confidence=obs_for_date.get("knowledge_time_confidence") if obs_for_date else None,
            )

    # --- DFF: inter-meeting move detection (spec's own z-gate exception) ---
    # DFF's own observation_date is the daily rate itself - unlike
    # DFEDTARU (the target UPPER BOUND, whose value changes land on the
    # calendar day AFTER the actual meeting - see
    # macro_signal_fomc_calendar.is_scheduled_fomc_date's own docstring
    # for the confirmed +1-day offset), DFF (the daily EFFECTIVE rate)
    # drifts continuously and a real target move shows up as a large
    # jump on the same +1-day-after-meeting date DFEDTARU uses - so the
    # same offset applies here, checked against the real, DB-backed FOMC
    # calendar (fomc_meeting_dates table).
    if "DFF" in by_series:
        dff_observations = by_series["DFF"]
        dff_zscores = compute_series_zscores(dff_observations)
        dff_sorted_obs_dates = sorted(o["observation_date"] for o in dff_observations if o.get("value") is not None)
        for obs_date, (change_bp, z) in dff_zscores.items():
            if not (from_dt <= obs_date <= to_dt):
                continue
            if not is_within_known_calendar_horizon(obs_date):
                logger.warning(
                    "[MACRO_SIGNAL] DFF %s is past the known FOMC calendar horizon - "
                    "cannot determine inter-meeting status, skipping this date's DFF tiering",
                    obs_date,
                )
                continue
            # CONFIRMED LIVE (2026-09-24 real 30-day classify run):
            # is_inter_meeting_date must NOT be "just not a scheduled
            # date" - that flagged every quiet, unchanged day between
            # meetings as an inter-meeting MOVE (change_bp==0, still
            # fired the no-gate HIGH path in _dff_rule), producing 4
            # consecutive days of fabricated HIGH events off a flat
            # rate. An inter-meeting move requires a REAL change on a
            # non-scheduled date - both conditions, not one.
            move = SeriesMove(
                "DFF", d1d_bp=change_bp, z=z,
                is_inter_meeting_date=(change_bp != 0 and not is_scheduled_fomc_date(obs_date)),
            )
            tier_result = tier_series_move(move)
            dff_obs_for_date = next((o for o in dff_observations if o["observation_date"] == obs_date), None)
            dff_prior_idx = dff_sorted_obs_dates.index(obs_date) - 1 if obs_date in dff_sorted_obs_dates else -1
            dff_prior_obs_date = dff_sorted_obs_dates[dff_prior_idx] if dff_prior_idx >= 0 else None
            dff_prior_obs = next((o for o in dff_observations if o["observation_date"] == dff_prior_obs_date), None) if dff_prior_obs_date else None
            tiered_by_date.setdefault(obs_date, {})["DFF"] = from_tier_result(
                tier_result,
                value=dff_obs_for_date["value"] if dff_obs_for_date else None,
                move_bp=change_bp,
                prior_value=dff_prior_obs["value"] if dff_prior_obs else None,
                source=dff_obs_for_date.get("source") if dff_obs_for_date else None,
                knowledge_time_confidence=dff_obs_for_date.get("knowledge_time_confidence") if dff_obs_for_date else None,
            )

    # --- FEDTARMD: SEP median shift (spec's own no-z-gate exception) ---
    # news-retrieval stores one row per (SEP release, projected target
    # year) - see macro_signal_fedtarmd.py's own docstring. Tier once
    # per real SEP release (vintage), keyed by the release date itself
    # (not a projection-year observation_date, which isn't a real event
    # date) so downstream SUPPRESS/COLLAPSE treat it the same as every
    # other series' obs_date-keyed dict.
    if "FEDTARMD" in by_series:
        fedtarmd_observations = by_series["FEDTARMD"]
        release_dates = sorted({o["vintage"] for o in fedtarmd_observations if o.get("vintage")})
        for release_date in release_dates:
            if not (from_dt <= release_date <= to_dt):
                continue
            sep_shift = compute_sep_median_shift_bp(fedtarmd_observations, release_date)
            move = SeriesMove("FEDTARMD", sep_median_shift_bp=sep_shift.shift_bp if sep_shift else None)
            tier_result = tier_series_move(move)
            representative_obs = next(
                (o for o in fedtarmd_observations if o.get("vintage") == release_date),
                None,
            )
            tiered_by_date.setdefault(release_date, {})["FEDTARMD"] = from_tier_result(
                tier_result,
                value=representative_obs["value"] if representative_obs else None,
                move_bp=sep_shift.shift_bp if sep_shift else None,
                target_year=sep_shift.target_year if sep_shift else None,
                prior_value=str(sep_shift.prior_value) if sep_shift else None,
                source=representative_obs.get("source") if representative_obs else None,
                knowledge_time_confidence=representative_obs.get("knowledge_time_confidence") if representative_obs else None,
            )

    # --- SUPPRESS (A then B) ---
    for obs_date, results in tiered_by_date.items():
        apply_derived_suppression(results)
        apply_collinear_suppression(results)

    # --- COLLAPSE ---
    release_id_of = {sid: MACRO_SERIES_TO_RELEASE_ID.get(sid, -1) for sid in by_series}
    all_events = []
    for obs_date, results in tiered_by_date.items():
        survivors = [r for r in results.values() if r.tier != Tier.NOISE]
        if not survivors:
            continue
        knowledge_time_of = {sid: obs_date for sid in results}
        events = collapse_events(survivors, release_id_of, knowledge_time_of)
        all_events.extend(events)

    # --- Persist suppressed/NOISE rows for audit (never deleted, per spec) ---
    audit_candidate_ids = []
    audit_rows = []
    for obs_date, results in tiered_by_date.items():
        for series_id, r in results.items():
            if r.suppressed_by is not None or r.tier == Tier.NOISE:
                source_id = f"{series_id}-{obs_date}"
                audit_candidate_ids.append(source_id)
                audit_rows.append({
                    "series_id": series_id, "observation_date": obs_date,
                    "tier": r.tier.value, "z_score": r.z_score,
                    "suppressed_by": r.suppressed_by, "knowledge_time": obs_date,
                    "source": r.source, "knowledge_time_confidence": r.knowledge_time_confidence,
                })
    already_audited = get_existing_macro_signal_source_ids(audit_candidate_ids)
    audited = 0
    for row in audit_rows:
        source_id = f"{row['series_id']}-{row['observation_date']}"
        if source_id in already_audited:
            continue
        try:
            insert_macro_signal_event(job_id, row, None)
            audited += 1
        except Exception:
            logger.exception("Failed to insert macro_signal audit row for %s (job %d)", source_id, job_id)

    # DFEDTARU context (spec section 7: excluded from tiering entirely,
    # but its own current level is real grounding context for a
    # policy_path event - "the funds rate target is currently 4.00-
    # 4.25%" - not itself a signal). {date: value} for O(1) lookup below;
    # value is the target UPPER BOUND in percent, converted to bp at
    # attach time to match every other series' native unit convention
    # in this pipeline.
    dfedtaru_by_date: dict[Any, float] = {
        o["observation_date"]: float(o["value"])
        for o in by_series.get("DFEDTARU", []) if o.get("value") is not None
    }
    dfedtaru_dates_sorted = sorted(dfedtaru_by_date)

    def _dfedtaru_ctx_bp(as_of: Any) -> float | None:
        """Most recent DFEDTARU value on or before `as_of` (its own
        daily series can lag a same-day event by the market-close
        knowledge_time convention every daily series uses - looking
        backward for the nearest known value is always safe here,
        this is context, not a timed observation)."""
        candidates = [d for d in dfedtaru_dates_sorted if d <= as_of]
        if not candidates:
            return None
        return dfedtaru_by_date[candidates[-1]] * 100

    # --- INTERPRET + persist survivors ---
    interpret_payloads = []
    for event in all_events:
        payload: dict[str, Any] = {
            "release_id": event.release_id,
            "knowledge_time": event.knowledge_time.isoformat() if hasattr(event.knowledge_time, "isoformat") else str(event.knowledge_time),
            "channel": event.channel.value,
            "members": [
                {
                    "series_id": m.series_id, "value": m.value,
                    # No rounding/truncation - move_bp is exact as of the
                    # Decimal fix in macro_signal_zscore.py/
                    # macro_signal_fedtarmd.py (real frontend ticket,
                    # 2026-09-25: a prior truncation step here made float
                    # noise WORSE, e.g. 4.999999999999996 truncated to
                    # 4.99 instead of the real exact value 5).
                    "move_bp": m.move_bp,
                    "target_year": m.target_year, "prior_value": m.prior_value,
                    "z_score": m.z_score, "tier": m.tier.value,
                    "classification_basis": m.reason,
                    "classification_reason": m.plain_reason,
                }
                for m in event.members
            ],
        }
        if event.channel == Channel.POLICY_PATH:
            ctx_bp = _dfedtaru_ctx_bp(event.knowledge_time)
            if ctx_bp is not None:
                payload["dfedtaru_ctx_bp"] = ctx_bp
        interpret_payloads.append(payload)

    interpreted = interpret_events_batch(interpret_payloads) if interpret_payloads else []

    # event.members (real SuppressibleResult objects, not yet flattened to
    # the plain dicts interpret_payloads carries) is where source/
    # knowledge_time_confidence still live - looked up back by
    # (release_id, channel, knowledge_time) below since interpret_payloads
    # and all_events share index order but interpreted's own order can
    # differ after the ThreadPoolExecutor fan-out.
    members_by_event_key = {
        (e.release_id, e.channel.value, e.knowledge_time.isoformat() if hasattr(e.knowledge_time, "isoformat") else str(e.knowledge_time)): e.members
        for e in all_events
    }

    inserted = 0
    for item in interpreted:
        event_payload = item["event"]
        interpretation = item["interpretation"]
        event_key = (event_payload["release_id"], event_payload["channel"], event_payload["knowledge_time"])
        real_members = members_by_event_key.get(event_key, [])
        macro_event = {
            "release_id": event_payload["release_id"],
            "channel": event_payload["channel"],
            "knowledge_time": event_payload["knowledge_time"],
            "tier": max((m["tier"] for m in event_payload["members"]), key=lambda t: t == "HIGH"),
            "member_series": [m["series_id"] for m in event_payload["members"]],
            "z_scores": {m["series_id"]: m["z_score"] for m in event_payload["members"]},
            # move_bp/target_year fill the exact gap a frontend hit live:
            # z_scores.FEDTARMD is always null (its rule is a no-z-gate
            # SEP-median-shift exception, not a z-score threshold - see
            # macro_signal_thresholds._fedtarmd_rule), so without these
            # there was no field anywhere saying WHAT number made it HIGH
            # or WHICH projected year "the medium-run fed funds target"
            # meant. Populated for every series (not just FEDTARMD) since
            # any member could in principle need the same Current/Prior/
            # Change display; target_year is None for every series except
            # FEDTARMD, where it is never None on an interpreted event.
            "move_bp": {m["series_id"]: m["move_bp"] for m in event_payload["members"]},
            "target_years": {
                m["series_id"]: m["target_year"]
                for m in event_payload["members"] if m.get("target_year") is not None
            },
            # Real ask (frontend ticket, 2026-09-25, item 7 - "all numbers
            # must be correct and consistent"): a UI's Current/Prior/
            # Change display needs the actual two readings move_bp was
            # computed from, not just the delta. current_values is
            # `value` (the current reading); prior_values is the
            # immediately preceding real observation for every standard
            # series, or the prior SEP release's own value for FEDTARMD
            # (see SuppressibleResult.prior_value's own docstring) -
            # populated for every series in member_series, not just
            # FEDTARMD (CONFIRMED LIVE a frontend found it missing on 28
            # of 29 events - it was only ever wired for FEDTARMD before
            # this fix). Both are strings, matching `value`'s own
            # convention, not bare floats.
            "current_values": {m["series_id"]: m["value"] for m in event_payload["members"]},
            "prior_values": {
                m["series_id"]: m["prior_value"]
                for m in event_payload["members"] if m.get("prior_value") is not None
            },
            # Real ask (frontend ticket, 2026-09-25): a FEDTARMD event is
            # HIGH with z_scores.FEDTARMD always null (its rule gates on
            # move_bp, not a z-score - see _fedtarmd_rule), and move_bp/
            # target_years alone don't say WHICH RULE decided the tier.
            # classification_basis is TierResult.reason verbatim (e.g.
            # "sep_median_shift_50.0bp_ge_25_no_zgate") - already computed
            # at TIER time for every series, just never persisted before.
            "classification_basis": {
                m["series_id"]: m["classification_basis"] for m in event_payload["members"]
            },
            # Real ask (frontend ticket, 2026-09-25): classification_basis
            # (above) is a machine-readable tag, not something the
            # frontend can show verbatim in "Why did AlphaStreet flag
            # it?" without parsing it. classification_reason is a real
            # plain-English sentence per series, written inline by the
            # exact rule branch that fired (TierResult.plain_reason - see
            # macro_signal_thresholds.py's own rule functions), always
            # naming the tier, the real threshold, and the z-score (or
            # "no z-score check" when the rule has none) - never derived
            # by parsing classification_basis, so it can't drift out of
            # sync with the logic that actually ran. classification_basis
            # itself is kept as-is for debugging, per explicit request.
            "classification_reason": {
                m["series_id"]: m["classification_reason"]
                for m in event_payload["members"] if m.get("classification_reason") is not None
            },
            # Real ask (frontend ticket, 2026-09-25): GET /results/summary
            # originally counted observations.* by the EVENT's rolled-up
            # signal_detection column (the row's own max-tier-across-
            # members value) applied to every member - CONFIRMED LIVE this
            # disagreed with classification_reason on real data: the
            # 2026-08-28 discount_rate event is signal_detection='signal'
            # (DFII5/T10Y2Y are HIGH), but DFII10/T5YIFR's own
            # classification_reason says WEAK - counting them as "high" in
            # the summary contradicted the very sentence shown for them.
            # series_tiers is the real fix: each member's OWN tier
            # (_AUDIT_TIER_TO_SIGNAL_DETECTION's HIGH->signal/WEAK->
            # weak_signal vocabulary, same as signal_detection's own,
            # models/jobs.py), written from the exact same m["tier"] value
            # classification_reason was generated from - the two can never
            # drift apart, since nothing derives one from the other.
            "series_tiers": {
                m["series_id"]: _TIER_TO_SIGNAL_DETECTION.get(m["tier"], "noise")
                for m in event_payload["members"]
            },
            # Deduplicated arrays, not a per-series map - the VALUE (which
            # source/confidence) is what matters here, and every member of
            # a collapsed event shares the same value in practice today,
            # but this stays a real array (not a single string) rather than
            # silently collapsing to one value if a genuinely mixed-source
            # event (e.g. a fred series collapsed alongside close_today_bal)
            # ever occurs - never guess, never quietly drop a real distinct
            # value, same principle as KnowledgeTimeDropped's own fail-loud
            # contract elsewhere in this pipeline.
            "sources": sorted({m.source for m in real_members if m.source}),
            "knowledge_time_confidences": sorted({m.knowledge_time_confidence for m in real_members if m.knowledge_time_confidence}),
        }
        # CONFIRMED LIVE: release_id alone is not a unique event key - some
        # release_ids are shared across channels (e.g. release_id=18 covers
        # both DFF/DFEDTARU's policy_path channel and DFII10/T10Y3M's
        # discount_rate channel, since COLLAPSE groups survivors by
        # (release_id, channel), not release_id alone). Without channel in
        # source_id, a second channel's real event on the same release+date
        # silently collided with an already-inserted event and was dropped
        # as a false "duplicate" - found live: a real DFF WEAK event on
        # 2026-09-17 was lost this way because release_id=18's
        # discount_rate event for the same date had already claimed
        # "18-2026-09-17".
        source_id = f"{macro_event['release_id']}-{macro_event['channel']}-{macro_event['knowledge_time']}"
        if source_id in get_existing_macro_signal_source_ids([source_id]):
            continue
        try:
            insert_macro_signal_event(job_id, macro_event, interpretation)
            inserted += 1
        except Exception:
            logger.exception("Failed to insert macro_signal event for source_id=%s (job %d)", source_id, job_id)

    logger.info(
        "[MACRO_SIGNAL] job=%d series=%d dates_evaluated=%d events_collapsed=%d"
        " interpreted=%d inserted=%d audited=%d",
        job_id, len(by_series), len(tiered_by_date), len(all_events),
        len(interpreted), inserted, audited,
    )
    update_job_status(job_id, "completed", article_count=inserted, set_completed_at=True)


async def run_macro_signal_full_chain(
    job_id: int, from_date: str, to_date: str, *, incremental: bool,
) -> None:
    """The full 'store the backfill, then classify' chain, as one
    agent_jobs-tracked background job: trigger news-retrieval's real
    macro fetch (POST /macro/fetch, itself tracked in news-retrieval's
    OWN runs table - GET /runs?domain=macro_signal) -> wait for it to
    complete -> refresh the FOMC calendar -> run the deterministic
    pipeline (Z-SCORE -> TIER -> SUPPRESS -> COLLAPSE -> INTERPRET) over
    [from_date, to_date].

    Two DIFFERENT run-tracking records are created by one call to this
    function - a real, deliberate consequence of macro_signal spanning
    two services that each track their own work in their own `runs`/
    `agent_jobs` table (see controllers.macro_run's own docstring in
    news-retrieval for why the fetch isn't tracked here instead). This
    function's own job_id (passed in, already created by the caller -
    routes/macro.py) is signal-detection-agent's job; the fetch's own
    run_id (returned by trigger_macro_fetch, logged below) is
    news-retrieval's - both are real, both queryable, neither
    fabricated to look unified.
    """
    update_job_status(job_id, "running")
    try:
        fetch_run_id = await trigger_macro_fetch(incremental=incremental)
        logger.info(
            "[MACRO_SIGNAL_CHAIN] job=%d triggered news-retrieval fetch run_id=%d (incremental=%s), waiting...",
            job_id, fetch_run_id, incremental,
        )
        await poll_run_until_done(fetch_run_id)
    except NewsRetrievalError:
        logger.exception("[MACRO_SIGNAL_CHAIN] job=%d news-retrieval fetch failed or timed out", job_id)
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    logger.info("[MACRO_SIGNAL_CHAIN] job=%d fetch complete, refreshing FOMC calendar...", job_id)
    try:
        await asyncio.to_thread(refresh_fomc_meeting_dates)
    except Exception:
        # A stale/unrefreshed calendar degrades DFF's inter-meeting
        # detection (calendar_staleness_warning inside
        # run_macro_signal_pipeline below will surface this in logs) but
        # does not block every other series' tiering - not fatal to the
        # whole chain, logged and continued rather than aborting a real
        # fetch+backfill over a calendar refresh hiccup.
        logger.exception("[MACRO_SIGNAL_CHAIN] job=%d FOMC calendar refresh failed - continuing anyway", job_id)

    logger.info("[MACRO_SIGNAL_CHAIN] job=%d running pipeline for [%s, %s]...", job_id, from_date, to_date)
    await run_macro_signal_pipeline(job_id, from_date, to_date)


async def run_macro_signal_confirmation_recheck(job_id: int, since_date: str) -> None:
    """Suppression mechanism C (spec section 5, Confirmation override):
    re-examines already-stored macro_signal rows for weekly/monthly
    series (CONFIRMATION_REVERSAL_ELIGIBLE - see
    macro_signal_frequency.py) and retroactively marks any whose move
    has since FULLY reversed at the next observation as suppressed,
    per check_confirmation_reversal's own definition of "fully
    reverses" (opposite sign, magnitude >= the original).

    Retroactive by nature - this is a SEPARATE pass from
    run_macro_signal_pipeline, not folded into it, because the
    confirming next observation usually does not exist yet at the time
    the original event was tiered/collapsed/interpreted (a monthly
    series' next print is up to ~4 weeks later). Safe to run daily:
    re-checking an event that has no new next-observation yet is a
    cheap no-op (see_next_change is None -> not reversed, per
    check_confirmation_reversal), and an already-suppressed row is
    excluded from the candidate query entirely.

    Only re-derives the ORIGINAL row's own change from a fresh
    observations pull (not from metadata.z_scores, which stores a
    z-score, not the raw native-unit change) - same compute_daily_
    changes function the main pipeline uses, so "the move" means the
    identical thing in both passes.
    """
    update_job_status(job_id, "running")
    eligible_series = sorted(CONFIRMATION_REVERSAL_ELIGIBLE)

    candidates = list_macro_signal_events_for_confirmation_recheck(eligible_series, since_date)
    if not candidates:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        logger.info("[MACRO_SIGNAL_RECHECK] job=%d no candidates since %s", job_id, since_date)
        return

    try:
        observations = await get_macro_observations(series_ids=eligible_series, from_date=since_date)
    except NewsRetrievalError:
        logger.exception("Failed to fetch macro observations for confirmation recheck job %d", job_id)
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    from datetime import date as _date
    for obs in observations:
        if isinstance(obs.get("observation_date"), str):
            obs["observation_date"] = _date.fromisoformat(obs["observation_date"])

    by_series: dict[str, list[dict[str, Any]]] = {}
    for obs in observations:
        by_series.setdefault(obs["series_id"], []).append(obs)
    changes_by_series = {sid: compute_daily_changes(obs_list) for sid, obs_list in by_series.items()}

    reversed_count = 0
    checked_count = 0
    for row in candidates:
        metadata = row["metadata"]
        row_date = row["published"].date() if hasattr(row["published"], "date") else row["published"]
        member_series = metadata.get("member_series") or (
            [metadata["series_id"]] if metadata.get("series_id") else []
        )
        eligible_members = [s for s in member_series if s in CONFIRMATION_REVERSAL_ELIGIBLE]
        if not eligible_members:
            continue

        any_reversed = False
        reversing_series = None
        for series_id in eligible_members:
            changes = changes_by_series.get(series_id, {})
            original_change = changes.get(row_date)
            if original_change is None:
                continue  # this row's own date isn't in the re-fetched window - can't recheck
            future_dates = sorted(d for d in changes if d > row_date)
            next_change = changes[future_dates[0]] if future_dates else None
            checked_count += 1
            result = check_confirmation_reversal(series_id, original_change, next_change)
            if result.reversed:
                any_reversed = True
                reversing_series = series_id
                break

        if any_reversed:
            rule = f"confirmation_reversal:{reversing_series}"
            try:
                updated = mark_macro_signal_event_suppressed(row["source_id"], rule)
                if updated:
                    reversed_count += 1
                    logger.info(
                        "[MACRO_SIGNAL_RECHECK] job=%d suppressed source_id=%s via %s",
                        job_id, row["source_id"], rule,
                    )
            except Exception:
                logger.exception(
                    "Failed to mark confirmation-reversed macro_signal row source_id=%s (job %d)",
                    row["source_id"], job_id,
                )

    logger.info(
        "[MACRO_SIGNAL_RECHECK] job=%d candidates=%d checked=%d reversed=%d",
        job_id, len(candidates), checked_count, reversed_count,
    )
    update_job_status(job_id, "completed", article_count=reversed_count, set_completed_at=True)


async def run_korea_signal_classification(job_id: int, from_date: str, to_date: str) -> None:
    """Classify korea_market_signal items across ALL of news-retrieval's
    completed runs in [from_date, to_date] - not just the latest run, same
    reasoning as run_taiwan_signal_classification (news-retrieval polls
    this domain every 4 hours, so a multi-day window or a slow day can
    still span more than one completed run).

    No Korea signal type judges an article against a field of peers (see
    classify_korea_signal_batch's own docstring), so this is pool ->
    classify -> dedup -> insert.

    Only inserts items not already classified - checked via
    get_existing_korea_signal_source_ids against the source_id
    classify_korea_signal_batch derives (rcept_no for DART filings, the
    article's own url for S7 qualification_news), not against article id,
    for the same reason Taiwan's controller checks source_id instead of
    id: the same underlying fact can legitimately reappear across
    multiple news-retrieval runs before this job ever ran.
    """
    update_job_status(job_id, "running")
    try:
        run_ids = await list_completed_runs(
            config.KOREA_SIGNAL_DOMAIN, from_date, to_date,
        )
        all_articles: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for run_id in run_ids:
            for article in await get_run_articles(run_id):
                url = article.get("url")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(article)
    except NewsRetrievalError:
        logger.exception(
            "Failed to fetch korea_market_signal articles for job %d", job_id,
        )
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    if not all_articles:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    classified = classify_korea_signal_batch(all_articles, tracked_companies=KOREA_TICKER_UNIVERSE)

    candidate_source_ids = [c["result"]["source_id"] for c in classified if c["result"].get("source_id")]
    already_done = get_existing_korea_signal_source_ids(candidate_source_ids)
    to_insert = [
        c for c in classified
        if c["result"].get("source_id") not in already_done
    ]

    update_job_status(job_id, "running", article_count=len(to_insert))

    inserted = 0
    for c in to_insert:
        try:
            insert_korea_signal_classification(job_id, c["article"], c["result"])
            inserted += 1
        except Exception:
            logger.exception(
                "Failed to insert korea_market_signal classification for"
                " source_id=%s (job %d)",
                c["result"].get("source_id"), job_id,
            )

    logger.info(
        "[KOREA_SIGNAL] job=%d runs=%d pooled_articles=%d classified=%d"
        " already_done=%d inserted=%d",
        job_id, len(run_ids), len(all_articles), len(classified),
        len(already_done), inserted,
    )
    update_job_status(job_id, "completed", article_count=inserted, set_completed_at=True)


async def refresh_china_baselines(
    from_date: str, to_date: str, refetch: bool = False,
    refetch_filed_since: str | None = None,
) -> int:
    """Recompute every company's C2 baseline from the FULL stored
    history and overwrite the market_signal_baselines cache.

    The refresh half of the split described in
    models/market_baselines.py. Pools a WIDE window - years, not the
    single day run_china_signal_classification pools - because a
    company's own trailing distribution cannot be rebuilt from one
    day's filings. Confirmed on real data: Hygon's FY2022 annual report
    produces no verdict at all when classified alone, and the correct
    one (121.83%, 3.39 MADs above its own annual median of 54.66%) when
    the cached baseline is present.

    Three ways to handle the underlying revenue figures:

      refetch=True            re-pull every company from cninfo and
                              Alpha Vantage (~90s). The monthly job.
      refetch_filed_since=D   re-pull only companies that filed a
                              periodic report on or after D (~1s each,
                              usually none). The daily classify pass.
      neither                 read what is stored, pull nothing.

    The middle one is what lets the daily pass stay honest without
    paying the whole-universe cost: a company's figures go stale only
    when it files, so the filings already in the pool say exactly whose
    to re-pull.

    Returns the number of baseline rows written, trusted and untrusted
    alike - an untrusted row records that a company was measured and
    found to have too little history, which is worth storing.
    """
    run_ids = await list_completed_runs(
        config.CHINA_SIGNAL_DOMAIN, from_date, to_date,
    )
    all_articles: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for run_id in run_ids:
        for article in await get_run_articles(run_id):
            url = article.get("url")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(article)

    if not all_articles:
        logger.warning(
            "[CHINA_BASELINES] no articles in %s..%s - cache left as it was "
            "rather than emptied, since an empty refresh would silence every "
            "classifier that reads it", from_date, to_date)
        return 0

    # Structured revenue first, filing text only where there is none.
    #
    # Which companies to re-fetch is decided from the pooled articles
    # rather than passed in. A company's stored figures go stale for
    # exactly one reason - it filed a periodic report - and that filing
    # is in this pool, so the pool already knows. Everyone else's
    # stored figures are still current and re-fetching them would be
    # twenty upstream requests to confirm nothing changed.
    #
    # Scoped to `refetch_filed_since`, NOT to the pooled window. The
    # pool spans five years so every company has filed something in it;
    # what matters is who filed since the figures were last pulled.
    # Without this the daily pass re-fetches all sixteen mainland
    # companies every run, which is the whole-universe cost it exists
    # to avoid.
    #
    # Only periodic reports count. A forecast or a flash states a
    # figure the company itself will restate, and cninfo carries the
    # audited number rather than either, so refreshing on one fetches
    # a figure that has not been published yet.
    filed_codes: set[str] = set()
    if refetch_filed_since and not refetch:
        for article in all_articles:
            meta = article.get("metadata") or {}
            if meta.get("source_type") != "cninfo_filing":
                continue
            published = str(article.get("published") or "")[:10]
            if not published or published < refetch_filed_since:
                continue
            title = article.get("title") or ""
            if (filing_period(title)
                    and filing_authority(title) >= FILING_RANK_PERIODIC
                    and meta.get("code")):
                filed_codes.add(meta["code"])
    # refetch=True with no codes is the monthly job asking for the whole
    # universe; the daily pass always names codes, even an empty set,
    # which correctly fetches nothing.
    revenue_series = await fetch_china_revenue_history(
        refresh=refetch or bool(filed_codes),
        codes=sorted(filed_codes) if not refetch else None,
    )
    if filed_codes and not refetch:
        logger.info(
            "[CHINA_BASELINES] %d company/companies filed a periodic report "
            "since %s - re-fetching their revenue only: %s",
            len(filed_codes), refetch_filed_since, ", ".join(sorted(filed_codes)))
    rows = compute_c2_baselines(
        all_articles, revenue_series=revenue_series,
    ) + compute_c3_baselines(all_articles) + compute_c5_baselines(
        all_articles)
    written = upsert_baselines(config.CHINA_SIGNAL_DOMAIN, rows)
    trusted = sum(1 for r in rows if r["is_trusted"])
    logger.info(
        "[CHINA_BASELINES] pooled %d articles from %d runs -> %d series "
        "(%d trusted, %d below the floor)",
        len(all_articles), len(run_ids), written, trusted, written - trusted)
    return written


async def run_china_signal_classification(job_id: int, from_date: str, to_date: str) -> None:
    """Classify china_market_signal items across ALL of news-retrieval's
    completed runs in [from_date, to_date] - same pooling reasoning as
    run_korea_signal_classification: news-retrieval polls this domain's
    policy sources every 4 hours, so a single day legitimately spans
    several completed runs and reading only the latest would silently
    drop earlier ones.

    Pool -> classify -> dedup -> insert, no re-ranking step: no China
    classifier judges an item against a field of peers the way Taiwan's
    YoY ranking does.

    Dedup is on the article's own url, which news-retrieval already
    guarantees globally unique - see insert_china_signal_classification.
    """
    update_job_status(job_id, "running")
    try:
        run_ids = await list_completed_runs(
            config.CHINA_SIGNAL_DOMAIN, from_date, to_date,
        )
        all_articles: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for run_id in run_ids:
            for article in await get_run_articles(run_id):
                url = article.get("url")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(article)
    except NewsRetrievalError:
        logger.exception(
            "Failed to fetch china_market_signal articles for job %d", job_id,
        )
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    if not all_articles:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    # DISCARD ALREADY-CLASSIFIED ARTICLES BEFORE CLASSIFYING, not after.
    #
    # The window selects RUNS by date, not articles by publication
    # date, so a day on which a multi-year backfill ran pools that
    # entire backfill - 8,241 articles against a normal day's ~935.
    # Classifying first and filtering afterwards meant re-running such
    # a day re-classified every article it had already judged (3,694 of
    # them, measured) at full LLM cost, to insert nothing: the skip
    # protected the table from duplicate rows but not the spend.
    #
    # The source_id IS the article url, which is known from the pooled
    # article without classifying it, so the filter can simply move
    # ahead of the classifier. Same rows out, cost proportional to what
    # is actually new. Taiwan deliberately keeps the opposite order -
    # its source_id is derived inside its classifier (ticker+period),
    # so it cannot be known in advance.
    candidate_urls = [a["url"] for a in all_articles if a.get("url")]
    already_done = get_existing_china_signal_source_ids(candidate_urls)
    pending = [
        a for a in all_articles if a.get("url") not in already_done
    ]
    logger.info(
        "[CHINA] pooled %d articles from %d runs - %d already classified, "
        "%d to classify", len(all_articles), len(run_ids),
        len(all_articles) - len(pending), len(pending))
    if not pending:
        update_job_status(
            job_id, "completed", article_count=0, set_completed_at=True)
        return

    # The cached baselines, so a daily pass judges against each
    # company's full history rather than the single day it pooled.
    # Empty is survivable: the classifier falls back to deriving what
    # it can from the batch, which is correct for a backfill and
    # produces nothing for a daily run - the behaviour before the cache
    # existed.
    baselines = load_baselines(config.CHINA_SIGNAL_DOMAIN, "revenue_yoy")
    c3_baselines = load_baselines(
        config.CHINA_SIGNAL_DOMAIN, "commitment_value")
    c5_baselines = load_baselines(
        config.CHINA_SIGNAL_DOMAIN, "capex_value")
    if not baselines:
        logger.warning(
            "[CHINA] no cached baselines - C2 will only judge companies "
            "whose full history happens to be in this batch. Run "
            "refresh-china-baselines.")
    # The raw revenue observations behind the C2 baselines, so a
    # filing can be judged against only what preceded its own
    # reporting period. The cache holds one current snapshot per
    # (code, period_type) and cannot answer "as of when" - see
    # point_in_time_baseline for the Cambricon case this cost.
    period_series: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for entry in await fetch_china_revenue_history():
        code = entry.get("code")
        period_type = entry.get("period_type")
        if code and period_type:
            period_series.setdefault((code, period_type), []).append(entry)

    classified = classify_china_signal_batch(
        pending, baselines=baselines, c3_baselines=c3_baselines,
        c5_baselines=c5_baselines, period_series=period_series)

    # Re-checked rather than assumed, in ONE batched query. The
    # pre-filter above removed everything classified before this run
    # started, but the two scheduled passes overlap by design and the
    # other may have inserted rows while this one was classifying.
    # This is what actually guarantees no duplicate row; the pre-filter
    # is what keeps the classification cheap.
    candidate_source_ids = [
        c["article"]["url"] for c in classified if c["article"].get("url")
    ]
    already_done = get_existing_china_signal_source_ids(candidate_source_ids)
    to_insert = [
        c for c in classified
        if c["article"].get("url") not in already_done
    ]

    update_job_status(job_id, "running", article_count=len(to_insert))

    inserted = 0
    for c in to_insert:
        try:
            insert_china_signal_classification(job_id, c["article"], c["result"])
            inserted += 1
        except Exception:
            logger.exception(
                "Failed to insert china_market_signal classification for"
                " url=%s (job %d)",
                c["article"].get("url"), job_id,
            )

    logger.info(
        "[CHINA_SIGNAL] job=%d runs=%d pooled_articles=%d classified=%d"
        " already_done=%d inserted=%d",
        job_id, len(run_ids), len(all_articles), len(classified),
        len(already_done), inserted,
    )
    update_job_status(job_id, "completed", article_count=inserted, set_completed_at=True)


async def pool_middle_east_articles(
    from_date: str, to_date: str,
) -> dict[str, Any]:
    """Step 1: every article from news-retrieval's completed
    middle_east_market_signal runs in [from_date, to_date], deduped by
    url, split into already-judged and new.

    Pooled across ALL completed runs in the window, not the latest one:
    news-retrieval fetches twice a day, and reading only the latest run
    would silently drop the other.

    The already-judged filter runs BEFORE any classification - the
    source_id is the article url, known without classifying - so a re-run
    costs nothing (the China finding: filtering afterwards re-paid for
    3,694 model calls to insert nothing).

    An article with no url cannot be deduped or stored; it is counted in
    `no_url` and left out rather than guessed at (news-retrieval
    guarantees one, so a non-zero count means something upstream broke).

    Raises NewsRetrievalError when news-retrieval cannot be read - the
    caller decides whether that fails a job.
    """
    run_ids = await list_completed_runs(
        config.MIDDLE_EAST_SIGNAL_DOMAIN, from_date, to_date)
    pooled: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    no_url = 0
    for run_id in run_ids:
        for article in await get_run_articles(run_id):
            url = article.get("url")
            if not url:
                no_url += 1
                continue
            if url not in seen_urls:
                seen_urls.add(url)
                pooled.append(article)
    already_judged = get_existing_middle_east_signal_source_ids(
        [a["url"] for a in pooled])
    new = [a for a in pooled if a["url"] not in already_judged]
    return {
        "run_ids": run_ids,
        "pooled": pooled,
        "new": new,
        "already_judged": len(already_judged),
        "no_url": no_url,
    }


async def load_middle_east_company_table() -> tuple[list[dict[str, Any]], list[str]]:
    """Step 2: the company table from news-retrieval, checked.

    Returns (companies, problems). Unreachable is a problem like any
    other - the caller treats any problem as "do not classify this run".
    """
    companies = await fetch_middle_east_universe()
    if companies is None:
        return [], ["news-retrieval did not serve /market/middle-east/universe"]
    return companies, company_table_problems(companies)


async def run_middle_east_signal_classification(
    job_id: int, from_date: str, to_date: str,
) -> None:
    """Pool -> skip already judged -> classify -> re-check -> insert.

    The re-check before insert exists because the two scheduled passes
    can overlap: rows the other pass stored while this one classified are
    dropped here (and the partial unique index would refuse them anyway).
    """
    update_job_status(job_id, "running")
    try:
        pool = await pool_middle_east_articles(from_date, to_date)
    except NewsRetrievalError:
        logger.exception(
            "Failed to fetch middle_east_market_signal articles for job %d", job_id)
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    logger.info(
        "[MIDDLE_EAST_SIGNAL] job=%d runs=%d pooled=%d already_judged=%d"
        " new=%d no_url=%d by_source=%s",
        job_id, len(pool["run_ids"]), len(pool["pooled"]),
        pool["already_judged"], len(pool["new"]), pool["no_url"],
        describe_pool(pool["new"]))
    if not pool["new"]:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    companies, problems = await load_middle_east_company_table()
    if problems:
        # Rows are written once and never revised: judging without a
        # sound table would store them permanently without (or with
        # wrong) read-through. Nothing is stored; the next pass retries
        # the same articles (the default window is two days).
        for problem in problems:
            logger.error("[MIDDLE_EAST_SIGNAL] company table: %s", problem)
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    # 13D/G filings are labelled against research-universe's US companies
    # (is the issuer a US AI company?). Loaded only when a filing is in the
    # batch; if it cannot be read those filings wait for the next run rather
    # than being stored with a wrong label.
    us_universe = None
    if any(source_name(a) == "sec_fund_stake" for a in pool["new"]):
        try:
            us_universe = await fetch_us_listed_companies()
        except ResearchUniverseError:
            logger.exception("[MIDDLE_EAST_SIGNAL] research-universe unavailable;"
                             " 13D/G filings wait for the next run")
    # Rows already stored for this batch's newsapi.ai event clusters: the
    # other outlets that covered the same event in an earlier run.
    event_uris = sorted({(a.get("metadata") or {}).get("event_uri") for a in pool["new"]}
                        - {None, "", "None"})
    stored_event_members = {
        uri: [stored_member(r) for r in rows]
        for uri, rows in get_middle_east_event_members(event_uris).items()}
    classified = await asyncio.to_thread(
        classify_middle_east_signal_batch, pool["new"], companies, us_universe,
        stored_event_members)
    stored_meanwhile = get_existing_middle_east_signal_source_ids(
        [c["article"]["url"] for c in classified])
    to_insert = [c for c in classified if c["article"]["url"] not in stored_meanwhile]

    inserted = 0
    for c in to_insert:
        try:
            insert_middle_east_signal_classification(job_id, c["article"], c["result"])
            inserted += 1
        except Exception:
            logger.exception(
                "Failed to insert middle_east_market_signal classification for"
                " url=%s (job %d)", c["article"].get("url"), job_id)

    logger.info(
        "[MIDDLE_EAST_SIGNAL] job=%d classified=%d stored_meanwhile=%d"
        " inserted=%d unjudged=%d",
        job_id, len(classified), len(stored_meanwhile), inserted,
        len(pool["new"]) - len(classified))
    if inserted:
        regroup_middle_east_stories(job_id)
    update_job_status(job_id, "completed", article_count=inserted, set_completed_at=True)


def regroup_middle_east_stories(job_id: int) -> None:
    """After each insert: fold same-event duplicates into one row
    (tombstones), then regroup the remaining signal/weak_signal rows into
    stories, from scratch - a later arrival can change which story an
    earlier row belongs to. Nothing is deleted; see
    pipeline/middle_east_signal_classifier.py."""
    folded = 0
    for group in find_same_event_groups(get_middle_east_story_candidates()):
        try:
            folded += collapse_middle_east_same_event(
                group["kept"]["id"],
                folded_corroboration(group["kept"], group["duplicates"]),
                [d["id"] for d in group["duplicates"]])
        except Exception:
            logger.exception("[MIDDLE_EAST_SIGNAL] failed to fold duplicates into row %s (job %d)",
                             group["kept"]["id"], job_id)
    logger.info("[MIDDLE_EAST_SIGNAL] same-event duplicates folded: job=%d rows=%d", job_id, folded)

    rows = get_middle_east_story_candidates()
    cleared = clear_middle_east_story_fields()
    stories = build_middle_east_stories(rows)
    absorbed = 0
    for story in stories:
        member_ids = [m["row"]["id"] for m in story["members"]]
        try:
            apply_middle_east_story(story["seed"]["id"],
                                    development=middle_east_story_development(story),
                                    member_ids=member_ids)
            absorbed += len(member_ids)
        except Exception:
            logger.exception("[MIDDLE_EAST_SIGNAL] failed to apply story seed=%s (job %d)",
                             story["seed"]["id"], job_id)
    logger.info("[MIDDLE_EAST_SIGNAL] stories: job=%d rows=%d cleared=%d stories=%d"
                " absorbed=%d visible=%d", job_id, len(rows), cleared, len(stories),
                absorbed, len(rows) - absorbed)


async def run_japan_signal_classification(job_id: int, from_date: str, to_date: str) -> None:
    """Classify japan_market_signal items (J1/jp_forecast only, so far)
    across ALL of news-retrieval's completed runs in [from_date, to_date] -
    same reasoning as run_korea_signal_classification/
    run_taiwan_signal_classification (news-retrieval polls this domain on
    its own daily/monthly schedule, so a multi-day window can still span
    more than one completed run).

    Loads the pre-computed per-company habit cache (models.
    japan_company_habits.get_all_habits(), refreshed separately by
    refresh_japan_habits() below - NOT recomputed from this window's own
    articles, per the explicit user instruction classify_forecast_
    revision's own docstring documents) and passes it through to
    classify_japan_signal_batch, so a routine daily classification run
    always compares a new revision against the company's real FULL
    history, not just whatever happens to be in this narrow window.
    """
    update_job_status(job_id, "running")
    try:
        run_ids = await list_completed_runs(
            config.JAPAN_SIGNAL_DOMAIN, from_date, to_date,
        )
        all_articles: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for run_id in run_ids:
            for article in await get_run_articles(run_id):
                url = article.get("url")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(article)
    except NewsRetrievalError:
        logger.exception(
            "Failed to fetch japan_market_signal articles for job %d", job_id,
        )
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    if not all_articles:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    stored_habits = get_all_habits()
    stored_progress_habits = get_all_progress_habits()
    stored_company_reference = get_all_company_reference()
    classified = classify_japan_signal_batch(
        all_articles, stored_habits=stored_habits, stored_progress_habits=stored_progress_habits,
        stored_company_reference=stored_company_reference,
    )

    candidate_source_ids = [c["result"]["source_id"] for c in classified if c["result"].get("source_id")]
    already_done = get_existing_japan_signal_source_ids(candidate_source_ids)
    to_insert = [
        c for c in classified
        if c["result"].get("source_id") not in already_done
    ]

    update_job_status(job_id, "running", article_count=len(to_insert))

    inserted = 0
    for c in to_insert:
        try:
            insert_japan_signal_classification(job_id, c["article"], c["result"])
            inserted += 1
        except Exception:
            logger.exception(
                "Failed to insert japan_market_signal classification for"
                " source_id=%s (job %d)",
                c["result"].get("source_id"), job_id,
            )

    logger.info(
        "[JAPAN_SIGNAL] job=%d runs=%d pooled_articles=%d classified=%d"
        " already_done=%d inserted=%d",
        job_id, len(run_ids), len(all_articles), len(classified),
        len(already_done), inserted,
    )
    update_job_status(job_id, "completed", article_count=inserted, set_completed_at=True)


async def refresh_japan_habits(from_date: str, to_date: str, computed_from_years: int) -> int:
    """Recompute every tracked company's forecast-revision habit from its
    FULL stored history and overwrite the japan_company_habits cache -
    explicit user instruction 2026-09-28: "save those [four numbers]
    against each company... then when a new revision comes in, compare it
    to that company's own four numbers instead of a fixed threshold."

    Pools ALL of news-retrieval's completed japan_market_signal runs in
    [from_date, to_date] - normally a wide window (e.g. 5 years back to
    today) covering the full backfilled history, not a narrow recent
    window the way run_japan_signal_classification's own window is -
    a habit computed from only recent revisions would drift from the
    company's own real long-run pattern (see compute_forecast_habit's own
    docstring on why median-based/multi-year framing matters).

    Returns the number of companies written (len(japan_ticker_universe()) -
    every tracked company gets a row, even one with zero revisions in the
    pooled window - see compute_all_japan_habits's own docstring).

    Also runs classify_stale_revision_pattern (Section 10.3's WATCHING
    section - see that function's own docstring) against this same
    pooled window right after the habits cache is written, and inserts
    any real result as an ordinary agent_classifications row (source_
    category='jp_watching') - a genuinely different kind of write from
    the three delete-then-replace caches above (japan_company_habits/
    japan_progress_habits/japan_company_reference): this is a
    classification result, not a reference cache, so it goes through
    insert_japan_signal_classification/get_existing_japan_signal_
    source_ids, deduped per calendar day (see that function's own
    source_id, which embeds as_of's date) rather than replaced wholesale
    on every refresh.

    Entry point for a scheduled task (refresh-japan-habits CLI command,
    __main__.py) - intended to run occasionally (e.g. monthly), not on
    every classification pass, since a company's real multi-year habit
    does not meaningfully change day to day. WATCHING piggybacks on this
    same occasional cadence deliberately - see classify_stale_revision_
    pattern's own docstring for why it needs the same full-history pool
    this function already assembles, which the daily classify-japan-
    signals job's own narrow window cannot provide.
    """
    run_ids = await list_completed_runs(config.JAPAN_SIGNAL_DOMAIN, from_date, to_date)
    all_articles: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for run_id in run_ids:
        for article in await get_run_articles(run_id):
            url = article.get("url")
            if url and url not in seen_urls:
                seen_urls.add(url)
                all_articles.append(article)

    habits = compute_all_japan_habits(all_articles, computed_from_years=computed_from_years)
    replace_habits(habits)

    # Spec Section 10.3's WATCHING section (classify_stale_revision_
    # pattern - see that function's own docstring for the full design) -
    # deliberately run HERE, not from run_japan_signal_classification,
    # because it needs each company's real FULL revision history to find
    # a genuine "last revision date", exactly the wide pooled window this
    # function already assembles (the daily classify job only ever pools
    # a single day - see __main__.py's classify-japan-signals docstring -
    # so it would almost never see a company's actual last revision).
    # Reads the habits JUST written above via get_all_habits() rather than
    # the list `habits` variable, since that function needs the same
    # {code: habit_dict} lookup shape classify_forecast_revision itself
    # reads - one extra read of what was just written, same "read back
    # what was just persisted" pattern already used lower down in this
    # function for company reference (see reference_articles below).
    #
    # This is a genuinely new kind of write for this function: every
    # other cache here is a full delete-then-replace table
    # (japan_company_habits/japan_progress_habits/japan_company_
    # reference), but a stale-revision reading is a CLASSIFICATION result
    # (source_type=japan_market_signal, like every J1-J7 row), not a
    # reference cache - so it goes through the same
    # insert_japan_signal_classification path
    # run_japan_signal_classification itself uses. Unlike that path, it
    # keys one row per COMPANY (not per company per day) and refreshes
    # it in place, so the Watching tab keeps showing an overdue company
    # between refreshes instead of emptying at midnight - see
    # classify_stale_revision_pattern's own source_id comment.
    watching_job_id = create_job(domain=config.JAPAN_SIGNAL_DOMAIN)
    update_job_status(watching_job_id, "running")
    watching_results = classify_stale_revision_pattern(all_articles, stored_habits=get_all_habits())
    # No already_done skip here, unlike every other Japan category. A
    # WATCHING source_id is now the company alone, so the row for a
    # company that is still overdue ALWAYS already exists - skipping it
    # would leave days_since_last_revision frozen at its first reading
    # forever. insert_japan_signal_classification upserts this one
    # category precisely so re-running refreshes the figure in place.
    watching_inserted = 0
    for r in watching_results:
        try:
            insert_japan_signal_classification(watching_job_id, r["article"], r["result"])
            watching_inserted += 1
        except Exception:
            logger.exception(
                "Failed to insert japan_market_signal WATCHING row for"
                " source_id=%s (job %d)",
                r["result"].get("source_id"), watching_job_id,
            )
    update_job_status(watching_job_id, "completed", article_count=watching_inserted, set_completed_at=True)
    logger.info(
        "[JAPAN_WATCHING] job=%d candidates=%d inserted=%d",
        watching_job_id, len(watching_results), watching_inserted,
    )

    # J2's progress habits are computed from the SAME pooled article set
    # as J1's own habits (both read the same jp_forecast history, just
    # different table _kinds within it - see compute_all_japan_progress_
    # habits's own docstring) - refreshed together in one job rather than
    # a separate CLI command/schedule, since there is no reason to pool
    # news-retrieval's articles twice for the same underlying data.
    company_name_by_code = {t["code"]: t["company"] for t in japan_ticker_universe()}
    progress_habits = compute_all_japan_progress_habits(all_articles)
    replace_progress_habits([
        {"code": code, "period_type": period_type, "company": company_name_by_code.get(code, code), **habit}
        for (code, period_type), habit in progress_habits.items()
    ])

    # J5/J6's own denominators (total assets, shares outstanding) - see
    # models.japan_company_reference's own docstring. Unlike habits/
    # progress_habits above, this is not computed from a full multi-year
    # history - irbank_company_reference articles are ALREADY only ever
    # the single newest known figure per company (see news-retrieval's
    # own comment on why only the last bs-table row is kept), so the
    # newest-published row per code in this SAME pooled article set is
    # simply used as-is, no aggregation needed. Pooled from the same
    # from_date/to_date window as the habits above rather than a separate
    # fetch, since irbank_company_reference is seeded into the same
    # japan_market_signal domain news-retrieval already polls.
    reference_articles = [
        a for a in all_articles
        if (a.get("metadata") or {}).get("source_category") == "jp_company_reference"
    ]
    latest_reference_by_code: dict[str, dict[str, Any]] = {}
    for a in reference_articles:
        meta = a.get("metadata") or {}
        code = meta.get("code")
        if not code:
            continue
        pub_dt = _article_published_dt(a)
        existing = latest_reference_by_code.get(code)
        if existing is None or (pub_dt or datetime.min.replace(tzinfo=timezone.utc)) > (existing["_pub_dt"] or datetime.min.replace(tzinfo=timezone.utc)):
            latest_reference_by_code[code] = {**meta, "_pub_dt": pub_dt}
    replace_company_reference([
        {
            "code": code,
            "company": ref.get("company", company_name_by_code.get(code, code)),
            "total_assets_jpy_millions": ref.get("total_assets_jpy_millions"),
            "shares_outstanding": ref.get("shares_outstanding"),
            "fiscal_period": ref.get("fiscal_period"),
        }
        for code, ref in latest_reference_by_code.items()
    ])

    logger.info(
        "[JAPAN_HABITS] refreshed %d companies from %d pooled article(s) across %d run(s);"
        " %d (code, period_type) progress habit(s); %d company reference row(s)",
        len(habits), len(all_articles), len(run_ids), len(progress_habits),
        len(latest_reference_by_code),
    )
    return len(habits)


def generate_korea_signal_summary_for_date(date: str) -> str:
    """Spec Section 8.4: read today's (or ``date``'s) already-classified
    korea_market_signal rows from this service's own DB and generate the
    twice-daily trader summary text.

    Not async (unlike the run_* functions above) - list_all_results is a
    plain synchronous DB call (models/jobs.py, same psycopg2 connection
    every other model function in this service uses), and
    generate_korea_signal_summary itself is a synchronous urllib call
    (matching every other LLM call in this codebase's pipeline/*.py
    modules - none of them are async either). No news-retrieval fetch
    here at all, unlike run_korea_signal_classification - this only reads
    rows this service has ALREADY classified and inserted.

    Rows are sorted signal before weak_signal, then newest published
    first - see korea_signal_summary.py's own module docstring for why no
    numeric rank is fabricated here instead.

    ``date``: YYYY-MM-DD (UTC) - passed straight to list_all_results's
    published_from/published_to as a single-day window.
    """
    result = list_all_results(
        source_type=config.KOREA_SIGNAL_DOMAIN,
        published_from=date,
        published_to=date,
        limit=200,
    )
    rows = result["results"]

    # signal before weak_signal, newest published first within each group -
    # a stable sort by published-descending, then a stable partition by
    # signal_detection, achieves exactly that without a double-sort: the
    # partition preserves each group's own already-newest-first order
    # (list_all_results' own query already returns rows ORDER BY
    # effective_date DESC).
    signal_rows = [r for r in rows if r.get("signal_detection") == "signal"]
    weak_rows = [r for r in rows if r.get("signal_detection") == "weak_signal"]
    ordered_rows = signal_rows + weak_rows

    logger.info(
        "[KOREA_SIGNAL_SUMMARY] date=%s signal=%d weak_signal=%d",
        date, len(signal_rows), len(weak_rows),
    )
    return generate_korea_signal_summary(ordered_rows)


def _json_list(value: Any) -> list[Any]:
    """Return a list from a column that may hold JSON text, a parsed
    list, or nothing. entities_json is a TEXT column holding JSON, so a
    driver may hand back either the string or an already-parsed value
    depending on the query; both shapes are read the same way here.
    """
    if isinstance(value, list):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except ValueError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


async def run_geopolitical_signal_stage_a(job_id: int, from_date: str, to_date: str) -> None:
    """Stage A only: pool all of news-retrieval's completed geopolitical_news
    runs in [from_date, to_date], apply the free rule-based filters, persist.

    Pools across every completed run in the window rather than just the
    latest (same reasoning as run_taiwan_signal_classification) - a single
    run isn't guaranteed to be "today's everything" for this domain either.

    Only classifies articles not already classified - checked via
    get_existing_geopolitical_signal_article_ids against article_id
    (Stage A rule 1, "already classified: skip"), unlike Taiwan's
    source_id-based check, since geopolitical_signal's natural key really
    is the news-retrieval article_id (no ticker+period/ticker+timestamp
    concept here). No model call anywhere in this function - see
    pipeline/geopolitical_signal_classifier.py's module docstring.
    """
    update_job_status(job_id, "running")
    try:
        run_ids = await list_completed_runs(
            config.GEOPOLITICAL_SIGNAL_DOMAIN, from_date, to_date,
        )
        all_articles: list[dict[str, Any]] = []
        seen_urls: set[str] = set()
        for run_id in run_ids:
            for article in await get_run_articles(run_id):
                url = article.get("url")
                if url and url not in seen_urls:
                    seen_urls.add(url)
                    all_articles.append(article)
    except NewsRetrievalError:
        logger.exception(
            "Failed to fetch geopolitical_news articles for job %d", job_id,
        )
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    if not all_articles:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    already_done = get_existing_geopolitical_signal_article_ids(
        [a["id"] for a in all_articles if a.get("id") is not None],
    )
    to_classify = [a for a in all_articles if a.get("id") not in already_done]

    update_job_status(job_id, "running", article_count=len(to_classify))

    # "signal" included: Stage A's Federal Register rule promotes an
    # action notice straight past Stage B, and leaving it out of this
    # dict kept those rows out of the log line entirely.
    outcome_counts: dict[str, int] = {
        "noise": 0, "weak_signal": 0, "waiting": 0, "signal": 0,
    }
    inserted = 0
    for article in to_classify:
        result = classify_geopolitical_signal_stage_a(article)
        outcome_counts[result["outcome"]] = outcome_counts.get(result["outcome"], 0) + 1
        try:
            insert_geopolitical_signal_classification(job_id, article, result)
            inserted += 1
        except Exception:
            logger.exception(
                "Failed to insert geopolitical_signal Stage A result for"
                " article_id=%s (job %d)",
                article.get("id"), job_id,
            )

    logger.info(
        "[GEOPOLITICAL_SIGNAL_STAGE_A] job=%d runs=%d pooled_articles=%d"
        " already_done=%d classified=%d inserted=%d noise=%d weak_signal=%d"
        " waiting=%d signal=%d",
        job_id, len(run_ids), len(all_articles), len(already_done),
        len(to_classify), inserted, outcome_counts["noise"],
        outcome_counts["weak_signal"], outcome_counts["waiting"],
        outcome_counts["signal"],
    )
    update_job_status(job_id, "completed", article_count=inserted, set_completed_at=True)


def _outlet_from_url(url: str | None) -> str:
    """The publisher's own domain, which is what identifies a source to
    a reader. Same derivation news-retrieval uses when it records a
    dropped duplicate, so both kinds of coverage entry read alike.
    """
    host = (url or "").split("://", 1)[-1].split("/", 1)[0].strip().lower()
    return host[4:] if host.startswith("www.") else host


async def run_geopolitical_signal_stage_b(job_id: int) -> None:
    """Stage B: one Haiku-class HIGH/WEAK call per Stage A survivor
    (signal_detection='waiting'), update each row in place. Rows are
    classified concurrently (bounded by CLASSIFY_CONCURRENCY, the same
    thread pool the news domain and Stage C share), not one at a time.

    No fetch from news-retrieval here - Stage B's entire input is
    already sitting in agent_classifications from Stage A
    (get_waiting_geopolitical_signal_articles), including the source
    summary and, where there was none, an excerpt of the body
    news-retrieval had already sent with the article. One response
    Stage A was given, rather than one HTTP round trip per waiting row.

    Context was added after measuring what this stage actually sees:
    of 13 rows in one real run, 11 had NO context beyond the headline -
    including "China Moves to Stave Off Wider EU Trade War With Hybrid
    ...", truncated mid-phrase, which was judged WEAK without the model
    ever learning what the measure was or whether it had been enacted.
    This is the stage whose verdict is IRREVERSIBLE: WEAK means Stage C
    never runs, so no body is ever read for that row and nothing
    downstream can recover the signal. Giving the cheapest call the
    least evidence, while the stage that only adds tags reads 24,000
    characters, had the context budget backwards.

    Still one word out (max_tokens stays 5) - only the INPUT grows.
    Summary generation deliberately stays in Stage C: 13 rows reach here
    and 3 reach there, so generating summaries at this stage would
    produce ~11 that are discarded with the WEAK rows that carried them.

    Daily cap (config.GEOPOLITICAL_SIGNAL_STAGE_B_DAILY_CAP, default 1000)
    is a safety tripwire against a bug flooding the worklist, not a cost
    control (even a 10x normal day costs under $1 per the spec's own cost
    math) - once hit, remaining waiting rows are left untouched for the
    next run, never forced through and never dropped.
    """
    update_job_status(job_id, "running")

    waiting = get_waiting_geopolitical_signal_articles()
    if not waiting:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    # Group by the supplier's own event id BEFORE spending calls. Seven
    # outlets reporting one UN Security Council vote on Haiti sanctions
    # is one event and one verdict, not seven - and they are not
    # duplicates that could simply be dropped: each carries a different
    # fact ("adds no new names", "with Russia abstaining", "aim to
    # starve gangs of bullets"), so every row is kept and every row
    # gets the group's verdict.
    #
    # One member is judged, on the fullest text in the group, because a
    # verdict reached on 4,000 characters is better than the same
    # verdict reached on a 300-character wire brief covering the same
    # vote. The rest inherit it.
    #
    # Rows with no event_uri - a single-source story, or any source that
    # supplies no clustering - are their own group of one, so this
    # changes nothing for them.
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in waiting:
        key = row.get("event_uri") or f"solo:{row['id']}"
        groups.setdefault(key, []).append(row)

    def _best_evidence(members: list[dict[str, Any]]) -> dict[str, Any]:
        """Which member of an event group Stage B reads: the fullest one.

        A talk-verb preference was tried here and removed - it cannot
        fire. Stage A already short-circuits every talk-verb headline to
        weak_signal before Stage B runs, so by the time a group is
        formed no member can carry one, and the rule ranked all three
        articles of the real ICC cluster identically.

        That cluster is worth recording anyway, because it is the case
        this choice cannot solve: "U.S. Sanctions ICC" (signal) and
        "European states, Canada and Japan back ICC" (weak_signal alone)
        share one event_uri and are both 2,500 characters. Neither
        length nor any keyword separates the article reporting the
        action from the one reporting reactions to it - only reading
        them does, which is what judging every article used to do.

        So length is the honest rule: more text is a better-evidenced
        verdict, which is what settles the Iranian tanker cluster (five
        articles, 303 to 1,962 characters, and both shortest were judged
        weak_signal when read alone). The residual risk is the ICC
        shape, measured at 1 of 21 clusters, and it is bounded by the
        promotion rule below - if any member is judged signal, the
        group is a signal.
        """
        return max(
            members,
            key=lambda r: len(r.get("body") or "") + len(r.get("summary") or ""),
        )

    already_today = count_geopolitical_signal_stage_b_calls_today()
    remaining_cap = max(0, config.GEOPOLITICAL_SIGNAL_STAGE_B_DAILY_CAP - already_today)
    # The cap counts MODEL CALLS, which is now one per group.
    group_keys = list(groups)[:remaining_cap]
    to_classify = [_best_evidence(groups[k]) for k in group_keys]
    followers = {k: groups[k] for k in group_keys}
    capped = len(groups) - len(group_keys)
    grouped_away = len(waiting) - len(groups)

    update_job_status(job_id, "running", article_count=len(to_classify))

    system_prompt = load_stage_b_prompt()
    model = config.GEOPOLITICAL_SIGNAL_MODEL
    api_key = config.OPENAI_API_KEY
    base_url = config.OPENAI_BASE_URL
    timeout = config.OPENAI_TIMEOUT

    # Reuses the module-level _executor/CLASSIFY_CONCURRENCY that
    # run_agent_pipeline (news) and Stage C both share, rather than a
    # dedicated pool - same kind of blocking single-call HTTP classification,
    # no reason to double the thread pools in this process for it.
    semaphore = asyncio.Semaphore(config.CLASSIFY_CONCURRENCY)
    loop = asyncio.get_event_loop()

    async def classify_one(row: dict[str, Any]) -> tuple[str, str | None] | None:
        """Classify + persist one row; returns (outcome, reason), or None on
        failure.

        The REASON is returned alongside the outcome, not just the
        outcome, because the siblings of an event group need it: a row
        that inherits a verdict used to be given only the provenance
        ("Same event as X"), which says how the verdict arrived and
        never why it was reached. A reader opening one of six inherited
        rows got no account of what happened - they had to find the
        judged row to learn that two ships had been sunk. Every member
        of a group now carries this sentence and nothing else.
        """
        async with semaphore:
            # Both come from the row Stage A already wrote - no fetch
            # here. news-retrieval sends the body with the article
            # (get_run_articles passes include_body=true), so Stage A
            # stores an excerpt alongside the summary and this stage
            # reads both from its own table. The source's own summary
            # is preferred where it exists; the excerpt covers the rest.
            summary = row.get("summary")
            # BOTH, where both exist - not the summary instead of the
            # body. A filed abstract states what a measure is called;
            # the document states what it covers. Rows 42115/42117 are
            # the case: a 216-character agency abstract over a
            # 3,673-character determination naming the automotive and
            # rail sectors, and Stage B saw only the abstract while
            # Stage C read the whole thing. Stage B makes the
            # irreversible call, so it gets at least as much evidence as
            # the stage that merely tags.
            body = row.get("body")
            # Recorded so a verdict reached with no article text stays
            # distinguishable from one reached by reading the document.
            # Stage B is irreversible - a weak_signal row never reaches
            # Stage C - so this is the one failure nothing downstream
            # can revisit.
            #
            # Both sources deliver a body on every row, so this now
            # fires for a different reason than it used to: a body
            # news-retrieval REJECTED as unreadable. Event Registry
            # serves its own crawl of a publisher's page, and one row in
            # 325 arrived column-interleaved - stored as NULL rather
            # than as 5,220 characters of noise.
            headline_only = not summary and not body
            outcome, reason = await loop.run_in_executor(
                _executor,
                lambda: classify_geopolitical_signal_headline(
                    row["title"] or "",
                    summary=summary,
                    body=body,
                    system_prompt=system_prompt,
                    model=model,
                    api_key=api_key,
                    base_url=base_url,
                    timeout=timeout,
                ),
            )
            try:
                update_geopolitical_signal_classification(
                    row["id"], outcome=outcome, reason=reason,
                    headline_only=headline_only,
                )
                return outcome, reason
            except Exception:
                logger.exception(
                    "Failed to update geopolitical_signal Stage B result for"
                    " row id=%s article_id=%s (job %d)",
                    row["id"], row.get("article_id"), job_id,
                )
                return None

    outcomes = await asyncio.gather(*[classify_one(row) for row in to_classify])
    outcome_counts: dict[str, int] = {"signal": 0, "weak_signal": 0, "noise": 0}
    updated = 0
    inherited = 0
    for key, result in zip(group_keys, outcomes):
        if result is None:
            continue
        outcome, judged_reason = result
        outcome_counts[outcome] += 1
        updated += 1
        # One row per event. The other members of this group are folded
        # into the judged row as coverage - {outlet, title, url} on its
        # metadata.also_reported_by, which Stage D merges into
        # metadata.corroboration - and then DELETED.
        #
        # They used to be kept, each carrying a copy of this row's
        # verdict and reason, which stored one verdict in several places
        # and showed a reader four rows for one Strait of Hormuz
        # sinking. Their headlines survive on the judged row, which is
        # the part worth keeping: the variance across a cluster is
        # itself information.
        #
        # See collapse_geopolitical_signal_event_group for what this
        # discards and why it cannot be undone.
        judged = to_classify[group_keys.index(key)]
        siblings = [r for r in followers[key] if r["id"] != judged["id"]]
        if siblings:
            try:
                inherited += collapse_geopolitical_signal_event_group(
                    judged["id"],
                    [
                        {
                            "id": s["id"],
                            "title": s.get("title"),
                            "url": s.get("url"),
                            "outlet": _outlet_from_url(s.get("url")),
                        }
                        for s in siblings
                    ],
                )
            except Exception:
                logger.exception(
                    "Failed to collapse geopolitical_signal event group"
                    " judged_row=%s members=%d (job %d)",
                    judged["id"], len(siblings), job_id,
                )

    logger.info(
        "[GEOPOLITICAL_SIGNAL_STAGE_B] job=%d waiting=%d events=%d"
        " grouped_away=%d already_today=%d capped=%d classified=%d"
        " updated=%d inherited=%d signal=%d weak_signal=%d noise=%d",
        job_id, len(waiting), len(groups), grouped_away, already_today,
        capped, len(to_classify), updated, inherited,
        outcome_counts["signal"], outcome_counts["weak_signal"],
        outcome_counts["noise"],
    )
    update_job_status(job_id, "completed", article_count=updated, set_completed_at=True)


async def run_geopolitical_signal_stage_c(job_id: int) -> None:
    """Stage C: tag every untagged HIGH article - channel, assets,
    impacted_categories, entities, event_date, summary.

    Flow per article, matching the locked Stage C plan:
      GET /articles/{id} (body, best-effort) -> Layer 1 (free) -> Layer 2
      (1 model call) -> deterministic overrides (free) -> update in place.
    Layer 3 (category -> every ticker in that sector) was removed: the
    tickers a reader can be shown are the ones the article names, which
    Layer 2 returns in `entities`.
    Rows are classified concurrently (bounded by CLASSIFY_CONCURRENCY,
    same pool as Stage B and the news domain), not one at a time - each
    row's flow above runs as a single unit in the shared thread pool.

    Stage C's row data - title, published, body and summary - comes
    entirely from this table (get_untagged_geopolitical_signal_high_
    articles), with NO per-article fetch from news-retrieval. Stage A
    stores the whole body under metadata.body and both reading stages
    slice it themselves, so Layer 2 isn't judging off a headline alone -
    a terse headline often omits the specific goods/sector the prompt's
    own empty-impacted_categories rule requires.

    This stage used to re-fetch each article with get_article. That cost
    one HTTP round-trip per row for text already stored here, and it
    fails open to headline-only whenever the article has expired from
    news-retrieval's 7-day geopolitical_news retention - so the rows
    most worth describing were the ones most likely to arrive bare.
    Measured on job 1449: 3 of 63 rows were tagged
    `coverage: unreadable` and produced ZERO entities while holding a
    stored body averaging 3,830 characters. A row with no body at all
    still degrades to headline-only rather than failing, the same
    fail-open discipline as Layer 2's own model-call handling.
    Layer 1's company list is loaded ONCE for the whole run, not
    once per article (see get_companies_for_name_matching's docstring) -
    this was a deliberate fix during design review, not the naive
    per-article refetch the original plan implied.

    Daily cap (config.GEOPOLITICAL_SIGNAL_STAGE_C_DAILY_CAP, default 500)
    is a safety tripwire, same reasoning as Stage B's cap - once hit,
    remaining untagged rows are left untouched for the next run.
    """
    update_job_status(job_id, "running")

    untagged = get_untagged_geopolitical_signal_high_articles()
    if not untagged:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    already_today = count_geopolitical_signal_stage_c_calls_today()
    remaining_cap = max(0, config.GEOPOLITICAL_SIGNAL_STAGE_C_DAILY_CAP - already_today)
    to_classify = untagged[:remaining_cap]
    capped = len(untagged) - len(to_classify)

    update_job_status(job_id, "running", article_count=len(to_classify))

    companies = get_companies_for_name_matching()
    system_prompt = config.GEOPOLITICAL_SIGNAL_STAGE_C_PROMPT.read_text(encoding="utf-8")
    model = config.GEOPOLITICAL_SIGNAL_STAGE_C_MODEL
    api_key = config.OPENAI_API_KEY
    base_url = config.OPENAI_BASE_URL
    timeout = config.OPENAI_TIMEOUT

    # Runs each row's whole Layer1->Layer2->overrides->DB-write body as
    # one executor job, not split across separate awaits - Layer 1's
    # name matching and the DB write are blocking calls, so splitting
    # them into their own run_in_executor would still block the event
    # loop for nothing (no other awaitable work happens between them and
    # the model call). Reuses the same _executor/CLASSIFY_CONCURRENCY
    # pool Stage B and the news domain already share.
    def _tag_one_sync(
        row: dict[str, Any], body: str | None, summary: str | None = None,
    ) -> dict[str, Any] | None:
        title = row["title"] or ""

        # Layer 1 - free exact match of tracked company names against the
        # headline. Kept as a cheap cross-check and as Stage D's
        # "specific" input, but it is no longer stored as a field of its
        # own: metadata.impacted_companies_direct was null on every
        # geopolitical row ever written, because these headlines name
        # countries and institutions ("UK", "Iran", "EU", "OFAC") and
        # the companies appear in the body. What the article names now
        # comes from Layer 2's entities, which reads both.
        direct_matches = find_direct_company_matches(title, companies)

        # Layer 2 - one model call; fail-open on any parse/shape failure.
        # body is None when news-retrieval's article already expired
        # (geopolitical_news is a 7-day retention domain) or the fetch
        # failed - classify_geopolitical_signal_tags degrades to
        # headline-only in that case rather than failing the row.
        layer2_result = classify_geopolitical_signal_tags(
            title,
            body=body,
            # So the summary can distinguish when a measure was taken
            # from when it surfaced - a Federal Register notice often
            # publishes a determination issued months earlier.
            published=str(row.get("published") or "")[:10] or None,
            system_prompt=system_prompt,
            model=model,
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
        )

        # Resolve company entities against the tracked universe before
        # anything reads them: the universe's own name and ticker win
        # where it matches, so one company reads identically on every
        # row, and a ticker the model left null is recovered (measured:
        # "MP Materials" resolved to MP, which no ticker-based reader
        # could otherwise have found). `companies` is the same list
        # Layer 1 already loaded once for this whole run, so this costs
        # no extra query.
        layer2_result["entities"] = resolve_against_universe(
            layer2_result["entities"], companies,
        )

        # Deterministic overrides - only ever add categories.
        final_categories = apply_overrides(
            channel=layer2_result["channel"],
            entities=layer2_result["entities"],
            assets=layer2_result["assets"],
            impacted_categories=layer2_result["impacted_categories"],
        )

        # Layer 3 (category -> every ticker in that category) was
        # REMOVED. It expanded each matched category into the whole
        # research-universe membership of that sector, which is a
        # directory, not evidence: a UK airbase story naming no company
        # at all carried 11 tickers, an EU sanctions row carried 25 of
        # 123, and not one of them could be explained in terms of what
        # the article said. The rule this field could never satisfy -
        # show a few tickers, each with the reason it is there, and show
        # none you cannot explain - is satisfied by entities_json, which
        # carries only companies the article itself names, each with its
        # ticker where one exists and the text's own claim about it.
        #
        # models/geopolitical_signal_companies.py and the category map
        # it caches are NOT removed: Layer 1 above still matches tracked
        # company names against the headline to feed `has_path` and
        # Stage D's `specific` grade.

        tags = {
            # The date the measure was taken, where the text states one.
            # Distinct from the row's `published`, which for a Federal
            # Register notice is the date it appeared in the Register,
            # not the date the determination was issued.
            "event_date": layer2_result["event_date"],
            # WHERE it physically happened, as the text names it -
            # paired with event_date above, which says WHEN. Null on a
            # legal instrument (a sanctions designation happens in no
            # place) and on anything whose text names no location;
            # a country where that is the finest the text gives.
            "event_location": layer2_result["event_location"],
            "channel": layer2_result["channel"],
            "assets": layer2_result["assets"],
            "impacted_categories": final_categories or None,
            # The source's own filed summary wins over a generated one.
            #
            # Federal Register documents carry an `abstract` the issuing
            # agency wrote and filed with the document itself - the
            # authoritative statement of what the measure does. Anything
            # the model writes from the same text is at best a faithful
            # paraphrase and at worst drifts on a number, which on a legal
            # record is the whole signal ("imposes 25%" vs "30%").
            #
            # Measured over 154 real documents: 113 carry an abstract, 41
            # do not - and every one of those 41 is a Presidential
            # Document, which is exactly the category Stage A's own
            # Federal Register rule exists to catch. Those are the rows
            # that need a generated summary, and they now have a real body
            # to generate it FROM (news-retrieval fetches raw_text_url),
            # rather than a bare bureaucratic title.
            #
            # `summary_source` records which path produced it, so a reader
            # can tell a filed abstract from a model's reading of the
            # document rather than having to trust them equally.
            # A summary written from the document beats the one filed
            # with it. The filed abstract is written for a filing desk,
            # not a reader: Venezuela GL 60's own abstract says OFAC "is
            # publishing a general license... previously made available
            # on OFAC's website" and never mentions that the licence
            # authorises earthquake-relief transactions or that it
            # expires 23 October 2026 - both of which are in the body.
            # So the generated summary wins wherever a body existed to
            # write it from, and the filed text is the fallback for rows
            # that had none.
            "summary": layer2_result["generated_summary"] or summary,
            "summary_source": (
                "generated" if layer2_result["generated_summary"]
                else ("source" if summary else None)
            ),
        }
        # Layer 2 returning all-null (network error, unparseable JSON)
        # means the question was never asked. "We could not check" must
        # not be recorded as "we checked and found nothing" - the same
        # distinction Stage A's drop_reason keeps - so such a row gets
        # no coverage value at all, rather than one asserting a finding.
        tagging_failed = (
            layer2_result["channel"] is None
            and layer2_result["impacted_categories"] is None
            and layer2_result["generated_summary"] is None
        )
        # Reaching no AI-economy category is RECORDED, never acted on.
        # Stage B already answered the question that decides a signal -
        # did a real action occur - and it answers it without any notion
        # of this universe. Letting Stage C overturn that on coverage
        # grounds confuses two different things: whether an event is
        # real, and whether we currently map it to companies.
        #
        # The OFAC determination applying sanctions to Iran's automotive
        # and rail sectors is the case that showed it. A genuine
        # expansion of a sanctions programme, correctly identified, and
        # it vanished from the feed because no company in a 19-category
        # AI taxonomy sits in Iranian rail. The event did not stop being
        # a sanctions action; our coverage of it is what is missing, and
        # that is a fact about us, not about the news.
        #
        # So the row stays `signal` and carries `coverage` instead.
        # Ticker mapping is unaffected - an unmapped row simply has no
        # tickers, which is already how it reads.
        # Layer 1's direct name match is its own evidence of a path: the
        # headline names a company in the tracked universe, whatever the
        # model concluded about sectors.
        has_path = bool(final_categories) or bool(direct_matches)
        if not has_path and not tagging_failed:
            # Two different reasons to have no category, kept apart
            # because they call for opposite handling. "unreadable"
            # means the publisher gave us nothing to read - reuters.com
            # answers 401 to every fetch strategy tried, including the
            # Internet Archive's own crawler, and ft.com serves a page
            # whose HTML does not contain the article's words - so the
            # row is worth re-reading if that ever changes.
            # "outside_coverage" means we read the document and the
            # sectors it names are genuinely not in this universe; there
            # is nothing to re-read.
            #
            # The mapped case writes nothing: impacted_categories
            # already says so, and a "coverage": "mapped" on every
            # signal would be a second field repeating the first.
            tags["coverage"] = (
                "unreadable" if (not body and not summary)
                else "outside_coverage"
            )

        try:
            update_geopolitical_signal_stage_c_tags(
                row["id"], tags=tags, entities=layer2_result["entities"],
            )
            return tags
        except Exception:
            logger.exception(
                "Failed to update geopolitical_signal Stage C tags for"
                " row id=%s article_id=%s (job %d)",
                row["id"], row.get("article_id"), job_id,
            )
            return None

    semaphore = asyncio.Semaphore(config.CLASSIFY_CONCURRENCY)
    loop = asyncio.get_event_loop()

    async def tag_one(row: dict[str, Any]) -> dict[str, Any] | None:
        async with semaphore:
            # Both come from the row Stage A already wrote - no fetch
            # here, same as Stage B. This used to call get_article per
            # row to re-read text this service already stores, which
            # cost one HTTP round-trip per row and, worse, failed open
            # to headline-only whenever news-retrieval had expired the
            # article (geopolitical_news is a 7-day retention domain).
            # Measured on job 1449: 3 of 63 rows came back
            # `coverage: unreadable` with ZERO entities while holding a
            # stored body averaging 3,830 characters - including an OFAC
            # Iran sanctions notice naming three institutions. The text
            # was never missing; nothing read it.
            body = row.get("body") or None
            # The source's OWN summary, where the fetcher stored one.
            # Federal Register rows carry the document's filed abstract
            # here (news-retrieval maps the API's `abstract` field to
            # `summary`); a generated summary can only ever paraphrase
            # that, so it is preferred verbatim - see _tag_one_sync's
            # summary/summary_source resolution.
            summary = row.get("summary") or None
            return await loop.run_in_executor(
                _executor, _tag_one_sync, row, body, summary,
            )

    results = await asyncio.gather(*[tag_one(row) for row in to_classify])
    updated = sum(1 for r in results if r is not None)

    logger.info(
        "[GEOPOLITICAL_SIGNAL_STAGE_C] job=%d untagged=%d already_today=%d"
        " capped=%d classified=%d updated=%d",
        job_id, len(untagged), already_today, capped, len(to_classify), updated,
    )
    update_job_status(job_id, "completed", article_count=updated, set_completed_at=True)


async def run_geopolitical_signal_stage_d(job_id: int) -> None:
    """Stage D: attach each signal's same-event sibling coverage - which
    other outlets reported this event, and what they called it.

    No model call, no daily cap (unlike Stage B/C - there's no external
    API cost here to tripwire against), and no news-retrieval call: the
    siblings are rows in this service's own table, written by Stage A,
    found by grouping on metadata.event_uri.

    This stage used to compute a TOP/STRONG/STANDARD grade from three
    booleans. All three had collapsed against the real two-source pool
    and the grade carried no information; see the module docstring in
    pipeline/geopolitical_signal_stage_d.py for the measurements and
    for why a rebuilt grade was rejected rather than retuned.
    """
    update_job_status(job_id, "running")

    ungraded = get_ungraded_geopolitical_signal_tagged_articles()
    if not ungraded:
        update_job_status(job_id, "completed", article_count=0, set_completed_at=True)
        return

    update_job_status(job_id, "running", article_count=len(ungraded))

    # Every cluster for this whole batch in ONE query, before the loop -
    # clusters overlap (seven signals can share one event), so a per-row
    # lookup would re-read the same event repeatedly. Same reasoning as
    # Stage C loading its company list once per run.
    event_uris = sorted({r["event_uri"] for r in ungraded if r.get("event_uri")})
    clusters = get_geopolitical_signal_event_members(event_uris)

    # No semaphore, no executor, no gather. This stage makes no model
    # call and no network call - it reads rows this service already has
    # and writes them back - so the async scaffolding the other stages
    # need would only add machinery around two synchronous queries.
    graded = 0
    with_siblings = 0
    for row in ungraded:
        corroboration = build_corroboration(
            event_uri=row.get("event_uri"),
            row_id=row["id"],
            cluster=clusters.get(row.get("event_uri") or ""),
            # The second source: what news-retrieval's own dedup
            # discarded as near-duplicates of this article. Ten of 87
            # signals carry this and no event_uri, and reported no
            # coverage at all until Stage D read it.
            also_reported_by=_json_list(row.get("also_reported_by")),
            # This row's own url, so an entry naming the article itself
            # is never counted as another outlet covering it.
            self_url=row.get("url"),
        )
        try:
            update_geopolitical_signal_stage_d_grade(
                row["id"], grade_fields={"corroboration": corroboration},
            )
            graded += 1
            if corroboration["count"]:
                with_siblings += 1
        except Exception:
            logger.exception(
                "Failed to update geopolitical_signal Stage D corroboration"
                " for row id=%s article_id=%s (job %d)",
                row["id"], row.get("article_id"), job_id,
            )

    logger.info(
        "[GEOPOLITICAL_SIGNAL_STAGE_D] job=%d ungraded=%d updated=%d"
        " events=%d with_siblings=%d alone=%d",
        job_id, len(ungraded), graded, len(event_uris),
        with_siblings, graded - with_siblings,
    )
    update_job_status(job_id, "completed", article_count=graded, set_completed_at=True)


async def run_geopolitical_signal_stage_e(job_id: int) -> None:
    """Stage E: group signals that belong to one developing STORY.

    No model call and no network call - the evidence is the entities
    Stage C already verified against the article text.

    Stage D asked who else reported the same EVENT. This asks whether
    two different events belong to one situation: a mine strike, a
    blockade and a Treasury designation can be three real events and
    still read as one Strait of Hormuz story to anyone scrolling.

    Nothing is deleted. The seed row gets metadata.story_development
    and the others metadata.in_story, so one row per story is a filter
    the caller applies:

        signal_detection = 'signal' AND NOT (metadata ? 'in_story')

    See pipeline/geopolitical_signal_stage_e.py for why grouping must
    not delete here where Stage B's collapse safely does.

    Idempotent by REWRITING, not by skipping: both keys are cleared
    first and every story regrouped. Grouping is a property of the
    whole set, so a row arriving later can change which story an
    existing row belongs to - skipping already-done rows would leave a
    stale pointer hiding a row from the default view permanently.
    """
    rows = get_geopolitical_signal_story_candidates()
    cleared = clear_geopolitical_signal_story_fields()

    stories = build_stories(rows)

    grouped = members_total = 0
    for story in stories:
        seed = story["seed"]
        member_ids = [m["row"]["id"] for m in story["members"]]
        try:
            apply_geopolitical_signal_story(
                seed["id"],
                development=story_development(story),
                member_ids=member_ids,
            )
            grouped += 1
            members_total += len(member_ids)
        except Exception:
            logger.exception(
                "Failed to apply story grouping seed=%s members=%d (job %d)",
                seed["id"], len(member_ids), job_id,
            )

    logger.info(
        "[GEOPOLITICAL_SIGNAL_STAGE_E] job=%d signals=%d cleared=%d"
        " stories=%d absorbed=%d visible=%d",
        job_id, len(rows), cleared, grouped, members_total,
        len(rows) - members_total,
    )
    update_job_status(
        job_id, "completed", article_count=grouped, set_completed_at=True)


async def run_agent_pipeline(job_id: int, domain: str, news_run_id: int, limit: int | None = None) -> None:
    """Background task: fetch → classify → persist.

    Routing is entirely dispatch.get_domain_config(domain) - sec_filing and
    taiwan_market_signal never reach this function (they have their own
    entry points: controllers/filing_run.py, run_taiwan_signal_classification
    above), so an unregistered domain here is either a caller error (typo,
    stale domain string) or a genuinely new domain that hasn't been
    registered in pipeline/dispatch.py yet - either way, fail the job loudly
    rather than silently classifying it against the wrong domain's prompt
    and schema.
    """
    update_job_status(job_id, "running")
    try:
        articles = await get_run_articles(news_run_id)
    except NewsRetrievalError:
        logger.exception("Pre-classification pipeline failed for job %d", job_id)
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    usable = [a for a in articles if has_usable_body(a)]
    if limit is not None:
        usable = usable[:limit]
    update_job_status(job_id, "running", article_count=len(usable))

    domain_config = get_domain_config(domain)
    if domain_config is None:
        logger.error(
            "No classifier registered for domain=%r (job %d) - known domains: %s",
            domain, job_id, ", ".join(known_domains()),
        )
        update_job_status(job_id, "failed", set_completed_at=True)
        return

    batch_context = [
        {"title": a.get("title", ""), "url": a.get("url", "")}
        for a in usable[:20]
    ]

    semaphore = asyncio.Semaphore(config.CLASSIFY_CONCURRENCY)
    loop = asyncio.get_event_loop()

    async def classify_one(article: dict[str, Any]) -> bool:
        """Classify a single article; returns True if skipped."""
        async with semaphore:
            try:
                result = await loop.run_in_executor(
                    _executor,
                    lambda a=article: domain_config.classify_one(a, batch_context=batch_context),
                )
                domain_config.insert_fn(job_id, article, result)
                return False
            except Exception:
                logger.exception(
                    "Classification failed for article %s (job %d, domain=%s)",
                    article.get("url"), job_id, domain,
                )
                return True

    outcomes = await asyncio.gather(*[classify_one(a) for a in usable])
    skipped = sum(outcomes)

    if skipped == len(usable) and usable:
        update_job_status(job_id, "failed", set_completed_at=True)
    else:
        update_job_status(job_id, "completed", set_completed_at=True)
