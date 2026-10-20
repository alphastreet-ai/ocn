"""Digest pipeline orchestration: check → run → fetch → group → email."""
import logging
from datetime import date, datetime, timezone
from typing import Any

from adapters import llm_client, signal_agent_client, smtp_sender
import config

logger = logging.getLogger(__name__)

_SIGNAL_TIER_ORDER = {"signal": 0, "weak_signal": 1, "noise": 2}


def dedupe_by_story(articles: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Collapse near-duplicate articles (same event, different outlet rewrite)
    to the single highest-scoring one, so one story can't flood a category or
    the top-conviction strip. Entity/title word overlap can't reliably tell
    paraphrased duplicates from distinct stories about the same company, so
    this delegates to an LLM clustering call (see llm_client.group_duplicate_stories)."""
    ordered = sorted(articles, key=_sort_key)
    if len(ordered) <= 1:
        return ordered
    keep_indices = llm_client.group_duplicate_stories(ordered)
    dropped = len(ordered) - len(keep_indices)
    if dropped:
        logger.info("Deduped %d near-duplicate article(s) across %d kept", dropped, len(keep_indices))
    return [ordered[i] for i in keep_indices]


def _is_today(iso_timestamp: str | None) -> bool:
    if not iso_timestamp:
        return False
    try:
        dt = datetime.fromisoformat(iso_timestamp)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.date() == date.today()
    except ValueError:
        return False


def _sort_key(a: dict[str, Any]) -> tuple:
    return (
        _SIGNAL_TIER_ORDER.get(a.get("signal_detection", "noise"), 2),
        -float(a.get("signal_score") or 0),
    )


def run_digest_polling(force: bool = False) -> None:
    """Run the full digest pipeline using polling (cron path)."""
    logger.info("Starting digest pipeline (polling mode, force=%s)", force)

    if not force:
        latest = signal_agent_client.get_latest_job()
        if latest and _is_today(latest.get("completed_at")):
            job_id = latest["id"]
            logger.info("Today's job already exists (job_id=%s)", job_id)
            _build_and_send(job_id=job_id)
            return

    logger.info("Triggering fresh pipeline run")
    job_id = signal_agent_client.trigger_run(domain="ai_news")
    logger.info("Pipeline triggered: job_id=%s", job_id)
    signal_agent_client.poll_job_until_done(job_id)
    logger.info("Pipeline completed: job_id=%s", job_id)
    _build_and_send(job_id=job_id)


async def run_digest_async() -> None:
    """Run the full digest pipeline asynchronously (A2A serve path)."""
    import asyncio

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, run_digest_polling)


def _build_and_send(job_id: int) -> None:
    """Fetch results, group by category, summarise, and send the email."""
    logger.info("Fetching results for job_id=%s", job_id)
    results = signal_agent_client.get_job_results(job_id)
    logger.info("Fetched %d classification rows", len(results))

    # Group all articles by category.
    #
    # `category` is a LIST - one entry for a news row, several for a
    # geopolitical_signal row, which reaches more than one sector. An
    # article in three categories is listed under each, because a reader
    # opening the Energy section wants it there whether or not it also
    # touches Semiconductors.
    #
    # A bare string is still accepted: the column was TEXT before it was
    # TEXT[], so this runs correctly against either. Without that branch
    # the string case falls into `"Energy" not in CATEGORIES` being
    # tested on a list, which is always True - every article skipped and
    # an EMPTY DIGEST sent, with no error raised anywhere.
    all_categorised: dict[str, list[dict[str, Any]]] = {}
    for r in results:
        raw = r.get("category")
        if not raw:
            continue
        for category in ([raw] if isinstance(raw, str) else raw):
            if category in config.CATEGORIES:
                all_categorised.setdefault(category, []).append(r)

    # Only keep categories that have at least one signal or weak_signal
    categorised: dict[str, list[dict[str, Any]]] = {
        cat: articles
        for cat, articles in all_categorised.items()
        if any(
            a.get("signal_detection") in ("signal", "weak_signal")
            for a in articles
        )
    }

    # Within each category: only signal + weak_signal articles, dedupe near-identical
    # outlet rewrites of the same story, signals first by score, max 10
    visible: dict[str, list[dict[str, Any]]] = {
        cat: dedupe_by_story(
            [a for a in articles if a.get("signal_detection") in ("signal", "weak_signal")]
        )[:10]
        for cat, articles in categorised.items()
    }

    # Order categories by their highest signal_score descending
    ordered_categories = sorted(
        visible.keys(),
        key=lambda cat: -float(visible[cat][0].get("signal_score") or 0),
    )

    logger.info(
        "Visible categories (%d): %s", len(ordered_categories), ordered_categories
    )

    # Summarise each visible category (pass all articles incl. noise for richer context)
    summaries: dict[str, str] = {}
    for cat in ordered_categories:
        articles = all_categorised[cat]
        logger.info("Summarising category: %s (%d articles)", cat, len(articles))
        summaries[cat] = llm_client.summarise_category(cat, articles)

    # Top 10 highest-scoring across all categories: signals first, fill with weak_signal
    all_visible = [a for articles in visible.values() for a in articles]
    signals = sorted(
        [a for a in all_visible if a.get("signal_detection") == "signal"],
        key=lambda a: -float(a.get("signal_score") or 0),
    )
    if len(signals) < 10:
        weak = sorted(
            [a for a in all_visible if a.get("signal_detection") == "weak_signal"],
            key=lambda a: -float(a.get("signal_score") or 0),
        )
        top_10 = (signals + weak)[:10]
    else:
        top_10 = signals[:10]

    smtp_sender.send_digest(
        summaries=summaries,
        visible=visible,
        ordered_categories=ordered_categories,
        top_articles=top_10,
        all_categorised=all_categorised,
    )
    logger.info("Digest pipeline complete")
