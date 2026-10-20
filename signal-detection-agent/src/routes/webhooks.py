"""Inbound webhooks from other OCN services."""
from __future__ import annotations

import asyncio
import logging
from typing import Any

from fastapi import APIRouter, BackgroundTasks, Depends
from pydantic import BaseModel

import config
from auth import require_auth

router = APIRouter()
logger = logging.getLogger(__name__)


class NewsRetrievalRunCompletedPayload(BaseModel):
    """Shape news-retrieval's _fire_webhook posts (controllers/run.py) -
    not owned by this service, kept in sync manually if that shape changes.
    """
    run_id: int
    status: str
    domain: str
    summary: str | None = None


def _run_geopolitical_signal_pipeline_in_background() -> None:
    """Kick off the full A->B->C->D pipeline, same entry point the daily
    CloudWatch schedule uses (controllers.run.run_geopolitical_signal_
    pipeline is a CLI-only wrapper; this calls the underlying stage
    functions directly since we're already inside the running server
    process, not a fresh one-shot CLI invocation).
    """
    from datetime import datetime, timezone

    from controllers.run import (
        run_geopolitical_signal_stage_a,
        run_geopolitical_signal_stage_b,
        run_geopolitical_signal_stage_c,
        run_geopolitical_signal_stage_d,
        run_geopolitical_signal_stage_e,
    )
    from models.jobs import create_job

    async def _run() -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        stage_a_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
        logger.info(
            "[WEBHOOK] geopolitical_news run completed - starting Stage A"
            " job_id=%s", stage_a_job_id,
        )
        await run_geopolitical_signal_stage_a(stage_a_job_id, today, today)

        stage_b_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
        await run_geopolitical_signal_stage_b(stage_b_job_id)

        stage_c_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
        await run_geopolitical_signal_stage_c(stage_c_job_id)

        stage_d_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
        await run_geopolitical_signal_stage_d(stage_d_job_id)

        # Stage E last, and never skipped: every API read of this domain
        # is expected to be post-Stage-E, because the default signals
        # view filters on NOT (metadata ? 'in_story'). If E never runs,
        # that filter matches every row and the reader sees one story
        # repeated under several headlines - the exact thing E exists to
        # prevent. Unlike A-D it is not scoped to the rows the previous
        # stage just wrote: grouping is a property of the whole signal
        # set, so it re-reads every Stage-D-graded signal and regroups
        # from scratch, clearing its own two keys first.
        stage_e_job_id = create_job(domain=config.GEOPOLITICAL_SIGNAL_DOMAIN)
        await run_geopolitical_signal_stage_e(stage_e_job_id)

        logger.info("[WEBHOOK] geopolitical_signal pipeline finished (triggered by webhook)")

    asyncio.run(_run())


@router.post("/webhooks/news-retrieval-run-completed")
async def news_retrieval_run_completed(
    payload: NewsRetrievalRunCompletedPayload,
    background_tasks: BackgroundTasks,
    caller: dict[str, Any] = Depends(require_auth),
) -> dict[str, Any]:
    """Receive news-retrieval's run-completion callback and immediately
    start the geopolitical_signal pipeline (Stage A -> B -> C -> D -> E),
    instead of waiting for the fixed-offset daily CloudWatch schedule to
    eventually pick it up.

    Only acts on domain=geopolitical_news + status=completed - a failed
    run has nothing new to classify, and this endpoint has no reason to
    care about any other domain. The existing 05:00 UTC schedule
    (infra/modules/ecs_cluster/services.tf, signal_detection_agent_
    geopolitical_signal_pipeline_daily) stays in place as a fallback: this
    webhook's delivery is fire-and-forget on news-retrieval's side (no
    retry - see controllers/run.py's _fire_webhook there), so a missed
    delivery (this service briefly down/redeploying) still gets picked up
    a few hours later by the schedule rather than silently never running.
    Running the pipeline twice in one day is harmless - every stage's
    worklist query only selects rows the previous stage hasn't processed
    yet, so a second run that finds nothing new to do is a fast no-op.
    """
    if payload.domain != config.GEOPOLITICAL_SIGNAL_DOMAIN or payload.status != "completed":
        logger.info(
            "[WEBHOOK] ignoring run_id=%s domain=%s status=%s (not a"
            " completed geopolitical_news run)",
            payload.run_id, payload.domain, payload.status,
        )
        return {"accepted": False}

    logger.info(
        "[WEBHOOK] geopolitical_news run_id=%s completed - queuing"
        " geopolitical_signal pipeline", payload.run_id,
    )
    background_tasks.add_task(_run_geopolitical_signal_pipeline_in_background)
    return {"accepted": True}
