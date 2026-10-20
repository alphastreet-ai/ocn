"""Pipeline execution controller."""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, TypedDict

import httpx
from pydantic import BaseModel, Field

import pipeline as pl
from typing import Any

from models.api_key_domains import has_domain_access
from models.articles import (
    create_articles,
    fetch_all_articles_for_run,
    filter_articles_for_window,
)
from models.atomic import atomic
from models.domains import get_domain_by_slug, lock_domain_row
from models.runs import (
    RunRow,
    complete_run,
    create_run,
    fail_run,
    get_cached_run_today,
    get_covering_run_today,
    get_run,
    get_running_run_for_domain,
)

logger = logging.getLogger(__name__)

# The relevance-filter LLM pass has been removed; runs.model is retained as
# a DB column (see get_cached_run_today / get_covering_run_today match
# queries) so this fixed value keeps those queries working unchanged.
_RUN_MODEL = "none"

# Same base64-encoded {"sub": 0, "role": "admin", "domains": []} shape
# signal-detection-agent sends as NEWS_RETRIEVAL_SERVICE_CALLER when it
# calls news-retrieval (adapters/news_client.py) - used here in the
# reverse direction so a receiving service's require_auth dependency
# accepts _fire_webhook's callback POST. Not a secret (this value is
# already public in signal-detection-agent's own default config); it only
# identifies the caller as an internal service, same trust boundary as
# every other service-to-service call in this mesh.
_WEBHOOK_CALLER_HEADER = os.environ.get(
    "WEBHOOK_CALLER_HEADER",
    "eyJzdWIiOiAwLCAicm9sZSI6ICJhZG1pbiIsICJkb21haW5zIjogW119",
)


class RunCreateResult(TypedDict):
    """Result of create_run_record - new run or cache hit."""

    run_id: int
    cache_hit: bool
    cached_run: Optional[RunRow]


class RunConflictError(Exception):
    """Raised when a run is already in progress for the requested domain."""

    def __init__(self, run_id: int) -> None:
        """Store the conflicting run id."""
        super().__init__(f"Run {run_id} already in progress.")
        self.run_id = run_id


class RunRequest(BaseModel):
    """Parameters for a pipeline run."""

    domain: str = Field(
        description="Domain slug, e.g. 'ai_news' or 'smart_money'."
    )
    days_back: int = Field(
        default=7,
        ge=1,
        description="Exclude articles older than this many days.",
    )
    max_articles: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "Cap on total articles fetched; omit for no limit."
        ),
    )
    focus: Optional[str] = Field(
        default=None,
        description="Optional instruction to narrow topics covered.",
    )
    callback_url: Optional[str] = Field(
        default=None,
        description=(
            "URL to POST a status payload to on run completion"
            " or failure."
        ),
    )
    force: bool = Field(
        default=False,
        description=(
            "Bypass the duplicate-run guard and start a new run"
            " regardless of any in-progress run for the domain."
        ),
    )


def _create_subset_run(
    request: RunRequest,
    resolved_model: str,
    covering: RunRow,
) -> RunCreateResult:
    """Create a completed run whose articles resolve through a covering run.

    Stores no article rows of its own — the global uniqueness constraint on
    ``articles.url`` (see db.py) means a URL already stored under the
    covering run's id cannot be copied into a new row. Instead this run's
    ``source_run_id`` points at the covering run; reads resolve through it,
    filtered to this run's narrower window (see CON-121, CON-147).

    The window's cutoff is computed once, here, and frozen as
    ``window_cutoff`` rather than recomputed against wall-clock time on
    every later read — otherwise articles near the window boundary could
    silently drop out of the resolved set (and article_count would drift
    out of sync with it) the longer this run sits between creation and
    being read.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=request.days_back)
    source = fetch_all_articles_for_run(covering["id"])
    filtered = filter_articles_for_window(
        source, request.days_back, request.max_articles, cutoff=cutoff
    )
    ts = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    subset_id = create_run(
        name=f"{request.domain}_{ts}",
        domain=request.domain,
        days_back=request.days_back,
        max_articles=request.max_articles,
        focus=request.focus,
        model=resolved_model,
        callback_url=request.callback_url,
        source_run_id=covering["id"],
        window_cutoff=cutoff,
    )
    complete_run(subset_id, len(filtered))
    return RunCreateResult(
        run_id=subset_id,
        cache_hit=True,
        cached_run=get_run(subset_id),
    )


def create_run_record(
    request: RunRequest, caller: dict[str, Any]
) -> RunCreateResult:
    """Validate domain ownership and create a run record.

    Returns a RunCreateResult. On a cache hit, cache_hit=True and
    cached_run contains the existing run; no new run is created.

    Raises:
        KeyError: if the domain slug is not found in the database.
        PermissionError: if the caller does not own the domain.
        RunConflictError: if a run is already in progress and force is False.
    """
    with atomic():
        lock_domain_row(request.domain)
        domain = get_domain_by_slug(request.domain)
        if domain is None:
            raise KeyError(
                f"Unknown domain slug: '{request.domain}'."
            )
        if caller["role"] != "admin":
            if domain.get("created_by") is not None:
                if not has_domain_access(
                    caller["id"], domain["id"]
                ):
                    raise PermissionError(
                        "You do not own this domain."
                    )
        if not request.force:
            cached = get_cached_run_today(
                request.domain,
                request.days_back,
                request.focus,
                _RUN_MODEL,
            )
            if cached is not None:
                return RunCreateResult(
                    run_id=cached["id"],
                    cache_hit=True,
                    cached_run=cached,
                )
            covering = get_covering_run_today(
                request.domain,
                request.days_back,
                request.focus,
                _RUN_MODEL,
            )
            if covering is not None:
                return _create_subset_run(request, _RUN_MODEL, covering)
            existing_id = get_running_run_for_domain(request.domain)
            if existing_id is not None:
                raise RunConflictError(existing_id)
        timestamp = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        run_id = create_run(
            name=f"{request.domain}_{timestamp}",
            domain=request.domain,
            days_back=request.days_back,
            max_articles=request.max_articles,
            focus=request.focus,
            model=_RUN_MODEL,
            callback_url=request.callback_url,
        )
        return RunCreateResult(
            run_id=run_id,
            cache_hit=False,
            cached_run=None,
        )


def _fire_webhook(url: str, payload: dict) -> None:
    """POST payload to url as JSON; log but swallow any error.

    Sends the same x-ocn-caller header every other internal service call
    in this codebase sends (see e.g. signal-detection-agent's
    adapters/news_client.py _headers()) - a receiving service's
    require_auth dependency expects this header on every request,
    including one it didn't originate itself. Confirmed live: without
    this header, signal-detection-agent's webhook route correctly
    rejected the call with 401, so the callback was silently dropped
    (this function swallows the error) until the receiving service's own
    fallback schedule eventually picked up the work hours later.
    """
    try:
        httpx.post(
            url, json=payload, timeout=10.0,
            headers={"x-ocn-caller": _WEBHOOK_CALLER_HEADER},
        )
    except Exception as exc:
        logger.warning("Webhook delivery failed for %s: %s", url, exc)


def _store_taiwan_revenue(articles: list[dict[str, Any]]) -> None:
    """Keep company_financials' Taiwan revenue history current from the
    run's own OpenAPI rows. Best-effort: the articles are already stored,
    and the monthly MOPS re-read (backfill-taiwan-revenue) fills any gap
    this leaves, so a failure here must never fail the run."""
    from models.company_financials import upsert_financials
    from taiwan_financials import DOMAIN, revenue_rows_from_articles

    try:
        written = upsert_financials(DOMAIN, revenue_rows_from_articles(articles))
        if written:
            logger.info("[TW_FIN] stored %d monthly revenue row(s) from this run", written)
    except Exception:
        logger.exception("[TW_FIN] failed to store monthly revenue rows from this run")


def run_pipeline(
    run_id: int,
    request: RunRequest,
) -> None:
    """Execute the pipeline in the background and update the run record."""
    max_articles = request.max_articles or 0
    try:
        result = pl.run(
            domain_slug=request.domain,
            days_back=request.days_back,
            max_articles=max_articles,
        )
    except Exception as exc:
        fail_run(run_id, str(exc))
        if request.callback_url:
            _fire_webhook(request.callback_url, {
                "run_id": run_id,
                "status": "failed",
                "domain": request.domain,
                "summary": str(exc),
            })
        return

    articles = result["articles"]
    all_articles = [
        {**art, "run_id": run_id} for art in articles
    ]
    if all_articles:
        create_articles(all_articles)
    if request.domain == "taiwan_market_signal":
        _store_taiwan_revenue(articles)
    complete_run(run_id, len(articles))
    if request.callback_url:
        _fire_webhook(request.callback_url, {
            "run_id": run_id,
            "status": "completed",
            "domain": request.domain,
            "summary": None,
        })
