"""Repository layer - agent_jobs and agent_classifications tables."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from cursor_utils import decode_cursor, encode_cursor
from db import get_db


def create_job(domain: str, news_run_id: int | None = None) -> int:
    """Insert a new agent_jobs row with status=pending; return its id."""
    with get_db() as conn:
        row = conn.execute(
            """
            INSERT INTO agent_jobs (domain, news_run_id, status)
            VALUES (%s, %s, 'pending')
            RETURNING id
            """,
            (domain, news_run_id),
        ).fetchone()
    return int(row["id"])


def update_job_status(
    job_id: int,
    status: str,
    *,
    article_count: int | None = None,
    set_completed_at: bool = False,
) -> None:
    """Update job status and optionally article_count / completed_at."""
    if article_count is not None and set_completed_at:
        with get_db() as conn:
            conn.execute(
                """
                UPDATE agent_jobs
                SET status = %s, article_count = %s,
                    completed_at = NOW()
                WHERE id = %s
                """,
                (status, article_count, job_id),
            )
    elif article_count is not None:
        with get_db() as conn:
            conn.execute(
                "UPDATE agent_jobs SET status = %s, article_count = %s WHERE id = %s",
                (status, article_count, job_id),
            )
    elif set_completed_at:
        with get_db() as conn:
            conn.execute(
                "UPDATE agent_jobs SET status = %s, completed_at = NOW() WHERE id = %s",
                (status, job_id),
            )
    else:
        with get_db() as conn:
            conn.execute(
                "UPDATE agent_jobs SET status = %s WHERE id = %s",
                (status, job_id),
            )


def insert_classification(job_id: int, article: dict[str, Any], result: dict[str, Any]) -> None:
    """Upsert one agent_classifications row."""
    entity_names_normalized = [
        e["name"].lower() for e in (result.get("entities") or []) if e.get("name")
    ]
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, article_id, url, title,
                signal_detection, signal_score, signal_reason,
                materiality, category, entities_json,
                base_signal_detection, base_signal_score,
                novelty, novelty_basis, confidence, confidence_basis,
                refinement_reason, entity_names_normalized,
                pre_verification_score, verification_qa,
                published
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                job_id,
                article.get("id"),
                article.get("url"),
                article.get("title"),
                result["signal_detection"],
                float(result["signal_score"]),
                result.get("signal_reason"),
                result["materiality"],
                # `category` is TEXT[] for every domain. The news
                # classifier picks exactly one, so it is stored as a
                # one-element array rather than given a column of its
                # own; geopolitical_signal stores several in the same
                # place. None stays NULL - an absent category and an
                # empty list are different answers.
                [result["category"]] if result.get("category") else None,
                json.dumps(result.get("entities") or [], ensure_ascii=False),
                result.get("base_signal_detection"),
                result.get("base_signal_score"),
                result.get("novelty"),
                result.get("novelty_basis"),
                result.get("confidence"),
                result.get("confidence_basis"),
                result.get("refinement_reason"),
                entity_names_normalized,
                result.get("pre_verification_score"),
                json.dumps(result.get("verification_qa") or [], ensure_ascii=False),
                article.get("published"),
            ),
        )


def insert_filing_classification(job_id: int, filing: dict[str, Any], result: dict[str, Any]) -> None:
    """Upsert one agent_classifications row for an SEC filing (source_type='sec_filing').

    category is left NULL - not computed at this stage for filings.
    materiality matches the base-pass news schema (high/medium/low/none).

    ticker/form_type/item_codes/filing_filed_at and result["filing_summary"]
    (the stage-1 summarizer's structured output - headline, guidance,
    stated_figures, positives, negatives, outlook, disclosure_flags,
    citations - set by classify_filing_two_stage(), absent when the
    extraction_found=False short-circuit skipped stage 1 entirely) all go in
    metadata, not dedicated typed columns - they're sec_filing-specific the
    same way Taiwan's rank/clause-reason/translated-text fields are
    taiwan_market_signal-specific, and metadata is the existing JSONB bag for
    exactly that (see insert_taiwan_signal_classification). ticker comes
    from news-retrieval's filing metadata (the same value already used to
    build `title` below) - added so callers can filter agent_classifications
    by ticker without parsing it back out of the title string.
    """
    entity_names_normalized = [
        e["name"].lower() for e in (result.get("entities") or []) if e.get("name")
    ]
    metadata: dict[str, Any] = {
        "ticker": result.get("ticker"),
        "form_type": result.get("form_type"),
        "item_codes": result.get("item_codes") or [],
        "filing_filed_at": result.get("filed_at"),
    }
    if result.get("filing_summary"):
        metadata["filing_summary"] = result["filing_summary"]
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, source_type, source_id, url, title,
                signal_detection, signal_score, signal_reason, materiality,
                entities_json, entity_names_normalized, metadata
            ) VALUES (%s, 'sec_filing', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                job_id,
                filing.get("accession_number"),
                result.get("primary_doc_url", ""),
                f"{filing.get('ticker', '')} {filing.get('form_type', '')} filing",
                result["signal_detection"],
                float(result["signal_score"]),
                result.get("signal_reason"),
                result.get("materiality"),
                json.dumps(result.get("entities") or [], ensure_ascii=False),
                entity_names_normalized,
                json.dumps(metadata, ensure_ascii=False),
            ),
        )


# Path 1/2 signals (revenue score, announcement_materiality_signal)
# use HIGH/WEAK/NOISE, distinct from this table's existing
# signal/weak_signal/noise vocabulary used by the LLM-classified sources
# (news, sec_filing) - mapped here so taiwan_market_signal
# rows are queryable via the same signal_detection column as everything
# else (e.g. "give me today's HIGH items" doesn't need a special case).
_TAIWAN_SIGNAL_MAP = {"HIGH": "signal", "WEAK": "weak_signal", "NOISE": "noise"}


def insert_taiwan_signal_classification(
    job_id: int, article: dict[str, Any], result: dict[str, Any],
) -> None:
    """Upsert one agent_classifications row for a taiwan_market_signal item
    (source_type='taiwan_market_signal') - a TWSE/TPEx revenue or material
    announcement row read from news-retrieval.

    Unlike the LLM-classified source types above, revenue ranking and
    clause-code lookup are computed here (not news-retrieval - see
    conversation decision to keep news-retrieval fetch/dedup-only), so
    ``result`` carries a rank- or lookup-derived HIGH/WEAK/NOISE signal
    rather than a model's signal/weak_signal/noise judgment. Mapped onto
    the existing signal_detection column via _TAIWAN_SIGNAL_MAP so these
    rows are queryable the same way as every other source_type.

    Digest-composition fields (rank, translated text, percentages, etc.)
    have no equivalent typed column on this table and go in ``metadata``
    instead - same JSONB-bag rationale as news-retrieval's articles.metadata.
    category/materiality are left at their defaults - this source type
    doesn't use the LLM-classification vocabulary those columns were built
    for. entities_json / entity_names_normalized carry the companies a
    material announcement names (see taiwan_announcement_classifier); empty
    for other Taiwan rows.
    """
    entities = result.get("entities") or []
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, source_type, source_id, url, title,
                signal_detection, signal_score, signal_reason,
                published, metadata, entities_json, entity_names_normalized
            ) VALUES (%s, 'taiwan_market_signal', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                job_id,
                result.get("source_id"),
                article.get("url"),
                article.get("title"),
                _TAIWAN_SIGNAL_MAP[result["signal"]],
                result.get("signal_score", 1.0),
                result.get("reason"),
                article.get("published"),
                json.dumps(result.get("metadata") or {}, ensure_ascii=False),
                json.dumps(entities, ensure_ascii=False),
                [e["name"].lower() for e in entities if e.get("name")],
            ),
        )


def insert_korea_signal_classification(
    job_id: int, article: dict[str, Any], result: dict[str, Any],
) -> None:
    """Upsert one agent_classifications row for a korea_market_signal item
    (source_type='korea_market_signal') - a DART filing or Korea Customs
    export-data row read from news-retrieval.

    Same shape as insert_taiwan_signal_classification (deterministic
    rule/lookup result, not an LLM judgment - signal_score is left NULL
    for these, not defaulted to 1.0, matching how
    classify_taiwan_signal_batch's own rank/clause-lookup paths already
    treat a computed-not-scored result). Not a call to that function with
    a parameter swapped in, because 'taiwan_market_signal' and its
    _TAIWAN_SIGNAL_MAP lookup are hardcoded there, not parameterized.
    """
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, source_type, source_id, url, title,
                signal_detection, signal_score, signal_reason,
                published, metadata
            ) VALUES (%s, 'korea_market_signal', %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                job_id,
                result.get("source_id"),
                article.get("url"),
                article.get("title"),
                result["signal"],
                result.get("signal_score"),
                result.get("reason"),
                article.get("published"),
                json.dumps(result.get("metadata") or {}, ensure_ascii=False),
            ),
        )


def get_existing_korea_signal_source_ids(source_ids: list[str]) -> set[str]:
    """Return the subset of source_ids already classified as
    source_type='korea_market_signal', across ALL prior jobs - same
    dedup role as get_existing_taiwan_source_ids, since a DART rcept_no
    (or other source-specific key) can legitimately reappear across
    multiple news-retrieval polls before this job ever ran.
    """
    if not source_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'korea_market_signal' AND source_id = ANY(%s)
            """,
            (source_ids,),
        ).fetchall()
    return {r["source_id"] for r in rows}


def insert_japan_signal_classification(
    job_id: int, article: dict[str, Any] | None, result: dict[str, Any],
) -> None:
    """Upsert one agent_classifications row for a japan_market_signal item
    (source_type='japan_market_signal') - J1 (jp_forecast/irbank_financials
    revisions) and J2 (results against forecast), same shape as
    insert_korea_signal_classification (deterministic rule result,
    signal_score left NULL, not a scored LLM judgment).

    J3 (missing revision, source_category 'jp_missing_revision') and
    WATCHING (classify_stale_revision_pattern, source_category
    'jp_watching') both describe an ABSENCE, not a real fetched article -
    there is no real url to point to for either, so `article.get("url")`
    is None for both and `url` is stored NULL, same "field absent means
    not applicable" convention this module already uses elsewhere. Both
    still pass a small synthetic dict with a real `title` and, critically,
    a real `published` (the as_of timestamp the absence/staleness was
    detected at) rather than a fully-None article - see classify_missing_
    revision's and classify_stale_revision_pattern's own result-shape
    comments for why: neither absence is timeless (both are only true AS
    OF a specific real moment), and a NULL published would otherwise make
    the row invisible to list_all_results' own published_from/
    published_to day-window filter - a real, previously-documented gap
    for J3 specifically, fixed 2026-09-30 to match WATCHING's own
    already-correct behavior.
    """
    article = article or {}
    # WATCHING alone is refreshed in place. Its source_id is now the
    # company, not the company-and-date, so the row survives the date
    # change instead of the Watching tab emptying until the next
    # monthly refresh (see classify_stale_revision_pattern's own
    # source_id comment). A stable id with DO NOTHING would freeze
    # days_since_last_revision at whatever it was on first insert - a
    # card reading "450 days overdue" forever, wrong and never
    # self-correcting, which is worse than an empty tab because it is
    # not visibly wrong. So this one category updates.
    #
    # Every other Japan row keeps DO NOTHING: those key on a filing's
    # own immutable doc id, so a conflict means the same filing seen
    # twice and there is nothing to update - and run_japan_signal_
    # classification's own already_done check exists to skip exactly
    # that work rather than redo it.
    # Same shape the ai_news and sec_filing inserts above build: the
    # lowercased entity names, so a caller can filter rows by customer
    # without parsing entities_json back out.
    entity_names_normalized = [
        e["name"].lower() for e in (result.get("entities") or []) if e.get("name")
    ]

    is_watching = (result.get("metadata") or {}).get("source_category") == "jp_watching"
    on_conflict = (
        """
        ON CONFLICT (source_type, source_id)
            WHERE source_type = 'japan_market_signal'
        DO UPDATE SET
            job_id           = EXCLUDED.job_id,
            title            = EXCLUDED.title,
            signal_detection = EXCLUDED.signal_detection,
            signal_score     = EXCLUDED.signal_score,
            signal_reason    = EXCLUDED.signal_reason,
            published        = EXCLUDED.published,
            metadata         = EXCLUDED.metadata,
            entities_json    = EXCLUDED.entities_json,
            entity_names_normalized = EXCLUDED.entity_names_normalized
        """
        if is_watching
        else "ON CONFLICT DO NOTHING"
    )
    with get_db() as conn:
        conn.execute(
            f"""
            INSERT INTO agent_classifications (
                job_id, source_type, source_id, url, title,
                signal_detection, signal_score, signal_reason,
                published, metadata, entities_json, entity_names_normalized
            ) VALUES (%s, 'japan_market_signal', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            {on_conflict}
            """,
            (
                job_id,
                result.get("source_id"),
                article.get("url"),
                article.get("title"),
                result["signal"],
                result.get("signal_score"),
                result.get("reason"),
                article.get("published"),
                json.dumps(result.get("metadata") or {}, ensure_ascii=False),
                json.dumps(result.get("entities") or [], ensure_ascii=False),
                entity_names_normalized,
            ),
        )


def get_existing_japan_signal_source_ids(source_ids: list[str]) -> set[str]:
    """Return the subset of source_ids already classified as
    source_type='japan_market_signal', across ALL prior jobs - same
    dedup role as get_existing_korea_signal_source_ids, since a filing's
    own synthetic irbank-financials:// url can legitimately reappear
    across multiple news-retrieval polls before this job ever ran.
    """
    if not source_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'japan_market_signal' AND source_id = ANY(%s)
            """,
            (source_ids,),
        ).fetchall()
    return {r["source_id"] for r in rows}


def insert_china_signal_classification(
    job_id: int, article: dict[str, Any], result: dict[str, Any],
) -> None:
    """Upsert one agent_classifications row for a china_market_signal item.

    Same shape as insert_korea_signal_classification and
    insert_japan_signal_classification - a deterministic rule/lookup
    result, so signal_score is left NULL rather than defaulted, even for
    C1 where a model supplies the binding verdict: the model answers
    BINDING/NON-BINDING/UNCLEAR, not a score, and inventing one would
    misrepresent what it returned.

    `source_id` is the article's own url, which news-retrieval already
    guarantees globally unique (a real article URL for policy and press,
    a synthetic cninfo-filing:// / hkex-filing:// / comtrade-china:// key
    for the rest), so the partial unique index in db.py has a real
    natural key to conflict against.

    Named entities go in `entities_json`, the column that exists for
    them, NOT in the metadata blob. Three separate name fields had
    accumulated there instead - `named_items` (product and technology
    categories a measure names), `named_foreign` (foreign companies or
    countries it names, extracted by the C1 model and then silently
    dropped, never persisted at all) and `named_us` (US tickers the C7
    matcher found). A consumer looking for "which entities does this
    row name" had to know all three keys and that one of them did not
    exist. They are one question and they now have one answer, in the
    shape the ai_news path already established: [{"name", "type"}],
    with `entity_names_normalized` carrying the lowercased names for
    filtering, same as insert_classification does.
    """
    entities = result.get("entities") or []
    entity_names_normalized = [
        e["name"].lower() for e in entities if e.get("name")
    ]
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, source_type, source_id, url, title,
                signal_detection, signal_score, signal_reason,
                published, entities_json, entity_names_normalized, metadata
            ) VALUES (%s, 'china_market_signal', %s, %s, %s, %s, %s, %s, %s,
                      %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                job_id,
                article.get("url"),
                article.get("url"),
                article.get("title"),
                result["signal"],
                result.get("signal_score"),
                result.get("reason"),
                article.get("published"),
                json.dumps(entities, ensure_ascii=False),
                entity_names_normalized,
                json.dumps(result.get("metadata") or {}, ensure_ascii=False),
            ),
        )


def get_existing_china_signal_source_ids(source_ids: list[str]) -> set[str]:
    """Return the subset of source_ids already classified as
    source_type='china_market_signal', across ALL prior jobs - same
    dedup role as get_existing_japan_signal_source_ids. A China article's
    url can legitimately reappear across several news-retrieval polls
    (the policy rules run every 4 hours) before this job ever sees it.
    """
    if not source_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'china_market_signal' AND source_id = ANY(%s)
            """,
            (source_ids,),
        ).fetchall()
    return {r["source_id"] for r in rows}


def insert_middle_east_signal_classification(
    job_id: int, article: dict[str, Any], result: dict[str, Any],
) -> None:
    """Insert one agent_classifications row for a middle_east_market_signal
    article. Same shape as insert_china_signal_classification.

    `source_id` is the article's own url (globally unique in
    news-retrieval), so a re-run or an overlapping scheduled pass hits the
    partial unique index and inserts nothing twice. `signal_score` stays
    NULL: labels here come from the spec's rules applied to extracted
    facts, not from a model's confidence, and inventing a score would
    misstate what decided the row. Entities go in `entities_json` - the
    companies the article is verified to be ABOUT, not every name in it.
    """
    entities = result.get("entities") or []
    entity_names_normalized = [
        e["name"].lower() for e in entities if e.get("name")
    ]
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, source_type, source_id, url, title,
                signal_detection, signal_score, signal_reason,
                published, entities_json, entity_names_normalized, metadata
            ) VALUES (%s, 'middle_east_market_signal', %s, %s, %s, %s, %s, %s,
                      %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                job_id,
                article.get("url"),
                article.get("url"),
                article.get("title"),
                result["signal"],
                result.get("signal_score"),
                result.get("reason"),
                article.get("published"),
                json.dumps(entities, ensure_ascii=False),
                entity_names_normalized,
                json.dumps(result.get("metadata") or {}, ensure_ascii=False),
            ),
        )


def get_middle_east_event_members(event_uris: list[str]) -> dict[str, list[dict[str, Any]]]:
    """{event_uri: [stored rows]} - the rows already stored for this batch's
    newsapi.ai event clusters, in ONE query. Every label counts: a noise
    row about the same event is still another outlet covering it."""
    if not event_uris:
        return {}
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id, url, title, metadata
            FROM agent_classifications
            WHERE source_type = 'middle_east_market_signal'
              AND metadata->'source'->>'event_uri' = ANY(%s)
            """,
            (event_uris,),
        ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        row = dict(row)
        metadata = row["metadata"] if isinstance(row["metadata"], dict) else json.loads(row["metadata"] or "{}")
        row["metadata"] = metadata
        grouped.setdefault(metadata["source"]["event_uri"], []).append(row)
    return grouped


def get_middle_east_story_candidates() -> list[dict[str, Any]]:
    """Every signal and weak_signal row of this domain - the story
    grouping's whole input (grouping is a property of the set, so it is
    regrouped from scratch, like geopolitical Stage E)."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, title, url, published, signal_detection, signal_reason,
                   entities_json, metadata
            FROM agent_classifications
            WHERE source_type = 'middle_east_market_signal'
              AND signal_detection IN ('signal', 'weak_signal')
            ORDER BY published
            """,
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["entities"] = json.loads(d.get("entities_json") or "[]")
        except (ValueError, TypeError):
            d["entities"] = []
        if not isinstance(d.get("metadata"), dict):
            d["metadata"] = json.loads(d.get("metadata") or "{}")
        out.append(d)
    return out


def get_middle_east_fold_candidates() -> list[dict[str, Any]]:
    """The story candidates plus every noise row that names an event and is
    not already folded: a noise report of an alerted event (an interception
    report of a confirmed strike) folds into that alert instead of showing
    as a row of its own. Noise rows only ever join; they never lead."""
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, title, url, published, signal_detection, signal_reason,
                   entities_json, metadata
            FROM agent_classifications
            WHERE source_type = 'middle_east_market_signal'
              AND signal_detection = 'noise'
              AND metadata -> 'event' ->> 'code' IS NOT NULL
              AND NOT jsonb_exists(metadata, 'collapsed_into')
            ORDER BY published
            """,
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["entities"] = json.loads(d.get("entities_json") or "[]")
        except (ValueError, TypeError):
            d["entities"] = []
        if not isinstance(d.get("metadata"), dict):
            d["metadata"] = json.loads(d.get("metadata") or "{}")
        out.append(d)
    return get_middle_east_story_candidates() + out


def clear_middle_east_story_fields() -> int:
    """Remove the story keys from every row of this domain before
    regrouping, so a row that left a story is never hidden by a stale
    in_story pointer."""
    with get_db() as conn:
        cur = conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = metadata - 'story_development' - 'in_story'
            WHERE source_type = 'middle_east_market_signal'
              AND (jsonb_exists(metadata, 'story_development')
                   OR jsonb_exists(metadata, 'in_story'))
            """,
        )
    return cur.rowcount if cur.rowcount is not None else 0


def apply_middle_east_story(seed_row_id: int, *, development: dict[str, Any],
                            member_ids: list[int]) -> None:
    """Write one story in ONE transaction: story_development on the seed,
    in_story = seed id on each member. Nothing is deleted - each member is
    a different event with its own label and reason."""
    with get_db() as conn:
        conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = metadata || jsonb_build_object('story_development', %s::jsonb)
            WHERE id = %s
            """,
            (json.dumps(development, ensure_ascii=False), seed_row_id),
        )
        conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = metadata || jsonb_build_object('in_story', %s::int)
            WHERE id = ANY(%s)
            """,
            (seed_row_id, member_ids),
        )


def collapse_middle_east_same_event(kept_row_id: int, corroboration: dict[str, Any],
                                    duplicate_ids: list[int],
                                    relabel: tuple[str, str, str] | None = None) -> int:
    """Fold same-event duplicates into one row, in ONE transaction: the kept
    row's corroboration gains each duplicate's outlet, headline and link,
    and the duplicates are TOMBSTONED - noise, metadata.collapsed_into =
    kept row id - never deleted (geopolitical Stage B's pattern: the
    already-judged skip reads their source_id, and a wrong fold stays
    reversible). `relabel` = (label, rule, reason) when the reports together
    confirm the event (confirmed_across_reports) - written to the kept row
    in the same transaction. Returns how many rows were tombstoned."""
    if not duplicate_ids:
        return 0
    with get_db() as conn:
        conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = metadata || jsonb_build_object('corroboration', %s::jsonb)
            WHERE id = %s
            """,
            (json.dumps(corroboration, ensure_ascii=False), kept_row_id),
        )
        if relabel:
            label, rule, reason = relabel
            conn.execute(
                """
                UPDATE agent_classifications
                SET signal_detection = %s, signal_reason = %s,
                    metadata = metadata || jsonb_build_object('rule', %s::text)
                WHERE id = %s
                """,
                (label, reason, rule, kept_row_id),
            )
        cur = conn.execute(
            """
            UPDATE agent_classifications
            SET signal_detection = 'noise',
                signal_reason = %s,
                metadata = (metadata - 'story_development' - 'in_story')
                    || jsonb_build_object('collapsed_into', %s::int)
            WHERE id = ANY(%s)
            """,
            ("Another report of the same event, folded into row %d; its outlet, headline"
             " and link are listed there." % kept_row_id, kept_row_id, duplicate_ids),
        )
    return cur.rowcount if cur.rowcount is not None else len(duplicate_ids)


def update_middle_east_label(row_id: int, label: str, rule: str, reason: str) -> None:
    """Raise one row's label after the fact (an attack wave completed by
    later rows) - label, reason and metadata.rule, nothing else."""
    with get_db() as conn:
        conn.execute(
            """
            UPDATE agent_classifications
            SET signal_detection = %s, signal_reason = %s,
                metadata = metadata || jsonb_build_object('rule', %s::text)
            WHERE id = %s AND source_type = 'middle_east_market_signal'
            """,
            (label, reason, rule, row_id),
        )


def get_middle_east_folded_into(kept_ids: list[int]) -> dict[int, list[dict[str, Any]]]:
    """{kept row id: [rows folded into it in earlier runs]} - id and
    metadata only, so an event's confirmation counts every report it ever
    had, not just this run's (confirmed_across_reports)."""
    if not kept_ids:
        return {}
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, metadata, (metadata->>'collapsed_into')::int AS kept_id
            FROM agent_classifications
            WHERE source_type = 'middle_east_market_signal'
              AND (metadata->>'collapsed_into')::int = ANY(%s)
            """,
            (kept_ids,),
        ).fetchall()
    out: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        md = r["metadata"] if isinstance(r["metadata"], dict) else json.loads(r["metadata"] or "{}")
        out.setdefault(r["kept_id"], []).append({"id": r["id"], "metadata": md})
    return out


def get_existing_middle_east_signal_source_ids(source_ids: list[str]) -> set[str]:
    """The subset of source_ids already stored as middle_east_market_signal,
    across ALL prior jobs - what lets the classifier skip an article it has
    already judged before paying for a model call on it."""
    if not source_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'middle_east_market_signal' AND source_id = ANY(%s)
            """,
            (source_ids,),
        ).fetchall()
    return {r["source_id"] for r in rows}


def insert_macro_signal_event(
    job_id: int, event: dict[str, Any], interpretation: dict[str, Any] | None,
) -> None:
    """Upsert one agent_classifications row for a macro_signal event
    (source_type='macro_signal') - EITHER a collapsed event that survived
    to INTERPRET (interpretation is not None, one row per member series'
    release_id+knowledge_time group) OR a suppressed/NOISE candidate kept
    only for audit (interpretation is None) - the spec's "nothing is ever
    deleted, a suppressed row persists with the rule that suppressed it"
    requirement (see macro_signal_suppress.py's suppressed_by field).

    source_id is the deterministic natural key: f"{release_id}-{channel}-
    {knowledge_time isoformat}" for a collapsed/interpreted event, or
    f"{series_id}-{observation_date}" for a single suppressed/NOISE
    observation (no release grouping applies to those - they never
    reached COLLAPSE). channel is part of the key because release_id
    alone is not unique per event - COLLAPSE groups survivors by
    (release_id, channel), and some release_ids are shared across
    channels (e.g. release_id=18 covers both DFF/DFEDTARU's policy_path
    channel and DFII10/T10Y3M's discount_rate channel) - CONFIRMED LIVE
    that omitting channel here silently dropped a real second-channel
    event as a false duplicate of an already-inserted first-channel
    event on the same release+date.

    signal_detection: HIGH -> 'signal', WEAK -> 'weak_signal' (an
    interpreted survivor is always HIGH or WEAK - NOISE never reaches
    COLLAPSE/INTERPRET), or the observation's own tier value lowercased
    for a suppressed/NOISE audit row written directly from TIER/SUPPRESS
    output. signal_score is always NULL - deterministic tier/z-score
    logic feeds this, not a model confidence, same convention as
    korea/taiwan's non-LLM paths.

    event['tier'] (HIGH/WEAK/NOISE) is used here only to COMPUTE
    signal_detection - it is deliberately NOT stored in metadata.
    macro_signal reads the same way every other domain does
    (signal_detection alone; no domain-specific '_tier' field, no
    macro_tier query param - explicit decision to keep the read/filter
    surface consistent across domains rather than macro_signal having
    its own parallel vocabulary for something signal_detection already
    expresses one-to-one for this domain).

    source ('fred' | 'fred_alfred' | 'treasury_fiscal') and
    knowledge_time_confidence ('verified' | 'known_lag') are carried
    through from news-retrieval's own macro_observations row (see
    SuppressibleResult in macro_signal_suppress.py) - real provenance
    about where a number came from and how certain its publication
    timing is, previously computed at fetch time and then silently
    dropped rather than stored. An interpreted event's members can each
    come from a different series/release, so these are stored as
    DEDUPLICATED ARRAYS (metadata.sources / metadata.knowledge_time_
    confidences) - the distinct VALUES an event's members carry, not a
    per-series map, since the value (which source, how confident) is
    the real information here, not which specific series it came from.
    Stays a real array rather than collapsing to one string so a
    genuinely mixed-source event (e.g. a fred series collapsed
    alongside close_today_bal) is never silently flattened to a single,
    wrong-for-some-members value. A suppressed/NOISE audit row is
    always exactly one series, so it gets a plain metadata.source /
    metadata.knowledge_time_confidence value instead.

    metadata.move_bp is a per-series map (series_id -> real change in
    bp, mirroring metadata.z_scores' own shape) - CONFIRMED LIVE (real
    frontend ticket): a FEDTARMD event ties to _fedtarmd_rule's no-
    z-gate exception (z_scores.FEDTARMD is always null - that rule
    gates on SEP median shift, not a rolling z-score), and without this
    field there was no way to see WHAT number made the event HIGH.
    metadata.target_years is a per-series map, populated only for
    series whose move is year-specific (today, only FEDTARMD) - answers
    "which projected year does this event's number refer to", since
    FEDTARMD's own text ("the medium-run fed funds target") never named
    a year and the frontend had no field to resolve it from.

    metadata.classification_basis is a per-series map of TierResult.
    reason verbatim (e.g. "sep_median_shift_50.0bp_ge_25_no_zgate") -
    already computed at TIER time for every series, just never
    persisted before. Answers "what rule/metric made this HIGH" for a
    series whose z_score is null by design (FEDTARMD's no-z-gate
    exception being the one live case today). Kept as-is (a debugging
    tag), per explicit request, alongside metadata.classification_reason
    below - the two are not redundant.

    metadata.classification_reason is a per-series map of TierResult.
    plain_reason - a real plain-English sentence written inline by the
    exact rule branch that fired (see macro_signal_thresholds.py),
    always naming the tier and the real threshold cleared, and the
    z-score when the rule uses one ("no z-score check" when it doesn't,
    e.g. FEDTARMD/DFF's inter-meeting path). This is what the frontend
    shows verbatim in "Why did AlphaStreet flag it?" - never derived by
    parsing classification_basis, so it can't drift from the logic that
    actually ran.

    metadata.series_tiers is a per-series map of each member's OWN tier
    (signal/weak_signal/noise, same vocabulary as the row-level
    signal_detection column below) - added because signal_detection
    itself is fundamentally event-level, not per-series: it's the EVENT's
    rolled-up MAX tier across all members (see the line right below this
    docstring - `"signal" if event.get("tier") == "HIGH" else
    "weak_signal"`, computed once, applied to the whole row). CONFIRMED
    LIVE (real frontend ticket, 2026-09-25) this caused a real, visible
    inconsistency: GET /results/summary counted observations.* using
    signal_detection applied uniformly to every member, so a WEAK member
    of a HIGH-rolled-up event (e.g. the 2026-08-28 discount_rate event -
    DFII5/T10Y2Y are HIGH, but DFII10/T5YIFR are WEAK per their own
    classification_reason) got counted as "high" in the summary while its
    own classification_reason plainly said "cleared the ... WEAK floor" -
    two fields disagreeing about the same fact. series_tiers is written
    from the exact same per-member Tier value classification_reason was
    generated from (m["tier"] in controllers.run's macro_event
    construction), so the two can never drift apart - GET /results/
    summary now counts observations.* from series_tiers, not
    signal_detection.
    """
    if interpretation is not None:
        source_id = f"{event['release_id']}-{event.get('channel')}-{event['knowledge_time']}"
        signal_detection = "signal" if event.get("tier") == "HIGH" else "weak_signal"
        signal_reason = interpretation.get("transmission")
        metadata = {
            "channel": interpretation.get("channel"),
            "entry_point": interpretation.get("entry_point"),
            "assets": interpretation.get("assets"),
            "transmission": interpretation.get("transmission"),
            "suspect": interpretation.get("suspect"),
            "suspect_reason": interpretation.get("suspect_reason"),
            "member_series": event.get("member_series"),
            "release_id": event.get("release_id"),
            "z_scores": event.get("z_scores"),
            "move_bp": event.get("move_bp"),
            "target_years": event.get("target_years"),
            "current_values": event.get("current_values"),
            "prior_values": event.get("prior_values"),
            "classification_basis": event.get("classification_basis"),
            "classification_reason": event.get("classification_reason"),
            "series_tiers": event.get("series_tiers"),
            "sources": event.get("sources"),
            "knowledge_time_confidences": event.get("knowledge_time_confidences"),
            "suppressed_by": None,
        }
    else:
        source_id = f"{event['series_id']}-{event['observation_date']}"
        # CONFIRMED LIVE: str(tier).lower() alone violates
        # agent_classifications_signal_detection_check - the column's
        # allowed values are 'signal'/'weak_signal'/'noise'/'waiting',
        # not the tier vocabulary's own HIGH/WEAK/NOISE lowercased
        # ('high'/'weak' are not allowed values). Map explicitly, same
        # HIGH->'signal', WEAK->'weak_signal' convention the interpreted-
        # event branch above already uses.
        _AUDIT_TIER_TO_SIGNAL_DETECTION = {"HIGH": "signal", "WEAK": "weak_signal", "NOISE": "noise"}
        signal_detection = _AUDIT_TIER_TO_SIGNAL_DETECTION.get(str(event.get("tier", "NOISE")).upper(), "noise")
        suppressed_by = event.get("suppressed_by")
        # signal_reason carries a human-readable explanation directly on
        # the row (not just buried in metadata->>'suppressed_by') - the
        # whole point of persisting a non-surfaced row is auditability
        # ("why wasn't this on the page?" should be a plain read of this
        # column, not a JSON-path query). Distinguishes a genuine NOISE
        # observation (nothing worth flagging happened) from an
        # observation that DID clear a HIGH/WEAK bar but was actively
        # suppressed by a specific rule.
        if suppressed_by is not None:
            signal_reason = f"Suppressed by rule: {suppressed_by}"
        else:
            signal_reason = f"NOISE: did not clear the {event.get('tier', 'NOISE')} threshold"
        metadata = {
            "series_id": event.get("series_id"),
            "observation_date": str(event.get("observation_date")),
            "z_score": event.get("z_score"),
            "suppressed_by": suppressed_by,
            "source": event.get("source"),
            "knowledge_time_confidence": event.get("knowledge_time_confidence"),
        }

    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, source_type, source_id,
                signal_detection, signal_score, signal_reason,
                published, metadata
            ) VALUES (%s, 'macro_signal', %s, %s, NULL, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                job_id,
                source_id,
                signal_detection,
                signal_reason,
                event.get("knowledge_time"),
                json.dumps(metadata, ensure_ascii=False, default=str),
            ),
        )


def list_macro_signal_events_for_confirmation_recheck(
    eligible_series: list[str], since_date: str,
) -> list[dict[str, Any]]:
    """Returns macro_signal rows (both interpreted events and
    suppressed/NOISE audit rows) that are candidates for mechanism C
    (confirmation reversal, spec section 5) - not already suppressed,
    not already NOISE (nothing to reverse), and involving at least one
    series in eligible_series (weekly/monthly only - see
    macro_signal_frequency.CONFIRMATION_REVERSAL_ELIGIBLE).

    An interpreted event's series live in metadata.member_series (a
    JSONB array); a suppressed/audit row's live in metadata.series_id
    (a plain string) - checks both shapes, matching
    list_all_results/list_results' own macro_series filter.

    since_date bounds the query to recent rows only (a daily recheck
    job has no reason to re-examine months-old, long-settled events
    every run) - callers should pass something like 60 days back,
    generous enough to cover even a slow-cadence monthly series'
    confirmation window with margin.
    """
    if not eligible_series:
        return []
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, source_id, signal_detection, metadata, published
            FROM agent_classifications
            WHERE source_type = 'macro_signal'
              AND signal_detection != 'noise'
              AND metadata->>'suppressed_by' IS NULL
              AND published >= %s
              AND (
                  EXISTS (
                      SELECT 1 FROM jsonb_array_elements_text(
                          CASE WHEN jsonb_typeof(metadata->'member_series') = 'array'
                               THEN metadata->'member_series' ELSE '[]'::jsonb END
                      ) t WHERE t = ANY(%s)
                  )
                  OR metadata->>'series_id' = ANY(%s)
              )
            ORDER BY published ASC
            """,
            (since_date, eligible_series, eligible_series),
        ).fetchall()
    return [dict(r) for r in rows]


def get_existing_macro_signal_source_ids(source_ids: list[str]) -> set[str]:
    """Same shape as get_existing_taiwan_source_ids - one batched query
    across all prior jobs, so a re-run of the pipeline never reclassifies
    an event or observation already stored."""
    if not source_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'macro_signal' AND source_id = ANY(%s)
            """,
            (source_ids,),
        ).fetchall()
    return {r["source_id"] for r in rows}


def mark_macro_signal_event_suppressed(source_id: str, rule: str) -> bool:
    """Retroactively marks an already-stored macro_signal event as
    suppressed by mechanism C (confirmation reversal) - updates
    metadata.suppressed_by AND signal_detection together. Returns True
    if a row was updated, False if source_id wasn't found. This is the
    one place macro_signal data is allowed to change after insert.

    signal_detection is forced to 'noise' here, not left at its
    original 'signal'/'weak_signal' value - a confirmation-reversed
    event is the spec's own "was measurement, not information" case
    (section 5's Confirmation override), the same real outcome as a
    NOISE audit row, just discovered later. Leaving signal_detection
    unchanged while only updating metadata.suppressed_by would let a
    signal_detection='signal'/'weak_signal' query still surface an
    event the pipeline has since determined was not real - silently
    stale in the one column every other filter actually reads.
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = jsonb_set(COALESCE(metadata, '{}'::jsonb), '{suppressed_by}', %s::jsonb),
                signal_detection = 'noise'
            WHERE source_type = 'macro_signal' AND source_id = %s
            """,
            (json.dumps(rule), source_id),
        )
        return cur.rowcount > 0


def get_existing_taiwan_source_ids(source_ids: list[str]) -> set[str]:
    """Return the subset of source_ids already classified as
    source_type='taiwan_market_signal', across ALL prior jobs (not scoped
    to today or to one job_id) - the twice-daily run must never reclassify
    a ticker+period/ticker+timestamp combination once it exists, since the
    idx_agent_classifications_taiwan_source_id unique index would reject a
    duplicate insert anyway; checking first avoids wasting an LLM/lookup
    call on something we already know will be a no-op.

    Same shape as get_existing_filing_source_ids - one batched query, not
    one per item.
    """
    if not source_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'taiwan_market_signal' AND source_id = ANY(%s)
            """,
            (source_ids,),
        ).fetchall()
    return {r["source_id"] for r in rows}


def list_taiwan_periods(source_category: str | None = None) -> list[str]:
    """Return every distinct metadata.period_gregorian value stored for
    source_type='taiwan_market_signal', newest first.

    Backs the "which reporting periods have data" question a period=
    filter/dropdown needs to answer without a client-side scan capped by
    some fetch limit - the frontend calls this once to populate the
    default period (periods[0]) and the rest of a picker, instead of
    guessing from a fixed-size unfiltered page. The result set is
    inherently small (one row per calendar month this domain has ever
    classified anything for - low tens at most, growing by one a month),
    so no pagination here.

    source_category optional - omit for periods across both mops_revenue
    (reporting period the filing is about) and mops_material (calendar
    month filed - see classify_material_announcements), or pass one to
    scope to just that category's periods.
    """
    params: list[Any] = ["taiwan_market_signal"]
    where = "source_type = %s AND metadata->>'period_gregorian' IS NOT NULL"
    if source_category:
        where += " AND metadata->>'source_category' = %s"
        params.append(source_category)
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT DISTINCT metadata->>'period_gregorian' AS period
            FROM agent_classifications
            WHERE {where}
            ORDER BY period DESC
            """,
            params,
        ).fetchall()
    return [r["period"] for r in rows]


def get_taiwan_recent_remarks(tickers: list[str], since_period: str) -> list[dict[str, Any]]:
    """Stored revenue remarks for these tickers from `since_period`
    ('YYYY-MM') onward - the history mark_repeated_remarks compares a new
    filing's explanation against. One query for the whole batch."""
    if not tickers:
        return []
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT metadata->>'ticker' AS ticker,
                   metadata->>'period_gregorian' AS period_gregorian,
                   metadata->>'remarks' AS remarks
            FROM agent_classifications
            WHERE source_type = 'taiwan_market_signal'
              AND metadata->>'source_category' = 'mops_revenue'
              AND metadata->>'ticker' = ANY(%s)
              AND metadata->>'period_gregorian' >= %s
              AND metadata->>'remarks' IS NOT NULL
            """,
            (list(set(tickers)), since_period),
        ).fetchall()
    return [dict(r) for r in rows]


def get_taiwan_remark_translations(texts: list[str]) -> dict[str, str]:
    """Chinese revenue remark -> its stored English translation, for the
    texts that already have one. Lets translate_taiwan_articles reuse a
    translation instead of paying a model call for a sentence the same
    company files month after month."""
    if not texts:
        return {}
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT DISTINCT ON (metadata->>'remarks')
                   metadata->>'remarks' AS remarks,
                   metadata->>'translated_remarks' AS translated
            FROM agent_classifications
            WHERE source_type = 'taiwan_market_signal'
              AND metadata->>'source_category' = 'mops_revenue'
              AND metadata->>'remarks' = ANY(%s)
              AND metadata->>'translated_remarks' IS NOT NULL
            ORDER BY metadata->>'remarks', stored_at DESC
            """,
            (list(set(texts)),),
        ).fetchall()
    return {r["remarks"]: r["translated"] for r in rows}


# geopolitical_signal's own vocabulary across both Stage A and Stage B
# (DROPPED/WEAK/WAITING/HIGH) mapped onto the existing signal_detection
# column, same rationale as _TAIWAN_SIGNAL_MAP - DROPPED->noise, WEAK->
# weak_signal (from either stage: Stage A's free rhetoric-keyword
# classification or Stage B's model call, both a real classification, not
# a rejection), WAITING->waiting (a Stage A survivor not yet judged by
# Stage B), HIGH->signal (Stage B's action classification). See db.py's
# agent_classifications_signal_detection_check for the corresponding
# schema change.
# Stage A stores the article body WHOLE, under metadata.body, and no
# storage-side cap exists. Each reading stage applies its own limit at
# prompt-build time instead: Stage B slices _MAX_BODY_CHARS (8,000),
# Stage C slices its own 24,000. One stored copy, two slices.
#
# There was a _STAGE_B_BODY_EXCERPT_CHARS here that truncated on write,
# and it had to be kept in step with Stage B's prompt-side cap by hand -
# raising one without the other changed nothing, which is a silent
# failure. Worse, it capped at the IRREVERSIBLE stage's limit while
# Stage C, licensed to read three times as much, got the short copy and
# went back to news-retrieval for the rest.



# Stage A's rules are deterministic, so their reasons are written here
# rather than by a model: each one states exactly which rule fired.
# Stage B's reasons come from the model itself (see _parse_answer) and
# describe the specific item, because a rule name is all Stage A can
# honestly say about a row it rejected without reading.
_GEOPOLITICAL_STAGE_A_REASON = {
    "non_english": "Dropped - the article is not in English.",
    "too_old": (
        "Dropped - published more than 14 days before this run, so it is "
        "an archive page the source surfaced rather than news."
    ),
    "signal": (
        "Federal Register notice whose title names a trade, sanctions or "
        "supply measure - treated as an action without a model call."
    ),
    "weak_signal": (
        "Headline uses a talk verb (warns, urges, vows, calls for) - "
        "rhetoric rather than an action."
    ),
    "waiting": None,  # Stage B writes the reason once it has judged
}


def insert_geopolitical_signal_classification(
    job_id: int, article: dict[str, Any], result: dict[str, Any],
) -> None:
    """Insert one agent_classifications row for a geopolitical_news article
    going through Stage A's free rule-based filtering
    (source_type='geopolitical_signal').

    ``result['outcome']`` is one of DROPPED/WEAK/WAITING (Stage A's own
    vocabulary - see the spec's Stage A section), mapped onto the existing
    signal_detection column directly - Stage A and Stage B both answer
    with the stored label rather than a synonym needing translation.
    ``result['drop_reason']`` (only set for DROPPED) selects the prose
    written to the signal_reason column; every other Stage A/B/C/D
    field goes in metadata - same JSONB-bag rationale as
    taiwan_market_signal.

    No ON CONFLICT clause: the idx_agent_classifications_geopolitical_signal_
    article_id partial unique index (db.py) enforces one row per article_id
    for this source_type: a caller must check get_existing_geopolitical_
    signal_article_ids first (Stage A rule 1, "already classified: skip")
    rather than rely on a silent conflict here, since Stage B/D need to
    UPDATE the same row in place rather than have an insert silently no-op.
    """
    metadata = {}
    # Every row's reason goes in the signal_reason COLUMN, the field
    # china_market_signal, news and macro_signal all populate on every
    # row ("Routine filing - 'meeting' is a disclosure type no China
    # signal rule reads"). geopolitical_signal was the only domain
    # leaving it null, so a reader of a dropped or weak row had nothing
    # explaining the verdict.
    #
    # This replaces metadata.drop_reason rather than sitting beside it:
    # two fields answering "why is this not a signal", one a code and
    # one a sentence, is a redundancy that drifts the moment a rule
    # changes in one place and not the other.
    signal_reason = _GEOPOLITICAL_STAGE_A_REASON.get(
        result.get("drop_reason") or result.get("outcome")
    )
    # The fetcher's own summary, carried from Stage A so Stage B can read
    # it without a second fetch from news-retrieval. Stage B is otherwise
    # headline-only, and a headline is often the least informative part
    # of the item: three BIS export-denial orders filed the same day by
    # the same agency, with titles differing only in the airline's name,
    # split 2 HIGH / 1 WEAK purely because "Order Renewing Temporary
    # Denial of Export Privileges" states nothing about what was denied.
    #
    # Only the SOURCE summary, never a generated one - at Stage A time no
    # model has read anything yet. Stage C's generated summary fills the
    # same field later for rows that reach it (see summary_source).
    if article.get("summary"):
        metadata["summary"] = article["summary"]
        metadata["summary_source"] = "source"
    # The body news-retrieval already sent. get_run_articles requests
    # include_body=true, so Stage A is handed the full text with the
    # article and was throwing it away - Stage B then re-fetched the
    # same text one row at a time over HTTP. One already-paid response
    # replaces N round trips, and news-retrieval stays the only place
    # that fetches anything.
    #
    # Only a WAITING row keeps it: a DROPPED row is terminal (Stage A
    # rejected its domain) and a WEAK row is too, so storing their text
    # would bloat the table for rows nothing will ever read. Truncated
    # for the same reason - this copy exists to judge HIGH vs WEAK, not
    # to mirror news-retrieval's storage; Stage C still fetches the full
    # 24,000 characters for the rows that reach it.
    # Stored whenever there is a body, NOT only when a summary is
    # absent. The earlier condition skipped the body on any row that had
    # a summary, on the reasoning that Stage B prefers the summary and
    # the body would go unread. That holds for a news article, whose
    # summary is an editorial lead over the same text. It fails for a
    # filed document: Federal Register rows 42115 and 42117 carry a
    # 216-character agency abstract over a 3,673-character
    # determination, a 17x compression, and Stage B - which makes the
    # IRREVERSIBLE call - judged the precis while Stage C, which only
    # tags, fetched the whole document. The less consequential stage had
    # the better evidence.
    if result.get("outcome") == "waiting":
        body = article.get("body")
        if body:
            # The WHOLE body, uncapped. Stage B slices its own 8,000
            # from this at prompt-build time and Stage C reads up to
            # 24,000 of the same string, so one stored copy serves both
            # rather than each stage keeping its own truncation.
            #
            # Stage C used to re-fetch the article from news-retrieval
            # per row instead of reading what was already here. That is
            # 63 HTTP calls a run for text we hold, and it fails open to
            # headline-only whenever the fetch misses - which on a
            # 7-day-retention domain it eventually does. Measured on job
            # 1449: 3 of 63 rows were tagged `coverage: unreadable` and
            # produced ZERO entities, each with a stored excerpt
            # averaging 3,830 characters. The text was never missing;
            # nothing read it. One of the three was an OFAC Iran
            # sanctions notice naming three institutions.
            #
            # Uncapped because capping at Stage B's 8,000 would truncate
            # 14% of this domain's articles (95 of 679 measured) for a
            # stage licensed to read 24,000 - and those long documents
            # are the filed determinations Stage C exists to read.
            # Median is 3,473 characters, so the typical row costs
            # nothing extra; p90 is 9,615 and the longest seen 112,168.
            metadata["body"] = body
    # Which other outlets carried the same story, as news-retrieval's
    # own title-similarity dedup recorded it. Carried for the same
    # reason as the body: it arrives with the article, and Stage D was
    # re-fetching the whole article per row just to read this one list.
    # Only a survivor needs it - a dropped row never reaches Stage D.
    if result.get("outcome") in ("waiting", "signal"):
        source_meta = article.get("metadata") or {}
        also = source_meta.get("also_reported_by")
        if also:
            metadata["also_reported_by"] = also
        # Event Registry (newsapi.ai) returns its own enrichment with
        # each article. Carried for the rows that survive, for the same
        # reason as also_reported_by: it arrives with the article and a
        # later stage would otherwise have to re-fetch to see it.
        #
        #   concepts   scored named entities the API extracted -
        #              "Shell plc" 3, "Equinor" 3, "ExxonMobil" 1. An
        #              INDEPENDENT read of the same question Stage C's
        #              own entity extraction answers, so the two can be
        #              compared rather than one trusted blindly.
        #   event_uri  the API's same-event cluster id: articles sharing
        #              one are the same underlying action.
        #   sentiment  stored as returned, NOT acted on - measured on
        #              real rows it tracks the outlet's prose tone, not
        #              the direction of the measure (two outlets on one
        #              EU sanctions package scored +0.271 and -0.075).
        #   uri        the API's id for THIS article, distinct from the
        #              two cluster ids above. Previously copied as
        #              ("er_uri", "er_uri") - reading a key no source
        #              writes, so it never copied anything.
        # Copied under the SAME names news-retrieval stores them under.
        # concepts/sentiment used to be renamed to er_concepts/er_sentiment
        # here, so one value had two names depending on which table you
        # were reading, and a query written against one service did not
        # work against the other. The prefix also claimed a distinction
        # the row does not make: event_uri is just as much Event
        # Registry's, and it was never prefixed.
        for key in ("concepts", "event_uri", "sentiment", "uri"):
            value = source_meta.get(key)
            if value is not None:
                metadata[key] = value
    with get_db() as conn:
        conn.execute(
            """
            INSERT INTO agent_classifications (
                job_id, source_type, article_id, url, title,
                signal_detection, signal_score, signal_reason, published, metadata
            ) VALUES (%s, 'geopolitical_signal', %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                job_id,
                article.get("id"),
                article.get("url"),
                article.get("title"),
                result["outcome"],
                None,
                signal_reason,
                article.get("published"),
                json.dumps(metadata, ensure_ascii=False),
            ),
        )


def get_existing_geopolitical_signal_article_ids(
    article_ids: list[int],
) -> set[int]:
    """Return the subset of article_ids already classified as
    source_type='geopolitical_signal', across ALL prior jobs.

    Stage A rule 1 ("already classified: skip") - checked before running any
    filter so a re-run (cursor replay, crashed-run resume) never reprocesses
    an article that already has a row, and never hits the unique index as an
    error path. Same batched-query shape as get_existing_taiwan_source_ids.
    """
    if not article_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT article_id FROM agent_classifications
            WHERE source_type = 'geopolitical_signal' AND article_id = ANY(%s)
            """,
            (article_ids,),
        ).fetchall()
    return {r["article_id"] for r in rows}


def get_waiting_geopolitical_signal_articles() -> list[dict[str, Any]]:
    """Return every agent_classifications row where
    source_type='geopolitical_signal' AND signal_detection='waiting' -
    Stage B's entire input (spec's "waiting" survivor state from Stage A).

    Returns id, article_id, title and the source summary Stage A carried
    over (metadata.summary, NULL where the fetcher had none) - not the
    full row. The summary is included because a bureaucratic headline
    alone is often not enough to tell an action from a proposal; see
    insert_geopolitical_signal_classification for the measured case.
    """
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, article_id, title, url,
                   metadata->>'summary' AS summary,
                   -- The full stored body. Stage B applies its own
                   -- _MAX_BODY_CHARS slice when building the prompt;
                   -- Stage C reads the same key up to its own 24,000.
                   -- One stored copy, sliced per stage - there is no
                   -- separate body_excerpt key.
                   metadata->>'body' AS body,
                   -- The supplier's own same-event cluster id, where the
                   -- source provides one. Stage B groups on it so seven
                   -- outlets reporting one UN vote cost one model call
                   -- rather than seven.
                   metadata->>'event_uri' AS event_uri
            FROM agent_classifications
            WHERE source_type = 'geopolitical_signal' AND signal_detection = 'waiting'
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def count_geopolitical_signal_stage_b_calls_today() -> int:
    """Count rows already stamped with metadata.stage_b_classified_at today
    (UTC) - the daily cap's tripwire counter (spec: 1000/day, a safety
    tripwire against a bug flooding the worklist, not a cost control - see
    update_geopolitical_signal_classification's docstring for why this
    timestamp exists at all).
    """
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT count(*) AS n FROM agent_classifications
            WHERE source_type = 'geopolitical_signal'
              AND (metadata->>'stage_b_classified_at')::timestamptz >= date_trunc('day', NOW())
            """,
        ).fetchone()
    return int(row["n"])



def update_geopolitical_signal_classification(
    row_id: int, *, outcome: str, reason: str | None = None,
    headline_only: bool = False,
) -> None:
    """Update one geopolitical_signal row in place after Stage B's HIGH/WEAK
    call - an update-in-place keyed on this table's own id (Stage A/B
    share one row per article_id, never insert a second one).

    outcome is already the stored label - Stage B answers with
    "signal", "weak_signal" or "noise" rather than a synonym this
    function then translates. metadata.stage_b_classified_at is stamped for the daily-cap
    counter above - there is no other reliable per-row "when was Stage B
    actually run" signal, since stored_at is set once at Stage A insert
    time and never reflects Stage B's later update.
    """
    with get_db() as conn:
        conn.execute(
            """
            UPDATE agent_classifications
            SET signal_detection = %s,
                metadata = metadata
                    || jsonb_build_object('stage_b_classified_at', NOW())
                    || CASE WHEN %s
                            THEN jsonb_build_object('judged_on', 'headline_only')
                            ELSE '{}'::jsonb END
                    -- judged_on records a verdict reached with no
                    -- article text at all.
                    --
                    -- It had a second value, 'same_event', written
                    -- alongside a judged_via_event key, for a verdict
                    -- inherited from another article in the same
                    -- event cluster. Both are gone: judged_via_event
                    -- stored a VERBATIM COPY of metadata.event_uri,
                    -- which is already on the row, and 'same_event'
                    -- only restated that that key was present. Nothing
                    -- downstream read either - no API filter, no route,
                    -- no consumer - and an inherited row now carries
                    -- the judged article's own reason text plus its
                    -- title, which is what a reader actually needs.
                    -- Both sources DELIVER a body on every row (measured
                    -- 31/31 and 325/325), so this is no longer about a
                    -- publisher serving nothing - it is about
                    -- news-retrieval REJECTING a body it was given.
                    -- Event Registry hands us its own crawl of an
                    -- arbitrary publisher's page, and one row in 325
                    -- arrived as column-interleaved letters; that body
                    -- is dropped at fetch time (_is_readable_prose in
                    -- news-retrieval's pipeline.py), which lands here as
                    -- a row with a title and no text.
                    --
                    -- Stage B is irreversible, so a verdict reached on a
                    -- headline alone must stay distinguishable from one
                    -- reached on a full document. That is the whole job
                    -- of this flag.
                    --
                    -- NB: never write a literal percent sign in this
                    -- query, comments included. psycopg2 scans the whole
                    -- string for format placeholders and does not skip
                    -- SQL comments, so a stray one is read as a
                    -- placeholder and every execute fails with
                    -- IndexError: tuple index out of range. That is
                    -- exactly what happened on job 1453: all 271 updates
                    -- failed and the rows stayed `waiting`.
                ,
                signal_reason = COALESCE(%s::text, signal_reason)
            WHERE id = %s
            """,
            (
                outcome,
                headline_only,
                reason,
                row_id,
            ),
        )


def collapse_geopolitical_signal_event_group(
    judged_row_id: int, siblings: list[dict[str, Any]],
) -> int:
    """Fold an event group into its judged row: record each other
    member as coverage, then TOMBSTONE those rows. Returns how many
    were tombstoned.

    Stage B groups articles by metadata.event_uri and judges one - the
    fullest text in the group. The rest used to be kept, each carrying a
    copy of the judged row's verdict and reason, which put one verdict
    in several places and showed a reader N rows for one event.

    They are now stored the way news-retrieval's own title-similarity
    duplicates already are: {outlet, title, url} appended to the judged
    row's metadata.also_reported_by, which Stage D merges into
    metadata.corroboration. One row per event, and the other outlets
    remain visible with their own headlines - the headline variance
    across a cluster is itself information, since "iPhones floating in
    Strait of Hormuz" and "US says container ship hit by projectile"
    are one event described two ways.

    The folded row is kept as a tombstone rather than deleted - see the
    note on the UPDATE below for why - so its body and article_id
    survive, and the fold is reversible by hand. What is still lost is
    the row's own place in the feed: the judged row's text is what a
    reader sees, so an event the judged row does not name is not
    surfaced anywhere. The grouping is Event Registry's, not ours,
    and ER's "event" is looser than "story" - a live-updates page
    covering two events carries one event_uri, so an event the judged
    row does not name is lost with it. Measured over 13 real clusters
    before this was built: 11 were cleanly one event, and one was a
    live-updates page whose other subject (a strike on a presidential
    palace) existed in no other row.

    Append and tombstone run in ONE transaction. A half-applied
    collapse would either hide rows that were never recorded as
    coverage, or record coverage for rows still live and counted twice.
    """
    entries = [
        {
            "outlet": s.get("outlet") or "",
            "title": (s.get("title") or "").strip(),
            "url": s.get("url") or "",
        }
        for s in siblings
    ]
    ids = [s["id"] for s in siblings if s.get("id") is not None]
    if not ids:
        return 0
    with get_db() as conn:
        # Appended with || onto whatever is already there, so the
        # duplicates news-retrieval dropped at fetch time are kept
        # alongside these. COALESCE covers a row with no such key yet.
        conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = jsonb_set(
                    metadata, '{also_reported_by}',
                    COALESCE(metadata->'also_reported_by', '[]'::jsonb)
                        || %s::jsonb)
            WHERE id = %s
            """,
            (json.dumps(entries, ensure_ascii=False), judged_row_id),
        )
        # TOMBSTONED, not deleted. The row keeps its article_id, which
        # is the key Stage A's "already classified: skip" check reads
        # (get_existing_geopolitical_signal_article_ids). Deleting it
        # destroyed the only record that this article_id had been seen,
        # so the next Stage A run found no row, called it new, and
        # re-inserted the very duplicate this function had just folded
        # away. Measured on two consecutive days of real runs: the
        # fallback pass resurrected 10 rows on one and 14 on the next,
        # and 24 articles ended up BOTH listed as a sibling on the
        # judged row AND present again as standalone rows - three of
        # them as `signal`, two of those visible in the default feed.
        #
        # The tombstone closes that loop without a new lookup: the
        # article_id check already in Stage A starts working again.
        # `noise` is also the terminal verdict every later stage
        # filters on - Stage C, D and E all select signal_detection =
        # 'signal' - so a tombstoned row reaches no stage and no
        # reader, which is the same outcome deletion intended.
        #
        # What this costs: ~19 rows a day are retained rather than
        # removed. What it buys, beyond the fix, is that the fold is
        # now auditable - collapsed_into names the row that absorbed
        # this one, where before the row simply vanished.
        cur = conn.execute(
            """
            UPDATE agent_classifications
            SET signal_detection = 'noise',
                signal_reason = %s,
                metadata = metadata || jsonb_build_object(
                    'collapsed_into', %s::int,
                    'collapsed_at', to_jsonb(NOW()))
            WHERE id = ANY(%s)
            """,
            (
                "Folded into row %d as same-event coverage; its outlet,"
                " headline and URL are recorded there in"
                " also_reported_by." % judged_row_id,
                judged_row_id,
                ids,
            ),
        )
    return cur.rowcount if cur.rowcount is not None else len(ids)


def get_untagged_geopolitical_signal_high_articles() -> list[dict[str, Any]]:
    """Return every geopolitical_signal row where signal_detection='signal'
    (HIGH, from Stage B) that Stage C hasn't SUCCESSFULLY tagged yet -
    Stage C's entire input.

    Two kinds of row qualify. A row never attempted has no
    stage_c_classified_at. A row whose model call failed carries
    stage_c_failed_at instead (see
    update_geopolitical_signal_stage_c_tags), and is deliberately picked
    up again: the common failure here is a gateway 504, which the HTTP
    layer already retries three times in-call, so a row that still
    arrives here failed for longer than those attempts covered and the
    next scheduled run is the right place to try again.

    This is what keeps an untagged row OUT of Stage D rather than
    letting it through empty - Stage D selects on
    stage_c_classified_at, which a failed row does not have.

    The failed row keeps its `signal` verdict and stays visible to
    readers while untagged; fail-open means never hiding a real signal,
    not pretending it was judged.
    """
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, article_id, title, published,
                   -- Read from the row, not re-fetched per article from
                   -- news-retrieval. Stage A stored the whole body here;
                   -- the fetch cost 63 HTTP calls a run for text already
                   -- in hand and failed open to headline-only whenever
                   -- the article had aged out of a 7-day-retention
                   -- domain. See store_geopolitical_signal_metadata.
                   metadata->>'body' AS body,
                   metadata->>'summary' AS summary
            FROM agent_classifications
            -- Key existence is tested with jsonb_exists(), never with
            -- the bare operator. psycopg2 scans the RAW query string
            -- for placeholders before the server sees it and does not
            -- skip comments, so even naming the broken form in a
            -- comment here would raise IndexError on this call -
            -- confirmed against a real Postgres.
            WHERE source_type = 'geopolitical_signal' AND signal_detection = 'signal'
              AND (NOT jsonb_exists(metadata, 'stage_c_classified_at')
                   OR jsonb_exists(metadata, 'stage_c_failed_at'))
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def count_geopolitical_signal_stage_c_calls_today() -> int:
    """Count rows already stamped with metadata.stage_c_classified_at today
    (UTC) - Stage C's own daily cap tripwire counter (spec: 50/day), same
    shape as count_geopolitical_signal_stage_b_calls_today.
    """
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT count(*) AS n FROM agent_classifications
            WHERE source_type = 'geopolitical_signal'
              AND (metadata->>'stage_c_classified_at')::timestamptz >= date_trunc('day', NOW())
            """,
        ).fetchone()
    return int(row["n"])


def update_geopolitical_signal_stage_c_tags(
    row_id: int, *, tags: dict[str, Any], entities: list[dict[str, Any]] | None = None,
) -> None:
    """Merge Stage C's tags into an existing HIGH row's metadata - update
    in place, same pattern as update_geopolitical_signal_classification.

    ``tags`` is expected to already contain channel, assets,
    impacted_categories, summary (Layer 2 + overrides' combined output,
    all possibly None on fail-open) - merged alongside
    stage_c_classified_at, which both marks this row done (see
    get_untagged_geopolitical_signal_high_articles) and feeds the Stage C
    daily-cap counter above. impacted_categories is the one key NOT
    merged into metadata: it is written to the `category` column instead,
    and removed from the blob so the same list never exists twice.

    signal_detection is never touched here. Stage C tags a row, it does
    not re-judge one: Stage B already answered whether a real action
    occurred, and it answers that without any notion of this universe.
    An earlier version let Stage C demote a signal to weak_signal when
    no AI-economy category was found, which conflated two separate
    questions and hid real events - an OFAC determination expanding
    sanctions to Iran's automotive and rail sectors disappeared from the
    feed because no company in a 19-category AI taxonomy sits in Iranian
    rail. That absence is a fact about our coverage, not about the news,
    and it is now recorded in metadata.coverage instead.

    ``entities`` are the companies and organisations the article itself
    names, written to the real entities_json column rather than into
    metadata - the same place the news and china_market_signal domains
    already put theirs, and the column entity_names_normalized indexes
    for filtering. This replaces metadata.impacted_companies_direct,
    which matched the HEADLINE only and was therefore null on every
    geopolitical row ever stored: these headlines name countries and
    institutions ("UK", "Iran", "EU", "OFAC"), not companies, while the
    companies are named in the body.
    """
    entity_names_normalized = [
        e["name"].lower() for e in (entities or []) if e.get("name")
    ]
    # The sectors this event reaches go to the `category` COLUMN, not into
    # metadata - the same move entities made above, for the same reason.
    # They were stored as metadata.impacted_categories while `category`
    # sat NULL on every row of this domain, which is one value under two
    # names: a query written against `category` silently missed this
    # domain entirely. Popped from `tags` so the metadata key is not
    # written at all, rather than written and then ignored.
    tags = dict(tags)
    categories = tags.pop("impacted_categories", None) or None

    # A row whose model call failed outright comes back with EVERY tag
    # null (pipeline/geopolitical_signal_layer2._EMPTY_RESULT). Stamping
    # stage_c_classified_at on it would be a lie with two consequences,
    # both measured live on one 504 in a 45-row run:
    #
    #   1. Stage D's worklist gate is exactly
    #      `jsonb_exists(metadata, 'stage_c_classified_at')`, so the row
    #      went on to be GRADED with a null channel, null assets and no
    #      category - an empty row reaching a stage that had nothing to
    #      read.
    #   2. Stage C's own worklist is the NEGATION of that key, so the
    #      row became invisible to every later run. It could not heal.
    #      Un-sticking it took manual SQL.
    #
    # So the stamp now records what actually happened. A failed row gets
    # stage_c_failed_at instead: Stage D does not select it (nothing
    # empty flows downstream), and Stage C does (it retries next run).
    # The row keeps its `signal` verdict and stays visible throughout -
    # fail-open is about never hiding a real signal, and that is
    # untouched. `channel` is the probe because Layer 2 cannot return a
    # usable result without one; a row that was genuinely judged always
    # has it.
    failed = tags.get("channel") is None and not (entities or []) and not categories
    stamp = "stage_c_failed_at" if failed else "stage_c_classified_at"

    with get_db() as conn:
        conn.execute(
            f"""
            UPDATE agent_classifications
            SET metadata = (metadata - 'stage_c_failed_at') || (%s::jsonb) ||
                jsonb_build_object('{stamp}', NOW()),
                entities_json = %s,
                entity_names_normalized = %s,
                category = %s
            WHERE id = %s
            """,
            (
                json.dumps(tags, ensure_ascii=False),
                json.dumps(entities or [], ensure_ascii=False),
                entity_names_normalized,
                categories,
                row_id,
            ),
        )


def get_ungraded_geopolitical_signal_tagged_articles() -> list[dict[str, Any]]:
    """Return every geopolitical_signal row Stage C has tagged
    (metadata.stage_c_classified_at set) that Stage D hasn't graded yet
    (metadata has no stage_d_graded_at key) - Stage D's entire input.
    Same "not yet" pattern as get_untagged_geopolitical_signal_high_articles:
    a row is only skipped once it actually has the stamp, not based on
    whether its tags came back null.

    Returns id, article_id, title, url, the entities Stage C extracted
    and the also_reported_by Stage A carried - not the full row.

    Scoped to 'signal' because only Stage B promotes a row to it and
    nothing demotes one afterwards - the filter is belt-and-braces
    against a weak_signal row somehow carrying a Stage C stamp, not a
    live distinction. A row whose coverage is 'outside_coverage' is
    still a signal and is still graded: the grade answers how far to
    trust the REPORT (corroborated, from a primary source, specific),
    which is a separate question from whether we map it to companies.
    """
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, article_id, title, url,
                   -- The supplier's own same-event cluster id. Stage D
                   -- groups on it to find the other outlets that
                   -- reported this event; see build_corroboration.
                   metadata->>'event_uri' AS event_uri,
                   -- news-retrieval's OWN title-similarity dedup: the
                   -- articles it discarded as near-duplicates of this
                   -- one. A second, independent source of the same
                   -- fact, and not a subset - measured over 87 real
                   -- signals, event_uri covered 48 rows and this
                   -- covered 30, overlapping on only 20. Ten rows had
                   -- this and no event_uri, and reported no other
                   -- coverage at all before Stage D read it.
                   metadata->'also_reported_by' AS also_reported_by
            FROM agent_classifications
            WHERE source_type = 'geopolitical_signal'
              AND signal_detection = 'signal'
              AND jsonb_exists(metadata, 'stage_c_classified_at')
              AND NOT jsonb_exists(metadata, 'stage_d_graded_at')
            """,
        ).fetchall()
    return [dict(r) for r in rows]


def get_geopolitical_signal_event_members(
    event_uris: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Return {event_uri: [member rows]} for every given event_uri.

    ONE query for the whole run, not one per row - Stage D grades every
    signal in a batch and the clusters overlap, so a per-row lookup
    would re-read the same event repeatedly. Same reasoning as Stage
    C's company list being loaded once per run.

    Members are EVERY row sharing the event_uri regardless of verdict,
    not just the signals: in a cluster of seven, Stage B judges one
    member and the other six inherit, so six of them are weak_signal
    rows. Those six ARE the corroborating coverage - filtering to
    signals would return an empty list for exactly the clusters that
    have the most coverage.

    `outlet` is derived from the member's own url rather than read
    from metadata: Stage A copies neither Event Registry's source_uri
    nor its display name onto the row (confirmed: 0 of 325 rows carry
    either), and the url is already stored. Deriving it costs nothing
    and avoids adding a field to Stage A whose only consumer is this
    query. Same "www." stripping every other domain derivation in this
    service uses, so "cnbc.com" reads the same way here as in
    also_reported_by.
    """
    if not event_uris:
        return {}
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, title, url,
                   metadata->>'event_uri' AS event_uri,
                   regexp_replace(
                       split_part(split_part(url, '://', 2), '/', 1),
                       '^www[.]', ''
                   ) AS outlet
            FROM agent_classifications
            WHERE source_type = 'geopolitical_signal'
              AND metadata->>'event_uri' = ANY(%s)
            """,
            (event_uris,),
        ).fetchall()
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(row["event_uri"], []).append(dict(row))
    return grouped


def update_geopolitical_signal_stage_d_grade(
    row_id: int, *, grade_fields: dict[str, Any],
) -> None:
    """Merge Stage D's grade into an existing tagged row's metadata -
    update in place, same pattern as update_geopolitical_signal_stage_c_tags.

    ``grade_fields`` is expected to contain grade, corroborated,
    corroborating_domains, primary_source, specific - merged alongside
    stage_d_graded_at, which marks this row done (see
    get_ungraded_geopolitical_signal_tagged_articles).
    """
    with get_db() as conn:
        conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = metadata || (%s::jsonb) ||
                jsonb_build_object('stage_d_graded_at', NOW())
            WHERE id = %s
            """,
            (json.dumps(grade_fields, ensure_ascii=False), row_id),
        )


def get_geopolitical_signal_story_candidates() -> list[dict[str, Any]]:
    """Every Stage-D-graded signal row - Stage E's entire input.

    Unlike the earlier stages this reads the WHOLE set in one query
    rather than only the rows not yet done, because grouping is a
    property of the set: a row cannot be told which story it belongs
    to without seeing the others. Stage E is therefore idempotent by
    rewriting, not by skipping - it clears its own two keys first and
    regroups from scratch, so a changed key or a later arrival never
    leaves a half-grouped set behind.

    Returns the fields the grouping reads: the entities Stage C
    verified against the article text, the published time, and the
    corroboration count that decides which member seeds a story.
    """
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT id, title, url, published, signal_reason,
                   entities_json, metadata
            FROM agent_classifications
            WHERE source_type = 'geopolitical_signal'
              AND signal_detection = 'signal'
              -- jsonb_exists(), never the bare ? operator: psycopg2
              -- scans the whole query for %s placeholders and reads
              -- a literal ? as one, so the bare form fails with
              -- 'type "s" does not exist'.
              AND jsonb_exists(metadata, 'stage_d_graded_at')
            ORDER BY published
            """,
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["entities"] = json.loads(d.get("entities_json") or "[]")
        except (ValueError, TypeError):
            d["entities"] = []
        out.append(d)
    return out


def clear_geopolitical_signal_story_fields() -> int:
    """Remove Stage E's own two keys from every row of this domain.

    Run before regrouping. Without it a row that moved out of a story
    would keep a stale in_story pointer and stay hidden from the
    one-row-per-story filter forever - the failure a reader could not
    see, since the row simply never appears.
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = metadata - 'story_development' - 'in_story'
            WHERE source_type = 'geopolitical_signal'
              AND (jsonb_exists(metadata, 'story_development')
                   OR jsonb_exists(metadata, 'in_story'))
            """,
        )
    return cur.rowcount if cur.rowcount is not None else 0


def apply_geopolitical_signal_story(
    seed_row_id: int, *, development: dict[str, Any],
    member_ids: list[int],
) -> None:
    """Write one story: the development onto its seed, a pointer onto
    each member. ONE transaction - a half-applied story would either
    hide members whose seed records nothing, or record members that are
    still visible and therefore counted twice.

    NOTHING IS DELETED, deliberately. Stage B collapses rows reporting
    the same EVENT and deleting there is safe, because the dropped
    article's headline survives on the row that kept it. A story's
    members are different events - a Treasury designation, a mine
    strike, a blockade - and each carries a verdict and a reason a
    query may still want. metadata.in_story hides them from the
    default view without destroying them:

        signal_detection = 'signal' AND NOT (metadata ? 'in_story')

    That also makes a wrong grouping reversible by clearing one key,
    which matters while the grouping key is still being proven.
    """
    with get_db() as conn:
        conn.execute(
            """
            UPDATE agent_classifications
            SET metadata = metadata || jsonb_build_object(
                    'story_development', %s::jsonb,
                    'stage_e_grouped_at', to_jsonb(NOW()))
            WHERE id = %s
            """,
            (json.dumps(development, ensure_ascii=False), seed_row_id),
        )
        if member_ids:
            conn.execute(
                """
                UPDATE agent_classifications
                SET metadata = metadata || jsonb_build_object(
                        'in_story', %s::int,
                        'stage_e_grouped_at', to_jsonb(NOW()))
                WHERE id = ANY(%s)
                """,
                (seed_row_id, member_ids),
            )


def get_existing_filing_source_ids(source_ids: list[str]) -> set[str]:
    """Return the subset of source_ids (accession_numbers) already classified
    as source_type='sec_filing'. One batched query, not one per filing - used
    to diff a day's fetched filings against what's already in the table before
    calling the classifier, so unchanged filings are never reclassified.
    """
    if not source_ids:
        return set()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'sec_filing' AND source_id = ANY(%s)
            """,
            (source_ids,),
        ).fetchall()
    return {r["source_id"] for r in rows}


def get_filing_source_ids_missing_ticker() -> list[str]:
    """Return source_ids (accession_numbers) for sec_filing rows whose
    metadata has no 'ticker' key yet - the backfill target set for
    backfill_filing_tickers(). One-time migration: rows written before
    insert_filing_classification() started storing ticker in metadata.
    """
    with get_db() as conn:
        # jsonb_exists(), not the `?` operator - get_db()'s execute()
        # wrapper does a blind sql.replace("?", "%s") for non-dict params
        # (to support sqlite-style positional placeholders), which mangles
        # a literal `?` in the query text itself, not just placeholders.
        # Confirmed empirically this session: `metadata ? 'ticker'` was
        # silently rewritten to `metadata %s 'ticker'` and errored with
        # "type "s" does not exist" - jsonb_exists(metadata, 'ticker') is
        # the function-call equivalent and has no `?` character to collide.
        rows = conn.execute("""
            SELECT source_id FROM agent_classifications
            WHERE source_type = 'sec_filing'
              AND (metadata IS NULL OR NOT jsonb_exists(metadata, 'ticker'))
        """).fetchall()
    return [r["source_id"] for r in rows if r["source_id"]]


def backfill_filing_tickers(accession_to_ticker: dict[str, str]) -> int:
    """Merge 'ticker' into metadata for existing sec_filing rows, keyed by
    accession_number (source_id). accession_to_ticker is built by the caller
    from news-retrieval's own GET /market/sec-filings/{ticker} - the same
    source insert_filing_classification() originally used - never guessed
    from the title string, which is display-only and not guaranteed to
    parse back into a valid ticker for every filer. Returns the number of
    rows updated; a source_id with no entry in the map is left untouched
    (news-retrieval's DynamoDB has a 180-day TTL, so a very old filing's
    ticker may no longer be resolvable this way).
    """
    if not accession_to_ticker:
        return 0
    with get_db() as conn:
        updated = 0
        for source_id, ticker in accession_to_ticker.items():
            result = conn.execute(
                """
                UPDATE agent_classifications
                SET metadata = COALESCE(metadata, '{}'::jsonb) || jsonb_build_object('ticker', %s)
                WHERE source_type = 'sec_filing' AND source_id = %s
                """,
                (ticker, source_id),
            )
            updated += result.rowcount
    return updated


def expire_classifications_for_source_type(source_type: str, days: int) -> int:
    """Delete agent_classifications rows for one source_type published more
    than `days` days ago. Rows with a NULL `published` are never deleted
    (fail-open) - there is no reliable age to judge them by, matching
    news-retrieval's own expire_articles_for_domain. Returns the number of
    rows deleted.
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            DELETE FROM agent_classifications
            WHERE source_type = %s
              AND published IS NOT NULL
              AND published < NOW() - (%s || ' days')::INTERVAL
            """,
            (source_type, days),
        )
        return cur.rowcount


def get_recent_entity_classifications(
    entity_names: list[str],
    *,
    days: int = 90,
    limit: int = 15,
) -> list[dict[str, Any]]:
    """Return recent rows that mention any of the given entity names.

    Includes signal/weak_signal rows (for novelty and downgrade decisions) and
    borderline noise rows with signal_score >= 0.28 (as escalation context).
    Stage 2 uses the signal_detection field to distinguish the two.
    """
    if not entity_names:
        return []
    normalized_names = [name.lower() for name in entity_names]
    placeholders = ", ".join(["%s"] * len(normalized_names))
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT signal_detection, signal_score, signal_reason, category,
                   entities_json, title, url, stored_at
            FROM agent_classifications
            WHERE stored_at >= NOW() - INTERVAL '{days} days'
              AND (
                signal_detection IN ('signal', 'weak_signal')
                OR (signal_detection = 'noise' AND signal_score >= 0.28)
              )
              AND entity_names_normalized && ARRAY[{placeholders}]::TEXT[]
            ORDER BY stored_at DESC
            LIMIT %s
            """,
            [*normalized_names, limit],
        ).fetchall()
    results = []
    for r in rows:
        rec = dict(r)
        rec["entities"] = json.loads(rec.pop("entities_json", "[]") or "[]")
        if rec.get("signal_score") is not None:
            rec["signal_score"] = float(rec["signal_score"])
        if rec.get("stored_at") is not None:
            rec["stored_at"] = rec["stored_at"].isoformat()
        results.append(rec)
    return results


def get_completed_job_for_run(news_run_id: int) -> dict[str, Any] | None:
    """Return the most recent completed job for a given news_run_id, or None."""
    with get_db() as conn:
        row = conn.execute(
            """
            SELECT * FROM agent_jobs
            WHERE news_run_id = %s AND status = 'completed'
            ORDER BY id DESC LIMIT 1
            """,
            (news_run_id,),
        ).fetchone()
    return dict(row) if row else None


def get_job(job_id: int) -> dict[str, Any] | None:
    """Return a single agent_jobs row, or None if not found."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT * FROM agent_jobs WHERE id = %s", (job_id,)
        ).fetchone()
    return dict(row) if row else None


def list_jobs(
    limit: int = 20,
    cursor: str | None = None,
    status: str | None = "completed",
    domain: str | None = None,
) -> dict[str, Any]:
    """Return cursor-paginated agent_jobs, newest-first. Defaults to completed only.

    domain filters to agent_jobs.domain - callers that poll for "the latest
    job" across multiple domains (news, sec_filing, ...) must pass this,
    otherwise "most recent completed job" can return a job from a different
    domain than the one they actually wanted (e.g. a same-day SEC filing job
    completing after the news job would otherwise silently win).
    """
    params: list[Any] = []
    conditions: list[str] = []
    if status:
        conditions.append("status = %s")
        params.append(status)
    if domain:
        conditions.append("domain = %s")
        params.append(domain)
    if cursor:
        after_id = decode_cursor(cursor)
        conditions.append("id < %s")
        params.append(after_id)
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit + 1)
    with get_db() as conn:
        rows = conn.execute(
            f"SELECT * FROM agent_jobs {where} ORDER BY id DESC LIMIT %s",
            params,
        ).fetchall()
    jobs = [dict(r) for r in rows]
    next_cursor = None
    if len(jobs) > limit:
        jobs = jobs[:limit]
        next_cursor = encode_cursor(jobs[-1]["id"])
    return {"jobs": jobs, "next_cursor": next_cursor}


# Effective "when did this actually happen" date, used for display/sort
# order instead of id (insertion order). Confirmed empirically this
# session: after a large batch reprocessing run, id order badly
# mismatched real filing dates (e.g. a 2018-07-31 filing sorted ahead of
# several 2017 filings, since batch insertion order has no relationship
# to the filing's own date). Falls back per source_type: sec_filing rows
# carry their real date in metadata.filing_filed_at (not all rows have a
# usable `published`); news/other rows use `published` when set - a real
# gap, confirmed live, only ~61% of `news` rows have it - falling back
# again to stored_at (never null) so every row always sorts by SOME real
# timestamp, never silently by id.
# The "events" view of /results and /results/summary: one row per event or
# story. A report folded into another row as the same event
# (metadata.collapsed_into - geopolitical Stage B, middle_east_market_signal)
# and an event listed inside a story's lead row (metadata.in_story) are left
# out; both travel inside the row a reader sees (corroboration.siblings,
# story_development.members). COALESCE because rows of some domains have
# NULL metadata, and jsonb_exists(NULL, ...) is NULL - which would drop them.
# Keys absent in every other domain, so for those the view changes nothing.
RESULT_VIEWS = ("events", "rows")
_EVENTS_VIEW_CONDITION = (
    "NOT jsonb_exists(COALESCE(metadata, '{}'::jsonb), 'in_story')"
    " AND NOT jsonb_exists(COALESCE(metadata, '{}'::jsonb), 'collapsed_into')"
)


_EFFECTIVE_DATE_EXPR = (
    "COALESCE((metadata->>'filing_filed_at')::timestamptz, published, stored_at)"
)


def list_all_results(
    limit: int = 100,
    cursor: str | None = None,
    signal_detection: str | None = None,
    source_type: str | None = None,
    ticker: str | None = None,
    code: str | None = None,
    period: str | None = None,
    source_category: str | None = None,
    impacted_category: str | None = None,
    impacted_ticker: str | None = None,
    channel: str | None = None,
    corroborated: bool | None = None,
    event_location: str | None = None,
    coverage: str | None = None,
    published_from: str | None = None,
    published_to: str | None = None,
    macro_series: str | None = None,
    macro_suspect: bool | None = None,
    macro_interpreted_only: bool | None = None,
    view: str = "events",
) -> dict[str, Any]:
    """Return cursor-paginated agent_classifications across all jobs, ordered
    by effective date (see _EFFECTIVE_DATE_EXPR), newest first - NOT by id,
    which only reflects insertion order (see that constant's comment for why
    this matters). Cursor is a composite {date, id} keyset, not a bare id -
    id alone is needed as a tiebreaker for same-timestamp rows (e.g. many
    filings sharing a stored_at fallback from the same batch job), so a
    single date value cannot uniquely resume a paginated position.

    ticker matches metadata->>'ticker' (JSONB) - the only place ticker is
    stored today (see insert_filing_classification). Only sec_filing rows
    populate this field currently, so combining ticker with another
    source_type filter returns no rows, same as querying a mismatched
    source_type/signal_detection pair would. Matched case-insensitively
    (UPPER() both sides) since callers may pass a lowercase ticker; this
    also matches the expression index on UPPER(metadata->>'ticker') in
    db.py, so the comparison stays index-friendly rather than falling back
    to a sequential scan.

    code matches metadata->>'code' (JSONB) - the japan_market_signal
    equivalent of ticker above, NOT the same field: every J1-J7/WATCHING
    Japan classifier stores a company's TSE 4-digit code (or Kioxia's own
    "285A" - always TEXT, never cast to int) under metadata.code, never
    metadata.ticker (see japan_companies.py's own JAPAN_TICKER_
    UNIVERSE). Without this, a caller wanting one company's own Japan
    Signals history (e.g. Advantest, 6857) had no way to filter
    server-side and had to page through every japan_market_signal row
    client-side instead. Same case-insensitive UPPER()-both-sides match
    and partial expression index as ticker (idx_agent_classifications_
    japan_code in db.py), for the same index-friendliness reason.

    period matches metadata->>'period_gregorian' (e.g. "2026-07") - only
    taiwan_market_signal mops_revenue rows populate this field, same
    only-some-rows-have-it shape as ticker above. Exact string match, not
    case-insensitive (period_gregorian is always digits/hyphen, no case to
    normalize). Without this, every period ever classified for a ticker
    comes back mixed in one list - callers that want "just this month's
    revenue" (e.g. the frontend showing July only, not July+August once
    both exist) need to filter client-side otherwise.

    source_category matches metadata->>'source_category' (e.g.
    'mops_revenue', 'mops_material', 'gdelt') - only taiwan_market_signal
    rows populate this field; distinguishes revenue rows from material
    announcements within that one source_type, since both share
    source_type='taiwan_market_signal' with no other way to tell them apart
    server-side. Needed alongside period/limit for callers that want ONLY
    revenue rows (e.g. to reliably compute the latest period from a small
    page, without a page of material-announcement rows - which have no
    period_gregorian at all - crowding out the revenue rows a small limit
    would otherwise return).


    impacted_category matches against the `category` column, a TEXT[] -
    only geopolitical_signal rows Stage C has tagged populate it in this
    domain. One category per call, as a single-element array overlap
    (category && ARRAY[%s]), which matches how a category picker/tab UI
    would naturally query, one category at a time.

    It reads a COLUMN rather than metadata->'impacted_categories' because
    the list moved there - see update_geopolitical_signal_tags. The
    jsonb_exists() function form this used to need is gone with it; the
    bare `?` operator was never usable here, being ambiguous with
    psycopg2's own %s paramstyle parser.

    impacted_ticker matches a ticker against entities_json - the
    companies the article itself names, each stored with its ticker (where
    one exists) and the reason the text gives for it. It previously read
    metadata.impacted_companies_direct and
    metadata.impacted_companies_by_category; both are gone. The first was
    null on every geopolitical row ever stored, and the second held every
    ticker in a matched SECTOR, so this filter returned rows that named no
    company at all while missing rows that named one.
    Deliberately a separate param from `ticker` above, not a
    reuse of it - `ticker` is an exact string match against a flat
    metadata->>'ticker' field (sec_filing only) and has different match
    semantics (case-insensitive equality vs. array-of-objects membership);
    giving geopolitical's array-shaped ticker data its own param name
    avoids one param silently behaving differently depending on
    source_type. Matched case-insensitively via UPPER() on each element's
    ->>'ticker' text extraction (jsonb_array_elements, not the _text
    variant - these are objects, not scalar strings).

    channel matches metadata->>'channel' (energy/trade/sanctions/shipping/
    conflict) - only geopolitical_signal rows Stage C has tagged populate
    this field.

    corroborated reads Stage D's metadata.corroboration.count - how many
    OTHER outlets reported the same event, found by grouping on
    event_uri. true means at least one, false means the story stands
    alone (measured on one real pool: 30 of 81 signals had a sibling, 51
    stood alone). It replaced a metadata.corroborated boolean that
    matched also_reported_by against Stage A's domain allowlist; that
    allowlist was removed from Stage A, the field went to 0 rows, and
    the filter silently returned nothing rather than erroring.

    event_location matches metadata->>'event_location' as a substring,
    case-insensitively, so "Hormuz" finds "Strait of Hormuz" and "Ohio"
    finds "Pike County, Ohio". Null on a legal instrument - a sanctions
    designation happens in no place - so this filter necessarily
    excludes most sanctions rows.

    coverage matches metadata->>'coverage' ('outside_coverage' or
    'unreadable'); the key is ABSENT on a row whose impacted_categories
    are populated, so there is no 'mapped' value to filter on - absence
    is that state.

    published_from/published_to filter by the article's own `published`
    timestamp (date-only, inclusive on both ends) - distinct from the
    cursor's keyset position, which orders by when a row was effectively
    dated for pagination purposes, not by publish date range. Needed
    because classification runs can process a backlog of old articles in
    one pass (see geopolitical_signal's Stage A pooling by run date, not
    publish date) - a digest wanting only "the last 7 days of real news"
    needs to filter published itself, not rely on when rows were
    classified.

    macro_signal has no domain-specific tier/grade filter - it reads the
    same way every other domain does, through signal_detection
    ('signal'/'weak_signal'/'noise') alone. tier (HIGH/WEAK/NOISE) is
    macro_signal's own internal threshold vocabulary (see
    pipeline/macro_signal_thresholds.py) but maps one-to-one onto
    signal_detection for this domain (HIGH->signal, WEAK->weak_signal,
    NOISE->noise - see insert_macro_signal_event) and is deliberately
    NOT stored in metadata or exposed as its own query param, to keep
    the read/filter surface consistent with news/taiwan_market_signal/
    geopolitical_signal, none of which have their own '_tier' field
    either.

    macro_series matches macro_signal's metadata.member_series (a JSONB
    array, populated on interpreted events) OR metadata.series_id (a
    plain string, populated on suppressed/audit rows) - the two
    row-shapes macro_signal actually produces store "which series" two
    different ways (see insert_macro_signal_event's two branches), so
    this filter checks both rather than requiring callers to know which
    shape a given row is before they can find it by series.

    macro_suspect matches metadata->>'suspect' (a JSON boolean stored as
    text, same boolean-as-text shape as `corroborated` above) - only
    macro_signal's interpreted-event rows populate this field (the
    model's own "does this coincide with something that mechanically
    explains it" flag from Appendix A's one override).

    macro_interpreted_only=True restricts to macro_signal rows that
    actually reached and passed INTERPRET (a real LLM call, real
    channel/entry_point/assets/transmission/suspect fields) - excluding
    suppressed/NOISE audit rows kept only for the spec's "nothing is
    ever deleted" requirement (see insert_macro_signal_event). Without
    this, signal_detection='signal' alone returns BOTH genuinely-
    interpreted events AND observations that cleared HIGH but were
    suppressed before COLLAPSE/INTERPRET ever ran (confirmed live: DGS2
    cleared HIGH on 2026-08-28 but was suppressed via the
    {DGS3MO,DGS1,DGS2} collinear cluster - the audit-row branch of
    insert_macro_signal_event mapped its original HIGH tier to
    signal_detection='signal' too, but metadata.transmission was never
    set, no LLM call was made for it). Implemented as
    metadata->>'suppressed_by' IS NULL AND
    source_type='macro_signal' AND signal_detection != 'noise' - an
    interpreted survivor's suppressed_by is always explicitly null (see
    insert_macro_signal_event's interpreted-event branch), while every
    audit row's is either a rule name or absent-but-still-a-NOISE-row.
    macro_interpreted_only=False (not None) is accepted symmetrically,
    for a caller that explicitly wants ONLY the audit/suppressed rows.
    """
    params: list[Any] = []
    conditions = []
    if view == "events":
        conditions.append(_EVENTS_VIEW_CONDITION)
    if signal_detection:
        conditions.append("signal_detection = %s")
        params.append(signal_detection)
    if source_type:
        conditions.append("source_type = %s")
        params.append(source_type)
    if ticker:
        conditions.append("UPPER(metadata->>'ticker') = UPPER(%s)")
        params.append(ticker)
    if code:
        conditions.append("UPPER(metadata->>'code') = UPPER(%s)")
        params.append(code)
    if period:
        conditions.append("metadata->>'period_gregorian' = %s")
        params.append(period)
    if source_category:
        conditions.append("metadata->>'source_category' = %s")
        params.append(source_category)
    if impacted_category:
        conditions.append("category && ARRAY[%s]::TEXT[]")
        params.append(impacted_category)
    if impacted_ticker:
        # Matches entities_json - the companies the article itself
        # names. It previously read metadata.impacted_companies_direct
        # (null on every geopolitical row ever stored) and
        # metadata.impacted_companies_by_category (every ticker in a
        # matched sector), so it found tickers no reader could explain
        # and MISSED the ones carrying a reason. Both fields are gone.
        conditions.append(
            """EXISTS (
                SELECT 1 FROM jsonb_array_elements(
                    CASE WHEN jsonb_typeof(entities_json::jsonb) = 'array'
                         THEN entities_json::jsonb ELSE '[]'::jsonb END
                ) t WHERE UPPER(t->>'ticker') = UPPER(%s)
            )"""
        )
        params.append(impacted_ticker)
    if channel:
        conditions.append("metadata->>'channel' = %s")
        params.append(channel)
    if corroborated is not None:
        # Reads Stage D's corroboration.count - how many OTHER outlets
        # reported the same event, found by grouping on event_uri.
        #
        # This used to test a metadata.corroborated boolean, which Stage
        # D no longer writes: that boolean matched also_reported_by
        # entries against Stage A's domain allowlist, and the allowlist
        # was removed from Stage A entirely. The field went to 0 rows,
        # so the filter accepted a value and silently returned nothing -
        # worse than an error, because it reads as "no matching rows".
        conditions.append(
            "COALESCE((metadata->'corroboration'->>'count')::int, 0)"
            + (" > 0" if corroborated else " = 0")
        )
    if event_location:
        conditions.append("metadata->>'event_location' ILIKE %s")
        params.append(f"%{event_location}%")
    if coverage:
        conditions.append("metadata->>'coverage' = %s")
        params.append(coverage)
    if macro_series:
        conditions.append(
            """(
                jsonb_exists(metadata->'member_series', %s)
                OR metadata->>'series_id' = %s
            )"""
        )
        params.append(macro_series)
        params.append(macro_series)
    if macro_suspect is not None:
        conditions.append("metadata->>'suspect' = %s")
        params.append("true" if macro_suspect else "false")
    if macro_interpreted_only is not None:
        if macro_interpreted_only:
            conditions.append("(metadata->>'suppressed_by' IS NULL AND signal_detection != 'noise')")
        else:
            conditions.append("(metadata->>'suppressed_by' IS NOT NULL OR signal_detection = 'noise')")
    if published_from:
        conditions.append("published >= %s")
        params.append(published_from)
    if published_to:
        conditions.append("published < (%s::date + interval '1 day')")
        params.append(published_to)
    if cursor:
        after = decode_cursor(cursor)
        # Composite keyset predicate: rows strictly after (date, id) in
        # DESC order - same date, lower id; or an earlier date outright.
        conditions.append(f"({_EFFECTIVE_DATE_EXPR}, id) < (%s, %s)")
        params.append(after["date"])
        params.append(after["id"])
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    params.append(limit + 1)
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT *, {_EFFECTIVE_DATE_EXPR} AS effective_date
            FROM agent_classifications
            {where}
            ORDER BY effective_date DESC, id DESC LIMIT %s
            """,
            params,
        ).fetchall()
    results = []
    for r in rows:
        rec = dict(r)
        rec["entities"] = json.loads(rec.pop("entities_json", "[]") or "[]")
        rec.pop("effective_date", None)
        results.append(rec)
    next_cursor = None
    if len(results) > limit:
        results = results[:limit]
        last_row = rows[limit - 1]
        next_cursor = encode_cursor({
            "date": last_row["effective_date"].isoformat(),
            "id": last_row["id"],
        })
    return {"results": results, "next_cursor": next_cursor}


def get_results_summary(
    source_type: str, published_from: str | None = None, published_to: str | None = None,
    view: str = "events",
) -> dict[str, Any]:
    """Real ask (frontend ticket, 2026-09-25): count HIGH/WEAK/NOISE/
    suppressed observations and events for a date window, without the
    frontend downloading every audit row just to count them.

    Must match list_all_results EXACTLY for the same filters, per the
    ticket's own requirement - so this reuses published (not
    _EFFECTIVE_DATE_EXPR) as the window field, same as
    published_from/published_to on /results, and the same published >=
    X / published < (Y + 1 day) inclusive-date-range predicate.

    "observations": one row PER SERIES per date, counted by that
    SERIES' OWN tier - CONFIRMED LIVE (real frontend ticket, 2026-09-25,
    a real reported inconsistency) this must read metadata.series_tiers
    (a per-member map, e.g. {"DFII10": "weak_signal", "T10Y2Y":
    "signal"}), NOT the row's own signal_detection column applied
    uniformly to every member - signal_detection is the EVENT's
    rolled-up MAX tier across all members (e.g. one member HIGH makes
    the whole row "signal"), so counting a WEAK member as "high" because
    it happens to share a row with a HIGH member is exactly the bug
    that was reported: classification_reason (also per-member, written
    from the same real Tier value as series_tiers) said "WEAK floor"
    for a series the summary was counting as "high".
    jsonb_each_text(series_tiers) unnests the interpreted-event shape
    into one row per series-tier pair; an audit row (always exactly one
    series) has no series_tiers, so it falls back to its own row-level
    signal_detection - correct there, since an audit row's
    signal_detection already IS that one series' own tier (see
    insert_macro_signal_event's audit branch - it's set directly from
    that series' Tier, never rolled up across members). An interpreted
    event inserted before this fix (no series_tiers yet) also falls
    back the same way, applying its rolled-up tier to every member -
    the same imprecise behavior as before, but only until it's
    re-interpreted; never silently drops the row from the count.

    "suppressed": rows with metadata.suppressed_by set - these ALSO
    count toward their own tier bucket above (an audit row can be both
    HIGH-tier AND suppressed, e.g. a collinear duplicate) - the ticket's
    own wording ("they also count toward their own tier") is explicit
    that this is not a mutually-exclusive bucket.

    "events": macro_interpreted_only=true rows (real surfaced events,
    excluding suppressed/audit rows) - a plain row count, NOT expanded
    by member_series, since one collapsed event is one event regardless
    of how many series fed into it.

    "series_reporting": distinct series_ids with ANY observation in the
    window - unions member_series elements (interpreted rows) with
    series_id (audit rows).

    "series_total": macro_signal_fetch.MACRO_SERIES_UNIVERSE's own count
    (news-retrieval), not queried here - the caller (routes/jobs.py)
    fills this in, since this module has no news-retrieval access.

    "last_run_at": latest completed agent_jobs.completed_at for the
    RETRIEVAL DOMAIN behind this source_type (see the mapping beside
    that query) - NOT scoped to the date window (the window is about
    which OBSERVATIONS to count, not which job ran them; a run can
    classify events spanning several days in one job).
    """
    date_conditions = ["source_type = %s"]
    date_params: list[Any] = [source_type]
    # Same view as /results, so the counts match the rows a reader sees.
    if view == "events":
        date_conditions.append(_EVENTS_VIEW_CONDITION)
    if published_from:
        date_conditions.append("published >= %s")
        date_params.append(published_from)
    if published_to:
        date_conditions.append("published < (%s::date + interval '1 day')")
        date_params.append(published_to)
    date_where = " AND ".join(date_conditions)

    with get_db() as conn:
        obs_rows = conn.execute(
            f"""
            SELECT
                COALESCE(per_series.tier, agent_classifications.signal_detection) AS effective_tier,
                metadata->>'suppressed_by' IS NOT NULL AS is_suppressed
            FROM agent_classifications
            LEFT JOIN LATERAL jsonb_each_text(metadata->'series_tiers') AS per_series(series_id, tier)
                ON jsonb_typeof(metadata->'series_tiers') = 'object'
            WHERE {date_where}
            """,
            date_params,
        ).fetchall()

        events_rows = conn.execute(
            f"""
            SELECT signal_detection, COUNT(*) AS c
            FROM agent_classifications
            WHERE {date_where}
              AND metadata->>'suppressed_by' IS NULL AND signal_detection != 'noise'
            GROUP BY signal_detection
            """,
            date_params,
        ).fetchall()

        series_row = conn.execute(
            f"""
            SELECT COUNT(DISTINCT series_id) AS c FROM (
                SELECT jsonb_array_elements_text(metadata->'member_series') AS series_id
                FROM agent_classifications
                WHERE {date_where} AND jsonb_typeof(metadata->'member_series') = 'array'
                UNION
                SELECT metadata->>'series_id' AS series_id
                FROM agent_classifications
                WHERE {date_where} AND metadata->>'series_id' IS NOT NULL
            ) all_series
            """,
            date_params + date_params,
        ).fetchone()

        # agent_jobs.domain is the RETRIEVAL domain; source_type is what
        # the resulting rows are stored as. They are the same string for
        # most domains, and deliberately different for two: the
        # ai_news domain stores source_type='news', and the
        # geopolitical_news domain stores
        # source_type='geopolitical_signal'. Only those two need an
        # entry here; anything absent falls through to source_type
        # itself, which is correct for the rest and stays correct for a
        # domain added later under the matching-name convention.
        #
        # This was hardcoded to 'macro_signal', so every caller got
        # macro_signal's last run whatever they asked for - confirmed
        # live: geopolitical_signal, macro_signal and sec_filing all
        # returned 2026-10-20T23:00:53, while geopolitical's own latest
        # job had completed 13 hours later. The counts above were always
        # filtered correctly; this one field was not.
        job_domain = {
            "news": "ai_news",
            "geopolitical_signal": "geopolitical_news",
        }.get(source_type, source_type)
        last_run_row = conn.execute(
            """
            SELECT completed_at FROM agent_jobs
            WHERE domain = %s AND status = 'completed'
            ORDER BY completed_at DESC LIMIT 1
            """,
            (job_domain,),
        ).fetchone()

    observations = {"total": 0, "high": 0, "weak": 0, "noise": 0, "suppressed": 0}
    _tier_key = {"signal": "high", "weak_signal": "weak", "noise": "noise"}
    for row in obs_rows:
        key = _tier_key.get(row["effective_tier"])
        if key is None:
            continue
        observations[key] += 1
        observations["total"] += 1
        if row["is_suppressed"]:
            observations["suppressed"] += 1

    events = {"high": 0, "weak": 0}
    for row in events_rows:
        key = _tier_key.get(row["signal_detection"])
        if key in ("high", "weak"):
            events[key] = row["c"]

    return {
        "window": {"from": published_from, "to": published_to},
        "observations": observations,
        "events": events,
        "series_reporting": series_row["c"] if series_row else 0,
        "last_run_at": last_run_row["completed_at"].isoformat() if last_run_row and last_run_row["completed_at"] else None,
    }


def list_results(
    job_id: int,
    limit: int = 100,
    cursor: str | None = None,
    signal_detection: str | None = None,
    source_type: str | None = None,
    ticker: str | None = None,
    period: str | None = None,
    source_category: str | None = None,
    impacted_category: str | None = None,
    impacted_ticker: str | None = None,
    channel: str | None = None,
    corroborated: bool | None = None,
    event_location: str | None = None,
    coverage: str | None = None,
    published_from: str | None = None,
    published_to: str | None = None,
    macro_series: str | None = None,
    macro_suspect: bool | None = None,
    macro_interpreted_only: bool | None = None,
) -> dict[str, Any]:
    """Return cursor-paginated agent_classifications for a job.

    ticker matches metadata->>'ticker' - see list_all_results for details.
    period matches metadata->>'period_gregorian' - see list_all_results for
    details; only taiwan_market_signal mops_revenue rows populate it.
    source_category matches metadata->>'source_category' - see
    list_all_results for details.
    corroborated, event_location and coverage behave exactly as in
    list_all_results - see that function for what each reads and why.
    impacted_category matches the `category` column (TEXT[]) - see
    list_all_results for details.
    impacted_ticker, channel, corroborated, published_from/published_to -
    see list_all_results for details.
    """
    params: list[Any] = [job_id]
    extra_conditions = ""
    if signal_detection:
        extra_conditions += " AND signal_detection = %s"
        params.append(signal_detection)
    if source_type:
        extra_conditions += " AND source_type = %s"
        params.append(source_type)
    if ticker:
        extra_conditions += " AND UPPER(metadata->>'ticker') = UPPER(%s)"
        params.append(ticker)
    if period:
        extra_conditions += " AND metadata->>'period_gregorian' = %s"
        params.append(period)
    if source_category:
        extra_conditions += " AND metadata->>'source_category' = %s"
        params.append(source_category)
    if impacted_category:
        extra_conditions += " AND category && ARRAY[%s]::TEXT[]"
        params.append(impacted_category)
    if impacted_ticker:
        # Same entities_json match as the sibling query above. This site
        # additionally used jsonb_array_elements_TEXT on arrays of
        # objects, so UPPER(t) compared a ticker against a whole JSON
        # object and could never match anything.
        extra_conditions += """ AND EXISTS (
            SELECT 1 FROM jsonb_array_elements(
                CASE WHEN jsonb_typeof(entities_json::jsonb) = 'array'
                     THEN entities_json::jsonb ELSE '[]'::jsonb END
            ) t WHERE UPPER(t->>'ticker') = UPPER(%s)
        )"""
        params.append(impacted_ticker)
    if channel:
        extra_conditions += " AND metadata->>'channel' = %s"
        params.append(channel)
    if corroborated is not None:
        # Same Stage D corroboration.count read as list_all_results -
        # see that function for why the old boolean is gone.
        extra_conditions += (
            " AND COALESCE((metadata->'corroboration'->>'count')::int, 0)"
            + (" > 0" if corroborated else " = 0")
        )
    if event_location:
        extra_conditions += " AND metadata->>'event_location' ILIKE %s"
        params.append(f"%{event_location}%")
    if coverage:
        extra_conditions += " AND metadata->>'coverage' = %s"
        params.append(coverage)
    if macro_series:
        extra_conditions += """ AND (
            jsonb_exists(metadata->'member_series', %s)
            OR metadata->>'series_id' = %s
        )"""
        params.append(macro_series)
        params.append(macro_series)
    if macro_suspect is not None:
        extra_conditions += " AND metadata->>'suspect' = %s"
        params.append("true" if macro_suspect else "false")
    if macro_interpreted_only is not None:
        if macro_interpreted_only:
            extra_conditions += " AND (metadata->>'suppressed_by' IS NULL AND signal_detection != 'noise')"
        else:
            extra_conditions += " AND (metadata->>'suppressed_by' IS NOT NULL OR signal_detection = 'noise')"
    if published_from:
        extra_conditions += " AND published >= %s"
        params.append(published_from)
    if published_to:
        extra_conditions += " AND published < (%s::date + interval '1 day')"
        params.append(published_to)
    if cursor:
        after_id = decode_cursor(cursor)
        extra_conditions += " AND id > %s"
        params.append(after_id)
    params.append(limit + 1)
    with get_db() as conn:
        rows = conn.execute(
            f"""
            SELECT * FROM agent_classifications
            WHERE job_id = %s {extra_conditions}
            ORDER BY id ASC LIMIT %s
            """,
            params,
        ).fetchall()
    results = []
    for r in rows:
        rec = dict(r)
        rec["entities"] = json.loads(rec.pop("entities_json", "[]") or "[]")
        results.append(rec)
    next_cursor = None
    if len(results) > limit:
        results = results[:limit]
        next_cursor = encode_cursor(results[-1]["id"])
    return {"results": results, "next_cursor": next_cursor}
