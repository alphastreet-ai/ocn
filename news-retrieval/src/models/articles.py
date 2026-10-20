"""DB query functions for article records."""
import json
from datetime import date, datetime, timedelta, timezone
from email.utils import parsedate_to_datetime as _parse_rfc2822
from typing import Optional

from cursor_utils import decode_cursor, encode_cursor
from db import get_db


def _encode_article_cursor(article_id: int) -> str:
    """Encode an article keyset position as an opaque cursor string."""
    return encode_cursor({"id": article_id})


def _decode_article_cursor(cursor: str) -> int:
    """Decode an article cursor; raises ValueError if malformed."""
    return decode_cursor(cursor)["id"]


def _parse_published_date(published) -> Optional[datetime]:
    """Try RFC 2822 then ISO 8601; return None if unparseable.

    Accepts either a string (as fetched from pipeline sources) or a
    ``datetime`` (as read back from the ``TIMESTAMPTZ`` articles.published
    column via psycopg2). Always returns a UTC-aware datetime.
    """
    if not published:
        return None
    if isinstance(published, datetime):
        return (
            published if published.tzinfo is not None
            else published.replace(tzinfo=timezone.utc)
        )
    try:
        return _parse_rfc2822(published)
    except Exception:
        pass
    try:
        dt = datetime.fromisoformat(published)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def filter_articles_for_window(
    articles: list[dict],
    days_back: int,
    max_articles: Optional[int],
    cutoff: Optional[datetime] = None,
) -> list[dict]:
    """Return articles within days_back of now, capped by max_articles.

    Articles with unparseable or missing published dates are included
    (fail-open).

    Args:
        cutoff: Explicit cutoff to filter against, e.g. a subset run's
            frozen ``window_cutoff``. Defaults to ``now() - days_back`` when
            omitted — callers that need a stable result across repeated
            reads (not recomputed against wall-clock time on every call)
            must pass an explicit, previously-frozen cutoff.
    """
    if cutoff is None:
        cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    filtered = [
        a for a in articles
        if (pub := _parse_published_date(a.get("published", "")))
        is None or pub >= cutoff
    ]
    if max_articles:
        filtered = filtered[:max_articles]
    return filtered


def expire_articles_for_domain(domain: str, days: int) -> int:
    """Delete articles for a domain published more than ``days`` days ago.

    Articles with a NULL ``published`` date are never deleted (fail-open) -
    there is no reliable age to judge them by, and dropping them silently
    would lose real data. Returns the number of rows deleted.
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            DELETE FROM articles a
            USING runs r
            WHERE a.run_id = r.id
              AND r.domain = :domain
              AND a.published IS NOT NULL
              AND a.published < NOW() - (:days || ' days')::INTERVAL
            """,
            {"domain": domain, "days": days},
        )
        return cur.rowcount


def get_recent_gdelt_articles_for_ticker(
    ticker: str, hours: int = 24,
) -> list[dict]:
    """Return recently-stored GDELT articles for one ticker, for title-
    similarity dedup.

    Scoped by ``metadata->>'ticker'`` and ``created_at`` (not ``published``)
    - ``created_at`` is when we first saw the fact, which is what a rolling
    dedup window needs; GDELT's own ``seendate``/``published`` can lag or
    lead when we actually ingested it. Bounded to one ticker's recent
    articles only (a handful of rows), not the full corpus, since the
    similarity check only ever needs to compare against same-company,
    same-window candidates.

    Returns each row's ``id``, ``title``, ``url``, and ``metadata`` (which
    carries the stored ``title_embedding``, if present - rows stored before
    this field existed, or where the embedding call failed, have it absent
    and are skipped by the caller's similarity check, not treated as
    errors).
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT a.id, a.title, a.url, a.metadata
            FROM articles a
            JOIN runs r ON r.id = a.run_id
            WHERE r.domain = 'taiwan_market_signal'
              AND a.metadata->>'ticker' = :ticker
              AND a.metadata->>'source_type' = 'gdelt'
              AND a.created_at > NOW() - (:hours || ' hours')::INTERVAL
            ORDER BY a.created_at DESC
            """,
            {"ticker": ticker, "hours": hours},
        )
        return [dict(row) for row in cur.fetchall()]


def get_recent_articles_for_domain(
    domain: str, hours: int = 24,
) -> list[dict]:
    """Return recently-stored articles for one domain, for title-similarity
    dedup (same purpose as ``get_recent_gdelt_articles_for_ticker``, scoped
    by domain instead of ticker since ai_news/smart_money have no ticker
    concept).

    Scoped by ``created_at`` (not ``published``) - ``created_at`` is when we
    first saw the fact, which is what a rolling dedup window needs; a
    source's own ``published`` timestamp can lag or lead when we actually
    ingested it. Bounded to one domain's recent articles only, not the full
    corpus, since the similarity check only ever needs to compare against
    same-domain, same-window candidates.

    Returns each row's ``id``, ``title``, ``url``, and ``metadata`` (which
    carries the stored ``title_embedding``, if present - rows stored before
    this field existed, or where the embedding call failed, have it absent
    and are skipped by the caller's similarity check, not treated as
    errors).

    Loads the entire window in one round-trip - fine for callers that need
    every candidate in memory at once. ``iter_recent_articles_for_domain``
    below covers the case (title-similarity dedup) where the caller only
    ever needs one candidate at a time and holding the full window in
    memory is the actual memory cost, not a per-row field.
    """
    with get_db() as conn:
        cur = conn.execute(
            """
            SELECT a.id, a.title, a.url, a.metadata
            FROM articles a
            JOIN runs r ON r.id = a.run_id
            WHERE r.domain = :domain
              AND a.created_at > NOW() - (:hours || ' hours')::INTERVAL
            ORDER BY a.created_at DESC
            """,
            {"domain": domain, "hours": hours},
        )
        return [dict(row) for row in cur.fetchall()]


_RECENT_ARTICLES_CHUNK_SIZE = 250


def iter_recent_articles_for_domain(
    domain: str, hours: int = 24, chunk_size: int = _RECENT_ARTICLES_CHUNK_SIZE,
):
    """Yield recently-stored articles for one domain in bounded-size pages,
    instead of loading the entire window into memory at once like
    ``get_recent_articles_for_domain`` does.

    For title-similarity dedup, the caller (``_dedup_by_title_similarity_
    for_domain`` in pipeline.py) only ever needs to compare the new batch
    against one candidate at a time - it never needs the whole window held
    in memory simultaneously. Confirmed live 2026-09-24: geopolitical_news's
    168h window held 2127+ rows at once (~30KB each, dominated by the
    stored title_embedding, not by other metadata fields - narrowing the
    SELECT to fewer columns was tried and measured to save only ~124
    bytes/row out of ~31KB, i.e. not the real cost) during the exact phase
    a production run died with no traceback. Paginating the fetch caps
    memory to one page's worth of rows regardless of how large the domain's
    total window grows, with no loss of dedup coverage (every candidate in
    the window is still compared, just not all at once) - unlike shortening
    the window, which would permanently stop catching duplicates published
    further back.

    Keyset-paginated on ``a.id`` (not OFFSET) so later pages don't degrade
    in query cost as the scan progresses, and so a page boundary can't
    duplicate or skip a row the way OFFSET can under concurrent writes -
    not a concern here since these are already-stored historical rows, but
    keyset pagination costs nothing extra and is the safer default.
    """
    last_id: int | None = None
    while True:
        with get_db() as conn:
            if last_id is None:
                cur = conn.execute(
                    """
                    SELECT a.id, a.title, a.url, a.metadata
                    FROM articles a
                    JOIN runs r ON r.id = a.run_id
                    WHERE r.domain = :domain
                      AND a.created_at > NOW() - (:hours || ' hours')::INTERVAL
                    ORDER BY a.id DESC
                    LIMIT :chunk_size
                    """,
                    {"domain": domain, "hours": hours, "chunk_size": chunk_size},
                )
            else:
                cur = conn.execute(
                    """
                    SELECT a.id, a.title, a.url, a.metadata
                    FROM articles a
                    JOIN runs r ON r.id = a.run_id
                    WHERE r.domain = :domain
                      AND a.created_at > NOW() - (:hours || ' hours')::INTERVAL
                      AND a.id < :last_id
                    ORDER BY a.id DESC
                    LIMIT :chunk_size
                    """,
                    {
                        "domain": domain, "hours": hours,
                        "last_id": last_id, "chunk_size": chunk_size,
                    },
                )
            rows = [dict(row) for row in cur.fetchall()]
        if not rows:
            return
        yield rows
        if len(rows) < chunk_size:
            return
        last_id = rows[-1]["id"]


def append_also_reported_by(article_id: int, entry: dict) -> None:
    """Record that another outlet also covered the story already stored
    as ``article_id``, without inserting a duplicate row.

    ``entry`` is {outlet, title, url} for the article being discarded as
    a near-duplicate. It used to be a bare domain string, which told a
    reader somebody else had the story but never what they called it -
    and the headline is the part worth having, since outlets covering
    one event describe it differently.

    Used when a new article's title matches an already-stored article
    from an earlier run (cross-run duplicate); the discarded article
    carries the "same story, different outlet" signal that would be lost
    by dropping it. Same-batch duplicates don't need this - the
    surviving article hasn't been inserted yet, so its list is merged
    in-memory before the first insert.

    Two guards, both enforced in SQL so a concurrent writer cannot slip
    past them:

      - NOT a duplicate url. Idempotent: re-recording the same discarded
        article is a no-op.
      - NOT the stored row's OWN outlet. Two Federal Register notices
        filed the same day matched on title and each recorded the other,
        leaving rows reading ["federalregister.gov"] - the publisher
        corroborating itself. A publisher is not independent
        confirmation of its own document.
    """
    with get_db() as conn:
        conn.execute(
            """
            UPDATE articles
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{also_reported_by}',
                COALESCE(metadata->'also_reported_by', '[]'::jsonb)
                    || jsonb_build_array(:entry::jsonb)
            )
            WHERE id = :article_id
              AND NOT EXISTS (
                  SELECT 1 FROM jsonb_array_elements(
                      COALESCE(metadata->'also_reported_by', '[]'::jsonb)
                  ) e
                  WHERE e->>'url' = :url OR e = to_jsonb(:outlet::text)
              )
              -- metadata.article_url first: a row stored under its own
              -- url scheme (er-me://{uri}) keeps the publisher's link
              -- there, and its `url` host would never equal an outlet.
              AND COALESCE(
                  regexp_replace(
                      split_part(split_part(
                          COALESCE(metadata->>'article_url', url), '://', 2), '/', 1),
                      '^www[.]', ''
                  ), ''
              ) <> :outlet
            """,
            {
                "article_id": article_id,
                "entry": json.dumps(entry, ensure_ascii=False),
                "url": entry.get("url") or "",
                "outlet": entry.get("outlet") or "",
            },
        )


def get_already_stored_urls(urls: list[str]) -> set[str]:
    """Return the subset of ``urls`` already stored, across all domains.

    Global (not domain-scoped) — a URL stored under any domain's run is
    considered seen everywhere, matching the DB's global uniqueness
    constraint on ``articles.url``. Bounded to the given candidate URLs
    rather than pulling a domain's entire article history, so cost scales
    with the size of one run's fetch, not with cumulative history.

    Used to exclude previously-seen articles from a new run before they
    reach the relevance filter, a body-fetch step, or re-insertion — the
    DB's unique index on ``url`` is the final backstop if this check is
    ever skipped or races with a concurrent run.
    """
    if not urls:
        return set()
    with get_db() as conn:
        cur = conn.execute(
            "SELECT url FROM articles WHERE url = ANY(:urls)",
            {"urls": urls},
        )
        return {row["url"] for row in cur.fetchall()}


def create_articles(articles: list[dict]) -> None:
    """Batch-insert article records.

    Args:
        articles: List of article dicts with keys:
            ``run_id``, ``url``, ``title``,
            ``summary``, ``source``, ``published``.
    """
    if not articles:
        return
    # `_pub_date` is the fetchers' internal field name; pipeline.run()
    # renames it to `published` at the end of _fetch_articles. A caller
    # that invokes a fetcher directly - a backfill script, a test - skips
    # that rename and every row lands with a NULL published, which no
    # days_back window can ever match again. That happened twice during
    # the China build and cost 444 rows before it was noticed, so the
    # rename is enforced here rather than left to the caller.
    for a in articles:
        if a.get("published") is None and a.get("_pub_date") is not None:
            a["published"] = a["_pub_date"]
        # Postgres cannot store a NUL byte in a text column, and the
        # driver raises on the whole batch rather than the one row, so
        # a single malformed PDF loses an entire run's articles. The
        # fetchers strip these at extraction; this is the backstop for
        # any that still arrive.
        for field in ("title", "summary", "body"):
            value = a.get(field)
            if isinstance(value, str) and "\x00" in value:
                a[field] = value.replace("\x00", "")
    with get_db() as conn:
        conn.execute_values(
            "INSERT INTO articles"
            " (run_id, url, title, summary, source, published, body, metadata)"
            " VALUES %s"
            " ON CONFLICT DO NOTHING",
            [
                (
                    a["run_id"],
                    a.get("url"),
                    a.get("title"),
                    a.get("summary"),
                    a.get("source"),
                    a.get("published"),
                    a.get("body"),
                    json.dumps(a["metadata"]) if a.get("metadata") else None,
                )
                for a in articles
            ],
        )


def list_articles_for_run(
    run_id: int,
    limit: int = 20,
    cursor: Optional[str] = None,
    include_body: bool = True,
) -> tuple[list[dict], Optional[str]]:
    """Return paginated articles for a run, ordered by id asc.

    Returns a (rows, next_cursor) tuple. next_cursor is None when
    there are no further pages. Body fields are omitted unless
    include_body is True.
    """
    params: dict = {"run_id": run_id, "limit": limit + 1}
    after_clause = ""

    if cursor is not None:
        after_id = _decode_article_cursor(cursor)
        after_clause = "AND id > :after_id"
        params["after_id"] = after_id

    with get_db() as conn:
        cur = conn.execute(
            f"""
            SELECT * FROM articles
            WHERE run_id = :run_id {after_clause}
            ORDER BY id ASC
            LIMIT :limit
            """,
            params,
        )
        rows = [dict(row) for row in cur.fetchall()]

    if not include_body:
        for row in rows:
            row.pop("body", None)

    next_cursor: Optional[str] = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_article_cursor(rows[-1]["id"])

    return rows, next_cursor


def list_articles_for_run_resolved(
    run: dict,
    limit: int = 20,
    cursor: Optional[str] = None,
    include_body: bool = True,
) -> tuple[list[dict], Optional[str]]:
    """Return paginated articles for a run, resolving through source_run_id.

    A subset run (CON-121) stores no article rows of its own — its
    ``source_run_id`` points at the covering run whose rows it draws from.
    This filters the covering run's articles to the subset's own
    ``days_back``/``max_articles`` window before paginating, so callers see
    the same result they would have if the rows had actually been copied.
    Runs without a ``source_run_id`` fall through to the normal per-run read.

    Filters against the subset run's frozen ``window_cutoff`` (set once at
    creation), not wall-clock time at read time — otherwise the resolved
    article set would keep shrinking on every later read as articles near
    the window boundary age out, drifting from the ``article_count`` that
    was recorded once at creation.

    Args:
        run: Full run row (as returned by ``get_run``), not just an id —
            needs ``id``, ``source_run_id``, ``days_back``, ``max_articles``,
            ``window_cutoff``.
    """
    if not run.get("source_run_id"):
        return list_articles_for_run(
            run["id"], limit=limit, cursor=cursor, include_body=include_body
        )

    source = fetch_all_articles_for_run(run["source_run_id"])
    filtered = filter_articles_for_window(
        source, run["days_back"], run.get("max_articles"),
        cutoff=run.get("window_cutoff"),
    )
    filtered.sort(key=lambda a: a["id"])

    if cursor is not None:
        after_id = _decode_article_cursor(cursor)
        filtered = [a for a in filtered if a["id"] > after_id]

    rows = filtered[:limit + 1]
    if not include_body:
        for row in rows:
            row.pop("body", None)

    next_cursor: Optional[str] = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_article_cursor(rows[-1]["id"])

    return rows, next_cursor


def list_articles(
    domains: Optional[list[str]] = None,
    from_date: Optional[date] = None,
    to_date: Optional[date] = None,
    limit: int = 20,
    cursor: Optional[str] = None,
    include_body: bool = True,
) -> tuple[list[dict], Optional[str]]:
    """Return paginated articles across all runs, newest id first.

    Optionally filtered to one or more domain slugs and/or a date range
    on the article published date. Body fields are omitted unless
    include_body is True.
    """
    params: dict = {"limit": limit + 1}
    clauses: list[str] = []

    if domains:
        clauses.append("r.domain = ANY(:domains)")
        params["domains"] = domains

    if from_date is not None:
        clauses.append("a.published >= :from_date")
        params["from_date"] = from_date

    if to_date is not None:
        # Same bug/fix as models/runs.py's list_runs: a.published is a
        # TIMESTAMPTZ, comparing directly against a bare date casts to
        # midnight and silently excludes every article published later
        # that same day. Exclusive upper bound at the next day instead.
        clauses.append("a.published < :to_date + INTERVAL '1 day'")
        params["to_date"] = to_date

    if cursor is not None:
        after_id = _decode_article_cursor(cursor)
        clauses.append("a.id < :after_id")
        params["after_id"] = after_id

    where = ("AND " + " AND ".join(clauses)) if clauses else ""

    with get_db() as conn:
        cur = conn.execute(
            f"""
            SELECT a.*, r.domain FROM articles a
            JOIN runs r ON r.id = a.run_id
            WHERE TRUE {where}
            ORDER BY a.id DESC
            LIMIT :limit
            """,
            params,
        )
        rows = [dict(row) for row in cur.fetchall()]

    if not include_body:
        for row in rows:
            row.pop("body", None)

    next_cursor: Optional[str] = None
    if len(rows) > limit:
        rows = rows[:limit]
        next_cursor = _encode_article_cursor(rows[-1]["id"])

    return rows, next_cursor  # type: ignore[return-value]


def fetch_all_articles_for_run(run_id: int) -> list[dict]:
    """Return all article rows for a run as dicts (no pagination)."""
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM articles"
            " WHERE run_id = :run_id ORDER BY id ASC",
            {"run_id": run_id},
        )
        return [dict(row) for row in cur.fetchall()]


def get_article(article_id: int) -> Optional[dict]:
    """Return a single article by id, or None if not found."""
    with get_db() as conn:
        cur = conn.execute(
            "SELECT * FROM articles WHERE id = ?",
            (article_id,),
        )
        row = cur.fetchone()
        return dict(row) if row else None
