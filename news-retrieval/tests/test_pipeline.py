"""Tests for pipeline.py fetch and dedup behaviour."""
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import pipeline as pipeline_module
from db import get_db

_GDELT_FIXTURE_SEENDATE = (
    datetime.now(timezone.utc) - timedelta(hours=1)
).strftime("%Y%m%dT%H%M%SZ")

GDELT_RESPONSE_FIXTURE = {
    "articles": [
        {
            "url": "https://example.com/conflict-story",
            "title": "Conflict escalates in region",
            "seendate": _GDELT_FIXTURE_SEENDATE,
            "socialimage": "",
            "domain": "example.com",
            "language": "English",
            "sourcecountry": "Ukraine",
        },
    ],
}


def _make_entry(*, content_words: int = 0) -> types.SimpleNamespace:
    """Return a fake feedparser entry for pipeline tests.

    Args:
        content_words: Number of words in the ``content:encoded`` value.
            0 means content:encoded is absent.
    """
    body_text = " ".join(["word"] * content_words)
    content_list = (
        [{"value": body_text, "type": "text/html"}]
        if content_words > 0
        else []
    )
    entry = types.SimpleNamespace(published_parsed=None)
    data = {
        "title": "Test Article",
        "link": "http://example.com/test",
        "published": "2026-01-01",
        "summary": "Short summary.",
        "content": content_list,
    }
    entry.get = lambda k, d="": data.get(k, d)  # type: ignore[assignment]
    return entry


def _fake_pipeline_run(
    source: dict,
    *,
    content_words: int = 0,
    trafilatura_patches: dict | None = None,
) -> dict:
    """Run the pipeline with a controlled source and feed entry.

    Args:
        source: Source dict with ``url`` and ``no_fetch`` keys.
        content_words: Words in the ``content:encoded`` value; 0 = absent.
        trafilatura_patches: Optional dict mapping
            ``"fetch_url"`` / ``"extract"`` to return values.

    Returns:
        pipeline.run result dict.
    """
    entry = _make_entry(content_words=content_words)
    fake_feed = types.SimpleNamespace(
        entries=[entry],
        feed=types.SimpleNamespace(get=lambda k, d="": "Test Feed"),
    )
    tf = trafilatura_patches or {}
    fetch_patch = patch(
        "trafilatura.fetch_url",
        return_value=tf.get("fetch_url"),
    )
    extract_patch = patch(
        "trafilatura.extract",
        return_value=tf.get("extract"),
    )
    with fetch_patch as mock_fetch, extract_patch as mock_extract:
        with (
            patch("feedparser.parse", return_value=fake_feed),
            patch("pipeline.load_sources", return_value=[source]),
        ):
            result = pipeline_module.run(
                domain_slug="ai_news",
                days_back=7,
            )
    return result, mock_fetch, mock_extract


def test_body_from_content_encoded() -> None:
    """body is populated from content:encoded when it has ≥ 150 words."""
    source = {
        "url": "http://example.com/feed",
        "min_days_back": 1,
        "no_fetch": False,
    }

    result, mock_fetch, _ = _fake_pipeline_run(source, content_words=150)

    assert len(result["articles"]) == 1
    body = result["articles"][0]["body"]
    assert body is not None
    assert len(body.split()) >= 150
    mock_fetch.assert_not_called()


@pytest.mark.parametrize(
    "url",
    [
        "https://www.bloomberg.com/news/articles/some-story",
        "https://www.example-unlisted-outlet.com/some-story",
    ],
)
def test_fetch_body_makes_no_second_attempt_on_miss(url: str) -> None:
    """A direct-fetch miss returns None after exactly one attempt, with no
    archive.ph (or any other) fallback round-trip - including for
    bloomberg.com, which was on the removed archive.ph allowlist.

    The archive.ph fallback was removed 2026-09-30: when archive.ph was
    unreachable, each fallback burned its full connect timeout serially,
    stretching a normal 4-7min ai_news run to 13.7min and tripping
    signal-detection-agent's poll timeout. This pins run duration to the
    direct fetch alone - see _fetch_body's own docstring.
    """
    with patch("trafilatura.fetch_url", return_value=None) as mock_fetch_url:
        body = pipeline_module._fetch_body(url)

    assert body is None
    mock_fetch_url.assert_called_once()


def test_fetch_body_serializes_extraction() -> None:
    """trafilatura.extract() is only ever called while _EXTRACT_LOCK is
    held - the invariant that keeps concurrent lxml tree copies from
    corrupting the heap and aborting the interpreter.

    Confirmed live from two real faulthandler tracebacks on the
    geopolitical_news 02:00 UTC run (run 550 2026-10-02 segfault, run 560
    2026-10-03 "double free or corruption"), both with several threads
    inside trafilatura.extract() - see _EXTRACT_LOCK's own comment.
    """
    seen_locked: list[bool] = []

    def _record_lock_state(*args, **kwargs):
        seen_locked.append(pipeline_module._EXTRACT_LOCK.locked())
        return "extracted body"

    with (
        patch("trafilatura.fetch_url", return_value="<html>page</html>"),
        patch("trafilatura.extract", side_effect=_record_lock_state),
    ):
        body = pipeline_module._fetch_body("http://example.com/a")

    assert body == "extracted body"
    assert seen_locked == [True]


def test_body_trafilatura_fallback() -> None:
    """body is populated via Trafilatura when content:encoded is absent."""
    source = {
        "url": "http://example.com/feed",
        "min_days_back": 1,
        "no_fetch": False,
    }
    trafilatura_body = "Extracted article body text from Trafilatura."

    result, mock_fetch, mock_extract = _fake_pipeline_run(
        source,
        content_words=0,
        trafilatura_patches={
            "fetch_url": "<html>page</html>",
            "extract": trafilatura_body,
        },
    )

    assert len(result["articles"]) == 1
    assert result["articles"][0]["body"] == trafilatura_body
    # fetch_url is called with config=_TRAFILATURA_CONFIG (the User-Agent
    # fix - see pipeline.py's own comment on _TRAFILATURA_CONFIG) on every
    # real call site, not just a bare url - assert on the url positional
    # arg only, since the config object itself isn't meaningful to compare
    # by identity here.
    mock_fetch.assert_called_once()
    assert mock_fetch.call_args.args == ("http://example.com/test",)
    assert "config" in mock_fetch.call_args.kwargs


def _insert_stored_run_with_article(domain: str, url: str) -> None:
    """Insert a completed run with one article already stored for domain."""
    with get_db() as conn:
        row = conn.execute(
            """
            INSERT INTO runs
                (name, domain, days_back, model,
                 status, completed_at, article_count)
            VALUES
                (?, ?, 7, 'test-model', 'completed', NOW(), 1)
            RETURNING id
            """,
            (f"prior-{domain}", domain),
        ).fetchone()
        conn.execute(
            """
            INSERT INTO articles (run_id, url, title, source)
            VALUES (?, ?, 'Already Seen', 'Test Feed')
            """,
            (row["id"], url),
        )


def test_cross_run_dedup_excludes_previously_stored_url() -> None:
    """An article whose URL is already stored for the domain is dropped,
    and never re-inserted."""
    domain = "ai_news"
    seen_url = "http://example.com/already-seen"
    _insert_stored_run_with_article(domain, seen_url)

    seen_entry = types.SimpleNamespace(published_parsed=None)
    seen_entry.get = lambda k, d="": {  # type: ignore[assignment]
        "title": "Already Seen",
        "link": seen_url,
        "published": "2026-01-01",
        "summary": "Summary.",
    }.get(k, d)
    new_entry = types.SimpleNamespace(published_parsed=None)
    new_entry.get = lambda k, d="": {  # type: ignore[assignment]
        "title": "Brand New Article",
        "link": "http://example.com/brand-new",
        "published": "2026-01-01",
        "summary": "Summary.",
    }.get(k, d)
    fake_feed = types.SimpleNamespace(
        entries=[seen_entry, new_entry],
        feed=types.SimpleNamespace(get=lambda k, d="": "Test Feed"),
    )

    with patch("feedparser.parse", return_value=fake_feed):
        result = pipeline_module.run(
            domain_slug=domain,
            days_back=7,
        )

    urls = {a["url"] for a in result["articles"]}
    # Membership, not equality. `ai_news` also has SerpAPI, GDELT and
    # NewsAPI sources, and only feedparser is patched here - so a real
    # network response from any of those lands in the same list and an
    # equality assertion fails on articles this test is not about
    # (confirmed live: 262 unrelated articles, one from cio.com). What
    # this test exists to prove is the dedup decision on the two entries
    # it supplied.
    assert "http://example.com/brand-new" in urls
    assert seen_url not in urls


def test_cross_domain_dedup_excludes_url_stored_under_different_domain() -> None:
    """A URL already stored under a DIFFERENT domain's run is dropped too -
    dedup is global, not scoped to the requesting domain."""
    seen_url = "http://example.com/stored-under-other-domain"
    _insert_stored_run_with_article("smart_money", seen_url)

    seen_entry = types.SimpleNamespace(published_parsed=None)
    seen_entry.get = lambda k, d="": {  # type: ignore[assignment]
        "title": "Already Seen Elsewhere",
        "link": seen_url,
        "published": "2026-01-01",
        "summary": "Summary.",
    }.get(k, d)
    fake_feed = types.SimpleNamespace(
        entries=[seen_entry],
        feed=types.SimpleNamespace(get=lambda k, d="": "Test Feed"),
    )

    with patch("feedparser.parse", return_value=fake_feed):
        result = pipeline_module.run(
            domain_slug="ai_news",
            days_back=7,
        )

    # The supplied entry must be dropped. Checked by membership rather
    # than by an empty list, for the same reason as the sibling test
    # above: `ai_news` carries unpatched SerpAPI/GDELT/NewsAPI sources
    # whose real responses arrive in the same list.
    assert seen_url not in {a["url"] for a in result["articles"]}


def test_fetch_gdelt_parses_doc_response() -> None:
    """_fetch_one_gdelt maps GDELT DOC API fields onto article dicts."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = GDELT_RESPONSE_FIXTURE
    mock_resp.raise_for_status.return_value = None

    with patch("httpx.get", return_value=mock_resp) as mock_get:
        results = pipeline_module._fetch_one_gdelt(
            "theme:ARMEDCONFLICT sourcelang:english", days_back=1
        )

    assert mock_get.call_args.kwargs["params"]["timespan"] == "1d"
    assert len(results) == 1
    article = results[0]
    assert article["url"] == "https://example.com/conflict-story"
    assert article["title"] == "Conflict escalates in region"
    assert article["source"] == "example.com"
    assert article["summary"] is None
    assert article["body"] is None
    assert article["metadata"]["sourcecountry"] == "Ukraine"
    assert article["_pub_date"] is not None


def test_fetch_one_gdelt_returns_sentinel_on_429_without_retrying() -> None:
    """_fetch_one_gdelt makes exactly one attempt and signals rate-limiting
    via the sentinel - retry/round-robin orchestration lives in
    _fetch_gdelt, not here."""
    rate_limited_resp = MagicMock()
    rate_limited_resp.status_code = 429

    with patch("httpx.get", return_value=rate_limited_resp) as mock_get:
        result = pipeline_module._fetch_one_gdelt(
            "theme:ARMEDCONFLICT sourcelang:english", days_back=1
        )

    assert result is pipeline_module._GDELT_RATE_LIMITED
    assert mock_get.call_count == 1


def test_fetch_gdelt_round_robins_before_retrying_a_rate_limited_query() -> None:
    """A rate-limited query is not retried immediately - _fetch_gdelt moves
    on to the next query first, and only retries it in a later pass."""
    ok_resp = MagicMock()
    ok_resp.status_code = 200
    ok_resp.json.return_value = GDELT_RESPONSE_FIXTURE
    ok_resp.raise_for_status.return_value = None
    rate_limited_resp = MagicMock()
    rate_limited_resp.status_code = 429

    source = {
        "source_type": "gdelt",
        "config": {"queries": ["theme:ARMEDCONFLICT", "theme:TERROR"]},
    }

    # theme:ARMEDCONFLICT: 429 then ok on retry. theme:TERROR: ok first try.
    # Call order proves TERROR (query 2) runs BEFORE ARMEDCONFLICT's retry.
    with (
        patch(
            "httpx.get",
            side_effect=[rate_limited_resp, ok_resp, ok_resp],
        ) as mock_get,
        patch("pipeline.time.sleep"),
    ):
        results = pipeline_module._fetch_gdelt([source], days_back=1)

    assert mock_get.call_count == 3
    first_call_query = mock_get.call_args_list[0].kwargs["params"]["query"]
    second_call_query = mock_get.call_args_list[1].kwargs["params"]["query"]
    assert first_call_query == "theme:ARMEDCONFLICT"
    assert second_call_query == "theme:TERROR"  # moved on, didn't retry immediately
    assert len(results) == 1  # same URL from both queries, deduped


def test_fetch_gdelt_gives_up_after_max_rounds_still_rate_limited() -> None:
    """A query still 429ing after _GDELT_MAX_ROUNDS passes is skipped, not
    retried forever."""
    rate_limited_resp = MagicMock()
    rate_limited_resp.status_code = 429

    source = {
        "source_type": "gdelt",
        "config": {"queries": ["theme:ARMEDCONFLICT"]},
    }

    with (
        patch("httpx.get", return_value=rate_limited_resp) as mock_get,
        patch("pipeline.time.sleep"),
    ):
        results = pipeline_module._fetch_gdelt([source], days_back=1)

    assert results == []
    assert mock_get.call_count == pipeline_module._GDELT_MAX_ROUNDS


def test_fetch_gdelt_dedupes_across_queries_and_paces_requests() -> None:
    """_fetch_gdelt dedupes by URL across theme queries and sleeps between calls."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = GDELT_RESPONSE_FIXTURE
    mock_resp.raise_for_status.return_value = None

    source = {
        "source_type": "gdelt",
        "config": {"queries": ["theme:ARMEDCONFLICT", "theme:TERROR"]},
    }

    with (
        patch("httpx.get", return_value=mock_resp) as mock_get,
        patch("pipeline.time.sleep") as mock_sleep,
        patch("trafilatura.fetch_url", return_value="<html>page</html>"),
        patch("trafilatura.extract", return_value="Extracted body text."),
    ):
        results = pipeline_module._fetch_gdelt([source], days_back=1)

    assert len(results) == 1  # same URL returned by both queries, deduped
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once()  # spacing enforced between the two queries
    assert results[0]["body"] == "Extracted body text."


def test_fetch_gdelt_body_none_when_extraction_fails() -> None:
    """_fetch_gdelt leaves body as None when Trafilatura can't fetch the page."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = GDELT_RESPONSE_FIXTURE
    mock_resp.raise_for_status.return_value = None

    source = {
        "source_type": "gdelt",
        "config": {"queries": ["theme:ARMEDCONFLICT"]},
    }

    with (
        patch("httpx.get", return_value=mock_resp),
        patch("pipeline.time.sleep"),
        patch("trafilatura.fetch_url", return_value=None),
    ):
        results = pipeline_module._fetch_gdelt([source], days_back=1)

    assert len(results) == 1
    assert results[0]["body"] is None


def test_fetch_gdelt_skips_body_fetch_for_already_stored_urls() -> None:
    """_fetch_gdelt drops globally-already-stored URLs before body-fetch."""
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.json.return_value = GDELT_RESPONSE_FIXTURE
    mock_resp.raise_for_status.return_value = None

    source = {
        "source_type": "gdelt",
        "config": {"queries": ["theme:ARMEDCONFLICT"]},
    }

    with (
        patch("httpx.get", return_value=mock_resp),
        patch("pipeline.time.sleep"),
        patch(
            "pipeline.get_already_stored_urls",
            return_value={"https://example.com/conflict-story"},
        ) as mock_lookup,
        patch("trafilatura.fetch_url") as mock_fetch_url,
    ):
        results = pipeline_module._fetch_gdelt([source], days_back=1)

    assert results == []
    mock_fetch_url.assert_not_called()  # no body fetch wasted on a stored URL
    mock_lookup.assert_called_once_with(
        ["https://example.com/conflict-story"]
    )


def test_body_null_for_no_fetch_source() -> None:
    """body is None and Trafilatura is not called for no_fetch sources."""
    source = {
        "url": "http://example.com/feed",
        "min_days_back": 1,
        "no_fetch": True,
    }

    result, mock_fetch, mock_extract = _fake_pipeline_run(
        source, content_words=0
    )

    assert len(result["articles"]) == 1
    assert result["articles"][0]["body"] is None
    mock_fetch.assert_not_called()
    mock_extract.assert_not_called()


def _article(title: str, url: str, source: str = "Test Feed") -> dict:
    """Return a minimal article dict for title-dedup tests."""
    return {
        "title": title,
        "url": url,
        "source": source,
        "summary": "",
        "published": "2026-01-01T00:00:00",
        "body": None,
    }


def _one_chunk(rows: list[dict]):
    """Wrap a flat row list as a single-page iter_recent_articles_for_domain
    result, for tests that don't care about multi-chunk pagination."""
    if rows:
        yield rows


def test_title_dedup_drops_cross_run_near_duplicate() -> None:
    """A new article whose title is a near-duplicate (by embedding
    similarity) of an already-stored ai_news article is dropped, and the
    new outlet's DOMAIN (derived from its own url, not its source display
    name) is recorded on the existing row via also_reported_by.

    Confirmed live: also_reported_by was previously recorded from
    article["source"] directly ("PBS", "Bloomberg Law News", etc.), which
    made downstream corroboration checks (comparing against a domain
    allowlist) structurally unable to ever match - fixed to derive a real
    domain from the article's own url instead
    (_extract_domain_for_also_reported_by).
    """
    new_article = _article(
        "OpenAI unveils its next-generation GPT-5 model",
        "https://www.example-outlet-b.com/new-story",
        source="Outlet B",
    )

    with (
        patch(
            "pipeline._embed_titles",
            return_value=[[1.0, 0.0]],
        ),
        patch(
            "pipeline.iter_recent_articles_for_domain",
            side_effect=lambda *a, **k: _one_chunk([{
                "id": 42,
                "title": "OpenAI Releases GPT-5",
                "url": "http://example.com/original-story",
                "metadata": {"title_embedding": [1.0, 0.0]},
            }]),
        ),
        patch("pipeline.append_also_reported_by") as mock_append,
    ):
        result = pipeline_module._dedup_by_title_similarity_for_domain(
            [new_article], "ai_news",
        )

    assert result == []
    # {outlet, title, url}, not a bare domain: the headline is the part
    # worth keeping, since outlets covering one event describe it
    # differently. See append_also_reported_by's docstring.
    mock_append.assert_called_once_with(42, {
        "outlet": "example-outlet-b.com",
        "title": "OpenAI unveils its next-generation GPT-5 model",
        "url": "https://www.example-outlet-b.com/new-story",
    })


def test_title_dedup_hard_match_found_in_later_chunk() -> None:
    """A hard match that only appears in the SECOND page of DB history
    candidates is still caught - proving the chunked/paginated scan
    (iter_recent_articles_for_domain) doesn't stop early or miss later
    pages, the way a bug in the chunking loop's early-break/pending-set
    handling could silently do.
    """
    new_article = _article(
        "OpenAI Releases GPT-5", "https://www.example-outlet-b.com/new-story",
        source="Outlet B",
    )

    def _two_chunks(*args, **kwargs):
        yield [{
            "id": 1,
            "title": "Unrelated Story",
            "url": "http://example.com/unrelated",
            "metadata": {"title_embedding": [0.0, 1.0]},
        }]
        yield [{
            "id": 42,
            "title": "OpenAI Releases GPT-5",
            "url": "http://example.com/original-story",
            "metadata": {"title_embedding": [1.0, 0.0]},
        }]

    with (
        patch("pipeline._embed_titles", return_value=[[1.0, 0.0]]),
        patch(
            "pipeline.iter_recent_articles_for_domain",
            side_effect=_two_chunks,
        ),
        patch("pipeline.append_also_reported_by") as mock_append,
    ):
        result = pipeline_module._dedup_by_title_similarity_for_domain(
            [new_article], "ai_news",
        )

    assert result == []
    mock_append.assert_called_once_with(42, {
        "outlet": "example-outlet-b.com",
        "title": "OpenAI Releases GPT-5",
        "url": "https://www.example-outlet-b.com/new-story",
    })


def test_title_dedup_later_chunk_hard_match_clears_earlier_borderline() -> None:
    """A borderline candidate recorded from chunk 1 must not linger and
    trigger a wasted same-event LLM call once a later chunk produces a
    hard match for that same article - the hard match should win outright
    with no LLM call at all, same as if hard and borderline candidates
    had been in one unchunked scan.
    """
    new_article = _article(
        "Iran president gives a wartime speech",
        "https://www.example-outlet-b.com/new-story",
    )

    def _two_chunks(*args, **kwargs):
        # Borderline-only candidate (similarity ~0.71, below the 0.82
        # geopolitical_news threshold but above the 0.50 same-event floor).
        yield [{
            "id": 1,
            "title": "Iran president delivers defiant UN speech",
            "url": "http://example.com/borderline",
            "metadata": {"title_embedding": [1.0, 0.0]},
        }]
        # Hard match (identical embedding) in the second chunk.
        yield [{
            "id": 42,
            "title": "Iran president gives a wartime speech",
            "url": "http://example.com/exact",
            "metadata": {"title_embedding": [1.0, 1.0]},
        }]

    with (
        patch("pipeline._embed_titles", return_value=[[1.0, 1.0]]),
        patch(
            "pipeline.iter_recent_articles_for_domain",
            side_effect=_two_chunks,
        ),
        patch(
            "pipeline._titles_describe_same_event",
        ) as mock_same_event,
        patch("pipeline.append_also_reported_by") as mock_append,
    ):
        result = pipeline_module._dedup_by_title_similarity_for_domain(
            [new_article], "geopolitical_news",
        )

    assert result == []
    mock_append.assert_called_once_with(42, {
        "outlet": "example-outlet-b.com",
        "title": "Iran president gives a wartime speech",
        "url": "https://www.example-outlet-b.com/new-story",
    })
    mock_same_event.assert_not_called()


def test_title_dedup_keeps_distinct_titles() -> None:
    """Articles with dissimilar title embeddings are both kept."""
    articles = [
        _article("OpenAI Releases GPT-5", "http://example.com/a"),
        _article("Tesla Reports Q3 Earnings", "http://example.com/b"),
    ]

    with (
        patch(
            "pipeline._embed_titles",
            return_value=[[1.0, 0.0], [0.0, 1.0]],
        ),
        patch(
            "pipeline.iter_recent_articles_for_domain",
            side_effect=lambda *a, **k: _one_chunk([]),
        ),
    ):
        result = pipeline_module._dedup_by_title_similarity_for_domain(
            articles, "ai_news",
        )

    assert len(result) == 2


def test_title_dedup_same_batch_duplicate_merged_in_memory() -> None:
    """Two near-duplicate articles surfacing in the same run are merged
    without a DB round-trip - only the first is kept, and the second
    article is recorded as {outlet, title, url} in its in-memory
    also_reported_by list. Same shape as the cross-run path writes via
    append_also_reported_by, so a consumer never has to handle two
    forms depending on which run the duplicate arrived in."""
    first = _article(
        "OpenAI Releases GPT-5", "http://example-outlet-a.com/a", source="Outlet A",
    )
    second = _article(
        "OpenAI unveils GPT-5", "http://example-outlet-b.com/b", source="Outlet B",
    )

    with (
        patch(
            "pipeline._embed_titles",
            return_value=[[1.0, 0.0], [1.0, 0.0]],
        ),
        patch(
            "pipeline.iter_recent_articles_for_domain",
            side_effect=lambda *a, **k: _one_chunk([]),
        ),
        patch("pipeline.append_also_reported_by") as mock_append,
    ):
        result = pipeline_module._dedup_by_title_similarity_for_domain(
            [first, second], "ai_news",
        )

    assert len(result) == 1
    assert result[0]["url"] == "http://example-outlet-a.com/a"
    assert result[0]["metadata"]["also_reported_by"] == [{
        "outlet": "example-outlet-b.com",
        "title": "OpenAI unveils GPT-5",
        "url": "http://example-outlet-b.com/b",
    }]
    mock_append.assert_not_called()


def test_title_dedup_same_event_cache_avoids_repeat_llm_call_for_same_pair() -> None:
    """Two different new articles that both land the exact same
    (new_title, candidate_title) borderline pair against DB history
    trigger only one _titles_describe_same_event call, not two.

    Confirmed live 2026-09-24: GDELT/SerpAPI/RSS fan-out on one wire
    story produced dozens of new articles with an identical raw title,
    each independently landing the same borderline candidate from DB
    history - one exact pair recurred 30 times in a single run, each a
    separate real LLM call before this cache was added.
    """
    new_articles = [
        _article("Wire Story Headline", "https://outlet-a.example.com/a"),
        _article("Wire Story Headline", "https://outlet-b.example.com/b"),
    ]

    with (
        patch(
            "pipeline._embed_titles",
            return_value=[[1.0, 1.0], [1.0, 1.0]],
        ),
        patch(
            "pipeline.iter_recent_articles_for_domain",
            side_effect=lambda *a, **k: _one_chunk([{
                "id": 99,
                "title": "Original Wire Story",
                "url": "http://example.com/original",
                "metadata": {"title_embedding": [1.0, 0.0]},
            }]),
        ),
        patch(
            "pipeline._titles_describe_same_event",
            return_value=True,
        ) as mock_same_event,
        patch("pipeline.append_also_reported_by"),
    ):
        pipeline_module._dedup_by_title_similarity_for_domain(
            new_articles, "geopolitical_news",
        )

    assert mock_same_event.call_count == 1
    called_args = mock_same_event.call_args[0]
    assert called_args[:2] == ("Wire Story Headline", "Original Wire Story")


def test_title_dedup_fails_open_on_embedding_failure() -> None:
    """An article whose title embedding failed (None) is kept, not dropped
    or silently deduped."""
    article = _article("Some Article", "http://example.com/a")

    with (
        patch("pipeline._embed_titles", return_value=[None]),
        patch(
            "pipeline.iter_recent_articles_for_domain",
            side_effect=lambda *a, **k: _one_chunk([]),
        ),
    ):
        result = pipeline_module._dedup_by_title_similarity_for_domain(
            [article], "ai_news",
        )

    assert len(result) == 1


def test_title_dedup_only_applied_for_configured_domains() -> None:
    """run() only invokes title-similarity dedup for ai_news/smart_money,
    not for domains pre-scoped by other means (e.g. company_news)."""
    entry = types.SimpleNamespace(published_parsed=None)
    entry.get = lambda k, d="": {  # type: ignore[assignment]
        "title": "Some Article",
        "link": "http://example.com/company-news-article",
        "published": "2026-01-01",
        "summary": "Summary.",
    }.get(k, d)
    fake_feed = types.SimpleNamespace(
        entries=[entry],
        feed=types.SimpleNamespace(get=lambda k, d="": "Test Feed"),
    )

    with (
        patch("feedparser.parse", return_value=fake_feed),
        patch(
            "pipeline._dedup_by_title_similarity_for_domain"
        ) as mock_dedup,
    ):
        pipeline_module.run(domain_slug="company_news", days_back=7)

    mock_dedup.assert_not_called()


class TestEdinetHoldingRatioPairs:
    """A large-shareholding filing states the holder's PREVIOUS ratio
    beside the current one, so a change report can be read as a buy or
    a sell. The parser used to keep only the last current value and
    ignore the prior entirely.

    Shapes here are transcribed from six real filings fetched live
    (S100Z25L, S100Z2O2, S100YNFG, S100Z0FE, S100Z0BQ, S100YZWG).
    """

    CUR = "jplvh_cor:HoldingRatioOfShareCertificatesEtc"
    PRV = "jplvh_cor:HoldingRatioOfShareCertificatesEtcPerLastReport"

    def _csv(self, rows):
        """Build the tab-separated, 9-column shape the real export has."""
        return "\n".join(
            '"%s"\t"ctx"\t""\t""\t""\t""\t""\t""\t"%s"' % (el, val)
            for el, val in rows)

    def _parse(self, text, monkeypatch):
        """Drive the real parser over `text` with the network stubbed."""
        import io, zipfile
        p = pipeline_module

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("x.csv", text.encode("utf-16"))
        payload = buf.getvalue()

        class _R:
            status_code = 200
            content = payload
            def raise_for_status(self): pass

        monkeypatch.setattr(p.httpx, "get", lambda *a, **k: _R())
        monkeypatch.setattr(p, "_edinet_rate_sleep", lambda: None)
        return p._fetch_edinet_holding_ratio("S1", "key")

    def test_single_pair_change_report(self, monkeypatch):
        """Toshiba/Kioxia S100Z25L: 14.06% -> 12.84%, a real sell-down."""
        text = self._csv([(self.CUR, "0.1284"), (self.PRV, "0.1406")])
        assert self._parse(text, monkeypatch) == (0.1284, 0.1406)

    def test_co_filer_filing_takes_the_aggregate_not_the_last_filer(self, monkeypatch):
        """S100YZWG lists each co-filer, then repeats every current
        value. The old parser returned the last bare value (0.0824 here
        only by luck of ordering); on S100Z2O2 that logic reported
        5.94% for a filing whose aggregate pair is the last COMPLETE
        one. The last pair is the aggregate."""
        text = self._csv([
            (self.CUR, "0.0217"), (self.PRV, "0.0286"),
            (self.CUR, "0.0607"), (self.PRV, "0.0645"),
            (self.CUR, "0.0824"), (self.PRV, "0.0931"),
            (self.CUR, "0.0217"), (self.CUR, "0.0607"), (self.CUR, "0.0824"),
        ])
        assert self._parse(text, monkeypatch) == (0.0824, 0.0931)

    def test_initial_report_has_no_previous(self, monkeypatch):
        """S100Z0FE is a 大量保有報告書 - the prior field is "－"."""
        text = self._csv([
            (self.CUR, "0.0150"), (self.PRV, "－"),
            (self.CUR, "0.0384"), (self.PRV, "－"),
            (self.CUR, "0.0535"), (self.PRV, "－"),
        ])
        assert self._parse(text, monkeypatch) == (0.0535, None)

    def test_negative_current_value_is_kept(self, monkeypatch):
        """S100Z0BQ really carries -0.0001 for one co-filer."""
        text = self._csv([
            (self.CUR, "-0.0001"), (self.PRV, "0.0119"),
            (self.CUR, "0.0614"), (self.PRV, "0.0722"),
        ])
        assert self._parse(text, monkeypatch) == (0.0614, 0.0722)

    def test_no_pair_falls_back_to_the_bare_current(self, monkeypatch):
        text = self._csv([(self.CUR, "0.0501")])
        assert self._parse(text, monkeypatch) == (0.0501, None)

    def test_unreadable_export_is_fail_open(self, monkeypatch):
        p = pipeline_module
        def _boom(*a, **k):
            raise OSError("network down")
        monkeypatch.setattr(p.httpx, "get", _boom)
        monkeypatch.setattr(p, "_edinet_rate_sleep", lambda: None)
        assert p._fetch_edinet_holding_ratio("S1", "key") == (None, None)


def test_english_check_published_is_a_datetime_not_a_raw_string() -> None:
    """ddgs does not always return an ISO date.

    Some results carry display text instead - "Opinion7 days ago" was
    seen live. That value reached Postgres as a `published` timestamp
    and raised InvalidDatetimeFormat, aborting the whole insert batch
    for the run. The fetcher must store the PARSED datetime (or None),
    never the raw string.

    Covers Japan, Korea and China at once: all three English-coverage
    checks call this one function.
    """
    results = [
        {"url": "https://example.com/a", "title": "Real date",
         "date": "2026-10-01T12:00:00+00:00"},
        {"url": "https://example.com/b", "title": "Display text",
         "date": "Opinion7 days ago"},
        {"url": "https://example.com/c", "title": "No date", "date": None},
    ]

    class _FakeDDGS:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def news(self, *a, **kw):
            return results

    fake_module = types.ModuleType("ddgs")
    fake_module.DDGS = lambda *a, **kw: _FakeDDGS()

    with patch.dict("sys.modules", {"ddgs": fake_module}):
        articles = pipeline_module._fetch_press_jp_english_check(
            [{"code": "6857", "company": "Advantest"}], days_back=30,
        )

    assert articles, "expected the fetcher to return rows"
    for article in articles:
        published = article.get("published")
        assert published is None or isinstance(published, datetime), (
            f"published must be a datetime or None, got {published!r}"
        )
        # _pub_date is the field _fetch_articles renames; it must agree.
        assert article.get("_pub_date") == published
