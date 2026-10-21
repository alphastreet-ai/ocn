# Structure

## Root

| Path | Description |
|------|-------------|
| `Dockerfile` | Builds a `python:3.11-slim` image; copies `src/` to `/app` and installs pip dependencies |
| `pyproject.toml` | pytest configuration: `asyncio_mode = "auto"`, `testpaths = ["tests"]` |
| `requirements-test.txt` | Test-only pip dependencies (`pytest`, `pytest-asyncio`) |
| `README.md` | Project overview and quick-start instructions |
| `CLAUDE.md` | AI assistant instructions: documentation index, Jira board, structural guide, maintenance rules |
| `STRUCTURE.md` | This file |
| `tests/` | Automated test suite - see Testing section below |
| `src/__main__.py` | CLI entry point - `click` + `uvicorn.run` |
| `src/app.py` | FastAPI app factory and lifespan hook |
| `src/pipeline.py` | Fetch pipeline (RSS + SerpAPI Google News + NewsAPI + Alpha Vantage + Federal Register + Event Registry + GDELT + the market-domain fetchers, branched by `source_type`); returns list of fetched articles |
| `src/db.py` | Thin adapter: `_new_connection()` (reads `POSTGRES_*` env vars), `init_db()`, and `db_utils.configure()`; re-exports `get_db`, `transaction`, and `DuplicateError` from `shared/src/db_utils.py` so all other imports are unaffected |
| `src/auth.py` | FastAPI dependency functions: `require_auth` (validate Bearer token), `require_admin` (role gate) |
| `src/seed.py` | Idempotent batch seed for `run_statuses`, `frequencies`, `domains`, `sources`, and admin API key |
| `src/poller.py` | Background market data poller: fetches from Alpha Vantage (GLOBAL_QUOTE, OVERVIEW, EARNINGS, TIME_SERIES_DAILY_ADJUSTED, MARKET_STATUS) and SEC EDGAR (8-K/10-Q/10-K metadata) and writes to DynamoDB; three modes — `quotes` (every 15 min), `daily` (once a day), `sec_filings` (once a day); distributed lock via `ocn-market-lock` prevents overlapping runs |
| `src/sec_edgar.py` | SEC EDGAR fetch helpers: ticker→CIK mapping (`company_tickers.json`, cached) + per-CIK recent filings lookup (`submissions/CIK{cik}.json`), filtered to 8-K/10-Q/10-K; returns metadata + primary document link only |
| `src/models/articles.py` | Article queries: pagination, dedup helpers, `expire_articles_for_domain(domain, days)` - deletes articles for one domain published more than `days` days ago (NULL `published` rows are never deleted); `get_recent_gdelt_articles_for_ticker()` - recent same-ticker GDELT rows (by `created_at`, not `published`) for the Taiwan title-similarity dedup window |
| `src/models/` | Pydantic request models + SQL query functions per entity |
| `src/routes/` | FastAPI `APIRouter` definitions, one file per resource |
| `src/routes/market.py` | Market data read endpoints: 6 `GET` routes reading from DynamoDB, served at `/market/*`; plus `/market/china/revenue-history` and `/market/taiwan/revenue-history`, read from Postgres `company_financials`, and `/market/middle-east/universe`, the Middle East company table from `seed.py` |
| `src/taiwan_financials.py` | Taiwan monthly revenue history for `company_financials`: MOPS per-month CSV parse/fetch (`backfill-taiwan-revenue`) and rows from the daily OpenAPI articles - see CLAUDE.md's Taiwan section |

## Market Signal domains

Five market domains - Taiwan, Korea, Japan, China, Middle East - all **fetch/dedup only**. Ranking,
translation, and LLM classification belong to `signal-detection-agent`, not here. Each has its
own company universe and source configs in `src/seed.py` and its own fetchers in
`src/pipeline.py`, partitioned by `source_type` in `_fetch_articles`. They share no code
paths with each other beyond the generic plumbing (`articles` table, the global
`uq_articles_url` dedup index, trafilatura, pdfplumber).

See `CLAUDE.md` for each domain's full source table, sourcing decisions, and schedule.

| Domain | Universe constant | `source_type`s |
|--------|-------------------|----------------|
| `taiwan_market_signal` | `TAIWAN_TICKER_UNIVERSE` | `twse_revenue`, `tpex_revenue`, `twse_material`, `tpex_material`, `gdelt` |
| `korea_market_signal` | `KOREA_TICKER_UNIVERSE` | `dart_filing`, `kr_customs_export`, `gdelt` (+ RSS sources) |
| `japan_market_signal` | `JAPAN_TICKER_UNIVERSE` | `irbank_financials`, `irbank_buyback`, `irbank_company_reference`, `kabutan_tdnet_mirror`, `kabutan_buyback`, `edinet_filing`, `edinet_buyback_status`, `edinet_extraordinary_report`, `seaj_billings`, `monoist_capex`, `press_jp`, `press_jp_english_check` |
| `china_market_signal` | `CHINA_TICKER_UNIVERSE` | `mofcom_policy`, `miit_policy`, `samr_action`, `cac_review`, `cn_state_press`, `hkex_filing`, `cninfo_filing`, `press_cn`, `press_cn_english_check` |
| `middle_east_market_signal` | `MIDDLE_EAST_TICKER_UNIVERSE` | `wam_wire`, `spa_wire`, `bis_press`, `sec_fund_stake`, `federal_register` (own url scheme), `eventregistry` (5 queries, own url scheme), `substack_post` (Middle East AI News) |

Japan reads its universe live from research-universe (`get_tracked_company_universe`) with
seeded `config.companies` as fallback. China is hardcoded - research-universe's `country`
column is inconsistent for Chinese companies; see `CLAUDE.md`.

## App layers

The application is a single FastAPI process. `POST /run` uses FastAPI `BackgroundTasks` to execute the pipeline after the HTTP response is sent. Control flow is entirely Python-driven (not LLM-driven). All domain configuration (sources, polling frequencies) lives in PostgreSQL and is loaded at request time - no code changes are needed to add new domains.

| Layer | File(s) | Responsibility |
|-------|---------|----------------|
| **Entry point** | `src/__main__.py` | CLI wiring (`click`), starts `uvicorn` |
| **App factory** | `src/app.py` | Creates `FastAPI` instance, registers routers, runs lifespan (`init_db` + `seed`) |
| **Routes** | `src/routes/` | Thin HTTP adapters: one `APIRouter` per resource, maps domain exceptions to status codes |
| **Controllers** | `src/controllers/` | Business logic and multi-step orchestration; owns transaction boundaries for composite operations |
| **Repository** | `src/models/` | SQL query functions + Pydantic input models; no HTTP concepts; cursor encode/decode delegated to `shared/src/cursor_utils.py` (`encode_cursor` / `decode_cursor`) |
| **Pipeline** | `src/pipeline.py` | Stateless pipeline: parallel RSS fetch + SerpAPI Google News fetch + NewsAPI top-headlines fetch (branched by `source_type`); returns list of fetched article dicts |
| **Database** | `src/db.py` + `shared/src/db_utils.py` | `db.py` is a thin adapter: supplies `_new_connection()` and calls `db_utils.configure()`; the `_Connection` wrapper, `DuplicateError`, `get_db()`, and `transaction()` live in the repo-level `shared/src/db_utils.py` and are re-exported from `db.py` for backward compatibility |
| **Auth** | `src/auth.py` | `require_auth` / `require_admin` FastAPI dependencies; delegates all token validation to `POST {AUTH_SERVICE_URL}/validate`; returns 503 if unconfigured |
| **Seed data** | `src/seed.py` | Idempotent batch seed for `run_statuses`, `frequencies`, `domains`, and `sources` |

### HTTP API

| Endpoint | Description |
|----------|-------------|
| `POST /run` | Submit a pipeline run; returns `202` with `run_id` immediately, or `200` with `cache_hit: true` if an identical run completed today UTC; optional `callback_url` receives a webhook on completion or failure; `force: true` bypasses both the cache guard and concurrent-run guard |
| `GET /runs` | List runs, newest-first; filter by one or more `domain` slugs (repeat param: `?domain=A&domain=B`), `status`, `from_date`, `to_date`; cursor-paginated (`limit`, `cursor`); returns `{"runs": [...], "next_cursor": str\|null}` |
| `GET /runs/{id}` | Single run record |
| `GET /runs/{id}/articles` | Articles for a run; cursor-paginated (`limit`, `cursor`); returns `{"articles": [...], "next_cursor": str\|null}` |
| `GET /articles/{id}` | Single article record |
| `GET /health` | Service health check |
| `GET /domains` | List all domains - **public**, no auth required |
| `POST /domains` | Create a domain - requires auth; records caller as owner; `PATCH /{id}` requires ownership or admin |
| `GET/POST /sources` | Manage sources (`POST` requires auth; users restricted to domains they own) |
| `GET/POST /frequencies` | Manage frequencies (`POST` admin only) |
| `POST /grants/{id}/domains` | Grant domain access to a key - admin only; upserts grants, returns updated domain list |
| `DELETE /grants/{id}/domains/{domain_id}` | Revoke a single domain grant - admin only; 204 on success, 404 if absent |

### Execution flow

```
POST /run  (returns 202 immediately, or 200 on cache hit)
  └─ create_run_record()        # validate domain + ownership
       ├─ get_cached_run_today() # cache hit → return 200 with cached run + cache_hit: true
       ├─ get_running_run_for_domain() # concurrent guard → 409
       └─ create_run()          # INSERT run row → run_id
  └─ BackgroundTasks.add_task(run_pipeline)  # skipped on cache hit

run_pipeline()  (background, after response is sent)
  └─ pl.run()
       ├─ load_sources()        # query sources WHERE min_days_back <= days_back
       └─ _fetch_articles()     # branches by source_type: _fetch_rss() (feedparser, 10 workers) + _fetch_serpapi() (SerpAPI Google News, 5 workers) + _fetch_newsapi() (NewsAPI top-headlines, 5 workers); respective KEY unset → sources skipped
  └─ create_articles()          # batch INSERT fetched articles
  └─ complete_run() / fail_run() # UPDATE runs SET status='completed'|'failed'
  └─ _fire_webhook()             # POST to callback_url if set (best-effort, 10s timeout)

GET /runs/{id}  →  live status poll
```

### Key behavioural rules

- Sources with `frequency.min_days_back > days_back` are skipped.
- Domain config is loaded fresh from the DB on every `POST /run` - adding a new domain via the API takes effect immediately without restarting.
- Same-day cache guard: if a completed run with identical `(domain, days_back, focus)` already exists for the current UTC day, `POST /run` returns it immediately with `cache_hit: true` (HTTP 200) without dispatching a new pipeline. `force: true` bypasses this.

## Testing

Tests live in `tests/` at the project root and run against a dedicated `news-retrieval-test` PostgreSQL database. `pipeline.run` is mocked at its boundary; all other app code runs in-process via `httpx.AsyncClient` + `ASGITransport`.

### Running the tests

```bash
# From the repo root - start the test DB
docker compose up postgres-news-test -d

# Install test dependencies
pip install -r news-retrieval/requirements-test.txt

# Run suite
pytest news-retrieval/tests/
```

`conftest.py` creates and wipes the test database on each `pytest` session, so no manual DB setup is needed beyond having postgres available.

### Test modules

| Module | Coverage |
|--------|----------|
| `test_auth.py` | Missing/invalid auth header → 422/401; non-admin on admin endpoint → 403; delegated-path: valid key, rejected key, 503 when `AUTH_SERVICE_URL` unset |
| `test_runs.py` | `POST /run`: 202 + DB record created; unknown domain → 404; non-owner → 403 |
| `test_guard_chain.py` | CON-111 concurrent guard → 409 with `run_id`; `force=true` bypasses guard |
| `test_cache_guard.py` | CON-120 same-day cache guard → 200 with `cache_hit: true`; different params miss cache; `force=true` bypasses; yesterday's run is not a hit |
| `test_subset_guard.py` | Time-window subset guard; reuses articles from wider same-day run |
| `test_pagination.py` | Cursor advances on `GET /runs` and `GET /runs/{id}/articles`; last page has `next_cursor: null` |
| `test_webhook.py` | `callback_url` POSTed with `status=completed` on success and `status=failed` on pipeline error |
| `test_ownership.py` | `POST /sources` and `PATCH /domains/{id}` reject non-owners → 403; null-owner domains visible to all users; multi-key grants; grant revocation; admin bypass |
| `test_pipeline.py` | Pipeline fetch behavior |
| `test_taiwan_financials.py` | Taiwan monthly revenue: MOPS CSV parsing, daily-feed rows, UPSERT replacing a restated month, `/market/taiwan/revenue-history` |
| `test_middle_east.py` | Middle East fetchers with faked HTTP: match filter boundaries, 13D/13G XML parsing, WAM paging + filter-before-body, Substack free-written-posts-only with API body, run()'s body and publication-window guards, BIS country filter + DOJ title match, Event Registry `query`/`url_scheme`/`require_body`/`max_pages`, Federal Register `terms`/`url_scheme`, and both shared fetchers unchanged without the new keys |

## Dependencies

### Python packages (installed in Docker image)

| Package | Purpose |
|---------|---------|
| `fastapi` | HTTP server framework |
| `uvicorn` | ASGI server |
| `pydantic` | Data validation and request/response models |
| `openai` | OpenAI-compatible SDK, pointed at Ollama |
| `feedparser` | RSS/Atom feed parsing |
| `httpx` | HTTP/1.1 client used inside the OpenAI SDK |
| `click` | CLI entry point (`--host`, `--port` flags) |
| `psycopg2-binary` | PostgreSQL database driver |

### Runtime requirements

| Variable / resource | Default | Description |
|--------------------|---------|-------------|
| `OPENROUTER_API_KEY` | - | Required. Server-level API key for OpenRouter |
| `AUTH_SERVICE_URL` | - | Required. URL of the auth-service (e.g. `http://auth-service:8001`). All Bearer tokens are validated by `POST {AUTH_SERVICE_URL}/validate`; returns 503 if unset |
| `POSTGRES_HOST` | `localhost` | PostgreSQL server hostname |
| `POSTGRES_PORT` | `5432` | PostgreSQL server port |
| `POSTGRES_DB` | `news-retrieval` | Database name |
| `POSTGRES_USER` | `news-retrieval` | Database user |
| `POSTGRES_PASSWORD` | - | Database password |
| `SERPAPI_KEY` | - | Optional. SerpAPI API key for `google_news` sources, used for all domains except `geopolitical_news`. If unset, those sources are skipped with a warning log. |
| `SERPAPI_KEY_GEOPOLITICAL` | - | Optional. SerpAPI API key for `google_news` sources under the `geopolitical_news` domain specifically (separate key/quota from `SERPAPI_KEY`). If unset, those sources are skipped with a warning log. |
| `NEWSAPI_KEY` | - | Optional. NewsAPI API key for `newsapi` sources. If unset, sources with `source_type = 'newsapi'` are skipped with a warning log. |

### External services

| Service | Used for |
|---------|---------|
| OpenRouter (`openrouter.ai/api/v1`) | LLM inference - relevance filtering, tag generation, cluster naming, embeddings |
| RSS feeds (various) | Source articles - managed via `POST /sources` API or seed data in `src/seed.py` |
| SerpAPI (`serpapi.com/search?engine=google_news`) | Google News article fetch for sources with `source_type = 'google_news'` |
| NewsAPI (`newsapi.org/v2`) | Top-headlines article fetch for sources with `source_type = 'newsapi'` |
| Alpha Vantage (`alphavantage.co/query`) | Market data (quotes, fundamentals, price history, earnings, market status) — polled every 15 min / daily by `poller.py` |

### DynamoDB tables (eu-north-1, IAM auth, PAY_PER_REQUEST)

All tables use TTL on the `ttl` attribute for automatic expiry. Created manually via AWS CLI; will be managed by Terraform after local testing is complete.

| Table | Partition key | Sort key | TTL | Poll mode | Description |
|-------|--------------|----------|-----|-----------|-------------|
| `ocn-market-quote` | `ticker` | `recorded_at` | 7 days | quotes | Current price, change%, volume, previous close |
| `ocn-market-indices` | `ticker` | `recorded_at` | 7 days | quotes | SPY/QQQ/SOXX price and change% |
| `ocn-market-status` | `market` | `recorded_at` | 1 day | quotes | US market open/closed status |
| `ocn-market-overview` | `ticker` | `recorded_at` | 30 days | daily | Fundamentals: market cap, P/E, 52-week range, analyst target, beta, sector |
| `ocn-market-price-history` | `ticker` | `date` | 1 year | daily | Last 10 trading days adjusted close (for actual-vs-simulation chart) |
| `ocn-market-earnings` | `ticker` | `recorded_at` | 30 days | daily | Next earnings date, estimated EPS, last quarter surprise % |
| `ocn-market-lock` | `lock_key` | — | 20 min | both | Distributed lock — prevents overlapping poller runs; auto-expires if poller crashes |
| `ocn-sec-filings` | `ticker` | `accession_number` | 180 days | sec_filings | 8-K/10-Q/10-K filing metadata + primary document link, deduped by accession_number |

### Market data env vars

| Variable | Default | Description |
|----------|---------|-------------|
| `ALPHA_VANTAGE_API_KEY` | - | Required for poller. Premium key (75 calls/min). |
| `AWS_REGION` | `eu-north-1` | AWS region for DynamoDB |
| `DYNAMODB_TABLE_QUOTE` | `ocn-market-quote` | DynamoDB table for quotes |
| `DYNAMODB_TABLE_INDICES` | `ocn-market-indices` | DynamoDB table for index strips |
| `DYNAMODB_TABLE_MARKET_STATUS` | `ocn-market-status` | DynamoDB table for market status |
| `DYNAMODB_TABLE_LOCK` | `ocn-market-lock` | DynamoDB table for distributed poller lock |
| `DYNAMODB_TABLE_OVERVIEW` | `ocn-market-overview` | DynamoDB table for fundamentals |
| `DYNAMODB_TABLE_PRICE_HISTORY` | `ocn-market-price-history` | DynamoDB table for price history |
| `DYNAMODB_TABLE_EARNINGS` | `ocn-market-earnings` | DynamoDB table for earnings |
| `DYNAMODB_TABLE_SEC_FILINGS` | `ocn-sec-filings` | DynamoDB table for SEC EDGAR filing metadata |

### Market data HTTP endpoints (served via api-gateway at `/news/market/*`)

| Endpoint | DynamoDB table | Response |
|----------|---------------|----------|
| `GET /market/quote/{ticker}` | `ocn-market-quote` | price, change, change_percent, volume, previous_close |
| `GET /market/overview/{ticker}` | `ocn-market-overview` | market_cap, pe_ratio, week_52_high, week_52_low, analyst_target, beta, sector |
| `GET /market/price-history/{ticker}` | `ocn-market-price-history` | last 10 days of adjusted_close |
| `GET /market/earnings/{ticker}` | `ocn-market-earnings` | next_report_date, estimated_eps, last_surprise_pct |
| `GET /market/indices` | `ocn-market-indices` | SPY/QQQ/SOXX price and change_percent |
| `GET /market/status` | `ocn-market-status` | current_status, local_open, local_close |
| `GET /market/sec-filings/{ticker}` | `ocn-sec-filings` | form_type, filed_at, accession_number, primary_doc_url (all recent filings, newest first) |

### Database schema

Seven normalized tables. `run_statuses`, `frequencies`, `domains`, and `sources` are populated at startup; new rows can be added through the API at runtime. `runs` and `articles` are populated by pipeline runs. API key lifecycle is managed entirely by auth-service - news-retrieval stores only per-key domain grants.

| Table | Key columns | Notes |
|-------|-------------|-------|
| `api_key_domains` | `api_key_id`, `domain_id` (composite PK) | Junction table: explicit domain-access grants per key (key IDs come from auth-service) |
| `run_statuses` | `name` (PK) | Lookup table: `running`, `completed`, `failed` |
| `frequencies` | `name`, `min_days_back` | e.g. daily=1, weekly=7, monthly=30 |
| `domains` | `name`, `slug`, `description`, `created_by` | `created_by` is audit-only (plain integer, no FK); access control uses `api_key_domains`; null = globally accessible |
| `sources` | `url`, `domain_id`, `frequency_id`, `name`, `description`, `source_type`, `config` | FK to `domains` and `frequencies`; `source_type` is `rss` (default), `google_news`, or `newsapi`; `config` is JSONB for source-type-specific params |
| `runs` | `name`, `domain`, `started_at`, `completed_at`, `status`, `article_count`, `summary`, `callback_url`, `model` | One row per `POST /run`; `status` FK to `run_statuses`; `model` records the LLM used |
| `articles` | `run_id`, `url`, `title`, `summary`, `body`, `source`, `published` | FK to `runs` |
