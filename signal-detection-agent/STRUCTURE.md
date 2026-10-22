# Structure

## Root

| File | Purpose |
|------|---------|
| `Dockerfile` | Multi-stage build: `base` (production), `dev` (hot-reload), `test` (pytest). Build context is the repo root; copies `signal-detection-agent/src/`, `signal-detection-agent/requirements.txt`, `shared/src/`, and `signal-detection-agent/prompts/` into the image. |
| `requirements.txt` | Production dependencies |
| `requirements-test.txt` | Test-only dependencies |
| `pyproject.toml` | pytest configuration (`asyncio_mode=auto`) |
| `prompts/ai_universe_signal_classifier_v1.txt` | Base-pass LLM system prompt - 11 AI-universe categories + "none" (noise only), signal/weak_signal/noise scoring bands, tie-breaker rules, 15 labelled examples |
| `prompts/ai_universe_signal_classifier_v2_refine.txt` | Second-pass (STORM-style) LLM system prompt - refinement rules, novelty/confidence definitions with score bands, 3 labelled examples |
| `prompts/middle_east_signal_extract_v1.txt` | middle_east_market_signal extraction prompt - FACTS ONLY (event code M1-M8, companies, deal level 1-6, amount, capacity, effective date, place, confirmation, good/bad news, quotes, 2-3 sentence summary); code decides the label |

## App Layers

```
src/
├── __main__.py          Entry point - Click group: serve, classify-filings,
│                                       classify-taiwan-signals
├── app.py               App factory - assembles FastAPI, registers routers
├── auth.py              Infrastructure - x-ocn-caller header extraction
├── db.py                Infrastructure - _new_connection(), init_db() (creates
│                                         agent_jobs + agent_classifications);
│                                         re-exports get_db/transaction/DuplicateError
│                                         from shared/src/db_utils.py
├── seed.py              Entry point helper - seeds agent_job_statuses on startup
├── config.py            Configuration - OPENAI_API_KEY, OPENAI_BASE_URL, OPENAI_MODEL,
│                                        NEWS_RETRIEVAL_URL, PIPELINE_POLL_TIMEOUT_SECS,
│                                        DEFAULT_PROMPT, DEFAULT_PROMPT_V2 paths,
│                                        WEB_SEARCH_PROVIDER, WEB_SEARCH_API_KEY
├── routes/
│   ├── health.py        Route - GET /health (Postgres liveness)
│   ├── run.py           Route - POST /run → 202 {job_id, cached:false} or 200 {job_id, cached:true}
│   └── jobs.py          Route - GET /jobs, GET /jobs/{id}, GET /jobs/{id}/results
├── controllers/
│   └── run.py           Controller - submit_run() resolves news_run_id + checks cache
│                                     (returns existing job on hit); run_agent_pipeline()
│                                     background task: get_run_articles → classify each
│                                     article → insert_classification → update status
├── models/
│   └── jobs.py          Repository - agent_jobs + agent_classifications;
│                                     cursor pagination via shared/src/cursor_utils.py
├── pipeline/
│   ├── classifier.py    LLM classifier - classify_article_two_stage() (STORM two-pass),
│   │                                     classify_article() (base pass),
│   │                                     validate_classification_v2() (novelty/confidence/
│   │                                       pre_verification_score/verification_qa validation),
│   │                                     validate_classification() (base-pass validation),
│   │                                     apply_verification_adjustments() (QA score → final score + trace),
│   │                                     build_user_prompt_v2(), build_user_prompt(),
│   │                                     has_usable_body(); OpenAI-compatible API (OPENAI_BASE_URL)
│   ├── category_candidates.py  PARKED - embedding cosine-similarity pre-step to
│   │                                     shortlist top-k categories per article;
│   │                                     not called in v1; re-enable via category_hints
│   │                                     param in classify_article() if needed
│   └── taiwan_signal_classifier.py  Deterministic + LLM classifier for
│                                     taiwan_market_signal - score_revenue_filing()
│                                     (mops_revenue, MoM + YoY-change vs the company's
│                                     own 36-month history, no LLM),
│                                     classify_material_announcements() (mops_material,
│                                     clause-code lookup table, no LLM),
│                                     translate_taiwan_articles() (LLM translation via
│                                     OPENAI_MODEL_V2, per source_category field map),
│                                     classify_gdelt_articles() (gdelt - the one real LLM
│                                     HIGH/WEAK classification call),
│                                     classify_taiwan_signal_batch() (top-level: score +
│                                     clause-lookup + translate, called by
│                                     controllers/run.py's run_taiwan_signal_classification)
│   ├── taiwan_announcement_classifier.py  taiwan_market_signal mops_material: ROC date
│   │                                     conversion, fixed-NOISE clauses, SEC_FILING_MODEL
│   │                                     rating + summary with output checks
│   ├── taiwan_listed_companies.py  generated TWSE/TPEx table (code, Chinese full/short,
│   │                                     English short) for the announcement name checks
│   ├── china_companies.py      The ONE static table for china_market_signal -
│                                     CHINA_COMPANIES keyed by exchange code, holding
│                                     each company's native_name, hk_code, signal_roles
│                                     and read_through (tickers + direction +
│                                     relationship). codes_with_role(),
│                                     read_through_for() and china_company_universe()
│                                     are DERIVED views over it, never maintained
│                                     beside it - three parallel structures used to
│                                     hold these facts and a missed edit failed
│                                     silently
│   ├── middle_east_signal_classifier.py  middle_east_market_signal: source naming,
│                                     the spec's events table, company-table check,
│                                     headline rules, SEC 13D/G rule, one extraction
│                                     call per article, fact checks, label rules
│                                     (decide_label, spec section 7), read-through,
│                                     corroboration and story grouping
│   └── china_signal_classifier.py  Deterministic + LLM classifier for
│                                     china_market_signal - gate_filings() (Gate 1
│                                     triage, pure regex, discards ~93% before any
│                                     LLM call), classify_policy_binding() (C1, the
│                                     one judgment call), classify_substitution_
│                                     progress() (C2), classify_capacity_commitment()
│                                     (C3), classify_accelerator_milestone() (C4),
│                                     classify_platform_capex() (C5),
│                                     classify_trade_deviation() (C6),
│                                     classify_us_company_action() (C7);
│                                     compute_c2/c3/c5_baselines() (written to
│                                     market_signal_baselines by the refresh job,
│                                     read by the daily pass), point_in_time_baseline()
│                                     and universe_yoy_floors() (only observations
│                                     PRECEDING a filing's own period may judge it),
│                                     _to_cny()/_fx_lookup() (ECB rate pinned to the
│                                     filing's publication date, cached per
│                                     currency/date), resolve_issuer() (the body that
│                                     published the document, title first and feed
│                                     only as fallback), attach_source_metadata()
│                                     (copies the article's identifying fields onto
│                                     every row centrally), classify_china_signal_
│                                     batch() (top-level, called by controllers/
│                                     run.py's run_china_signal_classification)
├── geopolitical_signal_classifier.py  geopolitical_signal Stage A - deterministic
│                                     keyword/lookup noise removal, no model call
│                                     anywhere; drops 80-90% of daily volume before
│                                     Stage B's first paid call. Cross-outlet dedup
│                                     is NOT here - news-retrieval owns it
├── geopolitical_signal_layer1.py      geopolitical_signal Stage A - row creation
├── geopolitical_signal_overrides.py   geopolitical_signal Stage A - source-specific
│                                     overrides applied over the layer-1 rules
├── geopolitical_signal_stage_b.py     geopolitical_signal Stage B - the one
│                                     irreversible verdict: talk vs action ->
│                                     signal / weak_signal / noise. Fails OPEN
│                                     (unsure -> weak_signal, never noise) because
│                                     weak_signal and noise never reach C/D/E
├── geopolitical_signal_layer2.py      geopolitical_signal Stage C - one model call
│                                     per HIGH article: channel, assets, category,
│                                     entities, event_date, generated_summary. Raw
│                                     urllib, same as Stage B. Fail-open: a parse
│                                     failure stores HIGH with null tags rather
│                                     than dropping a real signal
├── geopolitical_signal_stage_d.py     geopolitical_signal Stage D - corroboration
│                                     grading, no model call. Absent key = never
│                                     ran; {"count": 0} = ran and stands alone
└── geopolitical_signal_stage_e.py     geopolitical_signal Stage E - story grouping,
│                                     no model and no network call; reuses the
│                                     entities Stage C verified. build_stories()
│                                     (seeded, not union-find), story_development()
│                                     (the field written to the seed row),
│                                     _entity_keys()/_is_specific()/_linked().
│                                     MARKS, never deletes - see its docstring
└── adapters/
    ├── news_client.py   Infrastructure - async HTTP client for news-retrieval:
    │                                     trigger_run(), fetch_latest_run(),
    │                                     poll_run_until_done(), get_run_articles(),
    │                                     list_completed_runs() (pools ALL completed runs
    │                                     in a date range - used by the Taiwan path, which
    │                                     needs the full day's pooled articles rather than
    │                                     just the latest run)
    └── web_search.py    Infrastructure - urllib search client for entity context:
                                          search_entity_context(); providers: duckduckgo
                                          (default, free), tavily, brave
```

Dependencies flow one way: `__main__` → `app` → `routes` → `controllers` → `models` → `db`.
`pipeline/` and `adapters/` are called by `controllers/run.py` only.

## Pipeline Flow

`POST /run {domain, run_id?, force?}`:

1. Resolve `news_run_id`: if `run_id` given → poll until done; else if `use_latest_run` → reuse latest; else `trigger_run + poll`
2. Cache check (unless `force=true`): if a completed job already exists for this `news_run_id` → return 200 `{job_id, cached:true}` immediately
3. Create new `agent_jobs` row → return 202 `{job_id, cached:false}`

Background task (`run_agent_pipeline`):

4. `get_run_articles(run_id)` - paginates `GET /runs/{id}/articles?include_body=true`
5. Filter articles with a usable body (`has_usable_body`)
6. Build `batch_context` - list of `{title, url}` for all usable articles in this run (passed read-only to all concurrent classifiers)
7. For each article: `classify_article_two_stage()` - two-stage STORM pipeline:
   - **Base pass**: `classify_article()` → LLM (v1 prompt) → `validate_classification()`
   - **If noise**: final = base; second-pass fields = null (skip second pass)
   - **If signal/weak_signal**: gather context concurrently via `ThreadPoolExecutor`:
     - DB: `get_recent_entity_classifications()` - exact lowercased array-overlap on `entity_names_normalized`, last 90 days
     - Web: `search_entity_context()` - queries built from entity name + `signal_reason` (first 60 chars); up to 2 queries, top-3 snippets, 5s timeout; 1.5s rate delay between queries for DuckDuckGo; returns [] on failure
   - **Second pass**: LLM (v2 prompt + article + base result + entity history + web snippets + batch_context, max_tokens=2000) → `validate_classification_v2()`
   - `apply_verification_adjustments()`: sums QA `score_adjustment` values → final score; injects score trace into `refinement_reason`
   - **If second pass fails**: fall back to base result with null second-pass fields
8. `insert_classification(job_id, article, result)` - persists to `agent_classifications`
9. `update_job_status(job_id, "completed")`

## Taiwan Signal Pipeline (`taiwan_market_signal` domain)

Not triggered via `POST /run` - runs as a scheduled CLI job (`classify-taiwan-signals`),
twice daily (14:00 UTC post-Asia-close, 21:00 UTC pre-US-open, Mon-Fri; see
`infra/modules/ecs_cluster/services.tf`'s `signal_detection_agent_taiwan_signals` rule).
news-retrieval fetches this domain independently on its own 4-hourly schedule
(`news_retrieval_taiwan_market_signal`) and stops at fetch/dedup - all revenue scoring,
clause-code lookup, translation, and LLM classification happen here.

```
classify-taiwan-signals [--from-date] [--to-date]   (defaults: today, UTC)
                        [--revenue-period YYYY-MM] [--material-month YYYY-MM]
                        # one-off backfill scope; either set → only that revenue
                        # period + that month's announcements, gdelt skipped
  └─ create_job(domain=TAIWAN_SIGNAL_DOMAIN)
  └─ run_taiwan_signal_classification(job_id, from_date, to_date)
       ├─ list_completed_runs(domain, from_date, to_date)   # ALL runs in window, not just latest
       ├─ get_run_articles(run_id) per run, deduped by url  → pooled article set
       ├─ get_existing_taiwan_source_ids(taiwan_source_id(a))  # drop already-stored FIRST
       ├─ fetch_taiwan_revenue_history()          # news-retrieval company_financials; None → skip revenue this run
       ├─ get_taiwan_remark_translations()        # reuse stored translations of identical remarks
       ├─ classify_taiwan_signal_batch(pending)   # each item judged on its own
       │    ├─ score_revenue_articles()           # mops_revenue: vs the company's own 36 months
       │    ├─ classify_material_announcements()  # mops_material: clause-code table lookup
       │    ├─ translate_taiwan_articles()        # LLM translation (OPENAI_MODEL_V2), per-field
       │    └─ classify_gdelt_articles()          # gdelt: LLM HIGH/WEAK verdict on translated title
       └─ insert_taiwan_signal_classification() per new item → update_job_status("completed")
```

`source_id` is derived deterministically per source_category (ticker+period for
mops_revenue, ticker+timestamp for mops_material, url for gdelt) - not from the news-retrieval
article row id - so the same underlying fact re-appearing across multiple news-retrieval
polls is only ever classified/persisted once, even across the two daily runs.

## Middle East Signal Pipeline (`middle_east_market_signal`)

CLI only: `classify-middle-east-signals [--from-date --to-date --dry-run]` (default window
yesterday-today). `controllers/run.py:run_middle_east_signal_classification`:

```
pool runs in window, dedup by url, drop already-judged      (no model)
company table  <- news-retrieval GET /market/middle-east/universe (job fails if bad)
US companies   <- research-universe GET /companies  (only when a 13D/G is in the batch)
stored event clusters <- get_middle_east_event_members(event_uris)
classify_middle_east_signal_batch:
  sec_fund_stake      -> judge_sec_stake (code)
  market wrap/digest  -> noise by headline (code)
  everything else     -> ONE model call, full body -> check_extraction -> decide_label
                         -> read_through -> corroboration_for
insert (ON CONFLICT DO NOTHING) -> regroup_middle_east_stories (whole set, rewrite)
```

A failed model call stores nothing (retried next run); an unreadable answer is stored as
weak_signal (`rule = model_output_unusable`).

## Geopolitical Signal Pipeline (`geopolitical_signal`)

Not triggered via `POST /run`. The primary trigger is news-retrieval's `--callback-url`
webhook (`/webhooks/news-retrieval-run-completed`), which fires the moment that day's
`geopolitical_news` fetch finishes; the 05:00 UTC CloudWatch rule
(`signal_detection_agent_geopolitical_signal_pipeline_daily`) is a fallback for a lost
webhook, not the main path. Running twice in a day is harmless - every stage's worklist
query selects only rows the previous stage has not processed.

```
run-geopolitical-signal-pipeline [--from-date] [--to-date]   (defaults: today, UTC)
  └─ Stage A  run_geopolitical_signal_stage_a(job_id, from_date, to_date)
  │    ├─ list_completed_runs() -> pooled articles across ALL runs in window
  │    └─ rule-based filter (no model call) -> rows at status 'waiting'
  └─ Stage B  run_geopolitical_signal_stage_b(job_id)        # acts on A's 'waiting'
  │    ├─ LLM: talk vs action -> signal | weak_signal | noise   (IRREVERSIBLE)
  │    └─ collapse_geopolitical_signal_event_group()
  │         siblings -> metadata.also_reported_by, sibling rows DELETED, one txn
  └─ Stage C  run_geopolitical_signal_stage_c(job_id)        # acts on B's signals
  │    └─ LLM: channel, actors, assets, category[], entities, event_date, summary
  └─ Stage D  run_geopolitical_signal_stage_d(job_id)        # acts on C's tagged rows
  │    └─ metadata.corroboration, no model call
  └─ Stage E  run_geopolitical_signal_stage_e(job_id)        # acts on ALL graded signals
       ├─ get_geopolitical_signal_story_candidates()   # whole set, not a delta
       ├─ clear_geopolitical_signal_story_fields()     # idempotent by REWRITING
       ├─ build_stories() -> seeded entity-overlap grouping (no model, no network)
       └─ apply_geopolitical_signal_story() per story, one txn each
            seed row -> metadata.story_development ; members -> metadata.in_story
```

The same five stages, in the same order, run in `routes/webhooks.py`'s
`_run_geopolitical_signal_pipeline_in_background`. `classify-geopolitical-signals-stage-e`
also exists as a standalone command for re-grouping without re-running A-D.

**Stage E runs last in both trigger paths.** It is appended to
`run-geopolitical-signal-pipeline` and to `routes/webhooks.py`'s
`_run_geopolitical_signal_pipeline_in_background`, so every API read of this domain is
post-Stage-E. That is required, not cosmetic: the default signals view filters on
`NOT (metadata ? 'in_story')`, so if E never ran the filter would match every row and the
reader would see one story under several headlines. Any new trigger path must include it;
wiring only one of the two leaves the other writing no story fields at all.

Stage E differs from A-D in scope: A-D each act only on the rows the previous stage just
wrote, but grouping is a property of the whole signal set, so E reads every graded signal
and regroups from scratch, clearing its own two keys first. Re-running is therefore safe
and is expected - the webhook and the fallback schedule can both fire in one day. Verified
against the live 57-signal set: a second run changed 0 of 57 rows, preserved 5 seeds and 8
members, and left no orphaned `in_story` pointer, no row marked both seed and member, and
no story field on a non-signal row. Nothing is deleted at this stage - one row per story is
a filter the caller applies
(`signal_detection='signal' AND NOT (metadata ? 'in_story')`). In Python that predicate
must be written `jsonb_exists(metadata, 'in_story')`, since psycopg2 scans the whole query
string for `?` and `%` placeholders.

Each stage also has its own standalone CLI command (`classify-geopolitical-signals`,
`...-stage-b`, `-c`, `-d`, `-e`) for re-running one stage in isolation.
`refresh-geopolitical-category-map` (03:00 UTC, its own schedule) refreshes the
`geopolitical_signal_companies` cache Stage C reads - company/ticker/category data changes
rarely, so it is not tied to the news cycle.

## HTTP API

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `GET` | `/health` | None | Liveness - Postgres SELECT 1 |
| `POST` | `/run` | x-ocn-caller | Trigger pipeline; body: `{domain, run_id?, limit?, days_back?, use_latest_run?, force?}`; 202 `{job_id, cached:false}` or 200 `{job_id, cached:true}` if already completed |
| `GET` | `/jobs` | x-ocn-caller | Jobs, newest-first; cursor-paginated (`limit`, `cursor`) |
| `GET` | `/jobs/{id}` | x-ocn-caller | Single job status + stats |
| `GET` | `/jobs/{id}/results` | x-ocn-caller | Paginated `agent_classifications` for a job, every row as stored |
| `GET` | `/results` | x-ocn-caller | Results across jobs, filterable (`source_type`, `signal_detection`, dates, ...). `view=events` (default) = one row per event or story (drops `in_story` / `collapsed_into` rows); `view=rows` = every stored row |
| `GET` | `/results/summary` | x-ocn-caller | Counts for a date window, same filters and same `view` as `/results` |

## Database

Tables live in the signal-detection Postgres DB with an `agent_` prefix (mirrors signal-detection's `classification_jobs` / `classifications` pattern):

| Table | Key columns |
|-------|-------------|
| `agent_job_statuses` | `status TEXT PK` - reference values: pending, running, completed, failed |
| `agent_jobs` | `id SERIAL PK`, `news_run_id`, `domain`, `status FK`, `article_count`, `created_at`, `completed_at` |
| `agent_classifications` | `id SERIAL PK`, `job_id FK`, `article_id`, `url`, `title`, `signal_detection`, `signal_score`, `signal_reason`, `materiality`, `category TEXT[]`, `entities_json TEXT`, `entity_names_normalized TEXT[]`, `stored_at`, `base_signal_detection`, `base_signal_score`, `novelty`, `novelty_basis`, `confidence`, `confidence_basis`, `refinement_reason`, `pre_verification_score`, `verification_qa JSONB`, `source_type`, `source_id`, `metadata JSONB` |

`url` and `title` are lightweight display references only - full article content stays in news-retrieval.
Second-pass columns (`base_*`, `novelty*`, `confidence*`, `refinement_reason`, `pre_verification_score`, `verification_qa`) are NULL for noise articles and for rows classified before the two-stage pipeline (backward-compatible).
`entity_names_normalized` is a lowercased TEXT[] for exact array-overlap entity matching; GIN-indexed. NULL/empty on pre-normalisation rows (backfilled on startup).
`source_type` CHECK constraint includes `'taiwan_market_signal'`; `metadata JSONB` carries Taiwan-specific fields (rank, clause-code reason, translated text, etc.) that don't fit the AI-universe schema above. `signal_score` is nullable to support gdelt's unmeasured-confidence NULL case (HIGH/WEAK has no probabilistic score). A partial unique index on `(source_type, source_id) WHERE source_type='taiwan_market_signal'` enforces the dedup described in the Taiwan Signal Pipeline section above.

## LLM Output Schema

| Field | Values | Notes |
|-------|--------|-------|
| `signal_detection` | `signal` \| `weak_signal` \| `noise` | Final label (second pass if ran, else base) |
| `signal_score` | 0.00–1.00 (noise ≤ 0.40, weak_signal 0.41–0.59, signal ≥ 0.60) | Final score |
| `signal_reason` | ~30-word explanation naming specific entities | |
| `materiality` | `high` \| `medium` \| `low` \| `none` (none only for noise) | |
| `category` | one of 11 AI-universe categories - always a real category, including for noise | |
| `entities` | `[{name, type}]` - 1–5 named entities | |
| `base_signal_detection` | `signal` \| `weak_signal` \| `noise` | First-pass label |
| `base_signal_score` | 0.00–1.00 | First-pass score |
| `novelty` | `step_change` \| `meaningful_update` \| `incremental_update` \| `repeated_coverage` or null | null when second pass skipped (noise) |
| `novelty_basis` | ~20-word string or null | which novelty branch and what evidence |
| `confidence` | `high` \| `medium` \| `low` or null | null when second pass skipped (noise) |
| `confidence_basis` | ~20-word string or null | what evidence drove the confidence label |
| `refinement_reason` | 5-6 line string or null | context available → decision → evidence → novelty branch → confidence basis → edge case; score trace appended by code |
| `pre_verification_score` | 0.00–1.00 or null | Stage 2 score before QA adjustments; null when second pass skipped |
| `verification_qa` | list of 2–3 objects or null | per-question score adjustments: `question`, `answer`, `evidence` (verbatim quote + URL if from web), `score_adjustment` |

## Testing

```bash
docker compose up postgres-signal signal-detection-agent --build
curl http://localhost:8003/health
```
