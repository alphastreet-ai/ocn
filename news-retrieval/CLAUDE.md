# news-retrieval

Part of the [ocn monorepo](../CLAUDE.md).

## How to use this file
Do not load all documentation upfront. Read the index below,
identify which docs are relevant to your current task, and
fetch only those. Use the 'Read when' column as your guide.

## Documentation Index
| Doc | Read when | Page ID |
|-----|-----------|---------|
| [Technical Specifications](https://opengrowthventures.atlassian.net/wiki/spaces/Projects/pages/30113793/Technical+Specifications) | Making architectural or technical decisions | 30113793 |
| ↳ [CON-95: API Integration-Readiness - Open Questions](https://opengrowthventures.atlassian.net/wiki/spaces/Projects/pages/61898754/CON-95+API+Integration-Readiness+Open+Questions) | Reviewing open questions for the API integration-readiness epic | 61898754 |
| [Sources](https://opengrowthventures.atlassian.net/wiki/spaces/Projects/pages/28705610/Sources) | Adding, removing, or evaluating data sources | 28705610 |
| [PRD](https://opengrowthventures.atlassian.net/wiki/spaces/Projects/pages/28705568/PRD) | Implementing or questioning any feature | 28705568 |
| [Roadmap](https://opengrowthventures.atlassian.net/wiki/spaces/Projects/pages/28508185/Roadmap) | Planning, scoping, or prioritising work | 28508185 |

Confluence space: `Projects` - Cloud: `opengrowthventures.atlassian.net`

## Jira Board
| Board | URL | Project Key |
|-------|-----|-------------|
| OCN Board | https://opengrowthventures.atlassian.net/jira/software/projects/CON/boards/34 | CON |

## Structure

See [STRUCTURE.md](STRUCTURE.md) for descriptions.

```
news-retrieval/
├── Dockerfile
├── pyproject.toml        # pytest config (asyncio_mode=auto)
├── requirements-test.txt # test-only pip deps
├── README.md
├── CLAUDE.md
├── STRUCTURE.md
├── tests/                # automated test suite (pytest)
│   ├── conftest.py       # session/function fixtures (DB, keys, client)
│   ├── test_auth.py
│   ├── test_runs.py
│   ├── test_guard_chain.py
│   ├── test_cache_guard.py
│   ├── test_subset_guard.py
│   ├── test_pagination.py
│   ├── test_webhook.py
│   ├── test_ownership.py
│   ├── test_pipeline.py
│   └── test_middle_east.py
└── src/
    ├── __main__.py       # CLI entry point (uvicorn + click)
    ├── app.py            # FastAPI app factory
    ├── auth.py           # require_auth / require_admin FastAPI dependencies
    ├── pipeline.py       # Fetch + relevance filter pipeline (fetch → LLM title filter)
    ├── china_financials.py # Structured revenue history for the China universe (cninfo data20 + Alpha Vantage)
    ├── db.py             # Thin adapter: _new_connection() (POSTGRES_* env vars), init_db(), db_utils.configure(); re-exports get_db/transaction/DuplicateError from shared/src/db_utils.py
    ├── seed.py           # Idempotent seed for run_statuses, frequencies, domains, sources
    ├── models/           # DB query functions (repository layer)
    │   ├── api_key_domains.py
    │   ├── articles.py
    │   ├── atomic.py
    │   ├── company_financials.py  # upsert/read company_financials (reported figures per period)
    │   ├── domains.py
    │   ├── frequencies.py
    │   ├── runs.py
    │   └── sources.py
    ├── controllers/      # Business logic and multi-step orchestration
    │   ├── domains.py
    │   └── run.py
    ├── poller.py         # Background market data poller: AV fetch → DynamoDB; run_quotes (every 15 min) + run_daily (once daily); distributed lock via ocn-market-lock table
    └── routes/           # Thin HTTP adapters (FastAPI APIRouters)
        ├── grants.py
        ├── articles.py
        ├── domains.py
        ├── frequencies.py
        ├── health.py
        ├── market.py     # Market data read endpoints — 6 GET routes reading from DynamoDB, served at /market/*
        ├── run.py
        ├── runs.py
        └── sources.py
```

## Market Data

AV (Alpha Vantage) data is never fetched on the request path. A background poller writes to DynamoDB; the read endpoints serve from there.

### Poll modes

| Mode | Schedule | What it fetches |
|------|----------|----------------|
| `quotes` | Hourly, 14:00-20:00 UTC, Mon-Fri (CloudWatch) | `GLOBAL_QUOTE` per ticker, SPY/QQQ/SOXX indices, `MARKET_STATUS` |
| `daily` | 00:30 UTC daily (CloudWatch) | Macro indicators once per run, not per ticker: `FEDERAL_FUNDS_RATE`, `CPI`, `TREASURY_YIELD`, `UNEMPLOYMENT`, `NONFARM_PAYROLL`, `REAL_GDP`, `RETAIL_SALES`, `DURABLES`, `TOP_GAINERS_LOSERS`; then per ticker: `OVERVIEW` (+ `ROC` and `MOM` momentum, merged into the same item), `EARNINGS`, `TIME_SERIES_DAILY_ADJUSTED` |
| `sec_filings` | 12:00 UTC daily (CloudWatch) | SEC EDGAR 8-K/10-Q/10-K plus foreign-issuer 6-K/20-F/40-F metadata + filing link per ticker (see SEC Filings below) |

Run manually: `python __main__.py poll-market --mode quotes`

DynamoDB access for the poller is granted via an IAM role policy (`aws_iam_role_policy.news_retrieval_dynamodb_market` in `infra/modules/ecs_cluster/market_data.tf`) attached to `ecs_task_exec_ssm` (the same role used for ECS exec) - not the plain `ecs_task_execution` role. If `task_role_arn` on the news-retrieval task definition ever changes again, this policy attachment must move with it, or DynamoDB writes will start failing with an access-denied error that looks unrelated to the actual cause.

### DynamoDB tables (eu-north-1, PAY_PER_REQUEST, IAM auth)

| Table | Partition key | Sort key | TTL | Mode |
|-------|--------------|----------|-----|------|
| `ocn-market-quote` | `ticker` | `recorded_at` | 4 days | quotes |
| `ocn-market-indices` | `ticker` | `recorded_at` | 4 days | quotes |
| `ocn-market-status` | `market` | `recorded_at` | 4 days | quotes |
| `ocn-market-overview` | `ticker` | `recorded_at` | 30 days | daily |
| `ocn-market-price-history` | `ticker` | `date` | 1 year | daily |
| `ocn-market-earnings` | `ticker` | `recorded_at` | 30 days | daily |
| `ocn-market-lock` | `lock_key` | — | 20 min | both |
| `ocn-sec-filings` | `ticker` | `accession_number` | 180 days | sec_filings |
| `ocn-market-macro` | `indicator` | `recorded_at` | 90 days | daily |

### Market data HTTP endpoints (proxied via api-gateway at `/news/market/*`)

| Endpoint | Returns | 503 if |
|----------|---------|--------|
| `GET /market/quote/{ticker}` | price, change, change_percent, volume, previous_close | no data |
| `GET /market/overview/{ticker}` | market_cap, pe_ratio, 52w high/low, analyst_target, beta, sector, revenue_ttm, shares_outstanding, momentum_roc, momentum_mom, moving_avg_50day, moving_avg_200day, eps, forward_pe, price_to_book, ev_to_ebitda, profit_margin, operating_margin_ttm, return_on_equity_ttm, quarterly_earnings_growth_yoy, quarterly_revenue_growth_yoy, dividend_yield, dividend_per_share | no data |
| `GET /market/price-history/{ticker}` | last 10 days of adjusted_close | no data |
| `GET /market/earnings/{ticker}` | next_report_date, estimated_eps, last_surprise_pct | no data |
| `GET /market/indices` | SPY, QQQ, SOXX price + change_percent | no data |
| `GET /market/status` | current_status, local_open, local_close | no data |
| `GET /market/macro` | fed_funds_rate, cpi, treasury_yield_10y, unemployment, nonfarm_payroll, real_gdp, retail_sales, durables — each with date, value, unit; plus top_movers (top_gainers/top_losers/most_actively_traded lists) | no data |
| `GET /market/sec-filings/{ticker}` | recent 8-K/10-Q/10-K filings: form_type, filed_at, accession_number, primary_doc_url, cik, accepted_at, period_of_report, item_codes, filer_category | no data |
| `GET /market/middle-east/universe` | The Middle East company table (`MIDDLE_EAST_TICKER_UNIVERSE`) unchanged - listing, aliases, US `counterparties` with `share_price_on_good_news`. Read by signal-detection-agent once per classify run | never - static |
| `GET /market/china/revenue-history` | China universe revenue YoY per company and reporting period, from `company_financials` (Postgres, not DynamoDB). `?refresh=true` re-fetches upstream first; `?codes=a,b` narrows that re-fetch | never - returns `[]` |

### SEC Filings

Fetched from SEC EDGAR (`data.sec.gov`), not Alpha Vantage. Ticker→CIK mapping via `https://www.sec.gov/files/company_tickers.json` (cached process-lifetime), filings list via `https://data.sec.gov/submissions/CIK{cik}.json`. Six form types are kept: 8-K, 10-Q, 10-K and — for foreign private issuers, which file none of those three — 6-K, 20-F and 40-F. The foreign forms matter for the non-US names in the universe: Alibaba files 6-K and nothing else, so a domestic-only filter would store zero filings for it while looking like it worked. Deduplicated per ticker by `accession_number` — each filing is a permanent, unique key from EDGAR, so re-running the poller never creates duplicates and skips filings already stored. Stores metadata + a link to the primary document only, not the filing body — `signal-detection-agent`'s daily filing-classification job reads this metadata via `GET /market/sec-filings/{ticker}` and fetches the body text itself (see `src/sec_edgar.py`).

### Ticker universe

Single source of truth: `get_tracked_ticker_universe()` in `src/pipeline.py`, which fetches US-listed tickers live from research-universe (`GET /companies?country=United States&has_ticker=true`, requires `RESEARCH_UNIVERSE_URL`/`RESEARCH_UNIVERSE_API_KEY`) and normalizes them to Alpha Vantage/SEC EDGAR format (`_normalize_av_ticker`: dot share-class suffixes like `BRK.B` → `BRK-B`; other dotted tickers, e.g. foreign exchange suffixes, are dropped). Returns `[]` (poll run skipped) if `RESEARCH_UNIVERSE_URL` is unset or research-universe is unreachable — there is no hardcoded fallback list. Used by both the Alpha Vantage fetch and the `GET /market/tracked-tickers` route, so they never drift apart.

This call crosses a security-group boundary: `research_universe`'s security group must allow ingress on port 8007 from `news_retrieval`'s security group (in addition to the ALB), or the request silently times out rather than erroring clearly — see `infra/modules/security_groups/main.tf`.

## Article Retention (Postgres)

Unlike the DynamoDB market-data tables (which all use native TTL), the Postgres `articles` table has no built-in expiry — rows persist indefinitely by default. Two domains have an explicit weekly cleanup job; every other domain's articles are retained forever.

Manual run: `python __main__.py expire-articles --domain <slug> --days <n>` - deletes articles for one domain published more than `--days` days ago (default 7); rows with a NULL `published` date are never deleted (fail-open, no reliable age to judge them by). One-shot, runs to completion and exits, same shape as `trigger` and `poll-market`.

| Domain | Retention | Schedule (CloudWatch) | Rationale |
|--------|-----------|------------------------|-----------|
| `geopolitical_news` | 7 days | Sunday 04:00 UTC | Short-lived event coverage; volume is high and low-value past a week |
| `company_news` | 30 days | Sunday 05:00 UTC | Matches the TTL already used for Alpha Vantage's other data types (`ocn-market-overview`, `ocn-market-earnings`) |

Both schedules run after that domain's own daily fetch completes (`geopolitical_news` fetches at 02:00 UTC, `company_news` at 01:00 UTC) and are offset from each other, so no two scheduled jobs overlap. CloudWatch targets reference the task definition by family name only (no revision pinned), so a scheduled run always launches whatever revision is currently `ACTIVE` — see `infra/CLAUDE.md` for why revision-pinned targets are a real drift risk in this repo.

## Taiwan Market Signal (`taiwan_market_signal` domain)

Fetch-and-store only - all ranking, clause-code classification, translation, and LLM
classification happen downstream in `signal-detection-agent` (see that service's CLAUDE.md),
not here. This domain's own `POST /run`/`trigger` fetch is the only responsibility owned by
news-retrieval; the boundary is deliberate (`src/pipeline.py`, near the end of the Taiwan
fetch path) so that no source type computes a signal or translates text inside this service.

Ticker universe: `TAIWAN_TICKER_UNIVERSE` in `src/seed.py` - a fixed list of `{ticker,
company, native_name, exchange}` (TWSE or TPEx), used to scope every Taiwan source below and
to set `metadata.translated_company_name` directly from known-correct English names (LLM
translation of bare 2-4 character Taiwan company names was confirmed live to produce serious
errors, e.g. 2383 Elite Material mistranslated as "Taiwan Semiconductor Manufacturing
Company" - so this field is never LLM-translated).

Each entry also carries `customers`: the company's **verified** customers as
`{name, relationship}` - the one source of truth for Taiwan customers. A customer is listed only
when a primary source names it (the company's own annual report / MOPS filing / press release,
the customer's own SEC 10-K or Apple's official supplier list, or an official MOPS announcement
naming the counterparty); press and analyst claims, partners, investors and code-only customers
(客戶A) are left out; `[]` means checked and none verified (researched 2026-10-19, 68 customers
across 12 of the 20 companies). `pipeline._taiwan_customers` copies the list onto every Taiwan
revenue, material-announcement and GDELT article as `metadata.customers`, reading it from code at
fetch time - not from the seeded `sources.config` like `ticker_names` - so an edit takes effect on
the next run without a DB update. signal-detection-agent stores it unchanged.

| Source | `source_type` | `source_category` (metadata) | What it fetches |
|--------|---------------|-------------------------------|------------------|
| TWSE Monthly Revenue | `twse_revenue` | `mops_revenue` | TWSE OpenAPI monthly revenue (`t187ap05_L`), full-dump filtered to TWSE tickers in the universe, with MoM/YoY deltas |
| TPEx Monthly Revenue | `tpex_revenue` | `mops_revenue` | TPEx OpenAPI monthly revenue (`mopsfin_t187ap05_O`), same shape for TPEx/OTC tickers |
| TWSE Material Announcements | `twse_material` | `mops_material` | TWSE OpenAPI material announcements (`t187ap04_L`, 重大訊息) |
| TPEx Material Announcements | `tpex_material` | `mops_material` | TPEx OpenAPI material announcements (`mopsfin_t187ap04_O`) - different keyset than TWSE's, normalized to a common shape in the fetcher |
| GDELT DOC API (Taiwan) | `gdelt` | `gdelt` | Taiwan-language financial press via GDELT DOC 2.0, scoped `sourcelang:chinese sourcecountry:TW`, queried per ticker by native Chinese name (when ≥3 chars - GDELT rejects shorter query keywords) and English company name |

Both TWSE/TPEx OpenData endpoints are full-dump, keyless JSON with no ticker/date query
param - the fetcher pulls the entire dataset each poll and filters to
`TAIWAN_TICKER_UNIVERSE`. For material announcements and GDELT there is no backfill:
missing a poll window loses that day's new rows permanently. Monthly revenue is the
exception - see below.

### Monthly revenue history (`company_financials`, `src/taiwan_financials.py`)

signal-detection-agent judges each revenue filing against the company's own past 36
months, so revenue is also stored as a series in `company_financials`
(`domain = 'taiwan_market_signal'`, `metric = 'revenue'`, `period_type = 'M01'..'M12'`,
`period_year`, `value` = the month, `prior_value` = the same month last year **as printed
in that filing**, values in NT$ thousands). `yoy_pct` is left null (it is `value /
prior_value`) and last month's revenue is not stored (it is the previous row).
`prior_value` comes from the filing, never our own row from 12 months earlier, because
companies restate - KYEC restated 2023-04..2024-03 by about -28%.

Two writers, one table (UPSERT on company + month):
- **Daily:** every `taiwan_market_signal` run stores its OpenAPI revenue rows
  (`_store_taiwan_revenue` in `controllers/run.py`, best-effort - never fails the run).
- **MOPS CSV:** `python __main__.py backfill-taiwan-revenue` reads MOPS's per-month
  revenue CSV for each exchange (`mopsov.twse.com.tw/server-java/FileDownLoad`,
  `t21sc03_{ROC year}_{month}.csv`, `sii` / `otc`) - same columns as the OpenAPI feeds,
  confirmed equal from ECS. Default `--from-month 2019-01` is the one-time backfill (~186
  requests, ~5 min); `--months-back 3` is the monthly re-read
  (`news_retrieval_taiwan_revenue_monthly`, 15th 03:00 UTC) that recovers a missed poll
  and picks up restatements. Exits non-zero if any exchange-month could not be fetched.

Read by `GET /market/taiwan/revenue-history` (same row shape as the China endpoint).

GDELT's own API response (`mode=artlist`) has no body/snippet field, but this domain's GDELT
fetch does not rely on that - it separately fetches each surviving article's real webpage via
Trafilatura and sets `article["body"]` from the extracted page text. `taiwan_market_signal` is
now the only domain using GDELT; `geopolitical_news` fetches from Event Registry, which
returns the body with the article.

**Schedule:** `${env}-news-retrieval-taiwan-market-signal` CloudWatch rule, every 4 hours
bounded to 01:00-16:00 UTC (fires 01/05/09/13 UTC), Mon-Fri only. The window covers TWSE/TPEx's
9:00 AM-1:30 PM Taipei trading session (01:00-05:30 UTC) *and* extends through 16:00 UTC
because 重大訊息 material-announcement disclosures are confirmed to cluster after market
close through the evening (17:30 Taipei / 09:30 UTC onward), not during trading hours. Does
not account for Taiwan's own holiday calendar (e.g. Lunar New Year) - polling a closed market
day is harmless (dedup prevents duplicate inserts) but not currently suppressed.

Downstream: `signal-detection-agent`'s `classify-taiwan-signals` CLI command runs twice daily
(14:00 UTC post-Asia-close, 21:00 UTC pre-US-open, Mon-Fri) and pools **all** of a day's
completed runs for this domain via `GET /runs?domain=taiwan_market_signal&status=completed`
(news-retrieval's own poll cadence above can produce several completed runs per day - the
consumer must read across all of them, not just the latest, or it silently misses earlier
runs' data).

## Japan Market Signal (`japan_market_signal` domain)

Fetch-and-store only, same boundary as Taiwan above - no ranking, translation, or LLM
classification happens in this service. Modeled directly on `taiwan_market_signal`'s
fetch/classify split; downstream classification is a `signal-detection-agent` task, not yet
built as of this writing.

Ticker universe: `JAPAN_TICKER_UNIVERSE` in `src/seed.py` - 20 companies from narrow,
near-monopoly steps in chipmaking (wafers, patterning chemicals, mask inspection, wafer
dicing/test, substrates) plus wider AI-universe read-through names. `code` is `TEXT`
throughout (Kioxia's code, `285A`, contains a letter - confirmed this matters the same way
Taiwan's ticker-as-text convention does). `fiscal_year_end` (`MM-DD`) is stored per company
for future progress-vs-forecast math - most companies here end their fiscal year 03-31, not
12-31. **Shinko Electric (6967) confirmed delisted** 2026-09-28 via EDINET's own official
code list (see `edinet_code.py` below) - its own entry shows "非上場" (unlisted) with a blank
securities-code field, resolving the spec's earlier "take-private status unconfirmed" flag.
Kept in the universe (its EDINET code still resolves for any residual filings) but no
ticker-keyed fetcher will ever match it going forward.

**Sourcing decisions (confirmed live before building), see git history on this file's
`japan_market_signal` section for the full record:**
- **TDnet** (Tokyo Stock Exchange's own disclosure network) has no free API - official tiers
  start ~JPY 50,000/mo, and even those carry a documented lag during earnings season. Not
  used. **IRBANK** (irbank.net, free, unofficial) substitutes for it entirely below.
- **J-Quants** (JPX's official API) free tier caps history at 2 years with a 12-week delay,
  and even a paid tier's revision data has no reason/commentary field - a further paid add-on
  would be needed for that. Not used - IRBANK substitutes for both gaps at no cost, and
  additionally exposes the source PDF filing directly (see below), which J-Quants never would
  have.
- **EDINET** (Financial Services Agency's official filing API) - confirmed working, free,
  requires a registered API subscription key (`EDINET_API_KEY`) but no approval delay.

All 9 originally-planned source_types are built and confirmed working together end-to-end (a
combined `trigger --domain japan_market_signal --days-back 31` run produced 459 real articles
across all of them with zero conflicts). A 10th, `irbank_company_reference`, was added
afterward to support signal-detection-agent's J5/J6 classifiers (see below):

| Source | `source_type` | `source_category` (metadata) | What it fetches |
|--------|---------------|-------------------------------|------------------|
| IRBANK Forecast Revisions | `irbank_financials` | `jp_forecast` | Earnings-forecast revision notices per company, via IRBANK's free `irbank.net/{code}/tdnet` disclosure-history mirror - see full extraction design below. Primary source for revision figures + reason text |
| IRBANK Buyback Status | `irbank_buyback` | `jp_buyback` | Buyback-program history per company via IRBANK's free `irbank.net/{code}/buyback` page - genuinely clean structured data (date, share-count change, cumulative amount, cumulative % of program limit), grouped by board-resolution program/year. Primary source for buyback figures |
| IRBANK Company Reference | `irbank_company_reference` | `jp_company_reference` | Latest known total-assets AND shares-outstanding figures per company, via IRBANK's free `irbank.net/{code}/bs` ("financial condition") page plus its main `/{code}` page - a clean per-fiscal-period history table for total assets, of which only the single newest row is kept per run, plus a derived shares-outstanding estimate (market cap / previous-close price - no direct share-count field exists anywhere on IRBANK). Reference data, not news - see full design below. Feeds J5's investment-vs-total-assets ratio and J6's buyback-vs-shares-outstanding ratio |
| Kabutan TDnet Mirror | `kabutan_tdnet_mirror` | `jp_disclosure` | Same-day disclosure mirror via Kabutan's free, global, paginated feed (`kabutan.jp/disclosures/`), walked page-by-page until the date rolls to the prior trading day, filtered client-side to the ticker universe. Daily-only, not backfilled |
| Kabutan Buyback | `kabutan_buyback` | `jp_buyback_announce` | Same feed as above, scoped server-side to `kubun=j` (自社株取得) - the buyback announcement TRIGGER, distinct from `irbank_buyback`'s ongoing status figures |
| EDINET Large Shareholding | `edinet_filing` | `jp_ownership` | 5%-large-shareholding filings (docTypeCode 350) via EDINET's official API - percentage held is fetched from each filing's own CSV export (never present in list metadata) |
| EDINET Buyback Status | `edinet_buyback_status` | `jp_buyback_status` | Share-repurchase status filings (docTypeCode 220/230) via the same EDINET API/infra as above (`_fetch_edinet_by_mode`) - stores the filing's raw acquisition text block as-is (no discrete numeric field exists for this doctype); same-day corroboration only, `irbank_buyback` is the numeric source of record |
| SEAJ Equipment Billings | `seaj_billings` | `jp_industry` | Monthly Japan semiconductor-equipment billings (3-month moving average - confirmed no free source anywhere publishes a true single-month figure) via SEAJ's free English press-release PDF, resolved fresh from the index page each run (the PDF's own filename is not stable/predictable) |
| MONOist Factory News | `monoist_capex` | `jp_capex` | Capacity/investment news via MONOist's free 工場ニュース series listing, filtered client-side by company `native_name` OR `short_name` when present (no per-company query capability exists for this source). Fetches each surviving article's own full body text too (`_fetch_monoist_article_body`, cp932-decoded then trafilatura-extracted) - added after confirming live that only ~20% of real articles carried an investment yen figure in the listing's own title/summary snippet vs. ~62% once the real article body is fetched; needed for J5's investment-figure extraction |
| Japan Press (Jiji + Newswitch) | `press_jp` | `jp_press` | Free Nikkei-equivalent wire coverage via Jiji Press's economy-category listing (broad) and newswitch.jp's semiconductor keyword page (targeted) - headline+timestamp only, no article body. Reuters Japan (anti-bot blocked) and Kyodo News English (no business/economy category exists) were checked and confirmed unusable |
| Japan Press English-Coverage Check | `press_jp_english_check` | `jp_english_coverage_check` | Independent per-company DuckDuckGo News search (free, no API key) - the design spec's own Step 5 English-mirror requirement. Same shape as Korea's `KOREA_GDELT_ENGLISH_SOURCE` (an independent per-company search, not a per-article cross-reference) - a distinct, earlier-in-the-pipeline use of the same tool signal-detection-agent's `web_search.py` already uses for classification-time entity context, not a duplication of that job |

### `irbank_company_reference` design (confirmed live against all 20 universe companies)

Added to support signal-detection-agent's J5 (capacity commitment) and J6 (ownership/capital
policy) classifiers, whose spec rules need "investment >= 10% of total assets" (J5) and
"buyback >= 5% of shares outstanding" (J6). Folded into one source_type since both facts come
from the same IRBANK domain in the same per-company loop and share the same slow refresh
cadence - see pipeline.py's own top-of-section comment for the full reasoning.

**Total assets**: `irbank.net/{code}/bs` carries a clean, already-structured
`財務履歴（百万円）` (financial history, JPY millions) table with one row per fiscal period
(confirmed live: 23 real quarterly/annual rows for Disco, back to 2007/03), oldest-first -
only the LAST row (newest fiscal period) is kept, since this is a slow-changing reference fact
(a company's total assets moves at most once per fiscal quarter), not something needing
habit-style history. Confirmed live 2026-09-29 for all 20 companies with 0 failures, including
Kioxia's letter-code (285A) and delisted Shinko Electric (6967, which still returns its last
known real figure, dated 2025/03 - consistent with its confirmed 2026-09-28 delisting noted
above). One real bug caught during testing: an initial version's table-caption regex assumed a
bare `<caption>財務履歴（百万円）</caption>` with nothing else inside, which broke on
Advantest's own page (whose caption carries an extra class attribute and a nested "see more"
`<ul>` before the caption closes) - fixed by loosening the regex to match up to `</caption>`
rather than immediately after the label text.

**Shares outstanding**: no direct share-count field exists anywhere on IRBANK's pages checked
(confirmed live: no 発行済株式/上場株式数 field on the main `/{code}` page or the `/dividend`
page) - derived instead as `market_cap / previous_close_price`, both of which ARE present on
the main `/{code}` page. Confirmed live three distinct real market-cap label formats exist
depending on company size (trillion+oku e.g. `24兆8880億`; oku+man only for smaller caps e.g.
`1660億2262万`; oku alone e.g. `1兆681億`) - all three handled by
`_parse_irbank_market_cap_yen`. Confirmed live for all 20 companies with 0 failures (e.g.
Advantest: market cap ¥24.888tn / price ¥33,060 -> ~752.8M shares, consistent with its real
public share count). Allowed to be independently `None` on a row without dropping the whole
row - total assets alone still serves J5 even if this derivation fails for one company.

Dedup: synthetic url `irbank-company-reference://{code}/{period}` (e.g.
`irbank-company-reference://6146/2026/06`) - `period` is the `/bs` page's own fiscal-period
label, so a company's reference row is only re-stored once its next real fiscal-period row
actually appears on IRBANK, not on every scheduled poll (shares-outstanding is tagged with the
same period even though it technically comes from a live market-cap snapshot - see
pipeline.py's own comment on why this is deliberate). Seeded `frequency_name: "monthly"` -
total assets changes at most quarterly, so a monthly check is already more frequent than
needed, and a day-level check would waste requests without gaining anything.

**`short_name` and the same stale-seed gotcha `GDELT_SOURCE` already has**: some
`JAPAN_TICKER_UNIVERSE` entries carry a `short_name` (e.g. Resonac Holdings's full
`レゾナック・ホールディングス` vs. the abbreviated `レゾナック` real press articles actually
use - confirmed live this is a REAL gap, not theoretical: Resonac and Renesas Electronics
each matched **zero** real MONOist articles by full `native_name` alone vs. 5 and 7
respectively once `short_name` was added). `monoist_capex` and `press_jp` both match against
either field via `_match_japan_company()` in `pipeline.py`. Same caveat as `GDELT_SOURCE`
above applies here too: `sources` rows are seeded `ON CONFLICT DO NOTHING`, so an
already-seeded database's stored `config` will NOT pick up a `short_name` added in code later
- confirmed live this silently kept an already-seeded test DB matching only 62 articles
instead of the corrected 74 until the stored `config` for both `monoist_capex` and `press_jp`
was updated directly (`UPDATE sources SET config = ... WHERE source_type = ...`). Any
environment seeded before this note was added needs the same manual update.

### `irbank_financials` extraction design (confirmed live against real filings)

Three-step fetch, no API key required (IRBANK has no auth), confirmed working end-to-end for
Advantest (6857, 19 real revisions back to 2013) and Disco (6146, 50 real revisions):

1. `GET irbank.net/{code}/tdnet` - IMPORTANT: this 301-redirects to `/{edinet_code}/tdnet`
   (e.g. `/6857/tdnet` → `/E01950/tdnet`); `httpx.get()` does not follow redirects by
   default, so every call against irbank.net passes `follow_redirects=True` (confirmed live
   this is not optional - a bare call raises on the 301 rather than erroring clearly). Parse
   the page's "備考" (notes) section - `<dd id="note_N">` blocks, each labelled (修正
   [revision], 説明 [presentation materials], 配当 [dividend], etc.). Keep only blocks
   labelled exactly "修正" - this is more robust than matching on title text directly, since
   real title wording varies (confirmed: a variance-vs-actual + forward-guidance combo notice
   still correctly lands in a 修正-labelled block despite not matching a simpler
   "...の修正に関するお知らせ" title pattern).
2. For each revision notice found, `GET irbank.net/{code}/{doc_id}` (the per-filing detail
   page) and extract its PDF link (`https://f.irbank.net/pdf/{date}/{doc_id}.pdf` - confirmed
   present on every detail page tested; an earlier design depended on an opportunistic
   `/news/{id}` IRBANK-generated HTML summary instead, confirmed via live testing across 4
   companies NOT to be reliably paired with every revision - dropped entirely in favor of the
   PDF, which also recovers the company's own reason text the HTML summary never had).
3. Download and parse the PDF with `pdfplumber` (`requirements.txt` dependency,
   `_extract_irbank_pdf_tables()`/`_fetch_irbank_filing_pdf()` in `pipeline.py`). Real filing
   PDFs are text-based (not scanned images) and contain a clean structured table: a header
   row of line-item names (売上高/営業利益/経常利益/当期利益 etc.), a row ending in some
   variant of "(A)" (previous forecast) and one ending in "(B)" (revised forecast or, for a
   variance-vs-actual notice, actual results) - matched by a permissive `(A)$`/`(B)$` suffix
   regex rather than an exact-string label allowlist, since real label prefixes vary more than
   expected (confirmed live 4 distinct variants across 2 companies: 前回発表予想(A)/
   今回発表予想(B), 前回発表予想(A)/今回修正予想(B), 前回発表予想(A)/今回実績(B), and
   前回予想/今回予想(B) - an earlier version of this matcher enumerated exact labels and
   silently produced zero tables for Advantest's newest filing as a result). **A single PDF
   can contain multiple such tables** (confirmed live: Disco's variance notice had 4 - Q1
   consolidated variance, Q1 standalone variance, Q2 consolidated forecast, Q2 standalone
   forecast - plus an unrelated dividend table that is naturally excluded since no
   previous/revised row matches it).

   The company's own stated reason for the revision - the one field the original design
   assumed no free source carried - **is available this way**: it's plain prose in the PDF's
   extracted full text (not the tables), following a "理由" or "修正の理由" heading, up to a
   reliable disclaimer boilerplate stop-marker ("※ 将来の事象..." or "（注）上記の予想は...").
   Stored as `metadata.reason` and in the article's `summary` field.

Dedup: synthetic url `irbank-financials://{code}/{doc_id}` - `doc_id` is globally unique per
IRBANK filing, so the existing global `uq_articles_url` index handles all dedup, same
convention as `dart-filing://`, `twse-revenue://`, etc.

**Incremental fetch / backfill** - `irbank_financials` is designed to run daily without
re-walking a company's entire history every time. IRBANK's `/{code}/tdnet` page has no
server-side date-range filter (confirmed live - it always returns the full history), so
`days_back` is applied as a client-side cutoff on each parsed disclosure's own date, combined
with a check against `get_already_stored_urls()` before any PDF is fetched - both act as
early-stops since the page is confirmed newest-first (walking stops once either the date
cutoff or `_IRBANK_STOP_AFTER_CONSECUTIVE_SEEN` consecutive already-stored items is reached).
One code path serves both a one-time backfill and every day after it - no separate mode or
flag:
- **One-time backfill** (do this once, before relying on any habit-aware classification
  downstream - the spec's own Section 9 Step 1 calls for 5 years of history):
  `trigger --domain japan_market_signal --days-back 1825`
- **Daily scheduled run** (once backfilled, this is fast - confirmed live ~0.2-1s per
  company when nothing new has happened, vs. ~30-40s per company on the initial backfill):
  `trigger --domain japan_market_signal --days-back 1`

**Trigger this domain** the same way as any other - no domain-specific route or CLI command
exists or is needed (`POST /run`/`trigger` are fully generic on `domain`, reading source
config from the DB):
```bash
# CLI (runs the pipeline in-process, exits when done)
python __main__.py trigger --domain japan_market_signal --days-back 1

# API (returns 202 immediately, runs as a FastAPI background task)
curl -X POST http://localhost:8000/run \
  -H "Authorization: Bearer <api-key>" -H "Content-Type: application/json" \
  -d '{"domain": "japan_market_signal", "days_back": 1}'
```

**Schedules:** three CloudWatch rules in `infra/modules/ecs_cluster/services.tf`, anchored to
real TSE hours (09:00-15:00 JST, JST = UTC+9, no DST):

| Rule | Schedule (UTC) | `--days-back` |
|------|----------------|---------------|
| `..._japan_market_signal_pre_open` | `cron(0 23 * * ? *)` | 1 |
| `..._japan_market_signal_post_close` | `cron(30 6 * * ? *)` | 1 |
| `..._japan_market_signal_monthly` | `cron(0 4 25 * ? *)` | 31 |

The monthly rule exists because `seaj_billings` is seeded `frequency_name: "monthly"`
(`min_days_back` 30), so a daily `--days-back 1` trigger correctly skips it - the same gating
every other domain's sources use (see `FREQUENCIES` in `seed.py`). It is the ONLY schedule
that ever includes that source.

Run the 5-year backfill manually once before relying on habit-aware classification
downstream (`--days-back 1825`, see above): only `irbank_financials` needs a multi-year
window, every other source_type is daily/monthly-scoped already.

## China Market Signal (`china_market_signal` domain)

Fetch-and-store only, same boundary as Taiwan/Japan/Korea - no ranking, translation, or
LLM classification happens in this service. This is the only market domain where a positive
local signal is usually a **negative** read for the US names attached to it; that direction
is carried downstream in `signal-detection-agent`, not here.

Company universe: `CHINA_TICKER_UNIVERSE` in `src/seed.py` - 20 companies, **hardcoded**,
deliberately NOT read live from research-universe the way Japan is. research-universe files
these companies under two different `country` values (`China` for Hygon, Loongson and Inspur;
`China/Hong Kong` for Naura, SMIC, Cambricon and the rest) with no rule distinguishing them,
and Alibaba and Tencent are filed as `United States` with ADR tickers. Since
`get_tracked_company_universe()` matches `country` exactly, a live read today would return an
arbitrary subset. `company` is the exact `company_name` already stored in research-universe
for the 15 that exist there (hence `Amec`, `Lenovo`, `Inspur Electronic Information`), so a
later migration needs no name reconciliation; 5 (ACM Research Shanghai, Piotech, Hwatsing,
China Northern Rare Earth, JL MAG) are not in research-universe at all.

`native_name` is the exchange's **own registered short name** (`zwjc` in cninfo's
`szse_stock.json`), not the full legal name - confirmed live for all 16 mainland codes. This
is the same finding Japan made the hard way: Chinese press writes 中芯国际, never
中芯国际集成电路制造. `code` is `TEXT` throughout (002371, 000977, 000725, 00700 all lose
leading zeros as integers).

**Sourcing decisions (confirmed live 2026-10-05 before building):**
- **GACC customs** (`customs.gov.cn`, `stats.customs.gov.cn`) - answers HTTP 412 with a
  `__jsluid_h` cookie challenge and obfuscated JS. Reproducible across 5 consecutive requests
  on a persistent `httpx.Client` that carries the minted cookie forward, with full browser
  headers. (One isolated 70KB response was observed and chased down: a transient CDN cache
  hit, not a working bypass.) It needs a JS-executing client, which this service has no
  dependency for.
- **NBS** (`stats.gov.cn`) - blocked at the **network layer from local dev machines** ("The URL
  has been blocked as per the instructions of the Competent Government Authority" / HTTP 403),
  but **works normally from ECS** - confirmed live 2026-10-05 against the staging task, 127KB
  of real HTML. It is therefore built and wired (`nbs_ic_output`), and a developer running this
  domain locally will see that one source fail while every other one works. That is expected,
  not a bug. Its structured easyquery API (`data.stats.gov.cn/easyquery.htm`) is WAF-blocked
  even from ECS (`reason:UrlACL`), so the figure is read from the monthly press release.

### C6 (trade data): covered by two sources, import side and production side

C6 is served by a pair that answer different halves of the same question:

- **`comtrade_china_trade`** - what the world ships INTO China, in dollars (the import side)
- **`nbs_ic_output`** - what China itself makes, in units (the production side)

One without the other misreads the signal: a fall in imports looks like export controls biting
when it may simply be domestic substitution. The August 2026 figures show exactly why both are
needed - US equipment exports to China ran $82m while Chinese IC production grew **+20.6% YoY**.

Seven routes were probed live on 2026-10-05 before settling on these:

| Route | Result |
|-------|--------|
| GACC direct | HTTP 412 JS cookie challenge - reproducible from **ECS too**, so it is not a local-network artifact |
| **NBS** | **Works from ECS** (403 locally). Built as `nbs_ic_output` |
| UN Comtrade, **China-reported** | Monthly series stops at `202412`; `202501`-`202504` all return count=0. Annual is current but useless for a quarterly signal |
| **UN Comtrade, partner-reported** | **This is what we use.** Free, keyless, current to ~2-3 months |
| Eastmoney (`datacenter-web.eastmoney.com`) | Works and is current (Aug 2026), but country-level totals only - every HS-breakdown report name returns `报表配置不存在` |
| OEC (`oec.world`) | Annual only, ends 2024 |
| Trade press (DuckDuckGo news) | Returns stories but no reliable IC figure. Headlines carry *other* numbers - total trade surplus ($806bn), Nvidia quota percentages (13%) - that a naive extractor would silently record as IC trade data. Worse than no data |
| TradingEconomics / WITS / Macromicro / SEMI / SIA | 403, 410, 400, or captcha |

**Why the partner side is arguably the better measurement, not a consolation prize.** Export
controls bite on the *exporter's* side: Washington restricts what American and allied firms
may sell. So the US/Japan/Netherlands export series **is** the control measure, recorded by
the governments doing the restricting rather than by the country being restricted. The live
figures show it plainly - US semiconductor-equipment exports to China run $82-97m/month while
Japan's run $700m-1.28bn.

Reporters are the three equipment-export-control jurisdictions. **South Korea was tried and
dropped**: its Comtrade series lags ~9 months (newest `202512` vs `202607` for the other
three). Korea's own customs service publishes a current figure and is already fetched by this
service under `korea_market_signal` (`kr_customs_export`) - that is where a Korea read should
come from.

`net_weight_kg` travels with `value_usd` because a value move alone cannot separate a price
change from a volume change, which is exactly the distinction C6 needs to read export controls.

Comtrade's public preview tier answers 429 "Rate limit is exceeded" on back-to-back calls;
`_COMTRADE_MIN_INTERVAL` paces at 4s. With 3 reporters x 2 commodities x N months that pacing
dominates the fetch, which is why this source is seeded `monthly` (`min_days_back` 30) - a
daily `--days-back 1` run correctly skips it, the same gating `seaj_billings` uses.

**`nbs_ic_output` specifics.** NBS publishes the previous month's figure around the 15th, as a
table row inside the monthly industrial-output press release (集成电路（亿块） followed by the
month's output and its YoY percent). Two things the parser had to get right, both caught in
testing:

- The figures are **not adjacent to the label** - each sits inside a `<span>` within its own
  `<td>`, separated by ~250 characters of inline style attributes. Matching positionally
  returns nothing.
- `period` is read from the release **title**, not the URL date. NBS publishes August's figure
  on 15 September, so keying off the publication date labelled every month as the following
  one. `period` is the month measured; `published_date` records when it was released.

Confirmed live from ECS: `period=2026-08`, `value=529.0` (亿块 = 100m units), `yoy_pct=20.6`,
`published_date=2026-09-15`. The value is kept in the source's own unit with the unit named
rather than silently converted - a converted figure cannot be checked against the release it
came from.

Still open: GACC remains uncovered. Its 412 challenge reproduces from ECS as well as locally,
so it genuinely needs a JS-executing client (Playwright) rather than a different network path.
That is the only remaining C6 gap, and with the import and production sides both covered it is
now a nice-to-have rather than a blocker.
- **SZSE** (`szse.cn`) - reachable only with TLS verification disabled. Dropped rather than
  weakening TLS for one host: cninfo is the officially designated disclosure site and carries
  the same filings.
- **Yicai, STCN, semiinsights.com** - captcha challenge or connection refused. Excluded.
- **GDELT** - not used for this domain. Taiwan's GDELT path is scoped `sourcelang:chinese
  sourcecountry:TW` (traditional characters, Taiwanese press), not simplified mainland outlets.

| Source | `source_type` | `source_category` (metadata) | What it fetches |
|--------|---------------|-------------------------------|------------------|
| MOFCOM | `mofcom_policy` | `cn_policy` | Ministry of Commerce policy releases and 公告 - export licensing, trade countermeasures, anti-dumping rulings. The highest-impact China source |
| MIIT | `miit_policy` | `cn_policy` | Industrial policy, standards, encouraged/restricted technology catalogues |
| SAMR | `samr_action` | `cn_policy` | Antitrust investigations and penalty decisions, including those naming foreign firms |
| CAC | `cac_review` | `cn_policy` | Cybersecurity reviews of foreign technology products - the 2023 memory-procurement mechanism |
| Xinhua English | `cn_state_press` | `cn_policy` | State news agency; often publishes policy before the issuing ministry's own site updates |
| HKEX | `hkex_filing` | `cn_disclosure` | Filings for the 6 HK-listed names (SMIC, Hua Hong, Lenovo, Alibaba, Tencent, Baidu) - English by listing requirement, no translation needed |
| CNINFO | `cninfo_filing` | `cn_disclosure` | Announcements + filing PDF links for the 16 mainland-listed names, via the officially designated disclosure site |
| UN Comtrade | `comtrade_china_trade` | `cn_trade` | **C6, import side.** Monthly IC (HS 8542) and semiconductor-equipment (HS 8486) exports *to* China as reported by the US, Japan and the Netherlands - see the C6 section below for why the partner side rather than the Chinese side |
| NBS | `nbs_ic_output` | `cn_trade` | **C6, production side.** China's own monthly IC production volume (集成电路, 亿块) and YoY change, from the National Bureau of Statistics' industrial-output release. **ECS-only** - see below |
| China press | `press_cn` | `cn_press` | ITHome (RSS), Jiemian, EEFocus, ijiwei (集微网) - filtered client-side to the universe by `native_name`/alias, matched on **headline only** |
| English check | `press_cn_english_check` | `cn_english_coverage_check` | Per-company English news search, measuring coverage lag - same shape as Japan's and Korea's |

All five policy `source_type`s share one fetcher (`_fetch_china_policy`): they differ by which
body issues the measure, not by how the page is parsed.

**Every article carries `metadata.source_outlet_type`** ∈ `{official, state_press,
commercial_press}`, set at fetch time because it is a property of the source the classifier
cannot recover from article text. A state outlet is authoritative for *what policy is* but is
**not** independent confirmation of a commercial fact - conflating the two would let policy
signalling count as corroboration of a company claim.

### Article bodies: which sources carry one, and why the others don't

Every source whose body is actually obtainable now stores it. The split is not arbitrary:

| Source | `body` | `summary` | How |
|--------|--------|-----------|-----|
| The five policy types | ✅ | — | Announcement page text, tag-stripped (`_china_strip_html`) |
| `cninfo_filing` | ✅ | — | Filing PDF via `pdfplumber`, first 5 pages |
| `hkex_filing` | ✅ | — | Filing PDF, same path - English, no translation needed |
| `press_cn` via RSS (ITHome) | — | ✅ | The feed's own summary, ~456 real characters - no body fetch needed |
| `press_cn` via HTML (Jiemian, EEFocus) | ✅ | — | `trafilatura`, with a per-outlet container fallback |
| `press_cn` via HTML (ijiwei) | — | — | Headline only - its article pages are gated |
| `comtrade_china_trade` | — | — | Numeric API - the figure IS the data; there is no document |
| `nbs_ic_output` | — | — | A single table row, same reason |
| `press_cn_english_check` | — | — | Headline-only by design, same as Japan's and Korea's |

**`summary` is a real lead or it is NULL - never a slice of `body`.** An earlier version stored
`body[:500]` as the summary on all four China sources, which duplicated a prefix of a field the
consumer already has and cut mid-sentence. The rest of this service treats the two as different
fields answering different questions (`ai_news`: 2,207 summaries averaging 211 chars against
1,830 bodies averaging 6,455), and China now follows that. A government announcement and a
filing have no editorial lead - they open straight into the measure, or into HKEX's standard
liability disclaimer, which is identical across every filing and actively worse than NULL. The
ITHome RSS path is the one China source with a genuine lead, and it stores it.

Bodies are capped at 20,000 characters and PDFs at 5 pages: page 1 carries the issuer and the
substance, later pages are signatures, appendices and audit boilerplate. Real samples ran
2.2-8.2k characters, so the cap is not binding in practice - it exists so one 300-page annual
report cannot bloat the table.

An unreadable body returns `None`, never `""`. A null reads as "not extracted"; an empty string
would read as "this filing says nothing", and the two must not be conflated - the same rule the
Japan sources follow for image-only PDFs.

**Three extraction traps, all found live:**

- **`trafilatura` returns the site NAVIGATION on some Jiemian pages.** Where it cannot find an
  article body it falls back to the longest text block, which on a newsflash page (whose body is
  a single short `<p>`) is the nav menu - a real article came back as
  "首页 科技 金融 证券 地产 汽车 健康 ...". Detected (many short labels, no sentence punctuation)
  and replaced by a per-outlet container pattern, `_CHINA_ARTICLE_CONTAINERS`. The same article
  now yields its actual 31 characters: a Tencent buyback.
- **ijiwei's article pages are gated.** Its listing is free and carries real dated headlines, but
  `/n/<id>` returns a 1.9KB stub reading 该文章未发布 (error 70002) to an anonymous reader. Those
  URLs are never fetched (`_CHINA_GATED_ARTICLE_HOSTS`), and a general marker check
  (`_CHINA_GATE_MARKERS`) catches the same shape from any other outlet. The headline is still
  collected - 盛美上海在手订单突破170亿元 同比大增88.2% is a complete C2 signal on its own.
- **Press bodies are fetched AFTER the company filter**, not before, so a listing of 50 headlines
  costs one body fetch rather than fifty - the same ordering Japan's `monoist_capex` uses.

All of this is scoped to this domain: `_fetch_china_filing_body` and
`_fetch_china_article_body` are called only from the three China fetchers, and no other domain's
extraction behaviour changes.

### Press coverage: what is reachable, and what is not

The four outlets yield ~170-250 items per run between them, of which typically 3-4 mention a
universe company. That low hit rate is correct filtering, not a bug (see the headline-matching
section below) - but the reachable surface is genuinely thin, so it was measured rather than
assumed:

- **ITHome is fetched via RSS, not its HTML listing.** Its feed carries 60 dated entries with
  real editorial summaries against 50 undated headlines from the listing page - strictly more
  data for one request, and the summary removes a per-article body fetch. The HTML listing was
  dropped rather than kept alongside it.
- **No other Chinese outlet publishes a usable feed.** Jiemian, EEFocus, Yicai, STCN, CLS, 36kr,
  Sina, Huxiu, laoyaoba, eet-china and semiinsights all return 404, an empty feed, or are
  unreachable; tmtpost's feed parses but carries zero universe coverage.
- **No outlet offers usable pagination.** `ithome.com/list/2.htm`, `jiemian.com/lists/116_2.html`
  and `eefocus.com/news/` all 404. Only page 1 of each listing exists as a server-rendered page.
- **No outlet offers a working per-company search.** ITHome's search host does not resolve,
  Jiemian's returns 404, EEFocus's returns an empty 3.7KB shell.
- **ijiwei was added for coverage** - its `/kuaixun` newsflash carries 92 dated semiconductor
  items on its own, the densest China-specific listing found. Note its homepage times out and its
  `/news` and `/n` paths return a 1.8KB shell; only `/kuaixun` is server-rendered.
- **Checked and rejected:** laoyaoba, eet-china, semiinsights (all connection timeouts), Yicai and
  STCN (captcha), 36kr and jiqizhixin (no universe coverage), sina tech (TLS failure).

**A per-company Chinese search does not exist for free.** The obvious next step - run
`press_cn_english_check`'s per-company loop against Chinese query terms - was tested and does not
work: `ddgs.news()` in Chinese returns nothing for 5 of 6 universe names across all three
backends (DuckDuckGo, Bing, Yahoo), and the one that answers returns English articles.
`ddgs.text()` returns only static reference pages - Wikipedia, Baidu Baike, Eastmoney quote
pages. Going further costs money (a paid Chinese search API) or a headless browser for the
captcha-gated outlets.

### Why matching is on the HEADLINE only, and why that is not a limitation

`_match_china_company` matches the headline, never the body. Both directions of that choice were
measured rather than assumed:

- **Widening to the body recovers nothing.** Of 167 headlines in one run, 4 matched a universe
  company and 15 more carried sector vocabulary (半导体, 国产替代, 刻蚀, 存储…) without naming
  one. Bodies were fetched for 14 of those 15: **zero** named a universe company. They are
  genuinely about other firms - Qualcomm, Huawei, Longsys, STMicroelectronics, Rockchip, UNISOC.
  The filter was right and a body-matching pass would have cost ~15 extra fetches per run to
  recover nothing.
- **Widening to the summary manufactures false positives.** Matching ITHome's RSS on
  title+summary tripled the hit count from 1 to 3 - and 2 of the 3 were wrong: an Nvidia laptop
  story matched Lenovo on a passing 联想, and an Honor OS story matched Tencent the same way.
  Only the title match was actually about its company. A company "mentioned in passing" is
  exactly what the China Signals spec's own press classifier (§9.2) calls WEAK, so widening here
  would manufacture the noise the classifier then has to reject.

So the ~3-4 matches per run is the filter working, not the filter leaking. The company-specific
record is covered independently and cannot miss by name: `cninfo_filing` queries per company
code and `hkex_filing` per issuer ID, returning an order of magnitude more rows than press.

### Dedup, and why the two trade sources key on the VALUE as well as the period

Dedup is **global on `url`** (`uq_articles_url`, a partial unique index over the whole
`articles` table) combined with `ON CONFLICT DO NOTHING` - not per source_type, not per domain,
not per run. So every China source_type needs a URL that is unique across the entire table:
`cninfo-filing://{code}/{announcementId}`, `hkex-filing://{hk_code}/{news_id}`,
`comtrade-china://{reporter}/{commodity}/{period}/{value}`, `nbs-ic-output://{period}/{value}`;
policy and press rows use the real article URL. Confirmed live: 389 China rows, **0 duplicate
URLs**, and a repeat run of the same window inserted 0 new rows.

**The two trade sources originally keyed on the period alone, and that was a real bug.** Trade
figures are revised: Comtrade serves a recent month as its own ESTIMATE and replaces it with the
reporter's filed return weeks later. Confirmed live - the 2026-07 US equipment figure comes back
`isReported: false` with `legacyEstimationFlag: 6`, i.e. Comtrade saying outright that this
number will change. With a period-only key, the revision produced the same URL as the estimate,
hit the global index and was **silently discarded** (verified by re-inserting a changed figure -
it did not land). The estimate would have been stored forever as though it were final, with
nothing marking it provisional.

Including the value in the key makes a revised figure a NEW row, so the series carries both and
a consumer can see the correction rather than having history quietly rewritten. A re-run that
reads the SAME figure still dedups, which is the behaviour that mattered originally - verified:
fetched 6, inserted 0.

Both trade sources also carry `is_reported` and `estimation_flag` in metadata, and an estimated
figure says so in its title ("... $0.082bn (estimate)"). A consumer comparing a month against its
own trailing pattern must not treat an estimate as a confirmed move.

Checked and found NOT to be a problem: whether two policy fetchers could reach the same
announcement and dedup against each other, making `metadata.issuing_body` wrong. Each ministry
publishes on its own host (mofcom.gov.cn, miit.gov.cn, samr.gov.cn, cac.gov.cn,
english.news.cn), so no URL is reachable from two fetchers - 0 cross-source_type URL collisions
across 389 rows.

### Findings that cost real debugging time

- **Government section pages are client-side shells.** MIIT's `/zwgk/zcwj/wjfb/index.html` and
  SAMR's `/xw/mtjj/` return ~2-3KB with **zero** anchors; CAC's section indexes 404. In all
  three cases the **home page** is the only server-rendered listing, and is what the seed
  points at. MOFCOM is the exception - its section pages work.
- **MIIT writes `art_<hash>.htm`, not `.html`.** One character, and the source returned zero
  rows until the link pattern was loosened to `\.html?`.
- **Xinhua uses single-quoted `href='...'`** where the government CMSs use double quotes. A
  double-only regex matched nothing but its two footer links. The pattern now backreferences
  the quote character.
- **`english.news.cn/business/index.htm` is a dead page that still returns HTTP 200** - its
  newest article is from 2021. A fetcher pointed at it would run clean forever and produce
  nothing current. The home page is used instead.
- **HKEX's `stockId` is an internal issuer id, NOT the stock code.** SMIC's code is 00981 but
  its id is 7249; Lenovo's 00992 is 2325; Alibaba's 09988 is 1000015694. Passing the code
  returns an **empty 200, not an error** - five of six companies silently returned nothing, and
  Tencent only worked by coincidence (00700 → 700 is a valid unrelated id). Ids are resolved
  per run from HKEX's own `search/prefix.do` autocomplete - see `src/china_code.py` below.
- **cninfo's `stock` param must be `"CODE,ORGID"`.** A bare code returns zero rows with
  `totalAnnouncement=0` - again no error, so it looks exactly like a company that filed
  nothing. `column` must also be `szse` for 000/002/300 codes and `sse` for 600/603/688; the
  wrong one likewise returns zero rows silently (derived from the code prefix, not stored).

### `src/china_financials.py` + `company_financials` - structured revenue, not PDF text

C2 (substitution progress) compares a company's revenue growth against **its own** past
growth, so it needs a clean series per reporting period. Those were recovered by parsing
filing PDFs, which works but fails invisibly rather than loudly:

- the largest number in a document is not always the company's own figure (a GigaDevice
  filing's biggest amount was the VALUATION of the company it was investing in)
- periodic reports lose their table headers to PDF extraction, so a row sitting intact
  below a shuffled header is never read
- some issuers state growth qualitatively - BOE's pre-announcements say
  营业收入同比增长超10% ("grew by MORE than 10%"), which carries no figure at all

All three disappear with a structured source. Confirmed against the PDF-extracted series:
cninfo's own API returns +121.83 / +17.30 / +52.40 / +56.92 for Hygon's four annual
periods, exactly what extraction produced - and additionally covers five companies
extraction could not read at all. C2's trusted baselines went from 28 to 66.

| Source | Covers | Periods |
|--------|--------|---------|
| cninfo `data20/financialData/getIncomeStatement` | the 16 mainland-listed names | FY, H1, Q1, Q3 |
| Alpha Vantage `INCOME_STATEMENT` | Alibaba (BABA), Baidu (BIDU) - they file with the SEC | annual only |
| (neither) | Tencent, Lenovo - OTC ADRs on neither cninfo nor the SEC | stay on PDF extraction |

Both sources return **identical keys and types** (`code`, `period_type`, `year`, `revenue`,
`prior_revenue`, `yoy_pct`, `source`), so a consumer cannot depend on which one answered.
Alpha Vantage quarterly data is deliberately unused: Alibaba's fiscal year ends 31 March,
so its quarters do not line up with a mainland company's.

The cninfo endpoint is **undocumented** - `sign=1` is required and non-obvious (without it
the endpoint answers HTTP 500 with `{"msg":"validate fail!"}`), and it could change without
notice, which is the other reason the PDF path is kept rather than deleted. An HK-only code
is never sent to it: cninfo answers HTTP 500 for a five-digit code, which would log a
failure for something working exactly as intended.

**Stored in `company_financials`, not `articles`.** A revenue series is reference data, not
news: it has no url, no publication event, and a daily classify pass cannot make twenty
external calls every run. The table **UPSERTS** on
`(domain, code, metric, period_type, period_year)` - the opposite of this service's usual
append - because a restated figure must REPLACE its predecessor. Accumulating both would
put one period into its own baseline twice and narrow the spread around whichever period a
company restated most.

`GET /market/china/revenue-history` serves the stored rows (~0.3s). `?refresh=true`
re-fetches upstream first (~90s, one request per company) and `?codes=688041,002371` narrows
that re-fetch to named companies - what makes a daily refresh affordable, since a company's
figures change only when it files. The narrowing applies to the FETCH only; the response is
always the full stored history, because per-company baselines are built for the whole
universe regardless of who filed today.

### `src/china_code.py` - the two exchange ids are resolved, not stored

`org_id` and `hkex_stock_id` were hardcoded in `CHINA_TICKER_UNIVERSE` until 2026-10-06.
They are now looked up at run time, the same pattern Japan (`edinet_code.py`), Korea
(`dart_corp_code.py`) and the US (SEC `company_tickers.json`) already use:

| Source | Endpoint | Shape |
|--------|----------|-------|
| cninfo `orgId` | `new/data/szse_stock.json` | one bulk fetch, 6,259 rows, both exchanges despite the name |
| HKEX `stockId` | `search/prefix.do` | one request per code (no bulk list exists) |

Both cached for the process lifetime, and both fail-open: a failure caches an empty map and
the fetcher degrades to the seeded config rather than raising, the same trade-off
`edinet_code.py` documents.

**Why this matters as the universe grows:** adding a company now needs only its `code` (and
`hk_code` if HK-listed). Verified live against codes that were never hardcoded - Xiaomi
01810 → 190371, JD 09618 → 1000042149 - and against all 22 previously hardcoded values,
which resolved identically (16/16 orgIds, 6/6 stockIds). A hand-copied id is one
transcription error away from a source that silently returns nothing.

Two traps the resolver encodes, both found live:
- **`prefix.do` requires a `callback` parameter.** Without it the endpoint answers HTTP 200
  with an empty body, not JSON.
- **It is a PREFIX search.** Querying `00981` returns 5 entries (09810, 09812...). The
  resolver matches on exact code, never first-hit - taking the first works today but is the
  API's ordering choice, not a guarantee, and this domain has already been bitten once by an
  identifier that looked right.

`exchange` was removed from `CHINA_TICKER_UNIVERSE` at the same time: nothing read it.
- **Beijing midnight converts to the previous day in UTC.** These sources give a date with no
  time; stamping it at 00:00 CST and converting put a filing dated 2026-09-18 at
  2026-09-17T16:00Z, outside a `days_back` window that should have included it. `_china_day()`
  stamps midday instead, which survives the conversion in either direction.
- **Short English names return the wrong company.** "Amec" returned a Spanish article about the
  UK engineering firm; "Piotech" returned the German makers PVA TePla and SUSS MicroTec. Nine
  companies carry a `search_query` override in `_CHINA_ENGLISH_CHECK_QUERY_OVERRIDES` - same
  mechanism Japan uses for Disco and Towa.
- **SAMR puts no date in its listing at all.** All 59 of its rows came back with a null
  `published` on the first full run. An undated row can never be excluded by `days_back`, so it
  would be re-fetched on every run forever. Its article bodies carry `发布时间：2026-07-27`,
  now matched by `_CN_PUBLISH_TIME_RE`; SAMR went from 0/59 dated to 48/51. The document's own
  【发文日期】 still wins where present, since a stated document date outranks a CMS timestamp.
- **ITHome stamps rows with `data-ot` ISO timestamps; Jiemian and EEFocus carry none.** Those
  two are left null rather than guessed - a wrong date is worse than an absent one, and
  `expire-articles` never deletes a null-dated row, which is the safe direction.

**Live run (2026-10-05, `days_back=35`):** 262 articles - `cninfo_filing` 119,
`hkex_filing` 74, `press_cn_english_check` 36, `miit_policy` 18, `comtrade_china_trade` 6,
`cn_state_press` 4, `mofcom_policy` 3, `cac_review` 2. All 9 source_types producing, all three
`source_outlet_type` values present, zero rows missing required keys, zero duplicate URLs.
Persistence verified separately: 284 rows written to `articles`, and a second run of the same
window inserted 0 new rows - confirming the global `uq_articles_url` dedup covers the synthetic
`cninfo-filing://`, `hkex-filing://` and `comtrade-china://` schemes. After the date fixes
above, 100% of fetched rows carry a `published` date (was 211/299).

**Trigger** the same way as any other domain - no domain-specific route or CLI command:
```bash
python __main__.py trigger --domain china_market_signal --days-back 14
```

### Schedules

Three CloudWatch rules in `infra/modules/ecs_cluster/services.tf`. Each runs the whole domain -
`trigger` is domain-scoped with no per-source_type flag - so the rules differ only in cadence
and `--days-back`. The overlap is harmless (global URL dedup) and costs a few extra listing
walks a day.

| Rule | Schedule (UTC) | `--days-back` | Exists for |
|------|----------------|---------------|------------|
| `..._china_market_signal_policy` | `cron(0 0-12/4 * * ? *)` | 1 | Policy + press. 08:00-20:00 Beijing, every 4h, **every day** |
| `..._china_market_signal_filings` | `cron(0 1,8 * * ? *)` | 1 | Pre-open (01:00) and post-close (08:00) against the 01:30-07:00 UTC mainland session, **every day** |
| `..._china_market_signal_monthly` | `cron(0 5 20 * ? *)` | 35 | The **only** rule that satisfies the `monthly` gate, so the only one that ever runs `comtrade_china_trade` and `nbs_ic_output` |

Two more rules in the same file belong to **signal-detection-agent**, not this service, and
classify what these three fetch - without them the domain would fetch on all three schedules
and classify on none, leaving articles to accumulate unjudged:

| Rule | Schedule (UTC) | Covers |
|------|----------------|--------|
| `..._china_signals_filings` | `cron(0 10 * * ? *)` | 2h after the 08:00 post-close filings fetch; also picks up that morning's policy run |
| `..._china_signals_evening` | `cron(0 14 * * ? *)` | 22:00 Beijing, after the 12:00 fetch that ends the 4-hourly policy series; also the pass that picks up the monthly trade rows |

Both run `classify-china-signals`, which defaults to today (UTC) and skips already-classified
`source_id`s, so the two passes are additive and idempotent rather than duplicative.

**Why the daily rules run every day, and why their window is 1.** Both were `MON-FRI` with a
2- and 3-day window until the stored history was checked. Chinese issuers file at weekends:
of five years of stored articles, **1,715 cninfo filings carry a Saturday publication date**,
against 1,830 on Friday and 1,795 on Tuesday (Sunday is genuinely quiet at 93). That is not a
timezone artifact - `_china_day()` stamps midday UTC specifically so a date survives the
conversion in either direction.

So the weekend coverage was real but was being carried by the WINDOW rather than the
schedule: `--days-back 3` existed to let Monday's run reach back over a weekend, which made
every weekday run re-walk three days to serve one. Running daily lets every window be 1.

It also closed a hole on the classify side, which mattered more: the classify window selects
RUNS by date, so a Saturday fetch under a `MON-FRI` classify schedule was never judged at all
- Monday's pass pools Monday's runs, not the weekend's. Those rows were orphaned permanently,
not merely delayed.

Widening the window was never the cost: cninfo and HKEX are both date-RANGE queries sending
`seDate=start~today` in a single request per company, so `days_back` 1, 3 and 30 all take the
same time (measured). This is the opposite of Japan, whose EDINET fetcher loops per day and is
why that domain keeps its window at 1 for the other reason.

- **35 for monthly** - `_fetch_comtrade_china_trade` derives how many months it walks back from
  `days_back` (`max(4, days_back//30 + 3)`), and Comtrade's own 2-3 month publication lag means
  a narrow window finds nothing at all.

The 20th of the month is chosen for the monthly rule because NBS publishes the previous month's
industrial-output release around the 15th (confirmed: the August 2026 figure landed 15
September). Comtrade has no fixed release day and is indifferent to the date.

Unlike Taiwan's TWSE/TPEx feeds, **no China source is an always-latest snapshot** - cninfo is
date-scoped, HKEX takes a from/to range, Comtrade is keyed by period, and the government
listings carry weeks of history on page 1. A missed window is recoverable on the next run
rather than a permanent gap; these schedules exist for freshness, not for capture.

**`nbs_ic_output` only produces data on these scheduled runs**, never on a local one -
stats.gov.cn is network-blocked from dev machines but reachable from ECS. See the C6 section.

No backfill is needed before enabling these, unlike Japan: Comtrade walks back months on its
own and NBS publishes monthly.

### Sources: `geopolitical_news`

**Two sources, `federal_register` and `eventregistry`.** GDELT (`gdelt`) and SerpAPI Google
News (`google_news`) were removed from this domain; GDELT remains in use by
`taiwan_market_signal`, which is a different source row and unaffected.

Event Registry (newsapi.ai, `EVENT_REGISTRY_API_KEY`) replaces both and returns more per
article than either did: the body itself (no separate Trafilatura fetch), its own scored
`concepts`, a `sentiment` score, an article `uri`, and an `eventUri` cluster id that
signal-detection-agent's Stage D reads to group articles reporting one event.

Two things about the body are worth knowing. It is Event Registry's own crawl of the
publisher's page, not ours, so a page that renders oddly can produce prose-shaped noise: one
article in 325 arrived column-interleaved and was stored as NULL rather than as 5,220
characters of garbage (`_er_body_or_none`, a stopword-rate check that only applies above 400
characters). And title-similarity dedup drops near-duplicates before storage — 153 of 481
articles in the 2026-10-16 run — recording each dropped article as a
`{outlet, title, url}` entry in the survivor's `metadata.also_reported_by`, which is the only
record it ever existed.

Source rows are seeded `ON CONFLICT (url) DO NOTHING`, so changing a query list in
`src/seed.py` does **not** retroactively update an already-seeded database; an existing row
must be updated directly (`UPDATE sources SET config = ...`) for the change to take effect.

## Middle East Market Signal (`middle_east_market_signal` domain)

Fetch-and-store only, same boundary as the four Asian market domains. Built from the
"Middle East Market Signals for the AI Economy" spec (Oct 2026). Two rules shaped every
source choice: **data comes from an API or structured JSON, never PDF extraction**, and
**every stored row has a body** - a row whose text cannot be reached is dropped, not
stored headline-only.

Company universe: `MIDDLE_EAST_TICKER_UNIVERSE` in `src/seed.py`, the spec's section-4
companies and funds (25 rows - ADIA and ADQ are separate funds), stored in the same shape
as Taiwan's `TAIWAN_TICKER_UNIVERSE` pending research-universe, which holds neither the
unlisted Gulf names nor their US counterparties yet. Fields: `ticker` / `exchange` (the
company's OWN listing; `None` = unlisted), `company`, `country`, `aliases` (matching; a
common-word name is stored in full - "Elm Company", "Nova Ltd"), and `counterparties` - the
US companies the spec says its news moves, each `counterparty_name`, `counterparty_ticker`,
`relationship` (one sentence, role then why, citing the spec section) and
`share_price_on_good_news` (`rise` / `fall`, the spec's arrow - untested against prices).
`[]` means the spec names a group but no US company; no ticker is invented. 50 links over
18 companies. It supplies the WAM/SPA/BIS match terms, and is served unchanged at
`GET /market/middle-east/universe`. **Counterparties are NOT copied onto articles at fetch
time** (unlike Taiwan's `metadata.customers`): a Taiwan source is scoped to one company, but
a Middle East row is a wire or press story that can name a company in passing, so which
companies it is about is the classifier's decision. signal-detection-agent reads the table
once per run and attaches counterparties only for companies it verified the article is
about - and reads it live, so a corrected link is never frozen onto stored rows. The US
names' prices are read by ticker from `/market/*`.

| Source | `source_type` | Route | Body |
|--------|---------------|-------|------|
| WAM (UAE news agency, English) | `wam_wire` | `GET wam.ae/api/app/views/GetViewByUrl?url=en/list/latest-news`, further pages `POST .../GetSectionArticlesFDto` with the section's paging object | `GET .../articles/GetArticleBySlug?slug={urlSlug}` |
| SPA (Saudi Press Agency, English) | `spa_wire` | `GET portalapi.spa.gov.sa/api/v1/news?page=N` (20/page) | `GET .../news/{uuid}` -> `content` |
| Middle East AI News | `substack_post` | Substack `GET /api/v1/archive?sort=new&offset=N` | `GET /api/v1/posts/{slug}` -> `body_html` |
| BIS press releases | `bis_press` | `__NEXT_DATA__` JSON on `bis.gov/news-updates` (whole archive, one request) | inline body, else DOJ press API matched by title + date |
| Federal Register (BIS, Gulf terms) | `federal_register` | existing fetcher, `config.terms` + `config.url_scheme` | govinfo / raw text |
| SEC 13D/13G (PIF, Mubadala, ADIA, QIA) | `sec_fund_stake` | `data.sec.gov/submissions` | `primary_doc.xml`: cover facts + narrative items |
| newsapi.ai, 5 queries | `eventregistry` | existing fetcher, `config.query` | with the article |

WAM and SPA publish everything (weather, sport, condolence cables), so items are matched
against `match_terms` - universe names plus the spec's M1-M8 topic terms - **before** the
body fetch, the same order Japan and China use. Matching is word-bounded on "not a word
character or &", so "AI" does not match inside "Dubai" and "e&" matches on its own.

**Every source was probed live (2026-10-19), from a local machine and from ECS:**
- **WAM's site is an Angular shell** and newsapi.ai's copy of WAM English was ~21 days
  stale (WAM Arabic is current there), so WAM is read from the API its own site calls.
  `GetArticleBySlug` returns 204 for the bare `slug`; it keys on `urlSlug`
  (`{shortCode}-{slug}`). The paging POST rejects the paging object inside any envelope.
- **SPA's homepage data holds 7 items**; the list behind it is the portal API, which
  answers without auth. SPA is not indexed by newsapi.ai at all.
- **BIS links to DOJ pages that have since moved** - one BIS link 404s while the DOJ API
  serves the same release under a different slug. Matching on url found 77/130 linked
  releases; normalized title + a 14-day date window finds 122/130. The 14 releases whose
  only text is a scanned settlement PDF have no API route and are skipped.
- **13F was dropped.** It arrives up to 45 days after quarter end, and PIF's latest lists
  five stocks, none in the AI universe. 13D/13G arrive within days of a stake change, and
  the XML schema (mandatory since Dec 2024) carries the narrative - a 13D's item 4,
  "purpose of transaction", ran 1,700 characters. 13D and 13G spell the same facts
  differently (`percentOfClass`/`classPercent`, ...); `_SEC_STAKE_FIELDS` maps both.
- **Federal Register terms match Russian-airline denial orders** (they mention flights to
  the UAE) - about one a month. Kept: a denial order naming a Gulf company (M8) has the same
  form, so excluding them would drop real signals.

**newsapi.ai queries** (`eventregistry:me_*`). Each pairs an entity or place with a topic
term - an entity alone was measured to match astronomy ("helium"), Jio's IPO anchor book
(the Gulf funds) and Nvidia's MGX server platform ("MGX"). G42, Humain, MGX and Stargate UAE
have no usable Event Registry concept (G42 resolves to a Chinese expressway), so Gulf AI
news comes from the Gulf-outlet query (12 Gulf outlets, AI/energy headlines) plus WAM, SPA
and Middle East AI News. The free plan rejects a query with more than 15 keywords, counting
every word of a phrase. Measured volumes over a day: shipping ~100, Gulf outlets ~10,
QatarEnergy ~5, Israel sites ~5, helium ~3. The chip-exports-to-the-Gulf query returned 0,
so M1/M8 come from BIS, the Federal Register and DOJ.

**Own url schemes, because dedup is global.** `geopolitical_news` already fetches every
BIS Federal Register document and runs a "Strait of Hormuz" query; 28 of 100 shipping
articles were measured in both. Under the global `uq_articles_url` index the second domain
to fetch an article would silently lose it, so this domain stores
`me-federal-register://{document_number}` and `er-me://{uri}`, with the real link in
`metadata.html_url` / `metadata.article_url`. The two shared fetchers gained four optional
config keys for this, all defaulting to the old behaviour: `url_scheme`, `query` (a whole
Event Registry condition), `require_body`, `max_pages` (Event Registry, one token per page).

**Title-similarity dedup, same settings as `geopolitical_news`** (168h window; three
bands - at or above 0.82 same story outright, below `_SAME_EVENT_SIMILARITY_FLOOR` (0.50)
definitely not, and only the band between them costs an LLM same-event call;
near-duplicates recorded in the survivor's `metadata.also_reported_by` as
`{outlet, title, url}`). The 0.50 floor is measured, not round: the lowest similarity
between two confirmed-same-story headlines in real `geopolitical_news` data was 0.5951
(two framings of one September 2026 EU Council decision), and the floor sits just below
it. Widening the band downward from 0.72 was checked against real pairs first - every
missed US-Japan duplicate already sat inside the band at 0.62-0.80, while three correct
Strait of Hormuz separations sit at 0.41-0.48 and would have been wrongly merged by a
lower auto-threshold. The war is re-reported
across hundreds of outlets: the first run with it on went 148 -> 65 rows, one Hormuz mine
strike carrying 31 other outlets. Official and filing rows (`source_category`
`me_official_wire`, `me_us_policy`, `me_fund_stake`) are excluded from both sides of the
comparison - whichever copy is stored first survives, so a WAM release stored after the
press covered it would otherwise be dropped as the duplicate. `also_reported_by` reads the
publisher's link from `metadata.article_url` for `er-me://` rows (`_publisher_url`), and
`append_also_reported_by`'s own-outlet guard does the same in SQL; before that change an
outlet would have been recorded as "9509743911".

**Two domain-level guards in `run()`** (`_BODY_REQUIRED_DOMAINS`), applied to every source
after fetch: a row with no body is dropped (every source here already reads its body from
an API; this is the backstop for any source added later), and so is a row published more than `days_back` + 1 day
ago - Event Registry selects on crawl date, and measured late crawls brought in a 6-day-old
story on a 1-day run and a July story on a 30-day run. A failed later listing page (WAM,
SPA) stops paging and keeps the pages already read.

**End-to-end (local, 2026-10-19):** a 30-day run stored 369 rows from 641 fetched; two
1-day runs after it stored 5 and 3 (repeats of the window, no duplicate urls). All 377
rows checked: every core column and body filled, every source's metadata keys present, no
numeric or `er-me://` outlet in `also_reported_by`. Values were compared against each live
source (title, date, body, plus issuer/percent/form for SEC, `effective_on` for the
Federal Register) with no mismatch. Steady-state daily volume after dedup: ~50-60 rows -
newsapi.ai the bulk (shipping), WAM ~5, SPA ~5, Middle East AI News ~3; SEC ~1 a week,
Federal Register ~1 a month, BIS Gulf releases a few a year.

**Metadata is kept to fields with a use** (anything already in the url, the body or
another column is left out):
- every Middle East row: `source_type`, `source_category` (the dedup exclusion keys on it),
  `source_outlet_type` (`state_press` / `official` - the spec treats state outlets as proof
  of what a government said, never of a business fact)
- `bis_press`: `country_tags` (why it was kept), `topics` (Enforcement = M8, Regulations =
  M1), `body_source` (`bis` / `doj`), `external_link` (the link BIS published, which can be
  stale) and `doj_url` (the DOJ release actually read)
- `sec_fund_stake`: the url `sec-stake://{cik}/{accession}` is only a dedup key, so every
  value it encodes is also its own field - `fund_cik`, `accession_number`, `filing_date` -
  alongside `fund`, `form` (13D active / 13G passive), `filing_url`, `issuer_name`,
  `issuer_cik` (maps to a ticker), `issuer_cusip`, `event_date`, `percent_of_class`,
  `shares_owned` (the M3 >20% change rule) and `reporting_persons`
- `federal_register` copy: adds `effective_on`, `html_url`
- `eventregistry`: the shared field set, plus `article_url` and `er_query` (which M-type
  query found it)

**Full bodies, not just non-empty ones.** Checked per source (2026-10-19):
- **Federal Register:** the shared fetcher caps a body at 20,000 characters (geopolitical
  reads only the first 2,000). This source sets `body_max_chars: null`. The UAE A:5 rule is
  32,904 characters, and three Russian-airline orders of 21.5-21.8k had been cut. Every
  stored document now ends with its `[FR Doc. ... BILLING CODE]` end marker.
- **newsapi.ai:** some bodies are only part of the article. `_er_body_is_partial` drops
  (under `require_body`) a body ending in "..." (a paywall teaser - Bloomberg 3/3, Telegraph
  3/3, Bloomberg Law 2/2 over 871 rows), a video/podcast page (a description, 8/871), and
  the outlets whose body is only ever an opening (`_ER_PARTIAL_BODY_OUTLETS` - Reuters' first
  paragraph plus credits, The Africa Report's teaser). The repo's page-fetch tiers were
  tried and do not recover them: Reuters returned nothing, Telegraph only its navigation.
  A bracketed "[...]" ending counts as a teaser too. Plus a floor: every one of 851 stored
  bodies under 300 characters was partial (a video series' standing blurb, The National's
  "Manus on Markets" at 263; teasers) and the shortest complete one was a 310-character
  liveblog entry, so `_ER_MIN_FULL_BODY_CHARS = 300`. A title-word-overlap test was tried and
  rejected: the series blurb scored inside the range of real market wraps.
- **Substack:** stored body equals the API's `body_html` on 58/58 rows. Substack answers
  429 to bursts (19 of 68 posts lost with 5 parallel requests), so posts are fetched one at
  a time, 0.5s apart, and a 429 is retried after its `Retry-After` - the next 30-day run
  stored 68/68.
- **SPA:** bodies end with the agency sign-off ("-- SPA 16:24 Local Time ..."), i.e. whole.
- **SEC 13D/13G:** the body is the filing's own cover facts and narrative items; exhibits
  (agreements filed alongside) are separate documents and not included.

**Middle East AI News through Substack's API, not its RSS feed.** The feed's body is its
content field, and when that is short the shared RSS path falls back to a page fetch that
can fail and leave no body. Substack's own API gives every post's `body_html` directly.
Only `type: newsletter` with `audience: everyone` is kept: `podcast` posts are audio
episodes whose text is show notes repeating a written post under another title ("Only one
in ten UAE firms Agentic AI ready" vs the article "Only one in 10 UAE organisations feels
ready for agentic AI"), and a paid post's `body_html` is truncated. The archive listing
carries no body, so each kept post costs one `/posts/{slug}` request.

**Dropped after probing:** IMF PortWatch (data 8 days behind), UKMTO (incident list loads
from an API that could not be found), Lloyd's war-risk list (PDF only), ADX (PDF
disclosures), Tadawul and Israel's Home Front Command (Access Denied from ECS too), QNA
(newsapi.ai indexes its English feed within minutes), EIA/Alpha Vantage commodity series (3
trading days behind - BNO's quote is used instead), Congress.gov, USGS, Suez Canal Authority.

**Token budget:** 6 newsapi.ai tokens a run (shipping takes 2 pages). Twice a day is ~360 a
month, against a 2,000-token plan that `geopolitical_news` also draws on.

**Trigger:**
```bash
python __main__.py trigger --domain middle_east_market_signal --days-back 1
```
First local run (2026-10-19, `--days-back 1`): 121 rows - 108 newsapi.ai, 5 SPA, 4 WAM,
4 Middle East AI News - every one with a body.

**Needs before deploy:** `EVENT_REGISTRY_API_KEY` is not in the news-retrieval task
definition or Terraform yet (also true for `geopolitical_news`), and the domain has no
CloudWatch schedule yet (planned: twice a day).

## Guidance
- Read only the docs relevant to your task - not all of them
- Check the index above before asking for clarification; the answer is often in a doc
- When in doubt about scope or requirements, read the Functional Requirements or PRD first
- Use the Jira board (project key `CON`) to track and reference cards

## Maintenance
- Do not modify the Documentation Index, Jira Board, Guidance, or Maintenance sections unless explicitly asked
