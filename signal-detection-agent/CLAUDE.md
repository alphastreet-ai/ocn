# signal-detection-agent

Part of the [ocn monorepo](../CLAUDE.md).

## Overview

`signal-detection-agent` is a FastAPI service (port 8003) that classifies news articles as
**signal**, **weak_signal**, or **noise** using an LLM-driven pipeline. Unlike `signal-detection`
(which uses vector similarity + corpus centroids), this service sends each article's text directly
to an LLM and gets back a structured classification with a score, reason, materiality, category,
and named entities.

Articles are fetched from `news-retrieval`. The service can trigger a fresh run or reuse the
latest completed run (useful for testing). Classification results are stored locally; full article
content remains in news-retrieval.

## Jira Board
| Board | URL | Project Key |
|-------|-----|-------------|
| OCN Board | https://opengrowthventures.atlassian.net/jira/software/projects/CON/boards/34 | CON |

## Structure

See [STRUCTURE.md](STRUCTURE.md) for descriptions.

```
signal-detection-agent/
├── Dockerfile
├── requirements.txt
├── requirements-test.txt
├── pyproject.toml
├── CLAUDE.md
├── STRUCTURE.md
├── prompts/
│   ├── ai_universe_signal_classifier_v1.txt
│   ├── ai_universe_signal_classifier_v2_refine.txt
│   ├── geopolitical_signal_stage_b_v1.txt   (talk vs action; the irreversible verdict)
│   ├── middle_east_signal_extract_v1.txt   (facts only - code decides the label)
│   └── geopolitical_signal_stage_c_v1.txt   (channel definitions + precedence, assets, category)
├── src/
│   ├── __main__.py
│   ├── app.py
│   ├── auth.py
│   ├── config.py
│   ├── db.py
│   ├── seed.py
│   ├── routes/
│   │   ├── health.py
│   │   ├── run.py
│   │   ├── jobs.py
│   │   └── webhooks.py   (news-retrieval run-completed callback -> geopolitical_signal pipeline)
│   ├── controllers/
│   │   └── run.py
│   ├── models/
│   │   ├── jobs.py
│   │   └── market_baselines.py   (per-company C2/C3 baselines, written by refresh, read by classify)
│   ├── pipeline/
│   │   ├── classifier.py
│   │   ├── category_candidates.py   (parked - not wired in v1)
│   │   ├── taiwan_signal_classifier.py   (taiwan_market_signal: rank/clause-lookup/translate/classify)
│   │   ├── japan_companies.py        (japan_market_signal: the one static company/customer table)
│   │   ├── japan_signal_classifier.py   (japan_market_signal: J1-J7 + WATCHING)
│   │   ├── japan_signal_view.py      (japan_market_signal: row -> card shaping)
│   │   ├── china_companies.py        (china_market_signal: the one static company table - signal_roles + read-through)
│   │   ├── china_signal_classifier.py   (china_market_signal: Gate 1 + C1-C7, read-through, confirmation, translate)
│   │   ├── middle_east_signal_classifier.py   (middle_east_market_signal: M1-M8 extraction, spec label rules, read-through, corroboration, stories)
│   │   ├── geopolitical_signal_classifier.py  (geopolitical_signal Stage A: rule-based noise removal, no model call)
│   │   ├── geopolitical_signal_layer1.py      (geopolitical_signal Stage A: rule-based row creation)
│   │   ├── geopolitical_signal_overrides.py   (geopolitical_signal Stage A: source-specific overrides)
│   │   ├── geopolitical_signal_stage_b.py     (geopolitical_signal Stage B: talk vs action - the irreversible verdict)
│   │   ├── geopolitical_signal_layer2.py      (geopolitical_signal Stage C: channel/actors/assets/category/summary)
│   │   ├── geopolitical_signal_stage_d.py     (geopolitical_signal Stage D: corroboration grading, no model call)
│   │   └── geopolitical_signal_stage_e.py     (geopolitical_signal Stage E: story grouping, no model call)
│   └── adapters/
│       ├── news_client.py
│       └── web_search.py
└── tests/
    ├── conftest.py
    ├── test_caching.py
    └── test_smoke.py
```

## Key Environment Variables

| Variable | Purpose |
|----------|---------|
| `OPENAI_API_KEY` | LLM API key (OpenAI or OpenRouter) |
| `OPENAI_BASE_URL` | LLM base URL (default: `https://api.openai.com/v1`) |
| `SIGNAL_DETECTION_MODEL` / `OPENAI_MODEL` | Base pass model (default: `gpt-4.1`) |
| `SIGNAL_DETECTION_MODEL_V2` | Second pass model — falls back to `SIGNAL_DETECTION_MODEL` if unset |
| `NEWS_RETRIEVAL_URL` | news-retrieval base URL (default: `http://news-retrieval:8000`) |
| `POSTGRES_HOST/PORT/DB/USER/PASSWORD` | Signal-detection Postgres DB connection |
| `PIPELINE_POLL_TIMEOUT_SECS` | Max seconds to wait for a news-retrieval run (default: 1800) |
| `WEB_SEARCH_PROVIDER` | Web search backend: `duckduckgo` (default), `tavily`, `brave` |
| `WEB_SEARCH_API_KEY` | API key for Tavily or Brave (not required for DuckDuckGo) |
| `CLASSIFY_CONCURRENCY` | Max concurrent article classifiers (default: 8) — shared by the `news` domain and geopolitical_signal Stage B/C |
| `TAIWAN_CLASSIFY_CONCURRENCY` | Max concurrent Taiwan translate/GDELT-relevance calls (default: 5) |
| `TAIWAN_SIGNAL_DOMAIN` | news-retrieval domain slug for the Taiwan pipeline (default: `taiwan_market_signal`) |

## Taiwan Signal Pipeline

`taiwan_market_signal` (TWSE/TPEx revenue + material announcements, plus GDELT Taiwan
coverage) is classified by a separate path from the AI-universe `POST /run` pipeline above
- see [STRUCTURE.md](STRUCTURE.md) for the full flow. Entry point: `python -m src
classify-taiwan-signals` (Click command in `__main__.py`), scheduled twice daily (14:00 UTC
post-Asia-close, 21:00 UTC pre-US-open, Mon-Fri) via the `signal_detection_agent_taiwan_signals`
CloudWatch rule in `infra/modules/ecs_cluster/services.tf`. news-retrieval fetches this domain
on its own independent 4-hourly schedule and stops at fetch/dedup; all revenue scoring,
clause-code lookup, translation (`OPENAI_MODEL_V2`), and LLM classification (gdelt only) happen in
`pipeline/taiwan_signal_classifier.py`. Results persist to the same `agent_classifications`
table as the AI-universe pipeline, distinguished by `source_type = 'taiwan_market_signal'`,
with Taiwan-specific fields (revenue scores, clause reason, translated text) in the `metadata JSONB`
column. Dedup across the two daily runs (and across news-retrieval's ~4-6 polls per day) is by
a deterministic `source_id` (ticker+period, ticker+timestamp, or url), not article row id,
enforced by a partial unique index. Already-stored items are dropped BEFORE classifying, so
nothing is re-translated, and stored rows are never changed after insert.

### Revenue signal (`mops_revenue`)

Each monthly filing is judged against **that company's own** past 36 months (min 24), never
against the other companies - the old top-3-by-YoY ranking was worse than random in the
backtest and always produced exactly three HIGHs. Two scores, each `(value - median) /
(MAD x 1.4826)` on log changes:

- `mom_deviation` - this month vs last month
- `yoy_change_deviation` - this month's YoY minus last month's YoY (YoY from each filing's own
  prior-year figure, so restatements never show as a jump)

`revenue_deviation = (mom_deviation + yoy_change_deviation) / 2 / 0.87` (0.87 restores the scale of a
single score, since the two correlate at 0.52). `|score| >= 2.5` HIGH, `>= 1.5` WEAK, else
NOISE; **direction is the sign of `revenue_deviation`** (not stored separately). Named `deviation`, not
`surprise` or `signal_score`: "surprise" means vs analyst consensus in finance, and `signal_score`
is a column with a different meaning (a model's 0-1 confidence, `NUMERIC(5,4)`, max 9.9999).

Backtest, 2021-07..2026-06, 1,000 scored company-months, point-in-time: HIGH up moved +3.9%
and HIGH down -4.4% vs the other tracked companies by the 11th (t = +2.5 / -3.8); next-quarter
growth improved 82% / 24% of the time vs 54% normally; same pattern in 2021-23 and 2024-26;
HIGH fires a median of once a month (16 of 50 months had none). There is no edge after the
11th - the market prices it by then - so speed matters. Rejected after testing: YoY level
(+0.08 correlation), seasonal adjustment, 3-month acceleration, standard deviation instead of
MAD (65% fewer signals, downside lost), US read-through (no relationship).

Stored per row: `signal_reason` = one or two plain sentences built by template from the row's
own numbers (`revenue_reason`, no model call), e.g. "2026-09 revenue fell 31.0% from 2026-08;
year on year it was +196.2%, down from +344.4% in 2026-08. That is far outside King Slide's
usual month-to-month swings: a strong downward break from its own pattern." Unscored rows (all
NOISE) say why in a sentence: Jan/Feb (Lunar New Year - the score predicted nothing there),
short history, flat history or a bad value; the scorer's internal codes
(`not_scored_lunar_new_year` etc.) are only counted in the run log. `metadata`: `revenue_deviation`, `mom_deviation`,
`yoy_change_deviation` (only when scored - absent means not measured), `prior_yoy_pct` (last
month's YoY, for "slowed from +177% to +77%"), `is_12m_high` (revenue above each of the previous
12 months - a tag, not a signal), `is_repeated_remark_12m` (the company gave the same explanation,
ignoring whitespace, in any of its previous 12 months - 66% of 2019-26 remarks do, vs 58% caught
by a last-month-only check; absent when there is no explanation; reads stored rows, so it sees
less than 12 months back until the pipeline has run a year). History comes
from news-retrieval's `GET /market/taiwan/revenue-history`; if it cannot be read, revenue is
skipped for that run rather than stored as unscorable. Each distinct company remark is
translated once (stored translations reused; identical texts in a batch share one call).

### Customers (`GET /taiwan-signals/universe`)

The company's verified customers, `[{name, relationship}]`, are served per company by
`GET /taiwan-signals/universe` (`ticker`, `company_name`, `translated_company_name`, `customers`),
read live from news-retrieval's `GET /market/taiwan/universe` (`TAIWAN_TICKER_UNIVERSE` in
`news-retrieval/src/seed.py` - the one source of truth). `[]` means checked and none verified; 503
if news-retrieval is unreachable. A consumer joins on `metadata.ticker`. Customers are not stored on
rows and no classification step reads them; rows classified before 2026-10-22 may still carry a
`metadata.customers` copy, which nothing reads.

### Material announcements (`mops_material`): clause meaning

Every announcement stores `clause_label`, the plain-English meaning of its clause
(`disclosure_clause_code`, e.g. `第20款` -> "Asset purchase or sale"), looked up per
**exchange** in `taiwan_signal_classifier.py`: TWSE has 51 clauses (official procedure
FL007111, 2026-09-30); TPEx has 53 - the same as TWSE through 46 plus two TPEx-only clauses
(47 withdrawn move to TWSE, 50 annual report correction), so TPEx 48-53 = TWSE 47-51 and the
catch-all is TWSE 51 / TPEx 53. TPEx 39, 42 and 44 are not in TPEx's published documents and
get no label until confirmed; an unknown clause gets no label and a warning, never a guess.
It lives here, not in news-retrieval, because a clause's meaning and its rating belong in
one place, and because a lookup here can label any stored row without re-fetching.

**Rating and summary** (`pipeline/taiwan_announcement_classifier.py`, prompt
`prompts/taiwan_announcement_classifier_v1.txt`, model `SEC_FILING_MODEL`). A price backtest
over ~4,300 historical announcements showed the clause often cannot decide materiality, so:
- **Fixed NOISE, no model call:** clauses 12 (investor conference), 18, 22, 23, 36, and
  clause-31 meeting-DATE notices - each moved prices no more than an ordinary day; and
  clause-14 dividends filed on behalf of a subsidiary (paid to the parent, inside the group),
  and clause-14 ex-dividend / record-date timetables (subject names 基準日/除息/除權 - all 185
  such subjects in 2019-2026 are timetables of a dividend decided in an earlier filing).
- **Rules on top of the model's answer** (the prompt says the same; the model did not follow
  it reliably): clause 40 (trading halt) is always HIGH; a subject marking a supplement or
  correction (補充/更正/補正) caps the rating at WEAK - the news was the first filing. From the
  clause-14 dividend form's own fields (`dividend_figures`): no dividend at all (every amount 0,
  or 不分配…股利) is fixed NOISE; a stock dividend of half the par value or more (Wiwynn's NT$20
  per NT$10 share = 2 new shares per share) is HIGH with a reason built from the figures.
- **Known names** (`KNOWN_NAMES`): verified English names of group companies that file under
  their Chinese name only (Cyntec 乾坤科技, SPIL 矽品, ASE Inc. 日月光半導體製造, FII 富士康工業互聯網,
  MasterLink 元富證券 - LEI record) -
  given to the model when present and enforced on its answer and on the title translation; the
  model had given 乾坤科技 three different wrong names. Add a name only with a source pairing the two.
- **Par value check:** a share issue form's 發行總金額 is the par-value total; an answer that calls
  it the money raised is refused and repaired (BizLink: NT$50M at par vs ~NT$1.15bn raised).
- **Review 2026-10-19** (Ayushi Shah's doc, 17 card changes + 8 rule breaks): adopted the rules
  that are checkable on the cards (overseas-subsidiary injections, own equipment purchases and
  minority/land sales -> weak; new-plant construction and stated share offerings -> signal;
  stoppage notices capped at weak; results need figures; buyback form fields). Regression set:
  25 cards with a wanted label - 8/25 before, 23/25 after (the two left are a product choice -
  founder resignation - and a supplement kept at noise). Not adopted yet: an `ai_relevance`
  field, a company AI profile, code-computed deal size, linking related earlier filings.
- **Five-reviewer test 2026-10-19** (analyst, trader, AI-economy, technical, sceptic on 80
  unseen filings, set 5): model 82% vs the reviewers' majority before fixes, 91% after (tuned on
  it - optimistic); regression set 26/28. Code rules added from it: term-end board elections are
  counted from the form's old/new lists (`board_seat_changes`; the 三分之一 box reads 是 in every
  full election) - the count goes to the model, and an unchanged board is NOISE; a clause-20
  capital injection into a Latin-named (non-Taiwan, non-holding) subsidiary is at least WEAK;
  the plain own-equipment form is capped at WEAK; per-share dividend recalculations (調整配息率,
  58/58 share-count causes) are fixed NOISE. Name checks (generic - no per-company lists, work
  for any company added later): a romanised word in an English name must be spelled by its own
  Chinese in any pinyin or Wade-Giles reading (`romanisation_not_in_original`, pypinyin: catches
  "Suzhou" for 黃石, "Yuanta" for 元富, "Nanya" for 南茂); an English word that is another listed
  company's coined brand, where the original starts like that company, is refused
  (`other_listed_brand`, table `taiwan_listed_companies.py` generated from TWSE/TPEx open data:
  catches "Darfon" for 達豐). Measured on 521 stored glosses: 0 false refusals. Not catchable
  generically: a foreign brand that is neither pinyin nor Taiwan-listed (Entegris) - the prompt's
  pinyin rule covers it. Unverified Chinese-only names go in pinyin. Guard fixes:
  dates must be the filing's own (yearless 12/19 may take the filing's year; quarter ends allowed),
  table unit headers scale their figures ((1000000元) 958 = 958,000,000), names are not read
  across lines or from regulation titles. Full 160: 0 fallbacks, every row with a summary.
- **Review rules applied 2026-10-19 (user decision):** founder, chairman or CEO leaving is
  weak_signal, not signal (review: "a product choice for an AI-economy feed") - code caps a
  clause-6 HIGH at WEAK unless a third of the board really changes. Further code rules, each
  checked on the 2019-2026 history: leases of land/buildings with anyone outside the group
  (related parties such as WNC included) are WEAK; the company's own equipment/plant-works form is
  WEAK; overseas holding-company injections count as overseas injections (floor WEAK); a press
  report of an acquisition the company does not deny is at least WEAK; a working-capital/debt
  bond issue with no named investor is capped at WEAK; a ransomware/intrusion incident is at
  least WEAK (website-only DDoS excluded); stoppage/epidemic notices are at least WEAK; a CFO
  (財務主管) change is at least WEAK; routine officer posts (spokesperson, governance, audit,
  accounting, CISO) and same-firm audit-partner rotation (clause 7) are fixed NOISE. Prompt: capex
  revisions are weak unless they add a site or raise the year's budget by half or more; a
  subsidiary's new-plant contract stays signal. Results: set 5 79/80 (the one miss is a CEO
  change, weak by the new rule), regression 28/28, full 160 0 fallbacks.
- **Everything else:** one model call returns `signal_detection` (signal/weak_signal/noise -
  the column's own vocabulary), `signal_reason` (one or two plain sentences, readable on their
  own, naming the facts in the filing that decided it), a 3-6 sentence summary whose first
  sentence states what happened, and key facts. Fixed-NOISE and fallback rows get plain-sentence
  reasons too - no codes.
- **Stored fields:** the label goes to the `signal_detection` column and its reason to
  `signal_reason` (carried in memory as `announcement_materiality_signal` / `_reason` and
  popped before insert - never stored twice). In `metadata`: `translated_title` = literal
  translation of the subject line (same key GDELT rows use); `translated_summary` = the
  model's summary + key facts, written ONLY from the filing's own subject and body and
  checked against them - 1 to 6 sentences, as many as the filing supports, so a short
  notice gets a short summary and a missing amount or counterparty is simply left out;
  `translated_body` = literal translation of the whole filing, ONLY on rows with no checked
  summary (~1%). A row has a summary or a full translation, never both.
- **Every filing gets the summary call**, fixed-NOISE rows included (their label and reason
  stay set by rule; the model's label is ignored). Up to two repair attempts. A filing the
  model did not answer at all (outage, timeout) is retried once at the end of the same run
  after a 60s pause, then stored with the literal translation - never deferred to a later
  run, because the scheduled run reads only today's fetches and a deferred filing could be
  lost. Remaining refusals are logged (`[TAIWAN_ANNOUNCEMENT] ... model_output_rejected: <why>`):
  review them each test round and fix the cause (most so far were the checker being too
  strict - 49億, "April 2025", 每仟股, 7仟萬). The prompt ends with a FINAL CHECK that
  restates each code check with WRONG/RIGHT examples, so the first answer passes: first
  answers passing went from 71% to 84-88% on fresh 80-filing samples (2026-10-18); every
  retry is logged with its cause (`... retry: <why>`).
- **Run log:** two count lines per run. The path line (`fixed_noise=6 model_noise=32
  model_signal_repaired=1 ...`) says how each filing was decided; `model_*` is the
  model's OWN label, before `_rating`'s rules. The `stored: signal=N weak_signal=N noise=N
  rule_overrides=N` line is the final label - what the `signal_detection` column holds -
  and `rule_overrides` is how many model labels `_rating` changed, which is the whole
  difference between the two lines' model counts.
- **Cost:** the instructions (~4,600 tokens, ~94% of each call's input) are identical for
  every filing and sent first, so the provider caches them automatically (measured 86-90%
  of input tokens cached; ~$0.004-0.01 per filing). Keep anything per-filing out of the
  system prompt or the cache is lost. Each run logs
  `[TAIWAN_ANNOUNCEMENT] spend: N filings, N calls, N input tokens (N% cached), ..., $X`
  from the cost OpenRouter returns per call. At ~5 filings a day this is ~$1-2 a month.
- **Guardrails:** Taiwan-calendar dates (all six digit formats and Chinese-numeral years) are
  converted before the model sees the text; the filing is passed as delimited data; JSON mode,
  temperature 0; every number, Chinese name and company-style English name in the output must
  appear in the filing; month names, figure-free quantity words and predictions or
  generalisations ("potential", "could", "usually") are refused - the prompt alone did not stop
  the model writing them. A refused
  answer gets one repair attempt (with a unit hint when an amount failed - the model wrote
  49億 as "49 billion"), re-checked in full. If it still fails, the summary is discarded and
  the row keeps its literal translation; the label and reason are kept if they pass the checks
  on their own (a results release used to drop to WEAK because one key fact failed). Only
  when they fail too is the row rated WEAK with a fallback reason - never shown unverified and
  never hidden as NOISE. Chinese-numeral amounts are converted to digits, but never a unit
  after Arabic digits ("441佰萬元" once became "4411000000元", a ten-times error). Western
  years written the Chinese way (2025年4月) become 2025-04 too. In the output, a month name
  WITH a year ("April 2025") is rewritten to 2025-04 and then checked like any date; without
  a year it is refused. Two-part amounts (3,495億6,700萬元) are checked by their total value.
- **Names, English first:** every Chinese-only name (company, exchange, person) is written
  "English translation (exact Chinese as filed)" in the reason, summary, key facts, the
  translated title and entities_json. The checker refuses Chinese outside those brackets, and
  accepts an English company name only when it sits right before the filing's own Chinese
  name (or is in the filing). It cannot verify the English translation itself; the
  Chinese next to it is the filing's text. Exchanges, regulators, courts and prosecutors
  are dropped from entities by rule.
- **Entities, subsidiary:** the subsidiary a filing is made for is taken from the subject
  ("代子公司XXX公告") by rule - the model left it out on ~15 of 80 reviewed filings.
- **Entities:** companies that are parties to the event (counterparty, target, investor,
  contractor, the subsidiary filed for) go to the existing `entities_json` /
  `entity_names_normalized` columns, like the SEC classifier's. The filing company comes from
  our ticker list; the others from the model, each only if its name appears in the filing.
  Appraisers, law firms, payment banks and companies named only in a person's background are
  excluded; for clauses with no counterparty (6, 7, 8, 12, 17, 18, 29, 31) the filing company
  is the only entity, by rule - the model did not follow that instruction reliably.
- **What the label means: worth reading, not price impact.** `signal` = worth an analyst's or
  trader's time (new company-level information), `weak_signal` = worth a glance, `noise` = safe
  to skip (routine operations purchases, administrative notices, money moved inside the group).
  This matches what `signal_detection` means elsewhere in this service (relevance, not market
  impact). It was tested against prices on 400 historical filings and does NOT predict the
  event-day move (all three labels ~20% big moves vs 10% on ordinary days) - so the page must
  not present it as a price signal. The one filing type that does move prices is a results
  approval (clause 31, excluding date notices: ~30-40% big moves). Validation of the
  worth-reading label is by analyst agreement on a sample, not by price.
- **Validation (2026-10-18):** four independent reviewer personas (analyst, trader,
  technical auditor, sceptic) read the Chinese filing for 80 random filings at a time. On
  sets the prompt had never seen, the model matched the reviewers' majority 76-78%; the
  reviewers matched each other 90-93%. After each round's fixes the same set reached 84-89%
  (tuned, so optimistic) - the honest number is always the next unseen set. No filing the
  reviewers called signal was ever labelled noise. The last audit found no wrong number,
  unit or date in 80 summaries.

## Japan Signal Pipeline

Full technical reference: [JAPAN_SIGNALS.md](JAPAN_SIGNALS.md) - sources, thresholds and their
real-data derivation, habit tables, DB schema, API filters, schedules, known limitations, and
the operational runbook. The summary below is the orientation version.

`japan_market_signal` (IRBANK forecast revisions/buybacks/company reference, Kabutan/EDINET
filings, SEAJ industry billings, MONOist capex news) is classified by its own separate path,
`pipeline/japan_signal_classifier.py` - see that module's own docstring for the full design,
habit-computation rationale, and real-data findings per signal type (kept there, not
duplicated here, since it's the actual source of truth and changes as each signal type is
built/tuned). All 7 of the Japan Signals spec's signal types (J1-J7) are implemented and wired
into one entry point, `classify_japan_signal_batch()`. J7 (press) is the only one that makes a
real LLM call (spec Section 6.6/10.1) - the other six are pure arithmetic/lookup, per the
spec's own explicit design intent. Spec Section 10.3's WATCHING section (a tracked company's
real elapsed time since its last genuine revision vs. its own stored cadence) is a separate,
8th classifier, `classify_stale_revision_pattern` (`source_category = 'jp_watching'`) - NOT
wired into `classify_japan_signal_batch` (that entry point's own daily window is too narrow to
ever find a company's true last revision) but into `refresh_japan_habits` instead, which
already pools the full wide-history window this check needs.

All static company reference data lives in ONE module, `pipeline/japan_companies.py` -
`JAPAN_COMPANIES`, keyed by TSE code, with each company's native name, fiscal year end,
market weight (TSE Prime share, Japan rank, Japan sales share, market cap) and its
disclosed customers (name, ticker, `pct_of_sales`, filed `period`, aliases,
`relationship`). It replaced three overlapping modules (`japan_ticker_universe.py`,
`japan_company_profile.py`, `japan_read_through.py`) that each held part of the same facts
and disagreed in places. `JAPAN_TICKER_UNIVERSE` remains as a list view over the same
objects, so the two never drift. This table is the intended migration unit into
research-universe's DB, so anything added here must map to a column there.

The twice-daily trader summary was REMOVED (2026-10-02, explicit user instruction): the
cards carry the same facts, and the prose restated them at the cost of an LLM call per run.
`pipeline/japan_signal_summary.py`, `GET /japan-signals/summary`, the
`summarize-japan-signals` command and both of its CloudWatch schedules are gone. The four
formatting helpers it owned (`_age_hours`, `_format_pct`, `_format_jpy_millions`,
`signal_implication`) moved into `japan_signal_view.py`, which is their only caller now.

Two read-only routes exist for this domain (both under `routes/jobs.py`, both
`require_auth`): `GET /japan-signals/universe` (the static 19-company reference list, built
field-by-field from `JAPAN_COMPANIES` in memory - not `agent_classifications`), and
`GET /japan-signals/results` (paginated, filterable - `code`, `source_category`,
`signal_detection`, `published_from`/`published_to`; delegates to the same `list_all_results`
the generic `GET /results` uses, with `source_type` pinned to `japan_market_signal`, then
shapes each row through `japan_signal_view.to_jp_signal`). Neither route, nor any other
Japan Signals command, is triggerable over HTTP - `classify-japan-signals`/
`refresh-japan-habits` are CLI-only, same as every other non-`ai_news` domain in this
service (`pipeline/dispatch.py`'s own module docstring states this architecture explicitly:
only per-article domains route through `POST /run`).

Each shaped card carries `entities`: which of that company's own disclosed customers the
event's text actually names, matched against the English name, native Japanese name and
ticker aliases held in `JAPAN_COMPANIES`. Computed at classification time and stored in
`agent_classifications.entities_json` (plus `entity_names_normalized` for filtering), not
recomputed per request. An empty list is the common and correct answer - only 4 of 394 real
rows name a customer, because a forecast revision states figures and mentions nobody.

Entry points (Click commands in `__main__.py`, CloudWatch schedules in
`infra/modules/ecs_cluster/services.tf`), anchored to real TSE market hours
(09:00-15:00 JST, JST = UTC+9, no DST) rather than an arbitrary buffer: news-retrieval fetches
twice daily (23:00 UTC pre-open, 06:30 UTC post-close), `classify-japan-signals` runs once
after each fetch (00:00 UTC, 08:30 UTC - reads pre-computed habit/reference caches, never
recomputes them inline; always scoped to today's pooled runs only, even on its first-ever
invocation - never a full-history backfill, that is `refresh-japan-habits`'s own job).
`refresh-japan-habits` (05:00 UTC on the 25th of each month) is the only occasional job -
recomputes all three stored caches - `japan_company_habits`, `japan_progress_habits`,
`japan_company_reference` - from the FULL pooled history in one pass, and also runs the
WATCHING check against that same pool - see refresh_japan_habits's own docstring. news-retrieval
fetches this domain fetch/dedup-only, same boundary as Taiwan/Korea. Results persist to the
same `agent_classifications` table, `source_type = 'japan_market_signal'`, distinguished by
`metadata.source_category` per signal type (`jp_forecast`, `jp_industry`, `jp_capex`,
`jp_ownership`, `jp_buyback`, `jp_watching`, etc.).

## China Signal Pipeline

`china_market_signal` (MOFCOM/MIIT/SAMR/CAC policy, cninfo + HKEX filings, UN Comtrade and
NBS trade data, Chinese press) is classified by `pipeline/china_signal_classifier.py`. Same
fetch/classify boundary as Taiwan/Japan/Korea: news-retrieval fetches and dedups, everything
else happens here.

**This is the only domain where a positive local signal is usually a NEGATIVE read for the
US name attached to it.** Naura winning a tool slot is revenue leaving Applied Materials.
That inversion is carried explicitly as `direction` on every read-through link, and it is
why this domain needs a direction field where the others do not.

All seven of the spec's signal types are implemented, plus a triage gate:

| | What it reads |
|---|---|
| Gate 1 | Filing-type triage - 4 tiers, discards ~93% of filings before anything expensive runs |
| C1 | Policy binding verdict (BINDING/NON-BINDING/UNCLEAR) - the one judgment call, 2 confirmation calls, disagreement → UNCLEAR |
| C2 | Substitution progress - revenue YoY against **this company's own** past rates for the **same period type**, structured figures where available |
| C3 | Capacity commitment - with restatement collapsing (Hua Hong filed 13 announcements tracking one acquisition over 10 months) |
| C4 | Accelerator milestone - 4 filters, accelerator-role companies only |
| C5 | Platform capex - platform-role companies only, against that platform's own capex history |
| C6 | Trade deviation - per-series MAD over the import side (Comtrade partner-reported) and production side (NBS) |
| C7 | Named US-company action |

Thresholds are derived from the stored distribution (MAD, not standard deviation - outlier
resistant), never lifted from the spec. Trust floors make the classifier refuse to judge
rather than fabricate a baseline: `_C2_MIN_OBSERVATIONS = 3`, `_C3_MIN_OBSERVATIONS = 3`,
`_C5_MIN_OBSERVATIONS = 3`, `_C6_MIN_PERIODS = 6`. Below the floor the baseline fields are
ABSENT rather than zero, so "not measured" stays distinguishable from "measured and
ordinary".

**Three is forced by the data, not chosen.** Zero companies have eight observations in any
single period type - five of the twenty listed in 2022 or later, so their entire filing
history is four years or less, and an eight-year Hygon fetch returned exactly what a
four-year fetch already had. Three is the floor at which a spread can be computed at all.

**The band (`_C2_SIGNAL_MADS = 3.0` / `_C2_WEAK_MADS = 2.0`) was checked against the real
283-observation distribution**, not inherited. What it has to separate is a genuine break
from the top of a smooth rising trend - China's semiconductor names have been in a
continuous upcycle, so a company's newest figure is often its largest. Measuring how many
firings are the newest point of a monotonically rising series: at 1.0 MAD, 5% of firings;
at 2.0, 3%; at 2.5 and above, zero. 2.5 is the lowest defensible band and 3.0 sits inside
that region with margin - needed because at n=4 the MAD is roughly half the interquartile
spread. See the comment block on `_C2_SIGNAL_MADS` for the full table. The binding
constraint is sample size, not the band: 63 of 65 baselines have n <= 6.

### Per-company baselines: `market_signal_baselines`

C2 and C3 judge a figure against **that company's own history for the same period type**,
never against whoever else filed that quarter and never mixing an annual rate with a
quarterly one (GigaDevice's Q1 median runs 68 points above its annual, about 3x its own
MAD). Computing that needs years of filings; the scheduled classify pass pools one day.

`market_signal_baselines` (domain, code, metric, period_type) is what lets the two
coexist - a refresh rebuilds from the full history, the daily pass reads the cache. Without
it the same classifier gives different answers depending on how it was invoked. The table is
market-agnostic by design, China-only in practice; `japan_progress_habits` is the intended
second tenant.

`refresh-china-baselines --years 5 [--refetch]` rebuilds it. **It also runs in-process at
the start of every `classify-china-signals` pass** (~5s over 9,176 pooled articles), so a
company filing on the 1st is judged the same day against a baseline that includes it rather
than waiting for the monthly job. The two cannot be separate scheduled rules: a verdict is
written once and the next pass skips that `source_id`, so anything judged against a stale
cache keeps that verdict permanently. `--refetch` is the expensive half (one upstream
request per company, ~90s) and stays monthly; the daily pass re-pulls only companies that
filed a periodic report in its window, usually none.

Reporting periods are `FY`, `H1`, `Q1`, `Q3` for mainland issuers - no Q2 or Q4, since the
half-year covers Q2 and the annual covers Q4. The HK-listed names file in English and do
announce standalone `Q2`/`Q4`, kept as their own period types rather than folded into
H1/FY.

### The four values of `signal_detection`

| | Means |
|---|---|
| `signal` | cleared the company's own band |
| `weak_signal` | cleared the weak band but not the signal band, **or** broke the company's own pattern on growth too small to call substitution - see below |
| `noise` | **judged and found ordinary**, or a filing type no rule reads |
| `waiting` | a rule reads this shape but cannot judge it yet |

**C2's third weak path: a break on a flat history.** A company whose past is tightly
clustered has a small MAD by construction, so an ordinary move scores enormous - China
Northern Rare Earth's +36.8% sits 17.8 MADs above a half-year median of -18.1%. Those
rows used to be `noise` (they fail the growth floor) and their reason text read "+17.8
MADs ... within its usual range", a contradiction in one sentence.

They are now `weak_signal` when the deviation clears `_C2_SIGNAL_MADS` and growth is not
negative. Measured over the 88 observations with two priors and a known next period: the 6
rows in this band had a median NEXT period of +25.6%, four of six still above +25%, and
none went negative - not ordinary, but not the above-floor population either (+72.4%
median, 7 of 7 above +25%). Two magnitudes, two verdicts: this band is weak, never signal.
`pct >= 0` excludes genuine declines - a company whose revenue FELL is not substitution
progress whatever its spread.

**Evidence limit, recorded rather than buried:** n=6, 95% interval 30%-90%, and it needed
the trust floor relaxed to two priors to find a measurable sample at all. Adopted on
judgment, not on a result that clears the bar the rest of this module holds. Revisit once
enough of these rows have a known next period to test at n>=30. No observation floor on
this band, deliberately - it admits n=1 rows where the denominator is `_C2_MIN_MAD_PCT`
rather than a measured spread, labelled by `mad_is_floor` and the reason text, and capped
at weak so an assumed spread can never produce a signal.

**Every pooled article gets a row - noise is stored, never dropped.** A dropped article is
indistinguishable from one never fetched, and because the daily pass skips by `source_id`,
an article with no row was re-examined on every later pass forever (measured: 650 of one
real day's 935). Each noise row's `signal_reason` names the gate that actually turned it
away - C1's relevance vocabulary, C6's band, a Gate 1 filing category, an English coverage
check that is headline-only by design - so "we looked and it was ordinary" is recoverable.

In practice `waiting` is now empty: a Gate 1 candidate that reaches the end of the batch has
been offered to every rule that reads its shape and turned down by all of them, which is
`noise` with a reason saying which of three things stopped it (no fiscal period in the
title, no year-on-year revenue figure, or too little history for its own baseline).

**One static table: `pipeline/china_companies.py`.** `CHINA_COMPANIES`, keyed by exchange
code, holding each company's `signal_roles` (which classifiers apply) and `read_through`
(which US tickers the signal reaches, with direction and the signal categories it applies
to). Three separate code-keyed structures used to hold this and a missed edit failed
silently - a new platform absent from the C5 code set makes C5 never fire for it, with no
error. `codes_with_role()` and `read_through_for()` are now derived views over the one
table, the same consolidation `japan_companies.py` made for the same reason. Same intended
migration unit into research-universe, and the same caveat: the links are analyst judgments,
not filed facts.

Direction is per `(company, counterparty, signal type)`, not per pair - Baidu reads `same`
toward Nvidia under C5 (it buys GPUs) and `opposite` under C4 (its own accelerator
programme displaces them).

**WOULD CONFIRM / WOULD CONTRADICT** (spec Section 8) is attached to every signal after
read-through: one `CHINA_SIGNAL_MODEL` call stating what future observation would support
or undermine the reading, falling back to a per-`(signal type, direction)` template. Two
guards, both enforced rather than merely prompted - the model is passed the US tickers
rather than inferring them, and any number, date or calendar period the signal's own
metadata does not already contain is rejected. A specific-sounding fabricated threshold
reads as analysis and nothing downstream can catch it.

Every LLM call in this domain (C1's verdict, the confirmation pass, translation) routes
through `CHINA_SIGNAL_MODEL`, which falls back to `SEC_FILING_MODEL`.

Entry point: `python -m src classify-china-signals` (CLI only, same as every other
non-`ai_news` domain), scheduled twice daily - 10:00 UTC after the post-close filings fetch
and 14:00 UTC after the policy series ends - via
`signal_detection_agent_china_signals_filings`/`_evening` in
`infra/modules/ecs_cluster/services.tf`. Both default to today (UTC).

**Every day, not Mon-Fri.** Both rules were weekday-only until the stored history was
checked: Chinese issuers file at weekends - 1,715 Saturday-dated cninfo filings against
1,830 on Friday - and since the classify window selects RUNS by date, Monday's pass pools
Monday's runs, never the weekend's. Those rows were orphaned permanently rather than
delayed. The fetch rules moved to daily at the same time, which is what let every window
drop to `--days-back 1`.

**Already-classified articles are filtered out BEFORE the classifier runs**, not after. The
window selects runs by date, so a day on which a multi-year backfill landed pools that whole
backfill; filtering afterwards meant re-running such a day re-classified everything at full
LLM cost to insert nothing (measured: 3,694 articles). The `source_id` is the article url,
known without classifying it, so the filter moved ahead of the classifier. Re-running a
completed day now costs about three seconds. Taiwan deliberately keeps the opposite order -
its `source_id` is derived inside its classifier.

Results persist to `agent_classifications`, `source_type = 'china_market_signal'`,
distinguished by `metadata.source_category` (`cn_policy`, `cn_disclosure`, `cn_trade`,
`cn_press`) and `metadata.signal_type` (`C1`..`C7`, `gate1`).

**Every row carries the article's identifying fields**, copied centrally by
`attach_source_metadata` rather than assembled per classifier: `code`, `company`,
`native_name`, `sec_name`, `hk_code`, `filing_url`, `filing_language`, `announcement_id`,
`source_type`, `source_outlet_type`, and `summary` where the fetcher provided one (206 of
9,176 China rows - only the English coverage check carries an editorial lead; filings and
government notices open straight into the measure). Each rule used to build its own
metadata dict and they disagreed - C1 carried no code at all, gate1 carried neither code nor
filing_url - so `GET /results?code=688981` silently missed whole signal types and a reader
could not open the document a verdict came from.

**Results are read through the generic `GET /results`.** It returns the whole `metadata`
JSONB, so `read_through`, `direction`, `would_confirm` and every extracted figure come back
as stored. `GET /results?source_type=china_market_signal&code=688981` filters to one
company - `code` is a generic filter on `metadata.code`, serving Japan and China alike. A
card-shaping view (Japan's `japan_signal_view.py` equivalent) is deliberately not built
while the classifier is still being tuned: raw metadata shows the fields a view would hide.

**One China route exists: `GET /china-signals/universe`** - the 20 tracked companies
(`code`, `company`, `native_name`, `hk_code`, `signal_roles`, `read_through` with direction
and relationship), served from `CHINA_COMPANIES` in memory. It is reference data, not
results, and exists because the alternative was paging ~9k classification rows to recover a
20-row company list. Same role and shape as `/japan-signals/universe`. `native_name` and
`hk_code` are duplicated from news-retrieval's `CHINA_TICKER_UNIVERSE`, which stays the
source of truth for fetching - checked equal at 20 companies on 2026-10-09, and
research-universe is the intended owner of both.

**Money fields follow one rule: every `*_value` is CNY.** `currency` names what the filing
stated; branch on `native_value`, not on `currency`. Present means the value is converted
and `currency` describes `native_value`; absent means `currency` describes the value
itself. FX is the ECB reference rate for the filing's OWN publication date
(frankfurter.app, cached per currency/date), never the run date - so a reclassify
reproduces the same figure rather than re-pricing four years of history at today's rate.
`fx_rate`, `fx_rate_as_of` and `fx_rate_source` (`ecb` / `fallback`) travel with every
converted figure. The fallback table exists so a dollar amount never enters a renminbi
baseline unconverted - a ~7x error - not because the approximation is good.

**`issuer` names the body that published the document**, with `issuer_source` recording
whether it was read from the title (`document`) or defaulted from the feed (`source`).
news-retrieval's own `issuing_body` is fixed per feed and therefore wrong whenever one body
publishes on another's site: 8 of 87 MIIT-fed documents were issued by a provincial
communications administration and all 8 read as the ministry. A default that cannot be told
apart from a fact is how that happened, which is why both fields exist.

**`supports`/`weakens` are omitted where no testable claim fits.** C1's template asserts a
US-listed supplier will disclose a licence denial attributing it to the measure - true of a
trade remedy or export control, false of a statistics bulletin, an industry plan or an
exhibition notice. Gated on C1's own `binding` verdict rather than a new judgment: 11 of 13
stored C1 rows are `checkpoint_source: not_applicable`.

## Middle East Signal Pipeline

`middle_east_market_signal`, from "Middle East Market Signals for the AI Economy" (Ayushi Shah,
2026-10-15), **classification only** (no trader cards or daily summary yet). Code:
`pipeline/middle_east_signal_classifier.py`; flow in [STRUCTURE.md](STRUCTURE.md). Labels are
used directly: `signal` = the spec's Alert, `weak_signal` = Store, `noise` = Ignore - and noise
IS stored, with its reason, like every domain.

**The model extracts, code decides.** One `MIDDLE_EAST_SIGNAL_MODEL` call per article (falls
back to `SEC_FILING_MODEL`), the WHOLE body read once (60k-char safety cap,
`metadata.body_truncated`), prompt `prompts/middle_east_signal_extract_v1.txt`. A 50-article test
(2026-10-21) showed 700/1,500/3,000-character cuts changed 5/4/2 answers vs the full body, and a
yes/no relevance screen dropped items the spec stores - so there is no keyword gate and no
second call. The model returns facts; `decide_label` turns them into a label with one branch per
spec section 7 row, and `metadata.rule` names the branch.

- **No model:** SEC 13D/G (`judge_sec_stake`: issuer must be a US company in research-universe;
  new filing -> signal rise, amendment below 5% -> signal fall, other amendment -> weak - the old
  holding is not in the filing); market wraps and the newsletter's weekly digest (headline rules).
  Federal Register and BIS ARE read by the model - free text, and the Federal Register filter
  also matches notices about non-Gulf companies (a Russian airline's denial order).
- **Checks, never repairs:** every company, amount, capacity, date, place and confirmer must appear
  in the text, and every number in the summary and reason by value ("300MW" is refused when the
  text only has "GB300"); a failing item is removed, so a check can only LOWER a label. Bad enum
  -> weakest value (`unnamed`, `mixed`). Unnamed-only sources cap the deal level at 3 (spec 9.1).
  Not JSON / unknown event code -> weak_signal `model_output_unusable`. Retries only on transport
  errors; a failed call stores nothing and the next run retries it.
- **Code guards added from the live test** (the model got these wrong): M8 needs a company based
  in a Gulf country; a QatarEnergy force majeure alert needs Qatar/QatarEnergy/Ras Laffan in the
  headline (it is often background); a claim by one side of the war is `named_source`, never
  `official`.
- **Two deliberate departures from the spec (user decision 2026-10-22)**, after an audit of every
  weak_signal and noise row found exactly these two real events held back:
  - Hormuz / Red Sea closure or reopening: spec 7.5 wants ship-count confirmation within 72h and
    there is no ship-count feed. Now `signal` when the side that controls the waterway (Iran /
    IRGC, the Houthis) declares it (`m4_waterway_declared_by_controlling_party`, reason says
    traffic has not confirmed it), or when the article quotes a named tracker's ship counts
    (`m4_waterway_confirmed_by_ship_counts`). Other statements (the US saying it is open) stay weak.
  - Incident at a listed chip company's Israeli site (Intel Kiryat Gat fab explosion): spec 7.8
    wants the company to announce a stop; Intel never did. Now `signal` when officials (police,
    fire, ambulance) confirm it (`m7_site_incident_confirmed`).
  Also from that audit: "Why X stock fell" headlines are no longer skipped as market wraps (one
  carried the Intel blast); `m4_interception` (nothing hit) is noise; analysis, roundups and
  reaction statements are `m4_commentary`; a reaction headline (condemns / welcomes) is capped
  at weak_signal. Result on the 403 local articles: 11 signal rows = 4 alerts after story
  grouping (Qatar extends force majeure; Intel fab blast; Hormuz declared closed; Intel
  hazardous-materials leak a week later), 48 weak_signal, 344 noise.

**Storage - each field once, in its own column; related values grouped into one object.**
- `signal_detection` - the label (code decides it).
- `signal_reason` - shown to readers, plain language, no spec terms/levels/codes: the model's
  one-sentence `reason_facts` (what happened, with names and figures) + the deciding rule's
  plain "why" from `decide_label` ("The order is signed, but the capacity is not built or
  running yet."). For `noise` with no event the model's sentence says, for that article, why it
  changes nothing (a fixed sentence contradicted articles about Gulf data centres or helium).
- `entities_json` - companies the article is about (`role: subject`, ticker, country) and the
  read-through (`role: affected`, `share_price` rise/fall, `relationship` with "(spec ...)"
  citations stripped).
- `metadata`, nothing stored as null, one object per kind of information:
  `source {name, url, outlet, event_uri}` (url/outlet/event_uri newsapi.ai only - the row url is
  the synthetic `er-me://` id); `event {type, code, place, effective_date, lasts_over_24_hours}`
  (what happened, where, when it takes effect, how long a restriction lasts); `deal {level,
  amount, up_to, capacity}` (how firm and how big - deals only; `amount` is the deal's own
  value, never losses or market sizes); `confirmation {level, by}`; `new_development`;
  `news_effect` (good/bad/mixed for the companies - sets the read-through direction); `summary`
  (WHAT happened, usually 2-4 sentences, never comments on relevance); `rule` (internal branch
  name, never shown); `extracted_by {model, prompt}`; `body_truncated` (only when cut);
  `corroboration`; `story_development` / `in_story`. The model returns the same names.
  A rule change can re-label stored rows from these without a model call.
- **Confirmation checks:** `by` must be in the article body; anonymous wording ("two people
  aware", "sources familiar") -> `unnamed`; the publishing outlet (TokenPost, WAM) is never
  `official`. A level above `unnamed` with no valid `by` falls to `unnamed`.
- **SEC 13D/G rows** carry `event`, a code-built `summary` from the filing's fields, and both the
  fund and the issuer in `entities_json`.

**Read-through** (`read_through`): each tracked company's `counterparties` from news-retrieval's
table (`share_price_on_good_news`, reversed on bad news), then the spec "At a glance" row(s) for
the event CODE (`_ROWS_BY_CODE`) - a construction contract, a software deal, a ship incident or a
threat moves no row (a copied template had put "more AI hardware is sold" on a construction
contract). A helium restart reverses the helium rows with matching wording (spec card 8.1).
Mixed news gets no direction. M3 skips company counterparties (Mubadala -> GlobalFoundries is
what it owns, not what a new investment moves).

**Four-reviewer test (2026-10-22):** trader, analyst, technical and lay-reader personas on 30
articles (mostly unseen). Labels: trader 26/30, analyst 27/30 before fixes. Fixed from it:
anonymous/outlet confirmers, event code must be the HEADLINE's news (not background), Qatar
force majeure needs QatarEnergy itself, software partnerships are not data-centre deals (code
guard `m2_not_hardware`), number check by value ("AED 2.5 million" = "AED2.5M"), rows without a
published time never join a story, plain-language reasons. Re-run: all reviewer label
disagreements resolved; one summary of six still ended with a relevance remark.

**Full local run (2026-10-22, job 1498, all 403 articles, $2.64):** 8 signal / 63 weak / 332
noise at first. Reading the rows found 5 wrong signals, fixed in code and re-labelled from the
stored fields with no model call: the Qatar alert now needs QatarEnergy's own declare / extend /
lift / end / restart in the HEADLINE (`_qatar_announcement_headline` - four "exports plunge" /
"Pakistan ... amid Qatar force majeure" headlines had passed a bare "names Qatar" check); a Gulf
rule must concern AI, data or chips (`m6_not_ai_or_data` - Saudi boat-chartering rules). Story
keys: places are matched on the place name without direction words (`place_key`; initials of
"eastern Strait of Hormuz" had made the key "esh"), and Gulf countries and the Saudi-led
coalition never make a story alone. After: 3 signal / 67 weak / 333 noise, 6 stories.

**Corroboration** (`metadata.corroboration`, geopolitical Stage D's `build_corroboration`):
other outlets on the same event, from news-retrieval's `also_reported_by` and newsapi.ai's
`event_uri` cluster (this batch + stored rows). On every signal/weak_signal press row; `count: 0`
means looked and found none. Shown, never used to raise a label.

**Same-event duplicates - one row per event** (`metadata.collapsed_into`, geopolitical Stage B's
tombstone): after each insert, signal/weak_signal rows reporting the SAME event are folded into
the best one (signal before weak, then best-confirmed, most corroborated, earliest); each
duplicate's outlet, headline and link join that row's `corroboration.siblings` (`via:
same_event`), and the duplicate is set to noise with `collapsed_into` - never deleted (the
already-judged skip reads its source_id; a wrong fold stays auditable). "Same event" = same
event code, within 48h, and a shared subject company, or the same place for place-bound codes
(closures, site incidents, strikes on a site, force majeure); ship attacks and statements need a
shared ship / company / speaker. NOT newsapi.ai's event_uri: one held 15 different events on the
403 run. Duplicates cannot be skipped BEFORE the model call (as geopolitical does): measured on
that run, headline-embedding similarity was 0.34-0.74 for same-event pairs and up to 0.82 for
different tanker hits - no threshold separates them, and only 19 of 370 calls (~5%) were
duplicates. Run 1498: 18 reports folded; 4 signal rows remain, one per event.

**Stories** (`metadata.story_development` on the seed, `in_story` on members - geopolitical
Stage E's pattern): same event type, within 72h, sharing a specific name (a subject company or
the place); countries, armed forces, agencies, commodities and read-through companies never count.
Seed = signal before weak, then most corroborated, then earliest. Nothing deleted; regrouped from
scratch after every insert. One row per story: `NOT (metadata ? 'in_story')`.

**Reading the rows.** `GET /results` and `GET /results/summary` return one row per event or
story by default (`view=events`, see "Results views" below): story members (`in_story`) and
folded duplicates (`collapsed_into`) are left out and travel inside the lead row's
`story_development` and `corroboration`. `view=rows` returns every stored row.

**Cost** (live, 2026-10-22): ~$0.007/article (~2,900 input + ~200 output tokens, `gpt-4.1`);
prompt cache hit 0-7% so far even with a warm-up call. Each run logs
`[MIDDLE_EAST_SIGNAL] paths:` and `spend:`.

## Geopolitical Signal Pipeline

`geopolitical_signal` is a five-stage funnel over the `geopolitical_news` domain, which
fetches from exactly two sources: `federal_register` and `eventregistry` (newsapi.ai).
Same fetch/classify boundary as the other non-`ai_news` domains - news-retrieval fetches,
dedups and stops; every judgment happens here.

```
Stage A  rule-based   creates rows (status 'waiting')          geopolitical_signal_layer1/overrides
Stage B  LLM call     talk vs action -> signal/weak_signal/noise   geopolitical_signal_stage_b
Stage C  LLM call     channel, actors, assets, category, summary   prompts/..._stage_c_v1.txt
Stage D  no model     corroboration grading                     geopolitical_signal_stage_d
Stage E  no model     story grouping                            geopolitical_signal_stage_e
```

Each stage only ever acts on rows the previous stage wrote, so they are inherently
sequential, not independent jobs - `run-geopolitical-signal-pipeline` runs A→B→C→D→E in one
process for exactly that reason (separate cron rules risked B firing before A had
finished). The primary trigger is news-retrieval's run-completed webhook; the 03:00 UTC
CloudWatch rule is a fallback for a lost delivery.

**Stage E runs last in both paths, and is not optional.** Every API read of this domain is
expected to be post-Stage-E, because the default signals view filters on
`NOT (metadata ? 'in_story')`. If E never ran, that filter would match every row and a
reader would see one story repeated under several headlines - precisely what E exists to
prevent. So it is wired into *both* sequences: `__main__.py`'s combined command and
`routes/webhooks.py`'s background task. Anything that adds a third trigger path must
include it too; wiring only one leaves the other writing no story fields at all.

Unlike A-D, Stage E is not scoped to the rows the previous stage just wrote - grouping is a
property of the whole signal set, so it reads every Stage-D-graded signal and regroups from
scratch. That makes running it twice in a day safe, which matters because the webhook and
the fallback schedule can both fire: verified on the live 57-signal set, a second run
changed 0 of 57 rows and left 5 seeds / 8 members intact, with no orphaned `in_story`
pointer, no row marked both seed and member, and no story field on a non-signal row.

### Stage B's verdict is irreversible

`weak_signal` and `noise` never reach Stage C, D or E. That one-way door is why Stage B
fails open: a row the model could not judge becomes `weak_signal`, never `noise`, so
"we couldn't check" stays distinguishable from "we checked and found nothing". The same
discipline runs through the stored fields - an absent `corroboration` key means Stage D
never ran on that row, while `{"count": 0}` means it ran and the row stands alone. Code
reading these fields must not collapse the two.

### Stage D: corroboration (`metadata.corroboration`)

Grades how many independent outlets carried the same EVENT. Stage B has already collapsed
same-event rows: when news-retrieval's title-similarity dedup finds a cluster, the judged
row absorbs its siblings into `metadata.also_reported_by` (`{outlet, title, url}` each) and
the sibling rows are TOMBSTONED in the same transaction - set to `noise` and marked
`metadata.collapsed_into`, not deleted. Nothing unique is lost: the sibling's headline
survives on the judged row, and the tombstone keeps its `article_id` resolvable.

The row is kept rather than removed because Stage A's "already classified: skip" check
reads `article_id`. Deleting destroyed the only record that the article had been seen, so
the next Stage A pass called it new and re-inserted the duplicate the collapse had just
folded away - measured at 10 and 14 resurrected rows on two consecutive days.

`corroboration` is the INPUT (how well-sourced is this event); `also_reported_by` is the
OUTPUT (which specific outlets, for display). They are not redundant and both are kept.

### Stage E: story grouping (`metadata.story_development` / `metadata.in_story`)

Stage D asks *who else reported this same event*. Stage E asks a different question -
*is this the same situation I just read about*. Four Strait of Hormuz items in one morning
can be four genuinely different events (a mine strike, a blockade, a designation, a
suspension of raids) and still read as one story to anyone scrolling the feed.

It **marks, never deletes** - the opposite of Stage B's collapse, and deliberately so. The
six rows of one Hormuz story each reported a different state action; delete five to keep
one and a reader querying "sanctions actions this week" no longer finds the Treasury
designation. So the seed row carries `story_development` and the others carry `in_story`
pointing back at it. One row per story is applied **by the API** (`GET /results` and
`/results/summary`, `view=events` - the default since 2026-10-22; it used to be a filter each
caller applied, and the frontend did):

```sql
signal_detection = 'signal' AND NOT (metadata ? 'in_story')
```

That also makes a wrong grouping one field away from undone. Note for any code adding such
a query: **psycopg2 scans the whole query string for `%` and `?` placeholders**, so inside
Python use `jsonb_exists(metadata, 'in_story')`, never the bare `?` operator - the literal
above is SQL-console form only.

Grouping is entity-overlap, no model and no network call: it reuses the entities Stage C
already verified against the article text. Two rows join when they share at least 2 entity
keys, at least one of which is *specific*, within a 72-hour window. Four classes of name
can never carry story identity on their own, each one measured from a real wrong grouping:
countries (half these articles name the United States), issuing institutions (OFAC signs
both a horse-racing designation and a Lukoil licence), commodities (crude oil joined a
Russian sanctions item to a Hormuz incident), and armed forces (the IRGC appeared in a
Hormuz mine strike and a Houthi palace attack). They are admissible as supporting evidence,
never as the whole of it.

Seeded, not transitive: a row joins a story by matching that story's SEED, never by matching
a member that itself only just qualified. Union-find was tried first and chained two
unrelated stories through one weak edge. The seed is the best-corroborated member, since
that is the row a reader lands on.

`story_development.members[]` carries 8 keys per member - `id`, `title`, `url`, `published`,
`channel`, `reason`, `summary`, `shared`. Unlike Stage D's siblings (same event, so an
outlet and a headline suffice), story members are DIFFERENT events and a reader opening the
story needs to see what each one was. `summary` is a copy of Stage C's own field, so the
member row stays the source of truth - a consumer needing current text joins on `id`.
`shared` records which entity keys put each member there, so a grouping a reader disputes
can be interrogated rather than guessed at.

Stage E is **idempotent by rewriting, not by skipping**: it clears both keys and regroups
every time. A row arriving later can change which story an existing row belongs to, and
skipping already-grouped rows would leave a stale pointer hiding a row from the default
view permanently.

### `category` is a shared `TEXT[]` column

Both this domain and `news` write the `category` column directly. It replaced
`metadata.impacted_categories`; filters are `category && ARRAY[%s]::TEXT[]`. Any consumer
reading a single category string will silently drop every row once this is live - that is
the bug fixed in signal-herald's `digest.py`, which now handles both shapes.

## Results views (`GET /results`, `GET /results/summary`)

`view=events` (default) returns one row per event or story: rows carrying
`metadata.in_story` (an event listed inside a story's lead row) or `metadata.collapsed_into`
(a report folded into another row as the same event) are left out - both written by
geopolitical_signal (Stage E / Stage B) and middle_east_market_signal. Pages and summary counts
follow the same view, so `limit=50` is 50 visible rows and the counts match the feed.
`view=rows` returns every stored row (audits, re-labelling). The condition is
`_EVENTS_VIEW_CONDITION` in models/jobs.py; it COALESCEs NULL metadata (some domains store
none), so every other domain is unaffected. `/jobs/{job_id}/results` (signal-herald's read)
is NOT filtered: it returns one job's rows exactly as stored.

## Classification Retention (Postgres)

`agent_classifications` has no built-in expiry - rows persist indefinitely by default,
same as news-retrieval's `articles` table. One `source_type` value has an explicit
weekly cleanup job; every other `source_type` is retained forever.

Manual run: `python -m src expire-classifications --source-type <type> --days <n>` -
deletes `agent_classifications` rows for one `source_type` published more than `--days`
days ago (default 30); rows with a NULL `published` date are never deleted (fail-open,
no reliable age to judge them by) - same rule news-retrieval's `expire-articles` uses.

| `source_type` | Domain classified | Retention | Schedule (CloudWatch) | Rationale |
|---|---|-----------|------------------------|-----------|
| `news` | `ai_news` (`NEWS_DOMAIN`) | 180 days | Sunday 07:30 UTC | Deliberately longer than news-retrieval's 30-day `ai_news` article retention - classification is allowed to outlive its source article |

This schedule runs 1 hour after its corresponding `news_retrieval_ai_news_expire_weekly`
job (06:30 UTC - see `news-retrieval/CLAUDE.md`), so a classification is only ever expired
after its source article has already been deleted in news-retrieval, never the other way
around - the longer window just means the classification survives on its own for a while
after its source article is gone, before its own expiry catches up. `sec_filing` and
`taiwan_market_signal` are intentionally excluded - neither is driven by a
news-retrieval article with an expiry (SEC filing metadata is a permanent record;
`taiwan_market_signal` has no source-side expiry today).

## Guidance

- Use the Jira board (project key `CON`) to track and reference cards
- `category_candidates.py` is parked - re-enable by passing `category_hints` into `classify_article()` if category errors appear in production

## Maintenance

- Do not modify the Jira Board, Guidance, or Maintenance sections unless explicitly asked
