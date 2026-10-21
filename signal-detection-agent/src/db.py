"""Database access layer for the signal-detection-agent service."""
import os

import psycopg2
import psycopg2.extras

import db_utils
from db_utils import DuplicateError, get_db, transaction  # noqa: F401

__all__ = ["DuplicateError", "get_db", "transaction", "init_db"]


def _new_connection() -> db_utils._Connection:
    """Open a new PostgreSQL connection."""
    raw = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "signal-detection"),
        user=os.environ.get("POSTGRES_USER", "signal-detection"),
        password=os.environ.get("POSTGRES_PASSWORD", ""),
        sslmode=os.environ.get("PGSSLMODE", "prefer"),
    )
    raw.cursor_factory = psycopg2.extras.RealDictCursor
    return db_utils._Connection(raw)


db_utils.configure(_new_connection)


def init_db() -> None:
    """Create agent tables if they do not exist."""
    with get_db() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_job_statuses (
                status TEXT PRIMARY KEY
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_jobs (
                id           SERIAL PRIMARY KEY,
                news_run_id  INTEGER,
                domain       TEXT NOT NULL,
                status       TEXT NOT NULL REFERENCES agent_job_statuses(status),
                article_count INTEGER NOT NULL DEFAULT 0,
                created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                completed_at TIMESTAMPTZ
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS agent_classifications (
                id               SERIAL PRIMARY KEY,
                job_id           INTEGER NOT NULL REFERENCES agent_jobs(id),
                article_id       INTEGER,
                url              TEXT,
                title            TEXT,
                signal_detection TEXT NOT NULL
                    CHECK (signal_detection IN ('signal', 'weak_signal', 'noise')),
                signal_score     NUMERIC(5,4) NOT NULL,
                signal_reason    TEXT,
                materiality      TEXT NOT NULL
                    CHECK (materiality IN ('high', 'medium', 'low', 'none')),
                category         TEXT NOT NULL,
                entities_json    TEXT NOT NULL DEFAULT '[]',
                stored_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD COLUMN IF NOT EXISTS base_signal_detection TEXT,
                ADD COLUMN IF NOT EXISTS base_signal_score FLOAT,
                ADD COLUMN IF NOT EXISTS novelty_basis TEXT,
                ADD COLUMN IF NOT EXISTS novelty TEXT,
                ADD COLUMN IF NOT EXISTS confidence_basis TEXT,
                ADD COLUMN IF NOT EXISTS confidence TEXT,
                ADD COLUMN IF NOT EXISTS refinement_reason TEXT
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD COLUMN IF NOT EXISTS entity_names_normalized TEXT[] DEFAULT '{}'
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_entity_names_gin
                ON agent_classifications USING GIN(entity_names_normalized)
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD COLUMN IF NOT EXISTS pre_verification_score FLOAT,
                ADD COLUMN IF NOT EXISTS verification_qa JSONB
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD COLUMN IF NOT EXISTS published TIMESTAMPTZ
        """)
        conn.execute("""
            UPDATE agent_classifications
            SET entity_names_normalized = ARRAY(
                SELECT LOWER(e->>'name')
                FROM jsonb_array_elements(entities_json::jsonb) e
                WHERE e->>'name' IS NOT NULL
            )
            WHERE entity_names_normalized IS NULL OR entity_names_normalized = '{}'
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ALTER COLUMN category DROP NOT NULL,
                ALTER COLUMN materiality DROP NOT NULL
        """)
        # category TEXT -> TEXT[] (news writes one value, geopolitical_signal
        # 0-3). This was a manual step in MIGRATION_geopolitical_metadata_cleanup.md
        # that staging never got, while the list-writing code shipped: psycopg
        # turned each list into an array LITERAL string ('{"Energy ..."}') in
        # the TEXT column, signal-herald matched none of them, and sent an empty
        # digest. Running it here makes the schema follow the code. Guarded on
        # the current type so it runs once. Rows written in the window before
        # this hold that literal - cast it back rather than wrapping it again.
        conn.execute("""
            DO $$
            BEGIN
                IF (SELECT data_type FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name = 'agent_classifications'
                      AND column_name = 'category') = 'text' THEN
                    ALTER TABLE agent_classifications
                        ALTER COLUMN category TYPE TEXT[]
                        USING CASE
                            WHEN category IS NULL THEN NULL
                            WHEN left(category, 1) = '{' AND right(category, 1) = '}'
                                THEN category::TEXT[]
                            ELSE ARRAY[category]
                        END;
                END IF;
            END $$
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD COLUMN IF NOT EXISTS source_type TEXT NOT NULL DEFAULT 'news',
                ADD COLUMN IF NOT EXISTS source_id TEXT
        """)
        # company_rank/company_percentile: added for a company-relative ranking
        # feature that was deprioritized before any code ever read or wrote
        # them - confirmed unreferenced anywhere outside this migration, so
        # dropping rather than leaving them as permanently-empty dead columns.
        conn.execute("""
            ALTER TABLE agent_classifications
                DROP COLUMN IF EXISTS company_rank,
                DROP COLUMN IF EXISTS company_percentile
        """)
        # source_type's allowed set grows as new domains are added (news, sec_filing,
        # company_specific, ...) - drop/recreate rather than ALTER,
        # since Postgres has no ALTER CHECK. Add new values here, not a new migration.
        conn.execute("""
            ALTER TABLE agent_classifications
                DROP CONSTRAINT IF EXISTS agent_classifications_source_type_check
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD CONSTRAINT agent_classifications_source_type_check
                    CHECK (source_type IN ('news', 'sec_filing', 'company_specific', 'taiwan_market_signal', 'geopolitical_signal', 'korea_market_signal', 'macro_signal', 'japan_market_signal', 'china_market_signal', 'middle_east_market_signal'))
        """)
        # signal_detection's allowed set is widened (same drop/recreate
        # pattern as source_type above) to add 'waiting' - geopolitical_signal
        # Stage A writes this for a survivor that passed the free rule-based
        # filters but hasn't been judged HIGH/WEAK by Stage B yet. Every other
        # source_type only ever writes 'signal'/'weak_signal'/'noise'; adding
        # 'waiting' doesn't change what any of them can write.
        conn.execute("""
            ALTER TABLE agent_classifications
                DROP CONSTRAINT IF EXISTS agent_classifications_signal_detection_check
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD CONSTRAINT agent_classifications_signal_detection_check
                    CHECK (signal_detection IN ('signal', 'weak_signal', 'noise', 'waiting'))
        """)
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD COLUMN IF NOT EXISTS concreteness FLOAT,
                ADD COLUMN IF NOT EXISTS economic_scale FLOAT
        """)
        # metadata: source-specific fields that don't warrant their own typed
        # column, same rationale as news-retrieval's articles.metadata - the
        # taiwan_market_signal source_type is the first to use this (revenue
        # rank, clause-code reason, translated text, etc.), but it's a plain
        # JSONB bag any future source_type can use the same way rather than
        # adding narrow columns per field.
        conn.execute("""
            ALTER TABLE agent_classifications
                ADD COLUMN IF NOT EXISTS metadata JSONB
        """)
        # form_type/item_codes/filing_filed_at were sec_filing-only typed
        # columns, added before metadata existed - now redundant with it,
        # same JSONB-bag rationale as company_rank/company_percentile above.
        # insert_filing_classification() writes these into metadata instead.
        conn.execute("""
            ALTER TABLE agent_classifications
                DROP COLUMN IF EXISTS form_type,
                DROP COLUMN IF EXISTS item_codes,
                DROP COLUMN IF EXISTS filing_filed_at
        """)
        # signal_score was NOT NULL for every source_type, on the assumption
        # every classification produces a confidence value. GDELT's Stage B
        # relevance check (taiwan_market_signal, source_category=gdelt) is a
        # forced one-word HIGH/WEAK call that never elicits a real confidence
        # number - NULL says "not measured" instead of a fabricated constant.
        # Other source_types keep writing a real score; this only widens what
        # the column allows.
        conn.execute("""
            ALTER TABLE agent_classifications
                ALTER COLUMN signal_score DROP NOT NULL
        """)
        # agent_classifications has no unique constraint at all today, so
        # every insert_*_classification function's "ON CONFLICT DO NOTHING"
        # has nothing to conflict against - confirmed live that re-inserting
        # the same row creates a duplicate rather than a no-op. That's a
        # pre-existing gap across all source types; fixing it here only for
        # taiwan_market_signal (source_id is a real natural key for this
        # source - ticker+period for revenue, ticker+timestamp for material
        # announcements - unlike the other source types, which don't
        # consistently populate source_id). A partial index scopes this fix
        # to taiwan_market_signal only, leaving news/sec_filing
        # untouched rather than risking their existing behavior.
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_agent_classifications_taiwan_source_id
                ON agent_classifications (source_type, source_id)
                WHERE source_type = 'taiwan_market_signal'
        """)
        # geopolitical_signal: article_id is a real natural key here (unlike
        # source_id above, which most source_types don't consistently
        # populate) - one row per article_id is exactly Stage A's "already
        # classified, skip" rule from the spec, enforced by the database
        # rather than just application logic, and what makes a crashed run
        # or cursor replay safe to resume without reclassifying anything.
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_agent_classifications_geopolitical_signal_article_id
                ON agent_classifications (source_type, article_id)
                WHERE source_type = 'geopolitical_signal'
        """)
        # korea_market_signal: same condition and same fix as
        # taiwan_market_signal above - source_id is a real, consistently
        # populated natural key for every korea_signal_classifier.py
        # classify_* function (DART's own rcept_no for S2-S6, the article's
        # own url for S7's qualification_news - see classify_qualification_news),
        # so insert_korea_signal_classification's own "ON CONFLICT DO
        # NOTHING" had nothing to conflict against until this index exists,
        # same pre-existing gap as every other source_type not listed here.
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_agent_classifications_korea_source_id
                ON agent_classifications (source_type, source_id)
                WHERE source_type = 'korea_market_signal'
        """)
        # macro_signal: source_id is a deterministic natural key built from
        # (release_id, knowledge_time) - one row per collapsed event (or
        # per suppressed/non-surfaced candidate, stored for audit per the
        # spec's "nothing is ever deleted" rule - see
        # insert_macro_signal_event's docstring). Same fix as
        # taiwan_market_signal/korea_market_signal above.
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_agent_classifications_macro_signal_source_id
                ON agent_classifications (source_type, source_id)
                WHERE source_type = 'macro_signal'
        """)
        # japan_market_signal: same fix as taiwan_market_signal/
        # korea_market_signal above - source_id is J1's own synthetic
        # irbank-financials:// url (news-retrieval's own dedup key,
        # already globally unique per filing), a real, consistently
        # populated natural key for classify_forecast_revision.
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_agent_classifications_japan_source_id
                ON agent_classifications (source_type, source_id)
                WHERE source_type = 'japan_market_signal'
        """)
        # china_market_signal: same fix as every source_type above -
        # source_id is the article's own url from news-retrieval, which is
        # already globally unique there (the real article URL for policy
        # and press rows, a synthetic cninfo-filing:// / hkex-filing:// /
        # comtrade-china:// key for the rest), so it is a real,
        # consistently populated natural key for every China classifier.
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_agent_classifications_china_source_id
                ON agent_classifications (source_type, source_id)
                WHERE source_type = 'china_market_signal'
        """)
        # middle_east_market_signal: same as China - source_id is the
        # article's own url from news-retrieval, globally unique there (a
        # real url for WAM/SPA/BIS/Substack rows, a synthetic er-me:// /
        # me-federal-register:// / sec-stake:// key for the rest).
        conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS
                idx_agent_classifications_middle_east_source_id
                ON agent_classifications (source_type, source_id)
                WHERE source_type = 'middle_east_market_signal'
        """)
        # metadata->>'ticker' filtering (list_all_results/list_results) would
        # otherwise sequential-scan the whole table on every ticker-filtered
        # call - a partial index scoped to sec_filing (the only source_type
        # that populates ticker today) matches the UPPER()-wrapped comparison
        # those queries actually run, so it's usable by the planner rather
        # than requiring a separate un-wrapped index that the query can't hit.
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_agent_classifications_sec_filing_ticker
                ON agent_classifications (UPPER(metadata->>'ticker'))
                WHERE source_type = 'sec_filing'
        """)
        # metadata->>'code' - the Japan Signals equivalent of ticker above
        # (japan_market_signal rows never populate metadata.ticker - every
        # J1-J7/WATCHING classifier stores the TSE 4-digit code, or Kioxia's
        # own "285A", under metadata.code instead - see japan_ticker_
        # universe.py's own JAPAN_TICKER_UNIVERSE). Same partial-index
        # pattern as the sec_filing ticker index above, scoped to
        # japan_market_signal, for the same reason: matches the UPPER()-
        # wrapped comparison list_all_results actually runs (a real filing
        # code is always uppercase already, but matched case-insensitively
        # in case a caller passes lowercase).
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_agent_classifications_japan_code
                ON agent_classifications (UPPER(metadata->>'code'))
                WHERE source_type = 'japan_market_signal'
        """)
        # geopolitical_signal Stage C: one row per US-listed company, built
        # by a separate periodic refresh job that calls research-universe's
        # GET /companies (see adapters/research_universe_client.py +
        # controllers/category_map_refresh.py), not fetched live during
        # Stage C itself - keeps both layers below a free, deterministic
        # lookup, same spirit as the spec's own design intent.
        #
        # Serves BOTH Stage C layers from one refresh/one table, rather than
        # a category-keyed table serving only Layer 3: Layer 1 (direct
        # name-in-headline match) needs company_name+ticker: Layer 3
        # (category -> ticker expansion) needs categories+ticker - both are
        # just different WHERE/SELECT shapes over the same underlying data,
        # so there is no reason to fetch or store it twice. categories is an
        # array (a company can carry more than one - see Stage C plan's
        # multi-category decision: shown under every matching category, not
        # forced into one "primary").
        conn.execute("""
            CREATE TABLE IF NOT EXISTS geopolitical_signal_companies (
                ticker        TEXT PRIMARY KEY,
                company_name  TEXT NOT NULL,
                categories    TEXT[] NOT NULL DEFAULT '{}',
                refreshed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # Layer 1's word-boundary match is driven by company_name length
        # (longest-first, so "Nvidia Corporation" beats an accidental
        # substring match before a shorter name would) - this index isn't
        # needed for correctness (Layer 1 loads the whole table into memory
        # once per Stage C run, not per row), kept only for any future
        # direct SQL access that wants the same ordering without an
        # application-side sort.
        conn.execute("""
            CREATE INDEX IF NOT EXISTS
                idx_geopolitical_signal_companies_name_length
                ON geopolitical_signal_companies (length(company_name) DESC)
        """)
        # FOMC scheduled meeting dates, for DFF's inter-meeting move
        # detection (spec section 7, Note A). Refreshed from the Fed's own
        # calendar.json feed (real, structured, fetchable - see
        # pipeline/macro_signal_fomc_calendar.py's docstring for why FRED's
        # own release-dates endpoint could NOT be used instead) by a
        # scheduled job (refresh-fomc-calendar CLI command), not hand-typed
        # SQL - same refresh-cache shape as geopolitical_signal_companies
        # above, not a source of truth someone edits directly.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fomc_meeting_dates (
                meeting_date  DATE PRIMARY KEY,
                source        TEXT NOT NULL DEFAULT 'federalreserve.gov/json/calendar.json',
                refreshed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # Japan Signals spec Section 4.1's own stored pattern per company
        # ("how many times it typically revises in a year, in which
        # direction, by roughly how much, and at which point in the
        # year") - same refresh-cache shape as geopolitical_signal_
        # companies/fomc_meeting_dates above: computed once by a scheduled
        # job (refresh-japan-habits CLI command, see pipeline/
        # japan_signal_classifier.py's compute_forecast_habit) from the
        # company's own 5-year stored revision history, NOT recomputed
        # inline on every classify_forecast_revision call - explicit user
        # instruction 2026-09-28 ("save those against each company... then
        # when a new revision comes in, compare it to that company's own
        # four numbers instead of a fixed threshold").
        #
        # typical_month_1/2/3 hold up to 3 calendar months (1-12) this
        # company's real revisions most commonly land in - "what point in
        # the year does it usually happen" (the 4th stored number) is
        # naturally a SET of months, not one value, since real filers
        # revise at multiple points across a fiscal year (confirmed live:
        # Advantest's own real history clusters at Jan/Jul/Oct, three
        # distinct months, not one) - stored as up to 3 nullable columns
        # rather than an array so a plain SELECT reads directly without an
        # unnest, matching this table's own read pattern (one row per
        # company, read whole).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS japan_company_habits (
                code                  TEXT PRIMARY KEY,
                company               TEXT NOT NULL,
                revisions_per_year    NUMERIC,
                typical_size_pct      NUMERIC,
                typical_direction     TEXT,
                typical_month_1       SMALLINT,
                typical_month_2       SMALLINT,
                typical_month_3       SMALLINT,
                sample_size           INTEGER NOT NULL,
                is_trusted            BOOLEAN NOT NULL,
                computed_from_years   INTEGER NOT NULL,
                refreshed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # typical_size_mad_pct: the median absolute deviation (MAD) of a
        # company's own genuine revision sizes - see classify_forecast_
        # revision's own rule-6 comment for why this, not a fixed
        # multiple of the median, is the real bar for "unusually large
        # even for this volatile company" (real, confirmed-live finding
        # 2026-09-30: a tuned multiplier like 1.5x-typical was an
        # arbitrary number picked to hit a target reduction, not a
        # principled statistic; MAD is the same robust-outlier measure
        # this module already uses elsewhere for this exact kind of
        # small, fat-tailed per-company data - see the module's own
        # median-vs-mean-vs-MAD comparison work). Added via ALTER TABLE
        # rather than baked into the CREATE TABLE above, same "existing
        # table, new column" convention every other ADD COLUMN IF NOT
        # EXISTS in this file already follows.
        conn.execute("""
            ALTER TABLE japan_company_habits
                ADD COLUMN IF NOT EXISTS typical_size_mad_pct NUMERIC
        """)
        # A company's own baseline for one measured quantity, cached so
        # the daily classify pass does not have to rebuild it.
        #
        # WHY THIS EXISTS. China's C2 and C3 judge a figure against that
        # COMPANY's own history - "unusual for Hygon", not "unusual for
        # Chinese semiconductors". Computing that needs years of filings,
        # but the scheduled classify pass pools a single day. Without a
        # cache the daily run sees two or three observations per company,
        # falls below its own trust floor and issues no verdict at all,
        # while a one-off backfill over four years produces signals -
        # the same code giving different answers depending on how it was
        # invoked. The baseline is refreshed periodically from the full
        # history and read, not recomputed, by the daily pass. Same split
        # Japan already uses between refresh-japan-habits and
        # classify-japan-signals.
        #
        # GENERAL BY DESIGN, CHINA-ONLY IN PRACTICE. The columns are
        # market-agnostic because japan_progress_habits is the same
        # shape - keyed (code, period_type), holding a central value, a
        # sample size and a trust flag, for the same stated reason that
        # a company's half-year and quarterly numbers "are genuinely
        # different numbers". It is the intended second tenant
        # (domain='japan_market_signal', metric='progress_pct', mad
        # NULL) and was deliberately NOT migrated in the same change:
        # it is written by a monthly job and read by the Japan
        # classifier, so a bad move would surface a month later, and it
        # cannot be verified locally because the table is empty outside
        # staging.
        #
        # `metric` names WHAT is measured, since one company carries
        # several independent baselines - China alone needs revenue
        # growth (C2) and commitment size (C3), which share a key but
        # not a unit. `period_type` is '' where the measure has no
        # reporting period.
        #
        # `mad` is the median absolute deviation, not a standard
        # deviation: these distributions are small and fat-tailed, and
        # a standard deviation is computed from the very outliers it is
        # meant to detect. Measured on real stored data, one company's
        # +4,348% quarter scored 2.15 standard deviations and 48 MADs.
        #
        # `is_trusted` is the floor decision made at refresh time, so a
        # reader never has to re-derive it: false means the classifier
        # declined to judge rather than that the company was ordinary.
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_signal_baselines (
                domain        TEXT NOT NULL,
                code          TEXT NOT NULL,
                metric        TEXT NOT NULL,
                period_type   TEXT NOT NULL DEFAULT '',
                company       TEXT,
                median        NUMERIC,
                mad           NUMERIC,
                sample_size   INTEGER NOT NULL,
                is_trusted    BOOLEAN NOT NULL,
                computed_from TEXT,
                refreshed_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (domain, code, metric, period_type)
            )
        """)
        # Answers "every baseline for this domain" in one scan - the
        # read the classifier makes once per run.
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_market_baselines_domain
                ON market_signal_baselines (domain, metric)
        """)

        # Japan Signals spec Section 6.2's own stored comparison point for
        # J2 (results against forecast): "this company's typical progress
        # at the same point in previous years" - same refresh-cache
        # rationale as japan_company_habits above, kept as its own table
        # (not folded into japan_company_habits) because the natural key
        # is (code, period_type), not code alone - a company's typical
        # progress at the half-year mark and at an arbitrary quarter mark
        # are genuinely different numbers, confirmed live 2026-09-29 they
        # differ meaningfully for the same company (e.g. Ibiden: ~56% by a
        # typical quarter mark vs. ~62% by its typical half-year mark).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS japan_progress_habits (
                code                  TEXT NOT NULL,
                period_type           TEXT NOT NULL,
                company               TEXT NOT NULL,
                typical_progress_pct  NUMERIC,
                sample_size           INTEGER NOT NULL,
                is_trusted            BOOLEAN NOT NULL,
                refreshed_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (code, period_type)
            )
        """)
        # J5 (capacity commitment) and J6 (ownership/capital policy) need
        # each company's total assets and shares-outstanding as the
        # denominators for Japan Signals spec Section 6.5's own "investment
        # >= 10% of total assets" and "buyback >= 5% of shares outstanding"
        # rules - see japan_signal_classifier.classify_capacity_and_
        # ownership's own docstring. Unlike japan_company_habits/
        # japan_progress_habits above, this is NOT a habit computed from
        # stored history - it is a simple cache of the LATEST value
        # news-retrieval's irbank_company_reference source reported, kept
        # here for the same reason those two tables exist (a small,
        # cheap-to-read table read on every classification run, refreshed
        # on its own schedule, rather than every classify call re-fetching
        # or recomputing it inline).
        conn.execute("""
            CREATE TABLE IF NOT EXISTS japan_company_reference (
                code                     TEXT PRIMARY KEY,
                company                  TEXT NOT NULL,
                total_assets_jpy_millions NUMERIC,
                shares_outstanding       BIGINT,
                fiscal_period            TEXT,
                refreshed_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
