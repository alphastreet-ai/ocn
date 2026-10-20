"""Environment-variable configuration for signal-detection-agent."""
import os
from pathlib import Path

OPENAI_API_KEY: str = os.environ.get("OPENAI_API_KEY", "")
OPENAI_BASE_URL: str = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_MODEL: str = os.environ.get("SIGNAL_DETECTION_MODEL") or os.environ.get("OPENAI_MODEL") or "gpt-4.1"
OPENAI_MODEL_V2: str = os.environ.get("SIGNAL_DETECTION_MODEL_V2") or OPENAI_MODEL
SEC_FILING_MODEL: str = os.environ.get("SEC_FILING_MODEL") or OPENAI_MODEL
OPENAI_TIMEOUT: int = int(os.environ.get("OPENAI_TIMEOUT", "90"))
OPENAI_MAX_ATTEMPTS: int = int(os.environ.get("OPENAI_MAX_ATTEMPTS", "2"))
CLASSIFY_CONCURRENCY: int = int(os.environ.get("CLASSIFY_CONCURRENCY", "8"))

NEWS_RETRIEVAL_URL: str = os.environ.get("NEWS_RETRIEVAL_URL", "http://news-retrieval:8000")
NEWS_RETRIEVAL_SERVICE_CALLER: str = os.environ.get(
    "NEWS_RETRIEVAL_SERVICE_CALLER",
    "eyJzdWIiOiAwLCAicm9sZSI6ICJhZG1pbiIsICJkb21haW5zIjogW119",
)
# 30min: an ai_news run normally takes 4-7min, but the old 10min default left
# almost no headroom - a slow upstream day pushed one to 13.7min on 2026-09-30
# and timed out here, failing that day's signal-herald digest even though the
# run completed fine minutes later. Staging overrides this explicitly too.
PIPELINE_POLL_TIMEOUT_SECS: int = int(os.environ.get("PIPELINE_POLL_TIMEOUT_SECS", "1800"))

WEB_SEARCH_PROVIDER: str = os.environ.get("WEB_SEARCH_PROVIDER", "duckduckgo")
WEB_SEARCH_API_KEY: str | None = os.environ.get("WEB_SEARCH_API_KEY") or None

FILING_CLASSIFY_CONCURRENCY: int = int(os.environ.get("FILING_CLASSIFY_CONCURRENCY", "5"))
SEC_FILING_DOMAIN: str = os.environ.get("SEC_FILING_DOMAIN", "sec_filing")

PROMPTS_DIR: Path = Path(__file__).parent.parent / "prompts"
DEFAULT_PROMPT: Path = PROMPTS_DIR / "ai_universe_signal_classifier_v1.txt"
DEFAULT_PROMPT_V2: Path = PROMPTS_DIR / "ai_universe_signal_classifier_v2_refine.txt"

TAIWAN_SIGNAL_DOMAIN: str = os.environ.get("TAIWAN_SIGNAL_DOMAIN", "taiwan_market_signal")
KOREA_SIGNAL_DOMAIN: str = os.environ.get("KOREA_SIGNAL_DOMAIN", "korea_market_signal")
JAPAN_SIGNAL_DOMAIN: str = os.environ.get("JAPAN_SIGNAL_DOMAIN", "japan_market_signal")
CHINA_SIGNAL_DOMAIN: str = os.environ.get("CHINA_SIGNAL_DOMAIN", "china_market_signal")
NEWS_DOMAIN: str = os.environ.get("NEWS_DOMAIN", "ai_news")
GEOPOLITICAL_SIGNAL_DOMAIN: str = os.environ.get("GEOPOLITICAL_SIGNAL_DOMAIN", "geopolitical_news")
# Stage B is a forced one-word HIGH/WEAK call, but it is also the stage
# whose verdict is IRREVERSIBLE - a WEAK row never reaches Stage C, so
# nothing downstream can recover a signal this call gets wrong.
#
# Defaults to SEC_FILING_MODEL rather than to
# SIGNAL_DETECTION_MODEL_V2, which this deployment sets to gpt-4o-mini.
# That chain made the geopolitical domain's accuracy a side effect of a
# variable owned by the AI-universe second pass: retuning that for cost
# would silently downgrade this stage, with no test to catch it.
#
# SEC_FILING_MODEL is the right tier to share - it is the variable this
# service already uses for reading primary documents (SEC filings), and
# Federal Register notices are the same kind of text. Both stages here
# follow it so the model stays configurable per environment rather than
# hardcoded, and the two never drift apart.
_GEOPOLITICAL_DEFAULT_MODEL = SEC_FILING_MODEL

GEOPOLITICAL_SIGNAL_MODEL: str = (
    os.environ.get("GEOPOLITICAL_SIGNAL_MODEL") or _GEOPOLITICAL_DEFAULT_MODEL
)
GEOPOLITICAL_SIGNAL_STAGE_B_DAILY_CAP: int = int(
    os.environ.get("GEOPOLITICAL_SIGNAL_STAGE_B_DAILY_CAP", "1000")
)
# Raised 50 -> 200. Still a safety tripwire against a bug flooding the
# worklist, not a cost control (same reasoning as Stage B's cap above) -
# the headroom just got wider. Measured against the full stored staging
# history (5,487 rows): Stage B has promoted 19 rows to HIGH in total,
# peaking at 3 in a day, so the old 50 had never fired once (every run
# logged capped=0) and 200 is not expected to either. Sized to stay a
# tripwire after the Stage A allowlist expansion (32 -> 45 domains), which
# recovers ~126 historical drops and is projected to roughly double the
# HIGH rate to 3-4/day - still two orders of magnitude below this ceiling.
#
# Raised 200 -> 500 on 2026-10-20. The driver is RE-TAGGING, not daily
# volume: a change to the channel vocabulary (subsidies/regulation added
# the same day) means re-running Stage C over every stored signal, and
# the cap counts calls made today, so a re-tag pass and that day's
# normal run share one budget. Measured at the time: 68 signals stored,
# 23 calls already made that day - comfortable at 200, but the margin
# disappears once the pool grows or a re-tag is repeated after a fix.
# 500 keeps this a tripwire against a runaway loop rather than a limit
# real work has to plan around.
GEOPOLITICAL_SIGNAL_STAGE_C_DAILY_CAP: int = int(
    os.environ.get("GEOPOLITICAL_SIGNAL_STAGE_C_DAILY_CAP", "500")
)
# Stage B and Stage C (both single-model-call-per-row) share
# CLASSIFY_CONCURRENCY and the module-level _executor in controllers/run.py -
# same pool the news domain already uses, no dedicated knob needed since
# their own daily caps (1000/500) bound total volume anyway.

# Taiwan's translate/gdelt-relevance loops (pipeline/taiwan_signal_classifier.py)
# - both are single blocking HTTP calls per item, same shape as the other
# CLASSIFY_CONCURRENCY knobs.
TAIWAN_CLASSIFY_CONCURRENCY: int = int(os.environ.get("TAIWAN_CLASSIFY_CONCURRENCY", "5"))
# Taiwan material announcements: rating + English summary, one SEC_FILING_MODEL
# call per filing that is not fixed NOISE (pipeline/taiwan_announcement_classifier.py).
TAIWAN_ANNOUNCEMENT_PROMPT: Path = PROMPTS_DIR / "taiwan_announcement_classifier_v1.txt"

# research-universe: Stage C's category->ticker map (Layer 3) is generated
# by a separate periodic refresh job that calls GET /companies, not fetched
# live during Stage C itself - same "free, deterministic lookup" property
# the spec intends. New to this service (news-retrieval already has the
# same pair, same naming, for its own company_news domain).
RESEARCH_UNIVERSE_URL: str = os.environ.get("RESEARCH_UNIVERSE_URL", "http://research-universe:8007")
RESEARCH_UNIVERSE_API_KEY: str | None = os.environ.get("RESEARCH_UNIVERSE_API_KEY") or None

GEOPOLITICAL_SIGNAL_STAGE_C_PROMPT: Path = PROMPTS_DIR / "geopolitical_signal_stage_c_v1.txt"
# Stage C needs a stronger tier than Stage B, and this was measured
# rather than assumed. Stage B asks one binary and returns one word;
# Stage C returns six structured fields and must cross-check the sector
# a document names against the scope lines of 19 categories in a
# ~14,000-character prompt.
#
# On gpt-4o-mini (Stage B's default) that cross-check was being skipped:
# 4 of 11 real cases passed. "Chile restricts lithium mining exports"
# returned no category and `sector_not_named` even though the model's
# OWN summary called lithium "this critical mineral" and Raw Materials &
# Critical Minerals lists lithium by name - so step 1 found the sector
# and step 2 never checked it. Asset tagging degraded with it (that same
# row came back `natural_gas`).
#
# The same prompt on gpt-4.1 passes 10 of 11, including every
# sub-category case the taxonomy implies: lithium and aluminum to Raw
# Materials, submarine cables and satellite equipment to Telecom,
# uranium to Nuclear, pharmaceutical APIs to Life Sciences. The
# taxonomy was never the problem.
#
# Affordable because of where it sits: Stage A's allowlist and Stage B's
# verdict mean only a handful of rows a day reach this call.
#
# Pinned to the same explicit model as Stage B rather than chained to
# OPENAI_MODEL - the chain resolved to whatever base model the
# deployment happened to set, so "which model tags our signals" was not
# answerable from this file alone.
GEOPOLITICAL_SIGNAL_STAGE_C_MODEL: str = (
    os.environ.get("GEOPOLITICAL_SIGNAL_STAGE_C_MODEL")
    or _GEOPOLITICAL_DEFAULT_MODEL
)

# Macro Signal Backbone: INTERPRET is one short structured call per
# collapsed event (not per series/article). CONFIRMED LIVE over a real
# 2-week run: OPENAI_MODEL_V2 (gpt-4o-mini) produced a malformed/empty
# "assets" array on ~28% of real calls (5 of 18 collapsed events),
# intermittent rather than reproducible from the event shape alone - the
# validator correctly rejects it, but the event is then silently
# dropped with no fallback (see interpret_event's own no-retry
# rationale). Falls back to SEC_FILING_MODEL instead - a stronger model
# already proven reliable for this codebase's other structured-JSON
# extraction call, not the cheaper V2 tier Taiwan's forced-one-word
# calls use (a wrong word there just means "safely defaults to WEAK";
# a malformed macro event here means the event is lost entirely, a
# real cost this domain isn't willing to pay for the cheaper model).
MACRO_SIGNAL_DOMAIN: str = os.environ.get("MACRO_SIGNAL_DOMAIN", "macro_signal")
MACRO_SIGNAL_MODEL: str = os.environ.get("MACRO_SIGNAL_MODEL") or SEC_FILING_MODEL

# Japan Signals' every real LLM call (translate_japan_articles' field
# translation, classify_press's J7 relevance judgment, and
# the Japan summary's COMPANY LEVEL synthesis, since removed) - same
# reasoning as MACRO_SIGNAL_MODEL just above: CONFIRMED LIVE 2026-09-30
# OPENAI_MODEL_V2 (gpt-4o-mini) reliably fabricated numbers when asked to
# restate already-final classified data (WATCHING day-counts, INDUSTRY
# LEVEL's month/reading/spread) - a different wrong number on repeat
# calls at temperature 0, even after the prompt explicitly demanded
# verbatim copying (the Japan summary's own docstrings recorded this, where
# INDUSTRY LEVEL/WATCHING were subsequently moved out of the LLM path
# entirely). Falls back to SEC_FILING_MODEL - this codebase's other
# proven-reliable structured-extraction tier - rather than the cheaper V2
# tier, same trade-off MACRO_SIGNAL_MODEL already made: a wrong word in a
# forced one-word call (Taiwan's own use of V2) just defaults to WEAK,
# but a fabricated number in a trader-facing summary or translation is a
# real, worse cost this domain isn't willing to pay for the cheaper
# model. Independently tunable via its own env var, same pattern as every
# other *_MODEL constant here.
JAPAN_SIGNAL_MODEL: str = os.environ.get("JAPAN_SIGNAL_MODEL") or SEC_FILING_MODEL
# Every china_market_signal LLM call - C1's binding verdict and the
# translation pass - routes through here. Same reasoning as
# JAPAN_SIGNAL_MODEL and MACRO_SIGNAL_MODEL above: this domain's two
# model uses are a structured extraction (BINDING/NON-BINDING/UNCLEAR
# plus named entities and an effective date) and a translation, and both
# are the kind of call where a fabricated value is a real cost rather
# than a defaulted WEAK. C1 already demonstrated this concretely - the
# model returned a 发文日期 as an effective date, which the spec forbids,
# and a guard now drops any date the pattern matcher cannot corroborate.
# Independently tunable via its own env var, same pattern as every other
# *_MODEL constant here.
CHINA_SIGNAL_MODEL: str = os.environ.get("CHINA_SIGNAL_MODEL") or SEC_FILING_MODEL
MACRO_SIGNAL_PROMPT: Path = PROMPTS_DIR / "macro_signal_interpret_v1.txt"
# One blocking HTTP call per collapsed event, same shape as
# TAIWAN_CLASSIFY_CONCURRENCY - independently tunable since event volume
# (a handful/day) differs a lot from Taiwan's per-article volume.
MACRO_SIGNAL_CLASSIFY_CONCURRENCY: int = int(os.environ.get("MACRO_SIGNAL_CLASSIFY_CONCURRENCY", "5"))
# Rolling 2-year window per spec - each pipeline run re-pulls this much
# history per series rather than maintaining an incremental watermark
# (v1 simplicity tradeoff, see plan history).
MACRO_SIGNAL_ZSCORE_WINDOW_DAYS: int = int(os.environ.get("MACRO_SIGNAL_ZSCORE_WINDOW_DAYS", "730"))
