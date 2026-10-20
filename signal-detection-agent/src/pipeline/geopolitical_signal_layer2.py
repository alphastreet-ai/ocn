"""Stage C, Layer 2: one model call per HIGH article - channel, assets,
impacted_categories, entities, event_date, generated_summary.

Same raw-HTTP-via-urllib pattern as Stage B (pipeline/geopolitical_signal_
stage_b.py) - no openai SDK dependency, consistent with every other model
call in this service.

Fail-open on any parse/validation failure: per the spec, "any JSON parse
failure stores the item as HIGH with null tags rather than dropping it" -
a tagging failure must never delete or hide a real signal. This module
never raises for a malformed model response; callers get back a dict with
every field set to None on failure, same shape as a successful parse.
"""
from __future__ import annotations

import json
import logging
import re
import time
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pipeline.categories import ALLOWED_CATEGORIES
from pipeline.classifier import extract_json_object

logger = logging.getLogger(__name__)

# subsidies / regulation added 2026-10-20: 11 of 57 HIGH rows had been
# forced into a channel that misdescribed them - state chip schemes filed
# as "trade", data-centre moratoriums as "energy" or "trade".
# diplomacy added 2026-10-20 (user decision): talks, summits, visits,
# cooperation and statements between states - geopolitical news that uses
# no other instrument had no channel at all.
_VALID_CHANNELS = frozenset({"energy", "trade", "sanctions", "shipping", "conflict", "subsidies",
                             "regulation", "diplomacy"})
# Spellings of a valid channel (singular/plural, adjective, the
# instrument's everyday name) - rewritten, never guessed across meanings.
_CHANNEL_ALIASES = {
    "sanction": "sanctions",
    "subsidy": "subsidies",
    "regulations": "regulation",
    "regulatory": "regulation",
    "tariff": "trade",
    "tariffs": "trade",
    "military": "conflict",
    "diplomatic": "diplomacy",
    "security": "conflict",
}
_NO_CHANNEL = frozenset({"none", "null", "n/a", ""})
# One naming convention: the exposure's full name, lowercase, with
# underscores. The list previously mixed three - full nouns
# (crude_oil, natural_gas), abbreviations (semis, autos) and bare words
# - so a model tagging a car-tariff story had to guess between "autos",
# "auto" and "automotive", and anything but the exact string was
# discarded. "semis" was also genuinely ambiguous between semiconductors
# and semi-trailer trucks.
#
# NOTE: pipeline/macro_signal_interpret.py keeps its own asset list and
# still spells these "semis" / has no automotive. The two vocabularies
# overlap but are not the same set (macro has real_rates, credit,
# housing; geopolitical has crude_oil, gold, defense), so they are not
# unified here - this rename deliberately diverges geopolitical only.
_VALID_ASSETS = frozenset({
    "crude_oil", "natural_gas", "gold", "usd", "treasuries",
    "semiconductors", "defense", "shipping_freight", "automotive",
    "agriculture", "broad_equities",
})

# Old spellings accepted and rewritten rather than discarded: rows
# stored before the rename hold them, and a model that has seen the old
# list may still emit them. Mapping them is free; dropping them would
# silently empty the field.
_ASSET_ALIASES = {
    "semis": "semiconductors",
    "semiconductor": "semiconductors",
    "chips": "semiconductors",
    "autos": "automotive",
    "auto": "automotive",
    "oil": "crude_oil",
    "gas": "natural_gas",
    "shipping": "shipping_freight",
    "freight": "shipping_freight",
    "equities": "broad_equities",
    "dollar": "usd",
}

# HTTP statuses worth a second attempt: the request never reached a
# model, or the gateway gave up waiting for one. Measured live - a single
# 504 on one row of a 45-row run left that row with every Stage C field
# null, and the immediate manual re-run of that same row succeeded on
# its first attempt. That is the whole case for retrying: the failure
# was in the gateway, not in the request.
#
# Deliberately NOT retried: 400/401/403/404 (the request itself is
# wrong, and will be wrong again), and any parse or validation failure.
# The latter is not guesswork - a production run of this codebase's
# filing classifier found 54 rows hitting the same parse failure twice,
# "every one of the 54 failed IDENTICALLY on both attempts". Retrying a
# bad response just buys the same bad response at double the price.
_TRANSIENT_HTTP_STATUSES = frozenset({408, 429, 500, 502, 503, 504})
_HTTP_MAX_ATTEMPTS = 3


def _post_with_retry(req: Request, timeout: float) -> dict[str, Any]:
    """POST and decode, retrying only TRANSPORT failures.

    Raises the last exception once attempts are exhausted, so the
    caller's existing fail-open handler stays the single place that
    decides what a permanently failed row looks like.

    Backoff is 1s then 2s. It sleeps inside the executor worker that
    owns this row, so a retrying row holds one CLASSIFY_CONCURRENCY slot
    for up to 3 extra seconds - immaterial at this domain's ~45 rows a
    day, and worth re-checking if that volume grows by an order of
    magnitude.
    """
    for attempt in range(1, _HTTP_MAX_ATTEMPTS + 1):
        try:
            with urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8", errors="ignore"))
        except HTTPError as exc:
            if exc.code not in _TRANSIENT_HTTP_STATUSES:
                raise
            if attempt == _HTTP_MAX_ATTEMPTS:
                raise
            logger.warning(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] transient HTTP %s on attempt"
                " %d/%d, retrying", exc.code, attempt, _HTTP_MAX_ATTEMPTS,
            )
        except (URLError, TimeoutError) as exc:
            # No status to inspect - a connection reset or a read that
            # ran past `timeout`. Same class as a 504: nothing about the
            # request says it will fail again.
            if attempt == _HTTP_MAX_ATTEMPTS:
                raise
            logger.warning(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] transport error on attempt"
                " %d/%d (%s), retrying", attempt, _HTTP_MAX_ATTEMPTS, exc,
            )
        time.sleep(2 ** (attempt - 1))
    raise RuntimeError("unreachable")  # pragma: no cover


_EMPTY_RESULT: dict[str, Any] = {
    "channel": None,
    "assets": None,
    "impacted_categories": None,
    "generated_summary": None,
    "entities": None,
    "event_date": None,
    "event_location": None,
}

# Where the event physically happened, as the text names it - one value
# or None. Paired with event_date above and answering the same shape of
# question: WHERE it occurred, as against where the document announcing
# it was issued. A Federal Register notice is published in Washington
# and that is almost never where anything happened.
#
# Expected to be None on a large share of rows, by design. A sanctions
# designation or an export-control rule is an act of law, not an event
# with a place - measured over one real pool, roughly half of this
# domain's signals are legal instruments of that kind. The prompt is
# explicit that the country a measure APPLIES TO is not the answer:
# that country is already an "entities" row of type "country", and
# repeating it here would state a party as if it were a place.
#
# Where it IS answerable - a strike, a sinking, a plant or data centre,
# a pipeline or strait - it is usually in the BODY and not the
# headline. Two real examples from one run: "Cargo ship carrying over
# 2,500 containers sinks after reported naval mine" names no place at
# all in its title, and "Federal officials tout $33B energy investment,
# massive data center in So..." is cut off before its state. Stage C
# already reads up to 24,000 characters of body for its other fields,
# so this costs output tokens and no extra call.
_MAX_EVENT_LOCATION_CHARS = 120

# The date the action was taken or took effect, which is frequently NOT
# the date the article was published. Federal Register notices are the
# clearest case: one published 13 October carried a determination issued
# 1 October, another a general licence issued 25 June. Storing only
# `published` put every one of them on the publication date and lost the
# fact a reader needs - when the measure actually started.
_EVENT_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Companies and organisations the ARTICLE names, as opposed to the
# companies a category happens to contain. The distinction is the point:
# impacted_companies_by_category expanded a matched category into every
# member of it - one EU sanctions row produced 123 tickers in
# alphabetical order - which answers "who is in Semiconductor
# Manufacturing", not "who does this story touch".
#
# 10 rather than unlimited. A story that genuinely names more than ten
# companies is rare, and a list past that length stops being evidence
# and becomes a directory again, which is the failure being fixed.
_MAX_ENTITIES = 10

# link_type was removed. It offered direct/inferred/thematic, but this
# array holds only entities the article itself names, so "direct" is the
# only value it can legitimately take - "thematic" is defined as NOT
# named and is unreachable by construction, and "inferred" turned out to
# admit exactly what the array excludes: both stored inferred rows were
# "Department of the Treasury", reasoned "OFAC is an office within the
# Department of the Treasury", on documents that never name Treasury.
# The field was not recording a distinction, it was licensing a
# violation of the array's own definition, and an unparseable answer
# silently became "direct" - the strongest claim, asserted precisely
# when the model's answer could not be read. Nothing consumed it: no API
# filter, no frontend, no doc. The rule it weakened is now stated in the
# prompt and enforced by _entity_is_named below.
# country and product were added when `actors` was folded in. The type
# list previously had no slot for a country, so the model had nowhere to
# put Iran on an Iran sanctions document and the countries fell out into
# a separate `actors` field - two fields answering "who is in this
# story", only one of them validated. A sanctions action names the
# sanctioned state as surely as it names the issuing agency, and the
# goods a measure covers ("automotive and rail sectors", "semiconductors,
# radar, navigation systems") are the part a trader reads first.
_VALID_ENTITY_TYPES = frozenset({
    "company", "country", "government", "organisation", "person", "product",
})



# A generated summary is a FALLBACK for documents whose source filed no
# summary of their own (Federal Register Presidential Documents - 41 of 154
# real documents, none of which carry an abstract). The caller prefers the
# filed text wherever it exists, so this is never asked to compete with it.
#
# Capped rather than trusted to the prompt's own word limit: a model that
# ignores "3 to 4 sentences" would otherwise write a page into a metadata
# field that a card renders in a few lines.
# The prompt's own "0 to 3 category names", enforced in code. The
# overrides that run after this can push the list past it (the Taiwan
# rule alone force-adds four), which is deliberate - a Taiwan Strait
# conflict genuinely reaches all four - so the cap is applied here to
# the MODEL's own answer and again after overrides in the caller.
_MAX_IMPACTED_CATEGORIES = 3

_MAX_GENERATED_SUMMARY_CHARS = 1600


def _truncate_summary(text: str) -> str:
    """Cut to the cap on a sentence boundary, never mid-word.

    A hard slice is worse than a shorter summary: it strands a fragment
    that reads as finished prose. Measured on a real run, 4 of 5
    summaries hit the old cap exactly and all four were cut mid-clause -
    "effective through 12:01 a.m. EDT, Octob" dropped the expiry date it
    was in the middle of stating, and a reader had no way to tell the
    sentence had been severed.

    Falls back to the hard slice only when the first sentence alone
    already exceeds the cap, which no real summary has done.
    """
    if len(text) <= _MAX_GENERATED_SUMMARY_CHARS:
        return text
    window = text[:_MAX_GENERATED_SUMMARY_CHARS + 1]
    cut = max(window.rfind(". "), window.rfind(".\n"))
    if cut > 0:
        return text[:cut + 1]
    return text[:_MAX_GENERATED_SUMMARY_CHARS]

# Raised from 2,000 when generated_summary was added. At 2,000 this was
# supporting context for a tagging task and the cap barely mattered; it is
# now also the ONLY input to the summary of documents whose source filed no
# abstract, so anything cut here is a fact that summary cannot contain.
#
# 2,000 was cutting most real articles. Measured over the 234 stored
# geopolitical bodies: p50 3,641 / p90 9,451 / p99 29,602, and 176 of 234
# (75%) exceed 2,000 - so the common case was reading a third of the
# article. The Federal Register orders this was raised for are worse: both
# BIS export-denial orders store the full 20,000 and their specifics (tail
# numbers, dates, the conduct found) sit well past the 2,000 mark.
#
# 24,000 rather than uncapped. The distribution has a long tail - the
# largest stored body is 112,168 chars, ~28k tokens, which would cost more
# in one call than a normal day of this stage and risks the model's own
# context window. 24,000 covers p99 with margin while bounding the worst
# case to roughly 6k tokens, and news-retrieval's own cap means a Federal
# Register document never exceeds 20,000 anyway.
#
# Truncation stays mid-text rather than taking a head+tail slice: a
# government notice and a news article both open with the operative facts,
# and a spliced body would let the model read across a cut as if it were
# continuous prose.
_MAX_BODY_CHARS = 24000


# Dates and figures are the part of a generated summary that reads as
# authoritative and cannot be checked downstream, so they are verified
# against the source text rather than trusted to the prompt's own "never
# infer a figure" rule. Confirmed live that the rule alone is not enough:
# asked to summarize a BIS temporary-denial order, the model wrote
# "effective until September 16, 2026" for an order the document says was
# ISSUED on September 16, 2025 - an issuance date turned into an expiry
# date, with the year moved. Both halves wrong, and the sentence reads
# exactly like a fact lifted from the record.
#
# Whole date strings are matched as a unit, never their parts: "2026"
# alone DOES appear in that document (it is the publication year), so a
# bare-number check passes the very example this exists to catch. Same
# reasoning as china_signal_classifier's would_confirm guard - an enforced
# check, not a prompted one.
_MONTH_ALT = (
    r"(?:January|February|March|April|May|June|July|August|September"
    r"|October|November|December)"
)
# Three spellings, because the single "Month D, YYYY" form missed the
# one that was actually fabricated: article 42146's summary opened "On
# October 9" on a body that states no date at all, and a pattern
# requiring a year never looked at it. "13 October 2026" is the other
# common form.
_SUMMARY_DATE_RE = re.compile(
    rf"\b{_MONTH_ALT}\s+\d{{1,2}},\s+\d{{4}}\b"
    rf"|\b\d{{1,2}}\s+{_MONTH_ALT}\s+\d{{4}}\b"
    rf"|\b{_MONTH_ALT}\s+\d{{1,2}}\b"
)

# Figures that carry a unit are checked; bare integers are not. A percentage
# or a dollar amount is a claim about the measure ("a 25% duty"); a bare
# small integer is usually prose the source states differently ("one or more
# persons", "four aircraft"), and requiring a literal match on those would
# reject accurate summaries for paraphrasing - a false positive that costs a
# real summary is worse here than letting an unqualified integer through.
# No trailing \b after the unit group: "%" is not a word character, so a
# word boundary there can never match and the whole percentage branch
# silently found nothing. Confirmed live - "a 25% duty" and "$500,000 fine"
# both returned no match under the earlier pattern, which made the figure
# half of this check dead code while appearing to work.
# Counts of 10 or more are checked too. The unit rule alone exempted
# exactly the figures a reader acts on - "77 individuals", "823 Russian
# firms", "1,569 individuals and entities", "743 CEOs" carry no %, $ or
# magnitude word, so not one of them was ever verified. Single-digit
# counts stay exempt for the original reason: a source writing "four
# aircraft" may be summarised as "4 aircraft" and rejecting that costs
# an accurate summary. At two digits and up the number is a stated
# quantity, not prose.
_SUMMARY_FIGURE_RE = re.compile(
    r"\$\s?\d[\d,]*(?:\.\d+)?"
    r"|\b\d[\d,]*(?:\.\d+)?\s*(?:%|percent\b|billion\b|million\b|trillion\b)"
    r"|\b\d{1,3}(?:,\d{3})+\b"
    r"|\b\d{2,}\b",
    re.IGNORECASE,
)


# "Royal Air Force (RAF) base Fairford" -> the model's own gloss, not
# part of how the source writes the name.
_PAREN_GLOSS_RE = re.compile(r"\([^)]*\)")


def _normalize_for_match(text: str) -> str:
    """Collapse whitespace and case so a claim is compared against the body
    on content rather than on formatting (the GPO text wraps mid-sentence,
    so "September 16,\n2025" must still match "September 16, 2025")."""
    return " ".join(text.lower().split())


# A country is the one entity type whose name the model is RIGHT to
# normalise. An article writes "UK", "Britain", "Tehran" or "Washington";
# the useful stored value is "United Kingdom", "Iran", "United States",
# because that is what a reader filters on. Confirmed live: the Al
# Jazeera RAF Fairford story names the UK in its headline and first
# sentence and never once writes "United Kingdom", so a literal-string
# check dropped both it and the US as "not named" - exactly the
# false-negative this check is supposed to avoid.
#
# Only the forms a news article actually uses. A country absent from
# this table still matches on its own name, which is the common case -
# "Iran", "Russia", "China" are written as themselves.
_COUNTRY_ALIASES: dict[str, tuple[str, ...]] = {
    "united states": ("united states", "u.s.", "us", "usa", "america", "american", "washington"),
    "united kingdom": ("united kingdom", "uk", "britain", "british", "england", "london"),
    "china": ("china", "chinese", "prc", "beijing"),
    "russia": ("russia", "russian", "moscow", "kremlin"),
    "iran": ("iran", "iranian", "tehran"),
    "european union": ("european union", "european", "eu", "brussels"),
    "south korea": ("south korea", "korea", "seoul"),
    "north korea": ("north korea", "dprk", "pyongyang"),
    "japan": ("japan", "japanese", "tokyo"),
    "taiwan": ("taiwan", "taiwanese", "taipei"),
    "india": ("india", "indian", "new delhi", "delhi"),
    "israel": ("israel", "israeli", "jerusalem"),
    "saudi arabia": ("saudi arabia", "saudi", "riyadh"),
    "venezuela": ("venezuela", "venezuelan", "caracas"),
    "ukraine": ("ukraine", "ukrainian", "kyiv", "kiev"),
    "germany": ("germany", "german", "berlin"),
    "france": ("france", "french", "paris"),
    "netherlands": ("netherlands", "dutch", "amsterdam", "the hague"),
    "turkey": ("turkey", "turkish", "ankara", "turkiye"),
    "mexico": ("mexico", "mexican"),
    "canada": ("canada", "canadian", "ottawa"),
}

# Short aliases that would match inside an unrelated word ("us" inside
# "Russia", "eu" inside "Europe"). Matched as whole words only.
_SHORT_ALIASES = {"us", "uk", "eu", "prc", "usa", "dprk"}
_WORD_SPLIT_RE = re.compile(r"[^a-z]+")


def _country_is_named(name: str, hay: str) -> bool:
    """True when the article refers to this country under any name it
    plausibly uses - full name, adjective, or capital standing in for
    the government ("Tehran willing to assist").
    """
    for alias in _COUNTRY_ALIASES.get(name.strip().lower(), ()):
        if alias in _SHORT_ALIASES:
            if alias in set(_WORD_SPLIT_RE.split(hay)):
                return True
        elif alias in hay:
            return True
    return False


# A class of actors, not an entity. The model reaches for these when a
# document addresses a category of business rather than a named body -
# "U.S. financial institutions and registered money transmitters may
# rely on the originator of a funds transfer", which named nobody and
# still produced two entity rows. They pass the named-in-text check
# (the phrase IS in the text), carry no ticker, and resolve to no
# registry entry, so they occupy slots that named entities should hold.
_CLASS_NOUN_RE = re.compile(
    r"\b(institutions?|transmitters?|firms?|companies|banks|entities|"
    r"individuals?|nationals?|officials?|states|countries|citizens|"
    r"persons?|suppliers?|manufacturers?|producers?|exporters?|"
    r"importers?|operators?|providers?|agencies|authorities|"
    r"ministries|organisations|organizations)\b",
    re.IGNORECASE,
)
# A proper name carries a capitalised word that is not a leading
# article/adjective - "Council of the European Union", "RAF Fairford",
# "Nvidia". A class noun phrase generally does not ("registered money
# transmitters"), though some carry a nationality adjective ("U.S.
# financial institutions"), which is why the class check runs first.
_PROPER_NAME_RE = re.compile(r"\b[A-Z][A-Za-z.&'-]*")
# Nationality/geographic adjectives are capitalised but do not make a
# phrase a proper name on their own.
_NATIONALITY_WORDS = frozenset({
    "u.s.", "us", "american", "british", "uk", "european", "eu", "russian",
    "chinese", "iranian", "japanese", "korean", "indian", "german",
    "french", "dutch", "israeli", "saudi", "venezuelan", "ukrainian",
    "taiwanese", "turkish", "mexican", "canadian", "the", "a", "an",
})


# A legal instrument names the measure, not a good it covers. The
# instrument is already carried in the reason text and in event_date, so
# a product entry for it is the document describing itself.
_INSTRUMENT_RE = re.compile(
    # No trailing \b after the alternation: "e.o." ends in a period, and
    # \b never matches between "." and a following space.
    r"\b(general licen[cs]e|executive order|e\.o\.|regulation|directive|"
    r"statutory instrument|federal register|decree|ordinance)"
    r"|\b(?:no\.?|number)\s*\d+",
    re.IGNORECASE,
)
# "the automotive and rail sectors of the Iranian economy" - an industry
# read, which `assets` and `impacted_categories` already carry with a
# controlled vocabulary. A product is a thing, not a line of business.
_SECTOR_WORD_RE = re.compile(r"\b(sectors?|industr(?:y|ies)|economy)\b", re.IGNORECASE)


def resolve_against_universe(
    entities: list[dict[str, Any]] | None,
    companies: list[dict[str, Any]] | None,
) -> list[dict[str, Any]] | None:
    """Fill each company entity's ticker from the tracked universe, and
    mark whether it is in that universe at all.

    The universe is the authority where it matches: its own
    ``company_name`` and ``ticker`` replace whatever the model wrote, so
    one company reads identically on every row rather than as "Nvidia"
    here and "NVIDIA Corp" there. A match also recovers a ticker the
    model left null - measured on real rows, "MP Materials" came back
    with no ticker and resolved to MP, a rare-earths name in a
    critical-minerals story that was otherwise invisible to any
    ticker-based reader.

    ``in_universe`` is written on COMPANY entities only, and only as
    True. A country or a ministry is not a company and the question does
    not apply to it; an unmatched company gets no key rather than
    ``false``, so "not in this taxonomy" is absence, not an assertion -
    Sberbank and PDVSA are real companies that genuinely are not in an
    AI-economy universe, and saying so explicitly on every sanctioned
    foreign bank would be noise.

    Matching is EXACT on lowercased name or uppercased ticker,
    deliberately. A fuzzy match that attaches the wrong ticker is far
    worse than a miss: the ticker is the one field a reader acts on
    directly. "Russia's VTB Bank" therefore does not match a "VTB" row,
    and that is the intended trade.
    """
    if not entities or not companies:
        return entities
    by_name = {
        (c.get("company_name") or "").strip().lower(): c
        for c in companies if c.get("company_name")
    }
    by_ticker = {
        (c.get("ticker") or "").strip().upper(): c
        for c in companies if c.get("ticker")
    }
    for entity in entities:
        if entity.get("type") != "company":
            continue
        match = by_name.get((entity.get("name") or "").strip().lower())
        if match is None and entity.get("ticker"):
            match = by_ticker.get(entity["ticker"].strip().upper())
        if match is None:
            continue
        entity["name"] = match.get("company_name") or entity["name"]
        entity["ticker"] = match.get("ticker") or entity.get("ticker")
        entity["in_universe"] = True
    return entities


def _is_not_a_product(name: str) -> str | None:
    """Return why ``name`` is not a product, or None when it is one.

    Confirmed live, all three on real rows: "automotive and rail sectors
    of the Iranian economy" (a sector read, which assets already
    carries), "General License 60" (the instrument itself), and
    "earthquake relief efforts in Venezuela" (an activity). Each was
    stored as a product on a row where the field was meant to hold the
    goods a measure covers.
    """
    if _SECTOR_WORD_RE.search(name):
        return "names a sector/industry, not a good (assets carries that)"
    if _INSTRUMENT_RE.search(name):
        return "names a legal instrument, not a good it covers"
    return None


def _is_class_not_entity(name: str) -> bool:
    """True when ``name`` describes a CLASS of actors rather than one
    identifiable entity.

    Confirmed live: "U.S. financial institutions" and "U.S. registered
    money transmitters" were stored as organisations from an OFAC
    general licence that names neither. A reader cannot look either one
    up, and neither can carry a ticker.

    A phrase is a class when it contains a plural class noun AND has no
    capitalised word beyond a nationality adjective or article. That
    second half is what keeps real names safe: "Islamic Revolutionary
    Guard Corps" has no class noun at all, and "Council of the European
    Union" carries "Council", "European", "Union" - proper-name words.
    """
    if not _CLASS_NOUN_RE.search(name):
        return False
    proper = [
        w for w in _PROPER_NAME_RE.findall(name)
        if w.lower() not in _NATIONALITY_WORDS
    ]
    return not proper


# Grammatical glue only. Body-type words (department, council,
# commission, agency...) are deliberately NOT here: they are the single
# word distinguishing a Commission from a Council from a Parliament, and
# stripping them let "European Commission" match a text that named no
# commission - a plausible-looking body the document never mentions,
# which is the worst thing this check can admit.
_NAME_STOPWORDS = frozenset({
    "of", "the", "and", "for", "in", "on", "at", "to", "a", "an",
})


# Word -> the other spellings of THAT SAME word, derived from the
# country aliases above so the two never drift. Keyed per word, not per
# country, because the check below must ask "does this particular word
# appear in some form", never "does some word from this country's group
# appear".
#
# Keyed by single WORD, because the caller checks one word at a time:
# "United States Department of Commerce" is checked as {united, states,
# department, commerce}, so the entry that has to exist is "states" ->
# ("us", "u.s.", "america", ...), not the whole phrase "united states".
_GEOGRAPHIC_SYNONYMS: dict[str, tuple[str, ...]] = {}
for _aliases in _COUNTRY_ALIASES.values():
    _words = {w for a in _aliases for w in _WORD_SPLIT_RE.split(a) if len(w) > 2}
    for _w in _words:
        _GEOGRAPHIC_SYNONYMS.setdefault(_w, tuple(_aliases))
del _aliases, _words, _w


def _expanded_name_is_named(name: str, hay: str) -> bool:
    """True when the article names this entity under a shorter form.

    The prompt asks for the full official name ("Council of the European
    Union") while the article writes the short one ("Council of the EU"),
    so a verbatim check rejects the very expansion it asked for -
    confirmed live on article 42146.

    Matched on the DISTINCTIVE words of the name - everything but the
    structural vocabulary in _NAME_STOPWORDS - requiring all of them to
    appear. "Council of the European Union" reduces to {european}, which
    the text has; a fabricated "European Chemicals Agency" would reduce
    to {european, chemicals} and fail on "chemicals". Requiring ALL of
    them is what keeps this from matching any two bodies that share one
    word.

    Returns False when nothing distinctive is left (a name made entirely
    of structural words proves nothing), leaving the caller's stricter
    checks to decide.
    """
    distinctive = {
        w for w in _WORD_SPLIT_RE.split(_normalize_for_match(name).replace("’", "'"))
        if w and w not in _NAME_STOPWORDS and len(w) > 2
    }
    if not distinctive:
        return False
    hay_words = set(_WORD_SPLIT_RE.split(hay))
    for w in distinctive:
        if w in hay:
            continue
        # A geographic word may appear only as its abbreviation -
        # "European" written "EU", "United States" written "U.S.".
        # Only the aliases of THAT word count: an earlier version
        # tested whether any alias group containing the word had any
        # member in the text, which let "European Commission" pass on a
        # text naming no commission, because "european" matched and
        # "commission" was never really checked.
        if w in _GEOGRAPHIC_SYNONYMS and any(
            (a in hay_words if a in _SHORT_ALIASES else a in hay)
            for a in _GEOGRAPHIC_SYNONYMS[w]
        ):
            continue
        return False
    return True


def _entity_is_named(name: str, haystack: str | None, etype: str | None = None) -> bool:
    """True when ``name`` actually occurs in the article text.

    This array is defined as the entities the article ITSELF names, and
    that was previously only a prompt instruction - nothing checked it,
    so "Department of the Treasury" entered on two rows whose documents
    never mention Treasury, carried in by a link_type of "inferred".
    Removing that field without enforcing the rule it weakened would
    leave the same entities arriving with no label at all.

    The model writes a name as the text does plus its own gloss, and
    EITHER half can be the form the text actually uses. "Iran's Islamic
    Revolutionary Guard Corps (IRGC)" is written out in full by the
    source, so the gloss-stripped form matches; "Royal Air Force (RAF)
    base Fairford" is NOT - that source writes "RAF Fairford", so only
    the parenthesised form matches. Any variant matching is enough,
    because a false negative here deletes a correctly-named entity,
    which is strictly worse than keeping a doubtful one. Apostrophes are
    folded because the model emits U+2019 where a source uses U+0027 and
    vice versa.

    No text to check against means nothing is rejected - same fail-open
    rule as _unsupported_claims: this catches an entity invented from a
    document the model DID read, not one from a body that was never
    fetched.
    """
    if not haystack:
        return True
    hay = _normalize_for_match(haystack).replace("’", "'")
    # A country is checked against the names an article really uses,
    # not against its canonical form - see _COUNTRY_ALIASES.
    if etype == "country" and _country_is_named(name, hay):
        return True
    # The prompt asks for an entity's FULL official name while an
    # article writes the short form, so a verbatim test rejects exactly
    # the expansion that was asked for: "Council of the European Union"
    # against a text that says "Council of the EU". Checking the
    # distinctive words rather than the whole string keeps both halves
    # of that instruction satisfiable.
    if _expanded_name_is_named(name, hay):
        return True
    variants = [_PAREN_GLOSS_RE.sub(" ", name), *_PAREN_GLOSS_RE.findall(name)]
    for variant in variants:
        # Comma-split so a name carrying an apposition ("Ali Mousavi,
        # Iran's ambassador") is matched on its longest real run.
        longest = max(variant.strip("()").split(","), key=len, default="")
        needle = _normalize_for_match(longest).replace("’", "'")
        # Too short to be evidence of anything - a two-character
        # fragment matches by accident. Keep the entity rather than drop
        # it on a match this weak.
        if len(needle) < 4:
            return True
        if needle in hay:
            return True
    return False


_MONTH_NAMES = (
    "January", "February", "March", "April", "May", "June",
    "July", "August", "September", "October", "November", "December",
)


# A plausible listed-equity symbol: 1-6 letters, optionally a dotted
# exchange suffix (TSM, 000660.KS, 8035.JP), or a numeric Asian code.
# Shape only - this cannot confirm the symbol belongs to the company,
# which is why the prompt tells the model to leave it null unless sure.
_TICKER_RE = re.compile(r"^[A-Z]{1,6}(?:\.[A-Z]{1,3})?$|^\d{4,6}(?:\.[A-Z]{2})?$")

_PROSE_DATE_RE = re.compile(
    rf"(?:({_MONTH_ALT})\s+(\d{{1,2}})(?:,\s*(\d{{4}}))?"
    rf"|(\d{{1,2}})\s+({_MONTH_ALT})\s+(\d{{4}}))",
)


def _date_spellings(claim: str) -> list[str]:
    """Return the ways a source might write the date in ``claim``.

    "1 October 2026", "October 1, 2026" and "October 1" are the same
    date; a summary writing one where the body writes another is
    accurate, and matching the literal string only would strip the
    sentence as fabricated.
    """
    m = _PROSE_DATE_RE.search(claim)
    if not m:
        return [claim]
    month, day, year = (m.group(1), m.group(2), m.group(3))
    if not month:
        day, month, year = (m.group(4), m.group(5), m.group(6))
    d = str(int(day))
    out = [f"{month} {d}", f"{d} {month}"]
    if year:
        out += [f"{month} {d}, {year}", f"{d} {month} {year}"]
    return out


def _spell_date(iso: str) -> str:
    """Return the prose spellings of an ISO date, for matching.

    A summary writes "October 13, 2026" or "13 October 2026", never
    "2026-10-13", so the publication date has to be present in those
    forms for the figure check to recognise it as supported.
    """
    try:
        year, month, day = iso[:10].split("-")
        name = _MONTH_NAMES[int(month) - 1]
    except (ValueError, IndexError):
        return iso
    d = str(int(day))
    return " ".join((
        f"{name} {d}, {year}", f"{d} {name} {year}", f"{name} {d}",
        f"{d} {name}", f"{name} {year}",
    ))


# Does the body name ANY calendar date? Used to tell "this body states
# dates and none is the claimed one" (a fabrication) from "this body
# states no date at all" (nothing to check against).
_ANY_DATE_IN_BODY_RE = re.compile(
    r"\b(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\.?\s+\d{1,2}\b"
    r"|\b\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)[a-z]*\b"
    r"|\b\d{4}-\d{2}-\d{2}\b"
    r"|\b\d{1,2}/\d{1,2}/\d{2,4}\b",
    re.IGNORECASE,
)


def _day_month_is_supported(event_date: str, body: str | None) -> bool:
    """True when ``event_date``'s day and month appear in ``body``.

    The year check below was the field's only validation, and it leaves
    a real gap: a date whose year is right and whose day is invented
    passes it. "2026-08-09" and "2026-08-19" are equally defensible to a
    year test, and a reader cannot sanity-check either.

    Matched in the forms a document actually writes - "August 9",
    "9 August", "August 9, 2026", "9 August 2026" and the ISO form -
    reusing _spell_date, the same helper the summary's own figure check
    uses, so one date vocabulary serves both rather than two drifting
    copies.

    Numeric spellings (8/9/2026, 09.08.2026, 2026-08-09) are matched
    too: Federal Register and agency documents write dates that way far
    more often than news prose does, and rejecting a correct date
    because the source used slashes would be the exact failure the
    spelling list exists to prevent.

    Fail-open with no body, same rule as every other content check
    here - "we could not verify" must never be recorded as "we checked
    and it was wrong".
    """
    if not body:
        return True
    try:
        year, month, day = event_date[:10].split("-")
        mi, di = int(month), int(day)
    except (ValueError, IndexError):
        return True
    # Periods dropped from BOTH sides before comparing. _normalize_for_match
    # collapses whitespace and case but keeps punctuation, so searching
    # "oct 15" against a body reading "Oct. 15" missed - measured on three
    # real rows (138330 "Oct. 15", 138184 "Oct. 13", 138188 "Sept. 11")
    # whose dates were present verbatim and were still being rejected.
    haystack = _normalize_for_match(body).replace(".", "")
    name = _MONTH_NAMES[mi - 1] if 1 <= mi <= 12 else month
    # BOTH the full month and its abbreviation. Measured over 37 real
    # dated rows: testing "October" alone rejected 22 of them, because
    # news copy and agency notices overwhelmingly write "Oct. 16, 2026",
    # "an Oct. 7 work session", "overnight on Oct. 16". A check that
    # discards 59% of correct values is worse than the gap it closes.
    # "Sept" is listed separately because September abbreviates to four
    # letters as often as three.
    names = {name, name[:3], name[:4]}
    spellings: list[str] = []
    for nm in names:
        spellings += [
            f"{nm} {di}", f"{di} {nm}",
            f"{nm} {di}, {year}", f"{di} {nm} {year}",
        ]
    spellings += [
        event_date,                       # 2026-08-09
        f"{mi}/{di}/{year}", f"{di}/{mi}/{year}",
        f"{mi:02d}/{di:02d}/{year}", f"{di:02d}/{mi:02d}/{year}",
        f"{di:02d}.{mi:02d}.{year}",
    ]
    if any(_normalize_for_match(s).replace(".", "") in haystack for s in spellings):
        return True
    # A body that states no calendar date at all cannot refute one.
    # News copy routinely writes "the US military reported on Thursday"
    # and never prints the date; the model resolving that against the
    # publication date is correct behaviour, not invention. Rejecting
    # here would punish the model for reading the article properly.
    #
    # The check only bites when the body DOES name dates and none of
    # them is the one claimed - which is the fabrication case.
    return not _ANY_DATE_IN_BODY_RE.search(body)


def _year_is_supported(
    event_date: str, body: str | None, published: str | None,
) -> bool:
    """True when ``event_date``'s year is defensible.

    Defensible means the year is written in the article text, or it is
    the publication year, or the one before it - a document published
    in early January routinely describes an action taken in December,
    and a news story says "Monday" rather than printing the year at all.

    Nothing to check against means nothing is rejected, the same
    fail-open rule the other content checks use.
    """
    year = event_date[:4]
    if body and year in body:
        return True
    if published and len(published) >= 4:
        pub_year = published[:4]
        if pub_year.isdigit() and year.isdigit():
            return int(pub_year) - int(year) in (0, 1)
        return False
    # No body and no publication date: nothing to judge against.
    return not body and not published


def _unsupported_claims(summary: str, body: str | None) -> list[str]:
    """Return the dates/figures in ``summary`` that do not appear verbatim
    in ``body``. Empty list means every checked claim is supported.

    No body to check against (the row was classified headline-only) means
    nothing can be verified, so nothing is rejected - the caller keeps the
    summary. This check exists to catch a figure invented from a document
    the model DID read, not to require a body that was never there.
    """
    if not body:
        return []
    haystack = _normalize_for_match(body)
    unsupported = []
    for claim in _SUMMARY_DATE_RE.findall(summary):
        # A date is matched in any of its spellings: a summary may write
        # "1 October 2026" where the source writes "October 1, 2026",
        # and rejecting that would strip an accurate sentence.
        if not any(
            _normalize_for_match(v) in haystack for v in _date_spellings(claim)
        ):
            unsupported.append(claim)
    # Digits that are PART of a date are not separate figures: the date
    # rule above already judged them, and reporting "24" alongside
    # "October 24, 2026" both double-logs one fault and risks rejecting
    # a day number that appears legitimately elsewhere in the text.
    date_spans = [m.span() for m in _SUMMARY_DATE_RE.finditer(summary)]
    for fig in _SUMMARY_FIGURE_RE.finditer(summary):
        if any(s <= fig.start() and fig.end() <= e for s, e in date_spans):
            continue
        if _normalize_for_match(fig.group()) not in haystack:
            unsupported.append(fig.group())
    return unsupported


# Opening connectives that only make sense after a preceding sentence.
_DANGLING_CONNECTIVE_RE = re.compile(
    r"^(?:In parallel|Separately|In addition|Additionally|Meanwhile|"
    r"Furthermore|Moreover|In turn|At the same time|Also)\s*,\s*",
    re.IGNORECASE,
)


def _strip_unsupported_sentences(summary: str, body: str | None) -> str | None:
    """Drop whole sentences carrying a date or figure the body does not
    state, and return what is left (or None if nothing is).

    Sentence-level rather than whole-summary: a four-sentence summary with
    one bad date still holds three accurate sentences, and discarding all
    of them to punish one would leave the row with no summary at all -
    which for a Presidential Document means no content anywhere, since
    those file no abstract.

    Splitting must not break on an abbreviation's own period. A bare
    "split on '. ' before a capital" rule is not enough - confirmed live
    that it cut "The U.S. Department of Commerce has renewed..." into
    "The U.S." + the rest, so dropping the bad sentence left the stub
    "The U.S." stranded at the front of the summary. Abbreviations are
    excluded explicitly, and a sentence must also be long enough to BE
    one, which catches any abbreviation not on the list.
    """
    if not summary:
        return None
    sentences = re.split(
        r"(?<=\.)\s+(?=[A-Z])(?<!\bU\.S\.\s)(?<!\bU\.K\.\s)(?<!\bE\.U\.\s)"
        r"(?<!\bNo\.\s)(?<!\bInc\.\s)(?<!\bCo\.\s)(?<!\bSt\.\s)",
        summary,
    )
    # Re-join any fragment too short to be a real sentence onto the one
    # before it, so an unlisted abbreviation degrades to a slightly long
    # sentence rather than to a stranded stub.
    merged: list[str] = []
    for part in sentences:
        if merged and len(part.split()) < 4:
            merged[-1] = f"{merged[-1]} {part}"
        else:
            merged.append(part)
    sentences = merged
    kept = [s for s in sentences if not _unsupported_claims(s, body)]
    if len(kept) == len(sentences):
        return summary
    rebuilt = " ".join(kept).strip()
    if not rebuilt:
        return None
    # Dropping the opening sentence can leave the next one starting with
    # a connective that refers back to what was just removed - a real
    # row came back reading "In parallel, the EU sanctioned 1,569..."
    # with nothing before it. The facts are sound; the join word is a
    # dangling reference, so it is removed rather than the sentence.
    rebuilt = _DANGLING_CONNECTIVE_RE.sub("", rebuilt, count=1)
    return rebuilt[0].upper() + rebuilt[1:] if rebuilt else None


def _validate(
    parsed: Any, body: str | None = None, *, named_in: str | None = None,
    published_hint: str | None = None,
) -> dict[str, Any] | None:
    """Return a cleaned result dict, or None if the shape/values are
    unusable. Unrecognized values inside otherwise-valid arrays are
    dropped individually (not treated as a whole-response failure) - a
    model inventing one bad asset name shouldn't discard three good ones.
    """
    if not isinstance(parsed, dict):
        return None

    # Same near-miss rule as assets below: "Sanctions", " trade" or
    # "subsidy" is a spelling of a valid answer, not a missing one - an
    # exact-match check would store null and make it indistinguishable
    # from a failed call. Anything still unknown is logged, then null.
    # null is a valid answer: the article reports no instrument (analysis,
    # a forecast, a company's plan, a hearing). Five reviewer personas
    # agreed a "closest" channel there is a guess that floods the filters
    # (2026-10-20). "none" / "null" written as a string mean the same.
    raw_channel = parsed.get("channel")
    channel = raw_channel.strip().lower() if isinstance(raw_channel, str) else None
    channel = _CHANNEL_ALIASES.get(channel, channel)
    if channel not in _VALID_CHANNELS:
        if raw_channel is not None and channel not in _NO_CHANNEL:
            logger.warning(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] channel outside the vocabulary,"
                " stored as null: %r", raw_channel,
            )
        channel = None

    assets = parsed.get("assets")
    if not isinstance(assets, list):
        assets = None
    else:
        # A near-miss is rewritten, not deleted. Two of four real rows
        # stored [] where the prompt asks for 1-3, and a silent filter
        # made the two causes indistinguishable afterwards: the model
        # returning nothing, and the model returning "oil" where the
        # vocabulary says "crude_oil". The first is a real answer; the
        # second is a spelling the alias table now absorbs.
        #
        # Whatever survives neither is logged rather than dropped
        # quietly - the same distinction the rest of this pipeline
        # keeps: "we couldn't check" is not "we checked and found
        # nothing".
        normalized: list[str] = []
        rejected: list[Any] = []
        for a in assets:
            if not isinstance(a, str):
                rejected.append(a)
                continue
            key = a.strip().lower()
            key = _ASSET_ALIASES.get(key, key)
            if key in _VALID_ASSETS:
                if key not in normalized:
                    normalized.append(key)
            else:
                rejected.append(a)
        if rejected:
            logger.warning(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping %d asset(s) outside"
                " the allowed vocabulary: %r", len(rejected), rejected,
            )
        assets = normalized

    impacted_categories = parsed.get("impacted_categories")
    if not isinstance(impacted_categories, list):
        impacted_categories = None
    else:
        # Dedup as well as filter: a model that names the same category
        # twice should not produce it twice, and dict.fromkeys keeps the
        # model's own ordering, which is its relevance ordering.
        impacted_categories = list(dict.fromkeys(
            c for c in impacted_categories if c in ALLOWED_CATEGORIES
        ))
        # The prompt asks for 0-3 and the model does not always comply:
        # one real EU sanctions row came back with five. Enforced here
        # because the limit is the point of the field - a story that
        # transmits into five of nineteen categories has not been
        # narrowed to anything, and Layer 3 expands each one into
        # tickers, so an extra category silently widens the ticker list
        # too. Truncation keeps the model's own order rather than
        # choosing between them.
        impacted_categories = impacted_categories[:_MAX_IMPACTED_CATEGORIES]

    generated_summary = parsed.get("generated_summary")
    if not isinstance(generated_summary, str) or not generated_summary.strip():
        generated_summary = None
    else:
        generated_summary = _truncate_summary(generated_summary.strip())
        # Checked against the body alone. The publication date is
        # deliberately NOT added to this haystack: the summary is not
        # supposed to state it (the row carries it, and the card shows
        # it beside the summary), so admitting it here would license
        # exactly the repetition the prompt forbids. published_hint
        # still reaches event_date's year check, where it stands in for
        # a body that states no year.
        unsupported = _unsupported_claims(generated_summary, body)
        if unsupported:
            kept = _strip_unsupported_sentences(generated_summary, body)
            logger.warning(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] generated_summary stated"
                " %d figure/date(s) absent from the source text, dropping"
                " those sentences: %r", len(unsupported), unsupported,
            )
            generated_summary = kept


    # Each entity must carry a name, a type, and a reason naming what the
    # text says about it. An entity we cannot explain is dropped rather
    # than shown: an unexplained ticker is the problem this field exists
    # to replace, so admitting one here would reproduce it in a new
    # field. ticker is optional - a sanctioned Russian supplier is worth
    # naming even though it is unlisted.
    entities = parsed.get("entities")
    if not isinstance(entities, list):
        entities = None
    else:
        cleaned: list[dict[str, Any]] = []
        seen: set[str] = set()
        for e in entities:
            if not isinstance(e, dict):
                continue
            name = e.get("name")
            reason = e.get("reason")
            if not isinstance(name, str) or not name.strip():
                continue
            if not isinstance(reason, str) or not reason.strip():
                continue
            key = name.strip().lower()
            if key in seen:
                continue
            seen.add(key)
            etype = e.get("type")
            etype = etype if etype in _VALID_ENTITY_TYPES else "organisation"
            # The array's defining rule, now enforced rather than merely
            # asked for. Dropped with a log line, not silently: an
            # entity the text does not name is the model reaching past
            # the document, and that is worth seeing. The type is
            # resolved first because a country is matched against the
            # names an article really uses, not its canonical form.
            if not _entity_is_named(name, named_in, etype):
                logger.warning(
                    "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping entity not named"
                    " in the article text: %r (%s)", name.strip(), reason.strip()[:80],
                )
                continue
            # A category of actors is not an entity, however plainly the
            # text states it - see _is_class_not_entity.
            if _is_class_not_entity(name):
                logger.warning(
                    "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping class-noun entity"
                    " (names no identifiable body): %r", name.strip(),
                )
                continue
            if etype == "product":
                not_product = _is_not_a_product(name)
                if not_product:
                    logger.warning(
                        "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping product %r: %s",
                        name.strip(), not_product,
                    )
                    continue
            # The reason's own figures are checked against the text, the
            # same way the summary's are. It was previously required to
            # be non-empty and nothing more, so a reason could state a
            # count or date the document never gave - the same class of
            # fabrication the summary check exists to catch, in a field
            # a reader trusts just as much.
            bad_figures = _unsupported_claims(reason, body)
            if bad_figures:
                logger.warning(
                    "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping entity %r: its"
                    " reason states %r, absent from the article text",
                    name.strip(), bad_figures,
                )
                continue
            ticker = e.get("ticker")
            ticker = (
                ticker.strip().upper()
                if isinstance(ticker, str) and ticker.strip() else None
            )
            # A ticker is only kept on a company. The model attached
            # them to nothing else in practice, but a ticker on a
            # ministry or a country is meaningless, and a WRONG ticker
            # is the costliest fabrication this pipeline can emit - it
            # is the one field a reader would act on directly.
            if ticker and (etype != "company" or not _TICKER_RE.match(ticker)):
                logger.warning(
                    "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping ticker %r on"
                    " %s entity %r", ticker, etype, name.strip(),
                )
                ticker = None
            cleaned.append({
                "name": name.strip(),
                "type": etype,
                "reason": reason.strip(),
                "ticker": ticker,
            })
        entities = cleaned[:_MAX_ENTITIES] or None

    # Checked against the source text, exactly like event_date's year
    # below and for the same reason: a fabricated place reads as
    # authoritatively as a real one and nothing downstream could catch
    # it. The model is asked for a place THE TEXT NAMES, so requiring
    # the string to appear in the title or body is a direct test of
    # that instruction rather than a guess at plausibility.
    #
    # Matched case-insensitively on the whole value. A multi-part answer
    # ("El Dorado County, California") is also accepted when its first
    # part appears, since a text often names the county once and the
    # state separately, and the model composing the fuller form is
    # correct behaviour rather than invention.
    event_location = parsed.get("event_location")
    if not isinstance(event_location, str):
        event_location = None
    else:
        event_location = " ".join(event_location.split())[:_MAX_EVENT_LOCATION_CHARS]
        if not event_location:
            event_location = None
        else:
            haystack = (named_in or body or "").lower()
            needle = event_location.lower()
            head = needle.split(",")[0].strip()
            if needle not in haystack and (not head or head not in haystack):
                logger.warning(
                    "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping event_location"
                    " %r: absent from the article text", event_location,
                )
                event_location = None

    event_date = parsed.get("event_date")
    if not isinstance(event_date, str) or not _EVENT_DATE_RE.match(event_date.strip()):
        event_date = None
    else:
        event_date = event_date.strip()
        # The format check above only proves it LOOKS like a date. It
        # was the field's only validation, and the model fabricated one:
        # article 42146's body reads "Oct. 12 (UPI) ... announced
        # Monday" and contains no year at all, yet event_date came back
        # "2023-10-09" - three years before publication - and the
        # summary opened "On October 9". The summary's own figures are
        # checked against the body by _unsupported_claims; this field
        # was not, so a fabricated date passed into a column a reader
        # would trust.
        #
        # The year must appear in the text, which is the part a reader
        # cannot sanity-check and the part that was wrong. The day and
        # month are left unchecked: a document states them in many
        # forms ("Oct. 12", "12 October", "the 12th") and requiring a
        # literal match would reject correct dates.
        if not _year_is_supported(event_date, body, published_hint):
            logger.warning(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping event_date %r: its"
                " year appears neither in the article text nor in the"
                " publication date", event_date,
            )
            event_date = None
        elif not _day_month_is_supported(event_date, body):
            # A right year with an invented day is the gap the year
            # check alone leaves open, and it is the half a reader
            # cannot verify: "the determination was issued 9 August"
            # against a body saying 19 August reads identically.
            logger.warning(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] dropping event_date %r: its"
                " day and month appear nowhere in the article text",
                event_date,
            )
            event_date = None

    return {
        "event_date": event_date,
        "event_location": event_location,
        "channel": channel,
        "assets": assets,
        "impacted_categories": impacted_categories,
        "generated_summary": generated_summary,
        "entities": entities,
    }


def classify_geopolitical_signal_tags(
    title: str,
    *,
    body: str | None = None,
    published: str | None = None,
    system_prompt: str,
    model: str,
    api_key: str,
    base_url: str,
    timeout: int,
) -> dict[str, Any]:
    """Return {channel, assets, impacted_categories, entities, event_date,
    generated_summary}.

    ``body`` is optional article text (news-retrieval may not have it, or
    the fetch may have failed/404'd upstream by the time this runs) - when
    present it's included as extra context after the headline, truncated
    to _MAX_BODY_CHARS, so the model can name the specific goods/sector an
    intentionally terse headline leaves out (see the prompt's own
    empty-impacted_categories rule for why that distinction matters). When
    absent, the call degrades to the original headline-only prompt.

    Any failure (network error, non-JSON response, wrong shape, timeout)
    returns all-None fields rather than raising - the caller still writes
    the row as HIGH, just without tags, per the spec's fail-open rule.
    """
    user_content = f"Headline: {title}"
    # The publication date, so the summary can say when a measure was
    # taken AS AGAINST when it surfaced. Federal Register notices run
    # months behind the action: one published 13 October carried a
    # general licence issued 25 June. Without this the model has no way
    # to know the two differ, and a reader takes a months-old measure
    # for today's news. It is labelled as metadata rather than article
    # text so the entity/figure checks never treat it as something the
    # document itself states.
    if published:
        user_content += f"\n\n(Publication date, not part of the article: {published})"
    if body:
        user_content += f"\n\nArticle text: {body[:_MAX_BODY_CHARS]}"
    payload = {
        "model": model,
        "temperature": 0,
        # Sized for the summary, which dominates the response: the other
        # four fields fit in ~400 tokens and a 1,600-character summary
        # needs ~400 of its own, so this leaves real headroom rather than
        # sitting at the edge.
        #
        # Headroom matters more here than elsewhere because an overflow
        # does not truncate one field - it cuts the JSON object
        # mid-string, extract_json_object finds nothing, and EVERY tag on
        # the row nulls out. The summary's own length is bounded in
        # _validate by _truncate_summary, never by starving the response.
        # Raised from 1,100 after a live overflow: a 19-entity response
        # on the EU sanctions row was cut mid-summary, extract_json_object
        # found no closing brace, and EVERY tag on that row nulled out -
        # the exact failure the note above predicts. The entity cap is 10,
        # but it is enforced after parsing, so an over-long response is
        # spent before this code can trim it. The prompt now states the
        # limit too; this is the backstop for when the model ignores it.
        "max_tokens": 2000,
        # System prompt FIRST and byte-identical on every call, same
        # reason as Stage B's: this prompt is ~6,200 tokens against a
        # body that averages far less, so most of every call is the
        # instructions, and an OpenAI model caches a repeated prefix
        # automatically. The stake is larger here than in Stage B - this
        # prompt is half again as long.
        #
        # Two things keep it working: the prompt is read unmodified
        # (no per-call interpolation into the system message - the
        # publication date goes in the USER message above for exactly
        # this reason), and the user message stays SECOND, since caching
        # matches a prefix.
        #
        # Model is config.GEOPOLITICAL_SIGNAL_STAGE_C_MODEL, which
        # defaults to SEC_FILING_MODEL (openai/gpt-4.1 here). Anthropic
        # models on OpenRouter cache nothing without an explicit
        # cache_control breakpoint on the system block, so repointing
        # that variable at one silently ends the discount - the debug
        # line below is what would show it.
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
        # Ask OpenRouter to return token accounting, so a cost
        # regression - cached_tokens dropping to 0 after a prompt edit
        # or a model change - is visible in the logs rather than silent
        # on the bill.
        "usage": {"include": True},
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    try:
        req = Request(
            f"{base_url}/chat/completions",
            data=json.dumps(payload).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        data = _post_with_retry(req, timeout)
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        # Cache accounting, at debug level - there when wanted, silent
        # otherwise. A run whose cached count sits at 0 after the first
        # few calls means the prefix stopped matching, and that is a
        # cost regression nothing else reports. Logged BEFORE the parse
        # below, so a row that overflows max_tokens and fails to parse
        # still reports what it spent.
        usage = data.get("usage") or {}
        cached = (usage.get("prompt_tokens_details") or {}).get("cached_tokens")
        if cached is not None:
            logger.debug(
                "[GEOPOLITICAL_SIGNAL_STAGE_C] prompt_tokens=%s cached=%s"
                " completion_tokens=%s",
                usage.get("prompt_tokens"), cached,
                usage.get("completion_tokens"),
            )
        # confirmed live: this model wraps its JSON in a ```json ... ```
        # markdown fence despite the prompt saying "Return ONLY a JSON
        # object" - extract_json_object (pipeline/classifier.py) already
        # handles this exact case (fence-stripping + brace-boundary
        # extraction), reused here rather than duplicating that logic.
        parsed = extract_json_object(content)
        if not parsed:
            raise ValueError(f"no JSON object found in response: {content!r}")
    except Exception as exc:
        logger.warning(
            "[GEOPOLITICAL_SIGNAL_STAGE_C] Layer 2 tagging failed, storing"
            " HIGH with null tags: %s", exc,
        )
        return dict(_EMPTY_RESULT)

    # named_in is the headline plus article text - exactly what the
    # model was shown, so an entity named only in the headline is still
    # recognised as named, and passing body alone would reject it. The
    # publication-date line is deliberately EXCLUDED: it is metadata
    # this pipeline supplied, not something the document states, and
    # including it would let the summary's date check validate against a
    # date the article never carried.
    named_in = f"Headline: {title}"
    if body:
        named_in += f"\n\nArticle text: {body[:_MAX_BODY_CHARS]}"
    validated = _validate(parsed, body, named_in=named_in, published_hint=published)
    if validated is None:
        logger.warning(
            "[GEOPOLITICAL_SIGNAL_STAGE_C] Layer 2 response had an"
            " unusable shape, storing HIGH with null tags: %r", parsed,
        )
        return dict(_EMPTY_RESULT)
    return validated
