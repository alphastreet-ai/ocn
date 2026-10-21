"""middle_east_market_signal classifier.

Built from "Middle East Market Signals for the AI Economy" (Ayushi Shah,
2026-10-15), classification only - trader cards, "confirmed if / wrong
if" lines and the daily summary are later work.

Labels are this service's own, used directly: `signal` (the spec's
"Alert"), `weak_signal` ("Store"), `noise` ("Ignore"). One difference from
the spec, deliberate: "Ignore" there means discarded; here a `noise` row
is STORED with its reason, like every other domain, so an article we
judged stays distinguishable from one we never saw.

The pipeline, one step per section below (built and tested one step at a
time):
  1. pool today's articles and skip any already judged   (no model)
  2. load the reference tables                           (no model)
  3. route each article to an event type M1-M8           (no model)
  4. rule gate - cheap noise removal                     (no model)
  5. rule-decided sources: SEC, Federal Register, BIS    (no model)
  6. one model call per surviving press/wire article     (extraction only)
  7. code decides the label from the extracted facts     (no model)
  8. read-through: which US tickers, which way           (no model)
  9. corroboration and same-story grouping               (no model)
 10. store
The model extracts facts; code decides labels, so every label traces to a
spec rule.
"""
from __future__ import annotations

import re
from collections import Counter
from typing import Any

# ---------------------------------------------------------------------------
# Step 1 - which source an article came from
# ---------------------------------------------------------------------------
# Every later step routes on this. news-retrieval stamps
# metadata.source_type on all but one source: the Federal Register copy,
# whose fetcher is shared with geopolitical_news and writes no source_type
# - its rows are recognised by their own url scheme instead. The five
# newsapi.ai queries share one source_type, and WHICH query found the
# article (shipping, helium, ...) is the part routing needs, so they are
# told apart by metadata.er_query.

_FEDERAL_REGISTER_SCHEME = "me-federal-register://"


def source_name(article: dict[str, Any]) -> str:
    """The source an article came from, as one stable name:
    `wam_wire`, `spa_wire`, `substack_post`, `bis_press`, `sec_fund_stake`,
    `federal_register`, or `eventregistry:me_<query>`. `unknown` when the
    article carries none of these - logged and counted by the caller,
    never silently routed."""
    metadata = article.get("metadata") or {}
    source_type = metadata.get("source_type")
    if source_type == "eventregistry":
        return metadata.get("er_query") or "eventregistry"
    if source_type:
        return source_type
    if (article.get("url") or "").startswith(_FEDERAL_REGISTER_SCHEME):
        return "federal_register"
    return "unknown"


def describe_pool(articles: list[dict[str, Any]]) -> dict[str, int]:
    """Article count per source - what a dry run reports, and what each
    run logs, so a source that stops delivering shows up as a missing
    line rather than going unnoticed."""
    return dict(Counter(source_name(a) for a in articles).most_common())


# ---------------------------------------------------------------------------
# Step 2 - reference tables
# ---------------------------------------------------------------------------
# Two tables, one copy each:
#  - the COMPANY table lives in news-retrieval (seed.MIDDLE_EAST_TICKER_
#    UNIVERSE, read live from GET /market/middle-east/universe each run):
#    each Gulf/Israeli company and the US companies its news moves
#    (`counterparties`, with `share_price_on_good_news`).
#  - the EVENTS table lives here: the spec's "At a glance" rows for events
#    that name no company (Hormuz closed, oil prices spike, Qatar's helium
#    plant offline). Deciding "this article is a Hormuz closure" is
#    classification, so the table sits with the classifier that uses it.

# Spec section 3, in its own words.
MIDDLE_EAST_EVENT_TYPES: dict[str, str] = {
    "M1": "US decisions on Gulf chip sales",
    "M2": "Gulf data centres switching on",
    "M3": "Gulf money into US AI",
    "M4": "War and shipping",
    "M5": "Helium and gas supply",
    "M6": "Gulf government rules and supplier choices",
    "M7": "Israel",
    "M8": "Enforcement against a named Gulf company",
}


def _affected(names: dict[str, str], share_price: str) -> list[dict[str, str]]:
    """{ticker: name} -> affected-company rows sharing one direction."""
    return [{"company_name": name, "ticker": ticker, "share_price": share_price}
            for ticker, name in names.items()]


_GULF_DATA_CENTRE_SUPPLIERS = {
    "NVDA": "Nvidia", "AMD": "AMD", "CSCO": "Cisco", "ORCL": "Oracle",
    "MSFT": "Microsoft", "VRT": "Vertiv", "GEV": "GE Vernova", "ETN": "Eaton"}
_GULF_FUND_BENEFICIARIES = {"NVDA": "Nvidia", "AVGO": "Broadcom", "MU": "Micron"}

# Spec "At a glance", row by row. `share_price` is the move on THIS event,
# already signed (unlike the company table's share_price_on_good_news,
# which depends on whether the news is good or bad). Where the spec names
# a group rather than companies, its wording is kept in
# `affected_companies_described` and no ticker is invented.
MIDDLE_EAST_EVENTS: list[dict[str, Any]] = [
    {"event_type": "M2", "event": "A Gulf data centre switches on, or receives its chips",
     "why": "More AI hardware is actually sold",
     "affected_companies": _affected(_GULF_DATA_CENTRE_SUPPLIERS, "rise")},
    {"event_type": "M2", "event": "A Gulf data centre is delayed, damaged or cancelled",
     "why": "Expected sales disappear",
     "affected_companies": _affected(_GULF_DATA_CENTRE_SUPPLIERS, "fall")},
    {"event_type": "M6", "event": "A Gulf buyer picks a Chinese chip or cloud supplier",
     "why": "A lost sale, and a reason for Washington to tighten",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD", "MSFT": "Microsoft",
                                      "ORCL": "Oracle", "AMZN": "Amazon"}, "fall")},
    {"event_type": "M3", "event": "A Gulf sovereign fund invests in a US AI company",
     "why": "More cash for AI spending, which becomes chip and server orders",
     "affected_companies": _affected(_GULF_FUND_BENEFICIARIES, "rise"),
     "affected_companies_described": "Nvidia, Broadcom, Micron, data-centre builders"},
    {"event_type": "M3", "event": "A Gulf fund pulls back or misses a commitment",
     "why": "The AI spending cycle has less fuel",
     "affected_companies": _affected(_GULF_FUND_BENEFICIARIES, "fall"),
     "affected_companies_described": "Nvidia, Broadcom, Micron, data-centre builders"},
    {"event_type": "M1", "event": "The US approves chip sales to a Gulf country",
     "why": "Signed deals can now ship and become revenue",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD"}, "rise")},
    {"event_type": "M1", "event": "Congress blocks a deal, or a smuggling case names a Gulf firm",
     "why": "Shipments freeze",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD"}, "fall")},
    {"event_type": "M4", "event": "Hormuz or the Red Sea is closed, or ships are attacked",
     "why": "Oil, gas and freight costs rise; chips and equipment ship more slowly",
     "affected_companies": _affected({"MU": "Micron", "INTC": "Intel"}, "fall"),
     "affected_companies_described": "Chipmakers (Micron, Intel), Apple's supply chain, data-centre operators"},
    {"event_type": "M5", "event": "Qatar's helium plant stays offline",
     "why": "Chip factories run short of a gas they cannot replace",
     "affected_companies": _affected({"MU": "Micron", "INTC": "Intel", "NVDA": "Nvidia",
                                      "AAPL": "Apple"}, "fall")},
    {"event_type": "M5", "event": "Qatar's helium plant stays offline (other side)",
     "why": "Helium becomes scarce, so sellers charge more",
     "affected_companies": _affected({"XOM": "ExxonMobil", "LIN": "Linde",
                                      "APD": "Air Products"}, "rise")},
    {"event_type": "M4", "event": "Oil prices spike",
     "why": "US inflation rises, interest rates stay higher",
     "affected_companies": [],
     "affected_companies_described": "Highly valued tech stocks broadly"},
    {"event_type": "M4", "event": "A ceasefire holds and ships move through Hormuz again",
     "why": "Energy, helium and shipping recover",
     "affected_companies": [],
     "affected_companies_described": "Chipmakers, memory makers, the wider tech market"},
    {"event_type": "M7", "event": "Missile strikes or shutdowns in Israel",
     "why": "Key design centres and factories are disrupted",
     "affected_companies": _affected({"NVDA": "Nvidia", "INTC": "Intel",
                                      "TSEM": "Tower Semiconductor"}, "fall")},
]

_DIRECTIONS = {"rise", "fall"}
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_COMPANY_FIELDS = ("ticker", "company", "exchange", "country", "aliases", "counterparties")
_COUNTERPARTY_FIELDS = ("counterparty_name", "counterparty_ticker", "relationship",
                        "share_price_on_good_news")


def company_table_problems(companies: list[dict[str, Any]]) -> list[str]:
    """Everything wrong with the company table as served, one line each.

    Checked on every load because the table is edited by hand in another
    service: a typo ("rsie"), a lower-case ticker or a name claimed by two
    companies would otherwise produce wrong rows silently - and rows are
    written once and never revised.
    """
    problems: list[str] = []
    if not companies:
        return ["the company table is empty"]
    owner_of: dict[str, str] = {}
    for c in companies:
        label = c.get("company") or "<no name>"
        missing = [f for f in _COMPANY_FIELDS if f not in c]
        if missing:
            problems.append(f"{label}: missing {', '.join(missing)}")
            continue
        for name in [c["company"], *c["aliases"]]:
            key = name.casefold()
            if key in owner_of and owner_of[key] != label:
                problems.append(f"{label}: name {name!r} also belongs to {owner_of[key]}")
            owner_of[key] = label
        for cp in c["counterparties"]:
            cp_label = f"{label} -> {cp.get('counterparty_name') or '<no name>'}"
            missing = [f for f in _COUNTERPARTY_FIELDS if not cp.get(f)]
            if missing:
                problems.append(f"{cp_label}: missing {', '.join(missing)}")
                continue
            if cp["share_price_on_good_news"] not in _DIRECTIONS:
                problems.append(f"{cp_label}: share_price_on_good_news is "
                                f"{cp['share_price_on_good_news']!r}, not rise/fall")
            if not _TICKER_RE.match(cp["counterparty_ticker"]):
                problems.append(f"{cp_label}: ticker {cp['counterparty_ticker']!r} is malformed")
    return problems


def company_name_index(companies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Every company name and alias, case-folded -> its company row. What
    later steps use to check that a company the model says an article is
    about is really one of ours, and to find its counterparties."""
    return {name.casefold(): c
            for c in companies for name in [c["company"], *c["aliases"]]}


# ---------------------------------------------------------------------------
# Event type from the source
# ---------------------------------------------------------------------------
# Structured sources have a FINAL event type: what a 13D/G or a BIS release
# is, is certain from where it came. A press or wire story only has a HINT
# - the query that found it is a good guess (the shipping query is usually
# M4) but not proof, so the model, which reads the article, states the
# final type. Federal Register and BIS start as M1; the rule step moves an
# enforcement action naming a Gulf company to M8.
SOURCE_EVENT_TYPES: dict[str, dict[str, Any]] = {
    "sec_fund_stake": {"event_type": "M3", "final": True},
    "federal_register": {"event_type": "M1", "final": True},
    "bis_press": {"event_type": "M1", "final": True},
    "eventregistry:me_shipping": {"event_type": "M4", "final": False},
    "eventregistry:me_helium": {"event_type": "M5", "final": False},
    "eventregistry:me_qatarenergy": {"event_type": "M5", "final": False},
    "eventregistry:me_israel_sites": {"event_type": "M7", "final": False},
    # Gulf outlets and the wires carry every type (deals, money, rules,
    # war), so they get no hint at all.
    "eventregistry:me_gulf_outlets": {"event_type": None, "final": False},
    "wam_wire": {"event_type": None, "final": False},
    "spa_wire": {"event_type": None, "final": False},
    "substack_post": {"event_type": None, "final": False},
}

# Sources whose label rules decides alone (no model): their facts are
# already structured fields. Everything else goes through the rule gate,
# then the model.
RULE_DECIDED_SOURCES = frozenset({"sec_fund_stake", "federal_register", "bis_press"})


def event_type_from_source(source: str) -> dict[str, Any]:
    """{"event_type": "M1".."M8" | None, "final": bool} for a source name;
    an unknown source gets no type and is never final."""
    return SOURCE_EVENT_TYPES.get(source, {"event_type": None, "final": False})


# ---------------------------------------------------------------------------
# Rule gate - cheap noise removal before any model call
# ---------------------------------------------------------------------------
# A rule removes an article only when it is CLEARLY off target; anything
# uncertain passes to the model. Each rule was written from the real
# headlines (403 articles, 2026-10-19..21) and is re-measured on real data
# before any change - see the module's tests and CLAUDE.md for the counts.
# Every noise row gets a complete sentence saying what the article is and
# why it is not a Middle East market signal.

def _words(*terms: str) -> re.Pattern[str]:
    """Case-insensitive alternation of whole words/phrases."""
    return re.compile(r"\b(?:" + "|".join(terms) + r")\b", re.IGNORECASE)


# Price or market reports, analyst calls and predictions: a market move is
# not a Middle East event - the event behind it, if any, arrives in its own
# article.
_MARKET_WRAP_RE = re.compile(
    r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun):"                      # Globes' daily TASE wrap
    r"|\b(?:US|European|Asian|Gulf|Israel) (?:stocks|shares)\b"
    r"|\bstock market news\b|\bstock prediction\b|\bprice prediction\b"
    r"|\bmarket cap stock movers\b|\banalyst calls\b|\btop picks\b"
    r"|\b(?:starts|initiates|reiterates|keeps)\b[^.]{0,60}\b(?:buy|sell|hold|overweight|outperform)\b"
    r"|\bwhy (?:is )?[\w .]+ stock\b|\bstock (?:rallying|fell|hit the skids)\b"
    r"|\bdollar (?:heads|retreats|firms|slips)\b|\bTASE\b|\bindices\b",
    re.IGNORECASE)

# Where the Gulf, Red Sea and Iran-war theatre is - an article from the
# shipping or LNG queries must name one of these in its headline or its
# opening, or it is about another region (Black Sea, Algeria, Alaska).
_REGION_RE = _words(
    "Hormuz", "Red Sea", "Bab el-Mandeb", "Bab-el-Mandeb", "Gulf", "Persian Gulf",
    "Arabian Gulf", "Gulf of Oman", "Gulf of Aden", "Houthis?", "Iran", "Iranian",
    "IRGC", "Yemen", "Saudi", "Qatar", "Qatari", "QatarEnergy", "Ras Laffan",
    "UAE", "Emirati", "Abu Dhabi", "Dubai", "Fujairah", "Oman", "Middle East",
    "Mideast", "Suez", "ADNOC", "Kuwait", "Bahrain", "Iraq", "Israel")

# What makes helium news a Middle East market signal: the Qatari supply or
# the chip factories that use it (spec M5) - not a Canadian helium miner,
# the HNT crypto token or helium-3 research.
_HELIUM_ANCHOR_RE = _words(
    "Qatar", "Qatari", "QatarEnergy", "Ras Laffan", "Hormuz", "Iran", "Gulf",
    "semiconductors?", "chips?", "chipmakers?", "wafers?", "fabs?", "Linde",
    "Air Products", "Air Liquide", "Messer", "South Korea", "Korean", "Taiwan",
    "Japan", "Micron", "Intel", "Samsung", "SK Hynix", "TSMC")

# What makes Israel news a Middle East market signal (spec M7): a tracked
# Israeli company or chip site, not Israeli politics or archaeology.
_ISRAEL_ANCHOR_RE = _words(
    "Intel", "Nvidia", "Tower Semiconductor", "Tower", "Nova Ltd", "Camtek",
    "Mobileye", "ICL", "Kiryat Gat", "Haifa", "Herzliya", "chip", "chips",
    "semiconductors?", "fab", "factory", "plant", "Home Front Command")

# Third-party statements, visits and ceremonies on the state wires: talk
# about events, never an event (spec: "Ignore - commentary").
_PROTOCOL_RE = re.compile(
    r"\b(?:condemns?|condemning|welcomes?|congratulat\w*|condolences?|"
    r"receives? (?:a )?(?:phone call|French|President|credentials|delegation)|"
    r"attends|witnesses|sermon|discuss(?:es)? [\w\s]*escalation)\b",
    re.IGNORECASE)

_WEEKLY_DIGEST_RE = re.compile(r"^this week[’']s top stories", re.IGNORECASE)

# Event terms that make a wire or Gulf-outlet article worth a model's look
# even when it names no tracked company: the spec's M1-M8 subjects.
_EVENT_TERM_RE = _words(
    "data cent(?:re|er)s?", "megawatts?", "gigawatts?", r"\d+ ?(?:MW|GW)", "GPUs?",
    "chips?", "semiconductors?", "export controls?", "export licen[cs]es?",
    "entity list", r"smuggl\w*", "sovereign (?:wealth )?fund", "Huawei", "Inspur",
    "Alibaba Cloud", "helium", "LNG", "force majeure", "Hormuz", "Houthis?",
    "missiles?", "drones?", "intercepted", "strike", "attack", "tanker")

# Gate rules per source. (rule, sources) - a rule applies only to the
# sources listed; rule-decided sources (SEC, Federal Register, BIS) never
# pass through the gate.
_PRESS_AND_WIRE = frozenset({
    "eventregistry:me_shipping", "eventregistry:me_helium",
    "eventregistry:me_qatarenergy", "eventregistry:me_israel_sites",
    "eventregistry:me_gulf_outlets", "wam_wire", "spa_wire", "substack_post"})

_OPENING_CHARS = 800  # "headline or opening": the first ~2 paragraphs


def _opening(article: dict[str, Any]) -> str:
    return f"{article.get('title') or ''}\n{(article.get('body') or '')[:_OPENING_CHARS]}"


def _names_tracked(text: str, name_index: dict[str, dict[str, Any]],
                   counterparty_names: frozenset[str]) -> bool:
    """True when the text names a tracked company (or alias) or one of the
    US counterparties - whole words, case-insensitive."""
    lowered = text.casefold()
    for name in list(name_index) + list(counterparty_names):
        if re.search(rf"(?<![\w&]){re.escape(name)}(?![\w&])", lowered):
            return True
    return False


def gate_article(
    article: dict[str, Any],
    source: str,
    name_index: dict[str, dict[str, Any]],
    counterparty_names: frozenset[str],
) -> tuple[str, str] | None:
    """The rule gate for one press/wire article: (rule, reason) when the
    article is clearly not a Middle East market signal, else None (it goes
    on to the model). `counterparty_names` are the case-folded US company
    names from the company table.
    """
    if source not in _PRESS_AND_WIRE:
        return None
    title = article.get("title") or ""
    body = article.get("body") or ""
    opening = _opening(article)

    if _MARKET_WRAP_RE.search(title):
        return ("market_wrap",
                f'"{title}" is a market or price report, not an event; the event'
                " behind a price move, if any, is judged from its own article.")
    if source == "substack_post" and _WEEKLY_DIGEST_RE.search(title):
        return ("weekly_digest",
                "This is Middle East AI News' weekly roundup, which repeats"
                " stories already judged one by one from their own posts.")
    if source == "eventregistry:me_helium" and not _HELIUM_ANCHOR_RE.search(f"{title}\n{body}"):
        return ("helium_off_target",
                f'"{title}" is helium news with no link to the Qatari supply or to'
                " the chip factories that depend on it - the only helium the"
                " Middle East signal covers.")
    if source in ("eventregistry:me_shipping", "eventregistry:me_qatarenergy") \
            and not _REGION_RE.search(opening):
        return ("outside_region",
                f'"{title}" concerns shipping or gas outside the Gulf, Red Sea'
                " and Iran-war theatre; it names none of those places.")
    if source == "eventregistry:me_israel_sites" and not _ISRAEL_ANCHOR_RE.search(opening):
        return ("israel_unrelated",
                f'"{title}" is Israel news that names no tracked Israeli company,'
                " chip site or factory - not a disruption to the companies the"
                " signal follows.")
    if source in ("wam_wire", "spa_wire") and _PROTOCOL_RE.search(title):
        return ("protocol_statement",
                f'"{title}" reports a statement, visit or ceremony about events,'
                " not an event itself; the events are judged from their own"
                " reports.")
    if source in ("eventregistry:me_gulf_outlets", "wam_wire", "spa_wire", "substack_post") \
            and not _names_tracked(f"{title}\n{body}", name_index, counterparty_names) \
            and not _EVENT_TERM_RE.search(f"{title}\n{body}"):
        return ("no_tracked_subject",
                f'"{title}" names no tracked company, no US company the signal'
                " follows, and no data-centre, chip, export, fund, energy or"
                " security event.")
    return None


def classify_middle_east_signal_batch(
    articles: list[dict[str, Any]],
    companies: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Classify a batch of new articles against the company table (already
    checked by company_table_problems). Returns one {"article", "result"}
    per article it has judged.

    Steps 3-9 are not built yet, so this judges nothing and returns []:
    an article must never be stored before it has a real label, or the
    already-judged skip would hide it from every later run.
    """
    return []
