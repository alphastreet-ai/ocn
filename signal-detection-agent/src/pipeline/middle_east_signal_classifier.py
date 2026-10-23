"""middle_east_market_signal classifier.

Built from "Middle East Market Signals for the AI Economy" (Ayushi Shah,
2026-10-15), classification only - trader cards, "confirmed if / wrong
if" lines and the daily summary are later work.

Labels are this service's own, used directly: `signal` (the spec's
"Alert"), `weak_signal` ("Store"), `noise` ("Ignore"). One difference from
the spec, deliberate: "Ignore" there means discarded; here a `noise` row
is STORED with its reason, like every other domain, so an article we
judged stays distinguishable from one we never saw.

The pipeline, one step per section below:
  1. pool today's articles and skip any already judged   (no model)
  2. load the reference tables                           (no model)
  3. event type from the source: final or a hint         (no model)
  4. headline rules: market wraps, weekly digests        (no model)
  5. SEC 13D/G: labelled from the filing's fields        (no model)
  6. one model call per other article, full body, read once (facts only)
  7. code checks the facts against the text, decides the label from the
     spec's section 7 rules, and adds the read-through   (no model)
  8. store
The model extracts facts; code decides labels, so every label traces to a
spec rule, and a rule change can re-label stored rows from
`metadata.extracted` without calling the model again.

Not built (deliberately): same-story grouping. news-retrieval already
merges same-event articles into `also_reported_by`; a separate step is
only worth building if the stored rows show a need.
"""
from __future__ import annotations

import json
import logging
import re
import statistics
import threading
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import config
from pipeline.geopolitical_signal_stage_d import build_corroboration
from pipeline.geopolitical_signal_stage_e import _entity_keys, _is_specific

logger = logging.getLogger(__name__)

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

# Spec "At a glance", row by row. `news` is whether the row is the good or
# the bad side of its event type - how the read-through finds the row for an
# article. `share_price` is the move on THIS event, already signed (unlike the company table's share_price_on_good_news,
# which depends on whether the news is good or bad). Where the spec names
# a group rather than companies, its wording is kept in
# `affected_companies_described` and no ticker is invented.
MIDDLE_EAST_EVENTS: list[dict[str, Any]] = [
    {"event_type": "M2", "news": "good", "event": "A Gulf data centre switches on, or receives its chips",
     "why": "More AI hardware is actually sold",
     "affected_companies": _affected(_GULF_DATA_CENTRE_SUPPLIERS, "rise")},
    {"event_type": "M2", "news": "bad", "event": "A Gulf data centre is delayed, damaged or cancelled",
     "why": "Expected sales disappear",
     "affected_companies": _affected(_GULF_DATA_CENTRE_SUPPLIERS, "fall")},
    {"event_type": "M6", "news": "bad", "event": "A Gulf buyer picks a Chinese chip or cloud supplier",
     "why": "A lost sale, and a reason for Washington to tighten",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD", "MSFT": "Microsoft",
                                      "ORCL": "Oracle", "AMZN": "Amazon"}, "fall")},
    {"event_type": "M3", "news": "good", "event": "A Gulf sovereign fund invests in a US AI company",
     "why": "More cash for AI spending, which becomes chip and server orders",
     "affected_companies": _affected(_GULF_FUND_BENEFICIARIES, "rise"),
     "affected_companies_described": "Nvidia, Broadcom, Micron, data-centre builders"},
    {"event_type": "M3", "news": "bad", "event": "A Gulf fund pulls back or misses a commitment",
     "why": "The AI spending cycle has less fuel",
     "affected_companies": _affected(_GULF_FUND_BENEFICIARIES, "fall"),
     "affected_companies_described": "Nvidia, Broadcom, Micron, data-centre builders"},
    {"event_type": "M1", "news": "good", "event": "The US approves chip sales to a Gulf country",
     "why": "Signed deals can now ship and become revenue",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD"}, "rise")},
    {"event_type": "M1", "news": "bad", "event": "Congress blocks a deal, or a smuggling case names a Gulf firm",
     "why": "Shipments freeze",
     "affected_companies": _affected({"NVDA": "Nvidia", "AMD": "AMD"}, "fall")},
    {"event_type": "M4", "news": "bad", "event": "Hormuz or the Red Sea is closed, or ships are attacked",
     "why": "Oil, gas and freight costs rise; chips and equipment ship more slowly",
     "affected_companies": _affected({"MU": "Micron", "INTC": "Intel"}, "fall"),
     "affected_companies_described": "Chipmakers (Micron, Intel), Apple's supply chain, data-centre operators"},
    {"event_type": "M5", "news": "bad", "event": "Qatar's helium plant stays offline",
     "why": "Chip factories run short of a gas they cannot replace",
     "affected_companies": _affected({"MU": "Micron", "INTC": "Intel", "NVDA": "Nvidia",
                                      "AAPL": "Apple"}, "fall")},
    {"event_type": "M5", "news": "bad", "event": "Qatar's helium plant stays offline (other side)",
     "why": "Helium becomes scarce, so sellers charge more",
     "affected_companies": _affected({"XOM": "ExxonMobil", "LIN": "Linde",
                                      "APD": "Air Products"}, "rise")},
    {"event_type": "M4", "news": "bad", "event": "Oil prices spike",
     "why": "US inflation rises, interest rates stay higher",
     "affected_companies": [],
     "affected_companies_described": "Highly valued tech stocks broadly"},
    {"event_type": "M4", "news": "good", "event": "A ceasefire holds and ships move through Hormuz again",
     "why": "Energy, helium and shipping recover",
     "affected_companies": [],
     "affected_companies_described": "Chipmakers, memory makers, the wider tech market"},
    {"event_type": "M7", "news": "bad", "event": "Missile strikes or shutdowns in Israel",
     "why": "Key design centres and factories are disrupted",
     "affected_companies": _affected({"NVDA": "Nvidia", "INTC": "Intel",
                                      "TSEM": "Tower Semiconductor"}, "fall")},
]

_DIRECTIONS = {"rise", "fall"}
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")
_COMPANY_FIELDS = ("ticker", "company", "kind", "exchange", "country", "aliases", "counterparties")
# "fund": a sovereign fund - its counterparties are what it owns; "site": an
# Israeli site of a US company - an event there moves that owner only.
_COMPANY_KINDS = {"company", "fund", "site"}
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
        if c["kind"] not in _COMPANY_KINDS:
            problems.append(f"{label}: kind is {c['kind']!r}, not one of {sorted(_COMPANY_KINDS)}")
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
# Only the SEC 13D/G has a FINAL event type: what it is, is certain from
# where it came, and its facts are structured fields. Everything else only
# has a HINT - the query that found it is a good guess (the shipping query
# is usually M4) but not proof, so the model, which reads the article,
# states the final type. Federal Register and BIS are hinted M1 but read by
# the model too: they are free text, an enforcement order naming a Gulf
# company is M8, and the Federal Register filter also catches notices that
# mention the Gulf only in passing (a Russian airline's denial order).
SOURCE_EVENT_TYPES: dict[str, dict[str, Any]] = {
    "sec_fund_stake": {"event_type": "M3", "final": True},
    "federal_register": {"event_type": "M1", "final": False},
    "bis_press": {"event_type": "M1", "final": False},
    "eventregistry:me_shipping": {"event_type": "M4", "final": False},
    "eventregistry:me_energy_sites": {"event_type": "M4", "final": False},
    "eventregistry:me_strike_plans": {"event_type": "M4", "final": False},
    "eventregistry:me_gulf_money": {"event_type": "M3", "final": False},
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

# Sources code labels alone (no model): their facts are already structured
# fields. Everything else goes through the headline rules, then the model.
RULE_DECIDED_SOURCES = frozenset({"sec_fund_stake"})


def event_type_from_source(source: str) -> dict[str, Any]:
    """{"event_type": "M1".."M8" | None, "final": bool} for a source name;
    an unknown source gets no type and is never final."""
    return SOURCE_EVENT_TYPES.get(source, {"event_type": None, "final": False})




# ---------------------------------------------------------------------------
# Step 4 - headline rules: the only press articles skipped before the model
# ---------------------------------------------------------------------------
# Two shapes are certain from the headline alone and never carry an event of
# their own. Everything else - however off-topic it looks - goes to the
# model: the 50-article test (2026-10-21) showed keyword rules on the
# opening throw out items the spec stores (partnerships, plans, threats).

# Price or market reports, analyst calls and predictions: a market move is
# not a Middle East event - the event behind it, if any, arrives in its own
# article. "Why X stock fell" headlines are NOT skipped: one ("Why Intel
# Stock Hit the Skids on Wednesday") was the report of the Kiryat Gat fab
# explosion, so they go to the model.
_MARKET_WRAP_RE = re.compile(
    r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun):"                      # Globes' daily TASE wrap
    r"|\b(?:US|European|Asian|Gulf|Israel) (?:stocks|shares)\b"
    r"|\bstock market news\b|\bstock prediction\b|\bprice prediction\b"
    r"|\bmarket cap stock movers\b|\banalyst calls\b|\btop picks\b"
    r"|\b(?:starts|initiates|reiterates|keeps)\b[^.]{0,60}\b(?:buy|sell|hold|overweight|outperform)\b"
    r"|\bdollar (?:heads|retreats|firms|slips)\b|\bTASE\b|\bindices\b",
    re.IGNORECASE)

# Middle East AI News' weekly roundup repeats posts already judged one by one.
_WEEKLY_DIGEST_RE = re.compile(r"^this week[’']s top stories", re.IGNORECASE)


def headline_rule(article: dict[str, Any], source: str) -> tuple[str, str] | None:
    """(rule, reason) when the headline alone shows the article carries no
    event of its own, else None. Official sources are never skipped."""
    if source in RULE_DECIDED_SOURCES or source in ("federal_register", "bis_press"):
        return None
    title = (article.get("title") or "").strip()
    if source == "substack_post" and _WEEKLY_DIGEST_RE.search(title):
        return ("weekly_digest",
                "This is Middle East AI News' weekly roundup; each of its stories is"
                " covered on its own.")
    if _MARKET_WRAP_RE.search(title):
        return ("market_wrap",
                f'"{title}" is a report on share-price moves, not a news event.')
    return None


# ---------------------------------------------------------------------------
# Step 5 - SEC 13D/G: labelled from the filing's own fields
# ---------------------------------------------------------------------------
# Spec 7.4 (M3): a completed investment by a Gulf fund in a US AI company is
# an alert; so is a fund selling or reducing a stake. "US AI company" means
# a US company in research-universe's AI Economy Universe - the company list
# every other domain in this service already uses. A 13D/G is filed at 5%:
#  - a NEW filing (no "/A") is a new holding of 5% or more  -> signal, rise
#  - an amendment below 5% means the fund sold down           -> signal, fall
#  - any other amendment states the new holding but not the old one, so the
#    size of the change is unknown                            -> weak_signal
_SEC_FILING_THRESHOLD_PCT = 5.0
_COMPANY_SUFFIX_RE = re.compile(
    r"\b(?:inc|incorporated|corp|corporation|co|company|ltd|limited|plc|llc|lp|"
    r"holdings?|group|sa|se|nv|ag|the|class [abc])\b")


def company_key(name: str) -> str:
    """A company name reduced for matching across sources: "NVIDIA Corp."
    and "Nvidia Corporation" -> "nvidia"."""
    key = unicodedata.normalize("NFKC", name or "").casefold().replace("&", " and ")
    key = re.sub(r"[^\w\s]", " ", key)
    key = _COMPANY_SUFFIX_RE.sub(" ", key)
    return re.sub(r"\s+", " ", key).strip()


def us_universe_index(companies: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """research-universe's US companies by company_key of name and aliases."""
    index: dict[str, dict[str, Any]] = {}
    for c in companies:
        for name in [c.get("company_name") or "", *(c.get("aliases") or [])]:
            key = company_key(name)
            if key:
                index.setdefault(key, c)
    return index


def _pct(value: float) -> str:
    return f"{value:g}%"


def judge_sec_stake(article: dict[str, Any], universe: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """The result for one 13D/G filing - no model call. The summary is
    built from the filing's own fields, so it needs no check."""
    md = article.get("metadata") or {}
    fund = md.get("fund") or "A Gulf fund"
    issuer = md.get("issuer_name") or "an unnamed issuer"
    filed = md.get("filing_date") or "an unstated date"
    pct = md.get("percent_of_class")
    pct = float(pct) if isinstance(pct, (int, float)) or (isinstance(pct, str) and _is_number(pct)) else None
    shares = md.get("shares_owned")
    shares = float(shares) if isinstance(shares, (int, float)) or (isinstance(shares, str) and _is_number(shares)) else None
    amended = (md.get("form") or "").upper().endswith("/A")
    listed = universe.get(company_key(issuer))
    ticker = listed.get("ticker") if listed else None
    entities = [{"name": fund, "ticker": None, "country": None, "role": "subject"},
                {"name": issuer, "ticker": ticker, "country": None, "role": "subject"}]
    holding = (f"{shares:,.0f} shares ({_pct(pct)} of the class)" if shares is not None and pct is not None
               else f"{_pct(pct)} of the class" if pct is not None else "an undisclosed number of shares")
    summary = (f"{fund} {'updated its' if amended else 'filed a new'} US SEC ownership filing on {filed}"
               f" for {issuer}: it holds {holding}.")
    name = f"{issuer} ({ticker})" if ticker else issuer

    if not listed:
        return {"signal": "noise",
                "reason": f"{issuer} is not one of the US AI companies this signal follows.",
                "entities": entities,
                "metadata": {"summary": summary, "rule": "sec_issuer_not_ai_universe"}}
    if not amended:
        label, rule, code, share_price = "signal", "sec_new_stake", "m3_investment", "rise"
        reason = f"{fund} disclosed a new stake in {name}: Gulf money going into a US AI company."
    elif pct is not None and pct < _SEC_FILING_THRESHOLD_PCT:
        label, rule, code, share_price = "signal", "sec_stake_cut_below_5pct", "m3_pullback", "fall"
        reason = f"{fund} now holds {_pct(pct)} of {name}: it has sold most or all of its stake."
    else:
        label, rule, code, share_price = "weak_signal", "sec_stake_amended", "m3_investment", None
        reason = (f"{fund} updated its holding in {name}. The filing does not say what it held"
                  " before, so the size of the change is unknown.")
    if share_price:
        entities.append({"name": issuer, "ticker": ticker, "role": "affected",
                         "share_price": share_price,
                         "relationship": f"{fund} holds its shares"})
    return {"signal": label, "reason": reason, "entities": entities,
            "metadata": {"event": {"type": "M3", "code": code}, "summary": summary, "rule": rule}}


def _is_number(text: str) -> bool:
    try:
        float(text)
        return True
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Step 6 - one model call per article: facts only
# ---------------------------------------------------------------------------
PROMPT_VERSION = "middle_east_signal_extract_v1"

# The whole body is sent - the 50-article test showed shorter cuts change
# the answer on 2-5 articles in 50. This cap only guards against a freak
# page (a Federal Register rule can run to hundreds of thousands of
# characters); a cut body is recorded in metadata.body_truncated.
_BODY_MAX_CHARS = 60_000

_SOURCE_DESCRIPTIONS = {
    "wam_wire": "WAM, the UAE government's news agency",
    "spa_wire": "SPA, the Saudi government's news agency",
    "substack_post": "Middle East AI News, a specialist newsletter",
    "bis_press": "a US Bureau of Industry and Security press release",
    "federal_register": "a US Federal Register notice",
}


def _source_line(article: dict[str, Any], source: str) -> str:
    if source in _SOURCE_DESCRIPTIONS:
        return _SOURCE_DESCRIPTIONS[source]
    outlet = (article.get("metadata") or {}).get("source_uri")
    return f"a news article from {outlet}" if outlet else "a news article"


def _published(article: dict[str, Any]) -> str | None:
    md = article.get("metadata") or {}
    return article.get("published") or md.get("date_time") or md.get("filing_date")


def _whole_word(name: str, text_folded: str) -> bool:
    return re.search(rf"(?<![\w&]){re.escape(name)}(?![\w&])", text_folded) is not None


def tracked_names_in(text: str, name_index: dict[str, dict[str, Any]]) -> list[str]:
    """The table companies the text names (by name or alias), canonical
    names, in table order - given to the model so it writes them the same
    way, and so it is not left to guess which names are ours."""
    folded = text.casefold()
    found: dict[str, None] = {}
    for name, company in name_index.items():
        if _whole_word(name, folded):
            found[company["company"]] = None
    return list(found)


def _user_message(article: dict[str, Any], source: str, body: str,
                  tracked: list[str]) -> str:
    hint = event_type_from_source(source)["event_type"]
    lines = [
        "<article>",
        f"Source: {_source_line(article, source)}",
        f"Published: {_published(article) or 'not stated'}",
    ]
    if hint:
        lines.append(f"Found by a search for {hint} ({MIDDLE_EAST_EVENT_TYPES[hint]}) - a hint only.")
    effective = (article.get("metadata") or {}).get("effective_on")
    if effective:
        lines.append(f"Effective date stated by the Federal Register: {effective}")
    if tracked:
        lines.append(f"Tracked companies named in the text: {', '.join(tracked)}")
    lines += [f"Headline: {article.get('title') or ''}", "Text:", body, "</article>"]
    return "\n".join(lines)


# Spend of the current run. The instructions are identical for every
# article and sent first, so the provider caches them; keep anything that
# varies per article OUT of the system prompt or that cache is lost.
_usage_lock = threading.Lock()
_usage = {"calls": 0, "prompt": 0, "cached": 0, "completion": 0, "cost": 0.0}


def _record_usage(u: dict[str, Any]) -> None:
    with _usage_lock:
        _usage["calls"] += 1
        _usage["prompt"] += u.get("prompt_tokens") or 0
        _usage["cached"] += (u.get("prompt_tokens_details") or {}).get("cached_tokens") or 0
        _usage["completion"] += u.get("completion_tokens") or 0
        _usage["cost"] += float(u.get("cost") or 0)


def spend() -> dict[str, Any]:
    with _usage_lock:
        return dict(_usage)


def _reset_spend() -> None:
    with _usage_lock:
        _usage.update(calls=0, prompt=0, cached=0, completion=0, cost=0.0)


_RETRY_STATUS = {408, 429, 500, 502, 503, 504}


def _call_model(system: str, user: str, follow_up: list[dict[str, str]] | None = None) -> str | None:
    """One chat completion. Retried only when the request itself failed
    (timeout, rate limit, server error) - never to get a different answer;
    the one repair turn (`follow_up`, see _model_result) is a separate call.
    None when every attempt failed."""
    payload = {
        "model": config.MIDDLE_EAST_SIGNAL_MODEL,
        "temperature": 0,
        "max_tokens": 1300,
        "response_format": {"type": "json_object"},
        "messages": [{"role": "system", "content": system},
                     {"role": "user", "content": user}, *(follow_up or [])],
        "usage": {"include": True},
    }
    attempts = max(1, config.OPENAI_MAX_ATTEMPTS)
    for attempt in range(1, attempts + 1):
        try:
            req = Request(
                f"{config.OPENAI_BASE_URL}/chat/completions",
                data=json.dumps(payload).encode("utf-8"),
                headers={"Authorization": f"Bearer {config.OPENAI_API_KEY}",
                         "Content-Type": "application/json"},
                method="POST")
            with urlopen(req, timeout=config.OPENAI_TIMEOUT) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
            _record_usage(data.get("usage") or {})
            return (data.get("choices", [{}])[0].get("message", {}).get("content") or "").strip()
        except HTTPError as exc:
            if exc.code not in _RETRY_STATUS or attempt == attempts:
                logger.warning("[MIDDLE_EAST_SIGNAL] model call failed: HTTP %s", exc.code)
                return None
        except Exception as exc:  # timeout, connection reset, bad JSON envelope
            if attempt == attempts:
                logger.warning("[MIDDLE_EAST_SIGNAL] model call failed: %s", exc)
                return None
        time.sleep(2 * attempt)
    return None


# ---------------------------------------------------------------------------
# Step 7a - check the model's facts against the text
# ---------------------------------------------------------------------------
# Nothing the model writes is kept unless the text supports it. A fact that
# fails is removed - never repaired with a second call - and the label rules
# then see less, so a failed check can only LOWER a label, never raise it.

EVENT_CODES: dict[str, str] = {
    code: code[:2].upper() for code in (
        "m1_rule_or_licence", "m1_congress_action", "m1_export_category_change",
        "m1_draft_or_hearing", "m1_repeat_or_speech",
        "m2_switched_on", "m2_chips_delivered", "m2_setback", "m2_deal_or_construction",
        "m2_company_update", "m2_visit_or_ceremony",
        "m3_investment", "m3_pullback", "m3_talks",
        "m4_strike_on_site", "m4_other_strike", "m4_interception",
        "m4_waterway_closed", "m4_waterway_reopened", "m4_ship_traffic", "m4_shipping_or_insurance",
        "m4_ship_incident", "m4_ship_sunk_or_crew_killed", "m4_attack_tally", "m4_strike_plan",
        "m4_threat_or_statement", "m4_commentary",
        "m5_qatar_force_majeure", "m5_supplier_rationing", "m5_asia_shortfall", "m5_commentary",
        "m6_binding_rule", "m6_chinese_supplier", "m6_strategy", "m6_speech",
        "m7_site_disruption", "m7_site_incident", "m7_home_front_restriction", "m7_acquisition",
        "m7_company_deal", "m7_commentary",
        "m8_enforcement", "m8_investigation_unnamed",
    )}
_CONFIRMATIONS = ("official", "named_source", "unnamed")
_NEWS = ("good", "bad", "mixed")
_COMPANIES_MAX = 8

_PUNCT_MAP = str.maketrans({"‘": "'", "’": "'", "“": '"', "”": '"', "–": "-", "—": "-",
                            " ": " ", "‑": "-"})


def normalize_text(text: str) -> str:
    """Case, quote, dash, whitespace and abbreviation-dot differences
    removed, so a quote the model copied is found however the page typeset
    it: "U.K. Maritime Trade Operations" and "UK Maritime Trade Operations"
    are one name (a real UKMTO confirmation was dropped over the dots). Only
    a dot after a single letter goes - decimals ("1.5") are untouched."""
    text = unicodedata.normalize("NFKC", text or "").translate(_PUNCT_MAP).casefold()
    text = re.sub(r"\b([^\W\d_])\.", r"\1", text)
    return re.sub(r"\s+", " ", text).strip()


def _in_text(value: Any, text_norm: str) -> bool:
    return isinstance(value, str) and bool(value.strip()) and normalize_text(value) in text_norm


# A number with any letters stuck to its front ("GB300"), so "300MW" is not
# accepted because the text mentions Nvidia GB300 systems.
# A number, optionally after a currency code stuck to it ("aed2.5m") and
# before a scale word ("2.5 million", "680k"). A number with other letters
# stuck to its front ("gb300") is never a quantity: it is matched as a
# whole token instead, so "300MW" is not accepted because the text names
# Nvidia GB300 systems.
_CURRENCY = r"(?:aed|sar|qar|usd|eur|gbp|inr|rs|kwd|bhd|omr|ils|nis|us\$|\$|€|£)"
_SCALES = {"k": 1e3, "thousand": 1e3, "m": 1e6, "mn": 1e6, "million": 1e6,
           "b": 1e9, "bn": 1e9, "billion": 1e9, "tn": 1e12, "trillion": 1e12}
_QUANTITY_RE = re.compile(
    rf"(?<![a-z0-9.]){_CURRENCY}?\s?(\d[\d,]*(?:\.\d+)?)(?![\d])"
    rf"(?:\s?({'|'.join(sorted(_SCALES, key=len, reverse=True))})(?![a-z]))?")
_CODE_NUMBER_RE = re.compile(r"(?<![a-z0-9])[a-z]+(?<!aed)(?<!sar)(?<!qar)(?<!usd)\d[\d,.]*")


def _quantities(text: str) -> set[float]:
    """Every quantity in the text, both as written and scaled
    ("2.5m" -> 2.5 and 2500000)."""
    values: set[float] = set()
    for number, scale in _QUANTITY_RE.findall(text):
        value = float(number.replace(",", ""))
        values.add(value)
        if scale:
            values.add(round(value * _SCALES[scale], 6))
    return values


def _numbers_supported(sentence: str, text_norm: str) -> bool:
    """Every quantity in the sentence is in the text (as written or by
    value: "AED 2.5 million" matches "AED2.5M"), and every letter-number
    code ("GB300") appears as a whole token."""
    sentence = normalize_text(sentence)
    text_values = _quantities(text_norm)
    for number, scale in _QUANTITY_RE.findall(sentence):
        value = float(number.replace(",", ""))
        wanted = round(value * _SCALES[scale], 6) if scale else value
        if wanted not in text_values and value not in text_values:
            return False
    for code in _CODE_NUMBER_RE.findall(sentence):
        if not re.search(rf"(?<![a-z0-9]){re.escape(code)}(?![0-9])", text_norm):
            return False
    return True


_GULF_COUNTRIES = {
    "uae": "UAE", "united arab emirates": "UAE", "abu dhabi": "UAE", "dubai": "UAE",
    "saudi arabia": "Saudi Arabia", "saudi": "Saudi Arabia", "ksa": "Saudi Arabia",
    "qatar": "Qatar", "kuwait": "Kuwait", "bahrain": "Bahrain", "oman": "Oman",
}
_STRIKE_ALERT_COUNTRIES = {"UAE", "Saudi Arabia", "Qatar", "Israel"}
_HOME_FRONT_ALERT_PLACES = ("haifa", "tel aviv", "herzliya", "kiryat gat")


def country_name(country: Any) -> str | None:
    """The model's country, in the table's spelling for Gulf countries and
    Israel; other countries kept as written."""
    if not isinstance(country, str) or not country.strip():
        return None
    key = country.strip().casefold()
    if key in ("null", "none", "unknown", "n/a"):
        return None
    if key in _GULF_COUNTRIES:
        return _GULF_COUNTRIES[key]
    if key == "israel":
        return "Israel"
    return country.strip()


def _match_tracked(name: str, name_index: dict[str, dict[str, Any]]) -> dict[str, Any] | None:
    """The table company a name refers to: exact name or alias first, else
    the longest table name inside it ("Khazna Data Centers" -> G42)."""
    folded = name.casefold().strip()
    if folded in name_index:
        return name_index[folded]
    for key in sorted(name_index, key=len, reverse=True):
        if _whole_word(key, folded):
            return name_index[key]
    return None


_NAME_STOPWORDS = {"the", "of", "and", "for", "via", "its", "a", "an", "in", "on", "s"}


def _name_in_text(name: str, text_norm: str) -> bool:
    """A source name the text supports: the name as written, or every
    significant word of it as a whole word ("IRGC Navy" when the text says
    "the IRGC's navy"). An invented name fails both."""
    if normalize_text(name) in text_norm:
        return True
    words = [w for w in re.findall(r"[\w-]+", normalize_text(name)) if w not in _NAME_STOPWORDS]
    return bool(words) and all(_whole_word(w, text_norm) for w in words)


# Words that only say what KIND of place a name is. The model writes
# "Strait of Hormuz" where the text says "Hormuz"; an exact match dropped
# the place, and a row without a place cannot join its event or story.
_PLACE_KIND_WORDS = {"strait", "straits", "sea", "gulf", "port", "city", "province", "region",
                     "governorate", "emirate", "kingdom", "republic", "island", "islands",
                     "coast", "waters", "field", "plant", "terminal", "facility", "site"}


def _place_in_text(place: str, text_norm: str) -> bool:
    """A place the text supports: as written, word by word, or by its
    naming words alone ("Strait of Hormuz" when the text says "Hormuz")."""
    if _name_in_text(place, text_norm):
        return True
    words = [w for w in re.findall(r"[\w-]+", normalize_text(place))
             if w not in _NAME_STOPWORDS and w not in _PLACE_KIND_WORDS]
    return bool(words) and all(_whole_word(w, text_norm) for w in words)


_ANONYMOUS_RE = re.compile(
    r"\b(?:people|persons?|sources?|insiders?|familiar|aware|anonymity|unnamed|"
    r"officials? (?:said|say)|according to reports)\b", re.IGNORECASE)
# "an Arab diplomat posted to a major Middle Eastern country" or "a senior
# Iranian official" names nobody: an indefinite person described only by
# nationality or region (full run 2026-10-23: kept as a named source and,
# with Reuters, raised a strike-plan report to signal). "a spokesperson for
# the Houthi armed forces" or "a Pentagon spokesperson" names the body.
_INDEFINITE_RE = re.compile(r"^\s*(?:an?|one|two|three|four|several|some|multiple)\s+", re.IGNORECASE)
_DESCRIPTOR_WORDS = frozenset({
    "arab", "arabic", "iranian", "israeli", "saudi", "emirati", "qatari", "kuwaiti", "bahraini",
    "omani", "yemeni", "iraqi", "egyptian", "jordanian", "lebanese", "syrian", "turkish",
    "american", "us", "u.s", "european", "western", "eastern", "middle", "gulf", "asian",
    "chinese", "russian", "indian", "pakistani", "british", "french", "german", "japanese",
    "korean", "regional", "foreign", "senior"})


def is_anonymous_source(by: str) -> bool:
    """True when `by` names no person or body (see _INDEFINITE_RE)."""
    if _ANONYMOUS_RE.search(by):
        return True
    m = _INDEFINITE_RE.match(by)
    if not m:
        return False
    names = [w for w in re.findall(r"\b[A-Z][\w.'-]*", by[m.end():])
             if w.strip(".'").casefold() not in _DESCRIPTOR_WORDS]
    return not names


def outlet_name(article: dict[str, Any], source: str) -> str | None:
    """The publisher's own name, case-folded: what can never confirm its
    own story ("tokenpost" for tokenpost.com, "wam" for the UAE wire)."""
    if source in ("wam_wire", "spa_wire"):
        return source.split("_")[0]
    uri = (article.get("metadata") or {}).get("source_uri") or ""
    stem = uri.split(".")[-2] if uri.count(".") >= 1 else uri
    return stem.casefold() or None


def _month_end(year: int, month: int) -> date:
    return (date(year + month // 12, month % 12 + 1, 1) - timedelta(days=1))


def _as_date(value: Any) -> date | None:
    """A datetime, date or ISO string -> date; None when it is none of them.
    A month alone ("2026-09", an event dated only "last month") -> the last
    day of that month: the latest it can have happened."""
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if isinstance(value, str) and re.fullmatch(r"\d{4}-\d{2}", value.strip()):
        year, month = map(int, value.strip().split("-"))
        return _month_end(year, month) if 1 <= month <= 12 else None
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.strip().replace("Z", "+00:00")[:25]).date()
        except ValueError:
            return None
    return None


def event_date_checked(raw: Any, published: Any) -> str | None:
    """The model's event date, as stored: a real YYYY-MM-DD day no later
    than the day after the article, or a YYYY-MM month no later than the
    article's month (an event dated only "last month"); else None - a date
    after the article is a misreading."""
    if not isinstance(raw, str):
        return None
    raw = raw.strip()
    pub = _as_date(published)
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        when = _as_date(raw)
        if when is None or (pub is not None and when > pub + timedelta(days=1)):
            return None
        return when.isoformat()
    if re.fullmatch(r"\d{4}-\d{2}", raw):
        when = _as_date(raw)
        if when is None or (pub is not None and (when.year, when.month) > (pub.year, pub.month)):
            return None
        return raw
    return None


def _exact_day(value: Any) -> date | None:
    """An event date precise to the day - only these join reports of one
    event; a month is too coarse to tell two events apart."""
    return _as_date(value) if isinstance(value, str) and len(value.strip()) == 10 else None


def _checked_confirmation(conf: Any, body: str, outlet: str | None,
                          dropped: list[str], prefix: str = "confirmation") -> dict[str, Any]:
    """{"level", "by"} kept only where the text supports it. A level above
    "unnamed" needs a `by` the text contains - a confirmation nobody in the
    article gave is not one, so it falls to "unnamed" (which can only lower
    a label)."""
    conf = conf if isinstance(conf, dict) else {"level": conf}
    level_c = conf.get("level")
    if level_c not in _CONFIRMATIONS:
        dropped.append(prefix)
        level_c = "unnamed"
    by = conf.get("by") if isinstance(conf.get("by"), str) and conf.get("by").strip() else None
    # Checked against the BODY: a headline often ends "| <outlet>".
    if by and not _name_in_text(by, normalize_text(body)):
        dropped.append(f"{prefix}.by:{by}")
        by = None
    # "two people aware of the plans" names nobody.
    if by and is_anonymous_source(by):
        dropped.append(f"{prefix}.by_anonymous:{by}")
        by = None
    if level_c != "unnamed" and not by:
        dropped.append(f"{prefix}.level")
        level_c = "unnamed"
    # The outlet reporting a story does not confirm it: at most a named source.
    # Compared without spaces: the outlet is "ndtvprofit", the model writes
    # "NDTV Profit".
    if (by and level_c == "official" and outlet
            and (_whole_word(outlet, normalize_text(by))
                 or re.sub(r"\W", "", normalize_text(by)) == re.sub(r"\W", "", outlet))):
        dropped.append(f"{prefix}.outlet_not_official")
        level_c = "named_source"
    return {"level": level_c, **({"by": by} if by else {})}


# Event codes whose facts include a deal (and so a party to confirm it).
_DEAL_CODES = frozenset({"m2_deal_or_construction", "m2_chips_delivered", "m2_switched_on",
                         "m3_investment", "m3_talks", "m6_chinese_supplier", "m7_acquisition",
                         "m7_company_deal"})
# Plans, threats, talks and statements: listed as other events, but never
# taking over a row's label from what actually happened (full run
# 2026-10-23: a coalition evacuation notice became a "planned strike" alert).
_STATEMENT_CODES = frozenset({"m4_threat_or_statement", "m4_strike_plan", "m3_talks",
                              "m1_draft_or_hearing", "m2_company_update", "m6_strategy",
                              "m4_ship_traffic"})
# Codes that are never "another event" in an article: they describe the
# article itself (commentary), not something that happened.
_NOT_AN_OTHER_EVENT = frozenset({"m4_commentary", "m5_commentary", "m7_commentary",
                                 "m6_speech", "m1_repeat_or_speech"})
_OTHER_EVENTS_MAX = 2
_OTHER_EVENT_FRESH_DAYS = 2


def _checked_other_events(raw: Any, main_code: str, text_norm: str, body: str, outlet: str | None,
                          published: Any, dropped: list[str]) -> list[dict[str, Any]]:
    """Up to _OTHER_EVENTS_MAX other NEW events the article reports (review
    point 38: a closure, several attacks and a threat in one article lost
    two of them), each checked like the main event: a known code, a place
    the text supports, a real date, a confirmation the text gives."""
    out: list[dict[str, Any]] = []
    for i, item in enumerate(raw if isinstance(raw, list) else []):
        if not isinstance(item, dict):
            continue
        code = item.get("code")
        if code not in EVENT_CODES or code in _NOT_AN_OTHER_EVENT or code == main_code:
            continue
        place = item.get("place") if isinstance(item.get("place"), str) and item["place"].strip() else None
        if place and not _place_in_text(place, text_norm):
            dropped.append(f"other_events[{i}].place")
            place = None
        day = event_date_checked(item.get("date"), published)
        facts = item.get("facts") if isinstance(item.get("facts"), str) else ""
        facts = re.sub(r"\s+", " ", facts).strip()
        if facts and not _numbers_supported(facts, text_norm):
            dropped.append(f"other_events[{i}].facts")
            facts = ""
        event = {k: v for k, v in {
            "type": EVENT_CODES[code], "code": code, "place": place,
            "date": day,
            "country": country_name(item.get("country"))}.items() if v is not None}
        out.append({k: v for k, v in {
            "event": event,
            "confirmation": _checked_confirmation(item.get("confirmation"), body, outlet, dropped,
                                                  f"other_events[{i}].confirmation"),
            "facts": (facts if facts.endswith((".", "!", "?")) else facts + ".") if facts else None,
        }.items() if v is not None})
        if len(out) == _OTHER_EVENTS_MAX:
            break
    return out


def check_extraction(raw: str | None, title: str, body: str,
                     name_index: dict[str, dict[str, Any]],
                     counterparty_tickers: dict[str, str],
                     outlet: str | None = None,
                     published: Any = None) -> tuple[dict[str, Any] | None, list[str]]:
    """The model's answer, kept only where the text supports it.

    Returns (extracted, dropped): `extracted` is None when the answer is not
    usable at all (not JSON, or an event code we do not have); `dropped`
    names every item removed, for the run log.
    """
    try:
        data = json.loads(raw or "")
    except (TypeError, ValueError):
        return None, ["not_json"]
    if not isinstance(data, dict):
        return None, ["not_an_object"]
    # event: {"code", "place", "effective_date", "lasts_over_24_hours"};
    # deal: {"level", "amount", "up_to", "capacity"}. A bare code string is
    # accepted too, with the other fields read from the top level.
    raw_event = data.get("event")
    raw_event = raw_event if isinstance(raw_event, dict) else {"code": raw_event, **data}
    raw_deal = data.get("deal")
    raw_deal = raw_deal if isinstance(raw_deal, dict) else {}
    event = raw_event.get("code")
    if event in (None, "", "none"):
        event = None
    elif event not in EVENT_CODES:
        return None, [f"unknown_event:{event}"]

    text_norm = normalize_text(f"{title}\n{body}")
    dropped: list[str] = []

    def sentences(field: str) -> str | None:
        """The model's prose for `field`, or None when it states a number
        the text does not contain."""
        value = data.get(field) if isinstance(data.get(field), str) else ""
        value = re.sub(r"\s+", " ", value).strip()
        if not value or not _numbers_supported(value, text_norm):
            dropped.append(field)
            return None
        return value if value.endswith((".", "!", "?")) else value + "."

    headline = f'"{title.strip()}".'
    summary = sentences("summary") or f"The article is headlined {headline}"
    # reason_facts opens signal_reason; if it fails its check, the
    # summary's first sentence (already checked) stands in, then the headline.
    reason_facts = (sentences("reason_facts")
                    or (re.split(r"(?<=[.!?])\s", summary, maxsplit=1)[0]
                        if "summary" not in dropped else f"Headline: {headline}"))

    if event is None:
        return {"reason_facts": reason_facts, "summary": summary}, dropped

    companies: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in data.get("companies") or []:
        if not isinstance(item, dict) or not isinstance(item.get("name"), str):
            continue
        name = item["name"].strip()
        if not _in_text(name, text_norm):
            dropped.append(f"company:{name}")
            continue
        tracked = _match_tracked(name, name_index)
        canonical = tracked["company"] if tracked else name
        if canonical.casefold() in seen:
            continue
        seen.add(canonical.casefold())
        companies.append({
            "name": canonical,
            "ticker": tracked["ticker"] if tracked else counterparty_tickers.get(name.casefold()),
            "country": tracked["country"] if tracked else country_name(item.get("country")),
            "tracked": tracked is not None,
        })
        if len(companies) == _COMPANIES_MAX:
            break

    # The event's place may itself be a tracked site or company ("Kiryat
    # Gat" is Intel's fab in the company table): the event is about it even
    # when the model listed no company - so the right owner is named, and
    # officials confirming the incident count (review point 22).
    place_row = _match_tracked(raw_event.get("place") or "", name_index) \
        if isinstance(raw_event.get("place"), str) and _place_in_text(raw_event["place"], text_norm) else None
    if place_row and place_row["company"].casefold() not in seen and len(companies) < _COMPANIES_MAX:
        seen.add(place_row["company"].casefold())
        companies.append({"name": place_row["company"], "ticker": place_row["ticker"],
                          "country": place_row["country"], "tracked": True})

    def copied(value: Any, name: str) -> str | None:
        """`value` if the text contains it, else None (and logged)."""
        if value is None:
            return None
        if _in_text(value, text_norm):
            return value.strip()
        dropped.append(name)
        return None

    confirmation = _checked_confirmation(data.get("confirmation"), body, outlet, dropped)
    level_c = confirmation["level"]

    news_effect = data.get("news_effect", data.get("news"))
    if news_effect not in _NEWS:
        dropped.append("news_effect")
        news_effect = "mixed"               # no direction rather than a guessed one
    deal_level = raw_deal.get("level", data.get("deal_level"))
    if not (isinstance(deal_level, int) and not isinstance(deal_level, bool) and 1 <= deal_level <= 6):
        deal_level = None
    # Spec 9.1: "If the only source is unnamed, the highest level is 3."
    if deal_level and level_c == "unnamed" and deal_level > 3:
        deal_level = 3
    # When it happened - the date code judges "new" and "same event" on
    # (replaces the model's own yes/no "new_development", which marked an
    # IRGC "Hormuz remains closed" statement new).
    event_day = event_date_checked(raw_event.get("date"), published)
    if raw_event.get("date") not in (None, "", "null") and event_day is None:
        dropped.append("event.date")
    lasts = raw_event.get("lasts_over_24_hours", raw_event.get("over_24_hours"))
    if not isinstance(lasts, bool):
        lasts = None

    raw_amount = raw_deal.get("amount", data.get("amount"))
    up_to = raw_deal.get("up_to")
    if isinstance(raw_amount, dict):            # the older {"value", "up_to"} shape
        raw_amount, up_to = raw_amount.get("value"), raw_amount.get("up_to")
    amount = copied(raw_amount, "deal.amount")

    def present(d: dict[str, Any]) -> dict[str, Any]:
        """Absent means not stated: nothing is stored as null."""
        return {k: v for k, v in d.items() if v is not None}

    raw_place = raw_event.get("place")
    place = None
    if isinstance(raw_place, str) and raw_place.strip():
        if _place_in_text(raw_place, text_norm):
            place = raw_place.strip()
        else:
            dropped.append("event.place")
    event_obj = present({
        "type": EVENT_CODES[event], "code": event,
        "place": place,
        "date": event_day,
        # The country of the place - the one fact taken from the model's
        # general knowledge (the text says "Dhahran", not "Saudi Arabia");
        # it replaces hand-kept city lists for the region and alert checks.
        "country": country_name(raw_event.get("country")),
        "effective_date": copied(raw_event.get("effective_date"), "event.effective_date"),
        "lasts_over_24_hours": lasts,
    })
    # up_to means nothing without the amount it qualifies.
    deal = present({
        "level": deal_level, "amount": amount,
        "up_to": (up_to is True) if amount else None,
        "capacity": copied(raw_deal.get("capacity", data.get("capacity")), "deal.capacity"),
    })
    # For a deal, "official" means a PARTY to it says so: S&P reporting a
    # data-centre lease it is not part of was taken as the official
    # confirmation (full run 2026-10-23). A non-party is a named source.
    by_c = confirmation.get("by")
    if (event in _DEAL_CODES and confirmation["level"] == "official" and by_c and companies
            and not any(_same_source(by_c, c["name"]) for c in companies)):
        dropped.append("confirmation.not_a_party")
        confirmation = {**confirmation, "level": "named_source"}
    other_events = _checked_other_events(data.get("other_events"), event, text_norm, body,
                                         outlet, published, dropped)
    extracted = {
        "event": event_obj,
        "other_events": other_events or None,
        "companies": companies,
        "deal": deal or None,
        "confirmation": confirmation,
        "news_effect": news_effect,
        "reason_facts": reason_facts,
        "summary": summary,
    }
    return {k: v for k, v in extracted.items() if v is not None}, dropped


# ---------------------------------------------------------------------------
# Step 7b - the label, from spec section 7
# ---------------------------------------------------------------------------
# One branch per spec row. Each returns (label, rule, sentence): `rule` is
# stored so every row says which branch decided it; the sentence is the
# second half of signal_reason.

_EVENT_TYPE_LIST = ("US chip-sale decisions, Gulf data centres, Gulf money into US AI,"
                    " war and shipping, helium and gas, Gulf rules and supplier choices,"
                    " Israel's chip and AI sites, and enforcement against Gulf companies")


def event_code(ex: dict[str, Any]) -> str | None:
    return (ex.get("event") or {}).get("code")


def event_type(ex: dict[str, Any]) -> str | None:
    return (ex.get("event") or {}).get("type")


def _event(ex: dict[str, Any], key: str) -> Any:
    return (ex.get("event") or {}).get(key)


def _deal(ex: dict[str, Any], key: str) -> Any:
    return (ex.get("deal") or {}).get(key)


def _amount(ex: dict[str, Any]) -> str | None:
    return _deal(ex, "amount")


def _up_to(ex: dict[str, Any]) -> bool:
    return bool(_deal(ex, "up_to"))


def _gulf_company(ex: dict[str, Any]) -> dict[str, Any] | None:
    return next((c for c in ex.get("companies", [])
                 if c["country"] in set(_GULF_COUNTRIES.values())), None)


def _committed_amount(ex: dict[str, Any]) -> bool:
    return bool(_amount(ex)) and not _up_to(ex)


def _party_confirms(ex: dict[str, Any]) -> bool:
    """The confirmer is one of the companies the event is about."""
    by = (ex.get("confirmation") or {}).get("by") or ""
    return bool(by) and any(_same_source(by, c.get("name") or "") for c in ex.get("companies") or [])


_US_NAMES = frozenset({"united states", "us", "u.s.", "usa", "united states of america", "america"})
# Public bodies: they relay or regulate a company's decision, never make it.
_AUTHORITY_RE = re.compile(
    r"\b(?:authority|ministry|government|agency|council|command|coalition|navy|forces|embassy|"
    r"police|department|administration|commission|regulator)\b", re.IGNORECASE)


# The region by geography - its countries and the sea routes around them -
# not a list of places seen in articles. The event's country comes from the
# extraction (`event.country`), so a town no list names (Dhahran, Khamis
# Mushait) is judged by its country.
_MIDDLE_EAST_COUNTRIES = {"uae", "saudi arabia", "qatar", "kuwait", "bahrain", "oman",
                          "israel", "iran", "iraq", "yemen", "jordan", "lebanon", "syria",
                          "egypt", "palestine", "palestinian territories"}
_MIDDLE_EAST_SEAS_RE = re.compile(
    r"\b(?:hormuz|red sea|mandeb|mandab|gulf of oman|gulf of aden|persian gulf|"
    r"arabian gulf|arabian sea|suez)\b"
    # A bare "Gulf" ("the Gulf", "Saudi Arabia's Gulf coast") is the Persian
    # Gulf in this feed; "Gulf of Mexico" is not.
    r"|\bgulf\b(?!\s+of\b)", re.IGNORECASE)


def _place_country(ex: dict[str, Any]) -> str | None:
    """The country the event happened in, in the table's spelling for Gulf
    countries; None at sea or when not stated."""
    return country_name(_event(ex, "country"))


_SEA_WORD_RE = re.compile(r"\b(?:sea|ocean|strait|gulf|channel|bay)\b", re.IGNORECASE)


def _in_middle_east(ex: dict[str, Any]) -> bool:
    """False only on evidence of another region: a country outside it, or a
    named sea that is not one of its sea routes (the Black Sea). A place
    with no country given, or no place at all, is not that evidence."""
    country = _place_country(ex)
    if country:
        folded = country.casefold()
        return folded in _MIDDLE_EAST_COUNTRIES or any(
            _whole_word(c, folded) for c in _MIDDLE_EAST_COUNTRIES)
    place = _event(ex, "place") or ""
    folded = normalize_text(place)
    if any(_whole_word(c, folded) for c in _MIDDLE_EAST_COUNTRIES):
        return True                     # "off Saudi Arabia's Gulf coast"
    return bool(_MIDDLE_EAST_SEAS_RE.search(place)) or not _SEA_WORD_RE.search(place)


# What makes a Gulf rule an M6 rule: AI, data or chips. Added after the
# Saudi Red Sea Authority's recreational boat-chartering rules were labelled
# a binding Gulf AI rule.
_AI_DATA_RE = re.compile(
    r"\bAI\b|artificial intelligence|personal data|data (?:protection|centres?|centers?|"
    r"locali[sz]ation|residency|privacy|governance|sovereignty)|\bcloud\b|algorithm|"
    r"semiconductor|\bchips?\b", re.IGNORECASE)


_HARDWARE_RE = re.compile(
    r"data[ -]?cent(?:re|er)s?|\bgpus?\b|\bchips?\b|semiconductor|\bservers?\b|megawatt|"
    r"gigawatt|\d\s?(?:mw|gw)\b|compute capacity|computing capacity|\bcampus\b|"
    r"accelerators?|supercomputer|ai factor(?:y|ies)", re.IGNORECASE)


def _is_hardware_deal(ex: dict[str, Any], title: str, body: str | None = None) -> bool:
    """A data-centre deal's OWN facts - its capacity, headline, facts
    sentence and summary - talk about AI hardware (chips, servers, data
    centres, MW of compute). An amount alone proves nothing: a $100 million
    port-terminal expansion passed as a data-centre order; and a capacity is
    not compute unless it says so - QatarEnergy's "4 million tonnes per
    annum" LNG purchase passed, its body mentioning data-centre gas demand
    (full run 2026-10-23)."""
    text = _event_text(ex, title)
    if len(re.findall(r"[A-Za-z]{3,}", text)) < 5:
        text = body or ""                             # no facts of its own: read the article
    if len(re.findall(r"[A-Za-z]{3,}", text)) < 5:
        return True                                   # nothing to read: no evidence either way
    return bool(_HARDWARE_RE.search(f"{_deal(ex, 'capacity') or ''} {text}"))


# A headline that reacts to an event ("UN Security Council Members Strongly
# Condemn Houthi Attacks...") is never the first report of it: the model
# coded such a statement as a confirmed strike on the 403-article run.
_REACTION_HEADLINE_RE = re.compile(
    r"\b(?:condemn\w*|denounc\w*|welcom\w*|condolence\w*|solidarity|deplor\w*|sermon|"
    r"urges? restraint|expresses? concern)\b", re.IGNORECASE)


# An event is new when it happened within this many days of the article.
_NEW_WITHIN_DAYS = 7


def is_new(ex: dict[str, Any], published: Any) -> bool:
    """From the event's own date (event.date), not a yes/no from the
    model. No date, or no published time, is judged new - a repeat must be
    shown, not assumed."""
    day, pub = _as_date(_event(ex, "date")), _as_date(published)
    return day is None or pub is None or (pub - day).days <= _NEW_WITHIN_DAYS


def decide_label(ex: dict[str, Any], title: str = "", body: str | None = None,
                 waterway_status: dict[str, dict[str, Any]] | None = None,
                 published: Any = None) -> tuple[str, str, str]:
    """(label, rule, why) - see _decide_label. A reaction headline is capped
    at weak_signal: the event itself is alerted from its own report."""
    label, rule, why = _decide_label(ex, title, body, waterway_status, published)
    # An attack alert needs the day it happened: a report of an attack says
    # when; an attack an analysis piece recalls without a day is background
    # (a sinking from days before became three alerts, 2026-10-23). Same
    # for a force majeure extension: dated only "2026-10", Edison's 28 Sep
    # extension was re-alerted from a 13 Oct roundup (full run 2026-10-23).
    if (label == "signal" and event_code(ex) in _ATTACK_CODES | {"m4_attack_tally", "m5_qatar_force_majeure"}
            and not _exact_day(_event(ex, "date"))):
        return ("weak_signal", f"{rule}_undated",
                "The article does not say on which day this happened, so it is not reported as"
                " new.")
    if label == "signal" and _REACTION_HEADLINE_RE.search(title or ""):
        return ("weak_signal", f"{rule}_reaction_headline",
                "The article is a statement reacting to the event, not the first report of it.")
    return label, rule, why


# Who controls each waterway: Iran (Hormuz), the Houthis (Red Sea, Bab el-Mandeb).
_CONTROLLING_PARTY_RE = re.compile(
    r"\b(?:iran\w*|irgc|islamic revolutionary guard\w*|revolutionary guard\w*|tehran|"
    r"houthi\w*|ansar allah|ansarallah)\b", re.IGNORECASE)
# A named ship tracker with a count of ships or transits.
_SHIP_COUNT_RE = re.compile(
    r"\b(?:portwatch|kpler|vortexa|marinetraffic|windward|lloyd's list|ukmto)\b"
    r"[^.]{0,200}?\b\d[\d,]*\s+(?:ships|vessels|transits|tankers|crossings)\b"
    r"|\b\d[\d,]*\s+(?:ships|vessels|transits|tankers|crossings)\b[^.]{0,200}?"
    r"\b(?:portwatch|kpler|vortexa|marinetraffic|windward|lloyd's list|ukmto)\b",
    re.IGNORECASE)


# The waterways a closure or reopening is judged on, by a word of the place.
_WATERWAYS = {"hormuz": "the Strait of Hormuz", "red sea": "the Red Sea",
              "mandeb": "the Red Sea", "mandab": "the Red Sea"}
# How far back a statement by the other side still makes a status contested.
_CONTESTED_WINDOW_DAYS = 14


def current_waterway_status(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """{waterway: ...} from stored rows only - nothing set by hand:
      closed / as_of   the latest closure or reopening ALERTED (a signal),
                       i.e. a change already reported;
      said             {True: {"by", "date"}, False: {...}} - the latest
                       statement that it is closed (True) or open (False),
                       signal or weak, so a new statement can say who
                       contradicts it.
    Rows as returned by get_middle_east_story_candidates."""
    status: dict[str, dict[str, Any]] = {}
    for r in sorted((r for r in rows if r.get("published")), key=lambda r: r["published"]):
        md = r.get("metadata") or {}
        code = event_code(md)
        if "collapsed_into" in md or code not in ("m4_waterway_closed", "m4_waterway_reopened"):
            continue
        key = waterway_key(_event(md, "place"))
        if not key:
            continue
        closed = code == "m4_waterway_closed"
        entry = status.setdefault(key, {"said": {}})
        entry["said"][closed] = {"by": (md.get("confirmation") or {}).get("by"),
                                 "date": _as_date(_event(md, "date")) or _as_date(r["published"])}
        if r.get("signal_detection") == "signal":
            entry.update(closed=closed, as_of=f"since {r['published']:%d %B %Y}")
    return status


def waterway_key(place: str | None) -> str | None:
    """"hormuz" / "red sea" / "mandeb" for a place naming one, else None."""
    folded = (place or "").casefold()
    return next((k for k in _WATERWAYS if k in folded), None)


def _waterway_label(ex: dict[str, Any], body: str | None,
                    status: dict[str, dict[str, Any]]) -> tuple[str, str, str]:
    """A closure or reopening is news only when it CHANGES the waterway's
    status. Two things show it is not a change, neither set by hand:
      - its own date: a closure the text dates weeks back ("closed since
        March") is old - judged before this, by the repeat rule;
      - the stored history (`status`, current_waterway_status): the same
        status was already alerted."""
    event = event_code(ex)
    closing = event == "m4_waterway_closed"
    key = waterway_key(_event(ex, "place"))
    known = status.get(key) if key else None
    name = _WATERWAYS.get(key or "", "the waterway")
    if known and known.get("closed") == closing:
        return ("noise", "m4_waterway_status_unchanged",
                f"{name[0].upper()}{name[1:]} was already {'closed' if closing else 'open'}"
                f" ({known.get('as_of', 'earlier')}); this restates it.")
    confirmed = (ex.get("confirmation") or {}).get("level")
    by = (ex.get("confirmation") or {}).get("by") or ""
    # Departs from spec 7.5 (user decision 2026-10-22): the spec waits for
    # ship counts within 72h and there is no ship-count feed, so a named
    # tracker's counts in the article, or the side that CONTROLS the
    # waterway declaring it, confirm the change.
    if confirmed in ("official", "named_source") and body is not None and _SHIP_COUNT_RE.search(body):
        return ("signal", f"{event}_confirmed_by_ship_counts",
                "Ship-tracking figures in the article show the change in traffic.")
    if confirmed == "official" and _CONTROLLING_PARTY_RE.search(by):
        return ("signal", f"{event}_declared_by_controlling_party",
                "Declared by the side that controls the waterway; ship traffic has not yet"
                " confirmed it.")
    # Contested: the other side's latest stored statement, if recent - the
    # US saying Hormuz is open while Iran says it is closed (review point 2).
    other = ((known or {}).get("said") or {}).get(not closing)
    day = _as_date(_event(ex, "date"))
    if other and other.get("by") and (day is None or other.get("date") is None
                                      or abs((day - other["date"]).days) <= _CONTESTED_WINDOW_DAYS):
        when = f" on {other['date']:%d %B}" if other.get("date") else ""
        return ("weak_signal", f"{event}_contested",
                f"Contested: {other['by']} said{when} that {name} is"
                f" {'open' if closing else 'closed'}.")
    return ("weak_signal", f"{event}_unconfirmed",
            "The statement does not come from the side that controls the waterway, and no"
            " ship-tracking figures confirm it.")


def _decide_label(ex: dict[str, Any], title: str = "", body: str | None = None,
                  waterway_status: dict[str, dict[str, Any]] | None = None,
                  published: Any = None) -> tuple[str, str, str]:
    """(label, rule, why) from the checked facts.

    `why` is the second half of signal_reason, shown to readers in the UI,
    so it is plain language: no spec section numbers, no deal levels, no
    rule names. The first half - what happened, with names and figures -
    is the model's `reason_facts`. `rule` is the internal name of the
    branch, stored in metadata for us, never shown.

    `title` is the article's headline - one rule needs to know the event
    is the article's main subject, not background."""
    guarded = guarded_code(ex, title, body)
    if guarded != event_code(ex):
        if guarded is None:
            return ("noise", "guard_no_attack_in_text",
                    "The text reports no attack, so it is not a strike on a site.")
        ex = {**ex, "event": {**(ex.get("event") or {}), "code": guarded,
                              "type": EVENT_CODES[guarded]}}
    event = event_code(ex)
    if event is None:
        # The model's reason_facts already says, for THIS article, why it
        # changes nothing - a fixed sentence here contradicted articles that
        # are about Gulf data centres or helium but report no event.
        return ("noise", "not_an_event_type", "")
    confirmed_by = (ex.get("confirmation") or {}).get("level", "unnamed")
    official = confirmed_by == "official"
    named = confirmed_by in ("official", "named_source")
    level = _deal(ex, "level") or 0

    if not is_new(ex, published):
        if event_type(ex) == "M8":   # spec 7.9: an action naming a Gulf company is never ignored
            return ("weak_signal", "m8_repeat",
                    "This repeats an earlier US action against a Gulf company; such actions are always kept.")
        return ("noise", "repeat", "This repeats earlier news; nothing new has happened.")

    unconfirmed = "Only unnamed sources report it so far."

    # --- M1 US decisions on Gulf chip sales (spec 7.1)
    country = _place_country(ex)
    if event_type(ex) == "M1" and country and country.casefold() not in _US_NAMES:
        # China's draft export controls were stored as a US draft rule (full
        # run 2026-10-23): another government's decision is not M1.
        return ("noise", "m1_not_a_us_decision",
                "It is another government's decision, not a US decision on chip sales to the Gulf.")
    if event == "m1_rule_or_licence":
        # An approval is final when it is granted: review 2026-10-23 (point
        # 42) - approvals rarely state a start date, and waiting for one held
        # every licence at weak_signal.
        if official:
            date = _event(ex, "effective_date")
            return ("signal", "m1_final_decision",
                    f"It is a final US decision{f' and takes effect {date}' if date else ''}.")
        if not named:
            return ("weak_signal", "m1_unnamed_officials", unconfirmed)
        return ("weak_signal", "m1_not_official",
                "The decision is reported, but not by the US government itself.")
    if event == "m1_congress_action":
        if named and (_gulf_company(ex) or _amount(ex)):
            return ("signal", "m1_congress_named_deal",
                    "It is formal action in Congress aimed at a specific Gulf deal or company.")
        return ("weak_signal", "m1_congress_no_named_deal",
                "It names no specific Gulf deal or company, so no shipment is held up yet.")
    if event == "m1_export_category_change":
        if official:
            return ("signal", "m1_export_category_change",
                    "Changing a country's US export category changes how easily US chips can ship there.")
        return ("weak_signal", "m1_export_category_unconfirmed", unconfirmed)
    if event == "m1_draft_or_hearing":
        return ("weak_signal", "m1_draft_or_hearing",
                "It is a draft, a hearing or an unconfirmed report, not a final US decision.")
    if event == "m1_repeat_or_speech":
        return ("noise", "m1_repeat_or_speech",
                "It is a speech or a rule already in force; no US decision has changed.")

    # --- M2 Gulf data centres (spec 7.3, deal levels 7.2)
    if event == "m2_switched_on":
        if not _is_hardware_deal(ex, title, body):
            # A bank starting to use OpenAI's models was coded as capacity
            # switched on (review 2026-10-23, point 20).
            return ("noise", "m2_not_hardware",
                    "It is the use of an AI service; no chips, servers or data-centre capacity changed.")
        if _deal(ex, "capacity") and named:
            return ("signal", "m2_switched_on_with_capacity",
                    "The capacity is running now, which shows Gulf AI orders turning into real hardware sales.")
        return ("weak_signal", "m2_no_capacity_figure",
                "No figure is given for how much capacity is running.")
    if event == "m2_chips_delivered":
        if named and _gulf_company(ex):
            return ("signal", "m2_chips_delivered",
                    "The chips have arrived, which shows a Gulf order turning into real sales.")
        return ("weak_signal", "m2_delivery_unconfirmed",
                "The delivery is not confirmed for a named Gulf buyer.")
    if event == "m2_setback":
        if named:
            return ("signal", "m2_setback",
                    "A delay, cut or cancellation pushes back hardware sales that were expected.")
        return ("weak_signal", "m2_setback_unconfirmed", unconfirmed)
    if event == "m2_deal_or_construction":
        if not _is_hardware_deal(ex, title, body):
            # The model kept calling software / agentic-AI partnerships
            # (OrbitronAI-Aramco Digital) data-centre deals despite the prompt.
            return ("noise", "m2_not_hardware",
                    "It is a software or services partnership; it buys no chips, servers or"
                    " data-centre capacity.")
        if level == 1:
            return ("noise", "m2_pledge",
                    "It is a headline spending figure with no named contract behind it.")
        # A signed order with its amount is money committed to Gulf AI
        # hardware (review 2026-10-23, point 6: MIS's $500 million, 50 MW
        # order for Humain was held at weak_signal).
        if level == 2 and not _amount(ex) and not _deal(ex, "capacity"):
            # Departs from spec 7.2 (review 2026-10-23, point 21): a
            # memorandum or partnership with no amount, no capacity and
            # nothing binding - a company opening an office under a
            # partnership "to explore" opportunities - commits nothing.
            return ("noise", "m2_memorandum_without_figures",
                    "It is a memorandum or partnership with no amount or capacity; nothing is"
                    " committed yet.")
        if not _gulf_company(ex):
            return ("noise", "m2_no_gulf_company",
                    "No Gulf company is a party to the deal, so it is not a Gulf data-centre deal.")
        if level >= 4 and _committed_amount(ex) and named:
            if not (_party_confirms(ex) or official):
                # MEEZA's lease was cited inside an S&P sector report (full
                # run 2026-10-23): a third party's mention, not the deal news.
                return ("weak_signal", "m2_signed_reported_by_third_party",
                        "A third party mentions the order; neither party nor a government announced it.")
            return ("signal", "m2_signed_with_amount",
                    "The order is signed and its value is stated: committed spending on Gulf AI capacity.")
        stage = ("The order is signed, but no amount is stated." if level >= 4 else
                 "It is a partnership or plan, not a signed order." if level else
                 "It is a plan or construction update; nothing is running yet.")
        return ("weak_signal", "m2_deal_below_signed_amount", stage)
    if event == "m2_company_update":
        # Nvidia saying its Middle East plans "have not changed" speaks to its
        # Gulf sales without changing anything (review 2026-10-23, point 7).
        if named:
            return ("weak_signal", "m2_company_update",
                    "The company states where its Gulf plans stand; nothing new is ordered or cancelled.")
        return ("noise", "m2_company_update_unnamed", "No named source stands behind it.")
    if event == "m2_visit_or_ceremony":
        return ("noise", "m2_visit_or_ceremony",
                "It is a visit, a ceremony or a repeated target; no capacity has changed.")

    # --- M3 Gulf money into US AI (spec 7.4)
    if event == "m3_investment":
        if level == 1:
            return ("noise", "m3_pledge", "It is a headline pledge with no named deal behind it.")
        investees = [c for c in ex.get("companies") or [] if c.get("country") not in set(_GULF_COUNTRIES.values())]
        if len(investees) > 1:
            # Several different companies invested in = a roundup or review
            # of earlier investments (an MGX board meeting listing Anthropic,
            # Databricks and SpaceX, full run 2026-10-23), not one new one.
            return ("weak_signal", "m3_several_investments_listed",
                    "The article lists several investments rather than reporting one new one.")
        if (level >= 4 and _committed_amount(ex) and named and len(ex.get("companies") or []) >= 2
                and (_party_confirms(ex) or official)):
            return ("signal", "m3_completed_with_amount",
                    "It is a completed investment with the amount stated: more money for AI spending.")
        why = ("the amount is only an \"up to\" figure" if _up_to(ex)
               else "no amount is stated" if not _amount(ex)
               else "it is not signed or completed yet" if level < 4
               else "only unnamed sources report it")
        return ("weak_signal", "m3_investment_not_alertable",
                f"It is Gulf money for US AI, but {why}.")
    if event == "m3_pullback":
        if named:
            return ("signal", "m3_pullback",
                    "A Gulf fund pulling money out means less fuel for AI spending.")
        return ("weak_signal", "m3_pullback_unconfirmed", unconfirmed)
    if event == "m3_talks":
        if not _amount(ex):
            # Talks with no amount - a fund and a US AI company agreeing to
            # "explore investment opportunities" - commit nothing (review
            # point 21; same reasoning as a memorandum without figures).
            return ("noise", "m3_talks_without_amount",
                    "These are talks with no amount; nothing is committed.")
        return ("weak_signal", "m3_talks", "These are talks; no money has been committed yet.")

    # --- M4 War and shipping (spec 7.5)
    if event_type(ex) == "M4" and not _in_middle_east(ex):
        # A Black Sea drone attack on a tanker came in through the shipping
        # search and was stored as a ship incident (review 2026-10-23, point 16).
        return ("noise", "m4_outside_middle_east",
                "It happened outside the Middle East and its sea routes.")
    if event == "m4_strike_on_site":
        country = _place_country(ex)
        if official and country in _STRIKE_ALERT_COUNTRIES:
            return ("signal", "m4_strike_confirmed",
                    f"A government, the attacker or the site's operator confirms the strike on"
                    f" energy, industrial, port, data-centre or cable infrastructure in {country}.")
        if official:
            return ("weak_signal", "m4_strike_outside_alert_countries",
                    "The strike was not in the UAE, Saudi Arabia, Qatar or Israel, where the"
                    " infrastructure US AI companies depend on sits.")
        return ("weak_signal", "m4_strike_claimed",
                "Only the other side or unnamed sources say it happened; no government, attacker"
                " or site operator has confirmed it.")
    if event == "m4_interception":
        return ("noise", "m4_interception", "The missiles or drones were intercepted; nothing was hit.")
    if event == "m4_other_strike":
        # A Houthi projectile that damaged homes in Najran was kept as
        # relevant (review 2026-10-23, point 18): it reaches no market.
        return ("noise", "m4_no_economic_target",
                "Nothing economic was hit: no energy or industrial site, port, data centre or cable.")
    if event in ("m4_waterway_closed", "m4_waterway_reopened"):
        return _waterway_label(ex, body, waterway_status or {})
    if event == "m4_ship_traffic":
        if named:
            return ("weak_signal", "m4_ship_traffic",
                    "Ship-traffic figures show the state of the route; they are a measure, not a"
                    " new decision.")
        return ("noise", "m4_ship_traffic_unnamed", "No named tracker stands behind the figures.")
    if event == "m4_shipping_or_insurance":
        # The line or insurer itself, among the companies the event is
        # about, must announce it: freight-rate reports, a ministry's note on
        # one tanker and a government travel advisory were all signals here
        # (full run 2026-10-23).
        by = (ex.get("confirmation") or {}).get("by") or ""
        # ...and the confirmer must be that company, not a canal or port
        # authority listed among the parties (the Suez Canal Authority
        # relaying that Hapag-Lloyd "expects" to return, full run 2026-10-23).
        if official and _party_confirms(ex) and not _AUTHORITY_RE.search(by):
            return ("signal", "m4_shipping_line_or_insurer",
                    "Shipping lines and insurers changing course moves freight costs and delivery times"
                    " for oil, gas and chip supplies.")
        return ("weak_signal", "m4_shipping_not_announced_by_line",
                "No shipping line or insurer has announced the change itself.")
    # Ships at sea (review 2026-10-23, points 11-12): a sinking, crew killed
    # or an authority's count of attacks raises oil, freight and insurance
    # costs the way a hit on a port does; a single damaged ship is stored.
    if event == "m4_ship_sunk_or_crew_killed":
        if named:
            return ("signal", "m4_ship_sunk_or_crew_killed",
                    "A ship was sunk or crew were killed, which raises oil, freight and insurance"
                    " costs on the route.")
        return ("weak_signal", "m4_ship_sunk_unconfirmed", unconfirmed)
    if event == "m4_attack_tally":
        if named and not _event(ex, "date"):
            # A count with no period ("vessels attacked in recent weeks",
            # quoted at a forum) measures nothing new.
            return ("weak_signal", "m4_attack_tally_undated",
                    "The count of attacks gives no period, so it cannot show a new escalation.")
        if named:
            return ("signal", "m4_attack_tally",
                    "Several ships were attacked, which moves oil, freight and insurance costs on"
                    " the route.")
        return ("weak_signal", "m4_attack_tally_unnamed", "No named authority stands behind the count.")
    if event == "m4_ship_incident":
        if named:
            return ("weak_signal", "m4_ship_incident",
                    "A ship was hit, but none sank, no crew were killed and no shipping line has"
                    " stopped the route.")
        return ("noise", "m4_ship_incident_unnamed", "No named source confirms it.")
    if event == "m4_strike_plan":
        # A report of planned US strikes moved oil on 22 October (review
        # 2026-10-23, point 14).
        if official:
            return ("signal", "m4_strike_plan_announced",
                    "A government announces military action that has not happened yet; oil and"
                    " shipping prices move on it.")
        return ("weak_signal", "m4_strike_plan_reported",
                "A plan for new strikes is reported, but not announced by a government.")
    if event == "m4_threat_or_statement":
        if named:
            return ("weak_signal", "m4_threat_or_statement", "It is a threat or statement; nothing has happened yet.")
        return ("noise", "m4_threat_or_statement_unnamed", "No named source stands behind it.")
    if event == "m4_commentary":
        return ("noise", "m4_commentary", "It is commentary on the war, not a new event.")

    # --- M5 Helium and gas supply (spec 7.6)
    # A force majeure mentioned as background (a story about another
    # country's LNG) carries the date it was declared, weeks back, so the
    # repeat rule above already drops it - no headline wording test.
    if event == "m5_qatar_force_majeure" and not _event(ex, "date"):
        # A force majeure the text gives no time for is background (an EU
        # gas-market piece mentioning it, full run 2026-10-23); a month
        # alone is not enough for an alert either (see decide_label).
        return ("weak_signal", "m5_force_majeure_undated",
                "The article mentions Qatar's force majeure without saying when it was declared"
                " or extended.")
    if event == "m5_qatar_force_majeure":
        by = (ex.get("confirmation") or {}).get("by") or ""
        if official and re.search(r"\bqatar", by, re.IGNORECASE):
            return ("signal", event,
                    "QatarEnergy announced it itself; Ras Laffan also produces the helium chip factories use.")
        return ("weak_signal", f"{event}_unconfirmed",
                "QatarEnergy has not announced it itself.")
    if event == "m5_supplier_rationing":
        # Spec 7.6: the supplier itself announces it. "Suppliers have begun
        # rationing", per an unnamed consultant, was a signal (2026-10-23).
        if official:
            return ("signal", event,
                    "Helium rationing or surcharges raise costs for chip factories.")
        return ("weak_signal", f"{event}_unconfirmed", unconfirmed)
    if event == "m5_asia_shortfall":
        if official:
            return ("signal", "m5_asia_shortfall",
                    "Chip factories cannot run without helium.")
        return ("weak_signal", "m5_asia_shortfall_unconfirmed",
                "The shortfall has not been announced by the company or government itself.")
    if event == "m5_commentary":
        return ("weak_signal", "m5_commentary",
                "It is commentary on helium or gas supply, not a change in supply.")

    # --- M6 Gulf rules and supplier choices (spec 7.7)
    if event == "m6_binding_rule":
        if body is not None and not _AI_DATA_RE.search(f"{title}\n{body}"):
            return ("noise", "m6_not_ai_or_data",
                    "It is a Gulf rule, but not about AI, data or chips.")
        if official and _event(ex, "effective_date"):
            return ("signal", "m6_binding_rule_with_date",
                    f"It is a binding rule that takes effect {_event(ex, 'effective_date')}.")
        return ("weak_signal", "m6_rule_no_date", "No start date is given for the rule.")
    if event == "m6_chinese_supplier":
        if level >= 4 and named:
            return ("signal", "m6_chinese_supplier_signed",
                    "A signed deal with a Chinese supplier is a sale US chip and cloud companies did not win.")
        return ("weak_signal", "m6_chinese_supplier_not_signed",
                "The deal with the Chinese supplier is not signed yet.")
    if event == "m6_strategy":
        return ("weak_signal", "m6_strategy", "It is a strategy or plan; no binding rule has been set.")
    if event == "m6_speech":
        return ("noise", "m6_speech", "It is a speech; nothing has changed.")

    # --- M7 Israel (spec 7.8)
    if event == "m7_site_disruption":
        # A tracked Israeli company, or a listed one with Israeli sites
        # (Intel's Kiryat Gat fab, Nvidia's Israeli design centres).
        if official and any(c["tracked"] or c["ticker"] for c in ex.get("companies", [])):
            return ("signal", "m7_site_disruption",
                    "The company itself reports the disruption at a site it depends on.")
        return ("weak_signal", "m7_disruption_unconfirmed",
                "The disruption is not confirmed by a listed chip or AI company.")
    if event == "m7_site_incident":
        # Spec 7.8: an alert needs the company to announce a stop. The
        # 2026-10-22 departure (officials confirming the Intel Kiryat Gat
        # blast -> signal) was reversed by the review of 2026-10-23 (point
        # 3): no production stop was reported, so it is one weak signal,
        # and the later hazardous-materials leak joins it as one story.
        if official and any(c["tracked"] or c["ticker"] for c in ex.get("companies", [])):
            return ("weak_signal", "m7_site_incident_confirmed",
                    "Officials confirm the incident at a listed chip company's site in Israel;"
                    " the company has not announced a production stop.")
        return ("weak_signal", "m7_site_incident", "No official body has confirmed it yet.")
    if event == "m7_home_front_restriction":
        place = (_event(ex, "place") or "").casefold()
        if _event(ex, "lasts_over_24_hours") and any(p in place for p in _HOME_FRONT_ALERT_PLACES):
            return ("signal", "m7_home_front_over_24h",
                    "The restrictions last more than a day in an area with major chip and AI sites.")
        return ("weak_signal", "m7_home_front_short_or_elsewhere",
                "The restrictions are short or outside Haifa, Tel Aviv-Herzliya and Kiryat Gat,"
                " where the main chip and AI sites are.")
    if event == "m7_acquisition":
        if level >= 4 and _committed_amount(ex) and named:
            return ("signal", "m7_acquisition_with_price",
                    "The deal is agreed and the price is stated.")
        return ("weak_signal", "m7_acquisition_talks",
                "The deal is not agreed yet, or no price is stated.")
    if event == "m7_company_deal":
        # Tower Semiconductor's capacity deal with Semtech had no code and
        # was stored as noise (review 2026-10-23, point 9). Same bar as a
        # Gulf order: signed with its value -> signal.
        if level >= 4 and _committed_amount(ex) and named:
            return ("signal", "m7_company_deal_signed_with_amount",
                    "The deal is signed and its value is stated.")
        return ("weak_signal", "m7_company_deal",
                "It is business news from an Israeli chip or AI company; no signed amount is stated.")
    if event == "m7_commentary":
        return ("noise", "m7_commentary", "It is commentary on the war, not a new event.")

    # --- M8 Enforcement against a named Gulf company (spec 7.9)
    if event == "m8_enforcement" and body is not None and not _AI_DATA_RE.search(f"{title}\n{body}"):
        # US sanctions on oil-tanker agents were read as chip enforcement
        # (full run 2026-10-23). M8 is the gate on US chips reaching the Gulf.
        return ("noise", "m8_not_chips_or_technology",
                "The action is not about chips or technology, so it does not affect US chip sales.")
    if event == "m8_enforcement":
        gulf = _gulf_company(ex)
        if gulf:
            return ("signal", "m8_enforcement",
                    f"US action against {gulf['name']} decides whether US chips can ship to it.")
        # The model once called a Russian airline's denial order M8.
        return ("noise", "m8_no_gulf_company", "It involves no Gulf company.")
    return ("weak_signal", "m8_investigation_unnamed",
            "No Gulf company is named in the investigation yet.")


# ---------------------------------------------------------------------------
# Step 7c - read-through: which US companies, which way
# ---------------------------------------------------------------------------
# Two sources, company-specific first:
#  1. each tracked company the article is about -> its counterparties from
#     the company table (share_price_on_good_news, reversed on bad news);
#  2. the spec's events table row for this event type and side.
# A ticker already given by (1) is not repeated by (2). Mixed news gets no
# direction at all - a guessed direction is worse than none.
_FLIP = {"rise": "fall", "fall": "rise"}

# Which spec "At a glance" row(s) each event code moves, per side of the
# news. A code not listed (a construction contract, a ship incident with no
# route change, a threat, a software partnership) moves no row: the spec
# ties those stock moves to specific events, and a copied template
# ("more AI hardware is actually sold" on a construction contract) told
# readers something the article did not.
_ROWS_BY_CODE: dict[tuple[str, str], list[str]] = {
    ("m1_rule_or_licence", "good"): ["The US approves chip sales to a Gulf country"],
    ("m1_export_category_change", "good"): ["The US approves chip sales to a Gulf country"],
    ("m1_rule_or_licence", "bad"): ["Congress blocks a deal, or a smuggling case names a Gulf firm"],
    ("m1_congress_action", "bad"): ["Congress blocks a deal, or a smuggling case names a Gulf firm"],
    ("m8_enforcement", "bad"): ["Congress blocks a deal, or a smuggling case names a Gulf firm"],
    ("m2_switched_on", "good"): ["A Gulf data centre switches on, or receives its chips"],
    ("m2_chips_delivered", "good"): ["A Gulf data centre switches on, or receives its chips"],
    ("m2_setback", "bad"): ["A Gulf data centre is delayed, damaged or cancelled"],
    ("m3_investment", "good"): ["A Gulf sovereign fund invests in a US AI company"],
    ("m3_pullback", "bad"): ["A Gulf fund pulls back or misses a commitment"],
    ("m4_strike_on_site", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m4_waterway_closed", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    # Ships attacked are the spec row's own words (review 2026-10-23, point
    # 24: tanker attacks named no US stock).
    ("m4_ship_incident", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m4_ship_sunk_or_crew_killed", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m4_attack_tally", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m4_ship_traffic", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m7_site_incident", "bad"): ["Missile strikes or shutdowns in Israel"],
    ("m4_shipping_or_insurance", "bad"): ["Hormuz or the Red Sea is closed, or ships are attacked"],
    ("m4_waterway_reopened", "good"): ["A ceasefire holds and ships move through Hormuz again"],
    ("m4_ship_traffic", "good"): ["A ceasefire holds and ships move through Hormuz again"],
    ("m4_shipping_or_insurance", "good"): ["A ceasefire holds and ships move through Hormuz again"],
    ("m5_qatar_force_majeure", "bad"): ["Qatar's helium plant stays offline",
                                        "Qatar's helium plant stays offline (other side)"],
    ("m5_supplier_rationing", "bad"): ["Qatar's helium plant stays offline",
                                       "Qatar's helium plant stays offline (other side)"],
    ("m5_asia_shortfall", "bad"): ["Qatar's helium plant stays offline"],
    ("m6_chinese_supplier", "bad"): ["A Gulf buyer picks a Chinese chip or cloud supplier"],
    ("m7_site_disruption", "bad"): ["Missile strikes or shutdowns in Israel"],
    ("m7_home_front_restriction", "bad"): ["Missile strikes or shutdowns in Israel"],
}
# The good side of the helium rows (the plant restarts), which the spec's
# table does not list but its shipping card (8.1) words: directions
# reversed, and the "why" rewritten to match - a reversed direction under
# the original wording read as nonsense.
_HELIUM_RESTART_WHY = {
    "Qatar's helium plant stays offline": "Helium supply to chip factories is recovering",
    "Qatar's helium plant stays offline (other side)": "Helium shortage surcharges are likely to end",
}
_SPEC_REF_RE = re.compile(r"\s*\((?:spec|section)[^)]*\)", re.IGNORECASE)


def _event_rows(code: str, news: str) -> list[dict[str, Any]]:
    by_name = {r["event"]: r for r in MIDDLE_EAST_EVENTS}
    if code == "m5_qatar_force_majeure" and news == "good":
        return [{**by_name[name], "why": why,
                 "affected_companies": [{**c, "share_price": _FLIP[c["share_price"]]}
                                        for c in by_name[name]["affected_companies"]]}
                for name, why in _HELIUM_RESTART_WHY.items()]
    return [by_name[name] for name in _ROWS_BY_CODE.get((code, news), [])]


def plain_relationship(text: str) -> str:
    """A counterparty relationship as shown to readers: the table's
    "(spec 4.3)" citations are for us, not them."""
    return _SPEC_REF_RE.sub("", text or "").strip()


def read_through(ex: dict[str, Any], name_index: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    if event_code(ex) is None or ex.get("news_effect") not in ("good", "bad"):
        return []
    affected: dict[str, dict[str, Any]] = {}
    companies = [] if event_type(ex) == "M3" else ex.get("companies", [])
    for c in companies:
        row = name_index.get(c["name"].casefold()) if c["tracked"] else None
        # A fund's counterparties (Mubadala -> GlobalFoundries) are what it
        # OWNS, which its new deals do not move - for any event type (review
        # 2026-10-23, point 23). Which rows are funds is the table's `kind`.
        if (row or {}).get("kind") == "fund":
            continue
        for cp in (row or {}).get("counterparties", []):
            share = cp["share_price_on_good_news"]
            affected.setdefault(cp["counterparty_ticker"], {
                "name": cp["counterparty_name"], "ticker": cp["counterparty_ticker"],
                "role": "affected",
                "share_price": share if ex["news_effect"] == "good" else _FLIP[share],
                "relationship": plain_relationship(cp["relationship"])})
    # An Israeli site event that names its company moves that company only:
    # the generic Israel row added Nvidia and Tower to every Intel fab row
    # (review 2026-10-23, point 22). The company comes from the table
    # ("Intel Kiryat Gat") or as a listed name the model wrote ("Intel").
    own = {c["ticker"] for c in ex.get("companies", []) if c.get("ticker")}
    if event_type(ex) == "M7" and affected:
        return list(affected.values())
    for row in _event_rows(event_code(ex), ex["news_effect"]):
        named = [c for c in row["affected_companies"] if c["ticker"] in own]
        for c in (named if event_type(ex) == "M7" and named else row["affected_companies"]):
            affected.setdefault(c["ticker"], {
                "name": c["company_name"], "ticker": c["ticker"], "role": "affected",
                "share_price": c["share_price"], "relationship": row["why"]})
    return list(affected.values())


# ---------------------------------------------------------------------------
# Batch
# ---------------------------------------------------------------------------
_FALLBACK_REASON = ("This article could not be assessed automatically: the model's answer"
                    " could not be read. It is kept as possibly relevant rather than hidden.")


def _model_result(article: dict[str, Any], source: str, system: str,
                  name_index: dict[str, dict[str, Any]],
                  counterparty_tickers: dict[str, str],
                  waterway_status: dict[str, dict[str, Any]] | None = None) -> dict[str, Any] | None:
    title = article.get("title") or ""
    body = article.get("body") or ""
    truncated = len(body) > _BODY_MAX_CHARS
    body = body[:_BODY_MAX_CHARS]
    tracked = tracked_names_in(f"{title}\n{body}", name_index)
    user = _user_message(article, source, body, tracked)
    raw = _call_model(system, user)
    if raw is None:
        return None                       # not stored: the next run retries it
    base: dict[str, Any] = {"extracted_by": {"model": config.MIDDLE_EAST_SIGNAL_MODEL,
                                             "prompt": PROMPT_VERSION}}
    if truncated:
        base["body_truncated"] = True
    ex, dropped = check_extraction(raw, title, body, name_index, counterparty_tickers,
                                   outlet_name(article, source), _published(article))
    invented = [d.split(":", 1)[1] for d in dropped if d.startswith("unknown_event:")]
    if ex is None and invented:
        # The model blends two codes into one that does not exist
        # ("m4_ship_attack_tally", "m7_site_setback": 3 of 361 articles in
        # the full run, the same on a re-run). Guessing which code it meant
        # would be ours, not the text's, so it is asked once to pick from
        # the list; a second miss stays model_output_unusable.
        logger.info("[MIDDLE_EAST_SIGNAL] repair call url=%s: invented code %s",
                    article.get("url"), ", ".join(invented))
        repaired = _call_model(system, user, [
            {"role": "assistant", "content": raw},
            {"role": "user", "content":
                f"The event code {', '.join(invented)} is not one of the codes listed in STEP 1. Use"
                " only a code from that list, exactly as written, or null when none fits. Return the"
                " whole JSON answer again."}])
        if repaired is not None:
            ex, dropped = check_extraction(repaired, title, body, name_index, counterparty_tickers,
                                           outlet_name(article, source), _published(article))
    if dropped:
        logger.info("[MIDDLE_EAST_SIGNAL] dropped from model answer url=%s: %s",
                    article.get("url"), ", ".join(dropped))
    if ex is None:
        logger.warning("[MIDDLE_EAST_SIGNAL] unusable model answer url=%s: %s",
                       article.get("url"), dropped)
        return {"signal": "weak_signal", "reason": _FALLBACK_REASON, "entities": [],
                "metadata": {**base, "rule": "model_output_unusable"}}

    label, rule, sentence = decide_label(ex, title, body, waterway_status, _published(article))
    # The article's other new events (review point 38): each judged the same
    # way; the strongest becomes the row's event, the rest stay listed.
    published = _as_date(_published(article))
    for i, other in enumerate(list(ex.get("other_events") or [])):
        if other["event"]["code"] in _STATEMENT_CODES:
            continue
        # Only the article's OWN news takes over the row: an event within
        # _OTHER_EVENT_FRESH_DAYS of it. An analysis piece recalling a
        # sinking from days before became a sinking alert (2026-10-23).
        o_day = _exact_day(other["event"].get("date"))
        if not (o_day and published and (published - o_day).days <= _OTHER_EVENT_FRESH_DAYS):
            continue
        candidate = {**ex, "event": other["event"], "confirmation": other["confirmation"], "deal": None}
        o_label, o_rule, o_sentence = decide_label(candidate, title, body, waterway_status,
                                                   _published(article))
        if _STORY_LABEL_RANK.get(o_label, 2) < _STORY_LABEL_RANK.get(label, 2):
            previous = {"event": ex["event"], "confirmation": ex["confirmation"],
                        "facts": ex.get("reason_facts")}
            ex["other_events"][i] = {k: v for k, v in previous.items() if v}
            ex["event"], ex["confirmation"] = other["event"], other["confirmation"]
            ex.pop("deal", None)                 # a deal belongs to the event it came with
            if other.get("facts"):
                ex["reason_facts"] = other["facts"]
            label, rule, sentence = o_label, o_rule, o_sentence
    # Each model field is stored ONCE, in the column made for it:
    #  - companies -> entities_json (role "subject"), with the read-through
    #    companies (role "affected") - so entity_names_normalized finds a row
    #    by "g42" or by "nvidia";
    #  - summary (what happened) -> metadata.summary. It is NOT the reason:
    #    signal_reason says why the row got its label - the rule's sentence,
    #    then which US companies are likely to rise or fall;
    #  - every other field the label rules read -> metadata, in the shape
    #    the model returned it (event, amount and confirmation are objects),
    #    absent when not stated - nothing is stored as null. With these, a
    #    rule change re-labels a stored row without a model call.
    entities = [{"name": c["name"], "ticker": c["ticker"], "country": c["country"],
                 "role": "subject"} for c in ex.get("companies", [])]
    reason = " ".join(part for part in (ex.pop("reason_facts"), sentence) if part)
    if label != "noise":
        entities += read_through(ex, name_index)
    stored = {k: v for k, v in ex.items() if k != "companies"}
    return {"signal": label, "reason": reason, "entities": entities,
            "metadata": {**base, **stored, "rule": rule}}


def counterparty_ticker_index(companies: list[dict[str, Any]]) -> dict[str, str]:
    """US counterparty names (case-folded) -> ticker, so a subject the model
    names as "Nvidia" carries NVDA."""
    return {cp["counterparty_name"].casefold(): cp["counterparty_ticker"]
            for c in companies for cp in c["counterparties"]}


def classify_middle_east_signal_batch(
    articles: list[dict[str, Any]],
    companies: list[dict[str, Any]],
    us_universe: list[dict[str, Any]] | None = None,
    stored_event_members: dict[str, list[dict[str, Any]]] | None = None,
    waterway_status: dict[str, dict[str, Any]] | None = None,
) -> list[dict[str, Any]]:
    """Classify new articles against the company table (already checked by
    company_table_problems). Returns one {"article", "result"} per article
    judged; an article left out is NOT stored and is retried next run:
      - the model call failed (timeout, outage);
      - a 13D/G arrived while research-universe (`us_universe`) was
        unavailable - its label depends on that list.
    `stored_event_members` is {event_uri: rows already stored} for this
    batch's newsapi.ai event clusters - see corroboration_for().
    """
    _reset_spend()
    name_index = company_name_index(companies)
    cp_tickers = counterparty_ticker_index(companies)
    universe = us_universe_index(us_universe) if us_universe is not None else None
    system = config.MIDDLE_EAST_SIGNAL_PROMPT.read_text(encoding="utf-8")

    out: list[dict[str, Any] | None] = [None] * len(articles)
    to_model: list[int] = []
    paths: Counter = Counter()
    for i, article in enumerate(articles):
        source = source_name(article)
        if source == "sec_fund_stake":
            if universe is None:
                paths["sec_waiting_for_universe"] += 1
                continue
            out[i] = {"article": article, "result": judge_sec_stake(article, universe)}
            paths["sec_rule"] += 1
            continue
        skipped = headline_rule(article, source)
        if skipped:
            rule, reason = skipped
            out[i] = {"article": article, "result": {
                "signal": "noise", "reason": reason, "entities": [],
                "metadata": {"rule": rule}}}
            paths[f"headline_{rule}"] += 1
            continue
        to_model.append(i)

    def judge(i: int) -> dict[str, Any] | None:
        article = articles[i]
        try:
            return _model_result(article, source_name(article), system, name_index, cp_tickers,
                                 waterway_status)
        except Exception:
            logger.exception("[MIDDLE_EAST_SIGNAL] classify failed url=%s", article.get("url"))
            return None

    # The first call goes alone so the provider has cached the instructions
    # before the parallel calls start; sent together, they all miss the
    # cache (measured: 3% cached on an 18-article run).
    results = [judge(i) for i in to_model[:1]]
    with ThreadPoolExecutor(max_workers=max(1, config.MIDDLE_EAST_CLASSIFY_CONCURRENCY)) as pool:
        results += list(pool.map(judge, to_model[1:]))
    for i, result in zip(to_model, results):
        if result is None:
            paths["model_failed"] += 1
            continue
        out[i] = {"article": articles[i], "result": result}
        paths["model_" + result["signal"]] += 1

    judged = [r for r in out if r is not None]
    for r in judged:
        r["result"]["metadata"]["source"] = source_fields(r["article"])
    batch_members = event_members(judged)
    for r in judged:
        if r["result"]["signal"] != "noise" and source_name(r["article"]) != "sec_fund_stake":
            r["result"]["metadata"]["corroboration"] = corroboration_for(
                r["article"], batch_members, stored_event_members or {})

    used = spend()
    logger.info("[MIDDLE_EAST_SIGNAL] paths: %s", dict(sorted(paths.items())))
    logger.info(
        "[MIDDLE_EAST_SIGNAL] spend: %d calls, %d input tokens (%d%% cached), %d output tokens, $%.4f",
        used["calls"], used["prompt"],
        round(100 * used["cached"] / used["prompt"]) if used["prompt"] else 0,
        used["completion"], used["cost"])
    return judged


# ---------------------------------------------------------------------------
# Market reference - a check on what the run may have missed
# ---------------------------------------------------------------------------
# Prices are never an event and never move a label (a price report is
# m4_commentary). They are how the review of 2026-10-23 found the biggest
# miss: Brent at $110 on a day with no signal. So each run logs the oil
# reference and every US stock this module can name - from the company and
# events tables, not a hand list - with the biggest daily moves of the last
# 10 trading days next to the signal rows stored that day: a big move on a
# day with none is a row to look at by hand. Prices are news-retrieval's
# DynamoDB market data (GET /market/price-history).


def market_reference_tickers(companies: list[dict[str, Any]]) -> list[str]:
    """The oil reference (config.MIDDLE_EAST_OIL_REFERENCE_TICKER), then every
    US ticker the company table's counterparties and the events table name."""
    tickers = [config.MIDDLE_EAST_OIL_REFERENCE_TICKER]
    for c in companies:
        tickers += [cp["counterparty_ticker"] for cp in c.get("counterparties") or []]
    for row in MIDDLE_EAST_EVENTS:
        tickers += [a["ticker"] for a in row["affected_companies"]]
    return list(dict.fromkeys(t for t in tickers if t))


# What makes a market day unusual, measured on 95 days of prices (2026-10-23):
# single-stock jumps are mostly company news (Amazon +5% on 19 Oct); the
# Middle East shows as oil, or as many of these stocks moving together (22
# Oct: oil +2.8%, 9x its normal move; 13 stocks over 3x theirs).
UNUSUAL_MOVE_FACTOR = 3.0         # x the ticker's median daily move
UNUSUAL_MIN_STOCKS = 3            # stocks moving unusually on the same day
_MOVE_BASELINE_DAYS = 60
_MOVE_MIN_BASELINE = 10


def unusual_market_days(histories: dict[str, list[dict[str, Any]]], tickers: list[str],
                        since: str) -> list[dict[str, Any]]:
    """Unusual days on or after `since` (YYYY-MM-DD): oil (tickers[0]) moving
    UNUSUAL_MOVE_FACTOR x its median daily move of the previous
    _MOVE_BASELINE_DAYS days, or UNUSUAL_MIN_STOCKS stocks doing so. Each:
    {"day", "oil": (pct, x) or None, "stocks": [(ticker, pct, x)]}."""
    flagged: dict[str, dict[str, Any]] = {}
    for i, ticker in enumerate(tickers):
        closes = sorted((h["date"], h["adjusted_close"]) for h in histories.get(ticker) or []
                        if h.get("adjusted_close"))
        rets = [(day, 100 * (close / prev - 1))
                for (_, prev), (day, close) in zip(closes, closes[1:]) if prev]
        for j, (day, pct) in enumerate(rets):
            base = [abs(r) for _, r in rets[max(0, j - _MOVE_BASELINE_DAYS):j]]
            if day < since or len(base) < _MOVE_MIN_BASELINE:
                continue
            normal = statistics.median(base)
            if not normal or abs(pct) < UNUSUAL_MOVE_FACTOR * normal:
                continue
            entry = flagged.setdefault(day, {"day": day, "oil": None, "stocks": []})
            if i == 0:
                entry["oil"] = (pct, abs(pct) / normal)
            else:
                entry["stocks"].append((ticker, pct, abs(pct) / normal))
    return [e for _, e in sorted(flagged.items())
            if e["oil"] or len(e["stocks"]) >= UNUSUAL_MIN_STOCKS]


def missed_move_lines(days: list[dict[str, Any]], oil_ticker: str,
                      signals_by_day: dict[str, int]) -> list[str]:
    """One line per unusual day - COVERED when a Middle East signal was stored
    that day or the day before, MISS otherwise - then the score."""
    lines, misses = [], 0
    for e in days:
        day = date.fromisoformat(e["day"])
        covered = signals_by_day.get(e["day"], 0) + signals_by_day.get(f"{day - timedelta(days=1)}", 0)
        what = []
        if e["oil"]:
            what.append(f"oil ({oil_ticker}) {e['oil'][0]:+.1f}% ({e['oil'][1]:.1f}x normal)")
        if e["stocks"]:
            top = sorted(e["stocks"], key=lambda s: -s[2])[:5]
            what.append(f"{len(e['stocks'])} stocks over {UNUSUAL_MOVE_FACTOR:g}x normal ("
                        + ", ".join(f"{t} {p:+.1f}%" for t, p, _ in top) + ")")
        misses += not covered
        lines.append(f"{'COVERED' if covered else 'MISS'} {e['day']}: " + "; ".join(what)
                     + f" - Middle East signals that day or the day before: {covered}")
    lines.append(f"missed-move score: {misses} of {len(days)} unusual days had no Middle East signal")
    return lines


# ---------------------------------------------------------------------------
# Corroboration - who else reported this same event
# ---------------------------------------------------------------------------
# The geopolitical pipeline's Stage D, computed when a row is classified:
# no model, no network. Two independent sources of other coverage:
#  - news-retrieval's title-similarity dedup (`also_reported_by` on the
#    article): near-identical headlines it discarded as the same event;
#  - newsapi.ai's own event cluster (`event_uri`): articles it grouped as
#    one event even when the headlines share no words - in this batch, and
#    in rows already stored.
# Stored as {count, siblings} on every signal and weak_signal row from a
# press or wire source; an explicit count 0 means "looked, none found".
# Not stored on noise (nobody reads its coverage) or SEC filings (a filing
# has no other outlets). It is shown to the reader, never used to raise a
# label: seven outlets repeating unnamed sources are still unnamed sources.

def source_fields(article: dict[str, Any]) -> dict[str, str]:
    """metadata.source - where the article came from:
      name       the source or newsapi.ai search (`wam_wire`, `eventregistry:me_shipping`)
      url        newsapi.ai only: the publisher's link - the row's own url is
                 the synthetic er-me:// id, which a reader cannot open
      outlet     newsapi.ai only: who published it (`reuters.com`)
      event_uri  newsapi.ai only, when it has one: its event cluster - how
                 later articles about the same event find this row
    Other sources keep only `name`: their row url is already the real link."""
    md = article.get("metadata") or {}
    fields = {"name": source_name(article)}
    if md.get("source_type") == "eventregistry":
        fields.update(url=md.get("article_url"), outlet=md.get("source_uri"),
                      event_uri=md.get("event_uri"))
    return {k: v for k, v in fields.items() if v and v != "None"}


def _member(row_id: Any, title: str | None, src: dict[str, Any], url: str | None) -> dict[str, Any]:
    return {"id": row_id, "title": title or "", "url": src.get("url") or url or "",
            "outlet": src.get("outlet") or ""}


def event_members(judged: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    """{event_uri: [cluster member]} for the articles in this batch."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for r in judged:
        src = r["result"]["metadata"]["source"]
        if src.get("event_uri"):
            grouped.setdefault(src["event_uri"], []).append(
                _member(r["article"].get("url"), r["article"].get("title"), src, r["article"].get("url")))
    return grouped


def stored_member(row: dict[str, Any]) -> dict[str, Any]:
    """A stored agent_classifications row as a cluster member."""
    md = row.get("metadata") or {}
    if isinstance(md, str):
        md = json.loads(md)
    return _member(row.get("source_id") or row.get("url"), row.get("title"),
                   md.get("source") or {}, row.get("url"))


def corroboration_for(article: dict[str, Any],
                      batch_members: dict[str, list[dict[str, Any]]],
                      stored_members: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    md = article.get("metadata") or {}
    event_uri = md.get("event_uri") if md.get("event_uri") not in (None, "None") else None
    cluster = []
    if event_uri:
        cluster = stored_members.get(event_uri, []) + batch_members.get(event_uri, [])
    also = md.get("also_reported_by")
    if isinstance(also, str):
        try:
            also = json.loads(also)
        except ValueError:
            also = []
    return build_corroboration(
        event_uri=event_uri, row_id=article.get("url"), cluster=cluster,
        also_reported_by=also if isinstance(also, list) else [],
        self_url=md.get("article_url") or article.get("url"))


# ---------------------------------------------------------------------------
# Stories - which rows are one developing situation
# ---------------------------------------------------------------------------
# The geopolitical pipeline's Stage E, adapted. Corroboration asks "who
# else reported this EVENT"; a story asks "is this the same SITUATION":
# an IRGC closure of Hormuz, a tanker hitting a mine there and a shipping
# line pausing the route are three events and one story.
#
# Two rows are one story when, within 72 hours, they have the SAME event
# type (M1-M8) and share at least one SPECIFIC name - a company the
# article is about, or the place (Strait of Hormuz, Ras Laffan, Kiryat
# Gat). Countries, armed forces, issuing agencies and commodities never
# count on their own (geopolitical Stage E's lists, each from a measured
# wrong grouping). Same event type is required because a name alone
# joins different situations: G42 switching on capacity (M2) and G42 on a
# blacklist (M8) are two stories. The read-through companies (Nvidia,
# Micron) are never keys - they are on almost every row.
#
# Seeded, not transitive (a row joins by matching the story's SEED), and
# nothing is deleted: the seed carries metadata.story_development, the
# others metadata.in_story = seed id. One row per story is the filter
#     signal_detection IN ('signal','weak_signal') AND NOT (metadata ? 'in_story')
# Regrouped from scratch on every run, so a later arrival can re-seed.
_STORY_WINDOW_HOURS = 72
# An incident at one site develops over days, not hours: the Intel Kiryat
# Gat blast (14 Oct) and the hazardous-materials leak at the same fab (19
# Oct) were two stories under 72h (review 2026-10-23, point 36).
_SITE_STORY_CODES = frozenset({"m7_site_incident", "m7_site_disruption"})
_SITE_STORY_WINDOW_HOURS = 14 * 24
_STORY_LABEL_RANK = {"signal": 0, "weak_signal": 1}


# Names that never identify a story on their own here, on top of
# geopolitical Stage E's lists: the Gulf countries (two unrelated UAE AI
# governance pieces were grouped on "uae") and the Saudi-led coalition,
# which reports every Houthi interception - the actor, not the story.
_ME_GENERIC_KEYS = frozenset({
    "uae", "united arab emirates", "emirates", "saudi", "saudi arabia", "ksa", "qatar",
    "kuwait", "bahrain", "oman", "gcc", "gulf", "middle east", "uae government",
    "coalition restore legitimacy", "crly", "coalition", "saudi-led coalition",
})
# Words that name a country or region but no specific place.
_ME_GENERIC_WORDS = frozenset(
    w for k in _ME_GENERIC_KEYS | _MIDDLE_EAST_COUNTRIES for w in k.replace("-", " ").split()
) | {"province", "region", "kingdom", "territory", "territories"}
# Direction and position words around a place: "eastern Strait of Hormuz"
# and "east of the Strait of Hormuz" are one place.
_PLACE_FILLER = frozenset({
    "the", "of", "off", "near", "in", "at", "and", "around", "surrounding", "waters", "waterways",
    "coast", "area", "region", "east", "eastern", "west", "western", "north", "northern",
    "south", "southern", "central", "outside", "inside"})


def place_key(place: str) -> str:
    """A place as a grouping key - the place name only, never initials
    (initials of "eastern Strait of Hormuz" gave the junk key "esh")."""
    words = [w for w in re.findall(r"[a-z0-9]+", normalize_text(place)) if w not in _PLACE_FILLER]
    return " ".join(words)


def story_keys(row: dict[str, Any]) -> set[str]:
    """The SPECIFIC names a row can be grouped on: the companies it is
    about and the place of its event."""
    keys: set[str] = set()
    for e in row.get("entities") or []:
        if e.get("role") == "subject":
            keys |= _entity_keys(e.get("name") or "")
    place = _event(row.get("metadata") or {}, "place")
    if place:
        keys.add(place_key(place))
    return {k for k in keys if k and _is_specific(k) and k not in _ME_GENERIC_KEYS}


def _hours_apart(a: Any, b: Any) -> float | None:
    try:
        return abs((b - a).total_seconds()) / 3600.0
    except (TypeError, AttributeError):
        return None


def build_middle_east_stories(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group signal/weak_signal rows into stories of two or more. Each row
    needs id, title, url, published (datetime or None), signal_detection,
    signal_reason, entities (parsed) and metadata.

    The seed - the row a reader lands on - is a signal before a weak
    signal, then the best-confirmed, then the best-corroborated, then the
    earliest (confirmation added after review 2026-10-23, point 37: a
    better-confirmed later report must lead)."""
    for r in rows:
        r["_keys"] = story_keys(r)
    ordered = sorted(rows, key=lambda r: (
        _STORY_LABEL_RANK.get(r.get("signal_detection"), 2),
        _CONFIRMATION_RANK.get(((r.get("metadata") or {}).get("confirmation") or {}).get("level"), 3),
        *_lead_quality(r),
        -int(((r.get("metadata") or {}).get("corroboration") or {}).get("count") or 0),
        r.get("published").timestamp() if r.get("published") else float("inf")))
    stories: list[dict[str, Any]] = []
    for row in ordered:
        for story in stories:
            seed = story["seed"]
            if (event_type(row["metadata"]) != event_type(seed["metadata"])
                    or not event_type(row["metadata"])):
                continue
            shared = seed["_keys"] & row["_keys"]
            gap = _hours_apart(seed.get("published"), row.get("published"))
            window = (_SITE_STORY_WINDOW_HOURS
                      if {event_code(seed["metadata"]), event_code(row["metadata"])} <= _SITE_STORY_CODES
                      else _STORY_WINDOW_HOURS)
            # A row with no published time cannot be placed in the window,
            # so it never joins a story.
            if shared and gap is not None and gap <= window:
                story["members"].append({"row": row, "shared": shared})
                break
        else:
            stories.append({"seed": row, "members": []})
    return [s for s in stories if s["members"]]


# ---------------------------------------------------------------------------
# Same-event duplicates - folded into one row (tombstones, never deleted)
# ---------------------------------------------------------------------------
# Several outlets report one event (the IRGC closing Hormuz, the Intel fab
# blast) and each gets its own row and label. A reader must see the event
# once, so - as in geopolitical Stage B - the duplicates are folded into one
# row: their outlet, headline and link are added to that row's
# corroboration.siblings (via "same_event"), and the duplicate rows are
# TOMBSTONED: signal_detection = noise, metadata.collapsed_into = the kept
# row's id. Never deleted: the already-judged skip reads their url, and a
# wrong fold stays auditable and reversible.
#
# newsapi.ai's event_uri is NOT used to decide "same event" here: on the
# 403-article run one event_uri held 15 different rows (the closure,
# several different tanker hits, a US statement). Two rows are one event
# only when they have the same event code, are within 48 hours, and share
# either a company the article is about or - for events tied to one place
# (a closure, a site incident, a strike on a site) - the place. Ship
# attacks fold only on a shared ship or company name: several tankers hit
# near Hormuz on one day are separate events.
_SAME_EVENT_WINDOW_HOURS = 48
# With both events dated, reports of one event fold however far apart they
# were published, up to this - a sinking on 13 October was still being
# reported on the 19th, and each report outside 48h became its own alert.
_SAME_EVENT_DATED_WINDOW_HOURS = 14 * 24
_SAME_EVENT_DATE_SLACK_DAYS = 1      # "Sunday night" vs "Monday" for one event
_NAMED_SHIP_DATE_SLACK_DAYS = 3      # hit one day, sank under tow later
_PLACE_BOUND_CODES = frozenset({
    "m4_waterway_closed", "m4_waterway_reopened", "m4_strike_on_site", "m4_other_strike",
    "m4_attack_tally", "m4_strike_plan", "m5_qatar_force_majeure", "m7_site_incident", "m7_site_disruption",
    "m7_home_front_restriction"})
_CONFIRMATION_RANK = {"official": 0, "named_source": 1, "unnamed": 2}


def _lead_quality(row: dict[str, Any]) -> tuple[int, int, int]:
    """Tie-breakers for which report leads an event or story, after label
    and confirmation (review point 37): (1) the target, operator or an
    authority confirming before the attacker's own claim - both "official",
    but a target confirming a hit is the stronger evidence; (2) a report
    whose facts sentence passed the text check before one that fell back to
    its headline; (3) a dated event before an undated one."""
    md = row.get("metadata") or {}
    by = (md.get("confirmation") or {}).get("by") or ""
    attacker = int(event_code(md) in _ATTACK_CODES and bool(_CONTROLLING_PARTY_RE.search(by)))
    fallback = int((row.get("signal_reason") or "").startswith("Headline:"))
    undated = int(not _event(md, "date"))
    return attacker, fallback, undated


def _subject_keys(row: dict[str, Any],
                  name_index: dict[str, dict[str, Any]] | None = None) -> set[str]:
    """The specific names an event can be matched on: the companies the
    row is about, plus - with the company table - any tracked company its
    HEADLINE names. One Aramco HQ strike was reported at "Aramco compound,
    Eastern Province" and at "Dhahran", and the model listed different
    parties each time; both headlines name Aramco (review 2026-10-23)."""
    keys: set[str] = set()
    for e in row.get("entities") or []:
        if e.get("role") == "subject":
            keys |= _entity_keys(e.get("name") or "")
    if name_index:
        for name in tracked_names_in(row.get("title") or "", name_index):
            keys |= _entity_keys(name)
    return {k for k in keys if k and _is_specific(k) and k not in _ME_GENERIC_KEYS}


# Codes that describe the same kind of happening - an attack - so one
# attack coded differently by two reports ("Iran claims seven ships hit":
# a ship attack in one, a strike in another) is still one event.
_ATTACK_CODES = frozenset({"m4_strike_on_site", "m4_other_strike", "m4_ship_incident",
                           "m4_ship_sunk_or_crew_killed"})
_SHIP_CODES = frozenset({"m4_ship_incident", "m4_ship_sunk_or_crew_killed"})
_ATTACK_OR_INTERCEPTION = _ATTACK_CODES | {"m4_interception"}
_STRIKE_PLAN_WINDOW_HOURS = 72


def _same_place(a: str | None, b: str | None) -> bool:
    """One place written two ways: equal place keys, or one inside the
    other ("Strait of Hormuz" within "Persian Gulf and Strait of Hormuz")."""
    if not a or not b:
        return False
    ka, kb = set(place_key(a).split()), set(place_key(b).split())
    if not ka or not kb:
        return False
    if ka == kb:
        return True
    smaller = ka if len(ka) <= len(kb) else kb
    # A country or region alone ("Saudi Arabia") is inside every place in
    # it - it never joins two strikes on its own.
    if all(w in _ME_GENERIC_WORDS for w in smaller):
        return False
    return ka <= kb or kb <= ka


def _same_code(a: str | None, b: str | None) -> bool:
    return bool(a) and (a == b or (a in _ATTACK_CODES and b in _ATTACK_CODES))


def _same_event(kept: dict[str, Any], row: dict[str, Any],
                name_index: dict[str, dict[str, Any]] | None = None) -> bool:
    code, kept_code = event_code(row["metadata"]), event_code(kept["metadata"])
    gap = _hours_apart(kept.get("published"), row.get("published"))
    day_a, day_b = (_exact_day(_event(r["metadata"], "date")) for r in (kept, row))
    by_a, by_b = ((r["metadata"].get("confirmation") or {}).get("by") for r in (kept, row))
    # A drone "intercepted" whose debris killed workers at Shaybah was coded
    # an interception in four reports and a strike on the site in a fifth
    # (full run 2026-10-23): the same attack when the day and place match
    # exactly - a strike and an interception at one site a day apart are two.
    if {code, kept_code} & {"m4_interception"} and code in _ATTACK_OR_INTERCEPTION and \
            kept_code in _ATTACK_OR_INTERCEPTION:
        place_a, place_b = (_event(r["metadata"], "place") for r in (kept, row))
        return bool(day_a and day_a == day_b and (
            _same_place(place_a, place_b)
            or _subject_keys(kept, name_index) & _subject_keys(row, name_index)))
    if not _same_code(code, kept_code):
        return False
    # One count of attacks: dated within a day, and on the same waters - or
    # from the same speaker, since one side's claim is re-reported with the
    # place written three ways ("wider Gulf", "northern Arabian Gulf") and
    # the speaker two ways ("IRGC", "Iranian forces"; full run 2026-10-23).
    if code == "m4_attack_tally" and kept_code == "m4_attack_tally":
        if not (day_a and day_b and abs((day_a - day_b).days) <= _SAME_EVENT_DATE_SLACK_DAYS
                and gap is not None and gap <= _SAME_EVENT_DATED_WINDOW_HOURS):
            return False
        places = [_event(r["metadata"], "place") for r in (kept, row)]
        return _same_place(*places) or bool(by_a and by_b and _same_source(by_a, by_b))
    # A reported plan to strike one country, re-reported within days (the
    # "after the midterms" plan: a diplomat on the 20th, Trump on the 22nd).
    # Its date is the report's, so the publication window decides.
    if code == "m4_strike_plan" and kept_code == "m4_strike_plan":
        target_a, target_b = (_place_country(r["metadata"]) for r in (kept, row))
        return bool(target_a and target_a == target_b and gap is not None
                    and gap <= _STRIKE_PLAN_WINDOW_HOURS)
    subjects_a, subjects_b = _subject_keys(kept, name_index), _subject_keys(row, name_index)
    shared = subjects_a & subjects_b
    # Two deals by one Gulf company are two deals: when both name two or
    # more parties, they must share two (Humain-Nvidia 84 MW and
    # Humain-Nebius NEOM folded on "Humain" alone, full run 2026-10-23).
    if (code in _DEAL_CODES and kept_code in _DEAL_CODES and len(subjects_a) >= 2
            and len(subjects_b) >= 2 and len(shared) < 2):
        return False
    # A named ship is one event across a wider spread of dates: GFS Pride
    # was hit on the 13th and sank under tow - reports dated 13 and 15 Oct.
    slack = (_NAMED_SHIP_DATE_SLACK_DAYS if shared and code in _SHIP_CODES and kept_code in _SHIP_CODES
             else _SAME_EVENT_DATE_SLACK_DAYS)
    if day_a and day_b:
        # Both dated: the event's own day decides, not when it was reported.
        if (gap is None or gap > _SAME_EVENT_DATED_WINDOW_HOURS
                or abs((day_a - day_b).days) > slack):
            return False
    elif gap is None or gap > _SAME_EVENT_WINDOW_HOURS:
        return False
    if shared:
        return True
    place_a, place_b = (_event(r["metadata"], "place") for r in (kept, row))
    same_place = _same_place(place_a, place_b)
    if code in _PLACE_BOUND_CODES and kept_code in _PLACE_BOUND_CODES and same_place:
        return True
    # Ship attacks with no ship named (review point 35: one day's mine
    # strikes were three rows). Several tankers can be hit near Hormuz in
    # a day, so all three must match: the SAME day (no slack), the same
    # place, and the same source reporting it.
    if code in _SHIP_CODES and kept_code in _SHIP_CODES and same_place and day_a and day_a == day_b:
        return bool(by_a and by_b and _same_source(by_a, by_b))
    return False


def _same_event_groups(rows: list[dict[str, Any]],
                       name_index: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """[{"kept": row, "duplicates": [rows]}] for every event reported more
    than once. The kept row is the one a reader should see: a signal before
    a weak signal, then the best-confirmed, then the most corroborated,
    then the earliest. Seeded, not transitive - a row joins by matching the
    kept row itself. Each row needs id, url, title, published,
    signal_detection, entities (parsed) and metadata."""
    ordered = sorted(rows, key=lambda r: (
        _STORY_LABEL_RANK.get(r.get("signal_detection"), 2),
        _CONFIRMATION_RANK.get((r["metadata"].get("confirmation") or {}).get("level"), 3),
        *_lead_quality(r),
        -int((r["metadata"].get("corroboration") or {}).get("count") or 0),
        r.get("published").timestamp() if r.get("published") else float("inf")))
    groups: list[dict[str, Any]] = []
    for row in ordered:
        for group in groups:
            if _same_event(group["kept"], row, name_index):
                group["duplicates"].append(row)
                break
        else:
            # A noise report of an event (four "drone intercepted" reports of
            # the Shaybah hit) folds into the alert, but never leads one.
            if row.get("signal_detection") != "noise":
                groups.append({"kept": row, "duplicates": []})
    return [g for g in groups if g["duplicates"]]


def find_same_event_groups(rows: list[dict[str, Any]],
                           name_index: dict[str, dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    """_same_event_groups over `rows`, leaving out noise rows whose event is
    not new: an article mentioning March's Qatar force majeure as background
    is not a report of this week's extension, and must not corroborate or
    confirm it (full run 2026-10-23)."""
    return _same_event_groups([r for r in rows if r.get("signal_detection") != "noise"
                               or is_new(r.get("metadata") or {}, r.get("published"))], name_index)


_IRAN_FORCES_RE = re.compile(r"\b(?:irgc|revolutionary guards?|iranian (?:armed )?forces|iran's (?:armed )?forces)\b",
                             re.IGNORECASE)
_HOUTHI_FORCES_RE = re.compile(r"\b(?:houthi\w*|ansar ?allah|yemeni armed forces)\b", re.IGNORECASE)


def _armed_side(name: str) -> str | None:
    if _IRAN_FORCES_RE.search(name):
        return "iran"
    if _HOUTHI_FORCES_RE.search(name):
        return "houthis"
    return None


def _source_words(name: str) -> list[str]:
    return [w for w in re.findall(r"[a-z0-9]+", normalize_text(name).replace("'s", ""))
            if w not in _NAME_STOPWORDS]


def _same_source(a: str, b: str) -> bool:
    """"IRGC Navy" and "IRGC" are one source, and so are "Iran's navy" and
    "Iranian Navy" (a word and its adjective: one starts with the other);
    "UKMTO" and "Maritime and Port Authority of Singapore" are two."""
    na, nb = normalize_text(a), normalize_text(b)
    if na == nb or _whole_word(na, nb) or _whole_word(nb, na):
        return True
    # One side's armed forces, however named: "IRGC", "IRGC Navy", "Iranian
    # forces" (full run 2026-10-23: one claim of four ships hit stayed three
    # rows). Only military / official speakers of the side, so "Iran's
    # foreign minister" and the "Iranian navy" still compare by name.
    side_a, side_b = _armed_side(a), _armed_side(b)
    if side_a and side_a == side_b:
        return True
    wa, wb = _source_words(a), _source_words(b)
    if not wa or len(wa) != len(wb):
        return False
    return all(x == y or (min(len(x), len(y)) >= 4 and (x.startswith(y) or y.startswith(x)))
               for x, y in zip(wa, wb))


def confirmed_across_reports(reports: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The event's confirmation when its reports TOGETHER confirm more than
    the lead report does alone (review 2026-10-23, point 4: the "$110, nine
    ships attacked" report named no source, while another report of the same
    sinking quoted the maritime authority). `reports` = the lead row first,
    then every report folded into it.
      - any report confirmed officially -> official;
      - two or more DIFFERENT named sources -> official;
      - one source repeated by many outlets, or unnamed only -> no change.
    Returns {"level": "official", "by": [names]} or None. Uses only each
    row's stored metadata.confirmation."""
    own = (reports[0]["metadata"].get("confirmation") or {}).get("level")
    if own == "official":
        return None
    named: list[tuple[str, str]] = []
    for r in reports:
        conf = r["metadata"].get("confirmation") or {}
        by = conf.get("by")
        if conf.get("level") in ("official", "named_source") and by:
            if not any(_same_source(by, seen) for _, seen in named):
                named.append((conf["level"], by))
    officials = [by for level, by in named if level == "official"]
    if officials:
        return {"level": "official", "by": officials[:1] + [b for _, b in named if b not in officials[:1]]}
    if len(named) >= 2:
        return {"level": "official", "by": [b for _, b in named]}
    return None


def relabel_confirmed(kept: dict[str, Any], combined: dict[str, Any],
                      name_index: dict[str, dict[str, Any]],
                      waterway_status: dict[str, dict[str, Any]] | None = None,
                      ) -> tuple[str, str, str] | None:
    """(label, rule, reason) for the lead row judged with the combined
    confirmation, when that RAISES its label; else None. Rebuilt from the
    stored row - no model call. `reason` keeps the stored facts sentence and
    says who confirmed it."""
    md = kept["metadata"]
    companies = [{"name": e.get("name") or "", "ticker": e.get("ticker"), "country": e.get("country"),
                  "tracked": (e.get("name") or "").casefold() in name_index}
                 for e in kept.get("entities") or [] if e.get("role") == "subject"]
    ex = {**md, "companies": companies}
    _, _, old_why = decide_label(ex, kept.get("title") or "", None, waterway_status, kept.get("published"))
    upgraded = {**ex, "confirmation": {"level": "official", "by": combined["by"][0]}}
    label, rule, why = decide_label(upgraded, kept.get("title") or "", None, waterway_status,
                                    kept.get("published"))
    if _STORY_LABEL_RANK.get(label, 2) >= _STORY_LABEL_RANK.get(kept.get("signal_detection"), 2):
        return None
    facts = kept.get("signal_reason") or ""
    if old_why and facts.endswith(old_why):
        facts = facts[: -len(old_why)].rstrip()
    names = combined["by"]
    who = names[0] if len(names) == 1 else f"{', '.join(names[:-1])} and {names[-1]}"
    return (label, f"{rule}_confirmed_across_reports",
            " ".join(p for p in (facts, why, f"Confirmed across reports by {who}.") if p))


# --- Guardrails: what the TEXT must show for a strong code to stand --------
# The model's code is a reading; these checks need the words that make it
# true. A code that fails is lowered to the code its text does support -
# a guardrail can only lower a label, never raise one (full run 2026-10-23:
# a tanker explosion and a shot-down helicopter were "sunk or crew killed",
# an oil spill and a destroyed launcher were "strikes on a site").
_SANK_OR_KILLED_RE = re.compile(
    r"\b(?:sank|sunk|sinking|sink|capsiz\w*|killed|dead|died|deaths?|fatalit\w*|lost their lives)\b",
    re.IGNORECASE)
_SHIP_WORD_RE = re.compile(
    r"\b(?:ships?|vessels?|tankers?|supertankers?|carriers?|freighters?|boats?|bulkers?|containerships?)\b",
    re.IGNORECASE)
_ATTACK_WORD_RE = re.compile(
    r"\b(?:strikes?|struck|attack\w*|drones?|missiles?|rockets?|projectiles?|hit|bomb\w*|shell\w*|"
    r"blast|explosion|explod\w*|sabotag\w*|targeted)\b", re.IGNORECASE)
# A SITE, not a commodity: "oil spill" or "oil tanker" names no site.
_SITE_WORD_RE = re.compile(
    r"\b(?:oil ?fields?|gas ?fields?|fields?|refiner\w*|plants?|ports?|terminals?|facility|facilities|"
    r"pipelines?|data cent\w*|cables?|stations?|factory|factories|compound|headquarters|offices|"
    r"depots?|storage|desalination|aramco|complex|installation)\b", re.IGNORECASE)
_CLOSURE_WORD_RE = re.compile(r"\b(?:clos\w*|shut\w*|block\w*|ban\w*|bar(?:red|s)?|halt\w*|seal\w*)\b",
                              re.IGNORECASE)
_REOPEN_WORD_RE = re.compile(r"\b(?:reopen\w*|open\w*|resum\w*|ceasefire|truce|allow\w*|lift\w*|"
                            r"pass|passage|transit\w*)\b",
                             re.IGNORECASE)
_HELIUM_CODES = frozenset({"m5_supplier_rationing", "m5_asia_shortfall"})


def _event_text(ex: dict[str, Any], title: str) -> str:
    """What the guardrails read: the headline, the event's place and the
    model's own two checked sentences about the event."""
    return " ".join(str(p) for p in (title, _event(ex, "place") or "", ex.get("reason_facts") or "",
                                     ex.get("summary") or "") if p)


def guarded_code(ex: dict[str, Any], title: str = "", body: str | None = None) -> str | None:
    """The event code the text supports - the model's code, or a weaker one."""
    code = event_code(ex)
    text = _event_text(ex, title) + " " + (body or "")
    if len(re.findall(r"[A-Za-z]{3,}", text)) < 5:
        return code                                   # nothing to read: no evidence either way
    if code == "m4_ship_sunk_or_crew_killed":
        if not _SHIP_WORD_RE.search(text):
            return "m4_other_strike"                  # an aircraft, not a ship
        if not _SANK_OR_KILLED_RE.search(text):
            return "m4_ship_incident"                 # fire, explosion, disabled
    # Status words are read in the event's own sentences only: the body of
    # nearly every Hormuz story says "closed" somewhere as background.
    own = _event_text(ex, title)
    if code == "m4_waterway_closed" and not _CLOSURE_WORD_RE.search(own):
        return "m4_threat_or_statement"               # "mine threats" are not a closure
    if code == "m4_waterway_reopened" and not _REOPEN_WORD_RE.search(own):
        return "m4_threat_or_statement"
    if code in _HELIUM_CODES and not re.search(r"\bhelium\b", text, re.IGNORECASE):
        return "m5_commentary"                        # gas, not helium
    if code == "m4_strike_on_site":
        if not _ATTACK_WORD_RE.search(text):
            return None                               # a spill or accident: no attack at all
        site_text = " ".join(p for p in (_event(ex, "place") or "", ex.get("reason_facts") or "",
                                         ex.get("summary") or "", title) if p)
        if not _SITE_WORD_RE.search(site_text):
            return "m4_other_strike"                  # no site named as hit
    return code


# --- Attack waves: separate weak ship attacks adding up (review point 13) ---
# One damaged ship is stored as weak; a week of them is an escalation. After
# folding (so each event is counted once), the distinct ship-attack events
# on one waterway are counted by their own dates: the weak event that makes
# _ATTACK_WAVE_EVENTS within _ATTACK_WAVE_DAYS is raised to signal - the same
# three-ship bar as m4_attack_tally. One wave alert per waterway per window,
# so a week of attacks is one alert, not one per ship.
_ATTACK_WAVE_EVENTS = 3
_ATTACK_WAVE_DAYS = 7
_WAVE_CODES = frozenset({"m4_ship_incident", "m4_ship_sunk_or_crew_killed", "m4_attack_tally"})


def _wave_key(row: dict[str, Any]) -> str | None:
    place = _event(row["metadata"], "place")
    return waterway_key(place) or (place_key(place) if place else None)


def attack_wave_raises(rows: list[dict[str, Any]]) -> list[tuple[int, str, str, str]]:
    """[(row id, label, rule, reason)] for weak ship attacks that complete a
    wave. `rows` = the visible signal/weak rows (folded duplicates left out),
    as from get_middle_east_story_candidates. A row already raised by a wave
    (its rule) counts as that window's alert."""
    by_key: dict[str, list[tuple[date, dict[str, Any]]]] = {}
    for r in rows:
        md = r.get("metadata") or {}
        if "collapsed_into" in md or event_code(md) not in _WAVE_CODES:
            continue
        day = _as_date(_event(md, "date")) or _as_date(r.get("published"))
        key = _wave_key(r)
        if day and key:
            by_key.setdefault(key, []).append((day, r))
    raises: list[tuple[int, str, str, str]] = []
    for key, events in by_key.items():
        events.sort(key=lambda e: (e[0], str(e[1].get("published"))))
        # Count ATTACKS, not reports: reports of one attack that did not fold
        # (one tanker off Qatar written as "off Qatar", "north of Madinat ash
        # Shamal", dated 7 and 8 Oct) made a false wave (test set,
        # 2026-10-23). A report from the same source within a day of a
        # counted one is that attack again - conservative: it can delay a
        # wave, never invent one.
        counted: list[tuple[date, str]] = []
        distinct: list[bool] = []
        for day, r in events:
            by = ((r.get("metadata") or {}).get("confirmation") or {}).get("by") or ""
            again = any(abs((day - d).days) <= 1 and by and b and _same_source(by, b) for d, b in counted)
            distinct.append(not again)
            if not again:
                counted.append((day, by))
        last_alert: date | None = None
        for i, (day, r) in enumerate(events):
            window = [d for (d, _), new in zip(events[: i + 1], distinct[: i + 1])
                      if new and (day - d).days < _ATTACK_WAVE_DAYS]
            # Any signal in the window - a sinking, an authority's count, or a
            # wave already raised - is that window's alert: the wave is for
            # weeks of weak incidents only (review point 13).
            if r.get("signal_detection") == "signal":
                last_alert = day
                continue
            if (r.get("signal_detection") != "weak_signal" or len(window) < _ATTACK_WAVE_EVENTS
                    or (last_alert and (day - last_alert).days < _ATTACK_WAVE_DAYS)):
                continue
            last_alert = day
            name = _WATERWAYS.get(key, _event(r["metadata"], "place") or key)
            raises.append((r["id"], "signal", f"{event_code(r['metadata'])}_attack_wave",
                           f"{r.get('signal_reason') or ''} It is attack number {len(window)} on"
                           f" shipping near {name} in {_ATTACK_WAVE_DAYS} days - an escalation that"
                           " moves oil, freight and insurance costs.".strip()))
    return raises


def repeated_count_lowers(rows: list[dict[str, Any]]) -> list[tuple[int, str, str, str]]:
    """[(row id, label, rule, reason)] lowering later attack-count signals on
    one waterway within _ATTACK_WAVE_DAYS of an earlier one. Counts for
    overlapping periods ("22 vessels, 1-13 Oct"; "12 in the week to 16 Oct")
    are one escalation: they do not fold (different periods) but must not
    alert three times in four days (test set, 2026-10-23). The earliest
    stays the alert; the later ones stay visible as weak."""
    by_key: dict[str, list[tuple[date, dict[str, Any]]]] = {}
    for r in rows:
        md = r.get("metadata") or {}
        if ("collapsed_into" in md or event_code(md) != "m4_attack_tally"
                or r.get("signal_detection") != "signal"):
            continue
        # The attacking side announcing a new night of attacks (the IRGC's
        # four ships set afire on 22 Oct) is a new event, not a recount of
        # a tracker's period tally (Kpler's 18 vessels in two weeks).
        if _armed_side((md.get("confirmation") or {}).get("by") or ""):
            continue
        day = _as_date(_event(md, "date")) or _as_date(r.get("published"))
        key = _wave_key(r)
        if day and key:
            by_key.setdefault(key, []).append((day, r))
    lowers: list[tuple[int, str, str, str]] = []
    for key, counts in by_key.items():
        counts.sort(key=lambda e: (e[0], str(e[1].get("published"))))
        alert_day: date | None = None
        for day, r in counts:
            if alert_day is None or (day - alert_day).days >= _ATTACK_WAVE_DAYS:
                alert_day = day
                continue
            name = _WATERWAYS.get(key, key)
            lowers.append((r["id"], "weak_signal", "m4_attack_tally_same_escalation",
                           f"{r.get('signal_reason') or ''} Another count of the same escalation near"
                           f" {name}, already alerted on {alert_day:%d %B}.".strip()))
    return lowers


def folded_corroboration(kept: dict[str, Any], duplicates: list[dict[str, Any]]) -> dict[str, Any]:
    """The kept row's corroboration with each duplicate added as a
    sibling (deduped by url) - the duplicate's own headline stays visible."""
    current = kept["metadata"].get("corroboration") or {"count": 0, "siblings": []}
    siblings = list(current.get("siblings") or [])
    seen = {s.get("url") for s in siblings if s.get("url")}
    for d in duplicates:
        src = d["metadata"].get("source") or {}
        url = src.get("url") or d.get("url") or ""
        if url in seen:
            continue
        seen.add(url)
        siblings.append({"outlet": src.get("outlet") or src.get("name") or "",
                         "title": (d.get("title") or "").strip(), "url": url, "via": "same_event"})
    return {"count": len(siblings), "siblings": siblings}


def story_development(story: dict[str, Any]) -> dict[str, Any]:
    """The field written to the seed row: each member's own event, so a
    reader opening the story sees what each development was. Copies of the
    member rows' fields - the member row stays the source of truth (join
    on `id`). `shared` says which names put each member here."""
    members = []
    for m in sorted(story["members"], key=lambda m: (m["row"].get("published") is None,
                                                      m["row"].get("published") or 0)):
        r = m["row"]
        md = r.get("metadata") or {}
        members.append({
            "id": r["id"],
            "title": (r.get("title") or "").strip(),
            "url": (md.get("source") or {}).get("url") or r.get("url") or "",
            "published": r["published"].isoformat() if r.get("published") else None,
            "signal_detection": r.get("signal_detection"),
            "reason": r.get("signal_reason"),
            "summary": md.get("summary"),
            "shared": sorted(m["shared"]),
        })
    return {"count": len(members), "members": members}
