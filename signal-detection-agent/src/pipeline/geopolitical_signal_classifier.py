"""Stage A: free, rule-based noise removal for geopolitical_news articles
(source_type='geopolitical_signal' in agent_classifications).

No model call anywhere in this module - every rule here is a deterministic
lookup/keyword check, applied in the order specified by the spec's Stage A
section, so 80-90% of daily volume is filtered out before Stage B's first
AI call. Same-story cross-outlet dedup is NOT done here - it's handled
upstream in news-retrieval's _dedup_by_title_similarity_for_domain (see
pipeline.py's _TITLE_DEDUP_DOMAINS), so a duplicate never reaches this
module as a separate article to begin with.

Each article gets exactly one outcome, named with the signal_detection
label it will be stored under: "noise" (with a drop_reason), "signal"
(a Federal Register action notice, which skips Stage B), "weak_signal",
or "waiting" (a survivor held for Stage B). Written by
insert_geopolitical_signal_classification in models/jobs.py. An article whose
article_id already has a row is skipped entirely by the caller before this
module ever runs (see get_existing_geopolitical_signal_article_ids).
"""
from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

# How far back an article's own publication date may be before Stage A
# treats it as an archive page rather than news. See the check in
# classify_geopolitical_signal_stage_a for how this figure was chosen.
_MAX_ARTICLE_AGE_DAYS = 14


def _age_in_days(published: object) -> float | None:
    """Days between ``published`` and now, or None if unreadable.

    news-retrieval sends an ISO-8601 string; a datetime is accepted too
    so a caller holding a parsed row needs no conversion. Returns None
    rather than raising on anything else, because an unparseable date
    must not decide a verdict - the caller treats None as "no opinion"
    and lets the article through.

    A future date also yields a negative number, which is below any
    positive cutoff, so a clock skew or a publisher post-dating an
    article cannot drop it.
    """
    if isinstance(published, datetime):
        dt = published
    elif isinstance(published, str) and published.strip():
        try:
            dt = datetime.fromisoformat(published.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0

# Wire services and papers of record, plus official US government domains.
# Geopolitics is the worst news category for junk/propaganda outlets - this
# allowlist is the main defence (spec's own framing). Kept intentionally
# narrow; extend only with sources of the same tier, not general news sites.
# What counts as English in a language field. Event Registry writes the
# ISO 639-2 code "eng"; a source writing the plain word, or the 2-letter
# "en", is accepted too so the check does not depend on which vocabulary
# a fetcher happens to use. Anything else is treated as non-English.
_ENGLISH_LANGUAGE_VALUES: frozenset[str] = frozenset({"eng", "en", "english"})

ALLOWED_DOMAINS: frozenset[str] = frozenset({
    # spec-named wire services / papers of record
    "reuters.com",
    "apnews.com",
    "bloomberg.com",
    "ft.com",
    "wsj.com",
    "aljazeera.com",
    "asia.nikkei.com",
    # additional wire services / papers of record - fill geographic gaps
    # the spec's 7 don't cover (global wire, Japan/Asia, Germany/Europe)
    "afp.com",
    "upi.com",
    "nhk.or.jp",
    "dw.com",
    "theguardian.com",
    "economist.com",
    # official US government domains - GDELT/Google News surface press
    # releases and notices from these directly
    "federalregister.gov",
    "whitehouse.gov",
    "state.gov",
    "treasury.gov",
    "commerce.gov",
    "defense.gov",
    # added after reviewing real Stage A output against a local GDELT
    # fetch (2026-08-10 to 2026-08-17, 6322 articles): these three were
    # confirmed responsible for real HIGH-candidate stories being dropped
    # as domain_not_allowed - e.g. "Canada Sanctions Streit Group Over
    # Armored Vehicle Supplies to Russia" (militarnyi.com), "AFU: Enemy
    # attacks Ukraine with Zirkon, Iskander, drone missiles" (interfax.com.ua)
    # - specialized defense/regional-conflict papers of record, not the
    # general-wire-service tier above, but real enough to add narrowly
    # rather than broadening the whole list speculatively.
    "militarnyi.com",
    "kyivpost.com",
    "interfax.com.ua",
    "en.interfax.com.ua",  # real GDELT data observed this exact subdomain,
                            # not bare interfax.com.ua - both listed rather
                            # than relying on suffix matching (see _extract_
                            # domain's docstring)
    # second review pass, same fetch: two more regional wire services
    # confirmed responsible for real missed sanctions/trade stories -
    # armenpress.am (Armenia's state wire, "Iran slams U.S. sanctions
    # policy... after Bessent says Iran is being 'suffocated'"),
    # newsukraine.rbc.ua (RBC-Ukraine, "US sanctions against Russia bill
    # moves to Congress"). Most of the rest of this pass's dropped sample
    # (aggregator sites republishing wire copy, off-topic keyword
    # coincidences, state propaganda outlets) was correctly excluded -
    # not added just because a headline contained a relevant keyword.
    "armenpress.am",
    "newsukraine.rbc.ua",
    # third review pass, fresh independent fetch (2026-09-09, run 50, 797
    # articles - separate from the Aug 10-17 sample above): aa.com.tr
    # (Anadolu Agency, Turkey's state wire service, appeared 3x for real
    # sanctions/trade stories in one small sample), france24.com (France's
    # international public broadcaster, same tier as dw.com/nhk.or.jp
    # already listed), haaretz.com (major Israeli paper of record, relevant
    # given Middle East coverage is a major theme in this feed).
    "aa.com.tr",
    "france24.com",
    "haaretz.com",
    # fourth review pass, real data confirmed these dropped as
    # domain_not_allowed while carrying real HIGH-candidate stories:
    # nytimes.com and politico.com (12 and 6 real dropped articles
    # respectively in one sample - both are papers-of-record/serious
    # policy-trade coverage, same tier as ft.com/economist.com already
    # listed), spglobal.com (S&P Global - commodities/critical-minerals
    # trade coverage, e.g. "China's export controls weigh on global
    # critical minerals supply"), agenceurope.eu (EU policy wire - missed
    # "European Commission updates export control list for dual-use
    # civilian and military items", a textbook official regulatory action).
    "nytimes.com",
    "politico.com",
    "spglobal.com",
    "agenceurope.eu",
    # fifth review pass, measured against the FULL stored staging history
    # (5,487 rows) rather than a sample: every one of the 5,280 Stage A
    # drops was domain_not_allowed - no article has ever been dropped as
    # non_english or by the talk-verb rule - so this allowlist is the
    # entire funnel loss, and only 207 articles (3.8%) have ever reached
    # Stage B at all.
    #
    # Deliberately a narrow addition, not a broad loosening. The 5,280
    # drops span 2,142 distinct domains with no head to unlock (top 10 =
    # 7% of drops, top 100 = 30%, and 1,225 domains appear exactly once),
    # and the highest-volume rejects are aggregator/junk traffic the
    # allowlist exists to stop - dailymail.com (86), newsroomamerica.com
    # (53), bignewsnetwork.com (41), plus screenrant.com and collider.com
    # (film sites matching on keyword coincidence). Scanning the dropped
    # set for genuine wire/paper-of-record/trade-press tiers yields ~194
    # recoverable articles, 3.7% of drops; at the measured 9.2% Stage B
    # HIGH rate that is roughly +18 HIGH rows, about doubling the feed.
    # Everything below was confirmed by reading its real dropped
    # headlines, not added on reputation:
    #   cnbc.com (17) - "Russian gold floods Hong Kong as Western
    #     sanctions redraw bullion trade"
    #   gcaptain.com (5), tradewindsnews.com (2) - shipping trade press,
    #     the chokepoint/vessel channel Stage B rates HIGH and no listed
    #     source covers: "U.S. Sanctions Law Puts Russia's Arctic Yamal
    #     LNG Fleet in Crosshairs"
    #   oilprice.com (12) - energy supply trade press
    #   scmp.com (13) - "US sanctions boost China's science-driven
    #     innovation by over 70%, study finds"; China/HK trade coverage
    #   channelnewsasia.com (10) - Asia wire, fills the same gap
    #     asia.nikkei.com covers for Japan
    #   jpost.com (13) - Israeli paper of record, same tier as the
    #     already-listed haaretz.com
    #   defensenews.com (3), breakingdefense.com (3) - defense trade
    #     press for the arms-export/defense-action channel
    #   latimes.com (15), cbc.ca (15), abcnews.com (14) - US/Canadian
    #     papers of record, same tier as nytimes.com already listed
    #   rferl.org (4) - US-funded wire for Russia/Central Asia
    # Excluded despite high volume, on the module's own standard: the
    # partisan/commentary tier (foxnews.com 30, nypost.com 29,
    # zerohedge.com 24, newsweek.com 20, thedailybeast.com 19,
    # breitbart.com 19, naturalnews.com 15) and euronews.com (19) /
    # defence24.com (13), whose dropped headlines read as opinion and
    # analysis ("Will the EU help to extend the NATO pipeline?") rather
    # than the completed actions Stage B is looking for.
    "cnbc.com",
    "gcaptain.com",
    "tradewindsnews.com",
    "oilprice.com",
    "scmp.com",
    "channelnewsasia.com",
    "jpost.com",
    "defensenews.com",
    "breakingdefense.com",
    "latimes.com",
    "cbc.ca",
    "abcnews.com",
    "rferl.org",
    # Sixth review pass, and the first selected on a different question
    # than the five above. Those asked "is this outlet a wire service or a
    # paper of record" - a general-press test, which is why the list reads
    # as general geopolitics. This domain's actual job is narrower: policy
    # and trade developments that transmit into the AI economy, and the
    # list had almost no coverage of the channels that actually carry
    # that. Mapping the 45 existing entries against Stage C's own 19
    # impacted categories left three channels effectively uncovered -
    # semiconductor/export-control trade press, critical-minerals and
    # energy press, and non-US tech-policy jurisdictions.
    #
    # Each was confirmed to extract a real body before being added (the
    # same discipline the passes above used for headlines), since an
    # outlet that passes Stage A and then reaches Stage C with no body
    # is tagged on its headline alone - which is the failure this
    # domain's Federal Register work exists to fix, and there is no point
    # reintroducing it through the allowlist.
    #
    # Semiconductors / compute hardware / export controls - the single
    # biggest gap. No trade publication covering chips or fab equipment
    # was on the list at all, despite Semiconductor Manufacturing and
    # Compute Hardware & Edge Systems being two of the four categories
    # Stage C's own Taiwan override force-adds.
    "tomshardware.com",          # 20,023 chars extracted
    "semiconductor-digest.com",  # 7,943
    "globalsources.com",         # electronics sourcing / China supply chain
    # Raw materials, critical minerals and energy - Raw Materials &
    # Critical Minerals and Energy & Grid Infrastructure were served only
    # by spglobal.com and oilprice.com.
    "miningweekly.com",          # 8,033
    "argusmedia.com",            # 2,101
    # Shipping chokepoints - joins gcaptain.com/tradewindsnews.com as the
    # third source on a channel Stage B rates HIGH on its own.
    "lloydslist.com",            # 2,498
    # Non-US jurisdictions that WRITE the export-control and semiconductor
    # rules. taipeitimes.com is the most consequential single entry here:
    # Taiwan is the jurisdiction Stage C's deterministic override exists
    # for, and no Taiwan-domiciled outlet was on the list.
    "taipeitimes.com",           # 6,712
    "koreaherald.com",           # 851
    "politico.eu",               # 4,022 - EU tech/trade policy
    # Russia sanctions and energy, from a non-state-aligned outlet.
    "themoscowtimes.com",        # 4,456
    # US and Canadian papers of record confirmed dropping real
    # trade/tariff stories in the stored sample ("Latest U.S. tariffs on
    # Canadian goods take effect", "U.S. tariffs are throwing Canadian art
    # sales into trade-war chaos").
    "cnn.com",                   # 6,236
    "nbcnews.com",               # 9,659
    "globalnews.ca",             # 11,482
    "theglobeandmail.com",       # 6,131
    # Excluded on this pass despite appearing in the dropped set, each for
    # the reason the passes above would have excluded it:
    #   washingtonpost.com, axios.com - extracted 0 chars from their real
    #     stored URLs (paywalled), so they would pass Stage A and reach
    #     Stage C headline-only
    #   iranintl.com - extracts cleanly but is state-adjacent advocacy,
    #     not a paper of record
    #   marineinsight.com - trade blog, below the gcaptain/Lloyd's List
    #     tier already listed
    #   devdiscourse.com - highest drop volume of any domain (6) and still
    #     excluded: aggregator copy, headlines like "Europe's Stance:
    #     Unyielding in Geopolitical Choices" are the vague commentary
    #     this allowlist exists to stop
})

# Talk verbs: the headline is reporting rhetoric, not a completed/announced
# action. Matched against the title case-insensitively. Spec's own list,
# used as-is for v1 - tune later against real headlines, not more guessing
# upfront.
_TALK_VERBS: frozenset[str] = frozenset({
    "warns", "urges", "vows", "calls for", "may", "could",
    "expected to", "slams", "condemns",
})

# Action verbs: the headline is reporting a completed/announced action.
# A match here (or no match from either list - ambiguous) both continue to
# Stage B; only a talk-verb match short-circuits to WEAK for free. This set
# exists mainly for documentation/tuning visibility - it does not gate
# anything talk verbs don't already gate.
_ACTION_VERBS: frozenset[str] = frozenset({
    "strikes", "seizes", "bans", "cuts", "halts", "closes",
    "sanctions", "imposes", "extends",
})

# federalregister.gov notice titles use bureaucratic phrasing ("Adjusting
# Imports of X", "Continuation of X Assistance") that news-style talk/
# action-verb keywords don't match, and no body text is fetched for this
# source (confirmed live: article 11551's body is NULL) - so unlike a
# regular news headline, Stage B has only the bureaucratic title to judge
# from, with no fallback context. Confirmed live: even with an improved
# Stage B prompt, "Adjusting Imports of Polysilicon and Its Derivatives
# Into the United States" (a real presidential proclamation restricting
# polysilicon imports - HIGH-eligible under the spec's own trade-access
# rule) still classified WEAK, because the title alone doesn't state
# whether the measure is completed or what it does.
#
# A Federal Register notice is, by definition, the official record of an
# agency/presidential action - not a proposal or talk (unlike a news
# article, which can report on something not yet decided) - so a title
# containing one of these trade/sanctions/supply terms is treated as
# reporting a real action already, skipping Stage B's talk/action-verb
# ambiguity on an information-poor title.
_FEDERAL_REGISTER_ACTION_KEYWORDS: frozenset[str] = frozenset({
    "tariff", "import", "export control", "sanction", "embargo",
    "entity list", "denied persons", "trade restriction", "quota",
    # "duties" added after a real miss: "Modifying the Scope of Products
    # of Canada Subject to the Additional Duties Imposed To Offset
    # Canadian Discrimination..." is a genuine tariff-scope trade action
    # (part of the same Canada trade-war notice series as the "Excluding
    # Certain Canadian Products..." notices this list already caught) but
    # uses "duties" rather than "tariff" - the two are synonyms in
    # Federal Register/customs notice titles.
    "duties",
})


def _is_federal_register_action_notice(title: str, domain: str) -> bool:
    if domain != "federalregister.gov":
        return False
    lowered = title.lower()
    return any(keyword in lowered for keyword in _FEDERAL_REGISTER_ACTION_KEYWORDS)


def _matches_any(title: str, keywords: frozenset[str]) -> bool:
    lowered = title.lower()
    return any(keyword in lowered for keyword in keywords)


def _extract_domain(article: dict) -> str:
    """Return the registrable domain to check against ALLOWED_DOMAINS.

    Confirmed live (2026-08-10 to 2026-08-17 GDELT fetch, 6322 articles)
    that article["source"] is NOT reliably a domain - depending on which
    fetcher populated it, it can be a bare domain (bnnbloomberg.ca), a
    subdomain-prefixed domain (en.interfax.com.ua), an English display name
    ("Reuters", "Bloomberg" - no domain at all), or a native-script display
    name ("Мілітарний", "Українська правда"). Trusting source directly
    silently dropped 4 real Reuters/Bloomberg articles in that same fetch.
    article["url"] was confirmed well-formed in every case in that sample,
    so the domain is derived from url instead - source is no longer read
    for this check.

    "www." is stripped so both "reuters.com" and "www.reuters.com" match a
    plain "reuters.com" allowlist entry; a subdomain like "en.interfax.com.ua"
    is matched against the allowlist as its own full value (see
    ALLOWED_DOMAINS's "interfax.com.ua" being insufficient for this exact
    case - the entry itself must include the subdomain that's actually
    observed, same as the existing "asia.nikkei.com" entry already does).
    """
    url = article.get("url") or ""
    host = urlparse(url).hostname or ""
    host = host.lower()
    if host.startswith("www."):
        host = host[len("www."):]
    return host


def classify_geopolitical_signal_stage_a(article: dict) -> dict:
    """Apply Stage A's free rules, in order, to one article. Returns a dict
    with at least an "outcome" key (noise/weak_signal/waiting - the
    stored signal_detection labels themselves, not synonyms) and, for DROPPED,
    a "drop_reason" key - the shape insert_geopolitical_signal_classification
    expects as its ``result`` argument.

    Caller is responsible for Stage A rule 1 ("already classified: skip") -
    that's a database check (get_existing_geopolitical_signal_article_ids),
    not something this function can determine from one article in isolation.
    """
    # Both spellings of the key, and both spellings of the VALUE.
    #
    # This read metadata["language"] and compared against the literal
    # "english", and was dead on both counts: Event Registry writes
    # `lang`, not `language` (measured: 325 of 325 rows carry
    # lang="eng" and none carries `language` at all), so the lookup
    # returned None and the guard short-circuited. Even with the right
    # key, "eng" != "english" would have dropped every English article
    # as non-English - the opposite failure, and a far louder one.
    #
    # Federal Register carries no language field at all; its documents
    # are English by definition. An absent value is not a rejection -
    # the same fail-open rule the rest of this pipeline uses, since
    # "we don't know the language" must never read as "it is foreign".
    meta = article.get("metadata") or {}
    language = (meta.get("lang") or meta.get("language") or "").strip().lower()
    if language and language not in _ENGLISH_LANGUAGE_VALUES:
        return {"outcome": "noise", "drop_reason": "non_english"}

    # An article far older than the fetch window is an archive page the
    # source surfaced, not news. The fetch runs --days-back 1, so
    # nothing legitimate should be weeks old: measured over 690 stored
    # rows, 676 were under 3.4 days, 8 at ~6.5 days, and the handful
    # beyond that were a 2017 Tesla feature, three 2018-19 renewable
    # round-ups and a "Top 5 Uranium Stocks" listicle 82 days old. NOT
    # ONE of them was a signal.
    #
    # The cutoff is 14 days, not 7, because the age being measured is
    # the article's own publication date and a legitimate item can lag:
    # a Federal Register notice records a measure taken earlier, and
    # the fetch itself can pool several runs. 14 days clears every real
    # row observed by a factor of two while still catching the archive
    # pages, which miss it by years.
    #
    # An ABSENT date is not a rejection - same fail-open rule as the
    # language check above. "We don't know when this was published"
    # must never read as "it is old".
    published = article.get("published")
    if published is not None:
        age_days = _age_in_days(published)
        if age_days is not None and age_days > _MAX_ARTICLE_AGE_DAYS:
            return {"outcome": "noise", "drop_reason": "too_old"}

    # No domain filter. There was one here - every article from an
    # outlet outside ALLOWED_DOMAINS was dropped as
    # "domain_not_allowed" - and removing it was the point: measured
    # over the full stored history (5,487 rows), ALL 5,280 Stage A
    # drops were that one rule, so only 3.8% of articles ever reached
    # Stage B. Real sanctions and export-control stories were lost
    # because a publisher was unlisted, and five review passes adding
    # domains by hand never caught up with a tail of 2,142 distinct
    # ones.
    #
    # ALLOWED_DOMAINS itself is NOT dead - Stage D still reads it for
    # `corroborated` (which outlets count as independent confirmation)
    # and `primary_source`. It means "an outlet we trust", which is a
    # different question from "an outlet worth reading at all", and
    # only the second was ever this rule's job.
    #
    # `domain` is still derived: it is stored on the row and read
    # downstream for display and corroboration.
    domain = _extract_domain(article)

    title = article.get("title") or ""

    # Confirmed live: a Federal Register notice with a trade/sanctions
    # keyword in the title still reached Stage B (no talk-verb match) and
    # was classified WEAK there anyway, because the bureaucratic title
    # alone ("Adjusting Imports of Polysilicon...") doesn't state whether
    # the measure is completed - and no body text exists for this source
    # to give Stage B more context. Skip Stage B's ambiguity entirely: a
    # Federal Register notice IS the official record of an already-enacted
    # action (never a proposal, unlike a news article), so this is free
    # and more reliable than asking a model to judge intent from an
    # information-poor title.
    if _is_federal_register_action_notice(title, domain):
        return {"outcome": "signal"}

    if _matches_any(title, _TALK_VERBS):
        return {"outcome": "weak_signal"}

    # Action-verb match or genuinely ambiguous (no match either way) both
    # continue to Stage B - the spec treats these identically.
    return {"outcome": "waiting"}
