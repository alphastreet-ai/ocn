"""Stage D: free quality grading for geopolitical_signal rows Stage C has
already tagged (signal_detection='signal').

No model call anywhere in this module - every check is a deterministic
lookup, same free-checks philosophy as Stage A. Stage D answers a
different question than Stage C: Stage C says "who is this relevant to",
Stage D says "how much should a reader trust this specific item" - the
grade this produces is what would ultimately decide ordering in a digest
(TOP items first), not whether an item is included at all.

Three independent checks, each true/false:
  - corroborated: at least one other outlet (news-retrieval's
    metadata.also_reported_by on the source article) is itself a
    wire/paper-of-record on Stage A's own domain allowlist - not just any
    also_reported_by entry, since real data shows that field can be
    entirely local TV affiliates re-syndicating the same AP wire copy
    (confirmed live: article 16514 had 25 also_reported_by entries, all
    ABC/NBC local affiliates carrying one AP story - none of that is
    independent corroboration). The allowlisted subset that actually
    made this true is stored as corroborating_domains, alongside the
    boolean - previously read-then-discarded, so a caller could see
    THAT a story was corroborated but never WHICH outlets did it.
  - primary_source: the article's own domain is on Stage A's allowlist.
    Expected to be true for nearly every row reaching Stage D, since
    Stage A already only lets allowlisted-domain articles through in the
    first place - this check exists for completeness/documentation, not
    because it meaningfully differentiates rows in practice.
  - specific: the headline itself contains a digit (a concrete number -
    percentage, dollar amount, count, etc), OR Stage C extracted at
    least one named entity from the headline and body. Either is
    evidence the story is about something concrete rather than a vague,
    unquantified claim. This read Layer 1's headline-only company match
    until that proved null on every geopolitical row - these headlines
    name countries, not companies - which left the check digit-only.

Grade: TOP (all 3 true), STRONG (exactly 2 true), STANDARD (0 or 1 true).
"""
from __future__ import annotations

import re
from typing import Any
from urllib.parse import urlparse

from pipeline.geopolitical_signal_classifier import ALLOWED_DOMAINS

_DIGIT_RE = re.compile(r"\d")


def _extract_domain(url: str | None) -> str:
    """Same domain-derivation rule as Stage A's _extract_domain - strip
    "www.", keep any other subdomain as-is (e.g. en.interfax.com.ua stays
    its own full value, matching how ALLOWED_DOMAINS itself lists it).
    """
    host = urlparse(url or "").hostname or ""
    host = host.lower()
    if host.startswith("www."):
        host = host[len("www."):]
    return host


def _corroborating_domains(also_reported_by: list[str] | None) -> list[str]:
    """Return the subset of also_reported_by entries that are themselves a
    domain on Stage A's allowlist - a real second wire/paper-of-record
    covering the same story, not just any syndicating outlet.

    Deliberately filtered, not the raw also_reported_by list verbatim:
    confirmed live that field can be entirely local TV affiliates
    re-syndicating one AP wire copy (article 16514 had 25 also_reported_by
    entries, all ABC/NBC local affiliates carrying one AP story - none of
    that is independent corroboration). Storing the unfiltered list under
    a name like "corroborating_domains" would make it look like real
    corroboration when it's actually syndication noise - the same problem
    this filter exists to avoid in the boolean, so the stored list mirrors
    the boolean's own standard exactly rather than being a separate, more
    permissive view of the same field.

    news-retrieval now writes also_reported_by as a domain derived from
    the duplicate article's own url (pipeline.py's
    _extract_domain_for_also_reported_by), same normalization as this
    function's "www." stripping - so a fresh entry is already comparable
    as-is. Confirmed live this was NOT always true: this function
    originally compared against article["source"] display names ("PBS",
    "Bloomberg Law News", "ABC News - Breaking News, Latest News and
    Videos") that news-retrieval had been storing directly, which never
    matched ALLOWED_DOMAINS and made corroborated structurally
    unreachable. The "www." stripping here stays anyway as a defensive
    no-op against any older rows still holding a pre-fix display-name
    value (those will simply still not match, same as before - this
    function does not retroactively reclassify already-graded rows).
    """
    if not also_reported_by:
        return []
    matched: list[str] = []
    seen: set[str] = set()
    for entry in also_reported_by:
        domain = (entry or "").strip().lower()
        if domain.startswith("www."):
            domain = domain[len("www."):]
        if domain in ALLOWED_DOMAINS and domain not in seen:
            matched.append(domain)
            seen.add(domain)
    return matched


def _is_specific(title: str, named_entities: list[dict[str, Any]] | None) -> bool:
    # Only checked for truthiness - shape-agnostic to whatever the
    # caller passes. Reads Stage C's extracted entities (headline AND
    # body) rather than the old headline-only company match, which was
    # null on every geopolitical row and left this check digit-only.
    return bool(_DIGIT_RE.search(title or "")) or bool(named_entities)


def grade_geopolitical_signal_article(
    *,
    url: str | None,
    title: str | None,
    also_reported_by: list[str] | None,
    named_entities: list[dict[str, Any]] | None,
) -> dict[str, Any]:
    """Return {grade, corroborated, corroborating_domains, primary_source,
    specific} for one Stage-C-tagged article. Pure/free - no model call,
    no I/O; callers fetch also_reported_by from news-retrieval before
    calling this.

    corroborating_domains is stored alongside the boolean - previously
    also_reported_by was read only to compute corroborated, then
    discarded; a caller wanting to show WHICH outlets corroborated a
    story (not just a true/false badge) had nothing to read. Empty list
    (not null) when corroborated is False, same "always the type, just
    empty" convention Stage C's tag arrays already use.
    """
    corroborating_domains = _corroborating_domains(also_reported_by)
    corroborated = bool(corroborating_domains)
    primary_source = _extract_domain(url) in ALLOWED_DOMAINS
    specific = _is_specific(title or "", named_entities)

    true_count = sum((corroborated, primary_source, specific))
    if true_count == 3:
        grade = "TOP"
    elif true_count == 2:
        grade = "STRONG"
    else:
        grade = "STANDARD"

    return {
        "grade": grade,
        "corroborated": corroborated,
        "corroborating_domains": corroborating_domains,
        "primary_source": primary_source,
        "specific": specific,
    }
