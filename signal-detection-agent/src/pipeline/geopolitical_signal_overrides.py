"""Stage C deterministic overrides - applied after Layer 2's model call,
before Layer 3's ticker expansion. Force-add categories for a few
situations judged too certain to leave to the model, per the spec.

Category names below are the real values from ALLOWED_CATEGORIES
(pipeline/categories.py) - the spec's original examples used invented
snake_case slugs (semiconductor_manufacturing, etc.) that don't correspond
to anything in research-universe's real taxonomy; corrected here to the
real display names.

Overrides only ever ADD categories to what Layer 2 already returned -
never remove or replace the model's own picks.
"""
from __future__ import annotations

import re

_TAIWAN_CONFLICT_CATEGORIES: list[str] = [
    "Semiconductor Manufacturing",
    "Compute Hardware & Edge Systems",
    "Networking, Optical & Interconnect",
    "Cloud & Compute Platforms",
]

# Matched as substrings against each country entity, because a document
# writes a state's name many ways and both rules below were silently dead
# before this: they tested `"us" in actor_terms` and `"china" in
# actor_terms` for exact equality against the model's own output, which
# is "United States" and "China" - so Rule 2 could never fire on any
# real row, and nothing reported that it wasn't firing.
_TAIWAN_TERMS = ("taiwan",)
_US_TERMS = ("united states", "u.s.", "us", "usa", "america")
_CHINA_TERMS = ("china", "prc", "beijing")

# Matched as a whole word, never as a substring: "us" and "prc" are
# short enough to appear inside unrelated country names - "us" is inside
# Russia, Belarus, Cyprus and Mauritius, so a substring test would fire
# Rule 2 on a Russia-semis row and force Semiconductor Manufacturing
# onto a story that is not US-China at all.
_WORD_TERMS = {"us", "prc", "usa", "uk"}
_WORD_SPLIT_RE = re.compile(r"[^a-z]+")


def _mentions(countries: set[str], terms: tuple[str, ...]) -> bool:
    """True when any country entity names any of ``terms``.

    Substring for the long terms, because a document writes a state's
    name many ways: "People's Republic of China", "mainland China" and
    "China" must all satisfy the China test, and "Taiwan Strait" must
    satisfy the Taiwan one. Short terms in _WORD_TERMS are matched as
    whole words instead - see the note above on "us" inside "Russia".

    Hong Kong and Macau are deliberately NOT China terms: a measure
    naming only Hong Kong is a different measure.
    """
    for country in countries:
        words = set(_WORD_SPLIT_RE.split(country))
        for term in terms:
            if term in _WORD_TERMS:
                if term in words:
                    return True
            elif term in country:
                return True
    return False


def apply_overrides(
    *,
    channel: str | None,
    entities: list[dict] | None,
    assets: list[str] | None,
    impacted_categories: list[str] | None,
) -> list[str]:
    """Return the final impacted_categories list: Layer 2's own picks plus
    any categories a matching override rule forces in, deduplicated,
    order-preserving (Layer 2's picks first, then any newly-forced ones).

    Reads the country entities rather than the former `actors` field,
    which was folded into `entities` as type "country" - one validated
    list of who is in the story instead of two, only one of which was
    checked against the article text.
    """
    result = list(impacted_categories or [])
    result_set = set(result)

    countries = {
        (e.get("name") or "").strip().lower()
        for e in (entities or [])
        if isinstance(e, dict) and e.get("type") == "country"
    }
    countries.discard("")

    # Rule 1: Taiwan/Taiwan Strait conflict -> always force the 4 categories
    # most directly exposed to a Taiwan Strait military scenario.
    if channel == "conflict" and _mentions(countries, _TAIWAN_TERMS):
        for category in _TAIWAN_CONFLICT_CATEGORIES:
            if category not in result_set:
                result.append(category)
                result_set.add(category)

    # Rule 2: US-China trade/sanctions action touching semis -> at least
    # Semiconductor Manufacturing, even if Layer 2 missed it.
    if (
        channel in ("trade", "sanctions")
        and _mentions(countries, _US_TERMS)
        and _mentions(countries, _CHINA_TERMS)
        and assets
        # "semis" is the pre-rename spelling; rows stored before the
        # vocabulary was made consistent still carry it, and this module
        # is also called on re-tags of those rows.
        and ("semiconductors" in assets or "semis" in assets)
    ):
        if "Semiconductor Manufacturing" not in result_set:
            result.append("Semiconductor Manufacturing")
            result_set.add("Semiconductor Manufacturing")

    return result
