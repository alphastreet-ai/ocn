"""Stage E: group signals that belong to one developing STORY.

No model call, no network call - the inputs are entities Stage C
already extracted and verified against the article text.

WHAT THIS STAGE ANSWERS: "is this the same situation I just read
about". Stage D answers a narrower question - who else reported this
same EVENT - and the two must not be confused. Four Strait of Hormuz
items in one morning can be four genuinely different events (a mine
strike, a blockade, a sanctions announcement, a suspension of raids)
and still read as one story to anyone scrolling the feed.

WHY IT GROUPS AND NEVER DELETES. Stage B collapses rows reporting the
SAME event, which is safe: the deleted article's headline survives on
the judged row and nothing unique is lost. A story is different. The
six rows of one Hormuz story in the 2026-10-17 run each reported a
DIFFERENT state action - a Treasury designation, an IRGC attack, a
suspension of US raids, a blockade, a mine strike. Delete five to keep
one and a reader querying "sanctions actions this week" no longer
finds the Treasury announcement.

So the seed row carries metadata.story_development and the others
carry metadata.in_story pointing back at it. One row per story is a
FILTER, not a deletion:

    signal_detection = 'signal' AND NOT (metadata ? 'in_story')

That also makes a wrong grouping one field away from undone, which
matters while the key is still being proven - measured over 57 real
signals, 2 of 9 groups were wrong, both because an ISSUING INSTITUTION
was read as story identity (OFAC issues both a horse-racing
designation and a Lukoil licence; UKMTO reports every maritime
incident). Those names are in _GENERIC below, and the class of error
is why this stage marks rather than deletes.
"""
from __future__ import annotations

import re
from typing import Any

# How far apart two items can be and still be one story. A running
# story produces new developments for days; past three days the same
# subject recurring is a new story, not a continuation.
_WINDOW_HOURS = 72

# Two rows must share at least this many entity keys. One shared name
# is coincidence - half these articles name the United States.
_MIN_SHARED = 2

# A story needs at least this many members to be worth grouping. A
# "story" of one is just a signal.
_MIN_STORY = 2

# Names that can never carry story identity ON THEIR OWN. A country
# appears across unrelated events - "Iran" is on a third of these
# signals - so a pair sharing only countries is a TOPIC match, which
# is what merged a Yemen sanctions item into a shipping story before
# this list existed.
_COUNTRIES = frozenset({
    "united states", "america", "usa", "russia", "iran", "china",
    "israel", "ukraine", "india", "japan", "european union", "europe",
    "yemen", "saudi arabia", "iraq", "syria", "cuba", "venezuela",
    "nigeria", "taiwan", "korea", "north korea", "south korea",
    "canada", "mexico", "brazil", "turkey", "egypt", "haiti",
    "eritrea", "bolivia", "kazakhstan", "malaysia", "thailand",
    "indonesia", "united kingdom", "britain", "france", "germany",
    "netherlands", "belgium", "pakistan", "afghanistan",
})

# Commodities, assets and generic nouns. A traded good appears across
# every unrelated story in its sector - "crude oil" joined a Russian
# sanctions item to a Strait of Hormuz incident, which share nothing
# but the commodity. Like a country, these are admissible as
# supporting evidence and never as the whole of it.
_COMMODITIES = frozenset({
    "crude oil", "oil", "petroleum", "natural gas", "gas", "lng",
    "diesel", "gasoline", "fuel", "uranium", "gold", "copper",
    "rare earth", "rare earths", "critical minerals", "semiconductor",
    "semiconductors", "chips", "wheat", "grain", "fertilizer",
    "electricity", "power", "crude",
})

# Institutions that ISSUE or REPORT across unrelated stories. Measured:
# OFAC appeared in a horse-racing cartel designation and a Lukoil
# general licence, and grouping on it merged two unconnected sanctions
# stories. UKMTO reports every maritime incident in the region. The
# agency that announces a thing is not what the thing is about.
_GENERIC = frozenset({
    "donald trump", "trump", "white house", "government", "congress",
    "senate", "house representatives", "treasury", "state",
    "foreign assets control", "ofac", "fac", "central command",
    "centcom", "ukmto", "united kingdom maritime", "pentagon",
    "european commission", "european council", "united nations",
    "security council", "nato", "opec", "data centers", "data center",
    "reuters", "bloomberg", "associated press", "axios",
    # Armed forces and security services. A country's military is
    # named in every story about that country's actions - the IRGC
    # appeared in a Strait of Hormuz mine strike and in a Houthi
    # attack on a Yemeni palace, two unconnected events. Like the
    # country itself, the armed force is the actor in all of them.
    "islamic revolutionary guard", "irgc", "revolutionary guard",
    "houthis", "hezbollah", "israel defense forces", "idf",
    "russian armed forces", "peoples liberation army", "pla",
    "armed forces", "navy", "army", "air force",
})

_SUFFIX_RE = re.compile(
    r"\b(company|companies|corporation|corp|limited|ltd|plc|inc|"
    r"incorporated|llc|group|holdings|authority|agency|commission|"
    r"department|ministry|board|council|bureau|office|organization|"
    r"organisation|association|the|of|and)\b", re.I)

_PAREN_RE = re.compile(r"\(([^)]{2,12})\)")
_NONWORD_RE = re.compile(r"[^a-z0-9 ]")
# An all-consonant short token is an acronym, not a word: NNPC, TSMC,
# IRGC. Requiring no vowel keeps ordinary words ("oil", "gas", "bank")
# from becoming join keys.
_VOWEL_RE = re.compile(r"[aeiou]")


def _entity_keys(name: str) -> set[str]:
    """Every form a name should match on.

    One company is written several ways across outlets - "Nigerian
    National Petroleum Corporation (NNPC)" and "NNPC Gas
    Infrastructure Company Limited" are the same operator and share no
    exact string. Both a normalised head and any acronym are returned,
    so the two meet on the acronym.
    """
    raw = (name or "").lower().strip()
    if not raw:
        return set()
    keys: set[str] = set()

    # An acronym the source spelled out in parentheses.
    for acro in _PAREN_RE.findall(raw):
        a = _NONWORD_RE.sub("", acro)
        if len(a) >= 3:
            keys.add(a)

    stripped = _PAREN_RE.sub(" ", raw)
    stripped = _SUFFIX_RE.sub(" ", stripped)
    stripped = _NONWORD_RE.sub(" ", stripped)
    tokens = [t for t in stripped.split() if len(t) > 2]
    if tokens:
        # The distinctive head: enough to identify, short enough that a
        # longer legal name still matches a shorter trading name.
        keys.add(" ".join(tokens[:3]))
        if len(tokens) >= 3:
            keys.add("".join(t[0] for t in tokens[:5]))
        # A bare acronym USED AS A WORD inside the name. One operator's
        # subsidiaries are written both ways - "Nigerian National
        # Petroleum Corporation (NNPC)" and "NNPC Gas Infrastructure
        # Company Limited" are the same group, and without this they
        # share nothing: the first yields "nnpc" from its parentheses,
        # the second buries NNPC in its head. Length-capped so an
        # ordinary short word cannot become a join key.
        for t in tokens:
            if 3 <= len(t) <= 6 and not _VOWEL_RE.search(t):
                keys.add(t)

    return {k for k in keys if k and k not in _GENERIC}


def _is_specific(key: str) -> bool:
    """True when this key can identify a story by itself.

    Three classes cannot: a country (half these articles name the
    United States), an issuing or reporting institution (OFAC signs
    unrelated designations), and a commodity (every oil story names
    crude). Each appears across unrelated events, so a pair sharing
    only these matched on a TOPIC, not on a story.
    """
    return (key not in _COUNTRIES and key not in _GENERIC
            and key not in _COMMODITIES)


def _keys_for_row(row: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for e in row.get("entities") or []:
        keys |= _entity_keys(e.get("name") or "")
    return keys


def _hours_apart(a: Any, b: Any) -> float | None:
    if not a or not b:
        return None
    try:
        return abs((b - a).total_seconds()) / 3600.0
    except (TypeError, AttributeError):
        return None


def _linked(seed: dict[str, Any], other: dict[str, Any]) -> set[str]:
    """The shared keys IF these two belong to one story, else empty.

    Returns the evidence rather than a boolean so the caller can record
    WHY two rows were grouped - a grouping a reader cannot interrogate
    is one they cannot correct.
    """
    shared = seed["_keys"] & other["_keys"]
    if len(shared) < _MIN_SHARED:
        return set()
    # At least one shared name must be specific. Two countries in
    # common is a topic, not a story.
    if not any(_is_specific(k) for k in shared):
        return set()
    gap = _hours_apart(seed.get("published"), other.get("published"))
    if gap is not None and gap > _WINDOW_HOURS:
        return set()
    return shared


def build_stories(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group signal rows into stories. Returns one dict per story with
    two or more members; singletons are not stories and are omitted.

    Each row needs: id, title, url, published, entities (parsed), and
    metadata. Seeded rather than transitive: a row joins a story only
    by matching that story's SEED, never by matching a member that
    itself only just qualified. Union-find was tried and chained two
    unrelated stories through one weak edge - the seed rule is what
    stops a single bad link welding them together.

    The seed is the best-corroborated member, falling back to the
    earliest. That row is the one a reader lands on, so it should be
    the one other outlets confirmed, not an arbitrary first.
    """
    for r in rows:
        r["_keys"] = _keys_for_row(r)

    ordered = sorted(
        rows,
        key=lambda r: (
            -int(((r.get("metadata") or {}).get("corroboration") or {})
                 .get("count") or 0),
            r.get("published") or 0,
        ),
    )

    stories: list[dict[str, Any]] = []
    for row in ordered:
        for story in stories:
            shared = _linked(story["seed"], row)
            if shared:
                story["members"].append({"row": row, "shared": shared})
                break
        else:
            stories.append({"seed": row, "members": []})

    return [s for s in stories if len(s["members"]) + 1 >= _MIN_STORY]


def story_development(story: dict[str, Any]) -> dict[str, Any]:
    """The field written to the seed row.

    Each member carries its own title, url, time, channel and reason -
    the developments inside a story are DIFFERENT events, and a reader
    opening the story needs to see what each one was. This is the
    difference from Stage D's siblings, which are the same event told
    by several outlets and therefore need only an outlet and a
    headline.

    `shared` records which entities put each member here, so a reader
    who thinks a grouping is wrong can see the reason rather than
    guess at it.
    """
    members = []
    for m in sorted(story["members"],
                    key=lambda m: m["row"].get("published") or 0):
        r = m["row"]
        meta = r.get("metadata") or {}
        members.append({
            "id": r["id"],
            "title": (r.get("title") or "").strip(),
            "url": r.get("url") or "",
            "published": (r["published"].isoformat()
                          if r.get("published") else None),
            "channel": meta.get("channel"),
            "reason": r.get("signal_reason"),
            # Stage C already wrote this on the member's own row, so it
            # is a COPY and the row stays the source of truth: a
            # consumer that needs the current text joins on `id`. It is
            # carried here so a story card can show what each
            # development was without a second query, which is the one
            # thing `reason` does not do - `reason` says why the item is
            # a signal, the summary says what happened in it.
            # NULL where the member has none; Stage C writes a summary
            # only where the source published one or the model could
            # generate one from the body.
            "summary": meta.get("summary"),
            "shared": sorted(m["shared"]),
        })
    return {"count": len(members), "members": members}
