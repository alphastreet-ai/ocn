"""Stage D: attach each signal's same-event sibling coverage.

No model call, no network call - the siblings are already rows in this
service's own table, written by Stage A, and are found by a single
grouped query on metadata.event_uri.

WHAT THIS STAGE ANSWERS: "who else reported this same event, and what
did they call it". Seven outlets on one Strait of Hormuz attack is a
fact a reader uses directly - and the HEADLINE VARIANCE between them is
the part worth reading, because "projectile attacks", "naval mine
explosion" and "Iran expands attacks on shipping" are materially
different claims about one incident. That disagreement is invisible
from any single row.

WHAT IT NO LONGER DOES: grade. This stage used to compute
grade (TOP/STRONG/STANDARD) from three booleans - corroborated,
primary_source, specific. All three were measured against the real
two-source pool (75 signal rows) and all three had collapsed:

  - primary_source tested membership of Stage A's ALLOWED_DOMAINS,
    which no longer gates anything (the domain allowlist was removed
    from Stage A after it was found to drop 96% of articles). Fired on
    0 of 75 rows. Its own docstring already conceded it existed "for
    completeness, not because it differentiates".
  - corroborated matched also_reported_by entries against that same
    dead allowlist, so it was near-unreachable for the same reason.
  - specific was true when the row had ANY extracted entity, which
    Stage C now produces on 74 of 75 rows. A check that fires on 99% of
    its input sorts nothing.

So every row graded the same, and the grade carried no information.

Rebuilding the grade was considered and rejected on its own merits,
not only because the inputs were broken. A single word - TOP/STRONG/
STANDARD - compresses away the facts a reader would actually use, and
it compresses them in the wrong direction: corroboration measures
CONFIRMATION, and by the time seven outlets carry a story it is the
news everybody already has. A grade that ranks a 7-outlet event above
a 1-outlet one buries exactly the row with an information edge. The
raw count and the sibling headlines let a reader judge crowding
themselves; a grade decides it for them, wrongly.

The `grade` metadata key and its API filter are left alone rather than
removed - see update_geopolitical_signal_stage_d_grade.
"""
from __future__ import annotations

from typing import Any

# How many siblings are stored per row. The largest real cluster
# measured in this domain is 7 articles (6 siblings), so this is not
# binding today - it exists so one pathological cluster cannot write an
# unbounded list into every member's metadata.
#
# `count` is always len(siblings) AFTER this cap, never the uncapped
# total, so the two can never disagree. A consumer reading
# `corroboration.count` and then rendering `corroboration.siblings`
# must not find the second shorter than the first.
_MAX_SIBLINGS = 20


def _norm_url(url: str | None) -> str:
    """Return a url normalised enough to recognise one article twice.

    The two sources name the same article differently often enough to
    matter: a scheme, a "www." and a trailing slash are not differences
    a reader would accept as two outlets.
    """
    u = (url or "").strip().lower()
    for prefix in ("https://", "http://"):
        if u.startswith(prefix):
            u = u[len(prefix):]
            break
    if u.startswith("www."):
        u = u[len("www."):]
    return u.rstrip("/")


def build_corroboration(
    *,
    event_uri: str | None,
    row_id: int,
    cluster: list[dict[str, Any]] | None,
    also_reported_by: list[Any] | None = None,
    self_url: str | None = None,
) -> dict[str, Any]:
    """Return {count, siblings} - the OTHER articles reporting this event.

    ``cluster`` is every row sharing this row's event_uri, including
    this row itself; it is filtered out here by id, so `count` means
    "how many other outlets", not cluster size. A reader asking "who
    else has this" wants 6, not 7.

    Siblings come from TWO independent sources, merged and deduped by
    url: the event_uri cluster, and the articles news-retrieval's own
    title-similarity dedup discarded as near-duplicates of this one.
    Each sibling records which found it in `via`, because they are
    different kinds of evidence - one semantic, one lexical - and a
    surprising entry should be traceable to the mechanism that claimed
    it.

    What `via` records is how well that sibling is EVIDENCED, which is
    not the same as how it is related:

      also_reported_by  news-retrieval put this and the article at
                        >= 0.82 title similarity and, in the borderline
                        band, an LLM confirmed one event. Two
                        independent tests agreed.
      event_uri         Event Registry clustered them. Not checked here.

    Neither value means "same story" or "merely related". The lexical
    test only fires within 168 hours, against articles ALREADY STORED,
    and only when the headlines actually resemble each other - so a real
    duplicate arrives tagged event_uri whenever the two headlines share
    no words ("iPhones floating in Strait of Hormuz" vs "US says
    container ship hit by projectile"), both articles land in one batch,
    or the embedding failed. The stored distribution says no threshold
    fixes this: a confirmed 9-headline EU sanctions cluster scored
    0.5951-0.8177 pairwise, fully overlapping the 0.61-0.72 range of two
    confirmed DIFFERENT Canada-tariff stories.

    An article with neither source, or whose sources name no other
    article, returns {"count": 0, "siblings": []} rather than nothing.
    The explicit zero is the point: an ABSENT field cannot be told
    apart from a stage that failed or never ran, while a zero says "we
    looked for other coverage and there is none" - which is a real
    finding, and for a reader often the more interesting one, since it
    means the story is not yet crowded.

    That distinction also has to survive Federal Register rows, which
    carry no event_uri because a filed determination has no "other
    outlets" at all. Those are a different case again - not "checked
    and alone" - and keeping the zero explicit here is what leaves room
    to say so.
    """
    siblings: list[dict[str, Any]] = []
    seen = {_norm_url(self_url)} - {""}

    # Source 1: Event Registry's event_uri cluster. Semantic - it groups
    # articles describing one event even when the headlines share no
    # words ("iPhones floating in Strait of Hormuz" and "US says
    # container ship hit by projectile" are one event, zero overlapping
    # terms). This is why it finds more than the lexical dedup below.
    for member in cluster or []:
        if member.get("id") == row_id:
            continue
        url = member.get("url") or ""
        key = _norm_url(url)
        if key and key in seen:
            continue
        if key:
            seen.add(key)
        siblings.append({
            # The outlet's own domain, which is what identifies the
            # source to a reader.
            "outlet": member.get("outlet") or member.get("source") or "",
            # The headline VERBATIM. Never rewritten or collapsed by
            # similarity - near-identical wording across two outlets is
            # itself information (syndication), and so is divergence.
            "title": member.get("title") or "",
            "url": url,
            "via": "event_uri",
        })

    # Source 2: news-retrieval's own title-similarity dedup, which
    # discarded these as near-duplicates of this article. Lexical, so it
    # mostly catches syndication - the same wire copy under near-
    # identical headlines - which is exactly the shape Event Registry's
    # semantic clustering is least likely to be the only witness to.
    #
    # Not a subset of source 1. Measured over 87 real signals: event_uri
    # covered 48 rows, this covered 30, and they overlapped on 20. Ten
    # rows had only this, and Stage D reported zero corroboration for
    # them while the duplicates sat on the row.
    #
    # Entries written before this field carried headlines are bare
    # domain strings; they are read too, with empty title and url,
    # because an outlet with no headline still counts as coverage.
    for entry in also_reported_by or []:
        if isinstance(entry, dict):
            outlet, title, url = (
                entry.get("outlet") or "", entry.get("title") or "",
                entry.get("url") or "",
            )
        else:
            outlet, title, url = (str(entry or "").strip(), "", "")
        if not outlet and not url:
            continue
        # Deduped against the cluster by url, because one article can
        # legitimately be found by both mechanisms - 20 of 87 rows carry
        # both - and counting it twice would overstate the coverage.
        # With no url to compare (an old string entry), the outlet name
        # stands in.
        key = _norm_url(url) or f"outlet:{outlet.lower()}"
        if key in seen:
            continue
        seen.add(key)
        siblings.append({
            "outlet": outlet, "title": title, "url": url,
            "via": "also_reported_by",
        })

    siblings = siblings[:_MAX_SIBLINGS]
    # Counted AFTER the cap, from the stored list itself, so `count` can
    # never disagree with what a reader can see: it is always exactly
    # len(siblings).
    #
    # ONE count, not a breakdown by `via`. A same_story/same_event split
    # was built here and removed: it read as a partition ("2 are
    # duplicates, 5 are merely related") that the mechanisms do not
    # support. also_reported_by only fires on >= 0.82 title similarity,
    # within 168 hours, against articles ALREADY STORED - so a genuine
    # duplicate lands in the event_uri half instead whenever the two
    # headlines share no words, both arrive in one batch, or the
    # embedding failed. The difference between the two is how well the
    # sibling is EVIDENCED, not whether it is the same story, and a
    # pair of counts cannot say that without implying the opposite.
    # `via` on each sibling carries it honestly, per sibling.
    return {"count": len(siblings), "siblings": siblings}
