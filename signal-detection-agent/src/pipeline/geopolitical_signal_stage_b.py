"""Stage B: single Haiku-class model call - talk or action? - for
geopolitical_signal articles that survived Stage A's free filters
(signal_detection='waiting').

One call per headline, one word out (signal/weak_signal/noise), no retry - same shape and
cost-safety reasoning as taiwan_signal_classifier.py's
_classify_gdelt_relevance: failing toward WEAK (hiding a possible signal)
is the safer default than failing toward HIGH (surfacing noise as if it
were checked). Raw HTTP via urllib.request against this service's existing
OpenAI-compatible endpoint, same as every other model call in this service
- no openai SDK dependency.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).parent.parent.parent / "prompts" / "geopolitical_signal_stage_b_v1.txt"

# Three answers, not two. HIGH and WEAK alone put a ceremonial
# proclamation, a football scandal and a routine tax-form notice in the
# same bucket as a genuine EU-China trade deal - all `weak_signal`,
# indistinguishable to anyone reading the feed.
#
# The answers ARE the stored labels. An earlier version had the model
# say HIGH/WEAK/DROP and a lookup table translate those into
# signal/weak_signal/noise, which meant two vocabularies for one set of
# outcomes and a mapping to keep in sync for no benefit. The model now
# answers with the value the row will hold.
#
# noise is the same label Stage A's rejected domains carry. The
# distinction that matters downstream: weak_signal means "we read it and
# it is commentary or rhetoric about something real", noise means "there
# is no event here".
_VALID_ANSWERS = frozenset({"signal", "weak_signal", "noise"})


def load_stage_b_prompt() -> str:
    return _PROMPT_PATH.read_text(encoding="utf-8")


# The source's own summary, where the fetcher stored one, is sent after
# the headline. Capped well below Stage C's own body limit: this call
# answers one binary question and returns a single word, so it needs
# enough text to tell an enacted measure from a proposal, not the whole
# document. A real filed abstract runs 150-1,200 characters, so this cap
# is not binding in practice - it exists so one unusually long summary
# cannot turn the cheapest call in the pipeline into the most expensive.
_MAX_SUMMARY_CHARS = 1200

# Used only when there is no source summary. Far below Stage C's 24,000:
# this call decides one binary and returns one word, and a government
# notice or news lead states what it does in its opening paragraphs. The
# operative facts - who acted, on what, in force or proposed - are at the
# top of both document types, so a larger window would add tokens on
# every row without changing many verdicts.
_MAX_BODY_CHARS = 2500


# Written only when the call itself failed, so a reader can tell a
# verdict the model reached from one it never got to make. Every other
# row carries the model's own sentence about that specific item.
_UNPARSEABLE_REASON = (
    "Not classified - the model returned an answer this pipeline could "
    "not read, so the row was defaulted to weak_signal rather than judged."
)
_CALL_FAILED_REASON = (
    "Not classified - the model call failed (network, timeout or API "
    "error), so the row was defaulted to weak_signal rather than judged."
)


def _parse_answer(content: str | None) -> tuple[str, str | None]:
    """Split "LABEL|reason" into the stored label and its reason.

    The reason is the model's own sentence about THIS item, not a
    canned string keyed off the label - an earlier version wrote
    "Not an action - commentary, a statement, a threat or a meeting"
    on every weak_signal row, which lists what the rule covers rather
    than what was found, and reads as an assumption.

    A bare label with no reason is still accepted: the verdict is the
    part that must not be lost, and a missing reason is recorded as
    absent rather than invented.
    """
    raw = (content or "").strip()
    label, _, reason = raw.partition("|")
    label = label.strip().lower()
    reason = " ".join(reason.split()) or None
    if label in _VALID_ANSWERS:
        return label, reason
    logger.warning(
        "[GEOPOLITICAL_SIGNAL_STAGE_B] unrecognized response, defaulting"
        " to weak_signal: %r", content,
    )
    return "weak_signal", _UNPARSEABLE_REASON


def classify_geopolitical_signal_headline(
    title: str,
    *,
    summary: str | None = None,
    body: str | None = None,
    system_prompt: str,
    model: str,
    api_key: str,
    base_url: str,
    timeout: int,
) -> str:
    """Return (label, reason) for one headline - the stored
    signal_detection value and the model's own sentence explaining it.

    ``summary`` is the source's own summary where news-retrieval stored
    one (Federal Register documents carry the agency's filed abstract
    there). Included because the headline alone is often the least
    informative part of the item - confirmed live on three BIS
    export-denial orders filed the same day by the same agency, whose
    titles differ only in the airline's name and which split 2 HIGH /
    1 WEAK. It costs no extra call, only extra input on a call already
    being made, and only for rows that have one.

    Strict validation, no retry: any response that is not one of the
    three literal answers (extra text, wrong case after normalization,
    an empty response, a request error) is treated as WEAK - per the
    spec's own reasoning, a repair attempt costs more than the value of
    one extra try on a call this cheap, and hiding a possible signal is
    the safer failure direction than surfacing noise as checked.
    """
    user_content = f"Headline: {title}"
    if summary:
        user_content += f"\n\nSummary: {summary[:_MAX_SUMMARY_CHARS]}"
    # Both, where both exist - this was `elif`, so a row carrying a
    # filed abstract never showed the model the document. A Federal
    # Register abstract runs ~216 characters over a ~3,700-character
    # determination and states the measure's TITLE, not what it covers:
    # "Publication of an Iran-Related Determination" says nothing about
    # the automotive and rail sectors the document actually names, and
    # that distinction is exactly what this stage's verdict turns on.
    if body:
        user_content += f"\n\nArticle text: {body[:_MAX_BODY_CHARS]}"
    payload = {
        "model": model,
        "temperature": 0,
        # One word out, with headroom for a leading space or quote the
        # model sometimes emits - at 5 a stray character could truncate
        # the answer into something unrecognised, which silently becomes
        # WEAK.
        # Enough for the label plus one sentence of reason. The reason
        # is what makes a verdict reviewable, so starving it would save
        # a trivial amount and cost the thing a reader needs.
        "max_tokens": 80,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_content},
        ],
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
        with urlopen(req, timeout=timeout) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="ignore"))
        content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
        return _parse_answer(content)
    except Exception as exc:
        logger.warning(
            "[GEOPOLITICAL_SIGNAL_STAGE_B] classification failed, defaulting"
            " to weak_signal: %s", exc,
        )
        return "weak_signal", _CALL_FAILED_REASON
