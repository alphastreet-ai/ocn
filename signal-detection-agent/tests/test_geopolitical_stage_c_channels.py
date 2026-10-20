"""Stage C channel vocabulary.

`subsidies` and `regulation` were added 2026-10-20: of 57 HIGH rows, 11
had been forced into a channel that misdescribed them (state chip
schemes filed as "trade", data-centre moratoriums as "energy"). A value
outside the vocabulary is stored as null, so a new channel that the
validator does not know would be silently erased on every row.
"""
from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from pipeline.geopolitical_signal_layer2 import _VALID_CHANNELS, _validate  # noqa: E402


def test_new_channels_are_kept() -> None:
    for channel in ("subsidies", "regulation", "diplomacy"):
        assert channel in _VALID_CHANNELS
        out = _validate({"channel": channel, "assets": [], "impacted_categories": [], "entities": []})
        assert out is not None and out["channel"] == channel


def test_unknown_channel_is_still_null() -> None:
    out = _validate({"channel": "industrial_policy", "assets": [], "impacted_categories": [], "entities": []})
    assert out is not None and out["channel"] is None


def test_channel_spelling_variants_are_normalised() -> None:
    for raw, want in (("Sanctions", "sanctions"), (" trade ", "trade"), ("subsidy", "subsidies"),
                      ("Regulatory", "regulation"), ("tariffs", "trade")):
        out = _validate({"channel": raw, "assets": [], "impacted_categories": [], "entities": []})
        assert out is not None and out["channel"] == want, raw


def test_non_string_channel_is_null() -> None:
    out = _validate({"channel": ["trade"], "assets": [], "impacted_categories": [], "entities": []})
    assert out is not None and out["channel"] is None


def test_null_channel_is_a_valid_answer() -> None:
    # The prompt allows null for an article with no instrument; a string
    # spelling of it is the same answer, not an unknown value.
    for raw in (None, "none", "null", "None"):
        out = _validate({"channel": raw, "assets": [], "impacted_categories": [], "entities": []})
        assert out is not None and out["channel"] is None
