"""Clinical intent labels used before deterministic filter enrichment."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from typing import Optional


class ClinicalIntent(str, Enum):
    POSITIVE_EXISTENCE = "positive_existence"
    NEGATIVE_EXISTENCE = "negative_existence"
    RECORD_EXISTS = "record_exists"
    ANY_STATE = "any_state"
    UNKNOWN_STATE = "unknown_state"
    STATE_UNSPECIFIED = "state_unspecified"
    CURRENT_POSITIVE = "current_positive"
    HISTORICAL_POSITIVE = "historical_positive"


@dataclass(frozen=True)
class IntentResult:
    intent: ClinicalIntent
    source: str
    reason: str = ""


_ANY_STATE = re.compile(
    r"\b(?:regardless\s+of|any)\s+(?:state|status)\b"
    r"|\b(?:all|any)\s+(?:states|statuses)\b"
    r"|\bpresence\s+or\s+absence\b",
    re.I,
)
_UNKNOWN_STATE = re.compile(
    r"\bunknown\b[\w\s-]{0,35}\b(?:state|status)\b"
    r"|\b(?:state|status)\b[\w\s-]{0,35}\bunknown\b",
    re.I,
)
_RECORD_EXISTS = re.compile(
    r"\b(?:assessed\s+for|assessment\s+records?|records?\s+of|"
    r"underwent\s+(?:an?\s+)?assessment)\b"
    r"|\bassessments?\s+(?:has|have)\s+been\s+classified\b",
    re.I,
)
_NEGATIVE = re.compile(
    r"\b(?:without|absence\s+of|absent|negative\s+for|no\s+evidence\s+of)\b"
    r"|\b(?:do|does|did)\s+not\s+exhibit\b"
    r"|\bno\b[\w\s-]{0,25}\b(?:tumou?rs?|disease|finding|findings)\b",
    re.I,
)
_STATUS = re.compile(r"\b(?:state|status)\b", re.I)
_POSITIVE = re.compile(
    r"\b(?:with|has|have|having)\b"
    r"|\bexhibits?(?:ed|ing)?\b"
    r"|\b(?:located|found|seen|detected|present)\s+(?:at|on|in)\b"
    r"|\b(?:positive\s+for|evidence\s+of|presence\s+of|detected)\b",
    re.I,
)
_CURRENT = re.compile(r"\b(?:current|currently|active)\b", re.I)
_HISTORICAL = re.compile(r"\b(?:history\s+of|historical|previous|prior|past)\b", re.I)


# How far from a clause a cue may sit and still describe it.
_CUE_WINDOW = 60

# Cue -> intent, most specific first. `infer_intent` walks this in order and
# takes the first hit; `infer_intent_near` takes the hit closest to a span and
# uses this order only to break ties.
_CUES = (
    (ClinicalIntent.ANY_STATE, _ANY_STATE, "any state cue"),
    (ClinicalIntent.UNKNOWN_STATE, _UNKNOWN_STATE, "unknown status cue"),
    (ClinicalIntent.NEGATIVE_EXISTENCE, _NEGATIVE, "negative existence cue"),
    (ClinicalIntent.RECORD_EXISTS, _RECORD_EXISTS, "assessment/record cue"),
    (ClinicalIntent.STATE_UNSPECIFIED, _STATUS,
     "status mentioned without a concrete state"),
    (ClinicalIntent.CURRENT_POSITIVE, _CURRENT, "current positive cue"),
    (ClinicalIntent.HISTORICAL_POSITIVE, _HISTORICAL, "historical positive cue"),
    (ClinicalIntent.POSITIVE_EXISTENCE, _POSITIVE, "positive existence cue"),
)


def normalize_intent(value: Optional[str]) -> Optional[ClinicalIntent]:
    if not value:
        return None
    try:
        return ClinicalIntent(str(value).strip())
    except ValueError:
        return None


def infer_intent(text: str, *, model_intent: Optional[str] = None) -> IntentResult:
    """Infer a bounded clinical intent label.

    Strong deterministic cues win over the model. The model is useful when the
    text has no clear cue, but it should not be able to turn "without" into
    state_unspecified or "assessed for" into positive existence.
    """
    text = text or ""

    for intent, pattern, reason in _CUES:
        if pattern.search(text):
            if intent in (
                ClinicalIntent.CURRENT_POSITIVE,
                ClinicalIntent.HISTORICAL_POSITIVE,
            ) and not _POSITIVE.search(text):
                continue
            return IntentResult(intent, "heuristic", reason)

    explicit = normalize_intent(model_intent)
    if explicit is not None:
        return IntentResult(explicit, "model")

    return IntentResult(
        ClinicalIntent.STATE_UNSPECIFIED,
        "heuristic",
        "no reliable existence/state cue",
    )


def _gap(a: tuple[int, int], b: tuple[int, int]) -> int:
    """Edge-to-edge character gap between two spans (0 if they overlap)."""
    if a[1] <= b[0]:
        return b[0] - a[1]
    if b[1] <= a[0]:
        return a[0] - b[1]
    return 0


def infer_intent_near(
    text: str,
    span: tuple[int, int],
    *,
    window: int = _CUE_WINDOW,
    model_intent: Optional[str] = None,
) -> IntentResult:
    """Infer the intent for one part of the query, from the cue nearest `span`.

    A cohort query routinely mixes polarities -- "with metastatic tumors who do
    not exhibit MYCN amplification" asserts one finding and denies another --
    so a single label for the whole sentence is wrong for at least one of them.
    Each nested block is resolved against the cue closest to the text it came
    from; ties fall back to `_CUES` order, and a span with no cue within
    `window` falls back to reading the whole query.
    """
    text = text or ""
    best: Optional[tuple[int, int, ClinicalIntent, str]] = None

    for rank, (intent, pattern, reason) in enumerate(_CUES):
        for match in pattern.finditer(text):
            distance = _gap(match.span(), span)
            if distance > window:
                continue
            if intent in (
                ClinicalIntent.CURRENT_POSITIVE,
                ClinicalIntent.HISTORICAL_POSITIVE,
            ) and not _POSITIVE.search(text):
                continue
            candidate = (distance, rank, intent, reason)
            if best is None or candidate[:2] < best[:2]:
                best = candidate

    if best is None:
        return infer_intent(text, model_intent=model_intent)

    _distance, _rank, intent, reason = best
    return IntentResult(intent, "heuristic", f"{reason} (nearest cue)")


__all__ = [
    "ClinicalIntent",
    "IntentResult",
    "infer_intent",
    "infer_intent_near",
    "normalize_intent",
]
