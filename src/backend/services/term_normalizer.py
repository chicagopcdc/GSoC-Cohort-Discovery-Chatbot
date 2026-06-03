"""
Normalize raw user queries into structured filter hints.

This module extracts three kinds of information from a user query:

- schema enum values, matched exactly against known schema values
- numeric range constraints, such as "older than 5" or "between 5 and 10"
- simple negations, such as "not", "without", or "excluding"

The output is not a final GraphQL filter. It is an intermediate representation
used by later query-building steps.

Matching is intentionally conservative: exact enum phrases and curated synonyms
are handled here, while fuzzy matching and typo handling are left to the
retrieval layer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import yaml

from services.schema_loader import SchemaIndex


@dataclass(frozen=True)
class FieldPlacement:
    field: str
    path: Optional[str]           # None means the field is on the top-level subject.


@dataclass(frozen=True)
class RecognizedTerm:
    value: str                    # Canonical schema value, e.g. "Not Reported".
    placements: tuple[FieldPlacement, ...]   # All possible field/path placements.
    span: tuple[int, int]         # Character offsets in the original query text.
    negated: bool = False


@dataclass(frozen=True)
class NumericConstraint:
    op: str                       # gt | gte | lt | lte
    value: float
    unit: Optional[str]           # years | months | weeks | days | None
    quantity: Optional[str]       # "age" when it can be inferred, otherwise None.
    span: tuple[int, int]
    negated: bool = False         # The operator is already flipped when needed.


@dataclass
class NormalizedQuery:
    text: str
    terms: list[RecognizedTerm]
    ranges: list[NumericConstraint]
    negations: list[str]

    def to_dict(self) -> dict:
        return {
            "recognized_terms": [
                {
                    "value": t.value,
                    "placements": [
                        {"field": p.field, "path": p.path} for p in t.placements
                    ],
                    "negated": t.negated,
                    "span" : list(t.span),
                }
                for t in self.terms
            ],
            "ranges": [
                {
                    "op": r.op,
                    "value": r.value,
                    "unit": r.unit,
                    "quantity": r.quantity,
                    "negated": r.negated,
                    "span": list(r.span),
                }
                for r in self.ranges
            ],
            "negations": list(self.negations),
        }


# Treat nearby negation words as applying to the next matched term or range.
# The lookback window stays small to avoid linking unrelated phrases.
_NEGATION_CUES = {"not", "no", "without", "excluding", "non", "never"}
_NEG_LOOKBACK = 3

_FLIP = {"gt": "lte", "lte": "gt", "lt": "gte", "gte": "lt"}

# Trim only sentence punctuation from token edges so "skin." can match "Skin".
# Other punctuation is kept because some enum values contain characters such as
# parentheses, hyphens, percent signs, or commas.
_EDGE_PUNCT = ".,;:!?\"'"

_WORD = re.compile(r"\S+")

_NUM = r"(\d+(?:\.\d+)?)"
_UNIT = r"(?:\s+(years?|yrs?|months?|mos?|weeks?|days?))?"
_AGE_UNITS = {"years", "months", "weeks", "days"}

# Common ways users express numeric comparisons.
# Some phrases, such as "older than", imply an age comparison directly.
# Generic comparisons rely on the unit to infer the quantity when possible.
_RANGE_PATTERNS = [
    (re.compile(rf"\bolder than\s+{_NUM}{_UNIT}", re.I), "gt", "age"),
    (re.compile(rf"\byounger than\s+{_NUM}{_UNIT}", re.I), "lt", "age"),
    (re.compile(rf"\b(?:greater than|more than|over|above)\s+{_NUM}{_UNIT}", re.I), "gt", None),
    (re.compile(rf"\b(?:less than|under|below)\s+{_NUM}{_UNIT}", re.I), "lt", None),
    (re.compile(rf"\bat least\s+{_NUM}{_UNIT}", re.I), "gte", None),
    (re.compile(rf"\b(?:at most|up to|no more than)\s+{_NUM}{_UNIT}", re.I), "lte", None),
    (re.compile(rf"{_NUM}{_UNIT}\s+or older\b", re.I), "gte", "age"),
    (re.compile(rf"{_NUM}{_UNIT}\s+or younger\b", re.I), "lte", "age"),
]
_BETWEEN = re.compile(rf"\bbetween\s+{_NUM}\s+and\s+{_NUM}{_UNIT}", re.I)
_SYMBOLIC = re.compile(rf"([<>]=?)\s*{_NUM}{_UNIT}")
_SYMBOL_OP = {">": "gt", ">=": "gte", "<": "lt", "<=": "lte"}

_UNIT_CANON = {
    "year": "years", "years": "years", "yr": "years", "yrs": "years",
    "month": "months", "months": "months", "mo": "months", "mos": "months",
    "week": "weeks", "weeks": "weeks",
    "day": "days", "days": "days",
}


def _canon_unit(raw: Optional[str]) -> Optional[str]:
    return _UNIT_CANON.get(raw.lower()) if raw else None


def _quantity(unit: Optional[str], phrase_hint: Optional[str]) -> Optional[str]:
    if phrase_hint:
        return phrase_hint
    if unit in _AGE_UNITS:
        return "age"
    return None


def _tokenize(text: str) -> list[tuple[str, int, int]]:
    """Tokenize text while trimming edge punctuation and skipping punctuation-only tokens"""
    out: list[tuple[str, int, int]] = []
    for m in _WORD.finditer(text):
        raw = m.group()
        core = raw.strip(_EDGE_PUNCT)
        if not core or not any(ch.isalnum() for ch in core):
            continue
        offset = raw.find(core)
        start = m.start() + offset
        out.append((core.lower(), start, start + len(core)))
    return out


def _norm_phrase(s: str) -> str:
    """Normalize an index phrase the same way user input is tokenized."""
    return " ".join(w for w, _, _ in _tokenize(s))


def _negation_before(text: str, span_start: int) -> Optional[int]:
    """Return the start offset of a nearby negation cue, if one exists."""
    befores = [
        (m.group().lower(), m.start())
        for m in re.finditer(r"[a-zA-Z]+", text[:span_start])
    ]

    for word, pos in reversed(befores[-_NEG_LOOKBACK:]):
        if word in _NEGATION_CUES:
            return pos

    return None


def load_synonyms(path: Union[str, Path]) -> dict[str, str]:
    p = Path(path)

    if not p.exists():
        return {}

    # BaseLoader keeps values as strings, which avoids YAML converting words
    # like "No", "Yes", "On", or "Off" into booleans.
    data = yaml.load(p.read_text(encoding="utf-8"), Loader=yaml.BaseLoader) or {}

    return {str(k).strip().lower(): str(v) for k, v in data.items()}


class TermNormalizer:
    def __init__(self, schema: SchemaIndex, synonyms: Optional[dict[str, str]] = None):
        self._schema = schema

        # normalized phrase -> (canonical value, possible field placements)
        self._phrases: dict[str, tuple[str, tuple[FieldPlacement, ...]]] = {}
        self._max_words = 1

        self._build_index(synonyms or {})

    @classmethod
    def from_files(
        cls,
        schema: SchemaIndex,
        synonyms_path: Optional[Union[str, Path]] = None,
    ) -> "TermNormalizer":
        syn = load_synonyms(synonyms_path) if synonyms_path else {}
        return cls(schema, syn)

    def _placements_for(self, value: str) -> tuple[FieldPlacement, ...]:
        # A schema value can appear under more than one field or nested path.
        # Keep each valid placement so downstream code can decide where it fits.
        out: list[FieldPlacement] = []

        for name in self._schema.fields_containing_value(value):
            for spec in self._schema.get_fields(name):
                if value in spec.enum_values:
                    out.append(FieldPlacement(spec.name, spec.parent_path))

        return tuple(out)

    def _build_index(self, synonyms: dict[str, str]) -> None:
        # Add canonical enum values from the schema.
        for spec in self._schema.all_fields():
            for value in spec.enum_values:
                self._add(value, value)

        # Add curated user-facing phrases that map to canonical schema values.
        for surface, canonical in synonyms.items():
            self._add(surface, canonical)

    def _add(self, surface: str, canonical: str) -> None:
        key = _norm_phrase(surface)

        if not key:
            return

        placements = self._placements_for(canonical)

        # If the same phrase appears more than once, keep the broader mapping.
        existing = self._phrases.get(key)
        if existing is None or len(placements) > len(existing[1]):
            self._phrases[key] = (canonical, placements)

        self._max_words = max(self._max_words, len(key.split()))

    def normalize(self, text: str) -> NormalizedQuery:
        negations: list[str] = []

        terms = self._match_terms(text, negations)
        ranges = self._extract_ranges(text, negations)

        return NormalizedQuery(text=text, terms=terms, ranges=ranges, negations=negations)

    def _match_terms(self, text: str, negations: list[str]) -> list[RecognizedTerm]:
        tokens = _tokenize(text)
        out: list[RecognizedTerm] = []

        i, n = 0, len(tokens)

        while i < n:
            # Prefer the longest phrase so multi-word enum values stay intact.
            hit = None
            upper = min(self._max_words, n - i)

            for k in range(upper, 0, -1):
                phrase = " ".join(w for w, _, _ in tokens[i:i + k])
                entry = self._phrases.get(phrase)

                if entry is not None:
                    hit = (entry, tokens[i][1], tokens[i + k - 1][2], k)
                    break

            if hit is None:
                i += 1
                continue

            (value, placements), start, end, width = hit
            cue = _negation_before(text, start)
            negated = cue is not None

            if negated:
                negations.append(text[cue:end].strip())

            out.append(RecognizedTerm(value, placements, (start, end), negated))
            i += width

        return out

    def _extract_ranges(self, text: str, negations: list[str]) -> list[NumericConstraint]:
        raw: list[tuple[str, float, Optional[str], Optional[str], int, int]] = []

        # "between X and Y" becomes two constraints: >= X and <= Y.
        for m in _BETWEEN.finditer(text):
            unit = _canon_unit(m.group(3))
            q = _quantity(unit, None)

            raw.append(("gte", float(m.group(1)), unit, q, m.start(), m.end()))
            raw.append(("lte", float(m.group(2)), unit, q, m.start(), m.end()))

        for pattern, op, hint in _RANGE_PATTERNS:
            for m in pattern.finditer(text):
                unit = _canon_unit(m.group(2))
                raw.append(
                    (
                        op,
                        float(m.group(1)),
                        unit,
                        _quantity(unit, hint),
                        m.start(),
                        m.end(),
                    )
                )

        for m in _SYMBOLIC.finditer(text):
            unit = _canon_unit(m.group(3))
            raw.append(
                (
                    _SYMBOL_OP[m.group(1)],
                    float(m.group(2)),
                    unit,
                    _quantity(unit, None),
                    m.start(),
                    m.end(),
                )
            )

        out: list[NumericConstraint] = []
        seen: set = set()

        for op, value, unit, quantity, start, end in raw:
            key = (op, value, start)

            if key in seen:
                continue

            seen.add(key)

            cue = _negation_before(text, start)

            if cue is not None:
                # Example: "not older than 5" becomes "<= 5".
                negations.append(text[cue:end].strip())
                out.append(
                    NumericConstraint(
                        _FLIP[op],
                        value,
                        unit,
                        quantity,
                        (start, end),
                        True,
                    )
                )
            else:
                out.append(
                    NumericConstraint(
                        op,
                        value,
                        unit,
                        quantity,
                        (start, end),
                        False,
                    )
                )

        return out
    
    
#this is the v1 version
