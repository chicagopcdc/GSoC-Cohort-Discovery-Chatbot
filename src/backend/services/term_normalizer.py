"""
Normalize raw user queries into structured filter hints.

This module extracts, from natural-language cohort requests:

- schema enum values, matched against known schema values and synonyms, then
  disambiguated to a precise field when the surrounding words name one
  ("race was not reported" -> the race field, not every field with that value)
- "not reported / unknown / not available" phrasings that carry no literal enum
  token ("no race information reported" -> race = its missing-data value)
- numeric range constraints in many phrasings ("older than 5", "before age 5",
  "at least three months", "between 5 and 10"), including English number words
- simple negations ("not", "without", "excluding")

Age ranges get routing + unit conversion, delegated to NumericFieldResolver,
which derives its rules from the schema (see services/numeric_resolver.py).

The output is not a final GraphQL filter. It is an intermediate representation
used by later query-building steps. Matching stays conservative: exact enum
phrases, curated synonyms, and high-confidence patterns are handled here, while
fuzzy matching is left to the retrieval layer.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import yaml

from services.numeric_resolver import NumericConfig, NumericFieldResolver
from services.schema_loader import SchemaIndex


@dataclass(frozen=True)
class FieldPlacement:
    field: str
    path: Optional[str]           # None means the field is on the top-level subject.


@dataclass(frozen=True)
class RecognizedTerm:
    value: str                    # Canonical schema value, e.g. "Not Reported".
    placements: tuple[FieldPlacement, ...]   # Field/path placements after disambiguation.
    span: tuple[int, int]         # Character offsets in the original query text.
    negated: bool = False


@dataclass(frozen=True)
class NumericConstraint:
    op: str                       # gt | gte | lt | lte
    value: float                  # In the bound field's stored unit (days for age).
    unit: Optional[str]           # Unit of `value`: "days" once converted, else raw.
    quantity: Optional[str]       # "age" when it can be inferred, otherwise None.
    span: tuple[int, int]
    negated: bool = False         # The operator is already flipped when needed.
    field: Optional[str] = None   # resolved schema field, e.g. age_at_censor_status
    path: Optional[str] = None    # parent path of that field; None = top-level
    # Provenance for a converted age value, so downstream/logs stay auditable.
    original_value: Optional[float] = None   # the number the user actually wrote
    original_unit: Optional[str] = None      # its unit before conversion
    converted: bool = False                  # True if a unit conversion happened
    assumed_unit: bool = False               # True if the unit was assumed, not stated
    # "last/latest/most recent ..." was said near this comparison. A membership
    # filter cannot express per-record recency, so this is surfaced, not solved.
    latest: bool = False


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
                    "span": list(t.span),
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
                    "field": r.field,
                    "path": r.path,
                    "original_value": r.original_value,
                    "original_unit": r.original_unit,
                    "converted": r.converted,
                    "assumed_unit": r.assumed_unit,
                    "latest": r.latest,
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

# English number words, so "five years" parses like "5 years".
_ONES = {"one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
         "six": 6, "seven": 7, "eight": 8, "nine": 9}
_TEENS = {"ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14,
          "fifteen": 15, "sixteen": 16, "seventeen": 17, "eighteen": 18, "nineteen": 19}
_TENS = {"twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
         "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90}
_WORD_NUM = {"zero": 0, **_ONES, **_TEENS, **_TENS}

_tens_re = "|".join(_TENS)
_ones_re = "|".join(_ONES)
_teens_re = "|".join(_TEENS)
_wordnum = rf"(?:(?:{_tens_re})(?:[-\s](?:{_ones_re}))?|{_teens_re}|{_ones_re}|zero)"

# A number is digits or an English number word.
_NUM = rf"(\d+(?:\.\d+)?|{_wordnum}\b)"
_UNIT = r"(?:\s+(years?|yrs?|months?|mos?|weeks?|days?))?"
_AGE_UNITS = {"years", "months", "weeks", "days"}

# Comparison phrasings. Some imply an age comparison directly (hint "age");
# generic ones rely on the unit to infer the quantity.
_RANGE_PATTERNS = [
    (re.compile(rf"\bolder than\s+{_NUM}{_UNIT}", re.I), "gt", "age"),
    (re.compile(rf"\byounger than\s+{_NUM}{_UNIT}", re.I), "lt", "age"),
    (re.compile(rf"\bbefore\s+age\s+{_NUM}{_UNIT}", re.I), "lt", "age"),
    (re.compile(rf"\bafter\s+age\s+{_NUM}{_UNIT}", re.I), "gt", "age"),
    (re.compile(rf"\b(?:by|up to)\s+age\s+{_NUM}{_UNIT}", re.I), "lte", "age"),
    (re.compile(rf"\b(?:from|at least)\s+age\s+{_NUM}{_UNIT}", re.I), "gte", "age"),
    (re.compile(rf"\b(?:under|below)\s+age\s+{_NUM}{_UNIT}", re.I), "lt", "age"),
    (re.compile(rf"\b(?:over|above)\s+age\s+{_NUM}{_UNIT}", re.I), "gt", "age"),
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

# "last / latest / most recent" near a comparison signals recency intent.
_LATEST_CUE = re.compile(r"\b(?:last|latest|most\s+recent|final)\b", re.I)

# A missing-data phrasing: an explicit "un-" word, or a negation sitting close to
# a reporting verb ("no race information reported", "sex not available").
_MISSING_CUE = re.compile(
    r"\b(?:un(?:reported|available|specified|known|documented|recorded))\b"
    r"|\bnot\s+(?:reported|available|specified|known|recorded|documented|on\s+file)\b"
    r"|\b(?:no|not|non)\b[\w\s]{0,25}?\b(?:reported|available|specified|recorded|documented)\b",
    re.I,
)
# Canonical values that mean "no data", in preference order.
_MISSING_VALUES = ("Not Reported", "Unknown", "Not Available", "Unavailable")

_NUMERIC_RE = re.compile(r"^\d+(?:\.\d+)?$")


def _parse_num(s: str) -> float:
    """Parse a number written as digits or English words ("twenty-one" -> 21)."""
    s = s.strip().lower()
    try:
        return float(s)
    except ValueError:
        pass
    return float(sum(_WORD_NUM.get(part, 0) for part in re.split(r"[-\s]+", s)))


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


def _overlaps(span: tuple[int, int], spans: list[tuple[int, int]]) -> bool:
    s, e = span
    return any(s < b and a < e for a, b in spans)


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
    if not isinstance(data, dict):
        return {}

    return {str(k).strip().lower(): str(v).strip() for k, v in data.items()}


class TermNormalizer:
    def __init__(
        self,
        schema: SchemaIndex,
        synonyms: Optional[dict[str, str]] = None,
        *,
        numeric_resolver: Optional[NumericFieldResolver] = None,
    ):
        self._schema = schema
        # Routing + unit conversion for age ranges. Derived from the schema and a
        # small config; see services/numeric_resolver.py.
        self._numeric = numeric_resolver or NumericFieldResolver.from_schema(schema)

        # normalized phrase -> (canonical value, possible field placements)
        self._phrases: dict[str, tuple[str, tuple[FieldPlacement, ...]]] = {}
        self._max_words = 1

        # normalized field-name phrase -> placements, for mention disambiguation.
        self._field_aliases: dict[str, tuple[FieldPlacement, ...]] = {}
        self._field_alias_max_words = 1

        self._build_index(synonyms or {})

    @classmethod
    def from_files(
        cls,
        schema: SchemaIndex,
        synonyms_path: Optional[Union[str, Path]] = None,
        *,
        numeric_config_path: Optional[Union[str, Path]] = None,
    ) -> "TermNormalizer":
        syn = load_synonyms(synonyms_path) if synonyms_path else {}
        config = (
            NumericConfig.from_yaml(numeric_config_path) if numeric_config_path else None
        )
        resolver = NumericFieldResolver.from_schema(schema, config)
        return cls(schema, syn, numeric_resolver=resolver)

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

            # Index the field's own (humanized) name for mention disambiguation.
            self._add_field_alias(spec.name.replace("_", " "), spec)

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

    def _add_field_alias(self, surface: str, spec) -> None:
        key = _norm_phrase(surface)
        if not key:
            return
        placement = FieldPlacement(spec.name, spec.parent_path)
        existing = self._field_aliases.get(key, ())
        if placement not in existing:
            self._field_aliases[key] = existing + (placement,)
        self._field_alias_max_words = max(self._field_alias_max_words, len(key.split()))

    def _missing_value_for(self, placement: FieldPlacement) -> Optional[str]:
        """The field's canonical 'no data' value, if it has one."""
        for spec in self._schema.get_fields(placement.field):
            if spec.parent_path == placement.path:
                for candidate in _MISSING_VALUES:
                    if candidate in spec.enum_values:
                        return candidate
        return None

    def normalize(self, text: str) -> NormalizedQuery:
        negations: list[str] = []

        # Ranges first: their operand spans let us reject a bare number that
        # would otherwise be mis-matched as an enum value ("3 months" -> "3").
        ranges = self._extract_ranges(text, negations)
        range_spans = [r.span for r in ranges]

        # Precise field=missing-value terms ("no race information reported").
        missing_terms = self._match_missing(text)
        covered = [t.span for t in missing_terms] + range_spans

        terms = self._match_terms(text, negations, block_spans=covered)
        terms = [self._disambiguate(t, text) for t in terms]

        # A precise missing-data term and the generic "not reported" enum can both
        # resolve to the same (field, value); keep one.
        all_terms = sorted(missing_terms + terms, key=lambda t: t.span[0])
        deduped: list[RecognizedTerm] = []
        seen: set = set()
        for t in all_terms:
            key = (t.value, tuple(sorted((p.field, p.path or "") for p in t.placements)), t.negated)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(t)

        return NormalizedQuery(text=text, terms=deduped, ranges=ranges, negations=negations)

    def _match_terms(
        self,
        text: str,
        negations: list[str],
        *,
        block_spans: Optional[list[tuple[int, int]]] = None,
    ) -> list[RecognizedTerm]:
        tokens = _tokenize(text)
        out: list[RecognizedTerm] = []
        block_spans = block_spans or []

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

            # A bare "no"/"not" is a function word here, not the enum value "No".
            # These are handled by negation and missing-data detection instead.
            if _norm_phrase(value) in _NEGATION_CUES:
                i += 1
                continue

            # A purely numeric enum value ("3") that sits inside a numeric range
            # is a false positive from the range operand, not a real category.
            if _NUMERIC_RE.match(value) and _overlaps((start, end), block_spans):
                i += 1
                continue

            cue = _negation_before(text, start)
            negated = cue is not None

            if negated:
                negations.append(text[cue:end].strip())

            out.append(RecognizedTerm(value, placements, (start, end), negated))
            i += width

        return out

    def _find_field_mentions(self, text: str) -> list[tuple[tuple[FieldPlacement, ...], tuple[int, int]]]:
        """Scan for field-name mentions ("race", "tumor classification")."""
        tokens = _tokenize(text)
        out: list[tuple[tuple[FieldPlacement, ...], tuple[int, int]]] = []
        i, n = 0, len(tokens)

        while i < n:
            hit = None
            upper = min(self._field_alias_max_words, n - i)
            for k in range(upper, 0, -1):
                phrase = " ".join(w for w, _, _ in tokens[i:i + k])
                places = self._field_aliases.get(phrase)
                if places is not None:
                    hit = (places, tokens[i][1], tokens[i + k - 1][2], k)
                    break
            if hit is None:
                i += 1
                continue
            places, start, end, width = hit
            out.append((places, (start, end)))
            i += width

        return out

    def _disambiguate(self, term: RecognizedTerm, text: str) -> RecognizedTerm:
        """Narrow a multi-placement term to the field named nearby, if any."""
        if len(term.placements) <= 1:
            return term

        window = (max(0, term.span[0] - 45), term.span[1] + 20)
        mentioned: set = set()
        for places, span in self._find_field_mentions(text):
            if span[0] >= window[0] and span[1] <= window[1]:
                mentioned.update(places)

        if not mentioned:
            return term

        narrowed = tuple(p for p in term.placements if p in mentioned)
        if narrowed and len(narrowed) < len(term.placements):
            return RecognizedTerm(term.value, narrowed, term.span, term.negated)
        return term

    def _match_missing(self, text: str) -> list[RecognizedTerm]:
        """Bind "no <field> ... reported / unknown / not available" to that field."""
        out: list[RecognizedTerm] = []
        seen: set = set()

        for places, span in self._find_field_mentions(text):
            window = text[max(0, span[0] - 20):span[1] + 30]
            if not _MISSING_CUE.search(window):
                continue

            for placement in places:
                value = self._missing_value_for(placement)
                key = (placement.field, placement.path, span)
                if value is None or key in seen:
                    continue
                seen.add(key)
                out.append(RecognizedTerm(value, (placement,), span, negated=False))

        return out

    def _make_range(
        self,
        op: str,
        value: float,
        unit: Optional[str],
        quantity: Optional[str],
        span: tuple[int, int],
        negated: bool,
        text: str,
    ) -> NumericConstraint:
        """Build one constraint, delegating age routing/conversion to the resolver."""
        latest = bool(_LATEST_CUE.search(text[max(0, span[0] - 30):span[1] + 30]))

        if quantity != "age":
            return NumericConstraint(op, value, unit, quantity, span, negated, latest=latest)

        b = self._numeric.bind(value, unit, text, span)
        return NumericConstraint(
            op,
            b.value,
            b.unit,
            quantity,
            span,
            negated,
            b.field,
            b.path,
            b.original_value,
            b.original_unit,
            b.converted,
            b.assumed_unit,
            latest,
        )

    def _extract_ranges(self, text: str, negations: list[str]) -> list[NumericConstraint]:
        raw: list[tuple[str, float, Optional[str], Optional[str], int, int]] = []

        # "between X and Y" becomes two constraints: >= X and <= Y.
        for m in _BETWEEN.finditer(text):
            unit = _canon_unit(m.group(3))
            q = _quantity(unit, None)

            raw.append(("gte", _parse_num(m.group(1)), unit, q, m.start(), m.end()))
            raw.append(("lte", _parse_num(m.group(2)), unit, q, m.start(), m.end()))

        for pattern, op, hint in _RANGE_PATTERNS:
            for m in pattern.finditer(text):
                unit = _canon_unit(m.group(2))
                raw.append(
                    (op, _parse_num(m.group(1)), unit, _quantity(unit, hint), m.start(), m.end())
                )

        for m in _SYMBOLIC.finditer(text):
            unit = _canon_unit(m.group(3))
            raw.append(
                (
                    _SYMBOL_OP[m.group(1)],
                    _parse_num(m.group(2)),
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
            negated = cue is not None

            if negated:
                # Example: "not older than 5" becomes "<= 5".
                negations.append(text[cue:end].strip())

            final_op = _FLIP[op] if negated else op
            out.append(self._make_range(final_op, value, unit, quantity, (start, end), negated, text))

        return out
