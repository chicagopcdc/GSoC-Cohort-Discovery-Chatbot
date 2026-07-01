"""
Build the chat messages that ask the LLM to turn a normalized query into a
GraphQL filter.

The heavy lifting already happened upstream: term_normalizer matched enum
values and ranges, and candidate_retriever picked the fields worth showing.
This module only lays that out as a prompt. It does no schema lookups and no
model calls, so it stays cheap to import and easy to diff when we tune wording.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, List, Optional, Sequence

if TYPE_CHECKING:
    # Type-only imports keep this module from pulling numpy at runtime.
    from services.candidate_retriever import FieldCandidate
    from services.term_normalizer import NormalizedQuery


# Long enums (some PCDC fields have hundreds of values) get truncated so the
# prompt stays small. Values a term actually resolved to are pinned in first,
# so truncation never hides a value the model is supposed to emit.
_MAX_VALUES_SHOWN = 40

_OP_DISPLAY = {"gt": "GT", "gte": "GTE", "lt": "LT", "lte": "LTE"}


SYSTEM_PROMPT = """\
You translate a parsed clinical-cohort query into a single Guppy GraphQL filter.

The filter is one JSON object built from these clauses:
  - {"IN":  {"<field>": ["<value>", ...]}}   membership in a set of values
  - {"GTE": {"<field>": <number>}}            >=   (also LTE, GT, LT)
  - {"AND": [<clause>, ...]}                   all of
  - {"OR":  [<clause>, ...]}                   any of
  - {"nested": {"path": "<table>", "AND" | "OR": [<clause>, ...]}}

Every clause dict accepts only the keys shown here. Any extra key is rejected.

Rules:

1. Use only the fields and values given below. Never invent a field name or an
   enum value, and copy enum values exactly as written, including capitalization,
   spacing, and punctuation. Values under "Recognized terms" were already matched
   against the schema, so you may use them verbatim even when a field's value list
   further down is truncated.

2. A plain list of accepted values for one field is a single IN
   ("stage 1, 2, or 3" -> {"IN": {"stage": ["1", "2", "3"]}}). Switch to an OR,
   with each branch wrapped in its own AND block, when the alternatives span
   different fields or nested paths (a cross-field OR cannot be written as one IN),
   or when the request reads as a choice between cohorts ("either the INRG or the
   INSTRuCT cohort") -- that is recorded as an OR of AND blocks even on one field.
   When unsure, prefer the OR-of-AND-blocks form (especially for explicit
   either/or requests).

3. A field that lives under a nested path must be wrapped in a nested clause with
   that path. Only one level of nesting exists: a nested clause cannot contain
   another nested clause.

4. Numeric ranges are handed to you with negation already resolved. A range marked
   "already flipped" has had its operator reversed for you ("not older than 5"
   arrives as LTE 5) -- apply it as given and do no arithmetic of your own.
   If a numeric range includes a unit but no schema-unit converted value is
   provided, drop that numeric condition rather than guessing a conversion.


5. Return one JSON object and nothing else: no prose, no explanation, no code
   fences. A single condition is returned as its own clause; combine several
   independent conditions with a top-level AND.

6. There is no NOT operator. Range negation is already folded into the bound
   (Rule 4). A negated enum or category term ("not metastatic", "race is not
   white") cannot be expressed -- drop that condition rather than writing it as a
   positive IN.

The examples below show clause structure only. For the real answer, draw every
field and value from the candidate list given to you, never from the examples.

Query: INRG males
Filter: {"AND": [{"IN": {"consortium": ["INRG"]}}, {"IN": {"sex": ["Male"]}}]}

Query: subjects in either the INRG or INSTRuCT consortium
Filter: {"OR": [{"AND": [{"IN": {"consortium": ["INRG"]}}]}, {"AND": [{"IN": {"consortium": ["INSTRuCT"]}}]}]}

Query: patients with metastatic tumors
Filter: {"nested": {"AND": [{"IN": {"tumor_classification": ["Metastatic"]}}], "path": "tumor_assessments"}}
"""

def _pinned_values(nq: "NormalizedQuery") -> dict:
    """Map each (path, field) the recognized terms resolved to -> its values.

    A term carries the canonical schema value already, so this is what lets us
    force a resolved value to show even if it sits past the display cap.
    """
    pinned: dict = {}
    for term in nq.terms:
        for p in term.placements:
            pinned.setdefault((p.path, p.field), set()).add(term.value)
    return pinned


def _format_candidates(
    candidates: "Sequence[FieldCandidate]",
    pinned: Optional[dict] = None,
) -> str:
    pinned = pinned or {}
    lines: List[str] = []

    for c in candidates:
        where = c.path or "subject"
        head = f"- {c.field} ({c.field_type}, under {where})"
        if c.description:
            head += f": {c.description}"

        if c.field_type == "enum" and c.enum_values:
            pinned_vals = set(pinned.get((c.path, c.field), ()))
            must = [v for v in c.enum_values if v in pinned_vals]
            rest = [v for v in c.enum_values if v not in pinned_vals]
            shown = must + rest[: max(0, _MAX_VALUES_SHOWN - len(must))]
            hidden = len(c.enum_values) - len(shown)
            more = f" (+{hidden} more)" if hidden > 0 else ""
            head += "\n    values: " + ", ".join(shown) + more

        lines.append(head)

    return "\n".join(lines) if lines else "(no candidate fields)"


def _range_field(r) -> Optional[str]:
    return getattr(r, "field", None)


def _range_path(r) -> Optional[str]:
    return getattr(r, "path", None)


def _range_value(r):
    # Future normalizer versions may attach a schema-unit value, e.g. days for
    # age. Prefer that over the user's raw number when it exists.
    return getattr(r, "value_in_days", r.value)


def _format_number(value) -> object:
    return int(value) if float(value).is_integer() else value


def _has_schema_unit_value(r) -> bool:
    return r.unit is None or _range_field(r) is not None or hasattr(r, "value_in_days")


def _range_label(r, *, include_unit: bool = True) -> str:
    op = _OP_DISPLAY.get(r.op, r.op.upper())
    value = _format_number(_range_value(r))
    unit = ""
    if include_unit:
        if hasattr(r, "value_in_days"):
            unit = " days"
        elif r.unit:
            unit = f" {r.unit}"
    return f"{r.quantity or 'value'} {op} {value}{unit}"


def prompt_warnings(nq: "NormalizedQuery") -> List[str]:
    """Return constraints the prompt must not turn into filter clauses.

    The generator/query builder can surface these strings to the user. Keeping
    them here avoids silently dropping unsupported user intent.
    """
    warnings: List[str] = []

    for term in nq.terms:
        if term.negated:
            warnings.append(
                f'dropped negated enum/category term "{term.value}": '
                "NOT is not supported for enum/category values"
            )

    for r in nq.ranges:
        if not _has_schema_unit_value(r):
            warnings.append(
                f"dropped numeric range {_range_label(r)}: "
                "schema-unit converted value is not available"
            )

    return warnings


def _format_recognized(nq: "NormalizedQuery") -> str:
    terms = [term for term in nq.terms if not term.negated]
    if not terms:
        return "(none)"

    lines: List[str] = []
    for term in terms:
        if term.placements:
            places = ", ".join(
                f"{p.field} (under {p.path or 'subject'})" for p in term.placements
            )
            lines.append(f'- "{term.value}" -> {places}')
        else:
            lines.append(f'- "{term.value}" -> unresolved')

    return "\n".join(lines)


def _format_ranges(nq: "NormalizedQuery") -> str:
    if not nq.ranges:
        return "(none)"

    lines: List[str] = []
    for r in nq.ranges:
        if not _has_schema_unit_value(r):
            continue

        op = _OP_DISPLAY.get(r.op, r.op.upper())
        num = _format_number(_range_value(r))
        flipped = " (operator already flipped for negation)" if r.negated else ""
        field = _range_field(r)
        path = _range_path(r)
        if field is not None:
            # Already bound to a schema field; present it ready to use (value is
            # in the field's stored unit, applied as-is).
            where = f" (under {path})" if path else ""
            lines.append(f"- {field}{where} {op} {num}{flipped}")
        else:
            unit = " days" if hasattr(r, "value_in_days") else ""
            lines.append(f"- {r.quantity or 'value'} {op} {num}{unit}{flipped}")

    return "\n".join(lines) if lines else "(none)"


def _format_unsupported(nq: "NormalizedQuery") -> str:
    warnings = prompt_warnings(nq)
    if not warnings:
        return "(none)"
    return "\n".join(f"- {w}" for w in warnings)


def build_filter_messages(
    nq: "NormalizedQuery",
    candidates: "Sequence[FieldCandidate]",
) -> List[dict]:
    """Assemble the system + user messages for filter generation."""
    pinned = _pinned_values(nq)

    user_content = (
        f"User query:\n{nq.text}\n\n"
        f"Recognized terms (already matched to the schema):\n"
        f"{_format_recognized(nq)}\n\n"
        f"Numeric ranges:\n{_format_ranges(nq)}\n\n"
        f"Unsupported constraints (already dropped; do not emit):\n"
        f"{_format_unsupported(nq)}\n\n"
        f"Candidate fields:\n{_format_candidates(candidates, pinned)}\n\n"
        "Return the filter as a single JSON object."
    )

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_content},
    ]
