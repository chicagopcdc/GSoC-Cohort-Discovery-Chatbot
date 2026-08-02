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


# NOTE: FilterGenerator.generate() currently overrides this system prompt with
# services.filter_generator._TAGGED_SYSTEM_PROMPT (the tagged op/field/values
# form). This wire-form prompt is kept for tests and other callers; keep its
# rules aligned with _TAGGED_SYSTEM_PROMPT until prompt ownership is consolidated.
SYSTEM_PROMPT = """\
You translate a parsed clinical-cohort query into a single Guppy GraphQL filter.

The filter is one JSON object built from these clauses:
  - {"IN":  {"<field>": ["<value>", ...]}}   membership in a set of values
  - {"!=":  {"<field>": "<value>"}}           field value is not equal to scalar
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
   When unsure on a single field, prefer the OR-of-AND-blocks form; When unsure on an explicit either/or request, prefer the OR-of-AND-blocks form.

3. A field that lives under a nested path must be wrapped in a nested clause with
   that path. Only one level of nesting exists: a nested clause cannot contain
   another nested clause.

4. Numeric ranges are handed to you with negation already resolved and any units
   already converted to the field's stored unit. A range marked "already flipped"
   has had its operator reversed for you ("not older than 5" arrives as LTE); the
   number shown is ready to use as-is -- do no arithmetic of your own.

5. Return one JSON object and nothing else: no prose, no explanation, no code
   fences. A single condition is returned as its own clause; combine several
   independent conditions with a top-level AND.

6. There is no general NOT operator. Range negation is already folded into the
   bound (Rule 4). For negated enum/category terms listed under "Excluded terms",
   emit field-level !=. Drop unsupported negation rather than writing it as a
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
        where = c.path if c.path else "subject, TOP-LEVEL"
        head = f"- {c.field} ({c.field_type}, under {where})"
        if c.description:
            head += f": {c.description}"

        if c.field_type == "enum" and c.enum_values:
            pinned_values = set(pinned.get((c.path, c.field), ()))
            must = [v for v in c.enum_values if v in pinned_values]
            rest = [v for v in c.enum_values if v not in must]
            shown = must + rest[: max(0, _MAX_VALUES_SHOWN - len(must))]
            hidden = len(c.enum_values) - len(shown)
            more = f" (+{hidden} more)" if hidden > 0 else ""
            head += "\n    values: " + ", ".join(shown) + more

        lines.append(head)

    return "\n".join(lines) if lines else "(no candidate fields)"


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


def _format_excluded(nq: "NormalizedQuery") -> str:
    terms = [term for term in nq.terms if term.negated and term.placements]
    if not terms:
        return "(none)"

    lines: List[str] = []
    for term in terms:
        places = ", ".join(
            f"{p.field} (under {p.path or 'subject'})" for p in term.placements
        )
        lines.append(f'- "{term.value}" -> {places}')

    return "\n".join(lines)


def _fmt_num(value: float):
    """Show whole numbers without a trailing .0, keep real decimals."""
    return int(value) if float(value).is_integer() else value


def _unconvertible(r) -> bool:
    return r.field is None and r.unit is not None


def _range_label(r) -> str:
    op = _OP_DISPLAY.get(r.op, r.op.upper())
    unit = f" {r.unit}" if r.unit else ""
    return f"{r.quantity or 'value'} {op} {_fmt_num(r.value)}{unit}"


def prompt_warnings(nq: "NormalizedQuery") -> List[str]:
    """Constraints the prompt must not turn into filter clauses, so the caller
    can surface them instead of silently dropping user intent."""
    warnings: List[str] = []

    for term in nq.terms:
        if term.negated and not term.placements:
            warnings.append(
                f'dropped negated enum/category term "{term.value}": '
                "no schema field placement is available"
            )

    for r in nq.ranges:
        if _unconvertible(r):
            warnings.append(
                f"dropped numeric range {_range_label(r)}: "
                "schema-unit converted value is not available"
            )

    return warnings


def _format_unsupported(nq: "NormalizedQuery") -> str:
    warnings = prompt_warnings(nq)
    return "\n".join(f"- {w}" for w in warnings) if warnings else "(none)"


def _format_ranges(nq: "NormalizedQuery") -> str:
    if not nq.ranges:
        return "(none)"

    lines: List[str] = []
    for r in nq.ranges:
        if _unconvertible(r):
            continue

        op = _OP_DISPLAY.get(r.op, r.op.upper())
        num = _fmt_num(r.value)
        flipped = " (operator already flipped for negation)" if r.negated else ""
        # "latest record" intent cannot be expressed by a membership filter; the
        # filter matches any qualifying record. Surface it rather than hide it.
        latest = " (note: 'latest' record intent is not expressible; matches any record)" \
            if getattr(r, "latest", False) else ""
        flipped += latest

        if r.field is not None:
            # Bound to a schema field. The value is already in the field's stored
            # unit; show the conversion provenance so the number is auditable and
            # the model knows not to touch it.
            where = f" (under {r.path})" if r.path else ""
            prov = ""
            if r.converted and r.original_value is not None:
                assumed = "assumed " if r.assumed_unit else ""
                prov = (
                    f"  [from {_fmt_num(r.original_value)} {assumed}{r.original_unit}, "
                    f"converted to {r.unit}]"
                )
            lines.append(f"- {r.field}{where} {op} {num}{flipped}{prov}")
        else:
            # Unresolved: no target field, so the unit was not converted. Pass the
            # user's unit through rather than pretend it matches a field's unit.
            unit = f" {r.unit}" if r.unit else ""
            lines.append(f"- {r.quantity or 'value'} {op} {num}{unit}{flipped}")

    return "\n".join(lines) if lines else "(none)"


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
        f"Excluded terms (emit as !=):\n"
        f"{_format_excluded(nq)}\n\n"
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
