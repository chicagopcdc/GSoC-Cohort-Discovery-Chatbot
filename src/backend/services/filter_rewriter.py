"""
Deterministic rewrites applied to a generated filter before validation.

Each step is a pure wire-in/wire-out transform that repairs a shape the model
gets wrong in a predictable way. They run in a fixed order and every step
records whether it changed anything, so a surprising filter can be traced back
to the step that produced it rather than guessing between the model and four
anonymous post-processors.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Optional

from services.schema_loader import SchemaIndex
from services.semantic_enricher import SemanticEnricher
from services.term_normalizer import NormalizedQuery
from services.filter_validator import ValidationIssue


_DISJUNCTION = re.compile(r"\b(?:either|or)\b", re.I)
# "either" sits in front of the first alternative, so the region under test
# reaches back a little past it.
_DISJUNCTION_LOOKBACK = 25


def _reads_as_disjunction(values: list, nq: NormalizedQuery) -> bool:
    """True if the text joins these values with "or" rather than "and".

    Judged on the span the values actually occupy, not on the whole query: a
    query can say "found in bone and bone marrow" in one clause and "male or
    female" in another, and only the second is a disjunction. When the values
    cannot be located (the model used wording the normalizer did not recognise)
    this falls back to the whole query, which is permissive but no worse than
    leaving an impossible AND in place.
    """
    spans = [
        term.span
        for term in nq.terms
        if term.value in values and not term.negated
    ]
    if len(spans) < 2:
        return _DISJUNCTION.search(nq.text) is not None

    start = max(0, min(s for s, _ in spans) - _DISJUNCTION_LOOKBACK)
    end = max(e for _, e in spans)
    return _DISJUNCTION.search(nq.text[start:end]) is not None


def merge_disjunctive_same_field_in(wire: dict, nq: NormalizedQuery) -> dict:
    """Fold sibling IN clauses on one field into a single IN when the query
    reads as a choice.

    Guppy evaluates an AND against a single document, so two disjoint IN clauses
    on the same field can never match. When the wording says "or" that is the
    model writing a choice as an AND, and folding it is the repair. When the
    wording says "and" it is a genuine conflict, and the filter validator rejects
    it rather than this quietly turning the "and" into an "or".
    """

    def visit(clause):
        if not isinstance(clause, dict):
            return
        for op in ("AND", "OR"):
            for child in clause.get(op) or []:
                visit(child)
        nested = clause.get("nested")
        if isinstance(nested, dict):
            visit(nested)

        children = clause.get("AND")
        if not isinstance(children, list):
            return
        groups: dict[str, list[int]] = {}
        for index, child in enumerate(children):
            payload = child.get("IN") if isinstance(child, dict) else None
            if isinstance(payload, dict) and len(payload) == 1:
                field = next(iter(payload))
                groups.setdefault(field, []).append(index)

        remove: set[int] = set()
        for field, indexes in groups.items():
            if len(indexes) < 2:
                continue
            values = []
            for index in indexes:
                for value in children[index]["IN"][field]:
                    if value not in values:
                        values.append(value)
            if not _reads_as_disjunction(values, nq):
                continue
            children[indexes[0]] = {"IN": {field: values}}
            remove.update(indexes[1:])
        if remove:
            clause["AND"] = [
                child for index, child in enumerate(children) if index not in remove
            ]

    visit(wire)
    return wire


def restore_recognized_enum_values(wire: dict, nq: NormalizedQuery) -> dict:
    recognized: dict[tuple[Optional[str], str], list[str]] = {}
    for term in nq.terms:
        if term.negated:
            continue
        for placement in term.placements:
            key = (placement.path, placement.field)
            values = recognized.setdefault(key, [])
            if term.value not in values:
                values.append(term.value)

    def visit(clause, path=None):
        if not isinstance(clause, dict):
            return clause
        payload = clause.get("!=")
        if isinstance(payload, dict) and len(payload) == 1:
            field, excluded = next(iter(payload.items()))
            values = recognized.get((path, field), [])
            if values and str(excluded) not in values:
                return {"IN": {field: values}}
        for op in ("AND", "OR"):
            if isinstance(clause.get(op), list):
                clause[op] = [visit(child, path) for child in clause[op]]
        nested = clause.get("nested")
        if isinstance(nested, dict):
            nested_path = nested.get("path")
            for op in ("AND", "OR"):
                if isinstance(nested.get(op), list):
                    nested[op] = [visit(child, nested_path) for child in nested[op]]
        return clause

    return visit(wire)


def propagate_disease_phase(wire: dict, nq: NormalizedQuery, schema: SchemaIndex) -> dict:
    values = {
        term.value
        for term in nq.terms
        if any(place.field == "disease_phase" for place in term.placements)
    }
    if len(values) != 1:
        return wire
    value = next(iter(values))

    def has_phase(clause) -> bool:
        if not isinstance(clause, dict):
            return False
        payload = clause.get("IN")
        if isinstance(payload, dict) and "disease_phase" in payload:
            return True
        return any(
            has_phase(child)
            for op in ("AND", "OR")
            for child in (clause.get(op) or [])
        )

    def visit(clause):
        if not isinstance(clause, dict):
            return
        for op in ("AND", "OR"):
            for child in clause.get(op) or []:
                visit(child)
        nested = clause.get("nested")
        if not isinstance(nested, dict):
            return
        path = nested.get("path")
        for op in ("AND", "OR"):
            for child in nested.get(op) or []:
                visit(child)
        if not isinstance(path, str) or has_phase(nested):
            return
        if not schema.is_valid_value("disease_phase", value, path=path):
            return
        phase = {"IN": {"disease_phase": [value]}}
        if isinstance(nested.get("AND"), list):
            nested["AND"].append(phase)
        elif isinstance(nested.get("OR"), list):
            alternatives = nested.pop("OR")
            nested["AND"] = [{"OR": alternatives}, phase]

    visit(wire)
    return wire


@dataclass(frozen=True)
class RewriteStep:
    name: str
    changed: bool


@dataclass(frozen=True)
class RewriteResult:
    wire: dict
    steps: tuple
    intent: Optional[str] = None
    warnings: tuple = ()
    issues: tuple = ()

    @property
    def ok(self) -> bool:
        return not self.issues


class FilterRewriter:
    """Runs the deterministic repair steps over one generated filter."""

    def __init__(
        self,
        schema: SchemaIndex,
        *,
        semantic_enricher: Optional[SemanticEnricher] = None,
    ):
        self.schema = schema
        self.semantic_enricher = semantic_enricher

    def apply(
        self,
        wire: dict,
        nq: NormalizedQuery,
        *,
        model_intent: Optional[str] = None,
    ) -> RewriteResult:
        steps: list = []
        intent = model_intent
        warnings: list = []
        issues: tuple = ()

        def record(name: str, before: Any, after: Any):
            steps.append(RewriteStep(name, before != after))
            return after

        wire = record(
            "merge_disjunctive_same_field_in",
            _snapshot(wire),
            merge_disjunctive_same_field_in(wire, nq),
        )
        wire = record(
            "restore_recognized_enum_values",
            _snapshot(wire),
            restore_recognized_enum_values(wire, nq),
        )
        wire = record(
            "propagate_disease_phase",
            _snapshot(wire),
            propagate_disease_phase(wire, nq, self.schema),
        )

        if self.semantic_enricher is not None:
            before = _snapshot(wire)
            enriched = self.semantic_enricher.enrich(
                wire, nq.text, model_intent=model_intent, nq=nq
            )
            steps.append(RewriteStep("semantic_enrichment", enriched.wire != before))
            intent = enriched.intent.intent.value
            warnings.extend(enriched.warnings)
            issues = enriched.issues
            if not issues:
                wire = enriched.wire

        return RewriteResult(
            wire=wire,
            steps=tuple(steps),
            intent=intent,
            warnings=tuple(warnings),
            issues=issues,
        )


def _snapshot(wire: Any) -> Any:
    """Deep copy for change detection; the steps mutate in place."""
    from copy import deepcopy

    return deepcopy(wire)


__all__ = [
    "FilterRewriter",
    "RewriteResult",
    "RewriteStep",
    "merge_disjunctive_same_field_in",
    "propagate_disease_phase",
    "restore_recognized_enum_values",
]
