"""Deterministic clinical-state enrichment for generated Guppy filters."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Union

import yaml

from models.filters import GraphQLFilter
from services.filter_validator import ValidationIssue
from services.schema_loader import SchemaIndex
from services.semantic_intent import (
    ClinicalIntent,
    IntentResult,
    infer_intent,
    infer_intent_near,
)
from services.term_normalizer import NormalizedQuery


CODE_CONFLICTING_CLINICAL_STATE = "CONFLICTING_CLINICAL_STATE"
CODE_INVALID_SEMANTIC_METADATA = "INVALID_SEMANTIC_METADATA"


@dataclass(frozen=True)
class AssertionSemantics:
    field: str
    positive_values: tuple[str, ...]
    negative_values: tuple[str, ...]
    unknown_values: tuple[str, ...]
    any_values: tuple[str, ...] = ()


@dataclass(frozen=True)
class SemanticEntity:
    path: str
    entity: str
    descriptor_fields: tuple[str, ...]
    assertion: AssertionSemantics


@dataclass(frozen=True)
class SemanticMetadata:
    entities: dict[str, SemanticEntity]

    @classmethod
    def from_yaml(
        cls,
        path: Union[str, Path],
        schema: SchemaIndex,
    ) -> "SemanticMetadata":
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}
        return cls.from_dict(data, schema)

    @classmethod
    def from_dict(cls, data: dict, schema: SchemaIndex) -> "SemanticMetadata":
        raw_entities = data.get("semantic_entities", data)
        if not isinstance(raw_entities, dict):
            raise ValueError("semantic_entities must be a mapping")

        entities: dict[str, SemanticEntity] = {}
        for path, raw in raw_entities.items():
            if not isinstance(raw, dict):
                raise ValueError(f"semantic entity {path!r} must be a mapping")
            entity = _build_entity(str(path), raw, schema)
            entities[entity.path] = entity

        return cls(entities)

    def entity_for_path(self, path: str) -> Optional[SemanticEntity]:
        return self.entities.get(path)


@dataclass(frozen=True)
class SemanticEnrichmentResult:
    wire: dict
    intent: IntentResult
    issues: tuple[ValidationIssue, ...] = ()
    warnings: tuple[str, ...] = ()
    changed: bool = False

    @property
    def ok(self) -> bool:
        return not self.issues


def default_semantic_metadata_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "semantic_entities.yaml"


class SemanticEnricher:
    def __init__(self, metadata: SemanticMetadata):
        self.metadata = metadata

    @classmethod
    def from_yaml(
        cls,
        path: Union[str, Path],
        schema: SchemaIndex,
    ) -> "SemanticEnricher":
        return cls(SemanticMetadata.from_yaml(path, schema))

    def enrich(
        self,
        filter_obj: Union[GraphQLFilter, Dict[str, Any]],
        text: str,
        *,
        model_intent: Optional[str] = None,
        nq: Optional[NormalizedQuery] = None,
    ) -> SemanticEnrichmentResult:
        before = _as_wire(filter_obj)
        wire = deepcopy(before)
        intent = infer_intent(text, model_intent=model_intent)
        issues: list[ValidationIssue] = []
        warnings: list[str] = []

        wire = self._merge_nested_blocks(wire)
        self._enrich_clause(wire, intent.intent, issues, warnings, text, nq)

        return SemanticEnrichmentResult(
            wire=wire,
            intent=intent,
            issues=tuple(issues),
            warnings=tuple(warnings),
            changed=wire != before,
        )

    def _intent_for_block(
        self,
        nested: dict,
        fallback: ClinicalIntent,
        text: str,
        nq: Optional[NormalizedQuery],
    ) -> ClinicalIntent:
        """Resolve the intent for one nested block from the text it came from.

        The block is located through the terms whose values it filters on; the
        cue nearest that region wins. Without a locatable span there is nothing
        better than the query-wide reading.
        """
        if nq is None:
            return fallback

        path = nested.get("path")
        values = _values_in_clause(nested)
        spans = [
            term.span
            for term in nq.terms
            if term.value in values
            and any(place.path == path for place in term.placements)
        ]
        if not spans:
            return fallback

        span = (min(s for s, _ in spans), max(e for _, e in spans))
        return infer_intent_near(text, span).intent

    def _merge_nested_blocks(self, clause: Any) -> Any:
        if not isinstance(clause, dict):
            return clause

        if "AND" in clause and isinstance(clause["AND"], list):
            children = [self._merge_nested_blocks(c) for c in clause["AND"]]
            clause["AND"] = self._merge_sibling_nested_and(children)
            return clause

        if "OR" in clause and isinstance(clause["OR"], list):
            clause["OR"] = [self._merge_nested_blocks(c) for c in clause["OR"]]
            return clause

        nested = clause.get("nested")
        if isinstance(nested, dict):
            for key in ("AND", "OR"):
                if isinstance(nested.get(key), list):
                    nested[key] = [self._merge_nested_blocks(c) for c in nested[key]]
                    break
        return clause

    def _merge_sibling_nested_and(self, children: list[Any]) -> list[Any]:
        """Fold sibling nested blocks on one path together when it is safe.

        Two blocks on the same path mean "a record with A, and a record with B",
        which may be two different records; one merged block means "a single
        record with both". Merging therefore narrows, and is only safe when the
        blocks constrain disjoint fields -- that is the shape the model produces
        when it splits one finding across two blocks. Any field appearing in
        both is a genuine two-record request ("a bone tumour and a bone marrow
        tumour"), and merging it would collapse the cohort to nothing.
        """
        out: list[Any] = []
        first_by_path: dict[str, int] = {}

        for child in children:
            nested = child.get("nested") if isinstance(child, dict) else None
            if not isinstance(nested, dict):
                out.append(child)
                continue

            path = nested.get("path")
            body = nested.get("AND")
            if (
                not isinstance(path, str)
                or path not in self.metadata.entities
                or not isinstance(body, list)
            ):
                out.append(child)
                continue

            prior = first_by_path.get(path)
            if prior is None:
                first_by_path[path] = len(out)
                out.append(child)
                continue

            target = out[prior]["nested"]["AND"]
            if _fields_in_clause({"AND": target}) & _fields_in_clause({"AND": body}):
                out.append(child)
                continue

            for candidate in body:
                if candidate not in target:
                    target.append(candidate)

        return out

    def _enrich_clause(
        self,
        clause: Any,
        intent: ClinicalIntent,
        issues: list[ValidationIssue],
        warnings: list[str],
        text: str,
        nq: Optional[NormalizedQuery],
    ) -> None:
        if not isinstance(clause, dict):
            return

        for key in ("AND", "OR"):
            if key in clause:
                for child in clause.get(key) or []:
                    self._enrich_clause(child, intent, issues, warnings, text, nq)
                return

        nested = clause.get("nested")
        if not isinstance(nested, dict):
            return

        path = nested.get("path")
        entity = self.metadata.entity_for_path(path) if isinstance(path, str) else None
        if entity is None:
            return

        local = self._intent_for_block(nested, intent, text, nq)
        self._enrich_nested_body(nested, entity, local, issues, warnings)

    def _enrich_nested_body(
        self,
        body: dict,
        entity: SemanticEntity,
        intent: ClinicalIntent,
        issues: list[ValidationIssue],
        warnings: list[str],
    ) -> None:
        fields = _fields_in_clause(body)
        has_descriptor = any(f in entity.descriptor_fields for f in fields)
        if not has_descriptor:
            return

        if intent == ClinicalIntent.NEGATIVE_EXISTENCE:
            _normalize_negative_descriptors(body, entity.descriptor_fields)
            _normalize_negative_assertion(body, entity.assertion)
            fields = _fields_in_clause(body)

        explicit_values = _assertion_values(body, entity.assertion.field)
        implied_values = _values_for_intent(entity.assertion, intent)

        if explicit_values:
            if implied_values and explicit_values.isdisjoint(implied_values):
                # An explicit state read off the sentence beats a state guessed
                # from nearby wording: "exhibit an 11q deletion, and the result
                # is absent" puts a positive cue next to a negative finding.
                # Report the disagreement, keep the stated value.
                warnings.append(
                    f"{entity.path}: kept the stated {entity.assertion.field} "
                    f"{sorted(explicit_values)} even though the surrounding "
                    f"wording reads as {intent.value}"
                )
            return

        if implied_values:
            children = _ensure_and_children(body)
            children.append({"IN": {entity.assertion.field: sorted(implied_values)}})
            return

        if intent == ClinicalIntent.STATE_UNSPECIFIED:
            # Reported, not fatal. The filter without an assertion is the
            # record-existence reading, which is the safest of the candidates;
            # refusing to build anything leaves the user with no cohort at all.
            warnings.append(
                f"{entity.path}: the query does not say whether the finding is "
                "present, absent, unknown, or only assessed, so no state "
                "condition was added"
            )


def _build_entity(path: str, raw: dict, schema: SchemaIndex) -> SemanticEntity:
    if path not in schema.all_paths():
        raise ValueError(f"semantic path {path!r} is not in the schema")

    descriptor_fields = tuple(str(f) for f in raw.get("descriptor_fields", ()) or ())
    if not descriptor_fields:
        raise ValueError(f"{path}: descriptor_fields must not be empty")

    for field in descriptor_fields:
        _require_field(schema, path, field)

    assertion_raw = raw.get("assertion")
    if not isinstance(assertion_raw, dict):
        raise ValueError(f"{path}: assertion must be a mapping")

    assertion_field = str(assertion_raw.get("field", "")).strip()
    _require_field(schema, path, assertion_field)

    assertion = AssertionSemantics(
        field=assertion_field,
        positive_values=_require_values(
            schema, path, assertion_field, assertion_raw.get("positive_values")
        ),
        negative_values=_require_values(
            schema, path, assertion_field, assertion_raw.get("negative_values")
        ),
        unknown_values=_require_values(
            schema, path, assertion_field, assertion_raw.get("unknown_values")
        ),
        any_values=_optional_values(
            schema, path, assertion_field, assertion_raw.get("any_values")
        ),
    )

    return SemanticEntity(
        path=path,
        entity=str(raw.get("entity") or path),
        descriptor_fields=descriptor_fields,
        assertion=assertion,
    )


def _require_field(schema: SchemaIndex, path: str, field: str) -> None:
    if not field or schema.get_field(field, path=path) is None:
        raise ValueError(f"{path}: field {field!r} is not available under this path")


def _require_values(
    schema: SchemaIndex,
    path: str,
    field: str,
    raw_values: Any,
) -> tuple[str, ...]:
    values = tuple(str(v) for v in (raw_values or ()))
    if not values:
        raise ValueError(f"{path}.{field}: semantic value list must not be empty")
    for value in values:
        if not schema.is_valid_value(field, value, path=path):
            raise ValueError(f"{path}.{field}: {value!r} is not a schema enum value")
    return values


def _optional_values(
    schema: SchemaIndex,
    path: str,
    field: str,
    raw_values: Any,
) -> tuple[str, ...]:
    values = tuple(str(v) for v in (raw_values or ()))
    for value in values:
        if not schema.is_valid_value(field, value, path=path):
            raise ValueError(f"{path}.{field}: {value!r} is not a schema enum value")
    return values


def _as_wire(filter_obj: Union[GraphQLFilter, Dict[str, Any]]) -> Dict[str, Any]:
    if isinstance(filter_obj, GraphQLFilter):
        return filter_obj.model_dump()
    if isinstance(filter_obj, dict):
        return deepcopy(filter_obj)
    raise TypeError(f"filter_obj must be GraphQLFilter or dict, got {type(filter_obj).__name__}")


def _values_in_clause(clause: Any) -> set[str]:
    """Every enum value the clause filters on, at any depth."""
    values: set[str] = set()
    if not isinstance(clause, dict):
        return values

    payload = clause.get("IN")
    if isinstance(payload, dict):
        for raw in payload.values():
            values.update(str(v) for v in (raw or ()))

    payload = clause.get("!=")
    if isinstance(payload, dict):
        values.update(str(v) for v in payload.values())

    for key in ("AND", "OR"):
        children = clause.get(key)
        if isinstance(children, list):
            for child in children:
                values.update(_values_in_clause(child))

    return values


def _merge_values(current: Iterable[Any], extra: Iterable[Any]) -> list[Any]:
    out = list(current)
    for value in extra:
        if value not in out:
            out.append(value)
    return out


def _normalize_negative_descriptors(clause: Any, descriptor_fields: tuple[str, ...]) -> None:
    if isinstance(clause, dict):
        if "!=" in clause and isinstance(clause["!="], dict) and len(clause["!="]) == 1:
            field, value = next(iter(clause["!="].items()))
            if field in descriptor_fields:
                clause.clear()
                clause["IN"] = {field: [value]}
                return

        for key in ("AND", "OR"):
            children = clause.get(key)
            if isinstance(children, list):
                for child in children:
                    _normalize_negative_descriptors(child, descriptor_fields)

        nested = clause.get("nested")
        if isinstance(nested, dict):
            _normalize_negative_descriptors(nested, descriptor_fields)


def _normalize_negative_assertion(
    clause: Any,
    assertion: AssertionSemantics,
) -> None:
    if not isinstance(clause, dict):
        return

    payload = clause.get("!=")
    if isinstance(payload, dict) and len(payload) == 1:
        field, value = next(iter(payload.items()))
        if field == assertion.field and str(value) in assertion.positive_values:
            clause.clear()
            clause["IN"] = {field: list(assertion.negative_values)}
            return

    for key in ("AND", "OR"):
        children = clause.get(key)
        if isinstance(children, list):
            for child in children:
                _normalize_negative_assertion(child, assertion)


def _fields_in_clause(clause: Any) -> set[str]:
    fields: set[str] = set()
    if not isinstance(clause, dict):
        return fields

    for op in ("IN", "!=", "GTE", "LTE", "GT", "LT"):
        payload = clause.get(op)
        if isinstance(payload, dict):
            fields.update(str(field) for field in payload)

    for key in ("AND", "OR"):
        children = clause.get(key)
        if isinstance(children, list):
            for child in children:
                fields.update(_fields_in_clause(child))

    return fields


def _assertion_values(clause: Any, field: str) -> set[str]:
    values: set[str] = set()
    if not isinstance(clause, dict):
        return values

    payload = clause.get("IN")
    if isinstance(payload, dict) and field in payload:
        values.update(str(v) for v in payload.get(field, ()))

    payload = clause.get("!=")
    if isinstance(payload, dict) and field in payload:
        values.add(str(payload[field]))

    for key in ("AND", "OR"):
        children = clause.get(key)
        if isinstance(children, list):
            for child in children:
                values.update(_assertion_values(child, field))

    return values


def _ensure_and_children(body: dict) -> list:
    if isinstance(body.get("AND"), list):
        return body["AND"]
    if isinstance(body.get("OR"), list):
        old_or = body.pop("OR")
        body["AND"] = [{"OR": old_or}]
        return body["AND"]
    body["AND"] = []
    return body["AND"]


def _values_for_intent(
    assertion: AssertionSemantics,
    intent: ClinicalIntent,
) -> set[str]:
    if intent in (
        ClinicalIntent.POSITIVE_EXISTENCE,
        ClinicalIntent.CURRENT_POSITIVE,
        ClinicalIntent.HISTORICAL_POSITIVE,
    ):
        return set(assertion.positive_values)
    if intent == ClinicalIntent.NEGATIVE_EXISTENCE:
        return set(assertion.negative_values)
    if intent == ClinicalIntent.UNKNOWN_STATE:
        return set(assertion.unknown_values)
    if intent == ClinicalIntent.ANY_STATE:
        return set(assertion.any_values)
    return set()


__all__ = [
    "AssertionSemantics",
    "SemanticEntity",
    "SemanticMetadata",
    "SemanticEnricher",
    "SemanticEnrichmentResult",
    "CODE_CONFLICTING_CLINICAL_STATE",
    "CODE_INVALID_SEMANTIC_METADATA",
    "default_semantic_metadata_path",
]
