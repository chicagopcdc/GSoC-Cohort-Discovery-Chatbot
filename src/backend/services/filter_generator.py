"""
Generate a validated Guppy filter from a user query.

The pipeline is:
1. normalize the query
2. retrieve likely schema fields
3. ask the model for a tagged filter
4. convert it to the GraphQLFilter shape
5. validate it against the schema

If validation fails, the error is sent back to the model for another attempt.
"""

from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, List, Optional, Tuple, Union

from pydantic import ValidationError as PydanticValidationError

from models.filters import GraphQLFilter
from prompts.filter_prompt import build_filter_messages
from services.schema_loader import SchemaIndex
from services.term_normalizer import NormalizedQuery, TermNormalizer
from services.candidate_retriever import DEFAULT_EMBED_MODEL, CandidateRetriever
from services.filter_validator import (
    CODE_STRUCTURAL,
    ValidationIssue,
    ValidationResult,
    validate_filter,
)
from services.filter_rewriter import FilterRewriter
from services.semantic_enricher import (
    SemanticEnricher,
    default_semantic_metadata_path,
)
from services.semantic_intent import ClinicalIntent


DEFAULT_CHAT_MODEL = "gpt-4o-mini"

# Test hook for replacing the OpenAI chat call.
ChatFn = Callable[[List[dict], dict], str]

# Error text that usually means strict structured output was rejected.
_SCHEMA_REJECTION_MARKERS = (
    "json_schema",
    "response_format",
    "strict",
    "additionalproperties",
    "schema",
)


_REMOVAL_EDIT_RE = re.compile(
    r"\b(?:remove|drop|exclude|delete|take\s+out)\b", re.IGNORECASE
)


def _is_removal_edit(query: str) -> bool:
    """Whether the request explicitly removes an existing filter condition.

    "Without" is intentionally not a removal cue: elsewhere in the pipeline it
    expresses a negative cohort constraint, such as "patients without relapse".
    """
    return _REMOVAL_EDIT_RE.search(query) is not None


def _and_clauses(wire: dict) -> List[dict]:
    """Return top-level AND children without changing non-AND clauses."""
    if set(wire) == {"AND"} and isinstance(wire["AND"], list):
        return deepcopy(wire["AND"])
    return [deepcopy(wire)]


def _direct_fields(clause: dict) -> set[str]:
    """Return fields addressed directly by one non-nested clause."""
    fields: set[str] = set()
    for operator in ("IN", "!=", "GTE", "LTE", "GT", "LT"):
        payload = clause.get(operator)
        if isinstance(payload, dict):
            fields.update(field for field in payload if isinstance(field, str))
    return fields


def _nested_parts(clause: dict):
    """(path, body operator, children) for a nested clause, else None.

    A nested body carries exactly one of AND or OR. Recognising only AND made an
    OR-bodied clause look like a plain clause to the merge below, which then kept
    the old copy and appended the re-emitted one.
    """
    nested = clause.get("nested")
    if not isinstance(nested, dict):
        return None
    path = nested.get("path")
    if not isinstance(path, str):
        return None
    for operator in ("AND", "OR"):
        children = nested.get(operator)
        if isinstance(children, list):
            return path, operator, children
    return None


def _merge_updated_filter(current_filter: dict, addition: dict) -> dict:
    """Keep prior constraints while applying a non-removal edit.

    A new condition for the same field replaces its prior condition. Nested
    conditions on the same path are combined so their fields apply to one event.
    """
    base = _and_clauses(current_filter)
    added = _and_clauses(addition)
    added_top_fields = set().union(
        *(_direct_fields(clause) for clause in added if _nested_parts(clause) is None)
    ) if added else set()

    merged: List[dict] = []
    consumed_nested: set[int] = set()
    for old in base:
        old_nested = _nested_parts(old)
        if old_nested is None:
            if not (_direct_fields(old) & added_top_fields):
                merged.append(old)
            continue

        path, old_operator, old_children = old_nested
        matching = [
            (index, parts)
            for index, new in enumerate(added)
            if (parts := _nested_parts(new)) is not None and parts[0] == path
        ]
        if not matching:
            merged.append(old)
            continue

        # Pooling children is only sound for AND bodies, where AND(a,b) plus
        # AND(c) really is AND(a,b,c). Concatenating OR branches would widen the
        # condition instead of narrowing it, and re-emitting an unchanged clause
        # would double its branches. For anything involving OR the newer clause
        # replaces the old one.
        if old_operator != "AND" or any(op != "AND" for _, (_, op, _) in matching):
            for index, _ in matching:
                merged.append(added[index])
                consumed_nested.add(index)
            continue

        new_children = [child for _, (_, _, children) in matching for child in children]
        new_fields = set().union(*(_direct_fields(child) for child in new_children)) if new_children else set()
        kept_old = [child for child in old_children if not (_direct_fields(child) & new_fields)]
        merged.append({"nested": {"path": path, old_operator: kept_old + new_children}})
        consumed_nested.update(index for index, _ in matching)

    for index, new in enumerate(added):
        if index not in consumed_nested:
            merged.append(new)

    if len(merged) == 1:
        return merged[0]
    return {"AND": merged}


def _env_int(env, key, default):
    """Read an int from env, falling back on bad or missing values."""
    try:
        return int(env[key])
    except (KeyError, ValueError):
        return default


def _env_float(env, key, default):
    """Read a float from env, falling back on bad or missing values."""
    try:
        return float(env[key])
    except (KeyError, ValueError):
        return default


def _env_bool(env, key, default):
    """Read a bool-like env value."""
    raw = env.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


def _env_opt_int(env, key, default):
    """Read an optional int from env."""
    raw = env.get(key)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


@dataclass
class GeneratorConfig:
    """Settings for one filter-generation setup."""

    model: str = DEFAULT_CHAT_MODEL
    embedding_model: str = DEFAULT_EMBED_MODEL
    top_k: int = 12
    max_attempts: int = 3
    temperature: float = 0.0
    use_strict_schema: bool = True
    seed: Optional[int] = None

    @classmethod
    def from_env(cls, env: Optional[dict] = None) -> "GeneratorConfig":
        """Build config from FILTER_GENERATION_* environment variables."""
        env = os.environ if env is None else env
        d = cls()
        return cls(
            model=env.get("FILTER_GENERATION_MODEL", d.model),
            embedding_model=env.get("EMBEDDING_MODEL", d.embedding_model),
            top_k=_env_int(env, "FILTER_GENERATION_TOP_K", d.top_k),
            max_attempts=_env_int(env, "FILTER_GENERATION_MAX_ATTEMPTS", d.max_attempts),
            temperature=_env_float(env, "FILTER_GENERATION_TEMPERATURE", d.temperature),
            use_strict_schema=_env_bool(env, "FILTER_GENERATION_STRICT", d.use_strict_schema),
            seed=_env_opt_int(env, "FILTER_GENERATION_SEED", d.seed),
        )


# Schema for the model-facing tagged filter format.
_CLAUSE = {"$ref": "#/$defs/clause"}

FILTER_JSON_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {"filter": _CLAUSE},
    "required": ["filter"],
    "$defs": {
        "clause": {
            "anyOf": [
                {"$ref": "#/$defs/in_clause"},
                {"$ref": "#/$defs/not_equals_clause"},
                {"$ref": "#/$defs/range_clause"},
                {"$ref": "#/$defs/and_clause"},
                {"$ref": "#/$defs/or_clause"},
                {"$ref": "#/$defs/nested_clause"},
            ]
        },
        # Nested filters must contain an AND or OR body.
        "nested_body": {
            "anyOf": [
                {"$ref": "#/$defs/and_clause"},
                {"$ref": "#/$defs/or_clause"},
            ]
        },
        "in_clause": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "op": {"type": "string", "enum": ["IN"]},
                "field": {"type": "string"},
                "values": {"type": "array", "items": {"type": "string"}},
            },
            "required": ["op", "field", "values"],
        },
        "not_equals_clause": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "op": {"type": "string", "enum": ["!="]},
                "field": {"type": "string"},
                "value": {
                    "anyOf": [
                        {"type": "string"},
                        {"type": "number"},
                        {"type": "boolean"},
                    ]
                },
            },
            "required": ["op", "field", "value"],
        },
        "range_clause": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "op": {"type": "string", "enum": ["GTE", "LTE", "GT", "LT"]},
                "field": {"type": "string"},
                "value": {"type": "number"},
            },
            "required": ["op", "field", "value"],
        },
        "and_clause": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "op": {"type": "string", "enum": ["AND"]},
                "clauses": {"type": "array", "items": _CLAUSE},
            },
            "required": ["op", "clauses"],
        },
        "or_clause": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "op": {"type": "string", "enum": ["OR"]},
                "clauses": {"type": "array", "items": _CLAUSE},
            },
            "required": ["op", "clauses"],
        },
        "nested_clause": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "op": {"type": "string", "enum": ["nested"]},
                "path": {"type": "string"},
                "body": {"$ref": "#/$defs/nested_body"},
            },
            "required": ["op", "path", "body"],
        },
    },
}

FILTER_JSON_SCHEMA["properties"].update(
    {
        "intent": {
            "type": "string",
            "enum": [intent.value for intent in ClinicalIntent],
        },
        "ambiguity": {"type": "array", "items": {"type": "string"}},
    }
)


_TAGGED_SYSTEM_PROMPT = """\
You translate a parsed clinical-cohort query into a single GraphQL filter.

Return exactly one JSON object shaped as
{"filter": <clause>, "intent": "<intent>", "ambiguity": []},
nothing else. If you cannot choose the clinical state semantics safely, put a
short reason in "ambiguity" and use intent "state_unspecified".
A clause is one of:
  {"op": "IN",     "field": "<field>", "values": ["<value>", ...]}
  {"op": "!=",     "field": "<field>", "value": "<value>"}
  {"op": "GTE",    "field": "<field>", "value": <number>}   (also LTE, GT, LT)
  {"op": "AND",    "clauses": [<clause>, ...]}
  {"op": "OR",     "clauses": [<clause>, ...]}
  {"op": "nested", "path": "<table>", "body": <AND or OR clause>}

Rules:
1. Use only the fields and values given to you below. Never invent a field or an
   enum value, and copy enum values exactly. Values under "Recognized terms" were
   already matched to the schema, so use them verbatim even if a field's value
   list is truncated.
2. A plain list of accepted values for one field is a single IN. Use an OR, each
   branch wrapped in its own AND, when the alternatives span different fields or
   nested paths, or when the request reads as a choice between cohorts ("either
   the INRG or the INSTRuCT cohort").
3. Fields shown as "under subject" are top-level: place them directly in the
   top-level AND/OR and never inside a nested clause. A field under a named table
   goes inside a nested clause for that table, together with any timing value
   ("disease_phase") that qualifies that same event. Only one level of nesting
   exists; a nested body cannot contain another nested.
4. Numeric ranges arrive with negation already resolved and units already
   converted to the field's stored unit; apply the number as given and do no
   arithmetic of your own.
5. There is no general NOT operator. For a negated enum/category term listed
   under "Excluded terms", emit a field-level != clause. Drop unsupported
   negation rather than writing it as a positive IN.
6. Assessment descriptor fields are not the same thing as a positive clinical
   finding. "with/has X" usually means positive_existence; "without X" usually
   means negative_existence; "assessed for X" or "X assessment records" means
   record_exists; "regardless of state/status" means any_state. Explicit state
   words such as Present, Absent, Unknown, Positive, or Negative take priority.
   Do not invent assertion fields or state values; a deterministic semantic
   layer will add configured assertion fields after your output.
7. A finding paired with its own result field is stated positively and the
   presence or absence goes in the result field, never in a != on the finding.
   molecular_abnormality works this way: "MYCN amplified" is
   IN molecular_abnormality ["MYCN Amplification"] together with
   IN molecular_abnormality_result ["Present"], and "MYCN non-amplified" is the
   same IN with result ["Absent"]. Negating the finding itself asks for subjects
   tested for something else entirely, and Guppy rejects it.

The examples show clause structure only; for the real answer use only the
candidate fields and values, never the example fields.

Query: INRG males
{"filter": {"op": "AND", "clauses": [{"op": "IN", "field": "consortium", "values": ["INRG"]}, {"op": "IN", "field": "sex", "values": ["Male"]}]}, "intent": "state_unspecified", "ambiguity": []}

Query: subjects in either the INRG or INSTRuCT consortium
{"filter": {"op": "OR", "clauses": [{"op": "AND", "clauses": [{"op": "IN", "field": "consortium", "values": ["INRG"]}]}, {"op": "AND", "clauses": [{"op": "IN", "field": "consortium", "values": ["INSTRuCT"]}]}]}, "intent": "state_unspecified", "ambiguity": []}

Query: patients with metastatic tumors
{"filter": {"op": "nested", "path": "tumor_assessments", "body": {"op": "AND", "clauses": [{"op": "IN", "field": "tumor_classification", "values": ["Metastatic"]}]}}, "intent": "positive_existence", "ambiguity": []}

Query: INRG patients with metastatic tumors
{"filter": {"op": "AND", "clauses": [{"op": "IN", "field": "consortium", "values": ["INRG"]}, {"op": "nested", "path": "tumor_assessments", "body": {"op": "AND", "clauses": [{"op": "IN", "field": "tumor_classification", "values": ["Metastatic"]}]}}]}, "intent": "positive_existence", "ambiguity": []}

Query: patients assessed for metastatic tumors
{"filter": {"op": "nested", "path": "tumor_assessments", "body": {"op": "AND", "clauses": [{"op": "IN", "field": "tumor_classification", "values": ["Metastatic"]}]}}, "intent": "record_exists", "ambiguity": []}

Query: ARMS histology at initial diagnosis
{"filter": {"op": "nested", "path": "histologies", "body": {"op": "AND", "clauses": [{"op": "IN", "field": "disease_phase", "values": ["Initial Diagnosis"]}, {"op": "IN", "field": "histology", "values": ["Alveolar rhabdomyosarcoma (ARMS)"]}]}}, "intent": "record_exists", "ambiguity": []}
"""


def _tagged_to_wire(node: dict) -> dict:
    """Convert model output into the GraphQLFilter wire format."""
    op = node["op"]

    if op == "IN":
        values = node["values"]
        if not isinstance(values, list):
            raise ValueError(f"IN values must be a list, got {type(values).__name__}")
        return {"IN": {node["field"]: list(values)}}

    if op == "!=":
        value = node["value"]
        if isinstance(value, (dict, list)):
            raise ValueError("!= value must be a scalar")
        return {"!=": {node["field"]: value}}

    if op in ("GTE", "LTE", "GT", "LT"):
        return {op: {node["field"]: node["value"]}}

    if op == "AND":
        return {"AND": [_tagged_to_wire(c) for c in node["clauses"]]}

    if op == "OR":
        return {"OR": [_tagged_to_wire(c) for c in node["clauses"]]}

    if op == "nested":
        body = _tagged_to_wire(node["body"])
        inner: dict = {"path": node["path"]}

        if "AND" in body:
            inner["AND"] = body["AND"]
        elif "OR" in body:
            inner["OR"] = body["OR"]
        else:
            # Keep the bad body so Pydantic can produce the structural error.
            inner.update(body)

        return {"nested": inner}

    raise ValueError(f"unknown op {op!r}")


def _partition_ranges(ranges) -> Tuple[list, list]:
    """Split ranges into usable ranges and ranges waiting on unit conversion."""
    usable, deferred = [], []
    for r in ranges:
        # A range bound to a schema field is ready to use; only an unresolved
        # unit-bearing range is deferred until conversion lands.
        if r.field is not None or r.unit is None:
            usable.append(r)
        else:
            deferred.append(r)
    return usable, deferred


@dataclass(frozen=True)
class _ParsedOutput:
    """Model output after JSON parsing and tagged-filter conversion."""

    wire: dict
    intent: Optional[str] = None
    ambiguity: tuple[str, ...] = ()


@dataclass
class _Run:
    """Mutable state for one generate() call."""

    strict: bool
    downgraded: bool = False
    usage: Optional[dict] = None


@dataclass
class GenerationResult:
    """Output of a filter-generation attempt."""

    filter: Optional[GraphQLFilter]
    wire: Optional[dict]
    validation: ValidationResult
    attempts: int
    raw_outputs: List[str]
    model: str
    strict_downgraded: bool = False
    usage: Optional[dict] = None
    dropped_ranges: tuple = ()
    semantic_intent: Optional[str] = None
    # Non-fatal notes about clinical state the query left open; surfaced to the
    # caller so a cohort built without a state condition says so.
    semantic_warnings: tuple = ()

    @property
    def ok(self) -> bool:
        """Whether generation produced a valid filter."""
        return self.filter is not None and self.validation.ok


class FilterGenerator:
    """Generate a validated GraphQLFilter from a user query."""

    def __init__(
        self,
        schema: SchemaIndex,
        normalizer: TermNormalizer,
        retriever: CandidateRetriever,
        *,
        config: Optional[GeneratorConfig] = None,
        chat_fn: Optional[ChatFn] = None,
        client=None,
        semantic_enricher: Optional[SemanticEnricher] = None,
    ):
        self.config = config or GeneratorConfig.from_env()
        if self.config.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.config.max_attempts}")

        self.schema = schema
        self.normalizer = normalizer
        self.retriever = retriever

        self._chat_fn = chat_fn
        self._client = client
        self.semantic_enricher = semantic_enricher
        self.rewriter = FilterRewriter(schema, semantic_enricher=semantic_enricher)

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
        *,
        synonyms_path: Optional[Union[str, Path]] = None,
        numeric_config_path: Optional[Union[str, Path]] = None,
        config: Optional[GeneratorConfig] = None,
        embed_fn=None,
        client=None,
        cache_dir: Optional[Union[str, Path]] = None,
        semantic_config_path: Optional[Union[str, Path]] = None,
    ) -> "FilterGenerator":
        """Build a generator and its helper services from schema files."""
        config = config or GeneratorConfig.from_env()
        schema = SchemaIndex.from_files(pcdc_path, gitops_path)
        normalizer = TermNormalizer.from_files(
            schema, synonyms_path, numeric_config_path=numeric_config_path
        )
        retriever = CandidateRetriever(
            schema,
            embed_fn=embed_fn,
            client=client,
            model=config.embedding_model,
            cache_dir=cache_dir,
        )
        semantic_path = (
            Path(semantic_config_path)
            if semantic_config_path is not None
            else default_semantic_metadata_path()
        )
        semantic_enricher = (
            SemanticEnricher.from_yaml(semantic_path, schema)
            if semantic_path.exists()
            else None
        )
        return cls(
            schema,
            normalizer,
            retriever,
            config=config,
            client=client,
            semantic_enricher=semantic_enricher,
        )

    def generate(self, query: str, *, current_filter: Optional[dict] = None) -> GenerationResult:
        """Run normalization, retrieval, model generation, and validation."""
        nq = self.normalizer.normalize(query)
        candidates = self.retriever.retrieve(nq, top_k=self.config.top_k)

        usable, dropped = _partition_ranges(nq.ranges)

        # Skip the model call if there is nothing reliable to build from.
        if current_filter is None and not nq.terms and not usable and not candidates:
            code = "unconverted_range" if dropped else "no_signal"
            msg = (
                "the only constraint needs unit conversion before it can be used"
                if dropped
                else "no schema terms, usable ranges, or candidate fields were found"
            )
            return GenerationResult(
                filter=None,
                wire=None,
                validation=ValidationResult([ValidationIssue(code, msg)]),
                attempts=0,
                raw_outputs=[],
                model=self.config.model,
                dropped_ranges=tuple(dropped),
            )

        prompt_nq = NormalizedQuery(
            text=nq.text,
            terms=nq.terms,
            ranges=usable,
            negations=nq.negations,
        )
        messages = build_filter_messages(prompt_nq, candidates)

       
        messages[0] = {"role": "system", "content": _TAGGED_SYSTEM_PROMPT}
        if current_filter is not None:
            messages.insert(1, {"role": "user", 
                                          "content": (
                    "You are editing an existing filter, shown here in the "
                    "IN/AND/OR/nested wire form:\n"
                    + json.dumps(current_filter, ensure_ascii=False)
                    + "\nApply the change requested below and return the COMPLETE "
                    "updated filter, keeping every part the request does not mention."
                ),
            })

        run = _Run(strict=self.config.use_strict_schema)
        raw_outputs: List[str] = []
        last_result = ValidationResult([])

        for attempt in range(1, self.config.max_attempts + 1):
            text = self._complete(messages, run)
            raw_outputs.append(text)

            parsed, parse_issue = self._parse(text)
            if parse_issue is not None:
                last_result = ValidationResult([parse_issue])
                messages += self._repair_turn(text, last_result)
                continue

            # The model flagging ambiguity is a reason to say so, not a reason to
            # hand back nothing: a filter without a state condition is the
            # record-existence reading, which is still a usable cohort.
            rewritten = self.rewriter.apply(
                parsed.wire, nq, model_intent=parsed.intent
            )
            wire = rewritten.wire
            semantic_intent = rewritten.intent
            semantic_warnings = list(parsed.ambiguity) + list(rewritten.warnings)

            # A modification updates only the conditions it mentions. Removal
            # requests are left to the model because omission is their intent.
            if current_filter is not None and not _is_removal_edit(query):
                wire = _merge_updated_filter(current_filter, wire)

            if rewritten.issues:
                last_result = ValidationResult(list(rewritten.issues))
                return self._result(
                    None,
                    last_result,
                    attempt,
                    raw_outputs,
                    run,
                    dropped,
                    semantic_intent=semantic_intent,
                    semantic_warnings=semantic_warnings,
                )

            gf, result = self._validate(wire)
            last_result = result

            if result.ok and gf is not None:
                return self._result(
                    gf,
                    result,
                    attempt,
                    raw_outputs,
                    run,
                    dropped,
                    semantic_intent=semantic_intent,
                    semantic_warnings=semantic_warnings,
                )

            messages += self._repair_turn(text, result)

        return self._result(
            None,
            last_result,
            self.config.max_attempts,
            raw_outputs,
            run,
            dropped,
        )

    def _result(
        self,
        gf,
        result,
        attempts,
        raw_outputs,
        run,
        dropped,
        *,
        semantic_intent: Optional[str] = None,
        semantic_warnings: Optional[List[str]] = None,
    ) -> GenerationResult:
        """Create a GenerationResult from the current run state."""
        return GenerationResult(
            filter=gf,
            wire=gf.model_dump(exclude_none=True) if gf is not None else None,
            validation=result,
            attempts=attempts,
            raw_outputs=raw_outputs,
            model=self.config.model,
            strict_downgraded=run.downgraded,
            usage=run.usage,
            dropped_ranges=tuple(dropped),
            semantic_intent=semantic_intent,
            semantic_warnings=tuple(semantic_warnings or ()),
        )

    def _parse(self, text: str):
        """Parse the model response and convert it to wire format."""
        try:
            data = json.loads(text)
        except json.JSONDecodeError as e:
            return None, ValidationIssue("invalid_json", f"output was not JSON: {e}")

        node = data.get("filter") if isinstance(data, dict) else None
        if not isinstance(node, dict):
            return None, ValidationIssue(
                "missing_filter", "output had no object under a 'filter' key"
            )

        try:
            wire = _tagged_to_wire(node)
        except (KeyError, TypeError, ValueError) as e:
            return None, ValidationIssue("bad_tagged_shape", f"could not read filter: {e}")

        ambiguity = data.get("ambiguity", []) if isinstance(data, dict) else []
        if ambiguity is None:
            ambiguity = []
        if not isinstance(ambiguity, list) or not all(
            isinstance(item, str) for item in ambiguity
        ):
            return None, ValidationIssue(
                "bad_tagged_shape",
                "ambiguity must be a list of strings when provided",
            )

        intent = data.get("intent") if isinstance(data, dict) else None
        if intent is not None and not isinstance(intent, str):
            return None, ValidationIssue(
                "bad_tagged_shape",
                "intent must be a string when provided",
            )

        return _ParsedOutput(
            wire=wire,
            intent=intent,
            ambiguity=tuple(ambiguity),
        ), None

    def _validate(self, wire: dict):
        """Validate both the filter shape and its schema references."""
        try:
            gf = GraphQLFilter.model_validate(wire)
        except PydanticValidationError as e:
            return None, ValidationResult([ValidationIssue(CODE_STRUCTURAL, str(e))])

        return gf, validate_filter(gf, self.schema)

    def _repair_turn(self, text: str, result: ValidationResult) -> List[dict]:
        """Build the next chat turn after validation fails."""
        problems = "\n".join(f"- [{i.code}] {i.message}" for i in result.issues)

        return [
            {"role": "assistant", "content": text},
            {
                "role": "user",
                "content": (
                    "That filter has problems:\n"
                    f"{problems}\n"
                    "Return a corrected {\"filter\": <clause>} object. Use only the "
                    "fields and values from the candidate list."
                ),
            },
        ]

    def _response_format(self, strict: bool) -> dict:
        """Choose strict schema output or plain JSON output."""
        if strict:
            return {
                "type": "json_schema",
                "json_schema": {
                    "name": "cohort_filter",
                    "schema": FILTER_JSON_SCHEMA,
                    "strict": True,
                },
            }

        return {"type": "json_object"}

    def _complete(self, messages: List[dict], run: _Run) -> str:
        """Call the chat backend and return the raw content."""
        if self._chat_fn is not None:
            return self._chat_fn(messages, self._response_format(run.strict))

        client = self._get_client()

        try:
            resp = self._create(client, messages, self._response_format(run.strict))
        except Exception as e:
            # Retry once without strict schema if the backend rejects it.
            if run.strict and self._looks_like_schema_rejection(e):
                run.strict = False
                run.downgraded = True
                resp = self._create(client, messages, self._response_format(False))
            else:
                raise

        self._record_usage(resp, run)
        return resp.choices[0].message.content or ""

    def _create(self, client, messages, response_format):
        """Send one chat completion request."""
        kwargs = dict(
            model=self.config.model,
            messages=messages,
            temperature=self.config.temperature,
            response_format=response_format,
        )

        if self.config.seed is not None:
            kwargs["seed"] = self.config.seed

        return client.chat.completions.create(**kwargs)

    @staticmethod
    def _record_usage(resp, run: _Run) -> None:
        """Add token usage from one response to the run total."""
        usage = getattr(resp, "usage", None)
        if usage is None:
            return

        try:
            delta = {
                "prompt_tokens": usage.prompt_tokens,
                "completion_tokens": usage.completion_tokens,
                "total_tokens": usage.total_tokens,
            }
        except AttributeError:
            return

        if run.usage is None:
            run.usage = delta
        else:
            for key, value in delta.items():
                run.usage[key] = run.usage.get(key, 0) + value

    @staticmethod
    def _looks_like_schema_rejection(e: Exception) -> bool:
        """Check whether an exception came from response schema handling."""
        msg = str(e).lower()
        return any(marker in msg for marker in _SCHEMA_REJECTION_MARKERS)

    def _get_client(self):
        """Create the OpenAI client when it is first needed."""
        if self._client is None:
            from openai import OpenAI

            self._client = OpenAI()

        return self._client
