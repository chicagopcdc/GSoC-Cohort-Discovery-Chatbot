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


_TAGGED_SYSTEM_PROMPT = """\
You translate a parsed clinical-cohort query into a single GraphQL filter.

Return exactly one JSON object shaped as {"filter": <clause>}, nothing else.
A clause is one of:
  {"op": "IN",     "field": "<field>", "values": ["<value>", ...]}
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
3. A field under a nested path goes inside a nested clause with that path. Only
   one level of nesting exists; a nested body cannot contain another nested.
4. Numeric ranges arrive with negation already resolved; apply them as given and
   do no arithmetic of your own.
5. There is no NOT operator. A negated enum/category term ("not metastatic")
   cannot be expressed -- drop it rather than writing it as a positive IN.

The examples show clause structure only; for the real answer use only the
candidate fields and values, never the example fields.

Query: INRG males
{"filter": {"op": "AND", "clauses": [{"op": "IN", "field": "consortium", "values": ["INRG"]}, {"op": "IN", "field": "sex", "values": ["Male"]}]}}

Query: subjects in either the INRG or INSTRuCT consortium
{"filter": {"op": "OR", "clauses": [{"op": "AND", "clauses": [{"op": "IN", "field": "consortium", "values": ["INRG"]}]}, {"op": "AND", "clauses": [{"op": "IN", "field": "consortium", "values": ["INSTRuCT"]}]}]}}

Query: patients with metastatic tumors
{"filter": {"op": "nested", "path": "tumor_assessments", "body": {"op": "AND", "clauses": [{"op": "IN", "field": "tumor_classification", "values": ["Metastatic"]}]}}}
"""


def _tagged_to_wire(node: dict) -> dict:
    """Convert model output into the GraphQLFilter wire format."""
    op = node["op"]

    if op == "IN":
        values = node["values"]
        if not isinstance(values, list):
            raise ValueError(f"IN values must be a list, got {type(values).__name__}")
        return {"IN": {node["field"]: list(values)}}

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
    ):
        self.config = config or GeneratorConfig.from_env()
        if self.config.max_attempts < 1:
            raise ValueError(f"max_attempts must be >= 1, got {self.config.max_attempts}")

        self.schema = schema
        self.normalizer = normalizer
        self.retriever = retriever

        self._chat_fn = chat_fn
        self._client = client

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
        *,
        synonyms_path: Optional[Union[str, Path]] = None,
        config: Optional[GeneratorConfig] = None,
        embed_fn=None,
        client=None,
        cache_dir: Optional[Union[str, Path]] = None,
    ) -> "FilterGenerator":
        """Build a generator and its helper services from schema files."""
        config = config or GeneratorConfig.from_env()
        schema = SchemaIndex.from_files(pcdc_path, gitops_path)
        normalizer = TermNormalizer.from_files(schema, synonyms_path)
        retriever = CandidateRetriever(
            schema,
            embed_fn=embed_fn,
            client=client,
            model=config.embedding_model,
            cache_dir=cache_dir,
        )
        return cls(schema, normalizer, retriever, config=config, client=client)

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

            wire, parse_issue = self._parse(text)
            if parse_issue is not None:
                last_result = ValidationResult([parse_issue])
                messages += self._repair_turn(text, last_result)
                continue

            gf, result = self._validate(wire)
            last_result = result

            if result.ok and gf is not None:
                return self._result(gf, result, attempt, raw_outputs, run, dropped)

            messages += self._repair_turn(text, result)

        return self._result(
            None,
            last_result,
            self.config.max_attempts,
            raw_outputs,
            run,
            dropped,
        )

    def _result(self, gf, result, attempts, raw_outputs, run, dropped) -> GenerationResult:
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

        return wire, None

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