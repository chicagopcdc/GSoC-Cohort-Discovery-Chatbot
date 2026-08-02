"""
Function-calling agent over the cohort tools.

The model decides whether to build/modify a filter (via SessionManager) and
whether to run it for a count (via GuppyClient), looping until it produces a
final reply. Direct OpenAI SDK, no framework: the loop and tools live here, so it
mocks and traces like the rest of the pipeline.

Scope: five tools (build_query, count_cohort, explore_schema, summarize_cohort,
compare_cohort) + per-session memory of the last build + a step cap + a trace.
No clarify-tool (the model asks in plain text), no export or streaming.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

from services.cohort_analyzer import CohortAnalyzer, format_comparison, format_summary
from services.schema_explorer import SchemaExplorer, QUERY_TYPES
from services.session_manager import SessionManager


DEFAULT_AGENT_MODEL = "gpt-4o-mini"

# (messages, tools) -> the assistant message as a dict (role/content/tool_calls).
ChatFn = Callable[[List[dict], List[dict]], dict]


_SYSTEM_PROMPT = """\
You help researchers build and run cohort-discovery queries against a pediatric
cancer data commons.

- To turn a request into a filter, call build_query with the user's wording. It
  also handles follow-up edits like "change consortium to NODAL" or "also add
  males" — just pass the new wording.
- To get the number of matching subjects for the current filter, call
  count_cohort. Only call it after a successful build_query.
- To answer a question about the schema itself (which fields or nested tables
  exist, what values a field allows, or which field a value belongs to), call
  explore_schema instead of building a filter.
- To describe the make-up of the current cohort (its size and how it breaks down
  by sex, race, ethnicity, and consortium), call summarize_cohort. Only after a
  successful build_query.
- To compare the current cohort with another one, call compare_cohort with a
  short description of the other cohort (e.g. "NODAL" or "females"); it is read
  as an edit of the current cohort. Only after a successful build_query.

After the tools run, answer briefly and plainly. If build_query reports errors or
that part of the request could not be expressed, say so rather than guessing. If
the request is ambiguous, ask one short clarifying question instead of calling a
tool. Never invent counts or field names.
"""

_TOOLS: List[dict] = [
    {
        "type": "function",
        "function": {
            "name": "build_query",
            "description": (
                "Turn a natural-language cohort request into a validated filter. "
                "Use it for a new request or to modify the current one."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "The user's cohort request or modification, in natural language.",
                    }
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "count_cohort",
            "description": (
                "Run the most recently built query and return the number of "
                "matching subjects. Call after a successful build_query."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "explore_schema",
            "description": (
                "Look up the filterable PCDC schema. Use for questions about which "
                "fields or nested tables exist, what values a field allows, or which "
                "field a value belongs to — not to build a filter."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query_type": {
                        "type": "string",
                        "enum": list(QUERY_TYPES),
                        "description": (
                            "list_fields (fields of a table, or top-level if no path), "
                            "values (a field's allowed values), describe (a field's "
                            "type/values/description), find_value (which fields have a "
                            "value), list_tables (all nested tables)."
                        ),
                    },
                    "field": {"type": "string", "description": "Field name, for 'values' and 'describe'."},
                    "path": {"type": "string", "description": "Nested table name; for 'list_fields' or to disambiguate a field."},
                    "value": {"type": "string", "description": "An enum value, for 'find_value'."},
                },
                "required": ["query_type"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "summarize_cohort",
            "description": (
                "Summarize the current cohort: total subjects and the breakdown by "
                "sex, race, ethnicity, and consortium. Call after a successful build_query."
            ),
            "parameters": {"type": "object", "properties": {}},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "compare_cohort",
            "description": (
                "Compare the current cohort with another one, side by side. Pass a short "
                "natural-language description of the other cohort (e.g. 'NODAL' or "
                "'females'); it is read as an edit of the current cohort. Call after a "
                "successful build_query."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "comparison_query": {
                        "type": "string",
                        "description": "The other cohort to compare against, in natural language.",
                    }
                },
                "required": ["comparison_query"],
            },
        },
    },
]


@dataclass
class AgentStep:
    tool: str
    arguments: dict
    result: dict


@dataclass
class AgentResult:
    reply: str
    session_id: str
    steps: List[AgentStep] = field(default_factory=list)
    llm_calls: int = 0
    stopped: bool = False        # True if the step cap was hit before a reply


class CohortAgent:
    def __init__(
        self,
        session_manager: SessionManager,
        *,
        guppy_client=None,
        schema_explorer=None,
        cohort_analyzer=None,
        chat_fn: Optional[ChatFn] = None,
        client=None,
        model: str = DEFAULT_AGENT_MODEL,
        max_steps: int = 6,
        temperature: float = 0.0,
    ):
        if max_steps < 1:
            raise ValueError(f"max_steps must be >= 1, got {max_steps}")
        self.session_manager = session_manager
        self.model = model
        self.max_steps = max_steps
        self.temperature = temperature

        self._guppy = guppy_client
        self._explorer = schema_explorer
        self._analyzer = cohort_analyzer
        self._chat_fn = chat_fn
        self._client = client
        self._last_build: Dict[str, Any] = {}   # session_id -> last good BuildResult

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
        *,
        guppy_endpoint: Optional[str] = None,
        token_provider=None,
        session_store=None,
        model: str = DEFAULT_AGENT_MODEL,
        schema_preview_limit: int = 40,
        **builder_kwargs,
    ) -> "CohortAgent":
        sm = SessionManager.from_files(pcdc_path, gitops_path, store=session_store, **builder_kwargs)
        # Reuse the schema the pipeline already loaded; do not read it twice.
        explorer = SchemaExplorer(sm.qb.schema, preview_limit=schema_preview_limit)

        guppy = None
        analyzer = None
        if guppy_endpoint:
            from services.guppy_client import GuppyClient
            guppy = GuppyClient(guppy_endpoint, token_provider=token_provider)
            # Restrict the analyzer to fields this commons actually exposes at the
            # top level, so a default summary field it lacks is dropped, not errored.
            known = [s.name for s in sm.qb.schema.top_level_fields()]
            analyzer = CohortAnalyzer(guppy, known_fields=known)
        return cls(sm, guppy_client=guppy, schema_explorer=explorer,
                   cohort_analyzer=analyzer, model=model)

    def chat(self, session_id: str, message: str) -> AgentResult:
        messages: List[dict] = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": message},
        ]
        steps: List[AgentStep] = []
        calls = 0

        for _ in range(self.max_steps):
            assistant = self._complete(messages, _TOOLS)
            calls += 1
            messages.append(assistant)

            tool_calls = assistant.get("tool_calls") or []
            if not tool_calls:
                return AgentResult(
                    reply=assistant.get("content") or "",
                    session_id=session_id,
                    steps=steps,
                    llm_calls=calls,
                )

            for call in tool_calls:
                fn = call.get("function", {})
                name = fn.get("name", "")
                try:
                    args = json.loads(fn.get("arguments") or "{}")
                except json.JSONDecodeError:
                    args = {}

                result = self._dispatch(session_id, name, args)
                steps.append(AgentStep(tool=name, arguments=args, result=result))
                messages.append({
                    "role": "tool",
                    "tool_call_id": call.get("id"),
                    "content": json.dumps(result, ensure_ascii=False),
                })

        return AgentResult(
            reply="I wasn't able to finish that in a reasonable number of steps.",
            session_id=session_id,
            steps=steps,
            llm_calls=calls,
            stopped=True,
        )

    def reset(self, session_id: str) -> None:
        self.session_manager.reset(session_id)
        self._last_build.pop(session_id, None)

    # --- tool dispatch 

    def _dispatch(self, session_id: str, name: str, args: dict) -> dict:
        try:
            if name == "build_query":
                return self._build(session_id, str(args.get("query", "")))
            if name == "count_cohort":
                return self._count(session_id)
            if name == "explore_schema":
                return self._explore(args)
            if name == "summarize_cohort":
                return self._summarize(session_id)
            if name == "compare_cohort":
                return self._compare(session_id, str(args.get("comparison_query", "")))
            return {"error": f"unknown tool {name!r}"}
        except Exception as e:  # noqa: BLE001
            # Tool errors come back as a result, not an exception, so the loop survives.
            return {"error": f"tool {name!r} failed: {type(e).__name__}: {e}"}

    def _build(self, session_id: str, query: str) -> dict:
        if not query.strip():
            return {"error": "empty query"}
        turn = self.session_manager.turn(session_id, query)
        build = turn.build
        # Keep only a successful build; a failed one must not wipe the last good query.
        if build.ok:
            self._last_build[session_id] = build
        return {
            "mode": turn.mode,
            "ok": build.ok,
            "filter": build.wire,
            "errors": list(build.errors),
            "warnings": list(build.warnings),
        }

    def _count(self, session_id: str) -> dict:
        build = self._last_build.get(session_id)
        if build is None or not build.ok or build.graphql is None:
            return {"error": "no valid query has been built yet; call build_query first"}
        if self._guppy is None:
            return {"error": "execution is not available (no Guppy client configured)"}
        res = self._guppy.execute(build.graphql, data_type=build.data_type)
        if not res.ok:
            return {"error": "; ".join(res.errors) or "execution failed"}
        return {"total_count": res.total_count, "histograms": res.histograms}

    def _explore(self, args: dict) -> dict:
        if self._explorer is None:
            return {"error": "schema exploration is not available"}
        result = self._explorer.answer(
            str(args.get("query_type", "")),
            field=args.get("field"),
            path=args.get("path"),
            value=args.get("value"),
        )
        # Return the human-readable text only; the model relays it. The full
        # structured data stays out of the prompt to keep context lean.
        return {"kind": result.kind, "text": result.text}

    def _summarize(self, session_id: str) -> dict:
        build = self._last_build.get(session_id)
        if build is None or not build.ok or build.wire is None:
            return {"error": "no cohort built yet; call build_query first"}
        if self._analyzer is None:
            return {"error": "cohort analysis is not available (no Guppy client configured)"}

        summary = self._analyzer.summarize(build.wire, label="current cohort")
        if summary.errors:
            return {"error": "; ".join(summary.errors)}
        out: Dict[str, Any] = {"text": format_summary(summary)}
        if summary.warnings:
            out["warnings"] = list(summary.warnings)
        return out

    def _compare(self, session_id: str, comparison_query: str) -> dict:
        if not comparison_query.strip():
            return {"error": "empty comparison query"}
        build = self._last_build.get(session_id)
        if build is None or not build.ok or build.wire is None:
            return {"error": "no cohort built yet; call build_query first"}
        if self._analyzer is None:
            return {"error": "cohort analysis is not available (no Guppy client configured)"}

        # Build cohort B as an edit of the current cohort A, without touching
        # session state: this filter is a throwaway used only for the comparison,
        # so it is never stored in _last_build or the session store.
        b = self.session_manager.qb.build(comparison_query, current_filter=build.wire)
        if b.wire is None:
            return {"error": "; ".join(b.errors) or "could not build the comparison cohort"}

        comparison = self._analyzer.compare(
            build.wire, b.wire, label_a="current", label_b=comparison_query.strip(),
        )
        if comparison.errors:
            return {"error": "; ".join(comparison.errors)}

        out: Dict[str, Any] = {"text": format_comparison(comparison)}
        # Surface warnings from building B (e.g. dropped ranges) next to the
        # analyzer's own, so the model can caveat the comparison cohort.
        warnings = list(comparison.warnings) + [f"comparison cohort: {w}" for w in b.warnings]
        if warnings:
            out["warnings"] = warnings
        return out

    # --- model call -----------------------------------------------------------

    def _complete(self, messages: List[dict], tools: List[dict]) -> dict:
        if self._chat_fn is not None:
            return self._chat_fn(messages, tools)

        client = self._get_client()
        resp = client.chat.completions.create(
            model=self.model,
            messages=messages,
            tools=tools,
            temperature=self.temperature,
            parallel_tool_calls=False,
        )
        return resp.choices[0].message.model_dump(exclude_none=True)

    def _get_client(self):
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI()
        return self._client