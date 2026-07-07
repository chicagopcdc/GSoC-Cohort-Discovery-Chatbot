"""
Conversational orchestration on top of the query builder.

Routes each turn to either a fresh build or a modification of the session's
current filter, and records the outcome in the session store so the next turn
can build on it. This is what makes a follow-up like "change consortium to
NODAL" keep the rest of the previous filter instead of starting over.

Scope (intentional boundaries):
  - In:  per-session turns, heuristic new-vs-modify routing, recording results.
  - Out: LLM-based intent classification, deterministic structural editing,
         undo/redo or branching history, persistence (the store is in-memory).
A modification is delegated to the generation layer: it is a normal build seeded
with the current filter, so there is a single path for producing and validating
filters.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Sequence, Tuple, Union

from services.query_builder_v2 import BuildResult, QueryBuilder
from services.session_store import SessionStore


# A turn is treated as a modification only when the session already has a filter
# AND the text opens with / contains one of these cues. Conservative on purpose:
# when unsure, start a fresh query rather than silently edit the previous one.
_DEFAULT_MODIFY_CUES: Tuple[str, ...] = (
    "change", "replace", "instead", "make it", "switch", "update", "set ", "rename",
    "add", "also", "include", "as well", "plus",
    "remove", "drop", "without", "exclude", "delete", "take out",
    "narrow", "broaden", "restrict", "expand",
)


@dataclass
class TurnResult:
    session_id: str
    mode: str            # "new" or "modify"
    build: BuildResult


class SessionManager:
    def __init__(
        self,
        query_builder: QueryBuilder,
        store: Optional[SessionStore] = None,
        *,
        modify_cues: Optional[Sequence[str]] = None,
    ):
        self.qb = query_builder
        self.store = store or SessionStore()
        self._cues = tuple(modify_cues) if modify_cues is not None else _DEFAULT_MODIFY_CUES

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
        *,
        store: Optional[SessionStore] = None,
        **builder_kwargs,
    ) -> "SessionManager":
        qb = QueryBuilder.from_files(pcdc_path, gitops_path, **builder_kwargs)
        return cls(qb, store)

    def turn(
        self,
        session_id: str,
        text: str,
        *,
        data_type: Optional[str] = None,
    ) -> TurnResult:
        state = self.store.get(session_id)
        base = state.current_filter if state else None
        dt = data_type or (state.data_type if state else self.qb.default_data_type)

        if base is not None and self._looks_like_modification(text):
            build = self.qb.build(text, current_filter=base, data_type=dt)
            mode = "modify"
        else:
            build = self.qb.build(text, data_type=dt)
            mode = "new"

        # store.record is the lock-safe write path (and keeps the last good filter
        # if this turn failed).
        self.store.record(session_id, text, build.wire, build.ok, data_type=dt)
        return TurnResult(session_id=session_id, mode=mode, build=build)

    def reset(self, session_id: str) -> None:
        self.store.clear(session_id)

    def _looks_like_modification(self, text: str) -> bool:
        t = text.strip().lower()
        if not t:
            return False
        return any(t.startswith(cue) or f" {cue}" in t for cue in self._cues)