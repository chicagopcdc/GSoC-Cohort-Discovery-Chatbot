"""
Per-session conversational state for the query builder.
Storage only. Deciding whether a turn is a new query or a modification, and
applying a modification, belong to the conversational orchestrator that uses
this store. All writes go through SessionStore.record so they stay under the
store lock; callers should not mutate a returned SessionState directly.
"""

from __future__ import annotations

import copy
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional


def _require_session_id(session_id: str) -> str:
    if not isinstance(session_id, str) or not session_id.strip():
        raise ValueError("session_id must be a non-empty string")
    return session_id


@dataclass
class Turn:
    text: str
    filter: Optional[dict]        # wire filter produced this turn, None on failure
    ok: bool
    timestamp: float              # wall-clock (time.time), for display / history


@dataclass
class SessionState:
    session_id: str
    data_type: str = "subject"
    current_filter: Optional[dict] = None      # the active filter a modify turn edits
    turns: List[Turn] = field(default_factory=list)
    max_turns: Optional[int] = None            # cap on retained history, None = unbounded
    created: float = field(default_factory=time.time)           # wall-clock, for display
    last_active: float = field(default_factory=time.monotonic)  # monotonic, for eviction

    def apply_turn(self, text: str, filter_: Optional[dict], ok: bool) -> Turn:
        """Append a turn. Called by SessionStore under its lock; do not call this
        directly from request handlers (it is not self-synchronized)."""
        # Store our own copy so a caller mutating the passed-in dict later can't
        # alter this session's history or active filter.
        stored = copy.deepcopy(filter_) if filter_ is not None else None
        turn = Turn(text=text, filter=stored, ok=ok, timestamp=time.time())
        self.turns.append(turn)
        if self.max_turns is not None and len(self.turns) > self.max_turns:
            del self.turns[: len(self.turns) - self.max_turns]
        # Only a successful turn advances the filter a modify turn would edit; a
        # failed turn stays in history but must not clobber the last good filter.
        if ok and stored is not None:
            self.current_filter = stored
        self.last_active = time.monotonic()
        return turn

    @property
    def turn_count(self) -> int:
        return len(self.turns)


class SessionStore:
    """In-memory, thread-safe session store with optional idle/size eviction."""

    def __init__(
        self,
        *,
        ttl_seconds: Optional[float] = None,
        max_sessions: Optional[int] = None,
        max_turns_per_session: Optional[int] = None,
    ):
        if ttl_seconds is not None and ttl_seconds <= 0:
            raise ValueError(f"ttl_seconds must be > 0, got {ttl_seconds}")
        if max_sessions is not None and max_sessions < 1:
            raise ValueError(f"max_sessions must be >= 1, got {max_sessions}")
        if max_turns_per_session is not None and max_turns_per_session < 1:
            raise ValueError(f"max_turns_per_session must be >= 1, got {max_turns_per_session}")
        self._ttl = ttl_seconds
        self._max = max_sessions
        self._max_turns = max_turns_per_session
        self._lock = threading.Lock()
        self._sessions: Dict[str, SessionState] = {}

    def get(self, session_id: str) -> Optional[SessionState]:
        _require_session_id(session_id)
        with self._lock:
            self._evict_expired_locked()
            return self._sessions.get(session_id)

    def get_or_create(self, session_id: str, *, data_type: str = "subject") -> SessionState:
        _require_session_id(session_id)
        with self._lock:
            self._evict_expired_locked()
            state = self._get_or_create_locked(session_id, data_type)
            self._enforce_capacity_locked()
            return state

    def record(
        self,
        session_id: str,
        text: str,
        filter_: Optional[dict],
        ok: bool,
        *,
        data_type: str = "subject",
    ) -> Turn:
        """Append a turn under the store lock. This is the concurrency-safe write
        path; prefer it over mutating a returned SessionState."""
        _require_session_id(session_id)
        with self._lock:
            self._evict_expired_locked()
            state = self._get_or_create_locked(session_id, data_type)
            turn = state.apply_turn(text, filter_, ok)
            self._enforce_capacity_locked()
            return turn

    def touch(self, session_id: str) -> None:
        """Mark a session active without recording a turn, for callers that want
        access-based keep-alive (get() alone does not refresh idle time)."""
        _require_session_id(session_id)
        with self._lock:
            state = self._sessions.get(session_id)
            if state is not None:
                state.last_active = time.monotonic()

    def clear(self, session_id: str) -> None:
        _require_session_id(session_id)
        with self._lock:
            self._sessions.pop(session_id, None)

    def active_count(self) -> int:
        with self._lock:
            self._evict_expired_locked()
            return len(self._sessions)

    def _get_or_create_locked(self, session_id: str, data_type: str) -> SessionState:
        state = self._sessions.get(session_id)
        if state is None:
            state = SessionState(
                session_id=session_id,
                data_type=data_type,
                max_turns=self._max_turns,
            )
            self._sessions[session_id] = state
        # data_type is fixed at creation; a different value passed on a later call
        # for an existing session is intentionally ignored (one session = one data
        # type).
        return state

    def _evict_expired_locked(self) -> None:
        if self._ttl is None:
            return
        cutoff = time.monotonic() - self._ttl
        for sid in [s for s, st in self._sessions.items() if st.last_active < cutoff]:
            del self._sessions[sid]

    def _enforce_capacity_locked(self) -> None:
        if self._max is None or len(self._sessions) <= self._max:
            return
        # Drop least-recently-active first. A new session's last_active is its
        # creation time (the most recent monotonic value), so it sorts last and is
        # not dropped here.
        ordered = sorted(self._sessions.items(), key=lambda kv: kv[1].last_active)
        for sid, _ in ordered[: len(self._sessions) - self._max]:
            del self._sessions[sid]