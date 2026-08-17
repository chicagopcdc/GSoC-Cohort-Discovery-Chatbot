"""Run benchmark cases and collect comparison metrics."""

from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

from evaluation.cases import BenchmarkCase
from evaluation.comparator import Comparison, ErrorCategory, compare

BuildFn = Callable[[BenchmarkCase], "BuildOutcome"]
ExecuteFn = Callable[[dict], Any]


@dataclass
class BuildOutcome:
    wire: Optional[dict]
    ok: bool = False
    tokens: Optional[int] = None
    attempts: Optional[int] = None
    errors: List[str] = field(default_factory=list)


@dataclass
class CaseResult:
    case: BenchmarkCase
    generated: Optional[dict]
    comparison: Comparison
    ok: bool
    latency_ms: float
    tokens: Optional[int]
    attempts: Optional[int]
    errors: List[str]
    execution_match: Optional[bool] = None


def _outcome_from_build(build: Any) -> BuildOutcome:
    """Read the fields exposed by a BuildResult-like object."""
    gen = getattr(build, "generation", None)
    usage = getattr(gen, "usage", None)
    tokens = usage.get("total_tokens") if isinstance(usage, dict) else None
    return BuildOutcome(
        wire=getattr(build, "wire", None),
        ok=bool(getattr(build, "ok", False)),
        tokens=tokens,
        attempts=getattr(gen, "attempts", None),
        errors=list(getattr(build, "errors", ()) or ()),
    )


def query_builder_build_fn(qb: Any) -> BuildFn:
    """Build each case without session history."""
    def build_fn(case: BenchmarkCase) -> BuildOutcome:
        return _outcome_from_build(qb.build(case.natural_language))
    return build_fn


def session_build_fn(sm: Any) -> BuildFn:
    """Build cases through SessionManager when session_id is present."""
    def build_fn(case: BenchmarkCase) -> BuildOutcome:
        sid = case.session_id or case.name
        turn = sm.turn(sid, case.natural_language)
        return _outcome_from_build(turn.build)
    return build_fn


class Evaluator:
    def __init__(self, build_fn: BuildFn, *, execute_fn: Optional[ExecuteFn] = None):
        self._build_fn = build_fn
        self._execute_fn = execute_fn

    def run(self, cases: List[BenchmarkCase]) -> "EvalReport":
        return EvalReport([self._run_one(c) for c in cases])

    def _run_one(self, case: BenchmarkCase) -> CaseResult:
        t0 = time.perf_counter()
        try:
            outcome = self._build_fn(case)
        except Exception as e:  # noqa: BLE001
            outcome = BuildOutcome(wire=None, ok=False, errors=[f"{type(e).__name__}: {e}"])
        latency_ms = (time.perf_counter() - t0) * 1000

        cmp = compare(outcome.wire, case.expected_filter)
        exec_match = self._execution_match(case, outcome) if self._execute_fn else None

        return CaseResult(
            case=case, generated=outcome.wire, comparison=cmp, ok=outcome.ok,
            latency_ms=round(latency_ms, 1), tokens=outcome.tokens,
            attempts=outcome.attempts, errors=outcome.errors, execution_match=exec_match,
        )

    def _execution_match(self, case: BenchmarkCase, outcome: BuildOutcome) -> Optional[bool]:
        if outcome.wire is None or case.expected_filter is None:
            return False
        try:
            return self._execute_fn(outcome.wire) == self._execute_fn(case.expected_filter)
        except Exception:  # noqa: BLE001
            return None


def evaluate(cases: List[BenchmarkCase], build_fn: BuildFn,
             *, execute_fn: Optional[ExecuteFn] = None) -> "EvalReport":
    return Evaluator(build_fn, execute_fn=execute_fn).run(cases)


def _mean(values) -> Optional[float]:
    vals = [v for v in values if v is not None]
    return sum(vals) / len(vals) if vals else None


def _percentile(values: List[float], p: float) -> Optional[float]:
    if not values:
        return None
    s = sorted(values)
    k = (len(s) - 1) * p
    lo = int(k)
    hi = min(lo + 1, len(s) - 1)
    return s[lo] + (s[hi] - s[lo]) * (k - lo)


_DIFFICULTY_ORDER = ["simple", "medium", "complex", "unknown"]


@dataclass
class EvalReport:
    results: List[CaseResult]

    @property
    def gradeable(self) -> List[CaseResult]:
        return [r for r in self.results if r.case.expected_filter is not None]

    @property
    def skipped(self) -> List[CaseResult]:
        return [r for r in self.results if r.case.expected_filter is None]

    def exact(self):
        g = self.gradeable
        return sum(1 for r in g if r.comparison.exact_match), len(g)

    def structural(self):
        g = self.gradeable
        return sum(1 for r in g if r.comparison.structural_match), len(g)

    def mean_field(self):
        return _mean(r.comparison.field_accuracy for r in self.gradeable)

    def mean_value(self):
        return _mean(r.comparison.value_accuracy for r in self.gradeable)

    def mean_structure(self):
        return _mean(r.comparison.structure_accuracy for r in self.gradeable)

    def execution(self):
        scored = [r for r in self.gradeable if r.execution_match is not None]
        if not scored:
            return None
        return sum(1 for r in scored if r.execution_match), len(scored)

    def latency(self) -> Optional[Dict[str, float]]:
        vals = [r.latency_ms for r in self.results if r.latency_ms is not None]
        if not vals:
            return None
        return {"avg": sum(vals) / len(vals), "p50": _percentile(vals, 0.5), "p95": _percentile(vals, 0.95)}

    def avg_tokens(self) -> Optional[float]:
        vals = [r.tokens for r in self.results if r.tokens]
        return sum(vals) / len(vals) if vals else None

    def by_difficulty(self):
        out: Dict[str, List[int]] = {}
        for r in self.gradeable:
            d = r.case.difficulty or "unknown"
            bucket = out.setdefault(d, [0, 0])
            bucket[0] += 1 if r.comparison.exact_match else 0
            bucket[1] += 1
        ordered = [(d, tuple(out[d])) for d in _DIFFICULTY_ORDER if d in out]
        ordered += [(d, tuple(v)) for d, v in out.items() if d not in _DIFFICULTY_ORDER]
        return ordered

    def failure_patterns(self) -> "Counter[ErrorCategory]":
        c: "Counter[ErrorCategory]" = Counter()
        for r in self.gradeable:
            if not r.comparison.exact_match:
                for e in r.comparison.errors:
                    c[e.category] += 1
        return c

    def as_dict(self) -> dict:
        ex_n, ex_tot = self.exact()
        st_n, st_tot = self.structural()
        execu = self.execution()
        return {
            "total": len(self.results),
            "gradeable": len(self.gradeable),
            "skipped": [r.case.name for r in self.skipped],
            "exact_match": {"n": ex_n, "total": ex_tot},
            "structural_match": {"n": st_n, "total": st_tot},
            "field_accuracy": self.mean_field(),
            "value_accuracy": self.mean_value(),
            "structure_accuracy": self.mean_structure(),
            "execution_equiv": ({"n": execu[0], "total": execu[1]} if execu else None),
            "latency_ms": self.latency(),
            "avg_tokens": self.avg_tokens(),
            "by_difficulty": {d: {"exact": n, "total": t} for d, (n, t) in self.by_difficulty()},
            "failure_patterns": {cat.value: n for cat, n in self.failure_patterns().most_common()},
            "cases": [
                {
                    "name": r.case.name,
                    "difficulty": r.case.difficulty,
                    "ok": r.ok,
                    "latency_ms": r.latency_ms,
                    "tokens": r.tokens,
                    "comparison": r.comparison.as_dict(),
                    "generated": r.generated,
                    "expected": r.case.expected_filter,
                }
                for r in self.results
            ],
        }
