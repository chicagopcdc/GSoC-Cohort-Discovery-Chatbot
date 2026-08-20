"""Benchmark case loading and reference validation."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional, Set, Tuple, Union

_FILTER_OPS = {"AND", "OR", "IN", "!=", "nested", "GTE", "LTE", "GT", "LT"}


@dataclass
class BenchmarkCase:
    name: str
    natural_language: str
    expected_filter: Optional[dict]
    source: str = ""
    difficulty: Optional[str] = None
    group: Optional[str] = None
    session_id: Optional[str] = None
    notes: str = ""

    def __post_init__(self) -> None:
        if self.difficulty is None and isinstance(self.expected_filter, dict):
            self.difficulty = classify_difficulty(self.expected_filter)


@dataclass(frozen=True)
class ReferenceIssue:
    case: str
    code: str
    message: str
    field: Optional[str] = None
    path: Optional[str] = None


def _looks_like_filter(d: Any) -> bool:
    return isinstance(d, dict) and len(d) == 1 and next(iter(d)) in _FILTER_OPS


def _source(d: dict) -> str:
    s = d.get("source")
    if s:
        return str(s)
    gts = d.get("ground_truth_source")
    if isinstance(gts, dict):
        return str(gts.get("csv") or gts.get("note") or "")
    if isinstance(gts, str):
        return gts
    return ""


def case_from_dict(d: dict) -> BenchmarkCase:
    """Build a case from any supported case-file shape."""
    expected = d.get("expected_filter")
    if expected is None:
        expected = d.get("ground_truth")
    if not _looks_like_filter(expected):
        expected = None

    return BenchmarkCase(
        name=str(d.get("name") or d.get("id") or ""),
        natural_language=str(d.get("natural_language") or d.get("text") or ""),
        expected_filter=expected,
        source=_source(d),
        difficulty=d.get("difficulty"),
        group=d.get("group") or d.get("tier"),
        session_id=d.get("session_id"),
        notes=str(d.get("notes") or d.get("actual_note") or ""),
    )


def _parse_wire_filter(raw: Optional[str]) -> Tuple[Optional[dict], Optional[str]]:
    """Parse a CSV graphql_object cell without dropping bad rows."""
    if not raw or not raw.strip():
        return None, "no graphql_object"
    try:
        obj = json.loads(raw)
    except (ValueError, TypeError):
        return None, "graphql_object is not valid JSON"
    if not _looks_like_filter(obj):
        return None, "graphql_object is not a wire filter"
    return obj, None


def load_csv_cases(path: Union[str, Path]) -> List[BenchmarkCase]:
    """Load cases from an Amanuensis CSV export."""
    path = Path(path)
    cases: List[BenchmarkCase] = []
    with path.open(newline="") as f:
        for i, row in enumerate(csv.DictReader(f)):
            name = (row.get("name") or "").strip() or f"row-{i + 1}"
            nl = (row.get("llm_result") or "").strip() or name
            expected, reason = _parse_wire_filter(row.get("graphql_object"))

            notes: List[str] = []
            provenance = (row.get("filter_object") or "").strip()
            if provenance:
                notes.append(f"filter_object={provenance}")
            if reason:
                notes.append(f"ungradeable: {reason}")

            cases.append(BenchmarkCase(
                name=name,
                natural_language=nl,
                expected_filter=expected,
                source=f"amanuensis_csv:{path.name}",
                group="amanuensis",
                notes=" | ".join(notes),
            ))
    return cases


def load_cases(path: Union[str, Path]) -> List[BenchmarkCase]:
    """Load cases from CSV, JSON, or YAML."""
    path = Path(path)
    if path.suffix.lower() == ".csv":
        return load_csv_cases(path)
    raw = path.read_text()
    if path.suffix.lower() in (".yaml", ".yml"):
        import yaml
        data = yaml.safe_load(raw)
    else:
        data = json.loads(raw)
    if isinstance(data, dict):
        data = data.get("cases", [data])
    return [case_from_dict(d) for d in data]


def validate_references(cases: List[BenchmarkCase], schema: Any) -> List[ReferenceIssue]:
    """Check reference filters against the active schema."""
    from services.filter_validator import validate_dict

    issues: List[ReferenceIssue] = []
    for case in cases:
        if case.expected_filter is None:
            continue
        result = validate_dict(case.expected_filter, schema)
        for issue in result.issues:
            issues.append(ReferenceIssue(
                case=case.name,
                code=issue.code,
                message=issue.message,
                field=issue.field,
                path=issue.path,
            ))
    return issues


def _scan(node: Any, paths: Set[Optional[str]], ops: Set[str]) -> None:
    if not isinstance(node, dict) or len(node) != 1:
        return
    (op, body), = node.items()
    ops.add(op)
    if op in ("AND", "OR"):
        for child in (body if isinstance(body, list) else []):
            _scan(child, paths, ops)
    elif op == "nested" and isinstance(body, dict):
        paths.add(body.get("path"))
        for key in ("AND", "OR"):
            if isinstance(body.get(key), list):
                for child in body[key]:
                    _scan(child, paths, ops)


def classify_difficulty(f: dict) -> str:
    """Bucket filters by the amount of nesting and boolean structure."""
    paths: Set[Optional[str]] = set()
    ops: Set[str] = set()
    _scan(f, paths, ops)
    nested = {p for p in paths if p is not None}
    has_or = "OR" in ops
    if len(nested) >= 2 or (has_or and nested):
        return "complex"
    if nested or has_or:
        return "medium"
    return "simple"
