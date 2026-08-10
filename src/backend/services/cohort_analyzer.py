"""Tool 4: deterministic cohort summaries and comparisons."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple

from services.graphql_template import build_aggregation_query


# v1 keeps this to top-level categorical fields.
DEFAULT_SUMMARY_FIELDS: Tuple[str, ...] = ("sex", "race", "ethnicity", "consortium")

_VALID_FIELD = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass
class Bucket:
    key: str
    count: Optional[int]
    pct: Optional[float]
    masked: bool = False


@dataclass
class Distribution:
    field: str
    total: Optional[int]
    buckets: List[Bucket]


@dataclass
class CohortSummary:
    label: str
    total: Optional[int]
    total_masked: bool
    distributions: List[Distribution]
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict:
        return {
            "label": self.label,
            "total": self.total,
            "total_masked": self.total_masked,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "distributions": [
                {
                    "field": d.field,
                    "total": d.total,
                    "buckets": [
                        {"key": b.key, "count": b.count, "pct": b.pct, "masked": b.masked}
                        for b in d.buckets
                    ],
                }
                for d in self.distributions
            ],
        }


@dataclass
class ComparisonRow:
    field: str
    key: str
    a_count: Optional[int]
    a_pct: Optional[float]
    b_count: Optional[int]
    b_pct: Optional[float]
    delta_pct: Optional[float]


@dataclass
class CohortComparison:
    label_a: str
    label_b: str
    total_a: Optional[int]
    total_b: Optional[int]
    total_a_masked: bool
    total_b_masked: bool
    total_delta: Optional[int]
    rows: List[ComparisonRow]
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors

    def as_dict(self) -> dict:
        return {
            "label_a": self.label_a,
            "label_b": self.label_b,
            "total_a": self.total_a,
            "total_b": self.total_b,
            "total_a_masked": self.total_a_masked,
            "total_b_masked": self.total_b_masked,
            "total_delta": self.total_delta,
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "rows": [
                {
                    "field": r.field, "key": r.key,
                    "a_count": r.a_count, "a_pct": r.a_pct,
                    "b_count": r.b_count, "b_pct": r.b_pct,
                    "delta_pct": r.delta_pct,
                }
                for r in self.rows
            ],
        }


class CohortAnalyzer:
    def __init__(
        self,
        guppy_client,
        *,
        fields: Sequence[str] = DEFAULT_SUMMARY_FIELDS,
        known_fields: Optional[Sequence[str]] = None,
        data_type: str = "subject",
        accessibility: Optional[str] = "all",
    ):
        self._guppy = guppy_client
        self._data_type = data_type
        self._accessibility = accessibility
        self._default_fields = list(fields)
        self._allowed = set(known_fields) if known_fields is not None else None

    def _select_fields(self, fields: Optional[Sequence[str]]) -> Tuple[List[str], List[str]]:
        """Pick safe histogram fields and report anything dropped."""
        requested = list(fields) if fields is not None else list(self._default_fields)
        seen: set = set()
        selected: List[str] = []
        invalid: List[str] = []
        unknown: List[str] = []
        for f in requested:
            key = f if isinstance(f, str) else repr(f)
            if key in seen:
                continue
            seen.add(key)
            if not isinstance(f, str) or not _VALID_FIELD.match(f):
                invalid.append(key)
            elif self._allowed is not None and f not in self._allowed:
                unknown.append(f)
            else:
                selected.append(f)
        warnings: List[str] = []
        if invalid:
            warnings.append(f"ignored invalid field name(s): {', '.join(invalid)}")
        if unknown:
            warnings.append(f"dropped unknown field(s): {', '.join(unknown)}")
        return selected, warnings

    def summarize(
        self,
        filter_obj: Dict[str, Any],
        *,
        label: str = "Cohort",
        fields: Optional[Sequence[str]] = None,
    ) -> CohortSummary:
        use_fields, warnings = self._select_fields(fields)

        try:
            query = build_aggregation_query(
                filter_obj,
                data_type=self._data_type,
                accessibility=self._accessibility,
                histogram_fields=use_fields or None,
            )
        except (ValueError, TypeError) as e:
            return CohortSummary(label, None, False, [], errors=[f"invalid filter: {e}"], warnings=warnings)

        result = self._guppy.execute(query, data_type=self._data_type)
        masked = getattr(result, "total_masked", False)
        if not result.ok:
            return CohortSummary(
                label, None, masked, [],
                errors=list(result.errors) or ["aggregation failed"],
                warnings=warnings,
            )

        total = result.total_count
        distributions = [
            _distribution(f, total, result.histograms.get(f, []))
            for f in use_fields
        ]
        return CohortSummary(label, total, masked, distributions, warnings=warnings)

    def compare(
        self,
        filter_a: Dict[str, Any],
        filter_b: Dict[str, Any],
        *,
        label_a: str = "A",
        label_b: str = "B",
        fields: Optional[Sequence[str]] = None,
    ) -> CohortComparison:
        a = self.summarize(filter_a, label=label_a, fields=fields)
        b = self.summarize(filter_b, label=label_b, fields=fields)

        errors = [f"{label_a}: {e}" for e in a.errors] + [f"{label_b}: {e}" for e in b.errors]
        warnings = [f"{label_a}: {w}" for w in a.warnings] + [f"{label_b}: {w}" for w in b.warnings]
        total_delta = (
            b.total - a.total if a.total is not None and b.total is not None else None
        )
        rows = _comparison_rows(a, b)
        return CohortComparison(
            label_a, label_b, a.total, b.total, a.total_masked, b.total_masked,
            total_delta, rows, errors=errors, warnings=warnings,
        )


def _distribution(field_name: str, total: Optional[int], raw_buckets: List[dict]) -> Distribution:
    buckets: List[Bucket] = []
    for raw in raw_buckets:
        count = raw.get("count")
        masked = bool(raw.get("masked"))
        pct = (count / total) if (count is not None and isinstance(total, int) and total > 0) else None
        buckets.append(Bucket(key=str(raw.get("key")), count=count, pct=pct, masked=masked))
    buckets.sort(key=lambda b: (b.count is None, -(b.count or 0)))
    return Distribution(field_name, total, buckets)


def _comparison_rows(a: CohortSummary, b: CohortSummary) -> List[ComparisonRow]:
    a_dist = {d.field: d for d in a.distributions}
    b_dist = {d.field: d for d in b.distributions}

    rows: List[ComparisonRow] = []
    for fname in [d.field for d in a.distributions]:
        da, db = a_dist.get(fname), b_dist.get(fname)
        a_by = {bk.key: bk for bk in (da.buckets if da else [])}
        b_by = {bk.key: bk for bk in (db.buckets if db else [])}

        keys = list(a_by) + [k for k in b_by if k not in a_by]
        for k in keys:
            ba, bb = a_by.get(k), b_by.get(k)
            a_pct = ba.pct if ba else 0.0
            b_pct = bb.pct if bb else 0.0
            delta = (b_pct - a_pct) if (a_pct is not None and b_pct is not None) else None
            rows.append(ComparisonRow(
                field=fname, key=k,
                a_count=(ba.count if ba else 0),
                a_pct=a_pct,
                b_count=(bb.count if bb else 0),
                b_pct=b_pct,
                delta_pct=delta,
            ))
    return rows


def _pct(x: Optional[float]) -> str:
    return "n/a" if x is None else f"{round(x * 100)}%"


def _count(n: Optional[int]) -> str:
    return "n/a" if n is None else f"{n:,}"


def _bucket_str(b: Bucket) -> str:
    if b.masked:
        return f"{b.key} n/a (masked)"
    if b.count is None:
        return f"{b.key} n/a"
    return f"{b.key} {_count(b.count)} ({_pct(b.pct)})"


def format_summary(summary: CohortSummary) -> str:
    lines = [f"Cohort: {summary.label}", "-" * 17]
    total = "masked" if summary.total_masked else _count(summary.total)
    lines.append(f"Total subjects: {total}")

    for d in summary.distributions:
        parts = [_bucket_str(b) for b in d.buckets]
        lines.append(f"{d.field}: " + (" | ".join(parts) if parts else "(no data)"))

    if summary.warnings:
        lines.append("")
        lines.append("Note: " + "; ".join(summary.warnings))
    if summary.errors:
        lines.append("")
        lines.append("Errors: " + "; ".join(summary.errors))
    return "\n".join(lines) + "\n"


def format_comparison(comparison: CohortComparison) -> str:
    a, b = comparison.label_a, comparison.label_b
    key_width = max([len("Total")] + [len(r.key) for r in comparison.rows]) + 2
    num_width = max(len(a), len(b), len("Δ(B-A)"), 8) + 1

    def cell(s: str) -> str:
        return f"{s:>{num_width}}"

    lines = [f"{'':<{key_width}}{cell(a)}{cell(b)}{cell('Δ(B-A)')}"]
    lines.append("-" * (key_width + num_width * 3))

    ta = "masked" if comparison.total_a_masked else _count(comparison.total_a)
    tb = "masked" if comparison.total_b_masked else _count(comparison.total_b)
    dt = "" if comparison.total_delta is None else f"{comparison.total_delta:+,}"
    lines.append(f"{'Total':<{key_width}}{cell(ta)}{cell(tb)}{cell(dt)}")

    current_field = None
    for r in comparison.rows:
        if r.field != current_field:
            lines.append(f"[{r.field}]")
            current_field = r.field
        d = "" if r.delta_pct is None else f"{round(r.delta_pct * 100):+d}%"
        lines.append(f"{r.key:<{key_width}}{cell(_pct(r.a_pct))}{cell(_pct(r.b_pct))}{cell(d)}")

    if comparison.warnings:
        lines.append("")
        lines.append("Note: " + "; ".join(comparison.warnings))
    if comparison.errors:
        lines.append("")
        lines.append("Errors: " + "; ".join(comparison.errors))
    return "\n".join(lines) + "\n"


__all__ = [
    "CohortAnalyzer",
    "CohortSummary",
    "CohortComparison",
    "Distribution",
    "Bucket",
    "ComparisonRow",
    "DEFAULT_SUMMARY_FIELDS",
    "format_summary",
    "format_comparison",
]
