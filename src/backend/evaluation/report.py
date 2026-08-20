"""Plain-text formatting for EvalReport."""

from __future__ import annotations

from typing import Optional

from evaluation.runner import EvalReport


def _pct(x: Optional[float]) -> str:
    return "n/a" if x is None else f"{round(x * 100)}%"


def _rate(n: int, total: int) -> str:
    pct = (n / total) if total else 0.0
    return f"{round(pct * 100)}% ({n}/{total})"


def format_report(report: EvalReport) -> str:
    lines = ["EVALUATION REPORT", "=" * 17, ""]

    total = len(report.results)
    skipped = report.skipped
    head = f"Total cases:        {total}"
    if skipped:
        head += f"   (gradeable {len(report.gradeable)}, skipped {len(skipped)})"
    lines.append(head)
    lines.append("")

    lines.append(f"Exact Match Rate:   {_rate(*report.exact())}")
    lines.append(f"Structure Match:    {_rate(*report.structural())}")
    lines.append(f"Field Accuracy:     {_pct(report.mean_field())}  (avg)")
    lines.append(f"Value Accuracy:     {_pct(report.mean_value())}  (avg)")
    lines.append(f"Structure Accuracy: {_pct(report.mean_structure())}  (avg)")
    execu = report.execution()
    lines.append(f"Execution Equiv.:   {_rate(*execu) if execu else 'n/a'}")
    lines.append("")

    lat = report.latency()
    if lat:
        lines.append(f"Avg Latency:        {lat['avg']:.0f} ms  (p50 {lat['p50']:.0f}, p95 {lat['p95']:.0f})")
    toks = report.avg_tokens()
    if toks is not None:
        lines.append(f"Avg Tokens:         {toks:.0f} per query")
    lines.append("")

    by_diff = report.by_difficulty()
    if by_diff:
        lines.append("By difficulty:")
        for name, (n, tot) in by_diff:
            lines.append(f"    {name:<10}{_rate(n, tot)}")
        lines.append("")

    patterns = report.failure_patterns()
    if patterns:
        lines.append("Common failure patterns:")
        for cat, count in patterns.most_common():
            lines.append(f"    {cat.value:<20}{count}")
        lines.append("")

    return "\n".join(lines).rstrip() + "\n"
