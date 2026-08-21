"""Run the smoke benchmark from the command line."""

from __future__ import annotations

import glob
import os
import sys
from pathlib import Path


def _bootstrap() -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "backend").is_dir():
            sys.path.insert(0, str(parent / "backend"))
            return parent
    raise SystemExit("could not locate the src/backend package")


_SRC = _bootstrap()
_REPO = _SRC.parent

from evaluation.cases import load_cases, validate_references
from evaluation.report import format_report
from evaluation.runner import evaluate, query_builder_build_fn, session_build_fn
from services.query_builder_v2 import QueryBuilder
from services.session_manager import SessionManager


def _find_schema() -> tuple[str, str]:
    schema_dir = _REPO / "schema"
    pcdc = os.getenv("PCDC_SCHEMA_PATH")
    if not pcdc:
        hits = sorted(glob.glob(str(schema_dir / "pcdc-schema-prod-*.json")))
        if not hits:
            raise SystemExit(f"no pcdc-schema-prod-*.json under {schema_dir}")
        pcdc = hits[-1]
    gitops = os.getenv("GITOPS_PATH", str(schema_dir / "gitops.json"))
    return pcdc, gitops


def _data(name: str) -> str | None:
    p = _SRC / "backend" / "data" / name
    return str(p) if p.exists() else None


def _schema_valid(report) -> tuple[int, int]:
    """Count validated build outputs."""
    results = report.results
    return sum(1 for r in results if r.ok), len(results)


def _avg_latency(report) -> float:
    lat = report.latency()
    return lat["avg"] if lat else float("nan")


def _error_summary(report) -> list[tuple[str, int]]:
    """Group repeated build errors."""
    from collections import Counter
    c: Counter = Counter()
    for r in report.results:
        for e in r.errors:
            c[e] += 1
    return c.most_common()


def main() -> None:
    if not os.getenv("OPENAI_API_KEY"):
        raise SystemExit("set OPENAI_API_KEY first")

    pcdc, gitops = _find_schema()
    cases_path = Path(os.getenv(
        "BENCHMARK_CASES_PATH",
        _SRC / "backend" / "evaluation" / "data" / "smoke_cases.yaml",
    ))
    cases = load_cases(cases_path)
    model = os.getenv("FILTER_GENERATION_MODEL", "(default)")

    print(f"schema: {Path(pcdc).name}")
    print(f"model:  {model}")
    print(f"case file: {cases_path}")
    print(f"cases:  {len(cases)} total, {len([c for c in cases if c.expected_filter is not None])} gradeable\n")

    qb = QueryBuilder.from_files(
        pcdc, gitops,
        synonyms_path=_data("synonyms.yaml"),
        numeric_config_path=_data("numeric_fields.yaml"),
    )
    ref_issues = validate_references(cases, qb.schema)
    if ref_issues:
        print("=" * 60)
        print("REFERENCE FILTER ERRORS")
        print("-" * 60)
        for issue in ref_issues:
            where = f" field={issue.field!r}" if issue.field else ""
            path = f" path={issue.path!r}" if issue.path else ""
            print(f"  {issue.case}: [{issue.code}]{where}{path} {issue.message}")
        raise SystemExit("reference filters failed schema validation")

    sm = SessionManager(qb)

    single = evaluate(cases, query_builder_build_fn(qb))
    session = evaluate(cases, session_build_fn(sm))

    print("=" * 60)
    print("SINGLE-SHOT (QueryBuilder)\n")
    print(format_report(single))
    print("=" * 60)
    print("WITH SESSION (SessionManager)\n")
    print(format_report(session))

    sv_n, sv_t = _schema_valid(single)
    ex_s_n, ex_s_t = single.exact()
    ex_m_n, ex_m_t = session.exact()

    errors = _error_summary(single)
    if errors:
        print("=" * 60)
        print("BUILD ERRORS (single-shot)")
        print("-" * 60)
        for msg, n in errors:
            print(f"  x{n}  {msg}")
        if sv_n == 0:
            print("\n  NOTE: every case errored. This is almost certainly an API/"
                  "config problem\n  (quota, key, model access) — not model quality. "
                  "Fix that before\n  trusting the numbers below.")

    print("=" * 60)
    print("SLIDE-READY NUMBERS")
    print("-" * 60)
    print(f"  {sv_n} / {sv_t}    Schema-valid filters")
    print(f"  {ex_s_n} / {ex_s_t}    Exact match, single-shot")
    print(f"  {ex_m_n} / {ex_m_t}    Exact match, with session")
    print(f"  {_avg_latency(single) / 1000:.2f} s -> {_avg_latency(session) / 1000:.2f} s   Average latency (single -> session)")
    print("=" * 60)


if __name__ == "__main__":
    main()
