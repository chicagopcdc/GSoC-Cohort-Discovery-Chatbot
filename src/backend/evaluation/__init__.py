"""Offline evaluation helpers for the query builder."""

from evaluation.comparator import (
    Comparison,
    ErrorCategory,
    ErrorDetail,
    canonicalize,
    compare,
    equivalent,
)
from evaluation.cases import (
    BenchmarkCase,
    ReferenceIssue,
    case_from_dict,
    classify_difficulty,
    load_cases,
    load_csv_cases,
    validate_references,
)
from evaluation.runner import (
    BuildOutcome,
    CaseResult,
    EvalReport,
    Evaluator,
    evaluate,
    query_builder_build_fn,
    session_build_fn,
)
from evaluation.report import format_report

__all__ = [
    "Comparison", "ErrorCategory", "ErrorDetail",
    "canonicalize", "compare", "equivalent",
    "BenchmarkCase", "ReferenceIssue", "case_from_dict", "classify_difficulty",
    "load_cases", "load_csv_cases", "validate_references",
    "BuildOutcome", "CaseResult", "EvalReport", "Evaluator", "evaluate",
    "query_builder_build_fn", "session_build_fn",
    "format_report",
]
