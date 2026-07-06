import sys
from pathlib import Path
from types import SimpleNamespace


_SERVICES = Path(__file__).resolve().parents[1] / "backend"
if str(_SERVICES) not in sys.path:
    sys.path.insert(0, str(_SERVICES))

from prompts.filter_prompt import build_filter_messages, prompt_warnings
from services.term_normalizer import (
    FieldPlacement,
    NormalizedQuery,
    NumericConstraint,
    RecognizedTerm,
)


def _candidate(field, values):
    return SimpleNamespace(
        field=field,
        path=None,
        field_type="enum",
        description="",
        enum_values=tuple(values),
    )


def _content(nq, candidates=()):
    return build_filter_messages(nq, list(candidates))[1]["content"]


def test_unconverted_unit_range_is_dropped_and_warned():
    nq = NormalizedQuery(
        "older than 5 years",
        terms=[],
        ranges=[NumericConstraint("gt", 5.0, "years", "age", (0, 18))],
        negations=[],
    )

    content = _content(nq)

    assert "Numeric ranges:\n(none)" in content
    assert "Unsupported constraints (already dropped; do not emit):" in content
    assert (
        "dropped numeric range age GT 5 years: "
        "schema-unit converted value is not available"
    ) in content
    assert prompt_warnings(nq) == [
        "dropped numeric range age GT 5 years: "
        "schema-unit converted value is not available"
    ]


def test_negated_enum_term_is_dropped_and_warned():
    term = RecognizedTerm(
        "Metastatic",
        (FieldPlacement("tumor_classification", "tumor_assessments"),),
        (4, 14),
        negated=True,
    )
    nq = NormalizedQuery("not metastatic", [term], [], ["not metastatic"])

    content = _content(nq)

    assert "Recognized terms (already matched to the schema):\n(none)" in content
    assert (
        'dropped negated enum/category term "Metastatic": '
        "NOT is not supported for enum/category values"
    ) in content
    assert prompt_warnings(nq) == [
        'dropped negated enum/category term "Metastatic": '
        "NOT is not supported for enum/category values"
    ]


def test_pinned_values_keep_schema_order():
    terms = [
        RecognizedTerm("B", (FieldPlacement("stage", None),), (0, 1)),
        RecognizedTerm("A", (FieldPlacement("stage", None),), (2, 3)),
    ]
    nq = NormalizedQuery("B or A", terms, [], [])

    content = _content(nq, [_candidate("stage", ["A", "B", "C"])])

    assert "values: A, B, C" in content
