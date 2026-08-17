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


def test_negated_enum_term_is_rendered_as_excluded_term():
    term = RecognizedTerm(
        "Metastatic",
        (FieldPlacement("tumor_classification", "tumor_assessments"),),
        (4, 14),
        negated=True,
    )
    nq = NormalizedQuery("not metastatic", [term], [], ["not metastatic"])

    content = _content(nq)

    assert "Recognized terms (already matched to the schema):\n(none)" in content
    assert "Excluded terms (emit as !=):" in content
    assert '"Metastatic" -> tumor_classification (under tumor_assessments)' in content
    assert 'dropped negated enum/category term "Metastatic"' not in content
    assert prompt_warnings(nq) == []


def test_unresolved_negated_enum_term_is_dropped_and_warned():
    term = RecognizedTerm("Metastatic", (), (4, 14), negated=True)
    nq = NormalizedQuery("not metastatic", [term], [], ["not metastatic"])

    content = _content(nq)

    assert "Excluded terms (emit as !=):\n(none)" in content
    assert (
        'dropped negated enum/category term "Metastatic": '
        "no schema field placement is available"
    ) in content
    assert prompt_warnings(nq) == [
        'dropped negated enum/category term "Metastatic": '
        "no schema field placement is available"
    ]


def test_pinned_values_keep_schema_order():
    terms = [
        RecognizedTerm("B", (FieldPlacement("stage", None),), (0, 1)),
        RecognizedTerm("A", (FieldPlacement("stage", None),), (2, 3)),
    ]
    nq = NormalizedQuery("B or A", terms, [], [])

    content = _content(nq, [_candidate("stage", ["A", "B", "C"])])

    assert "values: A, B, C" in content


def test_value_named_in_the_query_survives_truncation():
    """Truncation must not hide a value the query explicitly names.

    molecular_abnormality carries 178 values and the display cap is 40, so the
    shown slice is alphabetical. "MYCN non-amplified" does not match the schema
    value "MYCN Amplification" literally, so the normalizer resolves nothing and
    pins nothing -- leaving "ALK Amplification" as the only amplification value
    the model can see, which is what it then emitted.
    """
    values = [f"AAA{i:03d} marker" for i in range(60)] + ["ALK Amplification", "MYCN Amplification"]
    nq = NormalizedQuery("INRG subjects whose tumors are MYCN non-amplified",
                         terms=[], ranges=[], negations=[])

    content = _content(nq, [_candidate("molecular_abnormality", values)])

    assert "MYCN Amplification" in content


def test_a_generic_word_does_not_pin_every_value():
    """Pinning keys off distinctive words, not ones shared across the field."""
    values = [f"Stage {n}" for n in range(1, 60)] + ["Stage 4s"]
    nq = NormalizedQuery("subjects with Stage 4s disease", terms=[], ranges=[], negations=[])

    content = _content(nq, [_candidate("stage", values)])

    assert "Stage 4s" in content
    # "stage" is in every value, so it must not drag all 30 past the cap.
    assert "more)" in content, "the display cap was bypassed by a generic word"
