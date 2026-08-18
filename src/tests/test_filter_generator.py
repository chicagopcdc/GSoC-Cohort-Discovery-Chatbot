import sys
from pathlib import Path


def _find_upwards(relative: str) -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / relative
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"could not find {relative} above {here}")


_SERVICES = _find_upwards("backend/services")
if str(_SERVICES.parent) not in sys.path:
    sys.path.insert(0, str(_SERVICES.parent))

from services.filter_generator import _is_removal_edit, _merge_updated_filter


def test_without_constraint_is_not_treated_as_filter_removal():
    assert not _is_removal_edit("also include patients without relapse")


def test_explicit_filter_removal_is_still_detected():
    assert _is_removal_edit("remove the sex restriction")


def test_updated_filter_preserves_top_level_and_merges_same_nested_path():
    current = {
        "AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"IN": {"sex": ["Female"]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_site": ["Bone"]}},
            ]}},
        ]
    }
    addition = {"nested": {"path": "tumor_assessments", "AND": [
        {"IN": {"tumor_classification": ["Metastatic"]}},
        {"IN": {"disease_phase": ["Relapse"]}},
    ]}}

    assert _merge_updated_filter(current, addition) == {
        "AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"IN": {"sex": ["Female"]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_site": ["Bone"]}},
                {"IN": {"tumor_classification": ["Metastatic"]}},
                {"IN": {"disease_phase": ["Relapse"]}},
            ]}},
        ]
    }


def test_updated_filter_does_not_duplicate_an_or_bodied_nested_clause():
    """An OR-bodied nested clause is still a nested clause.

    Anchored multi-value requests ("Stage 4 or Stage M at initial diagnosis")
    build `nested.OR`. A later edit re-emits that clause, and if the merge does
    not recognise the OR body it keeps the old copy *and* appends the new one,
    so the filter grows a duplicate on every edit.
    """
    staging = {"nested": {"path": "stagings", "OR": [
        {"AND": [
            {"IN": {"stage": ["Stage 4"]}},
            {"IN": {"disease_phase": ["Initial Diagnosis"]}},
        ]},
        {"AND": [
            {"IN": {"stage": ["Stage M"]}},
            {"IN": {"disease_phase": ["Initial Diagnosis"]}},
        ]},
    ]}}
    current = {"AND": [{"IN": {"consortium": ["INRG"]}}, staging]}
    addition = {"AND": [staging, {"IN": {"sex": ["Male"]}}]}

    merged = _merge_updated_filter(current, addition)

    staging_clauses = [c for c in merged["AND"] if "nested" in c]
    assert len(staging_clauses) == 1, "the OR-bodied nested clause was duplicated"
    assert merged == {"AND": [
        {"IN": {"consortium": ["INRG"]}},
        staging,
        {"IN": {"sex": ["Male"]}},
    ]}


def test_updated_filter_replaces_an_existing_condition_for_the_same_field():
    current = {"AND": [
        {"IN": {"consortium": ["INRG"]}},
        {"IN": {"sex": ["Female"]}},
    ]}
    addition = {"IN": {"sex": ["Male"]}}

    assert _merge_updated_filter(current, addition) == {
        "AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"IN": {"sex": ["Male"]}},
        ]
    }
