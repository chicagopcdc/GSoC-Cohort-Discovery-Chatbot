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

import pytest

from models.filters import GraphQLFilter
from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA, SchemaIndex
from services.filter_validator import (
    CODE_INVALID_ENUM,
    CODE_NESTED_IN_NESTED,
    CODE_STRUCTURAL,
    CODE_TYPE_MISMATCH,
    CODE_UNKNOWN_FIELD,
    CODE_UNKNOWN_PATH,
    CODE_WRONG_PATH,
    validate_dict,
    validate_filter,
)

SCHEMA_DIR = _find_upwards("schema")


@pytest.fixture(scope="module")
def schema() -> SchemaIndex:
    return SchemaIndex.from_files(
        SCHEMA_DIR / DEFAULT_PCDC_SCHEMA,
        SCHEMA_DIR / DEFAULT_GITOPS,
    )


def _codes(obj, schema) -> list:
    return validate_dict(obj, schema).codes()


class TestValidFilters:
    def test_top_level_and(self, schema):
        obj = {"AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"IN": {"sex": ["Male"]}},
        ]}
        assert validate_dict(obj, schema).ok

    def test_top_level_plus_nested(self, schema):
        obj = {"AND": [
            {"IN": {"consortium": ["INRG"]}},
            {"nested": {"path": "tumor_assessments", "AND": [
                {"IN": {"tumor_classification": ["Metastatic"]}},
                {"IN": {"tumor_site": ["Skin"]}},
            ]}},
        ]}
        assert validate_dict(obj, schema).ok

    def test_range_on_numeric_field(self, schema):
        obj = {"AND": [{"GTE": {"age_at_censor_status": 5}}]}
        assert validate_dict(obj, schema).ok

    def test_top_level_or(self, schema):
        obj = {"OR": [
            {"AND": [{"IN": {"consortium": ["INRG"]}}]},
            {"AND": [{"IN": {"consortium": ["NODAL"]}}]},
        ]}
        assert validate_dict(obj, schema).ok


class TestUnknownField:
    def test_unknown_top_level_field(self, schema):
        obj = {"AND": [{"IN": {"definitely_not_a_field": ["x"]}}]}
        assert CODE_UNKNOWN_FIELD in _codes(obj, schema)


class TestWrongPath:
    def test_nested_field_used_at_top_level(self, schema):
        # tumor_classification only exists under nested paths
        obj = {"AND": [{"IN": {"tumor_classification": ["Metastatic"]}}]}
        codes = _codes(obj, schema)
        assert CODE_WRONG_PATH in codes

    def test_field_under_wrong_nested_path(self, schema):
        # tumor_classification is not under histologies
        obj = {"AND": [{"nested": {"path": "histologies", "AND": [
            {"IN": {"tumor_classification": ["Metastatic"]}},
        ]}}]}
        assert CODE_WRONG_PATH in _codes(obj, schema)

    def test_wrong_path_message_names_real_paths(self, schema):
        obj = {"AND": [{"IN": {"tumor_classification": ["Metastatic"]}}]}
        issue = next(i for i in validate_dict(obj, schema).issues
                     if i.code == CODE_WRONG_PATH)
        assert "tumor_assessments" in issue.message


class TestInvalidEnum:
    def test_hallucinated_value(self, schema):
        obj = {"AND": [{"IN": {"sex": ["Martian"]}}]}
        assert CODE_INVALID_ENUM in _codes(obj, schema)

    def test_casing_mismatch_is_caught(self, schema):
        # tumor_classification's value is "Not reported" (lowercase r);
        # "Not Reported" (capital R) is sex/race's spelling — wrong here.
        obj = {"AND": [{"nested": {"path": "tumor_assessments", "AND": [
            {"IN": {"tumor_classification": ["Not Reported"]}},
        ]}}]}
        assert CODE_INVALID_ENUM in _codes(obj, schema)

    def test_valid_value_not_flagged(self, schema):
        obj = {"AND": [{"nested": {"path": "tumor_assessments", "AND": [
            {"IN": {"tumor_classification": ["Not reported"]}},
        ]}}]}
        assert CODE_INVALID_ENUM not in _codes(obj, schema)


class TestTypeMismatch:
    def test_range_on_enum_field(self, schema):
        obj = {"AND": [{"GTE": {"sex": 5}}]}
        assert CODE_TYPE_MISMATCH in _codes(obj, schema)
        
    def test_range_on_string_field(self, schema):
        obj = {"AND": [{"GTE": {"subject_submitter_id": 5}}]}
        assert CODE_TYPE_MISMATCH in _codes(obj, schema)


    def test_range_on_numeric_field_is_fine(self, schema):
        obj = {"AND": [{"LTE": {"age_at_censor_status": 18}}]}
        assert CODE_TYPE_MISMATCH not in _codes(obj, schema)


class TestUnknownPath:
    def test_bogus_nested_path(self, schema):
        obj = {"AND": [{"nested": {"path": "bogus_table", "AND": [
            {"IN": {"x": ["y"]}},
        ]}}]}
        assert _codes(obj, schema) == [CODE_UNKNOWN_PATH]


class TestNestedInNested:
    def test_nested_inside_nested(self, schema):
        obj = {"AND": [{"nested": {"path": "tumor_assessments", "AND": [
            {"nested": {"path": "histologies", "AND": [
                {"IN": {"histology": ["anything"]}},
            ]}},
        ]}}]}
        assert CODE_NESTED_IN_NESTED in _codes(obj, schema)


class TestStructural:
    def test_unknown_operator_is_structural(self, schema):
        obj = {"AND": [{"BOGUS": {"x": ["y"]}}]}
        codes = _codes(obj, schema)
        assert codes == [CODE_STRUCTURAL]

    def test_empty_and_is_structural(self, schema):
        obj = {"AND": []}
        assert _codes(obj, schema) == [CODE_STRUCTURAL]


class TestResultApi:
    def test_ok_true_when_clean(self, schema):
        obj = {"AND": [{"IN": {"consortium": ["INRG"]}}]}
        result = validate_dict(obj, schema)
        assert result.ok is True
        assert result.issues == []

    def test_ok_false_when_issues(self, schema):
        obj = {"AND": [{"IN": {"sex": ["Martian"]}}]}
        result = validate_dict(obj, schema)
        assert result.ok is False
        assert len(result.issues) >= 1

    def test_validate_filter_accepts_model(self, schema):
        # validate_filter takes an already-parsed GraphQLFilter
        gf = GraphQLFilter.model_validate(
            {"AND": [{"IN": {"consortium": ["INRG"]}}]}
        )
        assert validate_filter(gf, schema).ok

    def test_issue_carries_field_and_value(self, schema):
        obj = {"AND": [{"IN": {"sex": ["Martian"]}}]}
        issue = next(i for i in validate_dict(obj, schema).issues
                     if i.code == CODE_INVALID_ENUM)
        assert issue.field == "sex"
        assert issue.value == "Martian"