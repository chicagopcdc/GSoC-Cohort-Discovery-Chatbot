import csv
import json
import sys
from pathlib import Path

import pytest

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from evaluation.cases import load_cases, load_csv_cases, validate_references
from evaluation.comparator import compare, equivalent
from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA, SchemaIndex

_SMOKE_YAML = _BACKEND / "evaluation" / "data" / "smoke_cases.yaml"
_SCHEMA_DIR = _BACKEND.parents[1] / "schema"


def _write_csv(tmp_path, rows):
    p = tmp_path / "cases.csv"
    with p.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["name", "filter_object", "graphql_object", "llm_result"])
        for r in rows:
            w.writerow(r)
    return p


class TestCsvLoading:
    def test_valid_row_is_gradeable(self, tmp_path):
        gql = '{"AND": [{"IN": {"consortium": ["INRG"]}}, {"IN": {"sex": ["Male"]}}]}'
        cases = load_csv_cases(_write_csv(tmp_path, [
            ("INRG males", "ui-rep", gql, "Male INRG participants"),
        ]))
        assert len(cases) == 1
        c = cases[0]
        assert c.expected_filter == {
            "AND": [{"IN": {"consortium": ["INRG"]}}, {"IN": {"sex": ["Male"]}}]
        }
        assert c.difficulty == "simple"
        assert c.group == "amanuensis"

    def test_not_equal_row_is_gradeable(self, tmp_path):
        gql = '{"!=": {"sex": "Female"}}'
        case = load_csv_cases(_write_csv(tmp_path, [
            ("not female", "ui-rep", gql, "Exclude female participants"),
        ]))[0]

        assert case.expected_filter == {"!=": {"sex": "Female"}}
        assert case.difficulty == "simple"

    def test_llm_result_used_as_natural_language(self, tmp_path):
        c = load_csv_cases(_write_csv(tmp_path, [
            ("nm", "", '{"IN": {"sex": ["Male"]}}', "just the males"),
        ]))[0]
        assert c.natural_language == "just the males"

    def test_falls_back_to_name_when_llm_result_missing(self, tmp_path):
        c = load_csv_cases(_write_csv(tmp_path, [
            ("My Cohort", "", '{"IN": {"sex": ["Male"]}}', ""),
        ]))[0]
        assert c.natural_language == "My Cohort"

    def test_empty_invalid_and_blank_filters_are_ungradeable(self, tmp_path):
        cases = load_csv_cases(_write_csv(tmp_path, [
            ("empty",  "", "{}",         "nothing"),
            ("broken", "", "{not json",  "bad json"),
            ("blank",  "", "",           "no filter"),
        ]))
        assert [c.expected_filter for c in cases] == [None, None, None]
        assert all("ungradeable" in c.notes for c in cases)
        assert all(c.difficulty is None for c in cases)

    def test_filter_object_kept_as_provenance(self, tmp_path):
        c = load_csv_cases(_write_csv(tmp_path, [
            ("x", "UI-REPRESENTATION", '{"IN": {"sex": ["Male"]}}', "males"),
        ]))[0]
        assert "filter_object=UI-REPRESENTATION" in c.notes

    def test_load_cases_dispatches_csv(self, tmp_path):
        cases = load_cases(_write_csv(tmp_path, [
            ("x", "", '{"IN": {"sex": ["Male"]}}', "males"),
        ]))
        assert len(cases) == 1 and cases[0].expected_filter is not None


class TestExistingLoadersStillWork:
    def test_json_loader_unchanged(self, tmp_path):
        p = tmp_path / "cases.json"
        p.write_text(json.dumps([
            {"name": "A1", "natural_language": "INRG males",
             "expected_filter": {"AND": [{"IN": {"consortium": ["INRG"]}}]}},
        ]))
        c = load_cases(p)[0]
        assert c.name == "A1"
        assert c.expected_filter == {"AND": [{"IN": {"consortium": ["INRG"]}}]}

    def test_yaml_loader_unchanged(self, tmp_path):
        pytest.importorskip("yaml")
        import yaml
        p = tmp_path / "q.yaml"
        p.write_text(yaml.safe_dump([
            {"id": "A1", "text": "INRG males",
             "ground_truth": {"AND": [{"IN": {"consortium": ["INRG"]}}]}},
        ]))
        c = load_cases(p)[0]
        assert c.name == "A1"
        assert c.expected_filter == {"AND": [{"IN": {"consortium": ["INRG"]}}]}


class TestSmokeBenchmark:
    def test_smoke_set_loads(self):
        pytest.importorskip("yaml")
        cases = load_cases(_SMOKE_YAML)
        assert len(cases) == 11

        by_name = {c.name: c for c in cases}
        gradeable = [c for c in cases if c.expected_filter is not None]
        assert len(gradeable) == 10
        assert by_name["C2"].expected_filter is None

        assert by_name["A1"].difficulty == "simple"
        assert by_name["A2"].difficulty == "medium"
        assert by_name["E1_turn2"].session_id == "e1-session"

    def test_smoke_age_ground_truth_uses_schema_days(self):
        pytest.importorskip("yaml")
        by_name = {c.name: c for c in load_cases(_SMOKE_YAML)}

        assert by_name["B2"].expected_filter == {
            "AND": [{"GT": {"age_at_censor_status": 1826.25}}]
        }
        assert by_name["B3"].expected_filter["AND"][1] == {
            "LTE": {"age_at_censor_status": 1826.25}
        }
        assert by_name["D2"].expected_filter["AND"][1] == {
            "AND": [
                {"GTE": {"age_at_censor_status": 0}},
                {"LTE": {"age_at_censor_status": 6574.5}},
            ]
        }

    def test_smoke_references_validate_against_current_schema(self):
        pytest.importorskip("yaml")
        cases = load_cases(_SMOKE_YAML)
        schema = SchemaIndex.from_files(
            _SCHEMA_DIR / DEFAULT_PCDC_SCHEMA,
            _SCHEMA_DIR / DEFAULT_GITOPS,
        )
        assert validate_references(cases, schema) == []


class TestComparator:
    def test_not_equal_exact_match(self):
        expected = {"AND": [{"!=": {"sex": "Female"}}]}
        generated = {"!=": {"sex": "Female"}}

        comparison = compare(generated, expected)

        assert equivalent(generated, expected)
        assert comparison.exact_match
        assert comparison.field_accuracy == 1.0
        assert comparison.value_accuracy == 1.0

    def test_not_equal_wrong_value_is_caught(self):
        comparison = compare(
            {"!=": {"sex": "Male"}},
            {"!=": {"sex": "Female"}},
        )

        assert not comparison.exact_match
        assert comparison.value_accuracy == 0.0
        assert comparison.errors[0].category.value == "wrong_value"
