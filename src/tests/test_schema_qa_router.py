import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.schema_explorer import SchemaExplorer
from services.schema_loader import FieldSpec, SchemaIndex
from services.schema_qa_router import SchemaQARouter


def make_router():
    specs = [
        FieldSpec("sex", "enum", ("Male", "Female"), "Subject sex", None),
        FieldSpec("consortium", "enum", ("INRG", "NODAL"), "", None),
        FieldSpec("tumor_classification", "enum", ("Metastatic", "Localized"), "Tumor stage", "tumor_assessments"),
        FieldSpec("tumor_site", "enum", ("Skin", "Bone"), "Tumor site", "tumor_assessments"),
        FieldSpec("histology", "enum", ("ARMS", "ERMS"), "", "histologies"),
    ]
    schema = SchemaIndex({(s.parent_path, s.name): s for s in specs})
    return SchemaQARouter(SchemaExplorer(schema))


class TestSchemaQARouter:
    def test_values_question(self):
        answer = make_router().answer("What values does sex allow?")
        assert answer is not None
        assert answer.query_type == "values"
        assert answer.args == {"field": "sex"}
        assert "Male" in answer.text

    def test_list_fields_under_path(self):
        answer = make_router().answer("Which fields are available under tumor_assessments?")
        assert answer is not None
        assert answer.query_type == "list_fields"
        assert answer.args == {"path": "tumor_assessments"}
        assert "tumor_classification" in answer.text

    def test_find_value_owner(self):
        answer = make_router().answer("Which field contains the value Metastatic?")
        assert answer is not None
        assert answer.query_type == "find_value"
        assert answer.args == {"value": "Metastatic"}
        assert "tumor_classification" in answer.text

    def test_describe_field(self):
        answer = make_router().answer("Describe tumor_site")
        assert answer is not None
        assert answer.query_type == "describe"
        assert answer.args == {"field": "tumor_site"}
        assert "Tumor site" in answer.text

    def test_regular_cohort_request_is_not_routed(self):
        answer = make_router().answer("Find male patients from the INRG consortium")
        assert answer is None

    def test_unknown_schema_question_is_not_guessed(self):
        answer = make_router().answer("What values does totally_fake_field allow?")
        assert answer is None
