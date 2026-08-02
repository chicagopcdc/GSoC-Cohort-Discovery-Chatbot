import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import pytest

from services.schema_explorer import SchemaExplorer
from services.schema_loader import FieldSpec, SchemaIndex


def make_schema():
    specs = [
        FieldSpec("sex", "enum", ("Male", "Female"), "Subject sex", None),
        FieldSpec("consortium", "enum", ("INRG", "INSTRuCT", "NODAL"), "", None),
        FieldSpec("age_at_censor_status", "number", (), "Age in days", None),
        FieldSpec("tumor_classification", "enum", ("Metastatic", "Localized", "Regional"), "Tumor stage", "tumor_assessments"),
        FieldSpec("tumor_site", "enum", ("Brain", "Skin", "Bone"), "", "tumor_assessments"),
        FieldSpec("histology", "enum", ("ARMS", "ERMS"), "", "histologies"),
        # same field name under two paths, different enums -> ambiguous
        FieldSpec("stage", "enum", ("1", "2"), "", "tumor_assessments"),
        FieldSpec("stage", "enum", ("3", "4"), "", "histologies"),
    ]
    return SchemaIndex({(s.parent_path, s.name): s for s in specs})


def explorer(**kwargs):
    return SchemaExplorer(make_schema(), **kwargs)


class TestListFields:
    def test_top_level(self):
        r = explorer().list_fields()
        assert r.kind == "fields"
        assert r.data["path"] is None
        assert [f["name"] for f in r.data["fields"]] == ["age_at_censor_status", "consortium", "sex"]
        assert "subject (top-level)" in r.text

    def test_under_path_sorted(self):
        r = explorer().list_fields("tumor_assessments")
        assert [f["name"] for f in r.data["fields"]] == ["stage", "tumor_classification", "tumor_site"]
        assert r.data["field_count"] == 3


class TestFieldValues:
    def test_enum_keeps_schema_order(self):
        r = explorer().field_values("tumor_site")
        assert r.kind == "values"
        assert r.data["enum_preview"] == ["Brain", "Skin", "Bone"]
        assert r.data["enum_total"] == 3
        assert r.data["path"] == "tumor_assessments"

    def test_number_field_has_no_values(self):
        r = explorer().field_values("age_at_censor_status")
        assert r.kind == "values"
        assert r.data["field_type"] == "number"
        assert r.data["enum_total"] == 0

    def test_ambiguous_field_not_merged(self):
        r = explorer().field_values("stage")
        assert r.data["ambiguous"] is True
        by_path = {p["path"]: p["enum_preview"] for p in r.data["placements"]}
        assert by_path == {"tumor_assessments": ["1", "2"], "histologies": ["3", "4"]}
        assert all(p["field"] == "stage" for p in r.data["placements"])
        assert "specify a path" in r.text

    def test_ambiguous_resolved_by_path(self):
        r = explorer().field_values("stage", "histologies")
        assert r.data.get("ambiguous") is None
        assert r.data["enum_preview"] == ["3", "4"]
        assert r.data["path"] == "histologies"


class TestDescribe:
    def test_describe_enum_field(self):
        r = explorer().describe_field("tumor_classification")
        assert r.kind == "describe"
        p = r.data["placements"][0]
        assert p["field_type"] == "enum"
        assert p["description"] == "Tumor stage"
        assert "type: enum" in r.text
        assert "Tumor stage" in r.text

    def test_describe_lists_all_placements(self):
        r = explorer().describe_field("stage")
        assert [p["path"] for p in r.data["placements"]] == ["histologies", "tumor_assessments"]


class TestFindValue:
    def test_find_value(self):
        r = explorer().find_value("Metastatic")
        assert r.kind == "value_fields"
        assert r.data["fields"] == [{"field": "tumor_classification", "path": "tumor_assessments"}]
        assert "is a value of" in r.text

    def test_find_value_is_placement_precise(self):
        # "1" is only a value of stage under tumor_assessments, not histologies
        r = explorer().find_value("1")
        assert r.data["fields"] == [{"field": "stage", "path": "tumor_assessments"}]

    def test_unknown_value(self):
        r = explorer().find_value("Nope")
        assert r.data["code"] == "unknown_value"

    def test_empty_value(self):
        assert explorer().find_value("  ").data["code"] == "empty_value"


class TestListPaths:
    def test_list_paths(self):
        r = explorer().list_paths()
        assert r.kind == "paths"
        assert [p["path"] for p in r.data["paths"]] == ["histologies", "tumor_assessments"]
        assert {p["path"]: p["field_count"] for p in r.data["paths"]} == \
            {"histologies": 2, "tumor_assessments": 3}
        assert r.data["top_level_field_count"] == 3


class TestErrors:
    def test_unknown_field(self):
        r = explorer().field_values("nope")
        assert r.kind == "error"
        assert r.data["code"] == "unknown_field"
        assert "available_fields" in r.data
        assert r.data["available_fields_total"] >= 1

    def test_empty_field(self):
        assert explorer().field_values("").data["code"] == "empty_field"

    def test_unknown_path(self):
        assert explorer().list_fields("nope").data["code"] == "unknown_path"

    def test_empty_path(self):
        assert explorer().list_fields("   ").data["code"] == "empty_path"

    def test_wrong_path(self):
        r = explorer().field_values("tumor_site", "histologies")
        assert r.data["code"] == "wrong_path"
        assert r.data["field_paths"] == ["tumor_assessments"]

    def test_empty_path_in_resolve(self):
        assert explorer().field_values("stage", "  ").data["code"] == "empty_path"


class TestDispatch:
    def test_answer_strips_query_type(self):
        r = explorer().answer("  values  ", field="tumor_site")
        assert r.kind == "values"
        assert r.data["enum_total"] == 3

    def test_answer_routes_each_type(self):
        ex = explorer()
        assert ex.answer("list_tables").kind == "paths"
        assert ex.answer("list_fields").kind == "fields"
        assert ex.answer("describe", field="sex").kind == "describe"
        assert ex.answer("find_value", value="Brain").kind == "value_fields"

    def test_unknown_query_type(self):
        assert explorer().answer("frobnicate").data["code"] == "unknown_query_type"


class TestPreviewLimit:
    def test_preview_caps_enum(self):
        r = explorer(preview_limit=2).field_values("tumor_site")   # 3 values
        assert r.data["enum_preview"] == ["Brain", "Skin"]
        assert r.data["enum_more_count"] == 1
        assert "(+1 more)" in r.text

    def test_invalid_preview_limit(self):
        with pytest.raises(ValueError):
            SchemaExplorer(make_schema(), preview_limit=0)