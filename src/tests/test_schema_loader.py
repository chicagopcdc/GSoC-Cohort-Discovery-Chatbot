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

from services.schema_loader import (
    DEFAULT_GITOPS,
    DEFAULT_PCDC_SCHEMA,
    FieldSpec,
    SchemaIndex,
    _build_field_spec,
    _find_filterable_fields,
    _plural_candidates,
)

SCHEMA_DIR = _find_upwards("schema")


@pytest.fixture(scope="module")
def idx() -> SchemaIndex:
    return SchemaIndex.from_files(
        SCHEMA_DIR / DEFAULT_PCDC_SCHEMA,
        SCHEMA_DIR / DEFAULT_GITOPS,
    )


class TestResolution:
    def test_no_unresolved_fields(self, idx):
        assert idx.unresolved == []

    def test_field_count_reasonable(self, idx):
        assert len(list(idx.all_fields())) >= 90


class TestTopLevelFields:
    def test_consortium_is_enum(self, idx):
        spec = idx.get_field("consortium")
        assert spec is not None
        assert spec.field_type == "enum"
        assert spec.parent_path is None
        assert "INRG" in spec.enum_values

    def test_sex_resolves_from_person_yaml(self, idx):
        spec = idx.get_field("sex")
        assert spec is not None
        assert spec.field_type == "enum"
        assert spec.parent_path is None
        assert {"Male", "Female", "Not Reported"} <= set(spec.enum_values)

    def test_age_at_censor_status_is_number(self, idx):
        spec = idx.get_field("age_at_censor_status")
        assert spec is not None
        assert spec.field_type == "number"
        assert spec.parent_path is None


class TestNestedFields:
    def test_tumor_classification_is_ambiguous(self, idx):
        assert idx.get_field("tumor_classification") is None

    def test_tumor_classification_with_path(self, idx):
        spec = idx.get_field("tumor_classification", path="tumor_assessments")
        assert spec is not None
        assert spec.field_type == "enum"
        assert spec.parent_path == "tumor_assessments"
        assert "Metastatic" in spec.enum_values

    def test_get_fields_returns_all_occurrences(self, idx):
        specs = idx.get_fields("tumor_classification")
        paths = {s.parent_path for s in specs}
        assert {"tumor_assessments", "biopsy_surgical_procedures",
                "radiation_therapies"} <= paths

    def test_paths_of_lists_all_paths(self, idx):
        paths = idx.paths_of("tumor_classification")
        assert "tumor_assessments" in paths
        assert len(paths) >= 3

    def test_tumor_site_under_tumor_assessments(self, idx):
        spec = idx.get_field("tumor_site", path="tumor_assessments")
        assert spec is not None
        assert "Skin" in spec.enum_values
        assert "Brain" in spec.enum_values


class TestNewSchemaContent:
    def test_minimal_residual_diseases_path_exists(self, idx):
        assert "minimal_residual_diseases" in idx.all_paths()

    def test_mrd_result_resolves(self, idx):
        spec = idx.get_field("mrd_result", path="minimal_residual_diseases")
        assert spec is not None
        assert spec.parent_path == "minimal_residual_diseases"


class TestEnumQueries:
    def test_enum_values_union_without_path(self, idx):
        values = idx.enum_values("tumor_classification")
        assert "Metastatic" in values

    def test_enum_values_with_path(self, idx):
        values = idx.enum_values("tumor_classification", path="tumor_assessments")
        assert "Metastatic" in values

    def test_is_valid_value_true(self, idx):
        assert idx.is_valid_value("sex", "Male") is True

    def test_is_valid_value_false(self, idx):
        assert idx.is_valid_value("sex", "Martian") is False

    def test_is_valid_value_with_path(self, idx):
        assert idx.is_valid_value(
            "tumor_classification", "Metastatic", path="tumor_assessments"
        ) is True

    def test_fields_containing_value_multi(self, idx):
        hits = idx.fields_containing_value("Metastatic")
        assert "tumor_classification" in hits
        assert len(hits) >= 1


class TestOverrides:
    def test_year_at_disease_phase_redirects_to_timing(self, idx):
        spec = idx.get_field("year_at_disease_phase")
        assert spec is not None
        assert spec.field_type == "number"
        assert spec.parent_path is None

    def test_subject_submitter_id_inline(self, idx):
        spec = idx.get_field("subject_submitter_id")
        assert spec is not None
        assert spec.field_type == "string"

    def test_lkss_obfuscated_inline(self, idx):
        spec = idx.get_field("lkss_obfuscated", path="survival_characteristics")
        assert spec is not None
        assert spec.field_type == "string"

    def test_biospecimen_status_inline(self, idx):
        spec = idx.get_field("biospecimen_status")
        assert spec is not None
        assert spec.field_type == "string"
        assert spec.parent_path is None


class TestUnknownAndMissing:
    def test_unknown_field_returns_none(self, idx):
        assert idx.get_field("definitely_not_a_field") is None

    def test_is_known_field(self, idx):
        assert idx.is_known_field("consortium") is True
        assert idx.is_known_field("definitely_not_a_field") is False

    def test_enum_values_missing_field_empty(self, idx):
        assert idx.enum_values("definitely_not_a_field") == ()


class TestPluralCandidates:
    def test_simple_plural(self):
        assert "tumor_assessments" in _plural_candidates("tumor_assessment")

    def test_y_to_ies(self):
        assert "histologies" in _plural_candidates("histology")

    def test_already_plural_kept(self):
        assert _plural_candidates("molecular_analysis") == ["molecular_analysis"]


class TestBuildFieldSpec:
    def test_enum_property(self):
        spec = _build_field_spec("f", {"enum": ["A", "B"], "description": "d"}, None)
        assert spec.field_type == "enum"
        assert spec.enum_values == ("A", "B")
        assert spec.description == "d"

    def test_number_property(self):
        spec = _build_field_spec("f", {"type": ["number"]}, "p")
        assert spec.field_type == "number"
        assert spec.parent_path == "p"

    def test_string_property(self):
        spec = _build_field_spec("f", {"type": ["string"]}, None)
        assert spec.field_type == "string"

    def test_non_dict_property_unknown(self):
        spec = _build_field_spec("f", "$ref-string", None)
        assert spec.field_type == "unknown"

    def test_untyped_property_unknown(self):
        spec = _build_field_spec("f", {"description": "no type or enum"}, None)
        assert spec.field_type == "unknown"


class TestFindFilterableFields:
    def test_extracts_dotted_and_plain(self):
        gitops = {"x": {"fields": ["sex", "tumor_assessments.tumor_site"]}}
        out = _find_filterable_fields(gitops)
        assert (None, "sex") in out
        assert ("tumor_assessments", "tumor_site") in out

    def test_dedupes(self):
        gitops = {"a": {"fields": ["sex"]}, "b": {"fields": ["sex"]}}
        out = _find_filterable_fields(gitops)
        assert out.count((None, "sex")) == 1

    def test_ignores_non_string_entries(self):
        gitops = {"x": {"fields": ["sex", 123, None]}}
        out = _find_filterable_fields(gitops)
        assert out == [(None, "sex")]


class TestDirectConstruction:
    def test_build_from_synthetic_fields(self):
        fields = {
            (None, "sex"): FieldSpec("sex", "enum", ("Male", "Female"), "", None),
            ("tumor_assessments", "tumor_site"):
                FieldSpec("tumor_site", "enum", ("Skin",), "", "tumor_assessments"),
        }
        idx = SchemaIndex(fields)
        assert idx.get_field("sex").field_type == "enum"
        assert idx.fields_under_path("tumor_assessments")[0].name == "tumor_site"
        assert idx.top_level_fields()[0].name == "sex"