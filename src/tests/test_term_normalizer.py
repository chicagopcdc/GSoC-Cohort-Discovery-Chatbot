import sys
from pathlib import Path
from types import SimpleNamespace


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

from services.term_normalizer import TermNormalizer, _parse_num


def _spec(name, field_type="enum", parent_path=None, description="", enum_values=()):
    return SimpleNamespace(
        name=name,
        field_type=field_type,
        parent_path=parent_path,
        description=description,
        enum_values=tuple(enum_values),
    )


class _FakeSchema:
    """Minimal SchemaIndex stand-in for the normalizer."""

    def __init__(self, specs):
        self._specs = list(specs)
        self._by_name = {}
        self._by_value = {}
        for s in self._specs:
            self._by_name.setdefault(s.name, []).append(s)
            for v in s.enum_values:
                self._by_value.setdefault(v, []).append(s.name)

    def all_fields(self):
        return list(self._specs)

    def get_fields(self, name):
        return list(self._by_name.get(name, []))

    def fields_containing_value(self, value):
        seen, out = set(), []
        for name in self._by_value.get(value, []):
            if name not in seen:
                seen.add(name)
                out.append(name)
        return out


def _norm():
    schema = _FakeSchema([
        _spec("race", enum_values=("White", "Black or African American", "Not Reported")),
        _spec("sex", enum_values=("Male", "Female", "Not Reported")),
        _spec("ethnicity", enum_values=("Hispanic or Latino", "Not Reported")),
        _spec("cytodifferentiation", enum_values=("1", "2", "3")),
        _spec("stage", parent_path="stagings", enum_values=("1", "2", "3")),
        _spec("age_at_censor_status", field_type="number",
              description="Age (in days) of the subject at the event censor status."),
        _spec("age_at_tumor_assessment", field_type="number", parent_path="tumor_assessments",
              description="Age of subject (in days) at tumor assessment."),
    ])
    return TermNormalizer(schema)


class TestWordNumbers:
    def test_digits_and_words(self):
        assert _parse_num("5") == 5.0
        assert _parse_num("five") == 5.0
        assert _parse_num("twenty-one") == 21.0
        assert _parse_num("thirteen") == 13.0

    def test_word_number_in_range(self):
        nq = _norm().normalize("patients younger than five years")
        assert len(nq.ranges) == 1
        assert nq.ranges[0].op == "lt"
        assert nq.ranges[0].original_value == 5.0
        assert nq.ranges[0].value == 1826.25


class TestRangePhrasings:
    def test_before_age(self):
        r = _norm().normalize("subjects before age 5").ranges
        assert (r[0].op, r[0].field, r[0].value) == ("lt", "age_at_censor_status", 1826.25)

    def test_after_age(self):
        r = _norm().normalize("after age 2").ranges
        assert r[0].op == "gt"

    def test_up_to_age(self):
        r = _norm().normalize("up to age 10").ranges
        assert r[0].op == "lte"


class TestNumericFalsePositive:
    def test_bare_number_in_range_not_matched_as_enum(self):
        nq = _norm().normalize("age at censor status is at least 3 months")
        assert len(nq.ranges) == 1 and nq.ranges[0].op == "gte"
        assert all(t.value != "3" for t in nq.terms)   # "3" is not an enum term here

    def test_stage_number_still_matched_without_range(self):
        nq = _norm().normalize("stage 3 patients")
        assert any(t.value == "3" and t.placements[0].field == "stage" for t in nq.terms)


class TestFieldDisambiguation:
    def test_not_reported_narrows_to_race(self):
        nq = _norm().normalize("race was not reported")
        terms = [t for t in nq.terms if t.value == "Not Reported"]
        assert len(terms) == 1
        assert [(p.field, p.path) for p in terms[0].placements] == [("race", None)]

    def test_no_field_information_reported(self):
        # No literal "Not Reported" token; inferred from "no ... reported".
        nq = _norm().normalize("no race information reported")
        terms = [t for t in nq.terms if t.value == "Not Reported"]
        assert len(terms) == 1
        assert terms[0].placements[0].field == "race"

    def test_bare_no_is_not_an_enum(self):
        nq = _norm().normalize("no race information reported")
        assert all(t.value != "No" for t in nq.terms)

    def test_mention_disambiguates_shared_value(self):
        nq = _norm().normalize("subjects whose sex is not reported")
        terms = [t for t in nq.terms if t.value == "Not Reported"]
        assert terms and terms[0].placements[0].field == "sex"


class TestLatestFlag:
    def test_latest_detected(self):
        nq = _norm().normalize("younger than 5 on their last tumor assessment")
        assert nq.ranges[0].field == "age_at_tumor_assessment"
        assert nq.ranges[0].latest is True

    def test_no_latest_by_default(self):
        nq = _norm().normalize("younger than 5")
        assert nq.ranges[0].latest is False


def _amb():
    # "No"/"Yes" on 4 fields and "Present"/"Absent"/"Unknown"/"Not Reported" on
    # 4 fields -> generic markers; "Abdomen" on 2 fields -> specific, kept.
    schema = _FakeSchema([
        _spec("consortium", enum_values=("INRG", "NODAL")),
        _spec("bulk_disease", parent_path="disease_characteristics", enum_values=("No", "Yes")),
        _spec("bulky_nodal_aggregate", parent_path="disease_characteristics", enum_values=("No", "Yes")),
        _spec("mass_present", parent_path="biopsy_surgical_procedures", enum_values=("No", "Yes")),
        _spec("parameningeal_extension", parent_path="tumor_assessments", enum_values=("No", "Yes")),
        _spec("tumor_state", parent_path="tumor_assessments", enum_values=("Present", "Absent")),
        _spec("anaplasia", parent_path="molecular_analysis", enum_values=("Present", "Absent")),
        _spec("medical_history_status", parent_path="medical_histories", enum_values=("Present", "Absent")),
        _spec("molecular_abnormality_result", parent_path="molecular_analysis", enum_values=("Present", "Absent")),
        _spec("sex", enum_values=("Male", "Female", "Unknown", "Not Reported")),
        _spec("race", enum_values=("White", "Unknown", "Not Reported")),
        _spec("ethnicity", enum_values=("Hispanic or Latino", "Unknown", "Not Reported")),
        _spec("censor_status", enum_values=("Alive", "Unknown", "Not Reported")),
        _spec("tumor_site", parent_path="tumor_assessments", enum_values=("Abdomen", "Skin")),
        _spec("smn_site", parent_path="secondary_malignant_neoplasm", enum_values=("Abdomen",)),
    ])
    return TermNormalizer(schema)


class TestAmbiguousCommonValues:
    def _fields(self, result, value):
        t = next((t for t in result.terms if t.value == value), None)
        return {(p.field, p.path) for p in t.placements} if t else None

    def test_no_bulk_disease_binds_to_field(self):
        r = _amb().normalize("patients with no bulk disease")
        assert self._fields(r, "No") == {("bulk_disease", "disease_characteristics")}

    def test_no_bulky_nodal_aggregate_not_consortium(self):
        # "nodal" is part of the field name, must not become consortium NODAL
        r = _amb().normalize("no bulky nodal aggregate")
        assert all(t.value != "NODAL" for t in r.terms)
        assert self._fields(r, "No") == {("bulky_nodal_aggregate", "disease_characteristics")}

    def test_no_mass_present_drops_spurious_present(self):
        r = _amb().normalize("no mass present")
        assert self._fields(r, "No") == {("mass_present", "biopsy_surgical_procedures")}
        assert all(t.value != "Present" for t in r.terms)

    def test_no_race_information_reported_stays_not_reported(self):
        r = _amb().normalize("subjects with no race information reported")
        assert self._fields(r, "Not Reported") == {("race", None)}
        assert all(t.value != "No" for t in r.terms)

    def test_bare_no_race_is_not_missing(self):
        # bare "no race" is not "no race information reported"; no policy applies
        r = _amb().normalize("no race")
        assert all(t.value not in ("Not Reported", "No") for t in r.terms)

    def test_unknown_sex_prefers_unknown_enum(self):
        r = _amb().normalize("unknown sex")
        assert self._fields(r, "Unknown") == {("sex", None)}
        assert all(t.value != "Not Reported" for t in r.terms)

    def test_unnarrowed_generic_value_dropped(self):
        r = _amb().normalize("unknown patients")
        assert all(t.value != "Unknown" for t in r.terms)

    def test_specific_value_kept_across_multiple_fields(self):
        # Abdomen spans tumor_site + smn_site but is a real clinical value
        r = _amb().normalize("tumors in the abdomen")
        assert self._fields(r, "Abdomen") == {
            ("tumor_site", "tumor_assessments"),
            ("smn_site", "secondary_malignant_neoplasm"),
        }
