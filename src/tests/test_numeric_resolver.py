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

from services.numeric_resolver import NumericConfig, NumericFieldResolver


def _spec(name, field_type="number", parent_path=None, description="", enum_values=()):
    return SimpleNamespace(
        name=name,
        field_type=field_type,
        parent_path=parent_path,
        description=description,
        enum_values=tuple(enum_values),
    )


class _FakeSchema:
    """Minimal stand-in for SchemaIndex: only all_fields() is used."""

    def __init__(self, specs):
        self._specs = specs

    def all_fields(self):
        return list(self._specs)


def _resolver(specs, config=None):
    return NumericFieldResolver.from_schema(_FakeSchema(specs), config)


# --- identity ---------------------------------------------------------------
class TestIdentity:
    def test_only_age_at_numeric_fields_are_indexed(self):
        r = _resolver([
            _spec("age_at_censor_status", description="Age (in days) at censor status."),
            _spec("stage", field_type="enum", enum_values=("1", "2")),      # contains "age"
            _spec("language", field_type="string"),                          # contains "age"
            _spec("tumor_size", field_type="number"),                        # numeric, not age
        ])
        assert r.unit_of("age_at_censor_status", None) == "days"
        assert r.unit_of("stage", None) is None
        assert r.unit_of("tumor_size", None) is None

    def test_disabled_field_excluded(self):
        cfg = NumericConfig(disabled_fields=("age_at_lkss",))
        r = _resolver([
            _spec("age_at_censor_status", description="Age in days at censor status."),
            _spec("age_at_lkss", parent_path="survival_characteristics",
                  description="Age in days at the last known survival status."),
        ], cfg)
        assert r.unit_of("age_at_lkss", "survival_characteristics") is None


# --- unit derivation --------------------------------------------------------
class TestUnitDerivation:
    def test_unit_parsed_from_description(self):
        r = _resolver([_spec("age_at_foo", description="Age in months at foo.")])
        assert r.unit_of("age_at_foo", None) == "months"

    def test_blank_description_uses_default_unit(self):
        r = _resolver([_spec("age_at_foo", description="")])
        assert r.unit_of("age_at_foo", None) == "days"

    def test_unit_override_wins_over_description(self):
        cfg = NumericConfig(unit_overrides={"age_at_foo": "weeks"})
        r = _resolver([_spec("age_at_foo", description="Age in days at foo.")], cfg)
        assert r.unit_of("age_at_foo", None) == "weeks"


# --- routing ----------------------------------------------------------------
class TestRouting:
    def _built(self):
        return _resolver([
            _spec("age_at_censor_status", description="Age (in days) at the event censor status."),
            _spec("age_at_tumor_assessment", parent_path="tumor_assessments",
                  description="Age in days at tumor assessment"),
            _spec("age_at_lkss", parent_path="survival_characteristics",
                  description="Age of subject (in days) at the last known survival status."),
        ])

    def test_description_cue_routes_high_confidence(self):
        r = self._built()
        text = "younger than 5 on their last tumor assessment"
        res = r.route(text, (0, 14))
        assert (res.field, res.path, res.confidence) == (
            "age_at_tumor_assessment", "tumor_assessments", "high",
        )

    def test_description_tail_routes_lkss(self):
        r = self._built()
        text = "at least 2 years at last known survival status"
        res = r.route(text, (0, 18))
        assert res.field == "age_at_lkss"
        assert res.confidence == "high"

    def test_no_cue_falls_back_to_default(self):
        r = self._built()
        res = r.route("not older than 5", (0, 16))
        assert res.field == "age_at_censor_status"
        assert res.confidence == "default"

    def test_single_generic_token_does_not_route(self):
        # "assessment" alone is below min_cue_words, so it must not route.
        r = self._built()
        res = r.route("older than 5 at assessment", (0, 12))
        assert res.confidence == "default"

    def test_alias_routes_when_description_is_unhelpful(self):
        cfg = NumericConfig(cue_aliases={"lkss": "age_at_lkss"})
        r = _resolver([
            _spec("age_at_censor_status", description="Age in days at censor status."),
            _spec("age_at_lkss", parent_path="survival_characteristics", description=""),
        ], cfg)
        res = r.route("older than 3 lkss", (0, 12))
        assert res.field == "age_at_lkss"
        assert res.confidence == "high"


# --- conversion -------------------------------------------------------------
class TestConversion:
    def test_years_to_days(self):
        r = _resolver([_spec("age_at_x", description="Age in days at x.")])
        assert r.convert(5, "years", "days") == 1826.25

    def test_weeks_to_days(self):
        r = _resolver([_spec("age_at_x", description="Age in days at x.")])
        assert r.convert(1, "weeks", "days") == 7.0

    def test_unknown_unit_returns_none(self):
        r = _resolver([_spec("age_at_x", description="Age in days at x.")])
        assert r.convert(5, "furlongs", "days") is None

    def test_custom_factors_from_config(self):
        cfg = NumericConfig(factors_to_base={"days": 1.0, "years": 360.0})
        r = _resolver([_spec("age_at_x", description="Age in days at x.")], cfg)
        assert r.convert(1, "years", "days") == 360.0


# --- bind (route + convert + provenance) ------------------------------------
class TestBind:
    def _built(self):
        return _resolver([
            _spec("age_at_censor_status", description="Age (in days) at the event censor status."),
            _spec("age_at_tumor_assessment", parent_path="tumor_assessments",
                  description="Age in days at tumor assessment"),
        ])

    def test_bind_converts_and_records_provenance(self):
        r = self._built()
        b = r.bind(5, "years", "younger than 5 years on last tumor assessment", (0, 20))
        assert b.field == "age_at_tumor_assessment"
        assert b.path == "tumor_assessments"
        assert b.value == 1826.25
        assert b.unit == "days"
        assert (b.original_value, b.original_unit) == (5, "years")
        assert b.converted is True
        assert b.assumed_unit is False

    def test_bare_number_assumes_configured_unit(self):
        r = self._built()
        b = r.bind(5, None, "not older than 5", (0, 16))
        assert b.value == 1826.25
        assert b.assumed_unit is True
        assert b.original_unit == "years"

    def test_assumption_disabled_leaves_value_unconverted(self):
        cfg = NumericConfig(assumed_input_unit=None)
        r = _resolver([_spec("age_at_censor_status", description="Age in days at censor status.")], cfg)
        b = r.bind(5, None, "not older than 5", (0, 16))
        assert b.converted is False
        assert b.value == 5
