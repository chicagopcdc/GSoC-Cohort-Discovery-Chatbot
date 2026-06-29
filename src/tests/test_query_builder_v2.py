import sys
from pathlib import Path
from types import SimpleNamespace

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))
    
import services.query_builder_v2 as qb_module
from services.query_builder_v2 import BuildResult, QueryBuilder
from services.filter_generator import GenerationResult
from services.filter_validator import ValidationIssue, ValidationResult
from services.term_normalizer import NumericConstraint


def _schema(*top_level_names):
    fields = [SimpleNamespace(name=name) for name in top_level_names]
    return SimpleNamespace(top_level_fields=lambda: list(fields))


class _FakeGenerator:
    def __init__(self, result, schema):
        self._result = result
        self.schema = schema

    def generate(self, query, *, current_filter=None):
        return self._result


def _gen_ok(filter_obj="FILTER", wire=None, dropped=()):
    return GenerationResult(
        filter=filter_obj,
        wire=wire if wire is not None else {"IN": {"sex": ["Male"]}},
        validation=ValidationResult([]),
        attempts=1,
        raw_outputs=["{}"],
        model="fake",
        dropped_ranges=dropped,
    )


def _gen_fail(issues):
    return GenerationResult(
        filter=None,
        wire=None,
        validation=ValidationResult(issues),
        attempts=3,
        raw_outputs=["{}"],
        model="fake",
    )


def _builder(result, schema):
    return QueryBuilder(_FakeGenerator(result, schema))


class TestBuildResultOk:
    def test_ok_requires_generation_ok(self):
        gen = _gen_fail([ValidationIssue("unknown_field", "unknown field 'foo'")])
        res = BuildResult(
            graphql={"query": "AGG"},
            filter=None,
            wire=None,
            data_type="subject",
            histogram_fields=(),
            generation=gen,
        )
        assert res.ok is False

    def test_ok_true_when_query_and_generation_ok(self):
        gen = _gen_ok()
        res = BuildResult(
            graphql={"query": "AGG"},
            filter=gen.filter,
            wire=gen.wire,
            data_type="subject",
            histogram_fields=(),
            generation=gen,
        )
        assert res.ok is True


class TestBuild:
    def test_success_passes_filter_not_wire(self, monkeypatch):
        seen = {}

        def fake_template(filter_obj, *, data_type, accessibility, histogram_fields=None):
            seen.update(
                filter=filter_obj,
                data_type=data_type,
                accessibility=accessibility,
                histogram_fields=histogram_fields,
            )
            return {"query": "AGG"}

        monkeypatch.setattr(qb_module, "build_aggregation_query", fake_template)

        gen = _gen_ok()
        res = _builder(gen, _schema("sex", "race")).build(
            "INRG males", histogram_fields=["sex", "race"]
        )

        assert res.ok
        assert res.graphql == {"query": "AGG"}
        assert res.generation is gen
        assert seen["filter"] == "FILTER"
        assert seen["data_type"] == "subject"
        assert seen["accessibility"] == "all"
        assert seen["histogram_fields"] == ["sex", "race"]
        assert res.errors == ()
        assert res.warnings == ()

    def test_generation_failure_returns_errors_and_skips_template(self, monkeypatch):
        called = []
        monkeypatch.setattr(
            qb_module,
            "build_aggregation_query",
            lambda *a, **k: called.append(1) or {},
        )

        gen = _gen_fail([ValidationIssue("unknown_field", "unknown field 'foo'")])
        res = _builder(gen, _schema("sex")).build("foo")

        assert not res.ok
        assert res.graphql is None
        assert any("unknown_field" in e for e in res.errors)
        assert called == []

    def test_template_value_error_is_graceful(self, monkeypatch):
        def boom(*a, **k):
            raise ValueError("unknown accessibility 'x'")

        monkeypatch.setattr(qb_module, "build_aggregation_query", boom)

        gen = _gen_ok()
        res = _builder(gen, _schema("sex")).build("q", accessibility="x")

        assert not res.ok
        assert res.graphql is None
        assert any("graphql_template" in e for e in res.errors)
        assert res.wire == gen.wire


class TestHistograms:
    def test_invalid_histogram_warns_but_still_builds(self, monkeypatch):
        monkeypatch.setattr(
            qb_module,
            "build_aggregation_query",
            lambda f, *, data_type, accessibility, histogram_fields=None: {"h": histogram_fields},
        )

        res = _builder(_gen_ok(), _schema("sex", "race")).build(
            "q", histogram_fields=["sex", "not_a_field"]
        )

        assert res.ok
        assert res.graphql == {"h": ["sex"]}
        assert any("not_a_field" in w for w in res.warnings)

    def test_duplicate_histogram_is_deduped_in_order(self, monkeypatch):
        seen = {}
        monkeypatch.setattr(
            qb_module,
            "build_aggregation_query",
            lambda f, *, data_type, accessibility, histogram_fields=None: seen.update(h=histogram_fields) or {"q": 1},
        )

        res = _builder(_gen_ok(), _schema("sex", "race")).build(
            "q", histogram_fields=["sex", "sex", "race", "sex"]
        )

        assert seen["h"] == ["sex", "race"]
        assert res.histogram_fields == ("sex", "race")
        assert res.warnings == ()

    def test_non_subject_data_type_passes_histograms_through(self, monkeypatch):
        seen = {}
        monkeypatch.setattr(
            qb_module,
            "build_aggregation_query",
            lambda f, *, data_type, accessibility, histogram_fields=None: seen.update(h=histogram_fields, dt=data_type) or {"q": 1},
        )

        res = _builder(_gen_ok(), _schema("sex")).build(
            "q",
            data_type="tumor_assessment",
            histogram_fields=["tumor_classification", "tumor_classification"],
        )

        assert seen["dt"] == "tumor_assessment"
        assert seen["h"] == ["tumor_classification"]
        assert res.warnings == ()


class TestDroppedRanges:
    def test_dropped_range_becomes_warning(self, monkeypatch):
        monkeypatch.setattr(qb_module, "build_aggregation_query", lambda *a, **k: {"q": 1})

        dropped = (
            NumericConstraint(op="gt", value=5.0, unit="years", quantity="age", span=(0, 0)),
        )
        res = _builder(_gen_ok(dropped=dropped), _schema("sex")).build("older than 5 years")

        assert res.ok
        assert any("dropped range" in w and "years" in w for w in res.warnings)