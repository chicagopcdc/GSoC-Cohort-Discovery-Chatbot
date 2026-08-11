import sys
from pathlib import Path
from types import SimpleNamespace

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.cohort_analyzer import (
    CohortAnalyzer,
    DEFAULT_SUMMARY_FIELDS,
    format_comparison,
    format_summary,
)


# --- fake Guppy (mirrors GuppyResult's attributes) --------------------------
def gres(total, histograms=None, *, total_masked=False, errors=()):
    return SimpleNamespace(
        total_count=total,
        histograms=histograms or {},
        total_masked=total_masked,
        errors=list(errors),
        ok=(not errors),
    )


class FakeGuppy:
    def __init__(self, *results):
        self._results = list(results)
        self._i = 0
        self.calls = []

    def execute(self, graphql, *, data_type=None):
        self.calls.append((graphql, data_type))
        r = self._results[min(self._i, len(self._results) - 1)]
        self._i += 1
        return r


# --- summarize --------------------------------------------------------------
class TestSummarize:
    def test_counts_and_percentages(self):
        g = FakeGuppy(gres(1000, {"sex": [{"key": "Male", "count": 560},
                                           {"key": "Female", "count": 440}]}))
        s = CohortAnalyzer(g, fields=["sex"]).summarize(
            {"IN": {"consortium": ["INRG"]}}, label="INRG")

        assert s.ok and s.total == 1000
        d = s.distributions[0]
        assert d.field == "sex"
        assert [(b.key, b.count, round(b.pct, 2)) for b in d.buckets] == [
            ("Male", 560, 0.56), ("Female", 440, 0.44)]
        assert "Male 560 (56%)" in format_summary(s)

    def test_buckets_sorted_biggest_first(self):
        g = FakeGuppy(gres(100, {"race": [{"key": "A", "count": 10},
                                          {"key": "B", "count": 70},
                                          {"key": "C", "count": 20}]}))
        s = CohortAnalyzer(g, fields=["race"]).summarize({"IN": {"x": ["1"]}})
        assert [b.key for b in s.distributions[0].buckets] == ["B", "C", "A"]

    def test_masked_and_none_count_are_distinct(self):
        g = FakeGuppy(gres(1000, {"sex": [
            {"key": "Male", "count": 560},
            {"key": "Blocked", "count": None, "masked": True},   # access-limited
            {"key": "NoCount", "count": None},                    # just absent
        ]}))
        s = CohortAnalyzer(g, fields=["sex"]).summarize({"IN": {"x": ["1"]}})

        by = {b.key: b for b in s.distributions[0].buckets}
        assert by["Blocked"].masked is True and by["Blocked"].count is None
        assert by["NoCount"].masked is False and by["NoCount"].count is None

        text = format_summary(s)
        assert "Blocked n/a (masked)" in text
        assert "NoCount n/a" in text and "NoCount n/a (masked)" not in text

    def test_no_fields_gives_total_only(self):
        g = FakeGuppy(gres(42, {}))
        s = CohortAnalyzer(g, fields=[]).summarize({"IN": {"sex": ["Male"]}})
        assert s.ok and s.total == 42
        assert s.distributions == []
        assert "histogram" not in g.calls[0][0]["query"]   # total-only query

    def test_default_fields_requested_as_histograms(self):
        g = FakeGuppy(gres(10, {}))
        CohortAnalyzer(g).summarize({"IN": {"x": ["1"]}})
        query = g.calls[0][0]["query"]
        for f in DEFAULT_SUMMARY_FIELDS:
            assert f"{f} {{ histogram" in query


# --- field selection --------------------------------------------------------
class TestSelectFields:
    def test_dedup_and_allowlist(self):
        a = CohortAnalyzer(FakeGuppy(gres(1)),
                           fields=["sex", "sex", "bogus"], known_fields=["sex", "race"])
        selected, warnings = a._select_fields(None)
        assert selected == ["sex"]
        assert warnings == ["dropped unknown field(s): bogus"]

    def test_invalid_names_filtered(self):
        a = CohortAnalyzer(FakeGuppy(gres(1)))
        selected, warnings = a._select_fields(
            ["sex", "", "tumor_assessments.tumor_site", None, "sex"])
        assert selected == ["sex"]
        assert warnings == [
            "ignored invalid field name(s): , tumor_assessments.tumor_site, None"]

    def test_both_warning_categories(self):
        a = CohortAnalyzer(FakeGuppy(gres(1)), known_fields=["sex", "race"])
        _, warnings = a._select_fields(["sex", "", "bogus"])
        assert warnings == [
            "ignored invalid field name(s): ",
            "dropped unknown field(s): bogus",
        ]


class TestInvalidFilterKeepsWarnings:
    def test_dropped_field_warning_survives_bad_filter(self):
        a = CohortAnalyzer(FakeGuppy(gres(1)),
                           fields=["sex", "bogus"], known_fields=["sex"])
        s = a.summarize("NOT_A_DICT")
        assert not s.ok
        assert any("invalid filter" in e for e in s.errors)
        assert s.warnings == ["dropped unknown field(s): bogus"]


# --- compare ----------------------------------------------------------------
class TestCompare:
    def test_delta_is_b_minus_a(self):
        g = FakeGuppy(
            gres(1000, {"sex": [{"key": "Male", "count": 560},
                                {"key": "Female", "count": 440}]}),
            gres(800, {"sex": [{"key": "Male", "count": 360},
                               {"key": "Female", "count": 440}]}),
        )
        c = CohortAnalyzer(g, fields=["sex"]).compare(
            {"IN": {"x": ["1"]}}, {"IN": {"x": ["2"]}}, label_a="A", label_b="B")

        assert (c.total_a, c.total_b, c.total_delta) == (1000, 800, -200)   # B - A
        male = next(r for r in c.rows if r.key == "Male")
        assert round(male.a_pct, 2) == 0.56 and round(male.b_pct, 2) == 0.45
        assert round(male.delta_pct, 4) == round(0.45 - 0.56, 4)

    def test_independent_masked_flags(self):
        g = FakeGuppy(
            gres(1234, {"sex": [{"key": "Male", "count": 691}]}),
            gres(None, {"sex": [{"key": "Female", "count": 411}]}, total_masked=True),
        )
        c = CohortAnalyzer(g, fields=["sex"]).compare(
            {"IN": {"x": ["1"]}}, {"IN": {"x": ["2"]}}, label_a="INRG", label_b="NODAL")

        assert c.total_a_masked is False and c.total_b_masked is True
        assert c.total_delta is None                     # cannot diff a masked total
        male = next(r for r in c.rows if r.key == "Male")
        female = next(r for r in c.rows if r.key == "Female")
        assert male.b_count is None and male.b_pct is None and male.delta_pct is None
        assert female.a_count == 0 and female.a_pct == 0.0
        assert female.b_count == 411 and female.b_pct is None and female.delta_pct is None
        assert "masked" in format_comparison(c)

    def test_long_keys_stay_aligned(self):
        g = FakeGuppy(
            gres(100, {"race": [{"key": "Black or African American", "count": 50},
                                {"key": "White", "count": 50}]}),
            gres(100, {"race": [{"key": "Black or African American", "count": 40},
                                {"key": "White", "count": 60}]}),
        )
        c = CohortAnalyzer(g, fields=["race"]).compare(
            {"IN": {"x": ["1"]}}, {"IN": {"x": ["2"]}})
        text = format_comparison(c)
        # header, Total, and both data rows keep one shared width despite the long key
        rows = [ln for ln in text.splitlines()
                if ln and not ln.startswith("[") and not ln.startswith("-")]
        assert len({len(ln) for ln in rows}) == 1


# --- error path -------------------------------------------------------------
class TestErrorPath:
    def test_guppy_failure_surfaced(self):
        s = CohortAnalyzer(FakeGuppy(gres(None, {}, errors=["boom"]))).summarize(
            {"IN": {"sex": ["Male"]}})
        assert not s.ok
        assert s.errors == ["boom"]
