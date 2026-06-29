import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import pytest

from services.session_manager import SessionManager, TurnResult
from services.query_builder_v2 import BuildResult
from services.filter_generator import GenerationResult
from services.filter_validator import ValidationIssue, ValidationResult


def _ok_build(wire, data_type="subject"):
    gen = GenerationResult(
        filter="F", wire=wire, validation=ValidationResult([]),
        attempts=1, raw_outputs=["{}"], model="fake",
    )
    return BuildResult(
        graphql={"query": "agg"}, filter="F", wire=wire,
        data_type=data_type, histogram_fields=(), generation=gen,
    )


def _fail_build(data_type="subject"):
    gen = GenerationResult(
        filter=None, wire=None,
        validation=ValidationResult([ValidationIssue("structural", "bad")]),
        attempts=3, raw_outputs=["{}"], model="fake",
    )
    return BuildResult(
        graphql=None, filter=None, wire=None,
        data_type=data_type, histogram_fields=(), generation=gen,
    )


class FakeQueryBuilder:
    def __init__(self, results, *, default_data_type="subject"):
        self.default_data_type = default_data_type
        self.calls = []
        self._results = list(results)
        self._i = 0

    def build(self, query, *, current_filter=None, data_type=None,
              accessibility=None, histogram_fields=None):
        self.calls.append({
            "query": query,
            "current_filter": current_filter,
            "data_type": data_type,
        })
        result = self._results[min(self._i, len(self._results) - 1)]
        self._i += 1
        return result


class TestRouting:
    def test_first_turn_is_always_new_even_with_modify_cue(self):
        qb = FakeQueryBuilder([_ok_build({"IN": {"consortium": ["INRG"]}})])
        mgr = SessionManager(qb)
        r = mgr.turn("s1", "change consortium to NODAL")
        assert r.mode == "new"
        assert qb.calls[0]["current_filter"] is None

    def test_modify_cue_with_prior_filter_seeds_current_filter(self):
        f1 = {"AND": [{"IN": {"consortium": ["INRG"]}}, {"IN": {"sex": ["Male"]}}]}
        f2 = {"AND": [{"IN": {"consortium": ["NODAL"]}}, {"IN": {"sex": ["Male"]}}]}
        qb = FakeQueryBuilder([_ok_build(f1), _ok_build(f2)])
        mgr = SessionManager(qb)
        mgr.turn("s1", "find INRG males")
        r2 = mgr.turn("s1", "change consortium to NODAL")
        assert r2.mode == "modify"
        assert qb.calls[1]["current_filter"] == f1

    def test_modify_cue_without_prior_filter_is_new(self):
        qb = FakeQueryBuilder([_fail_build(), _ok_build({"IN": {"x": ["1"]}})])
        mgr = SessionManager(qb)
        mgr.turn("s1", "find something broken")        # fails -> no base stored
        r2 = mgr.turn("s1", "also add males")          # cue, but no base
        assert r2.mode == "new"
        assert qb.calls[1]["current_filter"] is None

    def test_non_modify_text_with_prior_filter_is_new(self):
        qb = FakeQueryBuilder([
            _ok_build({"IN": {"consortium": ["INRG"]}}),
            _ok_build({"IN": {"sex": ["Male"]}}),
        ])
        mgr = SessionManager(qb)
        mgr.turn("s1", "find INRG patients")
        r2 = mgr.turn("s1", "find male patients")
        assert r2.mode == "new"
        assert qb.calls[1]["current_filter"] is None


class TestRecording:
    def test_failed_modify_keeps_previous_filter(self):
        good = {"AND": [{"IN": {"consortium": ["INRG"]}}]}
        qb = FakeQueryBuilder([_ok_build(good), _fail_build()])
        mgr = SessionManager(qb)
        mgr.turn("s1", "find INRG")
        r2 = mgr.turn("s1", "change consortium to NODAL")
        assert r2.mode == "modify"
        assert r2.build.ok is False
        state = mgr.store.get("s1")
        assert state.current_filter == good
        assert state.turn_count == 2

    def test_successful_turn_advances_current_filter(self):
        qb = FakeQueryBuilder([
            _ok_build({"IN": {"consortium": ["INRG"]}}),
            _ok_build({"IN": {"consortium": ["NODAL"]}}),
        ])
        mgr = SessionManager(qb)
        mgr.turn("s1", "find INRG")
        mgr.turn("s1", "change consortium to NODAL")
        assert mgr.store.get("s1").current_filter == {"IN": {"consortium": ["NODAL"]}}

    def test_reset_clears_session(self):
        qb = FakeQueryBuilder([_ok_build({"IN": {"x": ["1"]}})])
        mgr = SessionManager(qb)
        mgr.turn("s1", "find x")
        assert mgr.store.get("s1") is not None
        mgr.reset("s1")
        assert mgr.store.get("s1") is None


class TestDataType:
    def test_data_type_carried_across_turns(self):
        qb = FakeQueryBuilder([
            _ok_build({"IN": {"x": ["1"]}}),
            _ok_build({"IN": {"x": ["2"]}}),
        ])
        mgr = SessionManager(qb)
        mgr.turn("s1", "find x", data_type="tumor_assessment")
        mgr.turn("s1", "also add y")
        assert qb.calls[0]["data_type"] == "tumor_assessment"
        assert qb.calls[1]["data_type"] == "tumor_assessment"


class TestCueDetection:
    def test_cue_matching(self):
        mgr = SessionManager(FakeQueryBuilder([_ok_build({"IN": {"x": ["1"]}})]))
        assert mgr._looks_like_modification("change consortium to NODAL")
        assert mgr._looks_like_modification("also include males")
        assert mgr._looks_like_modification("remove the age filter")
        assert not mgr._looks_like_modification("find INRG patients")
        assert not mgr._looks_like_modification("")


class TestTurnResult:
    def test_turn_result_carries_build_and_mode(self):
        f1 = _ok_build({"IN": {"x": ["1"]}})
        qb = FakeQueryBuilder([f1])
        mgr = SessionManager(qb)
        r = mgr.turn("s1", "find x")
        assert isinstance(r, TurnResult)
        assert r.mode == "new"
        assert r.build is f1
        assert r.session_id == "s1"