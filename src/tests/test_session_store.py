import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

import threading

import pytest

import services.session_store as session_store
from services.session_store import SessionStore


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, dt):
        self.now += dt


class TestValidation:
    def test_blank_or_none_session_id_rejected(self):
        store = SessionStore()
        for bad in ["", "   ", None, 123]:
            with pytest.raises(ValueError):
                store.get_or_create(bad)
            with pytest.raises(ValueError):
                store.record(bad, "q", {}, True)
            with pytest.raises(ValueError):
                store.get(bad)

    def test_init_param_validation(self):
        with pytest.raises(ValueError):
            SessionStore(ttl_seconds=0)
        with pytest.raises(ValueError):
            SessionStore(ttl_seconds=-5)
        with pytest.raises(ValueError):
            SessionStore(max_sessions=0)
        with pytest.raises(ValueError):
            SessionStore(max_turns_per_session=0)


class TestRecordAndState:
    def test_record_creates_and_advances_filter(self):
        store = SessionStore()
        f = {"AND": [{"IN": {"consortium": ["INRG"]}}]}
        store.record("s1", "find INRG", f, True)
        state = store.get("s1")
        assert state is not None
        assert state.current_filter == f
        assert state.turn_count == 1

    def test_failed_turn_keeps_history_but_not_current_filter(self):
        store = SessionStore()
        good = {"IN": {"consortium": ["INRG"]}}
        store.record("s1", "q1", good, True)
        store.record("s1", "q2 broken", None, False)
        state = store.get("s1")
        assert state.current_filter == good
        assert state.turn_count == 2
        assert state.turns[-1].ok is False
        assert state.turns[-1].filter is None

    def test_stored_filter_isolated_from_caller_mutation(self):
        store = SessionStore()
        f = {"IN": {"consortium": ["INRG"]}}
        store.record("s1", "q", f, True)
        f["IN"]["consortium"].append("NODAL")
        assert store.get("s1").current_filter == {"IN": {"consortium": ["INRG"]}}

    def test_max_turns_trims_oldest(self):
        store = SessionStore(max_turns_per_session=3)
        for i in range(5):
            store.record("s1", f"q{i}", {"IN": {"x": [str(i)]}}, True)
        state = store.get("s1")
        assert state.turn_count == 3
        assert [t.text for t in state.turns] == ["q2", "q3", "q4"]
        assert state.current_filter == {"IN": {"x": ["4"]}}


class TestEviction:
    def test_ttl_eviction(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(session_store.time, "monotonic", clock)
        store = SessionStore(ttl_seconds=10)
        store.record("s1", "q", {"IN": {"x": ["1"]}}, True)
        clock.advance(5)
        assert store.get("s1") is not None
        clock.advance(10)
        assert store.get("s1") is None

    def test_get_does_not_refresh_but_touch_does(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(session_store.time, "monotonic", clock)
        store = SessionStore(ttl_seconds=10)

        store.record("s1", "q", {"IN": {"x": ["1"]}}, True)
        clock.advance(8)
        store.get("s1")            # peek must not refresh
        clock.advance(5)
        assert store.get("s1") is None

        store.record("s2", "q", {"IN": {"x": ["1"]}}, True)
        clock.advance(8)
        store.touch("s2")          # refresh
        clock.advance(5)
        assert store.get("s2") is not None

    def test_max_sessions_evicts_least_recently_active(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(session_store.time, "monotonic", clock)
        store = SessionStore(max_sessions=2)
        store.record("s1", "q", {}, True); clock.advance(1)
        store.record("s2", "q", {}, True); clock.advance(1)
        store.record("s3", "q", {}, True)
        assert store.get("s1") is None
        assert store.get("s2") is not None
        assert store.get("s3") is not None

    def test_recording_existing_session_keeps_it_alive(self, monkeypatch):
        clock = FakeClock()
        monkeypatch.setattr(session_store.time, "monotonic", clock)
        store = SessionStore(max_sessions=2)
        store.record("s1", "q", {}, True); clock.advance(1)
        store.record("s2", "q", {}, True); clock.advance(1)
        store.record("s1", "q again", {}, True); clock.advance(1)  # s1 now newest
        store.record("s3", "q", {}, True)                          # should evict s2
        assert store.get("s2") is None
        assert store.get("s1") is not None
        assert store.get("s3") is not None


class TestMisc:
    def test_clear(self):
        store = SessionStore()
        store.record("s1", "q", {}, True)
        store.clear("s1")
        assert store.get("s1") is None

    def test_active_count(self):
        store = SessionStore()
        store.record("s1", "q", {}, True)
        store.record("s2", "q", {}, True)
        assert store.active_count() == 2

    def test_data_type_fixed_on_create(self):
        store = SessionStore()
        a = store.get_or_create("s1", data_type="subject")
        b = store.get_or_create("s1", data_type="tumor_assessment")
        assert a is b
        assert b.data_type == "subject"

    def test_get_or_create_idempotent(self):
        store = SessionStore()
        assert store.get_or_create("s1") is store.get_or_create("s1")


class TestConcurrency:
    def test_concurrent_records_lose_no_turns(self):
        store = SessionStore()

        def worker(n):
            for i in range(50):
                store.record("s1", f"q{n}-{i}", {"IN": {"x": [str(i)]}}, True)

        threads = [threading.Thread(target=worker, args=(n,)) for n in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert store.get("s1").turn_count == 200