import json
import sys
from pathlib import Path
from types import SimpleNamespace

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.agent import CohortAgent


# --- canned assistant messages ---------------------------------------------
def tool_call(call_id, name, args):
    return {
        "role": "assistant",
        "content": None,
        "tool_calls": [{
            "id": call_id, "type": "function",
            "function": {"name": name, "arguments": json.dumps(args)},
        }],
    }


def text_reply(text):
    return {"role": "assistant", "content": text}


class ScriptedChat:
    def __init__(self, *messages):
        self._messages = list(messages)
        self._i = 0
        self.calls = []

    def __call__(self, messages, tools):
        self.calls.append({"messages": messages, "tools": tools})
        msg = self._messages[min(self._i, len(self._messages) - 1)]
        self._i += 1
        return msg


# --- fake tools -------------------------------------------------------------
def build_obj(ok=True, wire=None, graphql=None, data_type="subject", errors=(), warnings=()):
    return SimpleNamespace(
        ok=ok,
        wire=wire or {"IN": {"sex": ["Male"]}},
        graphql=graphql or {"query": "agg"},
        data_type=data_type,
        errors=tuple(errors),
        warnings=tuple(warnings),
    )


def fail_build():
    return SimpleNamespace(
        ok=False, wire=None, graphql=None, data_type="subject",
        errors=("invalid_enum_value",), warnings=(),
    )


class FakeSessionManager:
    def __init__(self, *turns):
        self._turns = list(turns)          # each: (mode, build_obj)
        self._i = 0
        self.turn_calls = []
        self.reset_calls = []

    def turn(self, session_id, text, *, data_type=None):
        self.turn_calls.append((session_id, text))
        mode, build = self._turns[min(self._i, len(self._turns) - 1)]
        self._i += 1
        return SimpleNamespace(session_id=session_id, mode=mode, build=build)

    def reset(self, session_id):
        self.reset_calls.append(session_id)


def guppy_result(ok=True, total=None, histograms=None, errors=()):
    return SimpleNamespace(ok=ok, total_count=total, histograms=histograms or {}, errors=tuple(errors))


class FakeGuppy:
    def __init__(self, result):
        self._result = result
        self.execute_calls = []

    def execute(self, graphql, *, data_type=None):
        self.execute_calls.append((graphql, data_type))
        return self._result


# --- tests ------------------------------------------------------------------
class TestHappyPath:
    def test_build_then_count_then_reply(self):
        sm = FakeSessionManager(("new", build_obj(graphql={"query": "Q"})))
        guppy = FakeGuppy(guppy_result(total=123))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "INRG males"}),
            tool_call("c2", "count_cohort", {}),
            text_reply("There are 123 matching subjects."),
        )
        agent = CohortAgent(sm, guppy_client=guppy, chat_fn=chat)
        res = agent.chat("s1", "how many INRG males?")

        assert res.reply == "There are 123 matching subjects."
        assert res.llm_calls == 3
        assert not res.stopped
        assert [s.tool for s in res.steps] == ["build_query", "count_cohort"]
        assert res.steps[0].result["ok"] is True
        assert res.steps[1].result["total_count"] == 123
        assert guppy.execute_calls == [({"query": "Q"}, "subject")]
        assert sm.turn_calls == [("s1", "INRG males")]


class TestFailedBuild:
    def test_failed_build_keeps_last_good_query(self):
        sm = FakeSessionManager(
            ("new", build_obj(graphql={"query": "GOOD"})),   # first build: good
            ("modify", fail_build()),                         # second build: fails
        )
        guppy = FakeGuppy(guppy_result(total=42))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "find INRG males"}),
            text_reply("Built."),
            tool_call("c2", "build_query", {"query": "change sex to invalid"}),
            tool_call("c3", "count_cohort", {}),
            text_reply("Still 42."),
        )
        agent = CohortAgent(sm, guppy_client=guppy, chat_fn=chat)

        agent.chat("s1", "find INRG males")              # caches GOOD
        res = agent.chat("s1", "change sex to invalid")  # build fails, count GOOD

        assert res.steps[-2].result["ok"] is False        # failed build reported
        assert res.steps[-1].result["total_count"] == 42  # count ran the good query
        assert guppy.execute_calls[-1] == ({"query": "GOOD"}, "subject")


class TestErrors:
    def test_count_before_build_errors(self):
        chat = ScriptedChat(
            tool_call("c1", "count_cohort", {}),
            text_reply("Please describe a cohort first."),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())),
                            guppy_client=FakeGuppy(guppy_result(total=1)), chat_fn=chat)
        res = agent.chat("s1", "how many?")
        assert "error" in res.steps[0].result

    def test_unknown_tool_errors(self):
        chat = ScriptedChat(tool_call("c1", "frobnicate", {}), text_reply("ok"))
        agent = CohortAgent(FakeSessionManager(("new", build_obj())), chat_fn=chat)
        res = agent.chat("s1", "x")
        assert "unknown tool" in res.steps[0].result["error"]

    def test_tool_exception_is_caught(self):
        class Boom:
            def turn(self, *a, **k):
                raise RuntimeError("kaboom")
            def reset(self, *a, **k):
                pass

        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "x"}),
            text_reply("Sorry, something went wrong."),
        )
        agent = CohortAgent(Boom(), chat_fn=chat)
        res = agent.chat("s1", "x")                       # must NOT raise
        assert "kaboom" in res.steps[0].result["error"]
        assert res.reply == "Sorry, something went wrong."

    def test_guppy_unavailable(self):
        sm = FakeSessionManager(("new", build_obj(graphql={"query": "Q"})))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "x"}),
            tool_call("c2", "count_cohort", {}),
            text_reply("Execution isn't available right now."),
        )
        agent = CohortAgent(sm, chat_fn=chat)             # no guppy_client
        res = agent.chat("s1", "x")
        assert "not available" in res.steps[1].result["error"]


class TestLoopControl:
    def test_step_cap_stops(self):
        chat = ScriptedChat(tool_call("c1", "build_query", {"query": "x"}))  # never a text reply
        agent = CohortAgent(FakeSessionManager(("new", build_obj())), chat_fn=chat, max_steps=3)
        res = agent.chat("s1", "loop")
        assert res.stopped is True
        assert res.llm_calls == 3
        assert len(res.steps) == 3

    def test_reset_clears_session_and_cache(self):
        sm = FakeSessionManager(("new", build_obj(graphql={"query": "Q"})))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "x"}),
            text_reply("built"),
            tool_call("c2", "count_cohort", {}),
            text_reply("no query now"),
        )
        agent = CohortAgent(sm, guppy_client=FakeGuppy(guppy_result(total=9)), chat_fn=chat)
        agent.chat("s1", "x")          # builds + caches
        agent.reset("s1")              # clears cache + session
        res = agent.chat("s1", "count")
        assert sm.reset_calls == ["s1"]
        assert "error" in res.steps[-1].result