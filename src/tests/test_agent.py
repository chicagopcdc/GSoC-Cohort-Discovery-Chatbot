import json
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


class FakeQueryBuilder:
    def __init__(self, *builds):
        self._builds = list(builds)
        self._i = 0
        self.calls = []

    def build(self, query, *, current_filter=None, data_type=None):
        self.calls.append({
            "query": query,
            "current_filter": current_filter,
            "data_type": data_type,
        })
        if not self._builds:
            return build_obj(wire={"IN": {"consortium": [query]}})
        build = self._builds[min(self._i, len(self._builds) - 1)]
        self._i += 1
        return build


class FakeSessionManager:
    def __init__(self, *turns, qb=None):
        self._turns = list(turns)          # each: (mode, build_obj)
        self._i = 0
        self.qb = qb or FakeQueryBuilder()
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


def summary_obj(*, label="current cohort", total=10, errors=(), warnings=()):
    return SimpleNamespace(
        label=label,
        total=total,
        total_masked=False,
        distributions=[],
        errors=list(errors),
        warnings=list(warnings),
    )


def comparison_obj(*, label_a="current", label_b="NODAL", errors=(), warnings=()):
    return SimpleNamespace(
        label_a=label_a,
        label_b=label_b,
        total_a=10,
        total_b=5,
        total_a_masked=False,
        total_b_masked=False,
        total_delta=-5,
        rows=[],
        errors=list(errors),
        warnings=list(warnings),
    )


class FakeAnalyzer:
    def __init__(self, *, summary=None, comparison=None):
        self._summary = summary or summary_obj()
        self._comparison = comparison or comparison_obj()
        self.summarize_calls = []
        self.compare_calls = []

    def summarize(self, wire, *, label):
        self.summarize_calls.append({"wire": wire, "label": label})
        self._summary.label = label
        return self._summary

    def compare(self, filter_a, filter_b, *, label_a, label_b):
        self.compare_calls.append({
            "filter_a": filter_a,
            "filter_b": filter_b,
            "label_a": label_a,
            "label_b": label_b,
        })
        self._comparison.label_a = label_a
        self._comparison.label_b = label_b
        return self._comparison


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

    def test_count_returns_count_only(self):
        # Histograms stay out of the tool result (module scope says no histograms).
        sm = FakeSessionManager(("new", build_obj(graphql={"query": "Q"})))
        guppy = FakeGuppy(guppy_result(total=5, histograms={"sex": [{"key": "Male", "count": 5}]}))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "x"}),
            tool_call("c2", "count_cohort", {}),
            text_reply("5"),
        )
        agent = CohortAgent(sm, guppy_client=guppy, chat_fn=chat)
        res = agent.chat("s1", "x")
        assert res.steps[1].result == {"total_count": 5}


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

    def test_null_query_arg_is_rejected(self):
        # {"query": null} must not become the literal string "None".
        sm = FakeSessionManager(("new", build_obj()))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": None}),
            text_reply("What cohort would you like?"),
        )
        agent = CohortAgent(sm, chat_fn=chat)
        res = agent.chat("s1", "?")
        assert res.steps[0].result == {"error": "empty query"}
        assert sm.turn_calls == []                        # build never attempted

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


class FakeExplorer:
    def __init__(self, result):
        self._result = result
        self.calls = []

    def answer(self, query_type, *, field=None, path=None, value=None):
        self.calls.append({"query_type": query_type, "field": field, "path": path, "value": value})
        return self._result


class TestSchemaExplore:
    def test_explore_passes_args_and_returns_text(self):
        explorer = FakeExplorer(SimpleNamespace(kind="fields", text="Table: tumor_assessments", data={}))
        chat = ScriptedChat(
            tool_call("c1", "explore_schema", {"query_type": "list_fields", "path": "tumor_assessments"}),
            text_reply("It has these fields: ..."),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())),
                            schema_explorer=explorer, chat_fn=chat)
        res = agent.chat("s1", "what fields does tumor_assessments have?")

        assert res.steps[0].tool == "explore_schema"
        assert res.steps[0].result == {"kind": "fields", "text": "Table: tumor_assessments"}
        assert explorer.calls[0] == {
            "query_type": "list_fields", "field": None, "path": "tumor_assessments", "value": None,
        }

    def test_null_query_type_rejected(self):
        # {"query_type": null} must not reach the explorer as the string "None".
        explorer = FakeExplorer(SimpleNamespace(kind="fields", text="t", data={}))
        chat = ScriptedChat(
            tool_call("c1", "explore_schema", {"query_type": None}),
            text_reply("What would you like to know about the schema?"),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())),
                            schema_explorer=explorer, chat_fn=chat)
        res = agent.chat("s1", "?")
        assert "query_type" in res.steps[0].result["error"]
        assert explorer.calls == []                       # explorer never reached

    def test_explore_unavailable_without_explorer(self):
        chat = ScriptedChat(
            tool_call("c1", "explore_schema", {"query_type": "list_tables"}),
            text_reply("Schema browsing isn't available right now."),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())), chat_fn=chat)  # no explorer
        res = agent.chat("s1", "list tables")
        assert "not available" in res.steps[0].result["error"]


class TestCohortAnalysisTools:
    def test_summarize_after_build_returns_text_and_warnings(self):
        wire = {"IN": {"sex": ["Male"]}}
        sm = FakeSessionManager(("new", build_obj(wire=wire)))
        analyzer = FakeAnalyzer(summary=summary_obj(total=12, warnings=("dropped unknown field",)))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "male patients"}),
            tool_call("c2", "summarize_cohort", {}),
            text_reply("Here is the summary."),
        )

        agent = CohortAgent(sm, cohort_analyzer=analyzer, chat_fn=chat)
        res = agent.chat("s1", "summarize male patients")

        assert [s.tool for s in res.steps] == ["build_query", "summarize_cohort"]
        assert "Total subjects: 12" in res.steps[1].result["text"]
        assert res.steps[1].result["warnings"] == ["dropped unknown field"]
        assert analyzer.summarize_calls == [{"wire": wire, "label": "current cohort"}]

    def test_summarize_before_build_errors(self):
        chat = ScriptedChat(
            tool_call("c1", "summarize_cohort", {}),
            text_reply("Please build a cohort first."),
        )
        agent = CohortAgent(
            FakeSessionManager(("new", build_obj())),
            cohort_analyzer=FakeAnalyzer(),
            chat_fn=chat,
        )

        res = agent.chat("s1", "summarize")

        assert "no cohort built yet" in res.steps[0].result["error"]

    def test_summarize_unavailable_without_analyzer(self):
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "male patients"}),
            tool_call("c2", "summarize_cohort", {}),
            text_reply("Cohort analysis is unavailable."),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())), chat_fn=chat)

        res = agent.chat("s1", "summarize male patients")

        assert "not available" in res.steps[1].result["error"]

    def test_compare_builds_other_cohort_stateless_and_returns_warnings(self):
        current = {"IN": {"consortium": ["INRG"]}}
        other = {"IN": {"consortium": ["NODAL"]}}
        qb = FakeQueryBuilder(build_obj(wire=other, warnings=("ignored unsupported age range",)))
        sm = FakeSessionManager(("new", build_obj(wire=current)), qb=qb)
        analyzer = FakeAnalyzer(comparison=comparison_obj(warnings=("current: dropped unknown field",)))
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "INRG patients"}),
            tool_call("c2", "compare_cohort", {"comparison_query": "NODAL patients"}),
            text_reply("Here is the comparison."),
        )

        agent = CohortAgent(sm, cohort_analyzer=analyzer, chat_fn=chat)
        res = agent.chat("s1", "compare INRG to NODAL")

        assert [s.tool for s in res.steps] == ["build_query", "compare_cohort"]
        assert "Total" in res.steps[1].result["text"]
        assert qb.calls == [{
            "query": "NODAL patients",
            "current_filter": current,
            "data_type": None,
        }]
        assert analyzer.compare_calls == [{
            "filter_a": current,
            "filter_b": other,
            "label_a": "current",
            "label_b": "NODAL patients",
        }]
        assert res.steps[1].result["warnings"] == [
            "current: dropped unknown field",
            "comparison cohort: ignored unsupported age range",
        ]

    def test_compare_failed_other_build_does_not_call_analyzer_or_clear_current(self):
        current = {"IN": {"consortium": ["INRG"]}}
        qb = FakeQueryBuilder(fail_build())
        sm = FakeSessionManager(("new", build_obj(wire=current)), qb=qb)
        analyzer = FakeAnalyzer()
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "INRG patients"}),
            tool_call("c2", "compare_cohort", {"comparison_query": "invalid cohort"}),
            text_reply("That comparison could not be built."),
        )

        agent = CohortAgent(sm, cohort_analyzer=analyzer, chat_fn=chat)
        res = agent.chat("s1", "compare with invalid cohort")

        assert "invalid_enum_value" in res.steps[1].result["error"]
        assert analyzer.compare_calls == []
        assert agent._last_build["s1"].wire == current

    def test_compare_non_ok_other_build_with_wire_returns_build_error(self):
        current = {"IN": {"consortium": ["INRG"]}}
        other = {"IN": {"consortium": ["NODAL"]}}
        qb = FakeQueryBuilder(build_obj(ok=False, wire=other, errors=("render_error",)))
        sm = FakeSessionManager(("new", build_obj(wire=current)), qb=qb)
        analyzer = FakeAnalyzer()
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "INRG patients"}),
            tool_call("c2", "compare_cohort", {"comparison_query": "NODAL patients"}),
            text_reply("That comparison could not be built."),
        )

        agent = CohortAgent(sm, cohort_analyzer=analyzer, chat_fn=chat)
        res = agent.chat("s1", "compare with NODAL")

        assert "render_error" in res.steps[1].result["error"]
        assert analyzer.compare_calls == []

    def test_compare_null_query_arg_is_rejected(self):
        current = {"IN": {"consortium": ["INRG"]}}
        qb = FakeQueryBuilder()
        sm = FakeSessionManager(("new", build_obj(wire=current)), qb=qb)
        analyzer = FakeAnalyzer()
        chat = ScriptedChat(
            tool_call("c1", "build_query", {"query": "INRG patients"}),
            tool_call("c2", "compare_cohort", {"comparison_query": None}),
            text_reply("What cohort should I compare with?"),
        )

        agent = CohortAgent(sm, cohort_analyzer=analyzer, chat_fn=chat)
        res = agent.chat("s1", "compare with what?")

        assert res.steps[1].result == {"error": "empty comparison query"}
        assert qb.calls == []
        assert analyzer.compare_calls == []

    def test_compare_before_build_errors(self):
        chat = ScriptedChat(
            tool_call("c1", "compare_cohort", {"comparison_query": "NODAL"}),
            text_reply("Please build a cohort first."),
        )
        agent = CohortAgent(
            FakeSessionManager(("new", build_obj())),
            cohort_analyzer=FakeAnalyzer(),
            chat_fn=chat,
        )

        res = agent.chat("s1", "compare")

        assert "no cohort built yet" in res.steps[0].result["error"]
class FakeKnowledge:
    def __init__(self, answer):
        self.answer_obj = answer
        self.calls = []

    def answer(self, question):
        self.calls.append(question)
        return self.answer_obj


class TestAnswerFromDocs:
    def test_question_reaches_the_knowledge_base(self):
        knowledge = FakeKnowledge(SimpleNamespace(
            kind="answer", text="PCDC stands for Pediatric Cancer Data Commons.",
            sources=["PCDC overview > What PCDC is"],
        ))
        chat = ScriptedChat(
            tool_call("c1", "answer_from_docs", {"question": "what is PCDC?"}),
            text_reply("PCDC is the Pediatric Cancer Data Commons."),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())),
                            knowledge_qa=knowledge, chat_fn=chat)
        res = agent.chat("s1", "what is PCDC?")

        assert knowledge.calls == ["what is PCDC?"]
        assert res.steps[0].tool == "answer_from_docs"
        assert res.steps[0].result["kind"] == "answer"
        assert res.steps[0].result["sources"] == ["PCDC overview > What PCDC is"]

    def test_no_match_is_reported_not_hidden(self):
        """A miss has to reach the model so it can say the docs do not cover it."""
        knowledge = FakeKnowledge(SimpleNamespace(
            kind="no_match", text="I do not have enough curated documentation.",
            sources=[],
        ))
        chat = ScriptedChat(
            tool_call("c1", "answer_from_docs", {"question": "who funds PCDC?"}),
            text_reply("The curated documentation does not cover that."),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())),
                            knowledge_qa=knowledge, chat_fn=chat)
        res = agent.chat("s1", "who funds PCDC?")

        assert res.steps[0].result["kind"] == "no_match"

    def test_missing_question_rejected(self):
        knowledge = FakeKnowledge(SimpleNamespace(kind="answer", text="t", sources=[]))
        chat = ScriptedChat(
            tool_call("c1", "answer_from_docs", {"question": None}),
            text_reply("What would you like to know?"),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())),
                            knowledge_qa=knowledge, chat_fn=chat)
        res = agent.chat("s1", "?")

        assert "question" in res.steps[0].result["error"]
        assert knowledge.calls == []

    def test_unavailable_without_knowledge_base(self):
        chat = ScriptedChat(
            tool_call("c1", "answer_from_docs", {"question": "what is PCDC?"}),
            text_reply("I can't reach the documentation right now."),
        )
        agent = CohortAgent(FakeSessionManager(("new", build_obj())), chat_fn=chat)
        res = agent.chat("s1", "what is PCDC?")

        assert "not available" in res.steps[0].result["error"]
