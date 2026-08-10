import sys
from pathlib import Path

import pytest

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

pytest.importorskip("fastapi")
from fastapi import FastAPI
from fastapi.testclient import TestClient

import api_agent
from api_agent import router, set_agent, set_counter
from services.agent import AgentResult, AgentStep
from services.knowledge_qa import KnowledgeQA


def _build_step(ok=True):
    return AgentStep(
        tool="build_query",
        arguments={"query": "INRG males"},
        result={"mode": "new", "ok": ok,
                "filter": {"IN": {"sex": ["Male"]}} if ok else None,
                "errors": [], "warnings": []},
    )


def _count_step(total=123):
    return AgentStep(tool="count_cohort", arguments={},
                     result={"total_count": total, "histograms": {}})


class FakeAgent:
    def __init__(self, result=None):
        self.chat_calls = []
        self.reset_calls = []
        self._result = result

    def chat(self, session_id, message):
        self.chat_calls.append((session_id, message))
        if self._result is not None:
            return self._result
        return AgentResult(
            reply="There are 123 matching subjects.",
            session_id=session_id,
            steps=[_build_step(ok=True), _count_step(123)],
            llm_calls=3, stopped=False,
        )

    def reset(self, session_id):
        self.reset_calls.append(session_id)


class KnowledgeOnlyAgent:
    def __init__(self):
        self.knowledge_qa = KnowledgeQA.from_dir(_BACKEND / "data" / "knowledge")
        self.chat_calls = []

    def chat(self, session_id, message):
        self.chat_calls.append((session_id, message))
        return AgentResult(reply="fell through", session_id=session_id, steps=[])

    def reset(self, session_id):
        pass


@pytest.fixture
def app_client():
    app = FastAPI()
    app.include_router(router)
    yield TestClient(app)
    set_agent(None)
    set_counter(None)


class TestChat:
    def test_returns_reply_filter_count(self, app_client):
        set_agent(FakeAgent())
        r = app_client.post("/agent/chat", json={"session_id": "s1", "message": "how many INRG males?"})
        assert r.status_code == 200
        body = r.json()
        assert body["reply"] == "There are 123 matching subjects."
        assert body["filter"] == {"IN": {"sex": ["Male"]}}
        assert body["count"] == 123
        assert body["steps"] == [{"tool": "build_query", "ok": True},
                                 {"tool": "count_cohort", "ok": True}]
        assert body["stopped"] is False

    def test_passes_session_and_message(self, app_client):
        fake = FakeAgent()
        set_agent(fake)
        app_client.post("/agent/chat", json={"session_id": "s7", "message": "hello"})
        assert fake.chat_calls == [("s7", "hello")]

    def test_filter_none_when_build_failed(self, app_client):
        result = AgentResult(reply="Could not build that.", session_id="s1",
                             steps=[_build_step(ok=False)], llm_calls=1, stopped=False)
        set_agent(FakeAgent(result))
        body = app_client.post("/agent/chat", json={"session_id": "s1", "message": "x"}).json()
        assert body["filter"] is None
        assert body["count"] is None
        assert body["steps"] == [{"tool": "build_query", "ok": False}]

    def test_unavailable_when_agent_cannot_build(self, app_client, monkeypatch):
        set_agent(None)
        def boom():
            raise RuntimeError("no OPENAI_API_KEY")
        monkeypatch.setattr(api_agent, "_build_agent", boom)
        body = app_client.post("/agent/chat", json={"session_id": "s1", "message": "x"}).json()
        assert "unavailable" in body["reply"].lower()
        assert "no OPENAI_API_KEY" in body["error"]
        assert body["filter"] is None


class TestNoOpenAISchemaFallback:
    def test_knowledge_question_answer_without_openai_key(self, app_client, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
        fake = KnowledgeOnlyAgent()
        set_agent(fake)

        body = app_client.post(
            "/agent/chat",
            json={"session_id": "s-knowledge", "message": "What is PCDC?"},
        ).json()

        assert "Pediatric Cancer Data Commons" in body["reply"]
        assert body["steps"] == [{"tool": "knowledge_qa", "ok": True}]
        assert body["filter"] is None
        assert body["count"] is None
        assert fake.chat_calls == []

    def test_knowledge_fallback_does_not_steal_cohort_request(self, app_client, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
        fake = KnowledgeOnlyAgent()
        set_agent(fake)

        body = app_client.post(
            "/agent/chat",
            json={"session_id": "s1", "message": "Find male patients from the INRG consortium"},
        ).json()

        assert body["reply"] == "fell through"
        assert body["steps"] == []
        assert fake.chat_calls == [("s1", "Find male patients from the INRG consortium")]

    def test_schema_field_question_not_stolen_by_knowledge(self, app_client, monkeypatch):
        # Schema questions should beat the looser knowledge fallback.
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
        set_agent(None)
        body = app_client.post(
            "/agent/chat",
            json={"session_id": "s-schema", "message": "What values does consortium have?"},
        ).json()
        assert body["steps"] == [{"tool": "explore_schema", "ok": True}]
        assert body["reply"].startswith("consortium (")
        assert "INRG" in body["reply"]

    def test_schema_values_answer_without_openai_key(self, app_client, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
        set_agent(None)
        body = app_client.post(
            "/agent/chat",
            json={"session_id": "s-schema", "message": "What values does sex allow?"},
        ).json()
        assert body["reply"].startswith("sex (")
        assert "Male" in body["reply"]
        assert body["steps"] == [{"tool": "explore_schema", "ok": True}]
        assert body["filter"] is None
        assert body["count"] is None

    def test_schema_fields_answer_without_openai_key(self, app_client, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
        set_agent(None)
        body = app_client.post(
            "/agent/chat",
            json={
                "session_id": "s-schema",
                "message": "Which fields are available under tumor_assessments?",
            },
        ).json()
        assert "Table: tumor_assessments" in body["reply"]
        assert "tumor_classification" in body["reply"]
        assert body["steps"] == [{"tool": "explore_schema", "ok": True}]

    def test_schema_value_owner_answer_without_openai_key(self, app_client, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        monkeypatch.delenv("OPENAI_ADMIN_KEY", raising=False)
        set_agent(None)
        body = app_client.post(
            "/agent/chat",
            json={
                "session_id": "s-schema",
                "message": "Which field contains the value Metastatic?",
            },
        ).json()
        assert "'Metastatic' is a value of:" in body["reply"]
        assert "tumor_classification" in body["reply"]
        assert body["steps"] == [{"tool": "explore_schema", "ok": True}]

    def test_regular_cohort_request_does_not_use_schema_fallback(self, app_client):
        class BoomAgent:
            def chat(self, session_id, message):
                raise RuntimeError("no OpenAI key")

            def reset(self, session_id):
                pass

        set_agent(BoomAgent())
        body = app_client.post(
            "/agent/chat",
            json={"session_id": "s1", "message": "Find male patients from the INRG consortium"},
        ).json()
        assert "unavailable" in body["reply"].lower()
        assert "no OpenAI key" in body["error"]
        assert body["steps"] == []


class TestReset:
    def test_reset_calls_agent(self, app_client):
        fake = FakeAgent()
        set_agent(fake)
        r = app_client.post("/agent/reset", json={"session_id": "s1"})
        assert r.status_code == 200 and r.json()["reset"] is True
        assert fake.reset_calls == ["s1"]


class TestHealth:
    def test_health_shape(self, app_client, monkeypatch):
        monkeypatch.delenv("GUPPY_ENDPOINT", raising=False)
        monkeypatch.delenv("GUPPY_ACCESSIBILITY", raising=False)
        monkeypatch.delenv("GUPPY_USE_GEN3_AUTH", raising=False)
        set_agent(FakeAgent())
        body = app_client.get("/agent/health").json()
        assert isinstance(body["openai_key_configured"], bool)
        assert "guppy_endpoint" in body and "model" in body
        assert body["guppy_endpoint"] == "https://portal.pedscommons.org/guppy/graphql/"
        assert body["guppy_accessibility"] is None
        assert body["guppy_auth_enabled"] is False
        assert body["agent_loaded"] is True


class TestCountEndpoint:
    def test_count_filter_returns_counter_result(self, app_client):
        class Counter:
            def __init__(self):
                self.calls = []

            async def acount(self, filter_obj):
                self.calls.append(filter_obj)

                class Result:
                    def as_dict(self):
                        return {
                            "ok": True,
                            "count": 123,
                            "total_masked": False,
                            "errors": [],
                        }

                return Result()

        counter = Counter()
        set_counter(counter)

        body = app_client.post(
            "/agent/count",
            json={"filter": {"AND": [{"IN": {"consortium": ["INRG"]}}]}},
        ).json()

        assert body == {"ok": True, "count": 123, "total_masked": False, "errors": []}
        assert counter.calls == [{"AND": [{"IN": {"consortium": ["INRG"]}}]}]

    def test_count_filter_errors_are_friendly(self, app_client):
        class Counter:
            async def acount(self, filter_obj):
                raise RuntimeError("Incorrect API key sk-proj-ABCDEFGHIJ012345")

        set_counter(Counter())

        body = app_client.post(
            "/agent/count",
            json={"filter": {"AND": [{"IN": {"consortium": ["INRG"]}}]}},
        ).json()

        assert body["ok"] is False
        assert body["count"] is None
        assert "sk-proj-ABCDEFGHIJ012345" not in body["errors"][0]
        assert "[REDACTED]" in body["errors"][0]


class TestChatErrorHandling:
    """API errors stay friendly and redact secrets."""

    def test_agent_chat_exception_is_graceful_not_500(self, app_client):
        class BoomAgent:
            def chat(self, session_id, message):
                raise RuntimeError("kaboom")

            def reset(self, session_id):
                pass

        set_agent(BoomAgent())
        r = app_client.post("/agent/chat", json={"session_id": "s1", "message": "hi"})
        assert r.status_code == 200
        body = r.json()
        assert body["filter"] is None and body["count"] is None
        assert body["steps"] == [] and body["stopped"] is False
        assert "unavailable" in body["reply"].lower()
        assert "kaboom" in body["error"]

    def test_error_message_redacts_secrets(self, app_client):
        class LeakyAgent:
            def chat(self, session_id, message):
                raise RuntimeError(
                    "Incorrect API key provided: sk-proj-ABCDEFGHIJ012345. "
                    "Bearer eyJhbGciOiJIUzI1NiJ9.payload.signature"
                )

            def reset(self, session_id):
                pass

        set_agent(LeakyAgent())
        body = app_client.post("/agent/chat", json={"session_id": "s1", "message": "hi"}).json()
        assert body["error"]
        assert "sk-proj-ABCDEFGHIJ012345" not in body["error"]
        assert "eyJhbGciOiJIUzI1NiJ9" not in body["error"]
        assert "[REDACTED]" in body["error"]


class TestSchemaQARouterCaching:
    """The schema QA router is cached per explorer."""

    def test_router_built_once_across_calls(self, app_client, monkeypatch):
        from services.schema_explorer import SchemaExplorer
        from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA, SchemaIndex

        schema_dir = _BACKEND.parents[1] / "schema"
        explorer = SchemaExplorer(
            SchemaIndex.from_files(schema_dir / DEFAULT_PCDC_SCHEMA, schema_dir / DEFAULT_GITOPS)
        )

        class ExplorerAgent:
            schema_explorer = explorer

            def chat(self, session_id, message):
                raise AssertionError("a schema question must not reach the LLM agent")

            def reset(self, session_id):
                pass

        builds = {"n": 0}
        real_router = api_agent.SchemaQARouter

        def counting(exp):
            builds["n"] += 1
            return real_router(exp)

        monkeypatch.setattr(api_agent, "SchemaQARouter", counting)

        set_agent(ExplorerAgent())
        q = {"session_id": "s", "message": "What values does sex allow?"}
        r1 = app_client.post("/agent/chat", json=q).json()
        r2 = app_client.post("/agent/chat", json=q).json()

        assert r1["steps"] == [{"tool": "explore_schema", "ok": True}]
        assert r2["steps"] == [{"tool": "explore_schema", "ok": True}]
        assert builds["n"] == 1
