import sys
from pathlib import Path

import pytest

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

pytest.importorskip("fastapi")
from fastapi.testclient import TestClient


def test_app_imports_and_mounts_agent_without_legacy_deps():
    import app
    paths = {getattr(r, "path", None) for r in app.app.routes}
    assert "/agent/chat" in paths
    assert "/agent/count" in paths
    assert "/agent/health" in paths
    assert "/agent/reset" in paths


def test_agent_health_works_on_full_app():
    import app
    client = TestClient(app.app)
    r = client.get("/agent/health")
    assert r.status_code == 200
    assert "openai_key_configured" in r.json()


def test_legacy_endpoint_returns_503_when_legacy_unavailable():
    import app
    if app.LEGACY_AVAILABLE:
        pytest.skip("legacy deps are installed; the 503 guard is not exercised here")
    client = TestClient(app.app)
    r = client.post("/nested_graphql", json={"text": "x"})
    assert r.status_code == 503
    assert "agent" in r.json().get("detail", "").lower()
