"""HTTP entry point for the rebuilt cohort agent."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter
from pydantic import BaseModel
from starlette.concurrency import run_in_threadpool

from services.guppy_client import DEFAULT_PCDC_GUPPY_ENDPOINT
from services.schema_qa_router import SchemaQARouter

router = APIRouter(prefix="/agent", tags=["agent"])

_DEFAULT_MODEL = "gpt-4o-mini"
_DEFAULT_TIMEOUT = 30.0

_AGENT = None  # process-wide agent
_COUNTER = None
_QA_ROUTER = None
_QA_ROUTER_EXPLORER = None


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _schema_paths() -> tuple[Path, Path]:
    from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA

    schema_dir = _repo_root() / "schema"
    return schema_dir / DEFAULT_PCDC_SCHEMA, schema_dir / DEFAULT_GITOPS


def _guppy_endpoint() -> str:
    return os.environ.get("GUPPY_ENDPOINT", DEFAULT_PCDC_GUPPY_ENDPOINT)


def _guppy_accessibility() -> Optional[str]:
    value = os.environ.get("GUPPY_ACCESSIBILITY")
    if value is None or value.strip().lower() in ("", "none", "null", "omit"):
        return None
    return value.strip()


def _guppy_timeout() -> float:
    value = os.environ.get("GUPPY_TIMEOUT")
    if not value:
        return _DEFAULT_TIMEOUT
    try:
        return float(value)
    except ValueError:
        return _DEFAULT_TIMEOUT


def _use_gen3_auth() -> bool:
    return os.environ.get("GUPPY_USE_GEN3_AUTH", "").strip().lower() in (
        "1",
        "true",
        "yes",
        "on",
    )


def _token_provider():
    if not _use_gen3_auth():
        return None
    try:
        from utils.credential_helper import generate_access_token
    except (ImportError, ModuleNotFoundError):
        return None
    return generate_access_token


def _build_agent():
    from services.agent import CohortAgent

    pcdc_path, gitops_path = _schema_paths()
    knowledge_dir = _repo_root() / "src" / "backend" / "data" / "knowledge"

    return CohortAgent.from_files(
        pcdc_path,
        gitops_path,
        guppy_endpoint=_guppy_endpoint(),
        token_provider=_token_provider(),
        model=os.environ.get("AGENT_CHAT_MODEL", _DEFAULT_MODEL),
        knowledge_dir=knowledge_dir,
        default_accessibility=_guppy_accessibility(),
    )


def get_agent():
    global _AGENT
    if _AGENT is None:
        _AGENT = _build_agent()
    return _AGENT


def _build_counter():
    from services.cohort_counter import CohortCounter
    from services.guppy_client import PCDCGuppyClient
    from services.schema_loader import SchemaIndex

    pcdc_path, gitops_path = _schema_paths()
    schema = SchemaIndex.from_files(pcdc_path, gitops_path)
    guppy = PCDCGuppyClient(
        _guppy_endpoint(),
        token_provider=_token_provider(),
        timeout=_guppy_timeout(),
    )
    return CohortCounter(schema, guppy, accessibility=_guppy_accessibility())


def get_counter():
    global _COUNTER
    if _COUNTER is None:
        _COUNTER = _build_counter()
    return _COUNTER


def set_agent(agent) -> None:
    """Swap the singleton agent used by the API."""
    global _AGENT, _QA_ROUTER, _QA_ROUTER_EXPLORER
    _AGENT = agent
    _QA_ROUTER = None
    _QA_ROUTER_EXPLORER = None


def set_counter(counter) -> None:
    global _COUNTER
    _COUNTER = counter


class ChatRequest(BaseModel):
    session_id: str
    message: str


class CountRequest(BaseModel):
    filter: Dict[str, Any]


class ResetRequest(BaseModel):
    session_id: str


def _extract_filter(steps) -> Optional[dict]:
    wire = None
    for step in steps:
        res = step.result if isinstance(step.result, dict) else {}
        if step.tool == "build_query" and res.get("ok"):
            wire = res.get("filter")
    return wire


def _extract_count(steps) -> Optional[int]:
    count = None
    for step in steps:
        res = step.result if isinstance(step.result, dict) else {}
        if step.tool == "count_cohort" and "total_count" in res:
            count = res.get("total_count")
    return count


def _step_trace(steps) -> List[Dict[str, Any]]:
    out = []
    for step in steps:
        res = step.result if isinstance(step.result, dict) else {}
        ok = ("error" not in res) and (res.get("ok", True) is not False)
        out.append({"tool": step.tool, "ok": bool(ok)})
    return out


# Keep provider errors useful without leaking keys.
_SECRET_RE = re.compile(r"(sk-[A-Za-z0-9_\-]{6,}|Bearer\s+\S+|eyJ[A-Za-z0-9_\-.]{8,})")


def _safe_error(exc: Exception) -> str:
    msg = f"{type(exc).__name__}: {exc}"
    return _SECRET_RE.sub("[REDACTED]", msg)[:300]


def _unavailable(session_id: str, exc: Exception) -> Dict[str, Any]:
    """Return a stable chat response when the agent cannot run."""
    return {
        "session_id": session_id,
        "reply": (
            "The assistant is temporarily unavailable. Please try again in a "
            "moment; if this keeps happening, the server may be missing its API "
            "key or configuration."
        ),
        "filter": None,
        "count": None,
        "steps": [],
        "stopped": False,
        "error": _safe_error(exc),
    }


def _get_qa_router(explorer) -> SchemaQARouter:
    """Cache the schema router for the current explorer."""
    global _QA_ROUTER, _QA_ROUTER_EXPLORER
    if _QA_ROUTER is None or _QA_ROUTER_EXPLORER is not explorer:
        _QA_ROUTER = SchemaQARouter(explorer)
        _QA_ROUTER_EXPLORER = explorer
    return _QA_ROUTER


def _try_schema_qa(agent, session_id: str, message: str) -> Optional[Dict[str, Any]]:
    explorer = getattr(agent, "schema_explorer", None)
    if explorer is None:
        return None
    answer = _get_qa_router(explorer).answer(message)
    if answer is None:
        return None
    return {
        "session_id": session_id,
        "reply": answer.text,
        "filter": None,
        "count": None,
        "steps": [{"tool": "explore_schema", "ok": answer.kind != "error"}],
        "stopped": False,
    }


def _try_knowledge_qa(agent, session_id: str, message: str) -> Optional[Dict[str, Any]]:
    qa = getattr(agent, "knowledge_qa", None)
    # Be picky here; borderline cases can still go through normal tool routing.
    if qa is None or not qa.looks_like_knowledge_question(message):
        return None
    answer = qa.answer(message)
    if not answer.ok:
        return None
    return {
        "session_id": session_id,
        "reply": answer.text,
        "filter": None,
        "count": None,
        "steps": [{"tool": "knowledge_qa", "ok": True}],
        "stopped": False,
    }


@router.post("/chat")
async def chat(req: ChatRequest) -> Dict[str, Any]:
    try:
        agent = get_agent()
        # Exact schema questions should not be answered from the prose docs.
        schema_answer = _try_schema_qa(agent, req.session_id, req.message)
        if schema_answer is not None:
            return schema_answer
        knowledge_answer = _try_knowledge_qa(agent, req.session_id, req.message)
        if knowledge_answer is not None:
            return knowledge_answer
        result = await run_in_threadpool(agent.chat, req.session_id, req.message)
    except Exception as e:  # noqa: BLE001
        return _unavailable(req.session_id, e)

    return {
        "session_id": result.session_id,
        "reply": result.reply,
        "filter": _extract_filter(result.steps),
        "count": _extract_count(result.steps),
        "steps": _step_trace(result.steps),
        "stopped": result.stopped,
    }


@router.post("/count")
async def count_filter(req: CountRequest) -> Dict[str, Any]:
    try:
        result = await get_counter().acount(req.filter)
    except Exception as e:  # noqa: BLE001
        return {
            "ok": False,
            "count": None,
            "total_masked": False,
            "errors": [_safe_error(e)],
        }
    return result.as_dict()


@router.post("/reset")
async def reset(req: ResetRequest) -> Dict[str, Any]:
    agent = get_agent()
    await run_in_threadpool(agent.reset, req.session_id)
    return {"session_id": req.session_id, "reset": True}


@router.get("/health")
async def health() -> Dict[str, Any]:
    """Lightweight configuration check."""
    return {
        "openai_key_configured": bool(os.environ.get("OPENAI_API_KEY")),
        "guppy_endpoint": _guppy_endpoint(),
        "guppy_accessibility": _guppy_accessibility(),
        "guppy_auth_enabled": _use_gen3_auth(),
        "model": os.environ.get("AGENT_CHAT_MODEL", _DEFAULT_MODEL),
        "agent_loaded": _AGENT is not None,
    }
