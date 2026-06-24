"""
HTTP entry point for the rebuilt pipeline.
"""

from __future__ import annotations

import logging
import os
import re
import threading
import uuid
from pathlib import Path
from typing import List, Optional, Tuple

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from services.agent import CohortAgent

logger = logging.getLogger(__name__)
router = APIRouter()

# Built once on first request (embedding every schema field is expensive), then
# reused. Locked so concurrent first requests build it only once.
_agent: Optional[CohortAgent] = None
_agent_lock = threading.Lock()


def _schema_date(p: Path) -> str:
    m = re.search(r"(\d{8})", p.name)
    return m.group(1) if m else ""


def _find_schema() -> Tuple[str, str]:
    schema_dir = Path(os.getenv("SCHEMA_DIR", str(Path(__file__).resolve().parents[2] / "schema")))
    pcdc = os.getenv("PCDC_SCHEMA_PATH")
    if not pcdc:
        found = list(schema_dir.glob("pcdc-schema-prod-*.json"))
        if not found:
            raise RuntimeError(
                f"no pcdc-schema-prod-*.json under {schema_dir}; set PCDC_SCHEMA_PATH"
            )
        pcdc = str(max(found, key=_schema_date))   # newest by YYYYMMDD in the name
    gitops = os.getenv("GITOPS_PATH", str(schema_dir / "gitops.json"))
    return pcdc, gitops


def _build_agent() -> CohortAgent:
    pcdc, gitops = _find_schema()
    guppy_endpoint = os.getenv("GUPPY_ENDPOINT") or None

    token_provider = None
    if guppy_endpoint:
        # Only wire credentials when there is actually an endpoint to call.
        from utils.credential_helper import generate_access_token
        token_provider = generate_access_token

    return CohortAgent.from_files(
        pcdc,
        gitops,
        guppy_endpoint=guppy_endpoint,
        token_provider=token_provider,
        model=os.getenv("AGENT_CHAT_MODEL", "gpt-4o-mini"),   # agent loop only
        cache_dir=os.getenv("AGENT_CACHE_DIR") or None,
    )


def _get_agent() -> CohortAgent:
    global _agent
    if _agent is None:
        with _agent_lock:
            if _agent is None:
                _agent = _build_agent()
    return _agent


class ChatRequest(BaseModel):
    message: str
    session_id: Optional[str] = None


class TraceStep(BaseModel):
    tool: str
    ok: bool
    detail: Optional[str] = None


class ChatResponse(BaseModel):
    session_id: str
    reply: str
    filter: Optional[dict] = None
    count: Optional[int] = None
    histograms: Optional[dict] = None        # currently always empty (no histogram fields requested)
    warnings: List[str] = Field(default_factory=list)
    stopped: bool = False                    # True if the agent hit its step cap
    trace: List[TraceStep] = Field(default_factory=list)


def _latest_filter(steps) -> Optional[dict]:
    for step in reversed(steps):
        if step.tool == "build_query" and step.result.get("ok"):
            return step.result.get("filter")
    return None


def _latest_warnings(steps) -> List[str]:
    for step in reversed(steps):
        if step.tool == "build_query" and step.result.get("ok"):
            return list(step.result.get("warnings") or [])
    return []


def _latest_count(steps) -> Tuple[Optional[int], Optional[dict]]:
    for step in reversed(steps):
        if step.tool == "count_cohort" and "total_count" in step.result:
            return step.result["total_count"], step.result.get("histograms")
    return None, None


def _trace(steps) -> List[TraceStep]:
    out: List[TraceStep] = []
    for s in steps:
        r = s.result
        if "error" in r:
            out.append(TraceStep(tool=s.tool, ok=False, detail=r["error"]))
        elif s.tool == "build_query":
            ok = bool(r.get("ok"))
            detail = None if ok else ("; ".join(r.get("errors") or []) or None)
            out.append(TraceStep(tool=s.tool, ok=ok, detail=detail))
        elif s.tool == "count_cohort":
            out.append(TraceStep(tool=s.tool, ok=True, detail=f"count={r.get('total_count')}"))
        else:
            out.append(TraceStep(tool=s.tool, ok=True))
    return out


@router.post("/v2/chat", response_model=ChatResponse)
def chat(req: ChatRequest) -> ChatResponse:
    message = (req.message or "").strip()
    if not message:
        raise HTTPException(status_code=400, detail="message must not be empty")

    session_id = (req.session_id or "").strip() or uuid.uuid4().hex

    try:
        result = _get_agent().chat(session_id, message)
    except Exception:
        # Agent build failure (e.g. missing schema) or a model/transport error
        # inside the loop. Log the traceback, return a sanitized 503.
        logger.exception("/v2/chat failed")
        raise HTTPException(status_code=503, detail="the assistant is temporarily unavailable")

    count, histograms = _latest_count(result.steps)
    return ChatResponse(
        session_id=session_id,
        reply=result.reply,
        filter=_latest_filter(result.steps),
        count=count,
        histograms=histograms,
        warnings=_latest_warnings(result.steps),
        stopped=result.stopped,
        trace=_trace(result.steps),
    )