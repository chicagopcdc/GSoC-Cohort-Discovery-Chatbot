import chainlit as cl
from typing import Optional
import os
from datetime import datetime
from dotenv import load_dotenv
import json
import httpx

load_dotenv()

# Backend API URL. The agent pipeline is served at POST /v2/chat.
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")

# The agent may run several tool calls plus an LLM round trip per turn, so this
# is well above the old single-shot timeout.
def _request_timeout() -> float:
    try:
        timeout = float(os.getenv("BACKEND_TIMEOUT", "120"))
    except (TypeError, ValueError):
        return 120.0
    return timeout if timeout > 0 else 120.0


REQUEST_TIMEOUT = _request_timeout()

# Authentication using Chainlit's built-in password auth
@cl.password_auth_callback
def auth_callback(username: str, password: str) -> Optional[cl.User]:
    """Simple password authentication"""
    # In production, check against a database
    valid_users = {
        "test": "test",
        "admin": "admin",
        "user": "user"
    }
    
    if username in valid_users and valid_users[username] == password:
        return cl.User(
            identifier=username,
            metadata={
                "role": "admin" if username == "admin" else "user",
                "provider": "credentials"
            }
        )
    return None

@cl.on_chat_start
async def start():
    """Initialize a new chat session"""
    # Get current user
    user = cl.user_session.get("user")
    if not user:
        await cl.Message(
            content="Authentication required. Please log in first."
        ).send()
        return

    # Create session
    session_id = cl.context.session.thread_id
    cl.user_session.set("session_id", session_id)
    cl.user_session.set("message_count", 0)

    # Welcome message
    welcome_msg = f"""**Welcome to the PCDC Cohort Assistant**

Signed in as **{user.identifier}**. Past conversations are in the left sidebar.

Describe the cohort you want in plain language. Follow-ups edit the cohort you
already have, so you can refine it turn by turn.

**Build and count a cohort:**
- How many subjects are in the INRG consortium?
- INRG subjects with metastatic tumors in the skin where the tumor is absent
- ...then: change the consortium to INSTRuCT

**Ask about the schema:**
- What values does tumor_state allow?
- Which fields can I filter on under tumor_assessments?

**Ask about PCDC itself:**
- Why does my cohort count come back as -1?
- Do I need an account to get line-level patient data?

**Analyze a cohort** (after building one):
- Describe the make-up of this cohort
- How does that compare with NODAL?"""
    
    await cl.Message(content=welcome_msg, author="System").send()

@cl.on_message
async def main(message: cl.Message):
    """Process user messages"""
    # Get user session
    user = cl.user_session.get("user")
    if not user:
        await cl.Message(content="Please log in first.").send()
        return

    # Update message count
    count = cl.user_session.get("message_count", 0) + 1
    cl.user_session.set("message_count", count)

    # Send thinking message
    msg = cl.Message(content="Working on it...")
    await msg.send()
    
    try:
        session_id = cl.user_session.get("session_id")

        # One call per turn: the agent picks its own tools and keeps the cohort
        # across turns, keyed by session_id.
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BACKEND_URL}/v2/chat",
                json={"message": message.content, "session_id": session_id},
                headers={"Content-Type": "application/json"},
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()
            result = response.json()

        # The backend mints a session id when it gets none; keep whatever it
        # returns so later turns land in the same cohort.
        returned_session = result.get("session_id")
        if returned_session:
            cl.user_session.set("session_id", returned_session)

        response_content = _format_reply(result)

    except httpx.TimeoutException:
        response_content = f"""**Request timed out**

The assistant took longer than {REQUEST_TIMEOUT:.0f}s. Complex cohorts need
several tool calls; try again, or narrow the request.

**Input**: {message.content}"""

    except httpx.HTTPStatusError as e:
        detail = ""
        try:
            detail = e.response.json().get("detail", "")
        except Exception:
            detail = getattr(e.response, "text", "")
        hint = ""
        if e.response.status_code == 503:
            hint = ("\n\n**Hint**: the backend could not build the agent. Check the "
                    "uvicorn log — usually a missing `OPENAI_API_KEY` or an unreadable schema.")
        response_content = f"""**Request failed** (status {e.response.status_code})

**Input**: {message.content}
**Error**: {detail or 'Unknown error'}{hint}"""

    except httpx.ConnectError:
        response_content = f"""**Cannot reach the backend** at {BACKEND_URL}

Start it first, then resend:
`cd src/backend && python -m uvicorn app:app --reload --port 8000`

**Input**: {message.content}"""

    except Exception as e:
        response_content = f"""**Something went wrong**

**Input**: {message.content}
**Error**: {type(e).__name__}: {e}"""

    # Update the message with the result
    msg.content = response_content
    await msg.update()


def _format_reply(result: dict) -> str:
    """Render a /v2/chat response: the answer first, then the evidence behind it."""
    parts = [result.get("reply") or "_(the assistant returned an empty reply)_"]

    # The filter is what the wording was actually interpreted as, and it is the
    # thing to compare against the portal when a count looks surprising.
    filter_obj = result.get("filter")
    if filter_obj:
        parts.append("**Generated filter**:\n```json\n"
                     + json.dumps(filter_obj, indent=2, ensure_ascii=False)
                     + "\n```")

    total = result.get("count")
    if total is not None:
        if total == -1:
            parts.append("**Matching subjects**: `-1` — fewer than 5 subjects, "
                         "withheld by the privacy rule (not zero, not an error).")
        else:
            parts.append(f"**Matching subjects**: {total:,}")

    warnings = result.get("warnings") or []
    if warnings:
        parts.append("**Note**:\n" + "\n".join(f"- {w}" for w in warnings))

    if result.get("stopped"):
        parts.append("**Note**: the assistant hit its step limit before finishing this turn.")

    # Which tools ran, the message counter and the session id are diagnostics,
    # not something a researcher reading an answer needs. They stay in the
    # /v2/chat response (`trace`, `session_id`) for anyone debugging the pipeline.
    return "\n\n".join(parts)

@cl.on_chat_resume
async def on_chat_resume(thread):
    """Resume a previous conversation"""
    user = cl.user_session.get("user")
    if not user:
        return
    
    # Count previous messages
    message_count = 0
    if thread and "steps" in thread:
        message_count = len([s for s in thread["steps"] if s.get("type") == "user_message"])
    
    cl.user_session.set("message_count", message_count)

    session_id = thread["id"]
    cl.user_session.set("session_id", session_id)

    await cl.Message(
        content=f"**Welcome back, {user.identifier}.** Picking up where you left off "
                f"({message_count} earlier messages).",
        author="System"
    ).send()

@cl.author_rename
def rename(orig_author: str):
    """Rename authors for display"""
    rename_dict = {
        "System": "Assistant",
        "User": "You"
    }
    return rename_dict.get(orig_author, orig_author)

if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)
