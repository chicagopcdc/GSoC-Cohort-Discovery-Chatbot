import chainlit as cl
from typing import Optional
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv
import json
import httpx

load_dotenv()

BACKEND_URL = os.environ.get("BACKEND_URL", "http://localhost:8000")

@cl.password_auth_callback
def auth_callback(username: str, password: str) -> Optional[cl.User]:
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
    user = cl.user_session.get("user")
    if not user:
        await cl.Message(content="Authentication required. Please login first.").send()
        return

    session_id = str(uuid.uuid4())[:8]
    cl.user_session.set("session_id", session_id)
    cl.user_session.set("message_count", 0)

    llm_note = ""
    try:
        async with httpx.AsyncClient() as client:
            h = await client.get(f"{BACKEND_URL}/agent/health", timeout=10.0)
            if h.status_code == 200 and not h.json().get("openai_key_configured", False):
                llm_note = (
                    "\n\n⚠️ The assistant's language model isn't configured on the "
                    "server yet (no API key), so building queries may be unavailable."
                )
    except Exception:
        pass

    welcome_msg = f"""**Welcome to the PCDC Cohort Discovery Assistant!**

**Logged in as**: {user.identifier}
**Session ID**: {session_id}

Ask in plain language. I can build and refine cohort filters, count matching
subjects, look up schema fields and values, and summarize or compare cohorts.

**Try:**
- What is PCDC?
- Find male patients from the INRG consortium
- Also add patients with metastatic tumors in the skin
- How many subjects match?
- What values does tumor_classification allow?
- Summarize this cohort
- Compare with NODAL{llm_note}"""

    await cl.Message(content=welcome_msg, author="System").send()


@cl.on_message
async def main(message: cl.Message):
    user = cl.user_session.get("user")
    if not user:
        await cl.Message(content="Please login first.").send()
        return

    count = cl.user_session.get("message_count", 0) + 1
    cl.user_session.set("message_count", count)
    session_id = cl.user_session.get("session_id")

    thinking = cl.Message(content="Thinking...")
    await thinking.send()

    response_content = ""
    response_elements = []

    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post(
                f"{BACKEND_URL}/agent/chat",
                json={"session_id": session_id, "message": message.content},
                headers={"Content-Type": "application/json"},
                timeout=60.0,
            )
            resp.raise_for_status()
            data = resp.json()

        lines = [data.get("reply") or "(no reply)"]

        cohort_count = data.get("count")
        if cohort_count is not None:
            lines.append(f"\n**Matching subjects: {cohort_count:,}**")

        if data.get("stopped"):
            lines.append("\n_(stopped early — the request took too many steps.)_")
        if data.get("error"):
            lines.append(f"\n_(server note: {data['error']})_")

        wire = data.get("filter")
        if wire:
            lines.append("\n_Open **Generated filter** (right) to see or reuse the technical query._")
            response_elements = [cl.Text(
                name="Generated filter",
                content=json.dumps(wire, indent=2, ensure_ascii=False),
                display="side",
                language="json",
            )]

        response_content = "\n".join(lines)

    except httpx.TimeoutException:
        response_content = "The request timed out. Please try again with a simpler query."
    except httpx.HTTPStatusError as e:
        body = getattr(e.response, "text", "")
        response_content = f"Backend error (status {e.response.status_code}). {body[:200]}"
    except Exception as e:
        response_content = f"Something went wrong: {e}"

    thinking.content = response_content
    thinking.elements = response_elements
    await thinking.update()


@cl.on_chat_resume
async def on_chat_resume(thread):
    user = cl.user_session.get("user")
    if not user:
        return

    tid = (thread or {}).get("id")
    cl.user_session.set("session_id", str(tid)[:8] if tid else str(uuid.uuid4())[:8])

    message_count = 0
    if thread and "steps" in thread:
        message_count = len([s for s in thread["steps"] if s.get("type") == "user_message"])

    cl.user_session.set("message_count", message_count)

    await cl.Message(
        content=f"**Conversation Resumed**\n\nWelcome back, {user.identifier}! You have {message_count} previous messages.",
        author="System"
    ).send()


@cl.author_rename
def rename(orig_author: str):
    rename_dict = {
        "System": "Assistant",
        "User": "You"
    }
    return rename_dict.get(orig_author, orig_author)


if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)
