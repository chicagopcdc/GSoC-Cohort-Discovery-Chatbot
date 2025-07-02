import chainlit as cl
from typing import Optional
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv

load_dotenv()

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
            content="❌ Authentication required. Please login first."
        ).send()
        return
    
    # Create session
    session_id = str(uuid.uuid4())[:8]
    cl.user_session.set("session_id", session_id)
    cl.user_session.set("message_count", 0)
    
    # Welcome message
    welcome_msg = f"""👋 **Welcome to PCDC GraphQL Query Generator!**

✅ **Logged in as**: {user.identifier}
🔗 **Session ID**: {session_id}
📋 **Chat History**: Enabled (check left sidebar)

Enter your natural language query to generate GraphQL queries.

**Example queries:**
- Get all users older than 18
- Find patients with diabetes
- Show available fields"""
    
    await cl.Message(content=welcome_msg, author="System").send()

@cl.on_message
async def main(message: cl.Message):
    """Process user messages"""
    # Get user session
    user = cl.user_session.get("user")
    if not user:
        await cl.Message(content="❌ Please login first.").send()
        return
    
    # Update message count
    count = cl.user_session.get("message_count", 0) + 1
    cl.user_session.set("message_count", count)
    
    # Send thinking message
    msg = cl.Message(content="🤔 Processing your query...")
    await msg.send()
    
    # Simulate processing
    import asyncio
    await asyncio.sleep(1)
    
    # Generate mock response
    response = f"""✅ **Query Processed**

**Input**: {message.content}

**Generated GraphQL**:
```graphql
query GetData {{
  users(filter: {{ /* your conditions */ }}) {{
    id
    name
    age
  }}
}}
```

**Session Info**: Message #{count} from {user.identifier}"""
    
    # Update the message
    msg.content = response
    await msg.update()

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
    
    await cl.Message(
        content=f"📂 **Conversation Resumed**\n\nWelcome back, {user.identifier}! You have {message_count} previous messages.",
        author="System"
    ).send()

@cl.author_rename
def rename(orig_author: str):
    """Rename authors for display"""
    rename_dict = {
        "System": "🤖 Assistant",
        "User": "👤 You"
    }
    return rename_dict.get(orig_author, orig_author)

if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)