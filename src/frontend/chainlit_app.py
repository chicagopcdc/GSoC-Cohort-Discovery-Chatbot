import chainlit as cl
from typing import Optional
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv
import json
import httpx

load_dotenv()

# Backend API URL
BACKEND_URL = "http://localhost:8000"

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
    
    try:
        # Get session ID
        session_id = cl.user_session.get("session_id")
        
        # Call backend API using HTTP
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BACKEND_URL}/convert",
                json={
                    "text": message.content,
                    "session_id": session_id
                },
                headers={"Content-Type": "application/json"},
                timeout=30.0
            )
            response.raise_for_status()
            result = response.json()
        
        # Format the response
        query_str = result.get("query", "")
        variables_str = result.get("variables", "{}")
        
        # Try to format variables JSON for better display
        try:
            variables_obj = json.loads(variables_str)
            formatted_variables = json.dumps(variables_obj, indent=2)
        except:
            formatted_variables = variables_str
        
        response_content = f"""✅ **GraphQL Query Generated**

**Input**: {message.content}

**Generated GraphQL Query**:
```graphql
{query_str}
```

**Variables**:
```json
{formatted_variables}
```

**Session Info**: Message #{count} from {user.identifier}"""
        
    except httpx.TimeoutException:
        response_content = f"""⏰ **Request Timeout**

The query took too long to process. Please try again with a simpler query.

**Input**: {message.content}"""
        
    except httpx.HTTPStatusError as e:
        response_content = f"""❌ **API Error**

Failed to process your query. Status: {e.response.status_code}

**Input**: {message.content}
**Error**: {e.response.text if hasattr(e.response, 'text') else 'Unknown error'}"""
        
    except Exception as e:
        response_content = f"""❌ **Processing Error**

An error occurred while processing your query.

**Input**: {message.content}
**Error**: {str(e)}"""
    
    # Update the message with the result
    msg.content = response_content
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