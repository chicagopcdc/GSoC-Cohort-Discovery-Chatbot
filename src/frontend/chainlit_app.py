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
            content=" Authentication required. Please login first."
        ).send()
        return
    
    # Create session
    session_id = str(uuid.uuid4())[:8]
    cl.user_session.set("session_id", session_id)
    cl.user_session.set("message_count", 0)
    
    # Welcome message
    welcome_msg = f""" **Welcome to PCDC GraphQL Generator!**

 **Logged in as**: {user.identifier}
 **Session ID**: {session_id}
 **Chat History**: Enabled (check left sidebar)

Enter your natural language query to generate nested GraphQL filters for PCDC data.

**Example queries:**
- The cohort consists of participants from the INRG consortium who have metastatic tumors
- Find patients with absent tumor state and skin tumor site
- Show NODAL consortium participants with bulky nodal aggregate"""
    
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
        
        # Step 1: Call /nested_graphql API to generate nested GraphQL query
        async with httpx.AsyncClient() as client:
            nested_response = await client.post(
                f"{BACKEND_URL}/nested_graphql",
                json={
                    "text": message.content,
                    "session_id": session_id
                },
                headers={"Content-Type": "application/json"},
                timeout=30.0
            )
            nested_response.raise_for_status()
            nested_result = nested_response.json()
        
        # Extract results from nested_graphql response
        user_query = nested_result.get("user_query", "")
        extracted_keywords = nested_result.get("extracted_keywords", [])
        pcdc_schemas = nested_result.get("pcdc_schemas", [])
        gitops_nodes = nested_result.get("gitops_nodes", [])
        nested_graphql_filter = nested_result.get("nested_graphql_filter", {})
        executable_nested_graphql = nested_result.get("executable_nested_graphql", None)
        success = nested_result.get("success", False)
        
        # Format the nested GraphQL filter for display
        try:
            formatted_filter = json.dumps(nested_graphql_filter, indent=2, ensure_ascii=False)
        except:
            formatted_filter = str(nested_graphql_filter)
        
        # Format the executable nested GraphQL for display
        try:
            formatted_executable = json.dumps(executable_nested_graphql, indent=2, ensure_ascii=False) if executable_nested_graphql else "None"
        except:
            formatted_executable = str(executable_nested_graphql) if executable_nested_graphql else "None"
        
        # Step 2: Call /query API to execute the GraphQL query
        query_result = None
        query_error = None
        
        # graph query execution
        if executable_nested_graphql and isinstance(executable_nested_graphql, dict):  # Only execute if we have a valid executable GraphQL
            query_str = executable_nested_graphql.get("query", "")
            variables_obj = executable_nested_graphql.get("variables", {})
            
            if query_str.strip():  # Only execute if we have a valid query string
                try:
                    async with httpx.AsyncClient() as client:
                        query_response = await client.post(
                            f"{BACKEND_URL}/query",
                            json={
                                "query": query_str,
                                "variables": variables_obj,
                                "use_cached_token": True
                            },
                            headers={"Content-Type": "application/json"},
                            timeout=5.0
                        )
                        query_response.raise_for_status()
                        query_result = query_response.json()
                except Exception as e:
                    query_error = str(e)

        # Format the complete response
        status_icon = "✅" if success else "❌"
        response_content = f"""{status_icon} **Nested GraphQL Filter Generated**

**Input**: {message.content}

**Extracted Keywords**: {', '.join(extracted_keywords)}

**PCDC Schemas**: {', '.join(pcdc_schemas)}

**GitOps Nodes**: {', '.join(gitops_nodes)}

**Generated Nested GraphQL Filter**:
```json
{formatted_filter}
```

**Executable Nested GraphQL**:
```json
{formatted_executable}
```"""

        # Add query execution results
        if query_result:
            if query_result.get("success", False):
                query_data = query_result.get("data", {})
                formatted_data = json.dumps(query_data, indent=2)
                response_content += f"""

**Query Execution**: ✅ **Success**
```json
{formatted_data}
```"""
            else:
                errors = query_result.get("errors", [])
                formatted_errors = json.dumps(errors, indent=2)
                response_content += f"""

**Query Execution**: ❌ **Failed**
**Errors**:
```json
{formatted_errors}
```"""
        elif query_error:
            response_content += f"""

**Query Execution**: ❌ **Error**
**Error**: {query_error}"""
        elif not executable_nested_graphql:
            response_content += f"""

**Query Execution**: ⚠️ **Skipped** (No executable GraphQL generated)"""
        else:
            response_content += f"""

**Query Execution**: ⚠️ **Skipped** (Executable GraphQL available but not executed)"""

        # Add error information if processing failed
        if not success and nested_result.get("error"):
            response_content += f"""

**Error Details**:
```
{nested_result.get("error")}
```"""
        
        response_content += f"""

**Session Info**: Message #{count} from {user.identifier}

 **Tip**: If you need to query multiple fields, please provide more specific context to help generate accurate GraphQL."""
        
    except httpx.TimeoutException:
        response_content = f""" **Request Timeout**

The query took too long to process. Please try again with a simpler query.

**Input**: {message.content}"""
        
    except httpx.HTTPStatusError as e:
        response_content = f""" **API Error**

Failed to process your query. Status: {e.response.status_code}

**Input**: {message.content}
**Error**: {e.response.text if hasattr(e.response, 'text') else 'Unknown error'}"""
        
    except Exception as e:
        response_content = f""" **Processing Error**

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
        content=f" **Conversation Resumed**\n\nWelcome back, {user.identifier}! You have {message_count} previous messages.",
        author="System"
    ).send()

@cl.author_rename
def rename(orig_author: str):
    """Rename authors for display"""
    rename_dict = {
        "System": " Assistant",
        "User": " You"
    }
    return rename_dict.get(orig_author, orig_author)

if __name__ == "__main__":
    from chainlit.cli import run_chainlit
    run_chainlit(__file__)