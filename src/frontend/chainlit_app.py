import chainlit as cl
from typing import Optional, List
import os
from datetime import datetime
import uuid
from dotenv import load_dotenv
import json
import httpx
import re
import tempfile
import shutil

load_dotenv()

# Backend API URL
BACKEND_URL = "http://localhost:8000"

def extract_json_from_text(text: str) -> Optional[str]:
    """Extract JSON content from a text that contains markdown code blocks"""
    # Try to find JSON in markdown code blocks
    json_pattern = r'```json\s*([\s\S]*?)\s*```'
    matches = re.findall(json_pattern, text, re.MULTILINE)
    
    if matches:
        # Return the last JSON block found (usually the main result)
        return matches[-1].strip()
    
    # If no markdown blocks, try to find raw JSON
    # Look for content between first { and last }
    try:
        start = text.find('{')
        end = text.rfind('}')
        if start != -1 and end != -1 and end > start:
            potential_json = text[start:end+1]
            # Validate it's valid JSON
            json.loads(potential_json)
            return potential_json
    except:
        pass
    
    return None

async def handle_uploaded_files(files: List[cl.File]) -> List[str]:
    """
    Handle uploaded files and return their paths
    """
    uploaded_paths = []
    
    # Create temp directory if not exists
    temp_dir = cl.user_session.get("temp_dir")
    if not temp_dir:
        temp_dir = tempfile.mkdtemp(prefix="chainlit_logs_")
        cl.user_session.set("temp_dir", temp_dir)
        print(f"Created temp directory: {temp_dir}")
    
    for file in files:
        try:
            # Debug info
            print(f"Processing file: {file.name}")
            print(f"File attributes: {dir(file)}")
            
            # Save file to temp directory
            file_path = os.path.join(temp_dir, file.name)
            
            # Read file content properly
            content = None
            
            # Method 1: Try to read using path
            if hasattr(file, 'path') and file.path:
                print(f"Reading from path: {file.path}")
                with open(file.path, 'rb') as src:
                    content = src.read()
            
            # Method 2: Try to get content directly
            elif hasattr(file, 'content') and file.content is not None:
                print(f"Using direct content")
                content = file.content if isinstance(file.content, bytes) else file.content.encode()
            
            # Method 3: Try async read
            elif hasattr(file, 'read'):
                print(f"Using async read method")
                content = await file.read()
            
            # Method 4: Check for other possible attributes
            else:
                # List all attributes for debugging
                attrs = {attr: getattr(file, attr, None) for attr in dir(file) if not attr.startswith('_')}
                print(f"File attributes: {attrs}")
                raise ValueError(f"Could not find a way to read file content for {file.name}")
            
            if content is None:
                raise ValueError(f"File content is None for {file.name}")
            
            # Write content to temp location
            with open(file_path, 'wb') as f:
                f.write(content)
            
            print(f"Saved file to: {file_path}")
            uploaded_paths.append(file_path)
            
        except Exception as e:
            print(f"Error handling file {file.name}: {str(e)}")
            raise
        
    return uploaded_paths

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
    cl.user_session.set("uploaded_files", [])  # Store uploaded file paths
    cl.user_session.set("temp_dir", None)  # Store temp directory for cleanup
    
    # Welcome message
    welcome_msg = f"""👋 **Welcome to Cross-Component Interaction Analyzer!**

✅ **Logged in as**: {user.identifier}
🔗 **Session ID**: {session_id}
📋 **Chat History**: Enabled (check left sidebar)

This tool analyzes log files to detect cross-component interactions and categorize them into bug patterns.

**How to use:**
1. 📤 **Upload log files** - Drag and drop your log files or click the attachment button
2. 🔍 **Analyze** - Type "analyze" to analyze default files, or upload files first
3. 📊 **View results** - See categorized interactions and patterns

**Commands:**
- Type "analyze" to analyze default log files
- Upload files then type "analyze" to analyze your files
- Type "help" for more information

**Bug Pattern Categories:**
- 🔴 **Resource Leak** (resource_invocation)
- 🟡 **Resource Contention** (abnormal_usage)  
- 🟢 **Semantic Inconsistency** (shared_object)"""
    
    await cl.Message(content=welcome_msg, author="System").send()

@cl.on_message
async def main(message: cl.Message):
    """Process user messages and handle file uploads"""
    # Get user session
    user = cl.user_session.get("user")
    if not user:
        await cl.Message(content="❌ Please login first.").send()
        return
    
    # Update message count
    count = cl.user_session.get("message_count", 0) + 1
    cl.user_session.set("message_count", count)
    
    # Handle uploaded files if any
    if message.elements:
        uploaded_files = []
        for element in message.elements:
            if hasattr(element, 'type') and element.type == "file":
                uploaded_files.append(element)
            elif hasattr(element, 'name'):  # Alternative check for file elements
                uploaded_files.append(element)
        
        if uploaded_files:
            try:
                # Process uploaded files
                file_paths = await handle_uploaded_files(uploaded_files)
                
                # Update session with uploaded files
                existing_files = cl.user_session.get("uploaded_files", [])
                existing_files.extend(file_paths)
                cl.user_session.set("uploaded_files", existing_files)
                
                # Inform user about uploaded files
                file_list = "\n".join([f"- {os.path.basename(fp)}" for fp in file_paths])
                await cl.Message(
                    content=f"📤 **Files uploaded successfully!**\n\n{file_list}\n\nTotal files ready for analysis: {len(existing_files)}\n\nType 'analyze' to analyze these files."
                ).send()
                return
            except Exception as e:
                await cl.Message(
                    content=f"❌ **Error uploading files**: {str(e)}\n\nPlease try again or use default files by typing 'analyze'."
                ).send()
                return
    
    # Check if user wants help
    user_input = message.content.lower().strip()
    if user_input == "help":
        help_msg = """📚 **Help Information**

**How to use:**
1. **Upload files**: Drag and drop log files or use the attachment button
2. **Analyze**: Type 'analyze' after uploading files
3. **Clear files**: Type 'clear' to remove uploaded files

**Available Commands:**
- `analyze` - Run complete analysis (interaction + diagnosis) on uploaded files or defaults
- `clear` - Clear all uploaded files
- `help` - Show this help message

**What this tool does:**
1. **Interaction Analysis**: Identifies cross-component interactions and categorizes them into bug patterns
2. **Template Diagnosis**: Fills diagnostic templates based on log evidence
3. **Combined Results**: Shows both JSON analysis and template-based diagnosis

**Analysis includes:**
- 🔍 Component relationship mapping
- 🎯 Bug pattern classification (Resource Leak, Contention, Semantic Inconsistency)
- 📋 Template-based diagnosis with detailed reasoning
- 📊 All results in JSON format"""
        await cl.Message(content=help_msg).send()
        return
    
    # Check if user wants to clear files
    if user_input == "clear":
        cl.user_session.set("uploaded_files", [])
        await cl.Message(content="🗑️ All uploaded files have been cleared.").send()
        return
    
    # Check if user wants to analyze
    if "analyze" not in user_input:
        await cl.Message(
            content="💡 Upload log files first, then type 'analyze' to process them. Type 'help' for more information."
        ).send()
        return
    
    # Get uploaded files
    uploaded_files = cl.user_session.get("uploaded_files", [])
    
    # Send thinking message
    if uploaded_files:
        msg = cl.Message(content=f"🔍 Analyzing {len(uploaded_files)} uploaded log files...\n\nThis may take a few minutes...")
    else:
        msg = cl.Message(content="🔍 Analyzing default log files...\n\nThis may take a few minutes...")
    await msg.send()
    
    try:
        # Get session ID
        session_id = cl.user_session.get("session_id")
        
        # Prepare log files parameter
        log_files_param = uploaded_files if uploaded_files else None
        
        # Call /analyze_interaction API
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{BACKEND_URL}/analyze_interaction",
                json={
                    "log_files": log_files_param,
                    "templates_path": "./template/",
                    "session_id": session_id
                },
                headers={"Content-Type": "application/json"},
                timeout=300.0  # 5 minute timeout for analysis
            )
            response.raise_for_status()
            result = response.json()
        
        # Extract data from result
        interaction_pairs = result.get("interaction_pairs", "")
        dispatched_interactions = result.get("dispatched_interactions", "")
        log_files = result.get("log_files", [])
        success = result.get("success", False)
        message_text = result.get("message", "")
        
        if not success:
            await msg.update(content=f"❌ Analysis failed: {message_text}")
            return
        
        # Call /diagnose API for template-based diagnosis
        async with httpx.AsyncClient() as client:
            diagnose_response = await client.post(
                f"{BACKEND_URL}/diagnose",
                json={
                    "log_files": log_files_param,
                    "templates_path": None,  # Use default templates
                    "session_id": session_id
                },
                headers={"Content-Type": "application/json"},
                timeout=300.0  # 5 minute timeout for diagnosis
            )
            diagnose_response.raise_for_status()
            diagnose_result = diagnose_response.json()
        
        # Extract diagnosis data
        diagnosis_results = diagnose_result.get("results", {})
        diagnosis_success = diagnose_result.get("success", False)
        diagnosis_message = diagnose_result.get("message", "")
        
        # Extract JSON contents
        interaction_json = extract_json_from_text(interaction_pairs)
        dispatched_json = extract_json_from_text(dispatched_interactions)
        
        # Format the response
        response_content = f"""✅ **Cross-Component Analysis & Diagnosis Complete**

📁 **Analyzed Log Files** ({len(log_files)} files):"""
        
        for log_file in log_files:
            filename = os.path.basename(log_file)
            response_content += f"\n- {filename}"
        
        response_content += "\n\n"
        
        # Display all JSON results
        response_content += "📊 **Analysis Results:**\n\n"
        
        # Show interaction pairs JSON
        if interaction_json:
            response_content += "**1️⃣ Interaction Pairs (Component Relationships):**\n"
            response_content += "```json\n"
            try:
                parsed_json = json.loads(interaction_json)
                response_content += json.dumps(parsed_json, indent=2)
            except:
                response_content += interaction_json
            response_content += "\n```\n\n"
        
        # Show dispatched interactions JSON
        if dispatched_json:
            response_content += "**2️⃣ Categorized Interactions (Bug Patterns):**\n"
            response_content += "```json\n"
            try:
                parsed_json = json.loads(dispatched_json)
                response_content += json.dumps(parsed_json, indent=2)
                
                # Add summary statistics
                resource_invocation = len(parsed_json.get("resource_invocation", []))
                abnormal_usage = len(parsed_json.get("abnormal_usage", []))
                shared_object = len(parsed_json.get("shared_object", []))
                
                response_content += f"\n```\n\n📈 **Summary:**\n"
                response_content += f"- 🔴 Resource Invocation: {resource_invocation} patterns\n"
                response_content += f"- 🟡 Abnormal Usage: {abnormal_usage} patterns\n"
                response_content += f"- 🟢 Shared Object: {shared_object} patterns\n"
                
            except:
                response_content += dispatched_json
                response_content += "\n```\n"
        
        # Show diagnosis results
        if diagnosis_success and diagnosis_results:
            response_content += "\n**3️⃣ Template-Based Diagnosis Results:**\n"
            
            # Count total diagnosis results
            total_templates = len(diagnosis_results)
            total_responses = sum(len(responses) for responses in diagnosis_results.values())
            response_content += f"\n📋 **Templates Analyzed**: {total_templates}\n"
            response_content += f"📝 **Total Responses**: {total_responses}\n\n"
            
            # Show each template result
            for template_id, responses in diagnosis_results.items():
                response_content += f"**Template: {template_id}**\n"
                for i, response_text in enumerate(responses, 1):
                    response_content += f"```\nResponse {i}:\n{response_text}\n```\n\n"
        
        elif diagnosis_success:
            response_content += "\n**3️⃣ Template-Based Diagnosis:**\n"
            response_content += "⚠️ No template diagnosis results found.\n\n"
        else:
            response_content += "\n**3️⃣ Template-Based Diagnosis:**\n"
            response_content += f"❌ Diagnosis failed: {diagnosis_message}\n\n"
        
        # Add raw response for debugging
        response_content += f"\n<details>\n<summary>🔧 Full API Responses (click to expand)</summary>\n\n"
        response_content += "**Interaction Analysis Response:**\n```json\n"
        response_content += json.dumps(result, indent=2)
        response_content += "\n```\n\n"
        
        if diagnosis_success:
            response_content += "**Diagnosis Response:**\n```json\n"
            response_content += json.dumps(diagnose_result, indent=2)
            response_content += "\n```\n"
        
        response_content += "</details>"
        
        response_content += f"\n\n✅ Analysis: {message_text}"
        if diagnosis_success:
            response_content += f"\n✅ Diagnosis: {diagnosis_message}"
        response_content += f"\n\n**Session Info**: Message #{count} from {user.identifier}"
        
    except httpx.TimeoutException:
        response_content = """⏰ **Request Timeout**

The analysis took too long to complete. This can happen with large log files.

Please try again with fewer files or check the server logs for more information."""
        
    except httpx.HTTPStatusError as e:
        response_content = f"""❌ **API Error**

Failed to analyze log files. Status: {e.response.status_code}

**Error**: {e.response.text if hasattr(e.response, 'text') else 'Unknown error'}"""
        
    except Exception as e:
        response_content = f"""❌ **Processing Error**

An error occurred during analysis.

**Error**: {str(e)}

**Note**: If only diagnosis failed, interaction analysis may still be available in the full response."""
    
    # Update the message with the result
    msg.content = response_content
    await msg.update()

@cl.on_chat_end
async def on_chat_end():
    """Clean up temporary files when chat ends"""
    temp_dir = cl.user_session.get("temp_dir")
    if temp_dir and os.path.exists(temp_dir):
        try:
            shutil.rmtree(temp_dir)
            print(f"Cleaned up temp directory: {temp_dir}")
        except Exception as e:
            print(f"Error cleaning up temp directory: {e}")

@cl.on_chat_resume
async def on_chat_resume(thread):
    """Resume a previous conversation"""
    user = cl.user_session.get("user")
    if not user:
        return
    
    # Initialize file-related session variables
    cl.user_session.set("uploaded_files", [])
    cl.user_session.set("temp_dir", None)
    
    # Count previous messages
    message_count = 0
    if thread and "steps" in thread:
        message_count = len([s for s in thread["steps"] if s.get("type") == "user_message"])
    
    cl.user_session.set("message_count", message_count)
    
    await cl.Message(
        content=f"📂 **Conversation Resumed**\n\nWelcome back, {user.identifier}! You have {message_count} previous messages.\n\nYou can upload new log files or type 'analyze' to analyze default files. Type 'help' for more information.",
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