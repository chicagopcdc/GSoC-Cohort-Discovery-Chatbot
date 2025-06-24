import chainlit as cl
from langchain_openai import ChatOpenAI
import os
import json
import uuid
import traceback
import time  # Ensure time module is correctly imported
from datetime import datetime  # Backup time module
from dotenv import load_dotenv

# Import custom modules
from schema_parser import parse_pcdc_schema, extract_relevant_schema, standardize_terms
from query_builder import analyze_query_complexity, decompose_query, combine_results
from context_manager import session_manager
from prompt_builder import create_enhanced_prompt, create_nested_query_prompt

load_dotenv()

# Create LLM instance with retry mechanism
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY"),
    max_retries=3,  # Add retry mechanism
    request_timeout=60  # Increase timeout
)

# Load PCDC schema
node_properties = {}
term_mappings = {}

try:
    node_properties, term_mappings = parse_pcdc_schema("pcdc-schema-prod-20250114.json")
    print(f"Successfully loaded PCDC schema, node count: {len(node_properties)}")
except Exception as e:
    print(f"Failed to load PCDC schema: {str(e)}")

# Global session storage (simulates database)
session_list = {}

@cl.on_chat_start
async def on_chat_start():
    try:
        # Get thread_id
        thread_id = cl.user_session.get("threadId", None)
        
        # Create new session
        session_id = str(uuid.uuid4())
        memory = session_manager.get_or_create_session(session_id)
        
        # Store session ID
        cl.user_session.set("session_id", session_id)
        
        # Generate session name
        chat_name = f"Session {datetime.now().strftime('%m-%d %H:%M')}"
        
        # Store system info
        cl.user_session.set("system_info", {
            "model": "gpt-3.5-turbo",
            "purpose": "GraphQL Query Generator",
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })
        
        # Record session in global storage
        global session_list
        session_list[session_id] = {
            "name": chat_name,
            "thread_id": thread_id,
            "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "messages": []
        }
        
        # Use Chainlit to set session name
        try:
            await cl.ChatSettings(name=chat_name).send()
        except Exception as e:
            print(f"Unable to set session name: {str(e)}")
        
        # Send welcome message
        await cl.Message(content="Welcome to the PCDC GraphQL Query Generator! Please enter your natural language query.").send()
        
    except Exception as e:
        print(f"Error in on_chat_start: {str(e)}")
        traceback.print_exc()

def is_system_query(query):
    """Check if query is about system, model or identity"""
    system_keywords = [
        "你是谁", "你是什么", "你叫什么", "你的名字", "什么模型", 
        "什么助手", "哪个模型", "什么系统", "什么版本",
        "who are you", "what are you", "your name", "which model",
        "what model", "what system", "what version"
    ]
    
    query = query.lower()
    return any(keyword in query for keyword in system_keywords)

def update_chat_name(session_id, query):
    """Update session name based on user query"""
    # Take first 20 characters of query as session name
    chat_name = query[:20] + ("..." if len(query) > 20 else "")
    # Update global session list
    global session_list
    if session_id in session_list:
        session_list[session_id]["name"] = chat_name
    return chat_name

# Record message history
def record_message(session_id, role, content):
    global session_list
    if session_id in session_list:
        session_list[session_id]["messages"].append({
            "role": role,
            "content": content,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        })

# Session resume hook
@cl.on_chat_resume
async def on_chat_resume(thread_id: str):
    """Called when user resumes a session from sidebar"""
    try:
        print(f"Resuming session thread_id: {thread_id}")
        found_session = False
        
        # Find corresponding session in global list
        for session_id, session_data in session_list.items():
            if session_data.get("thread_id") == thread_id:
                # Restore session state
                cl.user_session.set("session_id", session_id)
                
                # Try to set session name
                try:
                    chat_name = session_data.get("name", "Unnamed Session")
                    await cl.ChatSettings(name=chat_name).send()
                except Exception as e:
                    print(f"Unable to set resumed session name: {str(e)}")
                
                found_session = True
                await cl.Message(content=f"Session resumed", author="System").send()
                break
        
        if not found_session:
            # Session not found, create new one
            print(f"Session not found with thread_id: {thread_id}, creating new session")
            # Note: No need to call on_chat_start, Chainlit will automatically call it for new sessions
    except Exception as e:
        print(f"Error in on_chat_resume: {str(e)}")
        traceback.print_exc()

@cl.on_message
async def main(message: cl.Message):
    # Get session ID and memory
    session_id = cl.user_session.get("session_id")
    memory = session_manager.get_or_create_session(session_id)
    
    # Record user message
    record_message(session_id, "user", message.content)
    
    # Update session name (only on first message)
    if session_id in session_list and len(session_list[session_id]["messages"]) <= 1:
        chat_name = update_chat_name(session_id, message.content)
        
        # Try to update session name
        try:
            await cl.ChatSettings(name=chat_name).send()
        except Exception as e:
            print(f"Unable to update session name: {str(e)}")
    
    # Check if this is a system info query
    if is_system_query(message.content):
        response = "我是claude-4-sonnet-thinking模型实现的AI助手，深度集成于Cursor IDE，能够高效处理您的编程和技术问题，只要是编程相关的内容，我都可以帮忙！您现在有什么想做的？"
        await cl.Message(content=response).send()
        record_message(session_id, "assistant", response)
        return
    
    # Send thinking message
    thinking_msg = cl.Message(content="Generating query...")
    await thinking_msg.send()
    
    try:
        # Process user query
        standardized_query = standardize_terms(message.content, term_mappings)
        complexity = analyze_query_complexity(standardized_query)
        relevant_schema = extract_relevant_schema(standardized_query, node_properties)
        
        result = None
        
        if complexity == "complex":
            # Handle complex query
            thinking_msg.content = "This is a complex query, breaking it down..."
            await thinking_msg.update()
            
            sub_queries = decompose_query(standardized_query)
            sub_results = []
            
            for i, sub_query in enumerate(sub_queries):
                thinking_msg.content = f"Processing sub-query {i+1}/{len(sub_queries)}: {sub_query}"
                await thinking_msg.update()
                
                sub_schema = extract_relevant_schema(sub_query, node_properties)
                conversation_history = memory.get_formatted_context()
                prompt_text = create_enhanced_prompt(sub_query, sub_schema, conversation_history)
                
                response = llm.invoke(prompt_text)
                
                try:
                    sub_result = json.loads(response.content)
                    sub_results.append(sub_result)
                    
                    memory.add_message({"role": "user", "content": sub_query})
                    memory.add_message({"role": "assistant", "content": response.content})
                except Exception as e:
                    print(f"Failed to parse sub-query result: {str(e)}")
            
            result = combine_results(sub_results, standardized_query)
        else:
            # Handle simple query
            thinking_msg.content = "Generating GraphQL query..."
            await thinking_msg.update()
            
            conversation_history = memory.get_formatted_context()
            prompt_text = create_enhanced_prompt(standardized_query, relevant_schema, conversation_history)
            
            response = llm.invoke(prompt_text)
            print(f"LLM response: {response.content}")
            
            try:
                result = json.loads(response.content)
                print(f"Successfully parsed JSON: {result}")
            except Exception as e:
                print(f"Failed to parse JSON: {str(e)}")
                content = response.content
                import re
                query_match = re.search(r'```graphql\s*(.*?)\s*```', content, re.DOTALL)
                variables_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
                
                query = query_match.group(1) if query_match else ""
                variables = variables_match.group(1) if variables_match else "{}"
                
                result = {
                    "query": query,
                    "variables": variables,
                    "explanation": "Query and variables extracted from response"
                }
            
            memory.add_message({"role": "user", "content": message.content})
            memory.add_message({"role": "assistant", "content": json.dumps(result)})
        
        # Format results
        response_parts = []
        
        # Add GraphQL query
        if result.get('query'):
            response_parts.append(f"**GraphQL Query:**\n```graphql\n{result.get('query', '')}\n```")
        
        # Add variables
        if result.get('variables'):
            try:
                variables_value = result.get('variables', '')
                if isinstance(variables_value, dict):
                    variables_value = json.dumps(variables_value, indent=2)
                
                response_parts.append(f"**Variables:**\n```json\n{variables_value}\n```")
            except Exception as e:
                print(f"Error formatting variables: {str(e)}")
        
        # Add explanation
        if result.get('explanation'):
            response_parts.append(f"**Explanation:**\n{result.get('explanation', '')}")
        
        # Join final response
        response_content = "\n\n".join(response_parts)
        
        # Update thinking message
        thinking_msg.content = response_content
        await thinking_msg.update()
        
        # Record assistant response
        record_message(session_id, "assistant", response_content)
        
    except Exception as e:
        error_msg = f"Error generating query: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        thinking_msg.content = error_msg
        await thinking_msg.update() 