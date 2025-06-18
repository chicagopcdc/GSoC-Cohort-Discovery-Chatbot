import chainlit as cl
from langchain_openai import ChatOpenAI
import os
import json
import uuid
import traceback
from dotenv import load_dotenv

# Import custom modules
from schema_parser import parse_pcdc_schema, extract_relevant_schema, standardize_terms
from query_builder import analyze_query_complexity, decompose_query, combine_results
from context_manager import session_manager
from prompt_builder import create_enhanced_prompt, create_nested_query_prompt

load_dotenv()

# Create LLM instance
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY")
)

# Load PCDC schema
node_properties = {}
term_mappings = {}

try:
    node_properties, term_mappings = parse_pcdc_schema("pcdc-schema-prod-20250114.json")
    print(f"Successfully loaded PCDC schema, node count: {len(node_properties)}")
except Exception as e:
    print(f"Failed to load PCDC schema: {str(e)}")

@cl.on_chat_start
async def on_chat_start():
    # Create new session
    session_id = str(uuid.uuid4())
    memory = session_manager.get_or_create_session(session_id)
    
    # Store session ID
    cl.user_session.set("session_id", session_id)
    
    await cl.Message(
        content="Welcome to PCDC GraphQL Query Generator! Please enter your query."
    ).send()

@cl.on_message
async def main(message: cl.Message):
    # Get session ID and memory
    session_id = cl.user_session.get("session_id")
    memory = session_manager.get_or_create_session(session_id)
    
    # Send thinking message
    thinking_msg = cl.Message(content="Generating query...")
    await thinking_msg.send()
    
    try:
        # Standardize user input
        standardized_query = standardize_terms(message.content, term_mappings)
        
        # Analyze query complexity
        complexity = analyze_query_complexity(standardized_query)
        
        # Extract relevant schema information
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
                
                # Extract schema related to sub-query
                sub_schema = extract_relevant_schema(sub_query, node_properties)
                
                # Get conversation history
                conversation_history = memory.get_formatted_context()
                
                # Create prompt
                prompt_text = create_enhanced_prompt(sub_query, sub_schema, conversation_history)
                
                # Call LLM
                response = llm.invoke(prompt_text)
                
                # Parse results
                try:
                    sub_result = json.loads(response.content)
                    sub_results.append(sub_result)
                    
                    # Update session memory
                    memory.add_message({"role": "user", "content": sub_query})
                    memory.add_message({"role": "assistant", "content": response.content})
                except Exception as e:
                    print(f"Failed to parse sub-query result: {str(e)}")
                    print(f"Response content: {response.content}")
            
            # Combine results
            result = combine_results(sub_results, standardized_query)
        else:
            # Handle simple query
            thinking_msg.content = "Generating GraphQL query..."
            await thinking_msg.update()
            
            # Get conversation history
            conversation_history = memory.get_formatted_context()
            
            # Create prompt
            prompt_text = create_enhanced_prompt(standardized_query, relevant_schema, conversation_history)
            
            # Call LLM
            response = llm.invoke(prompt_text)
            print(f"LLM response: {response.content}")
            
            # Parse results
            try:
                result = json.loads(response.content)
                print(f"Successfully parsed JSON: {result}")
            except Exception as e:
                # If parsing as JSON fails, try to extract the query and variables
                print(f"Failed to parse JSON: {str(e)}")
                content = response.content
                import re
                query_match = re.search(r'```graphql\s*(.*?)\s*```', content, re.DOTALL)
                variables_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
                
                query = query_match.group(1) if query_match else ""
                variables = variables_match.group(1) if variables_match else "{}"
                
                print(f"Extracted query: {query}")
                print(f"Extracted variables: {variables}")
                
                result = {
                    "query": query,
                    "variables": variables,
                    "explanation": "Query and variables extracted from response"
                }
            
            # Update session memory
            memory.add_message({"role": "user", "content": message.content})
            memory.add_message({"role": "assistant", "content": json.dumps(result)})
        
        # Format results
        query_display = f"```graphql\n{result.get('query', '')}\n```"
        variables_display = f"```json\n{result.get('variables', '')}\n```" if result.get('variables') else ""
        explanation = result.get('explanation', '')
        
        # Update message
        thinking_msg.content = f"**GraphQL Query:**\n{query_display}\n\n**Variables:**\n{variables_display}\n\n**Explanation:**\n{explanation}"
        await thinking_msg.update()
        
        # Add copy buttons
        elements = []
        if result.get('query'):
            elements.append(
                cl.Code(
                    name="query.graphql",
                    language="graphql",
                    value=result.get('query', '')
                )
            )
        if result.get('variables'):
            try:
                # Ensure variables is a string
                variables_value = result.get('variables', '')
                if isinstance(variables_value, dict):
                    variables_value = json.dumps(variables_value, indent=2)
                
                elements.append(
                    cl.Code(
                        name="variables.json",
                        language="json",
                        value=variables_value
                    )
                )
            except Exception as e:
                print(f"Error creating variables code element: {str(e)}")
                print(f"Variables value: {result.get('variables', '')}")
        
        if elements:
            try:
                await cl.Message(content="You can copy the following code:", elements=elements).send()
            except Exception as e:
                print(f"Error sending code elements: {str(e)}")
                traceback.print_exc()
    
    except Exception as e:
        print(f"Error in main function: {str(e)}")
        traceback.print_exc()
        thinking_msg.content = f"Error generating query: {str(e)}"
        await thinking_msg.update() 