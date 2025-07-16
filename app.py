import os
import time
import uuid
import re
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
import json

# Import custom modules
from schema_parser import parse_pcdc_schema, extract_relevant_schema, standardize_terms
from query_builder import build_graphql_filter, extract_query_conditions, build_graphql_query, analyze_query_complexity, decompose_query, combine_results
from context_manager import session_manager
from prompt_builder import create_enhanced_prompt, create_nested_query_prompt
from filter_utils import getFilterState, getGQLFilter, SchemaTypeHandler

# Load environment variables
load_dotenv()

app = FastAPI()

# Define input model
class Query(BaseModel):
    text: str
    session_id: Optional[str] = None

# Define output model
class GraphQLResponse(BaseModel):
    query: str
    explanation: Optional[str] = None
    variables: str = "{}"

# Create LangChain components
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY")
)

# Load PCDC schema
node_properties = {}
term_mappings = {}

# 创建SchemaTypeHandler实例
schema_handler = None

try:
    node_properties, term_mappings = parse_pcdc_schema("pcdc-schema-prod-20250114.json")
    schema_handler = SchemaTypeHandler(node_properties)
    print(f"Successfully loaded PCDC schema, node count: {len(node_properties)}")
except Exception as e:
    print(f"Failed to load PCDC schema: {str(e)}")

def process_dotted_paths(variables):
    """
    dot-separated paths to nested structure
    e.g. {"disease_characteristics.bulky_nodal_aggregate": ["No"]} to
    {"nested": {"path": "disease_characteristics", "AND": [{"IN": {"bulky_nodal_aggregate": ["No"]}}]}}
    """
    if not isinstance(variables, dict):
        return variables
    
    result = {}
    dotted_paths = {}
    
    # collect all dot-separated paths and group by parent path
    for key, value in list(variables.items()):
        if "." in key:
            parts = key.split(".", 1)
            parent, child = parts[0], parts[1]
            
            if parent not in dotted_paths:
                dotted_paths[parent] = []
            
            # check if value is a list (corresponding to IN operation)
            if isinstance(value, list):
                dotted_paths[parent].append({"IN": {child: value}})
            else:
                # non-list value
                dotted_paths[parent].append({child: value})
            
            # remove dot-separated path from original dict
            del variables[key]
        elif isinstance(value, dict):
            # recursive process nested dict
            variables[key] = process_dotted_paths(value)
    
    # build nested structure
    for parent, conditions in dotted_paths.items():
        nested_obj = {
            "nested": {
                "path": parent,
                "AND": conditions
            }
        }
        
        # if result already has nested structure for this parent path, merge them
        if parent in result and "nested" in result[parent]:
            result[parent]["nested"]["AND"].extend(conditions)
        else:
            result[parent] = nested_obj
    
    # merge processed results
    result.update(variables)
    
    return result

def process_variables_string(variables_string):
    """
    directly process dot-separated paths on variables string, no need to parse to dict first
    """
    try:
        # try standard processing first
        vars_dict = json.loads(variables_string)
        
        # recursive process dot-separated paths
        if isinstance(vars_dict, dict):
            # if it is a dict, start processing
            processed_vars = process_dotted_paths(vars_dict)
            
            # ensure filter wrapper layer is included
            if "filter" not in processed_vars:
                processed_vars = {"filter": processed_vars}
            
            return json.dumps(processed_vars)
        else:
            # if not a dict, return original string
            return variables_string
    except json.JSONDecodeError:
        # if cannot parse JSON, return original string
        print(f"Error decoding JSON string: {variables_string}")
        return variables_string
    except Exception as e:
        # other errors, return original string
        print(f"Error processing variables string: {str(e)}")
        return variables_string

# Set up route
@app.post("/convert")
async def convert_to_graphql(query: Query):
    try:
        # Get or create session
        session_id = query.session_id if query.session_id else str(uuid.uuid4())
        memory = session_manager.get_or_create_session(session_id)
        
        # Standardize user input
        standardized_query = standardize_terms(query.text, term_mappings)
        
        # Analyze query complexity
        complexity = analyze_query_complexity(standardized_query)
        
        # Extract relevant schema information
        relevant_schema = extract_relevant_schema(standardized_query, node_properties)
        # print(f"relevant schema: {relevant_schema}")
        
        result = None
        
        if complexity == "complex":
            # Handle complex query
            query_parts = decompose_query(standardized_query)
            
            # Create a comprehensive schema that includes all related nodes
            comprehensive_schema = relevant_schema.copy()
            
            # Add schema information for related nodes
            for node in query_parts["related_nodes"]:
                node_schema = extract_relevant_schema(node, node_properties)
                comprehensive_schema.update(node_schema)
            
            # Get conversation history
            conversation_history = memory.get_formatted_context()
            
            # Create prompt with enhanced schema to generate a single nested query
            prompt_text = create_enhanced_prompt(standardized_query, comprehensive_schema, conversation_history)
            
            # Call LLM
            response = llm.invoke(prompt_text)
            
            # Parse results
            try:
                result = json.loads(response.content)
                
                # Update session memory
                memory.add_message({"role": "user", "content": standardized_query})
                memory.add_message({"role": "assistant", "content": response.content})
            except Exception as e:
                print(f"Failed to parse complex query result: {str(e)}")
                
                # If parsing as JSON fails, try to extract the query and variables
                content = response.content
                query_match = re.search(r'```graphql\s*(.*?)\s*```', content, re.DOTALL)
                variables_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
                
                query_str = query_match.group(1) if query_match else ""
                variables_str = variables_match.group(1) if variables_match else "{}"
                
                result = {
                    "query": query_str,
                    "variables": variables_str,
                    "explanation": "Query and variables extracted from response"
                }
                
                print(f"Extracted content: {content}")
        else:
            # Handle simple query
            conversation_history = memory.get_formatted_context()
            
            # Create prompt
            prompt_text = create_enhanced_prompt(standardized_query, relevant_schema, conversation_history)
            
            # Call LLM
            response = llm.invoke(prompt_text)
            
            # Parse results
            try:
                result = json.loads(response.content)
            except Exception as json_error:
                # If parsing as JSON fails, try to extract the query and variables
                content = response.content
                query_match = re.search(r'```graphql\s*(.*?)\s*```', content, re.DOTALL)
                variables_match = re.search(r'```json\s*(.*?)\s*```', content, re.DOTALL)
                
                query_str = query_match.group(1) if query_match else ""
                variables_str = variables_match.group(1) if variables_match else "{}"
                
                result = {
                    "query": query_str,
                    "variables": variables_str,
                    "explanation": "Query and variables extracted from response"
                }
                
                print(f"Error parsing JSON response: {str(json_error)}")
                print(f"Extracted content: {content}")
            
            # Update session memory
            memory.add_message({"role": "user", "content": query.text})
            memory.add_message({"role": "assistant", "content": json.dumps(result)})
        
        # Save results to file
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        file_path = f"chat_history/{timestamp}.txt"
        print(f"Results saved to: {file_path}")
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Save query and results
        with open(file_path, "w") as f:
            f.write(f"Query: {query.text}\n")
            f.write(f"Standardized Query: {standardized_query}\n")
            f.write(f"GraphQL Query: {result.get('query', '')}\n")
            f.write(f"Variables: {result.get('variables', '')}\n")
            f.write(f"Explanation: {result.get('explanation', '')}")
        
        # process and format variables
        variables = result.get("variables", "{}")

        # === 新增：用getFilterState+getGQLFilter标准化 ===
        # 1. 解析LLM生成的variables为dict
        if isinstance(variables, str):
            try:
                variables_dict = json.loads(variables)
            except Exception:
                variables_dict = {}
        else:
            variables_dict = variables
        # 2. 反解为FilterState
        filter_state = getFilterState(variables_dict)
        # 3. 正向生成标准GQL filter (使用schema_handler自动处理所有字段类型)
        gql_filter = getGQLFilter(filter_state, schema_handler)
        # 4. 包裹成{"filter": ...}结构
        variables = json.dumps({"filter": gql_filter} if gql_filter is not None else {})
        # === 结束 ===

        # save processed variables for debugging
        with open(f"chat_history/{timestamp}_processed.txt", "w") as f:
            f.write(variables)
        
        return GraphQLResponse(
            query=result.get("query", ""),
            variables=variables,
            explanation=result.get("explanation", "")
        )
    except Exception as e:
        print(f"Error in convert_to_graphql: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Add session management routes
@app.post("/sessions/create")
async def create_session():
    session_id = str(uuid.uuid4())
    session_manager.get_or_create_session(session_id)
    return {"session_id": session_id}

@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str):
    session_manager.delete_session(session_id)
    return {"status": "success"}

@app.get("/sessions")
async def list_sessions():
    return {"sessions": session_manager.get_all_session_ids()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 