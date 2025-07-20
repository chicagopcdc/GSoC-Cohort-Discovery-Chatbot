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
from utils.schema_parser import *
from utils.query_builder import *
from utils.context_manager import session_manager
from utils.prompt_builder import *
from utils.filter_utils import *
from utils.filter_utils import parse_llm_response

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
    variables: str = "{}"

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

'''
    Todo:
        1. Change graphql format to aggregation query rather than line level query.
            转换要求：
                1. 使用 _aggregation 包装整个查询
                2. 删除 offset 和 first 参数
                3. 将 accessibility 改为 all
                4. 对每个字段使用 histogram 统计: field { histogram { key count } }
                5. 删除 subject_submitter_id(ID 在聚合中无意义)
                6. 保持 filter 和 variables 不变
        2. 前后端代码分离
        3. Simple mapping from user input to schema property.
'''
# Main convert function
@app.post("/convert")
async def convert_to_graphql(query: Query):
    # Load PCDC schema
    node_properties = {}
    term_mappings = {}
    schema_handler = None
    schema_file_path = "../../schema/gitops.json"
    
    llm = ChatOpenAI(
        model="gpt-3.5-turbo",
        temperature=0,
        api_key=os.getenv("OPENAI_API_KEY")
    )

    def convert_line_level_query_to_aggregation_query(line_level_query):
        ''' 
        In order to return non-empty data, convert line level query to aggregation query because:
            The user associated with that API key is a normal user so any query will work for aggregation only and will return zero results for line level data which is the normal behavior for any user.
        '''
        prompt = f"""
            Here is the line level query: \n
            {line_level_query} \n
            Change level query GraphQL format to aggregation query
            Requirements:
            1. Use _aggregation to wrap the entire query
            2. Remove offset and first parameters
            3. Change accessibility to all
            4. Use histogram statistics for each field: field {{ histogram {{ key count }} }}
            5. Remove subject_submitter_id (ID is meaningless in aggregation)
            6. Add _totalCount to show total count
            7. Keep filter and variables same as line level query
            8. Return single line JSON format

            Example for "Male subjects":
            {{
                "query": "query ($filter: JSON) {{ _aggregation {{ subject(accessibility: all, filter: $filter) {{ consortium {{ histogram {{ key count }} }} sex {{ histogram {{ key count }} }} _totalCount }} }} }}",
                "variables": {{"filter": {{"AND": [{{"IN": {{"sex": ["Male"]}}}}]}}}}
            }}
            """
        result = llm.invoke(prompt)
        return result

    try:
        node_properties, term_mappings = parse_pcdc_schema(schema_file_path)
        schema_handler = SchemaTypeHandler(node_properties)
        print(f"Successfully loaded PCDC schema, node count: {len(node_properties)}")
    except Exception as e:
        print(f"Failed to load PCDC schema: {str(e)}")
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
        # print(f"relevant schema: {relevant_schema} \n")
        
        result = None
        aggregation_query_mode = True
        if complexity == "complex":
            # Handle complex query
            query_parts = decompose_query(standardized_query)
            
            # Create a comprehensive schema that includes all related nodes
            comprehensive_schema = relevant_schema.copy()
            
            # Add schema information for related nodes
            for node in query_parts["related_nodes"]:
                node_schema = extract_relevant_schema(node, node_properties)
                comprehensive_schema.update(node_schema)
            
            # # LLM has its own memory, don't need to feed conversation_history again.
            # conversation_history = memory.get_formatted_context()
            
            # Create prompt with enhanced schema to generate a line level/aggregation query
            prompt_text = create_enhanced_prompt(standardized_query, comprehensive_schema)
            
            # Call LLM
            response = llm.invoke(prompt_text)
            if aggregation_query_mode:
                print(f"convert line level query to aggregation query.")
                response = convert_line_level_query_to_aggregation_query(response)

            # Parse results
            try:
                result = json.loads(response.content)
                
                # Update session memory
                # memory.add_message({"role": "user", "content": standardized_query})
                # memory.add_message({"role": "assistant", "content": response.content})
            except Exception as e:
                # Use the utility function to parse the response
                result = parse_llm_response(response.content, "Complex query")
        else:
            # Handle simple query
            # conversation_history = memory.get_formatted_context()
            
            # Create prompt
            prompt_text = create_enhanced_prompt(standardized_query, relevant_schema)
            
            # Call LLM
            response = llm.invoke(prompt_text)
            if aggregation_query_mode:
                print(f"convert line level query to aggregation query.")
                response = convert_line_level_query_to_aggregation_query(response)
            
            # Parse results
            try:
                result = json.loads(response.content)
            except Exception as json_error:
                # Use the utility function to parse the response
                result = parse_llm_response(response.content, "Simple query")
            
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
        
        # process and format variables
        variables = result.get("variables", "{}")

        # === 简化处理：直接使用LLM生成的variables ===
        # 1. 解析LLM生成的variables为dict
        if isinstance(variables, str):
            try:
                variables_dict = json.loads(variables)
            except Exception:
                variables_dict = {}
        else:
            variables_dict = variables
            
        # 2. 确保variables有正确的结构
        if variables_dict and "filter" not in variables_dict:
            # 如果没有filter包装，添加它
            variables = json.dumps({"filter": variables_dict})
        else:
            # 已经有正确结构或为空，直接使用
            variables = json.dumps(variables_dict) if variables_dict else "{}"
        # === 结束 ===

        # save processed variables for debugging
        with open(f"chat_history/{timestamp}_processed.txt", "w") as f:
            f.write(variables)
        
        return GraphQLResponse(
            query=result.get("query", ""),
            variables=variables,
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