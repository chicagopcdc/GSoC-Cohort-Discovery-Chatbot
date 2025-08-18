import os
import time
import uuid
import re
import httpx
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
import json

from utils.schema_parser import *
from utils.query_builder import *
from utils.context_manager import session_manager
from utils.prompt_builder import *
from utils.filter_utils import *
from utils.credential_helper import *

from utils.nested_graphql_helper import *

# Load environment variables
load_dotenv()

app = FastAPI()

BASE_URL = "https://portal-dev.pedscommons.org"
GRAPHQL_ENDPOINT = f"{BASE_URL}/guppy/graphql"
GUPPY_ACCESS_TOKEN = "eyJhbGciOiJSUzI1NiIsImtpZCI6ImZlbmNlX2tleV8yMDIyLTA5LTE1VDE2OjE5OjUwWiIsInR5cCI6IkpXVCJ9.eyJwdXIiOiJhY2Nlc3MiLCJpc3MiOiJodHRwczovL3BvcnRhbC1kZXYucGVkc2NvbW1vbnMub3JnL3VzZXIiLCJhdWQiOlsiaHR0cHM6Ly9wb3J0YWwtZGV2LnBlZHNjb21tb25zLm9yZy91c2VyIiwiZ29vZ2xlX2xpbmsiLCJ1c2VyIiwib3BlbmlkIiwiZGF0YSIsImdhNGdoX3Bhc3Nwb3J0X3YxIiwiZmVuY2UiLCJnb29nbGVfc2VydmljZV9hY2NvdW50IiwiZ29vZ2xlX2NyZWRlbnRpYWxzIiwiYWRtaW4iXSwiaWF0IjoxNzUzNTg4NDY3LCJleHAiOjE3NTM1OTIwNjcsImp0aSI6IjMwZTZmMWI1LTc4MjktNDZiZi04YTI0LTgwNTNlMDNhMGZhYiIsInNjb3BlIjpbImdvb2dsZV9saW5rIiwidXNlciIsIm9wZW5pZCIsImRhdGEiLCJnYTRnaF9wYXNzcG9ydF92MSIsImZlbmNlIiwiZ29vZ2xlX3NlcnZpY2VfYWNjb3VudCIsImdvb2dsZV9jcmVkZW50aWFscyIsImFkbWluIl0sImNvbnRleHQiOnsidXNlciI6eyJuYW1lIjoiZ3JhZ2xpYTAxQGdtYWlsLmNvbSIsImlzX2FkbWluIjp0cnVlLCJnb29nbGUiOnsicHJveHlfZ3JvdXAiOm51bGx9fX0sImF6cCI6IiIsInN1YiI6IjIifQ.VxQdRWarOzz5j947exC_yqGtoy2ieJ_0CLzseG0eQpV6dL7Vv2ObDvcNynE6tX8uTRQTrbMGy8DnnD36ZD0ux84R2pDseL-TgPrkW9euCfAMAewg0E1MmOvCU9AYun1qwJKTVPyme4IhBzeZvfpn5PU7Om6iAKT9KFAkh8n-rc6p_oqrG3vV9pOmh-aUnLgTLt94gCbXzK_rjAbndo6zELYBiu8vev7RQZIKc5itHDYXqZmRSE258jQU6CoglyFG69JwfXfcZRNTbv5u0gk9qdQ3DYPbXaBrMS1vKJUkvHShJcFBra74HNefNcwHeiB_-AW8vqW30MX03JzsWpcLpA"

# Define input model
class Query(BaseModel):
    text: str
    session_id: Optional[str] = None

# Define output model
class GraphQLResponse(BaseModel):
    query: str
    variables: str = "{}"

class GraphQLQuery(BaseModel):
    """Model for GraphQL query request"""
    query: str
    variables: Optional[Dict[str, Any]] = None
    use_cached_token: Optional[bool] = True

class GraphQLHttpResponse(BaseModel):
    """Model for GraphQL query response"""
    data: Optional[Dict[str, Any]] = None
    errors: Optional[list] = None
    success: bool
    message: Optional[str] = None

@app.post("/flat_graphql")
async def convert_to_flat_graphql(query: Query):
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
        # session_id = query.session_id if query.session_id else str(uuid.uuid4())
        # Standardize user input
        standardized_query = standardize_terms(query.text, term_mappings)
        # Extract relevant schema information
        relevant_schema = extract_relevant_schema(standardized_query, node_properties)

        result = None
        aggregation_query_mode = True
        query_parts = decompose_query(standardized_query)
        # Create a comprehensive schema that includes all related nodes
        comprehensive_schema = relevant_schema.copy()
        # Add schema information for related nodes
        for node in query_parts["related_nodes"]:
            node_schema = extract_relevant_schema(node, node_properties)
            comprehensive_schema.update(node_schema)
        # # LLM has its own memory, don't need to feed conversation_history again.
        prompt_text = create_enhanced_prompt(standardized_query, comprehensive_schema)
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

        if isinstance(variables, str):
            try:
                variables_dict = json.loads(variables)
            except Exception:
                variables_dict = {}
        else:
            variables_dict = variables
            
        if variables_dict and "filter" not in variables_dict:
            variables = json.dumps({"filter": variables_dict})
        else:
            variables = json.dumps(variables_dict) if variables_dict else "{}"

        with open(f"chat_history/{timestamp}_processed.txt", "w") as f:
            f.write(variables)
        
        return GraphQLResponse(
            query=result.get("query", ""),
            variables=variables,
        )
    except Exception as e:
        print(f"Error in convert_to_graphql: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
    
@app.post("/nested_graphql")
async def convert_to_nested_graphql(user_query: Query):
    """
    Worlflow:
        1. Extract context from user query. For example:
            User query:
                The cohort consists of participants from the INRG consortium who have metastatic tumors. Specifically, these tumors are classified as absent and are located on the skin.
            Context:
                ["INRG", "consortium", "metastatic", "tumors", "absent", "skin"]
        2. Map Context to all schemas needed in nested graphql
            1. query pcdc-schema-prod.json, for example:
                "INRG" -> "consortium"(pcdc-schema-prod.json)
                "Metastatic" -> "tumor_classification"(pcdc-schema-prod.json)
                "Absent" -> "tumor_state"(pcdc-schema-prod.json)
                "Skin" "tumor_site"(pcdc-schema-prod.json)
            2. query gitops.json, for example:
                "INRG" -> "consortium"(pcdc-schema-prod.json) -> ""(gitops.json)
                "Metastatic" -> "tumor_classification"(pcdc-schema-prod.json) -> "tumor_assessments"(gitops.json)
                "Absent" -> "tumor_state"(pcdc-schema-prod.json) -> "tumor_assessments"(gitops.json)
                "Skin" "tumor_site"(pcdc-schema-prod.json) -> "tumor_assessments"(gitops.json)
        3. Feed GraphQL generation code to LLM.
        4. Ask LLM to return nested graphql format(nested graphql control flow).
    """
    llm = ChatOpenAI(
        model="gpt-4o",
        temperature=0,
        api_key=os.getenv("OPENAI_API_KEY")
    )
    # 1. Extract context from user query.
    print(f"user_query: {user_query}")
    context = extract_context_from_user_query(user_query.text)
    print(f"keywords: {context}")

    # 2. Map Context to all schemas needed in nested graphql
    # Use code to generate two query tables(processed_pcdc_schema_prod_file & processed_gitops_file)
    processed_pcdc_schema_prod_file = "../../schema/processed_pcdc_schema_prod.json"
    if not os.path.exists(processed_pcdc_schema_prod_file) or os.path.getsize(processed_pcdc_schema_prod_file) == 0:
        pcdc_schema_prod_file = "../../schema/pcdc-schema-prod-20250114.json"
        processed_pcdc_result = parse_pcdc_schema_prod(pcdc_schema_prod_file)
    
    processed_gitops_file = "../../schema/processed_gitops.json"
    if not os.path.exists(processed_gitops_file) or os.path.getsize(processed_gitops_file) == 0:
        gitops_file = "../../schema/gitops.json"
        processed_gitops_result = parse_gitops(gitops_file)
    
    # 2.1 Query pcdc-schema-prod.json, map schemas in pcdc_schema_prod: ['consortium', 'tumor_classification', 'tumor_state', 'tumor_site']
    with open(processed_pcdc_schema_prod_file, 'r', encoding='utf-8') as f:
        processed_pcdc_schema_prod_dict = json.load(f)
    lowercase_pcdc_dict = {key.lower(): value for key, value in processed_pcdc_schema_prod_dict.items()}
    pcdc_schema_prod_result = []
    for keyword in context:
        pcdc_schema_prod_schema_mapping_result = await query_processed_pcdc_result(lowercase_pcdc_dict, keyword, user_query, llm)
        if pcdc_schema_prod_schema_mapping_result and pcdc_schema_prod_schema_mapping_result not in pcdc_schema_prod_result:
            pcdc_schema_prod_result.append(pcdc_schema_prod_schema_mapping_result)
    print(f"Mapping schemas in pcdc_schema_prod.json: {pcdc_schema_prod_result}")

    # 2.2 query gitops.json, and map context to gitops_file: ["tumor_assessments"].
    with open(processed_gitops_file, 'r', encoding='utf-8') as f:
        processed_gitops_dict = json.load(f)
    lowercase_gitops_dict = {key.lower(): value for key, value in processed_gitops_dict.items()}
    gitops_result = []
    for pcdc_schema in pcdc_schema_prod_result:
        gitops_schema_mapping_result = await query_processed_gitops_result(lowercase_gitops_dict, pcdc_schema, user_query, llm)
        if gitops_schema_mapping_result and gitops_schema_mapping_result not in gitops_result:
            gitops_result.append(gitops_schema_mapping_result)
    print(f"All schema terms: {pcdc_schema_prod_result} \n {gitops_result} \n for user query {user_query}. \n")
    
    # 3. Feed graphql generation code file("../../assets/queries.js"), 让llm从code中识别要生成的graphql的format
    try:
        with open("../../assets/queries.js", 'r', encoding='utf-8') as f:
            queries_js_content = f.read()
        print("Successfully loaded queries.js file")
    except Exception as e:
        print(f"Error loading queries.js: {str(e)}")
        queries_js_content = ""
    
    # 4. 给两个实际的nested graphql例子, 让 LLM 基于结果生成最终的nested graphql format
    nested_graphql_examples = [
        {"AND": [{"IN": {"consortium": ["INRG"]}}, {"nested": {"AND": [{"IN": {"tumor_classification": ["Metastatic"]}}, {"IN": {"tumor_state": ["Absent"]}}, {"IN": {"tumor_site": ["Skin"]}}], "path": "tumor_assessments"}}]},
        {"AND": [{"IN": {"consortium": ["NODAL"]}}, {"nested": {"AND": [{"IN": {"bulky_nodal_aggregate": ["No"]}}], "path": "disease_characteristics"}}]}
    ]
    
    # 构建最终的LLM提示
    final_prompt = f"""
    你是一个专业的GraphQL嵌套查询生成器，专门为儿童癌症数据库(PCDC)生成嵌套GraphQL过滤器。

    用户查询: {user_query.text}
    
    从用户查询中提取的PCDC Schema Properties: {pcdc_schema_prod_result}
    对应的GitOps Field Nodes: {gitops_result}
    
    参考以下生成的GraphQL代码作为格式规范:
    {queries_js_content[:1000]}...
    
    参考以下嵌套GraphQL查询示例:
    示例1: {json.dumps(nested_graphql_examples[0], ensure_ascii=False)}
    示例2: {json.dumps(nested_graphql_examples[1], ensure_ascii=False)}
    
    基于以上信息，请生成符合嵌套GraphQL格式的查询过滤器。
    
    规则:
    1. 使用AND逻辑连接多个条件
    2. consortium等主表字段直接使用IN操作符
    3. 需要嵌套查询的字段(如tumor_classification, tumor_state等)放在nested结构中
    4. nested结构必须包含path字段，指向对应的GitOps节点
    5. 从用户查询中推断合适的值(如"INRG", "Metastatic", "Absent", "Skin"等)
    6. 返回标准的JSON格式，不要包含任何解释文字
    
    请生成最终的嵌套GraphQL过滤器:
    """
    
    try:
        # 调用LLM生成嵌套GraphQL查询
        response = llm.invoke(final_prompt)
        response_content = response.content if hasattr(response, 'content') else str(response)
        
        # 尝试解析LLM返回的JSON
        try:
            # 移除可能的markdown格式标记
            clean_response = response_content.strip()
            if clean_response.startswith('```json'):
                clean_response = clean_response[7:-3]
            elif clean_response.startswith('```'):
                clean_response = clean_response[3:-3]
            
            nested_graphql_query = json.loads(clean_response.strip())
            print(f"Generated nested GraphQL: {json.dumps(nested_graphql_query, ensure_ascii=False, indent=2)}")
            
        except json.JSONDecodeError as e:
            print(f"Error parsing LLM response as JSON: {str(e)}")
            print(f"Raw LLM response: {response_content}")
            # 返回一个默认的格式
            nested_graphql_query = {
                "error": "Failed to parse LLM response",
                "raw_response": response_content,
                "pcdc_schemas": pcdc_schema_prod_result,
                "gitops_nodes": gitops_result
            }
        
        # 返回完整结果
        return {
            "user_query": user_query.text,
            "extracted_keywords": context,
            "pcdc_schemas": pcdc_schema_prod_result,
            "gitops_nodes": gitops_result,
            "nested_graphql_filter": nested_graphql_query,
            "success": True
        }
        
    except Exception as e:
        print(f"Error in LLM processing: {str(e)}")
        return {
            "user_query": user_query.text,
            "extracted_keywords": context,
            "pcdc_schemas": pcdc_schema_prod_result,
            "gitops_nodes": gitops_result,
            "error": str(e),
            "success": False
        }


async def execute_graphql_query(
    query: str,
    variables: Optional[Dict[str, Any]] = None,
    token: str = None
) -> Dict[str, Any]:
    """
    Execute GraphQL query via the guppy endpoint
    
    Args:
        query: GraphQL query string
        variables: Optional query variables
        token: Access token (if not provided, will be fetched)
        
    Returns:
        Query results
    """
    if not token:
        token = await generate_access_token()
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }
    payload = {
        "query": query
    }
    if variables:
        payload["variables"] = variables
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(
                GRAPHQL_ENDPOINT,
                headers=headers,
                json=payload
            )
            
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            raise HTTPException(
                status_code=e.response.status_code,
                detail=f"GraphQL query failed: {e.response.text}"
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Failed to execute query: {str(e)}"
            )


@app.post("/query", response_model=GraphQLHttpResponse)
async def run_graphql_query(query_request: GraphQLQuery) -> GraphQLHttpResponse:
    """
    Run GraphQL query via guppy/graphql API
    
    Args:
        query_request: GraphQL query request containing query and optional variables
        
    Returns:
        GraphQLResponse with query results
    """
    try:
        # Execute the query
        result = await execute_graphql_query(
            query=query_request.query,
            variables=query_request.variables,
            token=GUPPY_ACCESS_TOKEN
        )
        
        # Check if there are errors in the response
        if "errors" in result and result["errors"]:
            return GraphQLHttpResponse(
                data=result.get("data"),
                errors=result["errors"],
                success=False,
                message="Query executed with errors"
            )
        
        return GraphQLHttpResponse(
            data=result.get("data"),
            success=True,
            message="Query executed successfully"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Internal server error: {str(e)}"
        )

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