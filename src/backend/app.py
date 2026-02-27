import os
import time
import uuid
import json
import httpx
from typing import Dict, Any, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Header
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
import jwt

from utils.schema_parser import *
from utils.query_builder import *
from utils.context_manager import session_manager
from utils.prompt_builder import *
from utils.filter_utils import *
from utils.credential_helper import *
from utils.nested_graphql_helper import *

# ======================================================
# ENV + APP
# ======================================================
load_dotenv()
app = FastAPI()

BASE_URL = "https://portal-dev.pedscommons.org"
GRAPHQL_ENDPOINT = f"{BASE_URL}/guppy/graphql"

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret")
JWT_ALGO = "HS256"

# ======================================================
# AUTH
# ======================================================
def require_user(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    token = authorization.replace("Bearer ", "")
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid or expired token")

# ======================================================
# MODELS
# ======================================================
class Query(BaseModel):
    text: str
    session_id: Optional[str] = None

class GraphQLQuery(BaseModel):
    query: str
    variables: Optional[Dict[str, Any]] = None
    use_cached_token: Optional[bool] = True

class GraphQLHttpResponse(BaseModel):
    data: Optional[Dict[str, Any]] = None
    errors: Optional[list] = None
    success: bool
    message: Optional[str] = None

# ======================================================
# FLAT GRAPHQL (unchanged, but authenticated)
# ======================================================
@app.post("/flat_graphql")
async def convert_to_flat_graphql(
    query: Query,
    user=Depends(require_user)
):
    schema_file_path = "../../schema/gitops.json"

    llm = ChatOpenAI(
        model="gpt-3.5-turbo",
        temperature=0,
        api_key=os.getenv("OPENAI_API_KEY")
    )

    node_properties, term_mappings = parse_pcdc_schema(schema_file_path)
    standardized_query = standardize_terms(query.text, term_mappings)
    relevant_schema = extract_relevant_schema(standardized_query, node_properties)
    prompt_text = create_enhanced_prompt(standardized_query, relevant_schema)
    response = llm.invoke(prompt_text)

    result = parse_llm_response(response.content, "Simple query")
    variables = json.dumps(result.get("variables", {}))

    return {
        "query": result.get("query", ""),
        "variables": variables
    }

# ======================================================
# NESTED GRAPHQL (GENERATION ONLY)
# ======================================================
@app.post("/nested_graphql")
async def convert_to_nested_graphql(
    user_query: Query,
    user=Depends(require_user)
):
    llm = ChatOpenAI(
        model="gpt-4o",
        temperature=0,
        api_key=os.getenv("OPENAI_API_KEY")
    )

    context = extract_context_from_user_query(user_query.text)

    processed_pcdc_schema_prod_file = "../../schema/processed_pcdc_schema_prod.json"
    processed_gitops_file = "../../schema/processed_gitops.json"

    with open(processed_pcdc_schema_prod_file, "r") as f:
        pcdc_dict = json.load(f)
    with open(processed_gitops_file, "r") as f:
        gitops_dict = json.load(f)

    pcdc_schemas = []
    for keyword in context:
        result = await query_processed_pcdc_result(
            {k.lower(): v for k, v in pcdc_dict.items()},
            keyword,
            user_query,
            llm
        )
        if result and result not in pcdc_schemas:
            pcdc_schemas.append(result)

    gitops_nodes = []
    for schema in pcdc_schemas:
        result = await query_processed_gitops_result(
            {k.lower(): v for k, v in gitops_dict.items()},
            schema,
            user_query,
            llm
        )
        if result and result not in gitops_nodes:
            gitops_nodes.append(result)

    with open("../../assets/queries.js", "r") as f:
        queries_js_content = f.read()

    nested_graphql_examples = [
        {"AND": [{"IN": {"consortium": ["INRG"]}},
                 {"nested": {"AND": [{"IN": {"tumor_classification": ["Metastatic"]}}],
                             "path": "tumor_assessments"}}]}
    ]

    final_prompt = f"""
User Query: {user_query.text}
PCDC Schemas: {pcdc_schemas}
GitOps Nodes: {gitops_nodes}

Reference Code:
{queries_js_content[:1000]}

Example:
{json.dumps(nested_graphql_examples[0])}

Return ONLY JSON.
"""

    response = llm.invoke(final_prompt)
    clean = response.content.strip().replace("```json", "").replace("```", "")
    nested_filter = json.loads(clean)

    executable = convert_to_executable_nested_graphql(response.content, llm)

    return {
        "user_query": user_query.text,
        "extracted_keywords": context,
        "pcdc_schemas": pcdc_schemas,
        "gitops_nodes": gitops_nodes,
        "nested_graphql_filter": nested_filter,
        "executable_nested_graphql": executable,
        "success": True
    }

# ======================================================
# GRAPHQL EXECUTION (ADMIN ONLY)
# ======================================================
async def execute_graphql_query(
    query: str,
    variables: Optional[Dict[str, Any]],
    token: str
):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    payload = {"query": query, "variables": variables}

    async with httpx.AsyncClient(timeout=30.0) as client:
        res = await client.post(GRAPHQL_ENDPOINT, headers=headers, json=payload)
        res.raise_for_status()
        return res.json()

@app.post("/query", response_model=GraphQLHttpResponse)
async def run_graphql_query(
    query_request: GraphQLQuery,
    user=Depends(require_user)
):
    if user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin role required")

    if "mutation" in query_request.query.lower():
        raise HTTPException(status_code=400, detail="Mutations not allowed")

    token = await generate_access_token()

    result = await execute_graphql_query(
        query_request.query,
        query_request.variables,
        token
    )

    if "errors" in result:
        return GraphQLHttpResponse(
            data=result.get("data"),
            errors=result.get("errors"),
            success=False
        )

    return GraphQLHttpResponse(
        data=result.get("data"),
        success=True
    )

# ======================================================
# SESSION MANAGEMENT
# ======================================================
@app.post("/sessions/create")
async def create_session(user=Depends(require_user)):
    session_id = str(uuid.uuid4())
    session_manager.get_or_create_session(session_id)
    return {"session_id": session_id}

@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str, user=Depends(require_user)):
    session_manager.delete_session(session_id)
    return {"status": "success"}

@app.get("/sessions")
async def list_sessions(user=Depends(require_user)):
    return {"sessions": session_manager.get_all_session_ids()}

# ======================================================
# MAIN
# ======================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)