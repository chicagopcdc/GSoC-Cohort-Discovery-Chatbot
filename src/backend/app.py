"""
PCDC Chatbot Backend API - New Modular Architecture

This FastAPI application provides natural language to GraphQL conversion
using the new 5-step pipeline architecture.
"""

import os
import time
import uuid
from typing import Dict, List, Any
from datetime import datetime

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Import new modular architecture
from core.pipeline import Pipeline
from core.models import QueryRequest, PipelineResult, QueryExecutionRequest, QueryExecutionResponse
from core.config import config
from utils.logging import get_logger, setup_logging
from utils.errors import (
    QueryParsingError, FieldMappingError, ConflictResolutionError,
    FilterBuildingError, QueryGenerationError
)
from services.langfuse_tracker import SimpleTracker

# Load environment variables
load_dotenv()

# Setup logging
setup_logging()
logger = get_logger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="PCDC Chatbot Backend",
    description="Natural language to GraphQL query conversion API - New Architecture",
    version="2.0.0"
)

# Configure CORS for frontend ports
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:8082"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
pipeline = None
tracker = SimpleTracker()
server_start_time = time.time()

# Pydantic models for API
class QueryInput(BaseModel):
    text: str
    session_id: str = None

class HealthResponse(BaseModel):
    status: str
    message: str
    uptime: float
    catalog_loaded: bool

class QueryResponse(BaseModel):
    success: bool
    query: str = ""
    message: str = ""
    session_id: str
    processing_time: float

# Directory setup
def setup_directories():
    """Create necessary directories"""
    directories = ["chat_history", "logs"]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

@app.on_event("startup")
async def startup_event():
    """Initialize application on startup"""
    global pipeline
    
    try:
        setup_directories()
        
        # Initialize the pipeline
        pipeline = Pipeline()
        
        logger.info("PCDC Chatbot Backend (New Architecture) started successfully")
        logger.info(f"Using catalog path: {config.CATALOG_PATH}")
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check endpoint"""
    uptime = time.time() - server_start_time
    
    # Check if catalog is loaded
    catalog_loaded = False
    try:
        if pipeline and pipeline.catalog_index:
            catalog_loaded = pipeline.catalog_index.is_loaded()
    except Exception as e:
        logger.warning(f"Error checking catalog status: {e}")
    
    return HealthResponse(
        status="healthy",
        message="API is running",
        uptime=uptime,
        catalog_loaded=catalog_loaded
    )

@app.post("/convert", response_model=QueryResponse)
async def convert_query(query_input: QueryInput):
    """
    Convert natural language query to GraphQL using the new pipeline
    """
    start_time = time.time()
    session_id = query_input.session_id or str(uuid.uuid4())
    
    logger.info(f"Processing query: '{query_input.text}' (session: {session_id})")
    
    # Initialize tracker for this request
    try:
        tracker.initialize(session_id)
        logger.info("✅ Langfuse tracker initialized")
    except Exception as e:
        logger.warning(f"Failed to initialize tracker: {e}")
    
    try:
        # Create request object
        request = QueryRequest(
            text=query_input.text,
            session_id=session_id
        )
        
        # Process through pipeline
        result: PipelineResult = await pipeline.process_query(request)
        
        processing_time = time.time() - start_time
        
        # Check if we have a valid final query
        if result.final_query and result.final_query.query:
            logger.info(f"Successfully processed query in {processing_time:.3f}s")
            
            # Track success
            try:
                tracker.track_query(query_input.text, result.final_query.query, True)
            except Exception as e:
                logger.warning(f"Failed to track query: {e}")
            
            return QueryResponse(
                success=True,
                query=result.final_query.query,
                message="Query converted successfully",
                session_id=session_id,
                processing_time=processing_time
            )
        else:
            error_msg = "Failed to generate GraphQL query"
            logger.error(f"Pipeline failed: {error_msg}")
            
            # Track failure
            try:
                tracker.track_query(query_input.text, "", False, error_msg)
            except Exception as e:
                logger.warning(f"Failed to track error: {e}")
            
            return QueryResponse(
                success=False,
                message=f"Failed to convert query: {error_msg}",
                session_id=session_id,
                processing_time=processing_time
            )
            
    except Exception as e:
        processing_time = time.time() - start_time
        logger.error(f"Error in convert endpoint: {e}")
        
        # Track error
        try:
            tracker.track_query(query_input.text, "", False, str(e))
        except Exception as track_e:
            logger.warning(f"Failed to track error: {track_e}")
        
        return QueryResponse(
            success=False,
            message=f"Internal server error: {str(e)}",
            session_id=session_id,
            processing_time=processing_time
        )

@app.post("/query", response_model=QueryExecutionResponse)
async def execute_query(request: QueryExecutionRequest):
    """
    Execute a GraphQL query against the PCDC API
    """
    import httpx
    import json
    start_time = time.time()
    
    logger.info(f"Executing GraphQL query: {request.query[:100]}...")
    
    try:
        # Prepare the GraphQL request payload
        payload = {
            "query": request.query
        }
        
        # Add variables if provided
        if request.variables:
            payload["variables"] = request.variables
        
        # Execute the GraphQL query
        async with httpx.AsyncClient() as client:
            response = await client.post(
                "https://pcdcweb.phenome.dev/gql/v1/query",
                json=payload,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json"
                },
                timeout=30.0
            )
            
            response.raise_for_status()
            result_data = response.json()
        
        execution_time = time.time() - start_time
        
        # Check if the GraphQL response contains errors
        if "errors" in result_data:
            error_messages = [error.get("message", str(error)) for error in result_data["errors"]]
            error_msg = "; ".join(error_messages)
            
            return QueryExecutionResponse(
                success=False,
                data=result_data,
                error=f"GraphQL errors: {error_msg}",
                execution_time=execution_time
            )
        
        # Success case
        return QueryExecutionResponse(
            success=True,
            data=result_data.get("data", {}),
            execution_time=execution_time
        )
        
    except httpx.HTTPStatusError as e:
        execution_time = time.time() - start_time
        error_msg = f"HTTP {e.response.status_code}: {e.response.text}"
        logger.error(f"GraphQL query failed with HTTP error: {error_msg}")
        
        return QueryExecutionResponse(
            success=False,
            error=error_msg,
            execution_time=execution_time
        )
        
    except httpx.TimeoutException:
        execution_time = time.time() - start_time
        error_msg = "Query execution timed out"
        logger.error(error_msg)
        
        return QueryExecutionResponse(
            success=False,
            error=error_msg,
            execution_time=execution_time
        )
        
    except Exception as e:
        execution_time = time.time() - start_time
        error_msg = f"Unexpected error: {str(e)}"
        logger.error(f"Error executing GraphQL query: {error_msg}")
        
        return QueryExecutionResponse(
            success=False,
            error=error_msg,
            execution_time=execution_time
        )

@app.get("/")
async def root():
    """Root endpoint with API information"""
    return {
        "message": "PCDC Chatbot Backend API - New Architecture",
        "version": "2.0.0",
        "endpoints": {
            "health": "/health",
            "convert": "/convert",
            "query": "/query"
        },
        "uptime": time.time() - server_start_time
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=8000,
        log_level="info",
        reload=False
    ) 