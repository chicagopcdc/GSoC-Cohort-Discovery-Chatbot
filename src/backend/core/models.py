"""
Unified data models for PCDC Chatbot Backend

This module defines all Pydantic models used throughout the pipeline:
- Input/output models for each pipeline step
- API request/response models
- Internal data structures for processing
"""

from typing import Dict, List, Any, Optional, Union
from pydantic import BaseModel, Field, validator
from enum import Enum


class LogicOperator(str, Enum):
    """Logical operators for query combination"""
    AND = "AND"
    OR = "OR"
    NOT = "NOT"


class FieldType(str, Enum):
    """Types of catalog fields"""
    ENUMERATION = "enumeration"
    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"


class QueryRequest(BaseModel):
    """API request model for natural language queries"""
    text: str = Field(..., description="Natural language query text")
    session_id: Optional[str] = Field(None, description="Session identifier for conversation tracking")
    
    @validator('text')
    def text_must_not_be_empty(cls, v):
        if not v or not v.strip():
            raise ValueError('Query text cannot be empty')
        return v.strip()


class ParsedTerm(BaseModel):
    """A single parsed term from user query"""
    original: str = Field(..., description="Original term as it appears in query")
    normalized: str = Field(..., description="Normalized/cleaned version of the term")
    position: int = Field(..., description="Position in the original query")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="Confidence score for the extraction")


class ParsedQuery(BaseModel):
    """Result of Step 1: Parse user query"""
    terms: List[ParsedTerm] = Field(..., description="Extracted terms from the query")
    logic: LogicOperator = Field(LogicOperator.AND, description="Logical operator to combine terms")
    raw_query: str = Field(..., description="Original user query")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="Overall parsing confidence")


class CatalogField(BaseModel):
    """A field definition from the catalog"""
    path: str = Field(..., description="GraphQL path for the field")
    field_type: FieldType = Field(..., description="Type of the field")
    enum_values: Optional[List[str]] = Field(None, description="Valid enumeration values if applicable")
    description: Optional[str] = Field(None, description="Human-readable description")
    searchable_terms: List[str] = Field([], description="Terms that can match this field")


class FieldCandidate(BaseModel):
    """A candidate field match for a parsed term"""
    term: str = Field(..., description="Original term that matched")
    field: CatalogField = Field(..., description="Matching catalog field")
    match_score: float = Field(0.0, ge=0.0, le=1.0, description="Match confidence score")
    match_reason: str = Field("", description="Explanation of why this field matched")


class FieldMatches(BaseModel):
    """Result of Step 2: Find matching fields"""
    candidates: List[FieldCandidate] = Field(..., description="All field candidates found")
    unmatched_terms: List[str] = Field([], description="Terms that couldn't be matched to any field")


class ResolvedField(BaseModel):
    """A definitively resolved field mapping"""
    term: str = Field(..., description="Original term")
    field_path: str = Field(..., description="Resolved GraphQL field path")
    field_type: FieldType = Field(..., description="Type of the resolved field")
    value: Union[str, List[str]] = Field(..., description="Value(s) to filter on")
    operator: str = Field("eq", description="Filter operator (eq, in, contains, etc.)")
    confidence: float = Field(0.0, ge=0.0, le=1.0, description="Resolution confidence")


class ResolvedFields(BaseModel):
    """Result of Step 3: Resolve field conflicts"""
    resolved: List[ResolvedField] = Field(..., description="Successfully resolved field mappings")
    conflicts: List[Dict[str, Any]] = Field([], description="Unresolved conflicts that need attention")
    warnings: List[str] = Field([], description="Warnings about potential issues")


class GraphQLFilter(BaseModel):
    """A GraphQL filter condition"""
    field: str = Field(..., description="Field path to filter on")
    operator: str = Field(..., description="Filter operator")
    value: Union[str, List[str], int, bool] = Field(..., description="Filter value")


class FilterStructure(BaseModel):
    """Result of Step 4: Build GraphQL filter"""
    filters: List[GraphQLFilter] = Field(..., description="Individual filter conditions")
    logic: LogicOperator = Field(LogicOperator.AND, description="How to combine the filters")
    raw_structure: Dict[str, Any] = Field(..., description="Raw filter object for GraphQL")


class GraphQLQuery(BaseModel):
    """Final GraphQL query and variables"""
    query: str = Field(..., description="GraphQL query string")
    variables: str = Field(..., description="JSON-encoded variables")
    description: Optional[str] = Field(None, description="Human-readable description of the query")


class PipelineResult(BaseModel):
    """Complete result from the 5-step pipeline"""
    session_id: str = Field(..., description="Session identifier")
    parsed_query: ParsedQuery = Field(..., description="Step 1 result")
    field_matches: FieldMatches = Field(..., description="Step 2 result")
    resolved_fields: ResolvedFields = Field(..., description="Step 3 result")
    filter_structure: FilterStructure = Field(..., description="Step 4 result")
    final_query: GraphQLQuery = Field(..., description="Step 5 result")
    processing_time: float = Field(0.0, description="Total processing time in seconds")
    trace_id: Optional[str] = Field(None, description="Langfuse trace identifier")


class ErrorResponse(BaseModel):
    """Standardized error response model"""
    error: str = Field(..., description="Error type or category")
    message: str = Field(..., description="Human-readable error message")
    details: Optional[Dict[str, Any]] = Field(None, description="Additional error details")
    trace_id: Optional[str] = Field(None, description="Trace identifier for debugging")


class SessionInfo(BaseModel):
    """Session information model"""
    session_id: str = Field(..., description="Session identifier")
    created_at: str = Field(..., description="Session creation timestamp")
    message_count: int = Field(0, description="Number of messages in session")
    last_activity: str = Field(..., description="Last activity timestamp")


class HealthStatus(BaseModel):
    """Health check response model"""
    status: str = Field(..., description="Overall health status")
    version: str = Field("1.0.0", description="API version")
    catalog_loaded: bool = Field(False, description="Whether catalog is successfully loaded")
    llm_available: bool = Field(False, description="Whether LLM services are available")
    uptime: float = Field(0.0, description="Server uptime in seconds")


class QueryExecutionRequest(BaseModel):
    """Request model for executing GraphQL queries"""
    query: str = Field(..., description="GraphQL query string")
    variables: Optional[dict] = Field(None, description="Variables for the GraphQL query")


class QueryExecutionResponse(BaseModel):
    """Response model for GraphQL query execution"""
    success: bool = Field(..., description="Whether the query executed successfully")
    data: Optional[dict] = Field(None, description="Query result data")
    error: Optional[str] = Field(None, description="Error message if query failed")
    execution_time: float = Field(0.0, description="Query execution time in seconds") 