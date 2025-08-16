"""
Custom exception classes for PCDC Chatbot Backend

This module defines specific exception types for different error conditions:
- Catalog loading and parsing errors
- LLM service errors
- Query processing errors
- Configuration errors
"""

from typing import Optional, Dict, Any


class PCDCChatbotError(Exception):
    """Base exception class for PCDC Chatbot errors"""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class ConfigurationError(PCDCChatbotError):
    """Raised when there are configuration-related errors"""
    pass


class CatalogError(PCDCChatbotError):
    """Raised when there are catalog loading or parsing errors"""
    pass


class LLMServiceError(PCDCChatbotError):
    """Raised when LLM service calls fail"""
    
    def __init__(self, message: str, model: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.model = model
        super().__init__(message, details)


class QueryParsingError(PCDCChatbotError):
    """Raised when query parsing fails in Step 1"""
    
    def __init__(self, message: str, query: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.query = query
        super().__init__(message, details)


class FieldMappingError(PCDCChatbotError):
    """Raised when field mapping fails in Step 2"""
    
    def __init__(self, message: str, term: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.term = term
        super().__init__(message, details)


class ConflictResolutionError(PCDCChatbotError):
    """Raised when conflict resolution fails in Step 3"""
    
    def __init__(self, message: str, conflicts: Optional[list] = None, details: Optional[Dict[str, Any]] = None):
        self.conflicts = conflicts or []
        super().__init__(message, details)


class FilterBuildingError(PCDCChatbotError):
    """Raised when GraphQL filter building fails in Step 4"""
    pass


class QueryGenerationError(PCDCChatbotError):
    """Raised when final query generation fails in Step 5"""
    pass


class SessionError(PCDCChatbotError):
    """Raised when session management operations fail"""
    
    def __init__(self, message: str, session_id: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        self.session_id = session_id
        super().__init__(message, details)


class CacheError(PCDCChatbotError):
    """Raised when cache operations fail"""
    pass


class ValidationError(PCDCChatbotError):
    """Raised when data validation fails"""
    
    def __init__(self, message: str, field: Optional[str] = None, value: Optional[Any] = None, details: Optional[Dict[str, Any]] = None):
        self.field = field
        self.value = value
        super().__init__(message, details) 