"""
Configuration management for PCDC Chatbot Backend

This module centralizes all configuration settings including:
- LLM model switches and parameters
- Search thresholds and limits
- File paths and directories
- Feature flags for different processing modes
"""

import os
from typing import Dict, Any, Optional
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


class Config:
    """Centralized configuration management"""
    
    # === LLM Configuration ===
    OPENAI_API_KEY: str = os.getenv("OPENAI_API_KEY", "")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "gpt-3.5-turbo")
    LLM_TEMPERATURE: float = float(os.getenv("LLM_TEMPERATURE", "0"))
    LLM_MAX_TOKENS: int = int(os.getenv("LLM_MAX_TOKENS", "1000"))
    
    # === Feature Switches ===
    ENABLE_LLM_NORMALIZATION: bool = os.getenv("ENABLE_LLM_NORMALIZATION", "true").lower() == "true"
    ENABLE_LLM_DISAMBIGUATION: bool = os.getenv("ENABLE_LLM_DISAMBIGUATION", "true").lower() == "true"
    ENABLE_CACHING: bool = os.getenv("ENABLE_CACHING", "true").lower() == "true"
    ENABLE_LANGFUSE_TRACKING: bool = os.getenv("ENABLE_LANGFUSE_TRACKING", "false").lower() == "true"
    
    # === Search Thresholds ===
    KEYWORD_MATCH_THRESHOLD: float = float(os.getenv("KEYWORD_MATCH_THRESHOLD", "0.8"))
    MAX_CANDIDATES_PER_TERM: int = int(os.getenv("MAX_CANDIDATES_PER_TERM", "5"))
    MIN_TERM_LENGTH: int = int(os.getenv("MIN_TERM_LENGTH", "2"))
    
    # === File Paths ===
    PROJECT_ROOT: Path = Path(__file__).parent.parent.parent.parent
    CATALOG_PATH: str = os.getenv("CATALOG_PATH", str(PROJECT_ROOT / "schema" / "catalog_v6.json"))
    CHAT_HISTORY_DIR: str = os.getenv("CHAT_HISTORY_DIR", str(PROJECT_ROOT / "src" / "backend" / "chat_history"))
    LOG_DIR: str = os.getenv("LOG_DIR", str(PROJECT_ROOT / "logs"))
    
    # === Query Configuration ===
    DEFAULT_QUERY_LIMIT: int = int(os.getenv("DEFAULT_QUERY_LIMIT", "100"))
    
    # === API Configuration ===
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8000"))
    CORS_ORIGINS: list = os.getenv("CORS_ORIGINS", "http://localhost:3000,http://localhost:8082").split(",")
    
    # === Langfuse Configuration ===
    LANGFUSE_SECRET_KEY: str = os.getenv("LANGFUSE_SECRET_KEY", "")
    LANGFUSE_PUBLIC_KEY: str = os.getenv("LANGFUSE_PUBLIC_KEY", "")
    LANGFUSE_HOST: str = os.getenv("LANGFUSE_HOST", "")
    
    @classmethod
    def get_llm_config(cls) -> Dict[str, Any]:
        """Get LLM configuration parameters"""
        return {
            "model": cls.LLM_MODEL,
            "temperature": cls.LLM_TEMPERATURE,
            "max_tokens": cls.LLM_MAX_TOKENS,
            "api_key": cls.OPENAI_API_KEY
        }
    
    @classmethod
    def get_search_config(cls) -> Dict[str, Any]:
        """Get search configuration parameters"""
        return {
            "keyword_threshold": cls.KEYWORD_MATCH_THRESHOLD,
            "max_candidates": cls.MAX_CANDIDATES_PER_TERM,
            "min_term_length": cls.MIN_TERM_LENGTH
        }
    
    @classmethod
    def ensure_directories(cls) -> None:
        """Create necessary directories if they don't exist"""
        os.makedirs(cls.CHAT_HISTORY_DIR, exist_ok=True)
        os.makedirs(cls.LOG_DIR, exist_ok=True)
    
    @classmethod
    def validate_config(cls) -> bool:
        """Validate essential configuration settings"""
        if not cls.OPENAI_API_KEY and (cls.ENABLE_LLM_NORMALIZATION or cls.ENABLE_LLM_DISAMBIGUATION):
            raise ValueError("OpenAI API key is required when LLM features are enabled")
        
        if not os.path.exists(cls.CATALOG_PATH):
            raise ValueError(f"Catalog file not found at: {cls.CATALOG_PATH}")
        
        return True


# Global configuration instance
config = Config() 