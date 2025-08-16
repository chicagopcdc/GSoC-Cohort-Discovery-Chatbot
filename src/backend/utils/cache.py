"""
Simple caching utilities for PCDC Chatbot Backend

This module provides lightweight caching for:
- Candidate field search results
- Catalog version and metadata
- LLM response caching (when enabled)
- Session-based temporary storage
"""

import time
import hashlib
from typing import Dict, Any, Optional, List
from collections import OrderedDict
from threading import Lock

from ..core.config import config
from utils.logging import get_logger

logger = get_logger(__name__)


class LRUCache:
    """Thread-safe LRU cache implementation"""
    
    def __init__(self, max_size: int = 1000, ttl: float = 3600.0):
        """
        Initialize LRU cache
        
        Args:
            max_size: Maximum number of items to cache
            ttl: Time-to-live in seconds (0 = no expiration)
        """
        self.max_size = max_size
        self.ttl = ttl
        self.cache: OrderedDict = OrderedDict()
        self.timestamps: Dict[str, float] = {}
        self.lock = Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get item from cache"""
        if not config.ENABLE_CACHING:
            return None
        
        with self.lock:
            if key not in self.cache:
                return None
            
            # Check TTL
            if self.ttl > 0:
                age = time.time() - self.timestamps.get(key, 0)
                if age > self.ttl:
                    self._delete_item(key)
                    return None
            
            # Move to end (most recently used)
            value = self.cache.pop(key)
            self.cache[key] = value
            
            return value
    
    def put(self, key: str, value: Any) -> None:
        """Put item in cache"""
        if not config.ENABLE_CACHING:
            return
        
        with self.lock:
            if key in self.cache:
                # Update existing item
                self.cache.pop(key)
            elif len(self.cache) >= self.max_size:
                # Remove oldest item
                oldest_key = next(iter(self.cache))
                self._delete_item(oldest_key)
            
            # Add new item
            self.cache[key] = value
            self.timestamps[key] = time.time()
    
    def delete(self, key: str) -> bool:
        """Delete item from cache"""
        with self.lock:
            if key in self.cache:
                self._delete_item(key)
                return True
            return False
    
    def clear(self) -> None:
        """Clear all cache items"""
        with self.lock:
            self.cache.clear()
            self.timestamps.clear()
    
    def _delete_item(self, key: str) -> None:
        """Internal method to delete item (assumes lock is held)"""
        self.cache.pop(key, None)
        self.timestamps.pop(key, None)
    
    def size(self) -> int:
        """Get current cache size"""
        with self.lock:
            return len(self.cache)
    
    def stats(self) -> Dict[str, Any]:
        """Get cache statistics"""
        with self.lock:
            expired_count = 0
            if self.ttl > 0:
                current_time = time.time()
                for key, timestamp in self.timestamps.items():
                    if current_time - timestamp > self.ttl:
                        expired_count += 1
            
            return {
                "size": len(self.cache),
                "max_size": self.max_size,
                "ttl": self.ttl,
                "expired_items": expired_count
            }


class CacheManager:
    """Manages different types of caches for the application"""
    
    def __init__(self):
        """Initialize cache manager with different cache instances"""
        # Search result cache (short TTL for dynamic results)
        self.search_cache = LRUCache(max_size=500, ttl=300)  # 5 minutes
        
        # Catalog metadata cache (longer TTL for stable data)
        self.catalog_cache = LRUCache(max_size=100, ttl=3600)  # 1 hour
        
        # LLM response cache (medium TTL for AI responses)
        self.llm_cache = LRUCache(max_size=200, ttl=1800)  # 30 minutes
        
        # Session cache (no TTL, managed by session lifecycle)
        self.session_cache = LRUCache(max_size=1000, ttl=0)
        
        logger.info("Initialized cache manager")
    
    def cache_search_results(self, query_hash: str, results: List[Any]) -> None:
        """Cache search results for a query"""
        self.search_cache.put(query_hash, results)
        logger.debug(f"Cached search results for query hash: {query_hash[:8]}...")
    
    def get_cached_search_results(self, query_hash: str) -> Optional[List[Any]]:
        """Get cached search results for a query"""
        results = self.search_cache.get(query_hash)
        if results is not None:
            logger.debug(f"Cache hit for search query hash: {query_hash[:8]}...")
        return results
    
    def cache_catalog_info(self, info_type: str, data: Any) -> None:
        """Cache catalog metadata"""
        self.catalog_cache.put(info_type, data)
        logger.debug(f"Cached catalog info: {info_type}")
    
    def get_cached_catalog_info(self, info_type: str) -> Optional[Any]:
        """Get cached catalog metadata"""
        return self.catalog_cache.get(info_type)
    
    def cache_llm_response(self, prompt_hash: str, response: Any) -> None:
        """Cache LLM response"""
        self.llm_cache.put(prompt_hash, response)
        logger.debug(f"Cached LLM response for prompt hash: {prompt_hash[:8]}...")
    
    def get_cached_llm_response(self, prompt_hash: str) -> Optional[Any]:
        """Get cached LLM response"""
        response = self.llm_cache.get(prompt_hash)
        if response is not None:
            logger.debug(f"Cache hit for LLM prompt hash: {prompt_hash[:8]}...")
        return response
    
    def cache_session_data(self, session_id: str, data: Any) -> None:
        """Cache session-specific data"""
        self.session_cache.put(session_id, data)
    
    def get_cached_session_data(self, session_id: str) -> Optional[Any]:
        """Get cached session data"""
        return self.session_cache.get(session_id)
    
    def clear_session_cache(self, session_id: str) -> bool:
        """Clear cache for a specific session"""
        return self.session_cache.delete(session_id)
    
    def clear_all_caches(self) -> None:
        """Clear all caches"""
        self.search_cache.clear()
        self.catalog_cache.clear()
        self.llm_cache.clear()
        self.session_cache.clear()
        logger.info("Cleared all caches")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get statistics for all caches"""
        return {
            "search_cache": self.search_cache.stats(),
            "catalog_cache": self.catalog_cache.stats(),
            "llm_cache": self.llm_cache.stats(),
            "session_cache": self.session_cache.stats()
        }


def generate_query_hash(query_text: str, additional_params: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a hash for a query to use as cache key
    
    Args:
        query_text: The query text
        additional_params: Additional parameters to include in hash
        
    Returns:
        SHA256 hash string
    """
    hash_input = query_text.lower().strip()
    
    if additional_params:
        # Sort parameters for consistent hashing
        param_str = "&".join(f"{k}={v}" for k, v in sorted(additional_params.items()))
        hash_input += f"|{param_str}"
    
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


def generate_prompt_hash(prompt: str, model_params: Optional[Dict[str, Any]] = None) -> str:
    """
    Generate a hash for an LLM prompt to use as cache key
    
    Args:
        prompt: The prompt text
        model_params: Model parameters (temperature, etc.)
        
    Returns:
        SHA256 hash string
    """
    hash_input = prompt.strip()
    
    if model_params:
        # Include relevant model parameters in hash
        relevant_params = {
            k: v for k, v in model_params.items() 
            if k in ['model', 'temperature', 'max_tokens']
        }
        if relevant_params:
            param_str = "&".join(f"{k}={v}" for k, v in sorted(relevant_params.items()))
            hash_input += f"|{param_str}"
    
    return hashlib.sha256(hash_input.encode('utf-8')).hexdigest()


# Global cache manager instance
_cache_manager = None

def get_cache_manager() -> CacheManager:
    """Get the global cache manager instance"""
    global _cache_manager
    if _cache_manager is None:
        _cache_manager = CacheManager()
    return _cache_manager 