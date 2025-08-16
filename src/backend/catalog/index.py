"""
Catalog indexing and search for PCDC Chatbot Backend

This module provides efficient search capabilities for catalog fields:
- Keyword-based search with fuzzy matching
- Term frequency indexing for better relevance
- Candidate selection and ranking
- Support for different matching strategies
"""

import re
from typing import Dict, List, Tuple, Set, Optional
from collections import defaultdict, Counter
from difflib import SequenceMatcher

from core.config import config
from core.models import CatalogField, FieldCandidate
from utils.logging import get_logger
from .loader import get_catalog_loader

logger = get_logger(__name__)


class CatalogIndex:
    """Efficient search index for catalog fields"""
    
    def __init__(self):
        """Initialize the catalog index"""
        self.catalog_loader = get_catalog_loader()
        self.fields: List[CatalogField] = []
        self.term_index: Dict[str, List[int]] = defaultdict(list)  # term -> field indices
        self.path_index: Dict[str, int] = {}  # path -> field index
        self.is_built = False
        
        logger.info("Initialized catalog index")
    
    def build_index(self, force_rebuild: bool = False) -> None:
        """
        Build the search index from catalog data
        
        Args:
            force_rebuild: Force rebuild even if already built
        """
        if self.is_built and not force_rebuild:
            return
        
        logger.info("Building catalog search index")
        
        # Load catalog fields
        self.fields = self.catalog_loader.get_fields()
        
        # Clear existing indices
        self.term_index.clear()
        self.path_index.clear()
        
        # Build indices
        for i, field in enumerate(self.fields):
            # Index by path
            self.path_index[field.path] = i
            
            # Index searchable terms
            for term in field.searchable_terms:
                # Index full term
                clean_term = self._clean_term(term)
                if clean_term:
                    self.term_index[clean_term].append(i)
                
                # Index partial terms (for better matching)
                words = self._tokenize_term(clean_term)
                for word in words:
                    if len(word) >= config.MIN_TERM_LENGTH:
                        self.term_index[word].append(i)
        
        self.is_built = True
        logger.info(f"Built index with {len(self.fields)} fields and {len(self.term_index)} terms")
    
    def search(self, query_term: str, max_candidates: Optional[int] = None) -> List[FieldCandidate]:
        """
        Search for fields matching a query term
        
        Args:
            query_term: Term to search for
            max_candidates: Maximum number of candidates to return
            
        Returns:
            List of FieldCandidate objects ranked by relevance
        """
        if not self.is_built:
            self.build_index()
        
        if max_candidates is None:
            max_candidates = config.MAX_CANDIDATES_PER_TERM
        
        logger.debug(f"Searching for term: '{query_term}'")
        
        clean_query = self._clean_term(query_term)
        if not clean_query:
            return []
        
        # Find matching fields using different strategies
        candidates = []
        
        # Strategy 1: Exact match
        candidates.extend(self._exact_match_search(clean_query))
        
        # Strategy 2: Partial match
        candidates.extend(self._partial_match_search(clean_query))
        
        # Strategy 3: Fuzzy match
        candidates.extend(self._fuzzy_match_search(clean_query))
        
        # Remove duplicates and rank
        candidates = self._deduplicate_and_rank(candidates, clean_query)
        
        # Limit results
        candidates = candidates[:max_candidates]
        
        logger.debug(f"Found {len(candidates)} candidates for '{query_term}'")
        return candidates
    
    def _exact_match_search(self, query_term: str) -> List[FieldCandidate]:
        """Find fields with exact term matches"""
        candidates = []
        
        if query_term in self.term_index:
            for field_idx in self.term_index[query_term]:
                field = self.fields[field_idx]
                candidates.append(FieldCandidate(
                    term=query_term,
                    field=field,
                    match_score=1.0,
                    match_reason="Exact term match"
                ))
        
        return candidates
    
    def _partial_match_search(self, query_term: str) -> List[FieldCandidate]:
        """Find fields with partial matches"""
        candidates = []
        query_words = self._tokenize_term(query_term)
        
        # Score fields based on word overlap
        field_scores = defaultdict(list)
        
        for word in query_words:
            if len(word) >= config.MIN_TERM_LENGTH and word in self.term_index:
                for field_idx in self.term_index[word]:
                    field_scores[field_idx].append(word)
        
        # Convert to candidates
        for field_idx, matched_words in field_scores.items():
            if field_idx < len(self.fields):
                field = self.fields[field_idx]
                
                # Calculate score based on word overlap
                score = len(matched_words) / len(query_words)
                if score >= 0.3:  # Minimum threshold
                    candidates.append(FieldCandidate(
                        term=query_term,
                        field=field,
                        match_score=score * 0.8,  # Slight penalty for partial match
                        match_reason=f"Partial match ({len(matched_words)}/{len(query_words)} words)"
                    ))
        
        return candidates
    
    def _fuzzy_match_search(self, query_term: str) -> List[FieldCandidate]:
        """Find fields with fuzzy string matches"""
        candidates = []
        
        # Only do fuzzy matching for longer terms to avoid noise
        if len(query_term) < 3:
            return candidates
        
        for field in self.fields:
            best_score = 0.0
            best_match_term = ""
            
            for search_term in field.searchable_terms:
                similarity = SequenceMatcher(None, query_term.lower(), search_term.lower()).ratio()
                if similarity > best_score:
                    best_score = similarity
                    best_match_term = search_term
            
            # Only include if similarity is above threshold
            if best_score >= config.KEYWORD_MATCH_THRESHOLD:
                candidates.append(FieldCandidate(
                    term=query_term,
                    field=field,
                    match_score=best_score * 0.6,  # Penalty for fuzzy match
                    match_reason=f"Fuzzy match with '{best_match_term}' (similarity: {best_score:.2f})"
                ))
        
        return candidates
    
    def _deduplicate_and_rank(self, candidates: List[FieldCandidate], query_term: str) -> List[FieldCandidate]:
        """Remove duplicates and rank candidates by relevance"""
        # Group by field path
        field_groups = defaultdict(list)
        for candidate in candidates:
            field_groups[candidate.field.path].append(candidate)
        
        # Keep best candidate for each field
        unique_candidates = []
        for path, group in field_groups.items():
            best_candidate = max(group, key=lambda c: c.match_score)
            unique_candidates.append(best_candidate)
        
        # Sort by score (descending)
        unique_candidates.sort(key=lambda c: c.match_score, reverse=True)
        
        return unique_candidates
    
    def _clean_term(self, term: str) -> str:
        """Clean and normalize a search term"""
        if not isinstance(term, str):
            return ""
        
        # Convert to lowercase and strip
        clean = term.lower().strip()
        
        # Remove special characters but keep alphanumeric and spaces
        clean = re.sub(r'[^a-z0-9\s]', '', clean)
        
        # Normalize whitespace
        clean = re.sub(r'\s+', ' ', clean).strip()
        
        return clean
    
    def _tokenize_term(self, term: str) -> List[str]:
        """Split term into individual words"""
        if not term:
            return []
        
        words = term.split()
        return [word for word in words if len(word) >= config.MIN_TERM_LENGTH]
    
    def get_field_by_path(self, path: str) -> Optional[CatalogField]:
        """Get a field by its GraphQL path"""
        if not self.is_built:
            self.build_index()
        
        field_idx = self.path_index.get(path)
        if field_idx is not None and field_idx < len(self.fields):
            return self.fields[field_idx]
        
        return None
    
    def get_all_paths(self) -> List[str]:
        """Get all available field paths"""
        if not self.is_built:
            self.build_index()
        
        return list(self.path_index.keys())
    
    def is_loaded(self) -> bool:
        """Check if the catalog index is loaded and built"""
        return self.is_built and len(self.fields) > 0
    
    def get_entry_count(self) -> int:
        """Get the number of entries in the catalog"""
        return len(self.fields) if self.is_built else 0
    
    def get_stats(self) -> Dict[str, int]:
        """Get index statistics"""
        if not self.is_built:
            self.build_index()
        
        return {
            "total_fields": len(self.fields),
            "indexed_terms": len(self.term_index),
            "paths_indexed": len(self.path_index)
        }


# Global catalog index instance
_catalog_index = None

def get_catalog_index() -> CatalogIndex:
    """Get the global catalog index instance"""
    global _catalog_index
    if _catalog_index is None:
        _catalog_index = CatalogIndex()
        # Auto-build index on first access
        try:
            _catalog_index.build_index()
            logger.info("Auto-built catalog index during initialization")
        except Exception as e:
            logger.error(f"Failed to build catalog index: {e}")
    return _catalog_index 