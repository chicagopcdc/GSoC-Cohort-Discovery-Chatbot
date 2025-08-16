"""
Enhanced Catalog Search Engine for PCDC Chatbot

This module provides enhanced search capabilities using catalog_v6.json
to replace and improve the existing standardize_terms() and extract_relevant_schema() functions.

Integration Points:
- Replaces utils.schema_parser.standardize_terms()
- Enhances utils.schema_parser.extract_relevant_schema()
- Provides structured GraphQL filter generation
- Maintains compatibility with existing app.py workflow
"""

import json
import os
import re
from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


class EnhancedCatalogSearcher:
    """
    Enhanced search engine that uses catalog_v6.json for intelligent query processing
    
    This class provides:
    1. Smart keyword extraction from user queries
    2. Catalog-based search for field mappings
    3. GraphQL filter generation
    4. Schema information extraction
    """
    
    def __init__(self, catalog_path: str = "schema/catalog_v6.json"):
        """
        Initialize the enhanced catalog searcher
        
        Args:
            catalog_path: Path to the catalog_v6.json file
        """
        self.catalog_path = catalog_path
        self.catalog_data = self._load_catalog()
        self.keyword_index = self._build_keyword_index()
        self.node_properties = self._extract_node_properties()
        
    def _load_catalog(self) -> List[Dict]:
        """Load the catalog_v6.json file"""
        # Try different possible paths for the catalog file
        possible_paths = [
            self.catalog_path,
            f"../../{self.catalog_path}",
            f"../{self.catalog_path}",
            os.path.join(os.path.dirname(__file__), "..", "..", "..", self.catalog_path),
            os.path.join(os.path.dirname(__file__), "..", "..", self.catalog_path)
        ]
        
        for path in possible_paths:
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Successfully loaded catalog from {path} with {len(data)} entries")
                    return data
            except FileNotFoundError:
                continue
            except Exception as e:
                logger.error(f"Failed to load catalog from {path}: {e}")
                continue
        
        logger.warning(f"Could not find catalog file at any of these paths: {possible_paths}")
        return []
    
    def _build_keyword_index(self) -> Dict[str, List[Dict]]:
        """
        Build a comprehensive keyword index for fast lookup
        
        Returns:
            Dictionary mapping lowercase keywords to catalog entries
        """
        index = {}
        
        for entry in self.catalog_data:
            # Index by display name
            display_name = entry.get("display_name", "").lower()
            if display_name:
                self._add_to_index(index, display_name, entry)
                
                # Also index individual words from display name
                words = re.findall(r'\b\w+\b', display_name)
                for word in words:
                    if len(word) > 2:  # Skip very short words
                        self._add_to_index(index, word, entry)
            
            # Index by field name
            field = entry.get("field", "").lower()
            if field:
                self._add_to_index(index, field, entry)
            
            # Index by enum values
            enums = entry.get("enums", [])
            if enums:  # Check if enums is not None
                for enum_value in enums:
                    enum_lower = enum_value.lower()
                    self._add_to_index(index, enum_lower, {
                        **entry,
                        "matched_enum": enum_value
                    })
        
        logger.info(f"Built keyword index with {len(index)} entries")
        return index
    
    def _add_to_index(self, index: Dict, key: str, entry: Dict):
        """Helper method to add entry to index"""
        if key not in index:
            index[key] = []
        index[key].append(entry)
    
    def _extract_node_properties(self) -> Dict[str, Dict]:
        """
        Extract node properties in the format expected by existing code
        
        Returns:
            Dictionary compatible with existing node_properties format
        """
        node_properties = {}
        
        for entry in self.catalog_data:
            node = entry.get("node", "subject")
            field = entry.get("field", "")
            
            if not field:
                continue
                
            if node not in node_properties:
                node_properties[node] = {}
            
            # Build field information
            field_info = {
                "description": entry.get("description", ""),
                "type": entry.get("type", "string")
            }
            
            # Add enum values if present
            enums = entry.get("enums", [])
            if enums:
                field_info["enum"] = enums
            
            node_properties[node][field] = field_info
        
        return node_properties
    
    def extract_keywords(self, user_query: str) -> List[str]:
        """
        Extract meaningful keywords from user query
        
        Args:
            user_query: User's natural language query
            
        Returns:
            List of extracted keywords
        """
        # Remove common stop words
        stop_words = {
            'who', 'are', 'is', 'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 
            'of', 'with', 'by', 'from', 'as', 'has', 'have', 'had', 'will', 'would', 'could', 'should',
            'subjects', 'patients', 'cases', 'people', 'individuals', 'show', 'find', 'get', 'query'
        }
        
        # Extract words and clean them
        words = re.findall(r'\b[a-zA-Z]+\b', user_query.lower())
        keywords = [word for word in words if word not in stop_words and len(word) > 2]
        
        logger.debug(f"Extracted keywords from '{user_query}': {keywords}")
        return keywords
    
    def search_keyword(self, keyword: str) -> List[Dict]:
        """
        Search for a keyword in the catalog
        
        Args:
            keyword: Keyword to search for
            
        Returns:
            List of matching catalog entries
        """
        keyword_lower = keyword.lower()
        matches = []
        
        # Direct match
        if keyword_lower in self.keyword_index:
            matches.extend(self.keyword_index[keyword_lower])
        
        # Fuzzy matching for partial matches
        for indexed_key, entries in self.keyword_index.items():
            if (keyword_lower in indexed_key or indexed_key in keyword_lower) and keyword_lower != indexed_key:
                matches.extend(entries)
        
        # Remove duplicates while preserving order
        seen = set()
        unique_matches = []
        for match in matches:
            identifier = f"{match['graphql_path']}_{match.get('matched_enum', '')}"
            if identifier not in seen:
                seen.add(identifier)
                unique_matches.append(match)
        
        return unique_matches
    
    def enhanced_standardize_terms(self, user_query: str) -> Tuple[str, Dict[str, List[Dict]]]:
        """
        Enhanced replacement for utils.schema_parser.standardize_terms()
        
        Args:
            user_query: Original user query
            
        Returns:
            Tuple of (standardized_query, keyword_matches)
        """
        keywords = self.extract_keywords(user_query)
        keyword_matches = {}
        standardized_query = user_query
        
        for keyword in keywords:
            matches = self.search_keyword(keyword)
            if matches:
                keyword_matches[keyword] = matches
                
                # Find the best match for standardization
                best_match = matches[0]
                matched_enum = best_match.get("matched_enum")
                display_name = best_match.get("display_name", "")
                
                if matched_enum:
                    # Replace keyword with standardized term
                    pattern = re.compile(r'\b' + re.escape(keyword) + r'\b', re.IGNORECASE)
                    replacement = f"{matched_enum} ({display_name})"
                    standardized_query = pattern.sub(replacement, standardized_query)
        
        logger.info(f"Standardized '{user_query}' to '{standardized_query}'")
        return standardized_query, keyword_matches
    
    def enhanced_extract_relevant_schema(self, user_query: str, keyword_matches: Dict[str, List[Dict]] = None) -> Dict[str, Dict]:
        """
        Enhanced replacement for utils.schema_parser.extract_relevant_schema()
        
        Args:
            user_query: User query (can be standardized)
            keyword_matches: Pre-computed keyword matches (optional)
            
        Returns:
            Dictionary of relevant schema information
        """
        if keyword_matches is None:
            _, keyword_matches = self.enhanced_standardize_terms(user_query)
        
        relevant_schema = {}
        
        # Extract relevant nodes based on matches
        for keyword, matches in keyword_matches.items():
            for match in matches:
                node = match.get("node", "subject")
                field = match.get("field", "")
                
                if node not in relevant_schema:
                    relevant_schema[node] = {}
                
                if field and node in self.node_properties and field in self.node_properties[node]:
                    relevant_schema[node][field] = self.node_properties[node][field]
        
        # Always include subject node if nothing found
        if not relevant_schema:
            relevant_schema["subject"] = self.node_properties.get("subject", {})
        
        logger.debug(f"Extracted relevant schema for nodes: {list(relevant_schema.keys())}")
        return relevant_schema
    
    def generate_graphql_filter_hints(self, user_query: str) -> Dict[str, Any]:
        """
        Generate GraphQL filter hints that can assist the LLM
        
        Args:
            user_query: User's query
            
        Returns:
            Dictionary containing filter hints and metadata
        """
        standardized_query, keyword_matches = self.enhanced_standardize_terms(user_query)
        relevant_schema = self.enhanced_extract_relevant_schema(user_query, keyword_matches)
        
        # Generate filter structure hints
        filter_hints = []
        nested_hints = {}
        
        for keyword, matches in keyword_matches.items():
            if not matches:
                continue
                
            best_match = matches[0]
            node = best_match.get("node", "subject")
            field = best_match.get("field", "")
            nested_path = best_match.get("nested_path", "")
            matched_enum = best_match.get("matched_enum", keyword.title())
            graphql_path = best_match.get("graphql_path", "")
            
            if not field:
                continue
            
            # Create filter hint
            filter_hint = {
                "field": field,
                "value": matched_enum,
                "operator": "IN",
                "graphql_path": graphql_path
            }
            
            if nested_path and nested_path != "subject":
                # This is a nested field
                if nested_path not in nested_hints:
                    nested_hints[nested_path] = []
                nested_hints[nested_path].append(filter_hint)
            else:
                # Direct field
                filter_hints.append(filter_hint)
        
        return {
            "standardized_query": standardized_query,
            "keyword_matches": keyword_matches,
            "relevant_schema": relevant_schema,
            "filter_hints": filter_hints,
            "nested_hints": nested_hints,
            "confidence_score": self._calculate_confidence(keyword_matches)
        }
    
    def _calculate_confidence(self, keyword_matches: Dict[str, List[Dict]]) -> float:
        """Calculate confidence score for the matches"""
        if not keyword_matches:
            return 0.0
        
        total_keywords = len(keyword_matches)
        matched_keywords = sum(1 for matches in keyword_matches.values() if matches)
        
        return matched_keywords / total_keywords if total_keywords > 0 else 0.0
    
    def get_term_mappings_compatible(self) -> Dict[str, Any]:
        """
        Get term mappings in format compatible with existing code
        
        Returns:
            Dictionary compatible with existing term_mappings format
        """
        term_mappings = {}
        
        for entry in self.catalog_data:
            field = entry.get("field", "")
            enums = entry.get("enums", [])
            
            if field and enums:
                term_mappings[field] = enums
        
        return term_mappings


# Global instance for backward compatibility
_enhanced_searcher = None

def get_enhanced_searcher() -> EnhancedCatalogSearcher:
    """Get global enhanced searcher instance"""
    global _enhanced_searcher
    if _enhanced_searcher is None:
        _enhanced_searcher = EnhancedCatalogSearcher()
    return _enhanced_searcher


# Replacement functions for existing codebase
def enhanced_standardize_terms(user_input: str, term_mappings: Dict = None) -> str:
    """
    Drop-in replacement for utils.schema_parser.standardize_terms()
    
    Args:
        user_input: User input query
        term_mappings: Legacy term mappings (ignored, for compatibility)
        
    Returns:
        Standardized query string
    """
    searcher = get_enhanced_searcher()
    standardized_query, _ = searcher.enhanced_standardize_terms(user_input)
    return standardized_query


def enhanced_extract_relevant_schema(query: str, node_properties: Dict = None) -> Dict[str, Dict]:
    """
    Drop-in replacement for utils.schema_parser.extract_relevant_schema()
    
    Args:
        query: User query
        node_properties: Legacy node properties (ignored, for compatibility)
        
    Returns:
        Relevant schema information
    """
    searcher = get_enhanced_searcher()
    return searcher.enhanced_extract_relevant_schema(query)


def get_enhanced_node_properties() -> Dict[str, Dict]:
    """
    Get enhanced node properties extracted from catalog_v6.json
    
    Returns:
        Node properties compatible with existing code
    """
    searcher = get_enhanced_searcher()
    return searcher.node_properties


def get_enhanced_term_mappings() -> Dict[str, Any]:
    """
    Get enhanced term mappings extracted from catalog_v6.json
    
    Returns:
        Term mappings compatible with existing code
    """
    searcher = get_enhanced_searcher()
    return searcher.get_term_mappings_compatible()


# Testing function
def test_enhanced_search():
    """Test the enhanced search functionality"""
    searcher = EnhancedCatalogSearcher()
    
    test_queries = [
        "Skin Metastasis Absent",
        "Male subjects with brain tumors",
        "Patients with age greater than 10",
        "Show me subjects with bone tumors"
    ]
    
    print("=== Enhanced Catalog Search Test ===")
    
    for query in test_queries:
        print(f"\n🔍 Testing query: '{query}'")
        result = searcher.generate_graphql_filter_hints(query)
        
        print(f"  📝 Standardized: {result['standardized_query']}")
        print(f"  🎯 Confidence: {result['confidence_score']:.2f}")
        print(f"  🏷️  Matched keywords: {list(result['keyword_matches'].keys())}")
        print(f"  📊 Schema nodes: {list(result['relevant_schema'].keys())}")
        
        if result['filter_hints']:
            print(f"  🔧 Filter hints: {len(result['filter_hints'])} direct")
        if result['nested_hints']:
            print(f"  🔗 Nested hints: {list(result['nested_hints'].keys())}")


if __name__ == "__main__":
    test_enhanced_search() 