"""
Catalog loader for PCDC Chatbot Backend

This module handles loading and parsing of the catalog_v6.json file:
- Safe loading with error handling
- Data validation and structure verification
- Caching for performance
- Hot reloading capability
"""

import json
import os
from typing import Dict, List, Any, Optional
from pathlib import Path
from datetime import datetime

from core.config import config
from core.models import CatalogField, FieldType
from utils.errors import CatalogError
from utils.logging import get_logger

logger = get_logger(__name__)


class CatalogLoader:
    """Loads and manages the catalog data from catalog_v6.json"""
    
    def __init__(self, catalog_path: Optional[str] = None):
        """
        Initialize the catalog loader
        
        Args:
            catalog_path: Path to catalog file (defaults to config setting)
        """
        self.catalog_path = catalog_path or config.CATALOG_PATH
        self.catalog_data: Optional[List[Dict[str, Any]]] = None
        self.last_loaded: Optional[datetime] = None
        self.file_mtime: Optional[float] = None
        
        logger.info(f"Initialized catalog loader with path: {self.catalog_path}")
    
    def load_catalog(self, force_reload: bool = False) -> List[Dict[str, Any]]:
        """
        Load catalog data from JSON file
        
        Args:
            force_reload: Force reload even if already cached
            
        Returns:
            List of catalog entries
            
        Raises:
            CatalogError: If loading or parsing fails
        """
        try:
            # Check if we need to reload
            if not force_reload and self._is_cache_valid():
                logger.debug("Using cached catalog data")
                return self.catalog_data
            
            logger.info(f"Loading catalog from: {self.catalog_path}")
            
            # Verify file exists
            if not os.path.exists(self.catalog_path):
                raise CatalogError(f"Catalog file not found: {self.catalog_path}")
            
            # Load and parse JSON
            with open(self.catalog_path, 'r', encoding='utf-8') as file:
                raw_data = json.load(file)
            
            # Validate structure
            if not isinstance(raw_data, list):
                raise CatalogError("Catalog file must contain a JSON array")
            
            # Store loaded data
            self.catalog_data = raw_data
            self.last_loaded = datetime.now()
            self.file_mtime = os.path.getmtime(self.catalog_path)
            
            logger.info(f"Successfully loaded catalog with {len(raw_data)} entries")
            return self.catalog_data
            
        except json.JSONDecodeError as e:
            raise CatalogError(f"Invalid JSON in catalog file: {e}")
        except Exception as e:
            raise CatalogError(f"Failed to load catalog: {e}")
    
    def _is_cache_valid(self) -> bool:
        """Check if cached data is still valid"""
        if self.catalog_data is None or self.file_mtime is None:
            return False
        
        try:
            current_mtime = os.path.getmtime(self.catalog_path)
            return current_mtime == self.file_mtime
        except OSError:
            return False
    
    def get_fields(self) -> List[CatalogField]:
        """
        Get all catalog fields as structured objects
        
        Returns:
            List of CatalogField objects
        """
        if self.catalog_data is None:
            self.load_catalog()
        
        fields = []
        for entry in self.catalog_data:
            try:
                field = self._parse_catalog_entry(entry)
                if field:
                    fields.append(field)
            except Exception as e:
                logger.warning(f"Failed to parse catalog entry: {e}")
                continue
        
        return fields
    
    def _parse_catalog_entry(self, entry: Dict[str, Any]) -> Optional[CatalogField]:
        """
        Parse a single catalog entry into a CatalogField object
        
        Args:
            entry: Raw catalog entry from JSON
            
        Returns:
            CatalogField object or None if parsing fails
        """
        try:
            # Extract required fields
            path = entry.get('field_path', '')
            if not path:
                return None
            
            # Determine field type
            field_type = self._determine_field_type(entry)
            
            # Extract enum values if applicable
            enum_values = None
            if field_type == FieldType.ENUMERATION:
                enum_values = entry.get('enum_values', [])
                if isinstance(enum_values, str):
                    enum_values = [enum_values]
            
            # Extract searchable terms
            searchable_terms = []
            
            # Add terms from various sources
            if 'searchable_terms' in entry:
                searchable_terms.extend(entry['searchable_terms'])
            
            if 'field_name' in entry:
                searchable_terms.append(entry['field_name'])
            
            if 'description' in entry:
                searchable_terms.append(entry['description'])
            
            if enum_values:
                searchable_terms.extend(enum_values)
            
            # Clean and deduplicate terms
            searchable_terms = list(set([
                term.lower().strip() 
                for term in searchable_terms 
                if isinstance(term, str) and term.strip()
            ]))
            
            return CatalogField(
                path=path,
                field_type=field_type,
                enum_values=enum_values,
                description=entry.get('description', ''),
                searchable_terms=searchable_terms
            )
            
        except Exception as e:
            logger.warning(f"Error parsing catalog entry: {e}")
            return None
    
    def _determine_field_type(self, entry: Dict[str, Any]) -> FieldType:
        """
        Determine the field type from catalog entry
        
        Args:
            entry: Catalog entry
            
        Returns:
            FieldType enum value
        """
        # Check explicit type field
        if 'type' in entry:
            type_str = entry['type'].lower()
            if type_str in ['enumeration', 'enum']:
                return FieldType.ENUMERATION
            elif type_str in ['string', 'text']:
                return FieldType.STRING
            elif type_str in ['number', 'int', 'integer', 'float']:
                return FieldType.NUMBER
            elif type_str in ['boolean', 'bool']:
                return FieldType.BOOLEAN
            elif type_str in ['date', 'datetime']:
                return FieldType.DATE
        
        # Infer from enum_values presence
        if 'enum_values' in entry and entry['enum_values']:
            return FieldType.ENUMERATION
        
        # Default to string
        return FieldType.STRING
    
    def get_field_by_path(self, path: str) -> Optional[CatalogField]:
        """
        Get a specific field by its GraphQL path
        
        Args:
            path: GraphQL field path
            
        Returns:
            CatalogField object or None if not found
        """
        fields = self.get_fields()
        for field in fields:
            if field.path == path:
                return field
        return None
    
    def search_fields_by_term(self, term: str, limit: int = 10) -> List[CatalogField]:
        """
        Search for fields that match a given term
        
        Args:
            term: Search term
            limit: Maximum number of results
            
        Returns:
            List of matching CatalogField objects
        """
        if not term:
            return []
        
        term_lower = term.lower().strip()
        fields = self.get_fields()
        matches = []
        
        for field in fields:
            if any(term_lower in search_term for search_term in field.searchable_terms):
                matches.append(field)
                
        return matches[:limit]
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the loaded catalog
        
        Returns:
            Dictionary with catalog statistics
        """
        if self.catalog_data is None:
            self.load_catalog()
        
        fields = self.get_fields()
        
        type_counts = {}
        for field in fields:
            type_counts[field.field_type.value] = type_counts.get(field.field_type.value, 0) + 1
        
        return {
            "total_entries": len(self.catalog_data),
            "valid_fields": len(fields),
            "field_types": type_counts,
            "last_loaded": self.last_loaded.isoformat() if self.last_loaded else None,
            "file_path": self.catalog_path
        }


# Global catalog loader instance
_catalog_loader = None

def get_catalog_loader() -> CatalogLoader:
    """Get the global catalog loader instance"""
    global _catalog_loader
    if _catalog_loader is None:
        _catalog_loader = CatalogLoader()
    return _catalog_loader 