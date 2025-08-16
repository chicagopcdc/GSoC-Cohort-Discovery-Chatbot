"""
Field validation for PCDC Chatbot Backend

This module provides validation for:
- GraphQL field path validation
- Enumeration value validation and normalization
- Case-insensitive enumeration matching
- Type compatibility checking
"""

import re
from typing import List, Optional, Tuple, Dict, Any, Union

from core.config import config
from core.models import CatalogField, FieldType
from utils.errors import ValidationError
from utils.logging import get_logger
from .loader import get_catalog_loader

logger = get_logger(__name__)


class FieldValidator:
    """Validates field paths and values against catalog definitions"""
    
    def __init__(self):
        """Initialize the field validator"""
        self.catalog_loader = get_catalog_loader()
        logger.info("Initialized field validator")
    
    def validate_field_path(self, path: str) -> bool:
        """
        Validate if a field path exists in the catalog
        
        Args:
            path: GraphQL field path to validate
            
        Returns:
            True if path is valid, False otherwise
        """
        try:
            field = self.catalog_loader.get_field_by_path(path)
            return field is not None
        except Exception as e:
            logger.warning(f"Error validating field path '{path}': {e}")
            return False
    
    def validate_enumeration_value(self, field_path: str, value: str) -> Tuple[bool, Optional[str]]:
        """
        Validate and normalize an enumeration value
        
        Args:
            field_path: GraphQL field path
            value: Value to validate
            
        Returns:
            Tuple of (is_valid, normalized_value)
        """
        try:
            field = self.catalog_loader.get_field_by_path(field_path)
            if not field:
                return False, None
            
            if field.field_type != FieldType.ENUMERATION:
                logger.warning(f"Field '{field_path}' is not an enumeration")
                return False, None
            
            if not field.enum_values:
                return False, None
            
            # Case-insensitive matching
            value_lower = value.lower().strip()
            for enum_value in field.enum_values:
                if enum_value.lower().strip() == value_lower:
                    return True, enum_value  # Return original casing from catalog
            
            return False, None
            
        except Exception as e:
            logger.warning(f"Error validating enum value '{value}' for field '{field_path}': {e}")
            return False, None
    
    def validate_multiple_enumeration_values(self, field_path: str, values: List[str]) -> Tuple[List[str], List[str]]:
        """
        Validate multiple enumeration values
        
        Args:
            field_path: GraphQL field path
            values: List of values to validate
            
        Returns:
            Tuple of (valid_values, invalid_values)
        """
        valid_values = []
        invalid_values = []
        
        for value in values:
            is_valid, normalized_value = self.validate_enumeration_value(field_path, value)
            if is_valid and normalized_value:
                valid_values.append(normalized_value)
            else:
                invalid_values.append(value)
        
        return valid_values, invalid_values
    
    def normalize_enumeration_value(self, field_path: str, value: str) -> Optional[str]:
        """
        Normalize an enumeration value to match catalog casing
        
        Args:
            field_path: GraphQL field path
            value: Value to normalize
            
        Returns:
            Normalized value or None if invalid
        """
        is_valid, normalized = self.validate_enumeration_value(field_path, value)
        return normalized if is_valid else None
    
    def get_valid_enumeration_values(self, field_path: str) -> List[str]:
        """
        Get all valid enumeration values for a field
        
        Args:
            field_path: GraphQL field path
            
        Returns:
            List of valid enumeration values
        """
        try:
            field = self.catalog_loader.get_field_by_path(field_path)
            if field and field.field_type == FieldType.ENUMERATION and field.enum_values:
                return field.enum_values.copy()
            return []
        except Exception as e:
            logger.warning(f"Error getting enum values for field '{field_path}': {e}")
            return []
    
    def suggest_enumeration_values(self, field_path: str, partial_value: str, limit: int = 5) -> List[str]:
        """
        Suggest enumeration values based on partial input
        
        Args:
            field_path: GraphQL field path
            partial_value: Partial value to match against
            limit: Maximum number of suggestions
            
        Returns:
            List of suggested enumeration values
        """
        try:
            valid_values = self.get_valid_enumeration_values(field_path)
            if not valid_values or not partial_value:
                return []
            
            partial_lower = partial_value.lower().strip()
            suggestions = []
            
            # Exact prefix matches first
            for value in valid_values:
                if value.lower().startswith(partial_lower):
                    suggestions.append(value)
            
            # Then fuzzy matches
            if len(suggestions) < limit:
                from difflib import SequenceMatcher
                for value in valid_values:
                    if value not in suggestions:
                        similarity = SequenceMatcher(None, partial_lower, value.lower()).ratio()
                        if similarity >= 0.6:  # Threshold for suggestions
                            suggestions.append(value)
                            if len(suggestions) >= limit:
                                break
            
            return suggestions[:limit]
            
        except Exception as e:
            logger.warning(f"Error suggesting enum values for field '{field_path}': {e}")
            return []
    
    def validate_field_value_type(self, field_path: str, value: Any) -> bool:
        """
        Validate if a value is compatible with the field type
        
        Args:
            field_path: GraphQL field path
            value: Value to validate
            
        Returns:
            True if value type is compatible, False otherwise
        """
        try:
            field = self.catalog_loader.get_field_by_path(field_path)
            if not field:
                return False
            
            if field.field_type == FieldType.STRING:
                return isinstance(value, str)
            elif field.field_type == FieldType.NUMBER:
                return isinstance(value, (int, float))
            elif field.field_type == FieldType.BOOLEAN:
                return isinstance(value, bool)
            elif field.field_type == FieldType.ENUMERATION:
                if isinstance(value, str):
                    is_valid, _ = self.validate_enumeration_value(field_path, value)
                    return is_valid
                elif isinstance(value, list):
                    return all(isinstance(v, str) for v in value)
                return False
            elif field.field_type == FieldType.DATE:
                return isinstance(value, str)  # Expecting ISO date string
            
            return False
            
        except Exception as e:
            logger.warning(f"Error validating value type for field '{field_path}': {e}")
            return False
    
    def validate_graphql_path_syntax(self, path: str) -> bool:
        """
        Validate GraphQL path syntax
        
        Args:
            path: GraphQL field path to validate
            
        Returns:
            True if syntax is valid, False otherwise
        """
        if not path or not isinstance(path, str):
            return False
        
        # Basic GraphQL path pattern: field.subfield.subsubfield
        pattern = r'^[a-zA-Z_][a-zA-Z0-9_]*(\.[a-zA-Z_][a-zA-Z0-9_]*)*$'
        return bool(re.match(pattern, path.strip()))
    
    def get_field_info(self, field_path: str) -> Optional[Dict[str, Any]]:
        """
        Get comprehensive information about a field
        
        Args:
            field_path: GraphQL field path
            
        Returns:
            Dictionary with field information or None if not found
        """
        try:
            field = self.catalog_loader.get_field_by_path(field_path)
            if not field:
                return None
            
            info = {
                "path": field.path,
                "type": field.field_type.value,
                "description": field.description,
                "searchable_terms": field.searchable_terms
            }
            
            if field.field_type == FieldType.ENUMERATION and field.enum_values:
                info["enum_values"] = field.enum_values
                info["enum_count"] = len(field.enum_values)
            
            return info
            
        except Exception as e:
            logger.warning(f"Error getting field info for '{field_path}': {e}")
            return None
    
    def validate_filter_object(self, filter_obj: Dict[str, Any]) -> List[str]:
        """
        Validate a complete filter object
        
        Args:
            filter_obj: Filter object to validate
            
        Returns:
            List of validation error messages (empty if valid)
        """
        errors = []
        
        try:
            self._validate_filter_recursive(filter_obj, errors, "")
        except Exception as e:
            errors.append(f"Validation error: {e}")
        
        return errors
    
    def _validate_filter_recursive(self, obj: Any, errors: List[str], path: str) -> None:
        """Recursively validate filter object structure"""
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                
                # Check for logical operators
                if key in ["AND", "OR", "NOT"]:
                    if key == "NOT" and not isinstance(value, dict):
                        errors.append(f"NOT operator at '{current_path}' must have a dict value")
                    elif key in ["AND", "OR"] and not isinstance(value, list):
                        errors.append(f"{key} operator at '{current_path}' must have a list value")
                    else:
                        self._validate_filter_recursive(value, errors, current_path)
                
                # Check field paths
                elif self.validate_graphql_path_syntax(key):
                    if not self.validate_field_path(key):
                        errors.append(f"Unknown field path: '{key}'")
                    else:
                        # Validate field values
                        if not self.validate_field_value_type(key, value):
                            errors.append(f"Invalid value type for field '{key}': {type(value).__name__}")
                
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                self._validate_filter_recursive(item, errors, f"{path}[{i}]")


# Global field validator instance
_field_validator = None

def get_field_validator() -> FieldValidator:
    """Get the global field validator instance"""
    global _field_validator
    if _field_validator is None:
        _field_validator = FieldValidator()
    return _field_validator 