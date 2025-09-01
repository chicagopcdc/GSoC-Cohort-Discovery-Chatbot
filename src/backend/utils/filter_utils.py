"""
filter_utils.py - GraphQL filter conversion utilities

This module provides tools for converting frontend FilterState objects to GraphQL filter format,
and converting GraphQL filters back to FilterState objects.
Supports automatic field type reading from PCDC schema for dynamic handling of different filter types.

Main functions:
- getGQLFilter: Convert FilterState to GraphQL filter
- getFilterState: Convert GraphQL filter to FilterState
- SchemaTypeHandler: Auto-handle different field types based on schema
"""

import json
from typing import Dict, List, Any, Optional, Union, Tuple
import logging
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Filter type constants
class FILTER_TYPE:
    """Filter type constants"""
    COMPOSED = 'COMPOSED'
    ANCHORED = 'ANCHORED'
    STANDARD = 'STANDARD'
    OPTION = 'OPTION'
    RANGE = 'RANGE'

# Type definitions
FilterState = Dict[str, Any]
GqlFilter = Dict[str, Any]
GqlSimpleFilter = Dict[str, Any]
GqlNestedFilter = Dict[str, Dict[str, Any]]

class SchemaTypeHandler:
    """
    Handle filter conversion for different field types based on PCDC schema
    """
    
    def __init__(self, node_properties: Dict = None):
        """
        Initialize SchemaTypeHandler
        
        Args:
            node_properties: Node property info from schema_parser.parse_pcdc_schema
        """
        self.node_properties = node_properties or {}
        self._field_type_cache = {}  # Cache field type info
    
    def get_field_type_info(self, field_path: str) -> Tuple[str, Dict]:
        """
        Get field type information
        
        Args:
            field_path: Field path, may contain dots for nested fields
            
        Returns:
            Tuple of (field_type, field_details)
        """
        # Check cache
        if field_path in self._field_type_cache:
            return self._field_type_cache[field_path]
        
        # Parse field path
        parts = field_path.split('.')
        node_type = parts[0] if len(parts) > 1 else 'subject'
        field_name = parts[-1]
        
        # Get node properties
        node_info = self.node_properties.get(node_type, {})
        field_info = node_info.get(field_name, {})
        
        # Determine field type
        field_type = 'unknown'
        if 'enum' in field_info:
            field_type = 'enum'
        elif 'type' in field_info:
            type_info = field_info['type']
            if isinstance(type_info, list) and 'number' in type_info:
                field_type = 'number'
            elif isinstance(type_info, list) and 'string' in type_info:
                field_type = 'string'
            else:
                field_type = str(type_info)
        
        # Cache result
        result = (field_type, field_info)
        self._field_type_cache[field_path] = result
        return result
    
    def parse_filter_value(self, field_name: str, filter_values: Dict[str, Any]) -> Optional[GqlSimpleFilter]:
        """
        Parse filter value to GraphQL filter based on field type
        
        Args:
            field_name: Field name
            filter_values: Filter value object
            
        Returns:
            GraphQL simple filter object or None
        """
        # Get field type info
        field_type, field_info = self.get_field_type_info(field_name)
        
        # Handle by filter type
        filter_type = filter_values.get('__type')
        
        if filter_type == FILTER_TYPE.OPTION:
            # Option type filter
            selected_values = filter_values.get('selectedValues', [])
            if selected_values:
                return {'IN': {field_name: selected_values}}
        
        elif filter_type == FILTER_TYPE.RANGE:
            # Range type filter
            lower_bound = filter_values.get('lowerBound')
            upper_bound = filter_values.get('upperBound')
            
            if lower_bound is not None and upper_bound is not None:
                # Both bounds
                return {'AND': [
                    {'GTE': {field_name: lower_bound}},
                    {'LTE': {field_name: upper_bound}}
                ]}
            elif lower_bound is not None:
                # Lower bound only
                return {'GTE': {field_name: lower_bound}}
            elif upper_bound is not None:
                # Upper bound only
                return {'LTE': {field_name: upper_bound}}
        
        # Smart handling for other types based on schema
        if field_type == 'enum' and 'value' in filter_values:
            # Direct enum value
            return {field_name: filter_values['value']}
        elif field_type == 'number' and 'value' in filter_values:
            # Direct number value
            return {field_name: filter_values['value']}
        elif field_type == 'string' and 'value' in filter_values:
            # Direct string value
            return {field_name: filter_values['value']}
        
        # Unrecognized filter type
        logger.debug(f"Failed to parse filter: {field_name}={filter_values}, type={field_type}")
        return None


def parse_anchored_filters(field_name: str, filter_values: Dict[str, Any], combine_mode: str) -> List[Dict[str, Any]]:
    """
    Parse anchored type filters
    
    Args:
        field_name: Field name
        filter_values: Filter value object
        combine_mode: Combine mode (AND/OR)
        
    Returns:
        List of parsed filters
    """
    # Note: This function needs implementation based on actual anchored filter structure
    # Currently returns empty list as placeholder
    logger.warning(f"Anchored filter parsing not fully implemented: {field_name}")
    return []


def parse_simple_filter(field_name: str, filter_values: Dict[str, Any], schema_handler: Optional[SchemaTypeHandler] = None) -> Optional[GqlSimpleFilter]:
    """
    Parse simple filter
    
    Args:
        field_name: Field name
        filter_values: Filter value object
        schema_handler: Schema type handler
        
    Returns:
        GraphQL simple filter object or None
    """
    # Use schema handler for smart parsing if provided
    if schema_handler:
        return schema_handler.parse_filter_value(field_name, filter_values)
    
    # Fallback: only handle OPTION type
    if filter_values.get('__type') == FILTER_TYPE.OPTION:
        return {'IN': {field_name: filter_values.get('selectedValues', [])}}
    
    return None


def getGQLFilter(filter_state: Optional[FilterState], schema_handler: Optional[SchemaTypeHandler] = None) -> Optional[GqlFilter]:
    """
    Convert FilterState object to GraphQL filter format
    
    Args:
        filter_state: FilterState object
        schema_handler: Schema type handler
        
    Returns:
        GraphQL filter object or None
    """
    # Check null values
    if (
        filter_state is None or
        'value' not in filter_state or
        not filter_state['value']
    ):
        return None

    # Get combine mode
    combine_mode = filter_state.get('__combineMode', 'AND')
    
    # Handle composed filters
    if filter_state.get('__type') == FILTER_TYPE.COMPOSED:
        return {combine_mode: [getGQLFilter(fs, schema_handler) for fs in filter_state['value']]}

    # Initialize filter lists
    simple_filters = []
    nested_filters = []
    nested_filter_indices = {}
    nested_filter_index = 0

    # Process each filter condition
    for filter_key, filter_values in filter_state['value'].items():
        # Parse field path
        parts = filter_key.split('.')
        field_str = parts[0]
        nested_field_str = parts[1] if len(parts) > 1 else None
        is_nested_field = nested_field_str is not None
        field_name = nested_field_str if is_nested_field else field_str

        # Handle anchored type filters
        if filter_values.get('__type') == FILTER_TYPE.ANCHORED:
            parsed_anchored_filters = parse_anchored_filters(field_name, filter_values, combine_mode)
            for item in parsed_anchored_filters:
                if 'nested' in item:
                    nested = item['nested']
                    path = nested['path']
                    
                    if path not in nested_filter_indices:
                        nested_filter_indices[path] = nested_filter_index
                        nested_filters.append({
                            'nested': {'path': path, combine_mode: []}
                        })
                        nested_filter_index += 1
                    
                    nested_filters[nested_filter_indices[path]]['nested'][combine_mode].append({'AND': nested['AND']})
        
        # Handle simple filters
        else:
            simple_filter = parse_simple_filter(field_name, filter_values, schema_handler)
            
            if simple_filter is not None:
                if is_nested_field:
                    # Nested field
                    path = field_str
                    
                    if path not in nested_filter_indices:
                        nested_filter_indices[path] = nested_filter_index
                        nested_filters.append({
                            'nested': {'path': path, combine_mode: []}
                        })
                        nested_filter_index += 1
                    
                    nested_filters[nested_filter_indices[path]]['nested'][combine_mode].append(simple_filter)
                else:
                    # Regular field
                    simple_filters.append(simple_filter)

    # Combine all filters
    return {combine_mode: simple_filters + nested_filters} if simple_filters or nested_filters else None


def getFilterState(gql_filter: Optional[GqlFilter]) -> Optional[FilterState]:
    """
    Convert GraphQL filter to FilterState object
    
    Args:
        gql_filter: GraphQL filter object
        
    Returns:
        FilterState object or None
    """
    # Check null values
    if gql_filter is None:
        return None
    
    # Get combine mode
    combinator = list(gql_filter.keys())[0]
    filter_values = gql_filter[combinator]
    
    # Check null values
    if not filter_values:
        return None
    
    # Handle AND/OR combinations
    if combinator in ('AND', 'OR'):
        values = {}
        
        for filter_value in filter_values:
            # Get filter type and value
            value_combinator = list(filter_value.keys())[0]
            value = filter_value[value_combinator]
            
            # Handle IN operator (option type)
            if value_combinator == 'IN':
                option = {}
                
                for field, val in value.items():
                    option[field] = {
                        '__type': FILTER_TYPE.OPTION,
                        'selectedValues': val,
                        'isExclusion': False
                    }
                
                values = {**option, **values}
            
            # Handle GTE/LTE operators (range type)
            elif value_combinator in ('GTE', 'LTE') and isinstance(value, dict):
                for field, val in value.items():
                    if field not in values:
                        values[field] = {
                            '__type': FILTER_TYPE.RANGE,
                            'lowerBound': val if value_combinator == 'GTE' else None,
                            'upperBound': val if value_combinator == 'LTE' else None
                        }
                    else:
                        # Update existing range
                        if value_combinator == 'GTE':
                            values[field]['lowerBound'] = val
                        else:
                            values[field]['upperBound'] = val
            
            # Handle nested filters
            elif value_combinator == 'nested' and isinstance(value, dict):
                path = value.get('path')
                nested_combinator = 'AND'  # Default to AND
                
                # Find actual combinator used
                for key in value:
                    if key in ('AND', 'OR'):
                        nested_combinator = key
                        break
                
                # Process each nested filter condition
                for nested_filter in value.get(nested_combinator, []):
                    nested_value_combinator = list(nested_filter.keys())[0]
                    nested_value = nested_filter[nested_value_combinator]
                    
                    # Handle nested IN operator
                    if nested_value_combinator == 'IN':
                        for field, val in nested_value.items():
                            nested_field = f"{path}.{field}"
                            values[nested_field] = {
                                '__type': FILTER_TYPE.OPTION,
                                'selectedValues': val,
                                'isExclusion': False
                            }
            
            # Handle other filter types
            # Can be extended for more types as needed
        
        # Return FilterState object
        return {
            '__combineMode': combinator,
            '__type': FILTER_TYPE.STANDARD,
            'value': values
        }
    
    return None


def parse_llm_response(response_content: str, query_type: str = "") -> Dict[str, Any]:
    """
    Parse LLM response content to extract query and variables
    
    Args:
        response_content: Raw LLM response content
        query_type: Query type identifier (for logging)
        
    Returns:
        Dictionary containing query and variables
    """
    try:
        # Try direct JSON parsing
        result = json.loads(response_content)
        logger.info(f"{query_type} - Successfully parsed JSON directly")
        return result
    except Exception as e:
        logger.warning(f"{query_type} - Failed to parse JSON: {str(e)}")
        
        # Try fixing incomplete JSON
        content = response_content.strip()
        if not content.endswith('}'):
            content += '}'
        
        try:
            # Try parsing fixed content
            result = json.loads(content)
            logger.info(f"{query_type} - Fixed and parsed JSON successfully")
            return result
        except:
            # Manual extraction if still failing
            logger.warning(f"{query_type} - Still failed to parse, extracting manually")
            result = {
                "query": "",
                "variables": "{}"
            }
            
            # Try extracting query and variables from content
            try:
                # Find "query": "..." pattern
                query_match = re.search(r'"query":\s*"([^"]*)"', content)
                if query_match:
                    result["query"] = query_match.group(1)
                
                # Find "variables": {...} pattern (handle nested objects)
                variables_start = content.find('"variables":')
                if variables_start != -1:
                    # Find opening brace after "variables"
                    brace_start = content.find('{', variables_start)
                    if brace_start != -1:
                        # Count braces to find matching closing brace
                        brace_count = 0
                        end_pos = brace_start
                        for i, char in enumerate(content[brace_start:], brace_start):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    end_pos = i
                                    break
                        
                        if brace_count == 0:  # Found matching closing brace
                            variables_str = content[brace_start:end_pos + 1]
                            result["variables"] = variables_str
                            
            except Exception as extract_error:
                logger.error(f"{query_type} - Failed to extract: {str(extract_error)}")
            
            logger.info(f"{query_type} - Final extracted result: {result}")
            return result


# Main exported functions
__all__ = ['getGQLFilter', 'getFilterState', 'SchemaTypeHandler', 'FILTER_TYPE', 'parse_llm_response'] 