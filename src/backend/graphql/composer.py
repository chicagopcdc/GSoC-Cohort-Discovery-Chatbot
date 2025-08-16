"""
Filter Composer - Step 4 of Pipeline

Responsible for composing GraphQL filter structures from resolved fields
with proper logical operators and nesting.
"""

from typing import Dict, List, Any, Union
from core.models import ResolvedFields, FilterStructure, GraphQLFilter, LogicOperator, FieldType
from core.config import config
from utils.logging import get_logger
from utils.errors import FilterBuildingError

logger = get_logger(__name__)


class FilterComposer:
    """Composes GraphQL filter structures from resolved field mappings"""
    
    def __init__(self):
        """Initialize the filter composer"""
        logger.debug("Initialized FilterComposer")
    
    def compose_filter(self, resolved_fields: ResolvedFields, logic: LogicOperator) -> FilterStructure:
        """
        Compose GraphQL filter structure from resolved fields
        
        Args:
            resolved_fields: Resolved field mappings from Step 3
            logic: Logical operator to combine filters
            
        Returns:
            FilterStructure ready for GraphQL generation
            
        Raises:
            FilterBuildingError: If filter building fails
        """
        try:
            if not resolved_fields.resolved:
                # Return empty filter for no resolved fields
                return FilterStructure(
                    filters=[],
                    logic=logic,
                    raw_structure={}
                )
            
            # Create individual filters
            filters = []
            for resolved_field in resolved_fields.resolved:
                filter_obj = self._create_filter(resolved_field)
                if filter_obj:
                    filters.append(filter_obj)
            
            # Build the raw filter structure
            raw_structure = self._build_raw_structure(filters, logic)
            
            filter_structure = FilterStructure(
                filters=filters,
                logic=logic,
                raw_structure=raw_structure
            )
            
            logger.debug(f"Composed filter with {len(filters)} conditions using {logic} logic")
            return filter_structure
            
        except Exception as e:
            logger.error(f"Filter composition failed: {e}")
            raise FilterBuildingError(f"Failed to compose filter: {e}")
    
    def _create_filter(self, resolved_field) -> GraphQLFilter:
        """Create a GraphQL filter from a resolved field"""
        
        # Convert value based on field type and operator
        value = self._convert_filter_value(resolved_field)
        
        # Validate the filter value
        if not self._validate_filter_value(resolved_field, value):
            logger.warning(f"Invalid filter value for field {resolved_field.field_path}: {value}")
            return None
        
        return GraphQLFilter(
            field=resolved_field.field_path,
            operator=resolved_field.operator,
            value=value
        )
    
    def _convert_filter_value(self, resolved_field) -> Union[str, List[str], int, bool]:
        """Convert resolved field value to appropriate GraphQL filter value"""
        
        value = resolved_field.value
        field_type = resolved_field.field_type
        operator = resolved_field.operator
        
        # Handle different field types
        if field_type == FieldType.ENUMERATION:
            if operator == "in" and isinstance(value, str):
                # Convert single value to list for 'in' operator
                return [value]
            elif operator == "eq":
                return str(value)
            else:
                return value
        
        elif field_type == FieldType.STRING:
            if operator == "contains":
                return str(value).strip()
            elif operator == "in" and isinstance(value, str):
                return [value]
            else:
                return str(value)
        
        elif field_type == FieldType.NUMBER:
            try:
                # Try to convert to number if it looks like one
                if isinstance(value, str) and value.replace(".", "").replace("-", "").isdigit():
                    return float(value) if "." in value else int(value)
                elif isinstance(value, (int, float)):
                    return value
                else:
                    # Keep as string if not numeric
                    return str(value)
            except ValueError:
                return str(value)
        
        elif field_type == FieldType.BOOLEAN:
            if isinstance(value, bool):
                return value
            elif isinstance(value, str):
                return value.lower() in ["true", "yes", "1", "y"]
            else:
                return bool(value)
        
        elif field_type == FieldType.DATE:
            # Keep date values as strings for now
            return str(value)
        
        else:
            return str(value)
    
    def _validate_filter_value(self, resolved_field, value) -> bool:
        """Validate that the filter value is appropriate for the field"""
        
        if value is None or (isinstance(value, str) and not value.strip()):
            return False
        
        field_type = resolved_field.field_type
        
        if field_type == FieldType.ENUMERATION:
            # For enumeration fields, we should ideally validate against enum values
            # but we'll be permissive here since the disambiguator should handle this
            return True
        
        elif field_type == FieldType.NUMBER:
            if isinstance(value, (int, float)):
                return True
            elif isinstance(value, str):
                try:
                    float(value)
                    return True
                except ValueError:
                    # Allow string values for number fields (might be ranges, etc.)
                    return len(value.strip()) > 0
        
        elif field_type == FieldType.BOOLEAN:
            return isinstance(value, (bool, str, int))
        
        else:
            return True
    
    def _build_raw_structure(self, filters: List[GraphQLFilter], logic: LogicOperator) -> Dict[str, Any]:
        """Build the raw GraphQL filter structure"""
        
        if not filters:
            return {}
        
        if len(filters) == 1:
            # Single filter - no need for logic operators
            return self._filter_to_dict(filters[0])
        
        # Multiple filters - need to combine with logic operator
        filter_dicts = [self._filter_to_dict(f) for f in filters]
        
        if logic == LogicOperator.AND:
            return {"AND": filter_dicts}
        elif logic == LogicOperator.OR:
            return {"OR": filter_dicts}
        else:
            # Default to AND for unknown operators
            return {"AND": filter_dicts}
    
    def _filter_to_dict(self, filter_obj: GraphQLFilter) -> Dict[str, Any]:
        """Convert a GraphQL filter to dictionary format"""
        
        # Handle nested field paths (e.g., "disease_characteristics.diagnosis")
        field_parts = filter_obj.field.split(".")
        
        if len(field_parts) == 1:
            # Simple field
            return {
                filter_obj.field: {
                    self._graphql_operator(filter_obj.operator): filter_obj.value
                }
            }
        else:
            # Nested field - build nested structure
            result = {}
            current = result
            
            # Build nested structure
            for i, part in enumerate(field_parts[:-1]):
                current[part] = {}
                current = current[part]
            
            # Add the final field with operator and value
            final_field = field_parts[-1]
            current[final_field] = {
                self._graphql_operator(filter_obj.operator): filter_obj.value
            }
            
            return result
    
    def _graphql_operator(self, operator: str) -> str:
        """Convert internal operator to GraphQL operator"""
        
        operator_mapping = {
            "eq": "_eq",
            "ne": "_ne", 
            "in": "_in",
            "nin": "_nin",
            "contains": "_ilike",  # Case-insensitive like
            "startswith": "_ilike",
            "endswith": "_ilike",
            "gt": "_gt",
            "gte": "_gte", 
            "lt": "_lt",
            "lte": "_lte",
            "like": "_like",
            "ilike": "_ilike",
            "is_null": "_is_null"
        }
        
        graphql_op = operator_mapping.get(operator, "_eq")
        
        # Handle special case for contains operator
        if operator == "contains" and graphql_op == "_ilike":
            # The value transformation will be handled by the query builder
            pass
        
        return graphql_op
    
    def optimize_filter(self, filter_structure: FilterStructure) -> FilterStructure:
        """Optimize filter structure for better performance"""
        
        # For now, just return the original structure
        # Future optimizations could include:
        # - Combining similar filters
        # - Reordering filters by selectivity
        # - Removing redundant conditions
        
        return filter_structure
    
    def validate_filter_structure(self, filter_structure: FilterStructure) -> List[str]:
        """Validate the filter structure and return any warnings"""
        
        warnings = []
        
        # Check for empty filters
        if not filter_structure.filters:
            warnings.append("No filters generated - query may return all records")
        
        # Check for potentially inefficient patterns
        string_contains_count = sum(
            1 for f in filter_structure.filters 
            if f.operator == "contains"
        )
        
        if string_contains_count > 3:
            warnings.append(f"Many string containment filters ({string_contains_count}) may impact performance")
        
        # Check for conflicting filters on the same field
        field_counts = {}
        for f in filter_structure.filters:
            field_counts[f.field] = field_counts.get(f.field, 0) + 1
        
        for field, count in field_counts.items():
            if count > 1:
                warnings.append(f"Multiple filters on field '{field}' - may be conflicting")
        
        return warnings


# Global composer instance
_composer = None

def get_filter_composer() -> FilterComposer:
    """Get the global filter composer instance"""
    global _composer
    if _composer is None:
        _composer = FilterComposer()
    return _composer 