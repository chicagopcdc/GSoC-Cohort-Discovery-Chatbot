"""
GraphQL Query Builder - Step 5 of Pipeline

Responsible for generating final GraphQL queries from filter structures
with proper syntax, variables, and optimization.
"""

import json
from typing import Dict, List, Any, Optional, Union
from core.models import FilterStructure, GraphQLQuery
from core.config import config
from utils.logging import get_logger
from utils.errors import QueryGenerationError

logger = get_logger(__name__)


class QueryBuilder:
    """Builds final GraphQL queries from filter structures"""
    
    def __init__(self):
        """Initialize the query builder"""
        self.default_fields = self._get_default_fields()
        logger.debug("Initialized QueryBuilder")
    
    def build_query(self, filter_structure: FilterStructure) -> GraphQLQuery:
        """
        Build final GraphQL query from filter structure
        
        Args:
            filter_structure: Filter structure from Step 4
            
        Returns:
            GraphQLQuery with query string and variables
            
        Raises:
            QueryGenerationError: If query generation fails
        """
        try:
            # Build the filter clause from filters
            filter_clause = self._build_filter_clause(filter_structure)
            
            # Build the main query
            query_string = self._build_query_string(filter_clause)
            
            # Build variables
            variables = self._build_variables(filter_structure)
            
            # Generate description
            description = self._generate_description(filter_structure)
            
            graphql_query = GraphQLQuery(
                query=query_string,
                variables=json.dumps(variables, indent=2),
                description=description
            )
            
            logger.debug(f"Generated GraphQL query with {len(filter_structure.filters)} filters")
            return graphql_query
            
        except Exception as e:
            logger.error(f"GraphQL query generation failed: {e}")
            raise QueryGenerationError(f"Failed to generate GraphQL query: {e}")
    
    def _build_where_clause(self, filter_structure: FilterStructure) -> Dict[str, Any]:
        """Build the where clause from filter structure - DEPRECATED"""
        
        if not filter_structure.filters:
            return {}
        
        # Use the raw structure if available
        if filter_structure.raw_structure:
            return self._process_raw_structure(filter_structure.raw_structure)
        
        # Fallback: build from individual filters
        return self._build_from_filters(filter_structure)
    
    def _build_filter_clause(self, filter_structure: FilterStructure) -> Dict[str, Any]:
        """Build the filter clause in the correct format for subject queries"""
        
        if not filter_structure.filters:
            return {}
        
        # Convert filters to the expected filter format
        return self._build_subject_filter(filter_structure)
    
    def _build_subject_filter(self, filter_structure: FilterStructure) -> Dict[str, Any]:
        """Build filter structure in the format expected by subject queries"""
        
        conditions = []
        nested_conditions = {}
        
        for filter_obj in filter_structure.filters:
            field_path = filter_obj.field
            operator = filter_obj.operator
            value = filter_obj.value
            
            # Determine if this is a nested field
            if "." in field_path:
                # Nested field (e.g., tumor_assessments.tumor_site)
                parts = field_path.split(".", 1)
                nested_path = parts[0]
                nested_field = parts[1]
                
                if nested_path not in nested_conditions:
                    nested_conditions[nested_path] = []
                
                # Build the nested condition
                nested_condition = self._build_filter_condition(nested_field, operator, value)
                nested_conditions[nested_path].append(nested_condition)
            
            else:
                # Direct field on subject
                condition = self._build_filter_condition(field_path, operator, value)
                conditions.append(condition)
        
        # Build the final filter structure
        final_conditions = conditions.copy()
        
        # Add nested conditions
        for nested_path, nested_list in nested_conditions.items():
            nested_filter = {
                "nested": {
                    "path": nested_path,
                    "AND": nested_list
                }
            }
            final_conditions.append(nested_filter)
        
        if not final_conditions:
            return {}
        
        if len(final_conditions) == 1:
            return final_conditions[0]
        
        # Multiple conditions - use AND logic
        return {"AND": final_conditions}
    
    def _build_filter_condition(self, field: str, operator: str, value) -> Dict[str, Any]:
        """Build a single filter condition"""
        
        # Map operator to the expected format
        if operator in ["eq", "in"]:
            if isinstance(value, list):
                return {"IN": {field: value}}
            else:
                return {"IN": {field: [value]}}
        elif operator == "contains":
            return {"CONTAINS": {field: value}}
        elif operator == "gte":
            return {"GTE": {field: value}}
        elif operator == "lte":
            return {"LTE": {field: value}}
        elif operator == "gt":
            return {"GT": {field: value}}
        elif operator == "lt":
            return {"LT": {field: value}}
        else:
            # Default to IN for unknown operators
            if isinstance(value, list):
                return {"IN": {field: value}}
            else:
                return {"IN": {field: [value]}}
    
    def _process_raw_structure(self, raw_structure: Dict[str, Any]) -> Dict[str, Any]:
        """Process the raw filter structure and convert values"""
        
        if not raw_structure:
            return {}
        
        result = {}
        
        for key, value in raw_structure.items():
            if key in ["AND", "OR"]:
                # Logical operators with list of conditions
                processed_conditions = []
                for condition in value:
                    processed = self._process_raw_structure(condition)
                    if processed:
                        processed_conditions.append(processed)
                
                if processed_conditions:
                    if len(processed_conditions) == 1:
                        # Single condition, no need for logical wrapper
                        result.update(processed_conditions[0])
                    else:
                        result[key] = processed_conditions
            
            elif isinstance(value, dict):
                # Nested object (field with operators)
                processed_value = {}
                for op_key, op_value in value.items():
                    if op_key.startswith("_"):
                        # GraphQL operator
                        processed_value[op_key] = self._process_filter_value(op_key, op_value)
                    else:
                        # Nested field
                        nested_result = self._process_raw_structure({op_key: op_value})
                        processed_value.update(nested_result)
                
                if processed_value:
                    result[key] = processed_value
            
            else:
                # Direct value
                result[key] = value
        
        return result
    
    def _process_filter_value(self, operator: str, value: Any) -> Any:
        """Process a filter value based on its operator"""
        
        if operator == "_ilike":
            # Convert contains operator to ILIKE pattern
            if isinstance(value, str):
                return f"%{value}%"
            else:
                return value
        
        elif operator in ["_like", "_ilike"]:
            # Ensure LIKE patterns have wildcards if needed
            if isinstance(value, str) and "%" not in value:
                return f"%{value}%"
            else:
                return value
        
        else:
            return value
    
    def _build_from_filters(self, filter_structure: FilterStructure) -> Dict[str, Any]:
        """Build where clause from individual filters (fallback)"""
        
        conditions = []
        
        for filter_obj in filter_structure.filters:
            condition = self._filter_to_condition(filter_obj)
            if condition:
                conditions.append(condition)
        
        if not conditions:
            return {}
        
        if len(conditions) == 1:
            return conditions[0]
        
        # Multiple conditions - combine with logic operator
        logic_key = "AND" if filter_structure.logic.value == "AND" else "OR"
        return {logic_key: conditions}
    
    def _filter_to_condition(self, filter_obj) -> Dict[str, Any]:
        """Convert a filter object to a GraphQL condition"""
        
        field_parts = filter_obj.field.split(".")
        operator = filter_obj.operator
        value = filter_obj.value
        
        # Process the value
        processed_value = self._process_filter_value(
            self._map_operator_to_graphql(operator), 
            value
        )
        
        # Build nested structure for field path
        if len(field_parts) == 1:
            return {
                field_parts[0]: {
                    self._map_operator_to_graphql(operator): processed_value
                }
            }
        else:
            # Build nested structure
            result = {}
            current = result
            
            for part in field_parts[:-1]:
                current[part] = {}
                current = current[part]
            
            current[field_parts[-1]] = {
                self._map_operator_to_graphql(operator): processed_value
            }
            
            return result
    
    def _map_operator_to_graphql(self, operator: str) -> str:
        """Map internal operator to GraphQL operator"""
        
        mapping = {
            "eq": "_eq",
            "ne": "_ne",
            "in": "_in", 
            "nin": "_nin",
            "contains": "_ilike",
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
        
        return mapping.get(operator, "_eq")
    
    def _build_query_string(self, filter_clause: Dict[str, Any]) -> str:
        """Build the complete GraphQL query string"""
        
        # Start with the correct subject-based query structure
        query_parts = ["query ($filter: JSON) {"]
        
        # Add the main subject query with proper parameters
        query_parts.append("  subject(")
        query_parts.append("    accessibility: accessible,")
        query_parts.append("    offset: 0,")
        query_parts.append(f"    first: {config.DEFAULT_QUERY_LIMIT}")
        
        # Add filter if present
        if filter_clause:
            query_parts.append("    filter: $filter")
        
        query_parts.append("  ) {")
        
        # Add selected fields for subject-based schema
        subject_fields = [
            "    consortium",
            "    subject_submitter_id", 
            "    sex",
            "    race",
            "    ethnicity",
            "    age_at_censor_status",
            "    tumor_assessments {",
            "      tumor_site",
            "      tumor_state", 
            "      tumor_classification",
            "      age_at_tumor_assessment",
            "    }",
            "    histologies {",
            "      histology",
            "      histology_grade",
            "    }",
            "    disease_characteristics {",
            "      diagnosis",
            "      primary_site",
            "    }"
        ]
        
        for field in subject_fields:
            query_parts.append(field)
        
        query_parts.append("  }")
        query_parts.append("}")
        
        return "\n".join(query_parts)
    
    def _build_variables(self, filter_structure: FilterStructure) -> Dict[str, Any]:
        """Build GraphQL variables object"""
        
        variables = {}
        
        filter_clause = self._build_filter_clause(filter_structure)
        if filter_clause:
            variables["filter"] = filter_clause
        
        return variables
    
    def _generate_description(self, filter_structure: FilterStructure) -> str:
        """Generate human-readable description of the query"""
        
        if not filter_structure.filters:
            return "Query for all cases (no filters applied)"
        
        descriptions = []
        
        for filter_obj in filter_structure.filters:
            field_desc = self._get_field_description(filter_obj.field)
            op_desc = self._get_operator_description(filter_obj.operator)
            value_desc = self._get_value_description(filter_obj.value)
            
            descriptions.append(f"{field_desc} {op_desc} {value_desc}")
        
        logic_word = "and" if filter_structure.logic.value == "AND" else "or"
        
        if len(descriptions) == 1:
            return f"Cases where {descriptions[0]}"
        else:
            return f"Cases where {f' {logic_word} '.join(descriptions)}"
    
    def _get_field_description(self, field_path: str) -> str:
        """Get human-readable field description"""
        
        # Simple field name cleaning
        field_name = field_path.split(".")[-1]
        
        # Convert underscore notation to readable format
        readable = field_name.replace("_", " ").title()
        
        return readable
    
    def _get_operator_description(self, operator: str) -> str:
        """Get human-readable operator description"""
        
        descriptions = {
            "eq": "equals",
            "ne": "does not equal",
            "in": "is one of",
            "nin": "is not one of", 
            "contains": "contains",
            "startswith": "starts with",
            "endswith": "ends with",
            "gt": "is greater than",
            "gte": "is greater than or equal to",
            "lt": "is less than",
            "lte": "is less than or equal to",
            "like": "matches pattern",
            "ilike": "matches pattern (case insensitive)",
            "is_null": "is null"
        }
        
        return descriptions.get(operator, f"has operator {operator}")
    
    def _get_value_description(self, value: Any) -> str:
        """Get human-readable value description"""
        
        if isinstance(value, list):
            if len(value) == 1:
                return f"'{value[0]}'"
            elif len(value) <= 3:
                return f"[{', '.join(str(v) for v in value)}]"
            else:
                return f"[{value[0]}, {value[1]} and {len(value)-2} others]"
        else:
            return f"'{value}'"
    
    def _get_default_fields(self) -> List[str]:
        """Get the default fields to include in queries"""
        
        # Default fields that are commonly needed
        default_fields = [
            "submitter_id",
            "id",
            # Demographics
            "demographics {",
            "  gender",
            "  race", 
            "  ethnicity",
            "  age_at_diagnosis",
            "}",
            # Disease characteristics
            "disease_characteristics {",
            "  diagnosis",
            "  primary_site",
            "  stage",
            "}",
            # Treatment
            "treatments {",
            "  treatment_type",
            "  treatment_outcome",
            "}",
            # Follow up
            "follow_ups {",
            "  vital_status",
            "  days_to_last_follow_up",
            "}"
        ]
        
        return default_fields
    
    def customize_fields(self, fields: List[str]) -> None:
        """Customize the fields included in queries"""
        self.default_fields = fields
        logger.info(f"Customized query fields to {len(fields)} items")
    
    def optimize_query(self, query: GraphQLQuery) -> GraphQLQuery:
        """Optimize the generated query for better performance"""
        
        # For now, just return the original query
        # Future optimizations could include:
        # - Field selection optimization
        # - Query complexity analysis
        # - Pagination handling
        # - Fragment usage
        
        return query
    
    def validate_query(self, query: GraphQLQuery) -> List[str]:
        """Validate the generated query and return warnings"""
        
        warnings = []
        
        # Check query length
        if len(query.query) > 10000:
            warnings.append("Query is very long and may impact performance")
        
        # Check for potential performance issues
        if "_ilike" in query.query:
            ilike_count = query.query.count("_ilike")
            if ilike_count > 3:
                warnings.append(f"Query contains {ilike_count} text search operations which may be slow")
        
        # Check variables size
        if query.variables:
            try:
                vars_obj = json.loads(query.variables)
                if len(json.dumps(vars_obj)) > 5000:
                    warnings.append("Query variables are very large")
            except json.JSONDecodeError:
                warnings.append("Invalid JSON in query variables")
        
        return warnings


# Global builder instance
_builder = None

def get_query_builder() -> QueryBuilder:
    """Get the global query builder instance"""
    global _builder
    if _builder is None:
        _builder = QueryBuilder()
    return _builder 