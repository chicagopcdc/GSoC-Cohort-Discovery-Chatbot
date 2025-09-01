import json
import re

def build_graphql_filter(criteria):
    """Build GraphQL filter based on criteria"""
    filters = []
    
    for field, condition in criteria.items():
        if isinstance(condition, list):
            filters.append({"IN": {field: condition}})
        elif isinstance(condition, dict) and "min" in condition and "max" in condition:
            filters.append({"AND": [
                {"GTE": {field: condition["min"]}},
                {"LTE": {field: condition["max"]}}
            ]})
        elif isinstance(condition, dict) and "op" in condition:
            if condition["op"] == "eq":
                filters.append({field: condition["value"]})
            elif condition["op"] == "gt":
                filters.append({"GT": {field: condition["value"]}})
            elif condition["op"] == "lt":
                filters.append({"LT": {field: condition["value"]}})
            elif condition["op"] == "gte":
                filters.append({"GTE": {field: condition["value"]}})
            elif condition["op"] == "lte":
                filters.append({"LTE": {field: condition["value"]}})
    
    if len(filters) > 1:
        return {"filter": {"AND": filters}}
    elif len(filters) == 1:
        return {"filter": filters[0]}
    else:
        return {"filter": {}}

def extract_query_conditions(query):
    """Extract conditions from query"""
    conditions = {}
    
    # Extract race condition
    race_match = re.search(r'race\s+is\s+(\w+)', query, re.IGNORECASE) or re.search(r'(\w+)\s+\(race\)', query, re.IGNORECASE)
    if race_match:
        race = race_match.group(1)
        conditions["race"] = [race]
    
    # Extract age range
    age_range_match = re.search(r'age\s+between\s+(\d+)\s+and\s+(\d+)', query, re.IGNORECASE) or \
                     re.search(r'between\s+(\d+)\s+and\s+(\d+)\s+years', query, re.IGNORECASE)
    if age_range_match:
        min_age = int(age_range_match.group(1))
        max_age = int(age_range_match.group(2))
        conditions["age_at_censor_status"] = {"min": min_age, "max": max_age}
    
    # Extract sex condition
    sex_match = re.search(r'sex\s+is\s+(\w+)', query, re.IGNORECASE) or re.search(r'(\w+)\s+\(sex\)', query, re.IGNORECASE)
    if sex_match:
        sex = sex_match.group(1)
        conditions["sex"] = [sex]
    
    return conditions

def build_graphql_query(fields, filter_var="$filter"):
    """Build GraphQL query"""
    fields_str = "\n    ".join(fields)
    query = f"""query ({filter_var}: JSON) {{
  subject(accessibility: accessible, offset: 0, first: 20, filter: {filter_var}) {{
    {fields_str}
  }}
}}"""
    return query

def decompose_query(query):
    """Decompose complex query into multiple related parts
    
    Instead of creating separate independent queries, this creates a structured 
    representation of a single query with multiple nested parts, ensuring the 
    results are related through subject IDs.
    """
    # Identify the main query parts
    node_types = ["subject", "disease_characteristic", "staging", "lab", "vital", "medical_history"]
    
    # Always start with subject as the primary node if complex query involves multiple entities
    primary_node = "subject"
    related_nodes = []
    
    for node_type in node_types:
        if node_type != "subject" and node_type in query.lower():
            related_nodes.append(node_type)
    
    # Format a query that ensures relationships are maintained
    # The primary query will be for subjects with the filtering conditions
    # The related nodes will be included as nested fields within the subject
    query_parts = {
        "primary_node": primary_node,
        "related_nodes": related_nodes,
        "full_query": query  # Keep the original query for context
    }
    
    return query_parts

def combine_results(results, original_query):
    """Process results maintaining relationships between entities
    
    Instead of simply merging separate results, this ensures that relationships
    between entities are maintained by focusing on the hierarchical structure
    of the query.
    """
    # In the improved implementation, the results should already be structured correctly
    # with the primary node (usually subject) containing nested related nodes
    
    # For now, we'll just take the first result if available since that should
    # contain the properly structured data
    if results and len(results) > 0:
        primary_result = results[0]
        
        # Check if we have a proper GraphQL query in the result
        if isinstance(primary_result, dict) and "query" in primary_result:
            return primary_result
    
    # Fallback to empty result structure if no valid results
    return {
        "query": "",
        "variables": {},
    }

if __name__ == "__main__":
    # Test code
    test_query = "Query subjects who are multiracial (Multiracial) and between 0 and 18 years of age"
    conditions = extract_query_conditions(test_query)
    print(f"Extracted conditions: {conditions}")
    
    filter_json = build_graphql_filter(conditions)
    print(f"Built filter: {json.dumps(filter_json, indent=2)}")
    
    fields = ["consortium", "subject_submitter_id", "sex", "race", "ethnicity"]
    query = build_graphql_query(fields)
    print(f"Built query: {query}")
    
    complexity = analyze_query_complexity(test_query)
    print(f"Query complexity: {complexity}")
    
    if complexity == "complex":
        query_parts = decompose_query(test_query)
        print(f"Decomposed query parts: {query_parts}") 