import json

def create_enhanced_prompt(user_query, schema_info, conversation_history=None):
    """Create enhanced prompt template"""
    
    # Format schema information as string
    schema_str = json.dumps(schema_info, indent=2)
    
    # Add conversation history
    history_str = ""
    if conversation_history:
        history_str = f"""
Conversation history:
{conversation_history}
"""
    
    # Build prompt template
    template = f"""You are a professional GraphQL query converter. Please convert the user's natural language query into the corresponding GraphQL query.

User query: {user_query}

PCDC Schema information:
{schema_str}
{history_str}
Please construct the GraphQL query according to the following structure:
1. Use variables like `$filter` for dynamic parameterization
2. Use operators such as `AND`, `IN`, `GTE`, `LTE` to build complex filter conditions
3. Apply the provided schema to ensure field names match exactly
4. For complex queries involving multiple entity types (like subject, lab, disease_characteristic), use a nested structure rather than separate queries
5. Always use subject as the root node, with other entities nested within it to maintain relationships
6. Return both the query and variables separately

Example for a simple query "Subjects who are multiracial and between 0-18 years of age":

query ($filter: JSON) {{
  subject(accessibility: accessible, offset: 0, first: 20, filter: $filter) {{
    consortium
    subject_submitter_id
    sex
    race
    ethnicity
  }}
}}

Variables:
{{ "filter": {{ "AND": [
      {{ "IN": {{ "race": ["Multiracial"] }}}},
      {{ "AND": [{{"GTE": {{"age_at_censor_status": 0}}}}, 
                {{"LTE": {{"age_at_censor_status": 18}}}}]}}
    ]
}}}}

Example for a complex query "Subjects who are multiracial between 0-18 years with lab results and disease characteristics":

query ($filter: JSON) {{
  subject(accessibility: accessible, offset: 0, first: 20, filter: $filter) {{
    consortium
    subject_submitter_id
    sex
    race
    ethnicity
    lab_results {{
      lab_test_name
      lab_result_value
      lab_result_unit
    }}
    disease_characteristics {{
      disease_phase
      disease_type
      primary_site
    }}
  }}
}}

Variables:
{{ "filter": {{ "AND": [
      {{ "IN": {{ "race": ["Multiracial"] }}}},
      {{ "AND": [{{"GTE": {{"age_at_censor_status": 0}}}}, 
                {{"LTE": {{"age_at_censor_status": 18}}}}]}}
    ]
}}}}

Please ensure that the generated query field names match the schema exactly.
Please return the result in JSON format, including the following fields:
1. query: GraphQL query string
2. variables: Query variables JSON object
3. explanation: Brief explanation of what the query does
"""
    
    return template

def create_nested_query_prompt(user_query, schema_info, node_type, conversation_history=None):
    """Create nested query prompt template"""
    
    # Format schema information as string
    schema_str = json.dumps(schema_info, indent=2)
    
    # Add conversation history
    history_str = ""
    if conversation_history:
        history_str = f"""
Conversation history:
{conversation_history}
"""
    
    # Build prompt template
    template = f"""You are a professional GraphQL query converter. Please convert the user's natural language query into the corresponding GraphQL query.

User query: {user_query}

Node type to query: {node_type}

PCDC Schema information:
{schema_str}
{history_str}
Please construct the GraphQL query according to the following structure:
1. Use variables like `$filter` for dynamic parameterization
2. Use operators such as `AND`, `IN`, `GTE`, `LTE` to build complex filter conditions
3. Apply the provided schema to ensure field names match exactly
4. Use nested query syntax to query related nodes
5. Return both the query and variables separately

Example:
For the query "Subjects with histology grade of Differentiating":

query ($filter: JSON) {{
  subject(accessibility: accessible, offset: 0, first: 20, filter: $filter) {{
    consortium
    subject_submitter_id
    sex
    race
    ethnicity
    histologies {{
      histology_grade
    }}
  }}
}}

Variables:
{{
  "filter": {{
    "AND": [
      {{
        "nested": {{
          "path": "histologies",
          "AND": [{{"IN": {{"histology_grade": ["Differentiating"]}}}}]
        }}
      }}
    ]
  }}
}}

Please ensure that the generated query field names match the schema exactly.
Please return the result in JSON format, including the following fields:
1. query: GraphQL query string
2. variables: Query variables JSON object
3. explanation: Brief explanation of what the query does
"""
    
    return template

if __name__ == "__main__":
    # Test code
    test_schema = {
        "subject": {
            "consortium": None,
            "subject_submitter_id": None,
            "sex": None,
            "race": None,
            "ethnicity": None,
            "age_at_censor_status": None
        }
    }
    
    test_query = "Query subjects who are multiracial and between 0-18 years of age"
    prompt = create_enhanced_prompt(test_query, test_schema)
    print(prompt)
    
    # Test nested query prompt
    nested_prompt = create_nested_query_prompt(
        "Query subjects with histology grade of Differentiating", 
        {"subject": test_schema["subject"], "histologies": {"histology_grade": None}},
        "histologies"
    )
    print("\n\n" + nested_prompt) 