import json
import os
from typing import List
import re
import ast

def extract_context_from_user_query(input) -> List:
    """
    Split input by spaces or punctuation (, .) and return array
    Extract keywords from user query, filtering out common stop words
    """
    stop_words = {
        'the', 'a', 'an', 'but', 'on', 'at', 'to', 'for', 
        'of', 'with', 'by', 'from', 'up', 'about', 'into', 'through', 'during',
        'before', 'after', 'above', 'below', 'between', 'among', 'under', 'over',
        'is', 'are', 'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had',
        'do', 'does', 'did', 'will', 'would', 'could', 'should', 'may', 'might',
        'must', 'can', 'this', 'that', 'these', 'those', 'i', 'you', 'he', 'she',
        'it', 'we', 'they', 'me', 'him', 'her', 'us', 'them', 'my', 'your', 'his',
        'her', 'its', 'our', 'their', 'who', 'what', 'when', 'where', 'why', 'how',
        'consists', 'participants', 'specifically', 'classified', 'located', 'as',
        'show', 'find', 'get', 'select', 'search', 'list', 'display', 'return'
    }
    
    # Split using regex and filter
    words = re.split(r'[,.\s]+', input)
    
    # Filter out empty strings, stop words, and short words
    filtered_words = []
    for word in words:
        if (word and  # Not empty string
            len(word) >= 2 and  # At least 2 characters
            word.lower() not in stop_words and  # Not in stop words
            not word.isdigit()):  # Not pure number
            filtered_words.append(word)
    
    return filtered_words

def parse_pcdc_schema_prod(file):
    def recursive_enum_extract(obj, current_key=None, result=None):
        """Recursively extract all enum values and associate with corresponding keys"""
        if result is None:
            result = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "enum" and isinstance(value, list):
                    # Found enum, use each enum value as result key
                    # current_key is the parent key containing this enum
                    if current_key:
                        for enum_value in value:
                            if isinstance(enum_value, str):
                                if enum_value not in result:
                                    result[enum_value] = []
                                if current_key not in result[enum_value]:
                                    result[enum_value].append(current_key)
                else:
                    # Recursively process nested objects
                    recursive_enum_extract(value, key, result)
        elif isinstance(obj, list):
            # If list, recursively process each item
            for item in obj:
                recursive_enum_extract(item, current_key, result)
        
        return result
    
    try:
        # Read JSON file
        with open(file, 'r', encoding='utf-8') as f:
            schema_data = json.load(f)
        
        # Recursively extract all enum values
        result = recursive_enum_extract(schema_data)
        
        # Generate output file path
        file_dir = os.path.dirname(file)
        output_file = os.path.join(file_dir, "processed_pcdc_schema_prod.json")
        
        # Save result to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Processed schema saved to: {output_file}")
        print(f"Total enum values extracted: {len(result)}")
        
        return result
        
    except Exception as e:
        print(f"Error in parse_pcdc_schema_prod: {str(e)}")
        return {}

def parse_gitops(file):
    def recursive_fields_extract(obj, result=None):
        """Recursively extract all fields values and analyze field mappings, each field maps to a list of all table names"""
        if result is None:
            result = {}
        
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == "fields" and isinstance(value, list):
                    # Found fields, process each field in the list
                    for field in value:
                        if isinstance(field, str) and '.' in field:
                            # Split field name by dot
                            parts = field.split('.', 1)  # Split only on first dot
                            if len(parts) == 2:
                                table_name = parts[0]  # Part before dot as table name
                                field_name = parts[1]  # Part after dot as field name
                                
                                # Create new list if field name doesn't exist
                                if field_name not in result:
                                    result[field_name] = []
                                
                                # Append table name if not already in list (deduplication)
                                if table_name not in result[field_name]:
                                    result[field_name].append(table_name)
                        elif isinstance(field, str):
                            # Field without dot, use field name as key with empty list
                            if field not in result:
                                result[field] = []
                else:
                    # Recursively process nested objects
                    recursive_fields_extract(value, result)
        elif isinstance(obj, list):
            # If list, recursively process each item
            for item in obj:
                recursive_fields_extract(item, result)
        
        return result
    
    try:
        # Read JSON file
        with open(file, 'r', encoding='utf-8') as f:
            gitops_data = json.load(f)
        
        # Recursively extract all fields mappings
        result = recursive_fields_extract(gitops_data)
        
        # Generate output file path
        file_dir = os.path.dirname(file)
        output_file = os.path.join(file_dir, "processed_gitops.json")
        
        # Save result to JSON file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        
        print(f"Processed gitops saved to: {output_file}")
        print(f"Total field mappings extracted: {len(result)}")
        
        # Show statistics
        fields_with_multiple_tables = {k: v for k, v in result.items() if len(v) > 1}
        print(f"Fields appearing in multiple tables: {len(fields_with_multiple_tables)}")
        
        return result
        
    except Exception as e:
        print(f"Error in parse_gitops: {str(e)}")
        return {}


async def query_processed_pcdc_result(lowercase_pcdc_dict, keyword, user_query, llm):
    """
    Handle one-to-many mapping relationships, e.g.:
    "Metastatic": [
        "lesion_classification",
        "molecular_analysis_classification", 
        "tumor_classification"
    ],
    Let LLM decide final mapping schema based on user_query context
    """
    try:
        # Use lowercase keyword for lookup
        keyword_lower = keyword.lower()
        if keyword_lower in lowercase_pcdc_dict:
            mapping_schemas_in_pcdc_schema_prod = lowercase_pcdc_dict[keyword_lower]
            print(f"keyword: {keyword}, mapping_list_in_pcdc_schema_prod: {mapping_schemas_in_pcdc_schema_prod}")
            if len(mapping_schemas_in_pcdc_schema_prod) == 1:
                return mapping_schemas_in_pcdc_schema_prod[0]
            elif len(mapping_schemas_in_pcdc_schema_prod) > 1:
                prompt = f"""
                    Multiple medical terms from the query map to overlapping or conflicting database fields in pcdc-schema-prod.json. Resolve these conflicts to choose the most appropriate field.

                    Original Query: "{user_query}"
                    
                    Current Term: "{keyword}"
                    Conflicting Fields: {mapping_schemas_in_pcdc_schema_prod}
                    
                    Resolve by:
                    1. Identifying semantic overlaps (e.g., "cancer" and "tumor" might refer to same field)
                    2. Choosing more specific terms over general ones  
                    3. Maintaining clinical accuracy
                    4. Preserving user intent
                    5. Considering the medical context of the query
                    
                    From the conflicting fields list, select the ONE field that best matches the user query context.
                    Only return the selected field name as a string, no explanation needed.
                """
                llm_result = llm.invoke(prompt)
                print(f"llm_result: {llm_result}")
                # Extract content from the LLM response
                if hasattr(llm_result, 'content'):
                    llm_mapping_result = llm_result.content.strip().strip('"')  # Remove quotes if present
                else:
                    llm_mapping_result = str(llm_result).strip().strip('"')
                return llm_mapping_result
        return ""
    except Exception as e:
        print(f"Error in query_processed_pcdc_result: {str(e)}")
        return ""

async def query_processed_gitops_result(lowercase_gitops_dict, pcdc_schema, user_query, llm):
    """
    Handle one-to-many mapping relationships (e.g., one PCDC schema property maps to multiple GitOps field nodes)
    Let LLM decide final mapping schema based on user query context
    
    Args:
        query_pcdc_schema_prod_result: Property name from PCDC schema query
        processed_gitops_file: Processed GitOps file path
        user_query: User query
        llm: LLM agent
    Returns:
        Corresponding GitOps field node name
    """
    try:
        # Return empty string if PCDC query result is empty
        if not pcdc_schema:
            return ""
        # Use lowercase query_pcdc_schema_prod_result for lookup
        pcdc_property_lower = pcdc_schema.lower()
        if pcdc_property_lower in lowercase_gitops_dict:
            mapping_gitops_field_nodes = lowercase_gitops_dict[pcdc_property_lower]
            print(f"pcdc_schema: {pcdc_schema}, mapping_gitops_field_nodes: {mapping_gitops_field_nodes}")
            if len(mapping_gitops_field_nodes) == 0:
                return ""
            elif len(mapping_gitops_field_nodes) == 1:
                return mapping_gitops_field_nodes[0]
            elif len(mapping_gitops_field_nodes) > 1:
                # Multiple mappings, need LLM to choose most appropriate based on context
                prompt = f"""
                    Multiple GitOps field nodes map to the same PCDC schema property. Resolve this conflict to choose the most contextually appropriate field node.

                    Original Query: "{user_query}"
                    
                    PCDC Schema Property: "{pcdc_schema}"
                    Conflicting GitOps Field Nodes: {mapping_gitops_field_nodes}
                    
                    Example Context Mapping:
                    - If query mentions "tumors" + "assessment" → choose "tumor_assessments"
                    - If query mentions "surgery" or "biopsy" → choose "biopsy_surgical_procedures"  
                    - If query mentions "radiation" or "therapy" → choose "radiation_therapies"
                    
                    Resolve by:
                    1. Analyzing the medical procedure/context mentioned in the query
                    2. Choosing the field node that best matches the clinical workflow
                    3. Considering the temporal or procedural relationship
                    4. Maintaining semantic consistency with user intent
                    5. Prioritizing more specific contexts over general ones
                    
                    From the conflicting GitOps field nodes, select the ONE that best matches the user query context.
                    Only return the selected field node name as a string, no explanation needed.
                """
                llm_result = llm.invoke(prompt)
                print(f"gitops llm_result: {llm_result}")
                # Extract content from the LLM response
                if hasattr(llm_result, 'content'):
                    llm_mapping_result = llm_result.content.strip().strip('"')  # Remove quotes if present
                else:
                    llm_mapping_result = str(llm_result).strip().strip('"')
                return llm_mapping_result
        
        # Return empty string if no corresponding mapping found in GitOps
        return ""
        
    except Exception as e:
        print(f"Error in query_processed_gitops_result: {str(e)}")
        return ""

def convert_to_executable_nested_graphql(nested_graphql, llm):
    """
    Convert nested GraphQL filter to executable GraphQL format
    
    Args:
        nested_graphql: The raw LLM response content containing nested GraphQL
        llm: LLM instance for processing
        
    Returns:
        Executable GraphQL query in the format expected by execute_graphql_query()
    """
    prompt = f"""
    Generate an executable nested GraphQL version based on the following nested GraphQL result that can actually query the interface.

    Input nested GraphQL result:
    {nested_graphql}

    Please output an executable nested GraphQL in the following format:
    {{
      "query": "query GetAggregation($filter: JSON) {{ _aggregation {{ subject(accessibility: all, filter: $filter) {{ _totalCount }} }} }}",
      "variables": {{
        "filter": {{
          "AND": [
            {{
              "IN": {{
                "consortium": ["INRG"]
              }}
            }},
            {{
              "nested": {{
                "path": "tumor_assessments",
                "AND": [
                  {{
                    "IN": {{
                      "tumor_classification": ["Metastatic"]
                    }}
                  }},
                  {{
                    "IN": {{
                      "tumor_state": ["Absent"]
                    }}
                  }},
                  {{
                    "IN": {{
                      "tumor_site": ["Skin"]
                    }}
                  }}
                ]
              }}
            }}
          ]
        }}
      }}
    }}

    Requirements:
    1. Query field must use aggregation query format
    2. variables.filter must contain complete nested filter conditions
    3. Return standard JSON format without any explanatory text
    4. Ensure path field is in correct position within nested structure
    """
    
    try:
        # Call LLM to generate executable GraphQL
        response = llm.invoke(prompt)
        response_content = response.content if hasattr(response, 'content') else str(response)
        
        # Clean response content, remove possible markdown markers
        clean_response = response_content.strip()
        if clean_response.startswith('```json'):
            clean_response = clean_response[7:-3]
        elif clean_response.startswith('```'):
            clean_response = clean_response[3:-3]
        
        # Parse JSON response
        try:
            guppy_graphql = json.loads(clean_response.strip())
            
            # Validate returned result contains necessary fields
            if isinstance(guppy_graphql, dict) and "query" in guppy_graphql and "variables" in guppy_graphql:
                print(f"Successfully generated executable GraphQL: {json.dumps(guppy_graphql, ensure_ascii=False, indent=2)}")
                return guppy_graphql
            else:
                print(f"Invalid GraphQL format returned by LLM: {guppy_graphql}")
                return None
                
        except json.JSONDecodeError as e:
            print(f"Error parsing LLM response as JSON: {str(e)}")
            print(f"Raw LLM response: {response_content}")
            return None
            
    except Exception as e:
        print(f"Error in convert_to_executable_nested_graphql: {str(e)}")
        return None

def test_query_functions():
    pcdc_schema_prod_file = "../../schema/schema/pcdc-schema-prod-20250114.json"
    processed_pcdc_schema_prod_result = parse_pcdc_schema_prod(pcdc_schema_prod_file)
    gitops_file = "../../schema/gitops.json"
    processed_gitop_result = parse_gitops(gitops_file)

if __name__ == "__main__":
    # test_query_functions()
    pass