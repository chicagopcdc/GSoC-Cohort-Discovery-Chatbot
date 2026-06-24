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



async def resolve_pcdc_ambiguities(keywords, lowercase_pcdc_dict, user_query, llm):
    """Resolve multiple ambiguous keywords with one LLM call.

    Returns a dict mapping each keyword to the chosen field name.
    """
    if not keywords:
        return {}
    details = {}
    for kw in keywords:
        kw_lower = kw.lower()
        details[kw] = lowercase_pcdc_dict.get(kw_lower, [])
    prompt = f"""
        The following terms each map to multiple pcdc-schema-prod fields. Using
        the context of the entire user query, choose the most appropriate field
        for each term.

        Original Query: "{user_query}"

        Ambiguous terms and options:
        {json.dumps(details, ensure_ascii=False, indent=2)}

        For every term, return a JSON object with the term as the key and the
        chosen field name as the value. Example:
        {{"Metastatic": "tumor_classification", "Age": "age_at_diagnosis"}}
        """
    llm_result = llm.invoke(prompt)
    text = llm_result.content if hasattr(llm_result, 'content') else str(llm_result)
    try:
        mapping = json.loads(text)
    except Exception:
        # fallback: try one-per-line
        mapping = {}
        for line in text.splitlines():
            if ':' in line:
                term, val = line.split(':', 1)
                mapping[term.strip().strip('"')] = val.strip().strip('"')
    return mapping


async def query_processed_pcdc_result(lowercase_pcdc_dict, keyword, user_query, llm):
    """Backward-compatibility wrapper calling batch resolver for single keyword."""
    # reuse the batch function for a single-entry list
    result = await resolve_pcdc_ambiguities([keyword], lowercase_pcdc_dict, user_query, llm)
    return result.get(keyword, "")

async def resolve_gitops_ambiguities(pcdc_properties, lowercase_gitops_dict, user_query, llm):
    """Resolve multiple pcdc properties mapping to gitops nodes with one LLM call."""
    details = {}
    for prop in pcdc_properties:
        key = prop.lower()
        details[prop] = lowercase_gitops_dict.get(key, [])
    if not details:
        return {}
    prompt = f"""
        The following PCDC schema properties each map to multiple GitOps field nodes.
        Using context from the user query, choose the most appropriate node for each property.

        Original Query: "{user_query}"
        Ambiguous properties and choices:
        {json.dumps(details, ensure_ascii=False, indent=2)}

        Return a JSON object mapping property names to selected field node names.
    """
    llm_result = llm.invoke(prompt)
    text = llm_result.content if hasattr(llm_result, 'content') else str(llm_result)
    try:
        mapping = json.loads(text)
    except Exception:
        mapping = {}
        for line in text.splitlines():
            if ':' in line:
                left, right = line.split(':', 1)
                mapping[left.strip().strip('"')] = right.strip().strip('"')
    return mapping


async def query_processed_gitops_result(lowercase_gitops_dict, pcdc_schema, user_query, llm):
    """Backward-compatible wrapper for single-property resolution."""
    if not pcdc_schema:
        return ""
    result = await resolve_gitops_ambiguities([pcdc_schema], lowercase_gitops_dict, user_query, llm)
    return result.get(pcdc_schema, "")

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