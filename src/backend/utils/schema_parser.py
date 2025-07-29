import json
import re

def parse_pcdc_schema(schema_file):
    """Parse PCDC schema and build property mappings"""
    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        
        # Extract all nodes and properties
        node_properties = {}
        term_mappings = {}
        
        # Check schema structure
        if "subject.yaml" in schema:
            # Process yaml format schema
            for key, value in schema.items():
                if ".yaml" in key:
                    node_type = key.split('.')[0]
                    if "properties" in value:
                        node_properties[node_type] = {}
                        for prop, details in value["properties"].items():
                            if prop not in ["type", "id", "project_id", "created_datetime", "updated_datetime", "state"]:
                                node_properties[node_type][prop] = details
                                
                                # Build term mappings
                                if "enum" in details:
                                    term_mappings[prop] = details["enum"]
                                if "term" in details:
                                    for term in details.get("term", []):
                                        if isinstance(term, dict) and "$ref" in term:
                                            term_name = term["$ref"].split('/')[-1]
                                            term_mappings[term_name] = prop
        else:
            # Use hardcoded subject schema as fallback
            node_properties["subject"] = {
                "consortium": {"enum": ["INSTRuCT", "MaGIC", "INRG", "NODAL", "INTERACT", "HIBISCUS", "ALL"]},
                "sex": {"enum": ["Female", "Male", "Other", "Unknown", "Not Reported"]},
                "race": {"enum": ["American Indian or Alaska Native", "Asian", "Black or African American", "Native Hawaiian or Other Pacific Islander", "White", "Multiple races", "Other", "Unknown", "Not Reported"]},
                "ethnicity": {"enum": ["Hispanic or Latino", "Not Hispanic or Latino", "Unknown", "Not Reported"]},
                "age_at_censor_status": {"type": ["number"]}
            }
            
            # Add basic term mappings
            term_mappings = {
                "male": "sex",
                "female": "sex",
                "men": "sex",
                "women": "sex",
                "age": "age_at_censor_status",
                "years old": "age_at_censor_status",
                "multiracial": "race",
                "white": "race",
                "black": "race",
                "asian": "race",
                "hispanic": "ethnicity",
                "latino": "ethnicity"
            }
        
        return node_properties, term_mappings
    except Exception as e:
        print(f"Failed to parse PCDC schema: {str(e)}")
        # Return basic schema as fallback
        return {
            "subject": {
                "consortium": {"enum": ["INSTRuCT", "MaGIC", "INRG", "NODAL", "INTERACT", "HIBISCUS", "ALL"]},
                "sex": {"enum": ["Female", "Male", "Other", "Unknown", "Not Reported"]},
                "race": {"enum": ["American Indian or Alaska Native", "Asian", "Black or African American", "Native Hawaiian or Other Pacific Islander", "White", "Multiple races", "Other", "Unknown", "Not Reported"]},
                "ethnicity": {"enum": ["Hispanic or Latino", "Not Hispanic or Latino", "Unknown", "Not Reported"]},
                "age_at_censor_status": {"type": ["number"]}
            }
        }, {
            "male": "sex",
            "female": "sex",
            "men": "sex",
            "women": "sex",
            "age": "age_at_censor_status",
            "years old": "age_at_censor_status",
            "multiracial": "race",
            "white": "race",
            "black": "race",
            "asian": "race",
            "hispanic": "ethnicity",
            "latino": "ethnicity"
        }

def extract_relevant_schema(query, node_properties):
    """Extract relevant schema information based on query"""
    relevant_schema = {}
    
    # Check node types mentioned in the query
    node_types = ["subject", "disease_characteristic", "staging", "lab", "vital", "medical_history"]
    
    for node_type in node_types:
        if node_type in query.lower():
            relevant_schema[node_type] = node_properties.get(node_type, {})
    
    # If no relevant nodes found, default to subject node
    if not relevant_schema:
        relevant_schema["subject"] = node_properties.get("subject", {})
    
    return relevant_schema

def standardize_terms(user_input, term_mappings):
    """Standardize user input terms to PCDC schema terms"""
    standardized_input = user_input
    
    # Common term mappings
    common_mappings = {
        "male": "sex",
        "female": "sex",
        "men": "sex",
        "women": "sex",
        "age": "age_at_censor_status",
        "years old": "age_at_censor_status",
        "multiracial": "race",
        "white": "race",
        "black": "race",
        "asian": "race",
        "hispanic": "ethnicity",
        "latino": "ethnicity"
    }
    
    # Apply common mappings
    for term, mapped_term in common_mappings.items():
        pattern = re.compile(r'\b' + term + r'\b', re.IGNORECASE)
        standardized_input = pattern.sub(f"{term} ({mapped_term})", standardized_input)
    
    # Apply mappings extracted from schema
    for term, mapped_term in term_mappings.items():
        if isinstance(mapped_term, str):
            pattern = re.compile(r'\b' + term + r'\b', re.IGNORECASE)
            standardized_input = pattern.sub(f"{term} ({mapped_term})", standardized_input)
    
    return standardized_input

if __name__ == "__main__":
    # Test code
    schema_file_path = "../../schema/gitops.json"
    node_properties, term_mappings = parse_pcdc_schema(schema_file_path)
    print("Parsing complete")
    print(f"Node count: {len(node_properties)}")
    print(f"Term mappings count: {len(term_mappings)}")
    
    # Test extracting relevant schema
    test_query = "Query subjects who are multiracial (Multiracial) and between 0-18 years of age"
    relevant_schema = extract_relevant_schema(test_query, node_properties)
    print(f"Relevant schema: {list(relevant_schema.keys())}")
    
    # Test term standardization
    standardized_query = standardize_terms(test_query, term_mappings)
    print(f"Standardized query: {standardized_query}") 