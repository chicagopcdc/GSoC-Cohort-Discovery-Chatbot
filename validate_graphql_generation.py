import csv
import json
import requests
import re
from filter_object_to_nl import filter_object_to_nl
from deepdiff import DeepDiff

CSV_PATH = 'assets/amanuensis_search_dump-06-18-2025.csv'
API_URL = 'http://localhost:8000/convert'


def is_empty_graphql_object(obj_str):
    if not obj_str or obj_str.strip() in ('{}', 'null', ''):
        return True
    try:
        obj = json.loads(obj_str)
        return not bool(obj)
    except Exception:
        return False


def normalize_field_paths(variables):
    """
    change dot-separated paths to nested paths.
    e.g. {"disease_characteristics.bulky_nodal_aggregate": ["No"]} to
         {"nested": {"AND": [{"IN": {"bulky_nodal_aggregate": ["No"]}}], "path": "disease_characteristics"}}
    """
    if not isinstance(variables, dict):
        return variables
    
    result = {}
    
    # process special case: directly include dot-separated paths in result dict
    dotted_fields = {}
    regular_fields = {}
    
    for key, value in list(variables.items()):
        if '.' in key:
            # collect all dot-separated fields
            parts = key.split('.')
            parent = parts[0]
            child = '.'.join(parts[1:])
            
            if parent not in dotted_fields:
                dotted_fields[parent] = {}
            
            if isinstance(value, list):
                dotted_fields[parent][child] = value
            else:
                dotted_fields[parent][child] = value
        else:
            # recursive process non-dot-separated fields
            if isinstance(value, dict):
                regular_fields[key] = normalize_field_paths(value)
            else:
                regular_fields[key] = value
    
    # process normal fields
    result.update(regular_fields)
    
    # process dot-separated fields, convert to nested structure
    for parent, children in dotted_fields.items():
        nested_obj = {
            "nested": {
                "path": parent,
                "AND": []
            }
        }
        
        for child_key, child_value in children.items():
            if isinstance(child_value, list):
                nested_obj["nested"]["AND"].append({
                    "IN": {
                        child_key: child_value
                    }
                })
            else:
                # process non-list value
                nested_obj["nested"]["AND"].append({
                    child_key: child_value
                })
        
        result[parent] = nested_obj
    
    return result


def normalize_case(variables):
    """
    standardize field value case, capitalize first letter
    """
    if not isinstance(variables, dict):
        return variables
    
    result = {}
    
    for key, value in variables.items():
        if isinstance(value, dict):
            result[key] = normalize_case(value)
        elif isinstance(value, list):
            # check if it is a string list, may need case standardization
            if all(isinstance(item, str) for item in value):
                if key in ['sex', 'race', 'consortium']:
                    result[key] = [item.capitalize() if item.lower() in ['male', 'female', 'asian', 'white', 'black', 'instruct', 'inrg', 'interact', 'nodal'] else item for item in value]
                else:
                    result[key] = value
            else:
                result[key] = [normalize_case(item) if isinstance(item, dict) else item for item in value]
        else:
            result[key] = value
    
    return result


def normalize_variables(variables):
    """
    standardize variable structure, handle common differences:
    1. remove filter wrapper layer
    2. process nested paths
    3. standardize case
    """
    if isinstance(variables, str):
        try:
            variables = json.loads(variables)
        except json.JSONDecodeError:
            print(f"无法解析JSON字符串: {variables}")
            return variables
    
    if isinstance(variables, dict) and 'filter' in variables:
        # remove filter wrapper layer
        variables = variables['filter']
    
    # standardize nested paths
    variables = normalize_field_paths(variables)
    
    # standard
    variables = normalize_case(variables)
    
    return variables


def preprocess_api_response(response_text):
    """
    preprocess API response, ensure we can correctly extract and process variables field
    """
    # try to parse JSON response
    try:
        response = json.loads(response_text)
        variables = response.get("variables", "{}")
        
        # if variables is a string, try to parse it
        if isinstance(variables, str):
            try:
                variables = json.loads(variables)
                return variables
            except json.JSONDecodeError:
                # if it is an invalid JSON string, use regex to extract variables
                pattern = r'{\s*"filter"\s*:\s*({.*})\s*}'
                match = re.search(pattern, variables)
                if match:
                    filter_content = match.group(1)
                    try:
                        filter_dict = json.loads(filter_content)
                        return {"filter": filter_dict}
                    except json.JSONDecodeError:
                        pass
                return variables
        else:
            # if it is already a dict, return directly
            return variables
    except json.JSONDecodeError:
        print(f"cannot parse API response: {response_text}")
        return response_text


def main(max_cases=5):  # limit to test only the first 5 cases
    total = 0
    matched = 0
    mismatched = 0
    skipped = 0
    details = []

    with open(CSV_PATH, newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            if total >= max_cases:
                break  # limit to test only the first 5 cases
                
            name = row['name']
            filter_obj = row['filter_object']
            graphql_obj = row['graphql_object']
            
            if is_empty_graphql_object(graphql_obj):
                print(f"\n===== Testing case: {name} =====")
                print(f"[SKIPPED] Empty graphql_object for {name}")
                skipped += 1
                continue
                
            try:
                print(f"\n===== Testing case: {name} =====")
                nl_query = filter_object_to_nl(filter_obj)
                print(f"Natural language query: {nl_query}")
                
                print(f"Calling API with query: {nl_query}")
                response = requests.post(API_URL, json={"text": nl_query})
                response.raise_for_status()
                
                result = response.json()
                print(f"API Response: {json.dumps(result, indent=2)}")
                
                # preprocess and parse API response
                variables = preprocess_api_response(result)
                
                # parse standard answer
                try:
                    standard_variables = json.loads(graphql_obj)
                except json.JSONDecodeError:
                    print(f"[ERROR] Cannot parse standard variables: {graphql_obj}")
                    mismatched += 1
                    continue
                    
                # standardize both sides of the variables structure
                normalized_variables = normalize_variables(variables)
                normalized_standard = normalize_variables(standard_variables)
                
                print("\nstandardized generated variables:")
                print(json.dumps(normalized_variables, indent=2))
                print("\nstandardized standard variables:")
                print(json.dumps(normalized_standard, indent=2))
                
                # compare results
                diff = DeepDiff(normalized_standard, normalized_variables, ignore_order=True)
                
                if diff:
                    print(f"\n[MISMATCH] for {name}")
                    print(f"Differences: {diff}")
                    mismatched += 1
                    details.append({
                        "name": name, 
                        "status": "MISMATCH",
                        "nl_query": nl_query,
                        "diff": diff
                    })
                else:
                    print(f"\n[MATCH] for {name}")
                    matched += 1
                    details.append({
                        "name": name, 
                        "status": "MATCH",
                        "nl_query": nl_query
                    })
                    
                total += 1
                
            except Exception as e:
                print(f"[ERROR] Exception for {name}: {str(e)}")
                mismatched += 1
                details.append({
                    "name": name, 
                    "status": "ERROR",
                    "error": str(e)
                })
                
    print(f"\nTotal: {total}, Matched: {matched}, Mismatched: {mismatched}, Skipped: {skipped}")
    
    # output detailed results
    print("\n===== Detailed Results =====")
    for item in details:
        print(f"{item['name']}: {item['status']}")
        if item['status'] == "MISMATCH" and 'diff' in item:
            print(f"  Differences: {item['diff']}")


if __name__ == "__main__":
    main() 