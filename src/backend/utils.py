import os
import json
from typing import List, Dict
from datetime import datetime


def load_config(file):
    with open(file, "r", encoding="utf-8") as f:
        return json.load(f)
    

def partition_log_into_blocks(log_paths: list) -> list:
    """
    Splits log data evenly into blocks. Block size is defined in a config file.
    
    Args:
        log_data (str): The full input log data as a string.
    
    Returns:
        List[str]: A list of log blocks. Each block has the configured length, except possibly the last one.
    """
    config = load_config("./config.json")
    block_size = config["log_block_size"]

    all_lines = []
    for path in log_paths:
        try:
            with open(path, "r", encoding="utf-8") as f:
                all_lines.append(f"# Content from: {path}")
                all_lines.extend(f.read().splitlines())
        except FileNotFoundError:
            continue

    log_blocks = [
        "\n".join(all_lines[i:i + block_size])
        for i in range(0, len(all_lines), block_size)
    ]
    return log_blocks

def save_results_to_file(results: Dict[str, List[str]]) -> str:
    """
    Save grouped results (by template_id) into a timestamped log file.

    Args:
        results (Dict[str, List[str]]): Mapping from template_id to list of result strings.

    Returns:
        str: Full path of the saved log file.
    """
    # Generate timestamped filename
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"chat_history/SPARK-22713/{timestamp}.log"
    
    # Ensure the entire directory path exists
    os.makedirs(os.path.dirname(log_filename), exist_ok=True)

    # Write each result with template_id
    with open(log_filename, "w", encoding="utf-8") as f:
        for template_id, result_list in results.items():
            f.write(f"\n=== {template_id} Results ===\n")
            # Handle list of results
            for idx, result in enumerate(result_list):
                if len(result_list) > 1:
                    f.write(f"\n--- Response {idx + 1} ---\n")
                f.write(result.strip() + "\n\n")

    print(f"[INFO] Results written to: {log_filename}")
    json_path = convert_to_json(log_filename)
    print(f"[INFO] Convert Results to json file: {json_path}")
    return log_filename

def convert_to_json(file_path: str) -> str:
    """
    Convert the filled templates from a log file to JSON format.
    
    Args:
        file_path: Path to the log file containing template results
        
    Returns:
        str: Path to the saved JSON file
    """
    import re
    
    # Read the log file
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Dictionary to store all template results
    all_templates = {}
    
    # Split content by template sections
    template_sections = content.split('=== ')[1:]  # Skip the first empty split
    
    for section in template_sections:
        # Extract template ID
        template_id_match = re.match(r'^(\S+)\s+Results\s+===', section)
        if not template_id_match:
            continue
            
        template_id = template_id_match.group(1)
        
        # Find the Completed Template section
        completed_template_match = re.search(
            r'###?\s*Completed Template\s*\n(.*?)(?=###|\Z)', 
            section, 
            re.DOTALL | re.IGNORECASE
        )
        
        if not completed_template_match:
            continue
            
        completed_content = completed_template_match.group(1)
        
        # Extract filled blanks - looking for patterns like ___content___ or __content__
        # Pattern explanation:
        # _{2,} matches 2 or more underscores
        # ([^_]+(?:\([^)]*\))?) captures content including optional parentheses
        # _{2,} matches closing underscores
        filled_blanks = re.findall(
            r'_{2,}([^_]+(?:\([^)]*\))?)_{2,}',
            completed_content
        )
        
        # Also extract template structure lines
        template_lines = []
        for line in completed_content.split('\n'):
            line = line.strip()
            if line.startswith('-') and ('___' in line or '__' in line or '{{' in line):
                template_lines.append(line)
        
        # Parse each filled blank to extract variable name and value
        parsed_blanks = {}
        for blank in filled_blanks:
            # Try to parse format like "variable_name(value)" or just "value"
            match = re.match(r'^([a-zA-Z_]+)\s*\((.*)\)$', blank.strip())
            if match:
                var_name = match.group(1)
                value = match.group(2).strip()
                parsed_blanks[var_name] = value
            else:
                # If no variable name, use the position as key
                parsed_blanks[f"blank_{len(parsed_blanks) + 1}"] = blank.strip()
        
        # Store the template data
        all_templates[template_id] = {
            "template_lines": template_lines,
            "filled_blanks": parsed_blanks,
            "raw_filled_values": filled_blanks
        }
    
    # Generate output filename based on input log file
    base_name = os.path.basename(file_path).replace('.log', '')
    json_filename = f"chat_history/{base_name}_extracted.json"
    
    # Ensure directory exists
    os.makedirs("chat_history", exist_ok=True)
    
    # Save as JSON
    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(all_templates, f, indent=2, ensure_ascii=False)
    
    print(f"[INFO] Extracted templates saved to: {json_filename}")
    return json_filename