import os
import time
import uuid
import re
import httpx
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from langchain.memory import ConversationBufferMemory
from langchain.schema import HumanMessage, AIMessage, SystemMessage
import json
import glob
from collections import defaultdict

# Import only the needed function for log processing
from utils.log_handler import partition_log_into_blocks

# Load environment variables
load_dotenv()

app = FastAPI()

# Define input model for interaction analysis
class InteractionAnalysisRequest(BaseModel):
    log_files: Optional[List[str]] = None
    templates_path: Optional[str] = "./template/"
    session_id: Optional[str] = None

# Define output model for interaction analysis
class InteractionAnalysisResponse(BaseModel):
    interaction_pairs: str
    dispatched_interactions: str
    success: bool
    message: Optional[str] = None

# Define input model for diagnosis
class DiagnoseRequest(BaseModel):
    log_files: Optional[List[str]] = None
    templates_path: Optional[str] = None
    session_id: Optional[str] = None

# Define output model for diagnosis
class DiagnoseResponse(BaseModel):
    results: Dict[str, List[str]]
    success: bool
    message: Optional[str] = None

def process_log_inputs(inputs: List[str]) -> List[str]:
    """
    Process input arguments which can be files or folders.
    Returns a list of log file paths.
    """
    log_files = []
    
    for input_path in inputs:
        if os.path.isfile(input_path):
            # It's a single file
            log_files.append(input_path)
            print(f"Added file: {input_path}")
        elif os.path.isdir(input_path):
            # It's a directory, get all files in it
            folder_files = glob.glob(os.path.join(input_path, "*"))
            folder_files = [f for f in folder_files if os.path.isfile(f)]
            log_files.extend(folder_files)
            print(f"Added {len(folder_files)} files from folder: {input_path}")
            for f in folder_files:
                print(f"  - {f}")
        else:
            print(f"Warning: {input_path} is neither a file nor a directory, skipping...")
    
    return log_files

async def pattern_dispatcher(llm: ChatOpenAI, interaction_pairs: str, conversation_memory: ConversationBufferMemory) -> str:
    """
    Dispatch interaction pairs to bug categories using context-aware LLM
    """
    # interaction type -> three csi bug categories
    dispatcher = {
        "resource_invocation": "resource_leak",
        "abnormal_usage": "resource_contention",
        "shared_object": "semantic_inconsistency"
    }
    
    prompt = (
        "Based on the cross-component interaction pairs you identified from logs in the previous step formatted as a list of tuples like: '{ [component_A]: [component_B], ... }' \n"
        f"{interaction_pairs} \n"
        "your task is to classify each pair into one of the following **interaction patterns**:\n"

        "Interaction Patterns (Enum: resource_invocation, abnormal_usage, shared_object):\n"
        "  - resource_invocation: [component_A] invokes [component_B], which utilizes a [resource].\n"
        "  - abnormal_usage: [component_A] and [component_B] both exhibit abnormal usage on a shared [resource].\n"
        "  - shared_object: [component_A] and [component_B] both use the same [resource] (e.g., shared file, memory, or object).\n\n"
        "[resource] can be system resource such as memory, socket, I/O, disk usage, or abstract resource such as file, container. \n"
        "Your output must be a structured JSON object in the following format:\n"
        '{\n'
        '  "[interaction_pattern]": ([component_A], [component_B], [resource])\n'
        '}\n\n'

        "Instructions:\n"
        "1. For each valid interaction pair, determine the correct interaction pattern from the enum.\n"
        "2. Provide **regular expressions** that can be used to extract relevant log lines for each interaction.\n"
        "3. Describe your **reasoning process** for assigning the interaction type.\n"
        "4. Explicitly state any **assumptions** you make during classification or pattern matching.\n\n"

        "Please ensure the final output is a valid JSON object that contains all detected interaction patterns, with structured entries, reasoning, assumptions, and associated regexes."
    )
    
    # Add to memory and get response
    conversation_memory.chat_memory.add_user_message(prompt)
    
    # Get all messages from memory to maintain context
    messages = conversation_memory.chat_memory.messages
    response = llm.invoke(messages)
    
    # Add response to memory
    conversation_memory.chat_memory.add_ai_message(response.content)
    
    return response.content

def load_templates_recursive(templates_path: str) -> Dict[str, str]:
    """
    Recursively load template files from a directory or a single file.
    Returns a dictionary mapping template_id to template_content.
    """
    templates = {}
    
    if os.path.isfile(templates_path):
        # Single template file
        template_id = os.path.basename(templates_path).replace('.txt', '').replace('.template', '')
        try:
            with open(templates_path, 'r', encoding='utf-8') as f:
                templates[template_id] = f.read().strip()
            print(f"Loaded template: {template_id}")
        except Exception as e:
            print(f"Error loading template file {templates_path}: {e}")
    
    elif os.path.isdir(templates_path):
        # Recursively find all template files in directory and subdirectories
        for root, dirs, files in os.walk(templates_path):
            for file in files:
                if file.endswith('.txt') or file.endswith('.template'):
                    file_path = os.path.join(root, file)
                    # Create template ID based on relative path
                    relative_path = os.path.relpath(file_path, templates_path)
                    template_id = relative_path.replace(os.sep, '_').replace('.txt', '').replace('.template', '')
                    
                    try:
                        with open(file_path, 'r', encoding='utf-8') as f:
                            templates[template_id] = f.read().strip()
                        print(f"Loaded template: {template_id} from {relative_path}")
                    except Exception as e:
                        print(f"Error loading template file {file_path}: {e}")
    
    else:
        print(f"Warning: {templates_path} is neither a file nor a directory")
    
    return templates

# Main analyze interaction function
@app.post("/analyze_interaction")
async def analyze_interaction(request: InteractionAnalysisRequest):
    """
    Analyze log files for cross-component interactions using context-aware LLM.
    """
    try:
        # Initialize LLM with GPT-4o for better analysis
        llm = ChatOpenAI(
            model="gpt-4o",
            temperature=0,
            api_key=os.getenv("OPENAI_API_KEY")
        )

        # Create a new conversation memory for context awareness
        conversation_memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        
        # Initialize with system message
        system_message = SystemMessage(
            content=(
                "You are a log analysis expert that helps detect cross-component issues. "
                "Cross-component refers to different frameworks, may include Hive, Spark, Flink, Hadoop etc. Your task is to:\n"
                "1. Analyze log files to identify cross-component interactions via resource utilization.\n"
                "2. Maintain context from previous messages to build a comprehensive understanding."
            )
        )
        conversation_memory.chat_memory.add_message(system_message)
        
        # Use provided log files or default ones
        log_files = request.log_files
        if log_files is None:
            log_files = [
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hadoop_namenode.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hadoop_datanode.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_job_log.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_log.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_cli_terminal.log"
            ]
        
        print(f"\nProcessing {len(log_files)} log files:")
        for f in log_files:
            print(f"  - {f}")
        
        # Step 1: Feed logs into the LLM in blocks
        logs = [("Cross-Component log", log_files)]
        
        for log_type, log_paths in logs:
            log_blocks = partition_log_into_blocks(log_paths)
            
            for block_index, log_block in enumerate(log_blocks):
                if block_index == len(log_blocks) - 1:
                    prompt = (
                        f"{log_block}\n\n"
                        f"I have sent the final log block. I'll give you some templates."
                    )
                elif block_index == 0:
                    prompt = (
                        f"The following are {log_type}.\n\n"
                        f"I may send the log in multiple parts. Please respond only after I indicate that the final part has been provided.\n\n"
                        f"{log_block}"
                    )
                else:
                    prompt = (
                        f"{log_block}\n\n"
                        f"Let me continue sending the log in blocks. Please wait for my signal before responding."
                    )
                
                # Add to memory and get response with context
                conversation_memory.chat_memory.add_user_message(prompt)
                messages = conversation_memory.chat_memory.messages
                response = llm.invoke(messages)
                conversation_memory.chat_memory.add_ai_message(response.content)
        
        # Step 2: Find all interaction pairs (component_a, component_b)
        interaction_task = (
            f"Construct cross-component components interaction relationship graph from logs and return a JSON file that describes the interaction relationships.\n"
            "refers to different framework, may include Hive, Spark, Flink, Hadoop etc.\n"
            f"Instructions for constructing the graph:\n"
            f"1. Two components have an interaction relationship only if:\n"
            f"  1.1 [component_A] directly interacts with a resource that [component_B] also utilizes. \n"
            f"  1.2 [component_A] invokes [component_B], which utilizes the same resource. \n"
            f"For each object in the JSON file, the format of the file will be as follows: \n"
            "{ [component_A]: [component_B] }\n"
            f"   - If a specific interaction relationship exists, provide regular expressions that can help developers extract the corresponding log lines. \n"
            f"   - Describe your reasoning process for constructing the graph. \n"
            f"   - Specify any assumptions made during the process. \n"
            f"Please ensure the output is in a structured JSON format. \n"
        )
        
        # Get response with full context
        conversation_memory.chat_memory.add_user_message(interaction_task)
        messages = conversation_memory.chat_memory.messages
        interaction_response = llm.invoke(messages)
        conversation_memory.chat_memory.add_ai_message(interaction_response.content)
        
        interaction_pairs = interaction_response.content
        print(f"========= Interactions Response: ======== \n {interaction_pairs}")
        
        # Step 3: Dispatch interaction pairs to three categories
        dispatched_interactions = await pattern_dispatcher(llm, interaction_pairs, conversation_memory)
        print(f"========= Dispatched Interaction Response: ======== \n {dispatched_interactions}")
        
        # Save results
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        results_dir = "interaction_analysis_results"
        os.makedirs(results_dir, exist_ok=True)
        
        # Save interaction analysis results
        with open(f"{results_dir}/{timestamp}_analysis.json", "w") as f:
            json.dump({
                "interaction_pairs": interaction_pairs,
                "dispatched_interactions": dispatched_interactions,
                "log_files": log_files
            }, f, indent=2)
        
        return InteractionAnalysisResponse(
            interaction_pairs=interaction_pairs,
            dispatched_interactions=dispatched_interactions,
            success=True,
            message=f"Analysis completed successfully. Results saved to {results_dir}/{timestamp}_analysis.json"
        )
        
    except Exception as e:
        print(f"Error in analyze_interaction: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# Main diagnose function
@app.post("/diagnose")
async def diagnose(request: DiagnoseRequest):
    """
    Diagnose log files using templates to detect cross-component issues.
    """
    try:
        # Initialize LLM with GPT-4o for better analysis
        llm = ChatOpenAI(
            model="gpt-4o",
            temperature=0,
            api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Create a new conversation memory for context awareness
        conversation_memory = ConversationBufferMemory(
            memory_key="chat_history",
            return_messages=True
        )
        
        # Initialize with system message
        system_message = SystemMessage(
            content=(
                "You are a log analysis expert that helps detect cross-component issues,"
                "cross-component refers to different framework, such as Hive, Spark, Flink, Hadoop. Your task is to:\n"
                "1. Analyze log files to identify cross-component interaction\n"
                "2. For each feeded template, fill in blanks([]) based on context from the logs\n"
                "3. Write general template that can be applied to similar cases if current templates can't work\n"
                "4. Always provide clear reasoning for your conclusions"
            )
        )
        conversation_memory.chat_memory.add_message(system_message)
        
        # Use provided log files or default ones
        log_files = request.log_files
        if log_files is None:
            log_files = [
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hadoop_namenode.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hadoop_datanode.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_job_log.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_log.log",
                "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_cli_terminal.log"
            ]
        
        # Use provided templates path or default
        templates_path = request.templates_path
        if templates_path is None:
            templates_path = "/homes/gws/kanzhu/furina/furina/agents/template/"
        
        print(f"\nProcessing {len(log_files)} log files:")
        for f in log_files:
            print(f"  - {f}")
        
        # Step 1: Feed logs into the LLM in blocks
        logs = [("Cross-Component log", log_files)]
        
        for log_type, log_paths in logs:
            log_blocks = partition_log_into_blocks(log_paths)
            
            for block_index, log_block in enumerate(log_blocks):
                if block_index == len(log_blocks) - 1:
                    prompt = (
                        f"{log_block}\n\n"
                        f"I have sent the final log block. I'll give you some templates."
                    )
                elif block_index == 0:
                    prompt = (
                        f"The following are {log_type}.\n\n"
                        f"I may send the log in multiple parts. Please respond only after I indicate that the final part has been provided.\n\n"
                        f"{log_block}"
                    )
                else:
                    prompt = (
                        f"{log_block}\n\n"
                        f"Let me continue sending the log in blocks. Please wait for my signal before responding."
                    )
                
                # Add to memory and get response with context
                conversation_memory.chat_memory.add_user_message(prompt)
                messages = conversation_memory.chat_memory.messages
                response = llm.invoke(messages)
                conversation_memory.chat_memory.add_ai_message(response.content)
        
        # Step 2: Process templates and fill in blanks
        templates = load_templates_recursive(templates_path)
        if not templates:
            print(f"Warning: No templates found at {templates_path}")
            return DiagnoseResponse(
                results={},
                success=False,
                message=f"No templates found at {templates_path}"
            )
        
        print(f"\nProcessing {len(templates)} templates...")
        results = defaultdict(list)
        
        for template_id, template_content in templates.items():
            print(f"\nAnalyzing template: {template_id}")
            task = (
                f"In order to find cross-component issues from logs. Here is a template that may match with the root cause, try to fill blanks in the template based on the logs you've analyzed:\n\n"
                f"Template ID: {template_id}\n"
                f"Template Content:\n{template_content}\n\n"
                f"Instructions:\n"
                f"1. Fill in each blank (marked with []) based on evidence from the logs\n"
                f"2. If you cannot fill in each blank based on your analysis from the logs, fill in 'unknown'\n"
                f"3. After filling the template, provide a detailed explanation for each filled blank:\n"
                f"   - Which specific log lines provided the evidence\n"
                f"   - If specific log lines exist, write regular expressions that can help developers extract specific log lines\n"
                f"   - Your reasoning process\n"
                f"   - Any assumptions you made\n"
                f"4. If you cannot provide specific log lines to explain you fill a blank, still fill it with 'unknown' \n"
                f"5. If you think there are multiple ways to fill the template, please list all filled versions for the template. \n\n"
                f"Finally please provide:\n"
                f"1. The completed template with all blanks filled\n"
                f"2. A reasoning section explaining each filled value"
            )
            
            # Get response with context
            conversation_memory.chat_memory.add_user_message(task)
            messages = conversation_memory.chat_memory.messages
            response = llm.invoke(messages)
            conversation_memory.chat_memory.add_ai_message(response.content)
            
            print(f"=== Template {template_id} Analysis result: ===\n {response.content}")
            results[template_id].append(response.content)
        
        # Save results
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        results_dir = "diagnosis_results"
        os.makedirs(results_dir, exist_ok=True)
        
        # Save diagnosis results
        with open(f"{results_dir}/{timestamp}_diagnosis.json", "w") as f:
            json.dump({
                "results": dict(results),
                "log_files": log_files,
                "templates_path": templates_path
            }, f, indent=2)
        
        return DiagnoseResponse(
            results=dict(results),
            success=True,
            message=f"Diagnosis completed successfully. Results saved to {results_dir}/{timestamp}_diagnosis.json"
        )
        
    except Exception as e:
        print(f"Error in diagnose: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 