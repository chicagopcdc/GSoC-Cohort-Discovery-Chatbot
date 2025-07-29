import requests
import json
import time
import os

def test_analyze_interaction():
    """Test the analyze_interaction endpoint"""
    
    # API endpoint
    url = "http://localhost:8000/analyze_interaction"
    
    # Test case 1: Use default log files
    print("=== Test Case 1: Default log files ===")
    payload1 = {
        "log_files": None,
        "templates_path": "./template/",
        "session_id": "test-session-1"
    }
    
    try:
        print("Sending request to analyze_interaction endpoint...")
        response1 = requests.post(url, json=payload1, timeout=300)  # 5 minute timeout
        print(f"Status Code: {response1.status_code}")
        if response1.status_code == 200:
            result = response1.json()
            print("✅ Success!")
            print(f"Message: {result.get('message', 'No message')}")
            print(f"Interaction Pairs Preview: {result.get('interaction_pairs', '')[:200]}...")
            print(f"Dispatched Interactions Preview: {result.get('dispatched_interactions', '')[:200]}...")
            
            # Save full result for inspection
            with open("test_result_full.json", "w") as f:
                json.dump(result, f, indent=2)
            print("📁 Full results saved to test_result_full.json")
            
        else:
            print(f"❌ Error: {response1.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # Test case 2: Custom log files (using our sample files)
    print("=== Test Case 2: Custom log files ===")
    custom_log_files = [
        "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hadoop_namenode.log",
        "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hadoop_datanode.log",
        "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_job_log.log",
        "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_log.log",
        "/homes/gws/kanzhu/furina/source_code/bug_logs/HIVE-3335/hive_cli_terminal.log"
    ]
    
    payload2 = {
        "log_files": custom_log_files,
        "templates_path": "./template/",
        "session_id": "test-session-2"
    }
    
    try:
        print("Sending request with custom log files...")
        response2 = requests.post(url, json=payload2, timeout=300)
        print(f"Status Code: {response2.status_code}")
        if response2.status_code == 200:
            result = response2.json()
            print("✅ Success!")
            print(f"Message: {result.get('message', 'No message')}")
        else:
            print(f"❌ Error: {response2.text}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")

def test_basic_connection():
    """Test if the server is running"""
    try:
        response = requests.get("http://localhost:8000/docs", timeout=5)
        if response.status_code == 200:
            print("✅ Server is running! You can access Swagger UI at http://localhost:8000/docs")
            return True
        else:
            print(f"❌ Server returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Cannot connect to server: {e}")
        print("Please make sure the server is running with: python app.py")
        return False

def check_log_files():
    """Check if sample log files exist"""
    log_files = [
        "sample_logs/hive_log.log",
        "sample_logs/hadoop_namenode.log"
    ]
    
    print("=== Checking Log Files ===")
    all_exist = True
    for log_file in log_files:
        if os.path.exists(log_file):
            print(f"✅ {log_file} exists")
        else:
            print(f"❌ {log_file} not found")
            all_exist = False
    
    return all_exist

if __name__ == "__main__":
    print("Testing /analyze_interaction endpoint...")
    print("="*50)
    
    # Check if log files exist
    if not check_log_files():
        print("\n⚠️  Some log files are missing. The test may still work with default files.")
    
    print("\n")
    
    # First check if server is running
    if test_basic_connection():
        print("\n")
        test_analyze_interaction()
    else:
        print("\n🚀 To start the server:")
        print("cd furina/CS5500/src/backend")
        print("python app.py")
        print("\nThen run this test script again.") 