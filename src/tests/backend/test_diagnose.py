import requests
import json
import os

def test_diagnose():
    """Test the diagnose endpoint"""
    
    # API endpoint
    url = "http://localhost:8000/diagnose"
    
    # Test case 1: Use default log files and templates
    print("=== Test Case 1: Default log files and templates ===")
    payload1 = {
        "log_files": None,
        "templates_path": None,
        "session_id": "test-diagnose-1"
    }
    
    try:
        print("Sending request to diagnose endpoint...")
        response1 = requests.post(url, json=payload1, timeout=300)  # 5 minute timeout
        print(f"Status Code: {response1.status_code}")
        
        if response1.status_code == 200:
            result = response1.json()
            print("✅ Success!")
            print(f"Message: {result.get('message', 'No message')}")
            
            # Show results summary
            results = result.get('results', {})
            print(f"\nAnalyzed {len(results)} templates:")
            for template_id, responses in results.items():
                print(f"\n📋 Template: {template_id}")
                print(f"   Responses: {len(responses)} version(s)")
                # Show first 200 chars of first response
                if responses:
                    preview = responses[0][:200] + "..." if len(responses[0]) > 200 else responses[0]
                    print(f"   Preview: {preview}")
            
            # Save full result for inspection
            with open("test_diagnose_result.json", "w") as f:
                json.dump(result, f, indent=2)
            print("\n📁 Full results saved to test_diagnose_result.json")
            
        else:
            print(f"❌ Error: {response1.text}")
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
    
    print("\n" + "="*50 + "\n")
    
    # Test case 2: Custom templates path
    print("=== Test Case 2: Custom templates path ===")
    
    # You can specify a custom path if you have templates elsewhere
    custom_templates_path = "/homes/gws/kanzhu/furina/furina/agents/template/resource_leak/"
    
    payload2 = {
        "log_files": None,
        "templates_path": custom_templates_path,
        "session_id": "test-diagnose-2"
    }
    
    try:
        print(f"Testing with templates from: {custom_templates_path}")
        response2 = requests.post(url, json=payload2, timeout=300)
        print(f"Status Code: {response2.status_code}")
        
        if response2.status_code == 200:
            result = response2.json()
            print("✅ Success!")
            results = result.get('results', {})
            print(f"Analyzed {len(results)} templates from custom path")
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

if __name__ == "__main__":
    print("Testing /diagnose endpoint...")
    print("="*50)
    
    # First check if server is running
    if test_basic_connection():
        print("\n")
        test_diagnose()
    else:
        print("\n🚀 To start the server:")
        print("cd furina/CS5500/src/backend")
        print("python app.py")
        print("\nThen run this test script again.") 