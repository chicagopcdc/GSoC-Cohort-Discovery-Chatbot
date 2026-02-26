import requests
import json
import time

# API endpoint
API_URL = "http://localhost:8000/convert"

# Test queries
test_queries = [
    # Simple query
    "Query subjects who are multiracial and between 0 and 18 years of age",
    
    # Sample user questions
    "The INRG executive committee is working on a presentation and wants to include the number of patients represented by the INRG consortium who are included in the data portal. What number do you give the executive committee?",
    "What are the INRGSS stages represented within the data portal?",
    "INRG Stage MS is defined as patients less than 547 days old who have metastatic disease sites that are confined to the skin, liver, and/or bone marrow. Do a quality check to assess which metastatic disease sites are represented among patients with INRG Stage MS Schwannian stroma-poor neuroblastoma at initial diagnosis. Are any anatomical sites of metastatic disease that should not be included present? What is the maximum age of these patients?",
    "How many diseases are represented within the data portal?",
    "What proportion of patients with malignant peripheral nerve sheath tumor and Neurofibromatosis Type 1 within the data portal are females?",
    "Can we do survival analysis using the GraphQL interface?",
    "How among patients on study AHOD0031 with Ann Arbor Stage II disease experienced rapid early response? What disease do patients on this study have?"
]

def test_query(query, session_id=None):
    """Test a single query (must include valid backend session)"""
    try:
        if session_id is None:
            # create a temporary session for this query
            session_response = requests.post("http://localhost:8000/sessions/create")
            session_id = session_response.json().get("session_id")
        # Send request
        response = requests.post(
            API_URL,
            json={"text": query, "session_id": session_id}
        )
        
        # Check response
        if response.status_code == 200:
            result = response.json()
            print(f"Query: {query}")
            print(f"GraphQL Query:\n{result.get('query', '')}")
            print(f"Variables:\n{result.get('variables', '')}")
            print(f"Explanation:\n{result.get('explanation', '')}")
            print("-" * 50)
            return True
        else:
            print(f"Query failed: {query}")
            print(f"Error: {response.text}")
            print("-" * 50)
            return False
    except Exception as e:
        print(f"Query exception: {query}")
        print(f"Exception: {str(e)}")
        print("-" * 50)
        return False

def test_session():
    """Test session functionality"""
    try:
        # Create session
        session_response = requests.post("http://localhost:8000/sessions/create")
        session_data = session_response.json()
        session_id = session_data.get("session_id")
        
        print(f"Created session: {session_id}")
        
        # Send first query
        query1 = "Query subjects who are multiracial"
        response1 = requests.post(
            API_URL,
            json={"text": query1, "session_id": session_id}
        )
        result1 = response1.json()
        print(f"Query 1: {query1}")
        print(f"Result 1:\n{json.dumps(result1, indent=2, ensure_ascii=False)}")
        
        # Wait one second
        time.sleep(1)
        
        # Send second query, referencing the result of the first query
        query2 = "Based on the previous query, restrict age between 0 and 18 years"
        response2 = requests.post(
            API_URL,
            json={"text": query2, "session_id": session_id}
        )
        result2 = response2.json()
        print(f"Query 2: {query2}")
        print(f"Result 2:\n{json.dumps(result2, indent=2, ensure_ascii=False)}")
        
        # Delete session
        delete_response = requests.delete(f"http://localhost:8000/sessions/{session_id}")
        print(f"Deleted session: {delete_response.json()}")
        
        return True
    except Exception as e:
        print(f"Session test exception: {str(e)}")
        return False

if __name__ == "__main__":
    print("Starting query tests...")
    
    # Test all queries
    success_count = 0
    # create a single session to reuse for all queries
    session_response = requests.post("http://localhost:8000/sessions/create")
    general_session = session_response.json().get("session_id")
    for i, query in enumerate(test_queries):
        print(f"Testing query {i+1}/{len(test_queries)}")
        if test_query(query, session_id=general_session):
            success_count += 1
    
    print(f"Query tests completed: {success_count}/{len(test_queries)} successful")
    
    # verify that unauthenticated queries are rejected
    print("\n🔐 Testing unauthorized access...")
    try:
        unauth_resp = requests.post(API_URL, json={"text": "Should fail"})
        if unauth_resp.status_code == 401:
            print("✅ Unauthorized request was correctly rejected")
        else:
            print(f"❌ Unauthorized request returned status {unauth_resp.status_code}")
    except Exception as e:
        print(f"❌ Unauthorized test exception: {e}")

    # Test session functionality
    print("\nStarting session test...")
    if test_session():
        print("Session test successful")
    else:
        print("Session test failed") 