#!/usr/bin/env python3
"""
Test script for ChromaDB history features
"""

from db.ChromaDB.chromadb_history_reader import ChromaDBHistoryReader
import json

def test_history_reader():
    print("🧪 Testing ChromaDB History Reader")
    print("=" * 50)
    
    try:
        # Initialize history reader
        reader = ChromaDBHistoryReader()
        print("✅ History reader initialized")
        
        # Test get all sessions
        print("\n📂 Testing get_all_sessions():")
        sessions = reader.get_all_sessions()
        print(f"Found {len(sessions)} sessions:")
        for session in sessions[:3]:  # Show first 3
            print(f"  - {session['display_name']}")
            print(f"    Last: {session.get('last_message', 'Unknown')}")
        
        # Test get recent history
        print("\n📋 Testing get_recent_history():")
        recent = reader.get_recent_history(limit=2)
        print(f"Found {len(recent)} recent messages:")
        for i, msg in enumerate(recent, 1):
            print(f"\n  Message {i}:")
            print(f"    User Query: {msg.get('user_query', 'N/A')[:50]}...")
            print(f"    Timestamp: {msg.get('timestamp', 'N/A')}")
            print(f"    Session: {msg.get('session_id', 'N/A')[:8]}...")
        
        # Test search functionality
        print("\n🔍 Testing search_history():")
        search_results = reader.search_history("query", limit=2)
        print(f"Found {len(search_results)} search results for 'query':")
        for i, msg in enumerate(search_results, 1):
            print(f"\n  Result {i}:")
            print(f"    User Query: {msg.get('user_query', 'N/A')}")
            print(f"    Has GraphQL: {'Yes' if msg.get('graphql_query') else 'No'}")
        
        # Test session history if we have sessions
        if sessions:
            print(f"\n📝 Testing get_session_history() for first session:")
            session_id = sessions[0]['session_id']
            session_history = reader.get_session_history(session_id)
            print(f"Found {len(session_history)} messages in session {session_id[:8]}...")
            for i, msg in enumerate(session_history, 1):
                print(f"  Message {i}: {msg.get('user_query', 'N/A')[:30]}...")
        
        print("\n✅ All history reader tests completed successfully!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()

def test_formatting():
    print("\n🎨 Testing message formatting:")
    
    # Mock message data
    mock_msg = {
        'user_query': 'Get all users',
        'graphql_query': 'query { users { name email } }',
        'variables': '{}',
        'explanation': 'This query retrieves all users with their names and emails',
        'timestamp': '2025-01-24T20:30:00',
        'session_id': 'abc123def456'
    }
    
    # Import the formatting function
    import sys
    sys.path.append('.')
    
    try:
        from furina.langchain_demo.GSOC_COHORT_CLONE.src.frontend.chainlit_app import format_history_message
        formatted = format_history_message(mock_msg)
        print("Formatted message:")
        print("-" * 30)
        print(formatted)
        print("-" * 30)
        print("✅ Formatting test passed")
    except Exception as e:
        print(f"❌ Formatting test failed: {e}")

if __name__ == "__main__":
    test_history_reader()
    test_formatting() 