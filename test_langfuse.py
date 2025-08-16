#!/usr/bin/env python3
"""
Simple test script to verify Langfuse configuration and connectivity.
Updated for Langfuse SDK v3.x
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def test_langfuse_config():
    """Test Langfuse configuration and basic connectivity."""
    
    print("🔍 Testing Langfuse Configuration...")
    print("-" * 50)
    
    # Check environment variables
    secret_key = os.getenv("LANGFUSE_SECRET_KEY")
    public_key = os.getenv("LANGFUSE_PUBLIC_KEY") 
    host = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
    
    print(f"Secret Key: {'✅ Set' if secret_key else '❌ Missing'}")
    print(f"Public Key: {'✅ Set' if public_key else '❌ Missing'}")
    print(f"Host: {host}")
    
    if not secret_key or not public_key:
        print("\n❌ Missing required environment variables!")
        return False
    
    # Test Langfuse import and connection
    try:
        from langfuse import get_client
        print("✅ Langfuse import successful")
        
        # Initialize client using get_client()
        langfuse = get_client()
        
        # Test basic functionality - auth check
        auth_result = langfuse.auth_check()
        if auth_result:
            print("✅ Langfuse authentication successful")
        else:
            print("❌ Langfuse authentication failed")
            return False
        
        # Test creating a simple trace using the new API
        with langfuse.start_as_current_span(name="test_trace") as span:
            span.update(
                input={"test": "Hello Langfuse!"},
                metadata={"test": True}
            )
            
            # Test creating a generation within the trace
            with langfuse.start_as_current_generation(
                name="test_generation",
                model="test-model",
                input="Test input"
            ) as generation:
                generation.update(output="Test output")
        
        # Flush to ensure data is sent
        langfuse.flush()
        
        print("✅ Langfuse client initialized successfully")
        print("✅ Test trace created successfully")
        print("\n🎉 Langfuse is properly configured and working!")
        
        return True
        
    except ImportError:
        print("❌ Langfuse not installed")
        return False
    except Exception as e:
        print(f"❌ Error testing Langfuse: {e}")
        return False

def test_tracker_integration():
    """Test the custom tracker integration."""
    
    print("\n🔍 Testing Tracker Integration...")
    print("-" * 50)
    
    try:
        from src.backend.services.langfuse_tracker import tracker
        
        # Test tracker initialization
        if tracker.langfuse:
            print("✅ Tracker initialized successfully")
            
            # Test basic tracking flow
            tracker.start_trace("Test query", "test-session-123")
            
            tracker.track_step(
                "test_step",
                {"input": "test input"},
                {"output": "test output"}
            )
            
            tracker.track_llm_call(
                "Test prompt",
                "Test response",
                "gpt-3.5-turbo"
            )
            
            tracker.end_trace(
                success=True,
                final_output={"result": "test successful"}
            )
            
            print("✅ Tracker workflow completed successfully")
            print("\n🎉 Tracker integration is working!")
            
            return True
        else:
            print("❌ Tracker not properly initialized")
            return False
            
    except Exception as e:
        print(f"❌ Error testing tracker: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Langfuse Configuration Test\n")
    
    config_ok = test_langfuse_config()
    tracker_ok = test_tracker_integration()
    
    print("\n" + "="*50)
    print("📊 Test Results:")
    print(f"Langfuse Config: {'✅ PASS' if config_ok else '❌ FAIL'}")
    print(f"Tracker Integration: {'✅ PASS' if tracker_ok else '❌ FAIL'}")
    
    if config_ok and tracker_ok:
        print("\n🎉 All tests passed! Langfuse is ready to use.")
    else:
        print("\n⚠️  Some tests failed. Please check the configuration.") 