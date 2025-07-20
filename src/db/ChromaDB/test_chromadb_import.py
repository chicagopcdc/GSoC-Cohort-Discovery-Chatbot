#!/usr/bin/env python3
"""
Test script to verify ChromaDB imports work correctly
"""

try:
    print("Testing ChromaDB imports...")
    
    # Test importing ChromaDBManager from the new location
    from ChromaDB.chroma_manager import ChromaDBManager
    print("✅ ChromaDBManager import successful")
    
    # Test importing from the package
    from ChromaDB import ChromaDBManager as Manager2
    print("✅ Package import successful")
    
    print("\n🎉 All imports successful! ChromaDB integration is ready.")
    print("\nTo use ChromaDB features:")
    print("1. Install dependencies: pip install chromadb>=0.4.15 langchain-community>=0.0.10")
    print("2. Set OPENAI_API_KEY in .env file")
    print("3. Run: chainlit run chainlit_app.py")
    print("4. Use utils: python ChromaDB/chroma_utils.py --help")
    
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("\nPlease install missing dependencies:")
    print("pip install chromadb>=0.4.15 langchain-community>=0.0.10")
except Exception as e:
    print(f"❌ Unexpected error: {e}") 