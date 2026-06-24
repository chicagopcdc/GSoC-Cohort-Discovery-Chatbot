#!/usr/bin/env python3
"""
Setup test script for GSoC Cohort Discovery Chatbot
Tests all dependencies and configurations
"""

import sys
import os
import sqlite3

def test_python_version():
    """Test Python version"""
    print(f"Python version: {sys.version}")
    if sys.version_info < (3, 8):
        print("❌ Python 3.8+ required")
        return False
    print("✅ Python version OK")
    return True

def test_sqlite_version():
    """Test SQLite version with automatic fix"""
    try:
        # Check initial version
        initial_version = sqlite3.sqlite_version
        print(f"Initial SQLite version: {initial_version}")
        
        # Parse version string
        version_parts = [int(x) for x in initial_version.split('.')]
        required = [3, 35, 0]
        
        if version_parts >= required:
            print("✅ SQLite version is already sufficient")
            return True
        
        # Try to apply fix
        print(f"⚠️  SQLite {'.'.join(map(str, required))}+ required, found {initial_version}")
        print("🔧 Attempting to apply SQLite fix...")
        
        try:
            # Apply your fix
            __import__('pysqlite3')
            import sys
            sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
            
            # Re-import sqlite3 to get new version
            import sqlite3 as new_sqlite3
            new_version = new_sqlite3.sqlite_version
            print(f"✅ SQLite fix applied! New version: {new_version}")
            return True
            
        except ImportError:
print("⚠️  pysqlite3-binary not installed; continuing with built-in sqlite3")
            return False
            
    except Exception as e:
        print(f"❌ SQLite test failed: {e}")
        return False

def test_basic_imports():
    """Test basic required imports"""
    imports_to_test = [
        ('chainlit', 'chainlit'),
        ('langchain_openai', 'langchain-openai'),
        ('dotenv', 'python-dotenv'),
        ('fastapi', 'fastapi'),
        ('uvicorn', 'uvicorn'),
        ('pydantic', 'pydantic'),
    ]
    
    all_ok = True
    for module, package in imports_to_test:
        try:
            __import__(module)
            print(f"✅ {package}")
        except ImportError:
            print(f"❌ {package} not installed")
            all_ok = False
    
    return all_ok

def test_chromadb_imports():
    """Test ChromaDB related imports"""
    try:
        from ChromaDB import ChromaDBManager
        print("✅ ChromaDB integration loaded successfully")
        
        # Try to create an instance
        manager = ChromaDBManager()
        print("✅ ChromaDB manager instance created")
        
        # Check if it's mock or real
        stats = manager.get_statistics()
        if stats.get('is_mock'):
            print("ℹ️  Using mock ChromaDB (data stored in memory)")
        else:
            print("ℹ️  Using real ChromaDB (data persisted to disk)")
        
        return True
    except Exception as e:
        print(f"❌ ChromaDB test failed: {e}")
        return False

def test_environment():
    """Test environment variables"""
    openai_key = os.getenv('OPENAI_API_KEY')
    if openai_key:
        print("✅ OPENAI_API_KEY is set")
        return True
    else:
        print("⚠️  OPENAI_API_KEY not set in environment")
        print("💡 Create a .env file with: OPENAI_API_KEY=your_key_here")
        return False

def test_project_files():
    """Test required project files"""
    required_files = [
        'chainlit_app.py',
        'schema_parser.py',
        'query_builder.py',
        'context_manager.py',
        'prompt_builder.py',
        'pcdc-schema-prod-20250114.json'
    ]
    
    all_ok = True
    for file in required_files:
        if os.path.exists(file):
            print(f"✅ {file}")
        else:
            print(f"❌ {file} missing")
            all_ok = False
    
    return all_ok

def main():
    print("🔍 GSoC Cohort Discovery Chatbot - Setup Test")
    print("=" * 50)
    
    tests = [
        ("Python Version", test_python_version),
        ("SQLite Version", test_sqlite_version),
        ("Basic Dependencies", test_basic_imports),
        ("ChromaDB Integration", test_chromadb_imports),
        ("Environment Variables", test_environment),
        ("Project Files", test_project_files),
    ]
    
    all_passed = True
    for test_name, test_func in tests:
        print(f"\n🧪 Testing {test_name}:")
        print("-" * 30)
        result = test_func()
        all_passed = all_passed and result
    
    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All tests passed! You can start the application with:")
        print("   chainlit run chainlit_app.py")
        print("   or")
        print("   ./start_with_chromadb.sh")
    else:
        print("⚠️  Some tests failed. Please fix the issues above.")
        print("\n💡 Quick fix commands:")
        print("  pip install -r requirements.txt")
        print("  echo 'OPENAI_API_KEY=your_key_here' > .env")
    
    print(f"\nTest result: {'PASS' if all_passed else 'FAIL'}")
    return 0 if all_passed else 1

if __name__ == "__main__":
    sys.exit(main()) 