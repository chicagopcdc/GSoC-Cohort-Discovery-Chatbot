#!/usr/bin/env python3
"""
Test script for Official Chainlit Chat History functionality
测试官方Chainlit聊天历史功能的脚本
"""

import os
import sys
import sqlite3
import json
from datetime import datetime
import tempfile
import shutil

def test_config_files():
    """测试配置文件是否正确设置"""
    print("🔧 Testing configuration files...")
    
    # Check config.toml
    config_path = ".chainlit/config.toml"
    if not os.path.exists(config_path):
        print("❌ Config file not found")
        return False
    
    with open(config_path, 'r') as f:
        content = f.read()
        
    # Check data persistence
    if 'enabled = true' not in content:
        print("❌ Data persistence not enabled")
        return False
    
    # Check SQLite configuration
    if 'storage_provider = "sqlite"' not in content:
        print("❌ SQLite storage not configured")
        return False
    
    print("✅ Configuration files OK")
    return True

def test_sqlite_database():
    """测试SQLite数据库连接"""
    print("🗄️  Testing SQLite database...")
    
    db_path = "chainlit.db"
    
    try:
        # Try to connect to SQLite
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if we can create a test table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id INTEGER PRIMARY KEY,
                test_data TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Insert test data
        cursor.execute("""
            INSERT INTO test_table (test_data) VALUES (?)
        """, ("Test data for official history",))
        
        # Query test data
        cursor.execute("SELECT * FROM test_table WHERE test_data = ?", 
                      ("Test data for official history",))
        result = cursor.fetchone()
        
        # Clean up
        cursor.execute("DROP TABLE test_table")
        conn.commit()
        conn.close()
        
        if result:
            print("✅ SQLite database working")
            return True
        else:
            print("❌ SQLite test data not found")
            return False
            
    except Exception as e:
        print(f"❌ SQLite error: {str(e)}")
        return False

def test_authentication_setup():
    """测试认证设置"""
    print("🔐 Testing authentication setup...")
    
    # Check if auth callback is properly defined
    try:
        # Import the main app to check auth callback
        sys.path.append('.')
        
        # Read the app file and check for auth callback
        with open('chainlit_app.py', 'r') as f:
            content = f.read()
        
        if '@cl.password_auth_callback' not in content:
            print("❌ Password auth callback not found")
            return False
        
        if 'cl.User(' not in content:
            print("❌ User object creation not found")
            return False
        
        # Requirement #10: frontend must obtain session ID from backend instead of generating its own
        if "/sessions/create" not in content:
            print("❌ Frontend does not call /sessions/create (bypassed functional #10)")
            return False
        if 'uuid.uuid4' in content and 'sessions/create' not in content:
            print("❌ Frontend appears to generate session locally instead of using API")
            return False
        
        print("✅ Authentication setup OK")
        return True
        
    except Exception as e:
        print(f"❌ Authentication test error: {str(e)}")
        return False

def test_user_credentials():
    """测试用户凭据设置"""
    print("👤 Testing user credentials...")
    
    expected_users = ["admin", "user", "demo", "guest"]
    
    try:
        with open('chainlit_app.py', 'r') as f:
            content = f.read()
        
        # Check if all expected users are in the code
        for user in expected_users:
            if f'"{user}"' not in content:
                print(f"❌ User '{user}' not found in credentials")
                return False
        
        print("✅ User credentials OK")
        return True
        
    except Exception as e:
        print(f"❌ User credentials test error: {str(e)}")
        return False

def test_session_resume_callback():
    """测试会话恢复回调"""
    print("🔄 Testing session resume callback...")
    
    try:
        with open('chainlit_app.py', 'r') as f:
            content = f.read()
        
        if '@cl.on_chat_resume' not in content:
            print("❌ Chat resume callback not found")
            return False
        
        if 'cl.PersistedThread' not in content:
            print("❌ PersistedThread not used in resume callback")
            return False
        
        print("✅ Session resume callback OK")
        return True
        
    except Exception as e:
        print(f"❌ Session resume test error: {str(e)}")
        return False

def test_dual_storage_system():
    """测试双重存储系统"""
    print("💾 Testing dual storage system...")
    
    # Check ChromaDB integration
    if not os.path.exists('ChromaDB'):
        print("❌ ChromaDB directory not found")
        return False
    
    # Check file backup system
    if not os.path.exists('chat_history'):
        print("🔧 Creating chat_history directory...")
        os.makedirs('chat_history')
    
    # Check ChromaDB manager import
    try:
        with open('chainlit_app.py', 'r') as f:
            content = f.read()
        
        if 'ChromaDBManager' not in content:
            print("❌ ChromaDB manager not imported")
            return False
        
        if 'save_to_chat_history' not in content:
            print("❌ File backup function not found")
            return False
        
        print("✅ Dual storage system OK")
        return True
        
    except Exception as e:
        print(f"❌ Dual storage test error: {str(e)}")
        return False

def test_starter_buttons():
    """测试启动按钮设置"""
    print("🎯 Testing starter buttons...")
    
    try:
        with open('chainlit_app.py', 'r') as f:
            content = f.read()
        
        required_starters = [
            "📋 Chat History",
            "🔍 Search History", 
            "📂 View Sessions"
        ]
        
        for starter in required_starters:
            if starter not in content:
                print(f"❌ Starter button '{starter}' not found")
                return False
        
        print("✅ Starter buttons OK")
        return True
        
    except Exception as e:
        print(f"❌ Starter buttons test error: {str(e)}")
        return False

def test_environment_setup():
    """测试环境设置"""
    print("🌍 Testing environment setup...")
    
    # Check .env file
    if not os.path.exists('.env'):
        print("⚠️  .env file not found, will be created on startup")
    
    # Check required Python packages
    try:
        import chainlit
        import chromadb
        import sqlite3
        print("✅ Required packages available")
        return True
        
    except ImportError as e:
        print(f"❌ Missing required package: {str(e)}")
        return False

def create_test_summary():
    """创建测试总结报告"""
    print("\n" + "="*60)
    print("📊 OFFICIAL CHAT HISTORY TEST SUMMARY")
    print("="*60)
    
    tests = [
        ("Configuration Files", test_config_files),
        ("SQLite Database", test_sqlite_database),
        ("Authentication Setup", test_authentication_setup),
        ("User Credentials", test_user_credentials),
        ("Session Resume Callback", test_session_resume_callback),
        ("Dual Storage System", test_dual_storage_system),
        ("Starter Buttons", test_starter_buttons),
        ("Environment Setup", test_environment_setup)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n🧪 {test_name}:")
        if test_func():
            passed += 1
    
    print(f"\n📈 Test Results: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests passed! Official Chat History is ready!")
        print("\n🚀 To start the application:")
        print("   ./start_with_auth.sh")
        print("\n🔑 Login credentials:")
        print("   admin / admin123")
        print("   user / password")
        print("   demo / demo123")
        print("   guest / guest")
    else:
        print("⚠️  Some tests failed. Please check the configuration.")
    
    return passed == total

def main():
    """主测试函数"""
    print("🧪 Official Chainlit Chat History Test Suite")
    print("=" * 50)
    
    # Change to project directory if needed
    if not os.path.exists('chainlit_app.py'):
        print("❌ chainlit_app.py not found. Please run from project directory.")
        sys.exit(1)
    
    # Run comprehensive tests
    success = create_test_summary()
    
    if success:
        print(f"\n✅ All systems ready at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sys.exit(0)
    else:
        print(f"\n❌ Tests failed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        sys.exit(1)

if __name__ == "__main__":
    main() 