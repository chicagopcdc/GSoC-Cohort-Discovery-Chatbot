"""
SQLite3 version fix for ChromaDB compatibility

This module provides a workaround for systems with older SQLite3 versions
that don't meet ChromaDB's minimum requirements (>=3.35.0).
"""

import sys
import sqlite3

def apply_sqlite_fix():
    """
    Apply SQLite3 fix by replacing system sqlite3 with pysqlite3-binary
    
    Returns:
        bool: True if fix was applied successfully, False otherwise
    """
    try:
        # Check current SQLite version
        current_version = sqlite3.sqlite_version
        print(f"Current SQLite version: {current_version}")
        
        # Parse version
        version_parts = [int(x) for x in current_version.split('.')]
        required = [3, 35, 0]
        
        if version_parts >= required:
            print("[INFO] SQLite version is already sufficient")
            return True
        
        print(f"[WARNING]  SQLite version {current_version} is too old, applying fix...")
        
        # Apply the fix: replace sqlite3 with pysqlite3
        __import__('pysqlite3')
        sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
        
        # Verify the fix worked
        import sqlite3 as new_sqlite3
        new_version = new_sqlite3.sqlite_version
        print(f"[INFO] SQLite fix applied! New version: {new_version}")
        
        return True
        
    except ImportError as e:
        print(f"[Error] Failed to import pysqlite3: {e}")
        print("[INFO] Install with: pip install pysqlite3-binary")
        return False
    except Exception as e:
        print(f"[ERROR] SQLite fix failed: {e}")
        return False

def get_sqlite_info():
    """
    Get information about current SQLite installation
    
    Returns:
        dict: SQLite version information
    """
    try:
        return {
            "version": sqlite3.sqlite_version,
            "library_version": sqlite3.version,
            "threadsafety": sqlite3.threadsafety,
            "is_compatible": [int(x) for x in sqlite3.sqlite_version.split('.')] >= [3, 35, 0]
        }
    except Exception as e:
        return {"error": str(e)}

# Auto-apply fix when module is imported
if __name__ != "__main__":
    apply_sqlite_fix()