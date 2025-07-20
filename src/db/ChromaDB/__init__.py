"""
ChromaDB Integration Package

This package contains ChromaDB integration components for the GSoC Cohort Discovery Chatbot.
"""

# Apply SQLite fix before any ChromaDB imports
try:
    from .sqlite_fix import apply_sqlite_fix
    apply_sqlite_fix()
except Exception as e:
    print(f"⚠️  SQLite fix could not be applied: {e}")

def get_chromadb_manager():
    """
    Smart ChromaDB manager factory that handles various error conditions
    """
    try:
        # Try to import real ChromaDB
        from .chroma_manager import ChromaDBManager
        print("[INFO] Using real ChromaDB with vector storage")
        return ChromaDBManager
    except ImportError as e:
        if "chromadb" in str(e):
            print("[WARNING]  ChromaDB package not installed, using mock version")
        elif "sqlite3" in str(e):
            print("[WARNING]  SQLite version too old for ChromaDB, using mock version")
        else:
            print(f"[WARNING]  ChromaDB import error: {e}, using mock version")
        
        from .chroma_manager_mock import ChromaDBManager
        return ChromaDBManager
    except Exception as e:
        print(f"[WARNING]  Unexpected ChromaDB error: {e}, using mock version")
        from .chroma_manager_mock import ChromaDBManager
        return ChromaDBManager

# Export the factory function
ChromaDBManager = get_chromadb_manager()

__all__ = ['ChromaDBManager'] 