#!/usr/bin/env python3
"""
Direct ChromaDB SQLite reader
Read ChromaDB data without requiring ChromaDB package
"""

import sqlite3
import json
import sys
from datetime import datetime

def read_chromadb_sqlite():
    """Read ChromaDB data directly from SQLite database"""
    print("🔍 Reading ChromaDB Data (Direct SQLite)")
    print("=" * 50)
    
    db_path = "./chroma_db/chroma.sqlite3"
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get table names
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📊 Database tables: {[table[0] for table in tables]}")
        
        # Get collections
        cursor.execute("SELECT * FROM collections")
        collections = cursor.fetchall()
        
        if collections:
            print(f"\n📂 Collections:")
            for collection in collections:
                print(f"  ID: {collection[0]}, Name: {collection[1]}")
        
        # Get embeddings (documents)
        cursor.execute("""
            SELECT e.id, e.embedding, e.document, e.metadata, c.name as collection_name
            FROM embeddings e
            JOIN collections c ON e.collection_id = c.id
        """)
        
        embeddings = cursor.fetchall()
        
        if embeddings:
            print(f"\n📄 Documents ({len(embeddings)} found):")
            print("-" * 40)
            
            for i, (doc_id, embedding, document, metadata, collection_name) in enumerate(embeddings, 1):
                print(f"\n📄 Document {i}:")
                print(f"  ID: {doc_id}")
                print(f"  Collection: {collection_name}")
                print(f"  Content: {document}")
                
                if metadata:
                    try:
                        metadata_dict = json.loads(metadata)
                        print("  Metadata:")
                        for key, value in metadata_dict.items():
                            print(f"    {key}: {value}")
                    except:
                        print(f"  Metadata (raw): {metadata}")
        else:
            print("\n📄 No documents found")
        
        # Search functionality
        if len(sys.argv) > 1:
            search_term = " ".join(sys.argv[1:]).lower()
            print(f"\n🔍 Searching for: '{search_term}'")
            print("-" * 40)
            
            found = False
            for i, (doc_id, embedding, document, metadata, collection_name) in enumerate(embeddings, 1):
                if search_term in document.lower():
                    found = True
                    print(f"\n✅ Match found in Document {i}:")
                    print(f"  Content: {document}")
                    if metadata:
                        try:
                            metadata_dict = json.loads(metadata)
                            print("  Metadata:")
                            for key, value in metadata_dict.items():
                                print(f"    {key}: {value}")
                        except:
                            print(f"  Metadata: {metadata}")
            
            if not found:
                print("❌ No matches found")
        
        conn.close()
        
    except sqlite3.Error as e:
        print(f"❌ SQLite error: {e}")
    except FileNotFoundError:
        print(f"❌ Database file not found: {db_path}")
        print("💡 Make sure ChromaDB has been initialized and data has been stored")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    read_chromadb_sqlite() 