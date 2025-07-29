#!/usr/bin/env python3
"""
Simple ChromaDB query script
"""

import sys
import os

# Apply SQLite fix first
try:
    __import__('pysqlite3')
    sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
    print("[Info] SQLite fix applied")
except ImportError:
    print("[Warning] pysqlite3 not available, using system SQLite")

try:
    import chromadb
    from chromadb.config import Settings
    
    def query_chromadb():
        print("🔍 Querying ChromaDB")
        print("=" * 40)
        
        # Initialize client
        client_settings = Settings(
            persist_directory="./chroma_db",
            is_persistent=True
        )
        
        client = chromadb.Client(client_settings)
        
        # Get collection
        try:
            collection = client.get_collection("llm_responses")
            print(f"✅ Found collection: {collection.name}")
        except Exception as e:
            print(f"❌ Collection not found: {e}")
            collections = client.list_collections()
            print(f"Available collections: {[c.name for c in collections]}")
            return
        
        # Get basic stats
        count = collection.count()
        print(f"📊 Total documents: {count}")
        
        if count == 0:
            print("No documents found in collection")
            return
        
        # Get all documents
        print("\n📄 All Documents:")
        print("-" * 30)
        
        results = collection.get(
            include=["documents", "metadatas"]
        )
        
        for i, doc in enumerate(results['documents'], 1):
            print(f"\n📄 Document {i}:")
            print(f"Content: {doc[:200]}..." if len(doc) > 200 else f"Content: {doc}")
            
            if results['metadatas'] and i-1 < len(results['metadatas']):
                metadata = results['metadatas'][i-1]
                print("Metadata:")
                for key, value in metadata.items():
                    print(f"  {key}: {value}")
        
        # Simple search if user provides query
        if len(sys.argv) > 1:
            query_text = " ".join(sys.argv[1:])
            print(f"\n🔍 Searching for: '{query_text}'")
            print("-" * 30)
            
            # Simple text matching since we don't have embeddings setup
            matching_docs = []
            for i, doc in enumerate(results['documents']):
                if query_text.lower() in doc.lower():
                    matching_docs.append((i, doc, results['metadatas'][i] if results['metadatas'] else {}))
            
            if matching_docs:
                for idx, (doc_idx, doc, metadata) in enumerate(matching_docs, 1):
                    print(f"\n📄 Match {idx}:")
                    print(f"Content: {doc}")
                    print("Metadata:")
                    for key, value in metadata.items():
                        print(f"  {key}: {value}")
            else:
                print("No matching documents found")
    
    if __name__ == "__main__":
        query_chromadb()

except ImportError as e:
    print(f"❌ ChromaDB not available: {e}")
    print("💡 Install with: pip install chromadb==0.4.15 pysqlite3-binary")
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc() 