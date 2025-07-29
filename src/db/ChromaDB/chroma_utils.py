#!/usr/bin/env python3
"""
ChromaDB Utility Script for managing the vector database
"""

import argparse
import json
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from ChromaDB.chroma_manager import ChromaDBManager
except ImportError:
    # Fallback for direct execution
    from chroma_manager import ChromaDBManager

def search_responses(chroma_manager, query, k=5):
    """Search for similar responses"""
    print(f"\n🔍 Searching for: '{query}'")
    print("=" * 50)
    
    results = chroma_manager.search_similar_responses(query, k)
    
    if not results:
        print("No results found.")
        return
    
    for i, result in enumerate(results, 1):
        print(f"\n📄 Result {i} (Score: {result['similarity_score']:.4f})")
        print("-" * 30)
        print(f"Content:\n{result['content']}")
        print(f"\nMetadata:")
        for key, value in result['metadata'].items():
            print(f"  {key}: {value}")

def get_session_history(chroma_manager, session_id):
    """Get all responses for a specific session"""
    print(f"\n📝 Session History for: {session_id}")
    print("=" * 50)
    
    history = chroma_manager.get_session_history(session_id)
    
    if not history:
        print("No history found for this session.")
        return
    
    for i, item in enumerate(history, 1):
        print(f"\n📄 Entry {i}")
        print("-" * 30)
        print(f"Content:\n{item['content']}")
        print(f"\nMetadata:")
        for key, value in item['metadata'].items():
            print(f"  {key}: {value}")

def show_statistics(chroma_manager):
    """Show ChromaDB statistics"""
    print("\n📊 ChromaDB Statistics")
    print("=" * 50)
    
    stats = chroma_manager.get_statistics()
    
    for key, value in stats.items():
        print(f"{key}: {value}")

def clear_database(chroma_manager):
    """Clear all data from ChromaDB"""
    confirm = input("\n⚠️  Are you sure you want to clear all data? (yes/no): ")
    
    if confirm.lower() == 'yes':
        success = chroma_manager.clear_collection()
        if success:
            print("✅ Database cleared successfully.")
        else:
            print("❌ Failed to clear database.")
    else:
        print("Operation cancelled.")

def export_data(chroma_manager, output_file):
    """Export all data to JSON file"""
    print(f"\n💾 Exporting data to: {output_file}")
    
    try:
        # Get all documents
        collection = chroma_manager.collection
        results = collection.get(include=["documents", "metadatas"])
        
        export_data = {
            "exported_at": datetime.now().isoformat(),
            "total_documents": len(results['documents']),
            "documents": []
        }
        
        for i, doc in enumerate(results['documents']):
            export_data["documents"].append({
                "content": doc,
                "metadata": results['metadatas'][i] if results['metadatas'] else {}
            })
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Exported {len(results['documents'])} documents successfully.")
        
    except Exception as e:
        print(f"❌ Export failed: {str(e)}")

def main():
    parser = argparse.ArgumentParser(description="ChromaDB Utility Tool")
    parser.add_argument("--search", "-s", type=str, help="Search for similar responses")
    parser.add_argument("--session", type=str, help="Get session history")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--clear", action="store_true", help="Clear all data")
    parser.add_argument("--export", type=str, help="Export data to JSON file")
    parser.add_argument("--limit", "-k", type=int, default=5, help="Number of search results")
    
    args = parser.parse_args()
    
    # Initialize ChromaDB manager
    try:
        chroma_manager = ChromaDBManager()
    except Exception as e:
        print(f"❌ Failed to initialize ChromaDB: {str(e)}")
        return
    
    # Execute commands
    if args.search:
        search_responses(chroma_manager, args.search, args.limit)
    elif args.session:
        get_session_history(chroma_manager, args.session)
    elif args.stats:
        show_statistics(chroma_manager)
    elif args.clear:
        clear_database(chroma_manager)
    elif args.export:
        export_data(chroma_manager, args.export)
    else:
        # Interactive mode
        print("🚀 ChromaDB Interactive Mode")
        print("=" * 50)
        
        while True:
            print("\nOptions:")
            print("1. Search responses")
            print("2. Get session history")
            print("3. Show statistics")
            print("4. Clear database")
            print("5. Export data")
            print("6. Exit")
            
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == "1":
                query = input("Enter search query: ").strip()
                if query:
                    search_responses(chroma_manager, query, args.limit)
            elif choice == "2":
                session_id = input("Enter session ID: ").strip()
                if session_id:
                    get_session_history(chroma_manager, session_id)
            elif choice == "3":
                show_statistics(chroma_manager)
            elif choice == "4":
                clear_database(chroma_manager)
            elif choice == "5":
                filename = input("Enter export filename (e.g., export.json): ").strip()
                if filename:
                    export_data(chroma_manager, filename)
            elif choice == "6":
                print("👋 Goodbye!")
                break
            else:
                print("❌ Invalid choice. Please try again.")

if __name__ == "__main__":
    main() 