#!/usr/bin/env python3
"""
Test which ChromaDB version is being used (real or mock)
"""

print("=== Testing ChromaDB Version ===")

# Test 1: Check if chromadb package is installed
try:
    import chromadb
    print("✅ ChromaDB package is installed")
    print(f"   Version: {chromadb.__version__}")
except ImportError:
    print("❌ ChromaDB package is NOT installed")

# Test 2: Check SQLite version
import sqlite3
print(f"\n✅ SQLite version: {sqlite3.sqlite_version}")
print(f"   Required: >= 3.35.0")
print(f"   Status: {'OK' if sqlite3.sqlite_version >= '3.35.0' else 'Too old'}")

# Test 3: Check which ChromaDB manager is being used
print("\n=== Checking ChromaDB Manager ===")
from db.ChromaDB import ChromaDBManager

# Create an instance
manager = ChromaDBManager()

# Check if it's the mock version
if hasattr(manager, 'mock_storage'):
    print("⚠️  Using MOCK ChromaDB Manager")
    print("   - No actual vector storage")
    print("   - Limited functionality")
    print("   - Good for testing without dependencies")
else:
    print("✅ Using REAL ChromaDB Manager")
    print("   - Full vector storage support")
    print("   - All features available")

# Get statistics
stats = manager.get_statistics()
print(f"\n📊 Statistics:")
for key, value in stats.items():
    print(f"   {key}: {value}")

print("\n=== Conclusion ===")
if stats.get('is_mock', False):
    print("🔸 chroma_manager_mock.py IS being used")
    print("   This is useful when:")
    print("   - ChromaDB is not installed")
    print("   - SQLite version is too old")
    print("   - You want to test without dependencies")
    print("\n   To use real ChromaDB:")
    print("   1. pip install chromadb==0.4.15")
    print("   2. Ensure SQLite >= 3.35.0 (built‑in sqlite3 is fine on modern Python)")
else:
    print("🔸 chroma_manager_mock.py is NOT being used")
    print("   The real ChromaDB is active and working!")
    print("   The mock version serves as a fallback.") 