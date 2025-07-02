"""
Mock ChromaDB Manager for testing without ChromaDB dependencies
"""

import os
import json
from datetime import datetime
from typing import Dict, List, Optional

class ChromaDBManager:
    """Mock ChromaDB Manager that doesn't require actual ChromaDB installation"""
    
    def __init__(self, persist_directory: str = "./chroma_db", collection_name: str = "llm_responses"):
        """
        Initialize mock ChromaDB manager
        
        Args:
            persist_directory: Directory to persist ChromaDB data (simulated)
            collection_name: Name of the collection
        """
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        self.mock_storage = []  # Simple in-memory storage for testing
        
        print(f"Mock ChromaDB initialized with collection: {collection_name}")
        print("⚠️  This is a mock version. Install ChromaDB for full functionality.")
    
    def store_response(self, 
                      user_query: str, 
                      llm_response: Dict, 
                      session_id: str,
                      metadata: Optional[Dict] = None) -> str:
        """
        Mock store LLM response
        
        Args:
            user_query: User's original query
            llm_response: LLM response dict containing query, variables, explanation
            session_id: Session identifier
            metadata: Additional metadata
            
        Returns:
            Document ID
        """
        try:
            # Prepare document content
            document_content = self._format_document_content(user_query, llm_response)
            
            # Prepare metadata
            doc_metadata = {
                "user_query": user_query,
                "session_id": session_id,
                "timestamp": datetime.now().isoformat(),
                "type": "llm_response"
            }
            
            # Add GraphQL specific metadata
            if isinstance(llm_response, dict):
                if llm_response.get('query'):
                    doc_metadata["has_graphql_query"] = True
                    doc_metadata["query_type"] = "graphql"
                if llm_response.get('variables'):
                    doc_metadata["has_variables"] = True
                if llm_response.get('explanation'):
                    doc_metadata["has_explanation"] = True
            
            # Add additional metadata if provided
            if metadata:
                doc_metadata.update(metadata)
            
            # Generate document ID
            doc_id = f"{session_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Store in mock storage
            self.mock_storage.append({
                "id": doc_id,
                "content": document_content,
                "metadata": doc_metadata
            })
            
            print(f"[MOCK] Stored response with ID: {doc_id}")
            return doc_id
            
        except Exception as e:
            print(f"[MOCK] Error storing response: {str(e)}")
            return None
    
    def _format_document_content(self, user_query: str, llm_response: Dict) -> str:
        """Format document content for storage"""
        content_parts = [f"User Query: {user_query}"]
        
        if isinstance(llm_response, dict):
            if llm_response.get('query'):
                content_parts.append(f"GraphQL Query: {llm_response['query']}")
            if llm_response.get('variables'):
                content_parts.append(f"Variables: {llm_response['variables']}")
            if llm_response.get('explanation'):
                content_parts.append(f"Explanation: {llm_response['explanation']}")
        else:
            content_parts.append(f"Response: {str(llm_response)}")
        
        return "\n".join(content_parts)
    
    def search_similar_responses(self, query: str, k: int = 5) -> List[Dict]:
        """
        Mock search for similar responses
        """
        print(f"[MOCK] Searching for: '{query}' (returning stored items)")
        
        # Simple text matching for mock
        results = []
        for item in self.mock_storage[-k:]:  # Return last k items
            results.append({
                "content": item["content"],
                "metadata": item["metadata"],
                "similarity_score": 0.8  # Mock score
            })
        
        return results
    
    def get_session_history(self, session_id: str) -> List[Dict]:
        """
        Mock get all responses for a specific session
        """
        print(f"[MOCK] Getting session history for: {session_id}")
        
        session_history = []
        for item in self.mock_storage:
            if item["metadata"].get("session_id") == session_id:
                session_history.append({
                    "content": item["content"],
                    "metadata": item["metadata"]
                })
        
        return session_history
    
    def get_statistics(self) -> Dict:
        """Get mock statistics"""
        return {
            "total_documents": len(self.mock_storage),
            "collection_name": self.collection_name,
            "persist_directory": self.persist_directory,
            "is_mock": True
        }
    
    def clear_collection(self) -> bool:
        """Clear mock storage"""
        try:
            self.mock_storage.clear()
            print(f"[MOCK] Cleared collection: {self.collection_name}")
            return True
        except Exception as e:
            print(f"[MOCK] Error clearing collection: {str(e)}")
            return False 