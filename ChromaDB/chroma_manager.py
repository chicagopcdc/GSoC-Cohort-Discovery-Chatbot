import chromadb
from chromadb.config import Settings
from langchain_community.vectorstores import Chroma
from langchain_openai import OpenAIEmbeddings
import os
import json
from datetime import datetime
from typing import Dict, List, Optional
from dotenv import load_dotenv

load_dotenv()

class ChromaDBManager:
    """Manager class for ChromaDB operations"""
    
    def __init__(self, persist_directory: str = "./chroma_db", collection_name: str = "llm_responses"):
        """
        Initialize ChromaDB manager
        
        Args:
            persist_directory: Directory to persist ChromaDB data
            collection_name: Name of the collection
        """
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        
        # Initialize embeddings
        self.embeddings = OpenAIEmbeddings(
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        
        # Initialize ChromaDB client
        self.client_settings = Settings(
            persist_directory=persist_directory,
            is_persistent=True
        )
        
        self.client = chromadb.Client(self.client_settings)
        
        # Initialize or get collection
        try:
            self.collection = self.client.get_collection(collection_name)
        except:
            self.collection = self.client.create_collection(collection_name)
        
        # Initialize Langchain Chroma vectorstore
        self.vectorstore = Chroma(
            client=self.client,
            collection_name=collection_name,
            embedding_function=self.embeddings,
            persist_directory=persist_directory
        )
        
        print(f"ChromaDB initialized with collection: {collection_name}")
    
    def store_response(self, 
                      user_query: str, 
                      llm_response: Dict, 
                      session_id: str,
                      metadata: Optional[Dict] = None) -> str:
        """
        Store LLM response in ChromaDB
        
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
            
            # Store in vectorstore
            self.vectorstore.add_texts(
                texts=[document_content],
                metadatas=[doc_metadata],
                ids=[doc_id]
            )
            
            print(f"Stored response in ChromaDB with ID: {doc_id}")
            return doc_id
            
        except Exception as e:
            print(f"Error storing response in ChromaDB: {str(e)}")
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
        Search for similar responses in ChromaDB
        
        Args:
            query: Search query
            k: Number of results to return
            
        Returns:
            List of similar documents with metadata
        """
        try:
            results = self.vectorstore.similarity_search_with_score(query, k=k)
            
            formatted_results = []
            for doc, score in results:
                formatted_results.append({
                    "content": doc.page_content,
                    "metadata": doc.metadata,
                    "similarity_score": score
                })
            
            return formatted_results
            
        except Exception as e:
            print(f"Error searching ChromaDB: {str(e)}")
            return []
    
    def get_session_history(self, session_id: str) -> List[Dict]:
        """
        Get all responses for a specific session
        
        Args:
            session_id: Session identifier
            
        Returns:
            List of documents for the session
        """
        try:
            # Search by session_id in metadata
            results = self.collection.get(
                where={"session_id": session_id},
                include=["documents", "metadatas"]
            )
            
            session_history = []
            for i, doc in enumerate(results['documents']):
                session_history.append({
                    "content": doc,
                    "metadata": results['metadatas'][i] if results['metadatas'] else {}
                })
            
            return session_history
            
        except Exception as e:
            print(f"Error getting session history: {str(e)}")
            return []
    
    def get_statistics(self) -> Dict:
        """Get ChromaDB collection statistics"""
        try:
            count = self.collection.count()
            return {
                "total_documents": count,
                "collection_name": self.collection_name,
                "persist_directory": self.persist_directory
            }
        except Exception as e:
            print(f"Error getting statistics: {str(e)}")
            return {}
    
    def clear_collection(self) -> bool:
        """Clear all documents from the collection"""
        try:
            # Delete the collection and recreate it
            self.client.delete_collection(self.collection_name)
            self.collection = self.client.create_collection(self.collection_name)
            
            # Reinitialize vectorstore
            self.vectorstore = Chroma(
                client=self.client,
                collection_name=self.collection_name,
                embedding_function=self.embeddings,
                persist_directory=self.persist_directory
            )
            
            print(f"Cleared collection: {self.collection_name}")
            return True
            
        except Exception as e:
            print(f"Error clearing collection: {str(e)}")
            return False 