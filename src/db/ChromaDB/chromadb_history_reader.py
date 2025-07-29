"""
ChromaDB History Reader
Read and format chat history from ChromaDB for Chainlit display
"""

import sqlite3
import json
from datetime import datetime
from typing import List, Dict, Optional

class ChromaDBHistoryReader:
    def __init__(self, db_path: str = "./chroma_db/chroma.sqlite3"):
        """
        Initialize the history reader
        
        Args:
            db_path: Path to ChromaDB SQLite database
        """
        self.db_path = db_path
    
    def get_all_sessions(self) -> List[Dict]:
        """
        Get all unique sessions from ChromaDB
        
        Returns:
            List of session dictionaries with metadata
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all unique sessions with their metadata
            cursor.execute("""
                SELECT 
                    JSON_EXTRACT(metadata, '$.session_id') as session_id,
                    JSON_EXTRACT(metadata, '$.timestamp') as timestamp,
                    COUNT(*) as message_count,
                    MIN(JSON_EXTRACT(metadata, '$.timestamp')) as first_message,
                    MAX(JSON_EXTRACT(metadata, '$.timestamp')) as last_message
                FROM embeddings e
                JOIN collections c ON e.collection_id = c.id
                WHERE c.name = 'llm_responses'
                  AND JSON_EXTRACT(metadata, '$.session_id') IS NOT NULL
                GROUP BY JSON_EXTRACT(metadata, '$.session_id')
                ORDER BY MAX(JSON_EXTRACT(metadata, '$.timestamp')) DESC
            """)
            
            sessions = []
            for row in cursor.fetchall():
                session_id, timestamp, count, first, last = row
                if session_id:
                    sessions.append({
                        'session_id': session_id,
                        'message_count': count,
                        'first_message': first,
                        'last_message': last,
                        'display_name': f"Session {session_id[:8]}... ({count} messages)"
                    })
            
            conn.close()
            return sessions
        
        except Exception as e:
            print(f"Error getting sessions: {e}")
            return []
    
    def get_session_history(self, session_id: str) -> List[Dict]:
        """
        Get chat history for a specific session
        
        Args:
            session_id: Session ID to retrieve
            
        Returns:
            List of messages in chronological order
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Get all messages for this session
            cursor.execute("""
                SELECT 
                    e.document,
                    e.metadata,
                    JSON_EXTRACT(metadata, '$.timestamp') as timestamp
                FROM embeddings e
                JOIN collections c ON e.collection_id = c.id
                WHERE c.name = 'llm_responses'
                  AND JSON_EXTRACT(metadata, '$.session_id') = ?
                ORDER BY JSON_EXTRACT(metadata, '$.timestamp') ASC
            """, (session_id,))
            
            messages = []
            for document, metadata_str, timestamp in cursor.fetchall():
                try:
                    metadata = json.loads(metadata_str) if metadata_str else {}
                    
                    # Parse the document content
                    parsed_msg = self._parse_document_content(document, metadata)
                    if parsed_msg:
                        messages.append(parsed_msg)
                
                except Exception as e:
                    print(f"Error parsing message: {e}")
                    continue
            
            conn.close()
            return messages
        
        except Exception as e:
            print(f"Error getting session history: {e}")
            return []
    
    def _parse_document_content(self, document: str, metadata: Dict) -> Optional[Dict]:
        """
        Parse document content into structured message format
        
        Args:
            document: Raw document content
            metadata: Document metadata
            
        Returns:
            Parsed message dictionary
        """
        try:
            lines = document.split('\n')
            
            user_query = ""
            graphql_query = ""
            variables = ""
            explanation = ""
            
            for line in lines:
                line = line.strip()
                if line.startswith("User Query:"):
                    user_query = line.replace("User Query:", "").strip()
                elif line.startswith("GraphQL Query:"):
                    graphql_query = line.replace("GraphQL Query:", "").strip()
                elif line.startswith("Variables:"):
                    variables = line.replace("Variables:", "").strip()
                elif line.startswith("Explanation:"):
                    explanation = line.replace("Explanation:", "").strip()
            
            return {
                'user_query': user_query,
                'graphql_query': graphql_query,
                'variables': variables,
                'explanation': explanation,
                'timestamp': metadata.get('timestamp', ''),
                'session_id': metadata.get('session_id', ''),
                'metadata': metadata
            }
        
        except Exception as e:
            print(f"Error parsing document: {e}")
            return None
    
    def search_history(self, search_term: str, limit: int = 10) -> List[Dict]:
        """
        Search through chat history
        
        Args:
            search_term: Term to search for
            limit: Maximum number of results
            
        Returns:
            List of matching messages
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            # Search in document content
            cursor.execute("""
                SELECT 
                    e.document,
                    e.metadata,
                    JSON_EXTRACT(metadata, '$.timestamp') as timestamp,
                    JSON_EXTRACT(metadata, '$.session_id') as session_id
                FROM embeddings e
                JOIN collections c ON e.collection_id = c.id
                WHERE c.name = 'llm_responses'
                  AND LOWER(e.document) LIKE LOWER(?)
                ORDER BY JSON_EXTRACT(metadata, '$.timestamp') DESC
                LIMIT ?
            """, (f"%{search_term}%", limit))
            
            results = []
            for document, metadata_str, timestamp, session_id in cursor.fetchall():
                try:
                    metadata = json.loads(metadata_str) if metadata_str else {}
                    parsed_msg = self._parse_document_content(document, metadata)
                    if parsed_msg:
                        results.append(parsed_msg)
                
                except Exception as e:
                    continue
            
            conn.close()
            return results
        
        except Exception as e:
            print(f"Error searching history: {e}")
            return []
    
    def get_recent_history(self, limit: int = 10) -> List[Dict]:
        """
        Get recent chat history across all sessions
        
        Args:
            limit: Maximum number of recent messages
            
        Returns:
            List of recent messages
        """
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    e.document,
                    e.metadata,
                    JSON_EXTRACT(metadata, '$.timestamp') as timestamp
                FROM embeddings e
                JOIN collections c ON e.collection_id = c.id
                WHERE c.name = 'llm_responses'
                ORDER BY JSON_EXTRACT(metadata, '$.timestamp') DESC
                LIMIT ?
            """, (limit,))
            
            messages = []
            for document, metadata_str, timestamp in cursor.fetchall():
                try:
                    metadata = json.loads(metadata_str) if metadata_str else {}
                    parsed_msg = self._parse_document_content(document, metadata)
                    if parsed_msg:
                        messages.append(parsed_msg)
                
                except Exception as e:
                    continue
            
            conn.close()
            return messages
        
        except Exception as e:
            print(f"Error getting recent history: {e}")
            return [] 