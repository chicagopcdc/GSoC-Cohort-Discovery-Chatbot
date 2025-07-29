class QueryMemory:
    """Manages context memory for query sessions"""
    
    def __init__(self):
        self.messages = []
        self.schema_cache = {}
        self.query_results = {}
    
    def add_message(self, message):
        """Add message to context"""
        self.messages.append(message)
        
        # Keep context size reasonable
        if len(self.messages) > 20:
            self.messages = self.messages[-20:]
    
    def get_context(self):
        """Get current context"""
        return self.messages
    
    def cache_schema(self, node_type, schema_info):
        """Cache schema information"""
        self.schema_cache[node_type] = schema_info
    
    def get_cached_schema(self, node_type):
        """Get cached schema information"""
        return self.schema_cache.get(node_type)
    
    def store_query_result(self, query_id, result):
        """Store query result"""
        self.query_results[query_id] = result
    
    def get_query_result(self, query_id):
        """Get query result"""
        return self.query_results.get(query_id)
    
    def get_formatted_context(self):
        """Get formatted context for passing to LLM"""
        formatted_context = []
        
        for message in self.messages:
            if isinstance(message, dict) and "role" in message and "content" in message:
                formatted_context.append(f"{message['role'].capitalize()}: {message['content']}")
        
        return "\n\n".join(formatted_context)


class SessionManager:
    """Manages context for multiple sessions"""
    
    def __init__(self):
        self.sessions = {}
    
    def get_or_create_session(self, session_id):
        """Get or create session"""
        if session_id not in self.sessions:
            self.sessions[session_id] = QueryMemory()
        
        return self.sessions[session_id]
    
    def delete_session(self, session_id):
        """Delete session"""
        if session_id in self.sessions:
            del self.sessions[session_id]
    
    def get_all_session_ids(self):
        """Get all session IDs"""
        return list(self.sessions.keys())


# Global session manager instance
session_manager = SessionManager()


if __name__ == "__main__":
    # Test code
    memory = QueryMemory()
    
    # Add messages
    memory.add_message({"role": "user", "content": "Query subjects who are multiracial and between 0-18 years of age"})
    memory.add_message({"role": "assistant", "content": "Query generated, 20 records found"})
    
    # Get context
    context = memory.get_formatted_context()
    print(f"Formatted context:\n{context}")
    
    # Test session manager
    manager = SessionManager()
    session1 = manager.get_or_create_session("user1")
    session1.add_message({"role": "user", "content": "Query 1"})
    
    session2 = manager.get_or_create_session("user2")
    session2.add_message({"role": "user", "content": "Query 2"})
    
    print(f"Session IDs: {manager.get_all_session_ids()}")
    print(f"Session 1 messages: {session1.get_context()}")
    print(f"Session 2 messages: {session2.get_context()}") 