"""Session-scoped conversation memory for query pipelines.

Manages per-session context (message history, cached schemas, and prior
query results) so that multi-turn conversations with the LLM retain
continuity.  SessionManager is the top-level entry point; it maps
session IDs to individual QueryMemory instances.
"""


class QueryMemory:
    """Stores conversational context for a single query session.

    Keeps a rolling window of the most recent messages together with a
    schema cache and a dictionary of past query results so the LLM can
    reference earlier interactions.
    """

    MAX_MESSAGES = 20
    
    def __init__(self):
        self.messages = []
        self.schema_cache = {}
        self.query_results = {}
    
    def add_message(self, message):
        """Append a message and trim history to the most recent entries.

        Args:
            message: A dict with at least "role" and "content" keys.
        """
        self.messages.append(message)
        
        # Keep context size reasonable
        if len(self.messages) > self.MAX_MESSAGES:
            self.messages = self.messages[-self.MAX_MESSAGES:]
    
    def get_context(self):
        """Return the raw message list."""
        return self.messages
    
    def cache_schema(self, node_type, schema_info):
        """Cache parsed schema information for a node type.

        Args:
            node_type: Node name as it appears in the schema
                (e.g. "subject", "disease_characteristic").
            schema_info: The parsed schema dict to cache.
        """
        self.schema_cache[node_type] = schema_info
    
    def get_cached_schema(self, node_type):
        """Return cached schema for the given node type, or None."""
        return self.schema_cache.get(node_type)
    
    def store_query_result(self, query_id, result):
        """Persist a query result so it can be referenced later.

        Args:
            query_id: Unique identifier for the query.
            result: The result payload to store.
        """
        self.query_results[query_id] = result
    
    def get_query_result(self, query_id):
        """Return a previously stored query result, or None."""
        return self.query_results.get(query_id)
    
    def get_formatted_context(self):
        """Format all messages as a single string for an LLM prompt.

        Each message is rendered as "Role: content" separated by blank
        lines. Messages missing the expected keys are silently skipped.

        Returns:
            A newline-joined string of all formatted messages.
        """
        formatted_context = []
        
        for message in self.messages:
            if isinstance(message, dict) and "role" in message and "content" in message:
                formatted_context.append(f"{message['role'].capitalize()}: {message['content']}")
        
        return "\n\n".join(formatted_context)


class SessionManager:
    """Registry of QueryMemory instances keyed by session ID.

    Provides create, get, and delete operations for conversation sessions.
    """
    
    def __init__(self):
        self.sessions = {}
    
    def get_or_create_session(self, session_id):
        """Return the existing session or create a new one."""
        if session_id not in self.sessions:
            self.sessions[session_id] = QueryMemory()
        
        return self.sessions[session_id]
    
    def delete_session(self, session_id):
        """Remove a session and free its memory. No-op if not found."""
        if session_id in self.sessions:
            del self.sessions[session_id]
    
    def get_all_session_ids(self):
        """Return a list of all active session IDs."""
        return list(self.sessions.keys())