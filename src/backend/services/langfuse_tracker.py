import os
import json
from datetime import datetime
from typing import Dict, Any, Optional

try:
    from langfuse import get_client
    LANGFUSE_AVAILABLE = True
except ImportError:
    get_client = None
    LANGFUSE_AVAILABLE = False

class SimpleTracker:
    """
    Lightweight Langfuse tracker focused on monitoring agent pipeline steps.
    Tracks key nodes in the query conversion process to improve accuracy.
    Updated for Langfuse SDK v3.x
    """
    
    def __init__(self):
        self.langfuse = None
        self.current_span = None
        self.session_id = None
        self._initialized = False
        
    def _ensure_initialized(self):
        """Lazy initialization of Langfuse client."""
        if self._initialized:
            return
            
        self._initialized = True
        
        # Initialize Langfuse client if available and credentials are set
        try:
            if not LANGFUSE_AVAILABLE:
                print("⚠️ Langfuse not installed - tracking disabled")
            elif os.getenv("LANGFUSE_SECRET_KEY") and os.getenv("LANGFUSE_PUBLIC_KEY"):
                self.langfuse = get_client()
                # Test authentication
                if self.langfuse.auth_check():
                    print("✅ Langfuse tracker initialized")
                else:
                    print("❌ Langfuse authentication failed")
                    self.langfuse = None
            else:
                print("⚠️ Langfuse not configured - tracking disabled")
        except Exception as e:
            print(f"❌ Failed to initialize Langfuse: {e}")
    
    def start_trace(self, user_query: str, session_id: str):
        """Start a new trace session for query conversion pipeline."""
        self._ensure_initialized()
        
        if not self.langfuse:
            return
            
        try:
            self.session_id = session_id
            # Use the new context manager API to start a trace
            self.current_span = self.langfuse.start_span(name="query_conversion")
            self.current_span.update(
                input={"user_query": user_query},
                metadata={
                    "timestamp": datetime.now().isoformat(),
                    "session_id": session_id
                }
            )
            # Update trace-level attributes
            self.current_span.update_trace(
                session_id=session_id,
                input={"user_query": user_query}
            )
        except Exception as e:
            print(f"Error starting trace: {e}")
    
    def track_step(self, step_name: str, input_data: Dict[str, Any], output_data: Dict[str, Any], metadata: Optional[Dict[str, Any]] = None):
        """Track a processing step in the agent pipeline."""
        self._ensure_initialized()
        
        if not self.langfuse or not self.current_span:
            return
            
        try:
            # Create a child span for this step
            child_span = self.current_span.start_span(name=step_name)
            child_span.update(
                input=input_data,
                output=output_data,
                metadata=metadata or {}
            )
            child_span.end()
        except Exception as e:
            print(f"Error tracking step {step_name}: {e}")
    
    def track_llm_call(self, prompt: str, response: str, model: str = "gpt-3.5-turbo"):
        """Track LLM API calls with prompt and response data."""
        self._ensure_initialized()
        
        if not self.langfuse or not self.current_span:
            return
            
        try:
            # Create a generation for the LLM call
            generation = self.current_span.start_generation(
                name="llm_call",
                model=model,
                input=prompt
            )
            generation.update(
                output=response,
                metadata={"timestamp": datetime.now().isoformat()}
            )
            generation.end()
        except Exception as e:
            print(f"Error tracking LLM call: {e}")
    
    def end_trace(self, success: bool = True, final_output: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
        """End the current trace session with final results."""
        self._ensure_initialized()
        
        if not self.langfuse or not self.current_span:
            return
            
        try:
            output_data = final_output or {}
            if error:
                output_data["error"] = error
                
            self.current_span.update(
                output=output_data,
                metadata={
                    "success": success,
                    "session_id": self.session_id,
                    "end_time": datetime.now().isoformat()
                }
            )
            
            # Update trace-level output
            self.current_span.update_trace(output=output_data)
            
            # End the span
            self.current_span.end()
            
            # Clean up current span
            self.current_span = None
            self.session_id = None
            
        except Exception as e:
            print(f"Error ending trace: {e}")

# Global tracker instance
tracker = SimpleTracker() 