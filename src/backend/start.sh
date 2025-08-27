#!/bin/bash

# Start FastAPI backend
echo "Starting Natural Language to GraphQL Query Conversion System..."
echo "Access the system at: http://127.0.0.1:8000"
echo "Test results will be saved in the chat_history folder"
echo "========================================"

# Use the correct Python version to start the service
python -m uvicorn app:app --reload
