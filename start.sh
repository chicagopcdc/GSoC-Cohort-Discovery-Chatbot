#!/bin/bash

# Start FastAPI backend
echo "Starting Natural Language to GraphQL Query Conversion System..."
echo "Access the system at: http://127.0.0.1:8000"
echo "Test results will be saved in the chat_history folder"
echo "========================================"

# Use the correct Python version to start the service
/Users/ronghuang/.pyenv/versions/3.8.16/bin/python -m uvicorn app:app --reload

# If you need to run test queries, uncomment below
# echo "Running test queries..."
# /Users/ronghuang/.pyenv/versions/3.8.16/bin/python test_queries.py 