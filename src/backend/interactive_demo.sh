#!/bin/bash

echo "========================================"
echo "Natural Language to GraphQL Query Conversion Demo"
echo "========================================"

while true; do
  echo ""
  echo "Please enter your natural language query (type 'exit' to quit):"
  read -r query
  
  if [ "$query" = "exit" ]; then
    echo "Demo ended"
    break
  fi
  
  echo "Processing query..."
  
  # Send request to API
  response=$(curl -s -X POST "http://localhost:8000/convert" \
     -H "Content-Type: application/json" \
     -d "{\"text\": \"$query\"}")
  
  # Extract fields and beautify display
  graphql_query=$(echo $response | python -c "import sys, json; print(json.loads(sys.stdin.read())['query'])")
  variables=$(echo $response | python -c "import sys, json; print(json.loads(sys.stdin.read())['variables'])")
  
  # Format output
  echo ""
  echo "======== Generated GraphQL Query ========"
  echo "$graphql_query"
  echo ""
  echo "======== Query Variables ========"
  echo "$variables" | python -m json.tool
  echo ""
  echo "(Query history saved to chat_history folder)"
done 