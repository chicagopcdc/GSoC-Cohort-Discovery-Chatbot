#!/bin/bash
QUERY=$(<query.txt)
SCHEMA=$(<schema/subject.json)

# Read the content of the file into a variable, removing any newlines or extra spaces
BODY=$(jq -n --arg query "$QUERY" --arg schema "$SCHEMA" \
    '{"text": "\($query) I attach table schema as well, generating the final result combined with the table schema: \\schema: \($schema)"}')

curl -X POST "http://localhost:8000/convert" \
     -H "Content-Type: application/json" \
     -d "$BODY"
