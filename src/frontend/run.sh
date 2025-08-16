#!/bin/bash
# Load environment variables
if [ -f .env ]; then
    echo "$(date '+%Y-%m-%d %H:%M:%S') - Loaded .env file"
    export $(cat .env | xargs)
fi

# Navigate to frontend directory and run chainlit
cd "$(dirname "$0")"
chainlit run chainlit_app.py -w --port 8082