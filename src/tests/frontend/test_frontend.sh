#!/bin/bash

# Test script for Cross-Component Interaction Analyzer Frontend

echo "🚀 Starting Cross-Component Interaction Analyzer Frontend..."
echo ""

# Check if backend is running
echo "🔍 Checking if backend is running on port 8000..."
if curl -s http://localhost:8000/docs > /dev/null; then
    echo "✅ Backend is running!"
else
    echo "❌ Backend is not running. Please start it first:"
    echo "   cd ../backend"
    echo "   python app.py"
    exit 1
fi

echo ""
echo "🌐 Starting Chainlit frontend..."
echo ""
echo "📝 Login credentials:"
echo "   - Username: test, Password: test"
echo "   - Username: admin, Password: admin"
echo "   - Username: user, Password: user"
echo ""
echo "💡 How to use:"
echo "   1. 📤 Upload log files by dragging them into the chat"
echo "   2. 🔍 Type 'analyze' to analyze uploaded files (or default files)"
echo "   3. 🗑️  Type 'clear' to remove uploaded files"
echo "   4. ❓ Type 'help' for more information"
echo ""
echo "📊 The tool will return all JSON results:"
echo "   - Interaction Pairs (component relationships)"
echo "   - Categorized Interactions (bug patterns)"
echo ""

# Start Chainlit
chainlit run chainlit_app.py -w 