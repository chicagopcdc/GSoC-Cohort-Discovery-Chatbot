# PCDC GraphQL Query Generator

Welcome to the PCDC GraphQL Query Generator!

This tool helps you convert natural language queries into GraphQL query statements, making it easier to use in the PCDC data portal.

## How to Use

1. Enter your query, for example: "Find all cases from the INRG consortium"
2. The system will generate the appropriate GraphQL query and variables
3. You can directly copy the generated code for your query

## Configuration

# Enable session persistence
persistent: true

# Enable chat history
persistent_chat_history: true

# Client-side session storage (no database needed)
chat_history:
  client_side: true
