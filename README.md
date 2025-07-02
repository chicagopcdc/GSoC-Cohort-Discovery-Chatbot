# LangChain GraphQL Converter

A simple application built with LangChain and FastAPI that converts natural language queries into GraphQL queries.

## Installation

1. After cloning the project, install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up your OpenAI API key in the `.env` file:
```
OPENAI_API_KEY=your_api_key_here
DATABASE_URL=postgresql://postgres:your_postgresql_address
```

## Running the Application

Start the server:
```bash
bash run.sh
```
The server will run at http://localhost:8000
To login, you can use any of follwing accounts:
username: test password: test
username: admin password: admin
username: user password: user123

## API Usage

Send a POST request to the `/convert` endpoint:

```bash
curl -X POST "http://localhost:8000/convert" \
     -H "Content-Type: application/json" \
     -d '{"text": "Get all users names and emails"}'
```

## Example Response

```json
{
    "query": "query { users { name email } }",
    "explanation": "This query will return the names and email addresses of all users"
}
```


