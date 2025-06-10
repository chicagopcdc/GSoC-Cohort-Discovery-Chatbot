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
```

## Running the Application

Start the server:
```bash
python app.py
```

The server will run at http://localhost:8000

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

## Frontend Demo

You can also test the application using a simple HTML frontend.

### index.html

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>LangChain GraphQL Demo</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 40px; }
    input, button { font-size: 1em; }
    #result { margin-top: 20px; white-space: pre-wrap; }
  </style>
</head>
<body>
  <h2>LangChain GraphQL Demo</h2>
  <input id="queryInput" type="text" placeholder="Enter your question..." size="40">
  <button onclick="sendQuery()">Send</button>
  <div id="result"></div>
  <script>
    async function sendQuery() {
      const text = document.getElementById('queryInput').value;
      document.getElementById('result').innerText = 'Loading...';
      const res = await fetch('http://localhost:8000/convert', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ text })
      });
      const data = await res.json();
      document.getElementById('result').innerText = JSON.stringify(data, null, 2);
    }
  </script>
</body>
</html>
``` 