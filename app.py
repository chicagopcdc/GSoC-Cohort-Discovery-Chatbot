from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from typing import Optional
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = FastAPI()

# Define input model
class Query(BaseModel):
    text: str

# Define output model
class GraphQLResponse(BaseModel):
    query: str
    explanation: Optional[str] = None

# Create LangChain components
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY")
)

# Create prompt template
template = """You are a professional GraphQL query converter. Please convert the user's natural language query into a corresponding GraphQL query.

User query: {query}

Please provide:
1. The corresponding GraphQL query
2. A brief explanation of what this query does

{format_instructions}
"""

prompt = ChatPromptTemplate.from_template(template)

# Create output parser
parser = PydanticOutputParser(pydantic_object=GraphQLResponse)

# Set up route
@app.post("/convert")
async def convert_to_graphql(query: Query):
    try:
        # Prepare prompt
        _input = prompt.format_messages(
            query=query.text,
            format_instructions=parser.get_format_instructions()
        )
        
        # Get response
        output = llm.invoke(_input)
        
        # Parse response
        result = parser.parse(output.content)
        
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 