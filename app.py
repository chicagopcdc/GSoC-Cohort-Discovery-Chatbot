import os
import time
from typing import Optional
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser

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
    variables: Optional[str] = None # Add this to handle the `$filter` variable

# Create LangChain components
llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY")
)

# Create prompt template
template = """
You are a professional GraphQL query converter. Please convert the user's natural language query into a corresponding GraphQL query.

User query: {query}

Please structure the GraphQL query as follows:
1. Use variables like `$filter` for dynamic parameters.
2. Use `AND`, `IN`, `GTE`, `LTE` for complex filter conditions.
3. Apply the filter using the schema provided (i.e., ensure that fields like `age_at_censor_status`, `race`, etc. are included in the query).
4. Ensure the filter is formatted properly, with conditions grouped logically.
5. Return the query with clear and optimized indentation.
    
{format_instructions}
"""

prompt = ChatPromptTemplate.from_template(template)

# Create output parser
parser = PydanticOutputParser(pydantic_object=GraphQLResponse)

# Set up route
@app.post("/convert")
async def convert_to_graphql(query: Query):
    try:
        # Prepare prompt with the updated instructions
        _input = prompt.format_messages(
            query=query.text,
            format_instructions=parser.get_format_instructions()
        )
        
        # Get response from OpenAI model
        output = llm.invoke(_input)
        
        # Parse response into the GraphQL format
        result = parser.parse(output.content)

        # Saving the result to a file with a timestamp
        timestamp = time.strftime("%Y%m%d-%H%M%S")
        file_path = f"chat_history/{timestamp}.txt"
        print(f"result is writing to {file_path}")
        
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Save the query and explanation
        with open(file_path, "w") as f:
            f.write(f"Query: {result.query}\nExplanation: {result.explanation}")

        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 