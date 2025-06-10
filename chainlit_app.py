import chainlit as cl
from langchain_openai import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.output_parsers import PydanticOutputParser
from pydantic import BaseModel
from typing import Optional
import os
from dotenv import load_dotenv

load_dotenv()

class GraphQLResponse(BaseModel):
    query: str
    explanation: Optional[str] = None

llm = ChatOpenAI(
    model="gpt-3.5-turbo",
    temperature=0,
    api_key=os.getenv("OPENAI_API_KEY")
)

template = """You are a professional GraphQL query converter. Please convert the user's natural language query into a corresponding GraphQL query.

User query: {query}

Please provide:
1. The corresponding GraphQL query
2. A brief explanation of what this query does

{format_instructions}
"""

prompt = ChatPromptTemplate.from_template(template)
parser = PydanticOutputParser(pydantic_object=GraphQLResponse)

@cl.on_message
async def main(message: cl.Message):
    _input = prompt.format_messages(
        query=message.content,
        format_instructions=parser.get_format_instructions()
    )
    output = llm.invoke(_input)
    result = parser.parse(output.content)
    await cl.Message(
        content=f"**GraphQL Query:**\n```graphql\n{result.query}\n```\n\n**Explanation:**\n{result.explanation}"
    ).send() 