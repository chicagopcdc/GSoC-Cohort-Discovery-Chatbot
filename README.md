![gsoc](https://user-images.githubusercontent.com/129569933/267078707-df0e5058-eec5-4740-996b-085f56ae0f5a.png)

![D4CG](https://commons.cri.uchicago.edu/wp-content/uploads/2023/01/Color-D4CG-Standard-Logo-copy-680x154.png)

**Contributor**: **Regina Huang**

**Email:** huang.rong@northeastern.edu

**Github profile**: https://github.com/RongHuang14

**LinkedIn**: https://www.linkedin.com/in/ronghuang14/

**Mentor:** **Jooho Lee**

## About
**GraphQL Convertor** is a GraphQL generation agent application built with Chainlit and FastAPI that converts natural language queries into GraphQL queries.

**Overview:**
This project is a GraphQL generation agent that converts natural language queries into GraphQL queries using AI. Below is the project structure:

```
├── README.md                    
├── requirements.txt
├── .env                        # OpenAI key & postgresql url
│
├── src/    
│   ├── frontend/                
│   │   ├── chainlit_app.py     # Main Chainlit application
│   │   ├── chainlit.md         # Welcome page
│   │   └── run.sh              # Frontend startup script
│   │
│   ├── backend/                 
│   │   ├── app.py              # Main FastAPI application
│   │   ├── start.sh            # Backend startup script
│   │   ├── interactive_demo.sh 
│   │   └── utils/             
│   │       ├── prompt_builder.py    
│   │       ├── filter_utils.py      # Query or response filtering utilities
│   │       ├── schema_parser.py    
│   │       ├── query_builder.py     
│   │       └── context_manager.py   
│   │
│   ├── db/                      
│   │   └── ChromaDB/           
│   │       ├── chroma_manager.py       # ChromaDB connection manager
│   │       ├── chroma_utils.py         
│   │       ├── query_chromadb.py       # ChromaDB query interface
│   │       ├── chromadb_history_reader.py # Reading Chat history 
│   │       └── ChromaDB_SETUP.md      
│   │
│   └── tests/                   
│       ├── test_db.py          
│       ├── test_queries.py     
│       ├── test_filter_utils.py 
│       ├── validate_graphql_generation.py
│       └── test_setup.py       
│
├── schema/                      
│   ├── gitops.json             # GraphQL schema content
│   ├── pcdc-schema-prod-*.json 
│   └── subject.json            
│
├── assets/                     
```

## Getting Started
### 1. Installation
1. After cloning the project, install dependencies:  
You need to build frontend env and backend env seperately, because ```gen3``` and ```chainlit``` have version conflict on ```aiofiles```.  

Build frontend venv
```bash
python3 -m venv frontend_env
source frontend_env/bin/activate
pip install --upgrade pip
pip install -r frontend_requirements.txt
```
Build backend venv
```bash
python3 -m venv backend_env
source backend_env/bin/activate
pip install --upgrade pip
pip install -r backend_requirements.txt
```

2. Set up your OpenAI API key in the `.env` file:
```
OPENAI_API_KEY=your_api_key_here
DATABASE_URL=postgresql://postgres:your_postgresql_address
```

### 2. Run the Application Backend
First, make sure you start the backend server in ```backend_env```:
```bash
source backend_env/bin/activate
cd src/backend/
python -m uvicorn app:app --reload
```
The backend API will run at http://localhost:8000

#### Backend API Usage
```bash
cd src/backend/
```
##### 1. Convert user input to GraphQL:
```bash
curl -X POST "http://localhost:8000/convert" \
     -H "Content-Type: application/json" \
     -d '{"text": "I want to query all male patients"}'
```
##### Example Response
```json
{
    "query": "query ($filter: JSON) { _aggregation { subject(accessibility: all, filter: $filter) { consortium { histogram { key count } } race { histogram { key count } } _totalCount } } }",
    "variables": "{'AND': [{'IN': {'race': ['Asian']}}]}"
}
```
##### 2. Get query GraphQL result:
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "query ($filter: JSON) { _aggregation { subject(accessibility: all, filter: $filter) { consortium { histogram { key count } } sex { histogram { key count } } _totalCount } } }",
    "variables": {"filter": {"AND": [{"IN": {"sex": ["Male"]}}]}}
  }'
```
##### Example Response
```json
{"data":{"_aggregation":{"subject":{"consortium":{"histogram":[{"key":"INSTRuCT","count":52},{"key":"NODAL","count":44},{"key":"INRG","count":42},{"key":"INTERACT","count":38},{"key":"HIBISCUS","count":37},{"key":"MaGIC","count":33},{"key":"ALL","count":32}]},"sex":{"histogram":[{"key":"Other","count":60},{"key":"Male","count":48},{"key":"Undifferentiated","count":45},{"key":"Female","count":43},{"key":"Unknown","count":35},{"key":"Not Reported","count":31},{"key":"no data","count":57}]},"_totalCount":319}}}}
```
### 3. Run the Application Frontend
First, make sure you start the frontend server in ```frontend_env```:
```bash
source frontend_env/bin/activate
bash src/frontend/run.sh
```
It will run at http://localhost:8082

To login, you can use any of follwing accounts:
    username: test password: test
    username: admin password: admin
    username: user password: user123




