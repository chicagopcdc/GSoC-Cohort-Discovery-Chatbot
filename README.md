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
This project is a GraphQL generation agent that converts human language queries into GraphQL queries using AI. Below is the project workflow and code structure:

![Workflow](https://drive.google.com/file/d/1eIp6srsOGcDWLOuJTCv9Fw_0GUrhAad7/view?usp=sharing)

```
├── README.md  
├── frontend_requirements.txt                  
├── backend_requirements.txt
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
│   │       ├── nested_graphql_helper.py  # utils for generating nested graphql
│   │       ├── prompt_builder.py    
│   │       ├── filter_utils.py      
│   │       ├── credential_helper.py  # Generate token for guppy/graphql API
│   │       ├── schema_parser.py    
│   │       ├── query_builder.py     
│   │       └── context_manager.py       
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
##### 1. Convert user input to flat GraphQL:
```bash
curl -X POST "http://localhost:8000/flat_graphql" \
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
##### 2. Convert user input to nested GraphQL:
```bash
curl -X POST "http://localhost:8000/nested_graphql" \
     -H "Content-Type: application/json" \
     -d '{"text": "The cohort consists of participants from the INRG consortium who have metastatic tumors. Specifically, these tumors are classified as absent and are located on the skin."}'
```
##### Example Response
```json
Generated nested GraphQL: {
  "AND": [
    {
      "IN": {
        "consortium": [
          "INRG"
        ]
      }
    },
    {
      "nested": {
        "AND": [
          {
            "IN": {
              "tumor_classification": [
                "Metastatic"
              ]
            }
          },
          {
            "IN": {
              "tumor_state": [
                "Absent"
              ]
            }
          },
          {
            "IN": {
              "tumor_site": [
                "Skin"
              ]
            }
          }
        ],
        "path": "tumor_assessments"
      }
    }
  ]
}
```
##### 2. Get query GraphQL result:
##### Flat GraphQL Example 
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
##### Nested GraphQL Example (aggregation format)
```bash
curl -X POST http://localhost:8000/query \
  -H "Content-Type: application/json" \
  -d '{
  "query": "query GetAggregation($filter: JSON) { _aggregation { subject(accessibility: all, filter: $filter) { _totalCount } } }",
  "variables": {
    "filter": {
      "AND": [
        {
          "IN": {
            "consortium": [
              "INRG"
            ]
          }
        },
        {
          "nested": {
            "path": "tumor_assessments",
            "AND": [
              {
                "IN": {
                  "tumor_classification": [
                    "Metastatic"
                  ]
                }
              },
              {
                "IN": {
                  "tumor_state": [
                    "Absent"
                  ]
                }
              },
              {
                "IN": {
                  "tumor_site": [
                    "Skin"
                  ]
                }
              }
            ]
          }
        }
      ]
    }
  }
}'
```
##### Example Response (via https://portal.pedscommons.org/query)
```json
{
  "data": {
    "_aggregation": {
      "subject": {
        "_totalCount": 8508
      }
    }
  }
}
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

### 4. Postgresql db schema
```
create table public."Element" (
  id uuid not null default extensions.uuid_generate_v4 (),
  "threadId" uuid null,
  type text null,
  url text null,
  "chainlitKey" text null,
  name text not null,
  display text null,
  "objectKey" text null,
  size text null,
  page integer null,
  "forIds" text[] null,
  mime text null,
  "updatedAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  "deletedAt" timestamp with time zone null,
  "createdAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  constraint Element_pkey primary key (id),
  constraint Element_threadId_fkey foreign KEY ("threadId") references "Thread" (id) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_element_threadid on public."Element" using btree ("threadId") TABLESPACE pg_default;
```
```
create table public."Element" (
  id uuid not null default extensions.uuid_generate_v4 (),
  "threadId" uuid null,
  type text null,
  url text null,
  "chainlitKey" text null,
  name text not null,
  display text null,
  "objectKey" text null,
  size text null,
  page integer null,
  "forIds" text[] null,
  mime text null,
  "updatedAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  "deletedAt" timestamp with time zone null,
  "createdAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  constraint Element_pkey primary key (id),
  constraint Element_threadId_fkey foreign KEY ("threadId") references "Thread" (id) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_element_threadid on public."Element" using btree ("threadId") TABLESPACE pg_default;
```
```
create table public."Element" (
  id uuid not null default extensions.uuid_generate_v4 (),
  "threadId" uuid null,
  type text null,
  url text null,
  "chainlitKey" text null,
  name text not null,
  display text null,
  "objectKey" text null,
  size text null,
  page integer null,
  "forIds" text[] null,
  mime text null,
  "updatedAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  "deletedAt" timestamp with time zone null,
  "createdAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  constraint Element_pkey primary key (id),
  constraint Element_threadId_fkey foreign KEY ("threadId") references "Thread" (id) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_element_threadid on public."Element" using btree ("threadId") TABLESPACE pg_default;
```
```
create table public."Thread" (
  id uuid not null default extensions.uuid_generate_v4 (),
  "createdAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  name text null,
  "userId" uuid null,
  "userIdentifier" text null,
  tags text[] null,
  metadata jsonb null default '{}'::jsonb,
  "updatedAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  "deletedAt" timestamp with time zone null,
  participant jsonb null,
  constraint Thread_pkey primary key (id),
  constraint Thread_userId_fkey foreign KEY ("userId") references "User" (id) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_thread_userid on public."Thread" using btree ("userId") TABLESPACE pg_default;

create index IF not exists idx_thread_useridentifier on public."Thread" using btree ("userIdentifier") TABLESPACE pg_default;

create index IF not exists idx_thread_createdat on public."Thread" using btree ("createdAt" desc) TABLESPACE pg_default;
```
```
create table public."Thread" (
  id uuid not null default extensions.uuid_generate_v4 (),
  "createdAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  name text null,
  "userId" uuid null,
  "userIdentifier" text null,
  tags text[] null,
  metadata jsonb null default '{}'::jsonb,
  "updatedAt" timestamp with time zone null default CURRENT_TIMESTAMP,
  "deletedAt" timestamp with time zone null,
  participant jsonb null,
  constraint Thread_pkey primary key (id),
  constraint Thread_userId_fkey foreign KEY ("userId") references "User" (id) on delete CASCADE
) TABLESPACE pg_default;

create index IF not exists idx_thread_userid on public."Thread" using btree ("userId") TABLESPACE pg_default;

create index IF not exists idx_thread_useridentifier on public."Thread" using btree ("userIdentifier") TABLESPACE pg_default;

create index IF not exists idx_thread_createdat on public."Thread" using btree ("createdAt" desc) TABLESPACE pg_default;
```




