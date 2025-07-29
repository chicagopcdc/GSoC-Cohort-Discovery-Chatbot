## About
```
CS5500/
├── README.md                          
├── frontend_requirements.txt          
├── backend_requirements.txt           
│
├── src/                               
│   ├── backend/                       # Backend Services (FastAPI)
│   │   ├── app.py                     # Main FastAPI application
│   │   ├── utils.py                   # Utility functions
│   │   ├── config.json                # Backend configuration 
│   │   ├── start.sh                   # Backend startup script
│   │   │
│   │   ├── utils/                     
│   │   │   ├── log_handler.py         
│   │   │
│   │   ├── diagnosis_results/         
│   │   │   ├── 20250729-152547_diagnosis.json  
│   │   │
│   │   ├── interaction_analysis_results/  
│   │   │   ├── 20250729-152507_analysis.json   
│   │   │
│   │   ├── chat_history/              
│   │
│   ├── frontend/                      
│   │   ├── chainlit_app.py            
│   │   ├── README.md                  
│   │   ├── chainlit.md                
│   │   ├── run.sh                    
│   │   │
│   │   ├── .chainlit/                 # Chainlit configuration
│   │   │   ├── config.toml            # Chainlit settings
│   │   │   └── translations/          # Internationalization
│   │
│   ├── tests/                         
│   │   ├── backend/                   
│   │   │   ├── test_analyze_interaction.py  # Interaction analysis
│   │   │   ├── test_diagnose.py             # Diagnosis API tests 
│   │   │   └── test_result_full.json        # Test result data 
│   │   │
│   │   ├── frontend/                  # Frontend tests
│   │   │   ├── test_file_upload_simple.py   # File upload tests 
│   │   │   ├── test_file_upload.md          # Upload test documentation 
│   │   │   └── test_frontend.sh             # Frontend test script
│   │   │
│   │   └── db/                        # Database tests
│   │       ├── test_db.py                    # Basic DB tests
│   │       ├── test_setup.py                 # Database setup tests
│   │       ├── test_queries.py               # Query tests
│   │       ├── test_postgresql_connection.py # PostgreSQL tests
│   │       ├── test_official_history.py      # History tests
│   │       ├── test_history_features.py      # History feature tests
│   │       ├── test_filter_utils.py          # Filter utility tests
│   │       ├── test_chromadb_version.py      # ChromaDB tests
│   │       ├── filter_object_to_nl.py        # Filter conversion
│   │       └── validate_graphql_generation.py # GraphQL validation 
│   │
│   └── db/                            
```

To login, you can use any of follwing accounts:
    username: test password: test
    username: admin password: admin
    username: user password: user123




