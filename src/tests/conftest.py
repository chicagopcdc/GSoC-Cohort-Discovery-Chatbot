"""Pytest defaults for the local test run."""

collect_ignore = [
    "test_db.py",                     # live Postgres
    "test_postgresql_connection.py",  # live Postgres
    "test_chromadb_version.py",       # legacy ChromaDB stack
    "test_history_features.py",       # legacy ChromaDB stack
    "test_filter_utils.py",           # legacy utils pipeline
    "test_queries.py",                # expects a running backend
]
