"""Pytest configuration for optional legacy-test exclusion."""

import os


# These legacy scripts need live services or modules outside the current test
# environment. Keep them visible by default; local unit-only runs may opt out.
collect_ignore = []
if os.getenv("PCDC_SKIP_LEGACY_TESTS") == "1":
    collect_ignore = [
        "test_db.py",                     # live Postgres
        "test_postgresql_connection.py",  # live Postgres
        "test_chromadb_version.py",       # legacy ChromaDB stack
        "test_history_features.py",       # legacy ChromaDB stack
        "test_filter_utils.py",           # legacy utils pipeline
        "test_queries.py",                # expects a running backend
    ]
