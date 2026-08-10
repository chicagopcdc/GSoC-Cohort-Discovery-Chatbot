# Data access and Guppy

Last reviewed: 2026-08-09

Source URLs:
- https://docs.pedscommons.org/DataAccessAndGovernance/
- https://docs.pedscommons.org/TermsAndConditions/
- local project configuration: src/backend/api_agent.py

## Data access

The PCDC Data Portal is available for researchers to explore cohort-level information, but access to detailed line-level data is governed by account registration, terms of use, and the project request or data access process. Users should not treat portal search results as reviewed clinical advice.

## Guppy in this project

This chatbot uses the Gen3 Guppy GraphQL API as the execution endpoint for cohort counts and aggregation summaries. The query builder can generate a validated filter locally with OpenAI and local schema files. Public aggregated subject counts can be requested from the PCDC Guppy endpoint without Gen3 credentials; line-level data and any restricted access still require the portal's normal access process.

## What happens without Gen3 credentials

Without Gen3 credentials, the chatbot can still build and display technical filter JSON, answer schema questions from local schema files, answer curated documentation questions from this knowledge base, and request public aggregate counts when the Guppy endpoint is reachable. It cannot access patient-level details or restricted data.
