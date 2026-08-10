# Chatbot capabilities

Last reviewed: 2026-08-09

Source URLs:
- local proposal: D4CG_Proposal_2026.pdf
- local implementation: src/backend/services/agent.py
- local implementation: src/backend/services/query_builder_v2.py

## What this chatbot does

The chatbot helps researchers work with PCDC cohort discovery tasks in plain language. It can build schema-validated cohort filters, modify the current cohort across follow-up turns, look up filterable schema fields and enum values, count a cohort when Guppy execution is configured, and summarize or compare cohorts when aggregation execution is available.

## Agent tools

The current agent architecture separates responsibilities across tools. Knowledge QA answers curated documentation questions. Schema Explorer answers exact questions about filterable schema fields, paths, and enum values. Query Builder turns natural language cohort requests into validated filter JSON. Cohort counting executes validated filters against the public Guppy aggregation endpoint. Cohort Analyzer also uses Guppy aggregation and may need additional endpoint support for broader summaries.

## What the generated filter is

The generated filter is the technical JSON object sent as the `$filter` variable in Guppy GraphQL queries. It is not a natural-language answer and it is not a count by itself. It is the structured representation of the cohort definition.
