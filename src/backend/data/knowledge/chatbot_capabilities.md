# Chatbot capabilities

Last reviewed: 2026-08-10

Source URLs:
- local implementation: src/backend/services/agent.py
- local implementation: src/backend/services/query_builder_v2.py
- local implementation: src/backend/services/knowledge_qa.py

## What this assistant does

You describe the cohort you want in ordinary language and this assistant turns it into a filter that has been checked against the PCDC schema. It keeps the current cohort across turns, so a follow-up like "change consortium to NODAL" or "also add males" edits what is already there instead of starting over. It also answers questions about the schema and about PCDC itself.

## The four tools

Four tools sit behind the conversation. Build Query turns a request into validated filter JSON. Schema Explorer answers exact questions about filterable fields, nested tables and allowed values, straight from the schema files rather than from the model. Cohort counting runs a validated filter against the Guppy aggregation endpoint. Knowledge QA, which produced this answer, retrieves from a small set of curated documents. The assistant picks a tool per question; a schema question does not build a filter, and a documentation question does not run a count.

## What the generated filter is

The filter is the JSON object sent to Guppy as the `$filter` variable. Seeing it is useful because it shows exactly which fields and values your wording was interpreted as, which is often where a surprising count comes from. It is a cohort definition, so on its own it says nothing about how many subjects match until it is actually run.

## Where the schema comes from

Field names, nested table names and allowed values are read from local schema files, not recalled by the language model. The schema currently loaded exposes 10 filterable fields at the subject level and 17 nested tables, 108 filterable fields in total. The live index carries over a thousand fields, so the filterable set is deliberately a curated subset rather than everything stored.
