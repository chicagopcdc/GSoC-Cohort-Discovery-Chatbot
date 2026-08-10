# Chatbot and data limitations

Last reviewed: 2026-08-05

Source URLs:
- https://docs.pedscommons.org/StatisticalManual/
- https://docs.pedscommons.org/TermsAndConditions/
- local implementation: src/backend/services/cohort_analyzer.py

## Statistical and interpretation limits

PCDC portal outputs and chatbot outputs are research-support tools. Users are responsible for interpreting results carefully, respecting data access rules, and following appropriate statistical practice. The chatbot should not provide medical advice or invent unsupported data.

## Tool 4 v1 limits

Cohort Analyzer v1 reports total counts and top-level categorical distributions such as sex, race, ethnicity, and consortium. Nested field histograms such as tumor site, and numeric summaries such as mean age or age range, require additional Guppy query support and are outside the current v1 scope.

## Safe answer behavior

If the chatbot does not have enough curated documentation, schema evidence, or execution access, it should say that clearly instead of guessing. Counts and distributions must come from Guppy execution, not from the language model.
