# Data access and Guppy

Last reviewed: 2026-08-10

Source URLs:
- https://docs.pedscommons.org/DataAccessAndGovernance/
- https://docs.pedscommons.org/TermsAndConditions/
- local project configuration: src/backend/api_v2.py

## Do you need an account, and the two access tiers

Yes, exploring PCDC needs an account, and access comes in two tiers. Tier 1 is a free account on the portal: that account opens the data dictionaries, cohort exploration and the visualizations, but never individual patient records, and its output is licensed for feasibility work and early hypothesis exploration rather than publication. Anyone can register an account at portal.pedscommons.org. Tier 2 is line-level participant data, and no account alone unlocks it — it only comes with an approved project.

## Getting line-level data

Line-level data starts with a project request submitted through the relevant disease consortium, whose forms differ by cancer type. That request goes to the consortium's executive committee or its designated review group, which may accept it, reject it, or send it back for revision. After approval a data use agreement, or DUA, is executed with the PCDC legal team, and data is released only once that DUA is signed. A study spanning several cancer types needs separate approval from each consortium involved.

## Guppy, the query endpoint

Guppy is the Gen3 GraphQL service the portal queries underneath, and it is what this project talks to as well. Aggregate subject counts are available from https://portal.pedscommons.org/guppy/graphql/ without any Gen3 credentials, which is how cohort counts are produced here. The Explorer web interface is a different matter and redirects to a login page. Nothing about the public endpoint changes the rules above: line-level and restricted data still go through the normal access process.

## Working without Gen3 credentials

Without credentials this assistant still builds and validates filters, answers questions about schema fields and their allowed values, answers documentation questions from this knowledge base, and asks Guppy for public aggregate counts when the deployment has execution turned on. What it cannot do is reach patient-level detail or anything access-restricted, and no amount of rephrasing a question changes that.
