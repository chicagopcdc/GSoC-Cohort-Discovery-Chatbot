# PCDC consortia

Last reviewed: 2026-08-05

Source URLs:
- https://commons.cri.uchicago.edu/pcdc/
- local schema: schema/pcdc-schema-prod-20260414.json

## Disease-specific consortia

PCDC works with disease-specific alliances and consortia that collect and harmonize data for pediatric, adolescent and young adult, and adult cancer types. Public PCDC materials list groups such as INRG for neuroblastoma, NODAL for Hodgkin lymphoma, INSTRuCT for soft tissue sarcoma, MaGIC for germ cell tumors, INTERACT for acute myeloid leukemia, and others.

## Locally filterable consortium values

In the schema version currently loaded by this project, the top-level subject field `consortium` has these enum values: INSTRuCT, MaGIC, INRG, NODAL, INTERACT, HIBISCUS, and ALL. When the chatbot builds a cohort filter, it should use the exact enum values available in the local schema.
