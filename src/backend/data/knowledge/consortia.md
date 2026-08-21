# PCDC consortia

Last reviewed: 2026-08-10

Source URLs:
- https://commons.cri.uchicago.edu/pcdc/
- local schema: schema/pcdc-schema-prod-20260414.json

## Disease-specific consortia and the cancer types they cover

PCDC organizes its work around consortia, each one an international group of clinicians and researchers for a particular cancer type. The cancer types covered publicly are INRG for neuroblastoma, INSTRuCT for soft tissue sarcoma, NODAL for Hodgkin lymphoma, INTERACT for acute myeloid leukemia, MaGIC for germ cell tumors, ALLIES for acute lymphoblastic leukemia, HIBiSCus for bone tumors, INSPiRE for CNS tumors, CHIC for liver tumors, Global REACH for retinoblastoma, NOBLE for nasopharyngeal carcinoma, C3P for cancer predisposition, LINEAGE for Lynch syndrome, FRIENDS for Fanconi anemia, and Reproductive HOPE for oncofertility. Each consortium owns the data dictionary for its disease and reviews project requests against its own data.

## Consortium values you can filter on

The `consortium` field sits at the top level of the subject record, so it does not need a nested filter. In the schema this project loads, it accepts INSTRuCT, MaGIC, INRG, NODAL, INTERACT, HIBISCUS and ALL. Note the spelling: the schema writes HIBISCUS in capitals where the PCDC website writes HIBiSCus. A filter has to use the exact schema spelling or it will not validate.

## Which consortia actually have subjects

Seven consortium values validate, but only five return subjects in the current release: INRG with 26,529, INSTRuCT with 10,311, INTERACT with 4,384, NODAL with 2,715 and MaGIC with 2,708. HIBISCUS and ALL are accepted by the schema and come back empty. A filter on one of those two is not malformed, it simply matches nobody, which is worth knowing before spending time debugging a cohort that returns zero.
