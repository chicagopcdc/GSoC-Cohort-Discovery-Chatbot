# Chatbot and data limitations

Last reviewed: 2026-08-10

Source URLs:
- https://docs.pedscommons.org/StatisticalManual/
- https://docs.pedscommons.org/TermsAndConditions/
- local implementation: src/backend/services/cohort_analyzer.py

## Why a count comes back as -1 or minus one

The portal will not show a cohort smaller than five subjects. Filters landing under that limit appear locked in the Explorer, and the Guppy endpoint answers with a count of -1, minus one, instead of the real number. That masked count means "fewer than five subjects, withheld"; it is not a count of zero and not an error. Stacking several narrow filters is the usual way to reach it, and a cohort whose count is masked this way is not necessarily empty.

## Reading the data carefully

The data comes from many trials run over many years, and that shows. Treatment was not uniform across studies, arms differed, and eligibility criteria differed; results drawn from older legacy trials may not reflect what contemporary regimens achieve. Risk stratification and staging systems vary between studies, and anatomical sites are sometimes described at different levels of detail. Fields are missing in patterns that are rarely random, because some variables only became routine in later studies and others never applied to a given disease.

## Statistical pitfalls

The PCDC statistical manual is direct about the ways cohort exploration goes wrong. Running the same kind of test repeatedly inflates Type I error. Searching a large harmonized dataset for whatever comes out significant is p-hacking, and it is easy to do accidentally here. Published evidence behind the underlying trials carries its own bias, and external validation datasets are scarce, so internal validation needs its data split decided before the analysis starts.

## What this assistant will not do

It will not give medical advice, and it will not fill a gap with a plausible-sounding answer. Counts and distributions come from running a query against Guppy, never from the language model, so if execution is unavailable it says so rather than estimating. When the curated documentation does not cover a question, the honest answer is that it does not cover it.

## Terms of use

Search results carry a limited license for preparing a research project, and the source should be credited as the PCDC Data Portal, operated by Data for the Common Good (D4CG). Accounts are personal and credentials are not to be shared. D4CG provides the platform as is and does not warrant that the data is accurate or complete.

## Cohort analysis scope

Cohort analysis in its current version reports the total count plus distributions of top-level categorical fields such as sex, race, ethnicity and consortium. Histograms over nested fields like tumor site, and numeric summaries such as mean age or an age range, need Guppy query support that is not built yet.
