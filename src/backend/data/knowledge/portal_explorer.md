# Using the portal explorer

Last reviewed: 2026-08-10

Source URLs:
- https://docs.pedscommons.org/DataPortalUserGuide/
- https://portal.pedscommons.org/explorer

## Filters and how they combine

The Exploration page has a filter panel grouped into domains such as Subject, Disease and Molecular. Categorical variables are checkboxes; continuous ones like age use a slider measured in days. The combination rule catches people out: ticking several boxes inside one filter is an OR, while filters in different groups are ANDed together. The operator between filter groups can be switched, and individual filters can be dropped without clearing the rest.

## Include, exclude, and the anchor filter

Most filters can be set to include or exclude the values you pick. Separately, an anchor filter narrows which observations count at all by pinning them to a disease phase, either Initial Diagnosis or Relapse. That matters for the nested clinical tables, where a subject can have records from several phases and an unanchored filter will match any of them.

## Saving and sharing a cohort

A filter set can be saved with a name and an optional description. Duplicate copies one so it can be altered while the original stays intact, and Share issues a token that lets another user load the same filter set by email. Two constraints are worth knowing up front: a survival curve can only be built from a saved filter set, and using a Composed filter greys out the left-hand filter panel.

## How this relates to the chatbot

The filters this assistant builds target the same underlying data as the Explorer, through the same Guppy endpoint, so a cohort defined here should count the same as the equivalent one built by hand in the portal. The Explorer itself needs a login; the aggregate counts this assistant reports do not. Saving, sharing and survival curves are portal features and are not available through the conversation.
