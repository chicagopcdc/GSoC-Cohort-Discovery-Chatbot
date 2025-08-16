"""
LLM Prompt Templates for PCDC Chatbot Backend

This module contains all prompt templates used for:
1. Query normalization and term extraction (Step 1)
2. Field disambiguation and conflict resolution (Step 3)
"""

# Query Normalization Prompts (Step 1)
QUERY_NORMALIZATION_PROMPT = """
You are a medical data query assistant. Analyze this natural language query and extract meaningful terms that could match medical database fields.

Query: "{query}"

Extract terms following these rules:
1. Focus on medical conditions, treatments, demographics, and clinical characteristics
2. Normalize medical terminology (e.g., "cancer" → "carcinoma", "chemo" → "chemotherapy")
3. Identify logical operators (AND/OR relationships)
4. Remove stop words and non-medical terms
5. Assign confidence scores (0.0-1.0) based on medical relevance

Respond in this JSON format:
{{
    "terms": [
        {{
            "original": "original term as appears in query",
            "normalized": "medical standard term",
            "position": 0,
            "confidence": 0.95
        }}
    ],
    "logic": "AND" or "OR",
    "confidence": 0.9,
    "explanation": "Brief explanation of extraction logic"
}}

Examples:
- "kids with leukemia" → terms: [{{original: "kids", normalized: "pediatric", position: 0, confidence: 0.85}}, {{original: "leukemia", normalized: "leukemia", position: 1, confidence: 0.98}}]
- "breast or lung cancer" → logic: "OR", terms for breast and lung cancer
"""

TERM_EXPANSION_PROMPT = """
For the medical term "{term}", provide alternative terms and synonyms that might appear in a medical database.

Consider:
- Medical synonyms and alternative spellings
- Abbreviations and full forms
- Related conditions or subtypes
- Common misspellings

Respond with a JSON list of alternative terms with confidence scores:
{{
    "alternatives": [
        {{
            "term": "alternative_term",
            "confidence": 0.9,
            "type": "synonym|abbreviation|subtype|misspelling"
        }}
    ]
}}
"""

# Field Disambiguation Prompts (Step 3)
FIELD_DISAMBIGUATION_PROMPT = """
You are resolving conflicts where a medical term matches multiple database fields. Choose the most appropriate field based on the original query context.

Original Query: "{original_query}"
Term: "{term}"

Candidate Fields:
{candidate_fields}

Consider:
1. Medical context and domain relevance
2. Field descriptions and typical usage
3. Query intent and user's likely goal
4. Clinical significance

Respond in JSON format:
{{
    "chosen_field": "field_path",
    "confidence": 0.95,
    "reasoning": "Why this field is most appropriate",
    "alternative_fields": [
        {{
            "field_path": "alternative_path",
            "confidence": 0.3,
            "reason": "Why this could also work"
        }}
    ]
}}
"""

CONFLICT_RESOLUTION_PROMPT = """
Multiple medical terms from the query map to overlapping or conflicting database fields. Resolve these conflicts to create a coherent query.

Original Query: "{original_query}"

Conflicts:
{conflicts}

Resolve by:
1. Identifying semantic overlaps (e.g., "cancer" and "tumor" might refer to same field)
2. Choosing more specific terms over general ones
3. Maintaining clinical accuracy
4. Preserving user intent

Respond in JSON format:
{{
    "resolved_fields": [
        {{
            "term": "original_term",
            "field_path": "resolved_path",
            "value": "filter_value",
            "operator": "eq|in|contains",
            "confidence": 0.9
        }}
    ],
    "removed_conflicts": [
        {{
            "term": "conflicting_term",
            "reason": "Why this was not chosen"
        }}
    ],
    "warnings": ["Any potential issues with the resolution"]
}}
"""

# Value Extraction and Validation Prompts
VALUE_EXTRACTION_PROMPT = """
Extract the specific value that should be used to filter the field "{field_path}" based on the term "{term}" from the original query "{original_query}".

Field Information:
- Type: {field_type}
- Valid Values: {valid_values}
- Description: {field_description}

Extract the most appropriate filter value:
1. For enumerations, choose from valid values or suggest closest match
2. For strings, extract relevant substring for "contains" filtering
3. For numbers, extract numeric value or range
4. Consider partial matches and synonyms

Respond in JSON format:
{{
    "value": "extracted_value_or_list",
    "operator": "eq|in|contains|gte|lte",
    "confidence": 0.95,
    "reasoning": "Why this value was chosen",
    "alternatives": [
        {{
            "value": "alternative_value",
            "confidence": 0.7
        }}
    ]
}}
"""

# System Prompts for Context
SYSTEM_CONTEXT_PROMPT = """
You are an expert medical informatics assistant working with a pediatric cancer database. Your role is to help convert natural language queries into precise database searches while maintaining medical accuracy and clinical relevance.

Key Context:
- Database contains pediatric cancer patient data
- Fields cover demographics, diagnoses, treatments, outcomes
- Users are typically researchers, clinicians, or data analysts
- Queries should be interpreted with medical precision
- When uncertain, prefer broader matches over missed opportunities
"""

# Prompt Templates with Placeholders
PROMPT_TEMPLATES = {
    "normalize_query": QUERY_NORMALIZATION_PROMPT,
    "expand_term": TERM_EXPANSION_PROMPT,
    "disambiguate_field": FIELD_DISAMBIGUATION_PROMPT,
    "resolve_conflicts": CONFLICT_RESOLUTION_PROMPT,
    "extract_value": VALUE_EXTRACTION_PROMPT,
    "system_context": SYSTEM_CONTEXT_PROMPT
}

def get_prompt_template(template_name: str) -> str:
    """Get a prompt template by name"""
    return PROMPT_TEMPLATES.get(template_name, "")

def format_prompt(template_name: str, **kwargs) -> str:
    """Format a prompt template with provided variables"""
    template = get_prompt_template(template_name)
    return template.format(**kwargs) 