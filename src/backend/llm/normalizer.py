"""
Query Normalizer - Step 1 of Pipeline

Responsible for parsing natural language queries into structured terms
using LLM-based normalization or rule-based fallback.
"""

import json
import re
import asyncio
from typing import List, Dict, Any, Optional
from openai import AsyncOpenAI

from core.models import ParsedQuery, ParsedTerm, LogicOperator
from core.config import config
from utils.logging import get_logger
from utils.errors import QueryParsingError
from .prompts import format_prompt

logger = get_logger(__name__)


class QueryNormalizer:
    """LLM-based query normalizer for Step 1 of the pipeline"""
    
    def __init__(self):
        """Initialize the normalizer with OpenAI client"""
        self.client = None
        if config.OPENAI_API_KEY:
            self.client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)
        else:
            logger.warning("No OpenAI API key provided, will use rule-based parsing only")
    
    async def parse_query(self, query_text: str) -> ParsedQuery:
        """
        Parse natural language query into structured terms
        
        Args:
            query_text: Raw user query
            
        Returns:
            ParsedQuery with extracted terms and logic
            
        Raises:
            QueryParsingError: If parsing fails
        """
        try:
            if self.client and config.ENABLE_LLM_NORMALIZATION:
                return await self._llm_parse_query(query_text)
            else:
                return self._rule_based_parse_query(query_text)
        except Exception as e:
            logger.error(f"Query parsing failed for '{query_text}': {e}")
            raise QueryParsingError(f"Failed to parse query: {e}", query=query_text)
    
    async def _llm_parse_query(self, query_text: str) -> ParsedQuery:
        """Use LLM to parse the query"""
        try:
            prompt = format_prompt("normalize_query", query=query_text)
            
            response = await self.client.chat.completions.create(
                model=config.LLM_MODEL,
                messages=[
                    {"role": "system", "content": format_prompt("system_context")},
                    {"role": "user", "content": prompt}
                ],
                temperature=config.LLM_TEMPERATURE,
                max_tokens=1000
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            try:
                result = json.loads(result_text)
            except json.JSONDecodeError:
                # Try to extract JSON from the response
                json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    raise ValueError("No valid JSON found in LLM response")
            
            # Convert to ParsedQuery
            terms = []
            for i, term_data in enumerate(result.get("terms", [])):
                terms.append(ParsedTerm(
                    original=term_data["original"],
                    normalized=term_data["normalized"],
                    position=term_data.get("position", i),
                    confidence=term_data.get("confidence", 0.8)
                ))
            
            logic_str = result.get("logic", "AND").upper()
            logic = LogicOperator.OR if logic_str == "OR" else LogicOperator.AND
            
            parsed_query = ParsedQuery(
                terms=terms,
                logic=logic,
                raw_query=query_text,
                confidence=result.get("confidence", 0.8)
            )
            
            logger.debug(f"LLM parsed query into {len(terms)} terms with {logic} logic")
            return parsed_query
            
        except Exception as e:
            logger.warning(f"LLM parsing failed, falling back to rule-based: {e}")
            return self._rule_based_parse_query(query_text)
    
    def _rule_based_parse_query(self, query_text: str) -> ParsedQuery:
        """Fallback rule-based query parsing"""
        # Clean and tokenize
        words = re.findall(r'\b\w+\b', query_text.lower())
        
        # Detect logic operators
        logic = LogicOperator.AND
        if any(word in ["or", "either"] for word in words):
            logic = LogicOperator.OR
        
        # Medical term patterns and normalizations
        medical_normalizations = {
            # Demographics
            r'\b(kids?|children|child|pediatric|paediatric)\b': 'pediatric',
            r'\b(boys?|males?)\b': 'male',
            r'\b(girls?|females?)\b': 'female',
            r'\b(adults?|grown.?ups?)\b': 'adult',
            
            # Cancer types
            r'\b(cancers?|tumou?rs?|neoplasms?|malignancy)\b': 'cancer',
            r'\b(leukemia|leukaemia)\b': 'leukemia',
            r'\b(lymphomas?)\b': 'lymphoma',
            r'\b(sarcomas?)\b': 'sarcoma',
            r'\b(carcinomas?)\b': 'carcinoma',
            
            # Treatments
            r'\b(chemo|chemotherapy)\b': 'chemotherapy',
            r'\b(radio|radiotherapy|radiation)\b': 'radiotherapy',
            r'\b(surgery|surgical|operation)\b': 'surgery',
            r'\b(transplant|bone.?marrow.?transplant|bmt)\b': 'transplant',
            
            # Outcomes
            r'\b(died|death|mortality|fatal)\b': 'death',
            r'\b(survived|survival|alive)\b': 'survival',
            r'\b(relapsed?|recurrence|return)\b': 'relapse',
            r'\b(remission|complete.?response|cr)\b': 'remission',
            
            # Body parts/systems
            r'\b(brain|cranial|cerebral)\b': 'brain',
            r'\b(blood|hematologic|haematologic)\b': 'blood',
            r'\b(bone|skeletal|osseous)\b': 'bone',
            r'\b(lung|pulmonary|respiratory)\b': 'lung',
        }
        
        # Apply normalizations
        normalized_query = query_text.lower()
        for pattern, replacement in medical_normalizations.items():
            normalized_query = re.sub(pattern, replacement, normalized_query)
        
        # Extract meaningful terms
        normalized_words = re.findall(r'\b\w+\b', normalized_query)
        
        # Filter stop words and short terms
        stop_words = {
            "and", "or", "the", "a", "an", "is", "are", "was", "were",
            "with", "for", "of", "in", "at", "to", "from", "by", "as",
            "have", "has", "had", "do", "does", "did", "will", "would",
            "could", "should", "may", "might", "can", "than", "then",
            "who", "what", "where", "when", "why", "how", "all", "any",
            "both", "each", "few", "more", "most", "other", "some", "such",
            "only", "own", "same", "so", "than", "too", "very", "just"
        }
        
        terms = []
        position = 0
        seen_terms = set()
        
        for i, (original, normalized) in enumerate(zip(words, normalized_words)):
            if (normalized not in stop_words and 
                len(normalized) >= config.MIN_TERM_LENGTH and
                normalized not in seen_terms):
                
                # Calculate confidence based on medical relevance
                confidence = 0.6  # Base confidence for rule-based
                
                # Boost confidence for medical terms
                if any(pattern.search(normalized) for pattern in medical_normalizations.keys()):
                    confidence = 0.8
                
                # Boost confidence for cancer-related terms
                cancer_terms = {"cancer", "tumor", "carcinoma", "sarcoma", "lymphoma", "leukemia"}
                if normalized in cancer_terms:
                    confidence = 0.9
                
                terms.append(ParsedTerm(
                    original=original,
                    normalized=normalized,
                    position=position,
                    confidence=confidence
                ))
                
                seen_terms.add(normalized)
                position += 1
        
        # Ensure we have at least one term
        if not terms:
            # Use the longest word as a fallback
            longest_word = max(words, key=len) if words else "query"
            terms.append(ParsedTerm(
                original=longest_word,
                normalized=longest_word.lower(),
                position=0,
                confidence=0.5
            ))
        
        return ParsedQuery(
            terms=terms,
            logic=logic,
            raw_query=query_text,
            confidence=0.7  # Lower confidence for rule-based parsing
        )
    
    async def expand_term(self, term: str) -> List[Dict[str, Any]]:
        """
        Expand a term into alternative forms using LLM
        
        Args:
            term: Term to expand
            
        Returns:
            List of alternative terms with confidence scores
        """
        if not self.client or not config.ENABLE_LLM_NORMALIZATION:
            return self._rule_based_expand_term(term)
        
        try:
            prompt = format_prompt("expand_term", term=term)
            
            response = await self.client.chat.completions.create(
                model=config.LLM_MODEL,
                messages=[
                    {"role": "system", "content": format_prompt("system_context")},
                    {"role": "user", "content": prompt}
                ],
                temperature=config.LLM_TEMPERATURE,
                max_tokens=500
            )
            
            result_text = response.choices[0].message.content.strip()
            result = json.loads(result_text)
            
            return result.get("alternatives", [])
            
        except Exception as e:
            logger.warning(f"LLM term expansion failed for '{term}': {e}")
            return self._rule_based_expand_term(term)
    
    def _rule_based_expand_term(self, term: str) -> List[Dict[str, Any]]:
        """Rule-based term expansion (fallback)"""
        term_lower = term.lower()
        alternatives = []
        
        # Medical term expansions
        expansions = {
            "cancer": [
                {"term": "tumor", "confidence": 0.9, "type": "synonym"},
                {"term": "neoplasm", "confidence": 0.8, "type": "synonym"},
                {"term": "malignancy", "confidence": 0.8, "type": "synonym"},
                {"term": "carcinoma", "confidence": 0.7, "type": "subtype"},
                {"term": "sarcoma", "confidence": 0.7, "type": "subtype"},
            ],
            "leukemia": [
                {"term": "leukaemia", "confidence": 0.95, "type": "spelling"},
                {"term": "blood cancer", "confidence": 0.8, "type": "synonym"},
                {"term": "ALL", "confidence": 0.7, "type": "abbreviation"},
                {"term": "AML", "confidence": 0.7, "type": "abbreviation"},
            ],
            "pediatric": [
                {"term": "kids", "confidence": 0.9, "type": "synonym"},
                {"term": "children", "confidence": 0.9, "type": "synonym"},
                {"term": "child", "confidence": 0.9, "type": "synonym"},
                {"term": "paediatric", "confidence": 0.95, "type": "spelling"},
            ],
            "chemotherapy": [
                {"term": "chemo", "confidence": 0.9, "type": "abbreviation"},
                {"term": "chemotherapeutic", "confidence": 0.8, "type": "variant"},
                {"term": "systemic therapy", "confidence": 0.7, "type": "synonym"},
            ]
        }
        
        return expansions.get(term_lower, [])


# Global normalizer instance
_normalizer = None

def get_query_normalizer() -> QueryNormalizer:
    """Get the global query normalizer instance"""
    global _normalizer
    if _normalizer is None:
        _normalizer = QueryNormalizer()
    return _normalizer 