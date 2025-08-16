"""
Conflict Disambiguator - Step 3 of Pipeline

Responsible for resolving conflicts when multiple fields match the same term
using LLM-based disambiguation or rule-based fallback.
"""

import json
import re
from typing import List, Dict, Any, Optional
from openai import AsyncOpenAI

from core.models import FieldMatches, ResolvedFields, ResolvedField, FieldType
from core.config import config
from utils.logging import get_logger
from utils.errors import ConflictResolutionError
from .prompts import format_prompt

logger = get_logger(__name__)


class ConflictDisambiguator:
    """LLM-based conflict disambiguator for Step 3 of the pipeline"""
    
    def __init__(self):
        """Initialize the disambiguator with OpenAI client"""
        self.client = None
        if config.OPENAI_API_KEY:
            self.client = AsyncOpenAI(api_key=config.OPENAI_API_KEY)
        else:
            logger.warning("No OpenAI API key provided, will use rule-based disambiguation only")
    
    async def resolve_conflicts(self, field_matches: FieldMatches, original_query: str) -> ResolvedFields:
        """
        Resolve conflicts in field mappings
        
        Args:
            field_matches: Candidate field matches from Step 2
            original_query: Original user query for context
            
        Returns:
            ResolvedFields with definitive mappings
            
        Raises:
            ConflictResolutionError: If resolution fails
        """
        try:
            if self.client and config.ENABLE_LLM_DISAMBIGUATION:
                return await self._llm_resolve_conflicts(field_matches, original_query)
            else:
                return self._rule_based_resolve_conflicts(field_matches, original_query)
        except Exception as e:
            logger.error(f"Conflict resolution failed: {e}")
            raise ConflictResolutionError(f"Failed to resolve conflicts: {e}")
    
    async def _llm_resolve_conflicts(self, field_matches: FieldMatches, original_query: str) -> ResolvedFields:
        """Use LLM to resolve field conflicts"""
        
        # Group candidates by term
        term_groups = self._group_candidates_by_term(field_matches.candidates)
        
        resolved = []
        conflicts = []
        warnings = []
        
        for term, candidates in term_groups.items():
            try:
                if len(candidates) == 1:
                    # No conflict, use the single candidate
                    candidate = candidates[0]
                    resolved_field = await self._create_resolved_field(candidate, original_query)
                    resolved.append(resolved_field)
                else:
                    # Multiple candidates, need disambiguation
                    disambiguated = await self._disambiguate_candidates(
                        term, candidates, original_query
                    )
                    resolved.extend(disambiguated["resolved"])
                    conflicts.extend(disambiguated["conflicts"])
                    warnings.extend(disambiguated["warnings"])
                    
            except Exception as e:
                logger.warning(f"Failed to resolve term '{term}': {e}")
                # Fallback to rule-based for this term
                fallback = self._rule_based_resolve_term(term, candidates)
                resolved.extend(fallback["resolved"])
                conflicts.extend(fallback["conflicts"])
        
        return ResolvedFields(
            resolved=resolved,
            conflicts=conflicts,
            warnings=warnings + field_matches.unmatched_terms
        )
    
    async def _disambiguate_candidates(self, term: str, candidates: List, original_query: str) -> Dict[str, Any]:
        """Disambiguate multiple candidates for a single term using LLM"""
        
        # Format candidate information for LLM
        candidate_info = []
        for candidate in candidates:
            field_info = {
                "field_path": candidate.field.path,
                "description": candidate.field.description or "No description",
                "type": candidate.field.field_type.value,
                "match_score": candidate.match_score,
                "match_reason": candidate.match_reason
            }
            if candidate.field.enum_values:
                field_info["valid_values"] = candidate.field.enum_values[:10]  # Limit for prompt
            candidate_info.append(field_info)
        
        # Create prompt
        prompt = format_prompt(
            "disambiguate_field",
            original_query=original_query,
            term=term,
            candidate_fields=json.dumps(candidate_info, indent=2)
        )
        
        try:
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
                json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
                if json_match:
                    result = json.loads(json_match.group())
                else:
                    raise ValueError("No valid JSON in LLM response")
            
            # Find the chosen candidate
            chosen_path = result.get("chosen_field")
            chosen_candidate = next(
                (c for c in candidates if c.field.path == chosen_path), 
                candidates[0]  # Fallback to first candidate
            )
            
            # Create resolved field with LLM-extracted value
            resolved_field = await self._create_resolved_field_with_value(
                chosen_candidate, term, original_query, result.get("confidence", 0.8)
            )
            
            # Record conflict information
            conflict_info = {
                "term": term,
                "candidates": [c.field.path for c in candidates],
                "chosen": chosen_path,
                "reasoning": result.get("reasoning", "LLM disambiguation"),
                "confidence": result.get("confidence", 0.8),
                "alternatives": result.get("alternative_fields", [])
            }
            
            return {
                "resolved": [resolved_field],
                "conflicts": [conflict_info],
                "warnings": []
            }
            
        except Exception as e:
            logger.warning(f"LLM disambiguation failed for term '{term}': {e}")
            # Fallback to rule-based disambiguation
            return self._rule_based_resolve_term(term, candidates)
    
    async def _create_resolved_field(self, candidate, original_query: str) -> ResolvedField:
        """Create a resolved field from a candidate"""
        return await self._create_resolved_field_with_value(
            candidate, candidate.term, original_query, candidate.match_score
        )
    
    async def _create_resolved_field_with_value(
        self, candidate, term: str, original_query: str, confidence: float
    ) -> ResolvedField:
        """Create resolved field with LLM-extracted value"""
        
        # Extract appropriate value for the field
        if self.client and config.ENABLE_LLM_DISAMBIGUATION:
            value_result = await self._extract_field_value(
                candidate.field, term, original_query
            )
            value = value_result.get("value", term)
            operator = value_result.get("operator", self._default_operator(candidate.field))
        else:
            value = term
            operator = self._default_operator(candidate.field)
        
        return ResolvedField(
            term=term,
            field_path=candidate.field.path,
            field_type=candidate.field.field_type,
            value=value,
            operator=operator,
            confidence=confidence
        )
    
    async def _extract_field_value(self, field, term: str, original_query: str) -> Dict[str, Any]:
        """Extract the appropriate value for a field using LLM"""
        
        prompt = format_prompt(
            "extract_value",
            field_path=field.path,
            term=term,
            original_query=original_query,
            field_type=field.field_type.value,
            valid_values=field.enum_values or ["N/A"],
            field_description=field.description or "No description"
        )
        
        try:
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
            
            return {
                "value": result.get("value", term),
                "operator": result.get("operator", self._default_operator(field)),
                "confidence": result.get("confidence", 0.8),
                "reasoning": result.get("reasoning", "")
            }
            
        except Exception as e:
            logger.warning(f"Value extraction failed for field '{field.path}': {e}")
            return {
                "value": term,
                "operator": self._default_operator(field),
                "confidence": 0.6
            }
    
    def _rule_based_resolve_conflicts(self, field_matches: FieldMatches, original_query: str) -> ResolvedFields:
        """Fallback rule-based conflict resolution"""
        
        term_groups = self._group_candidates_by_term(field_matches.candidates)
        
        resolved = []
        conflicts = []
        
        for term, candidates in term_groups.items():
            result = self._rule_based_resolve_term(term, candidates)
            resolved.extend(result["resolved"])
            conflicts.extend(result["conflicts"])
        
        return ResolvedFields(
            resolved=resolved,
            conflicts=conflicts,
            warnings=field_matches.unmatched_terms
        )
    
    def _rule_based_resolve_term(self, term: str, candidates: List) -> Dict[str, Any]:
        """Rule-based resolution for a single term"""
        
        if len(candidates) == 1:
            # No conflict
            candidate = candidates[0]
            resolved_field = ResolvedField(
                term=term,
                field_path=candidate.field.path,
                field_type=candidate.field.field_type,
                value=term,
                operator=self._default_operator(candidate.field),
                confidence=candidate.match_score
            )
            return {"resolved": [resolved_field], "conflicts": []}
        
        # Multiple candidates - use scoring heuristics
        scored_candidates = []
        
        for candidate in candidates:
            score = candidate.match_score
            
            # Boost score for exact matches
            if term.lower() in candidate.field.path.lower():
                score += 0.1
            
            # Boost score for enumeration fields (more specific)
            if candidate.field.field_type == FieldType.ENUMERATION:
                score += 0.05
            
            # Boost score for fields with descriptions
            if candidate.field.description:
                score += 0.02
            
            # Check if term matches any enum values
            if (candidate.field.enum_values and 
                any(term.lower() in enum_val.lower() for enum_val in candidate.field.enum_values)):
                score += 0.15
            
            scored_candidates.append((candidate, score))
        
        # Sort by score and pick the best
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        best_candidate, best_score = scored_candidates[0]
        
        # Apply penalty for conflicts
        confidence = best_score * 0.9
        
        resolved_field = ResolvedField(
            term=term,
            field_path=best_candidate.field.path,
            field_type=best_candidate.field.field_type,
            value=self._extract_enum_value(term, best_candidate.field) if best_candidate.field.field_type == FieldType.ENUMERATION else term,
            operator=self._default_operator(best_candidate.field),
            confidence=confidence
        )
        
        conflict_info = {
            "term": term,
            "candidates": [c.field.path for c in candidates],
            "chosen": best_candidate.field.path,
            "reasoning": f"Highest score ({best_score:.3f}) using rule-based heuristics",
            "confidence": confidence
        }
        
        return {
            "resolved": [resolved_field],
            "conflicts": [conflict_info]
        }
    
    def _group_candidates_by_term(self, candidates: List) -> Dict[str, List]:
        """Group candidates by their matching term"""
        groups = {}
        for candidate in candidates:
            term = candidate.term
            if term not in groups:
                groups[term] = []
            groups[term].append(candidate)
        return groups
    
    def _default_operator(self, field) -> str:
        """Get default operator for a field type"""
        if field.field_type == FieldType.ENUMERATION:
            return "eq"
        elif field.field_type == FieldType.STRING:
            return "contains"
        elif field.field_type in [FieldType.NUMBER, FieldType.DATE]:
            return "eq"
        elif field.field_type == FieldType.BOOLEAN:
            return "eq"
        else:
            return "contains"
    
    def _extract_enum_value(self, term: str, field) -> str:
        """Extract the best matching enum value for a term"""
        if not field.enum_values:
            return term
        
        term_lower = term.lower()
        
        # Look for exact matches first
        for enum_val in field.enum_values:
            if term_lower == enum_val.lower():
                return enum_val
        
        # Look for partial matches
        for enum_val in field.enum_values:
            if term_lower in enum_val.lower() or enum_val.lower() in term_lower:
                return enum_val
        
        # Return first enum value as fallback
        return field.enum_values[0]


# Global disambiguator instance
_disambiguator = None

def get_conflict_disambiguator() -> ConflictDisambiguator:
    """Get the global conflict disambiguator instance"""
    global _disambiguator
    if _disambiguator is None:
        _disambiguator = ConflictDisambiguator()
    return _disambiguator 