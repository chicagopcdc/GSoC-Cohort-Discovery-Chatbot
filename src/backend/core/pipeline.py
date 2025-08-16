"""
5-Step Pipeline Orchestration for PCDC Chatbot Backend

This module implements the core 5-step workflow:
1. Parse user query into terms and logic
2. Find matching catalog fields for each term
3. Resolve conflicts and ambiguities
4. Build GraphQL filter structure
5. Generate final GraphQL query

The pipeline is designed to be modular, testable, and easily extensible.
"""

import time
import uuid
from typing import Dict, List, Any, Optional

from .config import config
from .models import (
    QueryRequest, ParsedQuery, FieldMatches, ResolvedFields, 
    FilterStructure, GraphQLQuery, PipelineResult
)
from utils.errors import (
    QueryParsingError, FieldMappingError, ConflictResolutionError,
    FilterBuildingError, QueryGenerationError
)
from utils.logging import get_logger, PipelineLogger
from catalog.index import get_catalog_index
from llm.normalizer import get_query_normalizer
from llm.disambiguator import get_conflict_disambiguator
from graphql.composer import get_filter_composer
from graphql.builder import get_query_builder

logger = get_logger(__name__)


class Pipeline:
    """Main pipeline orchestrator for the 5-step workflow"""
    
    def __init__(self):
        """Initialize the pipeline with all required components"""
        self.catalog_index = get_catalog_index()
        self.query_normalizer = get_query_normalizer()
        self.conflict_disambiguator = get_conflict_disambiguator()
        self.filter_composer = get_filter_composer()
        self.query_builder = get_query_builder()
        
        logger.info("Initialized pipeline orchestrator")
    
    async def process_query(self, request: QueryRequest) -> PipelineResult:
        """
        Process a complete query through the 5-step pipeline
        
        Args:
            request: Query request containing text and session info
            
        Returns:
            Complete pipeline result with all step outputs
            
        Raises:
            Various pipeline-specific exceptions for different failure modes
        """
        start_time = time.time()
        session_id = request.session_id or str(uuid.uuid4())
        trace_id = str(uuid.uuid4())
        
        pipeline_logger = PipelineLogger(trace_id=trace_id, session_id=session_id)
        
        try:
            logger.info(f"Starting pipeline for query: '{request.text}' (session: {session_id})")
            
            # Step 1: Parse user query
            pipeline_logger.log_step_start("parse_query", {"query": request.text})
            step1_start = time.time()
            parsed_query = await self._step1_parse_query(request.text)
            step1_duration = time.time() - step1_start
            pipeline_logger.log_step_end("parse_query", {"parsed_terms": len(parsed_query.terms)}, step1_duration)
            
            # Step 2: Find matching fields
            pipeline_logger.log_step_start("find_fields", {"terms": [t.original for t in parsed_query.terms]})
            step2_start = time.time()
            field_matches = await self._step2_find_matching_fields(parsed_query)
            step2_duration = time.time() - step2_start
            pipeline_logger.log_step_end("find_fields", {"candidates": len(field_matches.candidates)}, step2_duration)
            
            # Step 3: Resolve conflicts
            pipeline_logger.log_step_start("resolve_conflicts", {"candidates": len(field_matches.candidates)})
            step3_start = time.time()
            resolved_fields = await self._step3_resolve_conflicts(field_matches, request.text)
            step3_duration = time.time() - step3_start
            pipeline_logger.log_step_end("resolve_conflicts", {"resolved": len(resolved_fields.resolved)}, step3_duration)
            
            # Step 4: Build filter structure
            pipeline_logger.log_step_start("build_filter", {"resolved_fields": len(resolved_fields.resolved)})
            step4_start = time.time()
            filter_structure = await self._step4_build_filter(resolved_fields, parsed_query)
            step4_duration = time.time() - step4_start
            pipeline_logger.log_step_end("build_filter", {"filters": len(filter_structure.filters)}, step4_duration)
            
            # Step 5: Generate final query
            pipeline_logger.log_step_start("generate_query", {"filters": len(filter_structure.filters)})
            step5_start = time.time()
            final_query = await self._step5_generate_query(filter_structure)
            step5_duration = time.time() - step5_start
            pipeline_logger.log_step_end("generate_query", {"query_length": len(final_query.query)}, step5_duration)
            
            total_duration = time.time() - start_time
            
            result = PipelineResult(
                session_id=session_id,
                parsed_query=parsed_query,
                field_matches=field_matches,
                resolved_fields=resolved_fields,
                filter_structure=filter_structure,
                final_query=final_query,
                processing_time=total_duration,
                trace_id=trace_id
            )
            
            logger.info(f"Pipeline completed successfully in {total_duration:.3f}s (session: {session_id})")
            return result
            
        except Exception as e:
            pipeline_logger.log_step_error("pipeline", e, {"query": request.text})
            logger.error(f"Pipeline failed for session {session_id}: {e}")
            raise
    
    async def _step1_parse_query(self, query_text: str) -> ParsedQuery:
        """
        Step 1: Parse user query into terms and logical structure
        
        Args:
            query_text: Raw user query text
            
        Returns:
            ParsedQuery with extracted terms and logic
            
        Raises:
            QueryParsingError: If parsing fails
        """
        try:
            if config.ENABLE_LLM_NORMALIZATION:
                # Use LLM-based normalization
                parsed_query = await self.query_normalizer.parse_query(query_text)
            else:
                # Use simple rule-based parsing
                parsed_query = self._simple_parse_query(query_text)
            
            logger.debug(f"Step 1 completed: extracted {len(parsed_query.terms)} terms")
            return parsed_query
            
        except Exception as e:
            raise QueryParsingError(f"Failed to parse query: {e}", query=query_text)
    
    async def _step2_find_matching_fields(self, parsed_query: ParsedQuery) -> FieldMatches:
        """
        Step 2: Find catalog fields matching each parsed term
        
        Args:
            parsed_query: Result from step 1
            
        Returns:
            FieldMatches with candidates for each term
            
        Raises:
            FieldMappingError: If field mapping fails
        """
        try:
            all_candidates = []
            unmatched_terms = []
            
            for term in parsed_query.terms:
                candidates = self.catalog_index.search(term.normalized)
                if candidates:
                    all_candidates.extend(candidates)
                else:
                    unmatched_terms.append(term.original)
            
            field_matches = FieldMatches(
                candidates=all_candidates,
                unmatched_terms=unmatched_terms
            )
            
            logger.debug(f"Step 2 completed: found {len(all_candidates)} candidates, {len(unmatched_terms)} unmatched")
            return field_matches
            
        except Exception as e:
            raise FieldMappingError(f"Failed to find matching fields: {e}")
    
    async def _step3_resolve_conflicts(self, field_matches: FieldMatches, original_query: str) -> ResolvedFields:
        """
        Step 3: Resolve conflicts and ambiguities in field mappings
        
        Args:
            field_matches: Result from step 2
            original_query: Original user query for context
            
        Returns:
            ResolvedFields with definitive field mappings
            
        Raises:
            ConflictResolutionError: If conflict resolution fails
        """
        try:
            if config.ENABLE_LLM_DISAMBIGUATION:
                # Use LLM-based disambiguation
                resolved_fields = await self.conflict_disambiguator.resolve_conflicts(
                    field_matches, original_query
                )
            else:
                # Use simple rule-based resolution
                resolved_fields = self._simple_resolve_conflicts(field_matches)
            
            logger.debug(f"Step 3 completed: resolved {len(resolved_fields.resolved)} fields")
            return resolved_fields
            
        except Exception as e:
            raise ConflictResolutionError(f"Failed to resolve conflicts: {e}")
    
    async def _step4_build_filter(self, resolved_fields: ResolvedFields, parsed_query: ParsedQuery) -> FilterStructure:
        """
        Step 4: Build GraphQL filter structure from resolved fields
        
        Args:
            resolved_fields: Result from step 3
            parsed_query: Original parsed query for logic information
            
        Returns:
            FilterStructure ready for GraphQL generation
            
        Raises:
            FilterBuildingError: If filter building fails
        """
        try:
            filter_structure = self.filter_composer.compose_filter(
                resolved_fields, parsed_query.logic
            )
            
            logger.debug(f"Step 4 completed: built filter with {len(filter_structure.filters)} conditions")
            return filter_structure
            
        except Exception as e:
            raise FilterBuildingError(f"Failed to build filter: {e}")
    
    async def _step5_generate_query(self, filter_structure: FilterStructure) -> GraphQLQuery:
        """
        Step 5: Generate final GraphQL query from filter structure
        
        Args:
            filter_structure: Result from step 4
            
        Returns:
            GraphQLQuery ready for execution
            
        Raises:
            QueryGenerationError: If query generation fails
        """
        try:
            final_query = self.query_builder.build_query(filter_structure)
            
            logger.debug(f"Step 5 completed: generated GraphQL query")
            return final_query
            
        except Exception as e:
            raise QueryGenerationError(f"Failed to generate query: {e}")
    
    def _simple_parse_query(self, query_text: str) -> ParsedQuery:
        """Simple rule-based query parsing (fallback when LLM is disabled)"""
        import re
        from ..core.models import ParsedTerm, LogicOperator
        
        # Simple tokenization
        words = re.findall(r'\b\w+\b', query_text.lower())
        
        # Detect logic operators
        logic = LogicOperator.AND
        if any(word in ["or", "either"] for word in words):
            logic = LogicOperator.OR
        
        # Create terms (skip common stop words)
        stop_words = {"and", "or", "the", "a", "an", "is", "are", "with", "for", "of", "in"}
        terms = []
        position = 0
        
        for word in words:
            if word not in stop_words and len(word) >= config.MIN_TERM_LENGTH:
                terms.append(ParsedTerm(
                    original=word,
                    normalized=word,
                    position=position,
                    confidence=0.8  # Rule-based has lower confidence
                ))
                position += 1
        
        return ParsedQuery(
            terms=terms,
            logic=logic,
            raw_query=query_text,
            confidence=0.8
        )
    
    def _simple_resolve_conflicts(self, field_matches: FieldMatches) -> ResolvedFields:
        """Simple rule-based conflict resolution (fallback when LLM is disabled)"""
        from ..core.models import ResolvedField, FieldType
        
        resolved = []
        conflicts = []
        
        # Group candidates by term
        term_groups = {}
        for candidate in field_matches.candidates:
            if candidate.term not in term_groups:
                term_groups[candidate.term] = []
            term_groups[candidate.term].append(candidate)
        
        # Resolve each term group
        for term, candidates in term_groups.items():
            if len(candidates) == 1:
                # No conflict, use the single candidate
                candidate = candidates[0]
                resolved.append(ResolvedField(
                    term=term,
                    field_path=candidate.field.path,
                    field_type=candidate.field.field_type,
                    value=term,  # Use term as value
                    operator="eq" if candidate.field.field_type == FieldType.ENUMERATION else "contains",
                    confidence=candidate.match_score
                ))
            else:
                # Multiple candidates, pick highest scoring one
                best_candidate = max(candidates, key=lambda c: c.match_score)
                resolved.append(ResolvedField(
                    term=term,
                    field_path=best_candidate.field.path,
                    field_type=best_candidate.field.field_type,
                    value=term,
                    operator="eq" if best_candidate.field.field_type == FieldType.ENUMERATION else "contains",
                    confidence=best_candidate.match_score * 0.9  # Slight penalty for conflicts
                ))
                
                # Record the conflict
                conflicts.append({
                    "term": term,
                    "candidates": [c.field.path for c in candidates],
                    "chosen": best_candidate.field.path,
                    "reason": "Highest score"
                })
        
        return ResolvedFields(
            resolved=resolved,
            conflicts=conflicts,
            warnings=[]
        )


# Global pipeline instance
_pipeline = None

def get_pipeline() -> Pipeline:
    """Get the global pipeline instance"""
    global _pipeline
    if _pipeline is None:
        _pipeline = Pipeline()
    return _pipeline 