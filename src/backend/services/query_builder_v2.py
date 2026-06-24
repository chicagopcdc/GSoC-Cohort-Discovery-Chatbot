from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Sequence, Tuple, Union

from models.filters import GraphQLFilter
from services.filter_generator import FilterGenerator, GenerationResult, GeneratorConfig
from services.graphql_template import build_aggregation_query


@dataclass
class BuildResult:
    """Result of one QueryBuilder request."""

    graphql: Optional[dict]
    filter: Optional[GraphQLFilter]
    wire: Optional[dict]
    data_type: str
    histogram_fields: tuple
    generation: GenerationResult
    errors: tuple = ()
    warnings: tuple = ()

    @property
    def ok(self) -> bool:
        return self.graphql is not None and self.generation.ok


class QueryBuilder:
    """Build Guppy aggregation queries from natural-language cohort requests."""

    def __init__(
        self,
        generator: FilterGenerator,
        *,
        default_data_type: str = "subject",
        default_accessibility: str = "all",
    ):
        self.generator = generator

        # Reuse the schema already loaded by the generator.
        self.schema = generator.schema

        self.default_data_type = default_data_type
        self.default_accessibility = default_accessibility

    @classmethod
    def from_files(
        cls,
        pcdc_path: Union[str, Path],
        gitops_path: Union[str, Path],
        *,
        synonyms_path: Optional[Union[str, Path]] = None,
        generator_config: Optional[GeneratorConfig] = None,
        embed_fn=None,
        client=None,
        cache_dir: Optional[Union[str, Path]] = None,
        default_data_type: str = "subject",
        default_accessibility: str = "all",
    ) -> "QueryBuilder":
        """Create a QueryBuilder and load its dependencies from schema files."""
        generator = FilterGenerator.from_files(
            pcdc_path,
            gitops_path,
            synonyms_path=synonyms_path,
            config=generator_config,
            embed_fn=embed_fn,
            client=client,
            cache_dir=cache_dir,
        )
        return cls(
            generator,
            default_data_type=default_data_type,
            default_accessibility=default_accessibility,
        )

    def build(
        self,
        query: str,
        *,
        current_filter: Optional[dict] = None,
        data_type: Optional[str] = None,
        accessibility: Optional[str] = None,
        histogram_fields: Optional[Sequence[str]] = None,
    ) -> BuildResult:
        """Generate a validated filter, then render it as a Guppy query.

        Histogram fields are checked only for ``subject`` queries. Other data
        types are passed through and left for Guppy to validate.
        """
        data_type = data_type or self.default_data_type
        accessibility = accessibility or self.default_accessibility

        # Natural language -> structured, schema-validated filter.
        gen = self.generator.generate(query, current_filter=current_filter)

        valid_histograms, bad_histograms = self._check_histograms(
            histogram_fields,
            data_type,
        )
        warnings = self._warnings(bad_histograms, data_type, gen)

        # Never render GraphQL from an invalid filter.
        if not gen.ok:
            errors = tuple(
                f"[{issue.code}] {issue.message}"
                for issue in gen.validation.issues
            )
            return BuildResult(
                graphql=None,
                filter=None,
                wire=None,
                data_type=data_type,
                histogram_fields=tuple(valid_histograms),
                generation=gen,
                errors=errors,
                warnings=warnings,
            )

        try:
            # Rendering is deterministic; no LLM call happens here.
            graphql = build_aggregation_query(
                gen.filter,
                data_type=data_type,
                accessibility=accessibility,
                histogram_fields=valid_histograms or None,
            )
        except ValueError as e:
            return BuildResult(
                graphql=None,
                filter=gen.filter,
                wire=gen.wire,
                data_type=data_type,
                histogram_fields=tuple(valid_histograms),
                generation=gen,
                errors=(f"graphql_template: {e}",),
                warnings=warnings,
            )

        return BuildResult(
            graphql=graphql,
            filter=gen.filter,
            wire=gen.wire,
            data_type=data_type,
            histogram_fields=tuple(valid_histograms),
            generation=gen,
            warnings=warnings,
        )

    def _check_histograms(
        self,
        histogram_fields: Optional[Sequence[str]],
        data_type: str,
    ) -> Tuple[List[str], List[str]]:
        """Return valid and invalid histogram field names."""
        if not histogram_fields:
            return [], []

        # Keep request order, but remove duplicates.
        seen = set()
        unique = []
        for name in histogram_fields:
            if name not in seen:
                seen.add(name)
                unique.append(name)

        # Only subject fields have local validation rules for now.
        if data_type != "subject":
            return unique, []

        top_level = {spec.name for spec in self.schema.top_level_fields()}

        valid, bad = [], []
        for name in unique:
            (valid if name in top_level else bad).append(name)

        return valid, bad

    @staticmethod
    def _warnings(
        bad_histograms: Sequence[str],
        data_type: str,
        gen: GenerationResult,
    ) -> tuple:
        """Build non-fatal warnings for the caller."""
        out = [
            f"ignored histogram field {name!r}: not a top-level {data_type} field"
            for name in bad_histograms
        ]

        # Keep the request usable, but report ranges with unsupported units.
        for r in gen.dropped_ranges:
            unit = f" {r.unit}" if r.unit else ""
            out.append(
                f"dropped range {r.quantity or 'value'} {r.op} {r.value}{unit}: "
                "unit not yet converted to schema units"
            )

        return tuple(out)
