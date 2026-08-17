"""Validated cohort counts against PCDC Guppy."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from pydantic import ValidationError as PydanticValidationError

from models.filters import GraphQLFilter
from services.filter_validator import CODE_STRUCTURAL, ValidationIssue, validate_filter
from services.graphql_template import build_aggregation_query
from services.schema_loader import SchemaIndex


@dataclass
class CountResult:
    total_count: Optional[int]
    total_masked: bool
    errors: list[str]
    graphql: Optional[dict] = None
    raw: Optional[dict] = None

    @property
    def ok(self) -> bool:
        return not self.errors and (self.total_count is not None or self.total_masked)

    def as_dict(self) -> dict:
        return {
            "ok": self.ok,
            "count": self.total_count,
            "total_masked": self.total_masked,
            "errors": list(self.errors),
        }


class CohortCounter:
    def __init__(
        self,
        schema: SchemaIndex,
        guppy_client,
        *,
        data_type: str = "subject",
        accessibility: Optional[str] = None,
    ):
        self.schema = schema
        self._guppy = guppy_client
        self._data_type = data_type
        self._accessibility = accessibility

    def count(self, filter_obj: GraphQLFilter | Dict[str, Any]) -> CountResult:
        prepared = self._prepare(filter_obj)
        if prepared.errors:
            return prepared
        if prepared.graphql is None:
            return CountResult(None, False, ["could not build aggregation query"])

        result = self._guppy.execute(prepared.graphql, data_type=self._data_type)
        return self._from_guppy(result, prepared.graphql)

    async def acount(self, filter_obj: GraphQLFilter | Dict[str, Any]) -> CountResult:
        prepared = self._prepare(filter_obj)
        if prepared.errors:
            return prepared
        if prepared.graphql is None:
            return CountResult(None, False, ["could not build aggregation query"])

        if hasattr(self._guppy, "aexecute"):
            result = await self._guppy.aexecute(prepared.graphql, data_type=self._data_type)
        else:
            result = self._guppy.execute(prepared.graphql, data_type=self._data_type)
        return self._from_guppy(result, prepared.graphql)

    def _prepare(self, filter_obj: GraphQLFilter | Dict[str, Any]) -> CountResult:
        gf, errors = self._validated_filter(filter_obj)
        if errors:
            return CountResult(None, False, errors)

        try:
            graphql = build_aggregation_query(
                gf,
                data_type=self._data_type,
                accessibility=self._accessibility,
            )
        except (TypeError, ValueError) as e:
            return CountResult(None, False, [f"graphql_template: {e}"])

        return CountResult(None, False, [], graphql=graphql)

    def _validated_filter(
        self,
        filter_obj: GraphQLFilter | Dict[str, Any],
    ) -> tuple[Optional[GraphQLFilter], list[str]]:
        if isinstance(filter_obj, GraphQLFilter):
            gf = filter_obj
        elif isinstance(filter_obj, dict):
            try:
                gf = GraphQLFilter.model_validate(filter_obj)
            except PydanticValidationError as e:
                return None, [_format_issue(ValidationIssue(CODE_STRUCTURAL, str(e)))]
        else:
            return None, [f"[{CODE_STRUCTURAL}] filter must be an object"]

        validation = validate_filter(gf, self.schema)
        if not validation.ok:
            return None, [_format_issue(issue) for issue in validation.issues]
        return gf, []

    @staticmethod
    def _from_guppy(result, graphql: dict) -> CountResult:
        return CountResult(
            total_count=getattr(result, "total_count", None),
            total_masked=bool(getattr(result, "total_masked", False)),
            errors=list(getattr(result, "errors", []) or []),
            graphql=graphql,
            raw=getattr(result, "raw", None),
        )


def _format_issue(issue: ValidationIssue) -> str:
    where = f" at {issue.path or '(top-level)'}"
    return f"[{issue.code}] {issue.message}{where}"


__all__ = ["CohortCounter", "CountResult"]
