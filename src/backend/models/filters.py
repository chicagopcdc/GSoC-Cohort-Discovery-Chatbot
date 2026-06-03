"""
Models for Guppy filter objects used by the query-builder pipeline.

The goal here is only to validate the filter shape before it is passed to
the validator, renderer, evaluation code, or session state.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Union, Any

from pydantic import BaseModel, ConfigDict, RootModel, field_validator, model_validator


# Values that can appear in operator payloads.
Scalar = Union[str, int, float, bool]


class _SingleFieldClause(BaseModel):
    """
    Internal base: every atomic operator clause has exactly one field
    inside its operator dict, with non-empty field name.
    """

    model_config = ConfigDict(extra="forbid")

    def _validate_field_dict(self, op_name: str, d: Dict) -> None:
        if len(d) != 1:
            raise ValueError(
                f"{op_name} clause must contain exactly one field; got {list(d)}"
            )

        field = next(iter(d))

        if not isinstance(field, str) or not field.strip():
            raise ValueError(f"{op_name} field name must be non-empty")


class InClause(_SingleFieldClause):
    """{"IN": {field: [v1, v2, ...]}}"""

    IN: Dict[str, List[Scalar]]

    @model_validator(mode="after")
    def _check(self) -> "InClause":
        self._validate_field_dict("IN", self.IN)

        for field, values in self.IN.items():
            if not values:
                raise ValueError(f"IN values for {field!r} must be non-empty")

        return self


class GTEClause(_SingleFieldClause):
    """{"GTE": {field: value}}"""

    GTE: Dict[str, Union[int, float]]

    @model_validator(mode="after")
    def _check(self) -> "GTEClause":
        self._validate_field_dict("GTE", self.GTE)
        return self


class LTEClause(_SingleFieldClause):
    """{"LTE": {field: value}}"""

    LTE: Dict[str, Union[int, float]]

    @model_validator(mode="after")
    def _check(self) -> "LTEClause":
        self._validate_field_dict("LTE", self.LTE)
        return self


class GTClause(_SingleFieldClause):
    """{"GT": {field: value}}"""

    GT: Dict[str, Union[int, float]]

    @model_validator(mode="after")
    def _check(self) -> "GTClause":
        self._validate_field_dict("GT", self.GT)
        return self


class LTClause(_SingleFieldClause):
    """{"LT": {field: value}}"""

    LT: Dict[str, Union[int, float]]

    @model_validator(mode="after")
    def _check(self) -> "LTClause":
        self._validate_field_dict("LT", self.LT)
        return self


# Recursive clause type. Forward references are rebuilt at the bottom.
FilterClause = Union[
    "InClause",
    "GTEClause",
    "LTEClause",
    "GTClause",
    "LTClause",
    "AndClause",
    "OrClause",
    "NestedClause",
]


class AndClause(BaseModel):
    """{"AND": [clause, ...]}"""

    model_config = ConfigDict(extra="forbid")

    AND: List[FilterClause]

    @model_validator(mode="after")
    def non_empty(self) -> "AndClause":
        if not self.AND:
            raise ValueError("AND list must be non-empty")

        return self


class OrClause(BaseModel):
    """{"OR": [clause, ...]}"""

    model_config = ConfigDict(extra="forbid")

    OR: List[FilterClause]

    @model_validator(mode="after")
    def non_empty(self) -> "OrClause":
        if not self.OR:
            raise ValueError("OR list must be non-empty")

        return self


class NestedBody(BaseModel):
    """Body of a nested clause."""

    model_config = ConfigDict(extra="forbid")

    path: str
    AND: Optional[List[FilterClause]] = None
    OR: Optional[List[FilterClause]] = None

    @field_validator("path")
    @classmethod
    def non_blank_path(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("nested path must be non-empty")

        return value

    @model_validator(mode="after")
    def exactly_one_non_empty_logical_op(self) -> "NestedBody":
        has_and = self.AND is not None
        has_or = self.OR is not None

        if has_and == has_or:
            raise ValueError(
                "nested body must contain exactly one of AND or OR "
                f"(got AND={has_and}, OR={has_or})"
            )

        active = self.AND if has_and else self.OR

        if not active:
            raise ValueError(
                f"nested body's {'AND' if has_and else 'OR'} list must be non-empty"
            )

        return self


class NestedClause(BaseModel):
    """{"nested": {"path": <table>, "AND" | "OR": [clause, ...]}}"""

    model_config = ConfigDict(extra="forbid")

    nested: NestedBody


class GraphQLFilter(RootModel[FilterClause]):
    """Top-level filter wrapper."""
    def model_dump(self, *args: Any, **kwargs: Any) -> Any:
        kwargs.setdefault("exclude_none", True)
        return super().model_dump(*args, **kwargs)


# Needed because FilterClause contains forward references.
AndClause.model_rebuild()
OrClause.model_rebuild()
NestedBody.model_rebuild()
GraphQLFilter.model_rebuild()


__all__ = [
    "Scalar",
    "InClause",
    "GTEClause",
    "LTEClause",
    "GTClause",
    "LTClause",
    "AndClause",
    "OrClause",
    "NestedBody",
    "NestedClause",
    "FilterClause",
    "GraphQLFilter",
]
