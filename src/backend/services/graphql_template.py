"""
Build Guppy aggregation GraphQL queries.

This module only renders the query payload. It assumes schema-level checks
have already happened elsewhere.
"""
from __future__ import annotations
import re
from typing import Any, Dict, Iterable, List, Optional, Union
from models.filters import GraphQLFilter

__all__ = ["build_aggregation_query"]

# Valid GraphQL names only; this also keeps nested paths out.
_GQL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# Values accepted by Guppy's accessibility argument.
_ACCESSIBILITY = ("all", "accessible", "unaccessible")

def _check_name(name: str, what: str) -> str:
    """Check that a string is safe to use as a GraphQL name"""
    if not isinstance(name, str) or not _GQL_NAME.match(name):
        raise ValueError(
            f"{what} must be a valid GraphQL name matching {_GQL_NAME.pattern!r}, "
            f"got {name!r}"
        )
    return name


def _filter_dict(filter_obj: Union[GraphQLFilter, Dict[str, Any]]) -> Dict[str, Any]:
    """Return a validated filter dict"""
    if isinstance(filter_obj, GraphQLFilter):
        return filter_obj.model_dump()
    if isinstance(filter_obj, dict):
        return GraphQLFilter.model_validate(filter_obj).model_dump()
    raise TypeError(
        f"filter_obj must be a GraphQLFilter or dict, got {type(filter_obj).__name__}"
    )


def build_aggregation_query(
    filter_obj: Union[GraphQLFilter, Dict[str, Any]],
    *,
    data_type: str = "subject",
    accessibility: str = "all",
    histogram_fields: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    
    """Return a Guppy aggregation query payload"""

    _check_name(data_type, "data_type")

    if accessibility not in _ACCESSIBILITY:
        raise ValueError(
            f"accessibility must be one of {_ACCESSIBILITY}, got {accessibility!r}"
        )

    filter_payload = _filter_dict(filter_obj)

    # Always include the count; add histograms when requested
    selection_parts: List[str] = ["_totalCount"]
    if histogram_fields is not None:
        if isinstance(histogram_fields, (str, bytes)):
            raise TypeError("histogram_fields must be an iterable of field names, not a string")
        for field in histogram_fields:
            _check_name(field, "histogram field")
            selection_parts.append(f"{field} {{ histogram {{ key count }} }}")

    selection = " ".join(selection_parts)
    # Build this in pieces because GraphQL braces get messy quickly
    inner = "{ " + selection + " }"
    node = f"{data_type}(accessibility: {accessibility}, filter: $filter) " + inner
    aggregation = "_aggregation { " + node + " }"
    query = "query ($filter: JSON) { " + aggregation + " }"

    return {"query": query, "variables": {"filter": filter_payload}}
