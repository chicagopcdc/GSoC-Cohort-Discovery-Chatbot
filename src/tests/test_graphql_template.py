import sys
from pathlib import Path


def _find_upwards(relative: str) -> Path:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / relative
        if candidate.exists():
            return candidate
    raise FileNotFoundError(f"could not find {relative} above {here}")


_SERVICES = _find_upwards("backend/services")
if str(_SERVICES.parent) not in sys.path:
    sys.path.insert(0, str(_SERVICES.parent))

import pytest

from models.filters import GraphQLFilter
from services.graphql_template import build_aggregation_query


_FILTER = {"AND": [{"IN": {"consortium": ["INRG"]}}]}


class TestRendering:
    def test_basic_total_count(self):
        out = build_aggregation_query(_FILTER)
        assert out["query"] == (
            "query ($filter: JSON) { _aggregation { "
            "subject(accessibility: all, filter: $filter) { _totalCount } } }"
        )
        assert out["variables"] == {"filter": _FILTER}

    def test_braces_balanced(self):
        q = build_aggregation_query(_FILTER)["query"]
        assert q.count("{") == q.count("}")

    def test_histogram_fields(self):
        q = build_aggregation_query(_FILTER, histogram_fields=["sex", "race"])["query"]
        assert "sex { histogram { key count } }" in q
        assert "race { histogram { key count } }" in q

    def test_accepts_graphqlfilter_object(self):
        gf = GraphQLFilter.model_validate(_FILTER)
        out = build_aggregation_query(gf)
        assert out["variables"]["filter"] == _FILTER

    def test_filter_dump_excludes_none(self):
        # nested body with only AND set must not serialize "OR": null
        obj = {"AND": [{"nested": {"path": "tumor_assessments",
                                   "AND": [{"IN": {"tumor_site": ["Skin"]}}]}}]}
        out = build_aggregation_query(obj)
        assert "OR" not in out["variables"]["filter"]["AND"][0]["nested"]


class TestGuards:
    def test_data_type_injection_rejected(self):
        with pytest.raises(ValueError):
            build_aggregation_query(_FILTER, data_type="subject) { evil }")

    def test_accessibility_whitelist(self):
        with pytest.raises(ValueError):
            build_aggregation_query(_FILTER, accessibility="all) { evil }")

    def test_accessibility_valid_value(self):
        out = build_aggregation_query(_FILTER, accessibility="accessible")
        assert "accessibility: accessible" in out["query"]

    def test_histogram_field_injection_rejected(self):
        with pytest.raises(ValueError):
            build_aggregation_query(_FILTER, histogram_fields=["sex } } evil {"])

    def test_histogram_dotted_field_rejected(self):
        # nested-style dotted name isn't a valid top-level histogram field
        with pytest.raises(ValueError):
            build_aggregation_query(_FILTER, histogram_fields=["tumor_assessments.tumor_site"])

    def test_malformed_dict_rejected(self):
        with pytest.raises(Exception):
            build_aggregation_query({"BOGUS": [{"IN": {"x": ["y"]}}]})

    def test_empty_and_rejected(self):
        with pytest.raises(Exception):
            build_aggregation_query({"AND": []})