"""
Unit tests for models.filters.

Run from repository root:
    pytest src/tests/test_filters.py -v
"""

import pytest
from pydantic import ValidationError

from src.backend.models.filters import GraphQLFilter, InClause, NestedClause

class TestRoundTripBasic:
    """Baseline filter shapes that should round-trip unchanged."""

    def _rt(self, obj):
        return GraphQLFilter.model_validate(obj).model_dump()

    def test_a1_top_level_and_of_in(self):
        obj = {
            "AND": [
                {"IN": {"consortium": ["INRG"]}},
                {"IN": {"sex": ["Male"]}},
            ]
        }
        assert self._rt(obj) == obj

    def test_a2_consortium_plus_nested(self):
        obj = {
            "AND": [
                {"IN": {"consortium": ["INRG"]}},
                {
                    "nested": {
                        "path": "tumor_assessments",
                        "AND": [
                            {"IN": {"tumor_classification": ["Metastatic"]}},
                            {"IN": {"tumor_site": ["Skin"]}},
                        ],
                    }
                },
            ]
        }
        assert self._rt(obj) == obj

    def test_b2_simple_gte(self):
        obj = {"AND": [{"GTE": {"age_at_censor_status": 5}}]}
        assert self._rt(obj) == obj

    def test_b3_and_with_gte_lte_and_in(self):
        obj = {
            "AND": [
                {"IN": {"consortium": ["INRG"]}},
                {"LTE": {"age_at_censor_status": 5}},
                {"IN": {"race": ["Not Reported"]}},
            ]
        }
        assert self._rt(obj) == obj

    def test_d1_top_level_or_of_and(self):
        obj = {
            "OR": [
                {"AND": [{"IN": {"consortium": ["INRG"]}}]},
                {"AND": [{"IN": {"consortium": ["NODAL"]}}]},
            ]
        }
        assert self._rt(obj) == obj

    def test_nested_age_range_under_tumor_assessments(self):
        obj = {
            "AND": [
                {"IN": {"consortium": ["INRG"]}},
                {
                    "nested": {
                        "path": "tumor_assessments",
                        "AND": [
                            {
                                "AND": [
                                    {"GTE": {"age_at_tumor_assessment": 547}},
                                    {"LTE": {"age_at_tumor_assessment": 10983}},
                                ]
                            }
                        ],
                    }
                },
            ]
        }
        assert self._rt(obj) == obj


class TestRoundTripExtended:
    """Extra supported shapes not covered by the baseline cases."""

    def _rt(self, obj):
        return GraphQLFilter.model_validate(obj).model_dump()

    def test_strict_gt_clause(self):
        obj = {"AND": [{"GT": {"age_at_censor_status": 5}}]}
        assert self._rt(obj) == obj

    def test_strict_lt_clause(self):
        obj = {"AND": [{"LT": {"age_at_censor_status": 18}}]}
        assert self._rt(obj) == obj

    def test_in_with_multiple_values(self):
        obj = {
            "AND": [
                {
                    "IN": {
                        "race": ["Asian", "White", "Black or African American"]
                    }
                }
            ]
        }
        assert self._rt(obj) == obj

    def test_in_with_integer_values(self):
        obj = {"AND": [{"IN": {"year_at_disease_phase": [2010, 2011, 2012]}}]}
        assert self._rt(obj) == obj

    def test_nested_with_or_inside(self):
        obj = {
            "AND": [
                {
                    "nested": {
                        "path": "tumor_assessments",
                        "OR": [
                            {"IN": {"tumor_site": ["Skin"]}},
                            {"IN": {"tumor_site": ["Brain"]}},
                        ],
                    }
                }
            ]
        }
        assert self._rt(obj) == obj

    def test_deeply_nested_and_of_and(self):
        obj = {"AND": [{"AND": [{"AND": [{"IN": {"sex": ["Male"]}}]}]}]}
        assert self._rt(obj) == obj

    def test_multiple_nested_paths_in_same_filter(self):
        obj = {
            "AND": [
                {"IN": {"consortium": ["INRG"]}},
                {
                    "nested": {
                        "path": "tumor_assessments",
                        "AND": [{"IN": {"tumor_site": ["Brain"]}}],
                    }
                },
                {
                    "nested": {
                        "path": "histologies",
                        "AND": [
                            {
                                "IN": {
                                    "histology": [
                                        "Neuroblastoma (Schwannian Stroma-Poor)"
                                    ]
                                }
                            }
                        ],
                    }
                },
            ]
        }
        assert self._rt(obj) == obj


class TestValidation:
    """Malformed filters should fail validation."""

    def test_unknown_operator_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"BOGUS": {"field": ["x"]}}]})

    def test_empty_and_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": []})

    def test_empty_or_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"OR": []})

    def test_empty_in_values_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"IN": {"sex": []}}]})

    def test_in_with_two_fields_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {"AND": [{"IN": {"sex": ["Male"], "race": ["Asian"]}}]}
            )

    def test_gte_with_two_fields_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {"AND": [{"GTE": {"age": 5, "year": 2010}}]}
            )

    def test_nested_without_logical_op_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {"AND": [{"nested": {"path": "tumor_assessments"}}]}
            )

    def test_nested_with_both_and_and_or_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {
                    "AND": [
                        {
                            "nested": {
                                "path": "x",
                                "AND": [{"IN": {"f": ["v"]}}],
                                "OR": [{"IN": {"f": ["v"]}}],
                            }
                        }
                    ]
                }
            )

    def test_extra_field_in_in_clause_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {"AND": [{"IN": {"sex": ["Male"]}, "extra": "junk"}]}
            )

    def test_extra_field_in_nested_body_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {
                    "AND": [
                        {
                            "nested": {
                                "path": "x",
                                "AND": [{"IN": {"f": ["v"]}}],
                                "comment": "this should not be allowed",
                            }
                        }
                    ]
                }
            )


class TestStructuralEdgeCases:
    """Small malformed cases that should not pass quietly."""

    def test_empty_nested_and_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {"AND": [{"nested": {"path": "tumor_assessments", "AND": []}}]}
            )

    def test_empty_nested_or_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {"AND": [{"nested": {"path": "tumor_assessments", "OR": []}}]}
            )

    def test_empty_path_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {
                    "AND": [
                        {
                            "nested": {
                                "path": "",
                                "AND": [{"IN": {"x": ["v"]}}],
                            }
                        }
                    ]
                }
            )

    def test_empty_in_field_name_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"IN": {"": ["v"]}}]})

    def test_whitespace_only_in_field_name_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"IN": {"   ": ["v"]}}]})

    def test_empty_gte_field_name_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"GTE": {"": 5}}]})

    def test_empty_lte_field_name_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"LTE": {"": 5}}]})

    def test_empty_gt_field_name_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"GT": {"": 5}}]})

    def test_empty_lt_field_name_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate({"AND": [{"LT": {"": 5}}]})


class TestPathSemantics:
    """Behavior around nested.path values."""

    def test_whitespace_only_path_rejected(self):
        with pytest.raises(ValidationError):
            GraphQLFilter.model_validate(
                {
                    "AND": [
                        {
                            "nested": {
                                "path": "   ",
                                "AND": [{"IN": {"x": ["v"]}}],
                            }
                        }
                    ]
                }
            )

    def test_path_with_surrounding_whitespace_preserved(self):
        obj = {
            "AND": [
                {
                    "nested": {
                        "path": "  tumor_assessments  ",
                        "AND": [{"IN": {"x": ["v"]}}],
                    }
                }
            ]
        }

        dumped = GraphQLFilter.model_validate(obj).model_dump()

        assert dumped["AND"][0]["nested"]["path"] == "  tumor_assessments  "

    def test_valid_path_accepted(self):
        obj = {
            "AND": [
                {
                    "nested": {
                        "path": "tumor_assessments",
                        "AND": [{"IN": {"x": ["v"]}}],
                    }
                }
            ]
        }

        GraphQLFilter.model_validate(obj)


class TestPublicAPI:
    """Direct construction used by downstream code."""

    def test_can_construct_in_clause_directly(self):
        c = InClause(IN={"sex": ["Male"]})

        assert c.model_dump() == {"IN": {"sex": ["Male"]}}

    def test_can_construct_nested_clause_directly(self):
        n = NestedClause.model_validate(
            {
                "nested": {
                    "path": "tumor_assessments",
                    "AND": [{"IN": {"tumor_site": ["Skin"]}}],
                }
            }
        )

        assert n.nested.path == "tumor_assessments"
        assert n.nested.AND is not None
        assert n.nested.OR is None

    def test_graphqlfilter_model_dump_excludes_none(self):
        obj = {
            "AND": [
                {
                    "nested": {
                        "path": "tumor_assessments",
                        "AND": [{"IN": {"x": ["v"]}}],
                    }
                }
            ]
        }

        dumped = GraphQLFilter.model_validate(obj).model_dump()

        assert "OR" not in dumped["AND"][0]["nested"]
