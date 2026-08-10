import sys
from pathlib import Path
from types import SimpleNamespace

import pytest


_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from models.filters import GraphQLFilter
from services.cohort_counter import CohortCounter
from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA, SchemaIndex


_SCHEMA_DIR = _BACKEND.parents[1] / "schema"


def _schema():
    return SchemaIndex.from_files(
        _SCHEMA_DIR / DEFAULT_PCDC_SCHEMA,
        _SCHEMA_DIR / DEFAULT_GITOPS,
    )


def _result(total=12, *, ok=True, errors=()):
    return SimpleNamespace(
        ok=ok,
        total_count=total,
        total_masked=False,
        errors=list(errors),
        raw={"data": {"_aggregation": {"subject": {"_totalCount": total}}}},
    )


class FakeGuppy:
    def __init__(self, result):
        self._result = result
        self.calls = []

    def execute(self, graphql, *, data_type=None):
        self.calls.append({"graphql": graphql, "data_type": data_type})
        return self._result

    async def aexecute(self, graphql, *, data_type=None):
        self.calls.append({"graphql": graphql, "data_type": data_type, "async": True})
        return self._result


class TestCohortCounter:
    def test_valid_filter_is_counted_after_validation(self):
        guppy = FakeGuppy(_result(total=99))
        counter = CohortCounter(_schema(), guppy)

        res = counter.count({"AND": [{"IN": {"consortium": ["INRG"]}}]})

        assert res.ok
        assert res.total_count == 99
        assert guppy.calls[0]["data_type"] == "subject"
        assert guppy.calls[0]["graphql"]["query"] == (
            "query ($filter: JSON) { _aggregation { "
            "subject(filter: $filter) { _totalCount } } }"
        )
        assert guppy.calls[0]["graphql"]["variables"]["filter"] == {
            "AND": [{"IN": {"consortium": ["INRG"]}}],
        }

    def test_invalid_filter_does_not_reach_guppy(self):
        guppy = FakeGuppy(_result(total=99))
        counter = CohortCounter(_schema(), guppy)

        res = counter.count({"AND": [{"IN": {"not_a_real_field": ["x"]}}]})

        assert not res.ok
        assert any("unknown_field" in e for e in res.errors)
        assert guppy.calls == []

    def test_graphqlfilter_model_is_accepted(self):
        guppy = FakeGuppy(_result(total=5))
        counter = CohortCounter(_schema(), guppy)
        gf = GraphQLFilter.model_validate({"AND": [{"IN": {"sex": ["Male"]}}]})

        res = counter.count(gf)

        assert res.ok
        assert res.total_count == 5

    @pytest.mark.asyncio
    async def test_async_count_uses_async_client_path(self):
        guppy = FakeGuppy(_result(total=8))
        counter = CohortCounter(_schema(), guppy)

        res = await counter.acount({"AND": [{"IN": {"consortium": ["NODAL"]}}]})

        assert res.ok
        assert res.total_count == 8
        assert guppy.calls[0]["async"] is True

    def test_guppy_errors_are_returned(self):
        guppy = FakeGuppy(_result(total=None, ok=False, errors=("response had no _totalCount",)))
        counter = CohortCounter(_schema(), guppy)

        res = counter.count({"AND": [{"IN": {"consortium": ["INRG"]}}]})

        assert not res.ok
        assert res.errors == ["response had no _totalCount"]
