import os
import sys
from pathlib import Path

import pytest


_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.cohort_counter import CohortCounter
from services.guppy_client import PCDCGuppyClient
from services.schema_loader import DEFAULT_GITOPS, DEFAULT_PCDC_SCHEMA, SchemaIndex


@pytest.mark.skipif(
    os.environ.get("RUN_PCDC_GUPPY_INTEGRATION") != "1",
    reason="set RUN_PCDC_GUPPY_INTEGRATION=1 to query the public PCDC endpoint",
)
def test_public_pcdc_endpoint_returns_subject_count():
    schema_dir = _BACKEND.parents[1] / "schema"
    schema = SchemaIndex.from_files(
        schema_dir / DEFAULT_PCDC_SCHEMA,
        schema_dir / DEFAULT_GITOPS,
    )
    counter = CohortCounter(schema, PCDCGuppyClient(timeout=30.0))

    result = counter.count({"AND": [{"IN": {"consortium": ["INRG"]}}]})

    assert result.ok, result.errors
    assert isinstance(result.total_count, int)
    assert result.total_count >= 0
