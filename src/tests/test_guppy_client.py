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

from services.guppy_client import GuppyClient, GuppyTransportError


def make_response(total=None, histograms=None, data_type="subject", errors=None, data_present=True):
    node = {}
    if total is not None:
        node["_totalCount"] = total
    for field, buckets in (histograms or {}).items():
        node[field] = {"histogram": buckets}

    payload = {"data": {"_aggregation": {data_type: node}} if data_present else None}
    if errors:
        payload["errors"] = errors
    return payload


class RecordingTransport:
    def __init__(self, response):
        self._response = response
        self.calls = []

    def __call__(self, url, body, headers, timeout):
        self.calls.append({"url": url, "body": body, "headers": headers, "timeout": timeout})
        return self._response


class TestSuccess:
    def test_parses_count_and_histograms(self):
        resp = make_response(total=1234, histograms={"sex": [{"key": "Male", "count": 700}]})
        client = GuppyClient("http://x/graphql", transport=RecordingTransport(resp))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert res.ok
        assert res.total_count == 1234
        assert res.histograms == {"sex": [{"key": "Male", "count": 700}]}
        assert res.errors == []

    def test_auto_detects_single_node_when_no_data_type(self):
        resp = make_response(total=5, data_type="subject")
        client = GuppyClient("http://x", transport=RecordingTransport(resp))
        res = client.execute({"query": "Q"})           # no data_type
        assert res.ok
        assert res.total_count == 5

    def test_zero_count_is_valid(self):
        client = GuppyClient("http://x", transport=RecordingTransport(make_response(total=0)))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert res.ok
        assert res.total_count == 0


class TestMaskedCounts:
    def test_masked_total_count_is_ok_but_none(self):
        client = GuppyClient("http://x", transport=RecordingTransport(make_response(total=-1)))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert res.ok                              # masking is a valid response, not an error
        assert res.total_count is None
        assert res.total_masked is True
        assert res.has_masked is True
        assert res.errors == []

    def test_masked_bucket_normalized(self):
        resp = make_response(
            total=100,
            histograms={"sex": [{"key": "Male", "count": 60}, {"key": "Other", "count": -1}]},
        )
        client = GuppyClient("http://x", transport=RecordingTransport(resp))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert res.ok
        assert res.total_masked is False
        assert res.histograms["sex"][0] == {"key": "Male", "count": 60}
        assert res.histograms["sex"][1] == {"key": "Other", "count": None, "masked": True}
        assert res.has_masked is True

    def test_unmasked_response_reports_no_masking(self):
        resp = make_response(total=5, histograms={"sex": [{"key": "Male", "count": 5}]})
        client = GuppyClient("http://x", transport=RecordingTransport(resp))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert res.total_masked is False
        assert res.has_masked is False


class TestNestedHistograms:
    def test_nested_path_flattened_to_dotted_key(self):
        payload = {
            "data": {
                "_aggregation": {
                    "subject": {
                        "_totalCount": 3,
                        "tumor_assessments": {
                            "tumor_classification": {
                                "histogram": [{"key": "Metastatic", "count": 3}]
                            }
                        },
                    }
                }
            }
        }
        client = GuppyClient("http://x", transport=RecordingTransport(payload))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert res.ok
        assert res.histograms["tumor_assessments.tumor_classification"] == [
            {"key": "Metastatic", "count": 3}
        ]

    def test_masked_nested_bucket_surfaces_through_has_masked(self):
        payload = {
            "data": {
                "_aggregation": {
                    "subject": {
                        "_totalCount": 3,
                        "survival_characteristics": {
                            "lkss": {"histogram": [{"key": "Alive", "count": -1}]}
                        },
                    }
                }
            }
        }
        client = GuppyClient("http://x", transport=RecordingTransport(payload))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert res.has_masked is True
        assert res.histograms["survival_characteristics.lkss"][0]["count"] is None


class TestNumericBuckets:
    def test_numeric_stats_preserved(self):
        bucket = {"key": [0, 100], "count": 10, "min": 0, "max": 100, "avg": 40.5, "sum": 405}
        resp = make_response(total=10, histograms={"age_at_censor_status": [bucket]})
        client = GuppyClient("http://x", transport=RecordingTransport(resp))
        res = client.execute({"query": "Q"}, data_type="subject")
        parsed = res.histograms["age_at_censor_status"][0]
        assert parsed["key"] == [0, 100]
        assert parsed["count"] == 10
        assert (parsed["min"], parsed["max"], parsed["avg"], parsed["sum"]) == (0, 100, 40.5, 405)


class TestResponseErrors:
    def test_partial_data_with_graphql_errors(self):
        resp = make_response(total=10, errors=[{"message": "deprecated field"}])
        client = GuppyClient("http://x", transport=RecordingTransport(resp))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert not res.ok                          # errors present
        assert res.errors == ["deprecated field"]
        assert res.total_count == 10               # data still parsed

    def test_no_data(self):
        resp = {"data": None, "errors": [{"message": "boom"}]}
        client = GuppyClient("http://x", transport=RecordingTransport(resp))
        res = client.execute({"query": "Q"})
        assert not res.ok
        assert res.errors == ["boom"]

    def test_missing_total_count(self):
        client = GuppyClient("http://x", transport=RecordingTransport(make_response(total=None)))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert not res.ok
        assert any("_totalCount" in e for e in res.errors)

    def test_non_integer_total_count(self):
        client = GuppyClient("http://x", transport=RecordingTransport(make_response(total="123")))
        res = client.execute({"query": "Q"}, data_type="subject")
        assert not res.ok
        assert res.total_count is None
        assert any("not an integer" in e for e in res.errors)

    def test_non_dict_response(self):
        client = GuppyClient("http://x", transport=lambda u, b, h, t: ["not", "a", "dict"])
        res = client.execute({"query": "Q"})
        assert not res.ok
        assert any("not a JSON object" in e for e in res.errors)


class TestTransportErrors:
    def test_typed_transport_error(self):
        def boom(url, body, headers, timeout):
            raise GuppyTransportError("timeout", "took too long")
        res = GuppyClient("http://x", transport=boom).execute({"query": "Q"})
        assert not res.ok
        assert res.errors[0].startswith("timeout:")

    def test_generic_transport_exception(self):
        def boom(url, body, headers, timeout):
            raise RuntimeError("socket exploded")
        res = GuppyClient("http://x", transport=boom).execute({"query": "Q"})
        assert any("request failed" in e for e in res.errors)


class TestAuth:
    def test_token_added_to_header(self):
        t = RecordingTransport(make_response(total=1))
        GuppyClient("http://x", token_provider=lambda: "secret-token", transport=t).execute(
            {"query": "Q"}, data_type="subject"
        )
        assert t.calls[0]["headers"]["Authorization"] == "Bearer secret-token"

    def test_no_token_no_auth_header(self):
        t = RecordingTransport(make_response(total=1))
        GuppyClient("http://x", transport=t).execute({"query": "Q"}, data_type="subject")
        assert "Authorization" not in t.calls[0]["headers"]

    def test_token_provider_error_is_caught(self):
        def bad_token():
            raise RuntimeError("vault down")
        t = RecordingTransport(make_response(total=1))
        res = GuppyClient("http://x", token_provider=bad_token, transport=t).execute({"query": "Q"})
        assert any("token_error" in e for e in res.errors)
        assert t.calls == []                        # transport never reached

    def test_refresh_aware_provider_called_positionally(self):
        seen = []

        def provider(force_refresh):
            seen.append(force_refresh)
            return "tok"

        t = RecordingTransport(make_response(total=1))
        GuppyClient("http://x", token_provider=provider, transport=t).execute(
            {"query": "Q"}, data_type="subject"
        )
        assert seen == [False]
        assert t.calls[0]["headers"]["Authorization"] == "Bearer tok"

    def test_auth_error_refreshes_token_and_retries(self):
        class FlakyAuthTransport:
            def __init__(self, response):
                self._response = response
                self.headers_seen = []

            def __call__(self, url, body, headers, timeout):
                self.headers_seen.append(headers.get("Authorization"))
                if len(self.headers_seen) == 1:
                    raise GuppyTransportError("auth_error", "401: authentication failed")
                return self._response

        def provider(force_refresh):
            return "fresh" if force_refresh else "stale"

        t = FlakyAuthTransport(make_response(total=7))
        res = GuppyClient("http://x", token_provider=provider, transport=t).execute(
            {"query": "Q"}, data_type="subject"
        )
        assert res.ok
        assert res.total_count == 7
        assert t.headers_seen == ["Bearer stale", "Bearer fresh"]

    def test_provider_internal_type_error_not_masked(self):
        calls = []

        def provider(force_refresh):
            calls.append(force_refresh)
            raise TypeError("boom inside provider")

        t = RecordingTransport(make_response(total=1))
        res = GuppyClient("http://x", token_provider=provider, transport=t).execute({"query": "Q"})
        assert not res.ok
        assert "token_error" in res.errors[0]
        assert "boom inside provider" in res.errors[0]
        assert calls == [False]                     # invoked exactly once — no zero-arg re-probe
        assert t.calls == []


class TestTransportContract:
    def test_transport_receives_endpoint_and_timeout(self):
        t = RecordingTransport(make_response(total=1))
        GuppyClient("http://commons/guppy/graphql", transport=t, timeout=12.5).execute(
            {"query": "Q"}, data_type="subject"
        )
        call = t.calls[0]
        assert call["url"] == "http://commons/guppy/graphql"
        assert call["timeout"] == 12.5
        assert call["body"] == {"query": "Q"}
