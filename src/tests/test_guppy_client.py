import sys
from pathlib import Path

_BACKEND = Path(__file__).resolve().parents[1] / "backend"
if str(_BACKEND) not in sys.path:
    sys.path.insert(0, str(_BACKEND))

from services.guppy_client import GuppyClient, GuppyResult, GuppyTransportError


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
