"""
Execute a Guppy aggregation query against a Gen3 GraphQL endpoint.

Takes the payload from graphql_template / QueryBuilder, posts it with an auth
token, and parses the cohort count and histograms out of the response. The
transport and token lookup are injectable so this stays testable offline.

"""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict, List, Optional
from dataclasses import dataclass


TransportFn = Callable[[str, dict, dict, float], dict]
AsyncTransportFn = Callable[[str, dict, dict, float], Awaitable[dict]]
TokenProvider = Callable[..., Optional[str]]

MASKED_COUNT = -1


class GuppyTransportError(Exception):
    def __init__(self, kind: str, message: str):
        super().__init__(message)
        self.kind = kind


@dataclass
class GuppyResult:
    total_count: Optional[int]
    histograms: Dict[str, List[Dict[str, Any]]]
    errors: List[str]
    raw: Optional[dict]
    total_masked: bool = False

    @property
    def ok(self) -> bool:
        return not self.errors and (
            self.total_count is not None or self.total_masked
        )

    @property
    def has_masked(self) -> bool:
        if self.total_masked:
            return True
        return any(
            bucket.get("masked") is True
            for buckets in self.histograms.values()
            for bucket in buckets
        )


class GuppyClient:
    def __init__(
        self,
        endpoint: str,
        *,
        token_provider: Optional[TokenProvider] = None,
        transport: Optional[TransportFn] = None,
        async_transport: Optional[AsyncTransportFn] = None,
        timeout: float = 30.0,
        max_auth_retries: int = 1,
    ):
        self.endpoint = endpoint
        self.timeout = timeout
        self.max_auth_retries = max_auth_retries
        self._token_provider = token_provider
        self._transport = transport
        self._async_transport = async_transport

    def execute(
        self,
        graphql: dict,
        *,
        data_type: Optional[str] = None,
        variables: Optional[dict] = None,
    ) -> GuppyResult:
        graphql = self._with_variables(graphql, variables)
        attempt = 0
        force_refresh = False

        while True:
            try:
                token = self._get_token(force_refresh=force_refresh)
            except Exception as e:
                return GuppyResult(None, {}, [f"token_error: {e}"], None)

            try:
                payload = self._send(graphql, self._headers(token))
            except GuppyTransportError as e:
                if self._should_retry_auth(e, attempt):
                    attempt += 1
                    force_refresh = True
                    continue
                return GuppyResult(None, {}, [f"{e.kind}: {e}"], None)
            except Exception as e:
                return GuppyResult(None, {}, [f"request failed: {e}"], None)

            return self._parse(payload, data_type)

    async def aexecute(
        self,
        graphql: dict,
        *,
        data_type: Optional[str] = None,
        variables: Optional[dict] = None,
    ) -> GuppyResult:
        graphql = self._with_variables(graphql, variables)
        attempt = 0
        force_refresh = False

        while True:
            try:
                token = self._get_token(force_refresh=force_refresh)
            except Exception as e:
                return GuppyResult(None, {}, [f"token_error: {e}"], None)

            try:
                payload = await self._asend(graphql, self._headers(token))
            except GuppyTransportError as e:
                if self._should_retry_auth(e, attempt):
                    attempt += 1
                    force_refresh = True
                    continue
                return GuppyResult(None, {}, [f"{e.kind}: {e}"], None)
            except Exception as e:
                return GuppyResult(None, {}, [f"request failed: {e}"], None)

            return self._parse(payload, data_type)

    def _parse(self, payload: Any, data_type: Optional[str]) -> GuppyResult:
        if not isinstance(payload, dict):
            return GuppyResult(None, {}, ["response was not a JSON object"], None)

        errors = [
            item.get("message", str(item)) if isinstance(item, dict) else str(item)
            for item in (payload.get("errors") or [])
        ]

        data = payload.get("data")
        if data is None:
            return GuppyResult(
                None,
                {},
                errors or ["response had no data"],
                payload,
            )

        node = self._node(data, data_type)

        total = node.get("_totalCount")
        total_masked = False

        if total == MASKED_COUNT:
            total = None
            total_masked = True
        elif total is not None and (
            not isinstance(total, int) or isinstance(total, bool)
        ):
            errors.append(f"_totalCount was not an integer: {total!r}")
            total = None
        elif total is None and not errors:
            errors.append("response had no _totalCount for the requested data type")

        histograms: Dict[str, List[Dict[str, Any]]] = {}
        self._collect_histograms(node, "", histograms)

        return GuppyResult(
            total,
            histograms,
            errors,
            payload,
            total_masked=total_masked,
        )

    @classmethod
    def _collect_histograms(
        cls,
        node: dict,
        prefix: str,
        out: Dict[str, List[Dict[str, Any]]],
    ) -> None:
        for key, value in node.items():
            if key.startswith("_"):
                continue
            if not isinstance(value, dict):
                continue

            if "histogram" in value:
                out[f"{prefix}{key}"] = [
                    cls._parse_bucket(bucket)
                    for bucket in (value.get("histogram") or [])
                ]
            else:
                cls._collect_histograms(value, f"{prefix}{key}.", out)

    @staticmethod
    def _parse_bucket(raw: Any) -> Dict[str, Any]:
        if not isinstance(raw, dict):
            return {"key": raw, "count": None}

        count = raw.get("count")
        masked = count == MASKED_COUNT

        bucket: Dict[str, Any] = {
            "key": raw.get("key"),
            "count": None if masked else count,
        }

        if masked:
            bucket["masked"] = True

        for stat in ("min", "max", "avg", "sum"):
            if stat in raw and raw.get(stat) is not None:
                bucket[stat] = raw.get(stat)

        return bucket

    @staticmethod
    def _node(data: dict, data_type: Optional[str]) -> dict:
        agg = data.get("_aggregation") or {}

        if data_type is not None:
            node = agg.get(data_type)
        elif len(agg) == 1:
            node = next(iter(agg.values()))
        else:
            node = None

        return node if isinstance(node, dict) else {}

    def _send(self, graphql: dict, headers: dict) -> dict:
        if self._transport is not None:
            return self._transport(self.endpoint, graphql, headers, self.timeout)
        return self._httpx_post(graphql, headers)

    async def _asend(self, graphql: dict, headers: dict) -> dict:
        if self._async_transport is not None:
            return await self._async_transport(
                self.endpoint,
                graphql,
                headers,
                self.timeout,
            )

        if self._transport is not None:
            return self._transport(self.endpoint, graphql, headers, self.timeout)

        return await self._ahttpx_post(graphql, headers)

    def _httpx_post(self, graphql: dict, headers: dict) -> dict:
        import httpx

        try:
            response = httpx.post(
                self.endpoint,
                json=graphql,
                headers=headers,
                timeout=self.timeout,
            )
        except httpx.TimeoutException as e:
            raise GuppyTransportError("timeout", str(e)) from e
        except httpx.RequestError as e:
            raise GuppyTransportError("network_error", str(e)) from e

        return self._handle_http_response(response)

    async def _ahttpx_post(self, graphql: dict, headers: dict) -> dict:
        import httpx

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                response = await client.post(
                    self.endpoint,
                    json=graphql,
                    headers=headers,
                )
        except httpx.TimeoutException as e:
            raise GuppyTransportError("timeout", str(e)) from e
        except httpx.RequestError as e:
            raise GuppyTransportError("network_error", str(e)) from e

        return self._handle_http_response(response)

    def _handle_http_response(self, response: Any) -> dict:
        status = response.status_code

        if status in (401, 403):
            raise GuppyTransportError(
                "auth_error",
                f"{status}: authentication failed",
            )

        if status >= 400:
            body = self._safe_json(response)
            if isinstance(body, dict) and body.get("errors"):
                return body

            text = getattr(response, "text", "")
            raise GuppyTransportError("http_error", f"{status}: {text[:200]}")

        body = self._safe_json(response)
        if body is None:
            return {
                "data": None,
                "errors": [{"message": "response was not valid JSON"}],
            }

        return body

    @staticmethod
    def _safe_json(response: Any) -> Any:
        try:
            return response.json()
        except Exception:
            return None

    def _get_token(self, *, force_refresh: bool = False) -> Optional[str]:
        if self._token_provider is None:
            return None

        try:
            return self._token_provider(force_refresh)
        except TypeError:
            return self._token_provider()

    def _should_retry_auth(self, error: GuppyTransportError, attempt: int) -> bool:
        return (
            error.kind == "auth_error"
            and self._token_provider is not None
            and attempt < self.max_auth_retries
        )

    @staticmethod
    def _headers(token: Optional[str]) -> dict:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    @staticmethod
    def _with_variables(graphql: dict, variables: Optional[dict]) -> dict:
        if not variables:
            return graphql

        merged = dict(graphql or {})
        merged["variables"] = {
            **(merged.get("variables") or {}),
            **variables,
        }
        return merged


__all__ = [
    "GuppyClient",
    "GuppyResult",
    "GuppyTransportError",
    "TransportFn",
    "AsyncTransportFn",
    "TokenProvider",
    "MASKED_COUNT",
]
