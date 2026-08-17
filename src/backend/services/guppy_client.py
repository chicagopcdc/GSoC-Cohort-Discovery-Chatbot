"""Execute Guppy aggregation queries against a Gen3 GraphQL endpoint."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from models.filters import GraphQLFilter
from services.graphql_template import build_aggregation_query


TransportFn = Callable[[str, dict, dict, float], dict]
AsyncTransportFn = Callable[[str, dict, dict, float], Awaitable[dict]]
TokenProvider = Callable[..., Optional[str]]

MASKED_COUNT = -1
DEFAULT_PCDC_GUPPY_ENDPOINT = "https://portal.pedscommons.org/guppy/graphql/"


def _accepts_positional_arg(provider: Callable) -> bool:
    """
    True if the token provider can take one positional argument (the
    force_refresh flag). Detected once via introspection instead of probing
    with try/except TypeError, which would mask TypeErrors raised inside the
    provider and could invoke a side-effectful provider twice. Falls back to
    zero-arg calling when the signature cannot be introspected.
    """
    try:
        sig = inspect.signature(provider)
    except (TypeError, ValueError):
        return False

    for param in sig.parameters.values():
        if param.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
            inspect.Parameter.VAR_POSITIONAL,
        ):
            return True

    return False


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
        self._provider_accepts_refresh = (
            token_provider is not None and _accepts_positional_arg(token_provider)
        )
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
            headers, failure = self._headers_or_error(force_refresh)
            if failure is not None:
                return failure

            try:
                payload = self._send(graphql, headers)
            except Exception as e:
                retry, failure = self._transport_failure(e, attempt)
                if retry:
                    attempt += 1
                    force_refresh = True
                    continue
                return failure

            return self._parse(payload, data_type)

    def count_subjects(
        self,
        filter_obj: GraphQLFilter | Dict[str, Any],
        *,
        accessibility: Optional[str] = None,
    ) -> GuppyResult:
        graphql = build_aggregation_query(
            filter_obj,
            data_type="subject",
            accessibility=accessibility,
        )
        return self.execute(graphql, data_type="subject")

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
            headers, failure = self._headers_or_error(force_refresh)
            if failure is not None:
                return failure

            try:
                payload = await self._asend(graphql, headers)
            except Exception as e:
                retry, failure = self._transport_failure(e, attempt)
                if retry:
                    attempt += 1
                    force_refresh = True
                    continue
                return failure

            return self._parse(payload, data_type)

    async def acount_subjects(
        self,
        filter_obj: GraphQLFilter | Dict[str, Any],
        *,
        accessibility: Optional[str] = None,
    ) -> GuppyResult:
        graphql = build_aggregation_query(
            filter_obj,
            data_type="subject",
            accessibility=accessibility,
        )
        return await self.aexecute(graphql, data_type="subject")

    def _headers_or_error(
        self,
        force_refresh: bool,
    ) -> Tuple[Optional[dict], Optional[GuppyResult]]:
        """Fetch a token and build headers, or map the failure to a result."""
        try:
            token = self._get_token(force_refresh=force_refresh)
        except Exception as e:
            return None, self._error_result(f"token_error: {e}")

        return self._headers(token), None

    def _transport_failure(
        self,
        error: Exception,
        attempt: int,
    ) -> Tuple[bool, Optional[GuppyResult]]:
        """
        Map a transport exception to (retry, result).

        Shared by execute and aexecute so the retry policy and error-to-result
        mapping cannot drift between the sync and async paths.
        """
        if isinstance(error, GuppyTransportError):
            if self._should_retry_auth(error, attempt):
                return True, None
            return False, self._error_result(f"{error.kind}: {error}")

        return False, self._error_result(f"request failed: {error}")

    @staticmethod
    def _error_result(message: str) -> GuppyResult:
        return GuppyResult(None, {}, [message], None)

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
        except (TypeError, ValueError):
            return None

    def _get_token(self, *, force_refresh: bool = False) -> Optional[str]:
        if self._token_provider is None:
            return None

        # The calling convention was detected at construction time, so real
        # TypeErrors from inside the provider propagate to the caller and the
        # provider is never invoked twice for one token fetch.
        if self._provider_accepts_refresh:
            return self._token_provider(force_refresh)

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


class PCDCGuppyClient(GuppyClient):
    def __init__(
        self,
        endpoint: str = DEFAULT_PCDC_GUPPY_ENDPOINT,
        *,
        token_provider: Optional[TokenProvider] = None,
        transport: Optional[TransportFn] = None,
        async_transport: Optional[AsyncTransportFn] = None,
        timeout: float = 30.0,
        max_auth_retries: int = 1,
    ):
        super().__init__(
            endpoint,
            token_provider=token_provider,
            transport=transport,
            async_transport=async_transport,
            timeout=timeout,
            max_auth_retries=max_auth_retries,
        )


__all__ = [
    "GuppyClient",
    "PCDCGuppyClient",
    "GuppyResult",
    "GuppyTransportError",
    "DEFAULT_PCDC_GUPPY_ENDPOINT",
    "TransportFn",
    "AsyncTransportFn",
    "TokenProvider",
    "MASKED_COUNT",
]
