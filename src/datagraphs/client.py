"""DataGraphs API client for interacting with the DataGraphs service."""

import functools
import json
import logging
import math
import random
import time
import urllib.parse
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum, auto
from typing import TYPE_CHECKING, Any

import requests

from datagraphs.dataset import Dataset
from datagraphs.enums import HTTP, ISOLATION_MODE, SCHEMA_APPLY_MODE
from datagraphs.schema import Schema as DatagraphsSchema

if TYPE_CHECKING:
    from datagraphs.gateway import DumpResult, LoadResult

_logger = logging.getLogger(__name__)

class DatagraphsError(Exception):
    """Base exception for DataGraphs client errors."""

class AuthenticationError(DatagraphsError):
    """Raised when authentication or authorisation fails.

    :ivar result: When this stopped a `Client.put`, `Gateway.load_data` or
        `Gateway.dump_data` part-way, the outcome of what it did before
        stopping (a `PutResult`, `LoadResult` or `DumpResult`), accounting for
        what it did not do; otherwise ``None``.
    """

    result: 'PutResult | LoadResult | DumpResult | None' = None

class HttpStatusError(DatagraphsError):
    """Raised when the API responds with a non-success HTTP status.

    :ivar status_code: The HTTP status code of the response.
    :ivar errors: The error details reported by the API (or the raw response
        text when the body carries no structured errors).
    :ivar retry_after_s: The delay in seconds the API asked for before a
        retry (its ``Retry-After`` header), or ``None`` if it gave none.
    """

    def __init__(self, status_code: int, errors: list[Any], message: str, retry_after_s: float | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors
        self.retry_after_s = retry_after_s

class TokenRequestError(DatagraphsError):
    """Raised when an auth token cannot be obtained for a reason other than rejected credentials.

    The token endpoint failed or could not be reached, so the request that
    needed the token was never sent.

    :ivar status_code: The token endpoint's HTTP status, or ``None`` when no
        response was received (the underlying ``requests`` exception, e.g. a
        ``ReadTimeout``, is then the ``__cause__``).
    :ivar errors: The error details for the failure.
    :ivar result: When this stopped a `Client.put`, `Gateway.load_data` or
        `Gateway.dump_data` part-way, the outcome of what it did before
        stopping (a `PutResult`, `LoadResult` or `DumpResult`), accounting for
        what it did not do; otherwise ``None``.
    """

    result: 'PutResult | LoadResult | DumpResult | None' = None

    def __init__(self, status_code: int | None, errors: list[Any], message: str) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors

class IncompleteReadError(DatagraphsError):
    """Raised when a paged read does not return exactly the results the API reported.

    Every page request succeeded, but the pages did not add up to the
    ``totalResults`` the first page reported: the pages ran out early, held
    more results than reported, or repeated a result. This usually means the
    data changed while it was being read, so reading it again is the remedy.

    :ivar expected: The ``totalResults`` reported by the first page.
    :ivar received: The number of results received before the read stopped.
    """

    def __init__(self, expected: int, received: int, message: str) -> None:
        super().__init__(message)
        self.expected = expected
        self.received = received

@dataclass(frozen=True)
class QueryPage:
    """One page of search results, as returned by `Client.query_page`.

    :ivar results: The results on this page.
    :ivar total_results: The number of results across all pages.
    :ivar next_page_token: The token to pass as ``next_page_token`` to fetch
        the next page, or ``None`` if the API gave none.
    :ivar previous_page_token: The token to pass as ``previous_page_token``
        to fetch the previous page, or ``None`` if the API gave none.
    :ivar facets: The facet buckets, when facets were requested.
    """

    results: list[dict[str, Any]]
    total_results: int
    next_page_token: str | None = None
    previous_page_token: str | None = None
    facets: list[dict[str, Any]] | None = None

@dataclass(frozen=True)
class _SubmittedEntity:
    """Identifies an entity submitted to `Client.put`.

    :ivar index: The entity's position in the data passed to `Client.put`, so
        ``data[index]`` is the entity. This works whether or not it has an ``id``.
    :ivar entity_id: The entity's ``id``, or ``None`` if it has none (DataGraphs
        assigns one on write).
    """

    index: int
    entity_id: str | None

    def describe(self) -> str:
        """A short label for logs, e.g. ``#17 urn:proj:Agent:a1`` or ``#17 (no id)``."""
        return f'#{self.index} {self.entity_id}' if self.entity_id is not None else f'#{self.index} (no id)'

@dataclass(frozen=True)
class FailedWrite(_SubmittedEntity):
    """An entity the API did not accept.

    :ivar index: The entity's position in the data passed to `Client.put`.
    :ivar entity_id: The entity's ``id``, or ``None`` if it has none.
    :ivar status_code: The HTTP status of the failed request, or ``None`` when
        no response was received (e.g. a connection error or an unserialisable
        payload).
    :ivar errors: The error details for the failure.
    """

    status_code: int | None
    errors: list[Any]

@dataclass(frozen=True)
class TimedOutWrite(_SubmittedEntity):
    """An entity whose write timed out, and so may or may not have been persisted.

    :ivar index: The entity's position in the data passed to `Client.put`.
    :ivar entity_id: The entity's ``id``, or ``None`` if it has none.
    """

@dataclass(frozen=True)
class UnsentWrite(_SubmittedEntity):
    """An entity `Client.put` did not write because it stopped before doing so.

    The API did not accept it: it was not sent, or was refused for lack of
    valid authentication. So it can be resubmitted without creating a
    duplicate, even if it has no ``id``.

    :ivar index: The entity's position in the data passed to `Client.put`.
    :ivar entity_id: The entity's ``id``, or ``None`` if it has none.
    """

@dataclass
class PutResult:
    """The outcome of `Client.put`, accounting for every entity submitted.

    Each submitted entity is counted in exactly one of ``loaded``, ``failed``,
    ``timed_out`` or ``unsent``.

    :ivar loaded: The number of entities the API confirmed as written.
    :ivar failed: Entities that were not written, with the reason for each.
    :ivar timed_out: Entities whose request timed out (HTTP 504 or a read
        timeout), so whose write may or may not have completed server-side.
        One with an ``id`` can be verified by reading it back, or resubmitted,
        since a write with an ``id`` is an idempotent upsert. One without an
        ``id`` is given a new ``id`` by DataGraphs on every write, so
        resubmitting it may create a duplicate: verify it first by comparing
        its property values with the persisted entities of its type.
    :ivar unsent: Entities not written because `put` stopped early, on an
        `AuthenticationError` or `TokenRequestError`. Only a result attached
        to such an error (as its ``result``) has any.
    """

    loaded: int = 0
    failed: list[FailedWrite] = field(default_factory=list)
    timed_out: list[TimedOutWrite] = field(default_factory=list)
    unsent: list[UnsentWrite] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """True when every submitted entity was confirmed as written."""
        return not self.failed and not self.timed_out and not self.unsent

class _BatchOutcome(Enum):
    WRITTEN = auto()
    REJECTED = auto()   # the request payload was refused: worth resubmitting entity by entity
    FAILED = auto()     # a server or transport failure unrelated to the payload
    TIMED_OUT = auto()

class _PutLedger:
    """Accounts for each entity of one `Client.put` in its `PutResult` exactly once.

    It also notes which entities have been accounted for, so that if the put
    stops part-way the rest can be reported as unsent.
    """

    def __init__(self, entities: list[dict[str, Any]]) -> None:
        self.result = PutResult()
        self._entities = entities
        self._accounted = bytearray(len(entities))

    def record(self, batch: list[dict[str, Any]], start: int, outcome: '_BatchOutcome', error: Exception | None) -> None:
        """Account for every entity in *batch* according to *outcome*.

        *start* is the position of the batch's first entity in the data passed to `put`.
        """
        self._accounted[start:start + len(batch)] = b'\x01' * len(batch)
        if outcome is _BatchOutcome.WRITTEN:
            self.result.loaded += len(batch)
            return
        refs = [(index, _entity_id(entity)) for index, entity in enumerate(batch, start=start)]
        if outcome is _BatchOutcome.TIMED_OUT:
            timed_out = [TimedOutWrite(index, entity_id) for index, entity_id in refs]
            _logger.warning('Request timed out writing %d entities - the write may still have completed: %s',
                            len(timed_out), [t.describe() for t in timed_out])
            self.result.timed_out.extend(timed_out)
            return
        status_code = error.status_code if isinstance(error, HttpStatusError) else None
        errors = error.errors if isinstance(error, HttpStatusError) else [str(error)]
        failures = [FailedWrite(index, entity_id, status_code, errors) for index, entity_id in refs]
        _logger.error('Failed to write %d entities %s: %s', len(failures), [f.describe() for f in failures], errors)
        self.result.failed.extend(failures)

    def record_rest_unsent(self) -> None:
        """Report every entity not yet accounted for as unsent."""
        self.result.unsent.extend(
            UnsentWrite(index, _entity_id(entity))
            for index, (entity, accounted) in enumerate(zip(self._entities, self._accounted))
            if not accounted
        )

def _entity_id(entity: Any) -> str | None:
    return entity.get('id') if isinstance(entity, dict) else None

@dataclass
class _RejectionTally:
    """The rejected batches isolated so far in one `Client.put`, to gauge how dense rejections are.

    :ivar entities: The entities in the rejected batches that were isolated.
    :ivar rejected: Of those, the entities rejected on their own.
    """

    entities: int = 0
    rejected: int = 0

@dataclass
class _IsolationBudget:
    """The requests left to isolate the rejected entities of one rejected batch.

    Splitting is allowed only while the isolation could still finish one
    entity at a time within *limit*, so ``spent + unresolved <= limit`` holds
    throughout, and the isolation never makes more than *limit* requests.

    :ivar limit: The most requests the isolation may make.
    :ivar spent: The requests made so far.
    :ivar unresolved: The entities in rejected groups not yet isolated, which
        would cost one request each to finish one at a time.
    :ivar rejected: The entities found to be rejected on their own.
    """

    limit: int
    spent: int = 0
    unresolved: int = 0
    rejected: int = 0

    def can_split(self) -> bool:
        """Whether splitting an unresolved group (two requests) keeps room to finish one at a time.

        If both halves are rejected, nothing is resolved, so the cost of
        finishing one at a time then exceeds the cost now by two requests.
        """
        return self.spent + 2 + self.unresolved <= self.limit

# Typographic and invisible characters that routinely contaminate credentials
# and configuration when values are copied from rich-text sources (email, Word,
# Google Docs, Slack, web pages). Several of these — notably the "smart" quotes
# and dashes — cannot be encoded in latin-1, which is how ``requests`` serialises
# HTTP header values, so an unrepaired value fails with an opaque
# ``UnicodeEncodeError`` long before reaching the API. Mapping each to its plain
# ASCII equivalent (or stripping it, for zero-width artefacts) lets a pasted
# value keep working. The table is built once at import for cheap ``str.translate``.
_TEXT_REPLACEMENTS = {
    # Double quotation marks
    0x201C: '"', 0x201D: '"', 0x201E: '"', 0x201F: '"', 0x2033: '"',
    # Single quotation marks / apostrophes
    0x2018: "'", 0x2019: "'", 0x201A: "'", 0x201B: "'", 0x2032: "'",
    # Dashes and minus sign
    0x2013: '-', 0x2014: '-', 0x2015: '-', 0x2212: '-',
    # Ellipsis
    0x2026: '...',
    # Non-breaking and other exotic spaces
    0x00A0: ' ', 0x2007: ' ', 0x202F: ' ',
    # Zero-width characters and byte-order mark — strip entirely
    0x200B: None, 0x200C: None, 0x200D: None, 0xFEFF: None,
}

class Client:
    """Low-level HTTP client for the DataGraphs REST API.

    Handles authentication, pagination, and batched writes. Every read either
    returns complete results or raises: a failed request raises
    `HttpStatusError` (or `DatagraphsError` for a transport failure such as a
    timeout), and a paged read whose pages do not add up raises
    `IncompleteReadError`. An empty list always means there was nothing to read.
    """

    _PROD_URL = "https://api.datagraphs.io/"
    _AUTH_URL_SUFFIX = "oauth/token"

    _ALL_TYPES_FILTER = "_all"

    DEFAULT_BATCH_SIZE = 100
    DEFAULT_WAIT_TIME_MS = 200
    # (connect, read) in seconds. The read timeout is generous, so that a slow
    # request normally ends with the API's own 504 rather than a client timeout.
    DEFAULT_TIMEOUT_S = (10.0, 60.0)
    DEFAULT_MAX_READ_RETRIES = 3
    _DEFAULT_FACET_SIZE = 10
    _MAX_AUTH_RETRIES = 2
    _DATASETS_TIMEOUT_S = 300

    # Polling backoff for confirming dataset application. Each pair is
    # (elapsed-time threshold in seconds, poll interval in seconds while below
    # that threshold); past the final threshold, _POLL_SLOW_INTERVAL_S applies
    # until the timeout. This yields: every 1s for the first 10s, every 2s for
    # the next 20s, then every 5s.
    _POLL_SCHEDULE = ((10, 1), (30, 2))
    _POLL_SLOW_INTERVAL_S = 5

    # HTTP status codes
    _HTTP_BAD_REQUEST = 400
    _HTTP_UNAUTHORIZED = 401
    _HTTP_FORBIDDEN = 403
    _HTTP_GATEWAY_TIMEOUT = 504
    _HTTP_SUCCESS = range(200, 300)
    _AUTH_REJECTED = frozenset({400, 401, 403})
    # Transient statuses on which an idempotent read is retried.
    _RETRYABLE_READ_STATUSES = frozenset({429, 502, 503, 504})
    # Exponential backoff between read retries: 1s, 2s, 4s... capped at 30s,
    # each jittered to between half and all of that, so that concurrent
    # clients do not retry in lockstep. A Retry-After header overrides it.
    # Transport failures on which an idempotent read is retried. A
    # ChunkedEncodingError is a response body cut off part-way.
    _TRANSIENT_TRANSPORT_ERRORS = (
        requests.exceptions.ConnectionError,
        requests.exceptions.Timeout,
        requests.exceptions.ChunkedEncodingError,
    )
    _RETRY_BASE_DELAY_S = 1.0
    _RETRY_MAX_DELAY_S = 30.0
    _HTTP_CLIENT_ERRORS = range(400, 500)
    # 4xx statuses that say nothing about the payload's content, so resubmitting
    # a rejected batch entity by entity cannot help (and, for 429, would worsen it).
    _NON_PAYLOAD_CLIENT_ERRORS = frozenset({404, 405, 408, 429})
    _DEFAULT_DATASETS_PAGE_SIZE = 1000

    _DATA_EXISTS = "DATA_EXISTS"
    _IN_USE_BY_DATASET = "IN_USE_BY_DATASET"

    def __init__(
        self, 
        project_name: str, 
        api_key: str, 
        client_id: str = "", 
        client_secret: str = "", 
        batch_size: int = DEFAULT_BATCH_SIZE, 
        service_url: str = _PROD_URL,
        timeout: float | tuple[float, float] = DEFAULT_TIMEOUT_S,
        max_read_retries: int = DEFAULT_MAX_READ_RETRIES,
    ) -> None:
        """Initialise the DataGraphs client.

        :param project_name: Name of the project.
        :param api_key: API key for authentication.
        :param client_id: OAuth client ID (required for write operations).
        :param client_secret: OAuth client secret (required for write operations).
        :param batch_size: Number of items to process in each batch.
        :param service_url: Base URL for the API service.
        :param timeout: Seconds to wait for every request, including the auth
            token request: either one value for both connecting and reading,
            or a ``(connect, read)`` pair. A request that exceeds it raises.
        :param max_read_retries: How many times a GET request is retried after
            a transient failure (HTTP 429, 502, 503 or 504, a connection error
            or a timeout) before the failure is raised. ``0`` disables retries.
            Writes and GQL queries are never retried.
        """
        if project_name is None or len(project_name) == 0:
            raise ValueError("project_name is required")
        if api_key is None or len(api_key) == 0:
            raise ValueError("api_key is required")
        timeouts = timeout if isinstance(timeout, tuple) else (timeout,)
        if len(timeouts) not in (1, 2) or not all(t > 0 for t in timeouts):
            raise ValueError(f"timeout must be a positive number or a (connect, read) pair of them, not {timeout!r}")
        if max_read_retries < 0:
            raise ValueError(f"max_read_retries must not be negative, not {max_read_retries}")
        self.project_name = self._sanitise_text(project_name)
        self._api_key = self._sanitise_text(api_key)
        self._client_id = self._sanitise_text(client_id)
        self._client_secret = self._sanitise_text(client_secret)
        self._batch_size = batch_size
        service_url = self._sanitise_text(service_url)
        self._service_url = service_url if service_url.endswith('/') else f'{service_url}/'
        self._timeout = timeout
        self._max_read_retries = max_read_retries
        # A session reuses connections across requests, sparing a TLS handshake
        # for every page of a paged read.
        self._http_client = requests.Session()
        self._wait_time_ms = self.DEFAULT_WAIT_TIME_MS
        self._auth_token = ''

    def _sanitise_text(self, value: str) -> str:
        return value.translate(_TEXT_REPLACEMENTS).strip()

    @property
    def _base_url(self) -> str:
        return f'{self._service_url}{self.project_name}/'

    def set_wait_time(self, wait_time_ms: int) -> None:
        """Set the wait time between paginated requests.

        :param wait_time_ms: Wait time in milliseconds.
        """
        self._wait_time_ms = wait_time_ms

    @property
    def wait_time_ms(self) -> int:
        """The current wait time in milliseconds between paginated requests."""
        return self._wait_time_ms

    def _get_auth_token(self, force_refresh=False) -> str:
        if force_refresh or not self._auth_token:
            headers = {
                'Content-Type': 'application/json', 
                'Accept': 'application/json', 
                'x-api-key': self._api_key
            }
            body = { 
                'clientId': self._client_id, 
                'clientSecret': self._client_secret
            }
            try:
                response = self._http_client.post(
                    f'{self._service_url}{self._AUTH_URL_SUFFIX}',
                    headers=headers, 
                    data=json.dumps(body),
                    timeout=self._timeout,
                )
            except requests.exceptions.RequestException as e:
                # A transport failure says nothing about the credentials, so it
                # is not an AuthenticationError.
                raise TokenRequestError(None, [str(e)], f"Failed to obtain auth token: {e}") from e
            status = response.status_code
            if status in self._AUTH_REJECTED:
                raise AuthenticationError(f"Failed to obtain auth token: credentials rejected with status {status}: {response.text}")
            if status not in self._HTTP_SUCCESS:
                raise TokenRequestError(status, self._extract_errors(response), f"Failed to obtain auth token: status {status}: {response.text}")
            try:
                data = response.json()
                self._auth_token = f"{data['token_type']} {data['access_token']}"
            except (ValueError, KeyError, TypeError) as e:
                raise TokenRequestError(status, [response.text], f"Failed to obtain auth token: unexpected response: {response.text}") from e
        return self._auth_token

    def _request(self, method: HTTP, url: str, lang: str = 'all', _retry_count: int = 0, **kwargs) -> requests.Response:
        """Execute an HTTP request with automatic auth retry, raising on any failure.

        The headers, including any auth token, are built afresh for every
        attempt, so a token obtained or refreshed for one attempt is used by it.

        :param method: The HTTP method to use.
        :param url: The request URL.
        :param lang: The language to request results in.
        :param _retry_count: Internal retry counter (do not set externally).
        :param kwargs: Additional arguments forwarded to the HTTP client.
        :returns: The response, which always has a 2xx status.
        :raises AuthenticationError: If authentication fails after max retries.
        :raises TokenRequestError: If an auth token cannot be obtained; the
            request itself was then never sent.
        :raises HttpStatusError: If the API responds with any other non-2xx status.
        :raises DatagraphsError: If the request fails for any other reason,
            with the underlying ``requests`` exception (e.g. a ``ReadTimeout``)
            as its ``__cause__``.
        """
        headers = self._get_headers(lang)
        if method in [HTTP.PUT, HTTP.POST]:
            headers['Content-Type'] = 'application/json'
        try:
            response = self._http_client.request(str(method), url, headers=headers, timeout=self._timeout, **kwargs)
        except UnicodeError as e:
            # Strict in what we send: a value escaped sanitisation and cannot be
            # encoded into the HTTP request (latin-1 header serialisation). Surface
            # a clear, actionable error instead of a bare codec failure.
            raise DatagraphsError(
                "Request contains a character that cannot be encoded in an HTTP "
                "header or URL, most likely a non-ASCII character in a credential "
                "or project name copied from a rich-text source. Check api_key, "
                f"client_id, client_secret and project_name: {e}"
            ) from e
        except requests.exceptions.RequestException as e:
            raise DatagraphsError(f"Request failed: {e}") from e
        status = response.status_code
        if status in self._HTTP_SUCCESS:
            return response
        if status in [self._HTTP_UNAUTHORIZED, self._HTTP_FORBIDDEN]:
            if _retry_count < self._MAX_AUTH_RETRIES:
                if self._has_oauth_credentials():
                    self._get_auth_token(force_refresh=True)
                return self._request(method, url, lang, _retry_count=_retry_count + 1, **kwargs)
            raise AuthenticationError(f'Authentication failed after {self._MAX_AUTH_RETRIES+1} attempts')
        raise HttpStatusError(status, self._extract_errors(response), f"Request failed with status {status}: {response.text}",
                              retry_after_s=self._retry_after_s(response))

    def _send(self, method: HTTP, url: str, **kwargs) -> None:
        """Execute a request whose response body is not needed, raising on any failure."""
        self._request(method, url, **kwargs)

    def _read_json(self, method: HTTP, url: str, retries: int | None = None, **kwargs) -> Any:
        """Execute a request and return its JSON body, raising on any failure.

        A GET is idempotent, so one that fails transiently is retried, with
        backoff, before the failure is raised.

        :param retries: How many times to retry a transient failure: by
            default ``max_read_retries`` for a GET, and none otherwise.
        :raises DatagraphsError: If the request fails, or a successful response
            has an empty or unparseable body.
        """
        if retries is None:
            retries = self._max_read_retries if method == HTTP.GET else 0
        attempts = 1 + retries
        for attempt in range(1, attempts + 1):
            try:
                response = self._request(method, url, **kwargs)
                break
            except DatagraphsError as e:
                if attempt == attempts or not self._is_transient(e):
                    raise
                delay_s = self._retry_delay_s(attempt, e)
                _logger.warning('Request failed (%s) - retrying in %.1fs (attempt %d of %d): %s %s',
                                e, delay_s, attempt + 1, attempts, method, url)
                time.sleep(delay_s)
        if not response.content:
            raise DatagraphsError(f"Response to {method} {url} has no body (status {response.status_code})")
        try:
            return response.json()
        except ValueError as e:
            raise DatagraphsError(f"Response to {method} {url} is not valid JSON: {response.text[:200]}") from e

    def _is_transient(self, error: DatagraphsError) -> bool:
        """Whether *error* is a failure that retrying the same read could cure."""
        if isinstance(error, AuthenticationError):
            return False
        if isinstance(error, (HttpStatusError, TokenRequestError)) and error.status_code is not None:
            return error.status_code in self._RETRYABLE_READ_STATUSES
        return isinstance(error.__cause__, self._TRANSIENT_TRANSPORT_ERRORS)

    def _retry_delay_s(self, attempt: int, error: DatagraphsError) -> float:
        """The delay before retry number *attempt*, honouring any Retry-After the API sent."""
        if isinstance(error, HttpStatusError) and error.retry_after_s is not None:
            return min(error.retry_after_s, self._RETRY_MAX_DELAY_S)
        backoff_s = min(self._RETRY_BASE_DELAY_S * 2 ** (attempt - 1), self._RETRY_MAX_DELAY_S)
        return random.uniform(backoff_s / 2, backoff_s)  # noqa: S311 - jitter, not cryptography

    @staticmethod
    def _retry_after_s(response: requests.Response) -> float | None:
        """The delay in seconds from a response's ``Retry-After`` header, if it gives one in seconds."""
        value = response.headers.get('Retry-After')
        if isinstance(value, str):
            try:
                return max(float(value), 0.0)
            except ValueError:
                pass  # an HTTP-date: fall back to the backoff schedule
        return None

    def _is_timeout(self, error: Exception) -> bool:
        """Whether *error* means a request was sent but no response arrived in time.

        The API's 504 and a client read timeout both mean the request may
        still have completed server-side. A connect timeout does not: the
        request was never sent.
        """
        if isinstance(error, TokenRequestError):
            return False  # the token request failed, so the request itself was never sent
        if isinstance(error, HttpStatusError):
            return error.status_code == self._HTTP_GATEWAY_TIMEOUT
        return isinstance(error.__cause__, requests.exceptions.ReadTimeout)

    @staticmethod
    def _extract_errors(response: requests.Response) -> list[Any]:
        """Return the error details from a failure response body, falling back to its text."""
        try:
            body = response.json() if response.content else {}
        except ValueError:
            body = {}
        if isinstance(body, dict):
            if body.get('errors'):
                return body['errors']
            if body.get('message'):
                return [body['message']]
            if body.get('error'):
                return [body['error']]
        return [response.text or response.reason]

    def _has_oauth_credentials(self) -> bool:
        return bool(self._client_id and self._client_secret)

    def _get_headers(self, lang: str = 'all') -> dict[str, str]: 
        headers = {
            'Accept': 'application/json',
            'x-api-key': self._api_key,
            'Accept-Language': lang
        }
        if self._has_oauth_credentials():
            headers['Authorization'] = self._get_auth_token()
        return headers

    # Characters left unencoded in query values. Both are legal in a query
    # string, and they carry the API's syntax: ``type:Cat``, ``a,b``.
    _QUERY_SAFE_CHARS = ':,'

    @classmethod
    def _build_query(cls, params: list[tuple[str, Any]]) -> str:
        """Build a query string from key-value pairs, URL-encoding each value.

        Every character that could change how the query string is read (``&``,
        ``=``, ``#``, ``+``, ``%``, spaces, non-ASCII...) is encoded, so each
        value reaches the API exactly as given.
        """
        return '&'.join(f'{k}={urllib.parse.quote_plus(str(v), safe=cls._QUERY_SAFE_CHARS)}' for k, v in params)

    def _cache_buster(self) -> str:
        """Return a cache-busting timestamp value."""
        return str(datetime.now(tz=UTC).timestamp())

    def status(self) -> str:
        """Check the API service status.

        :returns: The API status string, or ``'unknown'`` if unavailable.
        :raises AuthenticationError: If authentication fails.
        """
        url = f'{self._service_url}status?t={self._cache_buster()}'
        try:
            # A status check should answer promptly, so it is not retried.
            response = self._read_json(HTTP.GET, url, retries=0)
        except AuthenticationError:
            raise
        except DatagraphsError as e:
            _logger.warning('API status unavailable: %s', e)
            return 'unknown'
        return response.get('api', 'unknown') if isinstance(response, dict) else 'unknown'

    def get(self, class_name: str, lang: str = 'all', include_date_fields: bool = False) -> list[dict[str, Any]]:
        """Retrieve all entities of a given type.

        Pages through every result, and returns them only once all have been
        read (see `query_all`).

        :param class_name: The entity class to fetch.
        :param lang: Language code for results (default ``'all'``).
        :param include_date_fields: Whether to include system date metadata.
        :returns: A list of every entity of the class.
        :raises HttpStatusError: If any page request fails.
        :raises IncompleteReadError: If the pages do not add up to the reported total.
        :raises DatagraphsError: If the read fails for any other reason.
        """
        return self.query_all(filters=f'type:{class_name}', lang=lang, include_date_fields=include_date_fields)

    def _get_query_url(self, 
            dataset: str = '_all', 
            q: str = '', 
            filters: str = '', 
            facets: str = '', 
            facet_size: int = -1, 
            date_facets: str = '', 
            fields: str = '', 
            embed: str = '', 
            sort: str = '', 
            ids: str = '', 
            lang: str = 'all', 
            page_no: int = -1,
            page_size: int = -1,
            previous_page_token: str = '',
            next_page_token: str = '',
            include_date_fields: bool = False
        ) -> str:
        params = [('lang', lang), ('t', self._cache_buster())]
        if q:
            params.append(('q', q))
        if filters:
            params.append(('filter', filters))
        if facets:
            effective_facet_size = facet_size if facet_size > -1 else self._DEFAULT_FACET_SIZE
            params.append(('facets', facets))
            params.append(('facetSize', effective_facet_size))
        if date_facets:
            params.append(('dateFacets', date_facets))
        if fields:
            params.append(('fields', fields))
        if embed:
            params.append(('embed', embed))
        if sort:
            params.append(('sort', sort))
        if ids:
            params.append(('ids', ids))
        if page_no > 0:
            params.append(('pageNo', page_no))
        if page_size > -1:
            params.append(('pageSize', page_size))
        if previous_page_token:
            params.append(('previousPageToken', previous_page_token))
        if next_page_token:
            params.append(('nextPageToken', next_page_token))
        if include_date_fields:
            params.append(('includeDateFields', 'true'))
        return f'{self._base_url}{dataset}?{self._build_query(params)}'

    def query(self, 
            gql: str = '',
            dataset: str = '_all', 
            q: str = '', 
            filters: str = '', 
            facets: str = '', 
            facet_size: int = -1, 
            date_facets: str = '', 
            fields: str = '', 
            embed: str = '', 
            sort: str = '', 
            ids: str = '', 
            lang: str = 'all', 
            page_no: int = -1,
            page_size: int = -1,
            previous_page_token: str = '',
            next_page_token: str = '',
            include_date_fields: bool = False
        ) -> list[dict[str, Any]] | tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Query the API with filters, facets, sorting, and pagination.

        Returns a single page of results. Use `query_page` to also get the
        total and the page tokens, or `query_all` to read every page.

        :param gql: a GQL query string - if this is specified then all other
            arguments except ``lang`` are ignored, and the rows the query
            returns are returned as a list.
        :param dataset: Dataset slug to query (default ``'_all'``).
        :param q: Free-text search query string.
        :param filters: Filter expression (e.g. ``'type:Person'``).
        :param facets: Comma-separated facet field names.
        :param facet_size: Number of facet values to return (default ``10``).
        :param date_facets: Date facet specification.
        :param fields: Comma-separated field names to include in results.
        :param embed: Embedding depth for related entities.
        :param sort: Sort expression (e.g. ``'label:asc'``).
        :param ids: Comma-separated entity IDs to fetch directly.
        :param lang: Language code for results (default ``'all'``).
        :param page_no: Page number for offset-based pagination.
        :param page_size: Number of results per page.
        :param previous_page_token: Token for cursor-based backward pagination.
        :param next_page_token: Token for cursor-based forward pagination.
        :param include_date_fields: Whether to include system date metadata.
        :returns: A list of result dicts, or a ``(results, facets)`` tuple when
            the response carries facets.
        :raises HttpStatusError: If the request fails.
        :raises DatagraphsError: If the request fails for any other reason, or
            the response is malformed.
        """
        if gql:
            return self._query_gql(gql, lang)
        if ids:
            url = self._get_query_url(
                dataset, q, filters, facets, facet_size, date_facets, fields, embed, sort, ids, lang, page_no,
                page_size if page_size > -1 else self._batch_size, previous_page_token, next_page_token, include_date_fields,
            )
            body = self._read_json(HTTP.GET, url, lang=lang)
            # An ids lookup answers with a plain list of the entities found.
            return body if isinstance(body, list) else self._parse_search_page(body, url).results
        page = self.query_page(
            dataset=dataset, q=q, filters=filters, facets=facets, facet_size=facet_size, date_facets=date_facets,
            fields=fields, embed=embed, sort=sort, lang=lang, page_no=page_no, page_size=page_size,
            previous_page_token=previous_page_token, next_page_token=next_page_token, include_date_fields=include_date_fields,
        )
        return (page.results, page.facets) if page.facets is not None else page.results

    def query_page(self,
            dataset: str = '_all',
            q: str = '',
            filters: str = '',
            facets: str = '',
            facet_size: int = -1,
            date_facets: str = '',
            fields: str = '',
            embed: str = '',
            sort: str = '',
            lang: str = 'all',
            page_no: int = -1,
            page_size: int = -1,
            previous_page_token: str = '',
            next_page_token: str = '',
            include_date_fields: bool = False
        ) -> QueryPage:
        """Fetch one page of search results, with the total and the tokens for paging on.

        Takes the same arguments as `query`, except ``gql`` and ``ids``.
        ``page_size`` defaults to the client's ``batch_size``.

        :returns: The page.
        :raises HttpStatusError: If the request fails.
        :raises DatagraphsError: If the request fails for any other reason, or
            the response is malformed.
        """
        url = self._get_query_url(
            dataset=dataset, q=q, filters=filters, facets=facets, facet_size=facet_size, date_facets=date_facets,
            fields=fields, embed=embed, sort=sort, lang=lang, page_no=page_no,
            page_size=page_size if page_size > -1 else self._batch_size,
            previous_page_token=previous_page_token, next_page_token=next_page_token, include_date_fields=include_date_fields,
        )
        return self._read_search_page(url, lang)

    def query_all(self,
            dataset: str = '_all',
            q: str = '',
            filters: str = '',
            fields: str = '',
            embed: str = '',
            sort: str = '',
            lang: str = 'all',
            page_size: int = -1,
            include_date_fields: bool = False
        ) -> list[dict[str, Any]]:
        """Read every result of a search, paging to the end.

        Results are returned only once all have been read, so a partial result
        is never returned: every page must succeed, and the results must add
        up to the ``totalResults`` the API reported, with no result repeated.
        Pages are fetched by ``nextPageToken`` whenever the API provides one,
        since page numbers cannot reach past the first 10,000 results.

        :param dataset: Dataset slug to query (default ``'_all'``).
        :param q: Free-text search query string.
        :param filters: Filter expression (e.g. ``'type:Person'``).
        :param fields: Comma-separated field names to include in results.
        :param embed: Embedding depth for related entities.
        :param sort: Sort expression (e.g. ``'label:asc'``).
        :param lang: Language code for results (default ``'all'``).
        :param page_size: Number of results per page request (default: the
            client's ``batch_size``).
        :param include_date_fields: Whether to include system date metadata.
        :returns: Every result, in the order the API returned them.
        :raises HttpStatusError: If any page request fails.
        :raises IncompleteReadError: If the pages do not add up to the reported total.
        :raises DatagraphsError: If the read fails for any other reason, or a
            response is malformed.
        """
        page_size = page_size if page_size > -1 else self._batch_size
        def page_url(next_page_token: str, page_no: int) -> str:
            return self._get_query_url(
                dataset=dataset, q=q, filters=filters, fields=fields, embed=embed, sort=sort, lang=lang,
                page_no=-1 if next_page_token else page_no, page_size=page_size,
                next_page_token=next_page_token, include_date_fields=include_date_fields,
            )
        return self._read_all_pages(page_url, lang)

    def _read_all_pages(self, page_url: Callable[[str, int], str], lang: str = 'all') -> list[dict[str, Any]]:
        """Read every page of a search, returning all results or raising.

        :param page_url: Builds the URL of a page from a ``nextPageToken``
            (``''`` when there is none) and a page number.
        """
        page = self._read_search_page(page_url('', 1), lang)
        expected = page.total_results
        # Token paging, once begun, must continue to the end: switching to page
        # numbers part-way would not resume where the tokens left off.
        token_paging = page.next_page_token is not None
        results: list[dict[str, Any]] = []
        seen_ids: set[Any] = set()
        page_no, token = 1, ''
        while True:
            for result in page.results:
                result_id = result.get('id') if isinstance(result, dict) else None
                if result_id is not None:
                    if result_id in seen_ids:
                        raise IncompleteReadError(expected, len(results), f'Page {page_no} repeated {result_id}: the data may have changed while it was read')
                    seen_ids.add(result_id)
                results.append(result)
            # The API gives a nextPageToken even on the last page, so the count,
            # not the token, says when the read is done.
            if len(results) >= expected or not page.results:
                break
            if token_paging:
                if not page.next_page_token:
                    break
                if page.next_page_token == token:
                    raise IncompleteReadError(expected, len(results), f'Page {page_no} returned the nextPageToken it was requested with')
                token = page.next_page_token
            page_no += 1
            time.sleep(self._wait_time_ms / 1000)
            page = self._read_search_page(page_url(token, page_no), lang)
            # A changed total means results were added or removed during the
            # read, so pages may have shifted to skip or repeat results - even
            # ones without an id, which the repeat check cannot see.
            if page.total_results != expected:
                raise IncompleteReadError(expected, len(results), f'The total changed from {expected} to {page.total_results} at page {page_no}: the data changed while it was read')
        if len(results) != expected:
            raise IncompleteReadError(expected, len(results), f'Read {len(results)} results, but the API reported {expected}')
        return results

    def _read_search_page(self, url: str, lang: str = 'all') -> QueryPage:
        """GET one page of search results."""
        return self._parse_search_page(self._read_json(HTTP.GET, url, lang=lang), url)

    @staticmethod
    def _parse_search_page(body: Any, url: str) -> QueryPage:
        """Parse a search response body, rejecting one that is not a search page."""
        search = body.get('search') if isinstance(body, dict) else None
        total = search.get('totalResults') if isinstance(search, dict) else None
        if not isinstance(total, int) or isinstance(total, bool) or total < 0:
            raise DatagraphsError(f'Malformed search response from {url}: no valid search.totalResults')
        results = body.get('results', [] if total == 0 else None)
        if not isinstance(results, list):
            raise DatagraphsError(f'Malformed search response from {url}: no results list')
        return QueryPage(
            results=results,
            total_results=total,
            next_page_token=search.get('nextPageToken') or None,
            previous_page_token=search.get('previousPageToken') or None,
            facets=body.get('facets'),
        )

    def _query_gql(self, gql:str, lang: str) -> list[dict[str, Any]]:
        url = f'{self._base_url}_cypher'
        body = self._read_json(HTTP.POST, url, json={'query': gql}, lang=lang)
        results = body.get('results') if isinstance(body, dict) else None
        if not isinstance(results, list):
            raise DatagraphsError(f'Malformed GQL response from {url}: no results list')
        return results

    def put(self, dataset: str, data: dict[str, Any] | list[dict[str, Any]], isolation: ISOLATION_MODE = ISOLATION_MODE.BISECT) -> PutResult:
        """Load entities into a dataset.

        Entities are sent in batches of the configured ``batch_size``, and every
        batch is attempted regardless of the outcome of earlier ones. A batch
        whose payload is rejected (a 4xx response other than 404, 405, 408 or
        429, or a value that cannot be JSON-encoded) is resubmitted as
        *isolation* directs, so that only the offending entities fail. A batch
        that fails for reasons unrelated to its payload (those 4xx statuses, a
        5xx response or a transport error) is not resubmitted, and all of its
        entities are reported as failed. A batch that times out (504 or a read
        timeout) is reported as timed out, since its write may still have
        completed. All of this applies equally to each part of a batch that is
        resubmitted.

        DataGraphs writes no part of a batch it rejects, so resubmitting parts
        of one cannot write an entity twice, even one without an ``id``. An
        entity is reported as rejected only once it has been rejected on its
        own, so each rejection carries that entity's own status and errors.

        :param dataset: Target dataset slug.
        :param data: A single entity dict or a list of entity dicts.
        :param isolation: How to find the rejected entities of a rejected
            batch. ``BISECT`` (the default) splits it in halves until they are
            isolated, switching to one entity at a time when most are
            rejected; ``INDIVIDUAL`` resubmits each entity on its own; ``NONE``
            reports the whole batch as failed without resubmitting it.
        :returns: The outcome for every submitted entity.
        :raises ValueError: If *isolation* is not an `ISOLATION_MODE` value.
        :raises AuthenticationError: If authentication fails; no further
            batches are attempted. Its ``result`` accounts for every entity,
            with those not written in ``unsent``.
        :raises TokenRequestError: If an auth token cannot be obtained; no
            further batches are attempted. Its ``result`` accounts for every
            entity, with those not written in ``unsent``.
        """
        isolation = ISOLATION_MODE(isolation)
        entities = [data] if isinstance(data, dict) else data
        length = len(entities)
        url = f'{self._base_url}{dataset}'
        ledger = _PutLedger(entities)
        _logger.info('Loading %d entities into dataset %s in repo: %s', length, dataset, self.project_name)
        try:
            self._put_batches(url, dataset, entities, isolation, ledger)
        except (AuthenticationError, TokenRequestError) as e:
            ledger.record_rest_unsent()
            e.result = ledger.result
            self._log_put_result(ledger.result, length, dataset)
            raise
        self._log_put_result(ledger.result, length, dataset)
        return ledger.result

    def _put_batches(self, url: str, dataset: str, entities: list[dict[str, Any]], isolation: ISOLATION_MODE, ledger: _PutLedger) -> None:
        """Send *entities* in batches, accounting for each in *ledger*."""
        length = len(entities)
        tally = _RejectionTally()
        for i in range(0, length, self._batch_size):
            batch = entities[i:i + self._batch_size]
            if length > self._batch_size:
                _logger.info('   Loading batch %d-%d of %d entities into dataset %s in repo: %s', i, i + len(batch), length, dataset, self.project_name)
            outcome, error = self._put_batch(url, batch)
            if outcome is _BatchOutcome.REJECTED and len(batch) > 1 and isolation is not ISOLATION_MODE.NONE:
                budget = _IsolationBudget(limit=len(batch) + self._isolation_slack(isolation, len(batch), tally), unresolved=len(batch))
                _logger.warning('Batch %d-%d of %d entities rejected by dataset %s (%s) - resubmitting to isolate the rejected entities (at most %d requests)',
                                i, i + len(batch), length, dataset, error, budget.limit)
                self._isolate(url, batch, i, ledger, budget)
                tally.entities += len(batch)
                tally.rejected += budget.rejected
            else:
                ledger.record(batch, i, outcome, error)

    @staticmethod
    def _log_put_result(result: PutResult, length: int, dataset: str) -> None:
        if not result.ok:
            _logger.error('Loaded %d of %d entities into dataset %s: %d failed, %d timed out, %d not sent',
                          result.loaded, length, dataset, len(result.failed), len(result.timed_out), len(result.unsent))

    def _isolation_slack(self, isolation: ISOLATION_MODE, size: int, tally: _RejectionTally) -> int:
        """The requests beyond one per entity that isolating a rejected batch of *size* may spend.

        Resubmitting one entity at a time costs *size* requests. Bisecting costs
        about 2·log₂(size) per rejected entity: far less when few are rejected,
        but up to twice as much when all are. Bisection is therefore allowed
        the cost of isolating one entity beyond *size*, and none at all when
        the rejections so far suggest it would cost more than *size*.
        """
        if isolation is ISOLATION_MODE.INDIVIDUAL:
            return 0
        expected = max(1, round(size * tally.rejected / tally.entities)) if tally.entities else 1
        if self._bisection_cost(size, expected) >= size:
            return 0
        return 2 * math.ceil(math.log2(size))

    @staticmethod
    @functools.cache
    def _bisection_cost(size: int, rejected: int) -> int:
        """The requests bisection makes to isolate *rejected* entities spread evenly through a rejected batch of *size*."""
        if size <= 1 or rejected == 0:
            return 0
        left_size = (size + 1) // 2
        left = min(left_size, (rejected + 1) // 2)
        right = min(size - left_size, rejected - left)
        left = rejected - right
        return 2 + Client._bisection_cost(left_size, left) + Client._bisection_cost(size - left_size, right)

    def _isolate(self, url: str, group: list[dict[str, Any]], start: int, ledger: _PutLedger, budget: _IsolationBudget) -> None:
        """Find the rejected entities of a rejected *group*, accounting for every entity in *ledger*.

        The group is split in halves while *budget* allows, and otherwise
        resubmitted one entity at a time. *start* is the position of its first
        entity in the data passed to `put`.
        """
        split = budget.can_split()
        budget.unresolved -= len(group)
        parts = [group[:(len(group) + 1) // 2], group[(len(group) + 1) // 2:]] if split else [[entity] for entity in group]
        rejected = []
        offset = start
        for part in parts:
            budget.spent += 1
            outcome, error = self._put_batch(url, part)
            if outcome is _BatchOutcome.REJECTED and len(part) > 1:
                budget.unresolved += len(part)
                rejected.append((part, offset))
            else:
                if outcome is _BatchOutcome.REJECTED:
                    budget.rejected += 1
                ledger.record(part, offset, outcome, error)
            offset += len(part)
        for part, part_start in rejected:
            self._isolate(url, part, part_start, ledger, budget)

    def _put_batch(self, url: str, batch: list[dict[str, Any]]) -> tuple[_BatchOutcome, Exception | None]:
        """PUT one batch and classify the outcome, returning it with the error (if any)."""
        try:
            self._send(HTTP.PUT, url, json=batch)
        except (AuthenticationError, TokenRequestError):
            # Nothing can be written without a token, so stop rather than
            # report every remaining entity as failed.
            raise
        except DatagraphsError as e:
            if self._is_timeout(e):
                return _BatchOutcome.TIMED_OUT, e
            if isinstance(e, HttpStatusError):
                if e.status_code in self._HTTP_CLIENT_ERRORS and e.status_code not in self._NON_PAYLOAD_CLIENT_ERRORS:
                    return _BatchOutcome.REJECTED, e
                return _BatchOutcome.FAILED, e
            # requests reports an out-of-range float (e.g. NaN) in the payload this way.
            if isinstance(e.__cause__, requests.exceptions.InvalidJSONError):
                return _BatchOutcome.REJECTED, e
            return _BatchOutcome.FAILED, e
        except TypeError as e:
            # requests lets a value that is not JSON serialisable escape as a bare TypeError.
            return _BatchOutcome.REJECTED, e
        return _BatchOutcome.WRITTEN, None

    def delete(self, class_name: str, entity_id: str) -> None:
        """Delete a single entity by class and ID.

        :param class_name: The entity class.
        :param entity_id: The entity identifier.
        :raises HttpStatusError: If the API does not confirm the delete.
        """
        url = f'{self._base_url}{class_name}/{entity_id}'
        self._send(HTTP.DELETE, url)

    def apply_schema(self, schema: DatagraphsSchema, mode: SCHEMA_APPLY_MODE = SCHEMA_APPLY_MODE.APPLY) -> None | list[dict[str, Any]]:
        """Apply a schema to the project, replacing the currently active domain model.

        :param schema: The schema to apply.
        :param mode: ``APPLY`` to apply it, ``VALIDATE_ONLY`` to report what
            would block it without applying it, or ``FORCE`` to clear and drop
            whatever blocks it and then apply it.
        :returns: For ``VALIDATE_ONLY``, the blocking dependencies the API
            reported (empty if none); otherwise ``None``.
        :raises ValueError: If *mode* is not a `SCHEMA_APPLY_MODE` value.
        :raises DatagraphsError: If the API rejects the schema (and, for
            ``FORCE``, its dependencies cannot be resolved).
        :raises HttpStatusError: If the request fails for any reason other
            than the schema being rejected, e.g. a 504.
        """
        mode = SCHEMA_APPLY_MODE(mode)
        _logger.info('Applying schema to project: %s', self.project_name)
        url = f'{self._base_url}models/_active'+('?dryRun=true' if mode == SCHEMA_APPLY_MODE.VALIDATE_ONLY else '')
        try:
            self._send(HTTP.PUT, url, data=schema.to_json())
            errors = []
        except HttpStatusError as e:
            # The API rejects a schema with a 400 whose errors name what blocks it.
            if e.status_code != self._HTTP_BAD_REQUEST:
                raise
            errors = e.errors

        if all(isinstance(e, dict) for e in errors):
            if mode == SCHEMA_APPLY_MODE.VALIDATE_ONLY:
                return errors
            if len(errors) > 0:
                if mode == SCHEMA_APPLY_MODE.FORCE:
                    self._resolve_schema_update_dependencies(errors)
                    self.apply_schema(schema, mode=SCHEMA_APPLY_MODE.APPLY)
                else:
                    _logger.error('Schema application failed with errors: %s', errors)
                    raise DatagraphsError(f'Schema application failed with errors: { errors }')
        else:
            _logger.error('Schema application failed with errors: %s', errors)
            raise DatagraphsError(f'Schema application failed with errors: { errors }')

    def _resolve_schema_update_dependencies(self, dependencies: list[dict]) -> None:
        """Resolve schema update dependencies by clearing or dropping classes as needed.
        :param dependencies: The list of schema update dependencies.
        """
        _logger.info('Resolving schema update dependencies: %s', dependencies)
        classes_to_clear, classes_to_drop = self._identify_schema_update_dependencies(dependencies)
        self._remove_schema_update_dependencies(classes_to_clear, classes_to_drop)

    def _identify_schema_update_dependencies(self, dependencies: list[dict]) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        """Identify classes that need to be cleared or dropped based on the new schema.

        :param dependencies: The list of schema update dependencies.
        :returns: A tuple containing two sets:
            - Classes to clear (set of (class_name, dataset_slug))
            - Classes to drop (set of (class_name, dataset_slug))
        """
        classes_to_clear: set[tuple[str, str]] = set()
        classes_to_drop: set[tuple[str, str]] = set()
        for issue in dependencies:
            for type_info in issue.get("types", []):
                code = issue.get("code")
                class_name = type_info.get("type")
                for dataset_urn in type_info.get("datasets", []):
                    class_data = (class_name, Dataset.get_slug_from_id(dataset_urn))
                    if code == self._DATA_EXISTS:
                        classes_to_clear.add(class_data)
                    elif code == self._IN_USE_BY_DATASET:
                        classes_to_clear.add(class_data)
                        classes_to_drop.add(class_data)
                    else:
                        _logger.warning('Unknown schema update dependency code: %s', code)
        return sorted(classes_to_clear), sorted(classes_to_drop)

    def _remove_schema_update_dependencies(self, classes_to_clear: set[tuple[str, str]], classes_to_drop: set[tuple[str, str]]) -> None:
        for class_name, dataset_slug in classes_to_clear:
            self.clear_class_from_dataset(dataset_slug, class_name)
        for class_name, dataset_slug in classes_to_drop:
            self.drop_class_from_dataset(dataset_slug, class_name)

    def get_schema_update_dependencies(self, schema: DatagraphsSchema) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
        """Get the schema update dependencies for a new schema.

        :param schema: The new schema to check.
        :returns: A tuple containing two sets:
            - Classes to clear (set of (class_name, dataset_slug))
            - Classes to drop (set of (class_name, dataset_slug))
        """
        dependencies = self.apply_schema(schema, mode=SCHEMA_APPLY_MODE.VALIDATE_ONLY)
        if dependencies:
            return self._identify_schema_update_dependencies(dependencies)
        return set(), set()

    def get_schema(self) -> DatagraphsSchema:
        """Retrieve the active schema for the project.

        :returns: The current project `Schema`.
        """
        url = f'{self._base_url}models/_active?t={self._cache_buster()}'
        response = self._read_json(HTTP.GET, url)
        if not isinstance(response, dict):
            raise DatagraphsError(f'Malformed schema response from {url}')
        return DatagraphsSchema.create_from(response)
        
    def get_datasets(self) -> list[Dataset]:
        """Retrieve all datasets in the project.

        :returns: A `Dataset` for every dataset in the project.
        :raises HttpStatusError: If any page request fails.
        :raises IncompleteReadError: If the pages do not add up to the reported total.
        """
        def page_url(next_page_token: str, page_no: int) -> str:
            params = [('pageSize', self._DEFAULT_DATASETS_PAGE_SIZE), ('t', self._cache_buster())]
            params.append(('nextPageToken', next_page_token) if next_page_token else ('pageNo', page_no))
            return f'{self._base_url}?{self._build_query(params)}'
        return [Dataset.create_from(item) for item in self._read_all_pages(page_url)]

    def get_dataset(self, dataset_slug: str) -> Dataset | None:
        """Retrieve a specific dataset by slug.

        :param dataset_slug: The slug of the dataset to retrieve.
        :returns: A `Dataset` object if found, otherwise ``None``.
        """
        datasets = self.get_datasets()
        return next((d for d in datasets if d.slug == dataset_slug), None)

    def apply_datasets(self, datasets: list[Dataset], timeout_s: float=_DATASETS_TIMEOUT_S) -> None:
        """Create or update datasets so they match the supplied list.

        New datasets are created; existing datasets with changes are updated.
        Waits for confirmation that all datasets have been applied.

        :param datasets: Datasets to apply.
        :param timeout_s: Maximum time in seconds to wait for the API to
            confirm all datasets are applied.
        :raises DatagraphsError: If datasets are not applied within the timeout.
        """
        _logger.info('Applying datasets update to project: %s', self.project_name)
        target_datasets = self.get_datasets()
        for dataset in datasets:
            match = next((d for d in target_datasets if d.slug == dataset.slug), None)
            try:
                if match is None:
                    self.create_dataset(dataset)
                elif match != dataset:
                    self.update_dataset(dataset)
            except DatagraphsError as e:
                # The write may still complete server-side, and the poll below
                # is what confirms it, so a timeout here is not yet a failure.
                if not self._is_timeout(e):
                    raise
                _logger.warning('Request to apply dataset %s timed out - confirming whether it was applied: %s', dataset.slug, e)
            time.sleep(self._wait_time_ms / 1000)
        self._assert_datasets_applied(datasets, timeout_s)

    def _assert_datasets_applied(self, datasets: list[Dataset], timeout_s: float) -> None:
        """Poll the API until the applied datasets match *datasets* or *timeout_s* elapses.

        Polling backs off on the schedule described by ``_POLL_SCHEDULE``: every
        1s for the first 10s, every 2s for the next 20s, then every 5s.

        :param datasets: The datasets expected to be applied.
        :param timeout_s: Maximum time in seconds to wait for confirmation.
        :raises DatagraphsError: If the datasets are not applied within the timeout.
        """
        _logger.info('Verifying all datasets have been applied successfully...')
        start = time.monotonic()
        while not self._datasets_match(self.get_datasets(), datasets):
            elapsed_s = time.monotonic() - start
            remaining_s = timeout_s - elapsed_s
            if remaining_s <= 0:
                _logger.error('Failed to apply datasets within timeout.')
                raise DatagraphsError('Failed to apply datasets within timeout.')
            _logger.info('Waiting for datasets to be applied...')
            # Never sleep past the deadline, so the timeout is honoured precisely.
            time.sleep(min(self._poll_interval(elapsed_s), remaining_s))
        _logger.info('All datasets have been applied successfully.')

    def _poll_interval(self, elapsed_s: float) -> float:
        """Select the poll interval for a given elapsed time, per ``_POLL_SCHEDULE``.

        :param elapsed_s: Seconds elapsed since polling began.
        :returns: The interval in seconds to wait before the next check.
        """
        for threshold_s, interval_s in self._POLL_SCHEDULE:
            if elapsed_s < threshold_s:
                return interval_s
        return self._POLL_SLOW_INTERVAL_S

    def _datasets_match(self, datasets_a: list[Dataset], datasets_b: list[Dataset]) -> bool:
        """Check if two lists of datasets match by slug and content.

        :param datasets_a: First list of datasets.
        :param datasets_b: Second list of datasets.
        :returns: ``True`` if the lists match, otherwise ``False``.
        """
        if len(datasets_a) != len(datasets_b):
            return False
        for dataset_a in datasets_a:
            match = next((d for d in datasets_b if d.slug == dataset_a.slug), None)
            if match is None or sorted(match.classes) != sorted(dataset_a.classes):
                return False
        return True

    def create_dataset(self, dataset: Dataset) -> None:
        """Create a new dataset.

        :param dataset: The dataset to create.
        """
        url = f'{self._base_url}datasets'
        self._send(HTTP.POST, url, json=dataset.to_dict())

    def update_dataset(self, dataset: Dataset) -> None:
        """Update an existing dataset.

        :param dataset: The dataset to update (matched by slug).
        """
        url = f'{self._base_url}datasets/{dataset.slug}'
        self._send(HTTP.PUT, url, json=dataset.to_dict())

    def clear_dataset(self, dataset_slug: str) -> None:
        """Delete all data from a dataset, keeping the dataset itself intact.

        :param dataset_slug: The slug of the dataset to clear.
        """
        self.clear_class_from_dataset(dataset_slug, self._ALL_TYPES_FILTER)

    def clear_class_from_dataset(self, dataset_slug: str, class_name: str) -> None:
        """Delete all data of a specific class from a dataset, keeping the dataset itself intact.

        :param dataset_slug: The slug of the dataset to clear.
        :param class_name: The name of the class to clear from the dataset.
        """
        _logger.info('Clearing down data of type %s from dataset: %s', class_name, dataset_slug)
        filter = class_name if class_name == self._ALL_TYPES_FILTER else f'type:{class_name}'
        url = f'{self._base_url}{dataset_slug}?{self._build_query([("filter", filter)])}'
        self._send(HTTP.DELETE, url)

    def drop_dataset(self, dataset_slug: str) -> None:
        """Drop a dataset and all its data entirely.

        :param dataset_slug: The slug of the dataset to drop.
        """
        _logger.info('Dropping dataset: %s', dataset_slug)
        url = f'{self._base_url}datasets/{dataset_slug}'
        self._send(HTTP.DELETE, url)

    def drop_class_from_dataset(self, dataset_slug: str, class_name: str) -> None:
        """Remove the specified class from a dataset, deleting all data of that class.

        :param dataset_slug: The slug of the dataset to update.
        :param class_name: The name of the class to drop from the dataset.
        """
        dataset = self.get_dataset(dataset_slug)
        if dataset is not None:
            if class_name in dataset.classes:
                _logger.info('Dropping class %s from dataset: %s', class_name, dataset_slug)
                dataset.classes.remove(class_name)
                self.update_dataset(dataset)
            else:
                _logger.warning('Class %s not found in dataset: %s', class_name, dataset_slug)
        else:
            raise DatagraphsError(f'Dataset {dataset_slug} not found in project {self.project_name}')

    def tear_down(self, drop_datasets: bool = True) -> None:
        """Remove all datasets and their data from the project.

        :param drop_datasets: If ``True``, drops each dataset entirely.
            If ``False``, only clears the data from each dataset.
        """
        datasets = self.get_datasets()
        for dataset in datasets:
            if drop_datasets:
                self.drop_dataset(dataset.slug)
            else:
                self.clear_dataset(dataset.slug)
            time.sleep(self._wait_time_ms / 1000)
