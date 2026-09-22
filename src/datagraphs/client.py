"""DataGraphs API client for interacting with the DataGraphs service."""

import json
import logging
import time
import urllib.parse
from datetime import UTC, datetime
from typing import Any

import requests

from datagraphs.dataset import Dataset
from datagraphs.enums import HTTP, SCHEMA_APPLY_MODE
from datagraphs.schema import Schema as DatagraphsSchema

_logger = logging.getLogger(__name__)

class DatagraphsError(Exception):
    """Base exception for DataGraphs client errors."""

class AuthenticationError(DatagraphsError):
    """Raised when authentication or authorisation fails."""

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

    Handles authentication, pagination, and batched writes.
    """

    _PROD_URL = "https://api.datagraphs.io/"
    _AUTH_URL_SUFFIX = "oauth/token"

    _ALL_TYPES_FILTER = "_all"

    DEFAULT_BATCH_SIZE = 100
    DEFAULT_WAIT_TIME_MS = 200
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
    _HTTP_OK = 200
    _HTTP_CREATED = 201
    _HTTP_NO_CONTENT = 204
    _HTTP_BAD_REQUEST = 400
    _HTTP_UNAUTHORIZED = 401
    _HTTP_FORBIDDEN = 403
    _HTTP_NOT_FOUND = 404
    _HTTP_GATEWAY_TIMEOUT = 504
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
        service_url: str = _PROD_URL
    ) -> None:
        """Initialise the DataGraphs client.

        :param project_name: Name of the project.
        :param api_key: API key for authentication.
        :param client_id: OAuth client ID (required for write operations).
        :param client_secret: OAuth client secret (required for write operations).
        :param batch_size: Number of items to process in each batch.
        :param service_url: Base URL for the API service.
        """
        if project_name is None or len(project_name) == 0:
            raise ValueError("project_name is required")
        if api_key is None or len(api_key) == 0:
            raise ValueError("api_key is required")
        self.project_name = self._sanitise_text(project_name)
        self._api_key = self._sanitise_text(api_key)
        self._client_id = self._sanitise_text(client_id)
        self._client_secret = self._sanitise_text(client_secret)
        self._batch_size = batch_size
        service_url = self._sanitise_text(service_url)
        self._service_url = service_url if service_url.endswith('/') else f'{service_url}/'
        self._http_client = requests
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
                    data=json.dumps(body)
                )
                response.raise_for_status()
                data = response.json()
                self._auth_token = f"{data['token_type']} {data['access_token']}"
            except requests.exceptions.RequestException as e:
                raise AuthenticationError(f"Failed to obtain auth token: {e}")
        return self._auth_token

    def _request(self, method: HTTP, url: str, _retry_count: int = 0, **kwargs) -> dict[str, Any] | None:
        """Execute an HTTP request with automatic auth retry.

        :param method: The HTTP method to use.
        :param url: The request URL.
        :param _retry_count: Internal retry counter (do not set externally).
        :param kwargs: Additional arguments forwarded to the HTTP client.
        :returns: Parsed JSON response for GET requests, or ``None`` for
            mutating requests.
        :raises AuthenticationError: If authentication fails after max retries.
        :raises DatagraphsError: If the request fails for any other reason.
        """
        try:
            if 'headers' in kwargs and method in [HTTP.PUT, HTTP.POST]:
                kwargs['headers']['Content-Type'] = 'application/json'            
            response = self._http_client.request(str(method), url, **kwargs)
            if response.status_code in [self._HTTP_OK, self._HTTP_CREATED, self._HTTP_NO_CONTENT, self._HTTP_BAD_REQUEST]:
                if response.status_code == self._HTTP_BAD_REQUEST:
                    body = response.json() if response.content else {}
                    if 'message' in body and 'errors' not in body:
                        return {'errors': [body['message']]}
                    else:
                        return body
                elif method == HTTP.GET:
                    return response.json()
                else:
                    return {}
            elif response.status_code == self._HTTP_GATEWAY_TIMEOUT:
                _logger.warning("%s - %s: continuing processing, but try a smaller batch size...", response.reason, response.text)
                return {}
            elif response.status_code in [self._HTTP_UNAUTHORIZED, self._HTTP_FORBIDDEN]:
                if _retry_count < self._MAX_AUTH_RETRIES:
                    if 'headers' in kwargs and 'Authorization' in kwargs['headers']:
                        kwargs['headers']['Authorization'] = self._get_auth_token(force_refresh=True)
                    return self._request(method, url, _retry_count=_retry_count + 1, **kwargs)
                else:
                    raise AuthenticationError(f'Authentication failed after {self._MAX_AUTH_RETRIES+1} attempts')
            else:
                raise DatagraphsError(f"Request failed with status {response.status_code}: {response.text}")
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

    @staticmethod
    def _build_query(params: list[tuple[str, str]]) -> str:
        """Build a query string from key-value pairs."""
        return '&'.join(f'{k}={v}' for k, v in params)

    def _get_data_url(
        self, 
        class_name: str, 
        page_size: int, 
        lang: str, 
        include_date_fields: bool,
        next_page_token: str = '', 
        page_no: int = -1
    ) -> str:
        params = [
            ('filter', f'type:{class_name}'),
            ('lang', lang),
            ('pageSize', page_size),
            ('t', self._cache_buster()),
        ]
        if len(next_page_token) > 0:
            params.append(('nextPageToken', next_page_token))
        elif page_no > 0:
            params.append(('pageNo', page_no))
        if include_date_fields:
            params.append(('includeDateFields', 'true'))
        return f'{self._base_url}_all?{self._build_query(params)}'

    def _cache_buster(self) -> str:
        """Return a cache-busting timestamp value."""
        return str(datetime.now(tz=UTC).timestamp())

    def status(self) -> str:
        """Check the API service status.

        :returns: The API status string, or ``'unknown'`` if unavailable.
        """
        url = f'{self._service_url}status?t={self._cache_buster()}'
        response = self._request(HTTP.GET, url, headers=self._get_headers())
        return response.get('api', 'unknown')

    def get(self, class_name: str, lang: str = 'all', include_date_fields: bool = False) -> list[dict[str, Any]]:
        """Retrieve all entities of a given type.

        Automatically paginates through all results.

        :param class_name: The entity class to fetch.
        :param lang: Language code for results (default ``'all'``).
        :param include_date_fields: Whether to include system date metadata.
        :returns: A list of entity dicts.
        """
        page_no = 1
        resp = self._request(
            HTTP.GET, 
            self._get_data_url(class_name, page_no=page_no, page_size=self._batch_size, lang=lang, include_date_fields=include_date_fields), 
            headers=self._get_headers(lang)
        )
        if 'search' in resp:
            total_results = resp['search']['totalResults']
            data = resp['results'] if total_results > 0 else []
            while page_no * self._batch_size < total_results:
                page_no += 1
                if 'nextPageToken' in resp['search']:
                    next_page_token = resp['search']['nextPageToken']
                    url = self._get_data_url(class_name, next_page_token=next_page_token, page_size=self._batch_size, lang=lang, include_date_fields=include_date_fields)
                else:
                    url = self._get_data_url(class_name, page_no=page_no, page_size=self._batch_size, lang=lang, include_date_fields=include_date_fields)
                resp = self._request(HTTP.GET, url, headers=self._get_headers(lang))
                if 'results' in resp:
                    data.extend(resp['results'])
                time.sleep(self._wait_time_ms / 1000)
            return data
        elif 'errors' in resp:
            return resp
        else:
            return []

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
            params.append(('q', urllib.parse.quote_plus(q)))
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
        
        :param gql: a GQL query string - if this is specified then all other arguments are ignored.
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
            facets are requested.
        """
        if gql:
            return self._query_gql(gql, lang)
        else:
            return self._query(dataset, q, filters, facets, facet_size, date_facets, fields, embed, sort, ids, lang, page_no, page_size, previous_page_token, next_page_token, include_date_fields)

    def _query(self, 
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
        if page_size == -1:
            page_size = self._batch_size
        url = self._get_query_url(
            dataset, q, filters, facets, facet_size, date_facets, fields, embed, sort, ids,
            lang, page_no, page_size, previous_page_token, next_page_token, include_date_fields
        )
        resp = self._request(HTTP.GET, url, headers=self._get_headers(lang))
        if resp and 'search' in resp:
            total_results = resp['search']['totalResults']
            results = resp['results'] if total_results > 0 else []
            if 'facets' in resp:
                return results, resp['facets']
            else:
                return results
        elif len(ids) > 0 and isinstance(resp, list):
            return resp
        else:
            return []

    def _query_gql(self, gql:str, lang: str) -> list[dict[str, Any]]:
        url = f'{self._base_url}_cypher'
        headers = self._get_headers(lang)
        headers['Content-Type'] = 'application/json'
        resp = self._request(HTTP.POST, url, json={'query': gql}, headers=headers)
        if resp and 'data' in resp: 
            return resp['data']
        else:
            return []

    def put(self, dataset: str, data: dict[str, Any] | list[dict[str, Any]]) -> int:
        """Load entities into a dataset.

        Automatically batches large payloads according to the configured
        ``batch_size``.

        :param dataset: Target dataset slug.
        :param data: A single entity dict or a list of entity dicts.
        :returns: The number of entities loaded.
        """
        entities = [data] if isinstance(data, dict) else data
        length = len(entities)
        _logger.info('Loading %d entities into dataset %s in repo: %s', length, dataset, self.project_name)
        if length > self._batch_size:
            for i in range(0, length, self._batch_size):
                batch = entities[i:i + self._batch_size]
                end = min(i + self._batch_size, length)
                _logger.info('   Loading batch %d-%d of %d entities into dataset %s in repo: %s', i, end, length, dataset, self.project_name)
                try:
                    resp = self._request(HTTP.PUT, f'{self._base_url}{dataset}', json=batch, headers=self._get_headers())
                    if resp and 'errors' in resp:
                        return resp
                except Exception as e:  # noqa: BLE001 - intentional: log the failed batch and continue loading the rest
                    _logger.error('Error loading batch %d-%d of %d entities into dataset %s: %s', i, end, length, dataset, str(e))
        else:
            resp = self._request(HTTP.PUT, f'{self._base_url}{dataset}', json=entities, headers=self._get_headers())
            if resp and 'errors' in resp:
                return resp


    def delete(self, class_name: str, entity_id: str) -> None:
        """Delete a single entity by class and ID.

        :param class_name: The entity class.
        :param entity_id: The entity identifier.
        """
        url = f'{self._base_url}{class_name}/{entity_id}'
        resp = self._request(HTTP.DELETE, url, headers=self._get_headers())
        if resp and 'errors' in resp:
            return resp

    def apply_schema(self, schema: DatagraphsSchema, mode: SCHEMA_APPLY_MODE = SCHEMA_APPLY_MODE.APPLY) -> None | list[dict[str, Any]]:
        """Apply a schema to the project, replacing the currently active domain model.

        :param schema: The schema to apply.
        """
        _logger.info('Applying schema to project: %s', self.project_name)
        url = f'{self._base_url}models/_active'+('?dryRun=true' if mode == SCHEMA_APPLY_MODE.VALIDATE_ONLY else '')
        resp = self._request(HTTP.PUT, url, data=schema.to_json(), headers=self._get_headers())
        errors = resp.get("errors", [])

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
        response = self._request(HTTP.GET, url, headers=self._get_headers())        
        return DatagraphsSchema.create_from(response)
        
    def get_datasets(self) -> list[Dataset]:
        """Retrieve all datasets in the project.

        :returns: A list of `Dataset` objects.
        """
        url = f'{self._base_url}?pageSize={self._DEFAULT_DATASETS_PAGE_SIZE}&t={self._cache_buster()}'
        resp = self._request(HTTP.GET, url, headers=self._get_headers())
        data = resp.get("results", []) if resp else []
        if len(data) >= self._DEFAULT_DATASETS_PAGE_SIZE:
            _logger.warning('Dataset results (%d) may have been truncated at page size limit (%d)', len(data), self._DEFAULT_DATASETS_PAGE_SIZE)
        return [Dataset.create_from(item) for item in data]

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
            if match is None:
                self.create_dataset(dataset)
            elif match != dataset:
                self.update_dataset(dataset)
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
        self._request(HTTP.POST, url, json=dataset.to_dict(), headers=self._get_headers())

    def update_dataset(self, dataset: Dataset) -> None:
        """Update an existing dataset.

        :param dataset: The dataset to update (matched by slug).
        """
        url = f'{self._base_url}datasets/{dataset.slug}'
        self._request(HTTP.PUT, url, json=dataset.to_dict(), headers=self._get_headers())

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
        url = f'{self._base_url}{dataset_slug}?filter={class_name}'
        self._request(HTTP.DELETE, url, headers=self._get_headers())

    def drop_dataset(self, dataset_slug: str) -> None:
        """Drop a dataset and all its data entirely.

        :param dataset_slug: The slug of the dataset to drop.
        """
        _logger.info('Dropping dataset: %s', dataset_slug)
        url = f'{self._base_url}datasets/{dataset_slug}'
        self._request(HTTP.DELETE, url, headers=self._get_headers())

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
