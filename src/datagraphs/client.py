"""DataGraphs API client for interacting with the DataGraphs service."""

import time
import logging
import requests
import json
import urllib.parse
from datetime import datetime, timezone
from enum import Enum
from typing import Optional, Dict, List, Any, Union
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.dataset import Dataset

logger = logging.getLogger(__name__)

class HTTP(Enum):
    GET = 'get'
    PUT = 'put'
    POST = 'post'
    DELETE = 'delete'
    
    def __str__(self) -> str:
        return str(self.value)

class DatagraphsError(Exception):
    """Base exception for DataGraphs client errors."""
    pass

class AuthenticationError(DatagraphsError):
    """Raised when authentication or authorisation fails."""
    pass

class Client:
    """Low-level HTTP client for the DataGraphs REST API.

    Handles authentication, pagination, and batched writes.
    """

    PROD_URL = "https://api.datagraphs.io/"
    AUTH_URL_SUFFIX = "oauth/token"

    DEFAULT_BATCH_SIZE = 100
    DEFAULT_WAIT_TIME_MS = 200
    DEFAULT_FACET_SIZE = 10
    MAX_AUTH_RETRIES = 2
    DATASETS_TIMEOUT_MS = 30000
    
    # HTTP status codes
    HTTP_OK = 200
    HTTP_CREATED = 201
    HTTP_NO_CONTENT = 204
    HTTP_UNAUTHORIZED = 401
    HTTP_FORBIDDEN = 403
    HTTP_GATEWAY_TIMEOUT = 504
    DEFAULT_DATASETS_PAGE_SIZE = 1000

    def __init__(
        self, 
        project_name: str, 
        api_key: str, 
        client_id: str = "", 
        client_secret: str = "", 
        batch_size: int = DEFAULT_BATCH_SIZE, 
        service_url: str = PROD_URL
    ) -> None:
        """Initialise the DataGraphs client.

        :param project_name: Name of the project.
        :param api_key: API key for authentication.
        :param client_id: OAuth client ID (required for write operations).
        :param client_secret: OAuth client secret (required for write operations).
        :param batch_size: Number of items to process in each batch.
        :param service_url: Base URL for the API service.
        """
        self.project_name = project_name
        self._api_key = api_key
        self._client_id = client_id
        self._client_secret = client_secret
        self._batch_size = batch_size
        self._auth_token = ''
        self._service_url = service_url if service_url.endswith('/') else f'{service_url}/'
        self._http_client = requests
        self._wait_time_ms = self.DEFAULT_WAIT_TIME_MS

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
                    f'{self._service_url}{self.AUTH_URL_SUFFIX}', 
                    headers=headers, 
                    data=json.dumps(body)
                )
                response.raise_for_status()
                data = response.json()
                self._auth_token = f"{data['token_type']} {data['access_token']}"
            except requests.exceptions.RequestException as e:
                raise AuthenticationError(f"Failed to obtain auth token: {e}")
        return self._auth_token

    def _request(self, method: HTTP, url: str, _retry_count: int = 0, **kwargs) -> Optional[Dict[str, Any]]:
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
            if response.status_code in [self.HTTP_OK, self.HTTP_CREATED, self.HTTP_NO_CONTENT]:
                if method == HTTP.GET:
                    return response.json()
                return None
            elif response.status_code == self.HTTP_GATEWAY_TIMEOUT:
                logger.warning("%s - %s: continuing processing, but try a smaller batch size...", response.reason, response.text)
                return {}
            elif response.status_code in [self.HTTP_UNAUTHORIZED, self.HTTP_FORBIDDEN]:
                if _retry_count < self.MAX_AUTH_RETRIES:
                    if 'headers' in kwargs and 'Authorization' in kwargs['headers']:
                        kwargs['headers']['Authorization'] = self._get_auth_token(force_refresh=True)
                    return self._request(method, url, _retry_count=_retry_count + 1, **kwargs)
                else:
                    raise AuthenticationError(f'Authentication failed after {self.MAX_AUTH_RETRIES+1} attempts')
            else:
                raise DatagraphsError(f"Request failed with status {response.status_code}: {response.text}")
        except requests.exceptions.RequestException as e:
            raise DatagraphsError(f"Request failed: {e}")

    def _has_oauth_credentials(self) -> bool:
        return bool(self._client_id and self._client_secret)

    def _get_headers(self, lang: str = 'all') -> Dict[str, str]: 
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
        return str(datetime.now(tz=timezone.utc).timestamp())

    def status(self) -> str:
        """Check the API service status.

        :returns: The API status string, or ``'unknown'`` if unavailable.
        """
        url = f'{self._service_url}status?t={self._cache_buster()}'
        response = self._request(HTTP.GET, url, headers=self._get_headers())
        return response.get('api', 'unknown')

    def get(self, class_name: str, lang: str = 'all', include_date_fields: bool = False) -> List[Dict[str, Any]]:
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
            effective_facet_size = facet_size if facet_size > -1 else self.DEFAULT_FACET_SIZE
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
        ) -> Union[List[Dict[str, Any]], tuple[List[Dict[str, Any]], List[Dict[str, Any]]]]:
        """Query the API with filters, facets, sorting, and pagination.

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

    def put(self, dataset: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> int:
        """Load entities into a dataset.

        Automatically batches large payloads according to the configured
        ``batch_size``.

        :param dataset: Target dataset slug.
        :param data: A single entity dict or a list of entity dicts.
        :returns: The number of entities loaded.
        """
        entities = [data] if isinstance(data, dict) else data
        length = len(entities)
        logger.info('Loading %d entities into dataset %s in repo: %s', length, dataset, self.project_name)
        if length > self._batch_size:
            for i in range(0, length, self._batch_size):
                batch = entities[i:i + self._batch_size]
                end = min(i + self._batch_size, length)
                logger.info('   Loading batch %d-%d of %d entities into dataset %s in repo: %s', i, end, length, dataset, self.project_name)
                self._request(HTTP.PUT, f'{self._base_url}{dataset}', json=batch, headers=self._get_headers())
        else:
            self._request(HTTP.PUT, f'{self._base_url}{dataset}', json=entities, headers=self._get_headers())
        return length

    def delete(self, class_name: str, entity_id: str) -> None:
        """Delete a single entity by class and ID.

        :param class_name: The entity class.
        :param entity_id: The entity identifier.
        """
        url = f'{self._base_url}{class_name}/{entity_id}'
        self._request(HTTP.DELETE, url, headers=self._get_headers())


    def apply_schema(self, schema: DatagraphsSchema) -> None:
        """Apply a schema to the project, replacing the currently active domain model.

        :param schema: The schema to apply.
        """
        logger.info('Applying schema to project: %s', self.project_name)
        url = f'{self._base_url}models/_active'
        self._request(HTTP.PUT, url, data=schema.to_json(), headers=self._get_headers())

    def get_schema(self) -> DatagraphsSchema:
        """Retrieve the active schema for the project.

        :returns: The current project `Schema`.
        """
        url = f'{self._base_url}models/_active?t={self._cache_buster()}'
        response = self._request(HTTP.GET, url, headers=self._get_headers())        
        return DatagraphsSchema.create_from(response)
        
    def get_datasets(self) -> List[Dataset]:
        """Retrieve all datasets in the project.

        :returns: A list of `Dataset` objects.
        """
        url = f'{self._base_url}?pageSize={self.DEFAULT_DATASETS_PAGE_SIZE}&t={self._cache_buster()}'
        resp = self._request(HTTP.GET, url, headers=self._get_headers())
        data = resp.get("results", []) if resp else []
        if len(data) >= self.DEFAULT_DATASETS_PAGE_SIZE:
            logger.warning('Dataset results (%d) may have been truncated at page size limit (%d)', len(data), self.DEFAULT_DATASETS_PAGE_SIZE)
        return [Dataset.create_from(item) for item in data]

    def apply_datasets(self, datasets: List[Dataset], timeout_ms: int=DATASETS_TIMEOUT_MS) -> None:
        """Create or update datasets so they match the supplied list.

        New datasets are created; existing datasets with changes are updated.
        Waits for confirmation that all datasets have been applied.

        :param datasets: Datasets to apply.
        :param timeout_ms: Maximum time in milliseconds to wait for the API to
            confirm all datasets are applied.
        :raises DatagraphsError: If datasets are not applied within the timeout.
        """
        logger.info('Applying datasets update to project: %s', self.project_name)
        target_datasets = self.get_datasets()
        for dataset in datasets:
            match = next((d for d in target_datasets if d.slug == dataset.slug), None)
            if match is None:
                self.create_dataset(dataset)
            elif match != dataset:
                self.update_dataset(dataset)
        self._assert_datasets_applied(datasets, timeout_ms)

    def _assert_datasets_applied(self, datasets: List[Dataset], timeout_ms: int) -> None:
        count = 1
        logger.info('Verifying all datasets have been applied successfully...')
        while len(self.get_datasets()) != len(datasets):
            if (count * self.wait_time_ms) < timeout_ms:
                logger.info('Waiting for datasets to be applied...')
                count += 1
                time.sleep(self.wait_time_ms / 1000)
            else:
                logger.error('Failed to apply datasets within timeout.')
                raise DatagraphsError('Failed to apply datasets within timeout.')
        logger.info('All datasets have been applied successfully.')
                
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
        logger.info('Clearing down data from dataset: %s', dataset_slug)
        url = f'{self._base_url}{dataset_slug}?filter=_all'

        print(url)

        self._request(HTTP.DELETE, url, headers=self._get_headers())

    def drop_dataset(self, dataset_slug: str) -> None:
        """Drop a dataset and all its data entirely.

        :param dataset_slug: The slug of the dataset to drop.
        """
        logger.info('Dropping dataset: %s', dataset_slug)
        url = f'{self._base_url}datasets/{dataset_slug}'
        self._request(HTTP.DELETE, url, headers=self._get_headers())

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
