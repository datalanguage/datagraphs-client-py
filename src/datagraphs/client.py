import requests
import json
import urllib.parse
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, List, Any, Union
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.dataset import Dataset
from datagraphs.utils import *

class HTTP(Enum):
    GET = 'get'
    PUT = 'put'
    POST = 'post'
    DELETE = 'delete'
    
    def __str__(self) -> str:
        return str(self.value)

class DatagraphsError(Exception):
    pass

class AuthenticationError(DatagraphsError):
    pass

class Client:

    PROD_URL = "https://api.datagraphs.io/"
    AUTH_URL_SUFFIX = "oauth/token"

    DEFAULT_BATCH_SIZE = 100
    DEFAULT_FACET_SIZE = 10
    MAX_AUTH_RETRIES = 2
    
    # HTTP status codes
    HTTP_OK = 200
    HTTP_CREATED = 201
    HTTP_NO_CONTENT = 204
    HTTP_UNAUTHORIZED = 401
    HTTP_FORBIDDEN = 403
    HTTP_GATEWAY_TIMEOUT = 504

    def __init__(
        self, 
        project_name: str, 
        api_key: str, 
        client_id: str = "", 
        client_secret: str = "", 
        batch_size: int = DEFAULT_BATCH_SIZE, 
        service_url: str = PROD_URL
    ) -> None:
        """
        Initialize the Datagraphs client.
        
        Args:
            project_name: Name of the project
            api_key: API key for authentication
            client_id: OAuth client ID (optional)
            client_secret: OAuth client secret (optional)
            batch_size: Number of items to process in each batch
            service_url: Base URL for the API service
        """
        self.project_name = project_name
        self.api_key = api_key
        self.client_id = client_id
        self.client_secret = client_secret
        self._batch_size = batch_size
        self._auth_token = ''
        self._retry_count = 0
        self._service_url = service_url if service_url.endswith('/') else f'{service_url}/'
        self._http_client = requests

    @property
    def _base_url(self) -> str:
        return f'{self._service_url}{self.project_name}/'

    def _get_auth_token(self, force_refresh=False) -> str:
        if force_refresh or not self._auth_token:
            headers = {
                'Content-Type': 'application/json', 
                'Accept': 'application/json', 
                'x-api-key': self.api_key
            }
            body = { 
                'clientId': self.client_id, 
                'clientSecret': self.client_secret
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

    def _request(self, method: HTTP, url: str, **kwargs) -> Optional[Dict[str, Any]]:
        try:
            if 'headers' in kwargs and method in [HTTP.PUT, HTTP.POST]:
                kwargs['headers']['Content-Type'] = 'application/json'            
            response = self._http_client.request(str(method), url, **kwargs)
            if response.status_code in [self.HTTP_OK, self.HTTP_CREATED, self.HTTP_NO_CONTENT]:
                self._retry_count = 0
                if method == HTTP.GET:
                    return response.json()
                return None
            elif response.status_code == self.HTTP_GATEWAY_TIMEOUT:
                print(f">>> {response.reason} - {response.text}: continuing processing, but try a smaller batch size...")
                return {}
            elif response.status_code in [self.HTTP_UNAUTHORIZED, self.HTTP_FORBIDDEN]:
                if self._retry_count < self.MAX_AUTH_RETRIES:
                    self._retry_count += 1
                    if 'headers' in kwargs and 'Authorization' in kwargs['headers']:
                        kwargs['headers']['Authorization'] = self._get_auth_token(force_refresh=True)
                    return self._request(method, url, **kwargs)
                else:
                    raise AuthenticationError(f'Authentication failed after {self.MAX_AUTH_RETRIES+1} attempts')
            else:
                raise DatagraphsError(f"Request failed with status {response.status_code}: {response.text}")
        except requests.exceptions.RequestException as e:
            raise DatagraphsError(f"Request failed: {e}")

    def _has_oauth_credentials(self) -> bool:
        return bool(self.client_id and self.client_secret)

    def _get_headers(self, lang: str = 'all') -> Dict[str, str]: 
        headers = {
            'Accept': 'application/json',
            'x-api-key': self.api_key,
            'Accept-Language': lang
        }
        if self._has_oauth_credentials():
            headers['Authorization'] = self._get_auth_token()
        return headers

    def _get_data_url(
        self, 
        type_name: str, 
        page_no: int, 
        page_size: int, 
        lang: str, 
        include_date_fields: bool
    ) -> str:
        url = (
            f'{self._base_url}_all?filter=type:{type_name}&lang={lang}'
            f'&pageNo={page_no}&pageSize={page_size}&{self._cache_buster()}'
        )
        if include_date_fields:
            url += "&includeDateFields=true"
        return url

    def _cache_buster(self) -> str:
        return f't={datetime.timestamp(datetime.now())}'

    def status(self) -> str:
        url = f'{self._service_url}status?{self._cache_buster()}'
        try:
            response = self._request(HTTP.GET, url, headers=self._get_headers())
            return response.get('api', 'unknown')
        except DatagraphsError as e:
            raise DatagraphsError(f"Failed to fetch status: {e}")

    def get(self, type_name: str, lang: str = 'all', include_date_fields: bool = False) -> List[Dict[str, Any]]:
        page_no = 1
        resp = self._request(
            HTTP.GET, 
            self._get_data_url(type_name, page_no, self._batch_size, lang, include_date_fields), 
            headers=self._get_headers(lang)
        )
        if 'search' in resp:
            total_results = resp['search']['totalResults']
            data = resp['results'] if total_results > 0 else []
            while page_no * self._batch_size < total_results:
                page_no += 1
                resp = self._request(
                    HTTP.GET, 
                    self._get_data_url(type_name, page_no, self._batch_size, lang, include_date_fields), 
                    headers=self._get_headers(lang)
                )
                if 'results' in resp:
                    data.extend(resp['results'])
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
        url = f'{self._base_url}{dataset}?lang={lang}&{self._cache_buster()}'        
        if q:
            url += f'&q={urllib.parse.quote_plus(q)}'
        if filters:
            url += f'&filter={filters}'
        if facets:
            effective_facet_size = facet_size if facet_size > -1 else self.DEFAULT_FACET_SIZE
            url += f'&facets={facets}&facetSize={effective_facet_size}'
        if date_facets:
            url += f'&dateFacets={date_facets}'
        if fields:
            url += f'&fields={fields}'
        if embed:
            url += f'&embed={embed}'
        if sort:
            url += f'&sort={sort}'
        if ids:
            url += f'&ids={ids}'
        if page_no > -1:
            url += f'&pageNo={page_no}'
        if page_size > -1:
            url += f'&pageSize={page_size}'
        if previous_page_token:
            url += f'&previousPageToken={previous_page_token}'
        if next_page_token:
            url += f'&nextPageToken={next_page_token}'
        if include_date_fields:
            url += "&includeDateFields=true"
        return url

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
            page_no: int = 0,
            page_size: int = 0,
            previous_page_token: str = '',
            next_page_token: str = '',
            include_date_fields: bool = False
        ) -> List[Dict[str, Any]]:
        """
        Query the API with various filters and options.
        
        Args:
            dataset: Dataset to query
            q: Search query string
            filters: Filter string
            facets: Facets to include
            facet_size: Number of facet values to return
            date_facets: Date facets to include
            fields: Fields to return
            embed: Related entities to embed
            sort: Sort order
            ids: Specific IDs to fetch
            lang: Language code
            page_no: Page number (0-indexed)
            page_size: Number of results per page
            previous_page_token: Token for previous page
            next_page_token: Token for next page
            include_date_fields: Whether to include date fields
            
        Returns:
            List of results
        """
        if page_size == 0:
            page_size = self._batch_size
            
        url = self._get_query_url(
            dataset, q, filters, facets, facet_size, date_facets, fields, embed, sort, ids, 
            lang, page_no, page_size, previous_page_token, next_page_token, include_date_fields
        )
        resp = self._request(HTTP.GET, url, headers=self._get_headers(lang))
        
        if resp and 'search' in resp:
            total_results = resp['search']['totalResults']
            return resp['results'] if total_results > 0 else []
        return []

    def put(self, dataset: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
        entities = [data] if isinstance(data, dict) else data
        length = len(entities)
        print(f'Loading {length} entities into dataset {dataset} in repo: {self.project_name}')
        if length > self._batch_size:
            for i in range(0, length, self._batch_size):
                batch = entities[i:i + self._batch_size]
                end = min(i + self._batch_size, length)
                print(f'   Loading batch {i}-{end} of {length} entities into dataset {dataset} in repo: {self.project_name}')
                self._request(HTTP.PUT, f'{self._base_url}{dataset}', json=batch, headers=self._get_headers())
        else:
            self._request(HTTP.PUT, f'{self._base_url}{dataset}', json=entities, headers=self._get_headers())

    def delete(self, type_name: str, id: str) -> None:
        url = f'{self._base_url}{type_name}/{id}'
        self._request(HTTP.DELETE, url, headers=self._get_headers())


    def apply_schema(self, schema: DatagraphsSchema) -> None:
        url = f'{self._base_url}models/_active'
        self._request(HTTP.PUT, url, data=schema.to_json(), headers=self._get_headers())

    def get_schema(self) -> DatagraphsSchema:
        url = f'{self._base_url}models/_active?{self._cache_buster()}'
        response = self._request(HTTP.GET, url, headers=self._get_headers())        
        return DatagraphsSchema(response)
        
    def get_datasets(self) -> List[Dataset]:
        datasets = []
        url = f'{self._base_url}?pageSize=1000&{self._cache_buster()}'
        resp = self._request(HTTP.GET, url, headers=self._get_headers())
        data = resp.get("results", []) if resp else []
        for item in data:
            datasets.append(Dataset.create_from(item))
        return datasets

    def apply_datasets(self, datasets: List[Dataset]) -> None:
        target_datasets = self.get_datasets()
        for dataset in datasets:
            slug = self.get_dataset_slug(dataset)
            match = next((d for d in target_datasets if self.get_dataset_slug(d) == slug), None)
            if match is None:
                self.create_dataset(dataset)
            else:
                self.update_dataset(dataset)

    def create_dataset(self, dataset: Dataset) -> None:
        url = f'{self._base_url}datasets'
        self._request(HTTP.POST, url, json=dataset.to_dict(), headers=self._get_headers())

    def update_dataset(self, dataset: Dataset) -> None:
        slug = self.get_dataset_slug(dataset)
        url = f'{self._base_url}datasets/{slug}'
        self._request(HTTP.PUT, url, json=dataset.to_dict(), headers=self._get_headers())

    def clear_dataset(self, slug: str) -> None:
        url = f'{self._base_url}{slug}?filter=_all'
        self._request(HTTP.DELETE, url, headers=self._get_headers())

    def get_dataset_slug(self, dataset: Dataset) -> str:
        return dataset.id[dataset.id.rfind(':') + 1:]

    def tear_down(self) -> None:
        datasets = self.get_datasets()
        for dataset in datasets:
            slug = self.get_dataset_slug(dataset)
            url = f'{self._base_url}datasets/{slug}'
            self._request(HTTP.DELETE, url, headers=self._get_headers())
