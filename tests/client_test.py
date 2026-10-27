import json
import logging
import math
import random
import urllib.parse
from typing import ClassVar

import pytest
import requests

from datagraphs.client import (
    AuthenticationError,
    DatagraphsError,
    FailedWrite,
    HttpStatusError,
    IncompleteReadError,
    QueryPage,
    TimedOutWrite,
    TokenRequestError,
)
from datagraphs.client import Client as DatagraphsClient
from datagraphs.dataset import Dataset
from datagraphs.enums import ISOLATION_MODE, SCHEMA_APPLY_MODE
from datagraphs.schema import Schema as DatagraphsSchema

TOKEN_TYPE = 'Bearer'
ACCESS_TOKEN = 'test_token'

# Fixtures for common test setup
@pytest.fixture(scope="function", autouse=True)
def sleep(mocker):
    """Replace the client's sleeps (between pages and before retries) so tests never wait."""
    return mocker.patch('datagraphs.client.time.sleep')

@pytest.fixture(scope="function")
def mock_http_client(mocker):
    """Create a mock HTTP client."""
    return mocker.MagicMock()

@pytest.fixture(scope="function") 
def get_client(mock_http_client, mocker):
    def create_client(client_id='', client_secret='', batch_size: int = -1, **options):
        batch_size = DatagraphsClient.DEFAULT_BATCH_SIZE if batch_size == -1 else batch_size
        if client_id and client_secret:
            client = DatagraphsClient("test_project", "test_api_key", client_id, client_secret, batch_size, **options)
            client._http_client = mock_http_client
            data = {'token_type': TOKEN_TYPE, 'access_token': ACCESS_TOKEN}
            client._http_client.post.return_value = create_response_mock(mocker, 200, data)
        else:
            client = DatagraphsClient("test_project", "test_api_key", batch_size=batch_size, **options)
            client._http_client = mock_http_client
        return client
    return create_client  

def create_response_mock(mocker, status_code: int = 200, data: dict | list | None = None, reason: str = '', text: str = '', headers: dict | None = None):
    response_mock = mocker.MagicMock()
    response_mock.status_code = status_code
    response_mock.json.return_value = data if data is not None else {}
    response_mock.content = json.dumps(data).encode('utf-8') if data is not None else b''
    response_mock.raise_for_status.return_value = None
    response_mock.reason = reason
    response_mock.text = text
    response_mock.headers = headers or {}
    return response_mock

def search_page(results: list, total: int, token: str = '') -> dict:
    """A search response body: one page of *results* out of *total*."""
    body = {'search': {'totalResults': total}, 'results': results}
    if token:
        body['search']['nextPageToken'] = token
    return body

def paged_responses(mocker, entities: list, page_size: int, tokens: bool) -> list:
    """The pages the API serves for *entities*, *page_size* at a time.

    With *tokens*, every page carries a ``nextPageToken`` - the last page too,
    as the real API does - so the total, not the token, marks the end.
    """
    chunks = [entities[i:i + page_size] for i in range(0, len(entities), page_size)]
    return [create_response_mock(mocker, 200, search_page(chunk, len(entities), f'token-{n + 1}' if tokens else ''))
            for n, chunk in enumerate(chunks)]

def query_params(url: str) -> dict[str, str]:
    """The query parameters of *url*, one value each."""
    return {k: v[0] for k, v in urllib.parse.parse_qs(urllib.parse.urlsplit(url).query).items()}

def requested_urls(client) -> list[str]:
    return [c.args[1] for c in client._http_client.request.call_args_list]

def get_search_response(data: list, token: str = '') -> dict:
    resp = {
        'search': { 
            'totalResults': len(data)
        },
        'results': data
    }
    if len(token) > 0:
        resp['search']['nextPageToken'] = token
    return resp

def get_faceted_search_response(data: list) -> dict:
    resp = get_search_response(data)
    resp['facets'] = [{
        'buckets': [
            { 'key': 'condition1', 'count': 5},
            {'key': 'condition2', 'count': 3}
        ]
    }]
    return resp

def generate_test_data_list(num_entities: int) -> list:
    """Generate a list of test entity dictionaries."""
    return [{'id': f'entity-{i}', 'payload': 'data'} for i in range(num_entities)]

# OAuth Authentication Tests
class TestAuthentication:
    def test_should_get_auth_token_for_fetching_data_if_oauth_creds_are_supplied(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        client.get('Test')
        args, kwargs = client._http_client.post.call_args
        assert args[0] == "https://api.datagraphs.io/oauth/token"
        assert kwargs['headers']['x-api-key'] == "test_api_key"
        assert kwargs['data'] == '{"clientId": "test_client_id", "clientSecret": "test_client_secret"}'

    def test_should_inject_auth_token_into_subsequent_requests(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        client.get('Test')
        kwargs = client._http_client.request.call_args.kwargs
        assert kwargs['headers']['Authorization'] == f"{TOKEN_TYPE} {ACCESS_TOKEN}"

    def test_should_cache_auth_token_for_multiple_requests(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        client.get('Test')
        client.get('Test')
        client.get('Test')
        # post should only be called once for auth token
        assert client._http_client.post.call_count == 1

    def test_should_raise_authentication_error_after_max_retries(self, get_client, mocker):
        client = get_client()
        client._http_client.request.return_value = create_response_mock(mocker, 401)
        with pytest.raises(AuthenticationError, match='Authentication failed after 3 attempts'):
            client.get('Test')
        assert client._http_client.request.call_count == 3

    def test_should_refresh_auth_token_on_401_response(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 401)
        with pytest.raises(AuthenticationError, match='Authentication failed after 3 attempts'):
            client.get('Test')
        assert client._http_client.post.call_count == 3

    def test_should_clear_auth_token_before_retry_on_403(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 403)
        with pytest.raises(AuthenticationError, match='Authentication failed after 3 attempts'):
            client.get('Test')
        assert client._http_client.post.call_count == 3

    def test_should_not_get_auth_token_for_fetching_data_if_oauth_creds_are_not_supplied(self, get_client, mocker):
        client = get_client()
        client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        client.get('Test')
        assert client._http_client.post.call_args is None

# Data Retrieval Tests
class TestDataRetrieval:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client()

    def test_should_check_service_status(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, {'api': 'OK'})
        status = self.client.status()
        assert status == 'OK'

    def test_should_set_custom_wait_time(self):
        self.client.set_wait_time(wait_time_ms=500)
        assert self.client.wait_time_ms == 500

    def test_should_get_data_for_specified_class_name_from_correct_endpoint(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get('Test')
        args = self.client._http_client.request.call_args.args
        assert args[1].startswith("https://api.datagraphs.io/test_project/_all?")
        assert query_params(args[1])['filter'] == 'type:Test'

    def test_should_get_data_for_specified_type_in_all_languages_by_default(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get('Test')        
        args, kwargs = self.client._http_client.request.call_args
        assert query_params(args[1])['lang'] == 'all'
        assert kwargs['headers']['Accept-Language'] == 'all'

    def test_should_get_data_for_specified_type_in_specified_language(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get(class_name='Test', lang='fr')
        args, kwargs = self.client._http_client.request.call_args
        assert query_params(args[1])['lang'] == 'fr'
        assert kwargs['headers']['Accept-Language'] == 'fr'

    def test_should_use_cache_busting_url_suffix_to_get_data(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get('Test')
        args = self.client._http_client.request.call_args.args
        assert '&t=' in args[1]

    def test_should_request_system_metadata_dates_if_specified(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get(class_name='Test', include_date_fields=True)
        args = self.client._http_client.request.call_args.args
        assert '&includeDateFields=true' in args[1]

    def test_should_not_request_system_metadata_dates_if_unspecified(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get(class_name='Test')
        args = self.client._http_client.request.call_args.args
        assert '&includeDateFields=true' not in args[1]

# Pagination Tests
class TestPagination:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2)

    def test_should_get_data_at_specified_page_number(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get('Test')
        args = self.client._http_client.request.call_args.args
        assert '&pageNo=1' in args[1]

    def test_should_get_data_with_specified_page_size(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.get('Test')
        args = self.client._http_client.request.call_args.args
        assert '&pageSize=2' in args[1]

    def test_should_batch_get_requests_if_initial_result_count_is_greater_than_batch_size(self, mocker):
        entities = generate_test_data_list(3)
        self.client._http_client.request.side_effect = paged_responses(mocker, entities, 2, tokens=False)
        assert self.client.get('Test') == entities
        params = [query_params(url) for url in requested_urls(self.client)]
        assert [(p.get('pageNo'), p['pageSize'], p.get('nextPageToken')) for p in params] == [('1', '2', None), ('2', '2', None)]

    def test_should_batch_get_requests_by_page_token_if_present(self, mocker):
        entities = generate_test_data_list(3)
        self.client._http_client.request.side_effect = paged_responses(mocker, entities, 2, tokens=True)
        assert self.client.get('Test') == entities
        params = [query_params(url) for url in requested_urls(self.client)]
        assert [(p.get('pageNo'), p['pageSize'], p.get('nextPageToken')) for p in params] == [('1', '2', None), (None, '2', 'token-1')]



# Error Handling Tests
class TestErrorHandling:
    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client()

    def test_should_raise_rather_than_return_nothing_on_gateway_timeout(self, mocker):
        self.client = DatagraphsClient("test_project", "test_api_key", max_read_retries=0)
        self.client._http_client = mocker.MagicMock()
        self.client._http_client.request.return_value = create_response_mock(mocker, 504, reason='Gateway Timeout', text='timeout occurred')
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.get('Test')
        assert exc_info.value.status_code == 504
        assert exc_info.value.errors == ['timeout occurred']

    def test_should_raise_error_for_unexpected_status_codes(self, mocker):
        response = create_response_mock(mocker, 500, reason='Internal Server Error', text='Something went wrong')
        self.client._http_client.request.return_value = response        
        with pytest.raises(DatagraphsError, match='Request failed with status 500'):
            self.client.get('Test')

    def test_should_raise_errors_for_bad_get_requests(self, mocker):
        error_response = { "errors":[
            "Invalid input data"
        ]}
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, data=error_response)
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.get('Test')
        assert exc_info.value.status_code == 400
        assert exc_info.value.errors == error_response['errors']

    def test_should_report_errors_for_bad_put_requests(self, mocker):
        error_response = { "errors":[
            "Invalid input data"
        ]}
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, data=error_response)
        res = self.client.put('Test', data={'id': 'entity-1', 'key': 'value'})
        assert res.failed == [FailedWrite(0, 'entity-1', 400, error_response['errors'])]
        assert res.loaded == 0
        assert not res.ok

    def test_should_raise_typed_status_error_for_unexpected_status_codes(self, mocker):
        response = create_response_mock(mocker, 500, data={'message': 'Something went wrong'}, text='Something went wrong')
        self.client._http_client.request.return_value = response
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.get('Test')
        assert exc_info.value.status_code == 500
        assert exc_info.value.errors == ['Something went wrong']

    def test_should_raise_errors_for_bad_delete_requests(self, mocker):
        error_response = { "errors":[
            "Invalid input data"
        ]}
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, data=error_response)
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.delete('Test', entity_id='123')
        assert exc_info.value.status_code == 400
        assert exc_info.value.errors == error_response['errors']

    def test_should_report_error_message_from_bad_request(self, mocker):
        error_response = { "message": "Invalid input data" }
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, data=error_response)
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.get('Test')
        assert exc_info.value.errors == [error_response['message']]

# Query Tests
class TestQuery:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client()

    def test_should_support_gql_query(self, mocker):
        rows = [{'n.name': {'@none': 'Henry'}}, {'n.name': {'@none': 'Felix'}}]
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, {'results': rows})
        gql_query = 'MATCH (n) RETURN n.name'
        assert self.client.query(gql=gql_query) == rows
        args, kwargs = self.client._http_client.request.call_args
        assert args[0] == "post"
        assert args[1].startswith('https://api.datagraphs.io/test_project/_cypher')
        assert kwargs['json']['query'] == gql_query

    def test_should_support_query_by_dataset(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response(['a', 'b']))
        self.client.query(dataset='test-dataset')
        args = self.client._http_client.request.call_args.args
        assert args[1].startswith('https://api.datagraphs.io/test_project/test-dataset?')

    def test_should_support_query_by_search_phrase(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(q='my query')
        args = self.client._http_client.request.call_args.args
        assert "&q=my+query" in args[1]

    def test_should_support_query_filters(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(filters='type:Person')
        args = self.client._http_client.request.call_args.args
        assert "&filter=type:Person" in args[1]

    def test_should_support_faceted_search(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_faceted_search_response(['a', 'b']))
        results, facets = self.client.query(facets='condition,intervention', facet_size=20)
        assert len(results) == 2
        assert len(facets[0]['buckets'][0]) == 2
        args = self.client._http_client.request.call_args.args
        assert "&facets=condition,intervention&facetSize=20" in args[1]

    def test_should_support_faceted_search_with_default_facet_size(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(facets='condition,intervention')
        args = self.client._http_client.request.call_args.args
        assert "&facets=condition,intervention&facetSize=10" in args[1]

    def test_should_support_date_faceted_search(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(date_facets='publishedDate:1d:1w:1M', facet_size=20)
        args = self.client._http_client.request.call_args.args
        assert "&dateFacets=publishedDate:1d:1w:1M" in args[1]

    def test_should_support_specified_fields_in_search_results(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(fields='name,age')
        args = self.client._http_client.request.call_args.args
        assert "&fields=name,age" in args[1]
        
    def test_should_support_requested_embed_level_in_search_results(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(embed='2')        
        args = self.client._http_client.request.call_args.args
        assert "&embed=2" in args[1]

    def test_should_support_search_results_sorting(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(sort='label:asc')
        args = self.client._http_client.request.call_args.args
        assert "&sort=label:asc" in args[1]

    def test_should_support_query_by_id(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, [{'id': 'urn:1'}, {'id': 'urn:2'}])
        results = self.client.query(ids='urn:1,urn:2')
        assert len(results) == 2
        args = self.client._http_client.request.call_args.args
        assert "&ids=urn:1,urn:2" in args[1]

    def test_should_request_query_results_in_all_languages_by_default(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(q='test')
        args, kwargs = self.client._http_client.request.call_args
        assert '?lang=all' in args[1]
        assert kwargs['headers']['Accept-Language'] == 'all'

    def test_should_return_query_results_in_specified_language(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(q='test', lang='fr')
        args, kwargs = self.client._http_client.request.call_args
        assert '?lang=fr' in args[1]
        assert kwargs['headers']['Accept-Language'] == 'fr'

    def test_should_support_paginated_queries(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(q='test', page_no=2, page_size=25)
        args = self.client._http_client.request.call_args.args
        assert '&pageNo=2' in args[1]
        assert '&pageSize=25' in args[1]

    def test_should_support_token_based_pagination(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(q='test', next_page_token='test-token', page_size=25)
        args = self.client._http_client.request.call_args.args
        assert '&nextPageToken=test-token' in args[1]        
        self.client.query(q='test', previous_page_token='test-token', page_size=25)
        args = self.client._http_client.request.call_args.args
        assert '&previousPageToken=test-token' in args[1]

    def test_should_request_system_metadata_dates_in_query_results_if_specified(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.query(q='test', include_date_fields=True)
        args = self.client._http_client.request.call_args.args
        assert '&includeDateFields=true' in args[1]

# Data Modification Tests
class TestDataModification:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret', batch_size=5)

    def test_should_update_simple_entity(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 201)
        self.client.put('test-dataset', {'payload': 'data'})
        args, kwargs = self.client._http_client.request.call_args
        assert args[0] == "put"
        assert args[1] == "https://api.datagraphs.io/test_project/test-dataset"
        assert kwargs['headers']['x-api-key'] == "test_api_key"
        assert kwargs['headers']['Authorization'] == "Bearer test_token"
        assert kwargs['json'] == [{'payload': 'data'}]

    def test_should_update_entity_collections_in_batches(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 201)
        test_data = generate_test_data_list(10)
        self.client.put('test-dataset', test_data)        
        assert self.client._http_client.request.call_count == 2
        second_call_kwargs = self.client._http_client.request.call_args_list[1].kwargs
        assert len(second_call_kwargs['json']) == 5
        assert second_call_kwargs['json'] == test_data[5:10]

    def test_should_delete_entities_by_id(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.delete('Test', '1234')
        args, kwargs = self.client._http_client.request.call_args
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/Test/1234"
        assert kwargs['headers']['x-api-key'] == "test_api_key"
        assert kwargs['headers']['Authorization'] == "Bearer test_token"

# Batched Write Outcome Tests
class TestBatchedWriteOutcomes:
    """`put` attempts every batch and accounts for every entity in exactly one outcome.

    Seven entities with a batch size of three are sent as batches [0-2], [3-5] and [6].
    """

    BATCH_SIZE = 3
    PAYLOAD_STATUSES = (400, 409, 413, 422)
    NON_PAYLOAD_STATUSES = (404, 405, 408, 429, 500, 502, 503)

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret', batch_size=self.BATCH_SIZE)
        self.entities = generate_test_data_list(7)

    def respond(self, mocker, decide, key: str = 'id'):
        """Serve each PUT with ``decide(batch_keys) -> (status, body)``.

        *key* names the entity field passed to *decide* (``id`` by default; use
        another field for entities without an ``id``). The batch is first
        JSON-encoded exactly as ``requests`` would, so encoding failures surface
        just as they do against the real library.
        """
        def side_effect(method, url, **kwargs):
            requests.Request(method, url, json=kwargs['json']).prepare()
            status, body = decide([entity.get(key) for entity in kwargs['json']])
            return create_response_mock(mocker, status, body, reason=f'Status {status}', text=f'status {status} text')
        self.client._http_client.request.side_effect = side_effect

    def sent_batches(self) -> list[list[str]]:
        return [[e['id'] for e in c.kwargs['json']] for c in self.client._http_client.request.call_args_list]

    def assert_accounts_for(self, result, loaded: int, failed: list[int], timed_out: list[int]) -> None:
        """Assert the outcome by entity position, and that every report identifies its entity."""
        assert result.loaded == loaded
        assert [f.index for f in result.failed] == failed
        assert [t.index for t in result.timed_out] == timed_out
        assert all(isinstance(t, TimedOutWrite) for t in result.timed_out)
        reported = [*result.failed, *result.timed_out]
        for ref in reported:
            assert ref.entity_id == self.entities[ref.index].get('id')
        assert len({ref.index for ref in reported}) == len(reported)
        assert result.loaded + len(reported) == len(self.entities)
        assert result.ok == (not failed and not timed_out)

    def test_should_report_success_when_every_batch_is_written(self, mocker):
        self.respond(mocker, lambda ids: (201, None))
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=7, failed=[], timed_out=[])
        assert self.sent_batches() == [['entity-0', 'entity-1', 'entity-2'], ['entity-3', 'entity-4', 'entity-5'], ['entity-6']]

    @pytest.mark.parametrize('status', PAYLOAD_STATUSES)
    def test_should_resubmit_rejected_batch_entity_by_entity_and_still_send_later_batches(self, mocker, status):
        errors = ['entity-4 has an invalid value']
        self.respond(mocker, lambda ids: (status, {'errors': errors}) if 'entity-4' in ids else (201, None))
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=6, failed=[4], timed_out=[])
        assert result.failed == [FailedWrite(4, 'entity-4', status, errors)]
        batches = self.sent_batches()
        assert len(batches) == 6
        for single in (['entity-3'], ['entity-4'], ['entity-5']):
            assert single in batches
        assert batches[-1] == ['entity-6']

    @pytest.mark.parametrize('status', NON_PAYLOAD_STATUSES)
    def test_should_fail_whole_batch_without_resubmitting_when_failure_is_not_about_the_payload(self, mocker, status):
        self.respond(mocker, lambda ids: (status, {'message': 'not a payload problem'}) if 'entity-4' in ids else (201, None))
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=4, failed=[3, 4, 5], timed_out=[])
        assert all(f.status_code == status and f.errors == ['not a payload problem'] for f in result.failed)
        assert len(self.sent_batches()) == 3

    def test_should_report_timed_out_batch_entities_without_resubmitting_them(self, mocker):
        self.respond(mocker, lambda ids: (504, None) if 'entity-4' in ids else (201, None))
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=4, failed=[], timed_out=[3, 4, 5])
        assert len(self.sent_batches()) == 3

    def test_should_report_timeout_of_an_individually_resubmitted_entity(self, mocker):
        def decide(ids):
            if ids == ['entity-5']:
                return 504, None
            return (400, {'errors': ['invalid']}) if 'entity-4' in ids else (201, None)
        self.respond(mocker, decide)
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=5, failed=[4], timed_out=[5])

    def test_should_fail_whole_batch_without_resubmitting_on_connection_error(self, mocker):
        def side_effect(method, url, **kwargs):
            if any(e['id'] == 'entity-4' for e in kwargs['json']):
                raise requests.exceptions.ConnectionError('connection reset')
            return create_response_mock(mocker, 201)
        self.client._http_client.request.side_effect = side_effect
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=4, failed=[3, 4, 5], timed_out=[])
        assert all(f.status_code is None and 'connection reset' in f.errors[0] for f in result.failed)
        assert len(self.sent_batches()) == 3

    def test_should_report_read_timeout_as_timed_out(self, mocker):
        def side_effect(method, url, **kwargs):
            if any(e['id'] == 'entity-4' for e in kwargs['json']):
                raise requests.exceptions.ReadTimeout('read timed out')
            return create_response_mock(mocker, 201)
        self.client._http_client.request.side_effect = side_effect
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=4, failed=[], timed_out=[3, 4, 5])

    @pytest.mark.parametrize('bad_value', [object(), float('nan')], ids=['unserialisable', 'nan'])
    def test_should_isolate_entity_whose_payload_cannot_be_encoded(self, mocker, bad_value):
        self.entities[4]['payload'] = bad_value
        self.respond(mocker, lambda ids: (201, None))
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=6, failed=[4], timed_out=[])
        assert result.failed[0].status_code is None

    def test_should_report_rejection_with_empty_body_as_failure(self, mocker):
        self.respond(mocker, lambda ids: (400, None) if 'entity-4' in ids else (201, None))
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=6, failed=[4], timed_out=[])
        assert result.failed == [FailedWrite(4, 'entity-4', 400, ['status 400 text'])]

    def test_should_not_resubmit_a_rejected_single_entity(self, mocker):
        self.respond(mocker, lambda ids: (400, {'errors': ['invalid']}))
        result = self.client.put('test-dataset', self.entities[0])
        assert result.failed == [FailedWrite(0, 'entity-0', 400, ['invalid'])]
        assert result.loaded == 0
        assert self.sent_batches() == [['entity-0']]

    def test_should_report_failed_entities_without_ids(self, mocker):
        entities = [{'payload': 'a'}, {'payload': 'b'}]
        self.client._http_client.request.return_value = create_response_mock(mocker, 500, text='boom')
        result = self.client.put('test-dataset', entities)
        assert result.failed == [FailedWrite(0, None, 500, ['boom']), FailedWrite(1, None, 500, ['boom'])]

    def test_should_send_nothing_for_an_empty_list(self):
        result = self.client.put('test-dataset', [])
        assert result.ok
        assert result.loaded == 0
        self.client._http_client.request.assert_not_called()

    def test_should_stop_and_raise_when_authentication_fails(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 401)
        with pytest.raises(AuthenticationError):
            self.client.put('test-dataset', self.entities)
        assert all(batch == ['entity-0', 'entity-1', 'entity-2'] for batch in self.sent_batches())

    def use_entities_without_ids(self, with_id: tuple[int, ...] = (1, 6)) -> None:
        """Replace the entities with ones named ``n0``..``n6``, only those at *with_id* having an ``id``."""
        self.entities = [{'name': f'n{i}', **({'id': f'entity-{i}'} if i in with_id else {})} for i in range(7)]

    def test_should_report_rejected_entity_without_id_by_its_position_in_the_data(self, mocker):
        self.use_entities_without_ids()
        errors = ['n4 has an invalid value']
        self.respond(mocker, lambda names: (400, {'errors': errors}) if 'n4' in names else (201, None), key='name')
        result = self.client.put('test-dataset', self.entities)
        # n4 is the second entity of the second batch, and is resubmitted alone:
        # its position is 4, not 1 (in the batch) or 0 (in its own resubmission).
        self.assert_accounts_for(result, loaded=6, failed=[4], timed_out=[])
        assert result.failed == [FailedWrite(4, None, 400, errors)]

    @pytest.mark.parametrize(('status', 'outcome'), [(500, 'failed'), (504, 'timed_out')])
    def test_should_report_every_entity_without_id_in_an_unsplit_batch_by_position(self, mocker, status, outcome):
        self.use_entities_without_ids(with_id=(4,))
        self.respond(mocker, lambda names: (status, None) if 'n4' in names else (201, None), key='name')
        result = self.client.put('test-dataset', self.entities)
        expected = {'failed': [], 'timed_out': []} | {outcome: [3, 4, 5]}
        self.assert_accounts_for(result, loaded=4, **expected)
        assert [ref.entity_id for ref in getattr(result, outcome)] == [None, 'entity-4', None]

    def test_should_report_timeout_of_resubmitted_entity_without_id_by_its_position(self, mocker):
        self.use_entities_without_ids()
        def decide(names):
            if names == ['n5']:
                return 504, None
            return (400, {'errors': ['invalid']}) if 'n4' in names else (201, None)
        self.respond(mocker, decide, key='name')
        result = self.client.put('test-dataset', self.entities)
        self.assert_accounts_for(result, loaded=5, failed=[4], timed_out=[5])
        assert result.timed_out == [TimedOutWrite(5, None)]

    def test_should_label_entities_by_position_and_id_in_logs(self, mocker, caplog):
        self.use_entities_without_ids(with_id=(3,))
        self.respond(mocker, lambda names: (500, None) if 'n4' in names else (201, None), key='name')
        with caplog.at_level(logging.ERROR):
            self.client.put('test-dataset', self.entities)
        assert "['#3 entity-3', '#4 (no id)', '#5 (no id)']" in caplog.text

    def test_should_log_a_summary_when_entities_are_not_written(self, mocker, caplog):
        self.respond(mocker, lambda ids: (504, None) if 'entity-6' in ids else (400, {'errors': ['bad']}) if 'entity-0' in ids else (201, None))
        with caplog.at_level(logging.ERROR):
            self.client.put('test-dataset', self.entities)
        assert 'Loaded 5 of 7 entities into dataset test-dataset: 1 failed, 1 timed out' in caplog.text

class TestSchemaOperations:

    SCHEMA_DATA: ClassVar[dict] = {
        "name": "Domain Model",
        "createdDate": "2024-06-01T00:00:00Z",
        "lastModifiedDate": "2024-06-01T00:00:00Z",
        "classes": []
    }

    SCHEMA_VALIDATION_ERROR_RESPONSE: ClassVar[dict] = {
        "statusCode": 400,
        "error": "Bad Request",
        "message": "Validation failed",
        "errors": [
            {
                "message": "Cannot change isLangString of property \"internalLabel\" from \"ActiveIngredientTotalDosageExpression\" as data exists for class",
                "class": "ActiveIngredientTotalDosageExpression",
                "code": "DATA_EXISTS",
                "property": "internalLabel",
                "types": [
                    {
                        "type": "DirectionForUse",
                        "datasets": [
                            "urn:croplife-dlc:registered-formulation"
                        ]
                    }
                ]
            },
            {
                "message": "Cannot change isNestedObject of property \"impactedCrop\" from \"AdverseEffect\" as data exists for class",
                "class": "AdverseEffect",
                "code": "DATA_EXISTS",
                "property": "impactedCrop",
                "types": [
                    {
                        "type": "RegisteredFormulation",
                        "datasets": [
                            "urn:croplife-dlc:registered-formulation"
                        ]
                    }
                ]
            },
            {
                "message": "Cannot change parentClass of \"AngleUnit\" as it is in use by a dataset",
                "class": "AngleUnit",
                "code": "IN_USE_BY_DATASET",
                "attribute": "parentClass",
                "types": [
                    {
                        "type": "RegisteredFormulation",
                        "datasets": [
                            "urn:croplife-dlc:registered-formulation"
                        ]
                    }
                ]
            }
        ]
    }
    
    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret')

    def test_should_get_schema_for_project(self, mocker):
        schema_response_data = {
            "name": "Model",
            "createdDate": "2024-06-01T00:00:00Z",
            "lastModifiedDate": "2024-06-01T00:00:00Z",
            "classes": []
        }
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, schema_response_data)
        self.client.get_schema()
        args = self.client._http_client.request.call_args.args
        assert args[1].startswith("https://api.datagraphs.io/test_project/models/_active?")

    def test_should_validate_schema_apply_via_dry_run(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, self.SCHEMA_VALIDATION_ERROR_RESPONSE)
        resp = self.client.apply_schema(DatagraphsSchema.create_from(self.SCHEMA_DATA), SCHEMA_APPLY_MODE.VALIDATE_ONLY)
        args, kwargs = self.client._http_client.request.call_args
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/models/_active?dryRun=true")
        sent_data = json.loads(kwargs['data'])
        assert sent_data['name'] == self.SCHEMA_DATA['name']
        assert sent_data['classes'] == []
        assert resp == self.SCHEMA_VALIDATION_ERROR_RESPONSE["errors"]

    def test_should_apply_schema_to_project(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.apply_schema(DatagraphsSchema.create_from(self.SCHEMA_DATA))
        args, kwargs = self.client._http_client.request.call_args
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/models/_active")
        sent_data = json.loads(kwargs['data'])
        assert sent_data['name'] == self.SCHEMA_DATA['name']
        assert sent_data['classes'] == []

    def test_should_raise_exception_when_schema_application_fails(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, self.SCHEMA_VALIDATION_ERROR_RESPONSE)
        with pytest.raises(DatagraphsError):
            self.client.apply_schema(DatagraphsSchema.create_from(self.SCHEMA_DATA))
            args, kwargs = self.client._http_client.request.call_args
            assert args[0] == "put"
            assert args[1].startswith("https://api.datagraphs.io/test_project/models/_active")
            sent_data = json.loads(kwargs['data'])
            assert sent_data['name'] == self.SCHEMA_DATA['name']
            assert sent_data['classes'] == []

    def test_should_resolve_dependencies_when_forcing_schema_update(self, mocker):
        datasets_response = get_search_response([{
            "id": "urn:croplife-dlc:registered-formulation",
            "name": "Registered Formulation",
            "classes": ["RegisteredFormulation", "DirectionForUse"]
        }])
        base = "https://api.datagraphs.io/test_project"
        expected_calls = {
            "initial apply reports the blocking dependencies":  ("put",    f"{base}/models/_active", create_response_mock(mocker, 400, self.SCHEMA_VALIDATION_ERROR_RESPONSE)),
            "clear DirectionForUse entities from the dataset":  ("delete", f"{base}/registered-formulation?filter=type:DirectionForUse", create_response_mock(mocker, 200, None)),
            "clear RegisteredFormulation entities from the dataset": ("delete", f"{base}/registered-formulation?filter=type:RegisteredFormulation", create_response_mock(mocker, 200, None)),
            "look up which datasets reference the dropped classes": ("get",  f"{base}/?pageSize=", create_response_mock(mocker, 200, datasets_response)),
            "update the dataset to detach the dropped classes":  ("put",    f"{base}/datasets/registered-formulation", create_response_mock(mocker, 200, None)),
            "re-apply the schema now that dependencies are cleared": ("put", f"{base}/models/_active", create_response_mock(mocker, 200, None)),
        }
        self.client._http_client.request.side_effect = [response for (_, _, response) in expected_calls.values()]
        self.client.apply_schema(DatagraphsSchema.create_from(self.SCHEMA_DATA), SCHEMA_APPLY_MODE.FORCE)
        actual_calls = self.client._http_client.request.call_args_list
        assert len(actual_calls) == len(expected_calls)
        for i, ((step, (method, url_prefix, _response)), call) in enumerate(zip(expected_calls.items(), actual_calls)):
            assert call.args[0] == method, f"call {i} ({step}): expected method {method!r}, got {call.args[0]!r}"
            assert call.args[1].startswith(url_prefix), f"call {i} ({step}): expected URL prefix {url_prefix!r}, got {call.args[1]!r}"

    def test_should_get_dependencies_for_schema_update(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, self.SCHEMA_VALIDATION_ERROR_RESPONSE)
        classes_to_clear, classes_to_drop = self.client.get_schema_update_dependencies(DatagraphsSchema.create_from(self.SCHEMA_DATA))
        args = self.client._http_client.request.call_args.args
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/models/_active?dryRun=true")
        assert classes_to_clear == [('DirectionForUse', 'registered-formulation'), ('RegisteredFormulation', 'registered-formulation')]
        assert classes_to_drop == [('RegisteredFormulation', 'registered-formulation')]

class TestDatasetOperations:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret')

    def test_should_list_datasets_in_project(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': 'ds1'}, {'name': 'ds2'}]))
        datasets = self.client.get_datasets()
        args = self.client._http_client.request.call_args.args
        assert args[1].startswith("https://api.datagraphs.io/test_project/?")
        assert datasets[0].name == 'ds1'
        assert datasets[1].name == 'ds2'

    def test_should_create_datasets_when_applying_new_datasets(self, mocker):
        existing_datasets = []
        new_datasets = [{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, get_search_response(existing_datasets)),
            create_response_mock(mocker, 201),
            create_response_mock(mocker, 201),
            create_response_mock(mocker, 200, get_search_response(new_datasets))
        ]
        datasets = [Dataset(name='1', project='ds'), Dataset(name='2', project='ds')]
        self.client.apply_datasets(datasets, timeout_s=0.5)
        args, kwargs = self.client._http_client.request.call_args_list[1]
        assert args[0] == "post"
        assert args[1].startswith("https://api.datagraphs.io/test_project/datasets")
        assert kwargs['json']['id'] == 'urn:ds:1'
        args, kwargs = self.client._http_client.request.call_args_list[2]
        assert args[0] == "post"
        assert args[1].startswith("https://api.datagraphs.io/test_project/datasets")
        assert kwargs['json']['id'] == 'urn:ds:2'

    def test_should_update_datasets_when_applying_existing_datasets(self, mocker):
        datasets = [Dataset(name='1', project='ds'), Dataset(name='2', project='ds')]
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]))
        self.client.apply_datasets(datasets, timeout_s=5)
        args, kwargs = self.client._http_client.request.call_args_list[1]
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/datasets/1")
        assert kwargs['json']['id'] == 'urn:ds:1'
        args, kwargs = self.client._http_client.request.call_args_list[2]
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/datasets/2")
        assert kwargs['json']['id'] == 'urn:ds:2'

    def test_should_raise_error_if_datasets_not_applied_within_expected_timeframe(self, mocker):
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, get_search_response([])),
            create_response_mock(mocker, 201),
            create_response_mock(mocker, 201),
            create_response_mock(mocker, 200, get_search_response([]))
        ]
        datasets = [Dataset(name='1', project='ds'), Dataset(name='2', project='ds')]
        with pytest.raises(DatagraphsError):
            self.client.apply_datasets(datasets, timeout_s=0)

    def test_should_back_off_poll_interval_on_schedule_while_waiting_for_datasets(self, mocker):
        # Observe the real waits between polls through the public apply_datasets():
        # every 1s for the first 10s, every 2s for the next 20s, then every 5s
        # until the timeout. A virtual clock advanced by each (mocked) sleep makes
        # the schedule deterministic with no real waiting and no reach into privates.
        clock = [0.0]
        poll_waits = []

        def fake_sleep(seconds):
            if seconds > 0:  # ignore the unrelated per-write pause (set to 0 below)
                poll_waits.append(seconds)
            clock[0] += seconds

        mocker.patch('datagraphs.client.time.sleep', side_effect=fake_sleep)
        mocker.patch('datagraphs.client.time.monotonic', side_effect=lambda: clock[0])

        # Datasets never report as applied, so polling continues until the timeout.
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([]))
        self.client.set_wait_time(0)

        datasets = [Dataset(name='1', project='ds'), Dataset(name='2', project='ds')]
        with pytest.raises(DatagraphsError):
            self.client.apply_datasets(datasets, timeout_s=40)

        # 0-10s: 1s x10  |  10-30s: 2s x10  |  30-40s: 5s x2 (then the 40s timeout fires)
        assert poll_waits == [1] * 10 + [2] * 10 + [5] * 2

    def test_should_delete_data_from_dataset(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]))
        dataset = Dataset(name='1', project='ds')
        self.client.clear_dataset(dataset.slug)
        args = self.client._http_client.request.call_args_list[0].args
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/1?filter=_all"

    def test_should_drop_dataset(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]))
        dataset = Dataset(name='1', project='ds')
        self.client.drop_dataset(dataset.slug)
        args = self.client._http_client.request.call_args_list[0].args
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/datasets/1"

    def test_should_drop_existing_datasets_during_hard_teardown(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]))
        self.client.tear_down(drop_datasets=True)
        args = self.client._http_client.request.call_args_list[1].args
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/datasets/1"
        args = self.client._http_client.request.call_args_list[2].args
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/datasets/2"

    def test_should_clear_existing_datasets_during_soft_teardown(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]))
        self.client.tear_down(drop_datasets=False)
        args = self.client._http_client.request.call_args_list[1].args
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/1?filter=_all"
        args = self.client._http_client.request.call_args_list[2].args
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/2?filter=_all"


# Credential Sanitisation Tests
class TestCredentialSanitisation:

    def test_should_replace_typographic_double_quotes_in_api_key(self):
        client = DatagraphsClient("test_project", "“test_api_key”")
        assert client._api_key == '"test_api_key"'

    def test_should_replace_typographic_single_quotes_in_credentials(self):
        client = DatagraphsClient(
            "test_project", "key", client_id="‘id’", client_secret="o’brien"
        )
        assert client._client_id == "'id'"
        assert client._client_secret == "o'brien"

    def test_should_replace_dashes_and_minus_sign(self):
        client = DatagraphsClient("test_project", "a–b—c−d")
        assert client._api_key == "a-b-c-d"

    def test_should_replace_non_breaking_space_and_strip_surrounding_whitespace(self):
        client = DatagraphsClient("test_project", "  key value  ")
        assert client._api_key == "key value"

    def test_should_strip_zero_width_characters_and_byte_order_mark(self):
        client = DatagraphsClient("test_project", "﻿key\u200bvalue")
        assert client._api_key == "keyvalue"

    def test_should_sanitise_project_name_used_in_urls(self):
        client = DatagraphsClient("“proj”", "key")
        assert client.project_name == '"proj"'

    def test_should_sanitise_service_url_and_preserve_trailing_slash(self):
        client = DatagraphsClient("test_project", "key", service_url="https://host\u200b.test/")
        assert client._service_url == "https://host.test/"

    def test_should_leave_plain_ascii_credentials_unchanged(self):
        client = DatagraphsClient("test_project", "plain_ascii_key-123")
        assert client._api_key == "plain_ascii_key-123"

    def test_sanitised_credentials_are_encodable_as_http_headers(self):
        # The contract that matters: whatever survives sanitisation must be
        # serialisable into a latin-1 HTTP header, which is the failure that
        # originally manifested as an opaque UnicodeEncodeError.
        client = DatagraphsClient(
            "“proj”",
            "“key”",
            client_id="‘id’",
            client_secret="sec–ret",
        )
        for value in (client.project_name, client._api_key, client._client_id, client._client_secret):
            value.encode("latin-1")  # must not raise


# Unencodable Request Tests
class TestUnencodableRequest:
    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client()

    def test_should_raise_clear_error_when_request_cannot_be_encoded(self):
        self.client._http_client.request.side_effect = UnicodeEncodeError(
            'latin-1', '“', 0, 1, 'ordinal not in range(256)'
        )
        with pytest.raises(DatagraphsError, match='cannot be encoded'):
            self.client.get('Test')



# Read Failure Tests
#
# Every read either returns what was asked for or raises: no failure is ever
# reported as an empty or partial result. Retries are disabled here so each
# failure is seen at once; TestReadRetries covers them.

READS = {
    'get': lambda c: c.get('Test'),
    'query': lambda c: c.query(filters='type:Test'),
    'query with facets': lambda c: c.query(facets='type'),
    'query by ids': lambda c: c.query(ids='urn:1,urn:2'),
    'query_page': lambda c: c.query_page(filters='type:Test'),
    'query_all': lambda c: c.query_all(filters='type:Test'),
    'gql query': lambda c: c.query(gql='MATCH (n) RETURN n'),
    'get_schema': lambda c: c.get_schema(),
    'get_datasets': lambda c: c.get_datasets(),
}
SEARCH_READS = ('get', 'query', 'query with facets', 'query_page', 'query_all', 'get_datasets')

FAILURES = {
    '400 with errors': (400, {'errors': ['Invalid filter']}, '', ['Invalid filter']),
    '400 with message': (400, {'statusCode': 400, 'error': 'Bad Request', 'message': 'Invalid filter'}, '', ['Invalid filter']),
    '400 with error only': (400, {'error': 'Syntax error [GQL-0003]'}, '', ['Syntax error [GQL-0003]']),
    '404': (404, None, 'Not Found', ['Not Found']),
    '500': (500, {'message': 'boom'}, 'boom', ['boom']),
    '502': (502, None, 'Bad Gateway', ['Bad Gateway']),
    '503': (503, None, 'Service Unavailable', ['Service Unavailable']),
    '504': (504, None, 'Endpoint request timed out', ['Endpoint request timed out']),
}

class TestReadFailures:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client(max_read_retries=0)

    @pytest.mark.parametrize('read', READS.values(), ids=READS.keys())
    @pytest.mark.parametrize(('status', 'body', 'text', 'errors'), FAILURES.values(), ids=FAILURES.keys())
    def test_should_raise_status_error_instead_of_returning_a_result(self, mocker, caplog, read, status, body, text, errors):
        self.client._http_client.request.return_value = create_response_mock(mocker, status, body, reason=text, text=text)
        with caplog.at_level(logging.WARNING), pytest.raises(HttpStatusError) as exc_info:
            read(self.client)
        assert exc_info.value.status_code == status
        assert exc_info.value.errors == errors
        assert 'continuing processing' not in caplog.text

    @pytest.mark.parametrize('read', READS.values(), ids=READS.keys())
    @pytest.mark.parametrize('error', [requests.exceptions.ReadTimeout('read timed out'), requests.exceptions.ConnectionError('connection reset')], ids=['read timeout', 'connection error'])
    def test_should_raise_on_transport_failure(self, read, error):
        self.client._http_client.request.side_effect = error
        with pytest.raises(DatagraphsError) as exc_info:
            read(self.client)
        assert exc_info.value.__cause__ is error

    @pytest.mark.parametrize('read', READS.values(), ids=READS.keys())
    def test_should_raise_on_success_without_a_body(self, mocker, read):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        with pytest.raises(DatagraphsError, match='has no body'):
            read(self.client)

    @pytest.mark.parametrize('read', READS.values(), ids=READS.keys())
    def test_should_raise_on_success_with_a_body_that_is_not_json(self, mocker, read):
        response = create_response_mock(mocker, 200, text='<html>gateway</html>')
        response.content = b'<html>gateway</html>'
        response.json.side_effect = requests.exceptions.JSONDecodeError('Expecting value', '<html>', 0)
        self.client._http_client.request.return_value = response
        with pytest.raises(DatagraphsError, match='not valid JSON'):
            read(self.client)

    @pytest.mark.parametrize('read', [READS[name] for name in SEARCH_READS], ids=SEARCH_READS)
    @pytest.mark.parametrize('body', [
        {'results': [{'id': 'a'}]},
        {'search': {}, 'results': []},
        {'search': {'totalResults': '2'}, 'results': []},
        {'search': {'totalResults': 2}},
        {'search': {'totalResults': 2}, 'results': {'id': 'a'}},
        [{'id': 'a'}],
    ], ids=['no search', 'no total', 'non-integer total', 'no results', 'results not a list', 'not an object'])
    def test_should_raise_on_success_with_a_body_that_is_not_a_search_page(self, mocker, read, body):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, body)
        with pytest.raises(DatagraphsError, match='Malformed search response'):
            read(self.client)

    @pytest.mark.parametrize('read', [READS[name] for name in SEARCH_READS], ids=SEARCH_READS)
    @pytest.mark.parametrize('body', [{'search': {'totalResults': 0}, 'results': []}, {'search': {'totalResults': 0}}], ids=['empty results', 'no results key'])
    def test_should_return_nothing_only_when_the_api_reports_nothing(self, mocker, read, body):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, body)
        result = read(self.client)
        results = result.results if isinstance(result, QueryPage) else result
        assert results == []
        assert self.client._http_client.request.call_count == 1

    def test_should_raise_on_gql_response_without_results(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, {'data': []})
        with pytest.raises(DatagraphsError, match='Malformed GQL response'):
            self.client.query(gql='MATCH (n) RETURN n')

    def test_should_raise_on_ids_query_response_that_is_neither_a_list_nor_a_search_page(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, {'unexpected': True})
        with pytest.raises(DatagraphsError, match='Malformed search response'):
            self.client.query(ids='urn:1')


# Paged Read Tests
#
# Five entities at a page size of two are served as three pages: [0-1], [2-3], [4].

PAGED_READS = {
    'get': lambda c: c.get('Test'),
    'query_all': lambda c: c.query_all(filters='type:Test'),
}

class TestPagedReadFailures:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2, max_read_retries=0)
        self.entities = generate_test_data_list(5)

    def serve(self, mocker, tokens: bool, failing_page: int, failure) -> None:
        pages = paged_responses(mocker, self.entities, 2, tokens)
        pages[failing_page - 1] = failure
        self.client._http_client.request.side_effect = pages

    @pytest.mark.parametrize('read', PAGED_READS.values(), ids=PAGED_READS.keys())
    @pytest.mark.parametrize('tokens', [True, False], ids=['token paging', 'page-number paging'])
    @pytest.mark.parametrize('failing_page', [1, 2, 3], ids=['first page', 'middle page', 'last page'])
    @pytest.mark.parametrize('status', [400, 504])
    def test_should_raise_and_stop_when_any_page_fails(self, mocker, read, tokens, failing_page, status):
        self.serve(mocker, tokens, failing_page, create_response_mock(mocker, status, {'errors': [f'status {status}']}))
        with pytest.raises(HttpStatusError) as exc_info:
            read(self.client)
        assert exc_info.value.status_code == status
        assert self.client._http_client.request.call_count == failing_page

    @pytest.mark.parametrize('read', PAGED_READS.values(), ids=PAGED_READS.keys())
    @pytest.mark.parametrize('tokens', [True, False], ids=['token paging', 'page-number paging'])
    @pytest.mark.parametrize('failing_page', [1, 2, 3], ids=['first page', 'middle page', 'last page'])
    @pytest.mark.parametrize('error', [requests.exceptions.ReadTimeout('read timed out'), requests.exceptions.ConnectionError('reset')], ids=['read timeout', 'connection error'])
    def test_should_raise_and_stop_when_any_page_request_cannot_complete(self, mocker, read, tokens, failing_page, error):
        self.serve(mocker, tokens, failing_page, error)
        with pytest.raises(DatagraphsError) as exc_info:
            read(self.client)
        assert exc_info.value.__cause__ is error
        assert self.client._http_client.request.call_count == failing_page

    @pytest.mark.parametrize('read', PAGED_READS.values(), ids=PAGED_READS.keys())
    @pytest.mark.parametrize('failing_page', [2, 3], ids=['middle page', 'last page'])
    def test_should_raise_when_a_later_page_is_not_a_search_page(self, mocker, read, failing_page):
        self.serve(mocker, True, failing_page, create_response_mock(mocker, 200, {'unexpected': True}))
        with pytest.raises(DatagraphsError, match='Malformed search response'):
            read(self.client)


class TestPagedReadCompleteness:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2, max_read_retries=0)
        self.entities = generate_test_data_list(5)

    def page_requests(self) -> list[tuple[str | None, str | None]]:
        """The (pageNo, nextPageToken) of each page requested."""
        return [(p.get('pageNo'), p.get('nextPageToken')) for p in map(query_params, requested_urls(self.client))]

    @pytest.mark.parametrize('read', PAGED_READS.values(), ids=PAGED_READS.keys())
    def test_should_read_every_page_by_token_and_stop_at_the_total(self, mocker, read):
        # The API gives a token on the last page too: the total marks the end.
        self.client._http_client.request.side_effect = paged_responses(mocker, self.entities, 2, tokens=True)
        assert read(self.client) == self.entities
        assert self.page_requests() == [('1', None), (None, 'token-1'), (None, 'token-2')]

    @pytest.mark.parametrize('read', PAGED_READS.values(), ids=PAGED_READS.keys())
    def test_should_read_every_page_by_number_when_the_api_gives_no_token(self, mocker, read):
        self.client._http_client.request.side_effect = paged_responses(mocker, self.entities, 2, tokens=False)
        assert read(self.client) == self.entities
        assert self.page_requests() == [('1', None), ('2', None), ('3', None)]

    @pytest.mark.parametrize('tokens', [True, False], ids=['token paging', 'page-number paging'])
    def test_should_read_every_page_when_the_api_serves_fewer_results_per_page_than_requested(self, mocker, tokens):
        self.client._http_client.request.side_effect = paged_responses(mocker, self.entities, 1, tokens)
        assert self.client.get('Test') == self.entities
        assert self.client._http_client.request.call_count == 5

    def test_should_keep_every_query_parameter_on_every_page(self, mocker):
        self.client._http_client.request.side_effect = paged_responses(mocker, self.entities, 2, tokens=True)
        self.client.query_all(dataset='pets', q='henry', filters='territory:GB', fields='id,name', sort='name:asc', lang='fr', include_date_fields=True)
        urls = requested_urls(self.client)
        assert all(url.startswith('https://api.datagraphs.io/test_project/pets?') for url in urls)
        for params in map(query_params, urls):
            assert {k: params[k] for k in ('q', 'filter', 'fields', 'sort', 'lang', 'pageSize', 'includeDateFields')} == {
                'q': 'henry', 'filter': 'territory:GB', 'fields': 'id,name', 'sort': 'name:asc',
                'lang': 'fr', 'pageSize': '2', 'includeDateFields': 'true',
            }

    def test_should_page_by_the_requested_page_size(self, mocker):
        self.client._http_client.request.side_effect = paged_responses(mocker, self.entities, 3, tokens=True)
        assert self.client.query_all(page_size=3) == self.entities
        assert {query_params(url)['pageSize'] for url in requested_urls(self.client)} == {'3'}

    def test_should_raise_when_token_pages_run_out_before_the_total(self, mocker):
        pages = paged_responses(mocker, self.entities, 2, tokens=True)
        pages[1] = create_response_mock(mocker, 200, search_page(self.entities[2:4], 5))  # no token: nothing more to fetch
        self.client._http_client.request.side_effect = pages
        with pytest.raises(IncompleteReadError) as exc_info:
            self.client.get('Test')
        assert (exc_info.value.expected, exc_info.value.received) == (5, 4)
        assert self.client._http_client.request.call_count == 2

    def test_should_raise_when_numbered_pages_run_out_before_the_total(self, mocker):
        pages = paged_responses(mocker, self.entities, 2, tokens=False)
        pages[2] = create_response_mock(mocker, 200, search_page([], 5))
        self.client._http_client.request.side_effect = pages
        with pytest.raises(IncompleteReadError) as exc_info:
            self.client.get('Test')
        assert (exc_info.value.expected, exc_info.value.received) == (5, 4)

    def test_should_raise_when_pages_hold_more_results_than_the_total(self, mocker):
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page(self.entities[0:2], 3, 'token-1')),
            create_response_mock(mocker, 200, search_page(self.entities[2:4], 3, 'token-2')),
        ]
        with pytest.raises(IncompleteReadError) as exc_info:
            self.client.get('Test')
        assert (exc_info.value.expected, exc_info.value.received) == (3, 4)

    def test_should_raise_when_a_page_repeats_a_result(self, mocker):
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page(self.entities[0:2], 5, 'token-1')),
            create_response_mock(mocker, 200, search_page(self.entities[1:3], 5, 'token-2')),
        ]
        with pytest.raises(IncompleteReadError, match='repeated entity-1'):
            self.client.get('Test')

    def test_should_raise_rather_than_loop_when_a_page_returns_the_token_it_was_requested_with(self, mocker):
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page(self.entities[0:2], 5, 'token-1')),
            create_response_mock(mocker, 200, search_page(self.entities[2:4], 5, 'token-1')),
        ]
        with pytest.raises(IncompleteReadError, match='nextPageToken it was requested with'):
            self.client.get('Test')
        assert self.client._http_client.request.call_count == 2

    def test_should_count_results_without_ids_by_position(self, mocker):
        rows = [{'name': 'Henry'}, {'name': 'Henry'}, {'name': 'Felix'}]
        self.client._http_client.request.side_effect = paged_responses(mocker, rows, 2, tokens=True)
        assert self.client.query_all(fields='name') == rows

    def test_should_pause_between_pages_but_not_after_the_last(self, mocker, sleep):
        self.client.set_wait_time(250)
        self.client._http_client.request.side_effect = paged_responses(mocker, self.entities, 2, tokens=True)
        self.client.get('Test')
        assert [c.args[0] for c in sleep.call_args_list] == [0.25, 0.25]


# Query Paging Tests
class TestQueryPaging:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2, max_read_retries=0)

    def test_should_return_page_with_total_and_tokens(self, mocker):
        body = search_page([{'id': 'a'}, {'id': 'b'}], 3000, 'next-token')
        body['search']['previousPageToken'] = 'previous-token'
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, body)
        page = self.client.query_page(filters='territory:GB')
        assert page == QueryPage(results=[{'id': 'a'}, {'id': 'b'}], total_results=3000,
                                 next_page_token='next-token', previous_page_token='previous-token', facets=None)
        assert query_params(requested_urls(self.client)[0])['pageSize'] == '2'

    def test_should_return_page_facets_when_requested(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_faceted_search_response([{'id': 'a'}]))
        page = self.client.query_page(facets='condition')
        assert page.facets == get_faceted_search_response([])['facets']

    def test_should_fetch_the_page_named_by_a_token(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, search_page([{'id': 'c'}], 3))
        page = self.client.query_page(filters='territory:GB', next_page_token='next-token')
        assert page.next_page_token is None
        assert query_params(requested_urls(self.client)[0])['nextPageToken'] == 'next-token'

    def test_should_page_through_a_filtered_query_to_the_end(self, mocker):
        entities = generate_test_data_list(7)
        self.client._http_client.request.side_effect = paged_responses(mocker, entities, 2, tokens=True)
        assert self.client.query_all(filters='territory:GB') == entities
        assert all(query_params(url)['filter'] == 'territory:GB' for url in requested_urls(self.client))

    def test_query_should_still_return_one_page_of_results(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, search_page([{'id': 'a'}, {'id': 'b'}], 3000, 'next-token'))
        assert self.client.query(filters='territory:GB') == [{'id': 'a'}, {'id': 'b'}]
        assert self.client._http_client.request.call_count == 1

    def test_query_by_ids_should_accept_a_search_page_too(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, search_page([{'id': 'urn:1'}], 1))
        assert self.client.query(ids='urn:1') == [{'id': 'urn:1'}]


# Timeout Tests
OPERATIONS = {
    'status': lambda c: c.status(),
    'get': lambda c: c.get('Test'),
    'query': lambda c: c.query(q='x'),
    'query_page': lambda c: c.query_page(),
    'query_all': lambda c: c.query_all(),
    'gql query': lambda c: c.query(gql='MATCH (n) RETURN n'),
    'put': lambda c: c.put('ds', [{'id': 'a'}]),
    'delete': lambda c: c.delete('Test', '1'),
    'get_schema': lambda c: c.get_schema(),
    'apply_schema': lambda c: c.apply_schema(DatagraphsSchema.create_from(TestSchemaOperations.SCHEMA_DATA)),
    'get_datasets': lambda c: c.get_datasets(),
    'create_dataset': lambda c: c.create_dataset(Dataset(name='1', project='ds')),
    'update_dataset': lambda c: c.update_dataset(Dataset(name='1', project='ds')),
    'clear_dataset': lambda c: c.clear_dataset('ds'),
    'drop_dataset': lambda c: c.drop_dataset('ds'),
}

class TestTimeouts:

    # A body every operation accepts: an empty search page that is also a
    # status, a schema and a GQL result.
    ANY_BODY: ClassVar[dict] = {**search_page([], 0), 'api': 'OK', **TestSchemaOperations.SCHEMA_DATA}

    def sent_timeouts(self, client) -> list:
        calls = [*client._http_client.post.call_args_list, *client._http_client.request.call_args_list]
        return [c.kwargs.get('timeout') for c in calls]

    @pytest.mark.parametrize('operation', OPERATIONS.values(), ids=OPERATIONS.keys())
    @pytest.mark.parametrize(('options', 'expected'), [
        ({}, DatagraphsClient.DEFAULT_TIMEOUT_S),
        ({'timeout': (3.0, 7.0)}, (3.0, 7.0)),
        ({'timeout': 5}, 5),
    ], ids=['default', 'connect and read', 'single value'])
    def test_should_send_every_request_including_the_token_request_with_the_timeout(self, mocker, get_client, operation, options, expected):
        client = get_client('test_client_id', 'test_client_secret', **options)
        client._http_client.request.return_value = create_response_mock(mocker, 200, self.ANY_BODY)
        operation(client)
        timeouts = self.sent_timeouts(client)
        assert client._http_client.post.call_count == 1
        assert len(timeouts) >= 2
        assert all(t == expected for t in timeouts)

    def test_should_default_to_a_read_timeout_longer_than_a_connect_timeout(self):
        connect_s, read_s = DatagraphsClient.DEFAULT_TIMEOUT_S
        assert 0 < connect_s < read_s

    @pytest.mark.parametrize('timeout', [0, -1, (1, 0), (0, 1), (1, 2, 3), ()], ids=str)
    def test_should_reject_a_timeout_that_is_not_positive(self, timeout):
        with pytest.raises(ValueError, match='timeout'):
            DatagraphsClient('test_project', 'test_api_key', timeout=timeout)

    def test_should_reject_a_negative_retry_count(self):
        with pytest.raises(ValueError, match='max_read_retries'):
            DatagraphsClient('test_project', 'test_api_key', max_read_retries=-1)

    def test_should_report_token_request_timeout_as_a_request_failure_not_bad_credentials(self, get_client):
        client = get_client('test_client_id', 'test_client_secret', max_read_retries=0)
        error = requests.exceptions.ReadTimeout('read timed out')
        client._http_client.post.side_effect = error
        with pytest.raises(TokenRequestError) as exc_info:
            client.get('Test')
        assert not isinstance(exc_info.value, AuthenticationError)
        assert exc_info.value.status_code is None
        assert exc_info.value.__cause__ is error
        client._http_client.request.assert_not_called()

    @pytest.mark.parametrize('status', [400, 401, 403])
    def test_should_report_rejected_credentials_as_an_authentication_error(self, mocker, get_client, status):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.post.return_value = create_response_mock(mocker, status, {'message': 'invalid client'}, text='invalid client')
        with pytest.raises(AuthenticationError, match='credentials rejected'):
            client.get('Test')
        client._http_client.request.assert_not_called()

    def test_should_report_token_service_failure_as_a_token_request_error(self, mocker, get_client):
        client = get_client('test_client_id', 'test_client_secret', max_read_retries=0)
        client._http_client.post.return_value = create_response_mock(mocker, 503, text='Service Unavailable')
        with pytest.raises(TokenRequestError) as exc_info:
            client.get('Test')
        assert not isinstance(exc_info.value, AuthenticationError)
        assert exc_info.value.status_code == 503
        assert exc_info.value.errors == ['Service Unavailable']
        client._http_client.request.assert_not_called()

    def test_should_report_malformed_token_response(self, mocker, get_client):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.post.return_value = create_response_mock(mocker, 200, {'unexpected': True})
        with pytest.raises(TokenRequestError, match='unexpected response'):
            client.get('Test')
        assert client._http_client.post.call_count == 1  # a malformed response is not retried

    def test_should_report_connect_timeout_on_write_as_failed_not_timed_out(self, get_client):
        # A connect timeout means the request was never sent, so it cannot have been written.
        client = get_client(batch_size=2)
        client._http_client.request.side_effect = requests.exceptions.ConnectTimeout('connect timed out')
        result = client.put('ds', [{'id': 'a'}, {'id': 'b'}])
        assert [f.index for f in result.failed] == [0, 1]
        assert result.timed_out == []
        assert all(f.status_code is None for f in result.failed)


# Read Retry Tests
class TestReadRetries:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2)
        self.entities = generate_test_data_list(5)

    def failure(self, mocker, status: int = 503, headers: dict | None = None):
        return create_response_mock(mocker, status, reason='transient', text='transient', headers=headers)

    @pytest.mark.parametrize('status', [429, 502, 503, 504])
    def test_should_retry_a_page_after_a_transient_status_and_return_every_result(self, mocker, status):
        pages = paged_responses(mocker, self.entities, 2, tokens=True)
        self.client._http_client.request.side_effect = [pages[0], self.failure(mocker, status), pages[1], pages[2]]
        assert self.client.get('Test') == self.entities
        urls = requested_urls(self.client)
        assert len(urls) == 4
        assert query_params(urls[1])['nextPageToken'] == query_params(urls[2])['nextPageToken'] == 'token-1'

    @pytest.mark.parametrize('error', [requests.exceptions.ConnectionError('reset'), requests.exceptions.ReadTimeout('slow'), requests.exceptions.ConnectTimeout('down')], ids=['connection error', 'read timeout', 'connect timeout'])
    def test_should_retry_a_read_after_a_transport_failure(self, mocker, error):
        pages = paged_responses(mocker, self.entities, 2, tokens=True)
        self.client._http_client.request.side_effect = [error, *pages]
        assert self.client.get('Test') == self.entities

    def test_should_raise_the_last_failure_once_retries_are_exhausted(self, mocker, get_client):
        client = get_client(max_read_retries=2)
        client._http_client.request.side_effect = [self.failure(mocker, 503), self.failure(mocker, 502), self.failure(mocker, 504)]
        with pytest.raises(HttpStatusError) as exc_info:
            client.get('Test')
        assert exc_info.value.status_code == 504
        assert client._http_client.request.call_count == 3

    @pytest.mark.parametrize('status', [400, 404, 409, 500])
    def test_should_not_retry_a_failure_that_a_retry_cannot_cure(self, mocker, status):
        self.client._http_client.request.return_value = self.failure(mocker, status)
        with pytest.raises(HttpStatusError):
            self.client.get('Test')
        assert self.client._http_client.request.call_count == 1

    def test_should_not_retry_when_retries_are_disabled(self, mocker, get_client):
        client = get_client(max_read_retries=0)
        client._http_client.request.return_value = self.failure(mocker, 503)
        with pytest.raises(HttpStatusError):
            client.get('Test')
        assert client._http_client.request.call_count == 1

    def test_should_back_off_exponentially_with_jitter(self, mocker, sleep):
        self.client._http_client.request.side_effect = [self.failure(mocker)] * 4
        with pytest.raises(HttpStatusError):
            self.client.get('Test')
        delays = [c.args[0] for c in sleep.call_args_list]
        assert len(delays) == DatagraphsClient.DEFAULT_MAX_READ_RETRIES
        for delay, (low, high) in zip(delays, [(0.5, 1), (1, 2), (2, 4)]):
            assert low <= delay <= high

    def test_should_wait_as_long_as_retry_after_asks(self, mocker, sleep):
        self.client._http_client.request.side_effect = [self.failure(mocker, 429, {'Retry-After': '7'}), *paged_responses(mocker, self.entities, 2, tokens=True)]
        self.client.get('Test')
        assert sleep.call_args_list[0].args[0] == 7

    def test_should_back_off_when_retry_after_is_a_date(self, mocker, sleep):
        self.client._http_client.request.side_effect = [self.failure(mocker, 503, {'Retry-After': 'Wed, 21 Oct 2026 07:28:00 GMT'}), *paged_responses(mocker, self.entities, 2, tokens=True)]
        self.client.get('Test')
        assert 0.5 <= sleep.call_args_list[0].args[0] <= 1

    def test_should_not_retry_a_gql_query(self, mocker):
        # A Cypher query may write, so it is not safe to repeat.
        self.client._http_client.request.return_value = self.failure(mocker, 503)
        with pytest.raises(HttpStatusError):
            self.client.query(gql='MATCH (n) RETURN n')
        assert self.client._http_client.request.call_count == 1

    @pytest.mark.parametrize('write', [
        lambda c: c.delete('Test', '1'),
        lambda c: c.create_dataset(Dataset(name='1', project='ds')),
        lambda c: c.clear_dataset('ds'),
    ], ids=['delete', 'create_dataset', 'clear_dataset'])
    def test_should_not_retry_a_write(self, mocker, write):
        self.client._http_client.request.return_value = self.failure(mocker, 503)
        with pytest.raises(HttpStatusError):
            write(self.client)
        assert self.client._http_client.request.call_count == 1

    def test_should_not_retry_a_put_batch_that_timed_out(self, mocker):
        self.client._http_client.request.return_value = self.failure(mocker, 504)
        result = self.client.put('ds', [{'id': 'a'}])
        assert result.timed_out == [TimedOutWrite(0, 'a')]
        assert self.client._http_client.request.call_count == 1


# Failure Tests for Other Operations
class TestOtherOperationFailures:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret', max_read_retries=0)
        self.schema = DatagraphsSchema.create_from(TestSchemaOperations.SCHEMA_DATA)

    @pytest.mark.parametrize('mode', list(SCHEMA_APPLY_MODE))
    @pytest.mark.parametrize('status', [500, 503, 504])
    def test_should_raise_rather_than_report_success_when_schema_apply_fails(self, mocker, mode, status):
        self.client._http_client.request.return_value = create_response_mock(mocker, status, text='failed')
        with pytest.raises(HttpStatusError) as exc_info:
            self.client.apply_schema(self.schema, mode)
        assert exc_info.value.status_code == status

    def test_should_report_no_dependencies_when_validation_passes(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        assert self.client.apply_schema(self.schema, SCHEMA_APPLY_MODE.VALIDATE_ONLY) == []

    def test_should_raise_when_schema_is_rejected_without_dependency_details(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 400, {'message': 'Invalid schema'})
        with pytest.raises(DatagraphsError, match='Invalid schema'):
            self.client.apply_schema(self.schema)

    def test_should_return_nothing_from_a_successful_delete(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, {'deleted': 1})
        assert self.client.delete('Test', '1') is None

    def test_should_report_unknown_status_when_the_service_is_unavailable(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 504, text='timeout')
        assert self.client.status() == 'unknown'

    def test_should_still_raise_authentication_failure_from_status_check(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 401)
        with pytest.raises(AuthenticationError):
            self.client.status()

    def test_should_list_datasets_across_pages(self, mocker):
        datasets = [{'id': 'urn:ds:1', 'name': '1'}, {'id': 'urn:ds:2', 'name': '2'}, {'id': 'urn:ds:3', 'name': '3'}]
        self.client._http_client.request.side_effect = paged_responses(mocker, datasets, 2, tokens=True)
        assert [d.name for d in self.client.get_datasets()] == ['1', '2', '3']
        assert query_params(requested_urls(self.client)[1])['nextPageToken'] == 'token-1'

    @pytest.mark.parametrize('drop_datasets', [True, False])
    def test_should_raise_rather_than_tear_down_nothing_when_datasets_cannot_be_listed(self, mocker, drop_datasets):
        self.client._http_client.request.return_value = create_response_mock(mocker, 504, text='timeout')
        with pytest.raises(HttpStatusError):
            self.client.tear_down(drop_datasets=drop_datasets)
        assert self.client._http_client.request.call_count == 1

    @pytest.mark.parametrize('operation', [
        lambda c: c.clear_dataset('ds'),
        lambda c: c.drop_dataset('ds'),
        lambda c: c.create_dataset(Dataset(name='1', project='ds')),
        lambda c: c.update_dataset(Dataset(name='1', project='ds')),
    ], ids=['clear_dataset', 'drop_dataset', 'create_dataset', 'update_dataset'])
    def test_should_raise_when_a_dataset_operation_times_out(self, mocker, operation):
        self.client._http_client.request.return_value = create_response_mock(mocker, 504, text='timeout')
        with pytest.raises(HttpStatusError):
            operation(self.client)

    @pytest.mark.parametrize('timeout', [
        lambda m: create_response_mock(m, 504, text='timeout'),
        lambda m: requests.exceptions.ReadTimeout('read timed out'),
    ], ids=['504', 'read timeout'])
    def test_should_apply_datasets_when_a_timed_out_create_is_confirmed_by_polling(self, mocker, timeout):
        applied = [{'name': '1', 'project': 'ds'}]
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page([], 0)),
            timeout(mocker),
            create_response_mock(mocker, 200, search_page(applied, 1)),
        ]
        self.client.apply_datasets([Dataset(name='1', project='ds')], timeout_s=1)
        assert self.client._http_client.request.call_count == 3

    def test_should_raise_when_a_timed_out_create_is_never_confirmed(self, mocker):
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page([], 0)),
            create_response_mock(mocker, 504, text='timeout'),
            create_response_mock(mocker, 200, search_page([], 0)),
        ]
        with pytest.raises(DatagraphsError, match='Failed to apply datasets within timeout'):
            self.client.apply_datasets([Dataset(name='1', project='ds')], timeout_s=0)

    @pytest.mark.parametrize('failure', [
        lambda m: create_response_mock(m, 500, text='boom'),
        lambda m: create_response_mock(m, 400, {'errors': ['invalid dataset']}),
        lambda m: requests.exceptions.ConnectionError('reset'),
    ], ids=['500', '400', 'connection error'])
    def test_should_raise_at_once_when_a_dataset_create_fails_other_than_by_timing_out(self, mocker, failure):
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page([], 0)),
            failure(mocker),
        ]
        with pytest.raises(DatagraphsError):
            self.client.apply_datasets([Dataset(name='1', project='ds'), Dataset(name='2', project='ds')], timeout_s=1)
        assert self.client._http_client.request.call_count == 2


# Token Failure Tests
#
# A request that needs a token is never sent when the token cannot be
# obtained, so the token's failure must not be read as the request's outcome.
class TestTokenFailures:

    TOKEN = {'token_type': TOKEN_TYPE, 'access_token': ACCESS_TOKEN}

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret', batch_size=2)

    def token(self, mocker, access_token: str = ACCESS_TOKEN):
        return create_response_mock(mocker, 200, {'token_type': TOKEN_TYPE, 'access_token': access_token})

    @pytest.mark.parametrize('failure', [
        lambda m: requests.exceptions.ReadTimeout('read timed out'),
        lambda m: create_response_mock(m, 504, text='timeout'),
        lambda m: create_response_mock(m, 500, text='boom'),
    ], ids=['read timeout', '504', '500'])
    def test_should_raise_rather_than_report_unsent_writes_as_timed_out(self, mocker, failure):
        failing = failure(mocker)
        self.client._http_client.post.side_effect = failing if isinstance(failing, Exception) else [failing]
        with pytest.raises(TokenRequestError):
            self.client.put('ds', generate_test_data_list(5))
        self.client._http_client.request.assert_not_called()

    def test_should_raise_when_a_token_refresh_during_a_write_fails(self, mocker):
        self.client._http_client.post.side_effect = [self.token(mocker), requests.exceptions.ReadTimeout('read timed out')]
        self.client._http_client.request.return_value = create_response_mock(mocker, 401)
        with pytest.raises(TokenRequestError):
            self.client.put('ds', generate_test_data_list(5))
        assert self.client._http_client.request.call_count == 1

    def test_should_raise_at_once_rather_than_poll_when_a_token_refresh_during_apply_datasets_fails(self, mocker):
        self.client._http_client.post.side_effect = [self.token(mocker), requests.exceptions.ReadTimeout('read timed out')]
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page([], 0)),
            create_response_mock(mocker, 401),
        ]
        with pytest.raises(TokenRequestError):
            self.client.apply_datasets([Dataset(name='1', project='ds')], timeout_s=300)
        assert self.client._http_client.request.call_count == 2

    def test_should_retry_a_read_whose_token_request_failed_transiently(self, mocker):
        self.client._http_client.post.side_effect = [requests.exceptions.ConnectionError('reset'), self.token(mocker)]
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, search_page([{'id': 'a'}], 1))
        assert self.client.get('Test') == [{'id': 'a'}]
        assert self.client._http_client.request.call_args.kwargs['headers']['Authorization'] == f'{TOKEN_TYPE} {ACCESS_TOKEN}'

    def test_should_send_a_retried_read_with_the_refreshed_token(self, mocker):
        self.client._http_client.post.side_effect = [self.token(mocker, 'first'), self.token(mocker, 'second')]
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 503, text='unavailable'),
            create_response_mock(mocker, 401),
            create_response_mock(mocker, 200, search_page([{'id': 'a'}], 1)),
        ]
        assert self.client.get('Test') == [{'id': 'a'}]
        sent = [c.kwargs['headers']['Authorization'] for c in self.client._http_client.request.call_args_list]
        assert sent == [f'{TOKEN_TYPE} first', f'{TOKEN_TYPE} first', f'{TOKEN_TYPE} second']


# Further Read Integrity Tests
class TestReadIntegrity:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2)

    def test_should_retry_a_read_whose_body_was_cut_off(self, mocker):
        self.client._http_client.request.side_effect = [
            requests.exceptions.ChunkedEncodingError('connection broken: incomplete read'),
            create_response_mock(mocker, 200, search_page([{'id': 'a'}], 1)),
        ]
        assert self.client.get('Test') == [{'id': 'a'}]

    @pytest.mark.parametrize('later_total', [4, 6])
    def test_should_raise_when_the_total_changes_during_a_read(self, mocker, later_total):
        # Results without ids: only the changed total reveals that pages shifted.
        rows = [{'name': f'n{i}'} for i in range(5)]
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page(rows[0:2], 5)),
            create_response_mock(mocker, 200, search_page(rows[1:3], later_total)),
        ]
        with pytest.raises(IncompleteReadError, match=f'total changed from 5 to {later_total}') as exc_info:
            self.client.query_all(fields='name')
        assert exc_info.value.expected == 5

    def test_should_answer_a_status_check_without_retrying(self, mocker, sleep):
        self.client._http_client.request.return_value = create_response_mock(mocker, 503, text='unavailable')
        assert self.client.status() == 'unknown'
        assert self.client._http_client.request.call_count == 1
        sleep.assert_not_called()


# Rejected Batch Isolation Tests
#
# The API mock behaves as DataGraphs does: it rejects a whole batch if any
# entity in it is invalid, writes nothing from a rejected batch, and names only
# the first invalid row in its error.
class TestRejectedBatchIsolation:

    BATCH_SIZE = 16
    ISOLATING_MODES = (ISOLATION_MODE.BISECT, ISOLATION_MODE.INDIVIDUAL)

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret', batch_size=self.BATCH_SIZE)
        self.written: list[str] = []

    def serve(self, mocker, invalid: set[str], override=None) -> None:
        """Answer each PUT as DataGraphs would, given the ids of the *invalid* entities.

        *override(ids)* may return a response (or raise) to replace the normal answer.
        """
        def side_effect(method, url, **kwargs):
            ids = [e['id'] for e in kwargs['json']]
            if override is not None and (response := override(ids)) is not None:
                return response
            first_bad = next((row for row, entity_id in enumerate(ids, start=1) if entity_id in invalid), None)
            if first_bad is not None:
                return create_response_mock(mocker, 400, {'errors': [f'Row {first_bad}: {ids[first_bad - 1]} is invalid']})
            self.written.extend(ids)
            return create_response_mock(mocker, 201)
        self.client._http_client.request.side_effect = side_effect

    def sent(self) -> list[list[str]]:
        return [[e['id'] for e in c.kwargs['json']] for c in self.client._http_client.request.call_args_list]

    def assert_isolated(self, result, entities: list[dict], invalid: set[str]) -> None:
        """Every invalid entity, and only those, is reported as rejected with its own error; every other is written once."""
        ids = [e['id'] for e in entities]
        assert [f.index for f in result.failed] == sorted(ids.index(i) for i in invalid)
        for failure in result.failed:
            assert failure.entity_id == ids[failure.index]
            assert failure.status_code == 400
            assert failure.errors == [f'Row 1: {failure.entity_id} is invalid']
            assert [failure.entity_id] in self.sent()  # rejected on its own, not by inference
        assert result.timed_out == []
        assert result.loaded == len(entities) - len(invalid)
        assert sorted(self.written) == sorted(set(ids) - invalid)  # each valid entity written exactly once

    INVALID_SETS: ClassVar[dict] = {
        'first': {0}, 'middle': {7}, 'last': {15}, 'two apart': {3, 12}, 'adjacent run': {4, 5, 6, 7},
        'every other': set(range(0, 16, 2)), 'all': set(range(16)),
    }

    @pytest.mark.parametrize('isolation', ISOLATING_MODES)
    @pytest.mark.parametrize('positions', INVALID_SETS.values(), ids=INVALID_SETS.keys())
    def test_should_report_exactly_the_invalid_entities_with_their_own_errors(self, mocker, isolation, positions):
        entities = generate_test_data_list(self.BATCH_SIZE)
        invalid = {f'entity-{i}' for i in positions}
        self.serve(mocker, invalid)
        result = self.client.put('ds', entities, isolation=isolation)
        self.assert_isolated(result, entities, invalid)

    @pytest.mark.parametrize('seed', range(20))
    @pytest.mark.parametrize('density', [0.02, 0.1, 0.3, 0.7])
    def test_should_isolate_correctly_within_the_request_bound_for_any_rejections(self, mocker, get_client, seed, density):
        size = 64
        self.client = get_client('test_client_id', 'test_client_secret', batch_size=size)
        rng = random.Random(seed)
        entities = generate_test_data_list(size * 3)
        invalid = {e['id'] for e in entities if rng.random() < density}
        self.serve(mocker, invalid)
        result = self.client.put('ds', entities)
        self.assert_isolated(result, entities, invalid)
        # Per batch: the batch itself, plus at most one request per entity and the cost of isolating one more.
        assert len(self.sent()) <= 3 * (1 + size + 2 * math.ceil(math.log2(size)))

    def test_should_isolate_one_rejected_entity_in_about_two_requests_per_halving(self, mocker):
        self.serve(mocker, {'entity-9'})
        result = self.client.put('ds', generate_test_data_list(self.BATCH_SIZE))
        assert len(self.sent()) <= 1 + 2 * math.ceil(math.log2(self.BATCH_SIZE))
        assert [f.index for f in result.failed] == [9]

    def test_should_bisect_by_default(self, mocker):
        self.serve(mocker, {'entity-9'})
        self.client.put('ds', generate_test_data_list(self.BATCH_SIZE))
        assert len(self.sent()) < 1 + self.BATCH_SIZE

    def test_should_resubmit_each_entity_on_its_own_when_asked(self, mocker):
        self.serve(mocker, {'entity-9'})
        self.client.put('ds', generate_test_data_list(self.BATCH_SIZE), isolation=ISOLATION_MODE.INDIVIDUAL)
        assert self.sent()[1:] == [[f'entity-{i}'] for i in range(self.BATCH_SIZE)]

    def test_should_report_the_whole_batch_as_failed_without_resubmitting_when_asked(self, mocker):
        self.serve(mocker, {'entity-9'})
        result = self.client.put('ds', generate_test_data_list(self.BATCH_SIZE), isolation=ISOLATION_MODE.NONE)
        assert len(self.sent()) == 1
        assert result.failed == [FailedWrite(i, f'entity-{i}', 400, ['Row 10: entity-9 is invalid']) for i in range(self.BATCH_SIZE)]
        assert result.loaded == 0

    def test_should_cost_little_more_than_one_at_a_time_when_every_entity_is_rejected(self, mocker):
        self.serve(mocker, {f'entity-{i}' for i in range(self.BATCH_SIZE)})
        self.client.put('ds', generate_test_data_list(self.BATCH_SIZE))
        assert len(self.sent()) <= 1 + self.BATCH_SIZE + 2 * math.ceil(math.log2(self.BATCH_SIZE))

    def test_should_resubmit_one_at_a_time_once_rejections_prove_dense(self, mocker):
        entities = generate_test_data_list(self.BATCH_SIZE * 3)
        self.serve(mocker, {e['id'] for e in entities})
        self.client.put('ds', entities)
        sent = self.sent()
        second_batch_start = next(n for n, ids in enumerate(sent) if ids[0] == f'entity-{self.BATCH_SIZE}' and len(ids) == self.BATCH_SIZE)
        # After the first batch, each all-rejected batch costs exactly one request per entity.
        assert len(sent) - second_batch_start == 2 * (1 + self.BATCH_SIZE)

    def test_should_keep_bisecting_while_rejections_stay_sparse(self, mocker):
        entities = generate_test_data_list(self.BATCH_SIZE * 3)
        self.serve(mocker, {'entity-3', 'entity-20', 'entity-40'})
        result = self.client.put('ds', entities)
        assert len(self.sent()) <= 3 * (1 + 2 * math.ceil(math.log2(self.BATCH_SIZE)))
        assert [f.index for f in result.failed] == [3, 20, 40]

    @pytest.mark.parametrize('timeout', [
        lambda m: create_response_mock(m, 504, text='timeout'),
        lambda m: (_ for _ in ()).throw(requests.exceptions.ReadTimeout('read timed out')),
    ], ids=['504', 'read timeout'])
    def test_should_report_a_part_that_times_out_as_timed_out_without_splitting_or_retrying_it(self, mocker, timeout):
        # entity-3 is invalid, so the batch is rejected; the half holding entity-12 then times out.
        timed_out_part = []
        def override(ids):
            if 'entity-12' in ids and len(ids) < self.BATCH_SIZE:
                timed_out_part.append(ids)
                return timeout(mocker)
            return None
        self.serve(mocker, {'entity-3'}, override)
        result = self.client.put('ds', generate_test_data_list(self.BATCH_SIZE))
        assert len(timed_out_part) == 1
        assert [t.index for t in result.timed_out] == [int(i.split('-')[1]) for i in timed_out_part[0]]
        assert [f.index for f in result.failed] == [3]
        assert result.loaded + len(result.failed) + len(result.timed_out) == self.BATCH_SIZE

    def test_should_report_a_part_that_fails_for_another_reason_as_failed_without_splitting_it(self, mocker):
        failed_part = []
        def override(ids):
            if 'entity-12' in ids and len(ids) < self.BATCH_SIZE:
                failed_part.append(ids)
                return create_response_mock(mocker, 500, text='boom')
            return None
        self.serve(mocker, {'entity-3'}, override)
        result = self.client.put('ds', generate_test_data_list(self.BATCH_SIZE))
        assert len(failed_part) == 1
        server_failures = [f for f in result.failed if f.status_code == 500]
        assert [f.entity_id for f in server_failures] == failed_part[0]
        assert all(f.errors == ['boom'] for f in server_failures)

    def test_should_write_every_entity_of_a_batch_rejected_only_for_its_size(self, mocker):
        self.serve(mocker, set(), lambda ids: create_response_mock(mocker, 413, text='Payload Too Large') if len(ids) > 4 else None)
        result = self.client.put('ds', generate_test_data_list(self.BATCH_SIZE))
        assert result.ok
        assert result.loaded == self.BATCH_SIZE
        assert sorted(self.written) == sorted(f'entity-{i}' for i in range(self.BATCH_SIZE))

    def test_should_stop_when_authentication_fails_during_isolation(self, mocker):
        self.serve(mocker, {'entity-3'}, lambda ids: create_response_mock(mocker, 401) if len(ids) < self.BATCH_SIZE else None)
        with pytest.raises(AuthenticationError):
            self.client.put('ds', generate_test_data_list(self.BATCH_SIZE * 2))
        assert all(len(ids) == self.BATCH_SIZE or 'entity-0' in ids for ids in self.sent())



# Query Encoding Tests
#
# Each value must reach the API exactly as given: decoding the query string the
# way a server does must give back every parameter unchanged, with no value
# split or merged by an unencoded '&', '=' or '#'.
AWKWARD_VALUES = {
    'ampersand': 'nickname:Tom & Jerry',
    'equals and hash': 'code:a=b#c',
    'plus and percent': 'range:now+1d %20',
    'quotes and brackets': 'name:"O\'Brien" AND (type:Cat OR type:Dog)',
    'slash and question mark': 'path:/a/b?c',
    'non-ascii': 'name:Müller – 東京',
    'plain filter': 'type:Cat,Dog',
}

class TestQueryEncoding:

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2, max_read_retries=0)

    def sent_params(self, n: int = 0) -> dict[str, list[str]]:
        query = urllib.parse.urlsplit(requested_urls(self.client)[n]).query
        return urllib.parse.parse_qs(query, keep_blank_values=True)

    @pytest.mark.parametrize('value', AWKWARD_VALUES.values(), ids=AWKWARD_VALUES.keys())
    @pytest.mark.parametrize('read', [
        lambda c, v: c.query(filters=v),
        lambda c, v: c.query_page(filters=v),
        lambda c, v: c.query_all(filters=v),
    ], ids=['query', 'query_page', 'query_all'])
    def test_should_send_filters_exactly_as_given(self, mocker, read, value):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, search_page([], 0))
        read(self.client, value)
        assert self.sent_params()['filter'] == [value]

    @pytest.mark.parametrize('value', AWKWARD_VALUES.values(), ids=AWKWARD_VALUES.keys())
    def test_should_send_every_query_value_exactly_as_given(self, mocker, value):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, search_page([], 0))
        self.client.query(q=value, filters=value, facets=value, date_facets=value, fields=value,
                          embed=value, sort=value, lang=value, next_page_token=value)
        params = self.sent_params()
        for name in ('q', 'filter', 'facets', 'dateFacets', 'fields', 'embed', 'sort', 'lang', 'nextPageToken'):
            assert params[name] == [value], name
        assert self.client._http_client.request.call_args.kwargs['headers']['Accept-Language'] == value

    def test_should_send_ids_exactly_as_given(self, mocker):
        ids = 'urn:pydg:Cat:a&b,urn:pydg:Cat:c#d'
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, [])
        self.client.query(ids=ids)
        assert self.sent_params()['ids'] == [ids]

    def test_should_keep_filters_on_every_page_exactly_as_given(self, mocker):
        value = AWKWARD_VALUES['ampersand']
        self.client._http_client.request.side_effect = paged_responses(mocker, generate_test_data_list(5), 2, tokens=True)
        self.client.query_all(filters=value)
        assert [self.sent_params(n)['filter'] for n in range(3)] == [[value]] * 3

    def test_should_send_page_tokens_exactly_as_given(self, mocker):
        # A base64 token can hold '+', '/' and '=': an unencoded '+' would arrive as a space.
        token = 'ab+cd/ef=='
        entities = generate_test_data_list(4)
        self.client._http_client.request.side_effect = [
            create_response_mock(mocker, 200, search_page(entities[:2], 4, token)),
            create_response_mock(mocker, 200, search_page(entities[2:], 4, 'next')),
        ]
        assert self.client.get('Test') == entities
        assert self.sent_params(1)['nextPageToken'] == [token]

    def test_should_send_class_filter_exactly_as_given_when_clearing_a_dataset(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.clear_class_from_dataset('ds', 'Odd&Class')
        assert self.sent_params()['filter'] == ['type:Odd&Class']

    def test_should_keep_filter_syntax_readable(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, search_page([], 0))
        self.client.query(filters='type:Cat,Dog')
        assert '&filter=type:Cat,Dog' in requested_urls(self.client)[0]
