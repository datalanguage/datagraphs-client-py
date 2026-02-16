import json
import pytest
from datagraphs.client import Client as DatagraphsClient, AuthenticationError, DatagraphsError
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.dataset import Dataset

TOKEN_TYPE = 'Bearer'
ACCESS_TOKEN = 'test_token'

# Fixtures for common test setup
@pytest.fixture(scope="function")
def mock_http_client(mocker):
    """Create a mock HTTP client."""
    return mocker.MagicMock()

@pytest.fixture(scope="function") 
def get_client(mock_http_client, mocker):
    def create_client(client_id='', client_secret='', batch_size: int = -1):
        batch_size = DatagraphsClient.DEFAULT_BATCH_SIZE if batch_size == -1 else batch_size
        if client_id and client_secret:
            client = DatagraphsClient("test_project", "test_api_key", client_id, client_secret, batch_size)
            client._http_client = mock_http_client
            data = {'token_type': TOKEN_TYPE, 'access_token': ACCESS_TOKEN}
            client._http_client.post.return_value = create_response_mock(mocker, 200, data)
        else:
            client = DatagraphsClient("test_project", "test_api_key", batch_size=batch_size)
            client._http_client = mock_http_client
        return client
    return create_client  

def create_response_mock(mocker, status_code: int = 200, data: dict = None, reason: str = '', text: str = ''):
    response_mock = mocker.MagicMock()
    response_mock.status_code = status_code
    response_mock.json.return_value = data if data is not None else {}
    response_mock.raise_for_status.return_value = None
    response_mock.reason = reason
    response_mock.text = text
    return response_mock

def get_search_response(data: list) -> dict:
    return {
        'search': { 
            'totalResults': len(data)
        },
        'results': data
    }

def generate_test_data_list(num_entities: int) -> list:
    """Generate a list of test entity dictionaries."""
    return [{'id': f'entity-{i}', 'payload': 'data'} for i in range(num_entities)]

# OAuth Authentication Tests
class TestAuthentication:
    def test_should_get_auth_token_for_fetching_data_if_oauth_creds_are_supplied(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 200)
        client.get('Test')
        args, kwargs = client._http_client.post.call_args
        assert args[0] == "https://api.datagraphs.io/oauth/token"
        assert kwargs['headers']['x-api-key'] == "test_api_key"
        assert kwargs['data'] == '{"clientId": "test_client_id", "clientSecret": "test_client_secret"}'

    def test_should_inject_auth_token_into_subsequent_requests(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 200)
        client.get('Test')
        args, kwargs = client._http_client.request.call_args
        assert kwargs['headers']['Authorization'] == f"{TOKEN_TYPE} {ACCESS_TOKEN}"

    def test_should_cache_auth_token_for_multiple_requests(self, get_client, mocker):
        client = get_client('test_client_id', 'test_client_secret')
        client._http_client.request.return_value = create_response_mock(mocker, 200)
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
        client._http_client.request.return_value = create_response_mock(mocker, 200)
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

    def test_should_get_data_for_specified_type_name_from_correct_endpoint(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get('Test')
        args, kwargs = self.client._http_client.request.call_args
        assert args[1].startswith("https://api.datagraphs.io/test_project/_all?filter=type:Test")

    def test_should_get_data_for_specified_type_in_all_languages_by_default(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get('Test')        
        args, kwargs = self.client._http_client.request.call_args
        assert '&lang=all' in args[1]
        assert kwargs['headers']['Accept-Language'] == 'all'

    def test_should_get_data_for_specified_type_in_specified_language(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get(type_name='Test', lang='fr')
        args, kwargs = self.client._http_client.request.call_args
        assert '&lang=fr' in args[1]
        assert kwargs['headers']['Accept-Language'] == 'fr'

    def test_should_use_cache_busting_url_suffix_to_get_data(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get('Test')
        args, kwargs = self.client._http_client.request.call_args
        assert '&t=' in args[1]

    def test_should_request_system_metadata_dates_if_specified(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get(type_name='Test', include_date_fields=True)
        args, kwargs = self.client._http_client.request.call_args
        assert '&includeDateFields=true' in args[1]

    def test_should_not_request_system_metadata_dates_if_unspecified(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get(type_name='Test')
        args, kwargs = self.client._http_client.request.call_args
        assert '&includeDateFields=true' not in args[1]

# Pagination Tests
class TestPagination:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client(batch_size=2)

    def test_should_get_data_at_specified_page_number(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get('Test')
        args, kwargs = self.client._http_client.request.call_args
        assert '&pageNo=1' in args[1]

    def test_should_get_data_with_specified_page_size(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.get('Test')
        args, kwargs = self.client._http_client.request.call_args
        assert '&pageSize=2' in args[1]

    def test_should_batch_get_requests_if_initial_result_count_is_greater_than_batch_size(self, mocker):
        data = get_search_response(['a', 'b', 'c'])
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, data)
        self.client.get('Test')
        assert self.client._http_client.request.call_count == 2
        args, kwargs = self.client._http_client.request.call_args_list[0]
        assert '&pageNo=1&pageSize=2' in args[1]
        args, kwargs = self.client._http_client.request.call_args_list[1]
        assert '&pageNo=2&pageSize=2' in args[1]

# Error Handling Tests
class TestErrorHandling:
    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client()

    def test_should_assume_successful_request_if_gateway_timeout(self, capsys, mocker):
        response = create_response_mock(mocker, 504, reason='Gateway Timeout', text='timeout occurred')
        self.client._http_client.request.return_value = response
        self.client.get('Test')
        captured = capsys.readouterr()
        assert "Gateway Timeout - timeout occurred" in captured.out

    def test_should_raise_error_for_unexpected_status_codes(self, mocker):
        response = create_response_mock(mocker, 500, reason='Internal Server Error', text='Something went wrong')
        self.client._http_client.request.return_value = response        
        with pytest.raises(DatagraphsError, match='Request failed with status 500'):
            self.client.get('Test')

# Query Tests
class TestQuery:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client()

    def test_should_support_query_by_dataset(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response(['a', 'b']))
        self.client.query(dataset='test-dataset')
        args, kwargs = self.client._http_client.request.call_args
        assert args[1].startswith('https://api.datagraphs.io/test_project/test-dataset?')

    def test_should_support_query_by_search_phrase(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(q='my query')
        args, kwargs = self.client._http_client.request.call_args
        assert "&q=my+query" in args[1]

    def test_should_support_query_filters(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(filters='type:Person')
        args, kwargs = self.client._http_client.request.call_args
        assert "&filter=type:Person" in args[1]

    def test_should_support_faceted_search(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(facets='condition,intervention', facet_size=20)
        args, kwargs = self.client._http_client.request.call_args
        assert "&facets=condition,intervention&facetSize=20" in args[1]

    def test_should_support_faceted_search_with_default_facet_size(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(facets='condition,intervention')
        args, kwargs = self.client._http_client.request.call_args
        assert "&facets=condition,intervention&facetSize=10" in args[1]

    def test_should_support_requested_embed_level_in_search_results(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(embed='2')        
        args, kwargs = self.client._http_client.request.call_args
        assert "&embed=2" in args[1]

    def test_should_support_search_results_sorting(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(sort='label:asc')
        args, kwargs = self.client._http_client.request.call_args
        assert "&sort=label:asc" in args[1]

    def test_should_support_query_by_id(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(ids='urn:1,urn:2')
        args, kwargs = self.client._http_client.request.call_args
        assert "&ids=urn:1,urn:2" in args[1]

    def test_should_request_query_results_in_all_languages_by_default(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(q='test')
        args, kwargs = self.client._http_client.request.call_args
        assert '?lang=all' in args[1]
        assert kwargs['headers']['Accept-Language'] == 'all'

    def test_should_return_query_results_in_specified_language(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(q='test', lang='fr')
        args, kwargs = self.client._http_client.request.call_args
        assert '?lang=fr' in args[1]
        assert kwargs['headers']['Accept-Language'] == 'fr'

    def test_should_support_paginated_queries(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(q='test', page_no=2, page_size=25)
        args, kwargs = self.client._http_client.request.call_args
        assert '&pageNo=2&pageSize=25' in args[1]

    def test_should_support_token_based_pagination(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(q='test', next_page_token='test-token', page_size=25)
        args, kwargs = self.client._http_client.request.call_args
        assert '&nextPageToken=test-token' in args[1]        
        self.client.query(q='test', previous_page_token='test-token', page_size=25)
        args, kwargs = self.client._http_client.request.call_args
        assert '&previousPageToken=test-token' in args[1]

    def test_should_request_system_metadata_dates_in_query_results_if_specified(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        self.client.query(q='test', include_date_fields=True)
        args, kwargs = self.client._http_client.request.call_args
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
        second_call_args, second_call_kwargs = self.client._http_client.request.call_args_list[1]
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

class TestUtilityFunctions:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client()

    def test_should_get_type_from_urn(self):    
        type_name = self.client.get_type_from_urn('urn:dg:Test:1234')
        assert type_name == 'Test'

    def test_should_get_id_from_urn(self):    
        entity_id = self.client.get_id_from_urn('urn:dg:Test:1234')
        assert entity_id == '1234'

    def test_should_raise_value_error_for_malformed_urn(self):    
        with pytest.raises(ValueError, match='Invalid URN: invalid-urn-format'):
            self.client.get_type_from_urn('invalid-urn-format')
        with pytest.raises(ValueError, match='Invalid URN: urn:urn-test:x'):
            self.client.get_id_from_urn('urn:urn-test:x')

    def test_should_map_project_name_for_simple_entity(self):
        obj = {'id': 'urn:projectA:Test:abc', 'payload': 'data'}
        mapped_obj = self.client.map_project_name(obj, from_urn='urn:projectA', to_urn='urn:projectB')
        assert mapped_obj['id'] == 'urn:projectB:Test:abc'

    def test_should_map_project_name_in_entity_collections(self):
        objs = [
            {'id': 'urn:projectA:Test:1', 'payload': 'data1'},
            {'id': 'urn:projectA:Test:2', 'payload': 'data2'}
        ]
        mapped_objs = self.client.map_project_name(objs, from_urn='urn:projectA', to_urn='urn:projectB')
        assert mapped_objs[0]['id'] == 'urn:projectB:Test:1'
        assert mapped_objs[1]['id'] == 'urn:projectB:Test:2'

    def test_should_map_project_name_in_nested_entity_collections(self):
        obj = {
            'id': 'urn:projectA:Test:abc', 
            'payload': [
                {'id': 'urn:projectA:Test:1', 'payload': 'data1'},
                {'id': 'urn:projectA:Test:2', 'payload': 'data2'}
            ]
        }
        mapped_obj = self.client.map_project_name(obj, from_urn='urn:projectA', to_urn='urn:projectB')
        assert mapped_obj['id'] == 'urn:projectB:Test:abc'
        assert mapped_obj['payload'][0]['id'] == 'urn:projectB:Test:1'
        assert mapped_obj['payload'][1]['id'] == 'urn:projectB:Test:2'

class TestSchemaOperations:
    
    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret')

    def test_should_get_schema_for_project(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, None)
        self.client.get_schema()
        args, kwargs = self.client._http_client.request.call_args
        assert args[1].startswith("https://api.datagraphs.io/test_project/models/_active?")

    def test_should_apply_schema_to_project(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200)
        schema_data = {
            "id": "urn:models:123",
            "guid": "123",
            "type": "DomainModel",
            "name": "Domain Model",
            "description": "",
            "project": "urn:datagraphs:project",
            "createdDate": "",
            "lastModifiedDate": "",
            "classes": []
        }
        self.client.apply_schema(DatagraphsSchema(schema_data))
        args, kwargs = self.client._http_client.request.call_args
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/models/_active")
        assert json.loads(kwargs['data'])['id'] == schema_data['id']
        assert json.loads(kwargs['data'])['type'] == schema_data['type']

class TestDatasetOperations:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self, get_client):
        self.client = get_client('test_client_id', 'test_client_secret')

    def test_should_list_datasets_in_project(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': 'ds1'}, {'name': 'ds2'}]))
        datasets = self.client.get_datasets()
        args, kwargs = self.client._http_client.request.call_args
        assert args[1].startswith("https://api.datagraphs.io/test_project/?")
        assert datasets[0].name == 'ds1'
        assert datasets[1].name == 'ds2'

    def test_should_create_datasets_when_applying_new_datasets(self, mocker):
        existing_datasets = []
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response(existing_datasets))
        datasets = [Dataset(name='1', project='ds'), Dataset(name='2', project='ds')]
        self.client.apply_datasets(datasets)
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
        self.client.apply_datasets(datasets)
        args, kwargs = self.client._http_client.request.call_args_list[1]
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/datasets/1")
        assert kwargs['json']['id'] == 'urn:ds:1'
        args, kwargs = self.client._http_client.request.call_args_list[2]
        assert args[0] == "put"
        assert args[1].startswith("https://api.datagraphs.io/test_project/datasets/2")
        assert kwargs['json']['id'] == 'urn:ds:2'

    def test_should_delete_data_from_dataset(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]))
        self.client.clear_dataset('1')
        args, kwargs = self.client._http_client.request.call_args_list[0]
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/1?filter=_all"

    def test_should_delete_existing_datasets_during_teardown(self, mocker):
        self.client._http_client.request.return_value = create_response_mock(mocker, 200, get_search_response([{'name': '1', 'project': 'ds'}, {'name': '2', 'project': 'ds'}]))
        self.client.tear_down()
        args, kwargs = self.client._http_client.request.call_args_list[1]
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/datasets/1"
        args, kwargs = self.client._http_client.request.call_args_list[2]
        assert args[0] == "delete"
        assert args[1] == "https://api.datagraphs.io/test_project/datasets/2"

