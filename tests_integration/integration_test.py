import os
import pytest   
import yaml
from datagraphs.client import Client as DatagraphsClient
from datagraphs.datatypes import DATATYPE
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.dataset import Dataset

current_folder = os.path.dirname(os.path.realpath(__file__))
config_file_location = os.path.join(current_folder, '.app.config.yml')

def get_client(config_key: str) -> DatagraphsClient:
    with open(config_file_location, 'r') as config_file:
        configs = yaml.safe_load(config_file)
        if config_key in configs:
            config = configs[config_key]
            dg_client = DatagraphsClient(
                project_name=config['project_name'], 
                api_key=config['api_key'], 
                client_id=config['client_id'], 
                client_secret=config['client_secret']
            )
            return dg_client
        else:
            print("Unrecognised config key - please select from: "+", ".join(config.keys()))

def get_empty_schema() -> DatagraphsSchema:
    schema = DatagraphsSchema(project='pydg')
    return schema

def get_schema() -> DatagraphsSchema:
    schema = DatagraphsSchema(project='pydg')
    schema.create_class("Cat", label_prop_name="name", description="Goes meow")
    schema.create_property("Cat", "nickname", description="Nickname", datatype=DATATYPE.KEYWORD)
    schema.create_property("Cat", "naps", description="Number of naps", datatype=DATATYPE.INTEGER)
    schema.create_property("Cat", "meals", description="Number of meals", datatype=DATATYPE.INTEGER)
    schema.create_class("Dog", label_prop_name="name", description="Goes woof")
    schema.create_property("Dog", "walks", description="Number of walks", datatype=DATATYPE.INTEGER)
    schema.create_property("Dog", "toys", description="Toys", datatype=DATATYPE.TEXT, is_array=True)
    return schema

def get_datasets() -> list[Dataset]:
    return [
        Dataset(
            name='Pets',
            classes=['Cat', 'Dog'],
            project='pydg'
        )
    ]

def get_data() -> list[dict]:
    return [
        {
            "name": "Henry",
            "nickname": "Cat1",
            "naps": 5,
            "meals": 3,
            "type": "Cat"
        },
        {
            "name": "Felix",
            "nickname": "Cat2",
            "naps": 4,
            "meals": 3,
            "type": "Cat"
        },
        {
            "name": "Spotty",
            "nickname": "Spot",
            "walks": 2,
            "toys": ["Postman"],
            "type": "Dog"
        },
        {
            "name": "Terminator",
            "nickname": "Arnie",
            "walks": 3,
            "toys": ["Burglars"],
            "type": "Dog"
        }
    ]

class TestClient:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self):
        self.client = get_client('integration-testing')
        self.client.tear_down() 
        self.client.apply_schema(get_empty_schema())

    def test_should_verify_service_status(self) -> None:
        assert self.client.status() == 'OK'

    def test_should_apply_schema(self) -> None:
        schema = get_schema()
        self.client.apply_schema(schema)
        schema = self.client.get_schema()
        assert len(schema.classes) == 2

    def test_should_apply_datasets(self) -> None:
        schema = get_schema()
        self.client.apply_schema(schema)
        datasets = get_datasets()
        self.client.apply_datasets(datasets)

class TestDataOperations:

    @pytest.fixture(scope="class",autouse=True)
    def setup(self, request):
        request.cls.client = get_client('integration-testing')
        request.cls.client.apply_schema(get_schema())
        request.cls.client.tear_down() 
        request.cls.client.apply_datasets(get_datasets())
        request.cls.client.put('pets', get_data())

    def test_should_create_data(self) -> None:
        assert len(self.client.get(type_name='Cat')) == 2
        assert len(self.client.get(type_name='Dog')) == 2

    def test_should_query_by_dataset(self) -> None:
        results = self.client.query(dataset='pets')
        assert len(results) == 4

    def test_should_query_by_search_phrase(self) -> None:
        results = self.client.query(q='Henry')
        assert len(results) == 1
        assert results[0]['name']['@none'] == 'Henry'

    def test_should_query_with_type_filter(self) -> None:
        results = self.client.query(filters='type:Cat')
        assert len(results) == 2
        assert results[0]['name']['@none'] == 'Henry'
        assert results[1]['name']['@none'] == 'Felix'

    def test_should_query_with_facets(self) -> None:
        results, facets = self.client.query(dataset='pets', facets='type')
        assert len(results) == 4 
        assert facets[0]['buckets'][0]['key'] == 'Cat'
        assert facets[0]['buckets'][0]['count'] == 2
        assert facets[0]['buckets'][1]['key'] == 'Dog'
        assert facets[0]['buckets'][1]['count'] == 2

    def test_should_query_with_custom_facet_size(self) -> None:
        results, facets = self.client.query(dataset='pets', facets='type', facet_size=1)
        assert len(results) == 4
        assert facets[0]['buckets'][0]['key'] == 'Cat'
        assert facets[0]['buckets'][0]['count'] == 2
        assert len(facets[0]['buckets']) == 1

    def test_should_query_with_date_facets(self) -> None:
        results, facets = self.client.query(dataset='pets', date_facets='_createdDate:1d:1w:1M')
        assert len(results) == 4
        assert facets[0]['buckets'][0]['key'] == '1d'
        assert facets[0]['buckets'][0]['count'] == 4
        assert len(facets[0]['buckets']) == 1

    def test_should_query_with_fields(self) -> None:
        results = self.client.query(dataset='pets', fields='name')
        assert len(results) == 4
        for r in results:
            assert 'name' in r and not 'nickname' in r            

    # def test_should_query_with_embed(self) -> None:
    #     results = self.client.query(dataset='pets', embed='1')
    #     assert len(results) == 4

    # def test_should_query_with_sort(self) -> None:
    #     results = self.client.query(dataset='pets', sort='nickname:asc')
    #     assert len(results) == 4
    #     assert results[0]['name']['@none'] == 'Fluffy'
    #     assert results[1]['name']['@none'] == 'Spotty'

    # def test_should_query_with_sort_descending(self) -> None:
    #     results = self.client.query(dataset='pets', sort='nickname:desc')
    #     assert len(results) == 4
    #     assert results[0]['name']['@none'] == 'Spotty'
    #     assert results[1]['name']['@none'] == 'Fluffy'

    # def test_should_query_by_ids(self) -> None:
    #     all_results = self.client.query(dataset='pets')
    #     first_id = all_results[0]['id']
    #     results = self.client.query(ids=first_id)
    #     assert len(results) == 1
    #     assert results[0]['id'] == first_id

    # def test_should_query_with_lang(self) -> None:
    #     results = self.client.query(dataset='pets', lang='en')
    #     assert len(results) == 2

    # def test_should_query_with_page_size(self) -> None:
    #     results = self.client.query(dataset='pets', page_size=1)
    #     assert len(results) == 1

    # def test_should_query_with_page_no(self) -> None:
    #     page1 = self.client.query(dataset='pets', page_size=1, page_no=1)
    #     page2 = self.client.query(dataset='pets', page_size=1, page_no=2)
    #     assert len(page1) == 1
    #     assert len(page2) == 1
    #     assert page1[0]['id'] != page2[0]['id']

    # def test_should_query_with_include_date_fields(self) -> None:
    #     results = self.client.query(dataset='pets', include_date_fields=True)
    #     assert len(results) == 2
    #     for r in results:
    #         assert '_createdDate' in r and '_lastModifiedDate' in r

    # def test_should_query_with_combined_parameters(self) -> None:
    #     results = self.client.query(
    #         dataset='pets',
    #         filters='type:Cat',
    #         fields='name',
    #         sort='label:asc',
    #         page_size=10,
    #         include_date_fields=True
    #     )
    #     assert len(results) == 1
    #     assert results[0]['name']['@none'] == 'Fluffy'



