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
            "name": "Fluffy",
            "naps": 5,
            "meals": 3,
            "type": "Cat"
        },
        {
            "name": "Spot",
            "walks": 2,
            "toys": ["Bone"],
            "type": "Dog"
        }
    ]

@pytest.fixture(scope="module",autouse=True)
def setup():
    client = get_client('integration-testing')
    client.tear_down() 
    client.apply_schema(get_empty_schema())

class TestClient:
 
    def test_should_verify_service_status(self) -> None:
        client = get_client('integration-testing')
        assert client.status() == 'OK'

    def test_should_apply_schema(self) -> None:
        client = get_client('integration-testing')
        schema = get_schema()
        client.apply_schema(schema)

    def test_should_apply_datasets(self) -> None:
        client = get_client('integration-testing')
        schema = get_schema()
        client.apply_schema(schema)
        datasets = get_datasets()
        client.apply_datasets(datasets)

class TestDataOperations:

    @pytest.fixture(scope="function",autouse=True)
    def setup(self):
        self.client = get_client('integration-testing')
        schema = get_schema()
        self.client.apply_schema(schema)
        datasets = get_datasets()
        self.client.apply_datasets(datasets)

    def test_should_create_data(self) -> None:
        self.client.put('pets', get_data())

