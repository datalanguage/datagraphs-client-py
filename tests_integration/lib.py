import os
import yaml
from datagraphs.client import Client as DatagraphsClient
from datagraphs.gateway import Gateway as DatagraphsGateway 
from datagraphs.enums import DATATYPE
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.dataset import Dataset

current_folder = os.path.dirname(os.path.realpath(__file__))
config_file_location = os.path.join(current_folder, '.app.config.yml')

def get_gateway(config_key: str) -> DatagraphsGateway:
    client = get_client(config_key)
    gateway = DatagraphsGateway(client)
    return gateway

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
    schema.create_property("Dog", "nickname", description="Nickname", datatype=DATATYPE.KEYWORD)
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

def get_data(type_filter="") -> list[dict]:
    data = [
        {
            "name": { 
                "en": "Henry",
                "de": "Heinrich"
            },
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
    if type_filter:
        data = [item for item in data if item["type"] == type_filter]
    return data
