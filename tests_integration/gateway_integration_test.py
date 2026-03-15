import pytest
import json
import os
from datagraphs.enums import VALIDATION_MODE
from lib import get_client, get_data, get_datasets, get_gateway, get_schema

def write_json(data: list[dict], filename: str, folder: str = './') -> None:
    if not folder.endswith('/'):
        folder = folder + '/'
    with open(folder+filename+'.json', 'w', encoding='utf-8') as data_file:
        json.dump(data, data_file, ensure_ascii=False, indent=2)

def read_json(filename: str, folder: str = './') -> list[dict]:
    filename+='.json' if not filename.endswith('.json') else ''
    folder+='/' if not folder.endswith('/') else ''
    file_path = folder+filename
    if os.path.isfile(file_path):
        with open(file_path, 'r', encoding='utf-8') as dataFile:
            return json.load(dataFile)
    else:
        raise ValueError('Could not find file '+file_path)

def delete_file(file_path) -> None:
    if os.path.isfile(file_path):
        os.remove(file_path)

class TestGatewayProjectOperations:

    @pytest.fixture(scope="class",autouse=True)
    def setup(self, request):
        request.cls.gateway = get_gateway('integration-testing')
        yield
        delete_file('./pydg-v1.0-schema.json')
        delete_file('./pydg-v1.0-datasets.json')

    def test_should_load_project(self) -> None:
        self.gateway.load_project(get_schema(), get_datasets(), VALIDATION_MODE.BYPASS)
        schema = self.gateway.client.get_schema()
        assert len(schema.classes) == len(get_schema().classes)
        datasets = self.gateway.client.get_datasets()
        assert [dataset.name for dataset in datasets] == [dataset.name for dataset in get_datasets()]
        assert [dataset.classes for dataset in datasets] == [dataset.classes for dataset in get_datasets()]

    def test_should_dump_project(self) -> None:
        client = get_client('integration-testing')
        client.apply_schema(get_schema())
        client.tear_down() 
        client.apply_datasets(get_datasets())
        self.gateway.dump_project(schema_path='./', datasets_path='./')
        schema = read_json('pydg-v1.0-schema')
        assert len(schema['classes']) == len(get_schema().classes)
        datasets = read_json('pydg-v1.0-datasets')
        assert len(datasets) == len(get_datasets())

class TestGatewayDataOperations:

    @pytest.fixture(scope="class",autouse=True)
    def setup(self, request):
        client = get_client('integration-testing')
        client.apply_schema(get_schema())
        client.tear_down() 
        client.apply_datasets(get_datasets())
        request.cls.client = client
        request.cls.gateway = get_gateway('integration-testing')

    @pytest.fixture(scope="function",autouse=True)
    def fn_setup(self):
        self.client.clear_dataset('pets')
        yield
        delete_file('./Cat.json')
        delete_file('./Dog.json')

    def test_should_load_data_from_file(self) -> None:
        cats = get_data('Cat')
        write_json(cats, 'Cat')
        stats = self.gateway.load_data(class_name='Cat', from_dir_path='./')
        assert stats['loaded'] == len(cats)

    def test_should_dump_data_to_file(self) -> None:
        self.client.put('pets', get_data('Dog'))
        stats = self.gateway.dump_data(to_dir_path='./', class_name='Dog')
        dogs = read_json('Dog')
        assert stats['exported'] == len(dogs)
