import pytest
import json
import os
from pathlib import Path
from datagraphs.enums import VALIDATION_MODE
from lib import get_client, get_data, get_datasets, get_gateway, get_schema

DATA_DIR = Path(__file__).parent / 'data'
WORKING_DIR = DATA_DIR / 'tmp'

def write_json(data: list[dict], filename: str, folder: str = WORKING_DIR) -> None:
    folder = Path(folder)
    folder.mkdir(parents=True, exist_ok=True)
    file_path = folder / f"{filename}.json"
    with open(file_path, 'w', encoding='utf-8') as data_file:
        json.dump(data, data_file, ensure_ascii=False, indent=2)

def read_json(filename: str, folder: str = WORKING_DIR) -> list[dict]:
    filename += '.json' if not filename.endswith('.json') else ''
    folder = Path(folder)
    file_path = folder / filename
    if file_path.is_file():
        with open(file_path, 'r', encoding='utf-8') as data_file:
            return json.load(data_file)
    else:
        raise ValueError('Could not find file ' + str(file_path))

def delete_file(file_path: str | Path) -> None:
    file_path = Path(file_path)
    if file_path.is_file():
        file_path.unlink()

class TestGatewayProjectOperations:

    @pytest.fixture(scope="class",autouse=True)
    def setup(self, request):
        WORKING_DIR.mkdir(exist_ok=True)
        request.cls.gateway = get_gateway('integration-testing')
        yield
        for file in WORKING_DIR.iterdir():
            if file.is_file():
                file.unlink()

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
        self.gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))
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
        delete_file(WORKING_DIR / 'Cat.json')
        delete_file(WORKING_DIR / 'Dog.json')

    def test_should_load_data_from_file(self) -> None:
        cats = get_data('Cat')
        write_json(cats, 'Cat')
        stats = self.gateway.load_data(class_name='Cat', from_dir_path=str(WORKING_DIR))
        assert stats['loaded'] == len(cats)

    def test_should_dump_data_to_file(self) -> None:
        self.client.put('pets', get_data('Dog'))
        stats = self.gateway.dump_data(to_dir_path=str(WORKING_DIR), class_name='Dog')
        dogs = read_json('Dog')
        assert stats['exported'] == len(dogs)
