import json
import logging
import os
import pytest
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch, call
from datagraphs.enums import VALIDATION_MODE
from datagraphs.gateway import Gateway as DatagraphsGateway
from datagraphs.client import Client as DatagraphsClient
from datagraphs.schema import Schema
from datagraphs.dataset import Dataset

DATA_DIR = Path(__file__).parent / 'data'
WORKING_DIR = DATA_DIR / 'tmp'
SUBSTANCE_ROLE_FILE = DATA_DIR / 'SubstanceRole.json'

def load_substance_role_data() -> list[dict]:
    with open(SUBSTANCE_ROLE_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

@pytest.fixture
def substance_role_data():
    return load_substance_role_data()

@pytest.fixture
def mock_client():
    client = MagicMock(spec=DatagraphsClient)
    client.project_name = 'test-project'
    client.get_datasets.return_value = [
        Dataset(name='Test', project='test-project', classes=['ClassA', 'ClassB'])
    ]
    return client

@pytest.fixture
def mock_schema():
    schema = MagicMock(spec=Schema)
    schema.find_subclasses.return_value = []
    return schema

@pytest.fixture
def gateway(mock_client, mock_schema):
    return DatagraphsGateway(mock_client, mock_schema, wait_time_ms=0)

class TestInit:

    def test_should_initialize_with_default_wait_time(self, mock_client, mock_schema):
        gateway = DatagraphsGateway(mock_client, mock_schema)
        assert gateway._client is mock_client
        assert gateway._schema is mock_schema
        assert gateway._wait_time_ms == DatagraphsGateway.DEFAULT_WAIT_TIME_MS

    def test_should_initialize_with_custom_wait_time(self, mock_client, mock_schema):
        gateway = DatagraphsGateway(mock_client, mock_schema, wait_time_ms=500)
        assert gateway._wait_time_ms == 500

    def test_client_property(self, gateway, mock_client):
        assert gateway.client is mock_client

class TestDumpData:

    def setup_method(self):
        WORKING_DIR.mkdir(exist_ok=True)
        for file in WORKING_DIR.iterdir():
            if file.is_file():
                file.unlink()

    def test_should_dump_single_datatype_to_file(self, gateway, mock_client, substance_role_data):
        mock_client.get.return_value = substance_role_data
        result = gateway.dump_data(to_dir_path=str(WORKING_DIR), class_name='SubstanceRole')
        mock_client.get.assert_called_once_with(class_name='SubstanceRole', include_date_fields=False)
        output_file = WORKING_DIR / 'SubstanceRole.json'
        assert output_file.exists()
        with open(output_file, 'r', encoding='utf-8') as f:
            written_data = json.load(f)
        assert written_data == substance_role_data
        assert result == {"exported": len(substance_role_data)}

    def test_should_dump_all_classes_from_all_datasets(self, gateway, mock_client, substance_role_data):
        dataset = Dataset(name='Test Dataset', project='test-project', classes=['SubstanceRole', 'BufferZoneType'])
        mock_client.get_datasets.return_value = [dataset]
        mock_client.get.return_value = substance_role_data
        result = gateway.dump_data(to_dir_path=str(WORKING_DIR))
        mock_client.get_datasets.assert_called_once()
        mock_client.get.assert_any_call(class_name='SubstanceRole', include_date_fields=False)
        mock_client.get.assert_any_call(class_name='BufferZoneType', include_date_fields=False)
        assert result == {"exported": len(substance_role_data) * 2}

    def test_should_skip_baseclasses_when_dumping_all(self, gateway, mock_client, mock_schema):
        dataset = Dataset(name='Test Dataset', project='test-project', classes=['BaseClass', 'SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        mock_schema.find_subclasses.side_effect = lambda name: [{'name': 'Child'}] if name == 'BaseClass' else []
        mock_client.get.return_value = []
        result = gateway.dump_data(to_dir_path=str(WORKING_DIR))
        mock_client.get.assert_called_once()

    def test_should_dump_multiple_datasets(self, gateway, mock_client, mock_schema):
        ds1 = Dataset(name='DS1', project='test-project', classes=['TypeA'])
        ds2 = Dataset(name='DS2', project='test-project', classes=['TypeB'])
        mock_client.get_datasets.return_value = [ds1, ds2]
        mock_schema.find_subclasses.return_value = []
        mock_client.get.return_value = []
        result = gateway.dump_data(to_dir_path=str(WORKING_DIR))
        assert mock_client.get.call_count == 2
        mock_client.get.assert_any_call(class_name='TypeA', include_date_fields=False)
        mock_client.get.assert_any_call(class_name='TypeB', include_date_fields=False)

    def test_should_write_empty_list_when_no_data(self, gateway, mock_client):
        mock_client.get.return_value = []
        output_file = WORKING_DIR / 'EmptyType.json'
        gateway.dump_data(to_dir_path=str(WORKING_DIR), class_name='EmptyType')
        with open(output_file, 'r', encoding='utf-8') as f:
            written_data = json.load(f)
        assert written_data == []
        os.remove(output_file)

    def test_should_create_output_directory_if_not_exists(self, gateway, mock_client):
        mock_client.get.return_value = [{"id": "urn:test:Type:1"}]
        new_dir = WORKING_DIR / 'auto_created'
        if new_dir.exists():
            for f in new_dir.iterdir():
                f.unlink()
            new_dir.rmdir()
        gateway.dump_data(to_dir_path=str(new_dir), class_name='SomeType')
        assert new_dir.exists()
        output_file = new_dir / 'SomeType.json'
        assert output_file.exists()
        os.remove(output_file)
        new_dir.rmdir()

    def test_should_accept_path_object_for_to_dir_path(self, gateway, mock_client):
        mock_client.get.return_value = []
        gateway.dump_data(to_dir_path=WORKING_DIR, class_name='PathTest')
        output_file = WORKING_DIR / 'PathTest.json'
        assert output_file.exists()
        os.remove(output_file)


# ---------- load_data ----------
class TestLoadData:

    def test_should_load_single_datatype_from_file(self, gateway, mock_client, substance_role_data):
        dataset = Dataset(name='Test Dataset', project='test-project', classes=['SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        result = gateway.load_data(
            class_name='SubstanceRole',
            file_path=str(SUBSTANCE_ROLE_FILE)
        )
        mock_client.put.assert_called_once()
        args, kwargs = mock_client.put.call_args
        assert args[0] == dataset.slug
        loaded_entities = args[1]
        assert len(loaded_entities) == len(substance_role_data)
        assert result["loaded"] == len(substance_role_data)
        assert result["skipped"] == 0

    def test_should_load_all_classes_from_all_datasets(self, gateway, mock_client, mock_schema, substance_role_data):
        dataset = Dataset(name='Test Dataset', project='test-project', classes=['SubstanceRole', 'BufferZoneType'])
        mock_client.get_datasets.return_value = [dataset]
        mock_schema.find_subclasses.return_value = []
        with patch.object(Path, 'is_file', return_value=True), \
             patch('builtins.open', mock_open(read_data=json.dumps(substance_role_data))):
            result = gateway.load_data(from_dir_path=str(DATA_DIR))
        assert mock_client.get_datasets.call_count == 1
        assert mock_client.put.call_count == 2
        assert result["loaded"] == len(substance_role_data) * 2

    def test_should_skip_baseclasses_when_loading_all(self, gateway, mock_client, mock_schema, substance_role_data):
        dataset = Dataset(name='Test', project='test', classes=['BaseType', 'SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        mock_schema.find_subclasses.side_effect = lambda name: [{'name': 'Child'}] if name == 'BaseType' else []
        with patch.object(Path, 'is_file', return_value=True), \
             patch('builtins.open', mock_open(read_data=json.dumps(substance_role_data))):
            gateway.load_data(from_dir_path=str(DATA_DIR))
        mock_client.put.assert_called_once()

    def test_should_log_warning_when_file_not_found_for_dir_scan(self, gateway, mock_client, caplog):
        dataset = Dataset(name='Test', project='test', classes=['SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        with caplog.at_level(logging.WARNING):
            result = gateway.load_data(class_name='SubstanceRole', from_dir_path='/nonexistent_path')
        mock_client.put.assert_not_called()
        assert 'No file found' in caplog.text
        assert result["skipped"] == 1

    def test_should_raise_when_explicit_file_path_not_found(self, gateway, mock_client, caplog):
        dataset = Dataset(name='Test', project='test', classes=['SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        with caplog.at_level(logging.ERROR):
            result = gateway.load_data(class_name='SubstanceRole', file_path='/nonexistent_path')
        mock_client.put.assert_not_called()
        assert 'No file found' in caplog.text
        assert result["skipped"] == 1

    def test_should_not_attempt_to_write_data_if_file_is_empty_list(self, gateway, mock_client):
        dataset = Dataset(name='Test', project='test', classes=['SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        with patch.object(Path, 'is_file', return_value=True), \
             patch('builtins.open', mock_open(read_data='[]')):
            result = gateway.load_data(class_name='SubstanceRole', file_path='dummy.json')
        mock_client.put.assert_not_called()
        assert result["loaded"] == 0
        assert result["skipped"] == 1

    def test_should_raise_when_datatype_not_in_any_dataset(self, gateway, mock_client, caplog):
        dataset = Dataset(name='Test', project='test', classes=['TypeA'])
        mock_client.get_datasets.return_value = [dataset]
        with caplog.at_level(logging.ERROR):
            result = gateway.load_data(class_name='NonExistentType')
        mock_client.put.assert_not_called()
        assert 'was not found in any dataset' in caplog.text
        assert result["skipped"] == 1
        
    def test_should_accept_path_object_for_from_dir_path(self, gateway, mock_client, substance_role_data):
        dataset = Dataset(name='Test', project='test', classes=['SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        gateway.load_data(class_name='SubstanceRole', from_dir_path=DATA_DIR, file_path=SUBSTANCE_ROLE_FILE)
        mock_client.put.assert_called_once()

    def test_should_log_baseclass_info(self, gateway, mock_client, mock_schema, caplog):
        dataset = Dataset(name='Test', project='test', classes=['BaseType'])
        mock_client.get_datasets.return_value = [dataset]
        mock_schema.find_subclasses.return_value = [{'name': 'Child'}]
        with caplog.at_level(logging.INFO):
            gateway.load_data()
        assert 'baseclass' in caplog.text

class TestClearData:

    def test_should_clear_down_project(self, gateway, mock_client):
        datasets = [
            Dataset(name='Test', project='test', classes=['TypeA']), 
            Dataset(name='Test2', project='test', classes=['TypeB'])
        ]
        mock_client.get_datasets.return_value = datasets
        gateway.clear_down()
        assert mock_client.clear_dataset.call_count == 2
        mock_client.clear_dataset.assert_any_call('test')
        mock_client.clear_dataset.assert_any_call('test2')


class TestMapDataProjectUrns:

    def test_should_not_remap_when_project_matches(self, gateway, mock_client, substance_role_data):
        mock_client.project_name = 'croplife-dlc'
        result = gateway._map_data_project_urns(substance_role_data)
        assert result == substance_role_data

    def test_should_remap_urns_when_project_differs(self, gateway, mock_client, substance_role_data):
        mock_client.project_name = 'my-project'
        result = gateway._map_data_project_urns(substance_role_data)
        for entity in result:
            assert entity['id'].startswith('urn:my-project:')

    def test_should_remap_all_urn_fields_recursively(self, gateway, mock_client):
        mock_client.project_name = 'target-proj'
        data = [
            {
                'id': 'urn:source-proj:Type:abc123',
                'ref': 'urn:source-proj:OtherType:def456',
                'nested': {
                    'link': 'urn:source-proj:Type:ghi789'
                },
                'list_field': ['urn:source-proj:Type:jkl012']
            }
        ]
        result = gateway._map_data_project_urns(data)
        assert result[0]['id'] == 'urn:target-proj:Type:abc123'
        assert result[0]['ref'] == 'urn:target-proj:OtherType:def456'
        assert result[0]['nested']['link'] == 'urn:target-proj:Type:ghi789'
        assert result[0]['list_field'][0] == 'urn:target-proj:Type:jkl012'

    def test_should_raise_if_entity_is_not_dict(self, gateway):
        data = ['not a dict']
        with pytest.raises(ValueError, match='Invalid format'):
            gateway._map_data_project_urns(data)

    def test_should_raise_for_entity_with_non_string_id(self, gateway):
        data = [{'id': 12345}]
        with pytest.raises(ValueError, match='Expected id property to be string'):
            gateway._map_data_project_urns(data)

    def test_should_raise_for_non_dict_entity(self, gateway):
        data = ['not a dict']
        with pytest.raises(ValueError, match='Invalid format'):
            gateway._map_data_project_urns(data)

    def test_should_handle_empty_list(self, gateway):
        result = gateway._map_data_project_urns([])
        assert result == []

    def test_should_preserve_non_urn_string_fields(self, gateway, mock_client):
        mock_client.project_name = 'target-proj'
        data = [
            {
                'id': 'urn:source-proj:Type:abc',
                'label': {'en': 'Some Label'},
                'type': 'Type'
            }
        ]
        result = gateway._map_data_project_urns(data)
        assert result[0]['label'] == {'en': 'Some Label'}
        assert result[0]['type'] == 'Type'

class TestLoadProject:
    
    def test_should_load_project_with_no_validation(self, gateway, mock_client, mock_schema):
        datasets = [Dataset(name='Test', project='test', classes=['ClassC'])]
        gateway.load_project(mock_schema, datasets, validation_mode=VALIDATION_MODE.BYPASS)
        assert mock_client.tear_down.call_count == 1
        assert mock_client.apply_schema.call_count == 1
        assert mock_client.apply_datasets.call_count == 1

    def test_should_catch_duplicate_classes_in_datasets(self, gateway, mock_client, mock_schema):
        datasets = [
            Dataset(name='Test1', project='test', classes=['ClassC']),
            Dataset(name='Test2', project='test', classes=['ClassC'])
        ]
        with pytest.raises(ValueError, match='Duplicate class ClassC found in dataset test2'):
            gateway.load_project(mock_schema, datasets, validation_mode=VALIDATION_MODE.NO_PROMPT)

    def test_should_prompt_user_on_dataset_mismatch_if_validation_mode_is_prompt(self, gateway, mock_client, mock_schema, caplog):
        new_dataset = Dataset(name='New', project='test', classes=['NewClass'])
        with patch('builtins.input', return_value='y'), caplog.at_level(logging.WARNING):
            gateway.load_project(mock_schema, [new_dataset], validation_mode=VALIDATION_MODE.PROMPT)
        assert 'Dataset validation found mismatches' in caplog.text
        assert mock_client.tear_down.call_count == 1
        assert mock_client.apply_schema.call_count == 1
        assert mock_client.apply_datasets.call_count == 1    

    def test_should_not_prompt_user_on_dataset_mismatch_if_validation_mode_is_no_prompt(self, gateway, mock_client, mock_schema, caplog):
        new_dataset = Dataset(name='New', project='test', classes=['NewClass'])
        gateway.load_project(mock_schema, [new_dataset], validation_mode=VALIDATION_MODE.NO_PROMPT)
        assert 'Dataset validation found mismatches' in caplog.text
        assert mock_client.tear_down.call_count == 1
        assert mock_client.apply_schema.call_count == 1
        assert mock_client.apply_datasets.call_count == 1    

    def test_should_not_prompt_user_if_no_dataset_mismatch(self, gateway, mock_client, mock_schema, caplog):
        new_dataset = Dataset(name='Test', project='test', classes=['ClassA', 'ClassB'])
        gateway.load_project(mock_schema, [new_dataset], validation_mode=VALIDATION_MODE.PROMPT)
        assert not caplog.text
        assert mock_client.tear_down.call_count == 1
        assert mock_client.apply_schema.call_count == 1
        assert mock_client.apply_datasets.call_count == 1    


class TestDumpProject:

    def setup_method(self):
        WORKING_DIR.mkdir(exist_ok=True)
        for file in WORKING_DIR.iterdir():
            if file.is_file():
                file.unlink()

    def test_should_dump_schema_and_datasets_to_files(self, gateway, mock_client, mock_schema):
        mock_schema.version = '2.0'
        mock_client.project_name = 'my-project'
        mock_client.get_schema.return_value = MagicMock(to_dict=lambda: {'name': 'test-schema'})
        datasets = [Dataset(name='DS1', project='my-project', classes=['TypeA'])]
        mock_client.get_datasets.return_value = datasets
        gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))
        schema_file = WORKING_DIR / 'my-project-v2.0-schema.json'
        datasets_file = WORKING_DIR / 'my-project-v2.0-datasets.json'
        assert schema_file.exists()
        assert datasets_file.exists()
        with open(schema_file, 'r', encoding='utf-8') as f:
            assert json.load(f) == {'name': 'test-schema'}
        with open(datasets_file, 'r', encoding='utf-8') as f:
            written_datasets = json.load(f)
            assert len(written_datasets) == 1
            assert written_datasets[0] == datasets[0].to_dict()

    def test_should_use_project_name_and_schema_version_in_filenames(self, gateway, mock_client, mock_schema):
        mock_schema.version = '3.1'
        mock_client.project_name = 'acme-corp'
        mock_client.get_schema.return_value = MagicMock(to_dict=lambda: {})
        mock_client.get_datasets.return_value = []
        gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))
        assert (WORKING_DIR / 'acme-corp-v3.1-schema.json').exists()
        assert (WORKING_DIR / 'acme-corp-v3.1-datasets.json').exists()

    def test_should_dump_multiple_datasets(self, gateway, mock_client, mock_schema):
        mock_schema.version = '1.0'
        mock_client.project_name = 'test-project'
        mock_client.get_schema.return_value = MagicMock(to_dict=lambda: {})
        ds1 = Dataset(name='DS1', project='test-project', classes=['TypeA'])
        ds2 = Dataset(name='DS2', project='test-project', classes=['TypeB'])
        mock_client.get_datasets.return_value = [ds1, ds2]

        gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))

        datasets_file = WORKING_DIR / 'test-project-v1.0-datasets.json'
        with open(datasets_file, 'r', encoding='utf-8') as f:
            written_datasets = json.load(f)
            assert len(written_datasets) == 2
            assert written_datasets[0] == ds1.to_dict()
            assert written_datasets[1] == ds2.to_dict()

    def test_should_dump_empty_datasets_list(self, gateway, mock_client, mock_schema):
        mock_schema.version = '1.0'
        mock_client.project_name = 'test-project'
        mock_client.get_schema.return_value = MagicMock(to_dict=lambda: {})
        mock_client.get_datasets.return_value = []

        gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))

        datasets_file = WORKING_DIR / 'test-project-v1.0-datasets.json'
        with open(datasets_file, 'r', encoding='utf-8') as f:
            assert json.load(f) == []

    def test_should_accept_path_objects(self, gateway, mock_client, mock_schema):
        mock_schema.version = '1.0'
        mock_client.project_name = 'test-project'
        mock_client.get_schema.return_value = MagicMock(to_dict=lambda: {'v': 1})
        mock_client.get_datasets.return_value = []

        gateway.dump_project(schema_path=WORKING_DIR, datasets_path=WORKING_DIR)

        assert (WORKING_DIR / 'test-project-v1.0-schema.json').exists()
        assert (WORKING_DIR / 'test-project-v1.0-datasets.json').exists()


class TestGatewayEndToEnd:

    def setup_method(self):
        WORKING_DIR.mkdir(exist_ok=True)
        for file in WORKING_DIR.iterdir():
            if file.is_file():
                file.unlink()

    def test_should_handle_multiple_types_in_dataset(self, gateway, mock_client, mock_schema, substance_role_data):
        dataset = Dataset('DS', 'test', classes=['TypeX', 'TypeY', 'TypeZ'])
        mock_client.get_datasets.return_value = [dataset]
        mock_schema.find_subclasses.return_value = []
        mock_client.get.return_value = substance_role_data
        result = gateway.dump_data(to_dir_path=str(WORKING_DIR))
        assert mock_client.get.call_count == 3
        for class_name in ['TypeX', 'TypeY', 'TypeZ']:
            mock_client.get.assert_any_call(class_name=class_name, include_date_fields=False)
            output = WORKING_DIR / f'{class_name}.json'
            assert output.exists()
            os.remove(output)
