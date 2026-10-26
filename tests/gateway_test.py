import json
import logging
import os
from pathlib import Path
from unittest.mock import MagicMock, mock_open, patch

import pytest

from datagraphs.client import AuthenticationError, FailedWrite, PutResult, TimedOutWrite
from datagraphs.client import Client as DatagraphsClient
from datagraphs.dataset import Dataset
from datagraphs.enums import VALIDATION_MODE
from datagraphs.gateway import Gateway as DatagraphsGateway
from datagraphs.gateway import SkippedClass, SkipReason
from datagraphs.schema import Schema

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
    client.put.side_effect = lambda dataset_slug, data: PutResult(loaded=len(data))
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
    mock_client.get_schema.return_value = mock_schema
    return DatagraphsGateway(mock_client, wait_time_ms=0)

class TestInit:

    def test_should_initialize_with_default_wait_time(self, mock_client):
        gateway = DatagraphsGateway(mock_client)
        assert gateway._client is mock_client
        assert gateway._schema is None
        assert gateway._wait_time_ms == DatagraphsClient.DEFAULT_WAIT_TIME_MS

    def test_should_initialize_with_custom_wait_time(self, mock_client):
        gateway = DatagraphsGateway(mock_client, wait_time_ms=500)
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
        gateway.dump_data(to_dir_path=str(WORKING_DIR))
        mock_client.get.assert_called_once()

    def test_should_exclude_specified_classes_when_dumping_all(self, gateway, mock_client, mock_schema):
        dataset = Dataset(name='Test Dataset', project='test-project', classes=['TypeA', 'TypeB', 'TypeC'])
        mock_client.get_datasets.return_value = [dataset]
        mock_schema.find_subclasses.return_value = []
        mock_client.get.return_value = []
        gateway.dump_data(to_dir_path=str(WORKING_DIR), excluded_class_names='TypeB, TypeC')
        mock_client.get.assert_called_once_with(class_name='TypeA', include_date_fields=False)

    def test_should_dump_multiple_datasets(self, gateway, mock_client, mock_schema):
        ds1 = Dataset(name='DS1', project='test-project', classes=['TypeA'])
        ds2 = Dataset(name='DS2', project='test-project', classes=['TypeB'])
        mock_client.get_datasets.return_value = [ds1, ds2]
        mock_schema.find_subclasses.return_value = []
        mock_client.get.return_value = []
        gateway.dump_data(to_dir_path=str(WORKING_DIR))
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
        args = mock_client.put.call_args.args
        assert args[0] == dataset.slug
        loaded_entities = args[1]
        assert len(loaded_entities) == len(substance_role_data)
        assert result.loaded == len(substance_role_data)
        assert result.skipped == {}

    def test_should_load_all_classes_from_all_datasets(self, gateway, mock_client, mock_schema, substance_role_data):
        dataset = Dataset(name='Test Dataset', project='test-project', classes=['SubstanceRole', 'BufferZoneType'])
        mock_client.get_datasets.return_value = [dataset]
        mock_schema.find_subclasses.return_value = []
        with patch.object(Path, 'is_file', return_value=True), \
             patch('builtins.open', mock_open(read_data=json.dumps(substance_role_data))):
            result = gateway.load_data(from_dir_path=str(DATA_DIR))
        assert mock_client.get_datasets.call_count == 1
        assert mock_client.put.call_count == 2
        assert result.loaded == len(substance_role_data) * 2

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
        assert result.skipped == {'SubstanceRole': SkippedClass(SkipReason.NO_FILE, Path('/nonexistent_path/SubstanceRole.json'))}

    def test_should_raise_when_explicit_file_path_not_found(self, gateway, mock_client, caplog):
        dataset = Dataset(name='Test', project='test', classes=['SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        with caplog.at_level(logging.ERROR):
            result = gateway.load_data(class_name='SubstanceRole', file_path='/nonexistent_path')
        mock_client.put.assert_not_called()
        assert 'No file found' in caplog.text
        assert result.skipped == {'SubstanceRole': SkippedClass(SkipReason.NO_FILE, Path('/nonexistent_path'))}

    def test_should_not_attempt_to_write_data_if_file_is_empty_list(self, gateway, mock_client):
        dataset = Dataset(name='Test', project='test', classes=['SubstanceRole'])
        mock_client.get_datasets.return_value = [dataset]
        with patch.object(Path, 'is_file', return_value=True), \
             patch('builtins.open', mock_open(read_data='[]')):
            result = gateway.load_data(class_name='SubstanceRole', file_path='dummy.json')
        mock_client.put.assert_not_called()
        assert result.loaded == 0
        assert result.skipped == {'SubstanceRole': SkippedClass(SkipReason.EMPTY_FILE, Path('dummy.json'))}

    def test_should_raise_when_datatype_not_in_any_dataset(self, gateway, mock_client, caplog):
        dataset = Dataset(name='Test', project='test', classes=['TypeA'])
        mock_client.get_datasets.return_value = [dataset]
        with caplog.at_level(logging.ERROR):
            result = gateway.load_data(class_name='NonExistentType')
        mock_client.put.assert_not_called()
        assert 'was not found in any dataset' in caplog.text
        assert result.skipped == {'NonExistentType': SkippedClass(SkipReason.NOT_IN_DATASET)}
        
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

class TestLoadDataOutcomes:
    """`load_data` reports, per class, every entity that was not confirmed as written."""

    @pytest.fixture(autouse=True)
    def setup(self, gateway, mock_client, substance_role_data, tmp_path):
        self.gateway = gateway
        self.client = mock_client
        self.data = substance_role_data
        self.dir = tmp_path
        mock_client.get_datasets.return_value = [Dataset(name='Test', project='test-project', classes=['ClassA', 'ClassB', 'ClassC'])]
        for class_name in ('ClassA', 'ClassB', 'ClassC'):
            (tmp_path / f'{class_name}.json').write_text(json.dumps(substance_role_data), encoding='utf-8')

    def put_outcomes(self, outcomes: dict) -> None:
        """Have the client return these `PutResult`s in class load order (keys name the class for readability)."""
        calls = iter(outcomes.values())
        self.client.put.side_effect = lambda dataset_slug, data: next(calls)

    def test_should_be_ok_when_every_entity_is_written(self):
        result = self.gateway.load_data(from_dir_path=self.dir)
        assert result.ok
        assert result.loaded == len(self.data) * 3
        assert (result.failed, result.timed_out, result.class_errors) == ({}, {}, {})

    def test_should_report_failed_and_timed_out_entities_per_class_and_keep_loading(self):
        failure = FailedWrite(0, 'urn:a', 400, ['invalid'])
        timeouts = [TimedOutWrite(1, 'urn:b1'), TimedOutWrite(2, None)]
        self.put_outcomes({
            'ClassA': PutResult(loaded=len(self.data) - 1, failed=[failure]),
            'ClassB': PutResult(loaded=len(self.data) - 2, timed_out=timeouts),
            'ClassC': PutResult(loaded=len(self.data)),
        })
        result = self.gateway.load_data(from_dir_path=self.dir)
        assert not result.ok
        assert result.loaded == len(self.data) * 3 - 3
        assert result.failed == {'ClassA': [failure]}
        assert result.timed_out == {'ClassB': timeouts}
        assert (result.failed_count, result.timed_out_count) == (1, 2)
        assert self.client.put.call_count == 3

    def test_should_record_a_class_whose_file_cannot_be_read_and_keep_loading(self):
        (self.dir / 'ClassB.json').write_text('{not json', encoding='utf-8')
        result = self.gateway.load_data(from_dir_path=self.dir)
        assert not result.ok
        assert list(result.class_errors) == ['ClassB']
        assert isinstance(result.class_errors['ClassB'], json.JSONDecodeError)
        assert result.loaded == len(self.data) * 2
        assert result.skipped == {}

    def test_should_treat_a_missing_class_file_as_skipped_not_failed(self):
        (self.dir / 'ClassB.json').unlink()
        result = self.gateway.load_data(from_dir_path=self.dir)
        assert result.ok
        assert result.skipped == {'ClassB': SkippedClass(SkipReason.NO_FILE, self.dir / 'ClassB.json')}
        assert result.loaded == len(self.data) * 2

    def test_should_stop_and_raise_when_authentication_fails(self):
        self.client.put.side_effect = AuthenticationError('denied')
        with pytest.raises(AuthenticationError):
            self.gateway.load_data(from_dir_path=self.dir)
        assert self.client.put.call_count == 1

    def test_should_log_the_entities_not_written(self, caplog):
        self.put_outcomes({
            'ClassA': PutResult(loaded=0, failed=[FailedWrite(0, 'urn:a', 400, ['invalid']), FailedWrite(3, None, 400, ['invalid'])]),
            'ClassB': PutResult(loaded=0, timed_out=[TimedOutWrite(5, None)]),
            'ClassC': PutResult(loaded=len(self.data)),
        })
        with caplog.at_level(logging.ERROR):
            self.gateway.load_data(from_dir_path=self.dir)
        assert 'Data load incomplete' in caplog.text
        assert "ClassA: 2 failed: ['#0 urn:a', '#3 (no id)']" in caplog.text
        assert "ClassB: 1 timed out" in caplog.text and "['#5 (no id)']" in caplog.text

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


class TestLoadDataEntityPositions:
    """Through a real `Client`, failures are reported by position in the class's data file."""

    def test_should_report_failed_entity_without_id_by_its_position_in_the_file(self, tmp_path):
        client = DatagraphsClient('test-project', 'test-api-key', batch_size=2)
        client._http_client = MagicMock()
        def respond(method, url, **kwargs):
            names = [entity['name'] for entity in kwargs['json']]
            response = MagicMock(status_code=201, content=b'', text='', reason='Created')
            if 'bad' in names:
                body = {'errors': ['bad has an invalid value']}
                response.status_code, response.content = 400, json.dumps(body).encode()
                response.json.return_value = body
            return response
        client._http_client.request.side_effect = respond
        client.get_datasets = MagicMock(return_value=[Dataset(name='Test', project='test-project', classes=['ClassA'])])
        entities = [
            {'id': 'urn:supplier-a:ClassA:e0', 'name': 'e0'},
            {'name': 'e1'},
            {'id': 'urn:supplier-a:ClassA:e2', 'name': 'e2'},
            {'name': 'bad'},
            {'name': 'e4'},
        ]
        data_file = tmp_path / 'ClassA.json'
        data_file.write_text(json.dumps(entities), encoding='utf-8')

        result = DatagraphsGateway(client, wait_time_ms=0).load_data(class_name='ClassA', file_path=data_file)

        # Batches of two: 'bad' is second in the second batch, which is rejected and
        # resubmitted entity by entity; it is reported at its position in the file.
        assert result.failed == {'ClassA': [FailedWrite(3, None, 400, ['bad has an invalid value'])]}
        assert json.loads(data_file.read_text(encoding='utf-8'))[result.failed['ClassA'][0].index]['name'] == 'bad'
        assert result.loaded == 4
        assert result.timed_out == {}

class TestLoadDataUrnRewriting:
    """Loading rewrites every project URN to the target project, whatever its source basepath.

    The target project is ``test-project`` (from the ``mock_client`` fixture).
    """

    @pytest.fixture(autouse=True)
    def setup(self, gateway, mock_client, tmp_path):
        self.gateway = gateway
        self.client = mock_client
        self.file = tmp_path / 'ClassA.json'

    def load(self, entities: list[dict]) -> list[dict]:
        """Load *entities* from a file and return what was sent to the API (the result is kept in ``self.result``)."""
        self.file.write_text(json.dumps(entities), encoding='utf-8')
        self.result = result = self.gateway.load_data(class_name='ClassA', file_path=self.file)
        assert result.ok
        self.client.put.assert_called_once()
        return self.client.put.call_args.args[1]

    def test_should_rewrite_links_to_entities_with_different_basepaths(self):
        entity = {
            'id': 'urn:supplier-a:Product:p1',
            'type': 'Product',
            'manufacturer': 'urn:supplier-b:Agent:a1',
            'formulation': {'id': 'urn:supplier-c:registry:eu:Formulation:f1', 'type': 'Formulation'},
            'components': [
                'urn:supplier-a:Substance:s1',
                {'id': 'urn:supplier-d:Substance:s2', 'role': 'urn:supplier-b:SubstanceRole:r1'},
                'urn:test-project:Substance:s3',
            ],
        }
        sent = self.load([entity])
        assert sent == [{
            'id': 'urn:test-project:Product:p1',
            'type': 'Product',
            'manufacturer': 'urn:test-project:Agent:a1',
            'formulation': {'id': 'urn:test-project:registry:eu:Formulation:f1', 'type': 'Formulation'},
            'components': [
                'urn:test-project:Substance:s1',
                {'id': 'urn:test-project:Substance:s2', 'role': 'urn:test-project:SubstanceRole:r1'},
                'urn:test-project:Substance:s3',
            ],
        }]

    def test_should_leave_platform_urns_and_non_urn_values_unchanged_alongside_rewritten_links(self):
        entity = {
            'id': 'urn:supplier-a:Product:p1',
            'manufacturer': 'urn:supplier-b:Agent:a1',
            'datatype': 'urn:datagraphs:datatypes:text',
            'model': 'urn:models:0f8fad5bd9cb469fa165',
            'label': {'en': 'Refers to urn:supplier-a:Product:p1', '@none': 'Product one'},
            'homepage': 'https://supplier-a.example/urn:supplier-a:Product:p1',
            'notes': ['plain text', ''],
            'count': 3,
            'active': True,
            'withdrawn': None,
        }
        sent = self.load([entity])
        assert sent == [{
            **entity,
            'id': 'urn:test-project:Product:p1',
            'manufacturer': 'urn:test-project:Agent:a1',
        }]

    def test_should_rewrite_each_entity_from_its_own_basepath(self):
        entities = [
            {'id': 'urn:supplier-a:Agent:a1', 'parent': 'urn:supplier-c:Agent:a9'},
            {'id': 'urn:supplier-b:Agent:a2', 'parent': 'urn:supplier-a:Agent:a1'},
            {'id': 'urn:test-project:Agent:a3', 'parent': 'urn:supplier-b:Agent:a2'},
        ]
        sent = self.load(entities)
        assert sent == [
            {'id': 'urn:test-project:Agent:a1', 'parent': 'urn:test-project:Agent:a9'},
            {'id': 'urn:test-project:Agent:a2', 'parent': 'urn:test-project:Agent:a1'},
            {'id': 'urn:test-project:Agent:a3', 'parent': 'urn:test-project:Agent:a2'},
        ]

    def test_should_load_root_entity_without_id_and_rewrite_its_links(self):
        # With no id, DataGraphs assigns one on load: the gateway must not invent one.
        entities = [
            {
                'type': 'Product',
                'manufacturer': 'urn:supplier-b:Agent:a1',
                'components': [
                    'urn:supplier-a:Substance:s1',
                    {'type': 'Ingredient', 'substance': 'urn:supplier-c:Substance:s2'},
                ],
                'datatype': 'urn:datagraphs:datatypes:text',
            },
            {'id': 'urn:supplier-a:Agent:a2', 'parent': 'urn:supplier-b:Agent:a1'},
        ]
        sent = self.load(entities)
        assert sent == [
            {
                'type': 'Product',
                'manufacturer': 'urn:test-project:Agent:a1',
                'components': [
                    'urn:test-project:Substance:s1',
                    {'type': 'Ingredient', 'substance': 'urn:test-project:Substance:s2'},
                ],
                'datatype': 'urn:datagraphs:datatypes:text',
            },
            {'id': 'urn:test-project:Agent:a2', 'parent': 'urn:test-project:Agent:a1'},
        ]
        assert self.result.loaded == 2

    def test_should_reject_a_file_containing_an_entity_with_a_non_string_id(self):
        self.file.write_text(json.dumps([
            {'id': 'urn:supplier-a:Agent:a1'},
            {'id': 12345, 'parent': 'urn:supplier-a:Agent:a1'},
        ]), encoding='utf-8')
        result = self.gateway.load_data(class_name='ClassA', file_path=self.file)
        assert not result.ok
        error = result.class_errors['ClassA']
        assert type(error) is ValueError
        assert 'Expected id property to be string' in str(error)
        self.client.put.assert_not_called()

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
        assert mock_client.tear_down.call_count == 0
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
        assert mock_client.tear_down.call_count == 0
        assert mock_client.apply_schema.call_count == 1
        assert mock_client.apply_datasets.call_count == 1    

    def test_should_not_prompt_user_on_dataset_mismatch_if_validation_mode_is_no_prompt(self, gateway, mock_client, mock_schema, caplog):
        new_dataset = Dataset(name='New', project='test', classes=['NewClass'])
        gateway.load_project(mock_schema, [new_dataset], validation_mode=VALIDATION_MODE.NO_PROMPT)
        assert 'Dataset validation found mismatches' in caplog.text
        assert mock_client.tear_down.call_count == 0
        assert mock_client.apply_schema.call_count == 1
        assert mock_client.apply_datasets.call_count == 1    

    def test_should_not_prompt_user_if_no_dataset_mismatch(self, gateway, mock_client, mock_schema, caplog):
        new_dataset = Dataset(name='Test', project='test', classes=['ClassA', 'ClassB'])
        gateway.load_project(mock_schema, [new_dataset], validation_mode=VALIDATION_MODE.PROMPT)
        assert not caplog.text
        assert mock_client.tear_down.call_count == 0
        assert mock_client.apply_schema.call_count == 1
        assert mock_client.apply_datasets.call_count == 1    

class TestDumpProject:

    def setup_method(self):
        WORKING_DIR.mkdir(exist_ok=True)
        for file in WORKING_DIR.iterdir():
            if file.is_file():
                file.unlink()

    def test_should_dump_schema_and_datasets_to_files(self, gateway, mock_client, mock_schema):
        schema_mock = MagicMock(version='2.0', to_dict=lambda: {'name': 'test-schema'})
        mock_client.get_schema.return_value = schema_mock
        mock_client.project_name = 'my-project'
        datasets = [Dataset(name='DS1', project='my-project', classes=['TypeA'])]
        mock_client.get_datasets.return_value = datasets
        gateway._schema = None
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
        schema_mock = MagicMock(version='3.1', to_dict=dict)
        mock_client.get_schema.return_value = schema_mock
        mock_client.project_name = 'acme-corp'
        mock_client.get_datasets.return_value = []
        gateway._schema = None
        gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))
        assert (WORKING_DIR / 'acme-corp-v3.1-schema.json').exists()
        assert (WORKING_DIR / 'acme-corp-v3.1-datasets.json').exists()

    def test_should_dump_multiple_datasets(self, gateway, mock_client, mock_schema):
        schema_mock = MagicMock(version='1.0', to_dict=dict)
        mock_client.get_schema.return_value = schema_mock
        mock_client.project_name = 'test-project'
        ds1 = Dataset(name='DS1', project='test-project', classes=['TypeA'])
        ds2 = Dataset(name='DS2', project='test-project', classes=['TypeB'])
        mock_client.get_datasets.return_value = [ds1, ds2]
        gateway._schema = None

        gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))

        datasets_file = WORKING_DIR / 'test-project-v1.0-datasets.json'
        with open(datasets_file, 'r', encoding='utf-8') as f:
            written_datasets = json.load(f)
            assert len(written_datasets) == 2
            assert written_datasets[0] == ds1.to_dict()
            assert written_datasets[1] == ds2.to_dict()

    def test_should_dump_empty_datasets_list(self, gateway, mock_client, mock_schema):
        schema_mock = MagicMock(version='1.0', to_dict=dict)
        mock_client.get_schema.return_value = schema_mock
        mock_client.project_name = 'test-project'
        mock_client.get_datasets.return_value = []
        gateway._schema = None

        gateway.dump_project(schema_path=str(WORKING_DIR), datasets_path=str(WORKING_DIR))

        datasets_file = WORKING_DIR / 'test-project-v1.0-datasets.json'
        with open(datasets_file, 'r', encoding='utf-8') as f:
            assert json.load(f) == []

    def test_should_accept_path_objects(self, gateway, mock_client, mock_schema):
        schema_mock = MagicMock(version='1.0', to_dict=lambda: {'v': 1})
        mock_client.get_schema.return_value = schema_mock
        mock_client.project_name = 'test-project'
        mock_client.get_datasets.return_value = []
        gateway._schema = None

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
        gateway.dump_data(to_dir_path=str(WORKING_DIR))
        assert mock_client.get.call_count == 3
        for class_name in ['TypeX', 'TypeY', 'TypeZ']:
            mock_client.get.assert_any_call(class_name=class_name, include_date_fields=False)
            output = WORKING_DIR / f'{class_name}.json'
            assert output.exists()
            os.remove(output)
