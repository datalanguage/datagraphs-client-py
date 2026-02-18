import pytest
from datagraphs.utils import *

def test_should_get_type_from_urn():    
    type_name = get_type_from_urn('urn:dg:Test:1234')
    assert type_name == 'Test'

def test_should_get_id_from_urn():    
    entity_id = get_id_from_urn('urn:dg:Test:1234')
    assert entity_id == '1234'

def test_should_raise_value_error_for_malformed_urn():    
    with pytest.raises(ValueError, match='Invalid URN: invalid-urn-format'):
        get_type_from_urn('invalid-urn-format')
    with pytest.raises(ValueError, match='Invalid URN: urn:urn-test:x'):
        get_id_from_urn('urn:urn-test:x')

def test_should_map_project_name_for_simple_entity():
    obj = {'id': 'urn:projectA:Test:abc', 'payload': 'data'}
    mapped_obj = map_project_name(obj, from_urn='urn:projectA', to_urn='urn:projectB')
    assert mapped_obj['id'] == 'urn:projectB:Test:abc'

def test_should_map_project_name_in_entity_collections():
    objs = [
        {'id': 'urn:projectA:Test:1', 'payload': 'data1'},
        {'id': 'urn:projectA:Test:2', 'payload': 'data2'}
    ]
    mapped_objs = map_project_name(objs, from_urn='urn:projectA', to_urn='urn:projectB')
    assert mapped_objs[0]['id'] == 'urn:projectB:Test:1'
    assert mapped_objs[1]['id'] == 'urn:projectB:Test:2'

def test_should_map_project_name_in_nested_entity_collections():
    obj = {
        'id': 'urn:projectA:Test:abc', 
        'payload': [
            {'id': 'urn:projectA:Test:1', 'payload': 'data1'},
            {'id': 'urn:projectA:Test:2', 'payload': 'data2'}
        ]
    }
    mapped_obj = map_project_name(obj, from_urn='urn:projectA', to_urn='urn:projectB')
    assert mapped_obj['id'] == 'urn:projectB:Test:abc'
    assert mapped_obj['payload'][0]['id'] == 'urn:projectB:Test:1'
    assert mapped_obj['payload'][1]['id'] == 'urn:projectB:Test:2'
