import pytest

from datagraphs.utils import (
    get_id_from_urn,
    get_project_from_urn,
    get_type_from_urn,
    is_platform_urn,
    map_project_name,
    rewrite_urns_for_project,
)


def test_should_get_type_from_urn():    
    class_name = get_type_from_urn('urn:dg:Test:1234')
    assert class_name == 'Test'

def test_should_get_project_from_urn():    
    project_name = get_project_from_urn('urn:dg:Test:1234')
    assert project_name == 'dg'

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

class TestIsPlatformUrn:

    @pytest.mark.parametrize('urn', [
        'urn:datagraphs:datatypes:text',
        'urn:datagraphs:validation:pattern',
        'urn:models:0f8fad5bd9cb469fa165',
        'URN:DataGraphs:datatypes:text',
        'urn:MODELS:abc',
    ], ids=['datatype', 'validation-rule', 'legacy-model', 'mixed-case-namespace', 'upper-case-models'])
    def test_should_recognise_platform_namespaces(self, urn):
        assert is_platform_urn(urn) is True

    @pytest.mark.parametrize('value', [
        'urn:my-test:id1',
        'urn:datagraphs-old:id1',
        'urn:dg:datagraphs:id1',
        'urn:datagraphs',
        'datagraphs:datatypes:text',
        'xurn:datagraphs:datatypes:text',
        '',
    ], ids=['project', 'platform-name-is-prefix', 'platform-name-in-path', 'no-segment-after-namespace',
            'no-urn-prefix', 'prefix-not-at-start', 'empty'])
    def test_should_not_treat_other_strings_as_platform_urns(self, value):
        assert is_platform_urn(value) is False

class TestRewriteUrnsForProject:

    PROJECT = 'dg-project'

    @pytest.mark.parametrize(('urn', 'expected'), [
        ('urn:my-test:id1', 'urn:dg-project:id1'),
        ('urn:my-test:path1:path2:id1', 'urn:dg-project:path1:path2:id1'),
        ('urn:dg-project:id1', 'urn:dg-project:id1'),
    ], ids=['single-segment', 'multi-segment', 'already-target-project'])
    def test_should_rewrite_urn_project_segment(self, urn, expected):
        assert rewrite_urns_for_project(urn, self.PROJECT) == expected

    @pytest.mark.parametrize(('urn', 'expected'), [
        ('urn:my-test:my-test:id1', 'urn:dg-project:my-test:id1'),
        ('urn:dg-project-old:id1', 'urn:dg-project:id1'),
        ('urn:dg:id1', 'urn:dg-project:id1'),
    ], ids=['old-project-name-repeated-in-path', 'target-is-prefix-of-source', 'source-is-prefix-of-target'])
    def test_should_rewrite_only_the_project_segment(self, urn, expected):
        assert rewrite_urns_for_project(urn, self.PROJECT) == expected

    @pytest.mark.parametrize('urn', [
        'urn:datagraphs:datatypes:text',
        'urn:datagraphs:validation:pattern',
        'urn:models:0f8fad5bd9cb469fa165',
        'URN:DataGraphs:datatypes:text',
    ], ids=['datatype', 'validation-rule', 'legacy-model', 'mixed-case-namespace'])
    def test_should_leave_platform_urns_unchanged(self, urn):
        assert rewrite_urns_for_project(urn, self.PROJECT) == urn

    def test_should_preserve_case_of_urn_prefix(self):
        assert rewrite_urns_for_project('URN:my-test:id1', self.PROJECT) == 'URN:dg-project:id1'

    @pytest.mark.parametrize('value', [
        'not a urn',
        '',
        'urn:my-test',
        'urn:urn-test:id1',
        'see urn:my-test:id1',
        'https://example.com/urn:my-test:id1',
    ], ids=['plain-text', 'empty', 'no-id-segment', 'invalid-nid', 'urn-not-at-start', 'urn-inside-url'])
    def test_should_leave_strings_that_are_not_valid_urns_unchanged(self, value):
        assert rewrite_urns_for_project(value, self.PROJECT) == value

    @pytest.mark.parametrize('value', [42, 1.5, True, None], ids=['int', 'float', 'bool', 'none'])
    def test_should_leave_non_string_scalars_unchanged(self, value):
        assert rewrite_urns_for_project(value, self.PROJECT) is value

    def test_should_rewrite_urns_throughout_nested_structures_and_nothing_else(self):
        obj = {
            'id': 'urn:my-test:Agent:1',
            'label': 'Agent one',
            'count': 3,
            'urn:my-test:key': 'urn:other:Agent:2',
            'datatype': 'urn:datagraphs:datatypes:text',
            'related': [
                {'id': 'urn:my-test:Agent:3', 'tags': ['urn:my-test:Tag:1', 'plain', None]},
                'urn:dg-project:Agent:4',
            ],
        }
        assert rewrite_urns_for_project(obj, self.PROJECT) == {
            'id': 'urn:dg-project:Agent:1',
            'label': 'Agent one',
            'count': 3,
            'urn:my-test:key': 'urn:dg-project:Agent:2',
            'datatype': 'urn:datagraphs:datatypes:text',
            'related': [
                {'id': 'urn:dg-project:Agent:3', 'tags': ['urn:dg-project:Tag:1', 'plain', None]},
                'urn:dg-project:Agent:4',
            ],
        }

    def test_should_not_mutate_the_input(self):
        obj = {'id': 'urn:my-test:Agent:1', 'related': [{'id': 'urn:my-test:Agent:2'}]}
        rewrite_urns_for_project(obj, self.PROJECT)
        assert obj == {'id': 'urn:my-test:Agent:1', 'related': [{'id': 'urn:my-test:Agent:2'}]}
