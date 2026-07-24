from pathlib import Path

import pytest

from datagraphs.utils import (
    SchemaTransformer,
    get_id_from_urn,
    get_project_from_urn,
    get_type_from_urn,
    map_project_name,
)

TEMP_DIR = Path(__file__).parent.parent / 'temp'

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


# --- SchemaTransformer Tests ---

class TestSchemaTransformerFormatDetection:

    def test_should_detect_legacy_format_by_objectProperties(self):
        schema = {"classes": [{"label": "Test", "objectProperties": []}]}
        assert SchemaTransformer.is_legacy_format(schema) is True

    def test_should_detect_legacy_format_by_label_without_type(self):
        schema = {"classes": [{"label": "Test"}]}
        assert SchemaTransformer.is_legacy_format(schema) is True

    def test_should_detect_legacy_format_by_guid(self):
        schema = {"guid": "abc", "classes": []}
        assert SchemaTransformer.is_legacy_format(schema) is True

    def test_should_detect_new_format(self):
        schema = {"classes": [{"type": "Class", "name": "Test", "properties": []}]}
        assert SchemaTransformer.is_legacy_format(schema) is False

    def test_should_detect_new_format_empty_classes(self):
        schema = {"name": "Test", "classes": []}
        assert SchemaTransformer.is_legacy_format(schema) is False


class TestSchemaTransformerOldToNew:

    def test_should_convert_top_level_fields(self):
        old = {
            "id": "urn:models:abc",
            "guid": "abc",
            "type": "DomainModel",
            "name": "Test Model",
            "description": "",
            "project": "urn:proj",
            "createdDate": "2024-01-01",
            "lastModifiedDate": "2024-01-02",
            "classes": [],
        }
        new = SchemaTransformer.old_to_new(old)
        assert new["name"] == "Test Model"
        assert new["createdDate"] == "2024-01-01"
        assert new["lastModifiedDate"] == "2024-01-02"
        assert "id" not in new
        assert "guid" not in new
        assert "type" not in new
        assert "project" not in new

    def test_should_convert_class_fields(self):
        old = {
            "classes": [{
                "label": "Person",
                "description": "A person",
                "labelProperty": "name",
                "identifierProperty": "id",
                "parentClass": "Agent",
                "parentClasses": ["Person", "Agent"],
                "objectProperties": [],
            }]
        }
        new = SchemaTransformer.old_to_new(old)
        cls = new["classes"][0]
        assert cls["type"] == "Class"
        assert cls["name"] == "Person"
        assert cls["description"] == {"en": "A person", "@none": "A person"}
        assert cls["subClassOf"] == "Agent"
        assert cls["isAbstract"] is False
        assert "label" not in cls
        assert "parentClass" not in cls
        assert "parentClasses" not in cls

    def test_should_convert_datatype_property(self):
        old = {
            "classes": [{
                "label": "Test",
                "objectProperties": [{
                    "propertyName": "age",
                    "isOptional": True,
                    "isArray": False,
                    "propertyDatatype": {
                        "id": "urn:datagraphs:datatypes:integer",
                        "type": "PropertyDatatype",
                        "label": "integer",
                        "elasticsearchDatatype": "long",
                        "xsdDatatype": "integer",
                    },
                    "isNestedObject": False,
                    "guid": "g1",
                    "propertyOrder": 0,
                    "id": "urn:models:abc:classes:Test:age",
                }],
            }]
        }
        new = SchemaTransformer.old_to_new(old)
        prop = new["classes"][0]["properties"][0]
        assert prop["type"] == "DatatypeProperty"
        assert prop["name"] == "age"
        assert prop["range"] == "integer"
        assert prop["isOptional"] is True
        assert prop["isArray"] is False
        assert "propertyName" not in prop
        assert "isFilterable" not in prop
        assert "propertyDatatype" not in prop
        assert "guid" not in prop
        assert "propertyOrder" not in prop
        assert "id" not in prop

    def test_should_convert_object_property(self):
        old = {
            "classes": [{
                "label": "Test",
                "objectProperties": [{
                    "propertyName": "relatedTo",
                    "isOptional": True,
                    "isArray": True,
                    "propertyDatatype": {
                        "id": "urn:datagraphs:datatypes:concept",
                        "range": "OtherClass",
                        "type": "PropertyDatatype",
                        "label": "OtherClass",
                    },
                    "isNestedObject": False,
                    "inverseOf": "relatedFrom",
                    "guid": "g2",
                    "propertyOrder": 1,
                    "id": "urn:models:abc:classes:Test:relatedTo",
                    "isFilterable": False # Should be ignored in new format
                }],
            }]
        }
        new = SchemaTransformer.old_to_new(old)
        prop = new["classes"][0]["properties"][0]
        assert prop["type"] == "ObjectProperty"
        assert prop["name"] == "relatedTo"
        assert prop["range"] == "OtherClass"
        assert prop["inverseOf"] == "relatedFrom"
        assert prop["isArray"] is True
        assert prop["isFilterable"] is False

    def test_should_convert_enum_validation_rules(self):
        old = {
            "classes": [{
                "label": "Test",
                "objectProperties": [{
                    "propertyName": "status",
                    "isOptional": True,
                    "isArray": False,
                    "propertyDatatype": {
                        "id": "urn:datagraphs:datatypes:enum",
                        "type": "PropertyDatatype",
                        "label": "enum",
                        "elasticsearchDatatype": "keyword",
                        "xsdDatatype": "string",
                    },
                    "validationRules": [{"id": "urn:datagraphs:validation:enumeration", "value": ["A", "B"]}],
                    "isNestedObject": False,
                    "guid": "g3",
                    "propertyOrder": 0,
                    "id": "urn:models:abc:classes:Test:status",
                }],
            }]
        }
        new = SchemaTransformer.old_to_new(old)
        prop = new["classes"][0]["properties"][0]
        assert prop["validationRules"][0]["type"] == "enumeration"
        assert prop["validationRules"][0]["value"] == ["A", "B"]

    def test_should_omit_description_if_empty(self):
        old = {
            "classes": [{
                "label": "Test",
                "description": "",
                "objectProperties": [{
                    "propertyName": "prop",
                    "propertyDescription": "",
                    "propertyDatatype": {"id": "urn:datagraphs:datatypes:text", "label": "text"},
                    "propertyOrder": 0,
                    "guid": "g", "id": "urn:m:c:Test:prop", "isNestedObject": False,
                }],
            }]
        }
        new = SchemaTransformer.old_to_new(old)
        assert "description" not in new["classes"][0]
        assert "description" not in new["classes"][0]["properties"][0]


class TestSchemaTransformerNewToOld:

    def test_should_convert_top_level_fields(self):
        new = {
            "name": "Test Model",
            "createdDate": "2024-01-01",
            "lastModifiedDate": "2024-01-02",
            "classes": [],
        }
        old = SchemaTransformer.new_to_old(new)
        assert old["name"] == "Test Model"
        assert old["type"] == "DomainModel"
        assert old["id"].startswith("urn:models:")
        assert "guid" in old
        assert old["description"] == ""
        assert old["project"] == ""

    def test_should_convert_class_with_subClassOf(self):
        new = {
            "classes": [{
                "type": "Class",
                "name": "Person",
                "description": {"en": "A person", "@none": "A person"},
                "subClassOf": "Agent",
                "labelProperty": "name",
                "identifierProperty": "id",
                "properties": [],
                "isAbstract": False,
            }]
        }
        old = SchemaTransformer.new_to_old(new)
        cls = old["classes"][0]
        assert cls["label"] == "Person"
        assert cls["parentClass"] == "Agent"
        assert "Person" in cls["parentClasses"]
        assert "Agent" in cls["parentClasses"]
        assert cls["description"] == "A person"

    def test_should_convert_datatype_property_back_to_legacy(self):
        new = {
            "classes": [{
                "type": "Class",
                "name": "Test",
                "properties": [{
                    "type": "DatatypeProperty",
                    "name": "age",
                    "range": "integer",
                    "isOptional": True,
                    "isArray": False,
                    "isLangString": False,
                    "isLabelSynonym": False,
                    "isFilterable": False,
                }],
            }]
        }
        old = SchemaTransformer.new_to_old(new)
        prop = old["classes"][0]["objectProperties"][0]
        assert prop["propertyName"] == "age"
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:integer"
        assert prop["propertyDatatype"]["label"] == "integer"
        assert prop["propertyDatatype"]["type"] == "PropertyDatatype"
        assert prop["propertyOrder"] == 0
        assert "guid" in prop
        assert prop["id"].endswith(":age")

    def test_should_convert_object_property_back_to_legacy(self):
        new = {
            "classes": [{
                "type": "Class",
                "name": "Test",
                "properties": [{
                    "type": "ObjectProperty",
                    "name": "ref",
                    "range": "Other",
                    "isOptional": True,
                    "isArray": False,
                    "isNestedObject": False,
                    "isLabelSynonym": False,
                    "isFilterable": False,
                }],
            }]
        }
        old = SchemaTransformer.new_to_old(new)
        prop = old["classes"][0]["objectProperties"][0]
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:concept"
        assert prop["propertyDatatype"]["range"] == "Other"

    def test_should_convert_enum_validation_rules_back(self):
        new = {
            "classes": [{
                "type": "Class",
                "name": "Test",
                "properties": [{
                    "type": "DatatypeProperty",
                    "name": "status",
                    "range": "enum",
                    "validationRules": [{"type": "enumeration", "value": ["X", "Y"]}],
                }],
            }]
        }
        old = SchemaTransformer.new_to_old(new)
        prop = old["classes"][0]["objectProperties"][0]
        assert prop["validationRules"][0]["id"] == "urn:datagraphs:validation:enumeration"
        assert prop["validationRules"][0]["value"] == ["X", "Y"]
