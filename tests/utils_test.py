import json
import pytest
from pathlib import Path
from datagraphs.utils import *
from datagraphs.utils import SchemaTransformer

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


class TestSchemaTransformerRoundTrip:

    @pytest.fixture(scope="class")
    def comprehensive_old_schema(self):
        path = TEMP_DIR / 'comprehensive_old_schema.json'
        if not path.exists():
            pytest.skip("comprehensive_old_schema.json fixture not found")
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    @pytest.fixture(scope="class")
    def comprehensive_new_schema(self):
        path = TEMP_DIR / 'comprehensive_new_schema.json'
        if not path.exists():
            pytest.skip("comprehensive_new_schema.json fixture not found")
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def test_old_to_new_should_produce_matching_class_count(self, comprehensive_old_schema, comprehensive_new_schema):
        converted = SchemaTransformer.old_to_new(comprehensive_old_schema)
        assert len(converted["classes"]) == len(comprehensive_new_schema["classes"])

    def test_old_to_new_should_produce_matching_class_names(self, comprehensive_old_schema, comprehensive_new_schema):
        converted = SchemaTransformer.old_to_new(comprehensive_old_schema)
        converted_names = sorted([c["name"] for c in converted["classes"]])
        expected_names = sorted([c["name"] for c in comprehensive_new_schema["classes"]])
        assert converted_names == expected_names

    def test_old_to_new_should_produce_matching_property_counts(self, comprehensive_old_schema, comprehensive_new_schema):
        converted = SchemaTransformer.old_to_new(comprehensive_old_schema)
        for conv_cls in converted["classes"]:
            expected_cls = next(c for c in comprehensive_new_schema["classes"] if c["name"] == conv_cls["name"])
            assert len(conv_cls["properties"]) == len(expected_cls["properties"]), \
                f"Property count mismatch for class {conv_cls['name']}"

    def test_new_to_old_should_produce_matching_class_count(self, comprehensive_old_schema, comprehensive_new_schema):
        converted = SchemaTransformer.new_to_old(comprehensive_new_schema)
        assert len(converted["classes"]) == len(comprehensive_old_schema["classes"])

    def test_new_to_old_should_produce_matching_class_labels(self, comprehensive_old_schema, comprehensive_new_schema):
        converted = SchemaTransformer.new_to_old(comprehensive_new_schema)
        converted_labels = sorted([c["label"] for c in converted["classes"]])
        expected_labels = sorted([c["label"] for c in comprehensive_old_schema["classes"]])
        assert converted_labels == expected_labels

    def test_new_to_old_should_produce_matching_property_counts(self, comprehensive_old_schema, comprehensive_new_schema):
        converted = SchemaTransformer.new_to_old(comprehensive_new_schema)
        for conv_cls in converted["classes"]:
            expected_cls = next(c for c in comprehensive_old_schema["classes"] if c["label"] == conv_cls["label"])
            assert len(conv_cls["objectProperties"]) == len(expected_cls["objectProperties"]), \
                f"Property count mismatch for class {conv_cls['label']}"

    def test_old_to_new_round_trip_preserves_structure(self, comprehensive_old_schema):
        """old -> new -> old should preserve class/property structure."""
        new = SchemaTransformer.old_to_new(comprehensive_old_schema)
        back = SchemaTransformer.new_to_old(new)
        assert len(back["classes"]) == len(comprehensive_old_schema["classes"])
        for orig_cls in comprehensive_old_schema["classes"]:
            round_cls = next(c for c in back["classes"] if c["label"] == orig_cls["label"])
            assert len(round_cls["objectProperties"]) == len(orig_cls["objectProperties"])
            for orig_prop in orig_cls["objectProperties"]:
                round_prop = next(
                    p for p in round_cls["objectProperties"]
                    if p["propertyName"] == orig_prop["propertyName"]
                )
                assert round_prop["propertyDatatype"]["label"] == orig_prop["propertyDatatype"]["label"]
