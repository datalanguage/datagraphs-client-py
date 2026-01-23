import json
import pytest
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.schema import SchemaError, ClassNotFoundError, PropertyExistsError, PropertyNotFoundError
from datagraphs.datatypes import DATATYPE

class TestSchemaInitialization:
    def test_should_initialize_empty_schema(self):
        schema = DatagraphsSchema()
        dict_schema = schema.to_dict()  
        assert dict_schema["id"] is not None
        assert dict_schema["guid"] in dict_schema["id"]
        assert dict_schema["type"] == "DomainModel"
        assert dict_schema["name"] == "Domain Model v1.0"
        assert dict_schema["createdDate"] is not None
        assert dict_schema["lastModifiedDate"] is not None
        assert dict_schema["classes"] == []

    def test_should_initialize_with_data(self):
        data = {
            "id": "urn:models:123",
            "guid": "123",
            "type": "DomainModel",
            "name": "Domain Model",
            "description": "",
            "project": "urn:datagraphs:custom_project",
            "createdDate": "2024-06-01T00:00:00Z",
            "lastModifiedDate": "2024-06-01T00:00:00Z",
            "classes": [{
                "TestClass": {
                    "properties": {
                        "name": {"type": "string"}
                    }
                }
            }]
        }
        schema = DatagraphsSchema(data)
        assert schema.to_dict() == data

    def test_should_raise_error_on_invalid_schema(self):
        invalid_data = {
            "project_urn": "urn:datagraphs:custom_project",
            # Missing version and classes
        }
        with pytest.raises(SchemaError):
            DatagraphsSchema(invalid_data)

class TestSchemaClassFunctions:
    def setup_method(self):
        self.schema = DatagraphsSchema()
        self.schema.create_class("TestClass")
        self.schema.create_prop("TestClass", "prop1", DATATYPE.TEXT)

    def test_should_find_existing_class_by_name(self):
        cls = self.schema.find_class("TestClass")
        assert cls is not None
        assert cls["label"] == "TestClass"

    def test_should_return_none_for_nonexistent_class(self):
        cls = self.schema.find_class("NonExistentClass")
        assert cls is None

    def test_should_create_a_simple_class(self):
        self.schema.create_class("NewClass", description="A new test class", label_prop_name="test", is_label_prop_lang_string=True)
        cls = self.schema.find_class("NewClass")
        assert cls["description"] == "A new test class"
        assert cls["labelProperty"] == "test"
        assert cls["objectProperties"][0]["propertyName"] == "test"
        assert cls["objectProperties"][0]["isLangString"] is True
        assert cls["label"] == "NewClass"

    def test_should_create_a_class_with_baseclass(self):
        self.schema.create_class("NewClass", parent_class_name="TestClass")
        cls = self.schema.find_class("NewClass")
        assert cls["label"] == "NewClass"
        assert cls["parentClass"] == "TestClass"
        assert "TestClass" in cls["parentClasses"]

    def test_should_raise_error_on_duplicate_class(self):
        with pytest.raises(SchemaError):
            self.schema.create_class("TestClass")

    def test_should_create_subclass_with_inherited_properties(self):
        self.schema.create_subclass("SubClass", "description", "TestClass")
        cls = self.schema.find_class("SubClass")
        assert len(cls["objectProperties"]) == 2

    def test_should_rename_a_class(self):
        self.schema.update_class("TestClass", new_name="RenamedClass")
        cls = self.schema.find_class("RenamedClass")
        assert cls is not None
        assert self.schema.find_class("TestClass") is None

    def test_should_update_a_class_description(self):
        self.schema.update_class("TestClass", new_description="Updated description")
        cls = self.schema.find_class("TestClass")
        assert cls["description"] == "Updated description"

    def test_should_assign_a_base_class(self):
        self.schema.create_class("BaseClass")
        self.schema.assign_baseclass("TestClass", parent_class_name="AnotherClass")
        cls = self.schema.find_class("TestClass")
        assert cls["parentClass"] == "AnotherClass"
        assert "AnotherClass" in cls["parentClasses"]

    def test_should_assign_a_new_base_class(self):
        self.schema.create_class("AnotherClass")
        self.schema.update_class("TestClass", parent_class_name="AnotherClass")
        cls = self.schema.find_class("TestClass")
        assert cls["parentClass"] == "AnotherClass"
        assert "AnotherClass" in cls["parentClasses"]

    def test_should_delete_class_from_schema(self):
        self.schema.create_class("ToBeDeleted")
        cls = self.schema.find_class("ToBeDeleted")
        assert cls is not None
        self.schema.delete_class("ToBeDeleted")
        cls = self.schema.find_class("ToBeDeleted")
        assert cls is None

    def test_should_remove_subclass_links_when_class_is_deleted(self):
        self.schema.create_class("ToBeDeleted")
        self.schema.assign_baseclass("TestClass", parent_class_name="ToBeDeleted")
        cls = self.schema.find_class("TestClass")
        assert "ToBeDeleted" in cls["parentClasses"]
        self.schema.delete_class("ToBeDeleted")
        cls = self.schema.find_class("TestClass")
        assert "ToBeDeleted" not in cls["parentClasses"]

    def test_should_delete_property_references_when_class_is_deleted(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_prop("AnotherClass", "refProp", "TestClass")
        self.schema.delete_class("TestClass", include_linked_props=True)
        another_cls = self.schema.find_class("AnotherClass")
        assert len(another_cls["objectProperties"]) == 1

    def test_should_assign_new_label_property(self):
        cls = self.schema.find_class("TestClass")
        assert cls["labelProperty"] == "label"
        self.schema.create_prop("TestClass", "newLabelProp", DATATYPE.TEXT)
        self.schema.assign_label_prop("TestClass", prop_name="newLabelProp")
        cls = self.schema.find_class("TestClass")
        assert cls["labelProperty"] == "newLabelProp"

    def test_should_assign_label_autogen_expression(self):
        autogen_pattern = "{{ CONCATENATE('hello', ' ', 'world') }}"
        cls = self.schema.find_class("TestClass")
        self.schema.assign_label_autogen("TestClass", pattern=autogen_pattern)
        cls = self.schema.find_class("TestClass")
        assert cls['objectProperties'][0]["propertyValuePattern"] == autogen_pattern

    def test_should_update_class_description(self):
        self.schema.assign_class_description("TestClass", description="New description")
        cls = self.schema.find_class("TestClass")
        assert cls["description"] == "New description"

class TestSchemaPropertyFunctions:
    def setup_method(self):
        self.schema = DatagraphsSchema()
        self.schema.create_class("TestClass")

    def test_should_create_property_in_class(self):
        self.schema.create_prop("TestClass", "newProp", DATATYPE.INTEGER, description="An integer property")
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop is not None

    def test_should_raise_error_on_duplicate_property(self):
        self.schema.create_prop("TestClass", "dupProp", DATATYPE.TEXT)
        with pytest.raises(PropertyExistsError):
            self.schema.create_prop("TestClass", "dupProp", DATATYPE.TEXT)