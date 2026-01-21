import json
import pytest
from datagraphs.schema import Schema as DatagraphsSchema, SchemaError, ClassNotFoundError, PropertyExistsError, PropertyNotFoundError

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

    def test_should_find_class_by_name(self):
        cls = self.schema.find_class("TestClass")
        assert cls is not None
        assert cls["label"] == "TestClass"

    def test_should_create_class(self):
        self.schema.create_class("NewClass")
        cls = self.schema.find_class("NewClass")
        assert cls is not None
        assert cls["label"] == "NewClass"

    # def test_should_raise_error_on_duplicate_class(self):
    #     with pytest.raises(SchemaError):
    #         self.schema.create_class("TestClass")

    # def test_should_find_existing_class(self):
    #     cls = self.schema.find_class("TestClass")
    #     assert cls is not None
    #     assert cls["label"] == "TestClass"

    # def test_should_return_none_for_nonexistent_class(self):
    #     cls = self.schema.find_class("NonExistentClass")
    #     assert cls is None