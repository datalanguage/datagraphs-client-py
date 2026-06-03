import copy
import json
import pytest
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.schema import (
    PropertyExistsError,
    InvalidInversePropertyError,
    SchemaError,
    ClassNotFoundError,
    PropertyNotFoundError,
    Change,
    RenameMap,
    _diff,
    _replay_identities,
    _annotate,
)
from datagraphs.enums import DATATYPE

class TestSchemaInitialization:
    def test_should_initialize_empty_schema(self):
        schema = DatagraphsSchema()
        dict_schema = schema.to_dict()  
        assert dict_schema["name"] == "Domain Model v1.0"
        assert dict_schema["createdDate"] is not None
        assert dict_schema["lastModifiedDate"] is not None
        assert dict_schema["classes"] == []
        assert schema.classes == []

    def test_should_initialize_with_data(self):
        data = {
            "name": "Domain Model",
            "createdDate": "2024-06-01T00:00:00Z",
            "lastModifiedDate": "2024-06-01T00:00:00Z",
            "classes": [{
                "type": "Class",
                "name": "TestClass",
                "labelProperty": "label",
                "identifierProperty": "id",
                "properties": [],
                "isAbstract": False,
            }]
        }
        schema = DatagraphsSchema.create_from(data)
        assert len(schema.classes) == 1
        assert schema.classes[0]["name"] == "TestClass"

    def test_should_initialize_from_legacy_format(self):
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
                "label": "TestClass",
                "labelProperty": "label",
                "identifierProperty": "id",
                "parentClasses": ["TestClass"],
                "objectProperties": [{
                    "propertyName": "label",
                    "isOptional": False,
                    "isArray": False,
                    "propertyDatatype": {
                        "id": "urn:datagraphs:datatypes:text",
                        "type": "PropertyDatatype",
                        "label": "text",
                        "elasticsearchDatatype": "text",
                        "xsdDatatype": "string"
                    },
                    "isNestedObject": False,
                    "guid": "abc",
                    "propertyOrder": 0,
                    "isLangString": True,
                    "id": "urn:models:123:classes:TestClass:label"
                }]
            }]
        }
        schema = DatagraphsSchema.create_from(data)
        assert len(schema.classes) == 1
        assert schema.classes[0]["name"] == "TestClass"
        assert schema.classes[0]["type"] == "Class"
        assert schema.classes[0]["properties"][0]["name"] == "label"

    def test_should_raise_error_on_invalid_schema(self):
        invalid_data = {
            "something": "unexpected",
        }
        with pytest.raises(SchemaError):
            DatagraphsSchema.create_from(invalid_data)

    def test_should_raise_error_if_dict_passed_to_constructor(self):
        with pytest.raises(TypeError):
            DatagraphsSchema({"name": "Invalid"})

    def test_should_generate_correct_name_with_version(self):
        schema = DatagraphsSchema(name="Custom Model", version="2.0")
        dict_schema = schema.to_dict()
        assert dict_schema["name"] == "Custom Model v2.0"

class TestSchemaClassFunctions:
    def setup_method(self):
        self.schema = DatagraphsSchema()
        self.schema.create_class("TestClass")
        self.schema.create_property("TestClass", "prop1", DATATYPE.TEXT)

    def test_should_find_existing_class_by_name(self):
        cls = self.schema.find_class("TestClass")
        assert cls is not None
        assert cls["name"] == "TestClass"

    def test_should_return_none_for_nonexistent_class(self):
        cls = self.schema.find_class("NonExistentClass")
        assert cls is None

    def test_should_create_a_simple_class(self):
        self.schema.create_class("NewClass", description="A new test class", label_prop_name="test", is_label_prop_lang_string=True)
        cls = self.schema.find_class("NewClass")
        assert cls["description"] == {"en": "A new test class", "@none": "A new test class"}
        assert cls["labelProperty"] == "test"
        assert cls["properties"][0]["name"] == "test"
        assert cls["properties"][0]["isLangString"] is True
        assert cls["name"] == "NewClass"

    def test_should_create_a_class_with_baseclass(self):
        self.schema.create_class("NewClass", parent_class_name="TestClass")
        cls = self.schema.find_class("NewClass")
        assert cls["name"] == "NewClass"
        assert cls["subClassOf"] == "TestClass"

    def test_should_raise_error_on_duplicate_class(self):
        with pytest.raises(SchemaError):
            self.schema.create_class("TestClass")

    def test_should_create_subclass_with_inherited_properties(self):
        self.schema.create_subclass("SubClass", "description", "TestClass")
        cls = self.schema.find_class("SubClass")
        assert len(cls["properties"]) == 2

    def test_should_rename_a_class(self):
        self.schema.update_class("TestClass", new_name="RenamedClass")
        cls = self.schema.find_class("RenamedClass")
        assert cls is not None
        assert self.schema.find_class("TestClass") is None

    def test_should_update_a_class_description(self):
        self.schema.update_class("TestClass", new_description="Updated description")
        cls = self.schema.find_class("TestClass")
        assert cls["description"] == {"en": "Updated description", "@none": "Updated description"}

    def test_should_assign_a_base_class(self):
        self.schema.create_class("BaseClass")
        self.schema.assign_baseclass("TestClass", parent_class_name="AnotherClass")
        cls = self.schema.find_class("TestClass")
        assert cls["subClassOf"] == "AnotherClass"

    def test_should_assign_a_new_base_class(self):
        self.schema.create_class("AnotherClass")
        self.schema.update_class("TestClass", parent_class_name="AnotherClass")
        cls = self.schema.find_class("TestClass")
        assert cls["subClassOf"] == "AnotherClass"

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
        assert cls["subClassOf"] == "ToBeDeleted"
        self.schema.delete_class("ToBeDeleted")
        cls = self.schema.find_class("TestClass")
        assert "subClassOf" not in cls

    def test_should_delete_property_references_when_class_is_deleted(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("AnotherClass", "refProp", "TestClass")
        self.schema.delete_class("TestClass", include_linked_properties=True)
        another_cls = self.schema.find_class("AnotherClass")
        assert len(another_cls["properties"]) == 1

    def test_should_assign_new_label_property(self):
        cls = self.schema.find_class("TestClass")
        assert cls["labelProperty"] == "label"
        self.schema.create_property("TestClass", "newLabelProp", DATATYPE.TEXT)
        self.schema.assign_label_property("TestClass", prop_name="newLabelProp")
        cls = self.schema.find_class("TestClass")
        assert cls["labelProperty"] == "newLabelProp"

    def test_should_assign_label_autogen_expression(self):
        autogen_pattern = "{{ CONCATENATE('hello', ' ', 'world') }}"
        cls = self.schema.find_class("TestClass")
        self.schema.assign_label_autogen("TestClass", pattern=autogen_pattern)
        cls = self.schema.find_class("TestClass")
        assert cls['properties'][0]["propertyValuePattern"] == autogen_pattern

    def test_should_update_class_description(self):
        self.schema.assign_class_description("TestClass", description="New description")
        cls = self.schema.find_class("TestClass")
        assert cls["description"] == {"en": "New description", "@none": "New description"}

    def test_should_raise_error_when_creating_subclass_with_nonexistent_parent(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.create_subclass("SubClass", "description", "NonExistent")

    def test_should_raise_error_when_updating_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.update_class("NonExistent", new_name="NewName")

    def test_should_raise_error_when_deleting_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.delete_class("NonExistent")

    def test_should_raise_error_when_assigning_label_property_to_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.assign_label_property("NonExistent", "prop1")

    def test_should_raise_error_when_assigning_label_property_with_nonexistent_property(self):
        with pytest.raises(PropertyNotFoundError):
            self.schema.assign_label_property("TestClass", "nonExistentProp")

    def test_should_raise_error_when_assigning_label_autogen_to_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.assign_label_autogen("NonExistent", "pattern")

    def test_should_raise_error_when_label_autogen_label_property_missing(self):
        self.schema.delete_property("TestClass", "label")
        with pytest.raises(PropertyNotFoundError):
            self.schema.assign_label_autogen("TestClass", "pattern")

    def test_should_raise_error_when_assigning_baseclass_to_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.assign_baseclass("NonExistent", "TestClass")

    def test_should_raise_error_when_assigning_description_to_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.assign_class_description("NonExistent", "description")

class TestSchemaPropertyFunctions:

    @pytest.fixture(scope="function",autouse=True)
    def setup_method(self):
        self.schema = DatagraphsSchema()
        self.schema.create_class("TestClass")

    def test_should_create_property_in_class(self):
        self.schema.create_property("TestClass", "newProp", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop is not None

    def test_should_create_cascading_properties_in_subclass(self):
        self.schema.create_subclass("SubClass", "description", "TestClass")
        self.schema.create_property("TestClass", "newProp", DATATYPE.INTEGER, apply_to_subclasses=True)
        cls = self.schema.find_class("SubClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop is not None

    def test_should_raise_error_on_duplicate_property(self):
        self.schema.create_property("TestClass", "dupProp", DATATYPE.TEXT)
        with pytest.raises(PropertyExistsError):
            self.schema.create_property("TestClass", "dupProp", DATATYPE.TEXT)

    def test_should_create_property_with_specified_description(self):
        desc = "A test property"
        self.schema.create_property("TestClass", "newProp", DATATYPE.INTEGER, description=desc)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop["description"] == {"en": desc, "@none": desc}

    def test_should_create_property_with_specified_datatype(self):
        self.schema.create_property("TestClass", "newProp", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop["type"] == "DatatypeProperty"
        assert prop["range"] == "integer"

    def test_should_create_text_property_with_multilanguage_support(self):
        self.schema.create_property("TestClass", "newProp", DATATYPE.TEXT, is_lang_string=True)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop["type"] == "DatatypeProperty"
        assert prop["range"] == "text"
        assert prop["isLangString"] is True 

    def test_should_create_array_property(self):
        self.schema.create_property("TestClass", "newProp", DATATYPE.TEXT, is_array=True)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop["isArray"] is True

    def test_should_create_nested_property(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("TestClass", "newProp", datatype="AnotherClass", is_nested=True)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop["isNestedObject"] is True

    def test_should_create_an_inverse_property(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("AnotherClass", "prop", "TestClass")
        self.schema.create_property("TestClass", "newProp", "AnotherClass", inverse_of="prop")
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["properties"] if p["name"] == "newProp"), None)
        assert prop["inverseOf"] == "prop"

    def test_should_not_allow_inverse_property_on_a_datatype_property(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("AnotherClass", "prop", DATATYPE.TEXT)
        with pytest.raises(InvalidInversePropertyError):
            self.schema.create_property("TestClass", "newProp", DATATYPE.TEXT, inverse_of="prop")

    def test_should_not_allow_inverse_property_if_property_does_not_exist_on_target(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("AnotherClass", "prop", DATATYPE.TEXT)
        with pytest.raises(InvalidInversePropertyError):
            self.schema.create_property("TestClass", "newProp", "AnotherClass", inverse_of="propNonExistent")

    def test_should_not_allow_inverse_property_if_range_type_mismatch(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("AnotherClass", "prop", DATATYPE.TEXT)
        with pytest.raises(InvalidInversePropertyError):
            self.schema.create_property("TestClass", "newProp", "AnotherClass", inverse_of="prop")

    def test_should_create_enum_property(self):
        enums = ["Option1", "Option2", "Option3"]
        self.schema.create_property("TestClass", "enumProp", DATATYPE.ENUM, enums=enums)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "enumProp")
        assert prop["type"] == "DatatypeProperty"
        assert prop["range"] == "enum"
        assert prop["validationRules"][0]["value"] == enums

    def test_should_create_required_property(self):
        self.schema.create_property("TestClass", "requiredProp", DATATYPE.TEXT, is_optional=False)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "requiredProp")
        assert prop["isOptional"] is False

    def test_should_create_synonym_property(self):
        self.schema.create_property("TestClass", "synonymProp", DATATYPE.TEXT, is_synonym=True)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "synonymProp")
        assert prop["isLabelSynonym"] is True

    def test_should_create_filterable_property(self):
        self.schema.create_property("TestClass", "filterableProp", DATATYPE.TEXT, is_filterable=True)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "filterableProp")
        assert prop["isFilterable"] is True

    def test_should_not_create_filterable_property_if_not_specified(self):
        self.schema.create_property("TestClass", "filterableProp", DATATYPE.TEXT)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "filterableProp")
        assert "isFilterable" not in prop

    def test_should_assign_property_description(self):
        self.schema.create_property("TestClass", "propToDescribe", DATATYPE.INTEGER)
        self.schema.update_property("TestClass", "propToDescribe", description="This is a description")
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "propToDescribe")
        assert prop["description"] == {"en": "This is a description", "@none": "This is a description"}

    def test_should_change_property_cardinality(self):
        self.schema.create_property("TestClass", "propToChangeCardinality", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "propToChangeCardinality")
        assert prop["isArray"] is False 
        self.schema.update_property("TestClass", "propToChangeCardinality", is_array=True)
        prop = self.schema.find_property(cls["properties"], "propToChangeCardinality")
        assert prop["isArray"] is True

    def test_should_set_property_filterability(self):
        self.schema.create_property("TestClass", "propToFilter", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "propToFilter")
        assert "isFilterable" not in prop
        self.schema.update_property("TestClass", "propToFilter", is_filterable=True)
        prop = self.schema.find_property(cls["properties"], "propToFilter")
        assert prop["isFilterable"] is True
        self.schema.update_property("TestClass", "propToFilter", is_filterable=False)
        prop = self.schema.find_property(cls["properties"], "propToFilter")
        assert prop["isFilterable"] is False

    def test_should_set_property_as_required(self):
        self.schema.create_property("TestClass", "propToRequire", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "propToRequire")
        assert prop["isOptional"] is True 
        self.schema.update_property("TestClass", "propToRequire", is_optional=False)
        prop = self.schema.find_property(cls["properties"], "propToRequire")
        assert prop["isOptional"] is False

    def test_should_update_property_datatype(self):
        self.schema.create_property("TestClass", "propToUpdate", DATATYPE.INTEGER)
        self.schema.update_property("TestClass", "propToUpdate", datatype=DATATYPE.TEXT)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "propToUpdate")
        assert prop["type"] == "DatatypeProperty"
        assert prop["range"] == "text"

    def test_should_update_property_to_enum(self):
        self.schema.create_property("TestClass", "propToEnum", DATATYPE.INTEGER)
        enums = ["OptionA", "OptionB"]
        self.schema.update_property("TestClass", "propToEnum", datatype=DATATYPE.ENUM, enums=enums)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "propToEnum")
        assert prop["type"] == "DatatypeProperty"
        assert prop["range"] == "enum"
        assert prop["validationRules"][0]["value"] == enums

    def test_should_update_enum_property_options(self):
        enums = ["Option1", "Option2", "Option3"]
        self.schema.create_property("TestClass", "enumProp", DATATYPE.ENUM, enums=enums)
        new_enums = ["OptionA", "OptionB"]
        self.schema.update_property("TestClass", "enumProp", enums=new_enums)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "enumProp")
        assert prop["validationRules"][0]["value"] == new_enums

    def test_should_update_property_across_subclasses(self):
        self.schema.create_subclass("SubClass", "description", "TestClass")
        self.schema.create_property("TestClass", "propToUpdate", DATATYPE.INTEGER, apply_to_subclasses=True)
        self.schema.update_property("TestClass", "propToUpdate", datatype=DATATYPE.TEXT, apply_to_subclasses=True)
        cls = self.schema.find_class("SubClass")
        prop = self.schema.find_property(cls["properties"], "propToUpdate")
        assert prop["type"] == "DatatypeProperty"
        assert prop["range"] == "text"

    def test_should_rename_property(self):
        self.schema.create_property("TestClass", "propToRename", DATATYPE.INTEGER)
        self.schema.rename_property("TestClass", "propToRename", "renamedProp")
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "renamedProp")
        assert prop is not None
        assert self.schema.find_property(cls["properties"], "propToRename") is None

    def test_should_delete_property_from_class(self):
        self.schema.create_property("TestClass", "propToDelete", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["properties"], "propToDelete")
        assert prop is not None
        self.schema.delete_property("TestClass", "propToDelete")
        prop = self.schema.find_property(cls["properties"], "propToDelete")
        assert prop is None

    def test_should_assign_property_orders(self):
        self.schema.create_property("TestClass", "firstProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "secondProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "thirdProp", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop_names = [p["name"] for p in cls["properties"]]
        assert prop_names == ["label", "firstProp", "secondProp", "thirdProp"]
        self.schema.assign_property_orders({"TestClass": ["label", "secondProp", "thirdProp", "firstProp"]})
        cls = self.schema.find_class("TestClass")
        prop_names = [p["name"] for p in cls["properties"]]
        assert prop_names == ["label", "secondProp", "thirdProp", "firstProp"]

    def test_should_assign_default_property_orders_if_not_specified(self):
        self.schema.create_property("TestClass", "firstProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "secondProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "thirdProp", DATATYPE.INTEGER)
        self.schema.assign_property_orders({})
        cls = self.schema.find_class("TestClass")
        prop_names = [p["name"] for p in cls["properties"]]
        assert prop_names == ["label", "firstProp", "secondProp", "thirdProp"]

    def test_should_perform_deep_copy_when_performing_clone_schema(self):
        self.schema.create_property("TestClass", "prop1", DATATYPE.TEXT)
        cloned_schema = self.schema.clone()
        cloned_schema.update_class("TestClass", new_description="Updated description")
        original_cls = self.schema.find_class("TestClass")
        cloned_cls = cloned_schema.find_class("TestClass")
        assert "description" not in original_cls or original_cls.get("description") != cloned_cls["description"]
        assert cloned_cls["description"] == {"en": "Updated description", "@none": "Updated description"}

    def test_should_raise_error_when_creating_property_on_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.create_property("NonExistent", "prop", DATATYPE.TEXT)

    def test_should_raise_error_when_renaming_property_on_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.rename_property("NonExistent", "oldProp", "newProp")

    def test_should_raise_error_when_updating_nonexistent_prop(self):
        with pytest.raises(PropertyNotFoundError):
            self.schema.update_property("TestClass", "nonExistentProp", description="desc")

    def test_should_raise_error_when_deleting_property_on_nonexistent_class(self):
        with pytest.raises(ClassNotFoundError):
            self.schema.delete_property("NonExistent", "prop")

    def test_should_raise_error_when_deleting_nonexistent_property(self):
        with pytest.raises(PropertyNotFoundError):
            self.schema.delete_property("TestClass", "nonExistentProp")


class TestChangeTrackingBaseline:
    """Phase 1 — tracking state and baseline capture.

    Asserts that the scaffolding is present and inert:
    - Tracking attributes are initialised correctly on every construction path.
    - Baseline is captured post-construction / post-transform (0 changes on load).
    - clone() routes through create_from and therefore starts at 0 changes with
      an independent baseline.
    - Tracking state never leaks into to_dict() or to_json().
    """

    # ------------------------------------------------------------------ helpers
    # Class-level dicts are CONSTANTS — never passed directly to create_from.
    # Always obtain a fresh deep copy via the factory methods below so that
    # mutations made by Schema (which aliases its input dict) cannot leak
    # between tests.

    _NEW_FORMAT_DATA = {
        "name": "My Model v1.0",
        "createdDate": "2024-06-01T00:00:00Z",
        "lastModifiedDate": "2024-06-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Drug",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    }
                ],
            }
        ],
    }

    _LEGACY_FORMAT_DATA = {
        "id": "urn:models:999",
        "guid": "999",
        "type": "DomainModel",
        "name": "Legacy Model",
        "description": "",
        "project": "urn:datagraphs:custom_project",
        "createdDate": "2024-06-01T00:00:00Z",
        "lastModifiedDate": "2024-06-01T00:00:00Z",
        "classes": [
            {
                "label": "Substance",
                "labelProperty": "label",
                "identifierProperty": "id",
                "parentClasses": ["Substance"],
                "objectProperties": [
                    {
                        "propertyName": "label",
                        "isOptional": False,
                        "isArray": False,
                        "propertyDatatype": {
                            "id": "urn:datagraphs:datatypes:text",
                            "type": "PropertyDatatype",
                            "label": "text",
                            "elasticsearchDatatype": "text",
                            "xsdDatatype": "string",
                        },
                        "isNestedObject": False,
                        "guid": "abc",
                        "propertyOrder": 0,
                        "isLangString": True,
                        "id": "urn:models:999:classes:Substance:label",
                    }
                ],
            }
        ],
    }

    def _new_format_data(self) -> dict:
        """Return a fresh deep copy of _NEW_FORMAT_DATA for each test."""
        return copy.deepcopy(TestChangeTrackingBaseline._NEW_FORMAT_DATA)

    def _legacy_format_data(self) -> dict:
        """Return a fresh deep copy of _LEGACY_FORMAT_DATA for each test."""
        return copy.deepcopy(TestChangeTrackingBaseline._LEGACY_FORMAT_DATA)

    # -------------------------------------------------------------- new-format

    def test_empty_schema_has_empty_change_log(self):
        schema = DatagraphsSchema()
        assert schema._change_log == []

    def test_empty_schema_has_zero_tracking_depth(self):
        schema = DatagraphsSchema()
        assert schema._tracking_depth == 0

    def test_empty_schema_has_baseline(self):
        schema = DatagraphsSchema()
        assert isinstance(schema._baseline, dict)
        assert "classes" in schema._baseline

    def test_empty_schema_baseline_is_independent_copy(self):
        """Mutating _schema after construction must not change _baseline."""
        schema = DatagraphsSchema()
        original_baseline_classes = list(schema._baseline["classes"])
        schema.create_class("NewClass")
        # Baseline should still reflect the empty-classes state.
        assert schema._baseline["classes"] == original_baseline_classes
        assert len(schema._schema["classes"]) == 1

    def test_create_from_has_empty_change_log(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        assert schema._change_log == []

    def test_create_from_has_zero_tracking_depth(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        assert schema._tracking_depth == 0

    def test_create_from_has_baseline_matching_loaded_data(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        # Baseline classes should reflect the loaded data, not the empty state.
        assert len(schema._baseline["classes"]) == 1
        assert schema._baseline["classes"][0]["name"] == "Drug"

    def test_create_from_baseline_is_independent_copy(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        original_count = len(schema._baseline["classes"])
        schema.create_class("ExtraClass")
        assert len(schema._baseline["classes"]) == original_count
        assert len(schema._schema["classes"]) == original_count + 1

    # ----------------------------------------------------------- legacy-format

    def test_legacy_load_has_empty_change_log(self):
        """Loading a legacy schema must report 0 changes (baseline is post-transform)."""
        schema = DatagraphsSchema.create_from(self._legacy_format_data())
        assert schema._change_log == []

    def test_legacy_load_has_zero_tracking_depth(self):
        schema = DatagraphsSchema.create_from(self._legacy_format_data())
        assert schema._tracking_depth == 0

    def test_legacy_load_baseline_reflects_transformed_data(self):
        """Baseline is captured after old_to_new transform, so it matches the
        new-format representation, not the original legacy dict."""
        schema = DatagraphsSchema.create_from(self._legacy_format_data())
        # After transform the class should have been renamed from the legacy
        # 'label' field to 'name'.
        assert len(schema._baseline["classes"]) == 1
        assert schema._baseline["classes"][0].get("name") == "Substance"
        assert schema._baseline["classes"][0].get("type") == "Class"

    # --------------------------------------------------------------- clone

    def test_clone_has_empty_change_log(self):
        """clone() routes through create_from so it must start at 0 changes."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        schema.create_class("TransientClass")  # would add to change_log once Phase 2 lands
        cloned = schema.clone()
        assert cloned._change_log == []

    def test_clone_has_zero_tracking_depth(self):
        cloned = DatagraphsSchema.create_from(self._new_format_data()).clone()
        assert cloned._tracking_depth == 0

    def test_clone_baseline_matches_cloned_state(self):
        """The clone's baseline must reflect what was cloned, not the original
        schema's baseline."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        cloned = schema.clone()
        assert cloned._baseline["classes"][0]["name"] == "Drug"

    def test_clone_baseline_is_independent_from_original(self):
        """Mutating the clone must not affect the original's baseline."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        cloned = schema.clone()
        cloned.create_class("CloneOnly")
        assert len(schema._baseline["classes"]) == 1
        assert len(cloned._baseline["classes"]) == 1  # cloned baseline unchanged
        assert len(cloned._schema["classes"]) == 2    # live dict has the addition

    # ------------------------------------------------ serialisation exclusion

    def test_baseline_absent_from_to_dict(self):
        schema = DatagraphsSchema()
        d = schema.to_dict()
        assert "_baseline" not in d
        assert "_change_log" not in d
        assert "_tracking_depth" not in d

    def test_baseline_absent_from_to_json(self):
        schema = DatagraphsSchema()
        j = schema.to_json()
        assert "_baseline" not in j
        assert "_change_log" not in j
        assert "_tracking_depth" not in j

    def test_create_from_baseline_absent_from_to_dict(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        d = schema.to_dict()
        assert "_baseline" not in d
        assert "_change_log" not in d
        assert "_tracking_depth" not in d

    def test_create_from_baseline_absent_from_to_json(self):
        schema = DatagraphsSchema.create_from(self._new_format_data())
        j = schema.to_json()
        assert "_baseline" not in j
        assert "_change_log" not in j
        assert "_tracking_depth" not in j

    def test_to_dict_output_byte_identical_before_and_after_phase1(self):
        """to_dict() must return the live _schema unchanged — no tracking keys."""
        schema = DatagraphsSchema.create_from(self._new_format_data())
        result = schema.to_dict()
        # The live _schema is exactly what was passed in (minus metadata updates
        # from _set_internal_schema, which update lastModifiedDate in-place).
        assert result is schema._schema  # same object, not a copy


class TestChangeTrackingRecording:
    """Phase 2 — op-log instrumentation of the 14 public mutating methods.

    Verifies that:
    - Each method records exactly one entry with the correct op name and
      intent-bearing args on a successful outermost call.
    - Re-entrant inner calls (create_subclass -> create_class + create_property,
      and apply_to_subclasses recursion) record nothing of their own.
    - A mutation that raises appends nothing.
    - Schema() / create_from / clone append nothing.
    """

    # ------------------------------------------------------------------ helpers
    # Fixtures are constants — always obtain fresh instances via the methods
    # below so that mutations (Schema aliases its input dict) cannot leak
    # between tests.

    _BASE_DATA = {
        "name": "Test Model v1.0",
        "createdDate": "2024-01-01T00:00:00Z",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Animal",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "age",
                        "range": "integer",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                ],
            }
        ],
    }

    def _base_data(self) -> dict:
        return copy.deepcopy(TestChangeTrackingRecording._BASE_DATA)

    def _schema(self) -> DatagraphsSchema:
        """Return a fresh Schema pre-populated with one class ('Animal') and
        two properties ('label', 'age') — change_log starts empty."""
        return DatagraphsSchema.create_from(self._base_data())

    def _schema_with_subclass(self) -> DatagraphsSchema:
        """Return a fresh Schema where 'Dog' is a subclass of 'Animal'."""
        s = self._schema()
        s.create_class("Dog", parent_class_name="Animal")
        s._change_log.clear()
        return s

    # ------------------------------------------- construction records nothing

    def test_construction_schema_init_records_nothing(self):
        schema = DatagraphsSchema()
        assert schema._change_log == []

    def test_construction_create_from_records_nothing(self):
        schema = DatagraphsSchema.create_from(self._base_data())
        assert schema._change_log == []

    def test_construction_clone_records_nothing(self):
        schema = DatagraphsSchema.create_from(self._base_data())
        cloned = schema.clone()
        assert cloned._change_log == []

    # ---------------------------------------------------- create_class

    def test_create_class_records_one_entry(self):
        s = self._schema()
        s.create_class("Plant")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "create_class"
        assert s._change_log[0]["args"] == {"class_name": "Plant"}

    def test_create_class_duplicate_records_nothing(self):
        s = self._schema()
        with pytest.raises(SchemaError):
            s.create_class("Animal")
        assert s._change_log == []

    # ---------------------------------------------------- create_subclass

    def test_create_subclass_records_exactly_one_entry(self):
        """create_subclass internally calls create_class + create_property xN.
        Only one 'create_subclass' entry must appear — not the inner calls."""
        s = self._schema()
        s.create_subclass("Dog", "A dog", "Animal")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "create_subclass"
        assert s._change_log[0]["args"] == {
            "class_name": "Dog",
            "parent_class_name": "Animal",
            # Inherited-at-creation property names captured for the report's
            # inherited count + post-creation add surfacing (round-4 B2).
            "inherited_properties": ["label", "age"],
        }

    def test_create_subclass_bad_parent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.create_subclass("Dog", "A dog", "NonExistent")
        assert s._change_log == []

    # ---------------------------------------------------- update_class

    def test_update_class_records_one_entry(self):
        s = self._schema()
        s.update_class("Animal", new_name="Creature")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "update_class"
        assert s._change_log[0]["args"] == {"class_name": "Animal", "new_name": "Creature"}

    def test_update_class_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.update_class("NoSuchClass", new_name="X")
        assert s._change_log == []

    # ---------------------------------------------------- delete_class

    def test_delete_class_records_one_entry_with_cascade_flag(self):
        s = self._schema()
        s.delete_class("Animal", cascade_to_subclasses=False)
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "delete_class"
        assert s._change_log[0]["args"] == {
            "class_name": "Animal",
            "cascade_to_subclasses": False,
        }

    def test_delete_class_default_cascade_flag_is_recorded(self):
        s = self._schema()
        s.delete_class("Animal")
        assert s._change_log[0]["args"]["cascade_to_subclasses"] is True

    def test_delete_class_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.delete_class("Ghost")
        assert s._change_log == []

    # ---------------------------------------------------- assign_label_property

    def test_assign_label_property_records_one_entry(self):
        s = self._schema()
        # 'age' exists but we set it as label to test recording
        s.assign_label_property("Animal", "age")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "assign_label_property"
        assert s._change_log[0]["args"] == {"class_name": "Animal", "prop_name": "age"}

    def test_assign_label_property_missing_prop_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.assign_label_property("Animal", "nonExistentProp")
        assert s._change_log == []

    # ---------------------------------------------------- assign_label_autogen

    def test_assign_label_autogen_records_one_entry(self):
        s = self._schema()
        s.assign_label_autogen("Animal", "{{ name }}")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "assign_label_autogen"
        assert s._change_log[0]["args"] == {"class_name": "Animal"}

    def test_assign_label_autogen_nonexistent_class_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.assign_label_autogen("Ghost", "{{ name }}")
        assert s._change_log == []

    # ---------------------------------------------------- assign_baseclass

    def test_assign_baseclass_records_one_entry(self):
        s = self._schema()
        s.create_class("Mammal")
        s._change_log.clear()
        s.assign_baseclass("Animal", "Mammal")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "assign_baseclass"
        assert s._change_log[0]["args"] == {
            "class_name": "Animal",
            "parent_class_name": "Mammal",
        }

    def test_assign_baseclass_nonexistent_class_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.assign_baseclass("Ghost", "Animal")
        assert s._change_log == []

    # ---------------------------------------------------- assign_class_description

    def test_assign_class_description_records_one_entry(self):
        s = self._schema()
        s.assign_class_description("Animal", "A living organism")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "assign_class_description"
        assert s._change_log[0]["args"] == {"class_name": "Animal"}

    def test_assign_class_description_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.assign_class_description("Ghost", "desc")
        assert s._change_log == []

    # ---------------------------------------------------- create_property

    def test_create_property_records_one_entry(self):
        s = self._schema()
        s.create_property("Animal", "weight", DATATYPE.INTEGER)
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "create_property"
        args = s._change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "weight"
        assert args["apply_to_subclasses"] is False
        # No apply_to_subclasses intent => the op touched no subclasses.
        assert args["applied_subclasses"] == []

    def test_create_property_apply_to_subclasses_records_one_entry(self):
        """create_property with apply_to_subclasses=True recurses for each
        subclass — the outer call must still produce exactly one entry, and that
        entry records the op-time set of subclasses it applied to (FIX VR-B3)."""
        s = self._schema_with_subclass()
        s.create_property("Animal", "weight", DATATYPE.INTEGER, apply_to_subclasses=True)
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "create_property"
        args = s._change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "weight"
        assert args["apply_to_subclasses"] is True
        assert args["applied_subclasses"] == ["Dog"]

    def test_create_property_nonexistent_class_records_nothing(self):
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.create_property("Ghost", "weight", DATATYPE.INTEGER)
        assert s._change_log == []

    # ---------------------------------------------------- update_property

    def test_update_property_records_one_entry(self):
        s = self._schema()
        s.update_property("Animal", "age", is_optional=False)
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "update_property"
        args = s._change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "age"
        assert args["apply_to_subclasses"] is False
        assert args["applied_subclasses"] == []

    def test_update_property_apply_to_subclasses_records_one_entry(self):
        """update_property with apply_to_subclasses=True recurses for each
        subclass — the outer call must still produce exactly one entry, and that
        entry records the op-time set of subclasses it applied to (FIX VR-B3)."""
        s = self._schema_with_subclass()
        # First, ensure 'age' exists on the subclass too
        s.create_property("Dog", "age", DATATYPE.INTEGER)
        s._change_log.clear()
        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "update_property"
        args = s._change_log[0]["args"]
        assert args["class_name"] == "Animal"
        assert args["prop_name"] == "age"
        assert args["apply_to_subclasses"] is True
        assert args["applied_subclasses"] == ["Dog"]

    def test_update_property_nonexistent_prop_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.update_property("Animal", "nonExistentProp", is_optional=False)
        assert s._change_log == []

    # ---------------------------------------------------- rename_property

    def test_rename_property_records_one_entry(self):
        s = self._schema()
        s.rename_property("Animal", "age", "years")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "rename_property"
        assert s._change_log[0]["args"] == {
            "class_name": "Animal",
            "old_prop_name": "age",
            "new_prop_name": "years",
        }

    def test_rename_property_conflict_records_nothing(self):
        """Renaming to an already-existing name must raise and record nothing."""
        s = self._schema()
        with pytest.raises(PropertyExistsError):
            s.rename_property("Animal", "age", "label")
        assert s._change_log == []

    def test_rename_property_nonexistent_prop_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.rename_property("Animal", "ghost", "anything")
        assert s._change_log == []

    # ---------------------------------------------------- delete_property

    def test_delete_property_records_one_entry(self):
        s = self._schema()
        s.delete_property("Animal", "age")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "delete_property"
        assert s._change_log[0]["args"] == {"class_name": "Animal", "prop_name": "age"}

    def test_delete_property_nonexistent_records_nothing(self):
        s = self._schema()
        with pytest.raises(PropertyNotFoundError):
            s.delete_property("Animal", "ghost")
        assert s._change_log == []

    # ---------------------------------------------------- assign_property_orders

    def test_assign_property_orders_records_one_entry_with_copy(self):
        s = self._schema()
        orders = {"Animal": ["age", "label"]}
        s.assign_property_orders(orders)
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "assign_property_orders"
        recorded_orders = s._change_log[0]["args"]["property_orders"]
        assert recorded_orders == {"Animal": ["age", "label"]}

    def test_assign_property_orders_recorded_copy_is_independent(self):
        """The recorded property_orders must be a copy, not an alias of the
        caller's dict, so post-call mutations to the original do not alter the
        op-log entry."""
        s = self._schema()
        orders = {"Animal": ["age", "label"]}
        s.assign_property_orders(orders)
        orders["Animal"].append("extra")  # mutate the INNER list after the call
        recorded_orders = s._change_log[0]["args"]["property_orders"]
        assert recorded_orders == {"Animal": ["age", "label"]}

    # ---------------------------------------------------- update_schema_metadata

    def test_update_schema_metadata_records_one_entry(self):
        s = self._schema()
        s.update_schema_metadata(name="New Model", version="2.0")
        assert len(s._change_log) == 1
        assert s._change_log[0]["op"] == "update_schema_metadata"
        assert s._change_log[0]["args"] == {"name": "New Model", "version": "2.0"}

    def test_update_schema_metadata_during_construction_records_nothing(self):
        """update_schema_metadata is called inside __init__ before tracking
        state is initialised — the no-op guard must prevent any recording."""
        schema = DatagraphsSchema(name="My Model", version="1.5")
        assert schema._change_log == []

    def test_update_schema_metadata_during_create_from_records_nothing(self):
        """update_schema_metadata is called inside _set_internal_schema during
        create_from. The subsequent _change_log reset ensures 0 entries."""
        schema = DatagraphsSchema.create_from(self._base_data())
        assert schema._change_log == []

    # ---------------------------------------------------- tracking depth resets

    def test_tracking_depth_restored_after_exception(self):
        """A raising mutation must leave _tracking_depth at 0."""
        s = self._schema()
        with pytest.raises(ClassNotFoundError):
            s.delete_class("Ghost")
        assert s._tracking_depth == 0

    def test_tracking_depth_restored_after_success(self):
        s = self._schema()
        s.create_class("Plant")
        assert s._tracking_depth == 0

    # ---------------------------------------------------- multiple sequential calls

    def test_multiple_sequential_calls_each_append_one_entry(self):
        s = self._schema()
        s.create_class("Plant")
        s.create_class("Fungus")
        s.delete_class("Fungus")
        assert len(s._change_log) == 3
        assert s._change_log[0]["op"] == "create_class"
        assert s._change_log[1]["op"] == "create_class"
        assert s._change_log[2]["op"] == "delete_class"


# ---------------------------------------------------------------------------
# Phase 3 — Structural diff engine tests
# ---------------------------------------------------------------------------

class TestChangeDiff:
    """Tests for _diff(), _diff_class_fields(), and _diff_properties().

    All fixtures are hand-built dicts (or Schema mutations where convenient)
    and are deep-copied per-test so mutations do not leak between tests.

    Invariants verified here:
    - Correct add / remove / modify for classes and properties.
    - Field-level before->after for class fields and property fields.
    - Description normalisation (dict->plain text).
    - Date fields (createdDate, lastModifiedDate) NEVER appear in any Change.
    - create-then-delete of a class yields no entry.
    - modify-then-revert yields no entry.
    - Reorder candidate detected when property-name SET unchanged but SEQUENCE differs.
    """

    # ------------------------------------------------------------------
    # Fixture helpers
    # ------------------------------------------------------------------

    #: Minimal baseline schema — never mutated directly; copy via _baseline().
    _BASE: dict = {
        "name": "Test Model v1.0",
        "createdDate": "2024-01-01T00:00:00Z",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Animal",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "age",
                        "range": "integer",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                ],
            }
        ],
    }

    def _baseline(self) -> dict:
        """Return a fresh deep copy of the base fixture."""
        return copy.deepcopy(TestChangeDiff._BASE)

    def _current_from(self, base: dict) -> dict:
        """Return an independent deep copy of *base* to use as 'current'."""
        return copy.deepcopy(base)

    def _schema_from_base(self) -> DatagraphsSchema:
        """Return a fresh Schema loaded from _BASE (change_log starts empty)."""
        return DatagraphsSchema.create_from(self._baseline())

    # ------------------------------------------------------------------
    # Helper: assert no field equals a date key in any Change
    # ------------------------------------------------------------------

    def _assert_no_date_fields(self, changes: list[Change]) -> None:
        DATE_FIELDS = {"createdDate", "lastModifiedDate"}
        for ch in changes:
            if ch.fields:
                for f in ch.fields:
                    assert f["field"] not in DATE_FIELDS, (
                        f"Date field '{f['field']}' leaked into {ch}"
                    )
            if ch.target in DATE_FIELDS:
                raise AssertionError(f"Date field appeared as target in {ch}")

    # ------------------------------------------------------------------
    # No changes
    # ------------------------------------------------------------------

    def test_identical_schemas_produce_no_changes(self):
        b = self._baseline()
        c = self._current_from(b)
        changes = _diff(b, c)
        assert changes == []

    # ------------------------------------------------------------------
    # Metadata changes
    # ------------------------------------------------------------------

    def test_metadata_name_change_emits_one_metadata_change(self):
        b = self._baseline()
        c = self._current_from(b)
        c["name"] = "Test Model v2.0"
        changes = _diff(b, c)
        meta = [ch for ch in changes if ch.kind == "metadata"]
        assert len(meta) == 1
        assert meta[0].target == "schema.name"
        assert meta[0].op == "modified"
        assert meta[0].from_ == "Test Model v1.0"
        assert meta[0].to == "Test Model v2.0"

    def test_metadata_date_change_never_emitted(self):
        """Changing createdDate / lastModifiedDate must produce zero changes."""
        b = self._baseline()
        c = self._current_from(b)
        c["createdDate"] = "2099-12-31T23:59:59Z"
        c["lastModifiedDate"] = "2099-12-31T23:59:59Z"
        changes = _diff(b, c)
        self._assert_no_date_fields(changes)
        assert changes == []

    # ------------------------------------------------------------------
    # Class: added / removed
    # ------------------------------------------------------------------

    def test_added_class_emits_class_added_change(self):
        b = self._baseline()
        c = self._current_from(b)
        c["classes"].append({
            "type": "Class",
            "name": "Plant",
            "labelProperty": "label",
            "identifierProperty": "id",
            "isAbstract": False,
            "properties": [],
        })
        changes = _diff(b, c)
        added = [ch for ch in changes if ch.kind == "class" and ch.op == "added"]
        assert len(added) == 1
        assert added[0].target == "Plant"

    def test_removed_class_emits_class_removed_change(self):
        b = self._baseline()
        c = self._current_from(b)
        c["classes"] = [cls for cls in c["classes"] if cls["name"] != "Animal"]
        changes = _diff(b, c)
        removed = [ch for ch in changes if ch.kind == "class" and ch.op == "removed"]
        assert len(removed) == 1
        assert removed[0].target == "Animal"

    # ------------------------------------------------------------------
    # Class: modified — field-level before/after
    # ------------------------------------------------------------------

    def test_class_field_isAbstract_change_emits_modified(self):
        b = self._baseline()
        c = self._current_from(b)
        c["classes"][0]["isAbstract"] = True
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "class" and ch.op == "modified"]
        assert len(modified) == 1
        assert modified[0].target == "Animal"
        f_entry = next(f for f in modified[0].fields if f["field"] == "isAbstract")
        assert f_entry["before"] is False
        assert f_entry["after"] is True

    def test_class_field_subClassOf_change_emits_modified(self):
        b = self._baseline()
        c = self._current_from(b)
        c["classes"][0]["subClassOf"] = "LivingThing"
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "class" and ch.op == "modified"]
        assert len(modified) == 1
        f_entry = next(f for f in modified[0].fields if f["field"] == "subClassOf")
        assert f_entry["before"] is None
        assert f_entry["after"] == "LivingThing"

    def test_class_field_labelProperty_change_emits_modified(self):
        b = self._baseline()
        c = self._current_from(b)
        c["classes"][0]["labelProperty"] = "name"
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "class" and ch.op == "modified"]
        assert len(modified) == 1
        f_entry = next(f for f in modified[0].fields if f["field"] == "labelProperty")
        assert f_entry["before"] == "label"
        assert f_entry["after"] == "name"

    def test_class_field_description_normalised_to_text(self):
        """Description dict is normalised to plain text in before/after."""
        b = self._baseline()
        b["classes"][0]["description"] = {"en": "Old desc", "@none": "Old desc"}
        c = self._current_from(b)
        c["classes"][0]["description"] = {"en": "New desc", "@none": "New desc"}
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "class" and ch.op == "modified"]
        assert len(modified) == 1
        f_entry = next(f for f in modified[0].fields if f["field"] == "description")
        assert f_entry["before"] == "Old desc"
        assert f_entry["after"] == "New desc"

    def test_class_unchanged_fields_not_emitted(self):
        """Only genuinely changed fields appear in the fields list."""
        b = self._baseline()
        c = self._current_from(b)
        c["classes"][0]["isAbstract"] = True
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "class" and ch.op == "modified"]
        assert len(modified) == 1
        field_names = [f["field"] for f in modified[0].fields]
        # isAbstract changed; labelProperty, identifierProperty did NOT
        assert "isAbstract" in field_names
        assert "labelProperty" not in field_names
        assert "identifierProperty" not in field_names

    def test_class_identical_content_produces_no_modified_change(self):
        b = self._baseline()
        c = self._current_from(b)
        # Touch a date field only — must produce zero changes
        c["classes"][0]["lastModifiedDate"] = "2099-01-01T00:00:00Z"
        changes = _diff(b, c)
        self._assert_no_date_fields(changes)
        assert changes == []

    # ------------------------------------------------------------------
    # Property: added / removed / modified
    # ------------------------------------------------------------------

    def test_property_added_emits_property_added_change(self):
        b = self._baseline()
        c = self._current_from(b)
        c["classes"][0]["properties"].append({
            "type": "DatatypeProperty",
            "name": "weight",
            "range": "decimal",
            "isOptional": True,
            "isArray": False,
            "isLangString": False,
            "isLabelSynonym": False,
        })
        changes = _diff(b, c)
        added = [ch for ch in changes if ch.kind == "property" and ch.op == "added"]
        assert len(added) == 1
        assert added[0].target == "Animal.weight"

    def test_property_removed_emits_property_removed_change(self):
        b = self._baseline()
        c = self._current_from(b)
        c["classes"][0]["properties"] = [
            p for p in c["classes"][0]["properties"] if p["name"] != "age"
        ]
        changes = _diff(b, c)
        removed = [ch for ch in changes if ch.kind == "property" and ch.op == "removed"]
        assert len(removed) == 1
        assert removed[0].target == "Animal.age"

    def test_property_modified_isOptional_emits_field_level_change(self):
        b = self._baseline()
        c = self._current_from(b)
        # age: isOptional True -> False
        age_prop = next(p for p in c["classes"][0]["properties"] if p["name"] == "age")
        age_prop["isOptional"] = False
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "property" and ch.op == "modified"]
        assert len(modified) == 1
        assert modified[0].target == "Animal.age"
        f_entry = next(f for f in modified[0].fields if f["field"] == "isOptional")
        assert f_entry["before"] is True
        assert f_entry["after"] is False

    def test_property_modified_range_emits_field_level_change(self):
        b = self._baseline()
        c = self._current_from(b)
        age_prop = next(p for p in c["classes"][0]["properties"] if p["name"] == "age")
        age_prop["range"] = "text"
        age_prop["type"] = "DatatypeProperty"
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "property" and ch.op == "modified"]
        assert any(m.target == "Animal.age" for m in modified)
        animal_age_mod = next(m for m in modified if m.target == "Animal.age")
        range_entry = next(f for f in animal_age_mod.fields if f["field"] == "range")
        assert range_entry["before"] == "integer"
        assert range_entry["after"] == "text"

    def test_property_modified_description_normalised(self):
        """Property description normalised from dict to text for before/after."""
        b = self._baseline()
        b["classes"][0]["properties"][1]["description"] = {"en": "old", "@none": "old"}
        c = self._current_from(b)
        c["classes"][0]["properties"][1]["description"] = {"en": "new", "@none": "new"}
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "property" and ch.op == "modified"]
        assert len(modified) == 1
        f_entry = next(f for f in modified[0].fields if f["field"] == "description")
        assert f_entry["before"] == "old"
        assert f_entry["after"] == "new"

    def test_property_only_changed_fields_emitted(self):
        b = self._baseline()
        c = self._current_from(b)
        age_prop = next(p for p in c["classes"][0]["properties"] if p["name"] == "age")
        age_prop["isArray"] = True  # only this changes
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "property" and ch.op == "modified"]
        assert len(modified) == 1
        field_names = [f["field"] for f in modified[0].fields]
        assert field_names == ["isArray"]

    def test_property_isFilterable_change_emitted(self):
        b = self._baseline()
        c = self._current_from(b)
        age_prop = next(p for p in c["classes"][0]["properties"] if p["name"] == "age")
        age_prop["isFilterable"] = True
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "property" and ch.op == "modified"]
        assert len(modified) == 1
        f_entry = next(f for f in modified[0].fields if f["field"] == "isFilterable")
        assert f_entry["before"] is None
        assert f_entry["after"] is True

    def test_property_validationRules_change_emitted(self):
        b = self._baseline()
        c = self._current_from(b)
        age_prop = next(p for p in c["classes"][0]["properties"] if p["name"] == "age")
        age_prop["validationRules"] = [{"type": "enumeration", "value": ["low", "high"]}]
        changes = _diff(b, c)
        modified = [ch for ch in changes if ch.kind == "property" and ch.op == "modified"]
        assert len(modified) == 1
        f_entry = next(f for f in modified[0].fields if f["field"] == "validationRules")
        assert f_entry["before"] is None
        assert f_entry["after"] == [{"type": "enumeration", "value": ["low", "high"]}]

    # ------------------------------------------------------------------
    # Date fields: NEVER appear in any Change
    # ------------------------------------------------------------------

    def test_date_fields_never_appear_in_any_change_via_schema_mutations(self):
        """Exercise a Schema through several mutations and confirm no date field
        leaks into any Change emitted by _diff."""
        s = self._schema_from_base()
        # Trigger various mutations that will update lastModifiedDate
        s.assign_class_description("Animal", "A living organism")
        s.create_class("Plant")
        s.create_property("Animal", "weight", DATATYPE.INTEGER)
        s.update_property("Animal", "age", is_optional=False)
        s.delete_class("Plant")
        changes = _diff(s._baseline, s._schema)
        self._assert_no_date_fields(changes)

    def test_only_date_field_mutation_produces_no_changes(self):
        """If the only delta is date fields, the result must be empty."""
        b = self._baseline()
        c = self._current_from(b)
        c["createdDate"] = "2099-06-01T00:00:00Z"
        c["lastModifiedDate"] = "2099-06-01T00:00:00Z"
        changes = _diff(b, c)
        assert changes == []

    # ------------------------------------------------------------------
    # Net-effect: create-then-delete and modify-then-revert
    # ------------------------------------------------------------------

    def test_create_then_delete_class_yields_no_entry(self):
        """A class created and then deleted must not appear in the diff at all."""
        s = self._schema_from_base()
        s.create_class("Transient")
        s.delete_class("Transient")
        changes = _diff(s._baseline, s._schema)
        class_changes = [ch for ch in changes if ch.kind == "class"]
        targets = [ch.target for ch in class_changes]
        assert "Transient" not in targets

    def test_modify_then_revert_yields_no_entry(self):
        """A field modified then restored to its original value must not appear."""
        s = self._schema_from_base()
        original_desc = s.find_class("Animal").get("description")
        s.assign_class_description("Animal", "Temporary description")
        # revert
        if original_desc is None:
            s.assign_class_description("Animal", "")
        else:
            # Restore original (it was None / absent so removal sets no description)
            s._schema["classes"][0].pop("description", None)
        changes = _diff(s._baseline, s._schema)
        class_modified = [ch for ch in changes if ch.kind == "class" and ch.op == "modified" and ch.target == "Animal"]
        # If there are class modifications, description must not be among them
        for cm in class_modified:
            desc_fields = [f for f in (cm.fields or []) if f["field"] == "description"]
            assert desc_fields == [], f"description field leaked after revert: {desc_fields}"

    def test_create_then_delete_property_yields_no_entry(self):
        """A property created then deleted on a class must not appear in the diff."""
        s = self._schema_from_base()
        s.create_property("Animal", "transient_prop", DATATYPE.TEXT)
        s.delete_property("Animal", "transient_prop")
        changes = _diff(s._baseline, s._schema)
        prop_changes = [ch for ch in changes if ch.kind == "property"]
        targets = [ch.target for ch in prop_changes]
        assert "Animal.transient_prop" not in targets

    # ------------------------------------------------------------------
    # Multiple classes
    # ------------------------------------------------------------------

    def test_multiple_class_changes_all_emitted(self):
        b = self._baseline()
        c = self._current_from(b)
        # Add Plant, remove Animal
        c["classes"] = [cls for cls in c["classes"] if cls["name"] != "Animal"]
        c["classes"].append({
            "type": "Class",
            "name": "Plant",
            "labelProperty": "label",
            "identifierProperty": "id",
            "isAbstract": False,
            "properties": [],
        })
        changes = _diff(b, c)
        class_changes = [ch for ch in changes if ch.kind == "class"]
        ops = {ch.target: ch.op for ch in class_changes}
        assert ops.get("Animal") == "removed"
        assert ops.get("Plant") == "added"

    # ------------------------------------------------------------------
    # Reorder candidate detection
    # ------------------------------------------------------------------

    def test_property_reorder_candidate_detected(self):
        """When property SET is unchanged but SEQUENCE differs, a reorder
        candidate Change is emitted with detail['reorder_candidate'] == True."""
        b = self._baseline()
        c = self._current_from(b)
        # Swap label and age
        c["classes"][0]["properties"] = list(reversed(c["classes"][0]["properties"]))
        changes = _diff(b, c)
        reorder = [
            ch for ch in changes
            if ch.detail and ch.detail.get("reorder_candidate") is True
        ]
        assert len(reorder) == 1
        assert reorder[0].target == "Animal.__order__"
        assert reorder[0].kind == "property"
        assert reorder[0].detail["before_order"] == ["label", "age"]
        assert reorder[0].detail["after_order"] == ["age", "label"]

    def test_property_reorder_candidate_not_emitted_when_set_also_changed(self):
        """Adding a property changes both the set and the sequence — no reorder
        candidate should be emitted since the set itself changed."""
        b = self._baseline()
        c = self._current_from(b)
        # Add a new property — set changed, so reorder candidate must NOT fire
        c["classes"][0]["properties"].append({
            "type": "DatatypeProperty",
            "name": "weight",
            "range": "decimal",
            "isOptional": True,
            "isArray": False,
            "isLangString": False,
            "isLabelSynonym": False,
        })
        changes = _diff(b, c)
        reorder = [
            ch for ch in changes
            if ch.detail and ch.detail.get("reorder_candidate") is True
        ]
        assert reorder == []

    def test_property_reorder_not_emitted_when_sequence_unchanged(self):
        """Same set, same order — no reorder candidate."""
        b = self._baseline()
        c = self._current_from(b)
        # No reorder
        changes = _diff(b, c)
        reorder = [
            ch for ch in changes
            if ch.detail and ch.detail.get("reorder_candidate") is True
        ]
        assert reorder == []

    # ------------------------------------------------------------------
    # via Schema API — real mutations, not hand-built dicts
    # ------------------------------------------------------------------

    def test_diff_via_schema_api_class_added(self):
        s = self._schema_from_base()
        s.create_class("Fungus")
        changes = _diff(s._baseline, s._schema)
        added = [ch for ch in changes if ch.kind == "class" and ch.op == "added"]
        assert any(ch.target == "Fungus" for ch in added)

    def test_diff_via_schema_api_property_added(self):
        s = self._schema_from_base()
        s.create_property("Animal", "speed", DATATYPE.DECIMAL)
        changes = _diff(s._baseline, s._schema)
        added = [ch for ch in changes if ch.kind == "property" and ch.op == "added"]
        assert any(ch.target == "Animal.speed" for ch in added)

    def test_diff_via_schema_api_property_modified(self):
        s = self._schema_from_base()
        s.update_property("Animal", "age", is_optional=False)
        changes = _diff(s._baseline, s._schema)
        modified = [ch for ch in changes if ch.kind == "property" and ch.op == "modified"]
        assert any(ch.target == "Animal.age" for ch in modified)
        animal_age = next(ch for ch in modified if ch.target == "Animal.age")
        f_entry = next(f for f in animal_age.fields if f["field"] == "isOptional")
        assert f_entry["before"] is True
        assert f_entry["after"] is False

    def test_diff_via_schema_api_class_field_modified(self):
        s = self._schema_from_base()
        s.assign_class_description("Animal", "A living creature")
        changes = _diff(s._baseline, s._schema)
        modified = [ch for ch in changes if ch.kind == "class" and ch.op == "modified"]
        assert any(ch.target == "Animal" for ch in modified)
        animal_mod = next(ch for ch in modified if ch.target == "Animal")
        f_entry = next(f for f in animal_mod.fields if f["field"] == "description")
        assert f_entry["before"] == ""
        assert f_entry["after"] == "A living creature"

    def test_diff_via_schema_api_reorder_via_assign_property_orders(self):
        """assign_property_orders reorders without changing the set."""
        s = self._schema_from_base()
        s.assign_property_orders({"Animal": ["age", "label"]})
        changes = _diff(s._baseline, s._schema)
        reorder = [
            ch for ch in changes
            if ch.detail and ch.detail.get("reorder_candidate") is True
        ]
        assert len(reorder) == 1
        assert reorder[0].target == "Animal.__order__"

    def test_rename_map_none_accepted_no_error(self):
        """_diff accepts rename_map=None (Phase 4 seam) without error."""
        b = self._baseline()
        c = self._current_from(b)
        c["classes"][0]["isAbstract"] = True
        changes = _diff(b, c, rename_map=None)
        assert any(ch.kind == "class" and ch.op == "modified" for ch in changes)


# ---------------------------------------------------------------------------
# Phase 4 — Rename reconciliation (identity through renames)
# ---------------------------------------------------------------------------

class TestChangeRenames:
    """Tests for _replay_identities() and identity-aware _diff() (ADR 0003).

    The heaviest-coverage area of the feature.  Verifies:
    - A folded RenameMap composes chains and drops round-trips.
    - Class renames (update_class new_name) and property renames
      (rename_property) match BY IDENTITY, emitting op='renamed' instead of
      remove+add.
    - A rename combined with a field modification is ONE record (op='renamed'
      with a fields entry).
    - Property renames stay scoped to the class's CANONICAL/baseline identity
      even when the class itself is renamed.
    - Round-trips (A->B->A) produce no rename entry.
    - An untracked to_dict() rename has no op-log event and correctly degrades
      to remove+add.
    - A class created after baseline and then renamed is just 'added' under its
      final name with no spurious rename.

    All Schema fixtures are deep-copied per-test; folding consumes the live
    op-log (s._change_log) and the diff runs against (s._baseline, s._schema).
    """

    _BASE: dict = {
        "name": "Test Model v1.0",
        "createdDate": "2024-01-01T00:00:00Z",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Drug",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "dosage",
                        "range": "text",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                ],
            }
        ],
    }

    def _schema(self) -> DatagraphsSchema:
        """Fresh Schema with class 'Drug' and properties 'label','dosage'."""
        return DatagraphsSchema.create_from(copy.deepcopy(TestChangeRenames._BASE))

    def _reconcile(self, s: DatagraphsSchema) -> list[Change]:
        """Replay identities over the baseline and run the identity-aware diff."""
        rename_map = _replay_identities(s._baseline, s._change_log)
        return _diff(s._baseline, s._schema, rename_map=rename_map)

    # ------------------------------------------------------------------
    # _replay_identities — unit-level correspondence (baseline<->current name)
    # ------------------------------------------------------------------

    def test_fold_empty_log_yields_empty_maps(self):
        rm = _replay_identities({"classes": []}, [])
        assert rm.classes == {}
        assert rm.properties == {}

    def test_fold_property_rename_yields_baseline_keyed_entry(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        rm = _replay_identities(s._baseline, s._change_log)
        assert rm.classes == {}
        assert rm.properties == {("Drug", "dosage"): "dose"}

    def test_fold_class_rename_yields_baseline_keyed_entry(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        rm = _replay_identities(s._baseline, s._change_log)
        assert rm.classes == {"Drug": "Medication"}

    def test_fold_class_rename_chain_composes(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        s.update_class("Medication", new_name="Remedy")
        rm = _replay_identities(s._baseline, s._change_log)
        assert rm.classes == {"Drug": "Remedy"}

    def test_fold_property_rename_chain_composes(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        s.rename_property("Drug", "dose", "amount")
        rm = _replay_identities(s._baseline, s._change_log)
        assert rm.properties == {("Drug", "dosage"): "amount"}

    def test_fold_class_round_trip_dropped(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        s.update_class("Medication", new_name="Drug")
        rm = _replay_identities(s._baseline, s._change_log)
        assert "Drug" not in rm.classes
        assert rm.classes == {}

    def test_fold_property_round_trip_dropped(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        s.rename_property("Drug", "dose", "dosage")
        rm = _replay_identities(s._baseline, s._change_log)
        assert rm.properties == {}

    def test_fold_property_rename_scoped_to_canonical_class_after_class_rename(self):
        """rename_property records the post-rename class name; the replay must
        resolve it back to the class's baseline identity."""
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        # rename_property's recorded class_name is now 'Medication'
        s.rename_property("Medication", "dosage", "dose")
        rm = _replay_identities(s._baseline, s._change_log)
        assert rm.classes == {"Drug": "Medication"}
        # Property key is scoped to the CANONICAL/baseline class 'Drug'
        assert rm.properties == {("Drug", "dosage"): "dose"}

    def test_fold_property_rename_then_class_rename_still_scoped_to_baseline(self):
        """Order reversed: property renamed first, then the class — the property
        key must still be keyed by the baseline class identity."""
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        s.update_class("Drug", new_name="Medication")
        rm = _replay_identities(s._baseline, s._change_log)
        assert rm.classes == {"Drug": "Medication"}
        assert rm.properties == {("Drug", "dosage"): "dose"}

    # ------------------------------------------------------------------
    # Property rename via the diff
    # ------------------------------------------------------------------

    def test_property_rename_emits_single_renamed_record(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        changes = self._reconcile(s)
        renamed = [ch for ch in changes if ch.kind == "property" and ch.op == "renamed"]
        assert len(renamed) == 1
        assert renamed[0].from_ == "dosage"
        assert renamed[0].to == "dose"
        assert renamed[0].target == "Drug.dose"
        # No remove+add masquerading as the rename
        assert not [ch for ch in changes if ch.op in ("removed", "added")]

    def test_property_rename_has_no_fields_when_only_renamed(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        changes = self._reconcile(s)
        renamed = next(ch for ch in changes if ch.op == "renamed")
        assert renamed.fields is None

    def test_property_rename_plus_modify_is_one_record_with_fields(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        s.update_property("Drug", "dose", is_optional=False)
        changes = self._reconcile(s)
        renamed = [ch for ch in changes if ch.kind == "property" and ch.op == "renamed"]
        assert len(renamed) == 1
        rec = renamed[0]
        assert rec.from_ == "dosage"
        assert rec.to == "dose"
        assert rec.fields is not None
        f_entry = next(f for f in rec.fields if f["field"] == "isOptional")
        assert f_entry["before"] is True
        assert f_entry["after"] is False
        # Exactly one record for this property — no separate 'modified'
        assert len([ch for ch in changes if ch.target == "Drug.dose"]) == 1

    def test_property_rename_round_trip_no_entry(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        s.rename_property("Drug", "dose", "dosage")
        changes = self._reconcile(s)
        prop_changes = [ch for ch in changes if ch.kind == "property"]
        assert prop_changes == []

    def test_property_chained_rename_single_record(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        s.rename_property("Drug", "dose", "amount")
        changes = self._reconcile(s)
        renamed = [ch for ch in changes if ch.kind == "property" and ch.op == "renamed"]
        assert len(renamed) == 1
        assert renamed[0].from_ == "dosage"
        assert renamed[0].to == "amount"

    # ------------------------------------------------------------------
    # Class rename via the diff
    # ------------------------------------------------------------------

    def test_class_rename_emits_single_renamed_record(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        changes = self._reconcile(s)
        renamed = [ch for ch in changes if ch.kind == "class" and ch.op == "renamed"]
        assert len(renamed) == 1
        assert renamed[0].from_ == "Drug"
        assert renamed[0].to == "Medication"
        assert renamed[0].target == "Medication"
        # No class remove+add
        assert not [ch for ch in changes if ch.kind == "class" and ch.op in ("removed", "added")]

    def test_class_chained_rename_single_record(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        s.update_class("Medication", new_name="Remedy")
        changes = self._reconcile(s)
        renamed = [ch for ch in changes if ch.kind == "class" and ch.op == "renamed"]
        assert len(renamed) == 1
        assert renamed[0].from_ == "Drug"
        assert renamed[0].to == "Remedy"

    def test_class_rename_round_trip_no_entry(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        s.update_class("Medication", new_name="Drug")
        changes = self._reconcile(s)
        class_changes = [ch for ch in changes if ch.kind == "class"]
        assert class_changes == []

    def test_class_rename_plus_modify_is_one_record_with_fields(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        s.update_class("Medication", new_description="A medicinal substance")
        changes = self._reconcile(s)
        renamed = [ch for ch in changes if ch.kind == "class" and ch.op == "renamed"]
        assert len(renamed) == 1
        rec = renamed[0]
        assert rec.from_ == "Drug"
        assert rec.to == "Medication"
        assert rec.fields is not None
        f_entry = next(f for f in rec.fields if f["field"] == "description")
        assert f_entry["before"] == ""
        assert f_entry["after"] == "A medicinal substance"
        assert len([ch for ch in changes if ch.kind == "class" and ch.target == "Medication"]) == 1

    # ------------------------------------------------------------------
    # Canonical-identity scoping: class renamed AND property renamed
    # ------------------------------------------------------------------

    def test_class_and_property_rename_both_reported_and_scoped(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        s.rename_property("Medication", "dosage", "dose")
        changes = self._reconcile(s)

        class_renamed = [ch for ch in changes if ch.kind == "class" and ch.op == "renamed"]
        assert len(class_renamed) == 1
        assert class_renamed[0].from_ == "Drug"
        assert class_renamed[0].to == "Medication"

        prop_renamed = [ch for ch in changes if ch.kind == "property" and ch.op == "renamed"]
        assert len(prop_renamed) == 1
        # target uses the CURRENT class name
        assert prop_renamed[0].target == "Medication.dose"
        assert prop_renamed[0].from_ == "dosage"
        assert prop_renamed[0].to == "dose"

        # No spurious remove+add for either entity
        assert not [ch for ch in changes if ch.op in ("removed", "added")]

    def test_class_renamed_property_unchanged_only_class_renamed(self):
        s = self._schema()
        s.update_class("Drug", new_name="Medication")
        changes = self._reconcile(s)
        # The unrenamed 'dosage' property must not appear at all
        prop_changes = [ch for ch in changes if ch.kind == "property"]
        assert prop_changes == []

    # ------------------------------------------------------------------
    # Untracked to_dict() rename — no op-log event => remove+add
    # ------------------------------------------------------------------

    def test_untracked_property_rename_degrades_to_remove_add(self):
        s = self._schema()
        # Mutate the live dict directly — NO mutating method, so NO op-log event.
        drug = next(c for c in s.to_dict()["classes"] if c["name"] == "Drug")
        prop = next(p for p in drug["properties"] if p["name"] == "dosage")
        prop["name"] = "dose"
        changes = self._reconcile(s)
        # No rename label
        assert not [ch for ch in changes if ch.op == "renamed"]
        prop_ops = {(ch.target, ch.op) for ch in changes if ch.kind == "property"}
        assert ("Drug.dosage", "removed") in prop_ops
        assert ("Drug.dose", "added") in prop_ops

    def test_untracked_class_rename_degrades_to_remove_add(self):
        s = self._schema()
        drug = next(c for c in s.to_dict()["classes"] if c["name"] == "Drug")
        drug["name"] = "Medication"
        changes = self._reconcile(s)
        assert not [ch for ch in changes if ch.op == "renamed"]
        class_ops = {(ch.target, ch.op) for ch in changes if ch.kind == "class"}
        assert ("Drug", "removed") in class_ops
        assert ("Medication", "added") in class_ops

    # ------------------------------------------------------------------
    # Class created after baseline then renamed — no spurious rename
    # ------------------------------------------------------------------

    def test_rename_of_class_created_after_baseline_is_just_added(self):
        s = self._schema()
        s.create_class("Vaccine")
        s.update_class("Vaccine", new_name="Immunisation")
        changes = self._reconcile(s)
        # The created-then-renamed class appears only as 'added' under final name
        added = [ch for ch in changes if ch.kind == "class" and ch.op == "added"]
        assert len(added) == 1
        assert added[0].target == "Immunisation"
        assert not [ch for ch in changes if ch.kind == "class" and ch.op == "renamed"]
        assert not [ch for ch in changes if ch.kind == "class" and ch.op == "removed"]

    def test_property_created_after_baseline_then_renamed_is_just_added(self):
        s = self._schema()
        s.create_property("Drug", "strength", DATATYPE.INTEGER)
        s.rename_property("Drug", "strength", "potency")
        changes = self._reconcile(s)
        prop_renamed = [ch for ch in changes if ch.kind == "property" and ch.op == "renamed"]
        assert prop_renamed == []
        added = [ch for ch in changes if ch.kind == "property" and ch.op == "added"]
        assert len(added) == 1
        assert added[0].target == "Drug.potency"

    # ------------------------------------------------------------------
    # rename_map=None still works (pure name-keyed remove+add)
    # ------------------------------------------------------------------

    def test_diff_without_rename_map_treats_rename_as_remove_add(self):
        s = self._schema()
        s.rename_property("Drug", "dosage", "dose")
        changes = _diff(s._baseline, s._schema, rename_map=None)
        prop_ops = {(ch.target, ch.op) for ch in changes if ch.kind == "property"}
        assert ("Drug.dosage", "removed") in prop_ops
        assert ("Drug.dose", "added") in prop_ops
        assert not [ch for ch in changes if ch.op == "renamed"]


# ---------------------------------------------------------------------------
# Phase 5 — Semantic annotation (compound, reorder, label-property)
# ---------------------------------------------------------------------------

class TestChangeSemanticAnnotation:
    """Tests for _annotate() — op-log-driven semantic upgrade of the diff.

    The annotation layer runs AFTER _diff + rename folding and only RELABELS or
    COLLAPSES Changes the diff already produced; it never invents or contradicts
    structural truth (ADR 0001), keys off the single boundary op-log entry
    (ADR 0002), and resolves op-log call-time names via the rename map
    (ADR 0003).  Each test pins the exact final shape of the annotated Change(s)
    so Phase 6's render has a stable contract.

    All Schema fixtures are deep-copied per-test.
    """

    _BASE: dict = {
        "name": "Test Model v1.0",
        "createdDate": "2024-01-01T00:00:00Z",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Animal",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "age",
                        "range": "integer",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                ],
            }
        ],
    }

    def _schema(self) -> DatagraphsSchema:
        """Fresh Schema with class 'Animal' and properties 'label','age'."""
        return DatagraphsSchema.create_from(copy.deepcopy(TestChangeSemanticAnnotation._BASE))

    def _rebaseline(self, s: DatagraphsSchema) -> None:
        """Snapshot the current live schema as the new baseline and clear the log.

        Used when set-up mutations (e.g. creating a subclass) should be part of
        the *starting* state rather than the change under test.
        """
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

    def _annotated(self, s: DatagraphsSchema) -> list[Change]:
        """diff -> fold -> annotate, exactly as Phase 6 will compose it."""
        rename_map = _replay_identities(s._baseline, s._change_log)
        changes = _diff(s._baseline, s._schema, rename_map=rename_map)
        return _annotate(changes, s._change_log, s._baseline, s._schema, rename_map=rename_map)

    # ------------------------------------------------------------------
    # create_subclass -> one subclass_created record (not class + N props)
    # ------------------------------------------------------------------

    def test_create_subclass_collapses_to_single_subclass_created(self):
        s = self._schema()
        s.create_subclass("Dog", "A dog", "Animal")
        changes = self._annotated(s)

        subclass_created = [ch for ch in changes if ch.op == "subclass_created"]
        assert len(subclass_created) == 1
        rec = subclass_created[0]
        assert rec.target == "Dog"
        assert rec.kind == "class"
        # The subclass inherits the parent's full property set (label + age).
        assert rec.detail == {"parent": "Animal", "inherited": 2}

    def test_create_subclass_suppresses_per_property_adds(self):
        """No stray class 'added' or per-property 'added' Changes for the
        subclass — exactly one semantic record represents the whole creation."""
        s = self._schema()
        s.create_subclass("Dog", "A dog", "Animal")
        changes = self._annotated(s)

        # No raw class-add for Dog.
        assert not [ch for ch in changes if ch.kind == "class" and ch.op == "added"]
        # No per-property adds scoped to Dog.
        dog_prop_adds = [
            ch for ch in changes
            if ch.kind == "property" and ch.op == "added" and ch.target.startswith("Dog.")
        ]
        assert dog_prop_adds == []
        # And the single subclass_created record is the only Change about Dog.
        dog_changes = [ch for ch in changes if ch.target == "Dog" or ch.target.startswith("Dog.")]
        assert len(dog_changes) == 1
        assert dog_changes[0].op == "subclass_created"

    # ------------------------------------------------------------------
    # apply_to_subclasses -> parent op references the subclass list
    # ------------------------------------------------------------------

    def test_create_property_apply_to_subclasses_references_subclasses(self):
        s = self._schema()
        s.create_class("Dog", parent_class_name="Animal")
        self._rebaseline(s)  # subclass exists at baseline; only the prop add is the change
        s.create_property("Animal", "weight", DATATYPE.INTEGER, apply_to_subclasses=True)
        changes = self._annotated(s)

        # Parent property add carries the subclass reference detail.
        parent = [ch for ch in changes if ch.target == "Animal.weight"]
        assert len(parent) == 1
        assert parent[0].op == "added"
        assert parent[0].detail == {"applied_to_subclasses": ["Dog"]}

        # Subclass property add remains as an ordinary structural record.
        sub = [ch for ch in changes if ch.target == "Dog.weight"]
        assert len(sub) == 1
        assert sub[0].op == "added"
        assert sub[0].detail is None

    def test_update_property_apply_to_subclasses_references_subclasses(self):
        s = self._schema()
        s.create_class("Dog", parent_class_name="Animal")
        s.create_property("Dog", "age", DATATYPE.INTEGER)
        self._rebaseline(s)
        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)
        changes = self._annotated(s)

        parent = [ch for ch in changes if ch.target == "Animal.age"]
        assert len(parent) == 1
        assert parent[0].op == "modified"
        assert parent[0].detail == {"applied_to_subclasses": ["Dog"]}
        # The structural field flip is preserved on the parent record.
        f = next(f for f in parent[0].fields if f["field"] == "isOptional")
        assert f["before"] is True and f["after"] is False

        sub = [ch for ch in changes if ch.target == "Dog.age"]
        assert len(sub) == 1
        assert sub[0].op == "modified"
        assert sub[0].detail is None

    def test_create_property_without_subclasses_has_no_reference_detail(self):
        """No subclasses -> no applied_to_subclasses annotation; just a plain add."""
        s = self._schema()
        s.create_property("Animal", "weight", DATATYPE.INTEGER, apply_to_subclasses=True)
        changes = self._annotated(s)
        parent = [ch for ch in changes if ch.target == "Animal.weight"]
        assert len(parent) == 1
        assert parent[0].detail is None

    # ------------------------------------------------------------------
    # assign_label_property -> coherent label-property annotation
    # ------------------------------------------------------------------

    def test_assign_label_property_fuses_into_single_coherent_record(self):
        s = self._schema()
        s.assign_label_property("Animal", "age")
        changes = self._annotated(s)

        # The property record carries the label_property designation.
        prop = [ch for ch in changes if ch.target == "Animal.age"]
        assert len(prop) == 1
        assert prop[0].op == "modified"
        assert prop[0].detail == {"label_property": "age"}
        # The structural truth (isOptional flip) is preserved on that record.
        f = next(f for f in prop[0].fields if f["field"] == "isOptional")
        assert f["before"] is True and f["after"] is False

    def test_assign_label_property_not_two_bare_field_flips(self):
        """Assert it is NOT rendered as a bare class labelProperty flip plus an
        unrelated property flip — the redundant class field is fused away."""
        s = self._schema()
        s.assign_label_property("Animal", "age")
        changes = self._annotated(s)

        # No standalone class Change carrying a bare labelProperty field flip.
        class_label_flips = [
            ch for ch in changes
            if ch.kind == "class"
            and ch.fields
            and any(f["field"] == "labelProperty" for f in ch.fields)
        ]
        assert class_label_flips == []
        # The only annotated record about this assignment is the property one.
        labelled = [
            ch for ch in changes if ch.detail and ch.detail.get("label_property") == "age"
        ]
        assert len(labelled) == 1

    def test_assign_label_property_keeps_other_class_field_changes(self):
        """If the class has an unrelated field change too, only the redundant
        labelProperty flip is fused away; the rest of the class Change stays."""
        s = self._schema()
        s.assign_label_property("Animal", "age")
        s.assign_class_description("Animal", "A living organism")
        changes = self._annotated(s)

        class_mods = [ch for ch in changes if ch.kind == "class" and ch.op == "modified" and ch.target == "Animal"]
        assert len(class_mods) == 1
        field_names = {f["field"] for f in class_mods[0].fields}
        assert "labelProperty" not in field_names
        assert "description" in field_names

    # ------------------------------------------------------------------
    # assign_property_orders -> single 'reordered' record
    # ------------------------------------------------------------------

    def test_assign_property_orders_emits_single_reordered_record(self):
        s = self._schema()
        s.assign_property_orders({"Animal": ["age", "label"]})
        changes = self._annotated(s)

        reordered = [ch for ch in changes if ch.op == "reordered"]
        assert len(reordered) == 1
        rec = reordered[0]
        assert rec.target == "Animal"
        assert rec.kind == "class"
        assert rec.detail == {"order": ["age", "label"]}
        # The raw reorder candidate must be gone.
        assert not [
            ch for ch in changes
            if ch.detail and ch.detail.get("reorder_candidate") is True
        ]

    def test_untracked_reorder_without_oplog_stays_raw_candidate(self):
        """A reorder with NO op-log entry (untracked to_dict() edit) must NOT be
        upgraded to a 'reordered' record — it degrades to the raw candidate
        (ADR 0001 graceful degradation).  Documented behaviour."""
        s = self._schema()
        # Reverse property order directly on the live dict — no mutating method,
        # so no assign_property_orders op-log entry.
        s._schema["classes"][0]["properties"] = list(
            reversed(s._schema["classes"][0]["properties"])
        )
        changes = self._annotated(s)

        # No finalised reorder.
        assert not [ch for ch in changes if ch.op == "reordered"]
        # The raw candidate survives untouched.
        candidates = [
            ch for ch in changes
            if ch.detail and ch.detail.get("reorder_candidate") is True
        ]
        assert len(candidates) == 1
        assert candidates[0].target == "Animal.__order__"

    # ------------------------------------------------------------------
    # delete_class(cascade_to_subclasses=True) -> pinned cascade rendering
    # ------------------------------------------------------------------

    def test_delete_class_cascade_renders_subclass_link_as_modification(self):
        """Pin the documented rendering: deleting a parent with cascade leaves
        the parent as a 'removed' class and the former subclass as a 'modified'
        class whose subClassOf field flips parent -> None.  Left as structural
        truth (not collapsed) per the task."""
        s = self._schema()
        s.create_class("Dog", parent_class_name="Animal")
        self._rebaseline(s)
        s.delete_class("Animal", cascade_to_subclasses=True)
        changes = self._annotated(s)

        removed = [ch for ch in changes if ch.kind == "class" and ch.op == "removed"]
        assert len(removed) == 1
        assert removed[0].target == "Animal"

        cascade = [ch for ch in changes if ch.kind == "class" and ch.op == "modified" and ch.target == "Dog"]
        assert len(cascade) == 1
        f = next(f for f in cascade[0].fields if f["field"] == "subClassOf")
        assert f["before"] == "Animal"
        assert f["after"] is None

    # ------------------------------------------------------------------
    # No-op / degradation: empty log leaves the diff untouched
    # ------------------------------------------------------------------

    def test_empty_change_log_leaves_changes_unchanged(self):
        s = self._schema()
        # Untracked structural edit only.
        s._schema["classes"][0]["isAbstract"] = True
        rename_map = _replay_identities(s._baseline, s._change_log)
        diffed = _diff(s._baseline, s._schema, rename_map=rename_map)
        annotated = _annotate(diffed, s._change_log, s._baseline, s._schema, rename_map=rename_map)
        assert annotated == diffed

    def test_annotation_preserves_diff_order(self):
        """Annotation edits in place and never reshuffles surviving Changes."""
        s = self._schema()
        # A class-field modification (emitted first) plus a pure reorder of the
        # existing property set (emitted after, finalised to 'reordered').
        s.assign_class_description("Animal", "A living organism")
        s.assign_property_orders({"Animal": ["age", "label"]})
        rename_map = _replay_identities(s._baseline, s._change_log)
        diffed = _diff(s._baseline, s._schema, rename_map=rename_map)
        annotated = _annotate(diffed, s._change_log, s._baseline, s._schema, rename_map=rename_map)

        # The class 'modified' (description) precedes the 'reordered' record,
        # mirroring the diff order; both share target 'Animal'.
        ops_in_order = [(ch.target, ch.op) for ch in annotated if ch.target == "Animal"]
        assert ops_in_order == [("Animal", "modified"), ("Animal", "reordered")]


# ---------------------------------------------------------------------------
# Phase 6 — Renderers + change_report() + determinism
# ---------------------------------------------------------------------------

class TestChangeReportRendering:
    """Phase 6 — change_report() public method, renderers, and determinism.

    Verifies:
    - change_report() returns str; change_report(fmt='records') returns list[dict].
    - change_report('bogus') raises ValueError.
    - The method is strictly read-only: _schema, _baseline, _change_log are never
      mutated, and two consecutive calls return identical output.
    - Determinism: identical mutation sequences produce byte-identical text and
      equal records regardless of dict insertion order.
    - Brief happy-path: Substance with dosage+deprecatedCode; create Drug subclass,
      rename dosage->dose, update dose is_optional=False, delete deprecatedCode.
      Text shows Drug as new subclass, combined rename+modify on Substance.dose in
      ONE line, deprecatedCode removed, and NO date churn anywhere.
    - Records shape: fixed keys target/kind/op present; from/to/fields/detail only
      when applicable; absent keys omitted (not None).
    """

    # ------------------------------------------------------------------
    # Fixture constants — never mutate directly; use factory methods below.
    # ------------------------------------------------------------------

    #: Minimal schema with Substance having dosage + deprecatedCode.
    _SUBSTANCE_BASE: dict = {
        "name": "My Model v1.0",
        "createdDate": "2024-01-01T00:00:00Z",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Substance",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "dosage",
                        "range": "text",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "deprecatedCode",
                        "range": "text",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                ],
            }
        ],
    }

    #: Minimal schema with Animal having label + age.
    _ANIMAL_BASE: dict = {
        "name": "Test Model v1.0",
        "createdDate": "2024-01-01T00:00:00Z",
        "lastModifiedDate": "2024-01-01T00:00:00Z",
        "classes": [
            {
                "type": "Class",
                "name": "Animal",
                "labelProperty": "label",
                "identifierProperty": "id",
                "isAbstract": False,
                "properties": [
                    {
                        "type": "DatatypeProperty",
                        "name": "label",
                        "range": "text",
                        "isOptional": False,
                        "isArray": False,
                        "isLangString": True,
                        "isLabelSynonym": False,
                    },
                    {
                        "type": "DatatypeProperty",
                        "name": "age",
                        "range": "integer",
                        "isOptional": True,
                        "isArray": False,
                        "isLangString": False,
                        "isLabelSynonym": False,
                    },
                ],
            }
        ],
    }

    def _substance_schema(self) -> DatagraphsSchema:
        """Fresh Schema loaded from _SUBSTANCE_BASE; change_log starts empty."""
        return DatagraphsSchema.create_from(copy.deepcopy(self._SUBSTANCE_BASE))

    def _animal_schema(self) -> DatagraphsSchema:
        """Fresh Schema loaded from _ANIMAL_BASE; change_log starts empty."""
        return DatagraphsSchema.create_from(copy.deepcopy(self._ANIMAL_BASE))

    def _happy_path_schema(self) -> DatagraphsSchema:
        """Execute the brief's happy-path mutation sequence on a fresh Substance schema.

        Sequence: create_subclass Drug (of Substance), rename dosage->dose,
        update_property dose is_optional=False, delete_property deprecatedCode.
        """
        s = self._substance_schema()
        s.create_subclass("Drug", "A pharmaceutical drug", "Substance")
        s.rename_property("Substance", "dosage", "dose")
        s.update_property("Substance", "dose", is_optional=False)
        s.delete_property("Substance", "deprecatedCode")
        return s

    # ------------------------------------------------------------------
    # Return type contracts
    # ------------------------------------------------------------------

    def test_default_format_returns_str(self):
        s = self._substance_schema()
        s.create_class("Extra")
        result = s.change_report()
        assert isinstance(result, str)

    def test_text_format_returns_str(self):
        s = self._substance_schema()
        s.create_class("Extra")
        result = s.change_report(fmt="text")
        assert isinstance(result, str)

    def test_records_format_returns_list(self):
        s = self._substance_schema()
        s.create_class("Extra")
        result = s.change_report(fmt="records")
        assert isinstance(result, list)

    def test_records_format_list_contains_dicts(self):
        s = self._substance_schema()
        s.create_class("Extra")
        result = s.change_report(fmt="records")
        assert all(isinstance(r, dict) for r in result)

    # ------------------------------------------------------------------
    # ValueError for unknown format
    # ------------------------------------------------------------------

    def test_invalid_format_raises_value_error(self):
        s = self._substance_schema()
        with pytest.raises(ValueError):
            s.change_report("bogus")

    def test_invalid_format_markdown_raises_value_error(self):
        s = self._substance_schema()
        with pytest.raises(ValueError):
            s.change_report("markdown")

    def test_invalid_format_empty_string_raises_value_error(self):
        s = self._substance_schema()
        with pytest.raises(ValueError):
            s.change_report("")

    # ------------------------------------------------------------------
    # Read-only: no mutation of _schema, _baseline, _change_log
    # ------------------------------------------------------------------

    def test_change_report_does_not_mutate_change_log(self):
        """Two consecutive calls must not append to or sort _change_log."""
        s = self._happy_path_schema()
        log_snapshot = copy.deepcopy(s._change_log)
        s.change_report()
        s.change_report()
        assert s._change_log == log_snapshot

    def test_change_report_does_not_mutate_schema(self):
        """change_report must not alter the live _schema dict."""
        s = self._happy_path_schema()
        schema_snapshot = copy.deepcopy(s._schema)
        s.change_report()
        s.change_report()
        assert s._schema == schema_snapshot

    def test_change_report_does_not_mutate_baseline(self):
        """change_report must not alter the _baseline dict."""
        s = self._happy_path_schema()
        baseline_snapshot = copy.deepcopy(s._baseline)
        s.change_report()
        s.change_report()
        assert s._baseline == baseline_snapshot

    def test_two_consecutive_calls_return_identical_text(self):
        """Calling change_report() twice returns identical str."""
        s = self._happy_path_schema()
        first = s.change_report()
        second = s.change_report()
        assert first == second

    def test_two_consecutive_calls_return_equal_records(self):
        """Calling change_report(fmt='records') twice returns equal lists."""
        s = self._happy_path_schema()
        first = s.change_report(fmt="records")
        second = s.change_report(fmt="records")
        assert first == second

    # ------------------------------------------------------------------
    # Determinism: identical mutation sequences => byte-identical output
    # ------------------------------------------------------------------

    def test_determinism_identical_mutation_sequences_produce_identical_text(self):
        """Two schemas built via the same mutation sequence produce byte-identical text."""
        def build():
            s = self._animal_schema()
            s.create_class("Cat")
            s.rename_property("Animal", "age", "years")
            s.delete_class("Cat")
            s.create_class("Dog")
            return s

        s1 = build()
        s2 = build()
        assert s1.change_report() == s2.change_report()

    def test_determinism_identical_mutation_sequences_produce_equal_records(self):
        """Two schemas built via the same mutation sequence produce equal records."""
        def build():
            s = self._animal_schema()
            s.create_class("Plant")
            s.update_property("Animal", "age", is_optional=False)
            return s

        s1 = build()
        s2 = build()
        assert s1.change_report(fmt="records") == s2.change_report(fmt="records")

    def test_determinism_ordering_stable_across_multiple_classes(self):
        """Multiple class changes are emitted alphabetically regardless of
        the order in which they were added."""
        # Build two schemas that add classes in different orders but with the
        # same net change.
        base = {
            "name": "My Model v1.0",
            "createdDate": "2024-01-01T00:00:00Z",
            "lastModifiedDate": "2024-01-01T00:00:00Z",
            "classes": [],
        }

        def build_ab():
            s = DatagraphsSchema.create_from(copy.deepcopy(base))
            s.create_class("Alpha")
            s.create_class("Beta")
            return s

        def build_ba():
            s = DatagraphsSchema.create_from(copy.deepcopy(base))
            s.create_class("Beta")
            s.create_class("Alpha")
            return s

        text_ab = build_ab().change_report()
        text_ba = build_ba().change_report()
        assert text_ab == text_ba

    # ------------------------------------------------------------------
    # Empty schema => empty output
    # ------------------------------------------------------------------

    def test_no_changes_text_is_empty_string(self):
        """A freshly loaded schema with no mutations returns an empty string."""
        s = self._substance_schema()
        assert s.change_report() == ""

    def test_no_changes_records_is_empty_list(self):
        """A freshly loaded schema with no mutations returns an empty list."""
        s = self._substance_schema()
        assert s.change_report(fmt="records") == []

    # ------------------------------------------------------------------
    # Brief happy-path scenario: end-to-end content assertions
    # ------------------------------------------------------------------

    def test_happy_path_text_contains_drug_as_new_subclass(self):
        """Drug must appear as a new subclass (not a plain new class)."""
        text = self._happy_path_schema().change_report()
        assert "Drug" in text
        assert "subclass of Substance" in text

    def test_happy_path_text_combined_rename_modify_on_dose_single_line(self):
        """dosage->dose rename and isOptional flip must be ONE combined line."""
        text = self._happy_path_schema().change_report()
        # The combined line contains the rename marker and the field flip.
        assert "dosage -> dose [renamed]" in text
        assert "isOptional: true -> false" in text
        # And there must be exactly one line mentioning 'dose' (combined, not split).
        dose_lines = [ln for ln in text.splitlines() if "dose" in ln and "deprecated" not in ln]
        # Only one logical line for the rename+modify (may also include the
        # Substance header, but the rename+isOptional data is on exactly one line).
        rename_lines = [ln for ln in text.splitlines() if "dosage -> dose" in ln]
        assert len(rename_lines) == 1

    def test_happy_path_text_deprecated_code_removed(self):
        """deprecatedCode must appear as [removed]."""
        text = self._happy_path_schema().change_report()
        assert "deprecatedCode" in text
        # Must carry the removed indicator.
        deprecated_lines = [ln for ln in text.splitlines() if "deprecatedCode" in ln]
        assert len(deprecated_lines) == 1
        assert "removed" in deprecated_lines[0]

    def test_happy_path_text_no_date_churn(self):
        """createdDate and lastModifiedDate must not appear anywhere in the text."""
        text = self._happy_path_schema().change_report()
        assert "createdDate" not in text
        assert "lastModifiedDate" not in text

    def test_happy_path_text_has_header_count(self):
        """Text output starts with a 'Schema changes (N):' header."""
        text = self._happy_path_schema().change_report()
        first_line = text.splitlines()[0]
        assert first_line.startswith("Schema changes (")
        assert first_line.endswith("):")

    def test_happy_path_text_exact_content(self):
        """Pin the exact text layout for the brief's happy-path scenario."""
        text = self._happy_path_schema().change_report()
        expected = (
            "Schema changes (2):\n"
            "+ Drug [new subclass of Substance] (+3 inherited)\n"
            "  subclass of: \"Substance\"\n"
            "  inherited count: 3\n"
            "~ Substance [modified]\n"
            "  ~ dose: dosage -> dose [renamed]; isOptional: true -> false\n"
            "  - deprecatedCode [removed]"
        )
        assert text == expected

    def test_happy_path_records_drug_entry(self):
        """Drug record must have op=subclass_created with parent+inherited detail."""
        records = self._happy_path_schema().change_report(fmt="records")
        drug = next((r for r in records if r.get("target") == "Drug"), None)
        assert drug is not None
        assert drug["kind"] == "class"
        assert drug["op"] == "subclass_created"
        assert drug["detail"]["parent"] == "Substance"
        assert drug["detail"]["inherited"] == 3

    def test_happy_path_records_dose_renamed_and_modified(self):
        """dose record must be op=renamed with from/to and fields for isOptional."""
        records = self._happy_path_schema().change_report(fmt="records")
        dose = next((r for r in records if r.get("target") == "Substance.dose"), None)
        assert dose is not None
        assert dose["kind"] == "property"
        assert dose["op"] == "renamed"
        assert dose["from"] == "dosage"
        assert dose["to"] == "dose"
        fields = dose["fields"]
        assert len(fields) == 1
        assert fields[0]["field"] == "isOptional"
        assert fields[0]["before"] is True
        assert fields[0]["after"] is False

    def test_happy_path_records_deprecated_code_removed(self):
        """deprecatedCode record must be op=removed with no extraneous keys."""
        records = self._happy_path_schema().change_report(fmt="records")
        dep = next(
            (r for r in records if r.get("target") == "Substance.deprecatedCode"), None
        )
        assert dep is not None
        assert dep["kind"] == "property"
        assert dep["op"] == "removed"
        # Absent keys must be omitted, not None.
        assert "from" not in dep
        assert "to" not in dep
        assert "fields" not in dep
        assert "detail" not in dep

    def test_happy_path_records_no_date_entries(self):
        """No record may reference createdDate or lastModifiedDate."""
        records = self._happy_path_schema().change_report(fmt="records")
        for rec in records:
            assert "createdDate" not in rec.get("target", "")
            assert "lastModifiedDate" not in rec.get("target", "")
            for f in rec.get("fields", []):
                assert f.get("field") not in ("createdDate", "lastModifiedDate")

    def test_happy_path_records_count(self):
        """Happy path must produce exactly 3 records: Drug, Substance.dose, Substance.deprecatedCode."""
        records = self._happy_path_schema().change_report(fmt="records")
        assert len(records) == 3

    # ------------------------------------------------------------------
    # Records shape: absent keys are omitted (not None)
    # ------------------------------------------------------------------

    def test_records_added_class_has_no_from_to_fields_detail(self):
        """A plain added class record must contain only target/kind/op."""
        s = self._animal_schema()
        s.create_class("NewClass")
        records = s.change_report(fmt="records")
        added = next((r for r in records if r.get("target") == "NewClass"), None)
        assert added is not None
        assert set(added.keys()) == {"target", "kind", "op"}

    def test_records_removed_property_has_no_from_to_fields_detail(self):
        """A removed property record must contain only target/kind/op."""
        s = self._animal_schema()
        s.delete_property("Animal", "age")
        records = s.change_report(fmt="records")
        removed = next(
            (r for r in records if r.get("target") == "Animal.age"), None
        )
        assert removed is not None
        assert set(removed.keys()) == {"target", "kind", "op"}

    def test_records_modified_property_has_fields_no_from_to(self):
        """A modified property record must have 'fields' but no 'from' or 'to'."""
        s = self._animal_schema()
        s.update_property("Animal", "age", is_optional=False)
        records = s.change_report(fmt="records")
        mod = next((r for r in records if r.get("target") == "Animal.age"), None)
        assert mod is not None
        assert mod["op"] == "modified"
        assert "fields" in mod
        assert "from" not in mod
        assert "to" not in mod

    def test_records_renamed_property_has_from_to(self):
        """A renamed property record must have 'from' and 'to' keys."""
        s = self._animal_schema()
        s.rename_property("Animal", "age", "years")
        records = s.change_report(fmt="records")
        ren = next((r for r in records if r.get("target") == "Animal.years"), None)
        assert ren is not None
        assert ren["op"] == "renamed"
        assert ren["from"] == "age"
        assert ren["to"] == "years"
        assert "from" in ren
        assert "to" in ren

    def test_records_subclass_created_has_detail_no_from_to_fields(self):
        """A subclass_created record must have 'detail' but no 'from'/'to'/'fields'."""
        s = self._animal_schema()
        s.create_subclass("Cat", "A cat", "Animal")
        records = s.change_report(fmt="records")
        sub = next((r for r in records if r.get("target") == "Cat"), None)
        assert sub is not None
        assert sub["op"] == "subclass_created"
        assert "detail" in sub
        assert sub["detail"]["parent"] == "Animal"
        assert "from" not in sub
        assert "to" not in sub
        assert "fields" not in sub

    def test_records_reordered_class_has_detail_no_from_to_fields(self):
        """A reordered class record must have 'detail' with order list."""
        s = self._animal_schema()
        s.assign_property_orders({"Animal": ["age", "label"]})
        records = s.change_report(fmt="records")
        reord = next((r for r in records if r.get("op") == "reordered"), None)
        assert reord is not None
        assert reord["target"] == "Animal"
        assert reord["kind"] == "class"
        assert "detail" in reord
        assert "order" in reord["detail"]
        assert "from" not in reord
        assert "to" not in reord
        assert "fields" not in reord

    def test_records_internal_detail_keys_omitted(self):
        """Pipeline-internal detail keys (reorder_candidate, before_order,
        after_order) must not appear in any record's detail dict."""
        s = self._animal_schema()
        s.assign_property_orders({"Animal": ["age", "label"]})
        records = s.change_report(fmt="records")
        for rec in records:
            detail = rec.get("detail", {})
            assert "reorder_candidate" not in detail
            assert "before_order" not in detail
            assert "after_order" not in detail

    # ------------------------------------------------------------------
    # Text renderer: ordering (classes before properties; stable by name)
    # ------------------------------------------------------------------

    def test_text_class_header_appears_before_its_properties(self):
        """The '~ ClassName [modified]' line must come before any '  + prop' lines."""
        s = self._animal_schema()
        s.create_property("Animal", "weight", DATATYPE.INTEGER)
        text = s.change_report()
        lines = text.splitlines()
        header_idx = next(
            (i for i, ln in enumerate(lines) if "Animal" in ln and "modified" in ln), None
        )
        prop_idx = next(
            (i for i, ln in enumerate(lines) if "weight" in ln), None
        )
        assert header_idx is not None and prop_idx is not None
        assert header_idx < prop_idx

    def test_text_sort_order_metadata_before_classes(self):
        """Metadata changes must appear before class changes in the text."""
        s = self._animal_schema()
        s.update_schema_metadata(name="Renamed Model", version="2.0")
        s.create_class("NewClass")
        text = s.change_report()
        lines = text.splitlines()
        # Find positions of a metadata line and the class-add line.
        meta_idx = next(
            (i for i, ln in enumerate(lines) if "schema.name" in ln), None
        )
        cls_idx = next(
            (i for i, ln in enumerate(lines) if "NewClass" in ln), None
        )
        assert meta_idx is not None and cls_idx is not None
        assert meta_idx < cls_idx

    def test_text_multiple_classes_alphabetical(self):
        """Class lines appear alphabetically for deterministic ordering."""
        base = {
            "name": "My Model v1.0",
            "createdDate": "2024-01-01T00:00:00Z",
            "lastModifiedDate": "2024-01-01T00:00:00Z",
            "classes": [],
        }
        s = DatagraphsSchema.create_from(copy.deepcopy(base))
        s.create_class("Zebra")
        s.create_class("Alpha")
        s.create_class("Mango")
        text = s.change_report()
        lines = [ln for ln in text.splitlines() if ln.startswith("+")]
        names = [ln.split()[1] for ln in lines]
        assert names == sorted(names)

    # ------------------------------------------------------------------
    # Text renderer: specific line forms
    # ------------------------------------------------------------------

    def test_text_new_class_format(self):
        """Plain new class renders as '+ ClassName [new class]'."""
        s = self._animal_schema()
        s.create_class("Plant")
        text = s.change_report()
        assert "+ Plant [new class]" in text

    def test_text_removed_class_format(self):
        """Removed class renders as '- ClassName [removed]'."""
        s = self._animal_schema()
        s.delete_class("Animal")
        text = s.change_report()
        assert "- Animal [removed]" in text

    def test_text_modified_class_shows_field_changes(self):
        """Modified class shows indented field: before -> after lines."""
        s = self._animal_schema()
        s.assign_class_description("Animal", "A living organism")
        text = s.change_report()
        assert "~ Animal [modified]" in text
        # description change is indented below the header
        desc_lines = [ln for ln in text.splitlines() if "description" in ln]
        assert len(desc_lines) >= 1
        assert desc_lines[0].startswith("  ")

    def test_text_added_property_indented(self):
        """Added property renders as '  + propName [added]'."""
        s = self._animal_schema()
        s.create_property("Animal", "weight", DATATYPE.INTEGER)
        text = s.change_report()
        assert "  + weight [added]" in text

    def test_text_removed_property_indented(self):
        """Removed property renders as '  - propName [removed]'."""
        s = self._animal_schema()
        s.delete_property("Animal", "age")
        text = s.change_report()
        assert "  - age [removed]" in text

    def test_text_reordered_class(self):
        """Reordered class renders with a 'properties reordered' sub-line."""
        s = self._animal_schema()
        s.assign_property_orders({"Animal": ["age", "label"]})
        text = s.change_report()
        assert "reordered" in text
        assert "properties reordered" in text

    def test_text_renamed_property_combined_line(self):
        """Renamed property renders as '  ~ newName: old -> new [renamed]'."""
        s = self._animal_schema()
        s.rename_property("Animal", "age", "years")
        text = s.change_report()
        assert "~ years: age -> years [renamed]" in text

    def test_text_renamed_and_modified_property_combined_line(self):
        """Rename+modify renders as one combined line with both markers."""
        s = self._animal_schema()
        s.rename_property("Animal", "age", "years")
        s.update_property("Animal", "years", is_optional=False)
        text = s.change_report()
        combined_lines = [ln for ln in text.splitlines() if "years" in ln and "renamed" in ln]
        assert len(combined_lines) == 1
        assert "isOptional" in combined_lines[0]


class TestReviewFindings:
    """Bug-first regression tests for the five BLOCKING review findings.

    Each test reproduces a subtle identity/intent bug that the original 243-test
    suite missed (it sits in the seam between the rename suite, which skips
    _annotate, and the annotation suite, which never renames between op and
    report).  See .sdlc/reviews/schema-change-tracking-uncommitted/verdict.md.
    """

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    def _animal(self) -> DatagraphsSchema:
        """Fresh Schema: class Animal with properties label (text) + age (int)."""
        base = {
            "name": "Test Model v1.0",
            "createdDate": "2024-01-01T00:00:00Z",
            "lastModifiedDate": "2024-01-01T00:00:00Z",
            "classes": [
                {
                    "type": "Class",
                    "name": "Animal",
                    "labelProperty": "label",
                    "identifierProperty": "id",
                    "isAbstract": False,
                    "properties": [
                        {
                            "type": "DatatypeProperty",
                            "name": "label",
                            "range": "text",
                            "isOptional": False,
                            "isArray": False,
                            "isLangString": True,
                            "isLabelSynonym": False,
                        },
                        {
                            "type": "DatatypeProperty",
                            "name": "age",
                            "range": "integer",
                            "isOptional": True,
                            "isArray": False,
                            "isLangString": False,
                            "isLabelSynonym": False,
                        },
                    ],
                }
            ],
        }
        return DatagraphsSchema.create_from(copy.deepcopy(base))

    # ------------------------------------------------------------------
    # FIX 1 / V-B1 — intermediate-name resolution in a rename chain
    # ------------------------------------------------------------------

    def test_fix1_intermediate_property_rename_chain_resolves_intent(self):
        """An op-log entry issued under an INTERMEDIATE property name in a chain
        dosage->x->dose must still resolve so its annotation survives.

        We rename age->x->years, issuing assign_label_property under the
        intermediate name 'x'.  The label-property detail must land on the final
        target Animal.years and the bare class labelProperty flip must be fused
        away.  Under the baseline-keyed RenameMap, the call-time name 'x'
        resolves to itself and the annotation evaporates.
        """
        s = self._animal()
        s.rename_property("Animal", "age", "x")
        # 'age' is not a langstring; make it the label under its intermediate name
        s.assign_label_property("Animal", "x", is_lang_string=True)
        s.rename_property("Animal", "x", "years")

        records = s.change_report(fmt="records")
        prop = [r for r in records if r["target"] == "Animal.years"]
        assert len(prop) == 1, records
        assert prop[0].get("detail", {}).get("label_property") == "years", records

    # ------------------------------------------------------------------
    # FIX 2 / V-B2 — recycled name swallows a deletion (class + property)
    # ------------------------------------------------------------------

    def test_fix2_class_renamed_into_deleted_name_emits_removal(self):
        """delete_class(B) then rename A->B must report BOTH A renamed to B AND
        the original B removed — not a benign rename that swallows a destructive
        recycle."""
        s = self._animal()
        s.create_class("A", "first")
        s.create_class("B", "second")
        # baseline includes A and B
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        s.delete_class("B")
        s.update_class("A", new_name="B")

        records = s.change_report(fmt="records")
        renamed = [r for r in records if r["op"] == "renamed" and r.get("from") == "A" and r.get("to") == "B"]
        removed = [r for r in records if r["op"] == "removed" and r["kind"] == "class" and r["target"] == "B"]
        assert len(renamed) == 1, records
        assert len(removed) == 1, records

    def test_fix2_property_renamed_into_deleted_name_emits_removal(self):
        """Property-level analogue: delete prop q, rename p->q on the same class
        must report p renamed to q AND original q removed."""
        s = self._animal()
        s.create_property("Animal", "p", DATATYPE.TEXT)
        s.create_property("Animal", "q", DATATYPE.TEXT)
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        s.delete_property("Animal", "q")
        s.rename_property("Animal", "p", "q")

        records = s.change_report(fmt="records")
        renamed = [r for r in records if r["op"] == "renamed" and r.get("from") == "p" and r.get("to") == "q"]
        removed = [r for r in records if r["op"] == "removed" and r["kind"] == "property" and r["target"] == "Animal.q"]
        assert len(renamed) == 1, records
        assert len(removed) == 1, records

    # ------------------------------------------------------------------
    # FIX 3 / V-B3 — reorder dropped when set also changed
    # ------------------------------------------------------------------

    def test_fix3_reorder_with_concurrent_add_still_reported(self):
        """create_property + assign_property_orders in one session must surface a
        'reordered' record even though the property SET changed."""
        s = self._animal()
        s.create_property("Animal", "colour", DATATYPE.TEXT)
        # Reorder the surviving properties (label/age) relative to baseline.
        s.assign_property_orders({"Animal": ["age", "label", "colour"]})

        records = s.change_report(fmt="records")
        reordered = [r for r in records if r["op"] == "reordered"]
        assert len(reordered) == 1, records
        assert reordered[0]["target"] == "Animal"

    # ------------------------------------------------------------------
    # FIX 4 / V-B4 — apply_to_subclasses annotation from intent, not effect
    # ------------------------------------------------------------------

    def test_fix4_apply_to_subclasses_when_subclass_already_has_value(self):
        """update_property(apply_to_subclasses=True) where the subclass already
        holds the target value (no diff effect) must still annotate the parent
        with applied_to_subclasses."""
        s = self._animal()
        s.create_class("Dog", parent_class_name="Animal")
        # Give Dog its own 'age' already in the exact post-update state so the
        # apply_to_subclasses recursion is a structural no-op on Dog.  (Note the
        # SDK's update_property passes apply_to_subclasses through into the
        # is_filterable slot, so the recursion sets isFilterable=True; pre-seed
        # that here so Dog genuinely shows no diff effect.)
        s.create_property("Dog", "age", DATATYPE.INTEGER, is_optional=False)
        dog_age = s.find_property(s.find_class("Dog")["properties"], "age")
        dog_age["isFilterable"] = True
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        # Apply to subclasses, but Dog.age already matches => no structural
        # change on Dog, so no Dog.age Change exists in the diff.
        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)

        records = s.change_report(fmt="records")
        parent = [r for r in records if r["target"] == "Animal.age"]
        assert len(parent) == 1, records
        assert "Dog" in parent[0].get("detail", {}).get("applied_to_subclasses", []), records

    # ------------------------------------------------------------------
    # FIX 5 / V-B5 — _annotate must not be a per-op linear scan (no O(L*D))
    # ------------------------------------------------------------------

    def test_fix5_annotate_scales_without_per_op_linear_scan(self):
        """A schema with many ops and many changes must report quickly.

        Under the old O(L*D) _find_index-inside-the-op-loop, L~500 ops and
        D~500 changes is ~250k predicate evals per branch; with the index-map
        fix it is O(L+D).  We assert correctness AND a generous wall-clock guard
        (well under a second) so the complexity regression cannot return without
        either breaking correctness or blowing the timer.
        """
        import time

        n = 500
        # Pre-create n classes, then rebaseline so the classes exist at baseline
        # and only the per-class property adds + reorders are the changes.
        s = DatagraphsSchema(name="Big", version="1.0")
        for i in range(n):
            s.create_class(f"C{i}", f"class {i}")
            s.create_property(f"C{i}", "a", DATATYPE.TEXT)
            s.create_property(f"C{i}", "b", DATATYPE.TEXT)
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        # L grows: a property add AND a reorder op per class (the reorder branch
        # is the one that, under the old code, also scanned per subclass).
        for i in range(n):
            s.create_property(f"C{i}", "c", DATATYPE.TEXT)
        for i in range(n):
            s.assign_property_orders({f"C{i}": ["b", "a", "c"]})

        start = time.perf_counter()
        records = s.change_report(fmt="records")
        elapsed = time.perf_counter() - start

        # Correctness: every class got a 'c' property added and a reorder.
        assert len([r for r in records if r["kind"] == "property" and r["op"] == "added"]) == n, len(records)
        assert len([r for r in records if r["op"] == "reordered"]) == n, len(records)
        # Performance guard: O(L+D) completes far under a second.
        assert elapsed < 1.0, f"change_report took {elapsed:.3f}s — possible O(L*D) regression"


# A logical change, comparable across formats, is (target, op, detail-signature).
# The detail signature canonicalises the annotation CONTENT both formats must
# agree on — applied_to_subclasses membership, the label-property flag, order,
# parent, and inherited count — so a divergence in those content dimensions
# (records carries it, text drops it) FAILS the invariant.
#
# SCOPE / HONESTY (round-5).  This invariant is the records-level logical-change
# comparison: it genuinely guarantees agreement on (target, op) and the five
# DETAIL dimensions enumerated below.  It deliberately does NOT claim totality
# over the `fields` / `from` / `to` record members, nor over un-escaped
# user-supplied field CONTENT (e.g. a description containing a newline) — those
# are documented known limitations of the best-effort text format (see
# `change_report`'s "Supported surface" note and the xfail
# `test_text_fields_content_divergence_is_a_known_limitation`).  `fmt="records"`
# is the supported, guaranteed output; this invariant polices exactly the slice
# the text rendering is expected to preserve, no more.
import collections

_LogicalChange = tuple  # (target, op, frozenset(detail-signature items))


def _detail_signature(detail: dict | None) -> frozenset:
    """Canonical, format-independent signature of an entry's reportable detail.

    Total over the FIVE published detail dimensions it compares — a divergence
    in any of ``applied_to_subclasses``, ``label_property``, ``order``,
    ``parent``, or ``inherited`` between the two renderers changes the signature,
    so the cross-format invariant cannot pass vacuously over those dimensions.

    It is NOT total over the change as a whole: the ``fields`` / ``from`` / ``to``
    record members and un-escaped field CONTENT are out of scope (a documented
    best-effort limitation of the text format — see the module note above).

    List dimensions are encoded by full content (not flattened to a boolean and
    not membership-only): ``order`` is order-PRESERVING (a reorder's whole
    meaning is the sequence), and ``applied_to_subclasses`` is a tuple in the
    rendered order so a member containing a delimiter cannot be mis-split.  The
    text renderer JSON-encodes these so the text-side parser reconstructs the
    identical content.
    """
    if not detail:
        return frozenset()
    items: set = set()
    applied = detail.get("applied_to_subclasses")
    if applied:
        items.add(("applied", tuple(applied)))
    if detail.get("label_property") is not None:
        items.add(("label", detail["label_property"]))
    if detail.get("order") is not None:
        items.add(("order", tuple(detail["order"])))
    if detail.get("parent") is not None:
        items.add(("parent", detail["parent"]))
    if detail.get("inherited") is not None:
        items.add(("inherited", detail["inherited"]))
    return frozenset(items)


def _logical_changes_from_records(records: list[dict]) -> "collections.Counter":
    """Counter of (target, op, detail-signature) logical changes from records."""
    return collections.Counter(
        (r["target"], r["op"], _detail_signature(r.get("detail")))
        for r in records
    )


def _logical_changes_from_text(text: str) -> "collections.Counter":
    """Counter of (target, op, detail-signature) logical changes from text.

    Parses the plain-text changelog back into the SAME shape the records format
    exposes — including the annotation CONTENT (``applied to subclasses: ...``
    and ``designated label property`` continuation lines) — so the two
    renderings can be compared as descriptions of one logical change set with
    multiplicity.  Continuation lines are attached to the most-recently parsed
    logical change of the enclosing entity.

    Grammar:

    * ``~ schema.X: a -> b``           -> ("schema.X", "modified")  [metadata]
    * ``+ Cls [new class]``            -> ("Cls", "added")
    * ``+ Cls [new subclass of P] ...``-> ("Cls", "subclass_created")
    * ``- Cls [removed]``              -> ("Cls", "removed")
    * ``~ Cls [reordered]``            -> ("Cls", "reordered")
    * ``~ Cls [renamed from X]``       -> ("Cls", "renamed")
    * ``~ Cls [modified]``             -> ("Cls", "modified")
    * indented ``+ p [added]`` / ``- p [removed]`` / ``~ p: ...``
                                       -> ("Cls.p", added|removed|renamed|modified)
    * continuation detail lines (JSON-encoded values) enrich the preceding
      logical change's signature, one per published dimension::

        applied to subclasses: ["A", "B"]   -> ("applied", ("A", "B"))
        designated label property: "name"   -> ("label", "name")
        reorder sequence: ["a", "b"]         -> ("order", ("a", "b"))
        subclass of: "Parent"                -> ("parent", "Parent")
        inherited count: 3                   -> ("inherited", 3)

    ``  field: a -> b`` class field detail, the human-readable
    ``  properties reordered: [...]`` summary, and the ``[new subclass of P]
    (+N inherited)`` header are NOT separate logical changes and carry no
    signature — the signature comes from the JSON continuation lines, so the
    parser is TOTAL over all five detail dimensions (round-4 B1).
    """
    # Accumulate (target, op) -> detail signature set, in encounter order, with
    # multiplicity.  We use a list of mutable records so continuation lines can
    # enrich the entry they follow, then collapse to a Counter at the end.
    parsed: list[list] = []  # each: [target, op, set(detail-items)]
    last: list | None = None
    current_class: str | None = None
    lines = text.splitlines()

    def _add(target: str, op: str) -> list:
        rec = [target, op, set()]
        parsed.append(rec)
        return rec

    # Continuation-line prefixes that carry a JSON-encoded detail dimension.
    # (prefix, signature-key) — value after the prefix is parsed with json.loads
    # so list/string content round-trips exactly (no naive delimiter splitting).
    _DETAIL_PREFIXES = (
        ("applied to subclasses:", "applied"),
        ("designated label property:", "label"),
        ("reorder sequence:", "order"),
        ("subclass of:", "parent"),
        ("inherited count:", "inherited"),
    )

    def _detail_item(body: str):
        """Parse a continuation detail line into a ``(key, value)`` signature item.

        Returns ``None`` when the line is not a detail continuation.  List values
        are normalised to tuples so the signature matches ``_detail_signature``.
        """
        for prefix, key in _DETAIL_PREFIXES:
            if body.startswith(prefix):
                raw = body[len(prefix):].strip()
                value = json.loads(raw)
                if isinstance(value, list):
                    value = tuple(value)
                return (key, value)
        return None

    def _is_class_field_line(idx: int) -> bool:
        if idx >= len(lines):
            return False
        ln = lines[idx]
        if not ln.startswith("  "):
            return False
        body = ln.strip()
        if body.startswith(("+ ", "- ", "~ ")):
            return False
        if body.startswith("properties reordered:"):
            return False
        if _detail_item(body) is not None:
            return False
        return " -> " in body

    for i, raw in enumerate(lines):
        if not raw or raw.startswith("Schema changes ("):
            continue
        if raw.startswith("  "):
            body = raw.strip()
            # Annotation continuation lines enrich the preceding logical change's
            # signature.  Each carries a JSON-encoded value parsed totally over
            # all five detail dimensions (round-4 B1).
            item = _detail_item(body)
            if item is not None:
                if last is not None:
                    last[2].add(item)
                continue
            # Property-level operator lines belong to the current class block.
            if body.startswith("+ ") and body.endswith("[added]"):
                prop = body[2:].rsplit(" [added]", 1)[0]
                last = _add(f"{current_class}.{prop}", "added")
            elif body.startswith("- ") and body.endswith("[removed]"):
                prop = body[2:].rsplit(" [removed]", 1)[0]
                last = _add(f"{current_class}.{prop}", "removed")
            elif body.startswith("~ "):
                rest = body[2:]
                prop = rest.split(":", 1)[0].strip()
                if "[renamed]" in rest:
                    last = _add(f"{current_class}.{prop}", "renamed")
                else:
                    last = _add(f"{current_class}.{prop}", "modified")
            # else: class field detail / "properties reordered" — not a change.
            continue
        # Top-level line.
        if raw.startswith("~ ") and ":" in raw and "[" not in raw:
            target = raw[2:].split(":", 1)[0].strip()
            last = _add(target, "modified")
            current_class = None
        elif raw.startswith("+ ") and raw.endswith("[new class]"):
            cls = raw[2:].rsplit(" [new class]", 1)[0]
            last = _add(cls, "added")
            current_class = cls
        elif raw.startswith("+ ") and "[new subclass of" in raw:
            cls = raw[2:].split(" [new subclass of", 1)[0]
            last = _add(cls, "subclass_created")
            current_class = cls
        elif raw.startswith("- ") and raw.endswith("[removed]"):
            cls = raw[2:].rsplit(" [removed]", 1)[0]
            last = _add(cls, "removed")
            current_class = cls
        elif raw.startswith("~ ") and raw.endswith("[reordered]"):
            cls = raw[2:].rsplit(" [reordered]", 1)[0]
            last = _add(cls, "reordered")
            current_class = cls
        elif raw.startswith("~ ") and "[renamed from" in raw:
            cls = raw[2:].split(" [renamed from", 1)[0]
            last = _add(cls, "renamed")
            current_class = cls
        elif raw.startswith("~ ") and raw.endswith("[modified]"):
            cls = raw[2:].rsplit(" [modified]", 1)[0]
            current_class = cls
            # A fieldless "~ Cls [modified]" is a SYNTHESISED header for a class
            # with only property-level changes — not itself a class change unless
            # it carries class field-detail or annotation continuation lines.
            if _is_class_field_line(i + 1):
                last = _add(cls, "modified")
            else:
                # It may still carry a class-level annotation continuation; if so
                # it is a genuine class change.  Peek the next line.
                nxt = lines[i + 1].strip() if i + 1 < len(lines) else ""
                if _detail_item(nxt) is not None:
                    last = _add(cls, "modified")
                else:
                    last = None
    return collections.Counter(
        (rec[0], rec[1], frozenset(rec[2])) for rec in parsed
    )


class TestReviewFindings2:
    """Bug-first + behavioural-contract tests for the SECOND adversarial review.

    These findings (VR-B1..VR-B5) were INTRODUCED by the first fix round: each
    cured a symptom in one output representation while regressing in another,
    because the round-1 tests asserted on records ONLY.  Every behavioural
    assertion here is therefore stated against BOTH public renderings
    (``fmt="text"`` AND ``fmt="records"``), including the negative cases, plus a
    standing cross-format invariant and cross-fix combination scenarios.

    See .sdlc/reviews/schema-change-tracking-uncommitted-2/verdict.md.
    """

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    def _animal(self) -> DatagraphsSchema:
        base = {
            "name": "Test Model v1.0",
            "createdDate": "2024-01-01T00:00:00Z",
            "lastModifiedDate": "2024-01-01T00:00:00Z",
            "classes": [
                {
                    "type": "Class",
                    "name": "Animal",
                    "labelProperty": "label",
                    "identifierProperty": "id",
                    "isAbstract": False,
                    "properties": [
                        {"type": "DatatypeProperty", "name": "label", "range": "text",
                         "isOptional": False, "isArray": False, "isLangString": True,
                         "isLabelSynonym": False},
                        {"type": "DatatypeProperty", "name": "age", "range": "integer",
                         "isOptional": True, "isArray": False, "isLangString": False,
                         "isLabelSynonym": False},
                    ],
                }
            ],
        }
        return DatagraphsSchema.create_from(copy.deepcopy(base))

    def _multi(self, *names: str) -> DatagraphsSchema:
        """Schema with one class per name, each carrying label + a text prop."""
        classes = []
        for n in names:
            classes.append({
                "type": "Class", "name": n, "labelProperty": "label",
                "identifierProperty": "id", "isAbstract": False,
                "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                ],
            })
        base = {"name": "M", "createdDate": "x", "lastModifiedDate": "x",
                "classes": classes}
        return DatagraphsSchema.create_from(copy.deepcopy(base))

    # ==================================================================
    # Standing cross-format invariant (guards renderer divergence for ANY
    # mutation mix, regardless of scenario).
    # ==================================================================

    def _assert_formats_agree(self, s: DatagraphsSchema) -> "collections.Counter":
        """Both renderings must agree on the records-level logical change MULTISET.

        Derives the (target, op, detail-signature) Counter from text and from
        records and asserts equality in BOTH directions over the slice the text
        format is expected to preserve: target, op, and the five compared detail
        dimensions, with multiplicity.

        SCOPE (round-5 honesty): this asserts the genuine, held guarantee — it is
        deliberately NOT total over ``fields`` / ``from`` / ``to`` or over
        un-escaped field CONTENT, which are documented best-effort limitations of
        the text format (see ``_detail_signature`` and the xfail
        ``test_text_fields_content_divergence_is_a_known_limitation``).  Returns
        the agreed Counter.
        """
        records = s.change_report(fmt="records")
        text = s.change_report(fmt="text")
        from_records = _logical_changes_from_records(records)
        from_text = _logical_changes_from_text(text)
        assert from_text == from_records, (
            f"renderer divergence:\n  text-only={from_text - from_records}\n"
            f"  records-only={from_records - from_text}\n--- text ---\n{text}\n"
            f"--- records ---\n{records}"
        )
        return from_records

    def test_cross_format_invariant_arbitrary_mutation_mix(self):
        """For an arbitrary mix of mutations the set of logical (target, op)
        changes described by text and by records must be IDENTICAL."""
        s = self._animal()
        s.create_class("Dog", parent_class_name="Animal")
        s.create_property("Animal", "weight", DATATYPE.INTEGER, apply_to_subclasses=True)
        s.rename_property("Animal", "age", "years")
        s.assign_label_property("Animal", "label", is_lang_string=True)
        s.create_property("Animal", "colour", DATATYPE.TEXT)
        s.assign_property_orders({"Animal": ["years", "label", "weight", "colour"]})
        s.create_class("Spare")
        s.delete_class("Spare")
        agreed = self._assert_formats_agree(s)
        # Sanity: the mix produced a non-trivial set.
        assert len(agreed) >= 4, agreed

    # ==================================================================
    # VR-B1 — text renderer must render ALL class-level Changes for a class
    # ==================================================================

    def test_vrb1_recycled_class_name_removed_visible_in_both_formats(self):
        """A class renamed into a deletion-freed name yields renamed + removed.
        BOTH must appear in records AND in the default text — a destructive
        recycle must never render as a benign rename in the default output."""
        s = self._multi("A", "B")
        s.delete_class("B")
        s.update_class("A", new_name="B")

        records = s.change_report("records")
        ops = {(r["target"], r["op"]) for r in records}
        assert ("B", "renamed") in ops, records
        assert ("B", "removed") in ops, records

        text = s.change_report("text")
        assert "[renamed from A]" in text, text
        assert "- B [removed]" in text, text  # destructive op NOT swallowed

        # Cross-format agreement is the corollary.
        self._assert_formats_agree(s)

    def test_vrb1_reorder_with_concurrent_add_visible_in_both_formats(self):
        """A class carrying both a modified/added change AND a reordered change
        must show the reorder in text as well as records (text must not drop the
        second class-level Change)."""
        s = self._animal()
        s.assign_class_description("Animal", "new desc")
        s.create_property("Animal", "colour", DATATYPE.TEXT)
        s.assign_property_orders({"Animal": ["age", "label", "colour"]})

        records = s.change_report("records")
        ops = {(r["target"], r["op"]) for r in records}
        assert ("Animal", "reordered") in ops, records
        assert ("Animal", "modified") in ops, records
        assert ("Animal.colour", "added") in ops, records

        text = s.change_report("text")
        assert "[reordered]" in text, text
        assert "properties reordered:" in text, text
        assert "[modified]" in text, text
        assert "+ colour [added]" in text, text

        self._assert_formats_agree(s)

    def test_vrb1_negative_single_class_change_unchanged_in_text(self):
        """Negative case: a class with ONLY one class-level Change still renders a
        single block (no spurious extra lines) in both formats."""
        s = self._animal()
        s.assign_class_description("Animal", "desc")
        text = s.change_report("text")
        assert text.count("[modified]") == 1, text
        assert "[reordered]" not in text and "[removed]" not in text, text
        self._assert_formats_agree(s)

    # ==================================================================
    # VR-B2 — recycle-aware alias resolution (annotation must not evaporate)
    # ==================================================================

    def test_vrb2_label_annotation_survives_name_recycle_both_formats(self):
        """assign_label_property under name A, then A->B and C->A (C recycles the
        freed name A).  The label-property fusion must still land on B.title — the
        flat last-writer-wins alias map dropped it; per-position resolution keeps
        it.  Asserted in BOTH formats."""
        base = {
            "name": "M", "createdDate": "x", "lastModifiedDate": "x",
            "classes": [
                {"type": "Class", "name": "A", "labelProperty": "label",
                 "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "title", "range": "text",
                     "isOptional": True, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False}]},
                {"type": "Class", "name": "C", "labelProperty": "label",
                 "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False}]},
            ],
        }
        s = DatagraphsSchema.create_from(copy.deepcopy(base))
        s.assign_label_property("A", "title", is_lang_string=True)
        s.update_class("A", new_name="B")
        s.update_class("C", new_name="A")

        records = s.change_report("records")
        b_title = [r for r in records if r["target"] == "B.title"]
        assert len(b_title) == 1, records
        assert b_title[0].get("detail", {}).get("label_property") == "title", records

        text = s.change_report("text")
        # The label-property fusion drops the bare class labelProperty flip, so
        # the property line carries the designation; the bare flip must be gone.
        assert "B.title" not in text or "labelProperty" not in text, text

        self._assert_formats_agree(s)

    def test_vrb2_apply_to_subclasses_detail_survives_parent_recycle(self):
        """update_property(apply_to_subclasses) on Parent, then Parent->P2 and a
        recycle of the freed name 'Parent' by another class.  The
        applied_to_subclasses annotation must resolve to P2.weight, not evaporate."""
        s = self._multi("Parent", "Other")
        s.create_class("Sub", parent_class_name="Parent")
        s.create_property("Parent", "weight", DATATYPE.INTEGER)
        s.create_property("Sub", "weight", DATATYPE.INTEGER)
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        s.update_property("Parent", "weight", is_optional=False, apply_to_subclasses=True)
        s.update_class("Parent", new_name="P2")
        s.update_class("Other", new_name="Parent")  # recycle freed name

        records = s.change_report("records")
        p2 = [r for r in records if r["target"] == "P2.weight"]
        assert len(p2) == 1, records
        assert "Sub" in p2[0].get("detail", {}).get("applied_to_subclasses", []), records
        self._assert_formats_agree(s)

    def test_vrb2_negative_no_recycle_label_still_fuses(self):
        """Control: WITHOUT a recycle the label fusion still works (the fix must
        not break the common case)."""
        s = self._animal()
        s.assign_label_property("Animal", "age", is_lang_string=False)
        records = s.change_report("records")
        animal_age = [r for r in records if r["target"] == "Animal.age"]
        assert len(animal_age) == 1, records
        assert animal_age[0].get("detail", {}).get("label_property") == "age", records
        self._assert_formats_agree(s)

    # ==================================================================
    # VR-B3 — apply_to_subclasses annotation from OP-TIME intent
    # ==================================================================

    def test_vrb3_no_overclaim_of_subclass_created_after_op(self):
        """A subclass created AFTER the apply_to_subclasses call must NOT appear in
        applied_to_subclasses (the op never touched it) — in both formats."""
        s = self._animal()
        s.create_class("Dog", parent_class_name="Animal")
        s.create_property("Dog", "age", DATATYPE.INTEGER, is_optional=True)
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)
        s.create_class("Cat", parent_class_name="Animal")  # AFTER the op

        records = s.change_report("records")
        parent = [r for r in records if r["target"] == "Animal.age"]
        assert len(parent) == 1, records
        applied = parent[0].get("detail", {}).get("applied_to_subclasses", [])
        assert "Dog" in applied, records
        assert "Cat" not in applied, f"over-claimed late subclass Cat: {records}"
        self._assert_formats_agree(s)

    def test_vrb3_annotation_survives_parent_no_op_cascade(self):
        """When only a subclass changes (the parent property is a no-op) the
        applied_to_subclasses annotation must still be surfaced from intent."""
        s = self._animal()
        s.create_class("Dog", parent_class_name="Animal")
        s.create_property("Dog", "age", DATATYPE.INTEGER, is_optional=True)
        s.update_property("Animal", "age", is_optional=False)  # parent now False
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        # Parent already False => no parent diff effect; only Dog changes.
        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)

        records = s.change_report("records")
        parent = [r for r in records if r["target"] == "Animal.age"]
        assert len(parent) == 1, records
        assert "Dog" in parent[0].get("detail", {}).get("applied_to_subclasses", []), records
        self._assert_formats_agree(s)

    def test_vrb3_transitive_cascade_recorded_and_applied(self):
        """apply_to_subclasses cascades transitively (N5 arg-slot fix); the
        annotation lists every transitive subclass actually touched."""
        s = self._animal()
        s.create_class("Dog", parent_class_name="Animal")
        s.create_class("Puppy", parent_class_name="Dog")
        s.create_property("Dog", "age", DATATYPE.INTEGER)
        s.create_property("Puppy", "age", DATATYPE.INTEGER)
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()

        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)

        # The grandchild was genuinely updated (N5: recursion no longer stops at
        # depth 1 because every arg is now passed by keyword).
        puppy_age = s.find_property(s.find_class("Puppy")["properties"], "age")
        assert puppy_age["isOptional"] is False
        records = s.change_report("records")
        parent = [r for r in records if r["target"] == "Animal.age"]
        applied = parent[0].get("detail", {}).get("applied_to_subclasses", [])
        assert set(applied) == {"Dog", "Puppy"}, records
        self._assert_formats_agree(s)

    def test_vrb3_negative_no_flag_no_annotation(self):
        """A create/update_property WITHOUT apply_to_subclasses must carry no
        applied_to_subclasses detail."""
        s = self._animal()
        s.create_class("Dog", parent_class_name="Animal")
        s._baseline = copy.deepcopy(s._schema)
        s._change_log.clear()
        s.update_property("Animal", "age", is_optional=False)  # no flag
        records = s.change_report("records")
        parent = [r for r in records if r["target"] == "Animal.age"]
        assert len(parent) == 1, records
        assert "applied_to_subclasses" not in (parent[0].get("detail") or {}), records
        self._assert_formats_agree(s)

    # ==================================================================
    # VR-B4 — net-effect collapse: at most ONE reordered per class
    # ==================================================================

    def test_vrb4_two_reorders_collapse_to_one_record_both_formats(self):
        """Two assign_property_orders on one class must emit AT MOST ONE net
        reordered — in records (no duplicate) and in text (no duplicate line)."""
        base = {
            "name": "M", "createdDate": "x", "lastModifiedDate": "x",
            "classes": [{
                "type": "Class", "name": "A", "labelProperty": "label",
                "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "p1", "range": "text",
                     "isOptional": True, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "p2", "range": "text",
                     "isOptional": True, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "p3", "range": "text",
                     "isOptional": True, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False}]}],
        }
        s = DatagraphsSchema.create_from(copy.deepcopy(base))
        s.assign_property_orders({"A": ["p3", "p2", "p1"]})
        s.assign_property_orders({"A": ["p2", "p3", "p1"]})

        records = s.change_report("records")
        reordered = [r for r in records if r["op"] == "reordered"]
        assert len(reordered) == 1, records
        # The single net reorder reflects the FINAL order.
        assert reordered[0]["detail"]["order"] == ["p2", "p3", "p1"], records

        text = s.change_report("text")
        assert text.count("[reordered]") == 1, text
        assert text.count("properties reordered:") == 1, text
        self._assert_formats_agree(s)

    def test_vrb4_two_reorders_netting_to_baseline_emit_none(self):
        """If the net order equals baseline, no reordered is emitted (collapse to
        the net effect) — in both formats."""
        base = {
            "name": "M", "createdDate": "x", "lastModifiedDate": "x",
            "classes": [{
                "type": "Class", "name": "A", "labelProperty": "label",
                "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "p1", "range": "text",
                     "isOptional": True, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "p2", "range": "text",
                     "isOptional": True, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False}]}],
        }
        s = DatagraphsSchema.create_from(copy.deepcopy(base))
        s.assign_property_orders({"A": ["p2", "p1"]})  # swap
        s.assign_property_orders({"A": ["p1", "p2"]})  # swap back -> baseline

        records = s.change_report("records")
        assert [r for r in records if r["op"] == "reordered"] == [], records
        text = s.change_report("text")
        assert "[reordered]" not in text, text
        self._assert_formats_agree(s)

    # ==================================================================
    # VR-B5 — no O(n^2): scale C at FIXED L, assert ~linear (not quadratic)
    # ==================================================================

    def test_vrb5_apply_to_subclasses_scales_linearly_in_class_count(self):
        """The apply_to_subclasses path must not re-scan all classes per op.

        Scale C (class count) at FIXED L (op count) and assert the wall-clock
        stays roughly linear, not quadratic, in C.  Under the regressed per-op
        O(C) subclass scan this was measured quadratic; with the once-built
        subclasses_by_parent index it is O(L + C).  Generous, non-flaky bound.
        """
        import time

        def run(num_subclasses: int, ops: int) -> float:
            base_classes = [{
                "type": "Class", "name": "Root", "labelProperty": "label",
                "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "p", "range": "integer",
                     "isOptional": True, "isArray": False, "isLangString": False,
                     "isLabelSynonym": False}]}]
            for i in range(num_subclasses):
                base_classes.append({
                    "type": "Class", "name": f"S{i}", "subClassOf": "Root",
                    "labelProperty": "label", "identifierProperty": "id",
                    "isAbstract": False, "properties": [
                        {"type": "DatatypeProperty", "name": "label", "range": "text",
                         "isOptional": False, "isArray": False, "isLangString": True,
                         "isLabelSynonym": False},
                        {"type": "DatatypeProperty", "name": "p", "range": "integer",
                         "isOptional": True, "isArray": False, "isLangString": False,
                         "isLabelSynonym": False}]})
            base = {"name": "M", "createdDate": "x", "lastModifiedDate": "x",
                    "classes": base_classes}
            s = DatagraphsSchema.create_from(copy.deepcopy(base))
            s._baseline = copy.deepcopy(s._schema)
            s._change_log.clear()
            # L FIXED at `ops`: repeat the same apply_to_subclasses op.
            flag = True
            for _ in range(ops):
                flag = not flag
                s.update_property("Root", "p", is_optional=flag, apply_to_subclasses=True)
            start = time.perf_counter()
            s.change_report(fmt="records")
            return time.perf_counter() - start

        ops = 40
        small = run(20, ops)
        large = run(160, ops)  # 8x the classes, SAME op count
        # If the per-op O(C) scan had returned, 8x C at fixed L would be ~8x time
        # (the report cost would scale with C per op).  Linear-in-C with a
        # once-built index keeps the ratio modest; allow generous slack for noise.
        assert large < small * 4 + 0.05, (
            f"apply_to_subclasses cost scales too steeply in C "
            f"(small={small:.4f}s @C=21, large={large:.4f}s @C=161) — "
            f"possible reintroduced per-op O(C) scan"
        )

    # ==================================================================
    # CROSS-FIX COMBINATION — the untested seam the reviewers flagged
    # ==================================================================

    def test_cross_fix_combination_session_both_formats(self):
        """ONE session exercising every fix together:

        * recycle a class name (delete X; rename Y->X),
        * a chained rename A->B->C with an op (label) issued under intermediate B,
        * a reorder that also adds a property,
        * an apply_to_subclasses that no-ops on the parent (only the subclass
          changes),
        * two reorders on one class collapsing to one net reordered.

        The full intended behaviour must hold in records AND text.
        """
        base = {
            "name": "M", "createdDate": "x", "lastModifiedDate": "x",
            "classes": [
                # Recycle pair
                {"type": "Class", "name": "X", "labelProperty": "label",
                 "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False}]},
                {"type": "Class", "name": "Y", "labelProperty": "label",
                 "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False}]},
                # Chained-rename class with a real property to relabel
                {"type": "Class", "name": "A", "labelProperty": "label",
                 "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "title", "range": "text",
                     "isOptional": True, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False}]},
                # Parent + subclass for the parent-no-op cascade
                {"type": "Class", "name": "Parent", "labelProperty": "label",
                 "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "flag", "range": "integer",
                     "isOptional": False, "isArray": False, "isLangString": False,
                     "isLabelSynonym": False}]},
                {"type": "Class", "name": "Kid", "subClassOf": "Parent",
                 "labelProperty": "label", "identifierProperty": "id",
                 "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "flag", "range": "integer",
                     "isOptional": True, "isArray": False, "isLangString": False,
                     "isLabelSynonym": False}]},
                # Class for the two-reorder collapse
                {"type": "Class", "name": "Ord", "labelProperty": "label",
                 "identifierProperty": "id", "isAbstract": False, "properties": [
                    {"type": "DatatypeProperty", "name": "label", "range": "text",
                     "isOptional": False, "isArray": False, "isLangString": True,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "o1", "range": "integer",
                     "isOptional": True, "isArray": False, "isLangString": False,
                     "isLabelSynonym": False},
                    {"type": "DatatypeProperty", "name": "o2", "range": "integer",
                     "isOptional": True, "isArray": False, "isLangString": False,
                     "isLabelSynonym": False}]},
            ],
        }
        s = DatagraphsSchema.create_from(copy.deepcopy(base))

        # 1. Recycle: delete X, rename Y -> X.
        s.delete_class("X")
        s.update_class("Y", new_name="X")

        # 2. Chained rename A->B->C with the label op issued under intermediate B.
        s.update_class("A", new_name="B")
        s.assign_label_property("B", "title", is_lang_string=True)  # op under B
        s.update_class("B", new_name="C")

        # 3. apply_to_subclasses that no-ops on the parent (Parent.flag already
        #    False; only Kid.flag changes False<-True).
        s.update_property("Parent", "flag", is_optional=False, apply_to_subclasses=True)

        # 4. Reorder that also adds a property (on C).
        s.create_property("C", "extra", DATATYPE.INTEGER)
        s.assign_property_orders({"C": ["title", "label", "extra"]})

        # 5. Two reorders on Ord collapsing to one net reordered.
        s.assign_property_orders({"Ord": ["o2", "o1"]})
        s.assign_property_orders({"Ord": ["o2", "label", "o1"]})

        records = s.change_report("records")
        ops = {(r["target"], r["op"]) for r in records}

        # --- Recycle: BOTH renamed and removed for X ---
        assert ("X", "renamed") in ops, records
        assert ("X", "removed") in ops, records

        # --- Chained rename + label fusion lands on C.title ---
        c_title = [r for r in records if r["target"] == "C.title"]
        assert len(c_title) == 1, records
        assert c_title[0].get("detail", {}).get("label_property") == "title", records
        assert ("C", "renamed") in ops, records

        # --- apply_to_subclasses cascade annotated even though parent no-op ---
        parent_flag = [r for r in records if r["target"] == "Parent.flag"]
        assert len(parent_flag) == 1, records
        assert "Kid" in parent_flag[0].get("detail", {}).get("applied_to_subclasses", []), records
        assert ("Kid.flag", "modified") in ops, records

        # --- Reorder + add on C ---
        assert ("C", "reordered") in ops, records
        assert ("C.extra", "added") in ops, records

        # --- Two reorders on Ord collapse to exactly one ---
        ord_reorders = [r for r in records if r["target"] == "Ord" and r["op"] == "reordered"]
        assert len(ord_reorders) == 1, records

        # --- No duplicate reordered anywhere ---
        reordered_targets = [r["target"] for r in records if r["op"] == "reordered"]
        assert len(reordered_targets) == len(set(reordered_targets)), records

        # --- The two formats describe the SAME logical change set ---
        self._assert_formats_agree(s)

        # --- Text spot-checks for the destructive/ordering signals ---
        text = s.change_report("text")
        assert "- X [removed]" in text, text
        assert "[renamed from Y]" in text, text
        assert "properties reordered:" in text, text

class TestReviewFindings3:
    """Bug-first + behavioural-contract tests for the FOURTH (redesign) review.

    The prior three rounds point-patched a name-keyed identity model whose
    defect family (recycle / quadratic / format-divergence) kept displacing one
    layer outward.  This round REPLACES the model with an event-sourced identity
    replay (``_replay_identities``): identity is minted on creation, rebound on
    rename, ended on deletion, so a recycled name binds a DIFFERENT identity by
    construction.

    Every behavioural assertion is stated against BOTH public renderings
    (``fmt="text"`` AND ``fmt="records"``), derived adversarially from the
    contract (including negative space), and the cross-format invariant is
    proven NON-VACUOUS.

    See .sdlc/reviews/schema-change-tracking-uncommitted-3/verdict.md and the
    redesign brief (mandated model: event-sourced identity replay).
    """

    # ------------------------------------------------------------------
    # Fixtures
    # ------------------------------------------------------------------

    def _empty(self) -> DatagraphsSchema:
        return DatagraphsSchema(name="T", version="1.0")

    def _from(self, s: DatagraphsSchema) -> DatagraphsSchema:
        """Re-baseline: a fresh Schema whose baseline IS s's current state."""
        return DatagraphsSchema.create_from(copy.deepcopy(s.to_dict()))

    def _ops(self, s: DatagraphsSchema) -> set[tuple[str, str]]:
        return {(r["target"], r["op"]) for r in s.change_report("records")}

    def _rec(self, s: DatagraphsSchema, target: str) -> list[dict]:
        return [r for r in s.change_report("records") if r["target"] == target]

    # ==================================================================
    # CONTRACT 1 — recycle-by-rename: delete B; rename A->B
    # ==================================================================

    def test_contract1_recycle_by_rename_both_formats(self):
        s = self._empty()
        s.create_class("A")
        s.create_class("B")
        s = self._from(s)
        s.delete_class("B")
        s.update_class("A", new_name="B")

        ops = self._ops(s)
        assert ("B", "renamed") in ops, ops
        assert ("B", "removed") in ops, ops
        renamed = [r for r in s.change_report("records")
                   if r["target"] == "B" and r["op"] == "renamed"]
        assert renamed[0]["from"] == "A" and renamed[0]["to"] == "B", renamed

        text = s.change_report("text")
        assert "~ B [renamed from A]" in text, text
        assert "- B [removed]" in text, text

    # ==================================================================
    # CONTRACT 2 — recycle-by-CREATION (the round-3 killer)
    # ==================================================================

    def test_contract2_recycle_by_creation_class_both_formats(self):
        """delete/rename frees A; CREATE a new A; rename new A->C.
        Original A's continuation is the rename; the NEW entity is added —
        the new entity must NEVER be 'renamed from A' (round-3 assumptions B1)."""
        s = self._empty()
        s.create_class("A")
        s = self._from(s)
        s.update_class("A", new_name="B")   # baseline A -> B
        s.create_class("A")                  # NEW class recycles freed name
        s.update_class("A", new_name="C")    # NEW A -> C

        ops = self._ops(s)
        assert ("B", "renamed") in ops, ops          # baseline A's continuation
        assert ("C", "added") in ops, ops            # net-new, NOT renamed
        assert ("C", "renamed") not in ops, ops      # the round-3 killer mislabel
        b = self._rec(s, "B")
        assert b[0]["op"] == "renamed" and b[0]["from"] == "A", b

        text = s.change_report("text")
        assert "~ B [renamed from A]" in text, text
        assert "+ C [new class]" in text, text
        assert "renamed from A]" in text and text.count("renamed from A") == 1, text

    def test_contract2_recycle_by_creation_via_delete_class_both_formats(self):
        """Free the name by DELETING B, then CREATE a new B, then rename B->C."""
        s = self._empty()
        s.create_class("B")
        s = self._from(s)
        s.delete_class("B")     # baseline B ends
        s.create_class("B")     # NEW B recycles the freed name
        s.update_class("B", new_name="C")

        ops = self._ops(s)
        assert ("B", "removed") in ops, ops      # baseline B destroyed
        assert ("C", "added") in ops, ops        # net-new
        assert ("C", "renamed") not in ops, ops  # never renamed-from-baseline

        text = s.change_report("text")
        assert "- B [removed]" in text, text
        assert "+ C [new class]" in text, text

    def test_contract2_recycle_by_creation_property_both_formats(self):
        """Property-level recycle-by-creation: rename x->x_old; create x; rename x->x_new."""
        s = self._empty()
        s.create_class("K")
        s.create_property("K", "x", DATATYPE.TEXT)
        s = self._from(s)
        s.rename_property("K", "x", "x_old")
        s.create_property("K", "x", DATATYPE.TEXT)
        s.rename_property("K", "x", "x_new")

        ops = self._ops(s)
        assert ("K.x_old", "renamed") in ops, ops    # baseline x's continuation
        assert ("K.x_new", "added") in ops, ops      # net-new
        assert ("K.x_new", "renamed") not in ops, ops
        old = self._rec(s, "K.x_old")
        assert old[0]["from"] == "x", old

        text = s.change_report("text")
        assert "~ x_old: x -> x_old [renamed]" in text, text
        assert "+ x_new [added]" in text, text

    def test_contract2_recycle_by_creation_via_create_subclass_both_formats(self):
        """create_subclass recycles a freed class name => the subclass is its own
        creation (subclass_created), never a rename of the freed baseline class."""
        s = self._empty()
        s.create_class("Parent")
        s.create_class("A")
        s = self._from(s)
        s.delete_class("A")                                   # free name A
        s.create_subclass("A", "desc", "Parent")              # NEW A as subclass

        ops = self._ops(s)
        assert ("A", "removed") in ops, ops
        assert ("A", "subclass_created") in ops, ops
        assert ("A", "renamed") not in ops, ops

        text = s.change_report("text")
        assert "- A [removed]" in text, text
        assert "[new subclass of Parent]" in text, text

    # ==================================================================
    # CONTRACT 3 — rename chain A->B->C with an op issued under intermediate B
    # ==================================================================

    def test_contract3_rename_chain_op_under_intermediate_both_formats(self):
        s = self._empty()
        s.create_class("A")
        s.create_property("A", "p", DATATYPE.INTEGER)
        s = self._from(s)
        s.update_class("A", new_name="B")
        s.update_property("B", "p", is_optional=False)   # op issued while named B
        s.update_class("B", new_name="C")

        ops = self._ops(s)
        assert ("C", "renamed") in ops, ops
        c = self._rec(s, "C")
        assert c[0]["from"] == "A", c
        # The property modification resolves onto the final entity C.p.
        assert ("C.p", "modified") in ops, ops

        text = s.change_report("text")
        assert "~ C [renamed from A]" in text, text
        assert "~ p: isOptional: true -> false" in text, text

    def test_contract3_rename_chain_property_under_intermediate_both_formats(self):
        s = self._empty()
        s.create_class("K")
        s.create_property("K", "p", DATATYPE.INTEGER)
        s = self._from(s)
        s.rename_property("K", "p", "q")
        s.update_property("K", "q", is_optional=False)   # under intermediate name q
        s.rename_property("K", "q", "r")

        ops = self._ops(s)
        assert ("K.r", "renamed") in ops, ops
        r = self._rec(s, "K.r")
        assert r[0]["from"] == "p", r
        # The modification merged into the single renamed record.
        assert r[0].get("fields"), r

        text = s.change_report("text")
        assert "~ r: p -> r [renamed]" in text, text
        assert "isOptional: true -> false" in text, text

    # ==================================================================
    # CONTRACT 4 — round-trip A->B->A (and property) => no entry
    # ==================================================================

    def test_contract4_class_round_trip_no_entry_both_formats(self):
        s = self._empty()
        s.create_class("A")
        s = self._from(s)
        s.update_class("A", new_name="B")
        s.update_class("B", new_name="A")
        assert s.change_report("records") == [], s.change_report("records")
        assert s.change_report("text") == "", s.change_report("text")

    def test_contract4_property_round_trip_no_entry_both_formats(self):
        s = self._empty()
        s.create_class("K")
        s.create_property("K", "p", DATATYPE.INTEGER)
        s = self._from(s)
        s.rename_property("K", "p", "q")
        s.rename_property("K", "q", "p")
        assert s.change_report("records") == [], s.change_report("records")
        assert s.change_report("text") == "", s.change_report("text")

    # ==================================================================
    # CONTRACT 5 — apply_to_subclasses (intent-driven, op-time set, transitive,
    # rendered in BOTH formats, deleted-after-op behaviour)
    # ==================================================================

    def _hierarchy(self) -> DatagraphsSchema:
        """Animal -> Dog -> Puppy (transitive), re-baselined to 0 changes."""
        s = self._empty()
        s.create_class("Animal")
        s.create_subclass("Dog", "d", "Animal")
        s.create_subclass("Puppy", "p", "Dog")
        return self._from(s)

    def test_contract5_apply_to_subclasses_annotation_both_formats(self):
        s = self._hierarchy()
        s.create_property("Animal", "tail", DATATYPE.TEXT, apply_to_subclasses=True)

        parent = self._rec(s, "Animal.tail")
        assert len(parent) == 1, parent
        applied = parent[0].get("detail", {}).get("applied_to_subclasses", [])
        assert set(applied) == {"Dog", "Puppy"}, applied   # transitive

        text = s.change_report("text")
        assert "applied to subclasses:" in text, text
        assert "Dog" in text and "Puppy" in text, text

    def test_contract5_annotation_renders_in_both_formats_invariant(self):
        s = self._hierarchy()
        s.create_property("Animal", "tail", DATATYPE.TEXT, apply_to_subclasses=True)
        # The strengthened invariant compares detail content across formats.
        TestReviewFindings2()._assert_formats_agree(s)

    def test_contract5_parent_no_op_still_annotates_both_formats(self):
        """Parent already holds the value (no structural diff on parent) — the
        intent is still surfaced from the op-log (ADR 0001)."""
        s = self._empty()
        s.create_class("Animal")
        s.create_property("Animal", "age", DATATYPE.INTEGER, is_optional=False)
        s.create_subclass("Dog", "d", "Animal")
        # Make Dog.age differ so the cascade has a real effect on Dog only.
        s.update_property("Dog", "age", is_optional=True)
        s = self._from(s)
        s.update_property("Animal", "age", is_optional=False, apply_to_subclasses=True)

        parent = self._rec(s, "Animal.age")
        assert len(parent) == 1, parent
        assert "Dog" in parent[0].get("detail", {}).get("applied_to_subclasses", []), parent

        text = s.change_report("text")
        assert 'applied to subclasses: ["Dog"]' in text, text
        TestReviewFindings2()._assert_formats_agree(s)

    def test_contract5_no_overclaim_of_subclass_created_after_op(self):
        s = self._empty()
        s.create_class("Animal")
        s.create_subclass("Dog", "d", "Animal")
        s = self._from(s)
        s.create_property("Animal", "tail", DATATYPE.TEXT, apply_to_subclasses=True)
        s.create_subclass("Cat", "c", "Animal")   # created AFTER the cascade op

        parent = self._rec(s, "Animal.tail")
        applied = parent[0].get("detail", {}).get("applied_to_subclasses", [])
        assert "Dog" in applied and "Cat" not in applied, applied

    def test_contract5_deleted_after_op_excluded_both_formats(self):
        """A subclass touched at op time but deleted AFTERWARDS must not be
        named in applied_to_subclasses (net-effect consistency: the same report
        marks it removed).  Both formats."""
        s = self._empty()
        s.create_class("Animal")
        s.create_subclass("Dog", "d", "Animal")
        s.create_subclass("Cat", "c", "Animal")
        s = self._from(s)
        s.create_property("Animal", "tail", DATATYPE.TEXT, apply_to_subclasses=True)
        s.delete_class("Cat")    # deleted after the cascade

        ops = self._ops(s)
        assert ("Cat", "removed") in ops, ops
        parent = self._rec(s, "Animal.tail")
        applied = parent[0].get("detail", {}).get("applied_to_subclasses", [])
        assert "Dog" in applied, applied
        assert "Cat" not in applied, applied   # not claimed while also removed

        text = s.change_report("text")
        assert "- Cat [removed]" in text, text
        assert "Cat" not in text.split("applied to subclasses:")[1].split("\n")[0], text
        TestReviewFindings2()._assert_formats_agree(s)

    def test_contract5_negative_no_flag_no_annotation_both_formats(self):
        s = self._hierarchy()
        s.create_property("Animal", "tail", DATATYPE.TEXT)  # no apply_to_subclasses
        parent = self._rec(s, "Animal.tail")
        assert "applied_to_subclasses" not in parent[0].get("detail", {}), parent
        assert "applied to subclasses:" not in s.change_report("text")

    # ==================================================================
    # CONTRACT 6 — reorder collapse (net), no __order__ sentinel leak
    # ==================================================================

    def test_contract6_multiple_reorders_collapse_to_one_both_formats(self):
        s = self._empty()
        s.create_class("K")
        s.create_property("K", "a", DATATYPE.TEXT)
        s.create_property("K", "b", DATATYPE.TEXT)
        s.create_property("K", "c", DATATYPE.TEXT)
        s = self._from(s)
        s.assign_property_orders({"K": ["label", "c", "b", "a"]})
        s.assign_property_orders({"K": ["label", "b", "c", "a"]})

        reordered = [r for r in s.change_report("records")
                     if r["target"] == "K" and r["op"] == "reordered"]
        assert len(reordered) == 1, reordered

        text = s.change_report("text")
        assert text.count("[reordered]") == 1, text
        assert "__order__" not in text, text
        assert all("__order__" not in r["target"] for r in s.change_report("records"))

    def test_contract6_reorder_net_to_baseline_emits_none_both_formats(self):
        s = self._empty()
        s.create_class("K")
        s.create_property("K", "a", DATATYPE.TEXT)
        s.create_property("K", "b", DATATYPE.TEXT)
        s = self._from(s)
        # Net back to baseline order (label, a, b) — list all so none are appended.
        s.assign_property_orders({"K": ["label", "b", "a"]})
        s.assign_property_orders({"K": ["label", "a", "b"]})
        assert s.change_report("records") == [], s.change_report("records")
        assert s.change_report("text") == "", s.change_report("text")

    def test_contract6_untracked_reorder_no_sentinel_leak_both_formats(self):
        """A reorder done via to_dict (no op-log entry) must not leak __order__."""
        s = self._empty()
        s.create_class("K")
        s.create_property("K", "a", DATATYPE.TEXT)
        s.create_property("K", "b", DATATYPE.TEXT)
        s = self._from(s)
        props = s.to_dict()["classes"][0]["properties"]
        props.reverse()   # untracked reorder
        recs = s.change_report("records")
        assert all("__order__" not in r["target"] for r in recs), recs
        assert "__order__" not in s.change_report("text")

    # ==================================================================
    # CONTRACT 7 — cross-format invariant, PROVEN NON-VACUOUS
    # ==================================================================

    def test_contract7_invariant_non_vacuous_detail_divergence_fails(self):
        """Inject a deliberately divergent output (records carries cascade detail
        the text drops) and confirm the strengthened invariant FAILS — proving
        it is NOT vacuous on the dimension (detail) the round-3 bug lived in."""
        text_without_detail = (
            "Schema changes (1):\n"
            "~ Animal [modified]\n"
            "  + tail [added]\n"
        )
        records_with_detail = [
            {"target": "Animal.tail", "kind": "property", "op": "added",
             "detail": {"applied_to_subclasses": ["Dog"]}},
        ]
        from_text = _logical_changes_from_text(text_without_detail)
        from_records = _logical_changes_from_records(records_with_detail)
        assert from_text != from_records, (
            "invariant is VACUOUS: it cannot see applied_to_subclasses divergence"
        )

    def test_contract7_invariant_non_vacuous_multiplicity_divergence_fails(self):
        """A duplicate logical change in one format only must FAIL the Counter
        invariant (set-based comparison was multiplicity-blind, round-3 N1)."""
        text_two = (
            "Schema changes (1):\n"
            "~ K [reordered]\n"
            "  properties reordered: [a, b]\n"
            "~ K [reordered]\n"
            "  properties reordered: [a, b]\n"
        )
        records_one = [{"target": "K", "kind": "class", "op": "reordered",
                        "detail": {"order": ["a", "b"]}}]
        from_text = _logical_changes_from_text(text_two)
        from_records = _logical_changes_from_records(records_one)
        assert from_text != from_records, "invariant is multiplicity-blind"

    def test_contract7_invariant_holds_on_rich_session(self):
        """The invariant PASSES (in both directions, with detail + multiplicity)
        on a real rich session — non-vacuous AND correct."""
        s = self._hierarchy()
        s.create_property("Animal", "tail", DATATYPE.TEXT, apply_to_subclasses=True)
        s.create_class("Spare")
        s.delete_class("Spare")
        s.create_property("Dog", "breed", DATATYPE.TEXT)
        agreed = TestReviewFindings2()._assert_formats_agree(s)
        # Detail signature is actually present (non-empty) for the cascade entry.
        assert any(sig for (_t, _o, sig) in agreed), agreed

    # ==================================================================
    # CONTRACT 8 — performance / NFR: no O(n^2)
    # ==================================================================

    def test_contract8_report_linear_in_class_count_at_fixed_L(self):
        """Scale C (classes/subclasses) at FIXED op-log length L; report time
        must stay roughly linear — no O(L*C) table.  Generous, non-flaky bounds.
        """
        import time

        def build_and_time(n_subclasses: int) -> float:
            s = self._empty()
            s.create_class("Animal")
            for i in range(n_subclasses):
                s.create_subclass(f"S{i}", "d", "Animal")
            s = self._from(s)
            # FIXED L: a small constant number of cascade ops regardless of C.
            for k in range(5):
                s.create_property("Animal", f"p{k}", DATATYPE.TEXT,
                                  apply_to_subclasses=True)
            start = time.perf_counter()
            for _ in range(3):
                s.change_report("records")
            return time.perf_counter() - start

        small = build_and_time(50)
        large = build_and_time(400)   # 8x the classes, L unchanged
        # Linear would predict ~8x; quadratic-in-C (the relocated defect) would
        # predict ~64x.  Allow a very generous 20x ceiling to avoid flakiness.
        assert large < small * 20 + 0.5, (
            f"super-linear in C: small={small:.4f}s large={large:.4f}s "
            f"ratio={large / max(small, 1e-6):.1f}x — possible O(L*C) regression"
        )

    def test_contract8_report_linear_in_oplog_length_at_fixed_C(self):
        """Scale L (op-log length) at FIXED class count C; report time must stay
        roughly linear in L."""
        import time

        def build_and_time(n_ops: int) -> float:
            s = self._empty()
            s.create_class("Animal")
            s.create_subclass("Dog", "d", "Animal")   # FIXED small C
            s = self._from(s)
            for k in range(n_ops):
                # Each op touches the cascade path (the historically-quadratic one).
                s.create_property("Animal", f"p{k}", DATATYPE.TEXT,
                                  apply_to_subclasses=True)
            start = time.perf_counter()
            for _ in range(3):
                s.change_report("records")
            return time.perf_counter() - start

        small = build_and_time(100)
        large = build_and_time(800)   # 8x the ops, C unchanged
        assert large < small * 20 + 0.5, (
            f"super-linear in L: small={small:.4f}s large={large:.4f}s "
            f"ratio={large / max(small, 1e-6):.1f}x"
        )

    # ==================================================================
    # CONTRACT 10 — hygiene: name-less dicts, metadata allow-list, total order
    # ==================================================================

    def test_contract10_nameless_class_dict_does_not_crash(self):
        s = self._empty()
        s.create_class("A")
        s = self._from(s)
        # Untracked to_dict edit producing a name-less class dict.
        s.to_dict()["classes"].append({"type": "Class", "properties": []})
        # Must not KeyError; degrades gracefully (name-less entry skipped).
        recs = s.change_report("records")
        text = s.change_report("text")
        assert isinstance(recs, list) and isinstance(text, str)

    def test_contract10_metadata_allowlist_excludes_server_keys(self):
        s = self._empty()
        s.create_class("A")
        s = self._from(s)
        d = s.to_dict()
        d["guid"] = "server-assigned-guid"      # server-internal
        d["@context"] = {"x": "y"}
        recs = s.change_report("records")
        targets = {r["target"] for r in recs}
        assert "schema.guid" not in targets, recs
        assert "schema.@context" not in targets, recs

    def test_contract10_sort_is_total_and_deterministic(self):
        """Identical mutation sequences yield byte-identical reports (total order)."""
        def run() -> str:
            s = self._empty()
            s.create_class("Zeta")
            s.create_class("Alpha")
            s.create_subclass("Beta", "b", "Alpha")
            s = self._from(s)
            s.delete_class("Zeta")
            s.update_class("Alpha", new_name="Aleph")
            s.create_property("Aleph", "x", DATATYPE.TEXT, apply_to_subclasses=True)
            return s.change_report("text")
        a, b = run(), run()
        assert a == b, f"non-deterministic:\n{a}\n---\n{b}"

    # ==================================================================
    # CONTRACT 11 — cross-format invariant TOTAL over all FIVE detail
    # dimensions, each PROVEN NON-VACUOUS (round-4 B1).
    #
    # For each dimension we inject a text rendering and a records rendering
    # that DISAGREE only on that dimension and assert the invariant FAILS.
    # If the invariant were blind to the dimension (as it was on order /
    # parent / inherited, and on the label_property VALUE), these would
    # PASS vacuously — so a failing assertion here is the proof of coverage.
    # The mutation-driven positive direction (both renderers agree) is
    # exercised by test_contract11_all_dimensions_agree_on_real_session.
    # ==================================================================

    @staticmethod
    def _diverges(text: str, records: list[dict]) -> bool:
        """True iff the invariant SEES a divergence between the two renderings."""
        return (
            _logical_changes_from_text(text)
            != _logical_changes_from_records(records)
        )

    def test_contract11_nonvacuous_applied_to_subclasses(self):
        """order/membership of applied_to_subclasses must be compared in full."""
        text = (
            "Schema changes (1):\n"
            "~ Animal [modified]\n"
            "  + tail [added]\n"
            '    applied to subclasses: ["Dog", "Cat"]\n'
        )
        records = [
            {"target": "Animal.tail", "kind": "property", "op": "added",
             "detail": {"applied_to_subclasses": ["Dog", "Puppy"]}},
        ]
        assert self._diverges(text, records), (
            "invariant is VACUOUS on applied_to_subclasses content"
        )

    def test_contract11_nonvacuous_applied_with_delimiter_in_name(self):
        """A subclass name containing ', ' must NOT be mis-split (JSON-encoded):
        ["A, B"] (one member) vs ["A","B"] (two) must DIVERGE."""
        text = (
            "Schema changes (1):\n"
            "~ Animal [modified]\n"
            "  + tail [added]\n"
            '    applied to subclasses: ["A, B"]\n'
        )
        records = [
            {"target": "Animal.tail", "kind": "property", "op": "added",
             "detail": {"applied_to_subclasses": ["A", "B"]}},
        ]
        assert self._diverges(text, records), (
            "invariant naively splits applied_to_subclasses on ', '"
        )

    def test_contract11_nonvacuous_label_property_value(self):
        """label_property VALUE (not just presence) must be compared."""
        text = (
            "Schema changes (1):\n"
            "~ Animal [modified]\n"
            "  ~ label\n"
            '    designated label property: "label"\n'
        )
        records = [
            {"target": "Animal.label", "kind": "property", "op": "modified",
             "detail": {"label_property": "DIFFERENT_PROP"}},
        ]
        assert self._diverges(text, records), (
            "invariant flattens label_property to a content-free boolean"
        )

    def test_contract11_nonvacuous_order(self):
        """reorder ORDER must be compared order-preservingly: [a,b,c] vs [c,b,a]."""
        text = (
            "Schema changes (1):\n"
            "~ Cls [reordered]\n"
            "  properties reordered: ['c', 'b', 'a']\n"
            '  reorder sequence: ["c", "b", "a"]\n'
        )
        records = [
            {"target": "Cls", "kind": "class", "op": "reordered",
             "detail": {"order": ["a", "b", "c"]}},
        ]
        assert self._diverges(text, records), (
            "invariant is VACUOUS on reorder order"
        )

    def test_contract11_nonvacuous_parent(self):
        """subclass_created PARENT must be compared: Base vs WRONGPARENT."""
        text = (
            "Schema changes (1):\n"
            "+ Leaf [new subclass of WRONGPARENT] (+5 inherited)\n"
            '  subclass of: "WRONGPARENT"\n'
            "  inherited count: 5\n"
        )
        records = [
            {"target": "Leaf", "kind": "class", "op": "subclass_created",
             "detail": {"parent": "Base", "inherited": 5}},
        ]
        assert self._diverges(text, records), (
            "invariant is VACUOUS on subclass parent"
        )

    def test_contract11_nonvacuous_inherited(self):
        """subclass_created INHERITED count must be compared: 5 vs 999."""
        text = (
            "Schema changes (1):\n"
            "+ Leaf [new subclass of Base] (+999 inherited)\n"
            '  subclass of: "Base"\n'
            "  inherited count: 999\n"
        )
        records = [
            {"target": "Leaf", "kind": "class", "op": "subclass_created",
             "detail": {"parent": "Base", "inherited": 5}},
        ]
        assert self._diverges(text, records), (
            "invariant is VACUOUS on inherited count"
        )

    def test_contract11_all_dimensions_agree_on_real_session(self):
        """Positive direction: a real session exercising ALL FIVE dimensions
        produces text and records the (now total) invariant agrees on, and the
        agreed signature is non-empty for every dimension-bearing entry."""
        # Baseline with existing props so designate-label + reorder are genuine
        # modifications (not collapsed into a fresh-add) — exercises all five
        # dimensions in one session.
        s = self._empty()
        s.create_class("Animal")
        s.create_subclass("Dog", "d", "Animal")
        s.create_property("Animal", "nickname", DATATYPE.TEXT)
        s = self._from(s)
        s.create_property("Animal", "tail", DATATYPE.TEXT, apply_to_subclasses=True)  # applied
        s.assign_label_property("Animal", "nickname", is_lang_string=True)            # label value
        s.assign_property_orders({"Animal": ["nickname", "label"]})                   # order
        s.create_subclass("Cat", "c", "Animal")                                       # parent + inherited
        agreed = TestReviewFindings2()._assert_formats_agree(s)
        # Every detail dimension is represented by a non-empty signature item.
        keys = {k for (_t, _o, sig) in agreed for (k, _v) in sig}
        assert {"applied", "label", "order", "parent", "inherited"} <= keys, agreed

    def test_records_carries_full_field_content_for_newline_description(self):
        """`fmt="records"` (the SUPPORTED, guaranteed output) carries the full
        field content faithfully even for a description containing a newline.

        This is the positive, genuinely-held guarantee that complements the
        best-effort text limitation below: records never loses or mangles the
        user-supplied content.
        """
        s = self._empty()
        s.create_class("Animal")
        s = self._from(s)
        desc = "An animal.\nUsed for: tracking pets -> owners"
        s.assign_class_description("Animal", desc)

        recs = s.change_report("records")
        animal = next(r for r in recs if r["target"] == "Animal")
        assert animal["op"] == "modified", animal
        field = next(f for f in animal["fields"] if f["field"] == "description")
        assert field["after"] == desc, field   # full content, byte-for-byte

    @pytest.mark.xfail(
        reason="Known limitation: fmt='text' is a best-effort human rendering "
               "that does NOT round-trip user field content containing newlines "
               "(a description newline injects a phantom field line). "
               "fmt='records' is the supported, guaranteed output. See "
               "change_report's 'Supported surface' note.",
        strict=True,
    )
    def test_text_fields_content_divergence_is_a_known_limitation(self):
        """A description containing a newline diverges between text and records:
        the text renderer spills the second line as a phantom field line, so the
        published text changelog does NOT faithfully carry the field content that
        records does.  We assert (and xfail) the IDEAL we do not hold — that the
        text rendering preserves the description on a single, unambiguous line.

        Asserted xfail(strict): the divergence is real today (the assertion
        fails), so xfail PASSES.  If a future render-from-one-model redesign makes
        the text format round-trip field content, this flips to XPASS(strict) and
        forces us to promote the guarantee (and the module-level scope note).
        This documents the limitation in executable form rather than pretending
        the (content-blind) cross-format invariant proves parity.
        """
        s = self._empty()
        s.create_class("Animal")
        s = self._from(s)
        desc = "An animal.\nUsed for: tracking pets -> owners"
        s.assign_class_description("Animal", desc)

        records = s.change_report("records")
        animal = next(r for r in records if r["target"] == "Animal")
        after = next(f["after"] for f in animal["fields"]
                     if f["field"] == "description")

        # The IDEAL we do NOT hold: the single rendered `description:` line
        # carries the COMPLETE field value, so a line-oriented consumer can
        # recover exactly what records carries.  Today the newline spills the
        # remainder onto a phantom line, so the `description:` line carries only
        # the first physical line — this assertion FAILS (hence xfail).
        text = s.change_report("text")
        desc_line = next(ln.strip() for ln in text.splitlines()
                         if ln.strip().startswith("description:"))
        rendered_value = desc_line.split("->", 1)[1].strip()
        assert rendered_value == after, (
            "text 'description:' line does not carry the full field value "
            f"(got {rendered_value!r}, records has {after!r})"
        )

    # ==================================================================
    # CONTRACT 12 — `inherited` reflects creation-time inheritance; a
    # property added to a subclass AFTER create_subclass surfaces as its
    # own `added` record (round-4 B2).  Both formats.
    # ==================================================================

    def test_contract12_post_creation_property_surfaces_as_added_both_formats(self):
        """create_subclass inherits 1 prop (label); a property added AFTERWARDS
        must (a) NOT inflate `inherited`, (b) appear as its own `added` record."""
        s = self._empty()
        s.create_class("Animal")
        s = self._from(s)
        s.create_subclass("Dog", "d", "Animal")
        s.create_property("Dog", "breed", DATATYPE.TEXT)   # AFTER create_subclass

        recs = s.change_report("records")
        dog = next(r for r in recs if r["target"] == "Dog")
        assert dog["op"] == "subclass_created", dog
        # Only the inherited-at-creation property (label) is counted; breed is NOT.
        assert dog["detail"]["inherited"] == 1, dog
        # breed surfaces as its own added record (not absorbed into the count).
        breed = [r for r in recs if r["target"] == "Dog.breed"]
        assert len(breed) == 1 and breed[0]["op"] == "added", recs

        text = s.change_report("text")
        assert "+ Dog [new subclass of Animal] (+1 inherited)" in text, text
        assert "inherited count: 1" in text, text
        assert "+ breed [added]" in text, text

        TestReviewFindings2()._assert_formats_agree(s)

    def test_contract12_inherited_count_is_creation_time_not_report_time(self):
        """A parent with 2 inherited props; subclass then gets 2 MORE added.
        `inherited` stays 2 (creation-time), the 2 adds surface individually —
        report-time len() would wrongly say 4."""
        s = self._empty()
        s.create_class("Animal")
        s.create_property("Animal", "age", DATATYPE.INTEGER)
        s = self._from(s)
        s.create_subclass("Dog", "d", "Animal")   # inherits label + age = 2
        s.create_property("Dog", "breed", DATATYPE.TEXT)
        s.create_property("Dog", "weight", DATATYPE.INTEGER)

        recs = s.change_report("records")
        dog = next(r for r in recs if r["target"] == "Dog")
        assert dog["detail"]["inherited"] == 2, dog   # NOT 4
        added = {r["target"] for r in recs if r["op"] == "added"
                 and r["target"].startswith("Dog.")}
        assert added == {"Dog.breed", "Dog.weight"}, recs

        text = s.change_report("text")
        assert "(+2 inherited)" in text, text
        assert "inherited count: 2" in text, text

        TestReviewFindings2()._assert_formats_agree(s)

    def test_contract12_inherited_only_no_post_creation_add_both_formats(self):
        """Negative case: a pure subclass with NO post-creation adds reports the
        full inherited count and emits no spurious property `added` records."""
        s = self._empty()
        s.create_class("Animal")
        s.create_property("Animal", "age", DATATYPE.INTEGER)
        s = self._from(s)
        s.create_subclass("Dog", "d", "Animal")   # inherits label + age = 2

        recs = s.change_report("records")
        dog = next(r for r in recs if r["target"] == "Dog")
        assert dog["detail"]["inherited"] == 2, dog
        assert not [r for r in recs if r["target"].startswith("Dog.")], recs

        text = s.change_report("text")
        assert "(+2 inherited)" in text, text
        assert "+ " not in text.split("\n", 2)[-1].replace(
            "+ Dog [new subclass of Animal] (+2 inherited)", ""
        ) or "Dog." not in text, text  # no stray property add lines

        TestReviewFindings2()._assert_formats_agree(s)

    def test_contract12_inherited_prop_deleted_after_creation_not_counted(self):
        """An inherited prop DELETED from the subclass after creation is no longer
        counted as inherited (net-effect, creation-time set intersected w/ live)."""
        s = self._empty()
        s.create_class("Animal")
        s.create_property("Animal", "age", DATATYPE.INTEGER)
        s = self._from(s)
        s.create_subclass("Dog", "d", "Animal")   # inherits label + age = 2
        s.delete_property("Dog", "age")           # drop one inherited prop

        recs = s.change_report("records")
        dog = next(r for r in recs if r["target"] == "Dog")
        assert dog["detail"]["inherited"] == 1, dog   # only label survives

        TestReviewFindings2()._assert_formats_agree(s)

    # ==================================================================
    # CONTRACT 13 — apply_to_subclasses cascade: O(descendants) not O(C^2),
    # no RecursionError on deep chains, ATOMIC on mid-cascade failure
    # (round-4 B3/B4).
    # ==================================================================

    def _wide_tree(self, n_children: int) -> DatagraphsSchema:
        """A parent 'Animal' with *n_children* direct subclasses, re-baselined."""
        s = self._empty()
        s.create_class("Animal")
        for i in range(n_children):
            s.create_subclass(f"S{i}", "d", "Animal")
        return self._from(s)

    def _deep_chain(self, depth: int) -> DatagraphsSchema:
        """A linear subClassOf chain C0 -> C1 -> ... of *depth* links, re-baselined."""
        s = self._empty()
        s.create_class("C0")
        for i in range(1, depth + 1):
            s.create_subclass(f"C{i}", "d", f"C{i - 1}")
        return self._from(s)

    def test_contract13_mutation_linear_in_subclass_count(self):
        """A single apply_to_subclasses create over a WIDE tree must scale ~linearly
        in the subclass count C, not O(C^2) (the relocated op-time quadratic B3)."""
        import time

        def time_create(n: int) -> float:
            s = self._wide_tree(n)
            start = time.perf_counter()
            s.create_property("Animal", "p", DATATYPE.TEXT, apply_to_subclasses=True)
            return time.perf_counter() - start

        small = time_create(200)
        large = time_create(1600)   # 8x the classes
        # Linear predicts ~8x; O(C^2) predicts ~64x.  Generous 20x ceiling.
        assert large < small * 20 + 0.5, (
            f"super-linear (O(C^2)?) op-time cascade: small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )

    def test_contract13_deep_chain_no_recursion_error(self):
        """A 2000-deep subClassOf chain must cascade WITHOUT RecursionError
        (iterative traversal, FIX B4) and reach EVERY descendant."""
        depth = 2000
        s = self._deep_chain(depth)
        # Must not raise RecursionError.
        s.create_property("C0", "deepprop", DATATYPE.TEXT, apply_to_subclasses=True)
        # Every class in the chain received the property (full transitive reach).
        for cls in s.to_dict()["classes"]:
            names = {p["name"] for p in cls["properties"]}
            assert "deepprop" in names, f"{cls['name']} missing deepprop"

    def test_contract13_deep_chain_update_no_recursion_error(self):
        """update_property cascade is also iterative — no RecursionError at depth."""
        depth = 2000
        s = self._deep_chain(depth)
        s.create_property("C0", "deepprop", DATATYPE.TEXT, apply_to_subclasses=True)
        s = self._from(s)
        s.update_property("C0", "deepprop", is_optional=False, apply_to_subclasses=True)
        for cls in s.to_dict()["classes"]:
            prop = next(p for p in cls["properties"] if p["name"] == "deepprop")
            assert prop["isOptional"] is False, cls["name"]

    def test_contract13_create_cascade_atomic_on_midchain_conflict(self):
        """A mid-cascade PropertyExistsError must leave the schema UNCHANGED
        (all-or-nothing pre-validation, FIX B4) — no partial write."""
        s = self._empty()
        s.create_class("A")
        s.create_subclass("B", "d", "A")
        s.create_subclass("C", "d", "B")
        # Pre-existing conflicting property on a mid-chain descendant.
        s.create_property("C", "foo", DATATYPE.TEXT)
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(PropertyExistsError):
            s.create_property("A", "foo", DATATYPE.TEXT, apply_to_subclasses=True)

        # Atomicity: nothing was written — not even to A or B (which precede C).
        assert s.to_dict() == before, "partial write on mid-cascade conflict"
        # The op recorded nothing (it raised before _record).
        assert s.change_report("records") == [], s.change_report("records")

    def test_contract13_update_cascade_atomic_on_missing_descendant_prop(self):
        """A mid-cascade PropertyNotFoundError on update must leave the schema
        UNCHANGED (pre-validation, FIX B4)."""
        s = self._empty()
        s.create_class("A")
        s.create_property("A", "foo", DATATYPE.TEXT, is_optional=True)
        s.create_subclass("B", "d", "A")   # inherits foo
        s.create_subclass("C", "d", "B")   # inherits foo
        s = self._from(s)
        # Remove foo from C so the cascade update will hit a missing prop at C.
        s.delete_property("C", "foo")
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(PropertyNotFoundError):
            s.update_property("A", "foo", is_optional=False, apply_to_subclasses=True)

        # A and B must NOT have been mutated despite preceding C in the walk.
        a_foo = next(p for p in s.find_class("A")["properties"] if p["name"] == "foo")
        b_foo = next(p for p in s.find_class("B")["properties"] if p["name"] == "foo")
        assert a_foo["isOptional"] is True, "partial write to A"
        assert b_foo["isOptional"] is True, "partial write to B"
        assert s.to_dict() == before, "partial write on mid-cascade conflict"

    def test_contract13_report_linear_in_oplog_at_large_fixed_C(self):
        """Report path: scale L cascade ops at a LARGE fixed C; report stays
        ~linear in L (memoised resolution, FIX B2 — no O(L*C) re-resolution)."""
        import time

        def build_and_time(n_ops: int) -> float:
            s = self._wide_tree(400)   # large fixed C
            for k in range(n_ops):
                s.create_property("Animal", f"p{k}", DATATYPE.TEXT,
                                  apply_to_subclasses=True)
            start = time.perf_counter()
            for _ in range(3):
                s.change_report("records")
            return time.perf_counter() - start

        small = build_and_time(20)
        large = build_and_time(160)   # 8x ops, C fixed
        assert large < small * 20 + 0.5, (
            f"super-linear in L at fixed C (O(L*C)?): small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )


class TestAtomicity:
    """Bug-first regression tests for the property-create/update partial-write.

    The atomic *pre-validation* (round-4 B4) only checked existence / duplicate /
    missing-class up front.  Every error the apply loop raises AFTER that —
    ``InvalidInversePropertyError``, a missing object-range ``ClassNotFoundError``,
    an enum/datatype error — fired *mid-apply*, after earlier cascade targets (and
    the current class's half-built property dict) were already mutated.  Because
    ``_record`` runs only on success, ``change_report`` then OMITTED the failed op
    while the schema was IS half-changed — a partial write with a lying audit trail.

    The fix is snapshot/rollback at the outermost public boundary: on ANY exception
    the schema is restored byte-for-byte, so create/update are genuinely
    all-or-nothing.  These tests therefore assert, after a raise, that:

      * ``to_dict()`` is byte-identical to the pre-call state (no partial write,
        not even a half-built property dict), AND
      * ``change_report`` records NOTHING for the failed op — in BOTH formats
        (the report must not lie about an op the caller saw raise).

    See .sdlc/reviews/schema-change-tracking-uncommitted-5/ (consequences B1 /
    assumptions B2 / maintainability B-HIGH) for the exact reproductions.
    """

    def _empty(self) -> DatagraphsSchema:
        return DatagraphsSchema(name="T", version="1.0")

    def _from(self, s: DatagraphsSchema) -> DatagraphsSchema:
        return DatagraphsSchema.create_from(copy.deepcopy(s.to_dict()))

    def _assert_unchanged_and_silent(self, s: DatagraphsSchema, before: dict) -> None:
        """The schema is byte-identical to *before* and the report is empty in
        BOTH formats — proving no partial write and no lying audit trail."""
        assert s.to_dict() == before, "partial write — schema mutated despite raise"
        assert s.change_report("records") == [], (
            f"report lies: surfaces a change for a raised op:\n"
            f"{s.change_report('records')}"
        )
        assert s.change_report("text") == "", (
            f"text report lies about a raised op:\n{s.change_report('text')}"
        )

    # ------------------------------------------------------------------
    # Fixtures producing a cascade whose inverse_of is valid for the PARENT
    # but invalid for a later subclass (the backref.range == "Parent" only).
    # ------------------------------------------------------------------

    def _cascade_with_subclass(self) -> DatagraphsSchema:
        """Parent -> Child cascade plus a Target class carrying a backref whose
        range is 'Parent', so an inverse_of cascade passes for Parent but raises
        InvalidInversePropertyError for Child."""
        s = self._empty()
        s.create_class("Parent")
        s.create_subclass("Child", "d", "Parent")
        s.create_class("Target")
        s.create_property("Target", "backref", "Parent")  # range == "Parent"
        return self._from(s)

    # ==================================================================
    # CASCADE create_property — mid-apply InvalidInversePropertyError
    # ==================================================================

    def test_create_cascade_inverse_of_invalid_for_subclass_is_atomic(self):
        s = self._cascade_with_subclass()
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.create_property("Parent", "rel", "Target",
                              inverse_of="backref", apply_to_subclasses=True)

        self._assert_unchanged_and_silent(s, before)
        # No half-built property dict on Parent OR Child.
        for cls in ("Parent", "Child"):
            names = {p["name"] for p in s.find_class(cls)["properties"]}
            assert "rel" not in names, f"{cls} carries a half-built 'rel'"

    # ==================================================================
    # CASCADE update_property — mid-apply InvalidInversePropertyError
    # ==================================================================

    def test_update_cascade_inverse_of_invalid_for_subclass_is_atomic(self):
        s = self._cascade_with_subclass()
        # A valid object property to UPDATE (no inverse yet) on Parent + Child.
        s.create_property("Parent", "rel", "Target", apply_to_subclasses=True)
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.update_property("Parent", "rel", description="NEW DESC",
                              inverse_of="backref", apply_to_subclasses=True)

        self._assert_unchanged_and_silent(s, before)
        # Neither Parent.rel nor Child.rel gained the new description or inverseOf.
        for cls in ("Parent", "Child"):
            rel = next(p for p in s.find_class(cls)["properties"]
                       if p["name"] == "rel")
            assert "description" not in rel, f"{cls}.rel got NEW DESC (partial)"
            assert "inverseOf" not in rel, f"{cls}.rel got inverseOf (partial)"

    # ==================================================================
    # CASCADE create_property — mid-apply missing object-range
    # (ClassNotFoundError fires inside _assign_datatype, after append)
    # ==================================================================

    def test_create_cascade_missing_object_range_is_atomic(self):
        s = self._empty()
        s.create_class("Parent")
        s.create_subclass("Child", "d", "Parent")
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(ClassNotFoundError):
            s.create_property("Parent", "rel", "NoSuchClass",
                              apply_to_subclasses=True)

        self._assert_unchanged_and_silent(s, before)
        for cls in ("Parent", "Child"):
            names = {p["name"] for p in s.find_class(cls)["properties"]}
            assert "rel" not in names, f"{cls} carries a half-built 'rel'"

    # ==================================================================
    # SINGLE-CLASS (no cascade) create_property — mid-apply raise leaves
    # NO half-built property and records nothing.
    # ==================================================================

    def test_single_class_create_inverse_of_raise_is_atomic(self):
        s = self._empty()
        s.create_class("Target")
        s.create_class("Owner")
        # backref.range is NOT "Owner", so inverse_of validation raises.
        s.create_property("Target", "backref", "Owner")
        # Make backref point at the wrong class so the inverse is invalid for Owner.
        s.update_property("Target", "backref", datatype="Target")
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.create_property("Owner", "rel", "Target", inverse_of="backref")

        self._assert_unchanged_and_silent(s, before)
        # The half-built property must NOT linger on Owner.
        names = {p["name"] for p in s.find_class("Owner")["properties"]}
        assert "rel" not in names, "single-class partial write: 'rel' lingers"

    def test_single_class_create_missing_object_range_is_atomic(self):
        s = self._empty()
        s.create_class("Owner")
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(ClassNotFoundError):
            s.create_property("Owner", "rel", "NoSuchClass")

        self._assert_unchanged_and_silent(s, before)
        names = {p["name"] for p in s.find_class("Owner")["properties"]}
        assert "rel" not in names, "single-class partial write: 'rel' lingers"

    # ==================================================================
    # SINGLE-CLASS update_property — mid-apply raise is atomic.
    # ==================================================================

    def test_single_class_update_inverse_of_raise_is_atomic(self):
        s = self._empty()
        s.create_class("Target")
        s.create_class("Owner")
        s.create_property("Target", "backref", "Target")  # range is Target, not Owner
        s.create_property("Owner", "rel", "Target")        # object prop to update
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.update_property("Owner", "rel", description="NEW DESC",
                              inverse_of="backref")

        self._assert_unchanged_and_silent(s, before)
        rel = next(p for p in s.find_class("Owner")["properties"]
                   if p["name"] == "rel")
        assert "description" not in rel, "single-class partial write: description set"
        assert "inverseOf" not in rel, "single-class partial write: inverseOf set"

    # ==================================================================
    # create_subclass — a mid-cascade raise (a parent property whose
    # inverse_of is valid for the parent but NOT for the new subclass)
    # must leave NO half-built subclass: the compound create_class +
    # per-property loop is a single all-or-nothing op.
    # ==================================================================

    def _subclass_with_invalid_inherited_inverse(self) -> DatagraphsSchema:
        """Parent carries an object property 'rel' whose inverseOf 'backref'
        (on Target) has range 'Parent'.  Copying 'rel' onto a NEW subclass
        re-validates inverseOf against the subclass name and raises, so
        create_subclass fails mid-loop AFTER create_class already ran."""
        s = self._empty()
        s.create_class("Parent")
        s.create_class("Target")
        s.create_property("Target", "backref", "Parent")  # backref.range == "Parent"
        s.create_property("Parent", "rel", "Target", inverse_of="backref")  # valid for Parent
        return self._from(s)

    def test_create_subclass_midloop_raise_is_atomic(self):
        s = self._subclass_with_invalid_inherited_inverse()
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(InvalidInversePropertyError):
            s.create_subclass("Child", "a child", "Parent")

        self._assert_unchanged_and_silent(s, before)
        # The half-built subclass (created by the inner create_class before the
        # loop raised) must NOT linger — not even its default label property.
        assert s.find_class("Child") is None, "half-built subclass 'Child' lingers"

    # ==================================================================
    # assign_label_property — designating a NON-EXISTENT property must
    # raise WITHOUT first corrupting class.labelProperty (it set the
    # name before validating the property exists).
    # ==================================================================

    def test_assign_label_property_nonexistent_is_atomic(self):
        s = self._empty()
        s.create_class("C")  # default label property is "label"
        s = self._from(s)
        before = copy.deepcopy(s.to_dict())
        original_label = s.find_class("C")["labelProperty"]

        with pytest.raises(PropertyNotFoundError):
            s.assign_label_property("C", "nonexistent")

        self._assert_unchanged_and_silent(s, before)
        # labelProperty must be untouched — not corrupted to the non-existent name
        # (which would be serialisable to the backend via to_dict()/apply_schema).
        assert s.find_class("C")["labelProperty"] == original_label, (
            "labelProperty corrupted to a non-existent property name"
        )

    # ==================================================================
    # In-place rollback restore: an externally-held reference to the
    # classes list (e.g. obtained via the public `classes` view) must
    # remain consistent with the schema after a rolled-back mutation —
    # the snapshot is restored into the SAME list object, not rebound.
    # ==================================================================

    def test_rollback_keeps_external_classes_reference_consistent(self):
        s = self._empty()
        s.create_class("C")
        s = self._from(s)
        external_ref = s.classes  # caller holds the live list
        before = copy.deepcopy(s.to_dict())

        with pytest.raises(PropertyNotFoundError):
            s.assign_label_property("C", "nonexistent")

        # The externally-held reference still IS the schema's class list and
        # reflects the rolled-back (unchanged) state.
        assert external_ref is s.classes, "rollback rebound the classes list object"
        assert s.to_dict()["classes"] == before["classes"]


class TestMutationPerformanceScaling:
    """Regression tests for the atomic-rollback snapshot cost (perf bug).

    The all-or-nothing ``_atomic`` guard originally deep-copied the ENTIRE class
    list on every outermost mutation.  That made two common workloads quadratic:

      * **Narrow construction.**  Building an N-class schema one ``create_class`` /
        ``create_subclass`` at a time deep-copied the whole (growing) class list
        per call — O(N²).  An 8x larger schema took ~64x longer, not ~8x.
      * **Accumulated wide cascades.**  A sequence of L ``create_property`` /
        ``update_property`` calls with ``apply_to_subclasses=True`` at fixed
        subclass-count C deep-copied every class (each carrying the properties
        added by prior calls) on every call — O(L²·C).  8x the calls took ~55x
        longer.

    The fix scopes rollback state to each operation's footprint: a shallow class-
    list snapshot for structure plus a property-granular undo journal for the
    cascade hot path (one O(1) entry per appended/updated property instead of a
    whole-class deep copy).  Construction becomes O(N) and accumulated cascades
    O(L·C).

    These tests time an 8x size step and assert the ratio stays well under the
    quadratic prediction.  Pre-fix they FAIL (~64x / ~55x ≫ the 24x ceiling);
    post-fix they pass comfortably (~8x).  The 24x ceiling sits far from both the
    linear (~8x) and quadratic (~64x) predictions, so the test discriminates the
    asymptotic class while tolerating timing noise.
    """

    def _empty(self) -> DatagraphsSchema:
        return DatagraphsSchema(name="T", version="1.0")

    def test_narrow_construction_is_subquadratic_in_class_count(self):
        """Building N classes one at a time must scale ~linearly in N, not O(N²)
        (the whole-list deep-copy-per-mutation snapshot bug)."""
        import time

        def time_build(n: int) -> float:
            start = time.perf_counter()
            s = self._empty()
            s.create_class("Root")
            for i in range(n):
                s.create_subclass(f"C{i}", "d", "Root")
            return time.perf_counter() - start

        small = time_build(200)
        large = time_build(1600)  # 8x the classes
        assert large < small * 24 + 0.2, (
            f"super-linear (O(N^2)?) construction: small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )

    def _wide_tree(self, n_children: int) -> DatagraphsSchema:
        s = self._empty()
        s.create_class("Animal")
        for i in range(n_children):
            s.create_subclass(f"S{i}", "d", "Animal")
        return s

    def test_repeated_wide_create_cascade_is_subquadratic_in_op_count(self):
        """A sequence of L wide create-cascades at fixed C must scale ~linearly in
        L, not O(L²·C) (per-call whole-class deep copy of property-heavy classes)."""
        import time

        def time_cascades(n_ops: int) -> float:
            s = self._wide_tree(200)  # fixed C
            start = time.perf_counter()
            for k in range(n_ops):
                s.create_property("Animal", f"p{k}", DATATYPE.TEXT,
                                  apply_to_subclasses=True)
            return time.perf_counter() - start

        small = time_cascades(15)
        large = time_cascades(120)  # 8x the ops, C fixed
        assert large < small * 24 + 0.2, (
            f"super-linear (O(L^2*C)?) accumulated create-cascade: small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )

    def test_repeated_wide_update_cascade_is_subquadratic_in_op_count(self):
        """A sequence of L wide update-cascades at fixed C must scale ~linearly in
        L, not O(L²·C) — the update hot path journals one property per target,
        not the whole class dict."""
        import time

        def time_updates(n_ops: int) -> float:
            s = self._wide_tree(200)  # fixed C
            # Seed properties to update, OUTSIDE the timed region.
            for k in range(n_ops):
                s.create_property("Animal", f"p{k}", DATATYPE.TEXT,
                                  apply_to_subclasses=True)
            start = time.perf_counter()
            for k in range(n_ops):
                s.update_property("Animal", f"p{k}", is_optional=False,
                                  apply_to_subclasses=True)
            return time.perf_counter() - start

        small = time_updates(15)
        large = time_updates(120)  # 8x the ops, C fixed
        assert large < small * 24 + 0.2, (
            f"super-linear (O(L^2*C)?) accumulated update-cascade: small={small:.4f}s "
            f"large={large:.4f}s ratio={large / max(small, 1e-6):.1f}x"
        )
