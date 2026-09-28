import sys

import pytest

from datagraphs.enums import DATATYPE
from datagraphs.schema import (
    ClassNotFoundError,
    InvalidInversePropertyError,
    PropertyExistsError,
    PropertyNotFoundError,
    SchemaError,
)
from datagraphs.schema import Schema as DatagraphsSchema


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


def _hierarchy(*edges: tuple[str, str]) -> DatagraphsSchema:
    """Build a schema from (child, parent) edges, creating roots as needed."""
    schema = DatagraphsSchema()
    for child, parent in edges:
        if schema.find_class(parent) is None:
            schema.create_class(parent, "description")
        schema.create_subclass(child, "description", parent)
    return schema


def _names(classes: list[dict]) -> list[str]:
    """Class names in returned order — use where order is part of the contract."""
    return [c["name"] for c in classes]


def _nameset(classes: list[dict]) -> set[str]:
    """Class names as a set — use where order is not part of the contract."""
    return {c["name"] for c in classes}


class TestSchemaFindSubclasses:
    """``find_subclasses`` over a hierarchy built through the public API.

    Order is not part of the contract, so membership is asserted as a set of names.
    """

    def test_should_find_only_direct_subclasses_by_default(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        assert _nameset(schema.find_subclasses("Animal")) == {"Mammal"}

    def test_should_find_descendants_at_every_depth_when_recursive(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        assert _nameset(schema.find_subclasses("Animal", recursive=True)) == {"Mammal", "Dog", "Puppy"}

    def test_should_find_descendants_across_every_branch_when_recursive(self):
        schema = _hierarchy(
            ("Mammal", "Animal"), ("Bird", "Animal"),
            ("Dog", "Mammal"), ("Cat", "Mammal"), ("Robin", "Bird"),
        )
        assert _nameset(schema.find_subclasses("Animal", recursive=True)) == {
            "Mammal", "Bird", "Dog", "Cat", "Robin",
        }

    def test_should_exclude_classes_outside_the_subtree(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Saloon", "Vehicle"))
        found = _nameset(schema.find_subclasses("Animal", recursive=True))
        assert found == {"Mammal", "Dog"}
        assert "Vehicle" not in found and "Saloon" not in found

    def test_should_search_from_any_node_not_just_the_root(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        assert _nameset(schema.find_subclasses("Mammal", recursive=True)) == {"Dog", "Puppy"}

    def test_should_not_include_the_baseclass_itself(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"))
        assert "Animal" not in _nameset(schema.find_subclasses("Animal", recursive=True))

    def test_should_return_empty_when_class_is_a_leaf(self):
        schema = _hierarchy(("Mammal", "Animal"))
        assert schema.find_subclasses("Mammal") == []
        assert schema.find_subclasses("Mammal", recursive=True) == []

    def test_should_return_empty_when_class_does_not_exist(self):
        schema = _hierarchy(("Mammal", "Animal"))
        assert schema.find_subclasses("Nonexistent") == []
        assert schema.find_subclasses("Nonexistent", recursive=True) == []

    def test_should_return_empty_for_an_empty_schema(self):
        assert DatagraphsSchema().find_subclasses("Anything", recursive=True) == []

    def test_should_return_each_descendant_exactly_once(self):
        schema = _hierarchy(
            ("Mammal", "Animal"), ("Bird", "Animal"), ("Dog", "Mammal"), ("Robin", "Bird"),
        )
        found = [c["name"] for c in schema.find_subclasses("Animal", recursive=True)]
        assert sorted(found) == sorted(set(found))

    def test_should_return_the_live_class_dicts(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"))
        found = schema.find_subclasses("Animal", recursive=True)
        assert all(c is schema.find_class(c["name"]) for c in found)

    def test_recursive_results_should_be_a_superset_of_direct_results(self):
        schema = _hierarchy(
            ("Mammal", "Animal"), ("Bird", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"),
        )
        direct = _nameset(schema.find_subclasses("Animal"))
        assert direct < _nameset(schema.find_subclasses("Animal", recursive=True))

    def test_should_terminate_on_a_subclassof_cycle(self):
        schema = _hierarchy(("B", "A"), ("C", "B"))
        schema.assign_baseclass("A", "C")  # A -> B -> C -> A
        assert _nameset(schema.find_subclasses("A", recursive=True)) == {"B", "C"}

    def test_should_handle_a_chain_too_deep_to_recurse(self):
        depth = sys.getrecursionlimit() * 2
        schema = _hierarchy(*((f"C{i + 1}", f"C{i}") for i in range(depth)))
        found = schema.find_subclasses("C0", recursive=True)
        assert len(found) == depth
        assert _nameset(found) == {f"C{i + 1}" for i in range(depth)}


class TestSchemaFindBaseclasses:
    """``find_baseclasses`` walks up the ``subClassOf`` chain.

    Ancestry is a chain rather than a tree, so nearest-first ordering *is* part of the
    contract here and is asserted as a list.
    """

    def test_should_find_only_the_direct_parent_by_default(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        assert _names(schema.find_baseclasses("Puppy")) == ["Dog"]

    def test_should_find_the_whole_chain_nearest_first_when_recursive(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        assert _names(schema.find_baseclasses("Puppy", recursive=True)) == ["Dog", "Mammal", "Animal"]

    def test_should_exclude_classes_outside_the_ancestry_chain(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Bird", "Animal"), ("Dog", "Mammal"), ("Robin", "Bird"))
        assert _names(schema.find_baseclasses("Dog", recursive=True)) == ["Mammal", "Animal"]

    def test_should_return_empty_for_a_root_class(self):
        schema = _hierarchy(("Mammal", "Animal"))
        assert schema.find_baseclasses("Animal") == []
        assert schema.find_baseclasses("Animal", recursive=True) == []

    def test_should_return_empty_when_class_does_not_exist(self):
        schema = _hierarchy(("Mammal", "Animal"))
        assert schema.find_baseclasses("Nonexistent") == []
        assert schema.find_baseclasses("Nonexistent", recursive=True) == []

    def test_should_return_empty_for_an_empty_schema(self):
        assert DatagraphsSchema().find_baseclasses("Anything", recursive=True) == []

    def test_should_return_empty_when_the_parent_reference_is_dangling(self):
        schema = _hierarchy(("Mammal", "Animal"))
        schema.assign_baseclass("Mammal", "Ghost")
        assert schema.find_baseclasses("Mammal") == []
        assert schema.find_baseclasses("Mammal", recursive=True) == []

    def test_should_stop_at_the_last_resolvable_ancestor(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"))
        schema.assign_baseclass("Animal", "Ghost")
        assert _names(schema.find_baseclasses("Dog", recursive=True)) == ["Mammal", "Animal"]

    def test_should_return_each_ancestor_exactly_once(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        found = _names(schema.find_baseclasses("Puppy", recursive=True))
        assert sorted(found) == sorted(set(found))

    def test_should_return_the_live_class_dicts(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"))
        found = schema.find_baseclasses("Dog", recursive=True)
        assert all(c is schema.find_class(c["name"]) for c in found)

    def test_recursive_results_should_be_a_superset_of_direct_results(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        assert _nameset(schema.find_baseclasses("Puppy")) < _nameset(schema.find_baseclasses("Puppy", recursive=True))

    def test_should_terminate_on_a_subclassof_cycle(self):
        schema = _hierarchy(("B", "A"), ("C", "B"))
        schema.assign_baseclass("A", "C")  # A -> B -> C -> A
        assert _names(schema.find_baseclasses("C", recursive=True)) == ["B", "A"]

    def test_should_handle_a_chain_too_deep_to_recurse(self):
        depth = sys.getrecursionlimit() * 2
        schema = _hierarchy(*((f"C{i + 1}", f"C{i}") for i in range(depth)))
        found = _names(schema.find_baseclasses(f"C{depth}", recursive=True))
        assert found == [f"C{i}" for i in range(depth - 1, -1, -1)]

    def test_should_agree_with_find_subclasses_on_the_same_hierarchy(self):
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"), ("Puppy", "Dog"))
        descendants = schema.find_subclasses("Animal", recursive=True)
        assert len(descendants) == 3  # guards the loop below against passing vacuously
        for descendant in descendants:
            assert "Animal" in _nameset(schema.find_baseclasses(descendant["name"], recursive=True))


class TestInverseOfRangeCovariance:
    """An inverse property may point back at the class itself or any class it descends from.

    A property whose range is a base class already accepts instances of every subclass, so
    an inverse naming an ancestor does point back. The reverse does not hold: a range that
    is a *descendant* is not satisfied by every instance of the class, and stays invalid.
    """

    def _schema(self, backref_range: str) -> DatagraphsSchema:
        """Animal <- Mammal <- Dog, plus Target.backref with the given range."""
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"))
        schema.create_class("Unrelated", "description")
        schema.create_class("Target", "description")
        schema.create_property("Target", "backref", backref_range)
        return schema

    @staticmethod
    def _inverse_of(schema: DatagraphsSchema, class_name: str, prop_name: str) -> str | None:
        prop = schema.find_property(schema.find_class(class_name)["properties"], prop_name)
        return prop.get("inverseOf")

    def test_should_accept_a_range_naming_the_class_itself(self):
        schema = self._schema("Dog")
        schema.create_property("Dog", "rel", "Target", inverse_of="backref")
        assert self._inverse_of(schema, "Dog", "rel") == "backref"

    def test_should_accept_a_range_naming_the_direct_parent(self):
        schema = self._schema("Mammal")
        schema.create_property("Dog", "rel", "Target", inverse_of="backref")
        assert self._inverse_of(schema, "Dog", "rel") == "backref"

    def test_should_accept_a_range_naming_an_indirect_ancestor(self):
        schema = self._schema("Animal")
        schema.create_property("Dog", "rel", "Target", inverse_of="backref")
        assert self._inverse_of(schema, "Dog", "rel") == "backref"

    def test_should_reject_a_range_naming_an_unrelated_class(self):
        schema = self._schema("Unrelated")
        with pytest.raises(InvalidInversePropertyError):
            schema.create_property("Dog", "rel", "Target", inverse_of="backref")

    def test_should_reject_a_range_naming_a_descendant(self):
        schema = self._schema("Dog")
        with pytest.raises(InvalidInversePropertyError):
            schema.create_property("Animal", "rel", "Target", inverse_of="backref")

    def test_should_reject_a_range_naming_a_sibling_branch(self):
        schema = self._schema("Mammal")
        schema.create_subclass("Bird", "description", "Animal")
        with pytest.raises(InvalidInversePropertyError):
            schema.create_property("Bird", "rel", "Target", inverse_of="backref")

    def test_should_not_record_the_inverse_when_the_range_is_rejected(self):
        schema = self._schema("Unrelated")
        with pytest.raises(InvalidInversePropertyError):
            schema.create_property("Dog", "rel", "Target", inverse_of="backref")
        assert schema.find_property(schema.find_class("Dog")["properties"], "rel") is None

    def test_should_still_reject_an_inverse_naming_a_missing_property(self):
        schema = self._schema("Animal")
        with pytest.raises(InvalidInversePropertyError):
            schema.create_property("Dog", "rel", "Target", inverse_of="nonexistent")

    def test_should_still_reject_an_inverse_on_a_datatype_property(self):
        schema = self._schema("Animal")
        schema.create_property("Target", "plain", DATATYPE.TEXT)
        with pytest.raises(InvalidInversePropertyError):
            schema.create_property("Dog", "rel", "Target", inverse_of="plain")

    def test_should_cascade_an_ancestor_ranged_inverse_to_every_subclass(self):
        schema = self._schema("Animal")
        schema.create_property("Animal", "rel", "Target", inverse_of="backref", apply_to_subclasses=True)
        assert [self._inverse_of(schema, c, "rel") for c in ("Animal", "Mammal", "Dog")] == ["backref"] * 3

    def test_should_accept_an_ancestor_range_through_a_long_chain(self):
        depth = sys.getrecursionlimit() * 2
        schema = _hierarchy(*((f"C{i + 1}", f"C{i}") for i in range(depth)))
        schema.create_class("Target", "description")
        schema.create_property("Target", "backref", "C0")  # range is the root of the chain
        schema.create_property(f"C{depth}", "rel", "Target", inverse_of="backref")
        assert self._inverse_of(schema, f"C{depth}", "rel") == "backref"


class TestUpdatePropertyInverseOf:
    """Adding an inverse to an *existing* property via ``update_property``.

    The datatype is optional on update, so ``inverse_of`` is validated against the
    property's existing range when the caller does not restate it.
    """

    def _schema(self, backref_range: str = "Dog") -> DatagraphsSchema:
        schema = _hierarchy(("Mammal", "Animal"), ("Dog", "Mammal"))
        schema.create_class("Unrelated", "description")
        schema.create_class("Target", "description")
        schema.create_property("Target", "backref", backref_range)
        return schema

    @staticmethod
    def _inverse_of(schema: DatagraphsSchema, class_name: str, prop_name: str) -> str | None:
        prop = schema.find_property(schema.find_class(class_name)["properties"], prop_name)
        return prop.get("inverseOf")

    def test_should_add_an_inverse_without_restating_the_datatype(self):
        schema = self._schema()
        schema.create_property("Dog", "rel", "Target")
        schema.update_property("Dog", "rel", inverse_of="backref")
        assert self._inverse_of(schema, "Dog", "rel") == "backref"

    def test_should_keep_the_existing_range_when_the_datatype_is_omitted(self):
        schema = self._schema()
        schema.create_property("Dog", "rel", "Target")
        schema.update_property("Dog", "rel", inverse_of="backref")
        prop = schema.find_property(schema.find_class("Dog")["properties"], "rel")
        assert prop["range"] == "Target"

    def test_should_accept_an_ancestor_range_on_update(self):
        schema = self._schema("Animal")
        schema.create_property("Dog", "rel", "Target")
        schema.update_property("Dog", "rel", inverse_of="backref")
        assert self._inverse_of(schema, "Dog", "rel") == "backref"

    def test_should_still_accept_an_explicitly_restated_datatype(self):
        schema = self._schema()
        schema.create_property("Dog", "rel", "Target")
        schema.update_property("Dog", "rel", datatype="Target", inverse_of="backref")
        assert self._inverse_of(schema, "Dog", "rel") == "backref"

    def test_should_reject_an_unrelated_range_on_update(self):
        schema = self._schema("Unrelated")
        schema.create_property("Dog", "rel", "Target")
        with pytest.raises(InvalidInversePropertyError, match="does not point back"):
            schema.update_property("Dog", "rel", inverse_of="backref")

    def test_should_report_a_datatype_property_as_such_not_as_a_missing_class(self):
        """The old code passed datatype=None straight through, so this raised the
        misleading 'refers to non-existent class None' instead of naming the real
        problem: an inverse cannot be set on a datatype property."""
        schema = self._schema()
        schema.create_property("Dog", "plain", DATATYPE.TEXT)
        with pytest.raises(InvalidInversePropertyError, match="concept datatype"):
            schema.update_property("Dog", "plain", inverse_of="backref")

    def test_should_cascade_an_added_inverse_to_every_subclass(self):
        schema = self._schema("Animal")
        schema.create_property("Animal", "rel", "Target", apply_to_subclasses=True)
        schema.update_property("Animal", "rel", inverse_of="backref", apply_to_subclasses=True)
        assert [self._inverse_of(schema, c, "rel") for c in ("Animal", "Mammal", "Dog")] == ["backref"] * 3

    def test_should_leave_the_property_untouched_when_the_inverse_is_rejected(self):
        schema = self._schema("Unrelated")
        schema.create_property("Dog", "rel", "Target", description="ORIGINAL")
        with pytest.raises(InvalidInversePropertyError):
            schema.update_property("Dog", "rel", description="NEW", inverse_of="backref")
        prop = schema.find_property(schema.find_class("Dog")["properties"], "rel")
        assert "inverseOf" not in prop
        assert prop["description"]["@none"] == "ORIGINAL"
