import pytest
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.schema import PropertyExistsError, InvalidInversePropertyError, SchemaError, ClassNotFoundError, PropertyNotFoundError
from datagraphs.enums import DATATYPE

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
        assert schema.classes == []

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
        self.schema.create_property("AnotherClass", "refProp", "TestClass")
        self.schema.delete_class("TestClass", include_linked_properties=True)
        another_cls = self.schema.find_class("AnotherClass")
        assert len(another_cls["objectProperties"]) == 1

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
        assert cls['objectProperties'][0]["propertyValuePattern"] == autogen_pattern

    def test_should_update_class_description(self):
        self.schema.assign_class_description("TestClass", description="New description")
        cls = self.schema.find_class("TestClass")
        assert cls["description"] == "New description"

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
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop is not None

    def test_should_create_cascading_properties_in_subclass(self):
        self.schema.create_subclass("SubClass", "description", "TestClass")
        self.schema.create_property("TestClass", "newProp", DATATYPE.INTEGER, apply_to_subclasses=True)
        cls = self.schema.find_class("SubClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop is not None

    def test_should_raise_error_on_duplicate_property(self):
        self.schema.create_property("TestClass", "dupProp", DATATYPE.TEXT)
        with pytest.raises(PropertyExistsError):
            self.schema.create_property("TestClass", "dupProp", DATATYPE.TEXT)

    def test_should_create_property_with_specified_description(self):
        desc = "A test property"
        self.schema.create_property("TestClass", "newProp", DATATYPE.INTEGER, description=desc)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop["propertyDescription"] == desc

    def test_should_create_property_with_specified_datatype(self):
        self.schema.create_property("TestClass", "newProp", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:integer"
        assert prop["propertyDatatype"]["label"] == "integer"

    def test_should_create_text_property_with_multilanguage_support(self):
        self.schema.create_property("TestClass", "newProp", DATATYPE.TEXT, is_lang_string=True)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:text"
        assert prop["propertyDatatype"]["label"] == "text"
        assert prop["isLangString"] is True 

    def test_should_create_array_property(self):
        self.schema.create_property("TestClass", "newProp", DATATYPE.TEXT, is_array=True)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop["isArray"] is True

    def test_should_create_nested_property(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("TestClass", "newProp", datatype="AnotherClass", is_nested=True)
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
        assert prop["isNestedObject"] is True

    def test_should_create_an_inverse_property(self):
        self.schema.create_class("AnotherClass")
        self.schema.create_property("AnotherClass", "prop", "TestClass")
        self.schema.create_property("TestClass", "newProp", "AnotherClass", inverse_of="prop")
        cls = self.schema.find_class("TestClass")
        prop = next((p for p in cls["objectProperties"] if p["propertyName"] == "newProp"), None)
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
        prop = self.schema.find_property(cls["objectProperties"], "enumProp")
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:enum"
        assert prop["propertyDatatype"]["label"] == "enum"
        assert prop["validationRules"][0]["value"] == enums

    def test_should_create_required_property(self):
        self.schema.create_property("TestClass", "requiredProp", DATATYPE.TEXT, is_optional=False)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "requiredProp")
        assert prop["isOptional"] is False

    def test_should_create_synonym_property(self):
        self.schema.create_property("TestClass", "synonymProp", DATATYPE.TEXT, is_synonym=True)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "synonymProp")
        assert prop["isLabelSynonym"] is True

    def test_should_create_filterable_property(self):
        self.schema.create_property("TestClass", "filterableProp", DATATYPE.TEXT, is_filterable=True)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "filterableProp")
        assert prop["isFilterable"] is True

    def test_should_assign_property_description(self):
        self.schema.create_property("TestClass", "propToDescribe", DATATYPE.INTEGER)
        self.schema.update_property("TestClass", "propToDescribe", description="This is a description")
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToDescribe")
        assert prop["propertyDescription"] == "This is a description"

    def test_should_change_property_cardinality(self):
        self.schema.create_property("TestClass", "propToChangeCardinality", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToChangeCardinality")
        assert prop["isArray"] is False 
        self.schema.update_property("TestClass", "propToChangeCardinality", is_array=True)
        prop = self.schema.find_property(cls["objectProperties"], "propToChangeCardinality")
        assert prop["isArray"] is True

    def test_should_set_property_filterability(self):
        self.schema.create_property("TestClass", "propToFilter", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToFilter")
        assert "isFilterable" not in prop or prop["isFilterable"] is False 
        self.schema.update_property("TestClass", "propToFilter", is_filterable=True)
        prop = self.schema.find_property(cls["objectProperties"], "propToFilter")
        assert prop["isFilterable"] is True

    def test_should_set_property_as_required(self):
        self.schema.create_property("TestClass", "propToRequire", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToRequire")
        assert prop["isOptional"] is True 
        self.schema.update_property("TestClass", "propToRequire", is_optional=False)
        prop = self.schema.find_property(cls["objectProperties"], "propToRequire")
        assert prop["isOptional"] is False

    def test_should_update_property_datatype(self):
        self.schema.create_property("TestClass", "propToUpdate", DATATYPE.INTEGER)
        self.schema.update_property("TestClass", "propToUpdate", datatype=DATATYPE.TEXT)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToUpdate")
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:text"

    def test_should_update_property_to_enum(self):
        self.schema.create_property("TestClass", "propToEnum", DATATYPE.INTEGER)
        enums = ["OptionA", "OptionB"]
        self.schema.update_property("TestClass", "propToEnum", datatype=DATATYPE.ENUM, enums=enums)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToEnum")
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:enum"
        assert prop["validationRules"][0]["value"] == enums

    def test_should_update_enum_property_options(self):
        enums = ["Option1", "Option2", "Option3"]
        self.schema.create_property("TestClass", "enumProp", DATATYPE.ENUM, enums=enums)
        new_enums = ["OptionA", "OptionB"]
        self.schema.update_property("TestClass", "enumProp", enums=new_enums)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "enumProp")
        assert prop["validationRules"][0]["value"] == new_enums

    def test_should_update_property_across_subclasses(self):
        self.schema.create_subclass("SubClass", "description", "TestClass")
        self.schema.create_property("TestClass", "propToUpdate", DATATYPE.INTEGER, apply_to_subclasses=True)
        self.schema.update_property("TestClass", "propToUpdate", datatype=DATATYPE.TEXT, apply_to_subclasses=True)
        cls = self.schema.find_class("SubClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToUpdate")
        assert prop["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:text"

    def test_should_rename_property(self):
        self.schema.create_property("TestClass", "propToRename", DATATYPE.INTEGER)
        self.schema.rename_property("TestClass", "propToRename", "renamedProp")
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "renamedProp")
        assert prop is not None
        assert self.schema.find_property(cls["objectProperties"], "propToRename") is None

    def test_should_delete_property_from_class(self):
        self.schema.create_property("TestClass", "propToDelete", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop = self.schema.find_property(cls["objectProperties"], "propToDelete")
        assert prop is not None
        self.schema.delete_property("TestClass", "propToDelete")
        prop = self.schema.find_property(cls["objectProperties"], "propToDelete")
        assert prop is None

    def test_should_assign_property_orders(self):
        self.schema.create_property("TestClass", "firstProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "secondProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "thirdProp", DATATYPE.INTEGER)
        cls = self.schema.find_class("TestClass")
        prop_names = [p["propertyName"] for p in cls["objectProperties"]]
        assert prop_names == ["label", "firstProp", "secondProp", "thirdProp"]
        self.schema.assign_property_orders({"TestClass": ["label", "secondProp", "thirdProp", "firstProp"]})
        cls = self.schema.find_class("TestClass")
        prop_names = [p["propertyName"] for p in cls["objectProperties"]]
        prop_orders = [p["propertyOrder"] for p in cls["objectProperties"]]
        assert prop_names == ["label", "secondProp", "thirdProp", "firstProp"]
        assert prop_orders == [0, 1, 2, 3]

    def test_should_assign_default_property_orders_if_not_specified(self):
        self.schema.create_property("TestClass", "firstProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "secondProp", DATATYPE.INTEGER)
        self.schema.create_property("TestClass", "thirdProp", DATATYPE.INTEGER)
        self.schema.assign_property_orders({})
        cls = self.schema.find_class("TestClass")
        prop_names = [p["propertyName"] for p in cls["objectProperties"]]
        prop_orders = [p["propertyOrder"] for p in cls["objectProperties"]]
        assert prop_names == ["label", "firstProp", "secondProp", "thirdProp"]
        assert prop_orders == [0, 1, 2, 3]

    def test_should_perform_deep_copy_when_performing_clone_schema(self):
        self.schema.create_property("TestClass", "prop1", DATATYPE.TEXT)
        cloned_schema = self.schema.clone()
        assert cloned_schema.find_class("TestClass")["description"] is not None
        cloned_schema.update_class("TestClass", new_description="Updated description")
        assert self.schema.find_class("TestClass")["description"] == ""
        assert cloned_schema.find_class("TestClass")["description"] == "Updated description"

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