"""Schema definition and manipulation for DataGraphs domain models."""

import json
import datetime
from typing import Optional, Self, Union
from datagraphs.enums import DATATYPE
from datagraphs.utils import SchemaTransformer

class SchemaError(Exception):
    """Base exception for Schema-related errors."""
    pass

class ClassNotFoundError(SchemaError):
    """Raised when a class is not found in the schema."""
    pass

class PropertyNotFoundError(SchemaError):
    """Raised when a property is not found in a class."""
    pass

class PropertyExistsError(SchemaError):
    """Raised when attempting to create a property that already exists."""
    pass

class InvalidInversePropertyError(SchemaError):
    """Raised when an invalid inverse property is specified."""
    pass

class Schema:
    """In-memory representation of a DataGraphs domain model schema."""

    ALL_CLASSES = '__all_classes__'

    def __init__(self, name: str = "", version: str = "") -> None:
        """Create a new empty schema.

        :param name: Model name. Defaults to ``'Domain Model'`` if empty.
        :param version: Schema version. Defaults to ``'1.0'`` if empty.
        :raises TypeError: If a ``dict`` is passed instead of keyword arguments.
        """
        if isinstance(name, dict):
            raise TypeError("Schema constructor expects keyword arguments, not a dict. Use Schema.create_from() to create a schema from a dict.")
        now = datetime.datetime.now(datetime.UTC).isoformat()
        self._schema = {
            "name": "",
            "createdDate": now,
            "lastModifiedDate": now,
            "classes": [],
        }
        self.update_schema_metadata(name, version)

    @staticmethod
    def create_from(data: dict, version: str = "") -> Self:
        """Create a `Schema` from a dictionary.

        Automatically detects and converts legacy-format schemas.

        :param data: Schema dictionary (new or legacy format).
        :param version: Schema version override.
        :returns: A new `Schema` instance.
        :raises SchemaError: If the dict is missing required keys.
        """
        if Schema._is_legacy_format(data):
            data = SchemaTransformer.old_to_new(data)
        schema = Schema(version=version)
        schema._set_internal_schema(data, version)
        return schema

    def update_schema_metadata(self, name: str = "", version: str = "") -> None:
        """Update the schema's name, version, and last modified date.
        
        :param name: New name for the schema. If empty, the name is unchanged unless it was previously empty, in which case it defaults to 'Domain Model'.
        :param version: New version string. If empty, the version is unchanged unless it was previously empty, in which case it defaults to '1.0'.
        """
        self._version = version or '1.0'
        self._schema['lastModifiedDate'] = datetime.datetime.now(datetime.UTC).isoformat()
        if name or version or len(self._schema.get('name', '')) == 0:
            self._schema['name'] = f"{name or 'Domain Model'} v{self.version}"

    @staticmethod
    def _is_legacy_format(schema: dict) -> bool:
        """Detect whether a schema dict uses the legacy (old) format."""
        classes = schema.get('classes', [])
        if classes:
            first = classes[0]
            return 'objectProperties' in first or ('label' in first and 'type' not in first)
        return 'guid' in schema

    def _set_internal_schema(self, data: dict, version: str) -> None:
        self._validate_schema(data)
        self.update_schema_metadata(version=version)
        self._schema = data

    def _validate_schema(self, schema: dict) -> None:
        required_keys = {'name', 'createdDate', 'lastModifiedDate', 'classes'}
        if not all(key in schema for key in required_keys):
            missing_keys = required_keys - set(schema.keys())
            raise SchemaError(f"Invalid schema. Missing keys: {', '.join(missing_keys)}")

    @property
    def classes(self) -> list[dict]:
        """The list of class definitions in the schema."""
        return self._schema["classes"]

    @property
    def version(self) -> str:
        """The schema version string."""
        return self._version
    
    def _make_description(self, text: str) -> dict:
        """Create a description dict in the new format."""
        return {"en": text, "@none": text}

    def _get_description_text(self, desc: Union[str, dict]) -> str:
        """Extract plain text from a description (handles both str and dict)."""
        if isinstance(desc, dict):
            return desc.get('@none', desc.get('en', ''))
        return desc or ''

    def create_class(
        self,
        class_name: str,
        description: str = "",
        parent_class_name: str = "",
        label_prop_name: str = "label",
        is_label_prop_lang_string: bool = True,
    ) -> None:
        """Create a new class in the schema.

        :param class_name: Name of the new class.
        :param description: Human-readable description.
        :param parent_class_name: Name of the parent class (for inheritance).
        :param label_prop_name: Name of the label property created by default.
        :param is_label_prop_lang_string: Whether the label property supports
            multiple languages.
        :raises SchemaError: If a class with the same name already exists.
        """
        existing_class = self.find_class(class_name)
        if existing_class is not None:
            raise SchemaError(f"The class '{class_name}' already exists in the schema")
        class_def = {
            "type": "Class",
            "name": class_name,
            "labelProperty": label_prop_name,
            "identifierProperty": "id",
            "properties": [
                {
                    "type": "DatatypeProperty",
                    "name": label_prop_name,
                    "range": "text",
                    "isOptional": False,
                    "isArray": False,
                    "isLangString": is_label_prop_lang_string,
                    "isLabelSynonym": False
                }
            ],
            "isAbstract": False,
        }
        if description:
            class_def['description'] = self._make_description(description)
        if parent_class_name:
            class_def['subClassOf'] = parent_class_name
        self._schema['classes'].append(class_def)

    def create_subclass(self, class_name: str, description: str, parent_class_name: str) -> None:
        """Create a subclass that inherits all properties from the parent class.

        :param class_name: Name of the new subclass.
        :param description: Description for the subclass.
        :param parent_class_name: Name of the parent class to inherit from.
        :raises ClassNotFoundError: If the parent class does not exist.
        """
        class_def = self.find_class(parent_class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Parent class '{parent_class_name}' not found")
        label_prop_name = class_def['labelProperty']
        label_prop_def = self.find_property(class_def['properties'], label_prop_name)
        self.create_class(class_name, description, parent_class_name, label_prop_name, label_prop_def.get('isLangString', False))
        for prop_def in class_def['properties']:
            if prop_def['name'] != label_prop_name:
                validation_rules = prop_def.get('validationRules', [])
                range_value = prop_def['range']
                try:
                    datatype = DATATYPE(range_value)
                except ValueError:
                    datatype = range_value
                enums = validation_rules[0].get('value', []) if validation_rules else []
                desc = self._get_description_text(prop_def.get('description'))
                self.create_property(
                    class_name,
                    prop_def['name'],
                    datatype,
                    desc,
                    prop_def.get('isOptional', True),
                    prop_def.get('isArray', False),
                    prop_def.get('isNestedObject', False),
                    prop_def.get('isLangString', False),
                    prop_def.get('inverseOf', ''),
                    enums,
                    prop_def.get('isLabelSynonym', False),
                    prop_def.get('isFilterable', None),
                    apply_to_subclasses=False,
                )

    def update_class(self, class_name: str, new_name: str = "", new_description: str = "", parent_class_name: str = "") -> None:
        """Update a class's name, description, or parent class.

        :param class_name: Current class name.
        :param new_name: New class name, or empty to leave unchanged.
        :param new_description: New description, or empty to leave unchanged.
        :param parent_class_name: New parent class. Empty string removes the parent.
        :raises ClassNotFoundError: If the class does not exist.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        if new_name:
            class_def["name"] = new_name
        if parent_class_name:
            class_def['subClassOf'] = parent_class_name
        elif 'subClassOf' in class_def:
            del class_def['subClassOf']
        if new_description:
            class_def['description'] = self._make_description(new_description)

    def delete_class(self, class_name: str, include_linked_properties: bool = False, cascade_to_subclasses: bool = True) -> None:
        """Delete a class from the schema.

        :param class_name: Name of the class to delete.
        :param include_linked_properties: If ``True``, also removes ObjectProperties
            on other classes that reference this class.
        :param cascade_to_subclasses: If ``True``, removes ``subClassOf`` links
            from any subclasses of the deleted class.
        :raises ClassNotFoundError: If the class does not exist.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        self._schema["classes"].remove(class_def)
        if include_linked_properties:
            self._delete_linked_properties(class_name)
        if cascade_to_subclasses:
            for other_def in self._schema["classes"]:
                if other_def.get("subClassOf") == class_name:
                    other_def.pop("subClassOf", None)

    def assign_label_property(self, class_name: str, prop_name: str, is_lang_string: bool = True) -> None:
        """Designate an existing property as the label property for a class.

        The property is also marked as required (``isOptional=False``).

        :param class_name: Class name.
        :param prop_name: Property name to use as the label.
        :param is_lang_string: Whether the label supports multiple languages.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist on the class.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        class_def["labelProperty"] = prop_name
        prop_def = self.find_property(class_def["properties"], prop_name)
        if prop_def is None:
            raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
        prop_def["isOptional"] = False
        prop_def["isLangString"] = is_lang_string

    def assign_label_autogen(self, class_name: str, pattern: str) -> None:
        """Set an auto-generation pattern on the label property of a class.

        :param class_name: Class name.
        :param pattern: Auto-generation expression.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the label property does not exist.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        prop_name = class_def["labelProperty"]
        prop_def = self.find_property(class_def["properties"], prop_name)
        if prop_def is None:
            raise PropertyNotFoundError(f"Label property '{prop_name}' not found in class '{class_name}'")
        prop_def['propertyValuePattern'] = pattern

    def assign_baseclass(self, class_name: str, parent_class_name: str) -> None:
        """Set or change the parent (base) class for an existing class.

        :param class_name: The class to modify.
        :param parent_class_name: The new parent class name.
        :raises ClassNotFoundError: If *class_name* does not exist.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        class_def['subClassOf'] = parent_class_name

    def assign_class_description(self, class_name: str, description: str) -> None:
        """Set or clear the description of a class.

        :param class_name: Class name.
        :param description: New description. Pass an empty string to remove it.
        :raises ClassNotFoundError: If the class does not exist.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        if description:
            class_def['description'] = self._make_description(description)
        else:
            class_def.pop('description', None)

    def _delete_linked_properties(self, class_name: str) -> None:
        for class_def in self._schema["classes"]:
            properties_to_remove = [
                prop_def for prop_def in class_def["properties"]
                if (prop_def.get("type") == "ObjectProperty"
                    and prop_def.get("range") == class_name)
            ]
            for prop_def in properties_to_remove:
                class_def["properties"].remove(prop_def)

    def create_property(
        self,
        class_name: str,
        prop_name: str,
        datatype: Union[DATATYPE, str],
        description: str = "",
        is_optional: bool = True,
        is_array: bool = False,
        is_nested: bool = False,
        is_lang_string: bool = True,
        inverse_of: str = "",
        enums: Optional[list] = None,
        is_synonym: bool = False,
        is_filterable: Optional[bool] = None,
        apply_to_subclasses: bool = False,
    ) -> None:
        """Create a new property on a class.

        :param class_name: Class to add the property to.
        :param prop_name: Property name.
        :param datatype: A `DATATYPE` enum value for primitive types, or a class
            name string for object (relationship) properties.
        :param description: Human-readable description.
        :param is_optional: Whether the property is optional.
        :param is_array: Whether the property holds multiple values.
        :param is_nested: Whether an object property is nested (embedded).
        :param is_lang_string: For text properties, whether to support multiple
            languages.
        :param inverse_of: Name of the inverse property on the target class
            (object properties only).
        :param enums: Allowed values for ``DATATYPE.ENUM`` properties.
        :param is_synonym: Whether this property is a label synonym.
        :param is_filterable: Whether the property is available as a facet/filter.
        :param apply_to_subclasses: If ``True``, also creates the property on all
            existing subclasses.
        :raises ClassNotFoundError: If the class (or referenced class) does not exist.
        :raises PropertyExistsError: If a property with the same name already exists.
        :raises InvalidInversePropertyError: If the inverse property specification
            is invalid.
        """
        if enums is None:
            enums = []
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        existing_prop = self.find_property(class_def["properties"], prop_name)
        if not (hasattr(datatype, 'value') and (datatype.value in set(i.value for i in DATATYPE))) and not isinstance(datatype, str):
            raise TypeError(f"Unspecified datatype for {class_name}.{prop_name}")
        if existing_prop is not None:
            raise PropertyExistsError(f"The property '{prop_name}' already exists in the class: {class_name}")
        prop_def = {
            "name": prop_name,
        }
        class_def["properties"].append(prop_def)
        self._assign_datatype(prop_def, datatype, is_nested, is_lang_string)
        self._assign_property_description(prop_def, description)
        self._assign_is_optional(prop_def, is_optional)
        self._assign_is_array(prop_def, is_array)
        self._assign_inverse_of(prop_def, class_name, inverse_of, datatype)
        self._assign_enum(prop_def, datatype, enums)
        self._assign_is_synonym(prop_def, is_synonym)
        if is_filterable is not None:
            self._assign_is_filterable(prop_def, is_filterable)
        if apply_to_subclasses:
            subclasses = self.find_subclasses(class_name)
            for subclass in subclasses:
                self.create_property(subclass['name'], prop_name, datatype, description, is_optional, is_array, is_nested, is_lang_string, inverse_of, enums, is_filterable, apply_to_subclasses)

    def _assign_property_description(self, prop_def: dict, description: str) -> None:
        if description:
            prop_def["description"] = self._make_description(description)
        else:
            prop_def.pop("description", None)

    def _assign_is_optional(self, prop_def: dict, is_optional: bool = False) -> None:
        prop_def["isOptional"] = is_optional

    def _assign_is_array(self, prop_def: dict, is_array: bool = False) -> None:
        prop_def["isArray"] = is_array

    def _assign_datatype(self, prop_def: dict, datatype: Union[DATATYPE, str], is_nested: bool = False, is_lang_string: bool = True) -> None:
        if datatype in DATATYPE:
            prop_def["type"] = "DatatypeProperty"
            prop_def["range"] = str(datatype)
            if datatype == DATATYPE.TEXT:
                prop_def["isLangString"] = is_lang_string
            else:
                prop_def["isLangString"] = False
            prop_def.pop("isNestedObject", None)
            prop_def.pop("inferLocation", None)
            prop_def.pop("isSymmetric", None)
        else:
            if self.find_class(datatype) is None:
                raise ClassNotFoundError(f"Class '{datatype}' not found for property datatype")
            prop_def["type"] = "ObjectProperty"
            prop_def["range"] = str(datatype)
            prop_def["isNestedObject"] = is_nested
            prop_def.setdefault("inferLocation", False)
            prop_def.setdefault("isSymmetric", False)
            prop_def.pop("isLangString", None)

    def _assign_inverse_of(self, prop_def: dict, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> None:
        if inverse_of and self._is_valid_inverse_of(class_name, inverse_of, datatype):
            prop_def["inverseOf"] = inverse_of

    def _assign_enum(self, prop_def: dict, datatype: Union[DATATYPE, str], enums: list) -> None:
        if datatype == DATATYPE.ENUM:
            prop_def["validationRules"] = [{
                "type": "enumeration",
                "value": enums,
            }]

    def _assign_is_filterable(self, prop_def: dict, is_filterable: Optional[bool] = None) -> None:
        if is_filterable is not None:
            prop_def["isFilterable"] = is_filterable

    def _assign_is_synonym(self, prop_def: dict, is_synonym: bool) -> None:
        if is_synonym is not None:
            prop_def["isLabelSynonym"] = is_synonym

    def _is_valid_inverse_of(self, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> bool:
        is_valid = False
        if datatype not in DATATYPE:
            class_def = self.find_class(datatype)
            if class_def is not None:
                prop_def = self.find_property(class_def["properties"], inverse_of)
                if prop_def is None:
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' not found in class '{datatype}'")
                elif prop_def.get("type") != "ObjectProperty":
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' in class '{datatype}' has no range defined, expected '{class_name}'")
                elif prop_def.get("range") != class_name:
                    raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' in class '{datatype}' does not point back to class '{class_name}'")
                else:
                    is_valid = True
            else:
                raise InvalidInversePropertyError(f"Inverse property refers to non-existent class '{datatype}'")
        else:
            raise InvalidInversePropertyError(f"Inverse property can only be set for properties with concept datatype, not '{datatype}'")
        return is_valid

    def update_property(
        self,
        class_name: str,
        prop_name: str,
        datatype: Union[DATATYPE, str] = None,
        description: str = None,
        is_optional: bool = None,
        is_array: bool = None,
        is_nested: bool = None,
        is_lang_string: bool = None,
        inverse_of: str = "",
        enums: Optional[list] = None,
        is_synonym: bool = False,
        is_filterable: bool = None,
        apply_to_subclasses: bool = None,
    ) -> None:
        """Update an existing property on a class.

        Only parameters that are explicitly provided (non-``None``) will be
        changed.

        :param class_name: Class containing the property.
        :param prop_name: Property name to update.
        :param datatype: New data type.
        :param description: New description.
        :param is_optional: Whether the property is optional.
        :param is_array: Whether the property holds multiple values.
        :param is_nested: Whether an object property is nested.
        :param is_lang_string: Whether the property supports multiple languages.
        :param inverse_of: Name of the inverse property on the target class.
        :param enums: Allowed enumeration values.
        :param is_synonym: Whether this property is a label synonym.
        :param is_filterable: Whether the property is available as a filter.
        :param apply_to_subclasses: If ``True``, also updates the property on all
            existing subclasses.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        prop_def = self.find_property(class_def["properties"], prop_name)
        if prop_def is None:
            raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
        if description is not None:
            self._assign_property_description(prop_def, description)
        if is_optional is not None:
            self._assign_is_optional(prop_def, is_optional)
        if is_array is not None:
            self._assign_is_array(prop_def, is_array)
        if datatype is not None:
            self._assign_datatype(prop_def, datatype, is_nested, is_lang_string)
        if inverse_of is not None:
            self._assign_inverse_of(prop_def, class_name, inverse_of, datatype)
        if enums is not None:
            existing_datatype = prop_def["range"]
            self._assign_enum(prop_def, existing_datatype, enums)
        if is_filterable is not None:
            self._assign_is_filterable(prop_def, is_filterable)
        if is_synonym is not None:
            self._assign_is_synonym(prop_def, is_synonym)
        if apply_to_subclasses:
            subclasses = self.find_subclasses(class_name)
            for subclass in subclasses:
                self.update_property(subclass['name'], prop_name, datatype, description, is_optional, is_array, is_nested, is_lang_string, inverse_of, enums, is_filterable, apply_to_subclasses)

    def rename_property(self, class_name: str, old_prop_name: str, new_prop_name: str) -> None:
        """Rename a property.

        If the property is the class's label property, the label property
        reference is updated automatically.

        :param class_name: Class containing the property.
        :param old_prop_name: Current property name.
        :param new_prop_name: New property name.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If *old_prop_name* does not exist.
        :raises PropertyExistsError: If *new_prop_name* is already in use.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        prop_def = self.find_property(class_def["properties"], old_prop_name)
        if prop_def is None:
            raise PropertyNotFoundError(f"Property '{old_prop_name}' not found in class '{class_name}'")
        conflict_prop_def = self.find_property(class_def["properties"], new_prop_name)
        if conflict_prop_def is not None:
            raise PropertyExistsError(f"The new property name '{new_prop_name}' is already in use")
        prop_def["name"] = new_prop_name
        if class_def["labelProperty"] == old_prop_name:
            class_def["labelProperty"] = new_prop_name

    def delete_property(self, class_name: str, prop_name: str) -> None:
        """Remove a property from a class.

        :param class_name: Class containing the property.
        :param prop_name: Property name to delete.
        :raises ClassNotFoundError: If the class does not exist.
        :raises PropertyNotFoundError: If the property does not exist.
        """
        class_def = self.find_class(class_name)
        if class_def is None:
            raise ClassNotFoundError(f"Class '{class_name}' not found")
        prop_def = self.find_property(class_def["properties"], prop_name)
        if prop_def is None:
            raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
        class_def["properties"].remove(prop_def)

    def find_class(self, name: str) -> Optional[dict]:
        """Find a class definition by name.

        :param name: The class name to look up.
        :returns: The class dict, or ``None`` if not found.
        """
        return next((x for x in self._schema["classes"] if x['name'] == name), None)

    def find_subclasses(self, baseclass: str) -> list[dict]:
        """Find all direct subclasses of a given class.

        :param baseclass: The parent class name.
        :returns: A list of class dicts whose ``subClassOf`` matches *baseclass*.
        """
        return [x for x in self._schema["classes"] if x.get('subClassOf') == baseclass]

    def find_property(self, props: list, name: str) -> Optional[dict]:
        """Find a property by name within a list of property dicts.

        :param props: List of property dicts to search.
        :param name: The property name to look up.
        :returns: The property dict, or ``None`` if not found.
        """
        return next((x for x in props if x['name'] == name), None)

    def assign_property_orders(self, property_orders: dict) -> None:
        """Reorder properties within classes.

        Properties not listed in the order are appended at the end.

        :param property_orders: A dict mapping class names to ordered lists of
            property names.
        """
        for class_def in self._schema['classes']:
            if class_def['name'] in property_orders:
                ordered_names = property_orders[class_def['name']]
                props_by_name = {p['name']: p for p in class_def['properties']}
                ordered = [props_by_name[n] for n in ordered_names if n in props_by_name]
                remaining = [p for p in class_def['properties'] if p['name'] not in set(ordered_names)]
                class_def['properties'] = ordered + remaining

    def clone(self) -> Self:
        """Create a deep copy of the schema.

        :returns: A new independent `Schema` instance.
        """
        return Schema.create_from(json.loads(json.dumps(self._schema)))

    def to_dict(self) -> dict:
        """Convert the schema to a plain dictionary.

        :returns: The schema as a dict.
        """
        return self._schema

    def to_json(self) -> str:
        """Serialise the schema to a JSON string.

        :returns: A JSON-formatted string.
        """
        return json.dumps(self._schema, ensure_ascii=False, indent=2)

