import uuid
import json
import datetime
from typing import Optional, Self, Union
from datagraphs.datatypes import DATATYPE
from datagraphs.utils import *

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

  ALL_DATATYPES = '__all_classes__'

  # Class-level constant for datatype mappings
  DATATYPE_MAPPINGS = {
      DATATYPE.TEXT: {"elasticsearchDatatype": "text", "xsdDatatype": "string"},
      DATATYPE.DATE: {"elasticsearchDatatype": "date", "xsdDatatype": "date"},
      DATATYPE.DATETIME: {"elasticsearchDatatype": "dateTime", "xsdDatatype": "dateTime"},
      DATATYPE.BOOLEAN: {"elasticsearchDatatype": "boolean", "xsdDatatype": "boolean"},
      DATATYPE.DECIMAL: {"elasticsearchDatatype": "double", "xsdDatatype": "decimal"},
      DATATYPE.INTEGER: {"elasticsearchDatatype": "long", "xsdDatatype": "integer"},
      DATATYPE.KEYWORD: {"elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
      DATATYPE.URL: {"elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
      DATATYPE.IMAGE_URL: {"elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
      DATATYPE.ENUM: {"elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
  }

  def __init__(self, schema: Optional[dict] = None, name: str = "", version: str = '1.0', project: str = ''):
    if schema is None or len(schema) == 0:
      schema = self._create_schema(project)
    else: 
      self._validate_schema(schema)
    if version:
      schema['name'] = f"{name or 'Domain Model'} v{version}"
    schema['lastModifiedDate'] = datetime.datetime.now(datetime.UTC).isoformat()
    self._schema = schema

  @property
  def classes(self) -> list[dict]:
    return self._schema["classes"]

  @staticmethod
  def _create_schema(project: str) -> dict:
    guid = uuid.uuid4().hex
    now = datetime.datetime.now(datetime.UTC).isoformat()
    return {
      "id": f"urn:models:{guid}",
      "guid": str(guid),
      "type": "DomainModel",
      "name": "Domain Model",
      "description": "The project information model",
      "project": f"urn:{project}:" if project else "",
      "createdDate": now,
      "lastModifiedDate": now,
      "classes": []
    }

  @staticmethod
  def _validate_schema(schema: dict) -> None:
    required_keys = {'name', 'createdDate', 'lastModifiedDate', 'classes'}
    if 'project' in schema:
      required_keys.update({'project', 'id', 'type', 'guid'})
    if not all(key in schema for key in required_keys):
      raise SchemaError("Invalid schema.")

  def create_class(
      self,
      class_name: str,
      description: str = "",
      parent_class_name: str = "",
      label_prop_name: str = "label",
      is_label_prop_lang_string: bool = True
    ) -> None:
    existing_class = self.find_class(class_name)
    if existing_class is not None:
      raise SchemaError(f"The class '{class_name}' already exists in the schema")
    guid = uuid.uuid4().hex
    label_guid = uuid.uuid4().hex
    now = datetime.datetime.now(datetime.UTC).isoformat()
    class_def = {
        "id": f"{self._schema['id']}:classes:{class_name}",
        "guid": str(guid),
        "model": self._schema["id"],
        "project": self._schema.get("project", ""),
        "label": class_name,
        "labelProperty": label_prop_name,
        "createdDate": now,
        "lastModifiedDate": now,
        "parentClasses": [class_name],
        "identifierProperty": "id",
        "description": description,
        "objectProperties": [
          {
            "propertyName": label_prop_name,
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
            "guid": str(label_guid),
            "propertyOrder": 0,
            "isLangString": is_label_prop_lang_string,
            "id": f"{self._schema['id']}:classes:{class_name}:{label_prop_name}"
          }
        ]
      }
    if parent_class_name:
      class_def['parentClasses'] = [class_name, parent_class_name]
      class_def['parentClass'] = parent_class_name
    self._schema['classes'].append(class_def)

  def create_subclass(self, class_name: str, description: str, parent_class_name: str) -> None:
    class_def = self.find_class(parent_class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Parent class '{parent_class_name}' not found")
    label_prop_name = class_def['labelProperty']
    label_prop_def = self.find_property(class_def['objectProperties'], label_prop_name)
    self.create_class(class_name, description, parent_class_name, label_prop_name, label_prop_def.get('isLangString', False))
    for prop_def in class_def['objectProperties']:
      if prop_def['propertyName'] != label_prop_name:
        validation_rules = prop_def.get('validationRules', [])
        datatype = DATATYPE(prop_def['propertyDatatype']['label'])
        enums = validation_rules[0].get('value', []) if validation_rules else []
        self.create_property(
          class_name,
          prop_def['propertyName'],
          datatype,
          prop_def['propertyDescription'],
          prop_def.get('isOptional', True),
          prop_def['isArray'],
          prop_def.get('isNestedObject', False),
          prop_def.get('isLangString', False),
          prop_def.get('inverseOf', ''),
          enums,
          prop_def.get('isLabelSynonym', False),
          prop_def.get('isFilterable', False),
          apply_to_subclasses=False
        )

  def update_class(self, class_name: str, new_name: str = "", new_description: str = "", parent_class_name: str = "") -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    if new_name:
      class_def["id"] = f"{self._schema['id']}:classes:{new_name}"
      class_def["label"] = new_name
    effective_name = new_name or class_name
    if parent_class_name:
      class_def['parentClasses'] = [effective_name, parent_class_name]
      class_def['parentClass'] = parent_class_name
    else:
      class_def['parentClasses'] = [effective_name]
    if new_description:
      class_def['description'] = new_description

  def delete_class(self, class_name: str, include_linked_properties: bool = False, cascade_to_subclasses: bool = True) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    self._schema["classes"].remove(class_def)
    if include_linked_properties:
      self._delete_linked_properties(class_name)
    if cascade_to_subclasses:
      for parent_def in self._schema["classes"]:
        if parent_def.get("parentClass") == class_name:
          parent_def.pop("parentClass", None)
        if class_name in parent_def["parentClasses"]:
          parent_def["parentClasses"].remove(class_name)

  def assign_label_property(self, class_name: str, prop_name: str, is_lang_string: bool = True) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    class_def["labelProperty"] = prop_name
    prop_def = self.find_property(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
    prop_def["isOptional"] = False
    prop_def["isLangString"] = is_lang_string

  def assign_label_autogen(self, class_name: str, pattern: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_name = class_def["labelProperty"]
    prop_def = self.find_property(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Label property '{prop_name}' not found in class '{class_name}'")
    prop_def['propertyValuePattern'] = pattern

  def assign_baseclass(self, class_name: str, parent_class_name: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    class_def['parentClasses'] = [class_name, parent_class_name]
    class_def['parentClass'] = parent_class_name

  def assign_class_description(self, class_name: str, description: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    class_def['description'] = description

  def _delete_linked_properties(self, class_name: str) -> None:
    for class_def in self._schema["classes"]:
      properties_to_remove = [
        prop_def for prop_def in class_def["objectProperties"]
        if (prop_def["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:concept" 
            and prop_def["propertyDatatype"].get("range") == class_name)
      ]
      for prop_def in properties_to_remove:
        class_def["objectProperties"].remove(prop_def)

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
      is_filterable: bool = False,
      apply_to_subclasses: bool = False
    ) -> None:
    if enums is None:
      enums = []
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    existing_prop = self.find_property(class_def["objectProperties"], prop_name)
    if not (hasattr(datatype, 'value') and (datatype.value in set(i.value for i in DATATYPE))) and not isinstance(datatype, str):
      raise TypeError(f"Unspecified datatype for {class_name}.{prop_name}")
    if existing_prop is not None:
      raise PropertyExistsError(f"The property '{prop_name}' already exists in the class: {class_name}")
    prop_def = {
      "propertyName": prop_name,
      "id": f"{self._schema['id']}:classes:{class_name}:{prop_name}",
      "guid": str(uuid.uuid4().hex),
      "propertyOrder": len(class_def["objectProperties"])
    }
    class_def["objectProperties"].append(prop_def)
    self._assign_property_description(prop_def, description)
    self._assign_is_optional(prop_def, is_optional)
    self._assign_is_array(prop_def, is_array)
    self._assign_datatype(prop_def, datatype, is_nested, is_lang_string)
    self._assign_inverse_of(prop_def, class_name, inverse_of, datatype)
    self._assign_enum(prop_def, datatype, enums)
    self._assign_is_filterable(prop_def, is_filterable)
    self._assign_is_synonym(prop_def, is_synonym) 
    if apply_to_subclasses:
      subclasses = self.find_subclasses(class_name)
      for subclass in subclasses:
        self.create_property(subclass['label'], prop_name, datatype, description, is_optional, is_array, is_nested, is_lang_string, inverse_of, enums, is_filterable, apply_to_subclasses)

  def _assign_property_description(self, prop_def: dict, description: str) -> None:
    prop_def["propertyDescription"] = description

  def _assign_is_optional(self, prop_def: dict, is_optional: bool = False) -> None:
    prop_def["isOptional"] = is_optional

  def _assign_is_array(self, prop_def: dict, is_array: bool = False) -> None:
    prop_def["isArray"] = is_array

  def _assign_datatype(self, prop_def: dict, datatype: Union[DATATYPE, str], is_nested: bool = False, is_lang_string: bool = True) -> None:
    property_datatype = {
      "id": "urn:datagraphs:datatypes:",
      "type": "PropertyDatatype",
      "label": datatype.value if hasattr(datatype, 'value') else datatype,
      "elasticsearchDatatype": "",
      "xsdDatatype": ""
    }
    if datatype in DATATYPE:
      property_datatype["id"] += str(datatype)
      property_datatype["elasticsearchDatatype"] = self.DATATYPE_MAPPINGS[datatype]["elasticsearchDatatype"]
      property_datatype["xsdDatatype"] = self.DATATYPE_MAPPINGS[datatype]["xsdDatatype"]
      if datatype == DATATYPE.TEXT:
        prop_def["isLangString"] = is_lang_string
    else:
      if self.find_class(datatype) is None:
        raise ClassNotFoundError(f"Class '{datatype}' not found for property datatype")
      property_datatype["id"] += "concept"
      property_datatype["elasticsearchDatatype"] = "keyword"
      property_datatype["xsdDatatype"] = "string"
      property_datatype["range"] = str(datatype)
      prop_def["isNestedObject"] = is_nested
    prop_def["propertyDatatype"] = property_datatype

  def _assign_inverse_of(self, prop_def: dict, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> None:
    if inverse_of and self._is_valid_inverse_of(class_name, inverse_of, datatype):
      prop_def["inverseOf"] = inverse_of

  def _assign_enum(self, prop_def: dict, datatype: Union[DATATYPE, str], enums: list) -> None:
    if datatype == DATATYPE.ENUM:
      prop_def["validationRules"] = [{
        "id": "urn:datagraphs:validation:enumeration",
        "value": enums
      }]

  def _assign_is_filterable(self, prop_def: dict, is_filterable: bool) -> None:
    if is_filterable:
      prop_def["isFilterable"] = True

  def _assign_is_synonym(self, prop_def: dict, is_synonym: bool) -> None:
    if is_synonym:
      prop_def["isLabelSynonym"] = True

  def _is_valid_inverse_of(self, class_name: str, inverse_of: str, datatype: Union[DATATYPE, str]) -> bool:
    is_valid = False
    if datatype not in DATATYPE:
      class_def = self.find_class(datatype)
      if class_def is not None:
        prop_def = self.find_property(class_def["objectProperties"], inverse_of)
        if prop_def is None:
          raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' not found in class '{datatype}'")
        elif "range" not in prop_def["propertyDatatype"]:
          raise InvalidInversePropertyError(f"Inverse property '{inverse_of}' in class '{datatype}' has no range defined, expected '{class_name}'")
        elif prop_def["propertyDatatype"]["range"] != class_name:
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
      apply_to_subclasses: bool = None
    ) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_property(class_def["objectProperties"], prop_name)
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
      existing_datatype = get_id_from_urn(prop_def["propertyDatatype"]["id"])
      self._assign_enum(prop_def, existing_datatype, enums)
    if is_filterable is not None:
      self._assign_is_filterable(prop_def, is_filterable)
    if is_synonym is not None:
      self._assign_is_synonym(prop_def, is_synonym) 
    if apply_to_subclasses:
      subclasses = self.find_subclasses(class_name)
      for subclass in subclasses:
        self.update_property(subclass['label'], prop_name, datatype, description, is_optional, is_array, is_nested, is_lang_string, inverse_of, enums, is_filterable, apply_to_subclasses)

  def rename_property(self, class_name: str, old_prop_name: str, new_prop_name: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_property(class_def["objectProperties"], old_prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{old_prop_name}' not found in class '{class_name}'")
    conflict_prop_def = self.find_property(class_def["objectProperties"], new_prop_name)
    if conflict_prop_def is not None:
      raise PropertyExistsError(f"The new property name '{new_prop_name}' is already in use")
    prop_def["propertyName"] = new_prop_name
    if class_def["labelProperty"] == old_prop_name:
      class_def["labelProperty"] = new_prop_name

  def delete_property(self, class_name: str, prop_name: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_property(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
    class_def["objectProperties"].remove(prop_def)

  def find_class(self, name: str) -> Optional[dict]:
    return next((x for x in self._schema["classes"] if x['label'] == name), None)

  def find_subclasses(self, baseclass: str) -> list[dict]:
    return [x for x in self._schema["classes"] if x.get('parentClass') == baseclass]

  def find_property(self, props: list, name: str) -> Optional[dict]:
    return next((x for x in props if x['propertyName'] == name), None)

  def assign_property_orders(self, property_orders: dict) -> None:
    for class_def in self._schema['classes']:
      if class_def['label'] in property_orders:
        for i, pname in enumerate(property_orders[class_def['label']]):
          prop = self.find_property(class_def['objectProperties'], pname)
          if prop:
            prop['propertyOrder'] = i
        class_def['objectProperties'] = sorted(class_def['objectProperties'], key=lambda p: p['propertyOrder'])
      else:
        for i, prop in enumerate(class_def['objectProperties']):
          prop['propertyOrder'] = i

  def clone(self) -> Self:
    return Schema(json.loads(json.dumps(self._schema)))

  def to_dict(self) -> dict:
    return self._schema

  def to_json(self) -> str:
    return json.dumps(self._schema, ensure_ascii=False, indent=2)

