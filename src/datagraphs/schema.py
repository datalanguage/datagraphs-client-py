import uuid
import json
import datetime
from datagraphs.datatypes import DATATYPE
from typing import Optional, Self

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

class Schema:

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

  DEFAULT_PROJECT_URN = "urn:datagraphs:project"

  def __init__(self, schema: Optional[dict] = None, version: str = '1.0', project_urn: str = ''):
    if schema is None or len(schema) == 0:
      schema = self._create_schema(project_urn or self.DEFAULT_PROJECT_URN)
    else: 
      self._validate_schema(schema)
    if version:
      schema['name'] = f"Domain Model v{version}"
    schema['lastModifiedDate'] = datetime.datetime.now(datetime.UTC).isoformat()
    self._schema = schema

  @staticmethod
  def _create_schema(project_urn: str) -> dict:
    guid = uuid.uuid4().hex
    now = datetime.datetime.now(datetime.UTC).isoformat()
    return {
      "id": f"urn:models:{guid}",
      "guid": str(guid),
      "type": "DomainModel",
      "name": "Domain Model",
      "description": "",
      "project": project_urn,
      "createdDate": now,
      "lastModifiedDate": now,
      "classes": []
    }

  @staticmethod
  def _validate_schema(schema: dict) -> None:
    required_keys = {'id', 'guid', 'type', 'name', 'project', 'createdDate', 'lastModifiedDate', 'classes'}
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
        "project": self._schema.get("project", self.DEFAULT_PROJECT_URN),
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
    label_prop_def = self.find_prop(class_def['objectProperties'], label_prop_name)
    self.create_class(class_name, description, parent_class_name, label_prop_name, label_prop_def.get('isLangString', False))
    for prop_def in class_def['objectProperties']:
      if prop_def['propertyName'] != label_prop_name:
        validation_rules = prop_def.get('validationRules', [])
        datatype = DATATYPE(prop_def['propertyDatatype']['label'])
        enums = validation_rules[0].get('value', []) if validation_rules else []
        self.create_prop(
          class_name,
          prop_def['propertyName'],
          datatype,
          prop_def.get('description', ''),
          prop_def['isArray'],
          prop_def['isNestedObject'],
          prop_def.get('isLangString', False),
          prop_def.get('inverseOf', ''),
          enums
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

  def delete_class(self, class_name: str, include_linked_props: bool = False, cascade_to_subclasses: bool = True) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    self._schema["classes"].remove(class_def)
    if include_linked_props:
      self._delete_linked_props(class_name)
    if cascade_to_subclasses:
      for parent_def in self._schema["classes"]:
        if parent_def.get("parentClass") == class_name:
          parent_def.pop("parentClass", None)
        if class_name in parent_def["parentClasses"]:
          parent_def["parentClasses"].remove(class_name)

  def assign_label_prop(self, class_name: str, prop_name: str, is_lang_string: bool = True) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    class_def["labelProperty"] = prop_name
    prop_def = self.find_prop(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
    prop_def["isOptional"] = False
    prop_def["isLangString"] = is_lang_string

  def assign_label_autogen(self, class_name: str, pattern: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_name = class_def["labelProperty"]
    prop_def = self.find_prop(class_def["objectProperties"], prop_name)
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

  def _delete_linked_props(self, class_name: str) -> None:
    for class_def in self._schema["classes"]:
      props_to_remove = [
        prop_def for prop_def in class_def["objectProperties"]
        if (prop_def["propertyDatatype"]["id"] == "urn:datagraphs:datatypes:concept" 
            and prop_def["propertyDatatype"].get("range") == class_name)
      ]
      for prop_def in props_to_remove:
        class_def["objectProperties"].remove(prop_def)

  def create_cascading_property(
      self, 
      class_name: str,
      prop_name: str,
      datatype: DATATYPE,
      description: str = "",
      is_array: bool = False,
      is_nested: bool = False,
      is_lang_string: bool = True,
      inverse_of: str = "",
      enums: Optional[list] = None
    ) -> None:
    if enums is None:
      enums = []
    self.create_prop(class_name, prop_name, datatype, description, is_array, is_nested, is_lang_string, inverse_of, enums)
    subclasses = self.find_subclasses(class_name)
    for subclass in subclasses:
      self.create_prop(subclass['label'], prop_name, datatype, description, is_array, is_nested, is_lang_string, inverse_of, enums)

  def create_prop(
      self, 
      class_name: str,
      prop_name: str,
      datatype: DATATYPE,
      description: str = "",
      is_array: bool = False,
      is_nested: bool = False,
      is_lang_string: bool = True,
      inverse_of: str = "",
      enums: Optional[list] = None
    ) -> None:
    if enums is None:
      enums = []
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    existing_prop = self.find_prop(class_def["objectProperties"], prop_name)
    if not (hasattr(datatype, 'value') and (datatype.value in set(i.value for i in DATATYPE))) and not isinstance(datatype, str):
      raise TypeError(f"Unspecified datatype for {class_name}.{prop_name}")
    if existing_prop is not None:
      raise PropertyExistsError(f"The property '{prop_name}' already exists in the class: {class_name}")
    guid = uuid.uuid4().hex
    prop_def = {
      "propertyName": prop_name,
      "isOptional": prop_name != class_def["labelProperty"],
      "isArray": is_array,
      "propertyDatatype": {
        "id": "urn:datagraphs:datatypes:",
        "type": "PropertyDatatype",
        "label": datatype.value if hasattr(datatype, 'value') else datatype,
        "elasticsearchDatatype": "",
        "xsdDatatype": ""
      },
      "isNestedObject": is_nested,
      "guid": str(guid),
      "propertyOrder": len(class_def["objectProperties"]),
      "id": f"{self._schema['id']}:classes:{class_name}:{prop_name}",
      "propertyDescription": description
    }
    if inverse_of:
      prop_def["inverseOf"] = inverse_of
    prop_def["propertyDatatype"] = self._insert_datatype(prop_def, datatype, is_nested, is_lang_string)
    class_def["objectProperties"].append(prop_def)
    if datatype == DATATYPE.ENUM:
      prop_def["validationRules"] = [{
        "id": "urn:datagraphs:validation:enumeration",
        "value": enums
      }]

  def _insert_datatype(self, prop_def: dict, datatype: DATATYPE, is_nested: bool, is_lang_string: bool) -> dict:
    property_datatype = prop_def["propertyDatatype"]
    if datatype in DATATYPE:
      property_datatype["id"] += str(datatype)
      property_datatype["elasticsearchDatatype"] = self.DATATYPE_MAPPINGS[datatype]["elasticsearchDatatype"]
      property_datatype["xsdDatatype"] = self.DATATYPE_MAPPINGS[datatype]["xsdDatatype"]
      if datatype == DATATYPE.TEXT:
        prop_def["isLangString"] = is_lang_string
    else:
      property_datatype["id"] += "concept"
      property_datatype["elasticsearchDatatype"] = "keyword"
      property_datatype["xsdDatatype"] = "string"
      property_datatype["range"] = str(datatype)
      prop_def["isNestedObject"] = is_nested
    return property_datatype

  def update_prop(self, class_name: str, prop_name: str, datatype, description: str, is_array: bool = False, is_nested: bool = False, is_lang_string: bool = True) -> None:
    self.delete_prop(class_name, prop_name)
    self.create_prop(class_name, prop_name, datatype, description, is_array, is_nested, is_lang_string)

  def rename_prop(self, class_name: str, old_prop_name: str, new_prop_name: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_prop(class_def["objectProperties"], old_prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{old_prop_name}' not found in class '{class_name}'")
    conflict_prop_def = self.find_prop(class_def["objectProperties"], new_prop_name)
    if conflict_prop_def is not None:
      raise PropertyExistsError(f"The new property name '{new_prop_name}' is already in use")
    prop_def["propertyName"] = new_prop_name
    if class_def["labelProperty"] == old_prop_name:
      class_def["labelProperty"] = new_prop_name

  def assign_prop_description(self, class_name: str, prop_name: str, description: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_prop(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
    prop_def['propertyDescription'] = description

  def change_prop_cardinality(self, class_name: str, prop_name: str, is_array: bool = False) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_prop(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
    prop_def["isArray"] = is_array

  def set_prop_filterability(self, class_name: str, prop_name: str, is_filterable: bool) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_prop(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
    prop_def["isFilterable"] = is_filterable

  def delete_prop(self, class_name: str, prop_name: str) -> None:
    class_def = self.find_class(class_name)
    if class_def is None:
      raise ClassNotFoundError(f"Class '{class_name}' not found")
    prop_def = self.find_prop(class_def["objectProperties"], prop_name)
    if prop_def is None:
      raise PropertyNotFoundError(f"Property '{prop_name}' not found in class '{class_name}'")
    class_def["objectProperties"].remove(prop_def)

  def find_class(self, name: str) -> Optional[dict]:
    return next((x for x in self._schema["classes"] if x['label'] == name), None)

  def find_subclasses(self, baseclass: str) -> list[dict]:
    return [x for x in self._schema["classes"] if x.get('parentClass') == baseclass]

  def find_prop(self, props: list, name: str) -> Optional[dict]:
    return next((x for x in props if x['propertyName'] == name), None)

  def assign_property_orders(self, property_orders: dict) -> None:
    for class_def in self._schema['classes']:
      if class_def['label'] in property_orders:
        for i, pname in enumerate(property_orders[class_def['label']]):
          prop = self.find_prop(class_def['objectProperties'], pname)
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

