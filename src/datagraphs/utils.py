"""URN parsing, project-name mapping, and schema transformation utilities."""

import re
import uuid
import datetime
from typing import Dict, List, Any, Union

URN_PATTERN = re.compile(
    r"^[Uu][Rr][Nn]:"                          # "urn:" prefix (case-insensitive)
    r"(?![Uu][Rr][Nn]-)"                       # NID cannot start with "urn-"
    r"[a-zA-Z0-9][a-zA-Z0-9-]{1,31}:"          # NID: 2-32 chars
    r"(?:[a-zA-Z0-9()+,\-.:=@;$_!*']"          # NSS trans characters
    r"|%[0-9A-Fa-f]{2})+"                      # or percent-encoded
    r"$"
)

def is_valid_urn(urn: str) -> bool:
    """Return ``True`` if *urn* matches the RFC 2141 URN syntax."""
    return URN_PATTERN.match(urn) is not None

def get_type_from_urn(urn: str) -> str:
    """Extract the type segment from a URN (between the second and last colons)."""
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    second_colon = urn.index(':', urn.index(':') + 1)
    last_colon = urn.rfind(':')
    return urn[second_colon + 1:last_colon]

def get_project_from_urn(urn: str) -> str:
    """Extract the project name from a URN (between the first and second colons)."""
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    first_colon = urn.index(':')
    second_colon = urn.index(':', first_colon + 1)
    return urn[first_colon + 1:second_colon]    

def get_id_from_urn(urn: str) -> str:
    """Extract the trailing identifier from a URN (after the last colon)."""
    if not is_valid_urn(urn):
        raise ValueError(f'Invalid URN: {urn}') 
    return urn[urn.rfind(':') + 1:]

def map_project_name(
    obj: Union[Dict, List, str, Any], 
    from_urn: str, 
    to_urn: str
) -> Union[Dict, List, str, Any]:
    """Recursively replace *from_urn* with *to_urn* in all string values of *obj*."""
    if isinstance(obj, dict):
        return {key: map_project_name(value, from_urn, to_urn) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [map_project_name(item, from_urn, to_urn) for item in obj]
    elif isinstance(obj, str) and obj.startswith(from_urn):
        return obj.replace(from_urn, to_urn)
    return obj


class SchemaTransformer:
    """Transforms schemas between legacy (old) and new format."""

    DATATYPE_MAPPINGS = {
        'text': {"id": "urn:datagraphs:datatypes:text", "elasticsearchDatatype": "text", "xsdDatatype": "string"},
        'date': {"id": "urn:datagraphs:datatypes:date", "elasticsearchDatatype": "date", "xsdDatatype": "date"},
        'datetime': {"id": "urn:datagraphs:datatypes:datetime", "elasticsearchDatatype": "dateTime", "xsdDatatype": "dateTime"},
        'boolean': {"id": "urn:datagraphs:datatypes:boolean", "elasticsearchDatatype": "boolean", "xsdDatatype": "boolean"},
        'decimal': {"id": "urn:datagraphs:datatypes:decimal", "elasticsearchDatatype": "double", "xsdDatatype": "decimal"},
        'integer': {"id": "urn:datagraphs:datatypes:integer", "elasticsearchDatatype": "long", "xsdDatatype": "integer"},
        'keyword': {"id": "urn:datagraphs:datatypes:keyword", "elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
        'url': {"id": "urn:datagraphs:datatypes:url", "elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
        'imageUrl': {"id": "urn:datagraphs:datatypes:imageUrl", "elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
        'enum': {"id": "urn:datagraphs:datatypes:enum", "elasticsearchDatatype": "keyword", "xsdDatatype": "string"},
    }

    @staticmethod
    def is_legacy_format(schema: dict) -> bool:
        """Return ``True`` if *schema* uses the legacy (old) format."""
        classes = schema.get('classes', [])
        if classes:
            first = classes[0]
            return 'objectProperties' in first or ('label' in first and 'type' not in first)
        return 'guid' in schema

    @classmethod
    def old_to_new(cls, schema: dict) -> dict:
        """Convert a legacy schema dict to the new format."""
        return {
            "name": schema.get("name", ""),
            "createdDate": schema.get("createdDate", ""),
            "lastModifiedDate": schema.get("lastModifiedDate", ""),
            "classes": [cls._convert_class_old_to_new(c) for c in schema.get("classes", [])],
        }

    @classmethod
    def new_to_old(cls, schema: dict) -> dict:
        """Convert a new-format schema dict to the legacy format."""
        guid = uuid.uuid4().hex
        model_id = f"urn:models:{guid}"
        return {
            "id": model_id,
            "guid": guid,
            "type": "DomainModel",
            "name": schema.get("name", ""),
            "description": "",
            "project": "",
            "createdDate": schema.get("createdDate", ""),
            "lastModifiedDate": schema.get("lastModifiedDate", ""),
            "classes": [cls._convert_class_new_to_old(c, model_id, i) for i, c in enumerate(schema.get("classes", []))],
        }

    # ------------------------------------------------------------------
    # old -> new helpers
    # ------------------------------------------------------------------

    @classmethod
    def _convert_class_old_to_new(cls, class_def: dict) -> dict:
        new_class: dict = {
            "type": "Class",
            "name": class_def["label"],
        }
        description = class_def.get("description", "")
        if description:
            new_class["description"] = {"en": description, "@none": description}
        new_class["labelProperty"] = class_def.get("labelProperty", "label")
        new_class["identifierProperty"] = class_def.get("identifierProperty", "id")
        new_class["properties"] = [cls._convert_property_old_to_new(p) for p in class_def.get("objectProperties", [])]
        parent_class = class_def.get("parentClass")
        if parent_class:
            new_class["subClassOf"] = parent_class
        new_class["isAbstract"] = False
        return new_class

    @classmethod
    def _convert_property_old_to_new(cls, prop_def: dict) -> dict:
        property_datatype = prop_def.get("propertyDatatype", {})
        is_concept = property_datatype.get("id", "").endswith("concept")

        if is_concept:
            new_prop: dict = {
                "type": "ObjectProperty",
                "name": prop_def["propertyName"],
            }
        else:
            new_prop = {
                "type": "DatatypeProperty",
                "name": prop_def["propertyName"],
            }

        description = prop_def.get("propertyDescription", "")
        if description:
            new_prop["description"] = {"en": description, "@none": description}

        new_prop["range"] = property_datatype.get("range", property_datatype.get("label", ""))
        new_prop["isOptional"] = prop_def.get("isOptional", True)
        new_prop["isArray"] = prop_def.get("isArray", False)

        if is_concept:
            new_prop["isNestedObject"] = prop_def.get("isNestedObject", False)
            if "inverseOf" in prop_def:
                new_prop["inverseOf"] = prop_def["inverseOf"]
            new_prop["inferLocation"] = False
            new_prop["isLabelSynonym"] = prop_def.get("isLabelSynonym", False)
            new_prop["isFilterable"] = prop_def.get("isFilterable", False)
            new_prop["isSymmetric"] = False
        else:
            new_prop["isLangString"] = prop_def.get("isLangString", False)
            new_prop["isLabelSynonym"] = prop_def.get("isLabelSynonym", False)
            new_prop["isFilterable"] = prop_def.get("isFilterable", False)

        if "propertyValuePattern" in prop_def:
            new_prop["propertyValuePattern"] = prop_def["propertyValuePattern"]
        if "validationRules" in prop_def:
            new_prop["validationRules"] = [
                {"type": r.get("id", "").split(":")[-1] if "id" in r else r.get("type", ""), "value": r.get("value", [])}
                for r in prop_def["validationRules"]
            ]
        return new_prop

    # ------------------------------------------------------------------
    # new -> old helpers
    # ------------------------------------------------------------------

    @classmethod
    def _convert_class_new_to_old(cls, class_def: dict, model_id: str, order: int) -> dict:
        class_name = class_def["name"]
        guid = uuid.uuid4().hex
        now = datetime.datetime.now(datetime.UTC).isoformat()

        parent_classes = [class_name]
        parent_class = class_def.get("subClassOf")
        if parent_class:
            parent_classes.append(parent_class)

        desc = class_def.get("description")
        description_text = ""
        if isinstance(desc, dict):
            description_text = desc.get("@none", desc.get("en", ""))
        elif isinstance(desc, str):
            description_text = desc

        old_class: dict = {
            "id": f"{model_id}:classes:{class_name}",
            "guid": guid,
            "model": model_id,
            "project": "",
            "label": class_name,
            "labelProperty": class_def.get("labelProperty", "label"),
            "createdDate": now,
            "lastModifiedDate": now,
            "parentClasses": parent_classes,
            "identifierProperty": class_def.get("identifierProperty", "id"),
            "description": description_text,
            "objectProperties": [
                cls._convert_property_new_to_old(p, model_id, class_name, i)
                for i, p in enumerate(class_def.get("properties", []))
            ],
        }
        if parent_class:
            old_class["parentClass"] = parent_class
        return old_class

    @classmethod
    def _convert_property_new_to_old(cls, prop_def: dict, model_id: str, class_name: str, order: int) -> dict:
        prop_name = prop_def["name"]
        guid = uuid.uuid4().hex
        is_object = prop_def.get("type") == "ObjectProperty"

        range_value = prop_def.get("range", "")
        if is_object:
            datatype_id = "urn:datagraphs:datatypes:concept"
            mapping = cls.DATATYPE_MAPPINGS.get(range_value, {"elasticsearchDatatype": "keyword", "xsdDatatype": "string"})
            property_datatype = {
                "id": datatype_id,
                "range": range_value,
                "type": "PropertyDatatype",
                "label": range_value,
                "elasticsearchDatatype": "keyword",
                "xsdDatatype": "string",
            }
        else:
            mapping = cls.DATATYPE_MAPPINGS.get(range_value, {"id": f"urn:datagraphs:datatypes:{range_value}", "elasticsearchDatatype": "", "xsdDatatype": ""})
            property_datatype = {
                "id": mapping["id"],
                "type": "PropertyDatatype",
                "label": range_value,
                "elasticsearchDatatype": mapping["elasticsearchDatatype"],
                "xsdDatatype": mapping["xsdDatatype"],
            }

        desc = prop_def.get("description")
        description_text = ""
        if isinstance(desc, dict):
            description_text = desc.get("@none", desc.get("en", ""))
        elif isinstance(desc, str):
            description_text = desc

        old_prop: dict = {
            "propertyName": prop_name,
            "isOptional": prop_def.get("isOptional", True),
            "isArray": prop_def.get("isArray", False),
            "propertyDatatype": property_datatype,
            "propertyOrder": order,
            "isNestedObject": prop_def.get("isNestedObject", False),
            "guid": guid,
            "id": f"{model_id}:classes:{class_name}:{prop_name}",
            "propertyDescription": description_text,
        }
        if prop_def.get("isLangString"):
            old_prop["isLangString"] = True
        if prop_def.get("isLabelSynonym"):
            old_prop["isLabelSynonym"] = True
        if prop_def.get("isFilterable"):
            old_prop["isFilterable"] = True
        if "inverseOf" in prop_def:
            old_prop["inverseOf"] = prop_def["inverseOf"]
        if "propertyValuePattern" in prop_def:
            old_prop["propertyValuePattern"] = prop_def["propertyValuePattern"]
        if "validationRules" in prop_def:
            old_prop["validationRules"] = [
                {"id": f"urn:datagraphs:validation:{r.get('type', '')}", "value": r.get("value", [])}
                for r in prop_def["validationRules"]
            ]
        return old_prop
