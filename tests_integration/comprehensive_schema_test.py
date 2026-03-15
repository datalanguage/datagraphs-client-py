"""
Comprehensive schema integration test.

This script builds a schema that exercises every permutation of classes and
properties supported by the Schema class interface, writes it to the API,
reads it back, and saves both formats as test fixtures.

Permutations covered:
- Classes: simple, with description, with parent class (subClassOf), subclass (inherited props)
- Label properties: default "label", custom name, lang string vs non-lang string, autogen pattern
- Property datatypes: TEXT, DATE, DATETIME, BOOLEAN, DECIMAL, INTEGER, KEYWORD, URL, IMAGE_URL, ENUM
- ObjectProperty: reference to another class, nested object, inverse property
- Property flags: is_optional, is_array, is_lang_string, is_synonym, is_filterable
- Enum property with validation rules
- Property ordering
"""

import json
import os
import sys
import yaml

# Add parent dir so we can import datagraphs
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from datagraphs.client import Client as DatagraphsClient
from datagraphs.schema import Schema as DatagraphsSchema
from datagraphs.enums import DATATYPE

OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'temp')


def build_comprehensive_schema() -> DatagraphsSchema:
    """Build a schema that exercises all permutations of the Schema class interface."""

    schema = DatagraphsSchema(project='pydg', name='Comprehensive Test Model', version='1.0')

    # ---- Class 1: SimpleClass ----
    # Basic class with default label property ("label"), no description, no parent class
    schema.create_class("SimpleClass")

    # ---- Class 2: DescribedClass ----
    # Class with a description and custom label property name
    schema.create_class("DescribedClass", description="A class with a description", label_prop_name="name")

    # ---- Class 3: NonLangStringClass ----
    # Class with label property that is NOT a lang string
    schema.create_class("NonLangStringClass", label_prop_name="title", is_label_prop_lang_string=False)

    # ---- Class 4: AllDatatypes ----
    # Class with one property for each DATATYPE enum value
    schema.create_class("AllDatatypes", description="Class with all datatype properties", label_prop_name="name")
    schema.create_property("AllDatatypes", "textProp", DATATYPE.TEXT, description="A text property")
    schema.create_property("AllDatatypes", "textNonLang", DATATYPE.TEXT, description="A non-lang text property", is_lang_string=False)
    schema.create_property("AllDatatypes", "dateProp", DATATYPE.DATE, description="A date property")
    schema.create_property("AllDatatypes", "datetimeProp", DATATYPE.DATETIME, description="A datetime property")
    schema.create_property("AllDatatypes", "booleanProp", DATATYPE.BOOLEAN, description="A boolean property")
    schema.create_property("AllDatatypes", "decimalProp", DATATYPE.DECIMAL, description="A decimal property")
    schema.create_property("AllDatatypes", "integerProp", DATATYPE.INTEGER, description="An integer property")
    schema.create_property("AllDatatypes", "keywordProp", DATATYPE.KEYWORD, description="A keyword property")
    schema.create_property("AllDatatypes", "urlProp", DATATYPE.URL, description="A URL property")
    schema.create_property("AllDatatypes", "imageUrlProp", DATATYPE.IMAGE_URL, description="An image URL property")
    schema.create_property("AllDatatypes", "enumProp", DATATYPE.ENUM, description="An enum property",
                           enums=["OptionA", "OptionB", "OptionC"])

    # ---- Class 5: PropertyFlags ----
    # Class testing property flags: required, optional, array, filterable, synonym
    schema.create_class("PropertyFlags", description="Class testing property flags")
    schema.create_property("PropertyFlags", "requiredProp", DATATYPE.TEXT, description="A required property",
                           is_optional=False)
    schema.create_property("PropertyFlags", "optionalProp", DATATYPE.TEXT, description="An optional property",
                           is_optional=True)
    schema.create_property("PropertyFlags", "arrayProp", DATATYPE.TEXT, description="An array property",
                           is_array=True)
    schema.create_property("PropertyFlags", "filterableProp", DATATYPE.KEYWORD, description="A filterable property",
                           is_filterable=True)
    schema.create_property("PropertyFlags", "synonymProp", DATATYPE.TEXT, description="A synonym property",
                           is_synonym=True)
    schema.create_property("PropertyFlags", "requiredArrayProp", DATATYPE.INTEGER, description="A required array property",
                           is_optional=False, is_array=True)

    # ---- Class 6: ReferencedClass ----
    # A class that will be referenced via ObjectProperty by other classes
    schema.create_class("ReferencedClass", description="A class referenced by others", label_prop_name="name")
    schema.create_property("ReferencedClass", "code", DATATYPE.KEYWORD, description="A unique code")

    # ---- Class 7: ObjectPropertyClass ----
    # Class with ObjectProperty references (concept datatypes)
    schema.create_class("ObjectPropertyClass", description="Class with object properties", label_prop_name="name")
    schema.create_property("ObjectPropertyClass", "simpleRef", "ReferencedClass",
                           description="A simple object reference")
    schema.create_property("ObjectPropertyClass", "nestedRef", "ReferencedClass",
                           description="A nested object reference", is_nested=True)
    schema.create_property("ObjectPropertyClass", "arrayRef", "ReferencedClass",
                           description="An array of object references", is_array=True)
    schema.create_property("ObjectPropertyClass", "nestedArrayRef", "ReferencedClass",
                           description="An array of nested object references", is_array=True, is_nested=True)

    # ---- Class 8: InversePropertyClass ----
    # Class demonstrating inverse properties
    # First, add a property on ReferencedClass that points back to InversePropertyClass
    schema.create_class("InversePropertyClass", description="Class with inverse properties", label_prop_name="name")
    schema.create_property("InversePropertyClass", "ref", "ReferencedClass",
                           description="Reference to ReferencedClass")
    schema.create_property("ReferencedClass", "inverseRef", "InversePropertyClass",
                           description="Inverse reference back", inverse_of="ref")

    # ---- Class 9: ParentClass (for subclass testing) ----
    schema.create_class("ParentClass", description="A parent class", label_prop_name="name")
    schema.create_property("ParentClass", "sharedProp", DATATYPE.TEXT, description="Shared by subclasses")
    schema.create_property("ParentClass", "parentOnlyProp", DATATYPE.INTEGER, description="Only on parent")

    # ---- Class 10: ChildClass (subclass of ParentClass) ----
    schema.create_subclass("ChildClass", "A child class inheriting from ParentClass", "ParentClass")
    schema.create_property("ChildClass", "childOnlyProp", DATATYPE.KEYWORD, description="Only on child")

    # ---- Class 11: AutogenClass ----
    # Class with label autogeneration pattern
    schema.create_class("AutogenClass", description="Class with autogen label", label_prop_name="name")
    schema.create_property("AutogenClass", "code", DATATYPE.KEYWORD, description="A code value")
    schema.assign_label_autogen("AutogenClass", "{{ CONCATENATE(code, ' - autogen') }}")

    # ---- Apply property ordering ----
    schema.assign_property_orders({
        "AllDatatypes": ["name", "textProp", "textNonLang", "dateProp", "datetimeProp",
                        "booleanProp", "decimalProp", "integerProp", "keywordProp",
                        "urlProp", "imageUrlProp", "enumProp"],
        "PropertyFlags": ["label", "requiredProp", "optionalProp", "arrayProp",
                         "filterableProp", "synonymProp", "requiredArrayProp"],
    })

    return schema


def get_client(config_key: str = 'integration-testing') -> DatagraphsClient:
    """Create a client from the integration test config."""
    config_file_location = os.path.join(os.path.dirname(__file__), '.app.config.yml')
    with open(config_file_location, 'r') as config_file:
        configs = yaml.safe_load(config_file)
        if config_key not in configs:
            raise ValueError(f"Config key '{config_key}' not found. Available: {list(configs.keys())}")
        config = configs[config_key]
        return DatagraphsClient(
            project_name=config['project_name'],
            api_key=config['api_key'],
            client_id=config['client_id'],
            client_secret=config['client_secret']
        )


def main():
    print("Step 1: Building comprehensive schema...")
    schema = build_comprehensive_schema()
    old_format = schema.to_dict()

    # Save the old format (what Schema currently generates)
    old_output_path = os.path.join(OUTPUT_DIR, 'comprehensive_old_schema.json')
    with open(old_output_path, 'w', encoding='utf-8') as f:
        json.dump(old_format, f, ensure_ascii=False, indent=2)
    print(f"  Saved old format to {old_output_path}")
    print(f"  Classes: {len(old_format['classes'])}")
    for cls in old_format['classes']:
        print(f"    {cls['label']}: {len(cls['objectProperties'])} properties")

    print("\nStep 2: Writing schema to API...")
    client = get_client()
    client.tear_down()
    client.apply_schema(schema)
    print("  Schema applied successfully")

    print("\nStep 3: Reading schema back from API...")
    api_schema = client.get_schema()
    new_format = api_schema.to_dict()

    # Save the new format (what the API returns)
    new_output_path = os.path.join(OUTPUT_DIR, 'comprehensive_new_schema.json')
    with open(new_output_path, 'w', encoding='utf-8') as f:
        json.dump(new_format, f, ensure_ascii=False, indent=2)
    print(f"  Saved new format to {new_output_path}")
    print(f"  Classes: {len(new_format['classes'])}")
    for cls in new_format['classes']:
        print(f"    {cls.get('name', cls.get('label', '???'))}: {len(cls.get('properties', cls.get('objectProperties', [])))} properties")

    print("\nStep 4: Comparing class counts...")
    assert len(old_format['classes']) == len(new_format['classes']), \
        f"Class count mismatch: old={len(old_format['classes'])} new={len(new_format['classes'])}"
    print(f"  Both have {len(old_format['classes'])} classes ✓")

    print("\nStep 5: Comparing property counts per class...")
    old_classes_by_name = {c['label']: c for c in old_format['classes']}
    new_classes_by_name = {c.get('name', c.get('label')): c for c in new_format['classes']}
    for class_name in old_classes_by_name:
        old_prop_count = len(old_classes_by_name[class_name].get('objectProperties', []))
        new_cls = new_classes_by_name.get(class_name)
        if new_cls is None:
            print(f"  WARNING: Class '{class_name}' not found in API response")
            continue
        new_prop_count = len(new_cls.get('properties', new_cls.get('objectProperties', [])))
        status = "✓" if old_prop_count == new_prop_count else "✗"
        print(f"  {class_name}: old={old_prop_count} new={new_prop_count} {status}")

    print("\n✓ Done! Both formats saved to temp/")
    print(f"  Old format (current Schema output): {old_output_path}")
    print(f"  New format (API response):          {new_output_path}")


if __name__ == '__main__':
    main()
