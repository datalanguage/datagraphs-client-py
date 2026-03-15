"""Detailed comparison of old (Schema output) vs new (API response) format."""
import json

with open('temp/comprehensive_old_schema.json') as f:
    old = json.load(f)
with open('temp/comprehensive_new_schema.json') as f:
    new = json.load(f)

print("=== TOP-LEVEL KEYS ===")
print(f"Old: {sorted(old.keys())}")
print(f"New: {sorted(new.keys())}")
print(f"Extra in old: {set(old.keys()) - set(new.keys())}")
print(f"Extra in new: {set(new.keys()) - set(old.keys())}")

print("\n=== CLASS-LEVEL KEYS ===")
old_cls = old['classes'][0]  # SimpleClass
new_cls = next(c for c in new['classes'] if c['name'] == 'SimpleClass')
print(f"Old keys: {sorted(old_cls.keys())}")
print(f"New keys: {sorted(new_cls.keys())}")
print(f"Extra in old: {set(old_cls.keys()) - set(new_cls.keys())}")
print(f"Extra in new: {set(new_cls.keys()) - set(old_cls.keys())}")

print("\n=== CLASS WITH DESCRIPTION ===")
old_desc_cls = next(c for c in old['classes'] if c['label'] == 'DescribedClass')
new_desc_cls = next(c for c in new['classes'] if c['name'] == 'DescribedClass')
print(f"Old description: {repr(old_desc_cls['description'])}")
print(f"New description: {repr(new_desc_cls.get('description'))}")

print("\n=== CLASS WITHOUT DESCRIPTION ===")
print(f"Old SimpleClass description: {repr(old_cls.get('description'))}")
print(f"New SimpleClass description: {repr(new_cls.get('description'))}")

print("\n=== SUBCLASS ===")
old_child = next(c for c in old['classes'] if c['label'] == 'ChildClass')
new_child = next(c for c in new['classes'] if c['name'] == 'ChildClass')
print(f"Old parentClass: {old_child.get('parentClass')}")
print(f"Old parentClasses: {old_child.get('parentClasses')}")
print(f"New subClassOf: {new_child.get('subClassOf')}")

print("\n=== PROPERTY-LEVEL KEYS (DatatypeProperty) ===")
old_dt_prop = old_cls['objectProperties'][0]  # label prop
new_dt_prop = new_cls['properties'][0]        # label prop
print(f"Old keys: {sorted(old_dt_prop.keys())}")
print(f"New keys: {sorted(new_dt_prop.keys())}")
print(f"Extra in old: {set(old_dt_prop.keys()) - set(new_dt_prop.keys())}")
print(f"Extra in new: {set(new_dt_prop.keys()) - set(old_dt_prop.keys())}")

print("\n=== PROPERTY-LEVEL KEYS (ObjectProperty) ===")
old_obj_cls = next(c for c in old['classes'] if c['label'] == 'ObjectPropertyClass')
new_obj_cls = next(c for c in new['classes'] if c['name'] == 'ObjectPropertyClass')
old_obj_prop = next(p for p in old_obj_cls['objectProperties'] if p['propertyName'] == 'simpleRef')
new_obj_prop = next(p for p in new_obj_cls['properties'] if p['name'] == 'simpleRef')
print(f"Old keys: {sorted(old_obj_prop.keys())}")
print(f"New keys: {sorted(new_obj_prop.keys())}")
print(f"Extra in old: {set(old_obj_prop.keys()) - set(new_obj_prop.keys())}")
print(f"Extra in new: {set(new_obj_prop.keys()) - set(old_obj_prop.keys())}")

print("\n=== PROPERTY WITH DESCRIPTION ===")
old_alld = next(c for c in old['classes'] if c['label'] == 'AllDatatypes')
new_alld = next(c for c in new['classes'] if c['name'] == 'AllDatatypes')
old_text = next(p for p in old_alld['objectProperties'] if p['propertyName'] == 'textProp')
new_text = next(p for p in new_alld['properties'] if p['name'] == 'textProp')
print(f"Old propertyDescription: {repr(old_text.get('propertyDescription'))}")
print(f"New description: {repr(new_text.get('description'))}")

print("\n=== ENUM PROPERTY ===")
old_enum = next(p for p in old_alld['objectProperties'] if p['propertyName'] == 'enumProp')
new_enum = next(p for p in new_alld['properties'] if p['name'] == 'enumProp')
print(f"Old validationRules: {json.dumps(old_enum.get('validationRules'), indent=2)}")
print(f"New validationRules: {json.dumps(new_enum.get('validationRules'), indent=2)}")

print("\n=== INVERSE PROPERTY ===")
old_inv_cls = next(c for c in old['classes'] if c['label'] == 'ReferencedClass')
new_inv_cls = next(c for c in new['classes'] if c['name'] == 'ReferencedClass')
old_inv = next(p for p in old_inv_cls['objectProperties'] if p['propertyName'] == 'inverseRef')
new_inv = next(p for p in new_inv_cls['properties'] if p['name'] == 'inverseRef')
print(f"Old inverseOf: {old_inv.get('inverseOf')}")
print(f"New inverseOf: {new_inv.get('inverseOf')}")
print(f"New _inverseOf: {new_inv.get('_inverseOf')}")

print("\n=== LABEL AUTOGEN ===")
old_auto = next(c for c in old['classes'] if c['label'] == 'AutogenClass')
new_auto = next(c for c in new['classes'] if c['name'] == 'AutogenClass')
old_auto_label = next(p for p in old_auto['objectProperties'] if p['propertyName'] == 'name')
new_auto_label = next(p for p in new_auto['properties'] if p['name'] == 'name')
print(f"Old propertyValuePattern: {old_auto_label.get('propertyValuePattern')}")
print(f"New propertyValuePattern: {new_auto_label.get('propertyValuePattern')}")

print("\n=== isFilterable (old sends false as absent, new always present) ===")
old_simple_label = old_cls['objectProperties'][0]
new_simple_label = new_cls['properties'][0]
print(f"Old isFilterable: {old_simple_label.get('isFilterable', 'ABSENT')}")
print(f"New isFilterable: {new_simple_label.get('isFilterable', 'ABSENT')}")

print("\n=== isLabelSynonym (old sends false as absent, new always present) ===")
print(f"Old isLabelSynonym: {old_simple_label.get('isLabelSynonym', 'ABSENT')}")
print(f"New isLabelSynonym: {new_simple_label.get('isLabelSynonym', 'ABSENT')}")
