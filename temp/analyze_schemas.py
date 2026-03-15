import json

with open('temp/new_schema.json') as f:
    new_schema = json.load(f)

with open('temp/old_schema.json') as f:
    old_schema = json.load(f)

print("=== NEW SCHEMA ===")
print("Top keys:", list(new_schema.keys()))
print("Num classes:", len(new_schema['classes']))

print("\n--- subClassOf ---")
for c in new_schema['classes']:
    if 'subClassOf' in c:
        print(f"  {c['name']} extends {c['subClassOf']}")

print("\n--- inverseOf ---")
for c in new_schema['classes']:
    for p in c.get('properties', []):
        if 'inverseOf' in p:
            print(f"  {c['name']}.{p['name']} inverseOf={p['inverseOf']}")

print("\n--- validationRules (enums) ---")
for c in new_schema['classes']:
    for p in c.get('properties', []):
        if 'validationRules' in p:
            vals = p['validationRules'][0].get('value', [])[:3]
            print(f"  {c['name']}.{p['name']} enums={vals}...")

print("\n--- isAbstract=True ---")
for c in new_schema['classes']:
    if c.get('isAbstract'):
        print(f"  {c['name']}")

print("\n--- propertyValuePattern ---")
for c in new_schema['classes']:
    for p in c.get('properties', []):
        if 'propertyValuePattern' in p:
            print(f"  {c['name']}.{p['name']} -> {p['propertyValuePattern']}")

print("\n--- Sample ObjectProperty (nested) ---")
for c in new_schema['classes']:
    for p in c.get('properties', []):
        if p.get('type') == 'ObjectProperty' and p.get('isNestedObject'):
            print(json.dumps(p, indent=2))
            break
    else:
        continue
    break

print("\n--- Sample DatatypeProperty with description ---")
for c in new_schema['classes']:
    for p in c.get('properties', []):
        if p.get('type') == 'DatatypeProperty' and 'description' in p:
            print(json.dumps(p, indent=2))
            break
    else:
        continue
    break

print("\n--- All unique range values (DatatypeProperty) ---")
ranges = set()
for c in new_schema['classes']:
    for p in c.get('properties', []):
        if p.get('type') == 'DatatypeProperty':
            ranges.add(p.get('range', ''))
print(sorted(ranges))

print("\n=== OLD SCHEMA ===")
print("Top keys:", list(old_schema.keys()))
print("Num classes:", len(old_schema['classes']))

print("\n--- Sample old class (first) ---")
first_class = old_schema['classes'][0]
print(json.dumps({k:v for k,v in first_class.items() if k != 'objectProperties'}, indent=2))
print(f"  objectProperties count: {len(first_class.get('objectProperties', []))}")

print("\n--- Old class with parentClass ---")
for c in old_schema['classes']:
    if 'parentClass' in c:
        print(f"  {c['label']} parentClass={c['parentClass']} parentClasses={c.get('parentClasses')}")
        break

print("\n--- Old class with description (non-empty) ---")
for c in old_schema['classes']:
    if c.get('description') and len(c['description']) > 0:
        print(f"  {c['label']}: {c['description'][:60]}...")
        break

print("\n--- Old property with concept datatype ---")
for c in old_schema['classes']:
    for p in c.get('objectProperties', []):
        if p.get('propertyDatatype', {}).get('id', '').endswith('concept'):
            print(json.dumps(p, indent=2))
            break
    else:
        continue
    break
