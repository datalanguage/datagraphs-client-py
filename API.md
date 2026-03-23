# API Documentation

Complete reference for the `pydatagraphs` library — a Python client for the [DataGraphs](https://datagraphs.com) knowledge-graph API.

---

## Table of Contents

- [Client](#client)
- [Schema](#schema)
- [Dataset](#dataset)
- [Gateway](#gateway)
- [Enums](#enums)
- [Utility Functions](#utility-functions)
- [Exceptions](#exceptions)

---

## Client

```python
from datagraphs import Client
```

The `Client` class provides low-level HTTP access to the DataGraphs REST API, handling authentication, pagination, and batched writes.

### Constructor

```python
Client(
    project_name: str,
    api_key: str,
    client_id: str = "",
    client_secret: str = "",
    batch_size: int = 100,
    service_url: str = "https://api.datagraphs.io/"
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project_name` | `str` | *(required)* | Name of the DataGraphs project to connect to. |
| `api_key` | `str` | *(required)* | API key for authentication. Sufficient for read-only access. |
| `client_id` | `str` | `""` | OAuth client ID. Required (along with `client_secret`) for write operations. |
| `client_secret` | `str` | `""` | OAuth client secret. Required (along with `client_id`) for write operations. |
| `batch_size` | `int` | `100` | Number of entities per page/batch when reading or writing data. |
| `service_url` | `str` | `"https://api.datagraphs.io/"` | Base URL of the DataGraphs API. |

**Example — read-only client:**

```python
client = Client(project_name="my-project", api_key="your-api-key")
```

**Example — read/write client with OAuth:**

```python
client = Client(
    project_name="my-project",
    api_key="your-api-key",
    client_id="your-client-id",
    client_secret="your-client-secret",
)
```

---

### Properties

#### `wait_time_ms` → `int`

Returns the current delay (in milliseconds) between paginated API calls.

```python
delay = client.wait_time_ms
```

---

### Methods

#### `set_wait_time(wait_time_ms: int) → None`

Set the delay between successive paginated API requests.

| Parameter | Type | Description |
|-----------|------|-------------|
| `wait_time_ms` | `int` | Wait time in milliseconds. |

```python
client.set_wait_time(500)
```

---

#### `status() → str`

Check the API service status.

**Returns:** The API status string (e.g. `"OK"`), or `"unknown"` if unavailable.

```python
status = client.status()
print(status)  # "OK"
```

---

#### `get(class_name: str, lang: str = "all", include_date_fields: bool = False) → list[dict]`

Retrieve all entities of a given type. Automatically paginates through all results using the configured `batch_size`.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | *(required)* | The entity class/type to fetch. |
| `lang` | `str` | `"all"` | Language code to filter results. Use `"all"` for all languages. |
| `include_date_fields` | `bool` | `False` | Whether to include system date metadata (`createdDate`, `lastModifiedDate`). |

**Returns:** A list of entity dictionaries.

```python
products = client.get("Product")
french_products = client.get("Product", lang="fr")
products_with_dates = client.get("Product", include_date_fields=True)
```

---

#### `query(...) → list[dict] | tuple[list[dict], list[dict]]`

Execute a search query against the API with filters, facets, sorting, pagination, and more.

```python
query(
    dataset: str = "_all",
    q: str = "",
    filters: str = "",
    facets: str = "",
    facet_size: int = -1,
    date_facets: str = "",
    fields: str = "",
    embed: str = "",
    sort: str = "",
    ids: str = "",
    lang: str = "all",
    page_no: int = -1,
    page_size: int = -1,
    previous_page_token: str = "",
    next_page_token: str = "",
    include_date_fields: bool = False,
) → list[dict] | tuple[list[dict], list[dict]]
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `dataset` | `str` | `"_all"` | Dataset slug to query. Use `"_all"` to query across all datasets. |
| `q` | `str` | `""` | Free-text search query string. |
| `filters` | `str` | `""` | Type/field filter (e.g. `"type:Person"`). |
| `facets` | `str` | `""` | Comma-separated facet field names (e.g. `"condition,intervention"`). |
| `facet_size` | `int` | `-1` | Number of facet values to return. Defaults to `10` when facets are requested. |
| `date_facets` | `str` | `""` | Date facet specification (e.g. `"publishedDate:1d:1w:1M"`). |
| `fields` | `str` | `""` | Comma-separated field names to return (e.g. `"name,age"`). |
| `embed` | `str` | `""` | Embedding depth for related entities (e.g. `"2"`). |
| `sort` | `str` | `""` | Sort expression (e.g. `"label:asc"`). |
| `ids` | `str` | `""` | Comma-separated entity IDs to fetch directly. |
| `lang` | `str` | `"all"` | Language code for results. |
| `page_no` | `int` | `-1` | Page number for offset-based pagination. |
| `page_size` | `int` | `-1` | Number of results per page. Defaults to the client's `batch_size`. |
| `previous_page_token` | `str` | `""` | Token for cursor-based backward pagination. |
| `next_page_token` | `str` | `""` | Token for cursor-based forward pagination. |
| `include_date_fields` | `bool` | `False` | Include system date metadata in results. |

**Returns:**
- A `list[dict]` of results when no facets are requested.
- A `tuple[list[dict], list[dict]]` of `(results, facets)` when facets are requested.

```python
# Simple text search
results = client.query(q="Aspirin")

# Filtered query against a specific dataset
results = client.query(dataset="chemicals", filters="type:Substance")

# Paginated query
results = client.query(q="test", page_no=2, page_size=25)

# Token-based pagination
results = client.query(q="test", next_page_token="abc123")

# Faceted search
results, facets = client.query(
    facets="condition,intervention",
    facet_size=20,
)

# Fetch specific entities by ID
results = client.query(ids="urn:proj:Type:1,urn:proj:Type:2")

# Sort and select fields
results = client.query(sort="label:asc", fields="name,age")

# Include embedded related entities
results = client.query(embed="2")
```

---

#### `put(dataset: str, data: dict | list[dict]) → int`

Load entities into a dataset. Automatically batches large payloads according to the configured `batch_size`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset` | `str` | Target dataset slug. |
| `data` | `dict \| list[dict]` | A single entity dict or a list of entity dicts. |

**Returns:** The number of entities loaded.

```python
# Load a single entity
client.put("chemicals", {"id": "urn:proj:Substance:1", "label": "Aspirin"})

# Load a batch of entities
entities = [{"id": f"urn:proj:Substance:{i}", "label": f"Entity {i}"} for i in range(200)]
count = client.put("chemicals", entities)
print(f"Loaded {count} entities")
```

---

#### `delete(class_name: str, entity_id: str) → None`

Delete a single entity by class and ID.

| Parameter | Type | Description |
|-----------|------|-------------|
| `class_name` | `str` | The entity class/type. |
| `entity_id` | `str` | The entity identifier. |

```python
client.delete("Substance", "urn:proj:Substance:1")
```

---

#### `get_schema() → Schema`

Retrieve the active schema (domain model) for the project.

**Returns:** A `Schema` object.

```python
schema = client.get_schema()
print(schema.to_json())
```

---

#### `apply_schema(schema: Schema) → None`

Apply a schema to the project, replacing the currently active domain model.

| Parameter | Type | Description |
|-----------|------|-------------|
| `schema` | `Schema` | The schema to deploy. |

```python
schema = Schema(name="My Model", version="2.0")
schema.create_class("Product")
client.apply_schema(schema)
```

---

#### `get_datasets() → list[Dataset]`

Retrieve all datasets configured in the project.

**Returns:** A list of `Dataset` objects.

```python
datasets = client.get_datasets()
for ds in datasets:
    print(ds.name, ds.classes)
```

---

#### `apply_datasets(datasets: list[Dataset], timeout_ms: int = 30000) → None`

Create or update datasets so they match the supplied list. New datasets are created; existing datasets with changes are updated. Waits for confirmation from the API that all datasets have been applied.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `datasets` | `list[Dataset]` | *(required)* | Datasets to apply. |
| `timeout_ms` | `int` | `30000` | Maximum time (ms) to wait for the API to confirm all datasets are applied. |

**Raises:** `DatagraphsError` if datasets are not applied within the timeout.

```python
datasets = [
    Dataset(name="Chemicals", project="my-project", classes=["Substance", "Formulation"]),
    Dataset(name="Documents", project="my-project", classes=["Report"]),
]
client.apply_datasets(datasets)
```

---

#### `create_dataset(dataset: Dataset) → None`

Create a single new dataset.

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset` | `Dataset` | The dataset to create. |

```python
ds = Dataset(name="Chemicals", project="my-project", classes=["Substance"])
client.create_dataset(ds)
```

---

#### `update_dataset(dataset: Dataset) → None`

Update an existing dataset.

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset` | `Dataset` | The dataset to update (matched by slug). |

```python
ds.classes = ["Substance", "Formulation"]
client.update_dataset(ds)
```

---

#### `clear_dataset(dataset_slug: str) → None`

Delete all data from a dataset, keeping the dataset itself intact.

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset_slug` | `str` | The slug of the dataset to clear. |

```python
client.clear_dataset("chemicals")
```

---

#### `drop_dataset(dataset_slug: str) → None`

Drop a dataset and all of its data entirely.

| Parameter | Type | Description |
|-----------|------|-------------|
| `dataset_slug` | `str` | The slug of the dataset to drop. |

```python
client.drop_dataset("chemicals")
```

---

#### `tear_down(drop_datasets: bool = True) → None`

Remove all datasets and their data from the project.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `drop_datasets` | `bool` | `True` | If `True`, drops each dataset entirely. If `False`, only clears the data from each dataset. |

```python
# Hard teardown — drop everything
client.tear_down()

# Soft teardown — clear data but keep dataset definitions
client.tear_down(drop_datasets=False)
```

---

## Schema

```python
from datagraphs import Schema
```

The `Schema` class is an in-memory representation of a DataGraphs domain model. It provides a builder-style API for creating and modifying classes and their properties.

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `Schema.ALL_CLASSES` | `"__all_classes__"` | Sentinel value used to indicate all classes (e.g. in `Gateway.load_data`). |

### Constructor

```python
Schema(name: str = "", version: str = "")
```

Creates a new empty schema.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | `""` | Model name. Defaults to `"Domain Model"` if empty. |
| `version` | `str` | `""` | Schema version. Defaults to `"1.0"` if empty. |

The resulting schema has a `name` field of `"{name} v{version}"`.

```python
schema = Schema()                                    # "Domain Model v1.0"
schema = Schema(name="My Model", version="2.0")      # "My Model v2.0"
```

> **Note:** Do not pass a `dict` to the constructor. Use `Schema.create_from()` instead.

---

### Static Methods

#### `Schema.create_from(data: dict, version: str = "") → Schema`

Create a `Schema` instance from a dictionary. Automatically detects and converts legacy (old) format schemas to the new format.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `data` | `dict` | *(required)* | Schema dictionary (new or legacy format). |
| `version` | `str` | `""` | Schema version override. |

**Returns:** A new `Schema` instance.

**Raises:** `SchemaError` if the dict is missing required keys (`name`, `createdDate`, `lastModifiedDate`, `classes`).

```python
# From a new-format dict
schema = Schema.create_from({
    "name": "Domain Model",
    "createdDate": "2024-06-01T00:00:00Z",
    "lastModifiedDate": "2024-06-01T00:00:00Z",
    "classes": []
})

# From a legacy-format dict (auto-converted)
schema = Schema.create_from(legacy_schema_dict)

# From a JSON file
import json
with open("schema.json") as f:
    schema = Schema.create_from(json.load(f))
```

---

### Properties

#### `classes` → `list[dict]`

Returns the list of class definitions in the schema.

```python
for cls in schema.classes:
    print(cls["name"])
```

#### `version` → `str`

Returns the schema version string.

```python
print(schema.version)  # "1.0"
```

---

### Class Methods

#### `create_class(class_name, description, parent_class_name, label_prop_name, is_label_prop_lang_string) → None`

Create a new class in the schema.

```python
create_class(
    class_name: str,
    description: str = "",
    parent_class_name: str = "",
    label_prop_name: str = "label",
    is_label_prop_lang_string: bool = True,
) → None
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | *(required)* | Name of the new class. |
| `description` | `str` | `""` | Human-readable description. |
| `parent_class_name` | `str` | `""` | Name of the parent class (for inheritance). |
| `label_prop_name` | `str` | `"label"` | Name of the label property created by default. |
| `is_label_prop_lang_string` | `bool` | `True` | Whether the label property supports multiple languages. |

**Raises:** `SchemaError` if a class with the same name already exists.

```python
schema.create_class("Product", description="A commercial product")
schema.create_class("Drug", parent_class_name="Product", label_prop_name="drugName")
```

---

#### `create_subclass(class_name: str, description: str, parent_class_name: str) → None`

Create a subclass that inherits all properties from the specified parent class.

| Parameter | Type | Description |
|-----------|------|-------------|
| `class_name` | `str` | Name of the new subclass. |
| `description` | `str` | Description for the subclass. |
| `parent_class_name` | `str` | Name of the parent class to inherit from. |

**Raises:** `ClassNotFoundError` if the parent class doesn't exist.

```python
schema.create_class("Product")
schema.create_property("Product", "price", DATATYPE.DECIMAL)
schema.create_subclass("Drug", "A pharmaceutical product", "Product")
# Drug now has both "label" and "price" properties
```

---

#### `update_class(class_name: str, new_name: str = "", new_description: str = "", parent_class_name: str = "") → None`

Update a class's name, description, or parent class.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | *(required)* | Current class name. |
| `new_name` | `str` | `""` | New class name (rename). Empty to leave unchanged. |
| `new_description` | `str` | `""` | New description. Empty to leave unchanged. |
| `parent_class_name` | `str` | `""` | New parent class. Empty string removes the parent. |

**Raises:** `ClassNotFoundError` if the class doesn't exist.

```python
schema.update_class("Product", new_name="Item", new_description="A catalog item")
```

---

#### `delete_class(class_name: str, include_linked_properties: bool = False, cascade_to_subclasses: bool = True) → None`

Delete a class from the schema.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | *(required)* | Name of the class to delete. |
| `include_linked_properties` | `bool` | `False` | If `True`, also removes ObjectProperties on other classes that reference this class. |
| `cascade_to_subclasses` | `bool` | `True` | If `True`, removes `subClassOf` links from any subclasses of the deleted class. |

**Raises:** `ClassNotFoundError` if the class doesn't exist.

```python
schema.delete_class("Product")
schema.delete_class("Product", include_linked_properties=True)
```

---

#### `assign_label_property(class_name: str, prop_name: str, is_lang_string: bool = True) → None`

Designate an existing property as the label property for a class. The property is also marked as required (`isOptional=False`).

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | *(required)* | Class name. |
| `prop_name` | `str` | *(required)* | Property name to use as the label. |
| `is_lang_string` | `bool` | `True` | Whether the label supports multiple languages. |

**Raises:** `ClassNotFoundError` if the class doesn't exist. `PropertyNotFoundError` if the property doesn't exist on the class.

```python
schema.create_property("Product", "productName", DATATYPE.TEXT)
schema.assign_label_property("Product", "productName")
```

---

#### `assign_label_autogen(class_name: str, pattern: str) → None`

Set an auto-generation pattern on the label property of a class.

| Parameter | Type | Description |
|-----------|------|-------------|
| `class_name` | `str` | Class name. |
| `pattern` | `str` | Auto-generation expression (e.g. `"{{ CONCATENATE('hello', ' ', 'world') }}"`). |

**Raises:** `ClassNotFoundError` if the class doesn't exist. `PropertyNotFoundError` if the label property doesn't exist.

```python
schema.assign_label_autogen("Product", "{{ CONCATENATE(brandName, ' ', genericName) }}")
```

---

#### `assign_baseclass(class_name: str, parent_class_name: str) → None`

Set or change the parent (base) class for an existing class.

| Parameter | Type | Description |
|-----------|------|-------------|
| `class_name` | `str` | The class to modify. |
| `parent_class_name` | `str` | The new parent class name. |

**Raises:** `ClassNotFoundError` if `class_name` doesn't exist.

```python
schema.assign_baseclass("Drug", parent_class_name="Product")
```

---

#### `assign_class_description(class_name: str, description: str) → None`

Set or clear the description of a class.

| Parameter | Type | Description |
|-----------|------|-------------|
| `class_name` | `str` | Class name. |
| `description` | `str` | New description. Pass an empty string to remove the description. |

**Raises:** `ClassNotFoundError` if the class doesn't exist.

```python
schema.assign_class_description("Product", "A commercial product")
schema.assign_class_description("Product", "")  # removes description
```

---

### Property Methods

#### `create_property(...) → None`

Create a new property on a class.

```python
create_property(
    class_name: str,
    prop_name: str,
    datatype: DATATYPE | str,
    description: str = "",
    is_optional: bool = True,
    is_array: bool = False,
    is_nested: bool = False,
    is_lang_string: bool = True,
    inverse_of: str = "",
    enums: list | None = None,
    is_synonym: bool = False,
    is_filterable: bool = False,
    apply_to_subclasses: bool = False,
) → None
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | *(required)* | Class to add the property to. |
| `prop_name` | `str` | *(required)* | Property name. |
| `datatype` | `DATATYPE \| str` | *(required)* | Property data type. Use a `DATATYPE` enum value for primitive types, or a class name string for object (relationship) properties. |
| `description` | `str` | `""` | Human-readable description. |
| `is_optional` | `bool` | `True` | Whether the property is optional. |
| `is_array` | `bool` | `False` | Whether the property holds multiple values. |
| `is_nested` | `bool` | `False` | Whether an object property is nested (embedded). |
| `is_lang_string` | `bool` | `True` | For text properties, whether to support multiple languages. |
| `inverse_of` | `str` | `""` | Name of the inverse property on the target class (object properties only). |
| `enums` | `list \| None` | `None` | Allowed values for `DATATYPE.ENUM` properties. |
| `is_synonym` | `bool` | `False` | Whether this property is a label synonym (included in search). |
| `is_filterable` | `bool` | `False` | Whether the property is available as a facet/filter. |
| `apply_to_subclasses` | `bool` | `False` | If `True`, also creates the property on all existing subclasses. |

**Raises:**
- `ClassNotFoundError` if the class (or a referenced class for object properties) doesn't exist.
- `PropertyExistsError` if a property with the same name already exists on the class.
- `InvalidInversePropertyError` if the inverse property specification is invalid.

```python
from datagraphs.enums import DATATYPE

# Simple text property
schema.create_property("Product", "description", DATATYPE.TEXT)

# Required integer property
schema.create_property("Product", "quantity", DATATYPE.INTEGER, is_optional=False)

# Array property
schema.create_property("Product", "tags", DATATYPE.KEYWORD, is_array=True)

# Enum property with fixed values
schema.create_property("Product", "status", DATATYPE.ENUM, enums=["Active", "Inactive", "Pending"])

# Object (relationship) property
schema.create_property("Product", "manufacturer", "Company")

# Nested object property
schema.create_property("Product", "details", "ProductDetail", is_nested=True)

# Inverse property (bidirectional relationship)
schema.create_property("Company", "products", "Product")
schema.create_property("Product", "madeBy", "Company", inverse_of="products")

# Property applied to all subclasses
schema.create_property("Product", "sku", DATATYPE.KEYWORD, apply_to_subclasses=True)
```

---

#### `update_property(...) → None`

Update an existing property on a class. Only the parameters that are explicitly provided (non-`None`) will be changed.

```python
update_property(
    class_name: str,
    prop_name: str,
    datatype: DATATYPE | str = None,
    description: str = None,
    is_optional: bool = None,
    is_array: bool = None,
    is_nested: bool = None,
    is_lang_string: bool = None,
    inverse_of: str = "",
    enums: list | None = None,
    is_synonym: bool = False,
    is_filterable: bool = None,
    apply_to_subclasses: bool = None,
) → None
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | *(required)* | Class containing the property. |
| `prop_name` | `str` | *(required)* | Property name to update. |
| *(others)* | | | Same as `create_property`. Only non-`None` values are applied. |

**Raises:** `ClassNotFoundError` if the class doesn't exist. `PropertyNotFoundError` if the property doesn't exist.

```python
schema.update_property("Product", "description", description="Updated description text")
schema.update_property("Product", "quantity", is_optional=False, is_filterable=True)
schema.update_property("Product", "status", datatype=DATATYPE.ENUM, enums=["Active", "Archived"])
schema.update_property("Product", "tags", is_array=True, apply_to_subclasses=True)
```

---

#### `rename_property(class_name: str, old_prop_name: str, new_prop_name: str) → None`

Rename a property. If the property is the class's label property, the label property reference is also updated.

| Parameter | Type | Description |
|-----------|------|-------------|
| `class_name` | `str` | Class containing the property. |
| `old_prop_name` | `str` | Current property name. |
| `new_prop_name` | `str` | New property name. |

**Raises:**
- `ClassNotFoundError` if the class doesn't exist.
- `PropertyNotFoundError` if the old property name doesn't exist.
- `PropertyExistsError` if the new property name is already in use.

```python
schema.rename_property("Product", "desc", "description")
```

---

#### `delete_property(class_name: str, prop_name: str) → None`

Remove a property from a class.

| Parameter | Type | Description |
|-----------|------|-------------|
| `class_name` | `str` | Class containing the property. |
| `prop_name` | `str` | Property name to delete. |

**Raises:** `ClassNotFoundError` if the class doesn't exist. `PropertyNotFoundError` if the property doesn't exist.

```python
schema.delete_property("Product", "obsoleteField")
```

---

### Query Methods

#### `find_class(name: str) → dict | None`

Find a class definition by name.

**Returns:** The class dict, or `None` if not found.

```python
cls = schema.find_class("Product")
if cls:
    print(cls["name"], cls["labelProperty"])
```

---

#### `find_subclasses(baseclass: str) → list[dict]`

Find all direct subclasses of a given class.

**Returns:** A list of class dicts whose `subClassOf` matches the given name.

```python
subclasses = schema.find_subclasses("Product")
for sub in subclasses:
    print(sub["name"])
```

---

#### `find_property(props: list, name: str) → dict | None`

Find a property by name within a list of property dicts.

**Returns:** The property dict, or `None` if not found.

```python
cls = schema.find_class("Product")
prop = schema.find_property(cls["properties"], "price")
if prop:
    print(prop["range"])  # e.g. "decimal"
```

---

### Ordering

#### `assign_property_orders(property_orders: dict) → None`

Reorder properties within classes. Properties not listed in the order are appended at the end.

| Parameter | Type | Description |
|-----------|------|-------------|
| `property_orders` | `dict` | A dict mapping class names to ordered lists of property names. |

```python
schema.assign_property_orders({
    "Product": ["label", "sku", "price", "description"],
    "Company": ["label", "website"],
})
```

---

### Serialisation

#### `clone() → Schema`

Create a deep copy of the schema. Changes to the clone do not affect the original.

```python
copy = schema.clone()
copy.update_class("Product", new_description="Modified")
# original schema is unchanged
```

---

#### `to_dict() → dict`

Convert the schema to a plain dictionary.

```python
data = schema.to_dict()
```

---

#### `to_json() → str`

Serialise the schema to a JSON string.

```python
json_str = schema.to_json()
with open("schema.json", "w") as f:
    f.write(json_str)
```

---

## Dataset

```python
from datagraphs import Dataset
```

The `Dataset` class represents a dataset within a DataGraphs project. Each dataset contains a set of entity classes and has an auto-generated URN identifier.

### Constructor

```python
Dataset(
    name: str,
    project: str,
    account: str = "",
    is_private: bool = True,
    is_restricted: bool = False,
    classes: list[str] | None = None,
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `name` | `str` | *(required)* | Dataset name. |
| `project` | `str` | *(required)* | Project name. |
| `account` | `str` | `""` | Account name. |
| `is_private` | `bool` | `True` | Whether the dataset is private. |
| `is_restricted` | `bool` | `False` | Whether the dataset has restricted access. |
| `classes` | `list[str] \| None` | `None` | List of class names belonging to this dataset. |

```python
ds = Dataset(
    name="Chemicals",
    project="my-project",
    classes=["Substance", "Formulation"],
)
```

---

### Properties

| Property | Type | Writable | Description |
|----------|------|----------|-------------|
| `name` | `str` | No | Dataset name. |
| `account` | `str` | No | Account name. |
| `project` | `str` | No | Project name. |
| `is_private` | `bool` | Yes | Whether the dataset is private. |
| `is_restricted` | `bool` | Yes | Whether the dataset has restricted access. |
| `classes` | `list[str]` | Yes | Class names in the dataset. |
| `id` | `str` | No | Auto-generated URN (e.g. `"urn:my-project:chemicals"`). |
| `slug` | `str` | No | The slug portion of the ID (e.g. `"chemicals"`). |

```python
ds = Dataset(name="Test Dataset", project="my-project")
print(ds.id)    # "urn:my-project:test-dataset"
print(ds.slug)  # "test-dataset"

ds.classes = ["TypeA", "TypeB"]
ds.is_private = False
```

---

### Static Methods

#### `Dataset.create_from(data: dict) → Dataset`

Create a `Dataset` instance from a dictionary (e.g. as returned by the API).

| Parameter | Type | Description |
|-----------|------|-------------|
| `data` | `dict` | Dictionary with dataset fields. |

```python
ds = Dataset.create_from({
    "name": "Chemicals",
    "account": "acme",
    "project": "my-project",
    "isPrivate": True,
    "isRestricted": False,
    "classes": ["Substance", "Formulation"]
})
```

---

### Methods

#### `to_dict() → dict`

Convert the dataset to a dictionary suitable for API requests.

```python
data = ds.to_dict()
# {
#     "name": "Chemicals",
#     "account": "acme",
#     "project": "my-project",
#     "isPrivate": True,
#     "isRestricted": False,
#     "classes": ["Substance", "Formulation"],
#     "type": "DataSet",
#     "namespace": "urn:my-project:chemicals",
#     "id": "urn:my-project:chemicals",
# }
```

---

### Equality

Two `Dataset` instances are equal if their `to_dict()` representations are identical.

```python
ds1 = Dataset(name="Test", project="proj", classes=["A"])
ds2 = Dataset(name="Test", project="proj", classes=["A"])
assert ds1 == ds2
```

---

## Gateway

```python
from datagraphs import Gateway
```

The `Gateway` class is a higher-level wrapper around `Client` that provides workflows for deploying projects and bulk import/export of data.

### Constructor

```python
Gateway(client: Client, wait_time_ms: int = 200)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `client` | `Client` | *(required)* | A configured `Client` instance. |
| `wait_time_ms` | `int` | `200` | Delay in milliseconds between successive API calls. |

```python
client = Client(project_name="my-project", api_key="key")
gateway = Gateway(client, wait_time_ms=500)
```

---

### Properties

#### `client` → `Client`

Returns the underlying `Client` instance.

---

### Methods

#### `load_project(schema: Schema, datasets: list[Dataset], validation_mode: VALIDATION_MODE = VALIDATION_MODE.PROMPT) → None`

Deploy a full project (schema + datasets) to the API. This tears down existing datasets before applying the new schema and datasets.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `schema` | `Schema` | *(required)* | The schema to deploy. |
| `datasets` | `list[Dataset]` | *(required)* | The datasets to deploy. |
| `validation_mode` | `VALIDATION_MODE` | `VALIDATION_MODE.PROMPT` | How to handle mismatches between deployment and existing datasets. |

Validation modes:
- `PROMPT` — Warns on mismatches and asks the user for confirmation via `input()`.
- `NO_PROMPT` — Warns on mismatches but continues without prompting.
- `BYPASS` — Skips all validations.

**Raises:** `ValueError` if duplicate class names are found across datasets.

```python
from datagraphs.enums import VALIDATION_MODE

gateway.load_project(schema, datasets, validation_mode=VALIDATION_MODE.NO_PROMPT)
```

---

#### `dump_project(schema_path: str | Path, datasets_path: str | Path) → None`

Export the project's schema and dataset configurations to JSON files on disk. Files are named `{project_name}-v{version}-schema.json` and `{project_name}-v{version}-datasets.json`.

| Parameter | Type | Description |
|-----------|------|-------------|
| `schema_path` | `str \| Path` | Directory to write the schema JSON file. |
| `datasets_path` | `str \| Path` | Directory to write the datasets JSON file. |

```python
gateway.dump_project("./schemas", "./datasets")
# Creates e.g. ./schemas/my-project-v1.0-schema.json
#               ./datasets/my-project-v1.0-datasets.json
```

---

#### `load_data(class_name: str = Schema.ALL_CLASSES, from_dir_path: str | Path = "", file_path: str | Path = "") → dict`

Load entity data from JSON files into the project.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `class_name` | `str` | `Schema.ALL_CLASSES` | Class to load, or `Schema.ALL_CLASSES` to load all non-base classes across all datasets. |
| `from_dir_path` | `str \| Path` | `""` | Directory containing `<ClassName>.json` files. |
| `file_path` | `str \| Path` | `""` | Explicit path to a single JSON file (used with a specific `class_name`). |

**Returns:** A dict with `loaded` and `skipped` counts.

```python
# Load all classes from a directory
stats = gateway.load_data(from_dir_path="./data")
print(f"Loaded: {stats['loaded']}, Skipped: {stats['skipped']}")

# Load a specific class from a specific file
stats = gateway.load_data(class_name="Substance", file_path="./data/Substance.json")

# Load a specific class by convention (looks for <class_name>.json in from_dir_path)
stats = gateway.load_data(class_name="Substance", from_dir_path="./data")
```

> **Note:** Base classes (those with subclasses) are automatically skipped when loading all classes — their data is expected to be loaded via the subclasses.

---

#### `dump_data(to_dir_path: str | Path, class_name: str = Schema.ALL_CLASSES, include_date_fields: bool = False) → dict`

Export entity data from the project to JSON files on disk. Each class is written to a separate `<ClassName>.json` file.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `to_dir_path` | `str \| Path` | *(required)* | Target directory. Created automatically if it doesn't exist. |
| `class_name` | `str` | `Schema.ALL_CLASSES` | Class to dump, or `Schema.ALL_CLASSES` for all. |
| `include_date_fields` | `bool` | `False` | Whether to include system date metadata. |

**Returns:** A dict with an `exported` count.

```python
# Dump all data
stats = gateway.dump_data("./backup")
print(f"Exported: {stats['exported']}")

# Dump a specific class
stats = gateway.dump_data("./backup", class_name="Substance")

# Include system date fields
stats = gateway.dump_data("./backup", include_date_fields=True)
```

> **Note:** Base classes are automatically skipped — only leaf classes are exported.

---

#### `clear_down() → None`

Clear all data from all datasets in the project (datasets themselves are preserved).

```python
gateway.clear_down()
```

---

## Enums

```python
from datagraphs.enums import DATATYPE, VALIDATION_MODE
```

### DATATYPE

Enumeration of property data types available in a schema.

| Value | String | Description |
|-------|--------|-------------|
| `DATATYPE.TEXT` | `"text"` | Free text (supports multi-language). |
| `DATATYPE.DATE` | `"date"` | Date value. |
| `DATATYPE.DATETIME` | `"datetime"` | Date and time value. |
| `DATATYPE.BOOLEAN` | `"boolean"` | Boolean value. |
| `DATATYPE.DECIMAL` | `"decimal"` | Floating-point number. |
| `DATATYPE.INTEGER` | `"integer"` | Whole number. |
| `DATATYPE.KEYWORD` | `"keyword"` | Exact-match keyword string. |
| `DATATYPE.URL` | `"url"` | URL string. |
| `DATATYPE.IMAGE_URL` | `"imageUrl"` | Image URL string. |
| `DATATYPE.ENUM` | `"enum"` | Enumerated value (use with `enums` parameter). |

### VALIDATION_MODE

Enumeration of validation modes for project loading.

| Value | String | Description |
|-------|--------|-------------|
| `VALIDATION_MODE.PROMPT` | `"prompt"` | Warn on mismatches and ask user for confirmation. |
| `VALIDATION_MODE.NO_PROMPT` | `"no-prompt"` | Warn on mismatches but continue without prompting. |
| `VALIDATION_MODE.BYPASS` | `"bypass"` | Skip all dataset validations. |

---

## Utility Functions

```python
from datagraphs.utils import (
    is_valid_urn,
    get_type_from_urn,
    get_project_from_urn,
    get_id_from_urn,
    map_project_name,
    SchemaTransformer,
)
```

#### `is_valid_urn(urn: str) → bool`

Check if a string is a valid URN (per RFC 2141 syntax).

```python
is_valid_urn("urn:project:Type:123")  # True
is_valid_urn("not-a-urn")             # False
```

---

#### `get_type_from_urn(urn: str) → str`

Extract the type segment from a URN (the segment between the second and last colons).

```python
get_type_from_urn("urn:project:Substance:abc")  # "Substance"
```

**Raises:** `ValueError` if the URN is invalid.

---

#### `get_project_from_urn(urn: str) → str`

Extract the project name from a URN (the segment between the first and second colons).

```python
get_project_from_urn("urn:my-project:Type:123")  # "my-project"
```

**Raises:** `ValueError` if the URN is invalid.

---

#### `get_id_from_urn(urn: str) → str`

Extract the trailing identifier from a URN (after the last colon).

```python
get_id_from_urn("urn:project:Type:abc123")  # "abc123"
```

**Raises:** `ValueError` if the URN is invalid.

---

#### `map_project_name(obj, from_urn: str, to_urn: str) → dict | list | str | Any`

Recursively replace a URN prefix in all string values of a nested structure (dicts, lists, strings).

| Parameter | Type | Description |
|-----------|------|-------------|
| `obj` | `dict \| list \| str \| Any` | The object to process. |
| `from_urn` | `str` | URN prefix to replace. |
| `to_urn` | `str` | Replacement URN prefix. |

```python
entity = {"id": "urn:old-project:Type:123", "ref": "urn:old-project:Other:456"}
mapped = map_project_name(entity, "urn:old-project", "urn:new-project")
# {"id": "urn:new-project:Type:123", "ref": "urn:new-project:Other:456"}
```

---

### SchemaTransformer

A utility class for converting schemas between legacy and new formats.

#### `SchemaTransformer.is_legacy_format(schema: dict) → bool`

Detect whether a schema dict uses the legacy format.

```python
SchemaTransformer.is_legacy_format({"guid": "abc", "classes": []})  # True
SchemaTransformer.is_legacy_format({"classes": [{"type": "Class"}]})  # False
```

---

#### `SchemaTransformer.old_to_new(schema: dict) → dict`

Convert a legacy-format schema dict to the new format.

```python
new_schema = SchemaTransformer.old_to_new(legacy_schema)
```

---

#### `SchemaTransformer.new_to_old(schema: dict) → dict`

Convert a new-format schema dict to the legacy format.

```python
old_schema = SchemaTransformer.new_to_old(new_schema)
```

---

## Exceptions

All exceptions are importable from `datagraphs`:

```python
from datagraphs import AuthenticationError, DatagraphsError
from datagraphs.schema import SchemaError, ClassNotFoundError, PropertyNotFoundError, PropertyExistsError, InvalidInversePropertyError
```

| Exception | Parent | Description |
|-----------|--------|-------------|
| `DatagraphsError` | `Exception` | Base exception for all client errors. |
| `AuthenticationError` | `DatagraphsError` | Authentication/authorisation failure (e.g. invalid credentials, expired token after max retries). |
| `SchemaError` | `Exception` | Base exception for schema-related errors (e.g. invalid schema structure, duplicate class names). |
| `ClassNotFoundError` | `SchemaError` | A referenced class was not found in the schema. |
| `PropertyNotFoundError` | `SchemaError` | A referenced property was not found on a class. |
| `PropertyExistsError` | `SchemaError` | Attempted to create a property that already exists. |
| `InvalidInversePropertyError` | `SchemaError` | An inverse property specification is invalid (wrong type, missing, or range mismatch). |
