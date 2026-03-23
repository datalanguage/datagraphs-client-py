# DataGraphs Python Client

Python client library for [DataGraphs](https://datagraphs.com).

## Installation

```bash
uv add pydatagraphs
```
or
```bash
pip install pydatagraphs
```

## Quick starts

```python
from datagraphs import Client

# Connect to a project
client = Client(
    project_name="my-project",
    api_key="your-api-key",
)

# Check the API is reachable
print(client.status())

# Retrieve all entities of a given type
products = client.get("Product")

# Query with filters and pagination
results = client.query(q="Acme", page_size=50)
```

```python
from datagraphs import Client, Gateway, Schema

# Connect to a project
client = Client(
    project_name="my-project",
    api_key="your-api-key",
)

# Read the project schema and dataset configurations
schema = client.get_schema()
schema = client.get_datasets()

# Update the project schema and dataset configurations
client.apply_schema(schema)
client.apply_datasets(datasets)
```

```python
from datagraphs import Client, Gateway, Dataset

# Connect to a project
client = Client(
    project_name="my-project",
    api_key="your-api-key",
)
gateway = Gateway(client, schema)

# Load / dump data via the Gateway
gateway.dump_data("./backup")
gateway.load_data(from_dir_path="./backup")

# Load / dump project via the Gateway
schema_output_path = "./schemas/"
datasets_output_path = "./datasets/"
gateway.dump_project(schema_output_path, datasets_output_path)

schema_data = load_json("./schemas/myproject-model-v1.0.json")
datasets_data = load_json("./datasets/myproject-datasets-v1.0.json")
schema = Schema.create_from(schema_data)
datasets = [Dataset.create_from(dataset) for dataset in datasets]
gateway.load_project(schema, datasets)

```
For full API documentation, please [see here](https://datalanguage.github.io/datagraphs-client-py)

## Authentication

For read-only access, an API key is sufficient. For write operations, supply OAuth credentials as well:

```python
client = Client(
    project_name="my-project",
    api_key="your-api-key",
    client_id="your-client-id",
    client_secret="your-client-secret",
)
```

## Key classes

| Class | Description |
|-------|-------------|
| `Client` | Lower-level HTTP client for the DataGraphs REST API |
| `Gateway` | Higher-level wrapper class for deploying projects and bulk export/load of data |
| `Schema` | In-memory representation of a project's domain model |
| `Dataset` | Represents a dataset within a project |

## Development

Install [uv](https://docs.astral.sh/uv/getting-started/installation/), then:

```bash
uv sync                                          # create virtualenv & install deps
uv run pytest tests -v                            # unit tests
uv run pytest tests_integration -v                # integration tests
uv run pytest tests -v --capture=tee-sys          # capturing stdout/stderr
uv run pytest --cov=src --cov-report=html         # coverage report
uvx ruff check .                                  # lint
```

## License

[MIT](LICENSE) — Copyright (c) 2026 Data Language

