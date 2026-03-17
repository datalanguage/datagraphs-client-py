# DataGraphs Python Client

A Python client library for the [DataGraphs](https://datagraphs.com) knowledge-graph API.

## Installation

```bash
pip install pydatagraphs
```

## Quick start

```python
from datagraphs import Client, Gateway, Schema

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
results = client.query(filters="type:Product", page_size=50)

# Read the project schema
schema = client.get_schema()

# Load / dump data via the Gateway
gateway = Gateway(client, schema)
gateway.dump_data("./backup")
gateway.load_data(from_dir_path="./backup")
```

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
| `Client` | Low-level HTTP client for the DataGraphs REST API |
| `Gateway` | High-level helper that loads/dumps JSON files to/from a project |
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

