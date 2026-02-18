# DataGraphs Python Client

Install uv as per https://docs.astral.sh/uv/getting-started/installation/ then:

* To create your virtual env: `uv sync`
* To run the units tests: `uv run pytest tests --capture=tee-sys -v`
* To run the integration tests: `uv run pytest tests_integration -v`
* To run the tests with coverage: `uv run pytest --cov=src --cov-report=html`
* To lint the codebase: `uvx ruff check .`

## TODO
1. Clean up property handling schema interface - done
1. Merge create cascading property - done
1. Add standard/common project data load and dump logic
1. Improve property order handling
1. Add more integration tests
1. Migrate to new schema structure

