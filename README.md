# DataGraphs Python Client

Install uv as per https://docs.astral.sh/uv/getting-started/installation/, then:

* To create your virtual env: `uv sync`
* To run the tests: `uv run pytest --capture=tee-sys -v`
* To run the tests with coverage: `uv run pytest --cov=src --cov-report=html`
* To lint the codebase: `uvx ruff check .`

## TODO
1. Clean up property handling schema interface
1. Merge create cascading property
1. Add load() and dump() to client
1. Improve property order handling
1. Migrate to new schema structure

