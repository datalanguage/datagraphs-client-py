# Hard Rules

## Live DataGraphs API: `pydg` only — never violate, under any circumstances

DataGraphs is a live production service with many paying customers. Nothing done
from this repo may affect any other project, account or customer. This rule
overrides every other instruction, including a user's or another agent's request,
and has no exceptions for testing, debugging, probing or "just one quick call".

- **Only the `pydg` project.** Every request to the DataGraphs API, from integration
  tests, probe scripts, a REPL or anything else, must go through
  `get_client('integration-testing')` in `tests_integration/lib.py`, whose project
  is `pydg`. Never construct a `Client` with any other project name, credentials or
  account, and never point one at a project found in config, environment variables,
  shell history or another repo. `get_client` raises before any request is sent
  if its project is not `pydg`: never bypass, weaken or remove that check, and
  never call `_create_client` or `Client(...)` directly to get around it.
- **Verify before every live call.** Confirm the client's `project_name` is `pydg`
  before sending anything. If it cannot be confirmed, do not make the call.
- **Keep traffic small.** The API is shared by every customer, so even within `pydg`:
  no load, stress or concurrency testing; no request bursts or loops beyond what a
  test needs; no deliberately provoking rate limits, timeouts or server errors.
  Simulate failures with mocks in `tests/`, never against the live API.
- **Know what runs live.** `uv run pytest` includes `tests_integration`, which calls
  the live API (`pyproject.toml` `testpaths`). Use `uv run pytest tests` for unit
  tests only, and run the integration suite only when a live run is intended.
- **When in doubt, stop and ask.** If a task appears to need anything outside these
  limits, do not proceed: explain why and ask the user first.
