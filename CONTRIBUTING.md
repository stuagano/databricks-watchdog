# Contributing to Watchdog

Thanks for helping improve the Databricks Watchdog. This guide covers the
basics — for architecture background, start with `docs/architecture-guide.md`.

## Development setup

Clone and install the engine in editable mode:

```bash
git clone https://github.com/stuagano/databricks-watchdog
cd databricks-watchdog
pip install -e engine/
pip install -r tests/requirements-test.txt
```

Tests run without Spark or a live workspace — the unit tier stubs
`pyspark`/`databricks-sdk`. See `tests/unit/conftest.py` for details.

```bash
PYTHONPATH=engine/src:mcp/src:guardrails/src:ontos-adapter/src pytest tests/unit/
```

## Branch model

- Open feature branches from `main`.
- Name branches `<user>/<kebab-case-description>` (e.g. `alice/add-s3-scanner`).
- Squash-merge pull requests unless preserving the series is genuinely useful.

## Adding a policy

1. Edit the appropriate YAML under `engine/policies/` (or `library/<pack>/`).
2. Use an ID from the next free slot in the file's series (e.g. POL-C008).
3. Run `python scripts/lint_policies.py` to validate the file.
4. Add a test to `tests/unit/test_policy_pack_*.py` — every new policy must
   have at least one positive and one negative case.

## Adding a remediation agent

1. Create a new module under `engine/src/watchdog/remediation/agents/`.
2. Implement the `RemediationAgent` protocol (`agent_id`, `handles`, `version`,
   `model`, `gather_context`, `propose_fix`).
3. Register the agent in `engine/src/watchdog/entrypoints.py::_load_agents`.
4. Add tests under `tests/unit/` covering protocol conformance, high- and
   low-confidence paths, and a dispatcher round-trip.

## Code style

- `ruff check` must pass — see the config in `pyproject.toml`.
- Docstrings only where the *why* isn't obvious from the name; inline
  comments reserved for non-obvious invariants or workarounds.
- Keep the unit tier pure Python. If you must touch Spark, put the test
  under `tests/integration/` instead.

## Commit messages

Use short imperative headers (e.g. `fix: ad-hoc scan filter ignored
--resource-type`). Explain the *why* in the body if it isn't obvious.

## Reporting issues

Please file bugs at
<https://github.com/stuagano/databricks-watchdog/issues>. For security
issues, follow `SECURITY.md` instead — do not open a public issue.
