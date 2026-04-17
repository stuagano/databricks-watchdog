# Watchdog — End-to-End Tests

These tests require a live Databricks workspace with a catalog the tests can
write to. They are skipped by default (see `conftest.py`) so CI never runs
them automatically.

## Running locally

```bash
export WATCHDOG_E2E_ENABLED=1
export WATCHDOG_TEST_CATALOG=<your_catalog>
export DATABRICKS_CONFIG_PROFILE=<your_profile>
pytest tests/e2e/ -v
```

## Running in Databricks

Deploy the bundle and trigger the `watchdog_e2e_test` job — it invokes
`engine/notebooks/run_e2e_tests.py` which is the same harness the pytest
entrypoint delegates to, just executed inside a workspace notebook.

```bash
databricks bundle run watchdog_e2e_test
```

## What's covered

| Fixture | Expected violations |
|---------|---------------------|
| `gold_clean` | *(none)* |
| `pii_no_steward` | `POL-SEC-003` |
| `untagged` | `POL-C001`, `POL-C003` |
