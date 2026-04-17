# Watchdog — Operational Scripts

Helper scripts for deploying, linting, and operating Watchdog.

| Script | Purpose |
|--------|---------|
| `lint_policies.py` | Validate every YAML file under `engine/policies/` and `library/` — referenced by CI. |
| `deploy.sh` | Thin wrapper around `databricks bundle deploy` that targets the right workspace profile per environment. |
| `sync_policies.sh` | Sync YAML policies to the Delta `policies` table for an existing target without redeploying the whole bundle. |
| `seed_fixtures.sh` | Populate the E2E fixture tables for local smoke-testing. |
| `export_ontology.py` | Write the watchdog ontology out as an OWL/Turtle file for Ontos import. |

Run everything from the repo root; the scripts expect relative paths from there.
