# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres loosely to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches 1.0.

## [Unreleased]

### Added

- Continuous integration workflow (`.github/workflows/ci.yml`) running unit
  tests across Python 3.10–3.12, ruff linting, and policy-YAML linting.
- `scripts/` helpers: `lint_policies.py`, `deploy.sh`, `sync_policies.sh`,
  `seed_fixtures.sh`, `export_ontology.py`.
- Webhook notification channel in `watchdog.notifications` supporting
  generic, Slack, and Teams payloads. The `notify` entrypoint now dispatches
  to webhooks when `notification_webhook_url` is present in the secret scope.
- Remediation agents: `ClusterTaggerAgent` (POL-C002/C003/C004),
  `JobOwnerAgent` (POL-C001), `DQMonitorScaffoldAgent` (POL-Q001).
- Remediation pipeline entrypoints: `remediate`, `apply_remediations`,
  `verify_remediations` — all wired into `setup.py`, `run_task.py`, and a
  new `watchdog_remediation_pipeline` DABs job.
- `watchdog_multi_metastore_scan` job so `crawl_all_metastores` is
  deployable.
- End-to-end test harness: `engine/notebooks/run_e2e_tests.py`,
  `watchdog_e2e_test` job, and opt-in pytest tier at `tests/e2e/`.
- Unit tests for `config.py`, `notifications.py`, `policy_loader.py`,
  `ontology_export.py`, entrypoints, and the new agents (+88 tests).
- `CONTRIBUTING.md`, `CHANGELOG.md`, `SECURITY.md`, root `pyproject.toml`.

### Changed

- `ResourceCrawler.crawl_all()` now accepts `resource_types` and
  `resource_id` filters. The ad-hoc scan entrypoint uses them so
  `--resource-type` / `--resource-id` are no longer no-ops.
- `tests/unit/conftest.py` installs a shared pyspark/databricks-sdk stub so
  the unit tier can import production modules without the real packages.
