# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
adheres loosely to [Semantic Versioning](https://semver.org/spec/v2.0.0.html)
once it reaches 1.0.

## [Unreleased]

### Added

- `examples/mdm-entity-resolution/ctk/` and `caps/`: vendored the `ctk`
  anti-silent-failure test kit into the example, plus a `capabilities.yaml`
  with 3 proven `tier: cheap` capabilities (`matching-core-grounded-auto-match`,
  `mdm-checks-catch-real-defects`, `crosswalk-id-stability`) and a new
  `claim_vs_reality`-based regression test
  (`tests/test_matching_core_ctk.py`) encoding the exact grounding invariant
  that a live run once violated.
- `engine/src/watchdog/mdm_checks.py`: pure MDM data-quality check
  builders/interpreters (dedup, reconcile, completeness) for cross-table
  aggregate checks that don't fit the tag-based policy engine, plus unit
  tests (`tests/unit/test_mdm_checks.py`, 11 tests).
- `examples/mdm-entity-resolution/`: a worked example -- a Databricks-native
  batch fuzzy-match entity-resolution/MDM reference pipeline using
  `mdm_checks` as its quality gate. Verified live end to end (precision 1.0 /
  recall 1.0 against gold truth, all quality gates passing, id-stable across
  a re-run).
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
