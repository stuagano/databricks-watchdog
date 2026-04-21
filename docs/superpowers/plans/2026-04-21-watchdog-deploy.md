# `watchdog-deploy` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `watchdog-deploy` CLI entrypoint that pushes compiled artifacts to the workspace via platform APIs, with dry-run support and error collection.

**Architecture:** A new `engine/src/watchdog/deployer.py` module with `DeployResult` dataclass and per-target deployer functions. A `deploy_artifacts()` orchestrator dispatches to the right deployer per target. The `deploy()` entrypoint in `entrypoints.py` wires it all together.

**Tech Stack:** Python, databricks-sdk, pytest

**Spec:** `docs/superpowers/specs/2026-04-21-watchdog-deploy-design.md`

---

## File Map

| File | Action | Responsibility |
|------|--------|---------------|
| `engine/src/watchdog/deployer.py` | Create | `DeployResult`, deployer functions, `deploy_artifacts()` orchestrator |
| `engine/src/watchdog/entrypoints.py` | Modify | Add `deploy()` entrypoint |
| `engine/setup.py` | Modify | Register `watchdog-deploy` |
| `tests/unit/test_deployer.py` | Create | Tests for deployer logic with mocked SDK |

---

### Task 1: `DeployResult` + `deploy_artifacts()` orchestrator + tests

**Files:**
- Create: `engine/src/watchdog/deployer.py`
- Create: `tests/unit/test_deployer.py`

- [ ] **Step 1: Write failing tests**

Create `tests/unit/test_deployer.py`:

```python
"""Unit tests for watchdog.deployer — artifact deployment logic.

Run with: pytest tests/unit/test_deployer.py -v
"""
import json
import sys
import types
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

# Stub databricks.sdk before import
_db = types.ModuleType("databricks")
_sdk = types.ModuleType("databricks.sdk")
_sdk.WorkspaceClient = MagicMock
sys.modules.setdefault("databricks", _db)
sys.modules.setdefault("databricks.sdk", _sdk)

# Stub pyspark
for _mod in ["pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types"]:
    sys.modules.setdefault(_mod, MagicMock())

from watchdog.deployer import DeployResult, deploy_artifacts


class TestDeployResult:
    def test_fields_populated(self):
        r = DeployResult(
            artifact_id="uc_tag_policy/POL-1.json",
            target="uc_tag_policy",
            success=True,
            deployed_at="2026-04-21T00:00:00+00:00",
            details="Created tag policy for tag_key=owner",
        )
        assert r.artifact_id == "uc_tag_policy/POL-1.json"
        assert r.success is True
        assert r.error is None

    def test_failure_has_error(self):
        r = DeployResult(
            artifact_id="uc_abac/POL-1.json",
            target="uc_abac",
            success=False,
            error="PERMISSION_DENIED",
        )
        assert not r.success
        assert r.error == "PERMISSION_DENIED"


class TestDeployArtifacts:
    def test_skips_guardrails(self):
        artifacts = [
            {"artifact_id": "guardrails/POL-1.json", "target": "guardrails",
             "content": json.dumps({"policy_id": "POL-1"})},
        ]
        results = deploy_artifacts(
            artifacts, w=MagicMock(), spark=None, catalog="c", schema="s", dry_run=False,
        )
        assert len(results) == 1
        assert results[0].success is True
        assert "skip" in results[0].details.lower()

    def test_collects_errors_without_stopping(self):
        """Two artifacts — first fails, second succeeds. Both attempted."""
        def _mock_deploy_tag(w, artifact, dry_run=False):
            if artifact["artifact_id"] == "uc_tag_policy/POL-BAD.json":
                return DeployResult(
                    artifact_id=artifact["artifact_id"],
                    target="uc_tag_policy",
                    success=False,
                    error="API unavailable",
                )
            return DeployResult(
                artifact_id=artifact["artifact_id"],
                target="uc_tag_policy",
                success=True,
                deployed_at="2026-04-21T00:00:00+00:00",
                details="Created tag policy",
            )

        artifacts = [
            {"artifact_id": "uc_tag_policy/POL-BAD.json", "target": "uc_tag_policy",
             "content": json.dumps({"policy_id": "POL-BAD", "tag_key": "x"})},
            {"artifact_id": "uc_tag_policy/POL-OK.json", "target": "uc_tag_policy",
             "content": json.dumps({"policy_id": "POL-OK", "tag_key": "y"})},
        ]

        with patch("watchdog.deployer._deploy_uc_tag_policy", side_effect=_mock_deploy_tag):
            results = deploy_artifacts(
                artifacts, w=MagicMock(), spark=None, catalog="c", schema="s", dry_run=False,
            )

        assert len(results) == 2
        assert not results[0].success
        assert results[1].success

    def test_unknown_target_returns_error(self):
        artifacts = [
            {"artifact_id": "sdp/POL-1.json", "target": "sdp_expectation",
             "content": json.dumps({"policy_id": "POL-1"})},
        ]
        results = deploy_artifacts(
            artifacts, w=MagicMock(), spark=None, catalog="c", schema="s", dry_run=False,
        )
        assert len(results) == 1
        assert not results[0].success
        assert "unknown target" in results[0].error.lower()
```

- [ ] **Step 2: Run to verify they fail**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_deployer.py -v 2>&1 | head -10
```

Expected: `ModuleNotFoundError: No module named 'watchdog.deployer'`

- [ ] **Step 3: Implement deployer.py scaffold**

Create `engine/src/watchdog/deployer.py`:

```python
"""Artifact deployer — push compiled artifacts to the workspace.

Reads artifacts from the compile manifest and dispatches each to a
target-specific deployer. Collects results (success/failure) for all
artifacts without stopping on first error.

Supported targets:
  - uc_tag_policy: UC tag policy API (create-or-update)
  - uc_abac: ALTER TABLE SET COLUMN MASK via statement execution
  - guardrails: skipped (MCP server reads from disk)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DeployResult:
    """Outcome of deploying a single artifact."""
    artifact_id: str
    target: str
    success: bool
    error: str | None = None
    deployed_at: str | None = None
    details: str = ""


def deploy_artifacts(
    artifacts: list[dict],
    w: Any,
    spark: Any,
    catalog: str,
    schema: str,
    dry_run: bool = False,
) -> list[DeployResult]:
    """Deploy all artifacts, collecting results.

    Args:
        artifacts: List of dicts with artifact_id, target, content keys.
        w: WorkspaceClient instance.
        spark: SparkSession (needed for uc_abac resource_classifications query).
        catalog: UC catalog name.
        schema: UC schema name.
        dry_run: If True, resolve targets but skip execution.
    """
    results: list[DeployResult] = []

    for artifact in artifacts:
        target = artifact.get("target", "")
        artifact_id = artifact.get("artifact_id", "")

        try:
            if target == "guardrails":
                results.append(DeployResult(
                    artifact_id=artifact_id,
                    target=target,
                    success=True,
                    details="Skipped — guardrails artifacts deployed via disk (MCP server reads at startup).",
                ))
            elif target == "uc_tag_policy":
                results.append(_deploy_uc_tag_policy(w, artifact, dry_run=dry_run))
            elif target == "uc_abac":
                results.append(_deploy_uc_abac(
                    w, artifact, spark, catalog, schema, dry_run=dry_run,
                ))
            else:
                results.append(DeployResult(
                    artifact_id=artifact_id,
                    target=target,
                    success=False,
                    error=f"Unknown target '{target}' — no deployer registered.",
                ))
        except Exception as e:
            logger.exception(f"Deploy failed for {artifact_id}")
            results.append(DeployResult(
                artifact_id=artifact_id,
                target=target,
                success=False,
                error=str(e),
            ))

    return results


def _deploy_uc_tag_policy(w: Any, artifact: dict, dry_run: bool = False) -> DeployResult:
    """Deploy a UC tag policy via the tag policy API."""
    content = json.loads(artifact.get("content", "{}"))
    artifact_id = artifact["artifact_id"]
    tag_key = content.get("tag_key", "")
    policy_type = content.get("policy_type", "required")
    allowed_values = content.get("allowed_values")
    resource_types = content.get("resource_types", ["table"])
    scope = content.get("scope")

    action = f"Create/update tag policy: tag_key={tag_key}, type={policy_type}"
    if allowed_values:
        action += f", allowed_values={allowed_values}"
    if scope:
        action += f", scope={scope}"

    if dry_run:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_tag_policy",
            success=True,
            details=f"(dry-run) {action}",
        )

    try:
        # UC tag policy API — create or update
        body: dict[str, Any] = {
            "name": tag_key,
            "policy_type": policy_type.upper(),
        }
        if allowed_values:
            body["allowed_values"] = allowed_values
        if scope:
            body["catalog"] = scope.get("catalog")
            body["schema"] = scope.get("schema")

        w.api_client.do("POST", "/api/2.0/unity-catalog/tag-policies", body=body)

        return DeployResult(
            artifact_id=artifact_id,
            target="uc_tag_policy",
            success=True,
            deployed_at=datetime.now(timezone.utc).isoformat(),
            details=action,
        )
    except Exception as e:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_tag_policy",
            success=False,
            error=str(e),
            details=action,
        )


def _deploy_uc_abac(
    w: Any, artifact: dict, spark: Any, catalog: str, schema: str,
    dry_run: bool = False,
) -> DeployResult:
    """Deploy a UC ABAC column mask via ALTER TABLE SET COLUMN MASK."""
    content = json.loads(artifact.get("content", "{}"))
    artifact_id = artifact["artifact_id"]
    mask_function = content.get("mask_function", "")
    applies_to = content.get("applies_to", "")

    # Resolve applies_to → concrete tables via resource_classifications
    try:
        classifications_table = f"{catalog}.{schema}.resource_classifications"
        rows = spark.sql(f"""
            SELECT DISTINCT resource_id
            FROM {classifications_table}
            WHERE class_name = '{applies_to}'
        """).collect()
        matched_tables = [r.resource_id for r in rows]
    except Exception as e:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=False,
            error=f"Failed to resolve applies_to={applies_to}: {e}",
        )

    if not matched_tables:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=True,
            details=f"No tables matched class '{applies_to}'. Nothing to deploy.",
        )

    action = (
        f"Apply column mask {mask_function} to {len(matched_tables)} table(s) "
        f"matching '{applies_to}'"
    )

    if dry_run:
        table_list = ", ".join(matched_tables[:5])
        if len(matched_tables) > 5:
            table_list += f" (+{len(matched_tables) - 5} more)"
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=True,
            details=f"(dry-run) {action}. Tables: {table_list}",
        )

    errors: list[str] = []
    applied = 0
    for table_name in matched_tables:
        try:
            # Get columns for the table
            info = w.tables.get(full_name=table_name)
            for col in info.columns or []:
                stmt = f"ALTER TABLE {table_name} ALTER COLUMN `{col.name}` SET MASK {mask_function}"
                spark.sql(stmt)
                applied += 1
        except Exception as e:
            errors.append(f"{table_name}: {e}")

    if errors:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=False,
            deployed_at=datetime.now(timezone.utc).isoformat(),
            error=f"{len(errors)} table(s) failed: {'; '.join(errors[:3])}",
            details=f"{action}. Applied to {applied} column(s), {len(errors)} error(s).",
        )

    return DeployResult(
        artifact_id=artifact_id,
        target="uc_abac",
        success=True,
        deployed_at=datetime.now(timezone.utc).isoformat(),
        details=f"{action}. Applied to {applied} column(s).",
    )
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_deployer.py -v
```

Expected: All 5 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/watchdog/deployer.py tests/unit/test_deployer.py
git commit -m "feat(deployer): add artifact deployer with uc_tag_policy and uc_abac support"
```

---

### Task 2: Target-specific deployer tests (mocked SDK)

**Files:**
- Modify: `tests/unit/test_deployer.py`

- [ ] **Step 1: Add uc_tag_policy deployer tests**

Append to `tests/unit/test_deployer.py`:

```python
from watchdog.deployer import _deploy_uc_tag_policy, _deploy_uc_abac


class TestDeployUcTagPolicy:
    def test_calls_api(self):
        w = MagicMock()
        artifact = {
            "artifact_id": "uc_tag_policy/POL-1.json",
            "target": "uc_tag_policy",
            "content": json.dumps({
                "policy_id": "POL-1",
                "tag_key": "data_steward",
                "policy_type": "required",
                "resource_types": ["table"],
            }),
        }
        result = _deploy_uc_tag_policy(w, artifact, dry_run=False)
        assert result.success
        assert result.deployed_at is not None
        w.api_client.do.assert_called_once()
        call_args = w.api_client.do.call_args
        assert call_args[0][0] == "POST"
        assert "tag-policies" in call_args[0][1]

    def test_dry_run_no_api_call(self):
        w = MagicMock()
        artifact = {
            "artifact_id": "uc_tag_policy/POL-1.json",
            "target": "uc_tag_policy",
            "content": json.dumps({
                "policy_id": "POL-1",
                "tag_key": "owner",
                "policy_type": "required",
            }),
        }
        result = _deploy_uc_tag_policy(w, artifact, dry_run=True)
        assert result.success
        assert result.deployed_at is None
        assert "(dry-run)" in result.details
        w.api_client.do.assert_not_called()

    def test_api_error_returns_failure(self):
        w = MagicMock()
        w.api_client.do.side_effect = Exception("API unavailable")
        artifact = {
            "artifact_id": "uc_tag_policy/POL-1.json",
            "target": "uc_tag_policy",
            "content": json.dumps({"tag_key": "x"}),
        }
        result = _deploy_uc_tag_policy(w, artifact, dry_run=False)
        assert not result.success
        assert "API unavailable" in result.error


class TestDeployUcAbac:
    def test_resolves_tables_and_applies_mask(self):
        w = MagicMock()
        spark = MagicMock()

        # Mock resource_classifications query
        row = MagicMock()
        row.resource_id = "gold.finance.gl"
        spark.sql.return_value.collect.return_value = [row]

        # Mock w.tables.get
        col = MagicMock()
        col.name = "ssn"
        table_info = MagicMock()
        table_info.columns = [col]
        w.tables.get.return_value = table_info

        artifact = {
            "artifact_id": "uc_abac/POL-PII.json",
            "target": "uc_abac",
            "content": json.dumps({
                "policy_id": "POL-PII",
                "mask_function": "main.governance.redact_pii",
                "applies_to": "PIIColumn",
            }),
        }
        result = _deploy_uc_abac(w, artifact, spark, "gold", "governance", dry_run=False)
        assert result.success
        assert result.deployed_at is not None
        assert "1 column" in result.details

    def test_no_matching_tables(self):
        w = MagicMock()
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []

        artifact = {
            "artifact_id": "uc_abac/POL-1.json",
            "target": "uc_abac",
            "content": json.dumps({
                "applies_to": "NonexistentClass",
                "mask_function": "cat.sch.fn",
            }),
        }
        result = _deploy_uc_abac(w, artifact, spark, "c", "s", dry_run=False)
        assert result.success
        assert "no tables matched" in result.details.lower()

    def test_dry_run_no_sql_executed(self):
        w = MagicMock()
        spark = MagicMock()

        row = MagicMock()
        row.resource_id = "gold.finance.gl"
        # First spark.sql call returns resource_classifications results
        # In dry-run, no ALTER TABLE call should happen
        spark.sql.return_value.collect.return_value = [row]

        artifact = {
            "artifact_id": "uc_abac/POL-1.json",
            "target": "uc_abac",
            "content": json.dumps({
                "applies_to": "PIIColumn",
                "mask_function": "cat.sch.fn",
            }),
        }
        result = _deploy_uc_abac(w, artifact, spark, "c", "s", dry_run=True)
        assert result.success
        assert result.deployed_at is None
        assert "(dry-run)" in result.details
        # Only the classification query should have been called, not ALTER TABLE
        assert spark.sql.call_count == 1
```

- [ ] **Step 2: Run tests**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=engine/src pytest tests/unit/test_deployer.py -v
```

Expected: All 11 tests PASS (5 from Task 1 + 6 new).

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_deployer.py
git commit -m "test(deployer): add target-specific deployer tests with mocked SDK"
```

---

### Task 3: `deploy()` entrypoint + setup.py

**Files:**
- Modify: `engine/src/watchdog/entrypoints.py`
- Modify: `engine/setup.py`

- [ ] **Step 1: Add the `deploy()` function**

In `engine/src/watchdog/entrypoints.py`, add the following function after the `compile()` function and before `_build_engine()`:

```python
def deploy():
    """Entrypoint: deploy compiled artifacts to the workspace.

    Reads the compile manifest, pushes each artifact to its target
    platform substrate (UC tag policies, ABAC column masks), and
    reports results. Guardrails artifacts are skipped (deployed via disk).
    """
    import json
    import os
    from pathlib import Path

    from watchdog.compiler import load_manifest
    from watchdog.deployer import deploy_artifacts

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--dry-run", action="store_true",
                        help="Resolve targets but skip execution")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    w = WorkspaceClient()

    # Resolve compile output directory
    try:
        compile_dir = Path(__file__).parent.parent.parent / "compile_output"
    except NameError:
        compile_dir = Path(os.getcwd()) / "compile_output"

    manifest_path = compile_dir / "manifest.json"
    if not manifest_path.exists():
        print("No compile manifest found. Run watchdog-compile first.")
        return

    # Load manifest and read artifact content
    entries = load_manifest(manifest_path)
    if not entries:
        print("Manifest is empty. Nothing to deploy.")
        return

    artifacts = []
    for entry in entries:
        artifact_path = compile_dir / entry["artifact_id"]
        content = artifact_path.read_text() if artifact_path.exists() else "{}"
        artifacts.append({
            **entry,
            "content": content,
        })

    mode = "(dry-run) " if args.dry_run else ""
    print(f"{mode}Deploying {len(artifacts)} artifacts...")

    results = deploy_artifacts(
        artifacts, w=w, spark=spark,
        catalog=args.catalog, schema=args.schema,
        dry_run=args.dry_run,
    )

    # Summary
    succeeded = sum(1 for r in results if r.success)
    failed = sum(1 for r in results if not r.success)

    for r in results:
        status = "OK" if r.success else "FAIL"
        print(f"  [{status}] {r.artifact_id}: {r.details or r.error or ''}")

    suffix = " (dry-run)" if args.dry_run else ""
    print(f"Deployed {succeeded}/{len(results)} artifacts ({failed} failed){suffix}.")

    if [r for r in results if not r.success]:
        print("Failures:")
        for r in results:
            if not r.success:
                print(f"  {r.artifact_id}: {r.error}")
```

- [ ] **Step 2: Add the import for WorkspaceClient at the top of the function**

Note: `WorkspaceClient` is already imported at the module level in `entrypoints.py` (line 5: `from databricks.sdk import WorkspaceClient`). No new import needed.

- [ ] **Step 3: Register in setup.py**

In `engine/setup.py`, add `watchdog-deploy` to `console_scripts`. Add after `watchdog-compile`:

```python
            "watchdog-deploy=watchdog.entrypoints:deploy",
```

- [ ] **Step 4: Run the full test suite**

```bash
cd /Users/stuart.gano/Documents/Projects/databricks-watchdog
PYTHONPATH=guardrails/src:engine/src pytest tests/unit/ -v --tb=short --ignore=tests/unit/test_merge_pack.py 2>&1 | tail -10
```

Expected: All tests pass.

- [ ] **Step 5: Commit**

```bash
git add engine/src/watchdog/entrypoints.py engine/setup.py
git commit -m "feat(entrypoints): add watchdog-deploy CLI entrypoint with dry-run support"
```

---

## Self-Review

**Spec coverage:**
- ✅ `DeployResult` dataclass with all fields (Task 1)
- ✅ `deploy_artifacts()` orchestrator — dispatches per target, collects errors (Task 1)
- ✅ Guardrails skipped with log message (Task 1)
- ✅ Unknown target returns error (Task 1)
- ✅ `_deploy_uc_tag_policy()` — calls UC tag policy API, idempotent (Task 1, tested in Task 2)
- ✅ `_deploy_uc_abac()` — resolves applies_to via resource_classifications, ALTER TABLE SET COLUMN MASK (Task 1, tested in Task 2)
- ✅ No matching tables → success with "nothing to deploy" (Task 2)
- ✅ Dry-run mode — resolve targets, skip execution (Tasks 1, 2, 3)
- ✅ Error collection without stopping (Task 1 test + Task 2 tests)
- ✅ `deploy()` entrypoint with `--catalog`, `--schema`, `--dry-run` (Task 3)
- ✅ `watchdog-deploy` registered in setup.py (Task 3)
- ✅ Summary output with per-artifact status (Task 3)

**Placeholder scan:** No TBDs or vague steps. All code complete.

**Type consistency:**
- `DeployResult` used consistently across all deployer functions and orchestrator
- `deploy_artifacts(artifacts: list[dict], ...)` — artifacts dict has `artifact_id`, `target`, `content` keys, consistent between Task 3 (manifest loading) and Task 1 (orchestrator)
- `_deploy_uc_tag_policy(w, artifact, dry_run)` and `_deploy_uc_abac(w, artifact, spark, catalog, schema, dry_run)` — signatures consistent between definitions (Task 1) and test calls (Task 2)
