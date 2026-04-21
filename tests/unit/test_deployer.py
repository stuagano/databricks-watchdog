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

from watchdog.deployer import DeployResult, deploy_artifacts, _deploy_uc_tag_policy, _deploy_uc_abac


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

        row = MagicMock()
        row.resource_id = "gold.finance.gl"
        spark.sql.return_value.collect.return_value = [row]

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
        assert spark.sql.call_count == 1
