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
