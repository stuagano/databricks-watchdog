"""Unit tests for grant-awareness in guardrails watchdog_client.

Tests the new ResourceGovernanceState grant properties,
get_grants_for_resource, and get_service_principal_governance.

Run with: pytest tests/unit/test_guardrails_grants.py -v
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure guardrails package is importable
REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "guardrails" / "src"))

import pytest

from ai_devkit.watchdog_client import (
    ResourceGovernanceState,
    get_grants_for_resource,
    get_service_principal_governance,
)


# ── ResourceGovernanceState grant properties ─────────────────────────────────


class TestGrantProperties:
    def test_has_overprivileged_grants_true(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["DataAsset", "OverprivilegedGrant"],
        )
        assert state.has_overprivileged_grants is True

    def test_has_overprivileged_grants_false(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["DataAsset"],
        )
        assert state.has_overprivileged_grants is False

    def test_has_overprivileged_grants_from_ancestors(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["DataAsset"],
            ancestors=["OverprivilegedGrant"],
        )
        assert state.has_overprivileged_grants is True

    def test_has_direct_user_grants_true(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["DirectUserGrant"],
        )
        assert state.has_direct_user_grants is True

    def test_has_direct_user_grants_false(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["DataAsset"],
        )
        assert state.has_direct_user_grants is False

    def test_grant_violations_filters_pol_a(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            open_violations=[
                {"policy_id": "POL-A001", "severity": "high"},
                {"policy_id": "POL-D001", "severity": "medium"},
                {"policy_id": "POL-A003", "severity": "low"},
            ],
        )
        result = state.grant_violations
        assert len(result) == 2
        assert result[0]["policy_id"] == "POL-A001"
        assert result[1]["policy_id"] == "POL-A003"

    def test_grant_violations_empty_when_no_pol_a(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            open_violations=[
                {"policy_id": "POL-D001", "severity": "medium"},
            ],
        )
        assert state.grant_violations == []

    def test_grant_violations_empty_when_no_violations(self):
        state = ResourceGovernanceState(resource_id="catalog.schema.table1")
        assert state.grant_violations == []


# ── Inferred classification escalation ───────────────────────────────────────


class TestClassificationEscalation:
    def test_internal_escalates_to_confidential(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["InternalAsset", "OverprivilegedGrant"],
        )
        assert state.inferred_classification == "confidential"

    def test_public_escalates_to_internal(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["PublicAsset", "OverprivilegedGrant"],
        )
        assert state.inferred_classification == "internal"

    def test_confidential_escalates_to_restricted(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["ConfidentialAsset", "OverprivilegedGrant"],
        )
        assert state.inferred_classification == "restricted"

    def test_restricted_stays_restricted(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["RestrictedAsset", "OverprivilegedGrant"],
        )
        assert state.inferred_classification == "restricted"

    def test_no_escalation_without_overprivileged(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["InternalAsset"],
        )
        assert state.inferred_classification == "internal"

    def test_unclassified_escalates_to_internal(self):
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["OverprivilegedGrant"],
        )
        assert state.inferred_classification == "internal"

    def test_export_controlled_not_affected(self):
        """Export-controlled is already max; escalation doesn't apply."""
        state = ResourceGovernanceState(
            resource_id="catalog.schema.table1",
            classes=["ExportControlledAsset", "OverprivilegedGrant"],
        )
        assert state.inferred_classification == "restricted"


# ── get_grants_for_resource ──────────────────────────────────────────────────


class TestGetGrantsForResource:
    def test_returns_grant_dicts(self):
        mock_w = MagicMock()
        mock_config = MagicMock()
        mock_config.watchdog_schema = "platform.watchdog"
        mock_config.warehouse_id = "test-warehouse"

        mock_resp = MagicMock()
        mock_resp.result.data_array = [
            ["grant:abc", "grant_abc", "grant", '{"securable_full_name": "cat.sch.tbl"}'],
        ]
        mock_manifest_col = MagicMock()
        mock_manifest_col.name = "resource_id"
        mock_manifest_col2 = MagicMock()
        mock_manifest_col2.name = "resource_name"
        mock_manifest_col3 = MagicMock()
        mock_manifest_col3.name = "resource_type"
        mock_manifest_col4 = MagicMock()
        mock_manifest_col4.name = "metadata"
        mock_resp.manifest.schema.columns = [
            mock_manifest_col, mock_manifest_col2, mock_manifest_col3, mock_manifest_col4,
        ]
        mock_w.statement_execution.execute_statement.return_value = mock_resp

        result = get_grants_for_resource(mock_w, mock_config, "cat.sch.tbl")
        assert len(result) == 1
        assert result[0]["resource_id"] == "grant:abc"
        assert result[0]["resource_type"] == "grant"

    def test_returns_empty_on_exception(self):
        mock_w = MagicMock()
        mock_config = MagicMock()
        mock_config.watchdog_schema = "platform.watchdog"
        mock_config.warehouse_id = "test-warehouse"
        mock_w.statement_execution.execute_statement.side_effect = Exception("fail")

        result = get_grants_for_resource(mock_w, mock_config, "cat.sch.tbl")
        assert result == []

    def test_returns_empty_when_no_results(self):
        mock_w = MagicMock()
        mock_config = MagicMock()
        mock_config.watchdog_schema = "platform.watchdog"
        mock_config.warehouse_id = "test-warehouse"

        mock_resp = MagicMock()
        mock_resp.result.data_array = []
        mock_w.statement_execution.execute_statement.return_value = mock_resp

        result = get_grants_for_resource(mock_w, mock_config, "cat.sch.tbl")
        assert result == []


# ── get_service_principal_governance ─────────────────────────────────────────


class TestGetServicePrincipalGovernance:
    @patch("ai_devkit.watchdog_client.get_resource_governance")
    def test_delegates_with_correct_resource_id(self, mock_get_gov):
        mock_state = ResourceGovernanceState(
            resource_id="service_principal:app-123",
            classes=["ServicePrincipalAsset"],
        )
        mock_get_gov.return_value = mock_state

        mock_w = MagicMock()
        mock_config = MagicMock()

        result = get_service_principal_governance(mock_w, mock_config, "app-123")
        mock_get_gov.assert_called_once_with(
            mock_w, mock_config, "service_principal:app-123", metastore_id=None
        )
        assert result.resource_id == "service_principal:app-123"
