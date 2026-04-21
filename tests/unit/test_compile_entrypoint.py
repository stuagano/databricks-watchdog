"""Unit tests for compile entrypoint summary formatting.

Run with: pytest tests/unit/test_compile_entrypoint.py -v
"""
import sys
from unittest.mock import MagicMock

# Mock heavyweight runtime dependencies so tests run without pyspark/databricks.
_mock_modules = {}
for _mod in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "databricks", "databricks.sdk",
]:
    _mock_modules[_mod] = MagicMock()

_types = _mock_modules["pyspark.sql.types"]
_types.StructType = list
_types.StructField = lambda name, typ, nullable=True: name
_types.StringType = MagicMock
_types.TimestampType = MagicMock

with __import__("unittest.mock", fromlist=["patch"]).patch.dict(sys.modules, _mock_modules):
    from watchdog.entrypoints import format_compile_summary


class TestFormatCompileSummary:
    def test_basic_summary(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-2", "guardrails"),
            _artifact("POL-3", "uc_tag_policy"),
        ]
        drift = [
            _drift("POL-1", "guardrails", "in_sync"),
            _drift("POL-2", "guardrails", "in_sync"),
            _drift("POL-3", "uc_tag_policy", "in_sync"),
        ]
        result = format_compile_summary(artifacts, drift)
        assert "3 artifacts" in result
        assert "2 guardrails" in result
        assert "1 uc_tag_policy" in result
        assert "3 in_sync" in result

    def test_empty_artifacts(self):
        result = format_compile_summary([], [])
        assert "Nothing to compile" in result

    def test_mixed_drift_states(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-2", "uc_abac"),
            _artifact("POL-3", "uc_tag_policy"),
        ]
        drift = [
            _drift("POL-1", "guardrails", "in_sync"),
            _drift("POL-2", "uc_abac", "drifted"),
            _drift("POL-3", "uc_tag_policy", "missing"),
        ]
        result = format_compile_summary(artifacts, drift)
        assert "1 in_sync" in result
        assert "1 drifted" in result
        assert "1 missing" in result

    def test_multiple_targets_per_policy(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-1", "uc_abac"),
            _artifact("POL-2", "guardrails"),
        ]
        drift = [
            _drift("POL-1", "guardrails", "in_sync"),
            _drift("POL-1", "uc_abac", "in_sync"),
            _drift("POL-2", "guardrails", "drifted"),
        ]
        result = format_compile_summary(artifacts, drift)
        assert "3 artifacts" in result
        assert "2 guardrails" in result
        assert "1 uc_abac" in result

    def test_counts_unique_policies(self):
        artifacts = [
            _artifact("POL-1", "guardrails"),
            _artifact("POL-1", "uc_abac"),
        ]
        drift = []
        result = format_compile_summary(artifacts, drift)
        assert "1 policies" in result or "1 policy" in result


def _artifact(policy_id, target):
    """Minimal artifact-like object for summary formatting."""
    return type("A", (), {"policy_id": policy_id, "target": target})()


def _drift(policy_id, target, state):
    """Minimal drift-result-like object for summary formatting."""
    return type("D", (), {"policy_id": policy_id, "target": target, "state": state})()
