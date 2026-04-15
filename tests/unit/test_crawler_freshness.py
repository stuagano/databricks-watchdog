"""Unit tests for pipeline freshness tag derivation logic.

Tests the pure-Python health derivation function without Spark or SDK.
The actual crawler method reads system tables and calls this function.

Run with: pytest tests/unit/test_crawler_freshness.py -v
"""
import sys
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

# Mock pyspark and databricks SDK modules so tests run without them installed.
# These heavyweight dependencies are only needed at runtime on Databricks.
_mock_modules = {}
for mod_name in [
    "pyspark", "pyspark.sql", "pyspark.sql.functions", "pyspark.sql.types",
    "databricks", "databricks.sdk", "databricks.sdk.service",
    "databricks.sdk.service.catalog",
]:
    _mock_modules[mod_name] = MagicMock()

# Wire up StructType/StructField so INVENTORY_SCHEMA can be constructed
_types_mock = _mock_modules["pyspark.sql.types"]
_types_mock.StructType = list
_types_mock.StructField = lambda name, typ, nullable=True: name
_types_mock.StringType = MagicMock
_types_mock.MapType = MagicMock
_types_mock.TimestampType = MagicMock

from unittest.mock import patch
with patch.dict(sys.modules, _mock_modules):
    from watchdog.crawler import derive_pipeline_health


class TestDerivePipelineHealth:

    NOW = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)

    def test_healthy_recent_success_no_failures(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T10:00:00Z",
            last_failure_at=None,
            failure_count_7d=0,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "healthy"
        assert result["freshness_hours"] == "2"
        assert result["failure_count_7d"] == "0"

    def test_degraded_recent_success_with_failures(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T10:00:00Z",
            last_failure_at="2026-04-15T08:00:00Z",
            failure_count_7d=3,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "degraded"
        assert result["freshness_hours"] == "2"
        assert result["failure_count_7d"] == "3"

    def test_failing_last_run_failed(self):
        """Last failure is more recent than last success."""
        result = derive_pipeline_health(
            last_success_at="2026-04-14T10:00:00Z",
            last_failure_at="2026-04-15T11:00:00Z",
            failure_count_7d=1,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "failing"

    def test_failing_no_runs(self):
        result = derive_pipeline_health(
            last_success_at=None,
            last_failure_at=None,
            failure_count_7d=0,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "failing"
        assert result["freshness_hours"] == "-1"

    def test_failing_no_success_only_failures(self):
        result = derive_pipeline_health(
            last_success_at=None,
            last_failure_at="2026-04-15T11:00:00Z",
            failure_count_7d=5,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "failing"
        assert result["freshness_hours"] == "-1"

    def test_freshness_hours_rounds_down(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T09:30:00Z",
            last_failure_at=None,
            failure_count_7d=0,
            now=self.NOW,
        )
        assert result["freshness_hours"] == "2"  # 2.5 hours rounds down

    def test_tags_include_timestamps(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T10:00:00Z",
            last_failure_at="2026-04-14T08:00:00Z",
            failure_count_7d=1,
            now=self.NOW,
        )
        assert result["last_success_at"] == "2026-04-15T10:00:00Z"
        assert result["last_failure_at"] == "2026-04-14T08:00:00Z"
