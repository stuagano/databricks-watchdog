"""Unit tests for semantic views — SQL structure and table reference validation.

Tests verify that:
  - View SQL is syntactically valid (parseable, correct f-string interpolation)
  - Views reference correct table names from the watchdog schema
  - All six views are registered in ensure_semantic_views()
  - CDF properties are set on the right tables

These are pure Python tests — no Spark session needed. They mock PySpark
and inspect the SQL strings that view-creation functions produce.

Run with: pytest tests/unit/test_views.py -v
"""
import re
import sys
import types
import inspect
from unittest.mock import MagicMock

import pytest


# ── Mock PySpark before importing watchdog modules ───────────────────────────

# Create a fake pyspark module tree so watchdog.views (and friends) can import
# without a real Spark installation.
_pyspark = types.ModuleType("pyspark")
_pyspark_sql = types.ModuleType("pyspark.sql")
_pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
_pyspark_sql_types = types.ModuleType("pyspark.sql.types")

# Minimal stubs for types used at import time.
# Use plain lambdas for type constructors that get called with arguments
# (e.g., MapType(StringType(), StringType())) to avoid MagicMock spec issues.
_pyspark_sql.SparkSession = MagicMock
_pyspark_sql.DataFrame = MagicMock
_pyspark_sql.Row = MagicMock
_pyspark_sql_functions.col = MagicMock
_pyspark_sql_functions.current_timestamp = MagicMock

def _dummy_type(*args, **kwargs):
    """Stand-in for PySpark type constructors (StringType, StructField, etc.)."""
    return f"type({args})"

_pyspark_sql_types.StructType = _dummy_type
_pyspark_sql_types.StructField = _dummy_type
_pyspark_sql_types.StringType = _dummy_type
_pyspark_sql_types.BooleanType = _dummy_type
_pyspark_sql_types.IntegerType = _dummy_type
_pyspark_sql_types.TimestampType = _dummy_type
_pyspark_sql_types.MapType = _dummy_type
_pyspark_sql_types.DoubleType = _dummy_type

_pyspark.sql = _pyspark_sql

sys.modules.setdefault("pyspark", _pyspark)
sys.modules.setdefault("pyspark.sql", _pyspark_sql)
sys.modules.setdefault("pyspark.sql.functions", _pyspark_sql_functions)
sys.modules.setdefault("pyspark.sql.types", _pyspark_sql_types)

# Mock databricks.sdk too (needed by some watchdog modules)
_databricks = types.ModuleType("databricks")
_databricks_sdk = types.ModuleType("databricks.sdk")
_databricks_sdk.WorkspaceClient = MagicMock
_databricks.sdk = _databricks_sdk

sys.modules.setdefault("databricks", _databricks)
sys.modules.setdefault("databricks.sdk", _databricks_sdk)

# Now safe to import watchdog modules
from watchdog.views import (  # noqa: E402
    ensure_semantic_views,
    _ensure_resource_compliance_view,
    _ensure_class_compliance_view,
    _ensure_domain_compliance_view,
    _ensure_tag_policy_coverage_view,
    _ensure_data_classification_summary_view,
    _ensure_dq_monitoring_coverage_view,
    _ensure_compliance_trend_view,
)


# ── Fixtures ─────────────────────────────────────────────────────────────────

CATALOG = "test_catalog"
SCHEMA = "test_schema"


@pytest.fixture
def mock_spark():
    """Mock SparkSession that captures SQL strings passed to spark.sql()."""
    spark = MagicMock()
    spark.sql_calls = []

    def capture_sql(sql_str):
        spark.sql_calls.append(sql_str)
        return MagicMock()

    spark.sql.side_effect = capture_sql
    return spark


def _get_view_sql(mock_spark, view_fn):
    """Call a view function and return the SQL it generates."""
    mock_spark.sql_calls.clear()
    view_fn(mock_spark, CATALOG, SCHEMA)
    assert len(mock_spark.sql_calls) == 1, (
        f"Expected exactly 1 SQL call from {view_fn.__name__}, got {len(mock_spark.sql_calls)}"
    )
    return mock_spark.sql_calls[0]


# ── ensure_semantic_views registration ───────────────────────────────────────

class TestEnsureSemanticViews:
    """Verify all nine views are registered in the top-level orchestrator."""

    def test_calls_all_nine_view_functions(self, mock_spark):
        ensure_semantic_views(mock_spark, CATALOG, SCHEMA)
        assert mock_spark.sql.call_count == 9

    def test_creates_all_nine_views(self, mock_spark):
        ensure_semantic_views(mock_spark, CATALOG, SCHEMA)
        view_names = []
        for call_args in mock_spark.sql_calls:
            match = re.search(
                r"CREATE OR REPLACE VIEW\s+\S+\.(\w+)", call_args
            )
            if match:
                view_names.append(match.group(1))

        expected = {
            "v_resource_compliance",
            "v_class_compliance",
            "v_domain_compliance",
            "v_tag_policy_coverage",
            "v_data_classification_summary",
            "v_dq_monitoring_coverage",
            "v_cross_metastore_compliance",
            "v_cross_metastore_inventory",
            "v_compliance_trend",
        }
        assert set(view_names) == expected


# ── SQL syntax validation ────────────────────────────────────────────────────

class TestViewSqlSyntax:
    """Verify view SQL is well-formed after f-string interpolation."""

    @pytest.mark.parametrize("view_fn,view_name", [
        (_ensure_resource_compliance_view, "v_resource_compliance"),
        (_ensure_class_compliance_view, "v_class_compliance"),
        (_ensure_domain_compliance_view, "v_domain_compliance"),
        (_ensure_tag_policy_coverage_view, "v_tag_policy_coverage"),
        (_ensure_data_classification_summary_view, "v_data_classification_summary"),
        (_ensure_dq_monitoring_coverage_view, "v_dq_monitoring_coverage"),
        (_ensure_compliance_trend_view, "v_compliance_trend"),
    ])
    def test_creates_correct_view_name(self, mock_spark, view_fn, view_name):
        sql = _get_view_sql(mock_spark, view_fn)
        expected_fqn = f"{CATALOG}.{SCHEMA}.{view_name}"
        assert expected_fqn in sql

    @pytest.mark.parametrize("view_fn", [
        _ensure_resource_compliance_view,
        _ensure_class_compliance_view,
        _ensure_domain_compliance_view,
        _ensure_tag_policy_coverage_view,
        _ensure_data_classification_summary_view,
        _ensure_dq_monitoring_coverage_view,
        _ensure_compliance_trend_view,
    ])
    def test_sql_starts_with_create_or_replace(self, mock_spark, view_fn):
        sql = _get_view_sql(mock_spark, view_fn).strip()
        assert sql.startswith("CREATE OR REPLACE VIEW")

    @pytest.mark.parametrize("view_fn", [
        _ensure_resource_compliance_view,
        _ensure_class_compliance_view,
        _ensure_domain_compliance_view,
        _ensure_tag_policy_coverage_view,
        _ensure_data_classification_summary_view,
        _ensure_dq_monitoring_coverage_view,
        _ensure_compliance_trend_view,
    ])
    def test_sql_has_no_unresolved_fstring_braces(self, mock_spark, view_fn):
        """Ensure all {catalog} and {schema} placeholders were resolved."""
        sql = _get_view_sql(mock_spark, view_fn)
        assert "{catalog}" not in sql
        assert "{schema}" not in sql

    @pytest.mark.parametrize("view_fn", [
        _ensure_resource_compliance_view,
        _ensure_class_compliance_view,
        _ensure_domain_compliance_view,
        _ensure_tag_policy_coverage_view,
        _ensure_data_classification_summary_view,
        _ensure_dq_monitoring_coverage_view,
        _ensure_compliance_trend_view,
    ])
    def test_sql_has_select_keyword(self, mock_spark, view_fn):
        sql = _get_view_sql(mock_spark, view_fn).upper()
        assert "SELECT" in sql


# ── Table reference validation ───────────────────────────────────────────────

class TestViewTableReferences:
    """Verify views reference the correct watchdog schema tables."""

    def test_resource_compliance_references_correct_tables(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_resource_compliance_view)
        assert f"{CATALOG}.{SCHEMA}.resource_classifications" in sql
        assert f"{CATALOG}.{SCHEMA}.violations" in sql

    def test_class_compliance_references_correct_tables(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_class_compliance_view)
        assert f"{CATALOG}.{SCHEMA}.resource_classifications" in sql
        assert f"{CATALOG}.{SCHEMA}.violations" in sql

    def test_domain_compliance_references_correct_tables(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_domain_compliance_view)
        assert f"{CATALOG}.{SCHEMA}.violations" in sql

    def test_tag_policy_coverage_references_correct_tables(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.resource_inventory" in sql
        assert f"{CATALOG}.{SCHEMA}.policies" in sql
        assert f"{CATALOG}.{SCHEMA}.scan_results" in sql
        assert f"{CATALOG}.{SCHEMA}.violations" in sql
        assert f"{CATALOG}.{SCHEMA}.exceptions" in sql

    def test_data_classification_summary_references_correct_tables(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert f"{CATALOG}.{SCHEMA}.resource_inventory" in sql
        assert f"{CATALOG}.{SCHEMA}.resource_classifications" in sql

    def test_dq_monitoring_coverage_references_correct_tables(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.resource_inventory" in sql
        assert f"{CATALOG}.{SCHEMA}.resource_classifications" in sql


# ── New view column validation ───────────────────────────────────────────────

class TestTagPolicyCoverageView:
    """Verify v_tag_policy_coverage SQL structure."""

    def test_has_coverage_status_case(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert "coverage_status" in sql
        assert "'satisfied'" in sql
        assert "'violated'" in sql
        assert "'not_evaluated'" in sql

    def test_filters_active_policies(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert "p.active = true" in sql

    def test_filters_governance_domains(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert "'SecurityGovernance'" in sql
        assert "'DataClassification'" in sql

    def test_joins_exceptions_for_waivers(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert "has_exception" in sql
        assert "exception_expires" in sql


class TestDataClassificationSummaryView:
    """Verify v_data_classification_summary SQL structure."""

    def test_has_classification_pct(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "classification_pct" in sql

    def test_has_stewardship_pct(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "stewardship_pct" in sql

    def test_filters_tables_only(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "ri.resource_type = 'table'" in sql

    def test_groups_by_catalog(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "GROUP BY ri.domain" in sql

    def test_references_tag_keys(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "data_classification" in sql
        assert "data_steward" in sql

    def test_references_ontology_classes(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "'PiiAsset'" in sql
        assert "'ConfidentialAsset'" in sql


class TestDqMonitoringCoverageView:
    """Verify v_dq_monitoring_coverage SQL structure."""

    def test_has_monitoring_status_case(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert "monitoring_status" in sql
        assert "'both'" in sql
        assert "'dqm_only'" in sql
        assert "'lhm_only'" in sql
        assert "'none'" in sql

    def test_references_dqm_tags(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert "dqm_enabled" in sql
        assert "lhm_enabled" in sql
        assert "dqm_anomalies" in sql
        assert "dqm_metrics_checked" in sql

    def test_filters_tables_only(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert "ri.resource_type = 'table'" in sql

    def test_has_ontology_class(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert "ontology_class" in sql


# ── CDF enablement validation ────────────────────────────────────────────────

class TestCdfEnablement:
    """Verify CDF (Change Data Feed) is enabled on the right tables."""

    def test_resource_inventory_has_cdf(self):
        """resource_inventory CREATE TABLE should include CDF property."""
        from watchdog.crawler import ensure_inventory_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_inventory_table(spark, CATALOG, SCHEMA)

        assert len(sql_calls) == 1
        sql = sql_calls[0]
        assert "delta.enableChangeDataFeed" in sql
        assert "'true'" in sql

    def test_dq_status_has_cdf(self):
        """dq_status CREATE TABLE should include CDF property.

        The dq_status table is created inline in _crawl_dqm_status.
        We verify CDF is in the source code of the crawler module.
        """
        import watchdog.crawler as crawler_mod
        source = inspect.getsource(crawler_mod.ResourceCrawler._crawl_dqm_status)
        assert "delta.enableChangeDataFeed" in source

    def test_violations_table_has_cdf(self):
        """violations table should have CDF (pre-existing)."""
        from watchdog.violations import ensure_violations_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_violations_table(spark, CATALOG, SCHEMA)

        assert len(sql_calls) == 1
        sql = sql_calls[0]
        assert "delta.enableChangeDataFeed" in sql

    def test_resource_classifications_has_cdf(self):
        """resource_classifications table should have CDF (pre-existing)."""
        from watchdog.violations import ensure_classifications_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_classifications_table(spark, CATALOG, SCHEMA)

        assert len(sql_calls) == 1
        sql = sql_calls[0]
        assert "delta.enableChangeDataFeed" in sql

    def test_scan_summary_is_append_only(self):
        """scan_summary table should be append-only for immutable trend data."""
        from watchdog.violations import ensure_scan_summary_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_scan_summary_table(spark, CATALOG, SCHEMA)

        assert len(sql_calls) == 1
        sql = sql_calls[0]
        assert "delta.appendOnly" in sql
        assert "'true'" in sql


# ── Compliance trend view ────────────────────────────────────────────────────

class TestComplianceTrendView:
    """Verify v_compliance_trend SQL structure and computed columns."""

    def test_references_scan_summary(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_compliance_trend_view)
        assert f"{CATALOG}.{SCHEMA}.scan_summary" in sql

    def test_has_lag_deltas(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_compliance_trend_view)
        assert "open_violations_delta" in sql
        assert "compliance_pct_delta" in sql
        assert "resources_delta" in sql
        assert "critical_delta" in sql

    def test_has_trend_direction(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_compliance_trend_view)
        assert "trend_direction" in sql
        assert "'improving'" in sql
        assert "'declining'" in sql
        assert "'stable'" in sql

    def test_has_rolling_averages(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_compliance_trend_view)
        assert "compliance_pct_7scan_avg" in sql
        assert "open_violations_30scan_avg" in sql

    def test_partitions_by_metastore(self, mock_spark):
        """LAG windows should partition by metastore_id for multi-metastore support."""
        sql = _get_view_sql(mock_spark, _ensure_compliance_trend_view)
        assert "PARTITION BY metastore_id" in sql

    def test_orders_by_scanned_at_desc(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_compliance_trend_view)
        assert "ORDER BY scanned_at DESC" in sql
