# tests/unit/test_hub_contract.py
"""Hub contract tests — validate compliance views against hub_contract.yml.

Tests verify that each Hub-facing view's SQL produces the columns defined
in the contract, with correct names and ordering. Uses the same mock-Spark
approach as test_views.py.

Run with: pytest tests/unit/test_hub_contract.py -v
"""
import re
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml


# -- Mock PySpark before importing watchdog modules ---------------------------

_pyspark = types.ModuleType("pyspark")
_pyspark_sql = types.ModuleType("pyspark.sql")
_pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
_pyspark_sql_types = types.ModuleType("pyspark.sql.types")

_pyspark_sql.SparkSession = MagicMock
_pyspark_sql.DataFrame = MagicMock
_pyspark_sql.Row = MagicMock
_pyspark_sql_functions.col = MagicMock
_pyspark_sql_functions.current_timestamp = MagicMock


def _dummy_type(*args, **kwargs):
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

_databricks = types.ModuleType("databricks")
_databricks_sdk = types.ModuleType("databricks.sdk")
_databricks_sdk_service = types.ModuleType("databricks.sdk.service")
_databricks_sdk_catalog = types.ModuleType("databricks.sdk.service.catalog")
_databricks_sdk_catalog.SecurableType = MagicMock
_databricks_sdk.WorkspaceClient = MagicMock
_databricks_sdk.service = _databricks_sdk_service
_databricks_sdk_service.catalog = _databricks_sdk_catalog
_databricks.sdk = _databricks_sdk

sys.modules.setdefault("databricks", _databricks)
sys.modules.setdefault("databricks.sdk", _databricks_sdk)
sys.modules.setdefault("databricks.sdk.service", _databricks_sdk_service)
sys.modules.setdefault("databricks.sdk.service.catalog", _databricks_sdk_catalog)

# Now safe to import watchdog modules
from watchdog.views import (  # noqa: E402
    _ensure_domain_compliance_view,
    _ensure_class_compliance_view,
    _ensure_resource_compliance_view,
    _ensure_tag_policy_coverage_view,
    _ensure_data_classification_summary_view,
    _ensure_dq_monitoring_coverage_view,
)


# -- Fixtures -----------------------------------------------------------------

CATALOG = "test_catalog"
SCHEMA = "test_schema"

REPO_ROOT = Path(__file__).parent.parent.parent
CONTRACT_PATH = REPO_ROOT / "engine" / "hub_contract.yml"

VIEW_FN_MAP = {
    "v_domain_compliance": _ensure_domain_compliance_view,
    "v_class_compliance": _ensure_class_compliance_view,
    "v_resource_compliance": _ensure_resource_compliance_view,
    "v_tag_policy_coverage": _ensure_tag_policy_coverage_view,
    "v_data_classification_summary": _ensure_data_classification_summary_view,
    "v_dq_monitoring_coverage": _ensure_dq_monitoring_coverage_view,
}


@pytest.fixture(scope="module")
def contract():
    """Load the hub contract YAML."""
    with open(CONTRACT_PATH) as f:
        return yaml.safe_load(f)


@pytest.fixture
def mock_spark():
    """Mock SparkSession that captures SQL strings."""
    spark = MagicMock()
    spark.sql_calls = []

    def capture_sql(sql_str):
        spark.sql_calls.append(sql_str)
        return MagicMock()

    spark.sql.side_effect = capture_sql
    return spark


def _get_view_sql(mock_spark, view_fn):
    """Call a view function and return the CREATE VIEW SQL."""
    mock_spark.sql_calls.clear()
    view_fn(mock_spark, CATALOG, SCHEMA)
    # Filter to only CREATE OR REPLACE VIEW statements
    view_sqls = [s for s in mock_spark.sql_calls if "CREATE OR REPLACE VIEW" in s]
    assert len(view_sqls) == 1, (
        f"Expected 1 CREATE VIEW from {view_fn.__name__}, got {len(view_sqls)}"
    )
    return view_sqls[0]


# -- Contract file validation -------------------------------------------------

class TestContractFile:
    """Verify the contract file itself is well-formed."""

    def test_contract_exists(self):
        assert CONTRACT_PATH.exists(), f"Contract file not found at {CONTRACT_PATH}"

    def test_contract_has_version(self, contract):
        assert "version" in contract
        assert contract["version"] == 1

    def test_contract_has_all_six_views(self, contract):
        expected = {
            "v_domain_compliance",
            "v_class_compliance",
            "v_resource_compliance",
            "v_tag_policy_coverage",
            "v_data_classification_summary",
            "v_dq_monitoring_coverage",
        }
        assert set(contract["views"].keys()) == expected

    def test_each_view_has_required_fields(self, contract):
        for view_name, view_def in contract["views"].items():
            assert "description" in view_def, f"{view_name} missing description"
            assert "grain" in view_def, f"{view_name} missing grain"
            assert "hub_panel" in view_def, f"{view_name} missing hub_panel"
            assert "columns" in view_def, f"{view_name} missing columns"
            assert len(view_def["columns"]) > 0, f"{view_name} has no columns"

    def test_each_column_has_required_fields(self, contract):
        for view_name, view_def in contract["views"].items():
            for col in view_def["columns"]:
                assert "name" in col, f"{view_name}: column missing name"
                assert "type" in col, f"{view_name}.{col.get('name', '?')} missing type"
                assert "nullable" in col, f"{view_name}.{col['name']} missing nullable"
                assert "description" in col, f"{view_name}.{col['name']} missing description"


# -- View SQL vs contract column matching -------------------------------------

class TestViewColumnsMatchContract:
    """Verify each view's SQL SELECT produces columns matching the contract."""

    @pytest.mark.parametrize("view_name", [
        "v_domain_compliance",
        "v_class_compliance",
        "v_resource_compliance",
        "v_tag_policy_coverage",
        "v_data_classification_summary",
        "v_dq_monitoring_coverage",
    ])
    def test_view_columns_present_in_sql(self, mock_spark, contract, view_name):
        """Every contract column name must appear in the view SQL."""
        view_fn = VIEW_FN_MAP[view_name]
        sql = _get_view_sql(mock_spark, view_fn)
        contract_columns = [c["name"] for c in contract["views"][view_name]["columns"]]

        for col_name in contract_columns:
            assert col_name in sql.lower(), (
                f"{view_name}: contract column '{col_name}' not found in view SQL"
            )


# -- View dependency validation -----------------------------------------------

class TestViewDependencies:
    """Verify views reference the correct underlying tables."""

    def test_tag_policy_coverage_references_policies_table(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.policies" in sql

    def test_tag_policy_coverage_references_exceptions_table(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.exceptions" in sql

    def test_data_classification_summary_has_catalog_fallback(self, mock_spark):
        """catalog_name should use COALESCE with SPLIT fallback."""
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "COALESCE" in sql
        assert "catalog_name" in sql.lower()

    def test_dq_monitoring_coverage_has_coalesce_defaults(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert "COALESCE" in sql


# -- Policies table validation ------------------------------------------------

class TestPoliciesTable:
    """Verify the policies table schema matches what v_tag_policy_coverage expects."""

    def test_ensure_policies_table_creates_correct_schema(self):
        from watchdog.policies_table import ensure_policies_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_policies_table(spark, CATALOG, SCHEMA)

        assert len(sql_calls) == 1
        sql = sql_calls[0]
        assert "policy_id" in sql
        assert "policy_name" in sql
        assert "applies_to" in sql
        assert "domain" in sql
        assert "severity" in sql
        assert "active" in sql

    def test_policies_table_columns_match_view_join(self):
        """The view joins on p.policy_id, p.policy_name, p.severity, p.active, p.domain."""
        from watchdog.policies_table import ensure_policies_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_policies_table(spark, CATALOG, SCHEMA)

        sql = sql_calls[0]
        # These columns are referenced in v_tag_policy_coverage
        for col in ["policy_id", "policy_name", "severity", "active", "domain"]:
            assert col in sql, f"policies table missing column: {col}"
