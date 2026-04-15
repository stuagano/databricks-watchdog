"""Integration tests that validate the 6 Hub-facing compliance views against hub_contract.yml.

Tests verify that:
  - The contract YAML file exists and is well-formed
  - Each of the 6 Hub views produces SQL that contains all contract-defined columns
  - Key view dependencies and SQL patterns are present

Run with: PYTHONPATH=engine/src pytest tests/unit/test_hub_contract.py -v
"""
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml

# ── Mock PySpark before importing watchdog modules ───────────────────────────

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

# Mock databricks.sdk too
_databricks = types.ModuleType("databricks")
_databricks_sdk = types.ModuleType("databricks.sdk")
_databricks_sdk.WorkspaceClient = MagicMock
_databricks.sdk = _databricks_sdk

_databricks_sdk_service = types.ModuleType("databricks.sdk.service")
_databricks_sdk_service_catalog = types.ModuleType("databricks.sdk.service.catalog")
_databricks_sdk_service_catalog.SecurableType = MagicMock
_databricks_sdk.service = _databricks_sdk_service
_databricks_sdk_service.catalog = _databricks_sdk_service_catalog

sys.modules.setdefault("databricks", _databricks)
sys.modules.setdefault("databricks.sdk", _databricks_sdk)
sys.modules.setdefault("databricks.sdk.service", _databricks_sdk_service)
sys.modules.setdefault("databricks.sdk.service.catalog", _databricks_sdk_service_catalog)

# Now safe to import watchdog modules
from watchdog.views import (  # noqa: E402
    _ensure_domain_compliance_view,
    _ensure_class_compliance_view,
    _ensure_resource_compliance_view,
    _ensure_tag_policy_coverage_view,
    _ensure_data_classification_summary_view,
    _ensure_dq_monitoring_coverage_view,
)
from watchdog.policies_table import ensure_policies_table  # noqa: E402


# ── Constants ─────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).parent.parent.parent
CONTRACT_PATH = REPO_ROOT / "engine" / "hub_contract.yml"

CATALOG = "test_catalog"
SCHEMA = "test_schema"

EXPECTED_VIEW_NAMES = {
    "v_domain_compliance",
    "v_class_compliance",
    "v_resource_compliance",
    "v_tag_policy_coverage",
    "v_data_classification_summary",
    "v_dq_monitoring_coverage",
}

VIEW_FN_MAP = {
    "v_domain_compliance": _ensure_domain_compliance_view,
    "v_class_compliance": _ensure_class_compliance_view,
    "v_resource_compliance": _ensure_resource_compliance_view,
    "v_tag_policy_coverage": _ensure_tag_policy_coverage_view,
    "v_data_classification_summary": _ensure_data_classification_summary_view,
    "v_dq_monitoring_coverage": _ensure_dq_monitoring_coverage_view,
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_contract():
    """Load hub_contract.yml, returning (views_dict, version)."""
    with open(CONTRACT_PATH) as f:
        raw = yaml.safe_load(f)
    views_dict = {v["name"]: v for v in raw["views"]}
    return views_dict, raw.get("version", 0)


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
    """Call a view function and return the CREATE OR REPLACE VIEW SQL it generates.

    Some view functions (e.g. _ensure_tag_policy_coverage_view) emit multiple
    SQL statements (CREATE TABLE for dependencies + CREATE VIEW). This helper
    filters for the single CREATE OR REPLACE VIEW statement.
    """
    mock_spark.sql_calls.clear()
    view_fn(mock_spark, CATALOG, SCHEMA)
    view_sqls = [s for s in mock_spark.sql_calls if "CREATE OR REPLACE VIEW" in s]
    assert len(view_sqls) == 1, (
        f"Expected exactly 1 CREATE OR REPLACE VIEW from {view_fn.__name__}, "
        f"got {len(view_sqls)} (total SQL calls: {len(mock_spark.sql_calls)})"
    )
    return view_sqls[0]


# ── TestContractFile ──────────────────────────────────────────────────────────

class TestContractFile:
    """Validate the hub_contract.yml file structure."""

    def test_contract_exists(self):
        assert CONTRACT_PATH.exists(), f"Contract file not found at {CONTRACT_PATH}"

    def test_contract_has_version(self):
        _, version = _load_contract()
        assert version == 1, f"Expected version 1, got {version}"

    def test_contract_has_all_six_views(self):
        views, _ = _load_contract()
        assert set(views.keys()) == EXPECTED_VIEW_NAMES, (
            f"Contract views mismatch.\nExpected: {EXPECTED_VIEW_NAMES}\nGot: {set(views.keys())}"
        )

    def test_each_view_has_required_fields(self):
        views, _ = _load_contract()
        required_fields = {"description", "grain", "hub_panel", "columns"}
        for view_name, view_def in views.items():
            missing = required_fields - set(view_def.keys())
            assert not missing, (
                f"View '{view_name}' is missing required fields: {missing}"
            )

    def test_each_column_has_required_fields(self):
        views, _ = _load_contract()
        required_col_fields = {"name", "type", "nullable", "description"}
        for view_name, view_def in views.items():
            for col in view_def["columns"]:
                missing = required_col_fields - set(col.keys())
                assert not missing, (
                    f"Column '{col.get('name', '?')}' in view '{view_name}' "
                    f"is missing required fields: {missing}"
                )


# ── TestViewColumnsMatchContract ──────────────────────────────────────────────

class TestViewColumnsMatchContract:
    """Verify each view's SQL contains all column names defined in the contract."""

    @pytest.mark.parametrize("view_name", sorted(EXPECTED_VIEW_NAMES))
    def test_view_sql_contains_contract_columns(self, mock_spark, view_name):
        views, _ = _load_contract()
        view_def = views[view_name]
        view_fn = VIEW_FN_MAP[view_name]

        sql = _get_view_sql(mock_spark, view_fn).lower()

        missing_columns = []
        for col in view_def["columns"]:
            col_name = col["name"].lower()
            if col_name not in sql:
                missing_columns.append(col["name"])

        assert not missing_columns, (
            f"View '{view_name}' SQL is missing contract columns: {missing_columns}"
        )


# ── TestViewDependencies ──────────────────────────────────────────────────────

class TestViewDependencies:
    """Verify key table references and SQL patterns in the Hub views."""

    def test_tag_policy_coverage_references_policies_table(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.policies" in sql

    def test_tag_policy_coverage_references_exceptions_table(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.exceptions" in sql

    def test_data_classification_summary_has_catalog_fallback(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "COALESCE" in sql.upper()
        assert "catalog_name" in sql.lower()

    def test_dq_monitoring_coverage_has_coalesce_defaults(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert "COALESCE" in sql.upper()


# ── TestPoliciesTable ─────────────────────────────────────────────────────────

class TestPoliciesTable:
    """Verify the policies table schema matches what views expect."""

    def test_ensure_policies_table_creates_correct_schema(self, mock_spark):
        ensure_policies_table(mock_spark, CATALOG, SCHEMA)
        assert len(mock_spark.sql_calls) == 1
        sql = mock_spark.sql_calls[0]
        assert "CREATE TABLE IF NOT EXISTS" in sql or "CREATE OR REPLACE TABLE" in sql
        assert f"{CATALOG}.{SCHEMA}.policies" in sql

    def test_policies_table_columns_match_view_join(self, mock_spark):
        ensure_policies_table(mock_spark, CATALOG, SCHEMA)
        sql = mock_spark.sql_calls[0].lower()
        required_columns = {"policy_id", "policy_name", "severity", "active", "domain"}
        missing = [col for col in required_columns if col not in sql]
        assert not missing, (
            f"policies table SQL is missing columns expected by views: {missing}"
        )
