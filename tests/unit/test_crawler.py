"""Unit tests for ResourceCrawler — grants, service_principals, and metastore_id.

Tests use lightweight mocks for SparkSession, WorkspaceClient, and SDK objects.
No Spark or Databricks connection required.

Run with: pytest tests/unit/test_crawler.py -v
"""
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

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
_types_mock.StructType = list  # StructType([fields]) becomes a plain list
_types_mock.StructField = lambda name, typ, nullable=True: name  # just store field name
_types_mock.StringType = MagicMock
_types_mock.MapType = MagicMock
_types_mock.TimestampType = MagicMock

with patch.dict(sys.modules, _mock_modules):
    from watchdog.crawler import ResourceCrawler, INVENTORY_SCHEMA


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_crawler(metastore_id="ms-abc-123"):
    """Build a ResourceCrawler with mocked Spark and SDK clients."""
    spark = MagicMock()
    w = MagicMock()

    # Mock metastores.current() to return metastore_id
    metastore_summary = SimpleNamespace(metastore_id=metastore_id)
    w.metastores.current.return_value = metastore_summary

    crawler = ResourceCrawler(spark=spark, w=w, catalog="platform", schema="watchdog")
    return crawler


# ── metastore_id in _make_row ────────────────────────────────────────────────

class TestMetastoreId:
    def test_metastore_id_populated(self):
        crawler = _make_crawler(metastore_id="ms-abc-123")
        row = crawler._make_row(
            resource_type="table",
            resource_id="cat.schema.table1",
            resource_name="table1",
        )
        # metastore_id is at index 8 in the tuple (after metadata, before discovered_at)
        assert row[8] == "ms-abc-123"

    def test_metastore_id_cached(self):
        crawler = _make_crawler(metastore_id="ms-cached")
        # Access twice — SDK should only be called once
        _ = crawler.metastore_id
        _ = crawler.metastore_id
        crawler.w.metastores.current.assert_called_once()

    def test_metastore_id_defaults_on_error(self):
        crawler = _make_crawler()
        crawler.w.metastores.current.side_effect = Exception("no access")
        assert crawler.metastore_id == ""

    def test_make_row_tuple_matches_schema(self):
        crawler = _make_crawler()
        row = crawler._make_row(
            resource_type="catalog",
            resource_id="my_catalog",
            resource_name="my_catalog",
            owner="admin",
            domain="",
            tags={"env": "prod"},
            metadata={"comment": "test"},
        )
        # Tuple length must match INVENTORY_SCHEMA field count (10 fields)
        assert len(row) == 10


# ── _crawl_service_principals ────────────────────────────────────────────────

class TestCrawlServicePrincipals:
    def _make_sp(self, app_id, display_name, active=True, entitlements=None):
        sp = SimpleNamespace(
            application_id=app_id,
            display_name=display_name,
            active=active,
            entitlements=entitlements,
        )
        return sp

    def test_basic_row_format(self):
        crawler = _make_crawler()
        sp = self._make_sp("app-001", "my-sp", active=True)
        crawler.w.service_principals.list.return_value = [sp]

        rows = crawler._crawl_service_principals()

        assert len(rows) == 1
        row = rows[0]
        # resource_type at index 1
        assert row[1] == "service_principal"
        # resource_id at index 2
        assert row[2] == "service_principal:app-001"
        # resource_name at index 3
        assert row[3] == "my-sp"
        # owner at index 4 — SPs have no owner
        assert row[4] is None
        # metadata at index 7
        metadata = row[7]
        assert metadata["application_id"] == "app-001"
        assert metadata["active"] == "True"
        assert metadata["entitlements"] == ""

    def test_entitlements_joined(self):
        crawler = _make_crawler()
        entitlements = [
            SimpleNamespace(value="workspace-access"),
            SimpleNamespace(value="databricks-sql-access"),
        ]
        sp = self._make_sp("app-002", "ent-sp", entitlements=entitlements)
        crawler.w.service_principals.list.return_value = [sp]

        rows = crawler._crawl_service_principals()
        metadata = rows[0][7]
        assert metadata["entitlements"] == "workspace-access,databricks-sql-access"

    def test_no_display_name_falls_back_to_app_id(self):
        crawler = _make_crawler()
        sp = self._make_sp("app-003", None)
        crawler.w.service_principals.list.return_value = [sp]

        rows = crawler._crawl_service_principals()
        assert rows[0][3] == "app-003"

    def test_empty_list(self):
        crawler = _make_crawler()
        crawler.w.service_principals.list.return_value = []

        rows = crawler._crawl_service_principals()
        assert rows == []


# ── _crawl_grants ────────────────────────────────────────────────────────────

class TestCrawlGrants:
    def _make_catalog(self, name):
        return SimpleNamespace(name=name, tags=None, owner="admin",
                               isolation_mode=None, comment="")

    def _make_sql_row(self, **kwargs):
        """Create a SimpleNamespace mimicking a Spark Row."""
        return SimpleNamespace(**kwargs)

    def test_table_grant_row_format(self):
        crawler = _make_crawler()
        cat = self._make_catalog("prod")
        crawler.w.catalogs.list.return_value = [cat]

        # Mock table_privileges query
        table_row = self._make_sql_row(
            grantee="data_team",
            table_catalog="prod",
            table_schema="sales",
            table_name="orders",
            privilege_type="SELECT",
            is_grantable="NO",
        )
        # Mock schema_privileges to return empty
        # Mock grants.get to return empty
        def sql_side_effect(query):
            result = MagicMock()
            if "table_privileges" in query:
                result.collect.return_value = [table_row]
            elif "schema_privileges" in query:
                result.collect.return_value = []
            return result

        crawler.spark.sql.side_effect = sql_side_effect
        crawler.w.grants.get.return_value = SimpleNamespace(privilege_assignments=None)

        rows = crawler._crawl_grants()

        assert len(rows) == 1
        row = rows[0]
        assert row[1] == "grant"  # resource_type
        assert row[2] == "table:prod.sales.orders:data_team:SELECT"  # resource_id
        assert row[3] == "SELECT on table prod.sales.orders"  # resource_name
        assert row[4] == "data_team"  # owner = grantee
        metadata = row[7]
        assert metadata["securable_type"] == "table"
        assert metadata["securable_full_name"] == "prod.sales.orders"
        assert metadata["grantee"] == "data_team"
        assert metadata["privilege"] == "SELECT"

    def test_schema_grant_row_format(self):
        crawler = _make_crawler()
        cat = self._make_catalog("prod")
        crawler.w.catalogs.list.return_value = [cat]

        schema_row = self._make_sql_row(
            grantee="analysts",
            catalog_name="prod",
            schema_name="finance",
            privilege_type="USE_SCHEMA",
            is_grantable="NO",
        )

        def sql_side_effect(query):
            result = MagicMock()
            if "table_privileges" in query:
                result.collect.return_value = []
            elif "schema_privileges" in query:
                result.collect.return_value = [schema_row]
            return result

        crawler.spark.sql.side_effect = sql_side_effect
        crawler.w.grants.get.return_value = SimpleNamespace(privilege_assignments=None)

        rows = crawler._crawl_grants()

        assert len(rows) == 1
        row = rows[0]
        assert row[2] == "schema:prod.finance:analysts:USE_SCHEMA"
        assert row[3] == "USE_SCHEMA on schema prod.finance"
        metadata = row[7]
        assert metadata["securable_type"] == "schema"

    def test_catalog_grant_via_sdk(self):
        crawler = _make_crawler()
        cat = self._make_catalog("analytics")
        crawler.w.catalogs.list.return_value = [cat]

        # SQL queries return empty
        def sql_side_effect(query):
            result = MagicMock()
            result.collect.return_value = []
            return result

        crawler.spark.sql.side_effect = sql_side_effect

        # SDK grants.get returns a catalog-level grant
        priv = SimpleNamespace(
            privilege=SimpleNamespace(value="USE_CATALOG"),
            inherited_from_name="",
        )
        assignment = SimpleNamespace(
            principal="all_users",
            privileges=[priv],
        )
        crawler.w.grants.get.return_value = SimpleNamespace(
            privilege_assignments=[assignment]
        )

        rows = crawler._crawl_grants()

        assert len(rows) == 1
        row = rows[0]
        assert row[2] == "catalog:analytics:all_users:USE_CATALOG"
        assert row[3] == "USE_CATALOG on catalog analytics"
        metadata = row[7]
        assert metadata["securable_type"] == "catalog"
        assert metadata["grantee"] == "all_users"

    def test_skips_inaccessible_catalogs(self):
        """Catalogs that throw errors are silently skipped."""
        crawler = _make_crawler()
        cat = self._make_catalog("locked")
        crawler.w.catalogs.list.return_value = [cat]

        # All SQL queries throw
        crawler.spark.sql.side_effect = Exception("PERMISSION_DENIED")
        crawler.w.grants.get.side_effect = Exception("PERMISSION_DENIED")

        rows = crawler._crawl_grants()
        assert rows == []
