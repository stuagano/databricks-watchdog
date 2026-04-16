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
        # metastore_id is at index 1 in the tuple (after scan_id)
        assert row[1] == "ms-abc-123"

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
        # Tuple length must match INVENTORY_SCHEMA field count (10 fields:
        # scan_id, metastore_id, resource_type, resource_id, resource_name,
        # owner, domain, tags, metadata, discovered_at)
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
        # resource_type at index 2 (after scan_id, metastore_id)
        assert row[2] == "service_principal"
        # resource_id at index 3
        assert row[3] == "service_principal:app-001"
        # resource_name at index 4
        assert row[4] == "my-sp"
        # owner at index 5 — SPs have no owner
        assert row[5] is None
        # metadata at index 8
        metadata = row[8]
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
        metadata = rows[0][8]
        assert metadata["entitlements"] == "workspace-access,databricks-sql-access"

    def test_no_display_name_falls_back_to_app_id(self):
        crawler = _make_crawler()
        sp = self._make_sp("app-003", None)
        crawler.w.service_principals.list.return_value = [sp]

        rows = crawler._crawl_service_principals()
        assert rows[0][4] == "app-003"

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
        assert row[2] == "grant"  # resource_type
        assert row[3] == "table:prod.sales.orders:data_team:SELECT"  # resource_id
        assert row[4] == "SELECT on table prod.sales.orders"  # resource_name
        assert row[5] == "data_team"  # owner = grantee
        metadata = row[8]
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
        assert row[3] == "schema:prod.finance:analysts:USE_SCHEMA"
        assert row[4] == "USE_SCHEMA on schema prod.finance"
        metadata = row[8]
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
        assert row[3] == "catalog:analytics:all_users:USE_CATALOG"
        assert row[4] == "USE_CATALOG on catalog analytics"
        metadata = row[8]
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


# ── Fixtures for row_filters and column_masks tests ───────────────────────────

def _make_catalog_ns(name):
    return SimpleNamespace(name=name)


def _make_row_filter_record():
    return SimpleNamespace(
        table_catalog="gold",
        table_schema="finance",
        table_name="gl_balances",
        filter_function_name="gold.sec.filter_gl",
    )


def _make_column_mask_record():
    return SimpleNamespace(
        table_catalog="gold",
        table_schema="finance",
        table_name="gl_balances",
        column_name="cost_center_owner",
        mask_function_name="gold.sec.mask_cost_center",
    )


@pytest.fixture
def crawler():
    """Crawler with mocked catalogs and SQL for row_filters / column_masks."""
    c = _make_crawler()
    c.w.catalogs.list.return_value = [_make_catalog_ns("gold")]

    def sql_side_effect(query):
        result = MagicMock()
        if "row_filters" in query:
            result.collect.return_value = [_make_row_filter_record()]
        elif "column_masks" in query:
            result.collect.return_value = [_make_column_mask_record()]
        else:
            result.collect.return_value = []
        return result

    c.spark.sql.side_effect = sql_side_effect
    return c


@pytest.fixture
def crawler_no_catalogs():
    """Crawler with no catalogs — every per-catalog crawl returns empty."""
    c = _make_crawler()
    c.w.catalogs.list.return_value = []
    return c


# ── Row filters ───────────────────────────────────────────────────────────────


class TestCrawlRowFilters:
    def test_emits_one_row_per_filter(self, crawler):
        """Each row_filters record produces one resource row."""
        rows = crawler._crawl_row_filters()
        assert len(rows) == 1
        row = rows[0]
        assert row[2] == "row_filter"
        assert row[3] == "row_filter:gold.finance.gl_balances"
        meta = row[8]
        assert meta["table_full_name"] == "gold.finance.gl_balances"
        assert meta["filter_function"] == "gold.sec.filter_gl"

    def test_empty_catalogs_returns_no_rows(self, crawler_no_catalogs):
        rows = crawler_no_catalogs._crawl_row_filters()
        assert rows == []


# ── Column masks ──────────────────────────────────────────────────────────────


class TestCrawlColumnMasks:
    def test_emits_one_row_per_mask(self, crawler):
        """Each column_masks record produces one resource row."""
        rows = crawler._crawl_column_masks()
        assert len(rows) == 1
        row = rows[0]
        assert row[2] == "column_mask"
        assert row[3] == "column_mask:gold.finance.gl_balances.cost_center_owner"
        meta = row[8]
        assert meta["table_full_name"] == "gold.finance.gl_balances"
        assert meta["column_name"] == "cost_center_owner"
        assert meta["mask_function"] == "gold.sec.mask_cost_center"

    def test_empty_catalogs_returns_no_rows(self, crawler_no_catalogs):
        rows = crawler_no_catalogs._crawl_column_masks()
        assert rows == []


# ── Group members ─────────────────────────────────────────────────────────────


def _make_mock_group_with_member():
    """Return a mock group object with one user member."""
    member = SimpleNamespace(
        value="alice@example.com",
        display="Alice",
        ref="https://host/api/2.0/scim/v2/Users/abc-123",
    )
    group = SimpleNamespace(
        id="grp-001",
        display_name="admins",
        meta=SimpleNamespace(resource_type="Group"),
        members=[member],
        entitlements=None,
    )
    return group


@pytest.fixture
def crawler_with_group():
    """Crawler with a mocked group that has one member."""
    c = _make_crawler()
    c.w.groups.list.return_value = [_make_mock_group_with_member()]
    return c


class TestCrawlGroupMembers:
    def test_emits_one_row_per_member(self, crawler_with_group):
        """Each group member produces a group_member resource row."""
        all_rows = crawler_with_group._crawl_groups()
        member_rows = [r for r in all_rows if r[2] == "group_member"]
        assert len(member_rows) >= 1
        row = member_rows[0]
        assert row[2] == "group_member"
        meta = row[8]  # metadata dict
        assert "group_name" in meta
        assert "member_value" in meta
        assert "member_type" in meta

    def test_member_type_inferred_from_ref(self, crawler_with_group):
        """Member type is inferred from the $ref field (Users -> user)."""
        all_rows = crawler_with_group._crawl_groups()
        member_rows = [r for r in all_rows if r[2] == "group_member"]
        assert len(member_rows) == 1
        meta = member_rows[0][8]
        assert meta["member_type"] == "user"
        assert meta["member_value"] == "alice@example.com"
        assert meta["group_name"] == "admins"

    def test_member_resource_id_format(self, crawler_with_group):
        """resource_id for group_member includes group name and member value."""
        all_rows = crawler_with_group._crawl_groups()
        member_rows = [r for r in all_rows if r[2] == "group_member"]
        row = member_rows[0]
        assert row[3] == "group_member:admins:alice@example.com"

    def test_group_resource_rows_still_emitted(self, crawler_with_group):
        """Original group rows are still emitted alongside group_member rows."""
        all_rows = crawler_with_group._crawl_groups()
        group_rows = [r for r in all_rows if r[2] == "group"]
        assert len(group_rows) >= 1

    def test_no_members_emits_no_member_rows(self):
        """A group with no members emits only the group row."""
        c = _make_crawler()
        group = SimpleNamespace(
            id="grp-002",
            display_name="empty-group",
            meta=SimpleNamespace(resource_type="Group"),
            members=None,
            entitlements=None,
        )
        c.w.groups.list.return_value = [group]
        all_rows = c._crawl_groups()
        member_rows = [r for r in all_rows if r[2] == "group_member"]
        assert member_rows == []
        group_rows = [r for r in all_rows if r[2] == "group"]
        assert len(group_rows) == 1

    def test_service_principal_member_type(self):
        """Members with ServicePrincipals in ref get member_type=service_principal."""
        c = _make_crawler()
        sp_member = SimpleNamespace(
            value="sp-app-id-123",
            display="my-sp",
            ref="https://host/api/2.0/scim/v2/ServicePrincipals/sp-app-id-123",
        )
        group = SimpleNamespace(
            id="grp-003",
            display_name="sp-group",
            meta=SimpleNamespace(resource_type="Group"),
            members=[sp_member],
            entitlements=None,
        )
        c.w.groups.list.return_value = [group]
        all_rows = c._crawl_groups()
        member_rows = [r for r in all_rows if r[2] == "group_member"]
        assert len(member_rows) == 1
        assert member_rows[0][8]["member_type"] == "service_principal"
