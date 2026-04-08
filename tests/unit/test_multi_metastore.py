"""Unit tests for multi-metastore scanning support.

Tests cover:
  - WatchdogConfig parsing of WATCHDOG_METASTORE_IDS
  - ResourceCrawler metastore_id override behavior
  - crawl_all_metastores entrypoint dispatch logic
  - Cross-metastore views registration
  - INVENTORY_SCHEMA includes metastore_id

Modules that depend on pyspark or databricks-sdk are imported behind mocks
so tests can run without those dependencies installed.

Run with: pytest tests/unit/test_multi_metastore.py -v
"""
import importlib
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Mock pyspark and databricks.sdk so watchdog modules can be imported
# without the actual packages installed.
# ---------------------------------------------------------------------------

def _ensure_mock_modules():
    """Install mock modules for pyspark and databricks.sdk if not available."""
    modules_to_mock = [
        "pyspark", "pyspark.sql", "pyspark.sql.functions",
        "pyspark.sql.types", "databricks", "databricks.sdk",
    ]
    for mod_name in modules_to_mock:
        if mod_name not in sys.modules:
            mock_mod = types.ModuleType(mod_name)
            # pyspark.sql.types needs real-looking schema classes
            if mod_name == "pyspark.sql.types":
                class _MockStructField:
                    def __init__(self, name, dataType, nullable=True):
                        self.name = name
                        self.dataType = dataType
                        self.nullable = nullable

                class _MockStructType:
                    def __init__(self, fields=None):
                        self.fields = fields or []

                class _MockType:
                    pass

                mock_mod.StructField = _MockStructField
                mock_mod.StructType = _MockStructType
                mock_mod.StringType = _MockType
                mock_mod.TimestampType = _MockType
                mock_mod.BooleanType = _MockType
                mock_mod.MapType = lambda *a: _MockType()

            if mod_name == "pyspark.sql":
                mock_mod.SparkSession = MagicMock()
                mock_mod.DataFrame = MagicMock()
                mock_mod.Row = MagicMock()

            if mod_name == "pyspark.sql.functions":
                mock_mod.col = MagicMock()

            if mod_name == "databricks.sdk":
                mock_mod.WorkspaceClient = MagicMock()

            sys.modules[mod_name] = mock_mod


_ensure_mock_modules()

# Now we can import watchdog modules safely
from watchdog.config import WatchdogConfig
from watchdog.crawler import INVENTORY_SCHEMA, ResourceCrawler


# ── WatchdogConfig ───────────────────────────────────────────────────────────

class TestWatchdogConfig:
    def test_empty_metastore_ids_default(self):
        """Empty env var produces an empty list."""
        with patch.dict(os.environ, {"WATCHDOG_METASTORE_IDS": ""}, clear=False):
            config = WatchdogConfig()
            assert config.metastore_ids == []
            assert config.is_multi_metastore is False

    def test_single_metastore_id(self):
        """Single metastore ID is parsed correctly."""
        with patch.dict(os.environ, {"WATCHDOG_METASTORE_IDS": "ms-001"}, clear=False):
            config = WatchdogConfig()
            assert config.metastore_ids == ["ms-001"]
            assert config.is_multi_metastore is False

    def test_multiple_metastore_ids(self):
        """Comma-separated metastore IDs are parsed and trimmed."""
        with patch.dict(os.environ, {"WATCHDOG_METASTORE_IDS": "ms-001, ms-002 , ms-003"}, clear=False):
            config = WatchdogConfig()
            assert config.metastore_ids == ["ms-001", "ms-002", "ms-003"]
            assert config.is_multi_metastore is True

    def test_trailing_comma_ignored(self):
        """Trailing commas produce no empty entries."""
        with patch.dict(os.environ, {"WATCHDOG_METASTORE_IDS": "ms-001,ms-002,"}, clear=False):
            config = WatchdogConfig()
            assert config.metastore_ids == ["ms-001", "ms-002"]

    def test_only_commas_produces_empty(self):
        """A value of just commas produces an empty list."""
        with patch.dict(os.environ, {"WATCHDOG_METASTORE_IDS": ",,,"}, clear=False):
            config = WatchdogConfig()
            assert config.metastore_ids == []

    def test_default_catalog_and_schema(self):
        """Defaults from environment or fallback."""
        env = {k: v for k, v in os.environ.items()
               if k not in ("WATCHDOG_CATALOG", "WATCHDOG_SCHEMA")}
        with patch.dict(os.environ, env, clear=True):
            config = WatchdogConfig()
            assert config.catalog == "platform"
            assert config.schema == "watchdog"
            assert config.qualified_schema == "platform.watchdog"

    def test_custom_catalog_and_schema(self):
        """Env vars override defaults."""
        with patch.dict(os.environ, {
            "WATCHDOG_CATALOG": "my_cat",
            "WATCHDOG_SCHEMA": "my_schema",
        }, clear=False):
            config = WatchdogConfig()
            assert config.catalog == "my_cat"
            assert config.schema == "my_schema"
            assert config.qualified_schema == "my_cat.my_schema"


# ── INVENTORY_SCHEMA ─────────────────────────────────────────────────────────

class TestInventorySchema:
    def test_metastore_id_column_present(self):
        """INVENTORY_SCHEMA includes the metastore_id column."""
        field_names = [f.name for f in INVENTORY_SCHEMA.fields]
        assert "metastore_id" in field_names

    def test_metastore_id_is_nullable(self):
        """metastore_id is nullable for backward compatibility."""
        for f in INVENTORY_SCHEMA.fields:
            if f.name == "metastore_id":
                assert f.nullable is True
                break
        else:
            pytest.fail("metastore_id field not found in INVENTORY_SCHEMA")

    def test_metastore_id_position(self):
        """metastore_id appears after scan_id and before resource_type."""
        field_names = [f.name for f in INVENTORY_SCHEMA.fields]
        assert field_names.index("metastore_id") == 1
        assert field_names.index("scan_id") == 0
        assert field_names.index("resource_type") == 2


# ── ResourceCrawler metastore_id ─────────────────────────────────────────────

class TestResourceCrawlerMetastoreId:
    def _make_crawler(self, metastore_id=None):
        """Create a ResourceCrawler with mocked Spark and WorkspaceClient."""
        spark = MagicMock()
        w = MagicMock()
        return ResourceCrawler(spark, w, "test_catalog", "test_schema",
                               metastore_id=metastore_id)

    def test_override_metastore_id(self):
        """When metastore_id is provided, it takes precedence."""
        crawler = self._make_crawler(metastore_id="ms-override-123")
        assert crawler.metastore_id == "ms-override-123"
        # Should not call the SDK
        crawler.w.metastores.current.assert_not_called()

    def test_auto_detect_metastore_id(self):
        """When no override, metastore_id is auto-detected from SDK."""
        crawler = self._make_crawler()
        mock_summary = MagicMock()
        mock_summary.metastore_id = "ms-auto-456"
        crawler.w.metastores.current.return_value = mock_summary

        assert crawler.metastore_id == "ms-auto-456"
        crawler.w.metastores.current.assert_called_once()

    def test_auto_detect_caches_result(self):
        """Auto-detected metastore_id is cached after first call."""
        crawler = self._make_crawler()
        mock_summary = MagicMock()
        mock_summary.metastore_id = "ms-cached"
        crawler.w.metastores.current.return_value = mock_summary

        _ = crawler.metastore_id
        _ = crawler.metastore_id
        # Only called once despite two accesses
        crawler.w.metastores.current.assert_called_once()

    def test_auto_detect_failure_returns_empty(self):
        """If SDK call fails, metastore_id returns empty string."""
        crawler = self._make_crawler()
        crawler.w.metastores.current.side_effect = Exception("API error")

        assert crawler.metastore_id == ""

    def test_make_row_includes_metastore_id(self):
        """_make_row stamps the metastore_id into position 1."""
        crawler = self._make_crawler(metastore_id="ms-row-test")
        row = crawler._make_row(
            resource_type="table",
            resource_id="cat.schema.tbl",
            resource_name="tbl",
            owner="user@example.com",
        )
        # Position 0: scan_id, 1: metastore_id
        assert row[1] == "ms-row-test"
        assert row[2] == "table"  # resource_type at position 2


# ── Cross-metastore views ───────────────────────────────────────────────────

class TestCrossMetastoreViews:
    def test_ensure_semantic_views_calls_cross_metastore(self):
        """ensure_semantic_views registers the two cross-metastore views."""
        from watchdog import views

        spark = MagicMock()
        views.ensure_semantic_views(spark, "cat", "sch")

        # Collect all SQL statements executed
        sql_calls = [call.args[0] for call in spark.sql.call_args_list]
        sql_text = "\n".join(sql_calls)

        assert "v_cross_metastore_compliance" in sql_text
        assert "v_cross_metastore_inventory" in sql_text

    def test_cross_metastore_compliance_view_sql(self):
        """v_cross_metastore_compliance groups by metastore_id."""
        from watchdog import views

        spark = MagicMock()
        views._ensure_cross_metastore_compliance_view(spark, "cat", "sch")

        sql = spark.sql.call_args[0][0]
        assert "ri.metastore_id" in sql
        assert "GROUP BY ri.metastore_id" in sql
        assert "compliance_pct" in sql

    def test_cross_metastore_inventory_view_sql(self):
        """v_cross_metastore_inventory groups by metastore_id, resource_type."""
        from watchdog import views

        spark = MagicMock()
        views._ensure_cross_metastore_inventory_view(spark, "cat", "sch")

        sql = spark.sql.call_args[0][0]
        assert "metastore_id" in sql
        assert "resource_type" in sql
        assert "resource_count" in sql
        assert "distinct_owners" in sql

    def test_cross_metastore_views_use_metastore_aware_subquery(self):
        """Both cross-metastore views use metastore-aware latest-scan subqueries."""
        from watchdog import views

        spark = MagicMock()

        views._ensure_cross_metastore_compliance_view(spark, "cat", "sch")
        compliance_sql = spark.sql.call_args[0][0]
        assert "WHERE metastore_id = ri.metastore_id" in compliance_sql

        views._ensure_cross_metastore_inventory_view(spark, "cat", "sch")
        inventory_sql = spark.sql.call_args[0][0]
        assert "WHERE metastore_id = resource_inventory.metastore_id" in inventory_sql


# ── crawl_all_metastores entrypoint ──────────────────────────────────────────

class TestCrawlAllMetastoresEntrypoint:
    @patch("watchdog.entrypoints.WorkspaceClient")
    @patch("watchdog.entrypoints.SparkSession")
    @patch("watchdog.entrypoints.crawl")
    def test_fallback_to_single_crawl_when_no_ids(self, mock_crawl,
                                                    mock_spark_cls,
                                                    mock_ws_cls):
        """When no metastore IDs configured, falls back to crawl()."""
        from watchdog.entrypoints import crawl_all_metastores

        with patch("sys.argv", ["run_task.py",
                                "--catalog", "test",
                                "--schema", "watchdog"]):
            with patch.dict(os.environ, {"WATCHDOG_METASTORE_IDS": ""}):
                crawl_all_metastores()
                mock_crawl.assert_called_once()

    @patch("watchdog.entrypoints.WorkspaceClient")
    @patch("watchdog.entrypoints.SparkSession")
    @patch("watchdog.crawler.ResourceCrawler.crawl_all")
    def test_iterates_over_metastore_ids(self, mock_crawl_all,
                                          mock_spark_cls,
                                          mock_ws_cls):
        """Crawls each configured metastore ID."""
        from watchdog.entrypoints import crawl_all_metastores
        from watchdog.crawler import CrawlResult

        mock_crawl_all.return_value = [
            CrawlResult(resource_type="table", count=5),
        ]

        with patch("sys.argv", ["run_task.py",
                                "--catalog", "test",
                                "--schema", "watchdog",
                                "--metastore-ids", "ms-A,ms-B"]):
            crawl_all_metastores()
            assert mock_crawl_all.call_count == 2

    @patch("watchdog.entrypoints.WorkspaceClient")
    @patch("watchdog.entrypoints.SparkSession")
    @patch("watchdog.crawler.ResourceCrawler.crawl_all")
    def test_cli_override_over_env(self, mock_crawl_all,
                                    mock_spark_cls,
                                    mock_ws_cls):
        """CLI --metastore-ids overrides WATCHDOG_METASTORE_IDS env var."""
        from watchdog.entrypoints import crawl_all_metastores
        from watchdog.crawler import CrawlResult

        mock_crawl_all.return_value = [
            CrawlResult(resource_type="table", count=3),
        ]

        with patch("sys.argv", ["run_task.py",
                                "--catalog", "test",
                                "--schema", "watchdog",
                                "--metastore-ids", "ms-CLI"]):
            with patch.dict(os.environ, {"WATCHDOG_METASTORE_IDS": "ms-ENV1,ms-ENV2"}):
                crawl_all_metastores()
                # Only 1 call (ms-CLI), not 2 (from env)
                assert mock_crawl_all.call_count == 1


# ── run_task.py entrypoint registration ──────────────────────────────────────

class TestRunTaskRegistration:
    def test_crawl_all_metastores_registered(self):
        """crawl_all_metastores is a registered entrypoint in run_task.py."""
        # Ensure engine/src is on path
        src_path = str(Path(__file__).parent.parent.parent / "engine" / "src")
        if src_path not in sys.path:
            sys.path.insert(0, src_path)

        # Load run_task module
        spec = importlib.util.spec_from_file_location(
            "run_task",
            str(Path(__file__).parent.parent.parent / "engine" / "src" / "run_task.py"),
        )
        run_task = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(run_task)

        assert "crawl_all_metastores" in run_task.ENTRYPOINTS
        assert run_task.ENTRYPOINTS["crawl_all_metastores"] == \
            "watchdog.entrypoints:crawl_all_metastores"
