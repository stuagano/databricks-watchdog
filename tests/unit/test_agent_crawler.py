"""Unit tests for agent and agent_execution crawlers (Phase 5A).

Tests cover:
  - _crawl_agents() row format for Apps and serving endpoints
  - _crawl_agents() heuristic filtering for agent-like apps
  - _crawl_agents() graceful handling of empty lists and errors
  - _crawl_agent_traces() row format for execution traces
  - _crawl_agent_traces() graceful handling of missing trace tables
  - Both crawlers registered in crawl_all()
  - Correct resource_type values
  - Tags include agent_owner and deployed_by when available

Run with: pytest tests/unit/test_agent_crawler.py -v
"""
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest

# Mock pyspark and databricks SDK modules so tests run without them installed.
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
_types_mock.BooleanType = MagicMock

with patch.dict(sys.modules, _mock_modules):
    from watchdog.crawler import ResourceCrawler


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_crawler(metastore_id="ms-abc-123"):
    """Build a ResourceCrawler with mocked Spark and SDK clients."""
    spark = MagicMock()
    w = MagicMock()

    metastore_summary = SimpleNamespace(metastore_id=metastore_id)
    w.metastores.current.return_value = metastore_summary

    crawler = ResourceCrawler(spark=spark, w=w, catalog="platform", schema="watchdog")
    return crawler


def _make_app(name, creator="user@example.com", description="", url="https://app.example.com",
              compute_status=None, create_time="2025-01-01T00:00:00Z"):
    """Create a mock Databricks App object."""
    app = SimpleNamespace(
        name=name,
        creator=creator,
        description=description,
        url=url,
        compute_status=compute_status or SimpleNamespace(state="RUNNING"),
        create_time=create_time,
    )
    return app


def _make_endpoint(name, creator="user@example.com", state=None, creation_timestamp=1700000000):
    """Create a mock serving endpoint object."""
    ep = SimpleNamespace(
        name=name,
        creator=creator,
        state=state or SimpleNamespace(ready="READY"),
        creation_timestamp=creation_timestamp,
    )
    return ep


# ── _crawl_agents: Apps ─────────────────────────────────────────────────────

class TestCrawlAgentsApps:
    def test_agent_app_row_format(self):
        crawler = _make_crawler()
        app = _make_app("my-agent-app", creator="alice@company.com")
        crawler.w.apps.list.return_value = [app]
        crawler.w.serving_endpoints.list.return_value = []

        rows = crawler._crawl_agents()

        # Should have 1 row from apps (endpoint list is empty)
        app_rows = [r for r in rows if r[3].startswith("agent:app:")]
        assert len(app_rows) == 1
        row = app_rows[0]

        # scan_id at index 0
        assert row[0] == crawler.scan_id
        # metastore_id at index 1
        assert row[1] == "ms-abc-123"
        # resource_type at index 2
        assert row[2] == "agent"
        # resource_id at index 3
        assert row[3] == "agent:app:my-agent-app"
        # resource_name at index 4
        assert row[4] == "my-agent-app"
        # owner at index 5
        assert row[5] == "alice@company.com"
        # domain at index 6
        assert row[6] == ""
        # tags at index 7
        tags = row[7]
        assert tags["agent_owner"] == "alice@company.com"
        assert tags["deployed_by"] == "alice@company.com"
        # metadata at index 8
        metadata = row[8]
        assert metadata["app_name"] == "my-agent-app"
        assert metadata["deployed_by"] == "alice@company.com"
        # discovered_at at index 9
        assert row[9] == crawler.now
        # Tuple length matches schema
        assert len(row) == 10

    def test_filters_non_agent_apps(self):
        """Apps without agent-related keywords are excluded."""
        crawler = _make_crawler()
        agent_app = _make_app("my-agent-service")
        dashboard_app = _make_app("sales-dashboard", description="Just a dashboard")
        mcp_app = _make_app("mcp-gateway", description="MCP proxy")
        bot_app = _make_app("support-bot")
        ai_app = _make_app("ai-insights")
        assistant_app = _make_app("data-assistant")
        plain_app = _make_app("data-pipeline")

        crawler.w.apps.list.return_value = [
            agent_app, dashboard_app, mcp_app, bot_app, ai_app, assistant_app, plain_app,
        ]
        crawler.w.serving_endpoints.list.return_value = []

        rows = crawler._crawl_agents()
        app_rows = [r for r in rows if r[3].startswith("agent:app:")]

        # Should match: agent, mcp, bot, ai, assistant — NOT dashboard or pipeline
        names = {r[4] for r in app_rows}
        assert "my-agent-service" in names
        assert "mcp-gateway" in names
        assert "support-bot" in names
        assert "ai-insights" in names
        assert "data-assistant" in names
        assert "sales-dashboard" not in names
        assert "data-pipeline" not in names

    def test_description_keyword_match(self):
        """An app with a plain name but agent keyword in description is included."""
        crawler = _make_crawler()
        app = _make_app("generic-service", description="This is an AI assistant for teams")
        crawler.w.apps.list.return_value = [app]
        crawler.w.serving_endpoints.list.return_value = []

        rows = crawler._crawl_agents()
        app_rows = [r for r in rows if r[3].startswith("agent:app:")]
        assert len(app_rows) == 1

    def test_empty_apps_list(self):
        """Empty apps list returns no app rows."""
        crawler = _make_crawler()
        crawler.w.apps.list.return_value = []
        crawler.w.serving_endpoints.list.return_value = []

        rows = crawler._crawl_agents()
        assert rows == []

    def test_apps_error_still_returns_endpoints(self):
        """If apps.list() fails, serving endpoints are still crawled."""
        crawler = _make_crawler()
        crawler.w.apps.list.side_effect = Exception("Apps API unavailable")
        ep = _make_endpoint("my-model-endpoint")
        crawler.w.serving_endpoints.list.return_value = [ep]

        rows = crawler._crawl_agents()
        # Should still get the endpoint row
        assert len(rows) == 1
        assert rows[0][3] == "agent:endpoint:my-model-endpoint"

    def test_no_creator_no_tags(self):
        """When creator is None, tags should be empty."""
        crawler = _make_crawler()
        app = _make_app("ai-tool", creator=None)
        crawler.w.apps.list.return_value = [app]
        crawler.w.serving_endpoints.list.return_value = []

        rows = crawler._crawl_agents()
        app_rows = [r for r in rows if r[3].startswith("agent:app:")]
        assert len(app_rows) == 1
        # Tags should be empty when no creator
        assert app_rows[0][7] == {}


# ── _crawl_agents: Serving Endpoints ────────────────────────────────────────

class TestCrawlAgentsEndpoints:
    def test_endpoint_row_format(self):
        crawler = _make_crawler()
        ep = _make_endpoint("agent-serving-ep", creator="bob@company.com")
        crawler.w.apps.list.return_value = []
        crawler.w.serving_endpoints.list.return_value = [ep]

        rows = crawler._crawl_agents()

        assert len(rows) == 1
        row = rows[0]
        assert row[2] == "agent"
        assert row[3] == "agent:endpoint:agent-serving-ep"
        assert row[4] == "agent-serving-ep"
        assert row[5] == "bob@company.com"
        # tags
        tags = row[7]
        assert tags["agent_owner"] == "bob@company.com"
        assert tags["deployed_by"] == "bob@company.com"
        assert tags["model_endpoint"] == "agent-serving-ep"
        # metadata
        metadata = row[8]
        assert metadata["endpoint_name"] == "agent-serving-ep"
        assert metadata["deployed_by"] == "bob@company.com"
        assert len(row) == 10

    def test_endpoint_no_creator(self):
        """Endpoint with no creator has empty owner and no owner tags."""
        crawler = _make_crawler()
        ep = _make_endpoint("orphan-endpoint", creator="")
        crawler.w.apps.list.return_value = []
        crawler.w.serving_endpoints.list.return_value = [ep]

        rows = crawler._crawl_agents()

        assert len(rows) == 1
        tags = rows[0][7]
        assert "agent_owner" not in tags
        assert "deployed_by" not in tags
        assert tags["model_endpoint"] == "orphan-endpoint"

    def test_endpoints_error_still_returns_apps(self):
        """If serving_endpoints.list() fails, app rows are still returned."""
        crawler = _make_crawler()
        app = _make_app("agent-app")
        crawler.w.apps.list.return_value = [app]
        crawler.w.serving_endpoints.list.side_effect = Exception("Serving API down")

        rows = crawler._crawl_agents()
        assert len(rows) == 1
        assert rows[0][3] == "agent:app:agent-app"

    def test_empty_endpoints_list(self):
        """Empty endpoints list returns no endpoint rows."""
        crawler = _make_crawler()
        crawler.w.apps.list.return_value = []
        crawler.w.serving_endpoints.list.return_value = []

        rows = crawler._crawl_agents()
        assert rows == []


# ── _crawl_agent_traces ─────────────────────────────────────────────────────

class TestCrawlAgentTraces:
    def _make_usage_row(self, endpoint_name="my-endpoint", served_entity_name="my-entity",
                        entity_type="FOUNDATION_MODEL", task="llm/v1/chat",
                        endpoint_creator="System-User",
                        requester="user@example.com", request_count=100,
                        total_input_tokens=5000, total_output_tokens=2000,
                        error_count=0, rate_limited_count=0,
                        first_request="2025-01-01T00:00:00Z",
                        last_request="2025-01-07T00:00:00Z"):
        """Create a mock row matching the endpoint_usage aggregation query."""
        return SimpleNamespace(
            endpoint_name=endpoint_name,
            served_entity_name=served_entity_name,
            entity_type=entity_type,
            task=task,
            endpoint_creator=endpoint_creator,
            requester=requester,
            request_count=request_count,
            total_input_tokens=total_input_tokens,
            total_output_tokens=total_output_tokens,
            error_count=error_count,
            rate_limited_count=rate_limited_count,
            first_request=first_request,
            last_request=last_request,
        )

    def test_trace_row_format(self):
        crawler = _make_crawler()
        usage = self._make_usage_row(endpoint_name="agent-ep", requester="alice@co.com")
        result_mock = MagicMock()
        result_mock.collect.return_value = [usage]
        crawler.spark.sql.return_value = result_mock

        rows = crawler._crawl_agent_traces()

        assert len(rows) == 1
        row = rows[0]
        assert row[0] == crawler.scan_id
        assert row[1] == "ms-abc-123"
        assert row[2] == "agent_execution"
        assert row[3] == "execution:agent-ep:alice@co.com"
        assert row[4] == "agent-ep (alice@co.com)"
        assert row[5] == "alice@co.com"  # owner is the requester
        assert row[6] == ""  # domain
        # tags
        tags = row[7]
        assert tags["trace_id"] == "agent-ep:alice@co.com"
        assert tags["execution_completed"] == "true"
        assert tags["model_endpoint"] == "agent-ep"
        assert tags["entity_type"] == "FOUNDATION_MODEL"
        assert tags["task_type"] == "llm/v1/chat"
        # metadata
        metadata = row[8]
        assert metadata["endpoint_name"] == "agent-ep"
        assert metadata["served_entity_name"] == "my-entity"
        assert metadata["entity_type"] == "FOUNDATION_MODEL"
        assert metadata["task"] == "llm/v1/chat"
        assert metadata["requester"] == "alice@co.com"
        assert metadata["request_count"] == "100"
        assert metadata["total_input_tokens"] == "5000"
        assert metadata["total_output_tokens"] == "2000"
        assert metadata["rate_limited_count"] == "0"
        assert metadata["error_count"] == "0"
        assert metadata["resource_type"] == "agent_execution"
        assert len(row) == 10

    def test_failed_execution_tag(self):
        """Rows with errors get execution_completed=false."""
        crawler = _make_crawler()
        usage = self._make_usage_row(error_count=5)
        result_mock = MagicMock()
        result_mock.collect.return_value = [usage]
        crawler.spark.sql.return_value = result_mock

        rows = crawler._crawl_agent_traces()

        assert rows[0][7]["execution_completed"] == "false"

    def test_missing_trace_table(self):
        """If endpoint_usage table doesn't exist, returns empty list gracefully."""
        crawler = _make_crawler()
        crawler.spark.sql.side_effect = Exception("TABLE_NOT_FOUND: system.serving.endpoint_usage")

        rows = crawler._crawl_agent_traces()
        assert rows == []

    def test_empty_trace_results(self):
        """Empty trace results return empty list."""
        crawler = _make_crawler()
        result_mock = MagicMock()
        result_mock.collect.return_value = []
        crawler.spark.sql.return_value = result_mock

        rows = crawler._crawl_agent_traces()
        assert rows == []

    def test_fallback_request_id(self):
        """When endpoint_name is None, falls back to empty string in composite key."""
        crawler = _make_crawler()
        usage = self._make_usage_row(endpoint_name=None, requester="bob@co.com")
        result_mock = MagicMock()
        result_mock.collect.return_value = [usage]
        crawler.spark.sql.return_value = result_mock

        rows = crawler._crawl_agent_traces()

        assert rows[0][3] == "execution::bob@co.com"
        assert rows[0][7]["trace_id"] == ":bob@co.com"

    def test_multiple_traces(self):
        """Multiple usage rows produce multiple rows."""
        crawler = _make_crawler()
        usages = [
            self._make_usage_row(endpoint_name="ep-1", requester="alice@co.com"),
            self._make_usage_row(endpoint_name="ep-1", requester="bob@co.com", error_count=3),
            self._make_usage_row(endpoint_name="ep-2", requester="alice@co.com"),
        ]
        result_mock = MagicMock()
        result_mock.collect.return_value = usages
        crawler.spark.sql.return_value = result_mock

        rows = crawler._crawl_agent_traces()

        assert len(rows) == 3
        assert all(r[2] == "agent_execution" for r in rows)
        ids = {r[3] for r in rows}
        assert ids == {
            "execution:ep-1:alice@co.com",
            "execution:ep-1:bob@co.com",
            "execution:ep-2:alice@co.com",
        }


# ── crawl_all() registration ────────────────────────────────────────────────

def _named_mock(name, return_value=None):
    """Create a MagicMock with __name__ set (required by _safe_crawl)."""
    m = MagicMock(return_value=return_value if return_value is not None else [])
    m.__name__ = name
    return m


class TestCrawlAllRegistration:
    def _stub_all_crawlers(self, crawler):
        """Replace all crawler methods with named mocks."""
        crawler._crawl_catalogs = _named_mock("_crawl_catalogs")
        crawler._crawl_schemas = _named_mock("_crawl_schemas")
        crawler._crawl_tables = _named_mock("_crawl_tables")
        crawler._crawl_volumes = _named_mock("_crawl_volumes")
        crawler._crawl_groups = _named_mock("_crawl_groups")
        crawler._crawl_service_principals = _named_mock("_crawl_service_principals")
        crawler._crawl_agents = _named_mock("_crawl_agents")
        crawler._crawl_agent_traces = _named_mock("_crawl_agent_traces")
        crawler._crawl_grants = _named_mock("_crawl_grants")
        crawler._crawl_jobs = _named_mock("_crawl_jobs")
        crawler._crawl_clusters = _named_mock("_crawl_clusters")
        crawler._crawl_warehouses = _named_mock("_crawl_warehouses")
        crawler._crawl_pipelines = _named_mock("_crawl_pipelines")
        crawler._crawl_dqm_status = _named_mock("_crawl_dqm_status")
        crawler._crawl_lhm_status = _named_mock("_crawl_lhm_status")

    def test_agents_registered_in_crawl_all(self):
        """Both agent crawlers are called during crawl_all()."""
        crawler = _make_crawler()
        self._stub_all_crawlers(crawler)

        crawler.crawl_all()

        crawler._crawl_agents.assert_called_once()
        crawler._crawl_agent_traces.assert_called_once()

    def test_crawl_all_results_include_agent_types(self):
        """crawl_all() results include 'agents' and 'agent_traces' resource types."""
        crawler = _make_crawler()
        self._stub_all_crawlers(crawler)

        results = crawler.crawl_all()

        resource_types = {r.resource_type for r in results}
        assert "agents" in resource_types
        assert "agent_traces" in resource_types
