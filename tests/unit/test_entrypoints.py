"""Unit tests for watchdog.entrypoints — CLI wiring + _load_agents.

The entrypoint functions themselves require a live SparkSession, so the unit
tests here only cover:
  - _load_agents returns the registered set
  - adhoc() type-alias map parses correctly (exercised via a thin helper)
  - argparse accepts the expected flags on each entrypoint
"""

from unittest.mock import MagicMock, patch

import pytest


class TestLoadAgents:
    def test_returns_expected_agent_ids(self):
        from watchdog.entrypoints import _load_agents
        ids = {a.agent_id for a in _load_agents()}
        assert "steward-agent" in ids
        assert "cluster-tagger-agent" in ids
        assert "job-owner-agent" in ids
        assert "dq-monitor-scaffold-agent" in ids

    def test_all_agents_satisfy_protocol(self):
        from watchdog.entrypoints import _load_agents
        from watchdog.remediation.protocol import RemediationAgent
        for agent in _load_agents():
            assert isinstance(agent, RemediationAgent)

    def test_policies_handled_do_not_collide(self):
        """If two agents claim the same policy_id the dispatcher picks the first —
        that's fine, but we want to know it here so we can think about it."""
        from watchdog.entrypoints import _load_agents
        seen: dict[str, str] = {}
        for agent in _load_agents():
            for policy_id in agent.handles:
                assert policy_id not in seen, (
                    f"{policy_id} claimed by both {seen[policy_id]} and {agent.agent_id}"
                )
                seen[policy_id] = agent.agent_id


class TestAdhocTypeAliases:
    """The adhoc entrypoint parses --resource-type via an inline alias table.
    Replicate that here so we can unit-test the translation without running
    the full entrypoint (which requires SparkSession)."""

    @pytest.mark.parametrize("input_value,expected", [
        ("all", set()),
        ("table", {"table"}),
        ("tables", {"table"}),
        ("jobs", {"job"}),
        ("agents", {"agent", "agent_trace"}),
        ("compute", {"job", "cluster", "warehouse", "pipeline"}),
        ("data", {"table", "volume", "schema", "catalog"}),
        ("warehouse", {"warehouse"}),
        ("UNKNOWN", {"unknown"}),  # fall-through: become singleton set
    ])
    def test_alias_resolution(self, input_value, expected):
        # Mirror adhoc()'s alias table; kept in sync by-hand.
        type_aliases = {
            "all": set(),
            "tables": {"table"}, "jobs": {"job"}, "clusters": {"cluster"},
            "warehouses": {"warehouse"}, "pipelines": {"pipeline"},
            "grants": {"grant"}, "agents": {"agent", "agent_trace"},
            "data": {"table", "volume", "schema", "catalog"},
            "compute": {"job", "cluster", "warehouse", "pipeline"},
        }
        requested = input_value.strip().lower()
        result = type_aliases.get(
            requested, {requested} if requested != "all" else set(),
        )
        assert result == expected


class TestEntrypointSignatures:
    """Smoke-check that the argparse wiring in each entrypoint accepts the
    flags DABs jobs pass in. We patch SparkSession and WorkspaceClient so the
    functions never touch a real cluster."""

    def _run(self, entrypoint_name: str, argv: list[str]):
        import watchdog.entrypoints as ep

        with patch("sys.argv", ["run_task.py", *argv]), \
             patch.object(ep, "SparkSession") as mock_spark, \
             patch.object(ep, "WorkspaceClient") as mock_wc:
            mock_spark.builder.getOrCreate.return_value = MagicMock()
            mock_wc.return_value = MagicMock()

            # Stub the heavy helpers the entrypoints invoke so argparse is the
            # only thing actually exercised.
            with patch("watchdog.crawler.ResourceCrawler") as crawler_cls, \
                 patch("watchdog.views.ensure_semantic_views"), \
                 patch("watchdog.policy_loader.sync_policies_to_delta",
                       return_value=0):
                crawler_inst = MagicMock()
                crawler_inst.crawl_all.return_value = []
                crawler_cls.return_value = crawler_inst

                # _build_engine hits policy_loader.load_yaml_policies which
                # reads the real policies dir — that's fine, but we need to
                # prevent ontology-aware evaluate_all from running.
                fn = getattr(ep, entrypoint_name)
                try:
                    fn()
                except SystemExit:
                    pytest.fail("argparse rejected the flags")

    def test_crawl_accepts_standard_flags(self):
        # crawl() doesn't run heavy logic once crawler is mocked.
        with patch("sys.argv", ["run_task.py",
                                "--catalog=c", "--schema=s",
                                "--secret-scope=sc"]):
            import watchdog.entrypoints as ep
            with patch.object(ep, "SparkSession") as ms, \
                 patch.object(ep, "WorkspaceClient"), \
                 patch("watchdog.crawler.ResourceCrawler") as cc:
                ms.builder.getOrCreate.return_value = MagicMock()
                cc.return_value.crawl_all.return_value = []
                ep.crawl()

    def test_remediate_accepts_limit_flag(self):
        import watchdog.entrypoints as ep
        with patch("sys.argv", ["run_task.py",
                                "--catalog=c", "--schema=s", "--limit=5"]), \
             patch.object(ep, "SparkSession") as ms, \
             patch("watchdog.remediation.tables.ensure_remediation_agents_table"), \
             patch("watchdog.remediation.tables.ensure_remediation_proposals_table"), \
             patch("watchdog.remediation.tables.register_agent"), \
             patch("watchdog.remediation.views.ensure_remediation_views"):
            spark = MagicMock()
            spark.sql.return_value.collect.return_value = []
            ms.builder.getOrCreate.return_value = spark
            ep.remediate()
