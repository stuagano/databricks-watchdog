"""End-to-end pipeline assertions — crawl + evaluate against a real workspace.

These tests create fixture tables in an isolated ``watchdog_test_<uid>`` schema
under ``$WATCHDOG_TEST_CATALOG``, run the production crawl + evaluate flow,
and assert that the expected violations appear. They are skipped in CI by
default — see ``conftest.py``.
"""
import uuid

import pytest


@pytest.fixture(scope="module")
def isolated_schema(spark, test_catalog):
    uid = uuid.uuid4().hex[:8]
    schema = f"watchdog_test_{uid}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{schema}")
    yield test_catalog, schema
    spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.{schema} CASCADE")


@pytest.fixture
def seeded_fixtures(spark, isolated_schema):
    catalog, schema = isolated_schema
    tables = {
        "gold_clean": {
            "tags": {"data_layer": "gold", "data_classification": "internal",
                     "business_unit": "platform", "environment": "prod"},
            "expected": set(),
        },
        "untagged": {
            "tags": {},
            "expected": {"POL-C001", "POL-C003"},
        },
    }
    for name, spec in tables.items():
        fqn = f"{catalog}.{schema}.{name}"
        spark.sql(f"CREATE TABLE IF NOT EXISTS {fqn} (id BIGINT) USING DELTA")
        for k, v in spec["tags"].items():
            spark.sql(f"ALTER TABLE {fqn} SET TAGS ('{k}' = '{v}')")
    return catalog, schema, tables


def test_crawl_populates_inventory(spark, seeded_fixtures):
    catalog, schema, tables = seeded_fixtures
    from databricks.sdk import WorkspaceClient
    from watchdog.crawler import ResourceCrawler

    crawler = ResourceCrawler(spark, WorkspaceClient(), catalog, schema)
    crawler.crawl_all(resource_types={"table"})

    row = spark.sql(f"""
        SELECT COUNT(*) AS c FROM {catalog}.{schema}.resource_inventory
        WHERE resource_type = 'table'
    """).collect()[0]
    assert row.c >= len(tables)


def test_evaluate_produces_expected_violations(spark, seeded_fixtures):
    catalog, schema, tables = seeded_fixtures
    from databricks.sdk import WorkspaceClient
    from watchdog.ontology import OntologyEngine
    from watchdog.policy_engine import PolicyEngine
    from watchdog.policy_loader import load_yaml_policies
    from watchdog.rule_engine import RuleEngine

    w = WorkspaceClient()
    engine = PolicyEngine(
        spark, w, catalog, schema,
        ontology=OntologyEngine(),
        rule_engine=RuleEngine(),
        policies=load_yaml_policies(),
    )
    engine.evaluate_all()

    for name, spec in tables.items():
        fqn = f"{catalog}.{schema}.{name}"
        rows = spark.sql(f"""
            SELECT DISTINCT policy_id
            FROM {catalog}.{schema}.violations
            WHERE resource_name = '{fqn}' AND status = 'open'
        """).collect()
        actual = {r.policy_id for r in rows}
        missing = spec["expected"] - actual
        assert not missing, f"{name}: missing expected violations {missing}"
