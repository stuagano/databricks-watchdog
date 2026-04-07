"""Fixtures for watchdog integration tests.

Integration tests need a live Spark session (databricks-connect or running
inside a Databricks cluster). They run against an isolated test schema that
is created fresh and torn down after each test session.

Configuration (environment variables):
    WATCHDOG_TEST_CATALOG      Unity Catalog catalog to use for the test schema.
                               Required — tests skip if not set.
    DATABRICKS_CONFIG_PROFILE  Databricks CLI profile for databricks-connect.
                               Optional if default profile is configured.

Run:
    export WATCHDOG_TEST_CATALOG=<your_catalog>
    export DATABRICKS_CONFIG_PROFILE=<your_profile>   # if non-default
    cd bundles/watchdog-bundle
    pytest tests/integration/ -v -m integration

Skip in CI when no catalog is configured:
    pytest tests/integration/ --ignore-glob="*integration*"

Test schema isolation:
    Each session creates <catalog>.watchdog_test_<uuid> to avoid conflicts
    with production data. The schema is dropped after the session.
"""
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

import pytest

# Fixture resources used across integration tests.
#
# Each entry maps to one row in resource_inventory.  Tags and metadata are
# chosen to produce known, predictable violation sets so tests can assert
# on specific (resource_id, policy_id) pairs.
FIXTURE_RESOURCES = [
    {
        # CLEAN: fully tagged gold table — should produce zero violations for
        # cost and security policies once properly tagged
        "resource_id": "test/table/gold_clean",
        "resource_type": "table",
        "resource_name": "gold_clean",
        "tags": {
            "data_layer": "gold",
            "data_classification": "internal",
            "owner": "owner@example.com",
            "business_unit": "dosimetry",
            "environment": "prod",
        },
        "metadata": {"comment": "Curated gold layer output"},
        "owner": "owner@example.com",
    },
    {
        # VIOLATING: PII table missing data_steward and retention_days
        # Expected: POL-S001 (PII assets need steward + retention)
        "resource_id": "test/table/pii_no_steward",
        "resource_type": "table",
        "resource_name": "pii_no_steward",
        "tags": {
            "data_classification": "pii",
            "owner": "owner@example.com",
            "business_unit": "medical",
            # Missing: data_steward, retention_days
        },
        "metadata": {},
        "owner": "owner@example.com",
    },
    {
        # VIOLATING: completely untagged table
        # Expected: POL-C001 (no owner), POL-S003 (no data_classification),
        #           POL-C003 (no business_unit)
        "resource_id": "test/table/untagged",
        "resource_type": "table",
        "resource_name": "untagged",
        "tags": {},
        "metadata": {},
        "owner": None,
    },
    {
        # VIOLATING: interactive cluster with no autotermination, no cost_center
        # Expected: POL-C006 (InteractiveCluster needs autotermination),
        #           POL-C002 (ComputeAsset needs cost_center)
        "resource_id": "test/cluster/no_autotermination",
        "resource_type": "cluster",
        "resource_name": "no_autotermination",
        "tags": {"environment": "dev", "owner": "owner@example.com"},
        # autotermination_minutes absent → metadata_not_empty fails
        "metadata": {"spark_version": "15.4.x-scala2.12"},
        "owner": "owner@example.com",
    },
    {
        # VIOLATING: production job running on old runtime
        # Expected: POL-S005 (ProductionJob must run >= 15.4)
        "resource_id": "test/job/old_runtime",
        "resource_type": "job",
        "resource_name": "old_runtime",
        "tags": {
            "environment": "prod",
            "owner": "owner@example.com",
            "business_unit": "detection",
            "cost_center": "CC-5000",
        },
        "metadata": {"spark_version": "10.4.x-scala2.12"},
        "owner": "owner@example.com",
    },
]

FIXTURE_SCAN_ID = "test-scan-fixture-001"


@pytest.fixture(scope="session")
def spark():
    """Live SparkSession — provided by databricks-connect or Databricks runtime."""
    from pyspark.sql import SparkSession
    return SparkSession.builder.getOrCreate()


@pytest.fixture(scope="session")
def test_catalog():
    """Unity Catalog catalog for the test schema. Set WATCHDOG_TEST_CATALOG."""
    import os
    catalog = os.environ.get("WATCHDOG_TEST_CATALOG", "").strip()
    if not catalog:
        pytest.skip(
            "WATCHDOG_TEST_CATALOG not set — skipping integration tests. "
            "Set to a catalog you have CREATE SCHEMA access on."
        )
    return catalog


@pytest.fixture(scope="session")
def test_schema(spark, test_catalog):
    """Create an isolated test schema and drop it after the session."""
    uid = uuid.uuid4().hex[:8]
    schema = f"watchdog_test_{uid}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{schema}")
    yield schema
    spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.{schema} CASCADE")


@pytest.fixture(scope="session")
def seed_inventory(spark, test_catalog, test_schema):
    """Populate resource_inventory with known fixture rows.

    Returns the scan_id so tests can reference it.
    """
    import pyspark.sql.types as T
    from watchdog.violations import ensure_violations_table

    inv_table = f"{test_catalog}.{test_schema}.resource_inventory"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {inv_table} (
            scan_id STRING NOT NULL,
            resource_id STRING NOT NULL,
            resource_type STRING NOT NULL,
            resource_name STRING,
            tags MAP<STRING, STRING>,
            metadata MAP<STRING, STRING>,
            owner STRING,
            crawled_at TIMESTAMP NOT NULL
        )
        USING DELTA
        CLUSTER BY (scan_id, resource_type)
    """)

    now = datetime.now(timezone.utc)
    rows = [
        (
            FIXTURE_SCAN_ID,
            r["resource_id"],
            r["resource_type"],
            r["resource_name"],
            r["tags"],
            r["metadata"],
            r["owner"],
            now,
        )
        for r in FIXTURE_RESOURCES
    ]

    _schema = T.StructType([
        T.StructField("scan_id", T.StringType()),
        T.StructField("resource_id", T.StringType()),
        T.StructField("resource_type", T.StringType()),
        T.StructField("resource_name", T.StringType()),
        T.StructField("tags", T.MapType(T.StringType(), T.StringType())),
        T.StructField("metadata", T.MapType(T.StringType(), T.StringType())),
        T.StructField("owner", T.StringType()),
        T.StructField("crawled_at", T.TimestampType()),
    ])
    df = spark.createDataFrame(rows, schema=_schema)
    df.write.mode("overwrite").saveAsTable(inv_table)

    # Ensure downstream tables exist
    ensure_violations_table(spark, test_catalog, test_schema)

    return FIXTURE_SCAN_ID


@pytest.fixture(scope="session")
def policy_engine(spark, test_catalog, test_schema):
    """PolicyEngine loaded with all YAML policies."""
    from databricks.sdk import WorkspaceClient
    from watchdog.ontology import OntologyEngine
    from watchdog.rule_engine import RuleEngine
    from watchdog.policy_engine import PolicyEngine
    from watchdog.policy_loader import load_yaml_policies, load_delta_policies

    bundle_root = Path(__file__).parent.parent.parent
    ontology = OntologyEngine(ontology_dir=str(bundle_root / "ontologies"))
    rule_engine = RuleEngine(primitives_dir=str(bundle_root / "ontologies"))
    yaml_policies = load_yaml_policies(policies_dir=str(bundle_root / "policies"))
    user_policies = load_delta_policies(spark, test_catalog, test_schema)

    w = WorkspaceClient()
    return PolicyEngine(
        spark, w, test_catalog, test_schema,
        ontology=ontology,
        rule_engine=rule_engine,
        policies=yaml_policies + user_policies,
    )
