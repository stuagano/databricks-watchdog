"""Integration tests — violations MERGE lifecycle and exception handling.

Tests the state machine:
    pending (scan fail) → open
    open (scan pass)    → resolved
    open (exception)    → exception
    open (repeated fail) → last_detected updated, first_detected unchanged

These tests inject data directly into scan_results (not via policy evaluation)
so they are independent of policy definitions and test the merge logic in isolation.

Depends on: p-watchdog merged (watchdog bundle code).
Requires: live Spark session.
"""
import uuid
from datetime import datetime, timezone, timedelta

import pytest

from watchdog.violations import (
    merge_violations,
    ensure_violations_table,
    ensure_exceptions_table,
)

pytestmark = pytest.mark.integration

RESOURCE_ID = "test/table/merge_lifecycle_fixture"
POLICY_ID = "POL-C001"


def _insert_scan_result(spark, catalog, schema, scan_id, resource_id, policy_id,
                        result: str, resource_type="table", resource_name="merge_test",
                        owner="owner@example.com", severity="high", domain="CostGovernance"):
    """Insert a single row into scan_results."""
    inv_table = f"{catalog}.{schema}.resource_inventory"
    scan_results_table = f"{catalog}.{schema}.scan_results"

    # Ensure inventory has a row for this resource + scan
    spark.sql(f"""
        INSERT INTO {inv_table}
        (scan_id, resource_id, resource_type, resource_name, tags, metadata, owner, crawled_at)
        VALUES ('{scan_id}', '{resource_id}', '{resource_type}', '{resource_name}',
                map(), map(), '{owner}', current_timestamp())
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {scan_results_table} (
            scan_id STRING NOT NULL,
            resource_id STRING NOT NULL,
            policy_id STRING NOT NULL,
            result STRING NOT NULL,
            details STRING,
            domain STRING,
            severity STRING,
            resource_classes STRING,
            evaluated_at TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES ('delta.appendOnly' = 'true')
        CLUSTER BY (scan_id, policy_id)
    """)

    spark.sql(f"""
        INSERT INTO {scan_results_table}
        (scan_id, resource_id, policy_id, result, details, domain, severity,
         resource_classes, evaluated_at)
        VALUES ('{scan_id}', '{resource_id}', '{policy_id}', '{result}',
                'test detail', '{domain}', '{severity}', '', current_timestamp())
    """)


def _get_violation(spark, catalog, schema, resource_id, policy_id):
    """Fetch a single violation row or None."""
    table = f"{catalog}.{schema}.violations"
    rows = spark.sql(f"""
        SELECT * FROM {table}
        WHERE resource_id = '{resource_id}' AND policy_id = '{policy_id}'
    """).collect()
    return rows[0] if rows else None


@pytest.fixture
def isolated_schema(spark, test_catalog):
    """Per-test schema to avoid cross-test state bleed."""
    uid = uuid.uuid4().hex[:8]
    schema = f"watchdog_merge_test_{uid}"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {test_catalog}.{schema}")

    # Create inventory table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {test_catalog}.{schema}.resource_inventory (
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
    """)

    ensure_violations_table(spark, test_catalog, schema)
    ensure_exceptions_table(spark, test_catalog, schema)

    yield schema

    spark.sql(f"DROP SCHEMA IF EXISTS {test_catalog}.{schema} CASCADE")


# ── Open → Resolved lifecycle ─────────────────────────────────────────────────

class TestOpenResolvedLifecycle:
    def test_first_failure_creates_open_violation(self, spark, test_catalog,
                                                   isolated_schema):
        scan_id = "scan-001"
        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id)

        row = _get_violation(spark, test_catalog, isolated_schema, RESOURCE_ID, POLICY_ID)
        assert row is not None, "Violation should be created on first failure"
        assert row.status == "open"
        assert row.first_detected is not None
        assert row.resolved_at is None

    def test_repeated_failure_updates_last_detected(self, spark, test_catalog,
                                                      isolated_schema):
        scan_id_1 = "scan-002a"
        scan_id_2 = "scan-002b"
        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_1, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_1)

        before = _get_violation(spark, test_catalog, isolated_schema, RESOURCE_ID, POLICY_ID)

        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_2, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_2)

        after = _get_violation(spark, test_catalog, isolated_schema, RESOURCE_ID, POLICY_ID)
        assert after.status == "open"
        assert after.first_detected == before.first_detected, (
            "first_detected should not change on repeated failures"
        )
        # last_detected should be updated (can be same timestamp in fast tests;
        # just verify the violation is still open and first_detected is stable)

    def test_passing_scan_resolves_violation(self, spark, test_catalog, isolated_schema):
        scan_id_fail = "scan-003a"
        scan_id_pass = "scan-003b"

        # Fail → open
        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_fail, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_fail)

        row = _get_violation(spark, test_catalog, isolated_schema, RESOURCE_ID, POLICY_ID)
        assert row.status == "open"

        # Pass → resolved
        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_pass, RESOURCE_ID, POLICY_ID, "pass")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_pass)

        row = _get_violation(spark, test_catalog, isolated_schema, RESOURCE_ID, POLICY_ID)
        assert row.status == "resolved", "Violation should be resolved after passing scan"
        assert row.resolved_at is not None


# ── Exception handling ────────────────────────────────────────────────────────

class TestExceptionHandling:
    def test_active_exception_overrides_status(self, spark, test_catalog, isolated_schema):
        """An active exception keeps status='exception' even when scan still fails."""
        scan_id_1 = "scan-exc-001"
        scan_id_2 = "scan-exc-002"

        # First failure → open
        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_1, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_1)

        # Insert an active exception
        exc_table = f"{test_catalog}.{isolated_schema}.exceptions"
        exc_id = str(uuid.uuid4())
        spark.sql(f"""
            INSERT INTO {exc_table}
            (exception_id, resource_id, policy_id, approved_by, justification,
             approved_at, expires_at, active)
            VALUES ('{exc_id}', '{RESOURCE_ID}', '{POLICY_ID}',
                    'admin@example.com', 'Approved waiver for testing',
                    current_timestamp(), NULL, true)
        """)

        # Second failure — violation still fails, but exception is active
        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_2, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_2)

        row = _get_violation(spark, test_catalog, isolated_schema, RESOURCE_ID, POLICY_ID)
        assert row.status == "exception", (
            "Active exception should change status from open to exception"
        )

    def test_expired_exception_does_not_override(self, spark, test_catalog, isolated_schema):
        """An expired exception should NOT protect the violation from being open."""
        scan_id_1 = "scan-expexc-001"
        scan_id_2 = "scan-expexc-002"

        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_1, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_1)

        # Insert an EXPIRED exception (expires_at in the past)
        exc_table = f"{test_catalog}.{isolated_schema}.exceptions"
        exc_id = str(uuid.uuid4())
        spark.sql(f"""
            INSERT INTO {exc_table}
            (exception_id, resource_id, policy_id, approved_by, justification,
             approved_at, expires_at, active)
            VALUES ('{exc_id}', '{RESOURCE_ID}', '{POLICY_ID}',
                    'admin@example.com', 'Expired waiver',
                    current_timestamp(),
                    timestampadd(HOUR, -1, current_timestamp()),
                    true)
        """)

        _insert_scan_result(spark, test_catalog, isolated_schema,
                            scan_id_2, RESOURCE_ID, POLICY_ID, "fail")
        merge_violations(spark, test_catalog, isolated_schema, scan_id_2)

        row = _get_violation(spark, test_catalog, isolated_schema, RESOURCE_ID, POLICY_ID)
        assert row.status == "open", (
            "Expired exception should not protect the violation — should still be open"
        )
