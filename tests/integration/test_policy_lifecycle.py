"""Integration tests — policy evaluation against fixture resource inventory.

These tests verify end-to-end: fixture data in resource_inventory → classify
→ evaluate rules → violations table.  Each test asserts on specific
(resource_id, policy_id) pairs so regressions are immediately locatable.

Depends on: p-watchdog merged (watchdog bundle code + ontologies + policies).
Requires: live Spark session (databricks-connect or Databricks cluster).
Run:   pytest tests/integration/test_policy_lifecycle.py -v -m integration
"""
import pytest

pytestmark = pytest.mark.integration


def _get_violations(spark, catalog, schema, resource_id=None):
    """Fetch violations for a resource (or all violations if resource_id is None)."""
    table = f"{catalog}.{schema}.violations"
    q = f"SELECT resource_id, policy_id, status, severity FROM {table}"
    if resource_id:
        q += f" WHERE resource_id = '{resource_id}'"
    return {(r.resource_id, r.policy_id): r for r in spark.sql(q).collect()}


@pytest.fixture(scope="module", autouse=True)
def run_evaluate(spark, test_catalog, test_schema, seed_inventory, policy_engine):
    """Run evaluate_all once for the whole module. seed_inventory populates the data."""
    policy_engine.evaluate_all()


# ── Clean resource: no violations ────────────────────────────────────────────

class TestCleanResource:
    def test_gold_clean_has_no_violations(self, spark, test_catalog, test_schema):
        """A fully tagged gold table should produce zero open violations."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/table/gold_clean")
        open_viols = {k: v for k, v in viols.items() if v.status == "open"}
        assert open_viols == {}, (
            f"Expected no open violations for gold_clean, got: "
            f"{[k[1] for k in open_viols]}"
        )


# ── PII table missing steward ─────────────────────────────────────────────────

class TestPiiNoSteward:
    def test_pii_s001_fires(self, spark, test_catalog, test_schema):
        """PII asset without data_steward + retention_days → POL-S001."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/table/pii_no_steward")
        key = ("test/table/pii_no_steward", "POL-S001")
        assert key in viols, "POL-S001 should fire for PII table missing steward"
        assert viols[key].status == "open"
        assert viols[key].severity == "critical"

    def test_pii_no_spurious_cost_violations(self, spark, test_catalog, test_schema):
        """The PII table has owner and business_unit — POL-C001 and POL-C003 should not fire."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/table/pii_no_steward")
        no_owner_key = ("test/table/pii_no_steward", "POL-C001")
        no_bu_key = ("test/table/pii_no_steward", "POL-C003")
        assert no_owner_key not in viols or viols[no_owner_key].status != "open"
        assert no_bu_key not in viols or viols[no_bu_key].status != "open"


# ── Completely untagged table ─────────────────────────────────────────────────

class TestUntaggedTable:
    def test_no_owner_violation(self, spark, test_catalog, test_schema):
        """Table with no owner tag or metadata owner → POL-C001."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/table/untagged")
        key = ("test/table/untagged", "POL-C001")
        assert key in viols, "POL-C001 should fire for untagged table"
        assert viols[key].status == "open"

    def test_no_classification_violation(self, spark, test_catalog, test_schema):
        """Table with no data_classification tag → POL-S003."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/table/untagged")
        key = ("test/table/untagged", "POL-S003")
        assert key in viols, "POL-S003 should fire for table missing data_classification"
        assert viols[key].status == "open"

    def test_no_business_unit_violation(self, spark, test_catalog, test_schema):
        """Table with no business_unit tag → POL-C003."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/table/untagged")
        key = ("test/table/untagged", "POL-C003")
        assert key in viols, "POL-C003 should fire for table missing business_unit"


# ── Interactive cluster: no autotermination ───────────────────────────────────

class TestClusterNoAutotermination:
    def test_autotermination_violation(self, spark, test_catalog, test_schema):
        """Interactive cluster without autotermination_minutes → POL-C006."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/cluster/no_autotermination")
        key = ("test/cluster/no_autotermination", "POL-C006")
        assert key in viols, "POL-C006 should fire for cluster without autotermination"
        assert viols[key].status == "open"

    def test_cost_center_violation(self, spark, test_catalog, test_schema):
        """Cluster without cost_center tag → POL-C002."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/cluster/no_autotermination")
        key = ("test/cluster/no_autotermination", "POL-C002")
        assert key in viols, "POL-C002 should fire for cluster missing cost_center"


# ── Production job: old runtime ───────────────────────────────────────────────

class TestOldRuntimeJob:
    def test_runtime_violation(self, spark, test_catalog, test_schema):
        """ProductionJob with spark_version 10.4.x < 15.4 → POL-S005."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/job/old_runtime")
        key = ("test/job/old_runtime", "POL-S005")
        assert key in viols, (
            "POL-S005 should fire for job with spark_version 10.4.x (below 15.4 threshold)"
        )
        assert viols[key].status == "open"
        assert viols[key].severity == "high"

    def test_job_no_owner_violation_absent(self, spark, test_catalog, test_schema):
        """The old runtime job has an owner — POL-C001 should NOT fire."""
        viols = _get_violations(spark, test_catalog, test_schema,
                                resource_id="test/job/old_runtime")
        key = ("test/job/old_runtime", "POL-C001")
        # Either not present or resolved/exception
        if key in viols:
            assert viols[key].status != "open", (
                "POL-C001 should not be open — job has owner=owner@example.com"
            )


# ── Ontology classification in views ─────────────────────────────────────────

class TestSemanticViews:
    @pytest.fixture(autouse=True)
    def refresh_views(self, spark, test_catalog, test_schema):
        from watchdog.views import ensure_semantic_views
        ensure_semantic_views(spark, test_catalog, test_schema)

    def test_v_class_compliance_has_pii_class(self, spark, test_catalog, test_schema):
        """PiiTable class should appear in v_class_compliance after evaluation."""
        rows = spark.sql(f"""
            SELECT class_name, open_violations
            FROM {test_catalog}.{test_schema}.v_class_compliance
            WHERE class_name = 'PiiTable'
        """).collect()
        assert rows, "PiiTable should appear in v_class_compliance"
        assert rows[0].open_violations > 0, "PiiTable should have open violations"

    def test_v_resource_compliance_clean_resource(self, spark, test_catalog, test_schema):
        """gold_clean should show compliance_status=clean in the view."""
        rows = spark.sql(f"""
            SELECT compliance_status
            FROM {test_catalog}.{test_schema}.v_resource_compliance
            WHERE resource_id = 'test/table/gold_clean'
        """).collect()
        assert rows, "gold_clean should appear in v_resource_compliance"
        assert all(r.compliance_status == "clean" for r in rows), (
            f"gold_clean should be clean, got: {[r.compliance_status for r in rows]}"
        )
