# Hub Integration: Schema-First Contract Hardening — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Harden the consumer contract for 6 Hub-facing compliance views with an explicit schema contract, fix broken view dependencies, add CI integration tests, and create a live smoke test notebook.

**Architecture:** A YAML contract file (`hub_contract.yml`) defines expected column names, types, and semantics for each view. Integration tests validate views produce SQL matching the contract. A policies Delta table is added so `v_tag_policy_coverage` can resolve its CROSS JOIN. A Databricks notebook validates views on a live workspace.

**Tech Stack:** Python, PyYAML, pytest, PySpark (mocked in tests), Databricks notebooks

---

### Task 1: Create the Hub Contract YAML

**Files:**
- Create: `engine/hub_contract.yml`

- [ ] **Step 1: Create the contract file**

This file defines the schema for all 6 Hub-facing views. Column types use Spark SQL type names. Each view includes grain, description, hub_panel, and a columns list.

```yaml
# engine/hub_contract.yml
# Schema contract for Hub-facing compliance views.
# Tests and smoke notebooks validate against this file.
version: 1

views:
  v_domain_compliance:
    description: "Aggregated compliance posture per governance domain"
    grain: "1 row per domain"
    hub_panel: "Governance Dashboard overlay"
    columns:
      - name: domain
        type: STRING
        nullable: false
        description: "Governance domain (SecurityGovernance, CostGovernance, etc.)"
        example: "SecurityGovernance"
      - name: resources_affected
        type: BIGINT
        nullable: false
        description: "Count of distinct resources with violations in this domain"
        example: 2352
      - name: open_violations
        type: BIGINT
        nullable: false
        description: "Count of open violations in this domain"
        example: 4480
      - name: critical_open
        type: BIGINT
        nullable: false
        description: "Count of open critical-severity violations"
        example: 8
      - name: high_open
        type: BIGINT
        nullable: false
        description: "Count of open high-severity violations"
        example: 4480
      - name: excepted_violations
        type: BIGINT
        nullable: false
        description: "Count of violations with approved exceptions"
        example: 12
      - name: resolved_violations
        type: BIGINT
        nullable: false
        description: "Count of resolved violations"
        example: 150
      - name: classes_with_open_violations
        type: ARRAY<STRING>
        nullable: true
        description: "Ontology classes that have open violations in this domain"
        example: ["PiiAsset,DataAsset", "GoldTable"]

  v_class_compliance:
    description: "Aggregated compliance posture per ontology class"
    grain: "1 row per class_name"
    hub_panel: "Drill-down by ontology class"
    columns:
      - name: class_name
        type: STRING
        nullable: false
        description: "Ontology class name"
        example: "PiiAsset"
      - name: root_class
        type: STRING
        nullable: true
        description: "Root ancestor class (DataAsset, ComputeAsset, etc.)"
        example: "DataAsset"
      - name: total_resources
        type: BIGINT
        nullable: false
        description: "Count of distinct resources classified into this class"
        example: 450
      - name: resources_with_open_violations
        type: BIGINT
        nullable: false
        description: "Count of resources with at least one open violation"
        example: 23
      - name: compliance_pct
        type: DOUBLE
        nullable: true
        description: "Percentage of resources with no open violations (0-100)"
        example: 94.9
      - name: open_violations
        type: BIGINT
        nullable: false
        description: "Total open violation count"
        example: 45
      - name: critical_open
        type: BIGINT
        nullable: false
        description: "Open critical violations"
        example: 2
      - name: high_open
        type: BIGINT
        nullable: false
        description: "Open high violations"
        example: 15
      - name: medium_open
        type: BIGINT
        nullable: false
        description: "Open medium violations"
        example: 28
      - name: excepted_violations
        type: BIGINT
        nullable: false
        description: "Violations with approved exceptions"
        example: 3
      - name: resolved_violations
        type: BIGINT
        nullable: false
        description: "Resolved violations"
        example: 120

  v_resource_compliance:
    description: "Per-resource compliance posture within each assigned ontology class"
    grain: "1 row per (resource_id, class_name)"
    hub_panel: "Drill-down by resource"
    columns:
      - name: resource_id
        type: STRING
        nullable: false
        description: "Unique resource identifier"
        example: "gold.finance.gl_balances"
      - name: resource_name
        type: STRING
        nullable: true
        description: "Human-readable resource name"
        example: "gold.finance.gl_balances"
      - name: resource_type
        type: STRING
        nullable: true
        description: "Resource type (table, volume, job, etc.)"
        example: "table"
      - name: owner
        type: STRING
        nullable: true
        description: "Resource owner"
        example: "stuart.gano@company.com"
      - name: class_name
        type: STRING
        nullable: false
        description: "Ontology class this row represents"
        example: "PiiAsset"
      - name: class_ancestors
        type: STRING
        nullable: true
        description: "Comma-separated ancestor chain"
        example: "ConfidentialAsset,DataAsset"
      - name: root_class
        type: STRING
        nullable: true
        description: "Root ancestor class"
        example: "DataAsset"
      - name: open_violations
        type: BIGINT
        nullable: false
        description: "Count of open violations for this resource"
        example: 3
      - name: critical_open
        type: BIGINT
        nullable: false
        description: "Open critical violations"
        example: 1
      - name: high_open
        type: BIGINT
        nullable: false
        description: "Open high violations"
        example: 2
      - name: medium_open
        type: BIGINT
        nullable: false
        description: "Open medium violations"
        example: 0
      - name: excepted_violations
        type: BIGINT
        nullable: false
        description: "Violations with approved exceptions"
        example: 0
      - name: oldest_open_violation
        type: TIMESTAMP
        nullable: true
        description: "First-detected date of the oldest open violation"
        example: "2026-03-15T10:00:00Z"
      - name: last_violation_at
        type: TIMESTAMP
        nullable: true
        description: "Most recent last_detected across all violations"
        example: "2026-04-14T08:30:00Z"
      - name: compliance_status
        type: STRING
        nullable: false
        description: "Overall status: critical, high, open, or clean"
        example: "high"

  v_tag_policy_coverage:
    description: "Per-resource tag policy compliance state"
    grain: "1 row per (resource_id, policy_id)"
    hub_panel: "Tag compliance panel"
    columns:
      - name: resource_id
        type: STRING
        nullable: false
        description: "Resource identifier"
        example: "gold.finance.gl_balances"
      - name: resource_type
        type: STRING
        nullable: true
        description: "Resource type"
        example: "table"
      - name: resource_name
        type: STRING
        nullable: true
        description: "Human-readable resource name"
        example: "gold.finance.gl_balances"
      - name: owner
        type: STRING
        nullable: true
        description: "Resource owner"
        example: "stuart.gano@company.com"
      - name: policy_id
        type: STRING
        nullable: false
        description: "Policy identifier"
        example: "POL-SEC-001"
      - name: policy_name
        type: STRING
        nullable: true
        description: "Policy display name"
        example: "PII tables must have data steward"
      - name: severity
        type: STRING
        nullable: true
        description: "Policy severity level"
        example: "critical"
      - name: coverage_status
        type: STRING
        nullable: false
        description: "One of: satisfied, violated, not_evaluated"
        example: "violated"
      - name: violation_status
        type: STRING
        nullable: true
        description: "Current violation status (open, exception, null)"
        example: "open"
      - name: first_detected
        type: TIMESTAMP
        nullable: true
        description: "When the violation was first detected"
        example: "2026-03-15T10:00:00Z"
      - name: last_detected
        type: TIMESTAMP
        nullable: true
        description: "When the violation was last seen"
        example: "2026-04-14T08:30:00Z"
      - name: has_exception
        type: BOOLEAN
        nullable: true
        description: "Whether an active exception exists"
        example: false
      - name: exception_expires
        type: TIMESTAMP
        nullable: true
        description: "When the exception expires (null if no exception)"
        example: null

  v_data_classification_summary:
    description: "Aggregated classification posture by catalog"
    grain: "1 row per catalog"
    hub_panel: "Classification coverage panel"
    columns:
      - name: catalog_name
        type: STRING
        nullable: true
        description: "Catalog name (derived from resource_inventory.domain)"
        example: "gold"
      - name: total_tables
        type: BIGINT
        nullable: false
        description: "Total table count in this catalog"
        example: 450
      - name: classified_tables
        type: BIGINT
        nullable: false
        description: "Tables with a data_classification tag"
        example: 380
      - name: tables_with_steward
        type: BIGINT
        nullable: false
        description: "Tables with a data_steward tag"
        example: 200
      - name: sensitive_tables
        type: BIGINT
        nullable: false
        description: "Tables classified as pii, confidential, or restricted"
        example: 45
      - name: ontology_classified
        type: BIGINT
        nullable: false
        description: "Tables with PiiAsset or ConfidentialAsset ontology class"
        example: 42
      - name: classification_pct
        type: DOUBLE
        nullable: true
        description: "Percentage of tables with data_classification tag (0-100)"
        example: 84.4
      - name: stewardship_pct
        type: DOUBLE
        nullable: true
        description: "Percentage of classified tables with data_steward (0-100)"
        example: 52.6

  v_dq_monitoring_coverage:
    description: "DQ monitoring status per table"
    grain: "1 row per table"
    hub_panel: "DQ monitoring panel"
    columns:
      - name: resource_id
        type: STRING
        nullable: false
        description: "Resource identifier"
        example: "gold.finance.gl_balances"
      - name: resource_type
        type: STRING
        nullable: true
        description: "Always 'table' (view is filtered)"
        example: "table"
      - name: resource_name
        type: STRING
        nullable: true
        description: "Human-readable resource name"
        example: "gold.finance.gl_balances"
      - name: owner
        type: STRING
        nullable: true
        description: "Resource owner"
        example: "stuart.gano@company.com"
      - name: catalog_name
        type: STRING
        nullable: true
        description: "Catalog (from resource_inventory.domain)"
        example: "gold"
      - name: dqm_enabled
        type: STRING
        nullable: false
        description: "Whether DQM is enabled ('true'/'false')"
        example: "true"
      - name: lhm_enabled
        type: STRING
        nullable: false
        description: "Whether LHM is enabled ('true'/'false')"
        example: "false"
      - name: monitoring_status
        type: STRING
        nullable: false
        description: "One of: both, dqm_only, lhm_only, none"
        example: "dqm_only"
      - name: dqm_anomalies
        type: STRING
        nullable: true
        description: "Anomaly count from DQM (tag value, may be null)"
        example: "3"
      - name: dqm_metrics_checked
        type: STRING
        nullable: true
        description: "Metrics checked count from DQM (tag value, may be null)"
        example: "12"
      - name: ontology_class
        type: STRING
        nullable: true
        description: "Primary ontology class assignment"
        example: "GoldTable"
```

- [ ] **Step 2: Commit**

```bash
git add engine/hub_contract.yml
git commit -m "feat: add Hub contract YAML for 6 compliance views"
```

---

### Task 2: Add `ensure_policies_table` and write policies during scan

**Files:**
- Create: `engine/src/watchdog/policies_table.py`
- Modify: `engine/src/watchdog/policy_engine.py:129` (add call in `evaluate_all`)
- Test: `tests/unit/test_hub_contract.py` (Task 4)

The `v_tag_policy_coverage` view does `CROSS JOIN {catalog}.{schema}.policies`. That table doesn't exist — policies are loaded from YAML into dataclasses. We need to persist them to Delta.

- [ ] **Step 1: Create `policies_table.py`**

```python
# engine/src/watchdog/policies_table.py
"""Policies Table — persists active policy definitions to Delta.

The policies table is consumed by v_tag_policy_coverage (CROSS JOIN) and
by any Hub dashboard that needs to display policy metadata alongside
violation data.
"""

from datetime import datetime, timezone

from pyspark.sql import SparkSession
import pyspark.sql.types as T


POLICIES_SCHEMA = T.StructType([
    T.StructField("policy_id", T.StringType(), False),
    T.StructField("policy_name", T.StringType(), False),
    T.StructField("applies_to", T.StringType(), True),
    T.StructField("domain", T.StringType(), True),
    T.StructField("severity", T.StringType(), True),
    T.StructField("description", T.StringType(), True),
    T.StructField("remediation", T.StringType(), True),
    T.StructField("active", T.BooleanType(), True),
    T.StructField("updated_at", T.TimestampType(), True),
])


def ensure_policies_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the policies table if it doesn't exist."""
    table = f"{catalog}.{schema}.policies"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            policy_id STRING NOT NULL,
            policy_name STRING NOT NULL,
            applies_to STRING,
            domain STRING,
            severity STRING,
            description STRING,
            remediation STRING,
            active BOOLEAN,
            updated_at TIMESTAMP
        )
        USING DELTA
    """)


def write_policies(spark: SparkSession, catalog: str, schema: str,
                   policies: list) -> int:
    """Overwrite the policies table with current active policy definitions.

    Args:
        policies: List of PolicyDefinition dataclasses from the policy engine.

    Returns:
        Number of policies written.
    """
    if not policies:
        return 0

    ensure_policies_table(spark, catalog, schema)
    table = f"{catalog}.{schema}.policies"
    now = datetime.now(timezone.utc)

    rows = [
        (p.policy_id, p.name, p.applies_to, p.domain, p.severity,
         p.description, p.remediation, p.active, now)
        for p in policies
    ]

    df = spark.createDataFrame(rows, schema=POLICIES_SCHEMA)
    df.write.mode("overwrite").saveAsTable(table)
    return len(rows)
```

- [ ] **Step 2: Wire into `policy_engine.py`**

Add the import and call at the top of `evaluate_all()`, before the scan results loop.

In `engine/src/watchdog/policy_engine.py`, add the import after line 8:

```python
from watchdog.policies_table import write_policies
```

In the `evaluate_all` method, add after line 205 (`active_policies = [p for p in self.policies if p.active]`):

```python
        # Persist policy definitions to Delta for v_tag_policy_coverage
        write_policies(self.spark, self.catalog, self.schema, active_policies)
```

- [ ] **Step 3: Commit**

```bash
git add engine/src/watchdog/policies_table.py engine/src/watchdog/policy_engine.py
git commit -m "feat: persist policies to Delta table for Hub views"
```

---

### Task 3: Audit and fix view SQL issues

**Files:**
- Modify: `engine/src/watchdog/views.py`

Three issues to verify and fix.

- [ ] **Step 1: Verify `v_data_classification_summary` catalog derivation**

The view uses `ri.domain AS catalog_name`. Check whether the crawler populates `domain` for tables. Read `engine/src/watchdog/crawler.py` and search for how `domain` is set on table resources. If `domain` is the catalog name, no change needed. If `domain` is sometimes null for tables, add a COALESCE fallback.

In `engine/src/watchdog/views.py`, in `_ensure_data_classification_summary_view`, change:

```sql
            ri.domain AS catalog_name,
```

to:

```sql
            COALESCE(ri.domain, SPLIT(ri.resource_name, '\\.')[0]) AS catalog_name,
```

This ensures catalog_name is derived from the three-part resource name if `domain` is null.

- [ ] **Step 2: Verify `v_dq_monitoring_coverage` degrades gracefully**

The view uses `COALESCE(ri.tags['dqm_enabled'], 'false')` which already handles missing tags correctly (returns 'false'). The `dqm_anomalies` and `dqm_metrics_checked` columns are nullable and read directly from tags — they'll be null if the DQ crawler hasn't run. No fix needed, but document this in the contract (nullable: true).

- [ ] **Step 3: Verify `v_tag_policy_coverage` exception join**

The `exceptions` table exists at `violations.py:62-81`. The view's LEFT JOIN on `exceptions` is structurally correct. However, the `exceptions` table is only created by `merge_violations()` — if a fresh workspace hasn't run a scan yet, the table might not exist and the view will fail.

Fix: call `ensure_exceptions_table` in the view creation path. In `engine/src/watchdog/views.py`, in `_ensure_tag_policy_coverage_view`, add before the `spark.sql(...)` call:

```python
    from watchdog.violations import ensure_exceptions_table
    ensure_exceptions_table(spark, catalog, schema)
```

Similarly, add `ensure_policies_table` call:

```python
    from watchdog.policies_table import ensure_policies_table
    ensure_policies_table(spark, catalog, schema)
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/views.py
git commit -m "fix: harden Hub views — catalog fallback, table dependencies"
```

---

### Task 4: Write Hub contract integration tests

**Files:**
- Create: `tests/unit/test_hub_contract.py`

Tests follow the existing pattern in `test_views.py`: mock PySpark, capture SQL, validate structure. Additionally, a contract loader validates view SQL columns against `hub_contract.yml`.

- [ ] **Step 1: Create the test file**

```python
# tests/unit/test_hub_contract.py
"""Hub contract tests — validate compliance views against hub_contract.yml.

Tests verify that each Hub-facing view's SQL produces the columns defined
in the contract, with correct names and ordering. Uses the same mock-Spark
approach as test_views.py.

Run with: pytest tests/unit/test_hub_contract.py -v
"""
import re
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

_databricks = types.ModuleType("databricks")
_databricks_sdk = types.ModuleType("databricks.sdk")
_databricks_sdk_catalog = types.ModuleType("databricks.sdk.service.catalog")
_databricks_sdk_catalog.SecurableType = MagicMock
_databricks_sdk.WorkspaceClient = MagicMock
_databricks.sdk = _databricks_sdk

sys.modules.setdefault("databricks", _databricks)
sys.modules.setdefault("databricks.sdk", _databricks_sdk)
sys.modules.setdefault("databricks.sdk.service", types.ModuleType("databricks.sdk.service"))
sys.modules.setdefault("databricks.sdk.service.catalog", _databricks_sdk_catalog)

from watchdog.views import (  # noqa: E402
    _ensure_resource_compliance_view,
    _ensure_class_compliance_view,
    _ensure_domain_compliance_view,
    _ensure_tag_policy_coverage_view,
    _ensure_data_classification_summary_view,
    _ensure_dq_monitoring_coverage_view,
)

# ── Fixtures ─────────────────────────────────────────────────────────────────

CATALOG = "test_catalog"
SCHEMA = "test_schema"

REPO_ROOT = Path(__file__).parent.parent.parent
CONTRACT_PATH = REPO_ROOT / "engine" / "hub_contract.yml"

VIEW_FN_MAP = {
    "v_domain_compliance": _ensure_domain_compliance_view,
    "v_class_compliance": _ensure_class_compliance_view,
    "v_resource_compliance": _ensure_resource_compliance_view,
    "v_tag_policy_coverage": _ensure_tag_policy_coverage_view,
    "v_data_classification_summary": _ensure_data_classification_summary_view,
    "v_dq_monitoring_coverage": _ensure_dq_monitoring_coverage_view,
}


@pytest.fixture(scope="module")
def contract():
    """Load the hub contract YAML."""
    with open(CONTRACT_PATH) as f:
        return yaml.safe_load(f)


@pytest.fixture
def mock_spark():
    """Mock SparkSession that captures SQL strings."""
    spark = MagicMock()
    spark.sql_calls = []

    def capture_sql(sql_str):
        spark.sql_calls.append(sql_str)
        return MagicMock()

    spark.sql.side_effect = capture_sql
    return spark


def _get_view_sql(mock_spark, view_fn):
    """Call a view function and return the CREATE VIEW SQL."""
    mock_spark.sql_calls.clear()
    view_fn(mock_spark, CATALOG, SCHEMA)
    # Filter to only CREATE OR REPLACE VIEW statements
    view_sqls = [s for s in mock_spark.sql_calls if "CREATE OR REPLACE VIEW" in s]
    assert len(view_sqls) == 1, (
        f"Expected 1 CREATE VIEW from {view_fn.__name__}, got {len(view_sqls)}"
    )
    return view_sqls[0]


def _extract_select_columns(sql: str) -> list[str]:
    """Extract column names/aliases from a CREATE VIEW SELECT statement.

    Parses the SELECT clause to find column aliases (AS name) and bare
    column references. Returns lowercase column names in order.
    """
    # Remove the CREATE OR REPLACE VIEW ... AS prefix
    select_match = re.search(r'\bSELECT\b(.+)', sql, re.DOTALL | re.IGNORECASE)
    if not select_match:
        return []

    select_body = select_match.group(1)

    # Find all AS aliases — these are the output column names
    aliases = re.findall(r'\bAS\s+(\w+)', select_body, re.IGNORECASE)
    return [a.lower() for a in aliases]


# ── Contract file validation ─────────────────────────────────────────────────

class TestContractFile:
    """Verify the contract file itself is well-formed."""

    def test_contract_exists(self):
        assert CONTRACT_PATH.exists(), f"Contract file not found at {CONTRACT_PATH}"

    def test_contract_has_version(self, contract):
        assert "version" in contract
        assert contract["version"] == 1

    def test_contract_has_all_six_views(self, contract):
        expected = {
            "v_domain_compliance",
            "v_class_compliance",
            "v_resource_compliance",
            "v_tag_policy_coverage",
            "v_data_classification_summary",
            "v_dq_monitoring_coverage",
        }
        assert set(contract["views"].keys()) == expected

    def test_each_view_has_required_fields(self, contract):
        for view_name, view_def in contract["views"].items():
            assert "description" in view_def, f"{view_name} missing description"
            assert "grain" in view_def, f"{view_name} missing grain"
            assert "hub_panel" in view_def, f"{view_name} missing hub_panel"
            assert "columns" in view_def, f"{view_name} missing columns"
            assert len(view_def["columns"]) > 0, f"{view_name} has no columns"

    def test_each_column_has_required_fields(self, contract):
        for view_name, view_def in contract["views"].items():
            for col in view_def["columns"]:
                assert "name" in col, f"{view_name}: column missing name"
                assert "type" in col, f"{view_name}.{col.get('name', '?')} missing type"
                assert "nullable" in col, f"{view_name}.{col['name']} missing nullable"
                assert "description" in col, f"{view_name}.{col['name']} missing description"


# ── View SQL vs contract column matching ─────────────────────────────────────

class TestViewColumnsMatchContract:
    """Verify each view's SQL SELECT produces columns matching the contract."""

    @pytest.mark.parametrize("view_name", [
        "v_domain_compliance",
        "v_class_compliance",
        "v_resource_compliance",
        "v_tag_policy_coverage",
        "v_data_classification_summary",
        "v_dq_monitoring_coverage",
    ])
    def test_view_columns_present_in_sql(self, mock_spark, contract, view_name):
        """Every contract column name must appear in the view SQL."""
        view_fn = VIEW_FN_MAP[view_name]
        sql = _get_view_sql(mock_spark, view_fn)
        contract_columns = [c["name"] for c in contract["views"][view_name]["columns"]]

        for col_name in contract_columns:
            assert col_name in sql.lower(), (
                f"{view_name}: contract column '{col_name}' not found in view SQL"
            )


# ── View dependency validation ───────────────────────────────────────────────

class TestViewDependencies:
    """Verify views reference the correct underlying tables."""

    def test_tag_policy_coverage_references_policies_table(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.policies" in sql

    def test_tag_policy_coverage_references_exceptions_table(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_tag_policy_coverage_view)
        assert f"{CATALOG}.{SCHEMA}.exceptions" in sql

    def test_data_classification_summary_has_catalog_fallback(self, mock_spark):
        """catalog_name should use COALESCE with SPLIT fallback."""
        sql = _get_view_sql(mock_spark, _ensure_data_classification_summary_view)
        assert "COALESCE" in sql
        assert "catalog_name" in sql.lower()

    def test_dq_monitoring_coverage_has_coalesce_defaults(self, mock_spark):
        sql = _get_view_sql(mock_spark, _ensure_dq_monitoring_coverage_view)
        assert "COALESCE" in sql


# ── Policies table validation ────────────────────────────────────────────────

class TestPoliciesTable:
    """Verify the policies table schema matches what v_tag_policy_coverage expects."""

    def test_ensure_policies_table_creates_correct_schema(self):
        from watchdog.policies_table import ensure_policies_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_policies_table(spark, CATALOG, SCHEMA)

        assert len(sql_calls) == 1
        sql = sql_calls[0]
        assert "policy_id" in sql
        assert "policy_name" in sql
        assert "applies_to" in sql
        assert "domain" in sql
        assert "severity" in sql
        assert "active" in sql

    def test_policies_table_columns_match_view_join(self):
        """The view joins on p.policy_id, p.policy_name, p.severity, p.active, p.domain."""
        from watchdog.policies_table import ensure_policies_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_policies_table(spark, CATALOG, SCHEMA)

        sql = sql_calls[0]
        # These columns are referenced in v_tag_policy_coverage
        for col in ["policy_id", "policy_name", "severity", "active", "domain"]:
            assert col in sql, f"policies table missing column: {col}"
```

- [ ] **Step 2: Run tests to verify they pass**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_hub_contract.py -v
```

Expected: All tests pass (some may fail if view fixes from Task 3 aren't applied yet — run Task 3 first).

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_hub_contract.py
git commit -m "test: add Hub contract integration tests for 6 views"
```

---

### Task 5: Create the live smoke test notebook

**Files:**
- Create: `engine/notebooks/hub_smoke_test.py`

Databricks notebook format (`.py` with `# COMMAND ----------` separators).

- [ ] **Step 1: Create the notebook**

```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Hub Smoke Test
# MAGIC
# MAGIC Validates that all Hub-facing compliance views are alive, queryable,
# MAGIC and conform to the schema defined in `hub_contract.yml`.
# MAGIC
# MAGIC **Run after deployment** to verify views work against real data.
# MAGIC
# MAGIC **Prerequisites:** At least one Watchdog scan must have completed.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Set these to match your deployment
CATALOG = dbutils.widgets.get("catalog") if "dbutils" in dir() else "platform"
SCHEMA = dbutils.widgets.get("schema") if "dbutils" in dir() else "watchdog"

try:
    dbutils.widgets.text("catalog", "platform", "Catalog")
    dbutils.widgets.text("schema", "watchdog", "Schema")
    CATALOG = dbutils.widgets.get("catalog")
    SCHEMA = dbutils.widgets.get("schema")
except Exception:
    pass

print(f"Testing views in: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Contract

# COMMAND ----------

import yaml
import os

# Contract is bundled in the deployment alongside engine code
contract_paths = [
    "/Workspace/Repos/databricks-watchdog/engine/hub_contract.yml",
    os.path.join(os.path.dirname(os.path.abspath("")), "hub_contract.yml"),
    "hub_contract.yml",
]

contract = None
for path in contract_paths:
    try:
        with open(path) as f:
            contract = yaml.safe_load(f)
        print(f"Loaded contract from: {path}")
        break
    except FileNotFoundError:
        continue

if contract is None:
    raise FileNotFoundError(
        "hub_contract.yml not found. Searched: " + ", ".join(contract_paths)
    )

print(f"Contract version: {contract['version']}")
print(f"Views to validate: {list(contract['views'].keys())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Views

# COMMAND ----------

results = []

for view_name, view_def in contract["views"].items():
    fqn = f"{CATALOG}.{SCHEMA}.{view_name}"
    status = "PASS"
    notes = []
    row_count = 0
    schema_match = True

    try:
        # Query the view
        df = spark.sql(f"SELECT * FROM {fqn} LIMIT 10")
        row_count = df.count()

        # Check column names
        actual_columns = [f.name.lower() for f in df.schema.fields]
        expected_columns = [c["name"].lower() for c in view_def["columns"]]

        missing = set(expected_columns) - set(actual_columns)
        extra = set(actual_columns) - set(expected_columns)

        if missing:
            schema_match = False
            status = "FAIL"
            notes.append(f"Missing columns: {', '.join(sorted(missing))}")

        if extra:
            notes.append(f"Extra columns (not in contract): {', '.join(sorted(extra))}")

        if row_count == 0:
            if status == "PASS":
                status = "WARN"
            notes.append("No rows returned — has a scan been run?")

    except Exception as e:
        status = "FAIL"
        schema_match = False
        error_msg = str(e)[:200]
        notes.append(f"Query failed: {error_msg}")

    results.append({
        "view": view_name,
        "status": status,
        "rows": row_count,
        "schema_match": "✓" if schema_match else "✗",
        "notes": "; ".join(notes) if notes else "",
    })

# COMMAND ----------

# MAGIC %md
# MAGIC ## Results

# COMMAND ----------

import pandas as pd

results_df = pd.DataFrame(results)
print("\n" + "=" * 90)
print("HUB SMOKE TEST RESULTS")
print("=" * 90)
print(results_df.to_string(index=False))
print("=" * 90)

passed = sum(1 for r in results if r["status"] == "PASS")
warned = sum(1 for r in results if r["status"] == "WARN")
failed = sum(1 for r in results if r["status"] == "FAIL")

print(f"\nSummary: {passed} PASS, {warned} WARN, {failed} FAIL out of {len(results)} views")

if failed > 0:
    print("\n⚠ FAILURES DETECTED — review notes above for details.")
else:
    print("\n✓ All views are queryable and schema-conformant.")

# Display as Databricks table for notebook UI
if "displayHTML" in dir():
    display(spark.createDataFrame(results))
```

- [ ] **Step 2: Commit**

```bash
git add engine/notebooks/hub_smoke_test.py
git commit -m "feat: add Hub smoke test notebook for post-deployment validation"
```

---

### Task 6: Run all tests and final commit

**Files:**
- None (validation only)

- [ ] **Step 1: Run the full test suite**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_hub_contract.py tests/unit/test_views.py -v
```

Expected: All tests pass. If any fail, fix and re-run before committing.

- [ ] **Step 2: Run existing tests to verify no regressions**

```bash
PYTHONPATH=engine/src pytest tests/unit/ -v
```

Expected: All existing tests still pass.

- [ ] **Step 3: Final commit if any fixes were needed**

```bash
git add -A
git commit -m "fix: address test failures from Hub contract integration"
```
