# Medallion Generalizations Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extract three medallion architecture patterns into Watchdog core: parameterize the Ontos IRI, add a pipeline freshness crawler, and create medallion governance policies.

**Architecture:** The Ontos IRI fix is a parameter extraction. The pipeline freshness crawler follows the existing DQM enrichment pattern (read system table, UPDATE inventory tags). The medallion policies use existing ontology classes and rule types, plus one new `metadata_lte` rule type.

**Tech Stack:** Python, PySpark, YAML (policies/ontology), pytest

---

## File Map

| File | Action | Responsibility |
|------|--------|----------------|
| `engine/src/watchdog/rule_engine.py` | modify | Add `metadata_lte` rule type |
| `engine/src/watchdog/crawler.py` | modify | Add `_crawl_pipeline_freshness()` enrichment |
| `engine/ontologies/rule_primitives.yml` | modify | Add 4 medallion primitives |
| `engine/policies/medallion_governance.yml` | create | 8 medallion policies |
| `ontos-adapter/src/watchdog_governance/ontos_sync.py` | modify | Parameterize IRI |
| `tests/unit/test_rule_engine.py` | modify | Tests for `metadata_lte` + medallion policy loading |
| `tests/unit/test_crawler_freshness.py` | create | Tests for pipeline freshness tag derivation |
| `tests/unit/test_ontos_sync.py` | create | Tests for IRI parameterization |

---

### Task 1: Add `metadata_lte` Rule Type

**Files:**
- Modify: `engine/src/watchdog/rule_engine.py:75-89` (dispatch table), append after `_eval_metadata_gte`
- Modify: `tests/unit/test_rule_engine.py` (append new test class)

- [ ] **Step 1: Write failing tests for `metadata_lte`**

Append to `tests/unit/test_rule_engine.py` after the `TestMetadataGte` class (after line ~206):

```python
# ── metadata_lte ─────────────────────────────────────────────────────────────

class TestMetadataLte:
    """metadata_lte: fail if metadata field exceeds threshold."""

    RULE = {"type": "metadata_lte", "field": "freshness_hours", "threshold": "48"}

    def test_pass_below_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "12"})
        assert result.passed

    def test_pass_exact_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "48"})
        assert result.passed

    def test_fail_above_threshold(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "72"})
        assert not result.passed
        assert "72" in result.detail

    def test_fail_empty_field(self, bare):
        result = bare.evaluate(self.RULE, {}, {})
        assert not result.passed

    def test_fail_non_numeric(self, bare):
        result = bare.evaluate(self.RULE, {}, {"freshness_hours": "unknown"})
        assert not result.passed
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py::TestMetadataLte -v`

Expected: FAIL — `metadata_lte` is an unknown rule type.

- [ ] **Step 3: Add `metadata_lte` to dispatch table and implement evaluator**

In `engine/src/watchdog/rule_engine.py`, add to the dispatch dict (line ~89, after the `"if_then"` entry):

```python
            "metadata_lte": self._eval_metadata_lte,
```

Add the inline shorthand handler in `_evaluate_inline()` (after the `metadata_gte` block around line ~151):

```python
        if "metadata_lte" in rule:
            inner = rule["metadata_lte"]
            return self._eval_metadata_lte({
                "field": inner.get("field", ""),
                "threshold": str(inner.get("value", inner.get("threshold", ""))),
            }, tags, metadata)
```

Add the evaluator method after `_eval_metadata_gte` (after line ~315):

```python
    def _eval_metadata_lte(self, rule: dict, tags: dict[str, str],
                           metadata: dict[str, str]) -> RuleResult:
        """Fail if a metadata field value exceeds the threshold.

        Uses the same version-aware comparison as metadata_gte but reverses
        the direction: field value must be <= threshold.
        """
        f = rule.get("field", "")
        threshold = str(rule.get("threshold", ""))
        actual = metadata.get(f, "")
        if not actual:
            return RuleResult(
                passed=False,
                detail=f"Metadata field '{f}' is empty (threshold: <= {threshold})",
                rule_type="metadata_lte",
            )
        try:
            actual_ver = self._extract_version(actual)
            threshold_ver = self._extract_version(threshold)
            if actual_ver > threshold_ver:
                return RuleResult(
                    passed=False,
                    detail=f"Metadata '{f}' is '{actual}' (> {threshold})",
                    rule_type="metadata_lte",
                )
        except (ValueError, TypeError):
            if actual > threshold:
                return RuleResult(
                    passed=False,
                    detail=f"Metadata '{f}' is '{actual}' (> {threshold})",
                    rule_type="metadata_lte",
                )
        return RuleResult(passed=True, rule_type="metadata_lte")
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py::TestMetadataLte -v`

Expected: All 5 tests PASS.

- [ ] **Step 5: Update rule primitives header comment**

In `engine/ontologies/rule_primitives.yml`, add `metadata_lte` to the rule types list in the header comment (after `metadata_gte` on line ~15):

```yaml
#   metadata_lte:      Metadata field <= threshold (numeric/version comparison)
```

- [ ] **Step 6: Run full rule engine test suite**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py -v`

Expected: All existing tests still pass + 5 new tests pass.

- [ ] **Step 7: Commit**

```bash
git add engine/src/watchdog/rule_engine.py engine/ontologies/rule_primitives.yml tests/unit/test_rule_engine.py
git commit -m "feat: add metadata_lte rule type for maximum threshold policies"
```

---

### Task 2: Add Medallion Rule Primitives

**Files:**
- Modify: `engine/ontologies/rule_primitives.yml` (append new primitives)
- Modify: `tests/unit/test_rule_engine.py` (add primitive evaluation tests)

- [ ] **Step 1: Write failing tests for new primitives**

Append to `tests/unit/test_rule_engine.py`:

```python
# ── Medallion primitives ─────────────────────────────────────────────────────

class TestMedallionPrimitives:
    """Verify medallion governance primitives load and evaluate correctly."""

    def test_has_source_system_pass(self, engine):
        result = engine.evaluate({"ref": "has_source_system"}, {"source_system": "SAP"}, {})
        assert result.passed

    def test_has_source_system_fail(self, engine):
        result = engine.evaluate({"ref": "has_source_system"}, {}, {})
        assert not result.passed
        assert "source_system" in result.detail

    def test_has_ingestion_owner_pass(self, engine):
        result = engine.evaluate({"ref": "has_ingestion_owner"}, {"ingestion_owner": "alice@co.com"}, {})
        assert result.passed

    def test_has_ingestion_owner_fail(self, engine):
        result = engine.evaluate({"ref": "has_ingestion_owner"}, {}, {})
        assert not result.passed

    def test_has_source_documentation_pass(self, engine):
        result = engine.evaluate({"ref": "has_source_documentation"}, {"source_documentation": "https://wiki/transform"}, {})
        assert result.passed

    def test_has_source_documentation_fail(self, engine):
        result = engine.evaluate({"ref": "has_source_documentation"}, {}, {})
        assert not result.passed

    def test_silver_has_classification_pass_silver_with_class(self, engine):
        result = engine.evaluate({"ref": "silver_has_classification"}, {"data_layer": "silver", "data_classification": "internal"}, {})
        assert result.passed

    def test_silver_has_classification_fail_silver_no_class(self, engine):
        result = engine.evaluate({"ref": "silver_has_classification"}, {"data_layer": "silver"}, {})
        assert not result.passed

    def test_silver_has_classification_pass_non_silver(self, engine):
        """Non-silver tables should pass vacuously (if_then condition not met)."""
        result = engine.evaluate({"ref": "silver_has_classification"}, {"data_layer": "bronze"}, {})
        assert result.passed
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py::TestMedallionPrimitives -v`

Expected: FAIL — primitives not found.

- [ ] **Step 3: Add primitives to rule_primitives.yml**

Append to `engine/ontologies/rule_primitives.yml` before the final comment block (before line ~309, after the agent governance section):

```yaml
  # ── Medallion Governance ──────────────────────────────────────────────

  has_source_system:
    type: tag_exists
    description: "Ingestion table identifies its source system (ERP, API, file drop, etc.)"
    keys: [source_system]

  has_ingestion_owner:
    type: tag_exists
    description: "Ingestion table has a designated owner responsible for the data feed"
    keys: [ingestion_owner]

  has_source_documentation:
    type: tag_exists
    description: "Table has a link to transformation or reconciliation documentation"
    keys: [source_documentation]

  silver_has_classification:
    type: if_then
    description: "Silver tables must have data classification applied"
    condition:
      type: tag_equals
      key: data_layer
      value: "silver"
    then:
      type: tag_exists
      keys: [data_classification]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py::TestMedallionPrimitives -v`

Expected: All 9 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/ontologies/rule_primitives.yml tests/unit/test_rule_engine.py
git commit -m "feat: add medallion governance rule primitives"
```

---

### Task 3: Create Medallion Governance Policies

**Files:**
- Create: `engine/policies/medallion_governance.yml`
- Modify: `tests/unit/test_rule_engine.py` (add policy loading/validation tests)

- [ ] **Step 1: Write failing tests for medallion policies**

Append to `tests/unit/test_rule_engine.py`:

```python
class TestMedallionPolicies:
    """Verify medallion_governance.yml loads and passes structural validation."""

    @staticmethod
    def _load_policies_yaml(policies_dir: str) -> list:
        import yaml
        policies = []
        for yaml_file in sorted(Path(policies_dir).glob("*.yml")):
            with open(yaml_file) as f:
                data = yaml.safe_load(f)
            if data and "policies" in data:
                for p in data["policies"]:
                    policies.append(p)
        return policies

    def test_medallion_policies_load(self, ontology_dir):
        policies_dir = str(Path(ontology_dir).parent / "policies")
        policies = self._load_policies_yaml(policies_dir)
        med_ids = [p["id"] for p in policies if p["id"].startswith("POL-MED")]
        assert len(med_ids) == 8
        for i in range(1, 9):
            assert f"POL-MED-{i:03d}" in med_ids

    def test_medallion_policies_have_required_fields(self, ontology_dir):
        policies_dir = str(Path(ontology_dir).parent / "policies")
        policies = self._load_policies_yaml(policies_dir)
        required_keys = ["id", "name", "applies_to", "domain", "severity",
                         "description", "remediation", "rule"]
        for p in policies:
            if not p["id"].startswith("POL-MED"):
                continue
            for key in required_keys:
                assert key in p and p[key], f"{p['id']} missing or empty: {key}"
            assert p.get("active") is True, f"{p['id']} should be active"

    def test_medallion_policies_reference_valid_classes(self, ontology_dir):
        """All applies_to values must be known ontology classes."""
        policies_dir = str(Path(ontology_dir).parent / "policies")
        policies = self._load_policies_yaml(policies_dir)
        import yaml
        with open(Path(ontology_dir) / "resource_classes.yml") as f:
            classes_data = yaml.safe_load(f)
        known_classes = set(classes_data.get("base_classes", {}).keys())
        known_classes.update(classes_data.get("derived_classes", {}).keys())
        for p in policies:
            if not p["id"].startswith("POL-MED"):
                continue
            assert p["applies_to"] in known_classes, (
                f"{p['id']} references unknown class: {p['applies_to']}"
            )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py::TestMedallionPolicies -v`

Expected: FAIL — no POL-MED policies found.

- [ ] **Step 3: Create `engine/policies/medallion_governance.yml`**

```yaml
# Medallion Governance Policies
# Domain: DataQuality + OperationalGovernance
#
# Policies for Bronze, Silver, and Gold layer governance plus
# pipeline freshness. Uses existing ontology classes (BronzeTable,
# SilverTable, GoldTable, ProductionPipeline) and medallion primitives.

policies:

  # ── Bronze Layer ─────────────────────────────────────────────────────

  - id: POL-MED-001
    name: "Bronze tables must identify their source system"
    applies_to: BronzeTable
    domain: DataQuality
    severity: high
    description: >
      Raw ingestion tables must be tagged with the source system they ingest
      from (e.g., SAP, Oracle, Salesforce, API). Without source tracking,
      lineage is broken and impact analysis is impossible.
    remediation: "Add a 'source_system' tag identifying the upstream source"
    active: true
    rule:
      ref: has_source_system

  - id: POL-MED-002
    name: "Bronze tables must have an ingestion owner"
    applies_to: BronzeTable
    domain: DataQuality
    severity: medium
    description: >
      Each ingestion feed needs a designated owner who is accountable for
      data freshness and schema changes from the source system.
    remediation: "Add an 'ingestion_owner' tag with the responsible person's email"
    active: true
    rule:
      ref: has_ingestion_owner

  # ── Silver Layer ─────────────────────────────────────────────────────

  - id: POL-MED-003
    name: "Silver tables must have a data steward"
    applies_to: SilverTable
    domain: DataQuality
    severity: high
    description: >
      The Silver layer is where raw data becomes business-usable. A named
      steward is accountable for transformation logic, data quality, and
      reconciliation correctness.
    remediation: "Add a 'data_steward' tag with the responsible person's email"
    active: true
    rule:
      ref: has_data_steward

  - id: POL-MED-004
    name: "Silver tables must have data classification"
    applies_to: SilverTable
    domain: DataQuality
    severity: high
    description: >
      Classification should be applied at the Silver layer where data is
      cleaned and conformed. Bronze is too raw (classifications may not
      yet be determinable). Gold inherits from Silver.
    remediation: "Add a 'data_classification' tag (public, internal, confidential, restricted, pii)"
    active: true
    rule:
      ref: has_data_classification

  - id: POL-MED-005
    name: "Silver tables must document their source transformations"
    applies_to: SilverTable
    domain: DataQuality
    severity: medium
    description: >
      Silver tables transform and reconcile data from one or more Bronze
      sources. The transformation logic must be documented so downstream
      consumers can understand data provenance.
    remediation: >
      Add a 'source_documentation' tag with a link to the transformation
      documentation (e.g., Confluence page, README, or notebook URL)
    active: true
    rule:
      ref: has_source_documentation

  # ── Gold Layer ───────────────────────────────────────────────────────

  - id: POL-MED-006
    name: "Gold tables must have a retention policy"
    applies_to: GoldTable
    domain: DataQuality
    severity: medium
    description: >
      Curated consumption tables must specify how long data is retained.
      Without retention policies, storage grows unbounded and compliance
      obligations (GDPR, HIPAA, SOX) cannot be verified.
    remediation: "Add a 'retention_days' tag with the maximum retention period in days"
    active: true
    rule:
      ref: has_retention_policy

  # ── Cross-Layer (Pipeline Freshness) ─────────────────────────────────

  - id: POL-MED-007
    name: "Production pipelines must be healthy"
    applies_to: ProductionPipeline
    domain: OperationalGovernance
    severity: critical
    description: >
      Production DLT pipelines must have completed successfully within the
      last 7 days. Failing pipelines mean stale data is reaching downstream
      consumers. Requires the pipeline freshness crawler to be active.
    remediation: >
      1. Check pipeline logs for failure cause
      2. Fix the failing pipeline and trigger a manual run
      3. Verify pipeline_health tag updates to 'healthy' on next Watchdog scan
    active: true
    rule:
      metadata_equals:
        pipeline_health: "healthy"

  - id: POL-MED-008
    name: "Production pipelines must have succeeded within 48 hours"
    applies_to: ProductionPipeline
    domain: OperationalGovernance
    severity: high
    description: >
      Production pipelines that haven't succeeded in 48 hours are delivering
      stale data. This is a tighter check than pipeline_health — a pipeline
      can be 'degraded' and still violate this policy if its last success
      is too old. Adjust the 48-hour threshold for your pipeline cadence.
    remediation: >
      1. Investigate why the pipeline hasn't completed successfully
      2. Check for upstream dependencies, cluster issues, or schema changes
      3. The 48-hour threshold assumes daily pipelines — adjust for your cadence
    active: true
    rule:
      type: metadata_lte
      field: freshness_hours
      threshold: "48"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py::TestMedallionPolicies -v`

Expected: All 3 tests PASS.

- [ ] **Step 5: Run full test suite to verify no regressions**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py -v`

Expected: All tests pass, including the existing `test_all_policy_files_load_without_errors` which now picks up the new file.

- [ ] **Step 6: Commit**

```bash
git add engine/policies/medallion_governance.yml tests/unit/test_rule_engine.py
git commit -m "feat: add medallion governance policies for Bronze/Silver/Gold layers"
```

---

### Task 4: Add Pipeline Freshness Crawler

**Files:**
- Create: `tests/unit/test_crawler_freshness.py`
- Modify: `engine/src/watchdog/crawler.py:107-168` (register in `crawl_all`), append method after `_crawl_pipelines`

- [ ] **Step 1: Write tests for freshness tag derivation**

Create `tests/unit/test_crawler_freshness.py`:

```python
"""Unit tests for pipeline freshness tag derivation logic.

Tests the pure-Python health derivation function without Spark or SDK.
The actual crawler method reads system tables and calls this function.

Run with: pytest tests/unit/test_crawler_freshness.py -v
"""
from datetime import datetime, timezone, timedelta

import pytest


def derive_pipeline_health(
    last_success_at: str | None,
    last_failure_at: str | None,
    failure_count_7d: int,
    now: datetime | None = None,
) -> dict:
    """Derive pipeline freshness tags from run history.

    Extracted as a pure function for testability. The crawler calls this
    with data from system.lakeflow.pipeline_event_log.

    Returns dict of tags to merge into the pipeline's inventory row.
    """
    # Import here so test can run before implementation exists
    from watchdog.crawler import derive_pipeline_health as impl
    return impl(last_success_at, last_failure_at, failure_count_7d, now)


class TestDerivePipelineHealth:

    NOW = datetime(2026, 4, 15, 12, 0, 0, tzinfo=timezone.utc)

    def test_healthy_recent_success_no_failures(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T10:00:00Z",
            last_failure_at=None,
            failure_count_7d=0,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "healthy"
        assert result["freshness_hours"] == "2"
        assert result["failure_count_7d"] == "0"

    def test_degraded_recent_success_with_failures(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T10:00:00Z",
            last_failure_at="2026-04-15T08:00:00Z",
            failure_count_7d=3,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "degraded"
        assert result["freshness_hours"] == "2"
        assert result["failure_count_7d"] == "3"

    def test_failing_last_run_failed(self):
        """Last failure is more recent than last success."""
        result = derive_pipeline_health(
            last_success_at="2026-04-14T10:00:00Z",
            last_failure_at="2026-04-15T11:00:00Z",
            failure_count_7d=1,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "failing"

    def test_failing_no_runs(self):
        result = derive_pipeline_health(
            last_success_at=None,
            last_failure_at=None,
            failure_count_7d=0,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "failing"
        assert result["freshness_hours"] == "-1"

    def test_failing_no_success_only_failures(self):
        result = derive_pipeline_health(
            last_success_at=None,
            last_failure_at="2026-04-15T11:00:00Z",
            failure_count_7d=5,
            now=self.NOW,
        )
        assert result["pipeline_health"] == "failing"
        assert result["freshness_hours"] == "-1"

    def test_freshness_hours_rounds_down(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T09:30:00Z",
            last_failure_at=None,
            failure_count_7d=0,
            now=self.NOW,
        )
        assert result["freshness_hours"] == "2"  # 2.5 hours rounds down

    def test_tags_include_timestamps(self):
        result = derive_pipeline_health(
            last_success_at="2026-04-15T10:00:00Z",
            last_failure_at="2026-04-14T08:00:00Z",
            failure_count_7d=1,
            now=self.NOW,
        )
        assert result["last_success_at"] == "2026-04-15T10:00:00Z"
        assert result["last_failure_at"] == "2026-04-14T08:00:00Z"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_crawler_freshness.py -v`

Expected: FAIL — `derive_pipeline_health` not found in `watchdog.crawler`.

- [ ] **Step 3: Implement `derive_pipeline_health` function**

Add to `engine/src/watchdog/crawler.py` as a module-level function (after the `INVENTORY_SCHEMA` definition, before the `ResourceCrawler` class, around line ~46):

```python
def derive_pipeline_health(
    last_success_at: str | None,
    last_failure_at: str | None,
    failure_count_7d: int,
    now: datetime | None = None,
) -> dict:
    """Derive pipeline freshness tags from run history.

    Pure function for testability. Called by _crawl_pipeline_freshness()
    with data from system.lakeflow.pipeline_event_log.

    Returns dict of tags to merge into the pipeline's inventory row.
    """
    now = now or datetime.now(timezone.utc)
    tags = {
        "last_success_at": last_success_at or "",
        "last_failure_at": last_failure_at or "",
        "failure_count_7d": str(failure_count_7d),
    }

    if not last_success_at:
        tags["freshness_hours"] = "-1"
        tags["pipeline_health"] = "failing"
        return tags

    success_dt = datetime.fromisoformat(last_success_at.replace("Z", "+00:00"))
    hours_since = int((now - success_dt).total_seconds() // 3600)
    tags["freshness_hours"] = str(hours_since)

    # Determine health
    if last_failure_at:
        failure_dt = datetime.fromisoformat(last_failure_at.replace("Z", "+00:00"))
        if failure_dt > success_dt:
            tags["pipeline_health"] = "failing"
            return tags

    if failure_count_7d > 0:
        tags["pipeline_health"] = "degraded"
    else:
        tags["pipeline_health"] = "healthy"

    return tags
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/test_crawler_freshness.py -v`

Expected: All 7 tests PASS.

- [ ] **Step 5: Implement `_crawl_pipeline_freshness` method**

Add to `ResourceCrawler` class in `engine/src/watchdog/crawler.py`, after the `_crawl_pipelines` method (after line ~798):

```python
    def _crawl_pipeline_freshness(self) -> list:
        """Enrich pipeline inventory rows with freshness tags from system tables.

        Reads system.lakeflow.pipeline_event_log for the last 7 days,
        computes per-pipeline health metrics, and UPDATEs the existing
        pipeline rows in resource_inventory with freshness tags.

        Follows the same pattern as _crawl_dqm_status: enrichment via
        UPDATE, graceful fallback if system table isn't available.
        """
        try:
            event_rows = self.spark.sql("""
                SELECT
                    pipeline_id,
                    event_type,
                    timestamp,
                    message
                FROM system.lakeflow.pipeline_event_log
                WHERE timestamp >= current_timestamp() - INTERVAL 7 DAY
                  AND event_type IN ('create_update', 'update_progress')
            """).collect()
        except Exception as e:
            print(f"  Pipeline event log not available: {e}")
            return []

        if not event_rows:
            return []

        # Aggregate per pipeline: last success, last failure, failure count
        from collections import defaultdict
        pipeline_stats = defaultdict(lambda: {
            "last_success": None, "last_failure": None, "failure_count": 0
        })

        for row in event_rows:
            pid = row.pipeline_id
            ts = row.timestamp.isoformat() if row.timestamp else None
            msg = (row.message or "").upper()

            if row.event_type == "update_progress":
                if "COMPLETED" in msg:
                    cur = pipeline_stats[pid]["last_success"]
                    if cur is None or ts > cur:
                        pipeline_stats[pid]["last_success"] = ts
                elif "FAILED" in msg or "ERROR" in msg:
                    pipeline_stats[pid]["failure_count"] += 1
                    cur = pipeline_stats[pid]["last_failure"]
                    if cur is None or ts > cur:
                        pipeline_stats[pid]["last_failure"] = ts

        # Enrich inventory rows
        for pid, stats in pipeline_stats.items():
            health_tags = derive_pipeline_health(
                last_success_at=stats["last_success"],
                last_failure_at=stats["last_failure"],
                failure_count_7d=stats["failure_count"],
                now=self.now,
            )
            # Build SET clause for tag updates
            tag_updates = ", ".join(
                f"'{k}', '{v}'" for k, v in health_tags.items()
            )
            self.spark.sql(f"""
                UPDATE {self.inventory_table}
                SET tags = map_concat(tags, map({tag_updates}))
                WHERE scan_id = '{self.scan_id}'
                  AND resource_type = 'pipeline'
                  AND resource_id = '{pid}'
            """)

        print(f"  pipeline_freshness: enriched {len(pipeline_stats)} pipelines")
        return []  # Enrichment only, no new inventory rows
```

- [ ] **Step 6: Register in `crawl_all()`**

In `engine/src/watchdog/crawler.py`, in the `crawl_all()` method, add `_crawl_pipeline_freshness` to the DQ enrichment block (around line ~161, after `_crawl_lhm_status`):

```python
        # DQ system table crawlers (enrich tags on table resources)
        for crawler_fn in [
            self._crawl_dqm_status,
            self._crawl_lhm_status,
            self._crawl_pipeline_freshness,
        ]:
```

- [ ] **Step 7: Run full unit test suite**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/ -v`

Expected: All tests pass.

- [ ] **Step 8: Commit**

```bash
git add engine/src/watchdog/crawler.py tests/unit/test_crawler_freshness.py
git commit -m "feat: add pipeline freshness crawler with system table enrichment"
```

---

### Task 5: Parameterize Ontos IRI

**Files:**
- Modify: `ontos-adapter/src/watchdog_governance/ontos_sync.py:44` (remove hardcoded IRI)
- Create: `tests/unit/test_ontos_sync.py`

- [ ] **Step 1: Write tests for IRI resolution**

Create `tests/unit/test_ontos_sync.py`:

```python
"""Unit tests for Ontos IRI parameterization.

Tests the IRI resolution fallback chain without calling any external APIs.

Run with: pytest tests/unit/test_ontos_sync.py -v
"""
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# Mock pyspark and databricks.sdk before importing ontos_sync
_pyspark = types.ModuleType("pyspark")
_pyspark_sql = types.ModuleType("pyspark.sql")
_pyspark_sql.SparkSession = MagicMock
sys.modules.setdefault("pyspark", _pyspark)
sys.modules.setdefault("pyspark.sql", _pyspark_sql)

_databricks = types.ModuleType("databricks")
_databricks_sdk = types.ModuleType("databricks.sdk")
_databricks_sdk.WorkspaceClient = MagicMock
sys.modules.setdefault("databricks", _databricks)
sys.modules.setdefault("databricks.sdk", _databricks_sdk)

# Add ontos-adapter to path
REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT / "ontos-adapter" / "src"))

from watchdog_governance.ontos_sync import resolve_ontology_base_iri


class TestResolveOntologyBaseIri:

    def test_explicit_parameter_wins(self):
        result = resolve_ontology_base_iri(
            ontology_base_iri="https://custom.com/onto/",
            workspace_host="https://workspace.cloud.databricks.com",
        )
        assert result == "https://custom.com/onto/"

    def test_env_var_fallback(self):
        with patch.dict(os.environ, {"WATCHDOG_ONTOLOGY_BASE_IRI": "https://env.com/onto/"}):
            result = resolve_ontology_base_iri(
                ontology_base_iri=None,
                workspace_host="https://workspace.cloud.databricks.com",
            )
        assert result == "https://env.com/onto/"

    def test_workspace_host_fallback(self):
        with patch.dict(os.environ, {}, clear=True):
            # Ensure env var is not set
            os.environ.pop("WATCHDOG_ONTOLOGY_BASE_IRI", None)
            result = resolve_ontology_base_iri(
                ontology_base_iri=None,
                workspace_host="https://myworkspace.cloud.databricks.com",
            )
        assert result == "https://myworkspace.cloud.databricks.com/ontology/watchdog/class/"

    def test_trailing_slash_enforced(self):
        result = resolve_ontology_base_iri(
            ontology_base_iri="https://custom.com/onto",
            workspace_host="https://workspace.cloud.databricks.com",
        )
        assert result.endswith("/")

    def test_env_var_trailing_slash_enforced(self):
        with patch.dict(os.environ, {"WATCHDOG_ONTOLOGY_BASE_IRI": "https://env.com/onto"}):
            result = resolve_ontology_base_iri(
                ontology_base_iri=None,
                workspace_host="https://workspace.cloud.databricks.com",
            )
        assert result.endswith("/")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && pytest tests/unit/test_ontos_sync.py -v`

Expected: FAIL — `resolve_ontology_base_iri` not found.

- [ ] **Step 3: Implement IRI resolution and update sync function**

In `ontos-adapter/src/watchdog_governance/ontos_sync.py`:

Replace the hardcoded constant (line 43-44):

```python
# Watchdog ontology class IRI base — matches the wdc: prefix in the TTL
ONTOLOGY_BASE_IRI = "https://<workspace>.databricks.com/ontology/watchdog/class/"
```

With:

```python
def resolve_ontology_base_iri(
    ontology_base_iri: str | None = None,
    workspace_host: str = "",
) -> str:
    """Resolve the ontology class IRI base with fallback chain.

    Priority:
      1. Explicit ontology_base_iri parameter
      2. WATCHDOG_ONTOLOGY_BASE_IRI environment variable
      3. Default: https://{workspace_host}/ontology/watchdog/class/
    """
    iri = ontology_base_iri or os.environ.get("WATCHDOG_ONTOLOGY_BASE_IRI", "")
    if not iri:
        host = workspace_host.rstrip("/")
        iri = f"{host}/ontology/watchdog/class/"
    if not iri.endswith("/"):
        iri += "/"
    return iri
```

Update `sync_classifications_to_ontos()` signature — add `ontology_base_iri` parameter after `ontos_url`:

```python
def sync_classifications_to_ontos(
    spark: SparkSession,
    catalog: str,
    schema: str,
    ontos_url: str,
    ontology_base_iri: Optional[str] = None,
    ontos_token: Optional[str] = None,
    ...
```

Inside the function body, resolve the IRI (add after the token resolution, before the classifications query):

```python
    client = w or WorkspaceClient()
    base_iri = resolve_ontology_base_iri(
        ontology_base_iri=ontology_base_iri,
        workspace_host=client.config.host or "",
    )
```

Update the IRI construction in the loop (replace `ONTOLOGY_BASE_IRI` reference):

```python
        iri = f"{base_iri}{row.class_name}"
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && pytest tests/unit/test_ontos_sync.py -v`

Expected: All 5 tests PASS.

- [ ] **Step 5: Run full unit test suite**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && PYTHONPATH=engine/src pytest tests/unit/ -v`

Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add ontos-adapter/src/watchdog_governance/ontos_sync.py tests/unit/test_ontos_sync.py
git commit -m "fix: parameterize Ontos IRI, remove hardcoded URL"
```

---

### Task 6: Update Documentation

**Files:**
- Modify: `docs/guide/concepts/architecture.md` (mention pipeline freshness in engine section)
- Modify: `docs/architecture-guide.md` (update crawler count, add `metadata_lte` to rule types)

- [ ] **Step 1: Update architecture guide crawler count**

In `docs/architecture-guide.md`, the component map shows "16 types" for crawlers. Update to "17 types" to reflect the new pipeline freshness enrichment crawler. Also add `metadata_lte` to the rule types mention if rule types are listed.

- [ ] **Step 2: Update concepts/architecture.md engine description**

In `docs/guide/concepts/architecture.md`, find the line about "Sixteen crawlers enumerate workspace resources" in the Stage 1 description. Update to "Seventeen crawlers" and add a note about pipeline freshness enrichment.

- [ ] **Step 3: Commit**

```bash
git add docs/guide/concepts/architecture.md docs/architecture-guide.md
git commit -m "docs: update crawler count and rule types for medallion governance"
```
