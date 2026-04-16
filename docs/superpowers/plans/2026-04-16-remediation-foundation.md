# Remediation Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the remediation agent framework foundation — Protocol, 3 Delta tables, dispatcher, and a stub agent that proves the dispatch path works.

**Architecture:** A `RemediationAgent` Protocol defines the agent contract. The dispatcher reads open violations, routes to registered agents by policy_id, and writes proposals to Delta. A NoOpAgent stub validates the end-to-end path without LLM dependencies.

**Tech Stack:** Python, Protocol (typing), PySpark (mocked in tests), pytest

---

### Task 1: Create remediation package with Protocol and tables

**Files:**
- Create: `engine/src/watchdog/remediation/__init__.py`
- Create: `engine/src/watchdog/remediation/protocol.py`
- Create: `engine/src/watchdog/remediation/tables.py`
- Create: `engine/src/watchdog/remediation/agents/__init__.py`

- [ ] **Step 1: Create package structure**

```python
# engine/src/watchdog/remediation/__init__.py
"""Remediation Agent Framework — detect → remediate → verify → measure."""

# engine/src/watchdog/remediation/agents/__init__.py
"""Remediation agent implementations."""
```

- [ ] **Step 2: Create the Protocol**

```python
# engine/src/watchdog/remediation/protocol.py
"""RemediationAgent Protocol — contract for pluggable remediation agents.

Agents implement this protocol to participate in the dispatch pipeline.
The dispatcher calls gather_context then propose_fix for each matching
violation. apply and verify are reserved for sub-project 3b.
"""

from typing import Protocol, runtime_checkable


@runtime_checkable
class RemediationAgent(Protocol):
    """Contract for a pluggable remediation agent.

    Attributes:
        agent_id: Unique identifier for this agent.
        handles: Policy IDs this agent can remediate (e.g., ["POL-S001"]).
        version: Agent version string for reproducibility.
        model: LLM model used, or empty string for deterministic agents.
    """
    agent_id: str
    handles: list[str]
    version: str
    model: str

    def gather_context(self, violation: dict) -> dict:
        """Collect context needed to propose a fix.

        Args:
            violation: Row from the violations table as a dict.

        Returns:
            Context dict with keys relevant to the agent's domain.
        """
        ...

    def propose_fix(self, context: dict) -> dict:
        """Generate a fix proposal from gathered context.

        Args:
            context: Dict returned by gather_context.

        Returns:
            Dict with keys: proposed_sql, confidence (float 0-1),
            context_json (serialized context), citations (optional).
        """
        ...
```

- [ ] **Step 3: Create table definitions**

```python
# engine/src/watchdog/remediation/tables.py
"""Remediation Delta tables — agents registry and proposals.

Three tables for the foundation layer:
  - remediation_agents: registry of available agents
  - remediation_proposals: proposed fixes with evidence trail
"""

from pyspark.sql import SparkSession


def ensure_remediation_agents_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the remediation_agents registry table."""
    table = f"{catalog}.{schema}.remediation_agents"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            agent_id STRING NOT NULL,
            handles STRING NOT NULL,
            version STRING NOT NULL,
            model STRING,
            config_json STRING,
            permissions STRING,
            active BOOLEAN DEFAULT true,
            registered_at TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported'
        )
    """)


def ensure_remediation_proposals_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the remediation_proposals table."""
    table = f"{catalog}.{schema}.remediation_proposals"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            proposal_id STRING NOT NULL,
            violation_id STRING NOT NULL,
            agent_id STRING NOT NULL,
            agent_version STRING NOT NULL,
            status STRING NOT NULL DEFAULT 'pending_review',
            proposed_sql STRING,
            confidence DOUBLE,
            context_json STRING,
            llm_prompt_hash STRING,
            citations STRING,
            created_at TIMESTAMP NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.feature.allowColumnDefaults' = 'supported'
        )
    """)


def register_agent(spark: SparkSession, catalog: str, schema: str,
                    agent) -> None:
    """Register an agent in the remediation_agents table.

    Args:
        agent: Object satisfying the RemediationAgent protocol.
    """
    import json
    from datetime import datetime, timezone

    ensure_remediation_agents_table(spark, catalog, schema)
    table = f"{catalog}.{schema}.remediation_agents"

    import pyspark.sql.types as T

    schema_def = T.StructType([
        T.StructField("agent_id", T.StringType(), False),
        T.StructField("handles", T.StringType(), False),
        T.StructField("version", T.StringType(), False),
        T.StructField("model", T.StringType(), True),
        T.StructField("config_json", T.StringType(), True),
        T.StructField("permissions", T.StringType(), True),
        T.StructField("active", T.BooleanType(), True),
        T.StructField("registered_at", T.TimestampType(), False),
    ])

    row = [(
        agent.agent_id,
        ",".join(agent.handles),
        agent.version,
        agent.model,
        None,
        None,
        True,
        datetime.now(timezone.utc),
    )]

    df = spark.createDataFrame(row, schema=schema_def)
    df.write.mode("append").saveAsTable(table)
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/remediation/
git commit -m "feat: add remediation package — Protocol, tables, package structure"
```

---

### Task 2: Add remediation_status column to violations table

**Files:**
- Modify: `engine/src/watchdog/violations.py`
- Test: `tests/unit/test_remediation.py`

- [ ] **Step 1: Add remediation_status to violations schema**

In `engine/src/watchdog/violations.py`, in `ensure_violations_table`, add `remediation_status STRING` after the `notified_at TIMESTAMP` line (before the closing parenthesis):

Change:
```sql
            notified_at TIMESTAMP
        )
```

To:
```sql
            notified_at TIMESTAMP,
            remediation_status STRING
        )
```

- [ ] **Step 2: Commit**

```bash
git add engine/src/watchdog/violations.py
git commit -m "feat: add remediation_status column to violations table"
```

---

### Task 3: Create NoOp stub agent

**Files:**
- Create: `engine/src/watchdog/remediation/agents/noop.py`
- Test: `tests/unit/test_remediation.py`

- [ ] **Step 1: Create test file with protocol and agent tests**

```python
# tests/unit/test_remediation.py
"""Unit tests for remediation framework — protocol, dispatcher, tables.

Run with: pytest tests/unit/test_remediation.py -v
"""
import json
import sys
import types
from unittest.mock import MagicMock

import pytest

# ── Mock PySpark before importing watchdog modules ───────────────────────────

_pyspark = types.ModuleType("pyspark")
_pyspark_sql = types.ModuleType("pyspark.sql")
_pyspark_sql_functions = types.ModuleType("pyspark.sql.functions")
_pyspark_sql_types = types.ModuleType("pyspark.sql.types")

_pyspark_sql.SparkSession = MagicMock
_pyspark_sql.DataFrame = MagicMock
_pyspark_sql.Row = MagicMock
_pyspark_sql_functions.col = MagicMock

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
_databricks_sdk.WorkspaceClient = MagicMock
_databricks.sdk = _databricks_sdk

sys.modules.setdefault("databricks", _databricks)
sys.modules.setdefault("databricks.sdk", _databricks_sdk)

from watchdog.remediation.protocol import RemediationAgent
from watchdog.remediation.agents.noop import NoOpAgent


# ── Protocol conformance ─────────────────────────────────────────────────────

class TestProtocolConformance:
    def test_noop_agent_is_remediation_agent(self):
        agent = NoOpAgent()
        assert isinstance(agent, RemediationAgent)

    def test_noop_agent_has_required_attributes(self):
        agent = NoOpAgent()
        assert agent.agent_id == "noop-agent"
        assert agent.handles == ["POL-TEST-001"]
        assert agent.version == "1.0.0"
        assert agent.model == ""

    def test_noop_agent_gather_context(self):
        agent = NoOpAgent()
        violation = {
            "violation_id": "v-001",
            "resource_id": "gold.finance.gl_balances",
            "resource_name": "gold.finance.gl_balances",
            "policy_id": "POL-TEST-001",
            "severity": "high",
            "owner": "stuart.gano@company.com",
        }
        context = agent.gather_context(violation)
        assert "violation" in context
        assert context["violation"] == violation

    def test_noop_agent_propose_fix(self):
        agent = NoOpAgent()
        context = {
            "violation": {
                "resource_name": "gold.finance.gl_balances",
                "policy_id": "POL-TEST-001",
            }
        }
        proposal = agent.propose_fix(context)
        assert "proposed_sql" in proposal
        assert "confidence" in proposal
        assert proposal["confidence"] == 0.1
        assert "gold.finance.gl_balances" in proposal["proposed_sql"]


# ── Table schema validation ──────────────────────────────────────────────────

class TestRemediationTables:
    def test_agents_table_has_correct_columns(self):
        from watchdog.remediation.tables import ensure_remediation_agents_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_remediation_agents_table(spark, "cat", "sch")

        sql = sql_calls[0]
        for col in ["agent_id", "handles", "version", "model", "active", "registered_at"]:
            assert col in sql, f"Missing column: {col}"

    def test_proposals_table_has_correct_columns(self):
        from watchdog.remediation.tables import ensure_remediation_proposals_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_remediation_proposals_table(spark, "cat", "sch")

        sql = sql_calls[0]
        for col in ["proposal_id", "violation_id", "agent_id", "agent_version",
                     "status", "proposed_sql", "confidence", "context_json",
                     "llm_prompt_hash", "citations", "created_at"]:
            assert col in sql, f"Missing column: {col}"

    def test_violations_table_has_remediation_status(self):
        from watchdog.violations import ensure_violations_table
        spark = MagicMock()
        sql_calls = []
        spark.sql.side_effect = lambda s: sql_calls.append(s) or MagicMock()

        ensure_violations_table(spark, "cat", "sch")

        sql = sql_calls[0]
        assert "remediation_status" in sql
```

- [ ] **Step 2: Create NoOpAgent**

```python
# engine/src/watchdog/remediation/agents/noop.py
"""NoOp Agent — stub implementation for testing the dispatch pipeline.

Always proposes a trivial fix with low confidence. Used to validate
that the dispatcher, tables, and protocol work end-to-end without
requiring an LLM or external data sources.
"""

import json


class NoOpAgent:
    """Stub agent that satisfies the RemediationAgent protocol."""

    agent_id: str = "noop-agent"
    handles: list[str] = ["POL-TEST-001"]
    version: str = "1.0.0"
    model: str = ""

    def gather_context(self, violation: dict) -> dict:
        """Returns the violation as-is — no enrichment needed."""
        return {"violation": violation}

    def propose_fix(self, context: dict) -> dict:
        """Proposes a trivial owner-tag fix with low confidence."""
        violation = context.get("violation", {})
        resource = violation.get("resource_name", "unknown_resource")
        return {
            "proposed_sql": f"ALTER TABLE {resource} SET TAGS ('owner' = 'unassigned')",
            "confidence": 0.1,
            "context_json": json.dumps(context),
            "citations": "",
        }
```

- [ ] **Step 3: Run tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation.py -v
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/remediation/agents/noop.py tests/unit/test_remediation.py
git commit -m "feat: add NoOpAgent stub and remediation tests"
```

---

### Task 4: Build the dispatcher

**Files:**
- Create: `engine/src/watchdog/remediation/dispatcher.py`
- Test: `tests/unit/test_remediation.py` (append dispatcher tests)

- [ ] **Step 1: Create the dispatcher**

```python
# engine/src/watchdog/remediation/dispatcher.py
"""Remediation Dispatcher — routes open violations to registered agents.

Reads violations with status='open' and no active remediation, finds the
first agent whose handles[] matches the violation's policy_id, calls
gather_context + propose_fix, and writes the proposal to Delta.

Idempotent: skips violations that already have a proposal from the same
agent version.
"""

import json
import uuid
from datetime import datetime, timezone

from watchdog.remediation.tables import (
    ensure_remediation_proposals_table,
)


def dispatch_remediations(violations: list[dict], agents: list,
                          existing_proposal_keys: set[tuple] | None = None
                          ) -> dict:
    """Route violations to agents and collect proposals.

    Pure function for testability. Does not read/write Spark tables directly —
    the caller handles that.

    Args:
        violations: List of violation dicts (rows from violations table)
            with keys: violation_id, policy_id, resource_name, etc.
        agents: List of objects satisfying the RemediationAgent protocol.
        existing_proposal_keys: Set of (violation_id, agent_id, agent_version)
            tuples for proposals that already exist. Used for idempotency.

    Returns:
        Dict with keys:
            proposals: list of proposal dicts ready for Delta insertion
            dispatched: count of new proposals created
            skipped: count of violations skipped (already proposed or no agent)
            errors: count of agent failures
    """
    if existing_proposal_keys is None:
        existing_proposal_keys = set()

    # Build policy_id → agent lookup (first match wins)
    policy_agent_map: dict[str, object] = {}
    for agent in agents:
        for policy_id in agent.handles:
            if policy_id not in policy_agent_map:
                policy_agent_map[policy_id] = agent

    proposals = []
    dispatched = 0
    skipped = 0
    errors = 0

    for violation in violations:
        policy_id = violation.get("policy_id", "")
        violation_id = violation.get("violation_id", "")

        # Find matching agent
        agent = policy_agent_map.get(policy_id)
        if agent is None:
            skipped += 1
            continue

        # Idempotency check
        key = (violation_id, agent.agent_id, agent.version)
        if key in existing_proposal_keys:
            skipped += 1
            continue

        # Dispatch
        try:
            context = agent.gather_context(violation)
            fix = agent.propose_fix(context)

            proposal = {
                "proposal_id": str(uuid.uuid4()),
                "violation_id": violation_id,
                "agent_id": agent.agent_id,
                "agent_version": agent.version,
                "status": "pending_review",
                "proposed_sql": fix.get("proposed_sql", ""),
                "confidence": fix.get("confidence", 0.0),
                "context_json": fix.get("context_json", ""),
                "llm_prompt_hash": "",
                "citations": fix.get("citations", ""),
                "created_at": datetime.now(timezone.utc),
            }
            proposals.append(proposal)
            dispatched += 1

        except Exception as e:
            errors += 1

    return {
        "proposals": proposals,
        "dispatched": dispatched,
        "skipped": skipped,
        "errors": errors,
    }
```

- [ ] **Step 2: Add dispatcher tests to test_remediation.py**

Append to `tests/unit/test_remediation.py`:

```python
from watchdog.remediation.dispatcher import dispatch_remediations


# ── Dispatcher ───────────────────────────────────────────────────────────────

class TestDispatcher:
    def _make_violation(self, violation_id="v-001", policy_id="POL-TEST-001"):
        return {
            "violation_id": violation_id,
            "policy_id": policy_id,
            "resource_id": "gold.finance.gl_balances",
            "resource_name": "gold.finance.gl_balances",
            "severity": "high",
            "owner": "stuart.gano@company.com",
            "status": "open",
            "remediation_status": None,
        }

    def test_dispatches_to_matching_agent(self):
        agent = NoOpAgent()
        violations = [self._make_violation()]
        result = dispatch_remediations(violations, [agent])
        assert result["dispatched"] == 1
        assert result["skipped"] == 0
        assert len(result["proposals"]) == 1

    def test_proposal_has_correct_fields(self):
        agent = NoOpAgent()
        violations = [self._make_violation()]
        result = dispatch_remediations(violations, [agent])
        proposal = result["proposals"][0]
        assert proposal["violation_id"] == "v-001"
        assert proposal["agent_id"] == "noop-agent"
        assert proposal["agent_version"] == "1.0.0"
        assert proposal["status"] == "pending_review"
        assert "proposed_sql" in proposal
        assert "confidence" in proposal
        assert "created_at" in proposal

    def test_skips_when_no_matching_agent(self):
        agent = NoOpAgent()  # handles POL-TEST-001
        violations = [self._make_violation(policy_id="POL-UNKNOWN-999")]
        result = dispatch_remediations(violations, [agent])
        assert result["dispatched"] == 0
        assert result["skipped"] == 1

    def test_skips_already_proposed_violations(self):
        agent = NoOpAgent()
        violations = [self._make_violation()]
        existing = {("v-001", "noop-agent", "1.0.0")}
        result = dispatch_remediations(violations, [agent], existing)
        assert result["dispatched"] == 0
        assert result["skipped"] == 1

    def test_dispatches_multiple_violations(self):
        agent = NoOpAgent()
        violations = [
            self._make_violation(violation_id="v-001"),
            self._make_violation(violation_id="v-002"),
            self._make_violation(violation_id="v-003", policy_id="POL-OTHER"),
        ]
        result = dispatch_remediations(violations, [agent])
        assert result["dispatched"] == 2  # v-001 and v-002
        assert result["skipped"] == 1    # v-003 (no matching agent)

    def test_handles_agent_error_gracefully(self):
        class FailingAgent:
            agent_id = "failing-agent"
            handles = ["POL-TEST-001"]
            version = "1.0.0"
            model = ""
            def gather_context(self, v):
                raise RuntimeError("LLM unavailable")
            def propose_fix(self, c):
                return {}

        violations = [self._make_violation()]
        result = dispatch_remediations(violations, [FailingAgent()])
        assert result["dispatched"] == 0
        assert result["errors"] == 1
```

- [ ] **Step 3: Run tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation.py -v
```

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/remediation/dispatcher.py tests/unit/test_remediation.py
git commit -m "feat: add remediation dispatcher with idempotent violation routing"
```

---

### Task 5: Run full test suite and validate

**Files:**
- None (validation only)

- [ ] **Step 1: Run remediation tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_remediation.py -v
```

- [ ] **Step 2: Run full unit test suite**

```bash
PYTHONPATH=engine/src pytest tests/unit/ --ignore=tests/unit/test_multi_metastore.py -q 2>&1 | tail -5
```

- [ ] **Step 3: Final commit if needed**

```bash
git add -A
git commit -m "fix: address any test failures from remediation foundation"
```
