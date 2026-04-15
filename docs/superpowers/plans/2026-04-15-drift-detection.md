# Drift Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `drift_check` rule type to the Watchdog rule engine that compares actual grant state against a declared expected state, producing violations through the standard pipeline.

**Architecture:** Expected state is loaded from a UC volume JSON file by the policy engine, injected into resource metadata, and compared by a pure-Python rule evaluator. The rule engine stays pure (no Spark, no file access). Grants-only in v1.

**Tech Stack:** Python, PyYAML, pytest, PySpark (mocked in unit tests)

---

### Task 1: Add `drift_check` rule type to the rule engine

**Files:**
- Modify: `engine/src/watchdog/rule_engine.py`
- Test: `tests/unit/test_drift.py`

- [ ] **Step 1: Create test file with drift_check rule type tests**

```python
# tests/unit/test_drift.py
"""Unit tests for drift_check rule type and expected state loading.

Run with: pytest tests/unit/test_drift.py -v
"""
import json
from pathlib import Path

import pytest
from watchdog.rule_engine import RuleEngine


@pytest.fixture(scope="module")
def engine(ontology_dir):
    """RuleEngine loaded with the live rule_primitives.yml."""
    return RuleEngine(primitives_dir=ontology_dir)


@pytest.fixture(scope="module")
def bare(tmp_path_factory):
    """RuleEngine with no primitives."""
    d = tmp_path_factory.mktemp("empty_ontology")
    return RuleEngine(primitives_dir=str(d))


DRIFT_RULE = {"type": "drift_check", "check": "grants"}


class TestDriftCheckPass:
    """drift_check should pass when actual matches expected or no expected state."""

    def test_pass_no_expected_state_in_metadata(self, bare):
        """No expected_grants key = vacuously true (no expectation declared)."""
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed

    def test_pass_empty_expected_grants(self, bare):
        """Empty expected_grants list = no expectations for this resource."""
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": "[]",
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed

    def test_pass_actual_matches_expected(self, bare):
        """Actual privilege is in the expected privileges list."""
        expected = json.dumps([{
            "catalog": "gold",
            "schema": "finance",
            "table": None,
            "principal": "finance-analysts",
            "privileges": ["SELECT", "USE_CATALOG", "USE_SCHEMA"],
        }])
        metadata = {
            "securable_type": "schema",
            "securable_full_name": "gold.finance",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed

    def test_pass_no_matching_principal_in_expected(self, bare):
        """Expected state exists but for a different principal — vacuously true."""
        expected = json.dumps([{
            "catalog": "gold",
            "schema": "finance",
            "table": None,
            "principal": "data-engineers",
            "privileges": ["SELECT"],
        }])
        metadata = {
            "securable_type": "schema",
            "securable_full_name": "gold.finance",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert result.passed


class TestDriftCheckFail:
    """drift_check should fail when actual does not match expected."""

    def test_fail_extra_privilege(self, bare):
        """Actual has a privilege not in expected list = unauthorized grant."""
        expected = json.dumps([{
            "catalog": "gold",
            "schema": "finance",
            "table": None,
            "principal": "finance-analysts",
            "privileges": ["SELECT", "USE_CATALOG"],
        }])
        metadata = {
            "securable_type": "schema",
            "securable_full_name": "gold.finance",
            "grantee": "finance-analysts",
            "privilege": "MODIFY",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert not result.passed
        assert "MODIFY" in result.detail
        assert "not in expected state" in result.detail

    def test_fail_detail_includes_resource_info(self, bare):
        """Failure detail should include the securable and principal."""
        expected = json.dumps([{
            "catalog": "gold",
            "schema": "finance",
            "table": "gl_balances",
            "principal": "finance-analysts",
            "privileges": ["SELECT"],
        }])
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "MODIFY",
            "expected_grants": expected,
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert not result.passed
        assert "finance-analysts" in result.detail
        assert "gold.finance.gl_balances" in result.detail

    def test_fail_malformed_json(self, bare):
        """Malformed expected_grants JSON should fail gracefully."""
        metadata = {
            "securable_type": "table",
            "securable_full_name": "gold.finance.gl_balances",
            "grantee": "finance-analysts",
            "privilege": "SELECT",
            "expected_grants": "not valid json{{{",
        }
        result = bare.evaluate(DRIFT_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()


class TestDriftCheckRuleType:
    """Verify drift_check is properly registered."""

    def test_rule_type_in_dispatch(self, bare):
        """drift_check should not return 'Unknown rule type'."""
        result = bare.evaluate(DRIFT_RULE, {}, {})
        assert result.rule_type == "drift_check"

    def test_unknown_check_type_fails(self, bare):
        """Unsupported check type should fail with clear message."""
        rule = {"type": "drift_check", "check": "row_filters"}
        metadata = {"expected_grants": "[]"}
        result = bare.evaluate(rule, {}, metadata)
        assert not result.passed
        assert "row_filters" in result.detail
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py -v
```

Expected: FAIL — `drift_check` not in dispatch table yet.

- [ ] **Step 3: Implement `_eval_drift_check` in rule_engine.py**

In `engine/src/watchdog/rule_engine.py`, add `"drift_check": self._eval_drift_check,` to the dispatch dict (after `"if_then"` at line 90).

Then add the evaluator method before the `# Utilities` section (before `_extract_version`):

```python
    def _eval_drift_check(self, rule: dict, tags: dict[str, str],
                          metadata: dict[str, str]) -> RuleResult:
        """Compare actual resource state against declared expected state.

        The expected state is injected into metadata by the policy engine
        before evaluation. If no expected state is present, the check passes
        vacuously (no declared expectation = no drift).

        Currently supports: grants (rule.check == "grants").
        """
        check_type = rule.get("check", "")
        if check_type != "grants":
            return RuleResult(
                passed=False,
                detail=f"Unsupported drift check type: {check_type}. Supported: grants",
                rule_type="drift_check",
            )

        expected_json = metadata.get("expected_grants", "")
        if not expected_json:
            return RuleResult(passed=True, rule_type="drift_check")

        try:
            expected_entries = json.loads(expected_json)
        except (json.JSONDecodeError, TypeError) as e:
            return RuleResult(
                passed=False,
                detail=f"Failed to parse expected_grants JSON: {e}",
                rule_type="drift_check",
            )

        if not expected_entries:
            return RuleResult(passed=True, rule_type="drift_check")

        actual_grantee = metadata.get("grantee", "")
        actual_privilege = metadata.get("privilege", "")
        securable = metadata.get("securable_full_name", "")

        # Find expected entries for this principal
        matching = [
            e for e in expected_entries
            if e.get("principal", "") == actual_grantee
        ]

        if not matching:
            # No expected state declared for this principal — pass
            return RuleResult(passed=True, rule_type="drift_check")

        # Check if actual privilege is in any matching entry's expected set
        for entry in matching:
            expected_privs = [p.upper() for p in entry.get("privileges", [])]
            if actual_privilege.upper() in expected_privs:
                return RuleResult(passed=True, rule_type="drift_check")

        return RuleResult(
            passed=False,
            detail=(
                f"Drift detected: grant '{actual_privilege}' on {securable} "
                f"for {actual_grantee} is not in expected state"
            ),
            rule_type="drift_check",
        )
```

Also add `import json` at the top of the file (after `import re`).

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py -v
```

Expected: All tests PASS.

- [ ] **Step 5: Run existing rule engine tests to verify no regressions**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_rule_engine.py -v
```

Expected: All existing tests still PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/src/watchdog/rule_engine.py tests/unit/test_drift.py
git commit -m "feat: add drift_check rule type to rule engine"
```

---

### Task 2: Create expected state loader

**Files:**
- Create: `engine/src/watchdog/drift.py`
- Test: `tests/unit/test_drift.py` (append to existing)

- [ ] **Step 1: Add loader tests to test_drift.py**

Append to `tests/unit/test_drift.py`:

```python
from watchdog.drift import load_expected_state, build_expected_grants_lookup


class TestLoadExpectedState:
    """Tests for the expected state file loader."""

    def test_load_valid_json(self, tmp_path):
        """Parses valid expected state JSON."""
        state = {
            "generated_at": "2026-04-14T10:00:00Z",
            "environment": "production",
            "grants": [
                {
                    "catalog": "gold",
                    "schema": "finance",
                    "table": None,
                    "principal": "finance-analysts",
                    "privileges": ["SELECT", "USE_CATALOG"],
                }
            ],
        }
        f = tmp_path / "expected_state.json"
        f.write_text(json.dumps(state))
        result = load_expected_state(str(f))
        assert result == state
        assert len(result["grants"]) == 1

    def test_load_missing_file(self, tmp_path):
        """Returns empty dict when file doesn't exist."""
        result = load_expected_state(str(tmp_path / "nonexistent.json"))
        assert result == {}

    def test_load_malformed_json(self, tmp_path):
        """Returns empty dict when JSON is invalid."""
        f = tmp_path / "bad.json"
        f.write_text("not json {{{")
        result = load_expected_state(str(f))
        assert result == {}

    def test_load_no_grants_section(self, tmp_path):
        """Returns dict with no grants when grants section is missing."""
        state = {"generated_at": "2026-04-14T10:00:00Z", "environment": "prod"}
        f = tmp_path / "expected_state.json"
        f.write_text(json.dumps(state))
        result = load_expected_state(str(f))
        assert "grants" not in result


class TestBuildExpectedGrantsLookup:
    """Tests for building the grants lookup from expected state."""

    def test_build_lookup_from_grants(self):
        """Creates a lookup keyed by principal."""
        grants = [
            {
                "catalog": "gold",
                "schema": "finance",
                "table": None,
                "principal": "finance-analysts",
                "privileges": ["SELECT", "USE_CATALOG"],
            },
            {
                "catalog": "gold",
                "schema": "finance",
                "table": "gl_balances",
                "principal": "data-engineers",
                "privileges": ["SELECT", "MODIFY"],
            },
        ]
        lookup = build_expected_grants_lookup(grants)
        assert "finance-analysts" in lookup
        assert "data-engineers" in lookup
        assert len(lookup["finance-analysts"]) == 1
        assert lookup["finance-analysts"][0]["privileges"] == ["SELECT", "USE_CATALOG"]

    def test_build_lookup_empty(self):
        """Empty grants list produces empty lookup."""
        lookup = build_expected_grants_lookup([])
        assert lookup == {}

    def test_build_lookup_multiple_entries_same_principal(self):
        """Multiple entries for the same principal are grouped."""
        grants = [
            {"catalog": "gold", "schema": "finance", "table": None,
             "principal": "analysts", "privileges": ["SELECT"]},
            {"catalog": "silver", "schema": "raw", "table": None,
             "principal": "analysts", "privileges": ["USE_CATALOG"]},
        ]
        lookup = build_expected_grants_lookup(grants)
        assert len(lookup["analysts"]) == 2
```

- [ ] **Step 2: Create `engine/src/watchdog/drift.py`**

```python
# engine/src/watchdog/drift.py
"""Drift Detection — expected state loading and lookup building.

Loads expected state JSON files from local paths (UC volume mounts in
Databricks, local files in tests). Builds lookup structures for the
policy engine to inject into resource metadata before rule evaluation.

The rule engine's drift_check evaluator consumes the injected metadata
without knowing where it came from — keeping the rule engine pure.
"""

import json
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def load_expected_state(file_path: str) -> dict:
    """Load expected state from a JSON file.

    Args:
        file_path: Path to the expected state JSON file. In Databricks,
            this is a UC volume mount path like
            /Volumes/{catalog}/{schema}/{volume}/expected_state.json

    Returns:
        Parsed JSON dict, or empty dict if file not found or invalid.
    """
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("Expected state file not found: %s", file_path)
        return {}
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load expected state from %s: %s", file_path, e)
        return {}


def build_expected_grants_lookup(grants: list[dict]) -> dict[str, list[dict]]:
    """Build a lookup from principal name to expected grant entries.

    Args:
        grants: List of grant entries from expected_state.json, each with
            catalog, schema, table, principal, and privileges.

    Returns:
        Dict mapping principal name to list of their expected grant entries.
    """
    lookup: dict[str, list[dict]] = defaultdict(list)
    for entry in grants:
        principal = entry.get("principal", "")
        if principal:
            lookup[principal].append(entry)
    return dict(lookup)
```

- [ ] **Step 3: Run tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py -v
```

Expected: All tests PASS.

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/drift.py tests/unit/test_drift.py
git commit -m "feat: add expected state loader and grants lookup builder"
```

---

### Task 3: Wire expected state injection into the policy engine

**Files:**
- Modify: `engine/src/watchdog/policy_engine.py`

- [ ] **Step 1: Add import**

In `engine/src/watchdog/policy_engine.py`, add `import json` at the top (after the existing stdlib imports), then add after the existing watchdog imports (after `from watchdog.policies_table import write_policies`):

```python
from watchdog.drift import load_expected_state, build_expected_grants_lookup
```

- [ ] **Step 2: Add expected state loading and injection**

In the `evaluate_all` method, add after the `write_policies` call (after line 209) and before `scan_results = []`:

```python
        # Load expected state for drift_check policies
        expected_grants_lookup: dict[str, list[dict]] = {}
        for policy in active_policies:
            if policy.rule.get("type") == "drift_check" and "source" in policy.rule:
                source_path = policy.rule["source"]
                # In Databricks, volumes are mounted at /Volumes/{catalog}/{schema}/{volume}
                volume_path = f"/Volumes/{self.catalog}/{self.schema}/{source_path}"
                state = load_expected_state(volume_path)
                grants = state.get("grants", [])
                if grants:
                    lookup = build_expected_grants_lookup(grants)
                    expected_grants_lookup.update(lookup)
```

Then in the evaluation loop, after the line `metadata = {**metadata, "owner": resource.owner}` (which is inside the `for resource in inventory:` loop), add:

```python
                # Inject expected state for drift_check evaluation
                if resource.resource_type == "grant" and expected_grants_lookup:
                    grantee = metadata.get("grantee", "")
                    if grantee in expected_grants_lookup:
                        metadata = {
                            **metadata,
                            "expected_grants": json.dumps(expected_grants_lookup[grantee]),
                        }
```

- [ ] **Step 3: Run all tests to verify no regressions**

```bash
PYTHONPATH=engine/src pytest tests/unit/ --ignore=tests/unit/test_multi_metastore.py -v 2>&1 | tail -10
```

Expected: All tests PASS (the policy engine tests use mocked Spark so the volume path won't be accessed).

- [ ] **Step 4: Commit**

```bash
git add engine/src/watchdog/policy_engine.py
git commit -m "feat: inject expected state into grant metadata for drift detection"
```

---

### Task 4: Add sample drift policy

**Files:**
- Create: `engine/policies/drift_detection.yml`

- [ ] **Step 1: Create the policy file**

```yaml
# Drift Detection Policies
# Domain: AccessControl
# Owner: security_admin
#
# Compares actual grants against declared expected state from UC volume.
# External systems produce expected_state.json; Watchdog detects drift.
#
# Policy namespace: POL-DRIFT-* (avoids collision with built-in POL-A* policies)

policies:

  - id: POL-DRIFT-001
    name: "Grant drift detection"
    applies_to: GrantAsset
    domain: AccessControl
    severity: critical
    description: "Detected grants that differ from the declared expected state. This indicates unauthorized manual grant changes that bypass the permissions-as-code pipeline."
    remediation: "Review the drift and either update the expected state file to include this grant or revoke the unauthorized grant."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: grants
```

- [ ] **Step 2: Verify policy loads correctly**

```bash
PYTHONPATH=engine/src python -c "
from watchdog.policy_loader import load_yaml_policies
policies = load_yaml_policies()
drift = [p for p in policies if p.policy_id.startswith('POL-DRIFT')]
print(f'Drift policies loaded: {len(drift)}')
for p in drift:
    print(f'  {p.policy_id}: {p.name} (rule type: {p.rule.get(\"type\", \"?\")})')
"
```

Expected output:
```
Drift policies loaded: 1
  POL-DRIFT-001: Grant drift detection (rule type: drift_check)
```

- [ ] **Step 3: Commit**

```bash
git add engine/policies/drift_detection.yml
git commit -m "feat: add sample grant drift detection policy (POL-DRIFT-001)"
```

---

### Task 5: Run full test suite and validate

**Files:**
- None (validation only)

- [ ] **Step 1: Run drift tests**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py -v
```

Expected: All tests PASS.

- [ ] **Step 2: Run full unit test suite**

```bash
PYTHONPATH=engine/src pytest tests/unit/ --ignore=tests/unit/test_multi_metastore.py -v 2>&1 | tail -5
```

Expected: All tests PASS, no regressions.

- [ ] **Step 3: Verify rule type count in README if referenced**

Check if README.md mentions rule type count and update if needed:

```bash
grep -n "rule type" README.md | head -5
```

If a count is mentioned (e.g., "15 rule types"), update to include drift_check.

- [ ] **Step 4: Final commit if any fixes needed**

```bash
git add -A
git commit -m "fix: address any test failures from drift detection integration"
```
