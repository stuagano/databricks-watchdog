# Drift Detection v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend drift detection with three new `drift_check` types — `row_filters`, `column_masks`, and `group_membership` — plus OPA bundle loading support.

**Architecture:** Each new check type follows the grants v1 pattern exactly: a new crawler method emits a new resource type into `resource_inventory`; `drift.py` builds a principal-keyed lookup; the policy engine injects the lookup into resource metadata before evaluation; `_eval_drift_check` in the rule engine compares actual vs expected. Bundle loading is a transparent extension to `load_expected_state()` — if the source path ends in `.tar.gz`, extract `data.json`; otherwise read JSON directly.

**Tech Stack:** Python stdlib (`tarfile`, `json`), pyyaml, pytest. No new dependencies.

---

## File Map

| Action | File | What changes |
|---|---|---|
| Modify | `engine/src/watchdog/rule_engine.py` | Refactor `_eval_drift_check` into sub-methods; add row_filters, column_masks, group_membership branches |
| Modify | `engine/src/watchdog/drift.py` | Add bundle loader, `build_expected_row_filters_lookup`, `build_expected_column_masks_lookup`, `build_expected_group_membership_lookup` |
| Modify | `engine/ontologies/resource_classes.yml` | Add `RowFilterAsset`, `ColumnMaskAsset`, `GroupMemberAsset` to base_classes |
| Modify | `engine/src/watchdog/crawler.py` | Add `_crawl_row_filters()`, `_crawl_column_masks()`, extend `_crawl_groups()` to emit group_member resources, register all three in `crawl_all()` |
| Modify | `engine/src/watchdog/policy_engine.py` | Extend expected state loading and injection for the three new types; add `data_path` support |
| Modify | `engine/policies/drift_detection.yml` | Add POL-DRIFT-002, POL-DRIFT-003, POL-DRIFT-004 |
| Modify | `tests/unit/test_drift.py` | Tests for all new rule engine branches, bundle loader, lookup builders |

---

### Task 1: Extend `_eval_drift_check` in the rule engine

**Files:**
- Modify: `engine/src/watchdog/rule_engine.py:479-537`
- Test: `tests/unit/test_drift.py`

- [ ] **Step 1: Append new test classes to `tests/unit/test_drift.py`**

Open `tests/unit/test_drift.py` and append the following after the last existing class:

```python
# ── row_filters drift_check ───────────────────────────────────────────────────

ROW_FILTER_RULE = {"type": "drift_check", "check": "row_filters"}


class TestDriftCheckRowFilters:
    def test_pass_no_expected_state(self, bare):
        """No expected_row_filters in metadata — vacuously true."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.my_filter",
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert result.passed

    def test_pass_function_matches_expected(self, bare):
        """Actual filter function matches expected — pass."""
        import json
        expected = json.dumps({"table": "gold.finance.gl_balances", "function": "gold.sec.my_filter"})
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.my_filter",
            "expected_row_filters": expected,
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert result.passed

    def test_fail_function_mismatch(self, bare):
        """Actual filter function differs from expected — fail."""
        import json
        expected = json.dumps({"table": "gold.finance.gl_balances", "function": "gold.sec.expected_filter"})
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.unauthorized_filter",
            "expected_row_filters": expected,
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert not result.passed
        assert "unauthorized_filter" in result.detail
        assert "gold.finance.gl_balances" in result.detail

    def test_fail_malformed_json(self, bare):
        """Malformed expected_row_filters JSON — fail gracefully."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "filter_function": "gold.sec.my_filter",
            "expected_row_filters": "not-json{{{",
        }
        result = bare.evaluate(ROW_FILTER_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()


# ── column_masks drift_check ──────────────────────────────────────────────────

COLUMN_MASK_RULE = {"type": "drift_check", "check": "column_masks"}


class TestDriftCheckColumnMasks:
    def test_pass_no_expected_state(self, bare):
        """No expected_column_masks in metadata — vacuously true."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.my_mask",
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert result.passed

    def test_pass_function_matches_expected(self, bare):
        """Actual mask function matches expected — pass."""
        import json
        expected = json.dumps({
            "table": "gold.finance.gl_balances",
            "column": "cost_center",
            "function": "gold.sec.my_mask",
        })
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.my_mask",
            "expected_column_masks": expected,
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert result.passed

    def test_fail_function_mismatch(self, bare):
        """Actual mask function differs from expected — fail."""
        import json
        expected = json.dumps({
            "table": "gold.finance.gl_balances",
            "column": "cost_center",
            "function": "gold.sec.expected_mask",
        })
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.unauthorized_mask",
            "expected_column_masks": expected,
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert not result.passed
        assert "unauthorized_mask" in result.detail
        assert "cost_center" in result.detail

    def test_fail_malformed_json(self, bare):
        """Malformed expected_column_masks JSON — fail gracefully."""
        metadata = {
            "table_full_name": "gold.finance.gl_balances",
            "column_name": "cost_center",
            "mask_function": "gold.sec.my_mask",
            "expected_column_masks": "not-json{{{",
        }
        result = bare.evaluate(COLUMN_MASK_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()


# ── group_membership drift_check ──────────────────────────────────────────────

GROUP_MEMBER_RULE = {"type": "drift_check", "check": "group_membership"}


class TestDriftCheckGroupMembership:
    def test_pass_no_expected_state(self, bare):
        """No expected_group_members in metadata — vacuously true."""
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert result.passed

    def test_pass_member_in_expected(self, bare):
        """Actual member is in the expected members list — pass."""
        import json
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
            "expected_group_members": json.dumps(["user@company.com", "other@company.com"]),
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert result.passed

    def test_fail_member_not_in_expected(self, bare):
        """Actual member not in expected list — fail with detail."""
        import json
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "unauthorized@company.com",
            "member_type": "user",
            "expected_group_members": json.dumps(["alice@company.com", "bob@company.com"]),
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert not result.passed
        assert "unauthorized@company.com" in result.detail
        assert "finance-analysts" in result.detail

    def test_fail_empty_expected_list(self, bare):
        """Empty expected_group_members list — any actual member is unauthorized."""
        import json
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
            "expected_group_members": json.dumps([]),
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert not result.passed

    def test_fail_malformed_json(self, bare):
        """Malformed expected_group_members JSON — fail gracefully."""
        metadata = {
            "group_name": "finance-analysts",
            "member_value": "user@company.com",
            "member_type": "user",
            "expected_group_members": "not-json{{{",
        }
        result = bare.evaluate(GROUP_MEMBER_RULE, {}, metadata)
        assert not result.passed
        assert "parse" in result.detail.lower() or "json" in result.detail.lower()
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py::TestDriftCheckRowFilters tests/unit/test_drift.py::TestDriftCheckColumnMasks tests/unit/test_drift.py::TestDriftCheckGroupMembership -v
```

Expected: FAIL — `"Unsupported drift check type: row_filters"` for all.

- [ ] **Step 3: Refactor `_eval_drift_check` and add new branches**

Replace the entire `_eval_drift_check` method in `engine/src/watchdog/rule_engine.py` (lines 479–537) with:

```python
    def _eval_drift_check(self, rule: dict, tags: dict[str, str],
                          metadata: dict[str, str]) -> RuleResult:
        """Compare actual resource state against declared expected state.

        The expected state is injected into metadata by the policy engine
        before evaluation. If no expected state is present, the check passes
        vacuously (no declared expectation = no drift).

        Supported check types: grants, row_filters, column_masks, group_membership.
        """
        check_type = rule.get("check", "")
        if check_type == "grants":
            return self._eval_drift_grants(metadata)
        elif check_type == "row_filters":
            return self._eval_drift_row_filters(metadata)
        elif check_type == "column_masks":
            return self._eval_drift_column_masks(metadata)
        elif check_type == "group_membership":
            return self._eval_drift_group_membership(metadata)
        else:
            return RuleResult(
                passed=False,
                detail=(
                    f"Unsupported drift check type: {check_type}. "
                    "Supported: grants, row_filters, column_masks, group_membership"
                ),
                rule_type="drift_check",
            )

    def _eval_drift_grants(self, metadata: dict[str, str]) -> RuleResult:
        """Check if actual grant is in declared expected state."""
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

        matching = [e for e in expected_entries if e.get("principal", "") == actual_grantee]
        if not matching:
            return RuleResult(passed=True, rule_type="drift_check")

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

    def _eval_drift_row_filters(self, metadata: dict[str, str]) -> RuleResult:
        """Check if actual row filter function matches declared expected state."""
        expected_json = metadata.get("expected_row_filters", "")
        if not expected_json:
            return RuleResult(passed=True, rule_type="drift_check")

        try:
            expected = json.loads(expected_json)
        except (json.JSONDecodeError, TypeError) as e:
            return RuleResult(
                passed=False,
                detail=f"Failed to parse expected_row_filters JSON: {e}",
                rule_type="drift_check",
            )

        table = metadata.get("table_full_name", "")
        actual_fn = metadata.get("filter_function", "")
        expected_fn = expected.get("function", "")

        if actual_fn == expected_fn:
            return RuleResult(passed=True, rule_type="drift_check")

        return RuleResult(
            passed=False,
            detail=(
                f"Drift detected: row filter '{actual_fn}' on {table} "
                f"does not match expected '{expected_fn}'"
            ),
            rule_type="drift_check",
        )

    def _eval_drift_column_masks(self, metadata: dict[str, str]) -> RuleResult:
        """Check if actual column mask function matches declared expected state."""
        expected_json = metadata.get("expected_column_masks", "")
        if not expected_json:
            return RuleResult(passed=True, rule_type="drift_check")

        try:
            expected = json.loads(expected_json)
        except (json.JSONDecodeError, TypeError) as e:
            return RuleResult(
                passed=False,
                detail=f"Failed to parse expected_column_masks JSON: {e}",
                rule_type="drift_check",
            )

        table = metadata.get("table_full_name", "")
        column = metadata.get("column_name", "")
        actual_fn = metadata.get("mask_function", "")
        expected_fn = expected.get("function", "")

        if actual_fn == expected_fn:
            return RuleResult(passed=True, rule_type="drift_check")

        return RuleResult(
            passed=False,
            detail=(
                f"Drift detected: column mask '{actual_fn}' on {table}.{column} "
                f"does not match expected '{expected_fn}'"
            ),
            rule_type="drift_check",
        )

    def _eval_drift_group_membership(self, metadata: dict[str, str]) -> RuleResult:
        """Check if actual group member is in declared expected members list."""
        expected_json = metadata.get("expected_group_members", "")
        if not expected_json:
            return RuleResult(passed=True, rule_type="drift_check")

        try:
            expected_members = json.loads(expected_json)
        except (json.JSONDecodeError, TypeError) as e:
            return RuleResult(
                passed=False,
                detail=f"Failed to parse expected_group_members JSON: {e}",
                rule_type="drift_check",
            )

        group = metadata.get("group_name", "")
        member = metadata.get("member_value", "")

        if member in expected_members:
            return RuleResult(passed=True, rule_type="drift_check")

        return RuleResult(
            passed=False,
            detail=(
                f"Drift detected: member '{member}' in group '{group}' "
                f"is not in expected state"
            ),
            rule_type="drift_check",
        )
```

- [ ] **Step 4: Run new tests to verify they pass**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py::TestDriftCheckRowFilters tests/unit/test_drift.py::TestDriftCheckColumnMasks tests/unit/test_drift.py::TestDriftCheckGroupMembership -v
```

Expected: All PASS.

- [ ] **Step 5: Run existing drift tests to verify no regression**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py -v
```

Expected: All 16 existing tests still PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/src/watchdog/rule_engine.py tests/unit/test_drift.py
git commit -m "feat: add row_filters, column_masks, group_membership drift_check types"
```

---

### Task 2: Extend `drift.py` — bundle loader and three new lookup builders

**Files:**
- Modify: `engine/src/watchdog/drift.py`
- Test: `tests/unit/test_drift.py`

- [ ] **Step 1: Append loader and lookup builder tests to `tests/unit/test_drift.py`**

Append after the last class:

```python
# ── OPA bundle loader ─────────────────────────────────────────────────────────

import tarfile as _tarfile


class TestLoadExpectedStateBundle:
    def test_load_bundle_reads_data_json(self, tmp_path):
        """Extracts data.json from a .tar.gz bundle and returns its contents."""
        state = {
            "grants": [{"principal": "analysts", "privileges": ["SELECT"]}],
            "row_filters": [{"table": "gold.fin.t", "function": "gold.sec.f"}],
        }
        data_json = tmp_path / "data.json"
        data_json.write_text(json.dumps(state))
        bundle = tmp_path / "bundle.tar.gz"
        with _tarfile.open(bundle, "w:gz") as tar:
            tar.add(data_json, arcname="data.json")

        result = load_expected_state(str(bundle))
        assert result["grants"][0]["principal"] == "analysts"
        assert result["row_filters"][0]["table"] == "gold.fin.t"

    def test_load_bundle_with_data_path(self, tmp_path):
        """Navigates nested data using data_path."""
        state = {"permissions": {"grants": [{"principal": "analysts", "privileges": ["SELECT"]}]}}
        data_json = tmp_path / "data.json"
        data_json.write_text(json.dumps(state))
        bundle = tmp_path / "bundle.tar.gz"
        with _tarfile.open(bundle, "w:gz") as tar:
            tar.add(data_json, arcname="data.json")

        result = load_expected_state(str(bundle), data_path="permissions")
        assert "grants" in result
        assert result["grants"][0]["principal"] == "analysts"

    def test_load_bundle_missing_data_json(self, tmp_path):
        """Bundle with no data.json returns empty dict."""
        bundle = tmp_path / "bundle.tar.gz"
        with _tarfile.open(bundle, "w:gz") as tar:
            pass  # empty archive

        result = load_expected_state(str(bundle))
        assert result == {}

    def test_load_bundle_invalid_data_path(self, tmp_path):
        """data_path that doesn't exist in the data returns empty dict."""
        state = {"grants": []}
        data_json = tmp_path / "data.json"
        data_json.write_text(json.dumps(state))
        bundle = tmp_path / "bundle.tar.gz"
        with _tarfile.open(bundle, "w:gz") as tar:
            tar.add(data_json, arcname="data.json")

        result = load_expected_state(str(bundle), data_path="nonexistent")
        assert result == {}


# ── New lookup builders ────────────────────────────────────────────────────────

from watchdog.drift import (
    build_expected_row_filters_lookup,
    build_expected_column_masks_lookup,
    build_expected_group_membership_lookup,
)


class TestBuildExpectedRowFiltersLookup:
    def test_keyed_by_table(self):
        rf = [{"table": "gold.fin.gl", "function": "gold.sec.f1"}]
        lookup = build_expected_row_filters_lookup(rf)
        assert "gold.fin.gl" in lookup
        assert lookup["gold.fin.gl"]["function"] == "gold.sec.f1"

    def test_empty_input(self):
        assert build_expected_row_filters_lookup([]) == {}

    def test_missing_table_key_skipped(self):
        rf = [{"function": "gold.sec.f1"}]  # no 'table' key
        lookup = build_expected_row_filters_lookup(rf)
        assert lookup == {}

    def test_last_entry_wins_on_duplicate_table(self):
        rf = [
            {"table": "gold.fin.gl", "function": "gold.sec.f1"},
            {"table": "gold.fin.gl", "function": "gold.sec.f2"},
        ]
        lookup = build_expected_row_filters_lookup(rf)
        assert lookup["gold.fin.gl"]["function"] == "gold.sec.f2"


class TestBuildExpectedColumnMasksLookup:
    def test_keyed_by_table_dot_column(self):
        cm = [{"table": "gold.fin.gl", "column": "cost_center", "function": "gold.sec.m1"}]
        lookup = build_expected_column_masks_lookup(cm)
        assert "gold.fin.gl.cost_center" in lookup
        assert lookup["gold.fin.gl.cost_center"]["function"] == "gold.sec.m1"

    def test_empty_input(self):
        assert build_expected_column_masks_lookup([]) == {}

    def test_missing_column_skipped(self):
        cm = [{"table": "gold.fin.gl", "function": "gold.sec.m1"}]
        lookup = build_expected_column_masks_lookup(cm)
        assert lookup == {}

    def test_multiple_columns_same_table(self):
        cm = [
            {"table": "gold.fin.gl", "column": "col_a", "function": "gold.sec.ma"},
            {"table": "gold.fin.gl", "column": "col_b", "function": "gold.sec.mb"},
        ]
        lookup = build_expected_column_masks_lookup(cm)
        assert "gold.fin.gl.col_a" in lookup
        assert "gold.fin.gl.col_b" in lookup


class TestBuildExpectedGroupMembershipLookup:
    def test_keyed_by_group(self):
        gm = [{"group": "analysts", "members": ["alice@co.com", "bob@co.com"]}]
        lookup = build_expected_group_membership_lookup(gm)
        assert "analysts" in lookup
        assert "alice@co.com" in lookup["analysts"]

    def test_empty_input(self):
        assert build_expected_group_membership_lookup([]) == {}

    def test_missing_group_skipped(self):
        gm = [{"members": ["alice@co.com"]}]
        lookup = build_expected_group_membership_lookup(gm)
        assert lookup == {}

    def test_returns_set_for_fast_membership_check(self):
        gm = [{"group": "analysts", "members": ["alice@co.com"]}]
        lookup = build_expected_group_membership_lookup(gm)
        assert isinstance(lookup["analysts"], set)
```

- [ ] **Step 2: Run new tests to verify they fail**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py::TestLoadExpectedStateBundle tests/unit/test_drift.py::TestBuildExpectedRowFiltersLookup tests/unit/test_drift.py::TestBuildExpectedColumnMasksLookup tests/unit/test_drift.py::TestBuildExpectedGroupMembershipLookup -v
```

Expected: FAIL — `load_expected_state` doesn't accept `data_path`, new functions don't exist yet.

- [ ] **Step 3: Replace `engine/src/watchdog/drift.py` with the extended version**

```python
# engine/src/watchdog/drift.py
"""Drift Detection — expected state loading and lookup building.

Loads expected state JSON files from local paths or OPA bundle tarballs
(UC volume mounts in Databricks, local files in tests). Builds lookup
structures for the policy engine to inject into resource metadata before
rule evaluation.

The rule engine's drift_check evaluator consumes the injected metadata
without knowing where it came from — keeping the rule engine pure.

OPA bundle support: if the source path ends in .tar.gz, extracts data.json
from the archive root. No OPA runtime dependency — stdlib tarfile only.
"""

import json
import logging
import tarfile
from collections import defaultdict

logger = logging.getLogger(__name__)


def load_expected_state(file_path: str, data_path: str | None = None) -> dict:
    """Load expected state from a JSON file or OPA bundle tarball.

    Args:
        file_path: Path to the expected state file. Either a plain .json file
            or an OPA bundle .tar.gz archive containing data.json at its root.
            In Databricks, this is a UC volume mount path like
            /Volumes/{catalog}/{schema}/{volume}/expected_state.json
        data_path: Optional dot-separated path to navigate into nested data.
            E.g., "permissions" navigates to data["permissions"] before
            returning. Useful for OPA bundles that namespace their data.

    Returns:
        Parsed JSON dict (or the nested sub-dict if data_path is given),
        or empty dict if file not found, not readable, or path not found.
    """
    try:
        if file_path.endswith(".tar.gz"):
            data = _load_from_bundle(file_path)
        else:
            with open(file_path) as f:
                data = json.load(f)
    except FileNotFoundError:
        logger.warning("Expected state file not found: %s", file_path)
        return {}
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load expected state from %s: %s", file_path, e)
        return {}

    if data_path:
        for key in data_path.split("."):
            if not isinstance(data, dict):
                return {}
            data = data.get(key, {})
        if not isinstance(data, dict):
            return {}

    return data


def _load_from_bundle(file_path: str) -> dict:
    """Extract and parse data.json from an OPA bundle tarball."""
    try:
        with tarfile.open(file_path, "r:gz") as tar:
            try:
                member = tar.getmember("data.json")
            except KeyError:
                logger.warning("data.json not found in bundle: %s", file_path)
                return {}
            f = tar.extractfile(member)
            if f is None:
                logger.warning("data.json is not a regular file in bundle: %s", file_path)
                return {}
            return json.load(f)
    except tarfile.TarError as e:
        logger.warning("Failed to open bundle %s: %s", file_path, e)
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


def build_expected_row_filters_lookup(row_filters: list[dict]) -> dict[str, dict]:
    """Build a lookup from table full name to expected row filter entry.

    Args:
        row_filters: List of row filter entries, each with table and function.

    Returns:
        Dict mapping table_full_name to the expected row filter entry.
        Last entry wins if multiple entries share the same table.
    """
    lookup: dict[str, dict] = {}
    for entry in row_filters:
        table = entry.get("table", "")
        if table:
            lookup[table] = entry
    return lookup


def build_expected_column_masks_lookup(column_masks: list[dict]) -> dict[str, dict]:
    """Build a lookup from "{table}.{column}" to expected column mask entry.

    Args:
        column_masks: List of column mask entries, each with table, column,
            and function.

    Returns:
        Dict mapping "{table_full_name}.{column_name}" to the expected entry.
    """
    lookup: dict[str, dict] = {}
    for entry in column_masks:
        table = entry.get("table", "")
        column = entry.get("column", "")
        if table and column:
            lookup[f"{table}.{column}"] = entry
    return lookup


def build_expected_group_membership_lookup(
    group_membership: list[dict],
) -> dict[str, set[str]]:
    """Build a lookup from group name to expected member set.

    Args:
        group_membership: List of group entries, each with group name and
            a list of expected member values (emails or SP app IDs).

    Returns:
        Dict mapping group_name to a set of expected member values.
    """
    lookup: dict[str, set[str]] = {}
    for entry in group_membership:
        group = entry.get("group", "")
        members = entry.get("members", [])
        if group:
            lookup[group] = set(members)
    return lookup
```

- [ ] **Step 4: Run new tests to verify they pass**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py::TestLoadExpectedStateBundle tests/unit/test_drift.py::TestBuildExpectedRowFiltersLookup tests/unit/test_drift.py::TestBuildExpectedColumnMasksLookup tests/unit/test_drift.py::TestBuildExpectedGroupMembershipLookup -v
```

Expected: All PASS.

- [ ] **Step 5: Run full drift test suite to verify no regression**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_drift.py -v
```

Expected: All tests PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/src/watchdog/drift.py tests/unit/test_drift.py
git commit -m "feat: add OPA bundle loader and row_filter/column_mask/group_membership lookup builders"
```

---

### Task 3: Add new ontology classes

**Files:**
- Modify: `engine/ontologies/resource_classes.yml`
- Test: `tests/unit/test_ontology.py` (append one test)

- [ ] **Step 1: Verify the test pattern in `test_ontology.py`**

```bash
grep -n "GrantAsset\|base_class\|matches_resource" tests/unit/test_ontology.py | head -10
```

Note the test pattern for checking that a resource type is classified into a base class.

- [ ] **Step 2: Append new class tests to `tests/unit/test_ontology.py`**

Open `tests/unit/test_ontology.py` and append:

```python
class TestNewDriftAssetClasses:
    """RowFilterAsset, ColumnMaskAsset, GroupMemberAsset classify correctly."""

    def test_row_filter_asset_classifies(self, ontology_dir):
        from watchdog.ontology import OntologyEngine
        engine = OntologyEngine(ontology_dir=ontology_dir)
        classes = engine.classify(resource_type="row_filter", tags={})
        assert "RowFilterAsset" in classes

    def test_column_mask_asset_classifies(self, ontology_dir):
        from watchdog.ontology import OntologyEngine
        engine = OntologyEngine(ontology_dir=ontology_dir)
        classes = engine.classify(resource_type="column_mask", tags={})
        assert "ColumnMaskAsset" in classes

    def test_group_member_asset_classifies(self, ontology_dir):
        from watchdog.ontology import OntologyEngine
        engine = OntologyEngine(ontology_dir=ontology_dir)
        classes = engine.classify(resource_type="group_member", tags={})
        assert "GroupMemberAsset" in classes
```

- [ ] **Step 3: Run tests to verify they fail**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_ontology.py::TestNewDriftAssetClasses -v
```

Expected: FAIL — classes not defined yet.

- [ ] **Step 4: Add three base classes to `engine/ontologies/resource_classes.yml`**

In the `base_classes:` section, after the `GrantAsset` entry, add:

```yaml
  RowFilterAsset:
    description: "A Unity Catalog row filter applied to a table"
    matches_resource_types: [row_filter]

  ColumnMaskAsset:
    description: "A Unity Catalog column mask applied to a table column"
    matches_resource_types: [column_mask]

  GroupMemberAsset:
    description: "A member of a workspace or account-level group"
    matches_resource_types: [group_member]
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_ontology.py::TestNewDriftAssetClasses -v
```

Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/ontologies/resource_classes.yml tests/unit/test_ontology.py
git commit -m "feat: add RowFilterAsset, ColumnMaskAsset, GroupMemberAsset ontology classes"
```

---

### Task 4: Add row filter and column mask crawlers

**Files:**
- Modify: `engine/src/watchdog/crawler.py`
- Test: `tests/unit/test_crawler.py`

- [ ] **Step 1: Append crawler tests to `tests/unit/test_crawler.py`**

Open `tests/unit/test_crawler.py`. The file already has `_make_crawler()` and the pyspark mock setup at the top. Append after the last test class:

```python
class TestCrawlRowFilters:
    def test_emits_row_filter_resources(self):
        """_crawl_row_filters emits one resource per row filter."""
        from types import SimpleNamespace
        crawler = _make_crawler()

        cat = SimpleNamespace(name="gold")
        crawler.w.catalogs.list.return_value = [cat]

        row = SimpleNamespace(
            table_catalog="gold",
            table_schema="finance",
            table_name="gl_balances",
            filter_catalog="gold",
            filter_schema="security",
            filter_name="permissions_filter_gl",
        )
        crawler.spark.sql.return_value.collect.return_value = [row]

        rows = crawler._crawl_row_filters()

        assert len(rows) == 1
        # tuple: (scan_id, metastore_id, resource_type, resource_id, resource_name, owner, domain, tags, metadata, discovered_at)
        assert rows[0][2] == "row_filter"
        assert rows[0][3] == "row_filter:gold.finance.gl_balances"
        assert rows[0][8]["table_full_name"] == "gold.finance.gl_balances"
        assert rows[0][8]["filter_function"] == "gold.security.permissions_filter_gl"

    def test_skips_catalog_on_exception(self):
        """Exceptions from information_schema are swallowed per catalog."""
        from types import SimpleNamespace
        crawler = _make_crawler()

        cat = SimpleNamespace(name="gold")
        crawler.w.catalogs.list.return_value = [cat]
        crawler.spark.sql.side_effect = Exception("Access denied")

        rows = crawler._crawl_row_filters()
        assert rows == []


class TestCrawlColumnMasks:
    def test_emits_column_mask_resources(self):
        """_crawl_column_masks emits one resource per column mask."""
        from types import SimpleNamespace
        crawler = _make_crawler()

        cat = SimpleNamespace(name="gold")
        crawler.w.catalogs.list.return_value = [cat]

        row = SimpleNamespace(
            table_catalog="gold",
            table_schema="finance",
            table_name="gl_balances",
            column_name="cost_center",
            mask_catalog="gold",
            mask_schema="security",
            mask_name="permissions_mask_cost_center",
        )
        crawler.spark.sql.return_value.collect.return_value = [row]

        rows = crawler._crawl_column_masks()

        assert len(rows) == 1
        # tuple: (scan_id, metastore_id, resource_type, resource_id, resource_name, owner, domain, tags, metadata, discovered_at)
        assert rows[0][2] == "column_mask"
        assert rows[0][3] == "column_mask:gold.finance.gl_balances.cost_center"
        assert rows[0][8]["table_full_name"] == "gold.finance.gl_balances"
        assert rows[0][8]["column_name"] == "cost_center"
        assert rows[0][8]["mask_function"] == "gold.security.permissions_mask_cost_center"

    def test_skips_catalog_on_exception(self):
        """Exceptions from information_schema are swallowed per catalog."""
        from types import SimpleNamespace
        crawler = _make_crawler()

        cat = SimpleNamespace(name="gold")
        crawler.w.catalogs.list.return_value = [cat]
        crawler.spark.sql.side_effect = Exception("Access denied")

        rows = crawler._crawl_column_masks()
        assert rows == []
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_crawler.py::TestCrawlRowFilters tests/unit/test_crawler.py::TestCrawlColumnMasks -v
```

Expected: FAIL — methods don't exist yet.

- [ ] **Step 3: Add `_crawl_row_filters` and `_crawl_column_masks` to `crawler.py`**

After the closing of `_crawl_grants` (find the end of the method, around line 750), add:

```python
    def _crawl_row_filters(self) -> list:
        """Crawl UC row filters from information_schema.

        Emits one resource per row filter applied to a table. Used by
        POL-DRIFT-002 to detect unauthorized row filter changes.
        """
        rows = []
        for cat in self.w.catalogs.list():
            try:
                results = self.spark.sql(f"""
                    SELECT table_catalog, table_schema, table_name,
                           filter_catalog, filter_schema, filter_name
                    FROM {cat.name}.information_schema.row_filters
                """).collect()
                for row in results:
                    fqn = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
                    filter_fn = f"{row.filter_catalog}.{row.filter_schema}.{row.filter_name}"
                    rows.append(self._make_row(
                        resource_type="row_filter",
                        resource_id=f"row_filter:{fqn}",
                        resource_name=row.table_name,
                        metadata={
                            "table_full_name": fqn,
                            "filter_function": filter_fn,
                        },
                    ))
            except Exception:
                continue
        return rows

    def _crawl_column_masks(self) -> list:
        """Crawl UC column masks from information_schema.

        Emits one resource per column mask applied to a table column. Used by
        POL-DRIFT-003 to detect unauthorized column mask changes.
        """
        rows = []
        for cat in self.w.catalogs.list():
            try:
                results = self.spark.sql(f"""
                    SELECT table_catalog, table_schema, table_name, column_name,
                           mask_catalog, mask_schema, mask_name
                    FROM {cat.name}.information_schema.column_masks
                """).collect()
                for row in results:
                    fqn = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
                    mask_fn = f"{row.mask_catalog}.{row.mask_schema}.{row.mask_name}"
                    rows.append(self._make_row(
                        resource_type="column_mask",
                        resource_id=f"column_mask:{fqn}.{row.column_name}",
                        resource_name=f"{row.table_name}.{row.column_name}",
                        metadata={
                            "table_full_name": fqn,
                            "column_name": row.column_name,
                            "mask_function": mask_fn,
                        },
                    ))
            except Exception:
                continue
        return rows
```

- [ ] **Step 4: Register the new crawlers in `crawl_all()`**

In `crawl_all()`, find the block that registers `_crawl_grants` (lines 185–191):

```python
        # UC grant resources via information_schema + SDK
        for crawler_fn in [
            self._crawl_grants,
        ]:
```

Replace it with:

```python
        # UC grant, row filter, and column mask resources via information_schema
        for crawler_fn in [
            self._crawl_grants,
            self._crawl_row_filters,
            self._crawl_column_masks,
        ]:
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_crawler.py::TestCrawlRowFilters tests/unit/test_crawler.py::TestCrawlColumnMasks -v
```

Expected: All PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/src/watchdog/crawler.py tests/unit/test_crawler.py
git commit -m "feat: add row filter and column mask crawlers"
```

---

### Task 5: Extend group crawler to emit group_member resources

**Files:**
- Modify: `engine/src/watchdog/crawler.py`
- Test: `tests/unit/test_crawler.py`

- [ ] **Step 1: Append group member crawler test to `tests/unit/test_crawler.py`**

```python
class TestCrawlGroupMembers:
    def test_emits_group_member_resources(self):
        """_crawl_groups emits one group_member resource per member."""
        from types import SimpleNamespace
        crawler = _make_crawler()

        member = SimpleNamespace(
            value="user@company.com",
            display="User Name",
            type=SimpleNamespace(__str__=lambda self: "User"),
        )
        group = SimpleNamespace(
            id="grp-001",
            display_name="finance-analysts",
            meta=None,
            members=[member],
            entitlements=None,
        )
        crawler.w.groups.list.return_value = [group]

        rows = crawler._crawl_groups()

        # Should emit: 1 group resource + 1 group_member resource
        # tuple: (scan_id, metastore_id, resource_type, resource_id, resource_name, owner, domain, tags, metadata, discovered_at)
        assert len(rows) == 2
        member_rows = [r for r in rows if r[2] == "group_member"]
        assert len(member_rows) == 1
        assert member_rows[0][3] == "group_member:finance-analysts:user@company.com"
        assert member_rows[0][8]["group_name"] == "finance-analysts"
        assert member_rows[0][8]["member_value"] == "user@company.com"

    def test_group_with_no_members_emits_only_group(self):
        """Groups with no members emit only the group resource."""
        from types import SimpleNamespace
        crawler = _make_crawler()

        group = SimpleNamespace(
            id="grp-002",
            display_name="empty-group",
            meta=None,
            members=None,
            entitlements=None,
        )
        crawler.w.groups.list.return_value = [group]

        rows = crawler._crawl_groups()
        assert len(rows) == 1
        assert rows[0][2] == "group"

    def test_member_without_value_skipped(self):
        """Members with no value or display are skipped."""
        from types import SimpleNamespace
        crawler = _make_crawler()

        member = SimpleNamespace(value=None, display=None, type=None)
        group = SimpleNamespace(
            id="grp-003",
            display_name="some-group",
            meta=None,
            members=[member],
            entitlements=None,
        )
        crawler.w.groups.list.return_value = [group]

        rows = crawler._crawl_groups()
        group_member_rows = [r for r in rows if r[2] == "group_member"]
        assert len(group_member_rows) == 0
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_crawler.py::TestCrawlGroupMembers -v
```

Expected: FAIL — `_crawl_groups` doesn't emit `group_member` resources yet.

- [ ] **Step 3: Extend `_crawl_groups` in `crawler.py`**

Find `_crawl_groups` (currently at line 263). Replace the entire method with:

```python
    def _crawl_groups(self) -> list:
        """Crawl workspace groups and their members.

        Emits one 'group' resource per group (for identity governance policies)
        and one 'group_member' resource per member (for drift detection via
        POL-DRIFT-004). Members are already fetched in the same SDK call.
        """
        rows = []
        for group in self.w.groups.list(attributes="id,displayName,meta,members,entitlements"):
            group_type = "account"
            if group.meta and group.meta.resource_type == "WorkspaceGroup":
                group_type = "workspace_local"

            member_count = len(group.members) if group.members else 0

            entitlements = []
            if group.entitlements:
                entitlements = [e.value for e in group.entitlements if e.value]

            rows.append(self._make_row(
                resource_type="group",
                resource_id=group.id,
                resource_name=group.display_name,
                metadata={
                    "group_type": group_type,
                    "member_count": str(member_count),
                    "entitlements": ",".join(entitlements),
                },
            ))

            # Emit one group_member resource per member for drift detection
            group_name = group.display_name or ""
            for member in (group.members or []):
                member_value = (member.value or member.display or "").strip()
                if not member_value:
                    continue
                member_type = "user"
                if member.type and "ServicePrincipal" in str(member.type):
                    member_type = "service_principal"
                elif member.type and "Group" in str(member.type):
                    member_type = "group"
                rows.append(self._make_row(
                    resource_type="group_member",
                    resource_id=f"group_member:{group_name}:{member_value}",
                    resource_name=group_name,
                    metadata={
                        "group_name": group_name,
                        "member_value": member_value,
                        "member_type": member_type,
                    },
                ))

        return rows
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_crawler.py::TestCrawlGroupMembers -v
```

Expected: All PASS.

- [ ] **Step 5: Run full crawler test suite**

```bash
PYTHONPATH=engine/src pytest tests/unit/test_crawler.py -v
```

Expected: All existing tests still PASS.

- [ ] **Step 6: Commit**

```bash
git add engine/src/watchdog/crawler.py tests/unit/test_crawler.py
git commit -m "feat: extend group crawler to emit group_member resources for drift detection"
```

---

### Task 6: Extend policy engine injection

**Files:**
- Modify: `engine/src/watchdog/policy_engine.py`

- [ ] **Step 1: Update the import line at the top of `policy_engine.py`**

Find line 29:
```python
from watchdog.drift import load_expected_state, build_expected_grants_lookup
```

Replace with:
```python
from watchdog.drift import (
    load_expected_state,
    build_expected_grants_lookup,
    build_expected_row_filters_lookup,
    build_expected_column_masks_lookup,
    build_expected_group_membership_lookup,
)
```

- [ ] **Step 2: Replace the expected state loading block**

Find the block starting at line 213:
```python
        # Load expected state for drift_check policies
        expected_grants_lookup: dict[str, list[dict]] = {}
        for policy in active_policies:
            if policy.rule.get("type") == "drift_check" and "source" in policy.rule:
                source_path = policy.rule["source"]
                volume_path = f"/Volumes/{self.catalog}/{self.schema}/{source_path}"
                state = load_expected_state(volume_path)
                grants = state.get("grants", [])
                if grants:
                    lookup = build_expected_grants_lookup(grants)
                    expected_grants_lookup.update(lookup)
```

Replace with:
```python
        # Load expected state for drift_check policies
        expected_grants_lookup: dict[str, list[dict]] = {}
        expected_row_filters_lookup: dict[str, dict] = {}
        expected_column_masks_lookup: dict[str, dict] = {}
        expected_group_membership_lookup: dict[str, set] = {}

        loaded_sources: set[str] = set()
        for policy in active_policies:
            if policy.rule.get("type") != "drift_check" or "source" not in policy.rule:
                continue
            source_path = policy.rule["source"]
            if source_path in loaded_sources:
                continue
            loaded_sources.add(source_path)
            volume_path = f"/Volumes/{self.catalog}/{self.schema}/{source_path}"
            data_path = policy.rule.get("data_path")
            state = load_expected_state(volume_path, data_path=data_path)

            grants = state.get("grants", [])
            if grants:
                expected_grants_lookup.update(build_expected_grants_lookup(grants))

            row_filters = state.get("row_filters", [])
            if row_filters:
                expected_row_filters_lookup.update(build_expected_row_filters_lookup(row_filters))

            column_masks = state.get("column_masks", [])
            if column_masks:
                expected_column_masks_lookup.update(build_expected_column_masks_lookup(column_masks))

            group_membership = state.get("group_membership", [])
            if group_membership:
                expected_group_membership_lookup.update(
                    build_expected_group_membership_lookup(group_membership)
                )
```

- [ ] **Step 3: Replace the metadata injection block**

Find the injection block (lines 238–245):
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

Replace with:
```python
                # Inject expected state for drift_check evaluation
                if resource.resource_type == "grant" and expected_grants_lookup:
                    grantee = metadata.get("grantee", "")
                    if grantee in expected_grants_lookup:
                        metadata = {
                            **metadata,
                            "expected_grants": json.dumps(expected_grants_lookup[grantee]),
                        }
                if resource.resource_type == "row_filter" and expected_row_filters_lookup:
                    table = metadata.get("table_full_name", "")
                    if table in expected_row_filters_lookup:
                        metadata = {
                            **metadata,
                            "expected_row_filters": json.dumps(
                                expected_row_filters_lookup[table]
                            ),
                        }
                if resource.resource_type == "column_mask" and expected_column_masks_lookup:
                    key = (
                        f"{metadata.get('table_full_name', '')}"
                        f".{metadata.get('column_name', '')}"
                    )
                    if key in expected_column_masks_lookup:
                        metadata = {
                            **metadata,
                            "expected_column_masks": json.dumps(
                                expected_column_masks_lookup[key]
                            ),
                        }
                if resource.resource_type == "group_member" and expected_group_membership_lookup:
                    group = metadata.get("group_name", "")
                    if group in expected_group_membership_lookup:
                        metadata = {
                            **metadata,
                            "expected_group_members": json.dumps(
                                list(expected_group_membership_lookup[group])
                            ),
                        }
```

- [ ] **Step 4: Run the full unit test suite to verify no regressions**

```bash
PYTHONPATH=engine/src pytest tests/unit/ --ignore=tests/unit/test_multi_metastore.py -v 2>&1 | tail -15
```

Expected: All tests PASS.

- [ ] **Step 5: Commit**

```bash
git add engine/src/watchdog/policy_engine.py
git commit -m "feat: extend policy engine to inject expected state for row_filter, column_mask, group_member resources"
```

---

### Task 7: Add sample policies and verify full suite

**Files:**
- Modify: `engine/policies/drift_detection.yml`

- [ ] **Step 1: Append three new policies to `engine/policies/drift_detection.yml`**

```yaml

  - id: POL-DRIFT-002
    name: "Row filter drift detection"
    applies_to: RowFilterAsset
    domain: AccessControl
    severity: critical
    description: "Detected a row filter that differs from the declared expected state. This indicates an unauthorized manual change to the row filter UDF applied to this table."
    remediation: "Review the drift and update the expected state file to include this filter or remove the unauthorized row filter from the table."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: row_filters

  - id: POL-DRIFT-003
    name: "Column mask drift detection"
    applies_to: ColumnMaskAsset
    domain: AccessControl
    severity: critical
    description: "Detected a column mask that differs from the declared expected state. This indicates an unauthorized manual change to the column mask UDF applied to this column."
    remediation: "Review the drift and update the expected state file to include this mask or remove the unauthorized column mask from the column."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: column_masks

  - id: POL-DRIFT-004
    name: "Group membership drift detection"
    applies_to: GroupMemberAsset
    domain: AccessControl
    severity: high
    description: "Detected a group member not present in the declared expected state. This indicates an unauthorized manual addition to the group that bypasses the identity management pipeline."
    remediation: "Review the drift and update the expected state file to include this member or remove them from the group."
    active: true
    rule:
      type: drift_check
      source: expected_permissions/expected_state.json
      check: group_membership
```

- [ ] **Step 2: Verify all drift policies load correctly**

```bash
PYTHONPATH=engine/src python -c "
from watchdog.policy_loader import load_yaml_policies
policies = load_yaml_policies()
drift = [p for p in policies if p.policy_id.startswith('POL-DRIFT')]
print(f'Drift policies loaded: {len(drift)}')
for p in drift:
    print(f'  {p.policy_id}: {p.name} (check: {p.rule.get(\"check\", \"?\")})')
"
```

Expected output:
```
Drift policies loaded: 4
  POL-DRIFT-001: Grant drift detection (check: grants)
  POL-DRIFT-002: Row filter drift detection (check: row_filters)
  POL-DRIFT-003: Column mask drift detection (check: column_masks)
  POL-DRIFT-004: Group membership drift detection (check: group_membership)
```

- [ ] **Step 3: Run the complete unit test suite**

```bash
PYTHONPATH=engine/src pytest tests/unit/ --ignore=tests/unit/test_multi_metastore.py -v 2>&1 | tail -10
```

Expected: All tests PASS, no regressions.

- [ ] **Step 4: Commit**

```bash
git add engine/policies/drift_detection.yml
git commit -m "feat: add POL-DRIFT-002/003/004 sample policies for row filter, column mask, group membership drift"
```
