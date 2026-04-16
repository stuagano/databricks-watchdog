"""Rule Engine — evaluates composite governance rules against resource data.

Rules are defined declaratively in YAML (either inline in policies or as
reusable primitives in ontologies/rule_primitives.yml). The engine recursively
evaluates rule trees, supporting:

  - tag_exists, tag_equals, tag_in, tag_not_in, tag_matches
  - metadata_equals, metadata_matches, metadata_not_empty, metadata_gte
  - has_owner (composite shorthand)
  - all_of, any_of, none_of (boolean composition)
  - if_then (conditional rules)
  - ref (reference to a named primitive)

Every rule evaluation returns a RuleResult with pass/fail and a human-readable
detail string for violation reporting.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class RuleResult:
    """Result of evaluating a single rule against a resource."""
    passed: bool
    detail: str = ""
    rule_type: str = ""


class RuleEngine:
    """Evaluates declarative rules against resource tags and metadata."""

    def __init__(self, primitives_dir: str | None = None):
        self.primitives: dict[str, dict] = {}
        if primitives_dir is None:
            primitives_dir = str(Path(__file__).parent.parent.parent / "ontologies")
        self._load_primitives(Path(primitives_dir))

    def _load_primitives(self, ontology_dir: Path) -> None:
        """Load reusable rule primitives from YAML."""
        prim_path = ontology_dir / "rule_primitives.yml"
        if not prim_path.exists():
            return
        with open(prim_path) as f:
            data = yaml.safe_load(f)
        self.primitives = data.get("primitives") or {}

    def evaluate(self, rule: dict, tags: dict[str, str],
                 metadata: dict[str, str]) -> RuleResult:
        """Evaluate a rule definition against resource tags and metadata.

        The rule can be an inline definition or a reference to a primitive.
        """
        # Handle reference to named primitive
        if "ref" in rule:
            prim_name = rule["ref"]
            if prim_name not in self.primitives:
                return RuleResult(
                    passed=False,
                    detail=f"Unknown rule primitive: {prim_name}",
                    rule_type="ref",
                )
            return self.evaluate(self.primitives[prim_name], tags, metadata)

        rule_type = rule.get("type", "")

        # If no explicit type, try to infer from keys
        if not rule_type:
            return self._evaluate_inline(rule, tags, metadata)

        dispatch = {
            "tag_exists": self._eval_tag_exists,
            "tag_equals": self._eval_tag_equals,
            "tag_in": self._eval_tag_in,
            "tag_not_in": self._eval_tag_not_in,
            "tag_matches": self._eval_tag_matches,
            "metadata_equals": self._eval_metadata_equals,
            "metadata_matches": self._eval_metadata_matches,
            "metadata_not_empty": self._eval_metadata_not_empty,
            "metadata_gte": self._eval_metadata_gte,
            "metadata_lte": self._eval_metadata_lte,
            "has_owner": self._eval_has_owner,
            "all_of": self._eval_all_of,
            "any_of": self._eval_any_of,
            "none_of": self._eval_none_of,
            "if_then": self._eval_if_then,
            "drift_check": self._eval_drift_check,
        }

        handler = dispatch.get(rule_type)
        if handler:
            return handler(rule, tags, metadata)

        return RuleResult(
            passed=False,
            detail=f"Unknown rule type: {rule_type}",
            rule_type=rule_type,
        )

    # ------------------------------------------------------------------
    # Inline rule detection (for shorthand in policy YAML)
    # ------------------------------------------------------------------

    def _evaluate_inline(self, rule: dict, tags: dict[str, str],
                         metadata: dict[str, str]) -> RuleResult:
        """Handle rules without an explicit 'type' field by inspecting keys.

        Policy YAML authors can write shorthand like:
            rule:
              tag_exists: [owner, env]
        instead of the typed form:
            rule:
              type: tag_exists
              keys: [owner, env]

        This method normalizes the shorthand into the typed form so the
        same evaluators handle both paths. Only the most common shorthand
        operators are supported here; full typed form is preferred for
        new policies.
        """
        if "tag_exists" in rule:
            return self._eval_tag_exists(
                {"keys": rule["tag_exists"]}, tags, metadata
            )
        if "tag_equals" in rule:
            r = {"type": "tag_equals"}
            for k, v in rule["tag_equals"].items():
                r["key"] = k
                r["value"] = v
            return self._eval_tag_equals(r, tags, metadata)
        if "all_of" in rule:
            return self._eval_all_of({"rules": rule["all_of"]}, tags, metadata)
        if "any_of" in rule:
            return self._eval_any_of({"rules": rule["any_of"]}, tags, metadata)
        if "none_of" in rule:
            return self._eval_none_of({"rules": rule["none_of"]}, tags, metadata)
        if "if_then" in rule:
            inner = rule["if_then"]
            # Normalize: YAML uses "if"/"then", handler expects "condition"/"then"
            return self._eval_if_then({
                "condition": inner.get("if", inner.get("condition", {})),
                "then": inner.get("then", {}),
            }, tags, metadata)
        if "metadata_gte" in rule:
            inner = rule["metadata_gte"]
            return self._eval_metadata_gte({
                "field": inner.get("field", ""),
                "threshold": str(inner.get("value", inner.get("threshold", ""))),
            }, tags, metadata)
        if "metadata_lte" in rule:
            inner = rule["metadata_lte"]
            return self._eval_metadata_lte({
                "field": inner.get("field", ""),
                "threshold": str(inner.get("value", inner.get("threshold", ""))),
            }, tags, metadata)
        if "ref" in rule:
            return self.evaluate(rule, tags, metadata)

        return RuleResult(passed=False, detail="Unrecognized rule format")

    # ------------------------------------------------------------------
    # Primitive evaluators
    # ------------------------------------------------------------------

    def _eval_tag_exists(self, rule: dict, tags: dict[str, str],
                         metadata: dict[str, str]) -> RuleResult:
        """Fail if any required tag keys are absent. Reports all missing keys."""
        keys = rule.get("keys", [])
        missing = [k for k in keys if k not in tags]
        if missing:
            return RuleResult(
                passed=False,
                detail=f"Missing required tag(s): {', '.join(missing)}",
                rule_type="tag_exists",
            )
        return RuleResult(passed=True, rule_type="tag_exists")

    def _eval_tag_equals(self, rule: dict, tags: dict[str, str],
                         metadata: dict[str, str]) -> RuleResult:
        """Fail if the tag value does not exactly equal the expected value."""
        key = rule.get("key", "")
        expected = str(rule.get("value", ""))
        actual = tags.get(key, "")
        if actual != expected:
            return RuleResult(
                passed=False,
                detail=f"Tag '{key}' is '{actual}', expected '{expected}'",
                rule_type="tag_equals",
            )
        return RuleResult(passed=True, rule_type="tag_equals")

    def _eval_tag_in(self, rule: dict, tags: dict[str, str],
                     metadata: dict[str, str]) -> RuleResult:
        """Fail if the tag value is not in the allowed set."""
        key = rule.get("key", "")
        allowed = [str(v) for v in rule.get("allowed", [])]
        actual = tags.get(key, "")
        if actual not in allowed:
            return RuleResult(
                passed=False,
                detail=f"Tag '{key}' is '{actual}', must be one of: {', '.join(allowed)}",
                rule_type="tag_in",
            )
        return RuleResult(passed=True, rule_type="tag_in")

    def _eval_tag_not_in(self, rule: dict, tags: dict[str, str],
                         metadata: dict[str, str]) -> RuleResult:
        """Fail if the tag value is in the disallowed set."""
        key = rule.get("key", "")
        disallowed = [str(v) for v in rule.get("disallowed", [])]
        actual = tags.get(key, "")
        if actual in disallowed:
            return RuleResult(
                passed=False,
                detail=f"Tag '{key}' is '{actual}', must NOT be: {', '.join(disallowed)}",
                rule_type="tag_not_in",
            )
        return RuleResult(passed=True, rule_type="tag_not_in")

    def _eval_tag_matches(self, rule: dict, tags: dict[str, str],
                          metadata: dict[str, str]) -> RuleResult:
        """Fail if the tag value does not match the regex pattern."""
        key = rule.get("key", "")
        pattern = rule.get("pattern", "")
        actual = tags.get(key, "")
        if not re.search(pattern, actual):
            return RuleResult(
                passed=False,
                detail=f"Tag '{key}' value '{actual}' does not match pattern '{pattern}'",
                rule_type="tag_matches",
            )
        return RuleResult(passed=True, rule_type="tag_matches")

    def _eval_metadata_equals(self, rule: dict, tags: dict[str, str],
                              metadata: dict[str, str]) -> RuleResult:
        """Fail if a metadata field does not exactly equal the expected value."""
        f = rule.get("field", "")
        expected = str(rule.get("value", ""))
        actual = metadata.get(f, "")
        if actual != expected:
            return RuleResult(
                passed=False,
                detail=f"Metadata '{f}' is '{actual}', expected '{expected}'",
                rule_type="metadata_equals",
            )
        return RuleResult(passed=True, rule_type="metadata_equals")

    def _eval_metadata_matches(self, rule: dict, tags: dict[str, str],
                               metadata: dict[str, str]) -> RuleResult:
        """Fail if a metadata field value does not match the regex pattern."""
        f = rule.get("field", "")
        pattern = rule.get("pattern", "")
        actual = metadata.get(f, "")
        if not re.search(pattern, actual):
            return RuleResult(
                passed=False,
                detail=f"Metadata '{f}' value '{actual}' does not match '{pattern}'",
                rule_type="metadata_matches",
            )
        return RuleResult(passed=True, rule_type="metadata_matches")

    def _eval_metadata_not_empty(self, rule: dict, tags: dict[str, str],
                                 metadata: dict[str, str]) -> RuleResult:
        """Fail if a metadata field is absent or blank.

        Special case: 'owner' is checked in both metadata AND tags because
        the policy engine injects owner into metadata but some resources
        carry it as a UC tag instead. Either source satisfies the check.
        """
        f = rule.get("field", "")
        actual = metadata.get(f, "")
        # Also check top-level owner field (common pattern)
        if f == "owner":
            actual = metadata.get("owner", "") or tags.get("owner", "")
        if not actual or not actual.strip():
            return RuleResult(
                passed=False,
                detail=f"Metadata field '{f}' is empty or missing",
                rule_type="metadata_not_empty",
            )
        return RuleResult(passed=True, rule_type="metadata_not_empty")

    def _eval_metadata_gte(self, rule: dict, tags: dict[str, str],
                           metadata: dict[str, str]) -> RuleResult:
        """Fail if a metadata field value is less than the threshold.

        Uses version-aware comparison: numeric parts are extracted and compared
        as tuples so '15.4.x-scala2.12' >= '13.3' works correctly for
        Databricks runtime version policies. Falls back to lexicographic
        string comparison if the value cannot be parsed as a version.
        """
        f = rule.get("field", "")
        threshold = str(rule.get("threshold", ""))
        actual = metadata.get(f, "")
        if not actual:
            return RuleResult(
                passed=False,
                detail=f"Metadata field '{f}' is empty (threshold: >= {threshold})",
                rule_type="metadata_gte",
            )
        # Version-aware comparison: extract leading numeric parts
        try:
            actual_ver = self._extract_version(actual)
            threshold_ver = self._extract_version(threshold)
            if actual_ver < threshold_ver:
                return RuleResult(
                    passed=False,
                    detail=f"Metadata '{f}' is '{actual}' (< {threshold})",
                    rule_type="metadata_gte",
                )
        except (ValueError, TypeError):
            # Fall back to string comparison
            if actual < threshold:
                return RuleResult(
                    passed=False,
                    detail=f"Metadata '{f}' is '{actual}' (< {threshold})",
                    rule_type="metadata_gte",
                )
        return RuleResult(passed=True, rule_type="metadata_gte")

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
            if not actual_ver:
                # No numeric parts — value is not a parseable number/version
                return RuleResult(
                    passed=False,
                    detail=f"Metadata '{f}' is '{actual}' (not numeric; threshold: <= {threshold})",
                    rule_type="metadata_lte",
                )
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

    def _eval_has_owner(self, rule: dict, tags: dict[str, str],
                        metadata: dict[str, str]) -> RuleResult:
        """Fail if the resource has no owner in either metadata or tags.

        Composite shorthand for the most common ownership check. Checks
        metadata.owner first (injected by PolicyEngine from the UC owner
        field), then falls back to the 'owner' tag. This covers resources
        that set ownership via tags rather than the UC metadata owner field.
        """
        owner = metadata.get("owner", "") or tags.get("owner", "")
        if owner and owner.strip():
            return RuleResult(passed=True, rule_type="has_owner")
        return RuleResult(
            passed=False,
            detail="Resource has no owner assigned and no 'owner' tag",
            rule_type="has_owner",
        )

    # ------------------------------------------------------------------
    # Composite evaluators
    # ------------------------------------------------------------------

    def _eval_all_of(self, rule: dict, tags: dict[str, str],
                     metadata: dict[str, str]) -> RuleResult:
        """Fail if any sub-rule fails. Collects ALL failure messages (no short-circuit).

        Unlike Python's `all()`, this evaluates every sub-rule even after the
        first failure so the violation detail includes every failed condition.
        This gives operators a complete picture of what to fix.
        """
        sub_rules = rule.get("rules", [])
        failures = []
        for sub in sub_rules:
            result = self.evaluate(sub, tags, metadata)
            if not result.passed:
                failures.append(result.detail)
        if failures:
            return RuleResult(
                passed=False,
                detail=" | ".join(failures),
                rule_type="all_of",
            )
        return RuleResult(passed=True, rule_type="all_of")

    def _eval_any_of(self, rule: dict, tags: dict[str, str],
                     metadata: dict[str, str]) -> RuleResult:
        """Pass if at least one sub-rule passes (short-circuits on first pass).

        On failure, all individual failure messages are joined so the violation
        detail explains why none of the alternatives matched.
        """
        sub_rules = rule.get("rules", [])
        details = []
        for sub in sub_rules:
            result = self.evaluate(sub, tags, metadata)
            if result.passed:
                return RuleResult(passed=True, rule_type="any_of")
            details.append(result.detail)
        return RuleResult(
            passed=False,
            detail=f"None of the alternatives passed: {' | '.join(details)}",
            rule_type="any_of",
        )

    def _eval_none_of(self, rule: dict, tags: dict[str, str],
                      metadata: dict[str, str]) -> RuleResult:
        """Fail if ANY sub-rule passes (inverted semantics — exclusion check).

        Used to block prohibited configurations, e.g. ensuring no resource
        has a tag value from a forbidden set. Passes only when nothing matches.
        """
        sub_rules = rule.get("rules", [])
        for sub in sub_rules:
            result = self.evaluate(sub, tags, metadata)
            if result.passed:
                return RuleResult(
                    passed=False,
                    detail=f"Exclusion rule matched: {sub}",
                    rule_type="none_of",
                )
        return RuleResult(passed=True, rule_type="none_of")

    def _eval_if_then(self, rule: dict, tags: dict[str, str],
                      metadata: dict[str, str]) -> RuleResult:
        """Conditional rule: enforce `then` only when `condition` passes.

        When the condition does NOT match, the rule is vacuously true — it
        simply does not apply to this resource. This is intentional: the rule
        only fires for resources that satisfy the precondition.

        Example: "if env=prod then require owner tag" — resources that are
        not tagged env=prod skip the owner check entirely rather than failing.
        This is the primary mechanism for environment- or class-scoped rules.
        """
        condition = rule.get("condition", {})
        then_rule = rule.get("then", {})

        cond_result = self.evaluate(condition, tags, metadata)
        if not cond_result.passed:
            # Condition doesn't match — rule is vacuously true (not applicable)
            return RuleResult(passed=True, rule_type="if_then")

        # Condition matched — evaluate the consequent
        then_result = self.evaluate(then_rule, tags, metadata)
        if not then_result.passed:
            return RuleResult(
                passed=False,
                detail=then_result.detail,
                rule_type="if_then",
            )
        return RuleResult(passed=True, rule_type="if_then")

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

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_version(version_str: str) -> tuple:
        """Extract numeric version tuple from a version string.

        Handles Databricks runtime versions like '15.4.x-scala2.12'
        by extracting the leading numeric parts: (15, 4).
        """
        nums = re.findall(r"(\d+)", version_str)
        return tuple(int(n) for n in nums[:3])
