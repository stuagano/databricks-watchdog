# Rule Types Reference

The rule engine supports 16 rule types organized into four categories: tag checks, metadata checks, composite operators, and drift checks. Every rule evaluation returns a `RuleResult` with pass/fail status and a human-readable detail string.

## Tag Rules

### tag_exists

Checks that one or more tag keys are present on the resource. Fails if any required key is missing.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `keys` | list[string] | Tag keys that must be present |

**Example:**

```yaml
rule:
  type: tag_exists
  keys: [data_steward, retention_days]
```

**Failure detail:** `Missing required tag(s): data_steward, retention_days`

---

### tag_equals

Checks that a specific tag has an exact value. Fails if the tag is missing or has a different value.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Tag key to check |
| `value` | string | Expected exact value |

**Example:**

```yaml
rule:
  type: tag_equals
  key: environment
  value: "prod"
```

**Failure detail:** `Tag 'environment' is 'dev', expected 'prod'`

---

### tag_in

Checks that a tag value is one of an allowed set. Fails if the value is not in the set.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Tag key to check |
| `allowed` | list[string] | Permitted values |

**Example:**

```yaml
rule:
  type: tag_in
  key: data_classification
  allowed: [public, internal, confidential, restricted, pii]
```

**Failure detail:** `Tag 'data_classification' is 'secret', must be one of: public, internal, confidential, restricted, pii`

---

### tag_not_in

Checks that a tag value is NOT in a disallowed set. Fails if the value matches any entry.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Tag key to check |
| `disallowed` | list[string] | Prohibited values |

**Example:**

```yaml
rule:
  type: tag_not_in
  key: environment
  disallowed: [deprecated, decommissioned]
```

**Failure detail:** `Tag 'environment' is 'deprecated', must NOT be: deprecated, decommissioned`

---

### tag_matches

Checks that a tag value matches a regular expression. Fails if the pattern does not match.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `key` | string | Tag key to check |
| `pattern` | string | Regular expression pattern |

**Example:**

```yaml
rule:
  type: tag_matches
  key: cost_center
  pattern: "^CC-\\d{4}$"
```

**Failure detail:** `Tag 'cost_center' value 'engineering' does not match pattern '^CC-\d{4}$'`

---

## Metadata Rules

### metadata_equals

Checks that a metadata field has an exact value. Metadata fields come from crawler-collected resource properties (not UC tags).

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Metadata field name |
| `value` | string | Expected exact value |

**Example:**

```yaml
rule:
  type: metadata_equals
  field: group_type
  value: "account"
```

**Failure detail:** `Metadata 'group_type' is 'workspace_local', expected 'account'`

---

### metadata_matches

Checks that a metadata field value matches a regular expression.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Metadata field name |
| `pattern` | string | Regular expression pattern |

**Example:**

```yaml
rule:
  type: metadata_matches
  field: grantee
  pattern: "^(group:|account group:)"
```

**Failure detail:** `Metadata 'grantee' value 'user@example.com' does not match '^(group:|account group:)'`

---

### metadata_not_empty

Checks that a metadata field exists and has a non-empty value. For the special case of the `owner` field, checks both metadata and tags.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Metadata field name |

**Example:**

```yaml
rule:
  type: metadata_not_empty
  field: comment
```

**Failure detail:** `Metadata field 'comment' is empty or missing`

---

### metadata_gte

Checks that a metadata field value is greater than or equal to a threshold. Uses version-aware comparison: numeric parts are extracted and compared as tuples so `15.4.x-scala2.12 >= 13.3` works correctly for Databricks runtime versions. Falls back to lexicographic string comparison if the value cannot be parsed as a version.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Metadata field name |
| `threshold` | string | Minimum value (numeric or version string) |

**Example:**

```yaml
rule:
  type: metadata_gte
  field: spark_version
  threshold: "15.4"
```

**Failure detail:** `Metadata 'spark_version' is '13.3.x-scala2.12' (< 15.4)`

---

### metadata_lte

Checks that a metadata field value is less than or equal to a threshold. Uses the same version-aware comparison as `metadata_gte` but reverses the direction: the field value must be <= the threshold. Falls back to lexicographic string comparison if the value cannot be parsed as a version.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `field` | string | Metadata field name |
| `threshold` | string | Maximum value (numeric or version string) |

**Example:**

```yaml
rule:
  type: metadata_lte
  field: spark_version
  threshold: "15.4"
```

**Failure detail:** `Metadata 'spark_version' is '16.0.x-scala2.12' (> 15.4)`

---

### has_owner

Composite shorthand that checks for an owner in both metadata and tags. Passes if either `metadata.owner` or `tags.owner` has a non-empty value.

**Parameters:** None.

**Example:**

```yaml
rule:
  type: has_owner
```

**Failure detail:** `Resource has no owner assigned and no 'owner' tag`

---

## Composite Rules

### all_of

Boolean AND. All child rules must pass. Evaluates every sub-rule even after the first failure so the violation detail includes all failed conditions.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `rules` | list[rule] | Sub-rules that must all pass |

**Example:**

```yaml
rule:
  type: all_of
  rules:
    - ref: has_data_steward
    - ref: has_retention_policy
    - ref: has_data_classification
```

**Failure detail:** `Missing required tag(s): data_steward | Missing required tag(s): retention_days`

---

### any_of

Boolean OR. At least one child rule must pass. Short-circuits on the first passing rule. On failure, all individual failure messages are joined.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `rules` | list[rule] | Sub-rules where at least one must pass |

**Example:**

```yaml
rule:
  type: any_of
  rules:
    - type: metadata_not_empty
      field: owner
    - type: tag_exists
      keys: [owner]
```

**Failure detail:** `None of the alternatives passed: Metadata field 'owner' is empty or missing | Missing required tag(s): owner`

---

### none_of

Boolean NOT. Fails if ANY sub-rule passes. Used for exclusion checks to block prohibited configurations.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `rules` | list[rule] | Sub-rules that must all fail (none should match) |

**Example:**

```yaml
rule:
  type: none_of
  rules:
    - type: metadata_equals
      field: privilege
      value: "ALL PRIVILEGES"
```

**Failure detail:** `Exclusion rule matched: {'type': 'metadata_equals', 'field': 'privilege', 'value': 'ALL PRIVILEGES'}`

---

### if_then

Conditional rule. Evaluates the `then` rule only when the `condition` passes. When the condition does not match, the rule is vacuously true (it does not apply to the resource).

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `condition` | rule | Precondition that triggers the check |
| `then` | rule | Rule to enforce when the condition matches |

**Example:**

```yaml
rule:
  type: if_then
  condition:
    type: tag_equals
    key: environment
    value: "prod"
  then:
    type: all_of
    rules:
      - type: tag_exists
        keys: [on_call_team]
      - type: tag_exists
        keys: [alert_channel]
```

**Failure detail:** (detail from the `then` rule)

---

## Drift Rules

### drift_check

Compares actual resource state against an externally declared expected state. The policy engine injects the expected state into metadata before evaluation. If no expected state is present for a resource, the check passes vacuously (no declared expectation means no drift).

Drift checks detect unauthorized manual changes that bypass permissions-as-code pipelines.

**Parameters:**

| Field | Type | Description |
|-------|------|-------------|
| `source` | string | Path to the expected state file (e.g., `expected_permissions/expected_state.json`) |
| `check` | string | Type of drift check to perform |

**Supported check types:**

| Check | Expected metadata key | What it compares |
|-------|----------------------|------------------|
| `grants` | `expected_grants` | Actual grantee/privilege against declared expected grants |
| `row_filters` | `expected_row_filters` | Actual row filter function against declared expected function |
| `column_masks` | `expected_column_masks` | Actual column mask function against declared expected function |
| `group_membership` | `expected_group_members` | Actual group member against declared expected members list |

**Example:**

```yaml
rule:
  type: drift_check
  source: expected_permissions/expected_state.json
  check: grants
```

**Failure details:**

- `Drift detected: grant 'SELECT' on catalog.schema.table for user@example.com is not in expected state`
- `Drift detected: row filter 'my_filter_fn' on catalog.schema.table does not match expected 'correct_filter_fn'`
- `Drift detected: column mask 'my_mask_fn' on catalog.schema.table.column does not match expected 'correct_mask_fn'`
- `Drift detected: member 'user@example.com' in group 'admins' is not in expected state`

---

## Reference Syntax

### ref

Not a rule type itself, but a way to reference a named primitive from `rule_primitives.yml`. The engine resolves the reference and evaluates the underlying rule definition.

**Example:**

```yaml
rule:
  ref: has_data_steward
```

References can be used anywhere a rule is expected, including inside `all_of`, `any_of`, `none_of`, and `if_then` sub-rules:

```yaml
rule:
  type: all_of
  rules:
    - ref: has_data_steward
    - ref: has_retention_policy
```

If the referenced primitive does not exist, the rule fails with: `Unknown rule primitive: <name>`
