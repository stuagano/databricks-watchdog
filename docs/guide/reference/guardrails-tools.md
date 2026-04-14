# Guardrails MCP Tools Reference

The Guardrails MCP server provides 13 tools for AI developers: 9 build-time tools for discovering and validating data, and 4 runtime tools for real-time agent governance. The server is separate from Watchdog MCP (see [MCP Tools](mcp-tools.md)) because it serves a different purpose: Watchdog MCP answers "what is the compliance posture?" while Guardrails answers "is it safe to do this?"

All tools run as the calling user's identity. UC grants govern what metadata the user can see.

## Build-Time Tools (9)

### get_table_lineage

Get upstream and downstream lineage for a table from Unity Catalog.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_name` | string | Yes | Fully qualified table name (catalog.schema.table) |

**Returns:** Upstream tables (what feeds into this table) and downstream tables (what depends on it). Essential for impact analysis before modifying data.

---

### get_table_permissions

List who has access to a table and at what level.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_name` | string | Yes | Fully qualified table name |

**Returns:** Grants by group and principal showing privilege levels (SELECT, MODIFY, ALL PRIVILEGES). Useful for understanding the access boundary before sharing data through AI tools.

---

### describe_table

Get detailed metadata for a table.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `table_name` | string | Yes | | Fully qualified table name |
| `include_column_tags` | boolean | No | `true` | Include column-level tags (PII, classification) |

**Returns:** Columns with types, comments, tags, properties, storage location, and row count. Richer than a table listing: use this when column-level detail is needed.

---

### search_tables_by_tag

Find tables matching governance tags.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `tag_name` | string | Yes | Tag key (e.g., `pii`, `classification`, `data_owner`) |
| `tag_value` | string | No | Tag value to match (omit to find all tables with the tag) |
| `catalog` | string | No | Limit search to a specific catalog |

**Returns:** Matching tables with their tag values.

---

### validate_ai_query

Pre-flight governance check before an AI operation.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `tables` | array[string] | Yes | Fully qualified table names to validate |
| `operation` | string | Yes | Intended operation: query, embed, chat_context, train |
| `purpose` | string | No | Why the data is needed (logged for audit) |

**Returns:** Verdict per table (proceed, warning, blocked) with findings. When blocked, suggests alternative tables in the same schema that are safe for the requested operation. Higher-risk operations (embed, train) trigger stricter checks on classified data.

---

### suggest_safe_tables

Find tables safe for a given AI operation.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `operation` | string | Yes | | query, embed, chat_context, train |
| `schema_name` | string | No | | Schema to search (catalog.schema) |
| `keyword` | string | No | | Keyword filter for table names or comments |
| `limit` | integer | No | 20 | Max results |

**Returns:** Tables whose classification level is compatible with the intended operation. Use when `validate_ai_query` blocks a table and an alternative is needed.

---

### preview_data

Peek at sample rows from a table.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `table_name` | string | Yes | | Fully qualified table name |
| `columns` | array[string] | No | all | Specific columns to preview |
| `limit` | integer | No | 10 | Number of rows (max: 50) |
| `where` | string | No | | Optional WHERE clause |

**Returns:** Sample data rows respecting UC grants. Use as a first step when exploring unfamiliar data.

---

### safe_columns

Identify which columns are safe for an operation on a partially restricted table.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_name` | string | Yes | Fully qualified table name |
| `operation` | string | Yes | query, embed, chat_context, train |

**Returns:** Columns grouped by safety level: `safe` (no sensitive tags), `warning` (sensitive but allowed for this operation), `blocked` (PII/PHI/export-controlled). Use when `validate_ai_query` warns or blocks: often the table can still be used by excluding specific columns.

---

### estimate_cost

Estimate the DBU cost of an AI operation on a table.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `table_name` | string | Yes | Fully qualified table name |
| `operation` | string | Yes | embed, chat_context, train, query |
| `columns` | array[string] | No | Columns to include (omit for worst case) |
| `row_limit` | integer | No | Expected row count for tighter estimate |

**Returns:** Estimated token count, DBU cost, and cost breakdown by operation type. Check this before embedding large tables or running bulk inference.

---

## Runtime Tools (4)

These tools are designed for agents to call during execution for real-time governance enforcement.

### check_before_access

Runtime governance check before an agent accesses a table.

**Parameters:**

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| `agent_id` | string | Yes | | Identifier of the calling agent |
| `table` | string | Yes | | Fully qualified table name |
| `operation` | string | No | SELECT | SELECT, INSERT, UPDATE, DELETE |
| `columns` | array[string] | No | all | Specific columns to access |

**Returns:** Allow or deny decision based on the table's classification, the agent's governance status, and applicable policies. When denied, suggests alternatives (e.g., a masked view).

---

### log_agent_action

Log an agent action for the governance audit trail.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string | Yes | Agent identifier |
| `action` | string | Yes | data_access, data_export, external_api_call, model_invocation, tool_call |
| `target` | string | Yes | What was accessed (table name, API URL, endpoint name) |
| `details` | object | No | Additional context (columns, row_count, response_status) |
| `classification` | string | No | Data classification of the target |

**Returns:** Confirmation that the action was logged.

---

### get_agent_compliance

Get the current compliance status of an agent.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string | Yes | Agent identifier |

**Returns:** Governance checks passed/failed in the current session, data classifications accessed, overall risk assessment.

---

### report_agent_execution

Generate a post-execution compliance report.

**Parameters:**

| Name | Type | Required | Description |
|------|------|----------|-------------|
| `agent_id` | string | Yes | Agent identifier |
| `execution_summary` | string | No | Brief description of what the agent did |

**Returns:** Comprehensive report: all governance checks, data accessed, policies triggered, overall compliance assessment. Call when the agent finishes its task.
