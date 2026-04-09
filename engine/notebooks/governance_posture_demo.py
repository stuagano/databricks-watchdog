# Databricks notebook source
# MAGIC %md
# MAGIC # UC Governance Hub + Watchdog — Compliance Posture Demo
# MAGIC
# MAGIC This notebook walks through what the Databricks Governance Hub provides natively,
# MAGIC where the gaps are, and what Watchdog adds. Run it after deploying the Watchdog engine
# MAGIC and completing at least one scan.
# MAGIC
# MAGIC **Prerequisites:**
# MAGIC - Watchdog engine deployed and scan completed
# MAGIC - SQL warehouse available
# MAGIC - Adjust `CATALOG` and `SCHEMA` in the first cell

# COMMAND ----------

# Configuration — adjust for your workspace
CATALOG = "serverless_stable_s0v155_catalog"
SCHEMA = "watchdog"
QS = f"{CATALOG}.{SCHEMA}"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 1: What the Governance Hub Shows You (System Tables)
# MAGIC
# MAGIC The native Governance Hub surfaces UC metadata through `system.information_schema`.
# MAGIC This is what you get out of the box — no additional tools needed.

# COMMAND ----------

# MAGIC %md
# MAGIC ### UC Asset Inventory
# MAGIC How many tables, views, and other assets exist in this metastore?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_type, COUNT(*) as asset_count
# MAGIC FROM system.information_schema.tables
# MAGIC WHERE table_catalog NOT IN ('system', 'samples')
# MAGIC GROUP BY table_type
# MAGIC ORDER BY asset_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### UC Tag Coverage
# MAGIC Which tags are applied to tables? How many tables have each tag?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tag_name, COUNT(DISTINCT concat(catalog_name, '.', schema_name, '.', table_name)) as tagged_tables
# MAGIC FROM system.information_schema.table_tags
# MAGIC WHERE catalog_name NOT IN ('system', 'samples')
# MAGIC GROUP BY tag_name
# MAGIC ORDER BY tagged_tables DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### UC Privilege Distribution
# MAGIC What privileges are granted, and to how many principals?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT privilege_type, COUNT(*) as grant_count, COUNT(DISTINCT grantee) as distinct_grantees
# MAGIC FROM system.information_schema.table_privileges
# MAGIC WHERE table_catalog NOT IN ('system', 'samples')
# MAGIC GROUP BY privilege_type
# MAGIC ORDER BY grant_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ### UC Undocumented Tables
# MAGIC Tables with no tags and no comments — metadata gaps the Hub can surface.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.table_catalog, t.table_schema, t.table_name, t.table_owner
# MAGIC FROM system.information_schema.tables t
# MAGIC LEFT JOIN system.information_schema.table_tags tt
# MAGIC   ON t.table_catalog = tt.catalog_name AND t.table_schema = tt.schema_name AND t.table_name = tt.table_name
# MAGIC WHERE t.table_catalog NOT IN ('system', 'samples')
# MAGIC   AND tt.tag_name IS NULL
# MAGIC   AND (t.comment IS NULL OR t.comment = '')
# MAGIC LIMIT 15

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 2: What the Hub Doesn't Show You (The Gaps)
# MAGIC
# MAGIC The Hub surfaces **metadata and access facts**. But it can't answer:
# MAGIC
# MAGIC | Question | Why the Hub Can't Answer It |
# MAGIC |---|---|
# MAGIC | "Across ALL my policies, what % of my estate is compliant?" | Hub shows domains separately — no cross-domain posture |
# MAGIC | "Which PII tables are missing a data steward?" | Hub knows tags exist, but can't evaluate cross-tag rules |
# MAGIC | "Who owns the most violations and how long have they been open?" | Hub has no concept of a "violation" as a tracked object |
# MAGIC | "If I add a new policy, how many resources would violate it?" | Hub has no policy simulation capability |
# MAGIC | "Are all production jobs configured with alerting?" | Hub doesn't crawl compute resources (jobs, clusters) |
# MAGIC
# MAGIC This is where Watchdog comes in.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 3: What Watchdog Adds

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross-Domain Compliance Posture
# MAGIC One view across security, data quality, cost governance, and operations.
# MAGIC **The Hub cannot produce this view.**

# COMMAND ----------

spark.sql(f"""
SELECT domain,
       COUNT(*) as total_violations,
       SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical,
       SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high,
       SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) as medium,
       SUM(CASE WHEN severity = 'low' THEN 1 ELSE 0 END) as low,
       COUNT(DISTINCT resource_id) as resources_affected,
       COUNT(DISTINCT owner) as owners_affected
FROM {QS}.violations
WHERE status = 'open'
GROUP BY domain
ORDER BY critical DESC, high DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ontology Classification with Inheritance
# MAGIC Resources are classified into a hierarchy. One policy on `ConfidentialAsset`
# MAGIC automatically covers every `PiiAsset`, `HipaaAsset`, and `SoxAsset`.
# MAGIC
# MAGIC **The Hub has flat tags. No inheritance. No taxonomy.**

# COMMAND ----------

spark.sql(f"""
SELECT class_name, class_ancestors, COUNT(DISTINCT resource_id) as resources
FROM {QS}.resource_classifications
WHERE scan_id = (SELECT MAX(scan_id) FROM {QS}.resource_classifications)
GROUP BY class_name, class_ancestors
ORDER BY resources DESC
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Owner Accountability
# MAGIC Every violation is attributed to a resource owner.
# MAGIC
# MAGIC **The Hub has no "violations per owner" concept.**

# COMMAND ----------

spark.sql(f"""
SELECT COALESCE(NULLIF(owner, ''), 'Unassigned') as owner,
       COUNT(*) as total_violations,
       SUM(CASE WHEN severity = 'critical' THEN 1 ELSE 0 END) as critical,
       SUM(CASE WHEN severity = 'high' THEN 1 ELSE 0 END) as high,
       SUM(CASE WHEN severity = 'medium' THEN 1 ELSE 0 END) as medium,
       SUM(CASE WHEN severity = 'low' THEN 1 ELSE 0 END) as low,
       COUNT(DISTINCT policy_id) as policies_violated,
       COUNT(DISTINCT domain) as domains
FROM {QS}.violations
WHERE status = 'open'
GROUP BY COALESCE(NULLIF(owner, ''), 'Unassigned')
ORDER BY critical DESC, high DESC, total_violations DESC
LIMIT 15
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Composable Rules the Platform Can't Express
# MAGIC
# MAGIC Tag Policies enforce "this tag must use these values."
# MAGIC Watchdog rules express cross-tag logic:
# MAGIC
# MAGIC - **POL-S001**: IF `data_classification = pii` THEN must have BOTH `data_steward` AND `retention_days`
# MAGIC - **POL-A002**: Grant grantee must match regex `^(group:|account group:)` — no direct user grants
# MAGIC - **POL-C003**: ALL resources must have `cost_center` tag
# MAGIC - **POL-O004**: IF `environment = prod` THEN must have `on_call_team` AND `alert_channel`
# MAGIC
# MAGIC None of these can be expressed as a native Tag Policy or ABAC rule.

# COMMAND ----------

spark.sql(f"""
SELECT policy_id, severity, domain,
       COUNT(*) as violation_count,
       COUNT(DISTINCT resource_id) as resource_count
FROM {QS}.violations
WHERE status = 'open'
GROUP BY policy_id, severity, domain
ORDER BY violation_count DESC
LIMIT 15
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Actionable Remediation: Direct User Grants
# MAGIC
# MAGIC Watchdog policy POL-A002 caught grants assigned directly to users instead of groups.
# MAGIC This is an immediately actionable remediation list.
# MAGIC
# MAGIC **The Hub shows grants in `information_schema.table_privileges` but doesn't flag
# MAGIC which ones violate your organization's policies.**

# COMMAND ----------

spark.sql(f"""
SELECT ri.metadata['grantee'] as grantee,
       ri.metadata['privilege'] as privilege,
       ri.metadata['securable_type'] as securable_type,
       ri.metadata['securable_full_name'] as securable_name
FROM {QS}.resource_inventory ri
JOIN {QS}.violations v ON ri.resource_id = v.resource_id
WHERE v.policy_id = 'POL-A002'
  AND v.status = 'open'
  AND ri.scan_id = (SELECT MAX(scan_id) FROM {QS}.resource_inventory)
  AND ri.resource_type = 'grant'
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 4: The Unified View (Hub + Watchdog)
# MAGIC
# MAGIC The real power is joining UC system tables (what the Hub knows) with Watchdog
# MAGIC compliance data (what the Hub doesn't track). These queries are only possible
# MAGIC when both data sources are available.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Who Has Access to Resources with Critical/High Violations?
# MAGIC
# MAGIC Joins Watchdog violations (severity) with UC table_privileges (who has access).
# MAGIC **Neither source can answer this alone.**

# COMMAND ----------

spark.sql(f"""
SELECT v.resource_name as violated_table,
       v.severity,
       v.policy_id,
       p.grantee as who_has_access,
       p.privilege_type
FROM {QS}.violations v
JOIN system.information_schema.table_privileges p
  ON v.resource_name = p.table_name
WHERE v.status = 'open'
  AND v.severity IN ('critical', 'high')
  AND p.table_catalog NOT IN ('system', 'samples')
ORDER BY v.severity, v.resource_name
LIMIT 20
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Untagged Tables That Also Have Violations
# MAGIC
# MAGIC Tables with no UC tags AND open Watchdog violations — the worst governance gaps.
# MAGIC **Requires joining UC table_tags with Watchdog violations.**

# COMMAND ----------

spark.sql(f"""
SELECT ri.resource_name,
       ri.owner,
       COUNT(DISTINCT v.violation_id) as open_violations,
       MAX(v.severity) as worst_severity
FROM {QS}.resource_inventory ri
JOIN {QS}.violations v
  ON ri.resource_id = v.resource_id AND v.status = 'open'
LEFT JOIN system.information_schema.table_tags tt
  ON ri.resource_name = tt.table_name
  AND tt.catalog_name NOT IN ('system', 'samples')
WHERE ri.scan_id = (SELECT MAX(scan_id) FROM {QS}.resource_inventory)
  AND ri.resource_type = 'table'
  AND tt.tag_name IS NULL
GROUP BY ri.resource_name, ri.owner
ORDER BY open_violations DESC
LIMIT 15
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Native UC Tag Coverage vs Watchdog Ontology Classification
# MAGIC
# MAGIC Side-by-side comparison: how many resources does UC tagging cover vs Watchdog's
# MAGIC ontology classification?

# COMMAND ----------

spark.sql(f"""
SELECT 'UC Tags (system tables)' as source,
       COUNT(DISTINCT concat(catalog_name, '.', schema_name, '.', table_name)) as covered_tables
FROM system.information_schema.table_tags
WHERE catalog_name NOT IN ('system', 'samples')

UNION ALL

SELECT 'Watchdog Ontology' as source,
       COUNT(DISTINCT resource_id) as covered_tables
FROM {QS}.resource_classifications
WHERE scan_id = (SELECT MAX(scan_id) FROM {QS}.resource_classifications)
  AND class_name != 'DataAsset'

UNION ALL

SELECT 'Total Tables' as source,
       COUNT(DISTINCT resource_id) as covered_tables
FROM {QS}.resource_inventory
WHERE scan_id = (SELECT MAX(scan_id) FROM {QS}.resource_inventory)
  AND resource_type = 'table'
""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part 5: Governance Hub Integration
# MAGIC
# MAGIC ### How Watchdog Fits with the Hub
# MAGIC
# MAGIC | Component | What It Does | Data Source |
# MAGIC |---|---|---|
# MAGIC | **Governance Hub** | Manages tags, ABAC, metastore settings. Shows usage/access dashboards. | `system.information_schema.*` |
# MAGIC | **Watchdog Engine** | Evaluates cross-domain policies. Tracks violations with lifecycle. | `<catalog>.watchdog.*` |
# MAGIC | **Unified Dashboard** | Joins both — who has access to violated resources, untagged + violated tables | Both |
# MAGIC | **Watchdog MCP** | AI assistants query compliance posture | `<catalog>.watchdog.*` via SQL |
# MAGIC | **Genie Space** | Business users ask governance questions in natural language | Both (19 tables) |
# MAGIC | **Guardrails MCP** | AI agents check governance before accessing data | `<catalog>.watchdog.*` |
# MAGIC
# MAGIC ### The One-Liner
# MAGIC
# MAGIC **The Hub manages governance. Watchdog measures compliance. Together they answer
# MAGIC questions neither can answer alone.**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC 1. **Explore the Lakeview Dashboard** — "UC Governance Hub + Watchdog Compliance" (5 pages)
# MAGIC 2. **Try the Genie Space** — ask "who has the most critical violations?" in natural language
# MAGIC 3. **Connect the MCP Server** — add `https://<app-url>/mcp/sse` to Claude Code
# MAGIC 4. **Add an industry policy pack** — copy `library/healthcare/` into `engine/ontologies/` and `engine/policies/`
# MAGIC 5. **Customize policies** — add your own YAML policies to `engine/policies/`
