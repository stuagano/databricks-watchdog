# Tables and Views Reference

Watchdog stores all governance data in Delta tables within a Unity Catalog schema (typically `platform.watchdog`). Compliance views provide pre-joined perspectives for dashboards and Genie Spaces.

## Core Tables

### resource_inventory

The central resource registry. One row per discovered resource per scan. Liquid clustered by `(scan_id, resource_type)`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `scan_id` | STRING | No | Scan batch identifier (format: YYYYMMDD_HHMMSS) |
| `metastore_id` | STRING | Yes | Unity Catalog metastore identifier |
| `resource_type` | STRING | No | Resource type (table, job, cluster, agent, etc.) |
| `resource_id` | STRING | No | Unique resource identifier |
| `resource_name` | STRING | Yes | Human-readable name |
| `owner` | STRING | Yes | Resource owner |
| `domain` | STRING | Yes | UC domain assignment |
| `tags` | MAP\<STRING, STRING\> | Yes | UC tags and crawler-injected governance tags |
| `metadata` | MAP\<STRING, STRING\> | Yes | Crawler-collected resource properties |
| `discovered_at` | TIMESTAMP | No | When the resource was discovered |

**Properties:** CDF enabled. Not append-only (enrichment crawlers update rows within a scan).

---

### violations

Deduplicated violation state. One row per `(resource_id, policy_id)` combination with current status. Updated via MERGE on each evaluate run.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `violation_id` | STRING | No | Unique violation identifier (UUID) |
| `resource_id` | STRING | No | Resource that violated the policy |
| `resource_type` | STRING | Yes | Resource type |
| `resource_name` | STRING | Yes | Human-readable resource name |
| `policy_id` | STRING | No | Policy that was violated |
| `severity` | STRING | Yes | critical, high, medium, low |
| `domain` | STRING | Yes | Governance domain |
| `detail` | STRING | Yes | Human-readable violation explanation |
| `remediation` | STRING | Yes | Steps to fix the violation |
| `owner` | STRING | Yes | Resource owner (for notification routing) |
| `resource_classes` | STRING | Yes | Comma-separated ontology class names |
| `metastore_id` | STRING | Yes | Metastore scope |
| `first_detected` | TIMESTAMP | No | When the violation was first detected |
| `last_detected` | TIMESTAMP | No | Most recent scan where violation was found |
| `resolved_at` | TIMESTAMP | Yes | When the violation was resolved (null if open) |
| `status` | STRING | No | open, resolved, exception |
| `notified_at` | TIMESTAMP | Yes | When the owner was notified (null if not yet) |
| `remediation_status` | STRING | Yes | Remediation pipeline status (none, pending_review, approved, applied, verified, etc.) |

**Properties:** CDF enabled, deletion vectors enabled, column defaults supported.

---

### policies

Current-state policy definitions. Updated via MERGE during `--sync-policies`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `policy_id` | STRING | No | Unique policy identifier |
| `policy_name` | STRING | Yes | Human-readable name |
| `applies_to` | STRING | Yes | Ontology class target or `*` |
| `domain` | STRING | Yes | Governance domain |
| `resource_types` | STRING | Yes | Legacy field (deprecated) |
| `severity` | STRING | Yes | critical, high, medium, low |
| `description` | STRING | Yes | Policy description |
| `remediation` | STRING | Yes | Remediation instructions |
| `active` | BOOLEAN | Yes | Whether the policy is evaluated |
| `rule_json` | STRING | Yes | JSON-serialized rule definition |
| `source_file` | STRING | Yes | YAML file name (for yaml-origin policies) |
| `origin` | STRING | No | `yaml` or `user` |
| `metastore_id` | STRING | Yes | Metastore scope |
| `updated_at` | TIMESTAMP | Yes | Last modification timestamp |

---

### scan_results

Append-only audit trail of every `(resource, policy)` evaluation. Liquid clustered by `(scan_id, policy_id)`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `scan_id` | STRING | No | Scan batch identifier |
| `resource_id` | STRING | No | Evaluated resource |
| `policy_id` | STRING | No | Evaluated policy |
| `result` | STRING | No | pass or fail |
| `details` | STRING | Yes | Rule evaluation detail string |
| `domain` | STRING | Yes | Governance domain |
| `severity` | STRING | Yes | Policy severity |
| `resource_classes` | STRING | Yes | Ontology class assignments |
| `metastore_id` | STRING | Yes | Metastore scope |
| `evaluated_at` | TIMESTAMP | No | Evaluation timestamp |

**Properties:** Append-only.

---

### resource_classifications

Per-scan ontology class assignments. One row per `(resource_id, class_name)` per scan.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `scan_id` | STRING | No | Scan batch identifier |
| `resource_id` | STRING | No | Classified resource |
| `resource_name` | STRING | Yes | Human-readable name |
| `resource_type` | STRING | Yes | Resource type |
| `owner` | STRING | Yes | Resource owner |
| `class_name` | STRING | No | Assigned ontology class |
| `class_ancestors` | STRING | Yes | Comma-separated ancestor classes |
| `root_class` | STRING | Yes | Top-level base class |
| `metastore_id` | STRING | Yes | Metastore scope |
| `classified_at` | TIMESTAMP | No | Classification timestamp |

---

### scan_summary

One row per scan. Provides aggregate metrics for trend analysis.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `scan_id` | STRING | No | Scan batch identifier |
| `metastore_id` | STRING | Yes | Metastore scope |
| `resources_scanned` | INT | Yes | Total resources evaluated |
| `classes_assigned` | INT | Yes | Total class assignments |
| `policies_evaluated` | INT | Yes | Number of policies run |
| `new_violations` | INT | Yes | New violations found |
| `resolved_violations` | INT | Yes | Violations resolved this scan |
| `total_open` | INT | Yes | Total open violations after scan |
| `scanned_at` | TIMESTAMP | No | Scan completion timestamp |

**Properties:** Append-only.

---

### exceptions

Approved policy waivers. Active exceptions override violation status from `open` to `exception`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `exception_id` | STRING | No | Unique exception identifier |
| `resource_id` | STRING | No | Resource covered by the exception |
| `policy_id` | STRING | No | Policy being waived |
| `approved_by` | STRING | No | Who approved the exception |
| `justification` | STRING | No | Why the exception was granted |
| `approved_at` | TIMESTAMP | No | Approval timestamp |
| `expires_at` | TIMESTAMP | Yes | Expiration (null for permanent) |
| `active` | BOOLEAN | Yes | Whether the exception is currently active |
| `metastore_id` | STRING | Yes | Metastore scope |

---

### notification_queue

Handoff table for the notification pipeline. One row per owner per notification batch. CDF enabled.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `notification_id` | STRING | No | Unique notification identifier |
| `owner` | STRING | No | Resource owner to notify |
| `severity_summary` | STRING | Yes | Human-readable severity breakdown |
| `violation_count` | INT | Yes | Total violations in digest |
| `critical_count` | INT | Yes | Critical violation count |
| `high_count` | INT | Yes | High violation count |
| `medium_count` | INT | Yes | Medium violation count |
| `low_count` | INT | Yes | Low violation count |
| `violation_ids` | STRING | Yes | Comma-separated violation UUIDs |
| `dashboard_url` | STRING | Yes | Deep link to compliance dashboard |
| `metastore_id` | STRING | Yes | Metastore scope |
| `created_at` | TIMESTAMP | No | Queue insertion time |
| `delivered_at` | TIMESTAMP | Yes | Delivery confirmation time |
| `delivery_channel` | STRING | Yes | How the notification was delivered |
| `status` | STRING | No | pending, delivered, failed |

---

## Remediation Tables

The remediation pipeline uses four tables to track agent-generated fix proposals through review, application, and verification. Created by functions in `watchdog.remediation.tables`.

### remediation_agents

Registry of available remediation agents. One row per registered agent.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `agent_id` | STRING | No | | Unique agent identifier |
| `handles` | STRING | No | | Comma-separated policy IDs or patterns the agent can remediate |
| `version` | STRING | No | | Agent version string |
| `model` | STRING | Yes | | LLM model used by the agent |
| `config_json` | STRING | Yes | | JSON-serialized agent configuration |
| `permissions` | STRING | Yes | | Permission scope for the agent |
| `active` | BOOLEAN | Yes | `true` | Whether the agent is available for proposal generation |
| `registered_at` | TIMESTAMP | No | | When the agent was registered |

---

### remediation_proposals

Proposed fixes generated by remediation agents. One row per proposal. Status tracks the proposal through the review-apply-verify lifecycle.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `proposal_id` | STRING | No | | Unique proposal identifier |
| `violation_id` | STRING | No | | Violation this proposal addresses |
| `agent_id` | STRING | No | | Agent that generated the proposal |
| `agent_version` | STRING | No | | Version of the agent at generation time |
| `status` | STRING | No | `pending_review` | Lifecycle status: pending_review, approved, rejected, applied, verified, verification_failed |
| `proposed_sql` | STRING | Yes | | SQL statement the agent proposes to execute |
| `confidence` | DOUBLE | Yes | | Agent's confidence score (0.0 to 1.0) |
| `context_json` | STRING | Yes | | JSON context the agent used to generate the proposal |
| `llm_prompt_hash` | STRING | Yes | | Hash of the LLM prompt for reproducibility |
| `citations` | STRING | Yes | | Evidence or references supporting the proposal |
| `created_at` | TIMESTAMP | No | | When the proposal was generated |

**Properties:** Column defaults supported.

---

### remediation_reviews

Human review decisions on proposals. One row per review action. Supports approval, rejection, and reassignment workflows.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `review_id` | STRING | No | Unique review identifier |
| `proposal_id` | STRING | No | Proposal being reviewed |
| `reviewer` | STRING | No | Who performed the review |
| `decision` | STRING | No | Review outcome: approved, rejected, reassigned |
| `reasoning` | STRING | Yes | Explanation for the decision |
| `reassigned_to` | STRING | Yes | New reviewer (when decision is reassigned) |
| `reviewed_at` | TIMESTAMP | No | When the review was performed |

---

### remediation_applied

Audit trail of applied proposals. One row per application. Captures pre/post state for rollback and links to a verification scan.

| Column | Type | Nullable | Default | Description |
|--------|------|----------|---------|-------------|
| `apply_id` | STRING | No | | Unique application identifier |
| `proposal_id` | STRING | No | | Proposal that was applied |
| `executed_sql` | STRING | Yes | | Actual SQL executed (may differ from proposed_sql) |
| `pre_state` | STRING | Yes | | Serialized resource state before application |
| `post_state` | STRING | Yes | | Serialized resource state after application |
| `applied_at` | TIMESTAMP | No | | When the proposal was applied |
| `verify_scan_id` | STRING | Yes | | Scan ID used to verify the fix |
| `verify_status` | STRING | No | `pending` | Verification outcome: pending, verified, failed |

**Properties:** Column defaults supported.

---

## Remediation Views

Four views measuring the remediation funnel, trends, agent effectiveness, and reviewer workload. All are regular views (not materialized), created by `ensure_remediation_views()`.

### v_remediation_funnel

**Answers:** What is the current state of the remediation pipeline?

Counts violations at each remediation stage: total open violations, those with remediation activity, pending review, approved, applied, verified, verification failed, and rejected. Joins `violations` with `remediation_proposals`.

---

### v_remediation_trend

**Answers:** How is remediation progressing over time?

One row per proposal date. Counts remediation-resolved, remediation-failed, and in-progress proposals per day. Reads from `remediation_proposals`.

---

### v_agent_effectiveness

**Answers:** How well is each remediation agent performing?

One row per `(agent_id, agent_version)`. Includes total proposals, verified count, failed count, rejected count, precision percentage, and average confidence score. Reads from `remediation_proposals`.

---

### v_reviewer_load

**Answers:** How is review work distributed across reviewers?

One row per reviewer. Shows pending reviews, total approved, rejected, reassigned, and total review count. Joins `remediation_reviews` with `remediation_proposals`.

---

## Compliance Views

All views are regular (not materialized) and refresh on every query. They are created or replaced after each evaluate run by `ensure_semantic_views()`.

### v_resource_compliance

**Answers:** What is the compliance posture of a specific resource within each of its ontology classes?

One row per `(resource_id, class_name)`. Joins `resource_classifications` with `violations`. Includes open violation counts by severity, oldest open violation, and a computed `compliance_status` (critical, high, open, clean).

---

### v_class_compliance

**Answers:** How compliant is each ontology class? How are GoldTables doing? How are PiiAssets doing?

One row per `class_name`. Aggregated violation counts, compliance percentage, and violation breakdown by severity. Ordered by open violations descending.

---

### v_domain_compliance

**Answers:** What is the executive compliance posture by governance domain?

One row per `domain` (CostGovernance, SecurityGovernance, etc.). Includes affected resource count, open violations by severity, and a collected set of classes with open violations.

---

### v_tag_policy_coverage

**Answers:** Which tag-based policies are satisfied, violated, or not evaluated for each resource?

One row per `(resource_id, policy_id)`. Crosses the latest resource inventory with active policies, joined to violations and exceptions.

---

### v_data_classification_summary

**Answers:** What is the classification coverage across catalogs?

One row per catalog. Aggregated metrics: percentage classified, percentage with data steward, percentage with sensitive data, and ontology classification coverage.

---

### v_dq_monitoring_coverage

**Answers:** Which tables have quality monitoring and what kind?

One row per table. Shows DQM status, LHM status, both, or neither. Includes anomaly counts and ontology class assignments.

---

### v_compliance_trend

**Answers:** Is compliance improving or degrading over time?

One row per scan. Reads from `scan_summary` with `LAG()` deltas for 30/60/90 day trend windows.

---

### v_cross_metastore_compliance

**Answers:** How does compliance compare across metastores?

Compliance summary per metastore: total resources, open violations, compliance percentage.

---

### v_cross_metastore_inventory

**Answers:** Are all metastores being scanned?

Resource counts per metastore by type, with latest scan timestamp.

---

### v_agent_inventory

**Answers:** What agents are deployed and what is their governance status?

One row per agent. Source (Apps or serving endpoint), owner, violation counts, governance status.

---

### v_agent_execution_compliance

**Answers:** How are agent executions behaving?

One row per agent execution. Usage metrics, violation status, risk flags.

---

### v_agent_risk_heatmap

**Answers:** Which agents pose the most risk?

One row per agent. Cross-tabulates data sensitivity against access frequency for risk scoring.

---

### v_agent_remediation_priorities

**Answers:** What single action resolves the most agent violations?

One row per `(policy_id, remediation action)`. Prioritized by impact with affected agent lists.

---

### v_ai_gateway_cost_governance

**Answers:** What is the AI Gateway token consumption and who is driving cost?

One row per `(endpoint, requester)`. Token counts, estimated cost, entity type, task type, rate-limiting flags, and Watchdog governance cross-reference.
