# Watchdog Architecture Guide

> How the components fit together, why the architecture looks this way, and what to know before modifying it.
>
> Last updated: 2026-04-21

## Design Principles

1. **Delta tables are the universal contract.** All consumers read the same tables. No consumer-specific APIs.
2. **The engine is read-only; the deploy pipeline writes.** The core engine (crawl + evaluate) never modifies workspace resources. The compile-down deploy pipeline (`watchdog-deploy`) is the sole write path — it pushes compiled artifacts (UC tag policies, ABAC column masks) to the workspace. This separation is intentional: the engine can always run safely, and writes are explicit, auditable, and support dry-run.
3. **Three integration surfaces, one data model.** Hub reads Delta for dashboards. Ontos reads via GovernanceProvider. Guardrails reads via `watchdog_client.py`.
4. **MCP is the AI gateway.** AI assistants and agents query governance posture through MCP tools, not direct SQL.
5. **Multi-metastore is a filter, not a partition.** All metastores write to the same tables with a `metastore_id` discriminator.

---

## Component Map

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Consumers                                     │
│                                                                      │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌──────────────┐  │
│  │ Lakeview   │  │ Genie      │  │ Claude /   │  │ AI Agents    │  │
│  │ Dashboard  │  │ Space      │  │ Assistants │  │ (autonomous) │  │
│  │ (10 pages) │  │ (27 tables)│  │            │  │              │  │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘  └──────┬───────┘  │
│        │ SQL            │ SQL           │ MCP/SSE        │ MCP/SSE  │
└────────┼────────────────┼───────────────┼────────────────┼──────────┘
         │                │               │                │
┌────────▼────────────────▼───────────────▼────────────────▼──────────┐
│                      Integration Layer                               │
│                                                                      │
│  ┌───────────────────┐  ┌──────────────────┐  ┌──────────────────┐  │
│  │ Watchdog MCP      │  │ Guardrails MCP   │  │ Ontos Adapter    │  │
│  │ (13 query tools)  │  │ (13 governance   │  │ (GovernanceProvider│ │
│  │                   │  │  tools: 9 build  │  │  protocol)       │  │
│  │ Compliance posture│  │  + 4 runtime)    │  │                  │  │
│  │ queries, simulate │  │                  │  │ Business catalog │  │
│  │ suggest, explore  │  │ Pre-access check │  │ compliance views │  │
│  └─────────┬─────────┘  └────────┬─────────┘  └────────┬─────────┘  │
│            │ SQL                  │ SQL + SDK            │ SQL        │
└────────────┼──────────────────────┼──────────────────────┼───────────┘
             │                      │                      │
┌────────────▼──────────────────────▼──────────────────────▼───────────┐
│                        Data Layer (Delta)                              │
│                                                                       │
│  ┌─────────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │
│  │ Core Tables (8)  │  │ Views (14)   │  │ UC System Tables         │ │
│  │                  │  │              │  │                          │ │
│  │ resource_        │  │ v_domain_    │  │ system.information_      │ │
│  │  inventory       │  │  compliance  │  │  schema.tables           │ │
│  │ violations       │  │ v_agent_     │  │ system.information_      │ │
│  │ policies         │  │  inventory   │  │  schema.table_privileges │ │
│  │ scan_results     │  │ v_compliance │  │ system.serving.          │ │
│  │ ...              │  │  _trend      │  │  endpoint_usage          │ │
│  └─────────┬────────┘  └──────────────┘  └──────────────────────────┘ │
└────────────┼──────────────────────────────────────────────────────────┘
             │
┌────────────▼──────────────────────────────────────────────────────────┐
│                        Engine (Daily Scan Job)                         │
│                                                                       │
│  ┌──────────┐  ┌───────────┐  ┌───────────┐  ┌────────┐  ┌────────┐ │
│  │ Crawlers │  │ Ontology  │  │ Rule      │  │ Policy │  │Violatio│ │
│  │ (16 types│  │ Engine    │  │ Engine    │  │ Engine │  │ns Merge│ │
│  │ SDK +    │  │ 31 classes│  │ 16 rule   │  │ YAML + │  │ dedup  │ │
│  │ system   │  │ tag-based │  │ types     │  │ Delta  │  │ + life │ │
│  │ tables)  │  │ hierarchy │  │ composable│  │ hybrid │  │ cycle  │ │
│  └──────────┘  └───────────┘  └───────────┘  └────────┘  └────────┘ │
│                                                                       │
│  Sources:                                                             │
│  ├─ UC: information_schema (tables, schemas, catalogs, volumes,       │
│  │      grants, tags, row_filters, column_masks)                      │
│  ├─ SDK: jobs, clusters, warehouses, service principals, groups,      │
│  │       group members                                                │
│  ├─ Apps API: Databricks Apps (agent heuristic)                       │
│  └─ System: system.serving.endpoint_usage + served_entities +         │
│             pipeline freshness                                        │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                  Compile-Down Pipeline (on demand)                      │
│                                                                       │
│  watchdog-compile                   watchdog-deploy                    │
│  ┌──────────┐  ┌──────────────┐    ┌──────────────┐  ┌────────────┐  │
│  │ Policy   │  │ Compile      │    │ Artifact     │  │ UC Target  │  │
│  │ Loader   │→ │ Targets:     │ →  │ Deployer     │→ │ Writes:    │  │
│  │          │  │ uc_tag_policy│    │ (dry-run     │  │ tag policies│ │
│  │ compile_ │  │ uc_abac      │    │  support)    │  │ ABAC masks │  │
│  │ to block │  │ guardrails   │    │              │  │            │  │
│  └──────────┘  └──────┬───────┘    └──────────────┘  └────────────┘  │
│                       │                                               │
│              compile_output/                                          │
│              ├── manifest.json (checksums + metadata)                 │
│              ├── *.sql (tag policy DDL)                               │
│              └── *.json (guardrails config)                           │
│                       │                                               │
│              Drift detection: manifest vs on-disk artifacts           │
│              Meta-violations: drifted/missing → violations table      │
└───────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────┐
│                  Remediation Pipeline (on demand)                       │
│                                                                       │
│  watchdog-remediate         watchdog-apply        watchdog-verify      │
│  ┌──────────┐  ┌────────┐  ┌──────────┐  ┌──────┐  ┌──────────┐    │
│  │Dispatcher│→ │Agents: │→ │ Applier  │→ │ SQL  │→ │ Verifier │    │
│  │routes    │  │Steward │  │(dry-run) │  │ exec │  │ (batch)  │    │
│  │violations│  │Cluster │  │          │  │      │  │          │    │
│  │to agents │  │DQMon   │  │          │  │      │  │          │    │
│  │          │  │JobOwner│  │          │  │      │  │          │    │
│  └──────────┘  └────────┘  └──────────┘  └──────┘  └──────────┘    │
│                                                                       │
│  Tables: remediation_agents → remediation_proposals →                 │
│          remediation_applied                                          │
│  Status: pending_review → approved → applied → verified               │
└───────────────────────────────────────────────────────────────────────┘
```

---

## Data Flow

### Write Path (Engine → Delta)

```
ResourceCrawler.crawl_all()
  ├─ _crawl_catalogs()      ──┐
  ├─ _crawl_schemas()        │
  ├─ _crawl_tables()         │
  ├─ _crawl_volumes()        │
  ├─ _crawl_grants()         ├──▶ resource_inventory (append per scan)
  ├─ _crawl_groups()         │     Liquid clustered by (scan_id, resource_type)
  ├─ _crawl_service_prns()   │
  ├─ _crawl_jobs()           │
  ├─ _crawl_clusters()       │
  ├─ _crawl_warehouses()     │
  ├─ _crawl_agents()         │     FMAPI auto-tagged: managed_endpoint=true
  └─ _crawl_agent_traces()  ──┘     AI Gateway metadata: entity_type, task

PolicyEngine.evaluate_all()
  ├─ Pass 1: OntologyEngine.classify()
  │    └──▶ resource_classifications (append per scan)
  │
  ├─ Pass 2: RuleEngine.evaluate() per (policy, resource)
  │    └──▶ scan_results (append-only audit trail)
  │
  ├─ merge_violations()
  │    └──▶ violations (MERGE: upsert + resolve, metastore-scoped)
  │
  └─ write_scan_summary()
       └──▶ scan_summary (append-only, one row per scan)
```

### Compile-Down Path (Policies → Workspace Artifacts)

```
compile_policies(policies)
  ├─ Filter policies with compile_to block
  ├─ For each target (uc_tag_policy, uc_abac, guardrails):
  │    └──▶ compile_output/<artifact_id>.sql|.json
  └─ write_manifest()
       └──▶ compile_output/manifest.json (checksums, metadata)

deploy_artifacts(manifest)
  ├─ uc_tag_policy: ALTER TAG SET ALLOWED VALUES
  ├─ uc_abac: CREATE OR REPLACE FUNCTION (column mask)
  └─ guardrails: disk-deployed (skipped by deployer)
```

### Remediation Path (Violations → Proposals → Applied)

```
dispatch_remediations(violations, agents)
  ├─ For each open violation, match agent.handles[]
  │    └──▶ agent.propose() → remediation_proposals (append)
  │
apply_proposal(proposal)
  │    └──▶ Execute proposed_sql → remediation_applied (append)
  │         └── proposal status: approved → applied
  │
batch_verify(applied, resolved_violations)
       └──▶ verify_status: pending → verified | verification_failed
```

### Read Path (Consumers → Delta)

```
Dashboard / Genie Space
  └─ SELECT FROM views (v_domain_compliance, v_agent_inventory, etc.)
       └─ Views JOIN: resource_inventory + violations + classifications + policies

Watchdog MCP (13 tools)
  └─ _execute_sql() → statement_execution API → Delta tables
       └─ All inputs sanitized via _esc()

Guardrails MCP (13 tools)
  ├─ Build-time: watchdog_client.get_resource_governance()
  │    └─ 3 queries: classifications + violations + exceptions
  │
  └─ Runtime: check_before_access()
       └─ get_resource_governance() → decision logic → session state

Ontos Adapter
  └─ GovernanceProvider protocol → WatchdogProvider → SQL queries
```

---

## Key Design Decisions

### Why Delta tables instead of APIs?

Every consumer (Dashboard, Genie, MCP, Guardrails, Ontos) reads the same Delta tables. No API layer between the engine and consumers means:
- No versioning headaches — schema evolves in one place
- No availability dependency — consumers work even if the engine job isn't running
- Full UC governance — table access is controlled by grants, not application auth
- Genie Space gets data for free — just point it at the tables

### Why ontology instead of flat tags?

UC has flat tags. A tag `data_classification=pii` doesn't know that PII is a subset of Confidential which is a subset of Internal. With flat tags:
- Changing a policy for "all confidential data" means editing 4 separate policies
- Adding a new sub-classification means updating every parent policy
- There's no concept of inheritance

The ontology gives you `PiiTable → PiiAsset → ConfidentialAsset → DataAsset`. One policy on `ConfidentialAsset` covers everything below it.

### Why MERGE for violations instead of append-only?

Violations have a lifecycle (open → resolved → exception). If we append-only, answering "what's currently open?" requires scanning all history. MERGE gives us:
- One row per (resource_id, policy_id) with current status
- `first_detected` preserved across scans for age tracking
- `last_detected` updated each scan for freshness
- Exception status override from the exceptions table

`scan_results` is append-only for audit. `violations` is MERGE for current state. Both serve different needs.

### Why two MCP servers instead of one?

Watchdog MCP answers "what's the compliance posture?" — read-only queries against governance data. Guardrails MCP answers "is it safe to do this?" — real-time decision-making with session state.

Separation means:
- Different deployment lifecycle (Watchdog MCP updates when policies change, Guardrails when agent tools change)
- Different auth models (Watchdog runs as the querying user, Guardrails can enforce per-agent policies)
- Different scaling requirements (Watchdog is query-heavy, Guardrails is latency-sensitive)

### Why a compile-down pipeline instead of direct writes?

Watchdog policies are declarative YAML. Some policies map directly to UC enforcement artifacts (tag policies that restrict allowed values, ABAC column masks). Rather than maintaining these artifacts by hand, the compile-down pipeline generates them from the same policies that drive evaluation.

The compile → deploy split is deliberate:
- **Compile is pure** — no workspace side effects, produces files you can review in a PR
- **Deploy is explicit** — dry-run support, artifact-level success/failure reporting
- **Drift detection** — the manifest tracks checksums, so the scanner can detect when on-disk artifacts diverge from what was deployed
- **Meta-violations** — drifted or missing artifacts produce violations in the same table as everything else, so the same dashboards and notifications apply

### Why remediation agents instead of auto-fix?

Auto-fixing violations (e.g., auto-tagging tables) sounds appealing but is dangerous at scale. The remediation pipeline adds a human review gate:
- Agents **propose** fixes, they don't execute them
- Every proposal enters a **review queue** (pending_review → approved → applied → verified)
- The **applier** supports dry-run so reviewers can preview SQL before execution
- The **verifier** checks whether the fix actually resolved the violation on the next scan
- All of this is auditable: `remediation_proposals`, `remediation_applied` tables

This pattern lets you start with fully manual review and progressively auto-approve low-risk, high-confidence proposals as trust builds.

### Why FMAPI endpoints get auto-classified?

Databricks Foundation Model API endpoints (`databricks-*`) are platform infrastructure, not customer agents. Without auto-classification, every FMAPI endpoint shows up as "ungoverned" in dashboards — pure noise. Auto-tagging them as `ManagedModelEndpoint` with `agent_owner=databricks` and `audit_logging_enabled=true` means they pass agent governance policies by default.

---

## Scalability Notes

### Current Bottleneck: `evaluate_all()` collects to driver

`PolicyEngine.evaluate_all()` calls `.collect()` to load the entire resource inventory into driver memory, then iterates policies × resources in Python. This is O(P*R) on the driver.

**Current capacity**: ~10K resources in under 15 minutes.
**At 100K+**: Will OOM. Needs refactoring to broadcast policies and evaluate via Spark UDF.

### Multi-Metastore: Sequential

`crawl_all_metastores()` iterates metastores sequentially. For 5+ metastores, this should be parallelized into separate Databricks workflow tasks (one per metastore).

### Views: Not Materialized

All 18 views (14 compliance + 4 remediation) are regular views. On tables with millions of rows, dashboard queries could be slow. Consider materializing the most-used views (`v_domain_compliance`, `v_agent_inventory`) if query latency becomes an issue.

---

## Adding a New Component

### New Crawler

1. Add `_crawl_mytype()` method to `engine/src/watchdog/crawler.py`
2. Register in `crawl_all()` with `_safe_crawl()`
3. Use `self._make_row()` to produce inventory rows
4. Add ontology classes in `engine/ontologies/resource_classes.yml`
5. Add policies in `engine/policies/`
6. Add tests in `tests/unit/`

### New MCP Tool

1. Add `Tool()` to `TOOLS` list in the governance module
2. Add dispatch in `handle()`
3. Implement async handler function
4. Sanitize all inputs with `_esc()`
5. Update test tool count assertion

### New Compliance View

1. Add `_ensure_myview_view()` function in `engine/src/watchdog/views.py`
2. Register in `ensure_semantic_views()`
3. Add to Genie Space table list in `deploy_genie_space.py`
4. Update test view count and name assertions

### New Industry Policy Pack

1. Create `library/myindustry/` with `ontology_classes.yml`, `rule_primitives.yml`, `policies.yml`
2. Add dashboard SQL in `library/myindustry/dashboards/`
3. Add tests in `tests/unit/test_policy_pack_myindustry.py`
4. Document in README industry packs table

### New Rule Type

The rule engine's dispatch table (`rule_engine.py` line ~75) maps rule type strings to evaluator methods. To add a new rule type:

1. Add evaluator method: `_eval_mytype(self, rule: dict, tags: dict, metadata: dict) -> RuleResult`
2. Register in the dispatch table: `"mytype": self._eval_mytype`
3. Add tests in `tests/unit/test_rule_engine.py`
4. Document the rule schema in this guide

All evaluators receive the same three arguments (rule config, resource tags, resource metadata) and return a `RuleResult(passed, detail, rule_type)`.

---

## Drift Detection Pattern — External Expected State

Watchdog's core model is **posture evaluation**: crawl actual state, evaluate against policies, report violations. But some governance scenarios require comparing actual state against a **declared expected state** maintained outside Watchdog — for example, a permissions-as-code system that defines what grants *should* exist.

This is the **drift detection pattern**: an external system produces an `expected_state.json` describing what should be true, uploads it to a UC volume, and Watchdog policies compare actual state against it.

### How It Works

```
External System (e.g., permissions compiler, IaC pipeline)
       │
  Generates expected_state.json
       │
  Uploads to UC volume: {catalog}.{schema}.{volume_name}/expected_state.json
       │
Watchdog Scanner (daily scan)
       │
  ┌────┴──────────────────────────────────────┐
  │ drift_check rule type                      │
  │  1. Reads expected_state.json from volume  │
  │  2. Queries actual state (INFORMATION_     │
  │     SCHEMA, SDK, resource_inventory)       │
  │  3. Diffs expected vs actual               │
  │  4. Returns FAIL with detail if mismatch   │
  └────────────────────────────────────────────┘
       │
  Violations table (same lifecycle as any other violation)
  Notifications (same pipeline)
  Dashboards (same views)
```

### The `drift_check` Rule Type

A planned extension to the rule engine dispatch table. Unlike other rule types that evaluate resource properties (tags, metadata), `drift_check` compares a resource against an external declaration of what should exist.

**Policy schema:**
```yaml
- id: POL-DRIFT-001
  name: "Grant drift detection"
  applies_to: GrantAsset
  domain: AccessControl
  severity: critical
  active: true
  rule:
    type: drift_check
    source: expected_permissions/expected_state.json   # path within UC volume
    check: grants                                       # section of expected_state.json
```

**Expected state JSON structure:**
```json
{
  "generated_at": "2026-04-14T10:00:00Z",
  "environment": "production",
  "grants": [
    {
      "catalog": "gold",
      "schema": "finance",
      "table": null,
      "principal": "finance-analysts",
      "privileges": ["SELECT", "USE_CATALOG", "USE_SCHEMA"]
    }
  ],
  "row_filters": [
    {
      "table": "gold.finance.gl_balances",
      "function": "permissions_filter_finance_gl_balances",
      "enforcement": "uc_native",
      "checksum": "sha256:a1b2c3..."
    }
  ],
  "column_masks": [
    {
      "table": "gold.finance.gl_balances",
      "column": "cost_center_owner",
      "function": "permissions_mask_cost_center_owner",
      "enforcement": "uc_native",
      "checksum": "sha256:d4e5f6..."
    }
  ]
}
```

**Evaluator behavior:**
- Loads expected state from the UC volume path specified in `rule.source`
- Reads the section specified by `rule.check` (grants, row_filters, column_masks, group_membership)
- Queries the corresponding actual state (grants crawler output, `INFORMATION_SCHEMA`, SDK)
- Returns `RuleResult(passed=False, detail="...")` listing extra, missing, or modified entries
- UDF integrity uses checksums — if a row filter function was manually edited, the checksum won't match even though the function name is correct

### Design Principles

1. **The drift engine is read-only.** Drift detection reports mismatches. It never creates, modifies, or revokes grants. For drift violations, remediation belongs to the external system that owns the expected state. (Non-drift violations can use the remediation pipeline.)

2. **External systems own expected state.** Watchdog doesn't know or care how the expected state was produced — it could be a permissions compiler, Terraform output, a spreadsheet export, or a manual JSON file. The contract is the JSON schema.

3. **Policy namespace convention.** External systems should use a distinct policy ID prefix to avoid collisions with Watchdog's built-in policies:

| Prefix | Owner |
|--------|-------|
| `POL-A*` | Watchdog (access governance) |
| `POL-AGENT-*` | Watchdog (agent governance) |
| `POL-PERM-*` | Permissions enforcement (external) |
| `POL-DRIFT-*` | Generic drift detection (external) |

4. **Same violation lifecycle.** Drift violations land in the same `violations` table, use the same notification pipeline, and appear in the same dashboards. No special handling needed.

### Use Cases

- **Permissions-as-code:** A YAML-based permissions compiler defines team grants, row filters, and column masks. Expected state is generated at deploy time. Watchdog detects unauthorized manual grants or modified UDFs.
- **Infrastructure-as-code:** Terraform defines workspace configuration (cluster policies, init scripts, network settings). Expected state captures what Terraform applied. Watchdog detects manual overrides.
- **Compliance baselines:** A compliance team defines required minimum grants for auditors. Expected state is a static JSON file. Watchdog detects if grants are revoked.

### Implementation Status

The `drift_check` rule type is **implemented** in the rule engine dispatch table. It supports four check types: `grants`, `row_filters`, `column_masks`, and `group_membership`. Four drift policies are active: POL-DRIFT-001 through POL-DRIFT-004. External systems produce the expected state file, upload it to a UC volume, and the evaluator consumes it on the next scan.
