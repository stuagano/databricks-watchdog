# Architecture

## The Observability Layer Model

Databricks Unity Catalog is the **control plane** -- it enforces governance at query time through ABAC, tag policies, row filters, and column masks. Watchdog is the **observability layer** -- it measures governance posture by crawling, classifying, and evaluating the same resources the control plane governs.

This separation is deliberate. The control plane answers "is this specific query allowed?" Watchdog answers "across all resources and all policies, how compliant is the estate?"

Neither replaces the other. Enforcement without measurement is blind. Measurement without enforcement is toothless.

## Design Principles

Five principles shape every architectural decision in Watchdog:

1. **Delta tables are the universal contract.** Every consumer -- dashboards, Genie spaces, MCP servers, business catalog adapters -- reads the same Delta tables. There is no consumer-specific API layer. Schema evolves in one place, consumers work even when the engine job is not running, and table access is governed by UC grants rather than application-level auth.

2. **Watchdog is read-only.** The engine crawls and evaluates. It never writes tags, creates grants, modifies ABAC policies, or revokes permissions. Remediation is always a human action. This eliminates the risk of automated governance tools making destructive changes.

3. **Three integration surfaces, one data model.** Dashboards read Delta via SQL. MCP servers read Delta via the statement execution API. Business catalog adapters read Delta via the GovernanceProvider protocol. All three surfaces query the same tables.

4. **MCP is the AI gateway.** AI assistants and autonomous agents query governance posture through MCP tools, not direct SQL. This provides tool-level access control, input sanitization, and a stable interface that does not change when the underlying schema evolves.

5. **Multi-metastore is a filter, not a partition.** All metastores write to the same tables with a `metastore_id` discriminator column. Cross-metastore views aggregate across metastores. Single-metastore queries filter by `metastore_id`. No separate table sets per metastore.

## Component Map

```
+---------------------------------------------------------------------+
|                        Consumers                                     |
|                                                                      |
|  +--------------+  +--------------+  +--------------+  +----------+ |
|  | Lakeview     |  | Genie        |  | Claude /     |  | AI       | |
|  | Dashboard    |  | Space        |  | Assistants   |  | Agents   | |
|  | (10 pages)   |  | (27 tables)  |  |              |  |          | |
|  +------+-------+  +------+-------+  +------+-------+  +----+-----+ |
|         | SQL              | SQL            | MCP/SSE       | MCP    |
+---------+------------------+----------------+---------------+--------+
          |                  |                |               |
+---------v------------------v----------------v---------------v--------+
|                      Integration Layer                               |
|                                                                      |
|  +---------------------+  +--------------------+  +----------------+ |
|  | Watchdog MCP        |  | Guardrails MCP     |  | Business       | |
|  | (13 query tools)    |  | (9 build-time +    |  | Catalog        | |
|  |                     |  |  4 runtime tools)  |  | Adapter        | |
|  | Compliance posture  |  | Pre-access checks  |  | GovernanceProv | |
|  | queries, simulate   |  | action logging     |  | protocol       | |
|  +----------+----------+  +---------+----------+  +--------+-------+ |
|             | SQL                    | SQL + SDK            | SQL     |
+-------------+------------------------+----------------------+--------+
              |                        |                      |
+-------------v------------------------v----------------------v--------+
|                        Data Layer (Delta)                            |
|                                                                      |
|  +-------------------+  +--------------+  +-----------------------+  |
|  | Core Tables (8)   |  | Views (14)   |  | UC System Tables      |  |
|  |                   |  |              |  |                       |  |
|  | resource_         |  | v_domain_    |  | system.information_   |  |
|  |  inventory        |  |  compliance  |  |  schema.tables        |  |
|  | violations        |  | v_agent_     |  | system.information_   |  |
|  | policies          |  |  inventory   |  |  schema.*_privileges  |  |
|  | scan_results      |  | v_compliance |  | system.serving.       |  |
|  | scan_summary      |  |  _trend      |  |  endpoint_usage       |  |
|  | resource_         |  | ...          |  |                       |  |
|  |  classifications  |  |              |  |                       |  |
|  | exceptions        |  |              |  |                       |  |
|  | notification_     |  |              |  |                       |  |
|  |  queue            |  |              |  |                       |  |
|  +--------+----------+  +--------------+  +-----------------------+  |
+-----------|--------------------------------------------------------------+
            |
+-----------v--------------------------------------------------------------+
|                        Engine (Daily Scan Job)                            |
|                                                                          |
|  +----------+  +-----------+  +-----------+  +--------+  +------------+  |
|  | Crawlers |  | Ontology  |  | Rule      |  | Policy |  | Violations |  |
|  | (16 types|  | Engine    |  | Engine    |  | Engine |  | Merge      |  |
|  | SDK +    |  | 28 classes|  | 14 rule   |  | YAML + |  | dedup +    |  |
|  | system   |  | tag-based |  | types     |  | Delta  |  | lifecycle  |  |
|  | tables)  |  | hierarchy |  | composable|  | hybrid |  |            |  |
|  +----------+  +-----------+  +-----------+  +--------+  +------------+  |
|                                                                          |
|  Sources:                                                                |
|   - UC: information_schema (tables, schemas, catalogs, volumes, grants)  |
|   - SDK: jobs, clusters, warehouses, service principals, groups          |
|   - Apps API: Databricks Apps (agent heuristic)                          |
|   - System: system.serving.endpoint_usage + served_entities              |
+--------------------------------------------------------------------------+
```

### Consumers

Four types of consumer read the data layer:

- **Lakeview Dashboards.** SQL-based dashboards with 10 pages covering domain compliance, agent inventory, cost governance, and trends.
- **Genie Space.** Natural-language governance queries backed by 27 tables (all 14 semantic views plus UC system tables and `system.serving.endpoint_usage`).
- **AI Assistants.** Claude, ChatGPT, or other assistants query governance posture through the Watchdog MCP server (13 tools).
- **Autonomous AI Agents.** Agents building on the lakehouse call the Guardrails MCP for real-time access checks, action logging, and compliance reporting.

### Integration Layer

Three servers expose the data layer to different personas:

- **Watchdog MCP.** 13 read-only tools for querying compliance posture -- violations, policies, scan history, what-if simulation, and governance exploration. Runs as an MCP server over SSE.
- **Guardrails MCP.** 13 tools split across build-time (9) and runtime (4). Build-time tools validate table usage and discover governed assets. Runtime tools provide `check_before_access` for real-time gate decisions.
- **Business Catalog Adapter.** Implements a GovernanceProvider protocol so business catalog tools can display classification and violation data in their governance views.

### Data Layer

Eight core Delta tables and 14 semantic views. The engine writes to the core tables; consumers read from views that join and aggregate across tables. UC system tables provide source data that the engine crawls.

### Engine

The daily scan job runs four stages in sequence: crawl, classify, evaluate, merge. Each stage produces Delta output. The engine is a Python package deployed as a Databricks Workflow with task-per-stage orchestration.

## Data Flow

### Write Path (Engine to Delta)

The engine produces data in four stages:

```
ResourceCrawler.crawl_all()
  |-- _crawl_catalogs()
  |-- _crawl_schemas()
  |-- _crawl_tables()
  |-- _crawl_volumes()
  |-- _crawl_grants()         --> resource_inventory
  |-- _crawl_groups()             (append per scan, liquid clustered
  |-- _crawl_service_prns()        by scan_id + resource_type)
  |-- _crawl_jobs()
  |-- _crawl_clusters()
  |-- _crawl_warehouses()
  |-- _crawl_agents()
  |-- _crawl_agent_traces()

PolicyEngine.evaluate_all()
  |-- Pass 1: OntologyEngine.classify()
  |     --> resource_classifications (append per scan)
  |
  |-- Pass 2: RuleEngine.evaluate() per (policy, resource)
  |     --> scan_results (append-only audit trail)
  |
  |-- merge_violations()
  |     --> violations (MERGE: upsert + resolve, metastore-scoped)
  |
  |-- write_scan_summary()
        --> scan_summary (append-only, one row per scan)
```

**Stage 1: Crawl.** Sixteen crawlers enumerate workspace resources from UC `information_schema`, the Databricks SDK, and system tables. All resources land in `resource_inventory` with a `scan_id` for point-in-time queries. FMAPI endpoints are auto-tagged as `ManagedModelEndpoint` to prevent noise in agent governance dashboards.

**Stage 2: Classify.** The ontology engine assigns each resource to one or more ontology classes based on its tags and metadata. Classifications are written to `resource_classifications` with full ancestry chains.

**Stage 3: Evaluate.** The rule engine evaluates every active policy against every resource whose ontology class matches the policy's `applies_to` field. Results are appended to `scan_results` as an immutable audit trail.

**Stage 4: Merge.** `merge_violations()` deduplicates scan results into the `violations` table using a MERGE statement keyed on `(resource_id, policy_id)`. New failures become open violations. Existing failures update `last_detected`. Resources that now pass are marked resolved. Active exceptions override status to `exception`.

### Read Path (Consumers to Delta)

```
Dashboards / Genie Space
  --> SELECT FROM views (v_domain_compliance, v_agent_inventory, etc.)
        Views JOIN: resource_inventory + violations + classifications + policies

Watchdog MCP (13 tools)
  --> _execute_sql() -> statement_execution API -> Delta tables
        All inputs sanitized via _esc()

Guardrails MCP (13 tools)
  |-- Build-time: watchdog_client.get_resource_governance()
  |     --> 3 queries: classifications + violations + exceptions
  |
  |-- Runtime: check_before_access()
        --> get_resource_governance() -> decision logic -> session state

Business Catalog Adapter
  --> GovernanceProvider protocol -> WatchdogProvider -> SQL queries
```

All consumers read from the same underlying Delta tables. Views provide pre-joined, pre-aggregated surfaces for common query patterns. MCP servers add tool-level access control and input sanitization. The business catalog adapter transforms governance data into the provider protocol's expected format.

## Key Design Decisions

### Why Delta tables instead of an API layer?

Every consumer reads the same Delta tables. No API layer sits between the engine and consumers. This means:

- Schema evolves in one place, not across API versions.
- Consumers function even when the engine job is not running.
- UC governance (grants) controls table access -- no application-level auth needed.
- Genie spaces get data for free by pointing at the tables.

### Why an ontology instead of flat tags?

UC tags are flat. A tag `data_classification=pii` has no knowledge that PII is a subset of Confidential, which is a subset of Internal. With flat tags, changing a policy for "all confidential data" requires editing four separate policies. Adding a sub-classification requires updating every parent policy.

The ontology provides inheritance: `PiiTable -> PiiAsset -> ConfidentialAsset -> DataAsset`. One policy on `ConfidentialAsset` covers everything below it in the hierarchy. Taxonomy changes propagate automatically.

### Why MERGE for violations instead of append-only?

Violations have a lifecycle (open, resolved, exception). An append-only model requires scanning all history to answer "what is currently open?" MERGE provides:

- One row per `(resource_id, policy_id)` with current status.
- `first_detected` preserved across scans for age tracking.
- `last_detected` updated each scan for freshness.
- Exception status overrides from the exceptions table.

The `scan_results` table remains append-only for audit purposes. The `violations` table uses MERGE for current-state queries. Both serve different needs.

### Why two MCP servers instead of one?

Watchdog MCP answers "what is the compliance posture?" -- read-only queries against governance data. Guardrails MCP answers "is it safe to do this?" -- real-time decision-making with per-agent session state.

Separation provides:

- Different deployment lifecycles (Watchdog MCP updates when policies change; Guardrails updates when agent tools change).
- Different auth models (Watchdog runs as the querying user; Guardrails enforces per-agent policies).
- Different scaling requirements (Watchdog is query-heavy; Guardrails is latency-sensitive).
