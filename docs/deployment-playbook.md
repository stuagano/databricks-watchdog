# Deployment Playbook

> Step-by-step guide for deploying Watchdog to a customer workspace. Covers prerequisites, deployment order, validation, and troubleshooting.
>
> Last updated: 2026-04-13

## Deployment Overview

```
┌───────────────────────────────────────────────────────────┐
│                    Deployment Order                         │
│                                                            │
│  1. Engine Bundle     ─── tables + job + policies          │
│         │                                                  │
│  2. First Scan        ─── populate data                    │
│         │                                                  │
│  3. Lakeview Dashboard─── visualize results                │
│         │                                                  │
│  4. Genie Space       ─── NL governance exploration        │
│         │                                                  │
│  5. Watchdog MCP      ─── AI assistant interface           │
│         │                                                  │
│  6. Guardrails MCP    ─── build-time + runtime governance  │
│                                                            │
│  Time: ~30 min for steps 1-3, ~15 min each for 4-6        │
└───────────────────────────────────────────────────────────┘
```

## Prerequisites

- Databricks workspace with **Unity Catalog** enabled
- **Databricks CLI** v0.230+ (`databricks --version`)
- A catalog where Watchdog can create a `watchdog` schema
- A SQL warehouse (Starter Warehouse works for demo, Pro recommended for production)
- CLI profile configured (`databricks configure --profile my-workspace`)

## Step 1: Deploy the Engine

```bash
# Clone the repo
git clone https://github.com/stuagano/databricks-watchdog.git
cd databricks-watchdog

# Edit databricks.yml — add your workspace target
vim engine/databricks.yml

# Validate and deploy
cd engine
databricks bundle validate -t my-workspace
databricks bundle deploy -t my-workspace
```

**What gets deployed:**
- Two workflow jobs (daily scan + ad-hoc scan)
- Source code uploaded to workspace
- Tables created on first scan run

**Validate:**
```bash
databricks jobs list --profile my-workspace | grep Watchdog
# Should show: Watchdog — Daily Governance Scan
#              Watchdog — Ad Hoc Scan
```

## Step 2: Run First Scan

```bash
# Trigger the ad-hoc scan
databricks bundle run watchdog_adhoc_scan -t my-workspace

# Or via job ID:
databricks jobs run-now <JOB_ID> --profile my-workspace --no-wait
```

**What happens:**
1. Crawls 16 resource types (~2-3 min)
2. Syncs YAML policies to Delta
3. Classifies resources via ontology (28 classes)
4. Evaluates 46 policies
5. Merges violations (dedup + lifecycle)
6. Writes scan summary
7. Refreshes 14 semantic views

**Validate:**
```sql
-- Check resource count
SELECT resource_type, COUNT(*) FROM your_catalog.watchdog.resource_inventory
WHERE scan_id = (SELECT MAX(scan_id) FROM your_catalog.watchdog.resource_inventory)
GROUP BY resource_type ORDER BY COUNT(*) DESC;

-- Check violations
SELECT severity, COUNT(*) FROM your_catalog.watchdog.violations
WHERE status = 'open' GROUP BY severity;

-- Check policies loaded
SELECT COUNT(*) FROM your_catalog.watchdog.policies;
```

## Step 3: Deploy Dashboards

```bash
python engine/dashboards/lakeview/deploy_dashboard.py \
  --profile my-workspace \
  --catalog your_catalog \
  --schema watchdog \
  --warehouse-id your_warehouse_id \
  --publish
```

**Dashboards:**
- **Watchdog Governance Posture** — standalone (5 pages)
- **UC Governance Hub + Watchdog** — unified (10 pages, includes agent compliance + remediation)

## Step 4: Deploy Genie Space

```bash
python mcp/genie/deploy_genie_space.py \
  --catalog your_catalog \
  --schema watchdog \
  --warehouse-id your_warehouse_id \
  --profile my-workspace
```

**Validate:** Open the Genie Space and ask "What's our compliance posture?"

## Step 5: Deploy Watchdog MCP

```bash
cd mcp
databricks bundle deploy -t my-workspace
databricks apps start watchdog-mcp-my-workspace --profile my-workspace
# Wait for ACTIVE
databricks apps deploy watchdog-mcp-my-workspace \
  --source-code-path /Workspace/Users/you@company.com/.bundle/watchdog-mcp/my-workspace/files \
  --profile my-workspace
```

**Validate:**
```bash
# Check app status
databricks apps get watchdog-mcp-my-workspace --profile my-workspace
# State should be: RUNNING
```

Connect from Claude Code: `https://<app-url>/mcp/sse`

## Step 6: Deploy Guardrails MCP

```bash
cd guardrails
databricks bundle deploy -t my-workspace
databricks apps start mcp-ai-guardrails-my-workspace --profile my-workspace
# Wait for ACTIVE
databricks apps deploy mcp-ai-guardrails-my-workspace \
  --source-code-path /Workspace/Users/you@company.com/.bundle/ai-devkit-guardrails/my-workspace/files \
  --profile my-workspace
```

---

## Troubleshooting

### Schema mismatch on first deploy

```
[DELTA_METADATA_MISMATCH] A schema mismatch detected when writing to the Delta table
```

Tables were created before a schema change (e.g., new `metastore_id` column). Fix:
```sql
ALTER TABLE your_catalog.watchdog.violations ADD COLUMNS (metastore_id STRING);
-- Repeat for affected tables
```

The engine uses `.option("mergeSchema", "true")` on writes, but existing tables created before the column was added need the ALTER TABLE first.

### Policies table empty / remediation shows null

The ad-hoc scan syncs policies automatically. If the policies table is empty, the scan hasn't run yet or failed before the sync step. Run a fresh scan.

### FMAPI endpoints showing as ungoverned

Deploy the latest engine code which auto-tags `databricks-*` endpoints as `ManagedModelEndpoint`. Re-run the scan.

### Serverless config errors

```
[CONFIG_NOT_AVAILABLE] Configuration spark.databricks.delta.schema.autoMerge.enabled
```

Serverless doesn't support this config. The engine uses `.option("mergeSchema", "true")` on DataFrame writes instead. Ensure you're running the latest code.

### Agent views not rendering in dashboard

The agent views (`v_agent_inventory`, etc.) are created by `ensure_semantic_views()` which runs at the end of the scan. If the scan failed mid-way, the views won't exist. Create them manually:

```sql
-- Run the view creation SQL from engine/src/watchdog/views.py
-- Or re-run a successful scan
```

---

## Production Checklist

- [ ] Daily scan job schedule enabled (default: paused)
- [ ] SQL warehouse is Pro (not Starter) for concurrent queries
- [ ] Notification channel configured (ACS or webhook)
- [ ] Industry policy pack deployed (if applicable)
- [ ] Genie Space instructions reviewed for customer context
- [ ] Dashboard published with embed_credentials=False
- [ ] Guardrails MCP URL shared with AI development team
- [ ] Agent governance policies reviewed for customer thresholds
