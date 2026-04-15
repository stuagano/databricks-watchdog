# Quickstart

Deploy Watchdog and run a first scan in 30 minutes.

## Step 1: Clone the Repository

```bash
git clone <repository-url> databricks-watchdog
cd databricks-watchdog
```

## Step 2: Configure Authentication

Ensure the Databricks CLI is authenticated to the target workspace:

```bash
# Option A: Configure a profile
databricks configure --profile my-workspace

# Option B: Use environment variables
export DATABRICKS_HOST=https://<workspace-url>
export DATABRICKS_TOKEN=<personal-access-token>

# Verify connectivity
databricks workspace list /
```

## Step 3: Create the Target Catalog and Schema

Watchdog needs a catalog and schema to store its tables and views. Create them if they do not already exist:

```sql
CREATE CATALOG IF NOT EXISTS my_catalog;
CREATE SCHEMA IF NOT EXISTS my_catalog.watchdog;
```

Run this via the Databricks SQL editor, a notebook, or the CLI:

```bash
databricks sql execute --statement "CREATE CATALOG IF NOT EXISTS my_catalog"
databricks sql execute --statement "CREATE SCHEMA IF NOT EXISTS my_catalog.watchdog"
```

## Step 4: Create a Service Principal (Recommended)

For production deployments, create a service principal rather than using a personal token:

1. Create the service principal in the Databricks account console.
2. Grant it access to the target workspace.
3. Grant Unity Catalog permissions:

```sql
GRANT USE CATALOG ON CATALOG my_catalog TO `watchdog-sp`;
GRANT USE SCHEMA ON SCHEMA my_catalog.watchdog TO `watchdog-sp`;
GRANT CREATE TABLE ON SCHEMA my_catalog.watchdog TO `watchdog-sp`;
GRANT CREATE VIEW ON SCHEMA my_catalog.watchdog TO `watchdog-sp`;
```

For the quickstart, a personal access token works fine. Switch to a service principal before scheduling daily scans.

## Step 5: Create a Secret Scope

The engine uses a secret scope for credentials:

```bash
databricks secrets create-scope watchdog
```

For ACS email notifications (optional), add the connection string and sender:

```bash
databricks secrets put-secret watchdog acs_connection_string --string-value "<connection-string>"
databricks secrets put-secret watchdog acs_sender_address --string-value "<sender@example.com>"
```

## Step 6: Deploy with DABs

Deploy the Watchdog bundle to the workspace:

```bash
databricks bundle deploy --profile my-workspace
```

This creates:

- A Databricks Workflow job with task-per-stage orchestration (crawl, evaluate, notify)
- The Python wheel package installed on the job cluster
- Ontology and policy YAML files deployed to the workspace

## Step 7: Run the Crawl

Trigger the crawl stage to inventory all workspace resources:

```bash
databricks bundle run watchdog-scan --profile my-workspace \
  --params catalog=my_catalog,schema=watchdog,secret_scope=watchdog
```

Or run the crawl entrypoint directly from a notebook:

```python
from watchdog.crawler import ResourceCrawler
from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
w = WorkspaceClient()

crawler = ResourceCrawler(spark, w, "my_catalog", "watchdog")
results = crawler.crawl_all()

for r in results:
    print(f"  {r.resource_type}: {r.count} resources")
```

Expected output:

```
  catalog: 5 resources (OK)
  schema: 23 resources (OK)
  table: 847 resources (OK)
  volume: 12 resources (OK)
  grant: 2,134 resources (OK)
  group: 18 resources (OK)
  service_principal: 4 resources (OK)
  job: 45 resources (OK)
  cluster: 8 resources (OK)
  warehouse: 3 resources (OK)
  agent: 38 resources (OK)
  agent_execution: 500 resources (OK)
```

## Step 8: Run the Evaluate

Evaluate policies against the crawled inventory:

```bash
databricks bundle run watchdog-evaluate --profile my-workspace \
  --params catalog=my_catalog,schema=watchdog,sync_policies=true
```

Or from a notebook:

```python
from watchdog.policy_loader import sync_policies_to_delta, load_yaml_policies, load_delta_policies
from watchdog.ontology import OntologyEngine
from watchdog.rule_engine import RuleEngine
from watchdog.policy_engine import PolicyEngine
from watchdog.views import ensure_semantic_views

# Sync YAML policies to Delta
count = sync_policies_to_delta(spark, "my_catalog", "watchdog")
print(f"Synced {count} policies")

# Build and run the engine
ontology = OntologyEngine()
rule_engine = RuleEngine()
policies = load_yaml_policies() + load_delta_policies(spark, "my_catalog", "watchdog")

engine = PolicyEngine(
    spark, w, "my_catalog", "watchdog",
    ontology=ontology,
    rule_engine=rule_engine,
    policies=policies,
)
results = engine.evaluate_all()

print(f"Classified: {results.classes_assigned} assignments")
print(f"Evaluated: {results.policies_run} policy checks")
print(f"Violations: {results.new_violations} new, {results.resolved} resolved")

# Create compliance views
ensure_semantic_views(spark, "my_catalog", "watchdog")
```

## Step 9: Check Results

Query the violations table to see what Watchdog found:

```sql
-- Open violations by severity
SELECT severity, COUNT(*) AS cnt
FROM my_catalog.watchdog.violations
WHERE status = 'open'
GROUP BY severity
ORDER BY
  CASE severity
    WHEN 'critical' THEN 1
    WHEN 'high' THEN 2
    WHEN 'medium' THEN 3
    WHEN 'low' THEN 4
  END;

-- Domain compliance summary
SELECT * FROM my_catalog.watchdog.v_domain_compliance;

-- Top 10 owners by violation count
SELECT owner, COUNT(*) AS total,
       COUNT(CASE WHEN severity = 'critical' THEN 1 END) AS critical
FROM my_catalog.watchdog.violations
WHERE status = 'open'
GROUP BY owner
ORDER BY total DESC
LIMIT 10;
```

## Next Steps

- **Set up a dashboard** -- see [First Dashboard](first-dashboard.md) for Lakeview, Genie, and custom SQL options.
- **Schedule daily scans** -- configure the Databricks Workflow to run the crawl-evaluate-notify pipeline on a cron schedule.
- **Customize policies** -- add YAML policies in `engine/policies/` or create user policies directly in the Delta table.
- **Add industry packs** -- copy policies from `library/healthcare/`, `library/financial/`, or `library/defense/` into the engine's policies directory.
- **Enable notifications** -- configure ACS secrets to send per-owner violation digests.
- **Deploy MCP servers** -- set up the Watchdog MCP and Guardrails MCP for AI assistant and agent access.
