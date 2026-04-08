# Watchdog Governance Genie Space

A pre-built Genie Space that lets business users explore governance data in
natural language. Combines **Watchdog compliance posture** (violations, ontology
classifications, policies) with **UC Governance Hub data** (system tables for
access, tags, metadata, and audit logs).

## What it provides

**19 data sources** across three layers:

| Layer | Tables | Purpose |
|-------|--------|---------|
| Watchdog base tables (6) | violations, resource_inventory, resource_classifications, policies, exceptions, scan_results | Compliance posture and policy evaluation |
| Watchdog views (6) | v_domain_compliance, v_class_compliance, v_resource_compliance, v_tag_policy_coverage, v_data_classification_summary, v_dq_monitoring_coverage | Pre-aggregated compliance metrics |
| UC system tables (7) | information_schema.tables/columns/table_privileges/schema_privileges/table_tags/column_tags, access.audit | Native UC metadata, access, and audit |

**Six curated SQL datasets** for common governance questions:

| Dataset | Purpose |
|---------|---------|
| `compliance_overview` | Executive posture by governance domain |
| `violations_by_owner` | Open violations grouped by resource owner |
| `resource_compliance` | Per-resource compliance with ontology class context |
| `classification_coverage` | Data classification and steward assignment rates |
| `policy_effectiveness` | Which policies generate the most violations |
| `dq_monitoring` | Data quality monitoring coverage (DQM/LHM) |

Curated instructions teach the model about both the Watchdog data model and
UC system tables so it can answer cross-cutting questions like "who has access
to tables with critical violations?"

## Prerequisites

- Watchdog scanner has run at least once (tables exist in `<catalog>.<schema>`)
- A SQL warehouse accessible to Genie Space users
- `databricks-sdk` installed (`pip install databricks-sdk`)
- Databricks CLI authentication configured (profile or environment variables)

## Deployment

### Dry run (inspect configuration without creating)

```bash
python deploy_genie_space.py \
  --catalog platform \
  --schema watchdog \
  --dry-run
```

### Create a new Genie Space

```bash
python deploy_genie_space.py \
  --catalog platform \
  --schema watchdog \
  --warehouse-id <WAREHOUSE_ID> \
  --profile prod
```

### Update an existing Genie Space

```bash
python deploy_genie_space.py \
  --catalog platform \
  --schema watchdog \
  --warehouse-id <WAREHOUSE_ID> \
  --update <SPACE_ID> \
  --profile prod
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--catalog` | `platform` | Unity Catalog catalog name |
| `--schema` | `watchdog` | Schema containing Watchdog tables |
| `--profile` | (env default) | Databricks CLI profile |
| `--warehouse-id` | (none) | SQL warehouse ID for queries |
| `--space-name` | `Watchdog Governance Explorer` | Display name |
| `--update SPACE_ID` | (none) | Update instead of create |
| `--dry-run` | off | Print config without deploying |

## Customizing the SQL templates

Each `.sql` file in this directory becomes a dataset in the Genie Space.
The files use `${catalog}` and `${schema}` placeholders that get replaced
at deploy time.

To add a new dataset:

1. Create a new `.sql` file in this directory
2. Start with a `-- Title` comment on line 1 and `-- Description` on line 2
3. Use `${catalog}.${schema}.table_name` for table references
4. Re-run the deploy script

To modify instructions, edit `instructions.md` and redeploy.

## Example questions

Once deployed, users can ask questions like:

**Compliance posture (Watchdog)**
- "What is our overall compliance posture by domain?"
- "Who has the most critical open violations?"
- "Which PII tables don't have a data steward?"
- "What percentage of tables have data quality monitoring?"

**Access and security (UC system tables)**
- "Who has access to tables with critical violations?"
- "Which tables have ALL PRIVILEGES grants?"
- "Show me tables tagged PII that are accessible to more than 5 groups"

**Cross-cutting (Watchdog + UC)**
- "Compare Watchdog ontology classifications to native UC table tags"
- "Which violating resources were queried most in the last 30 days?"
- "Show me undocumented tables in production that have no data classification"
