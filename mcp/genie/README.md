# Watchdog Governance Genie Space

A pre-built Genie Space template that lets business users explore governance
compliance data in natural language. Powered by the Delta tables written by the
Watchdog scanner in `platform.watchdog`.

## What it provides

Six curated datasets that cover the most common governance questions:

| Dataset | Purpose |
|---------|---------|
| `compliance_overview` | Executive posture by governance domain |
| `violations_by_owner` | Open violations grouped by resource owner |
| `resource_compliance` | Per-resource compliance with ontology class context |
| `classification_coverage` | Data classification and steward assignment rates |
| `policy_effectiveness` | Which policies generate the most violations |
| `dq_monitoring` | Data quality monitoring coverage (DQM/LHM) |

The Genie Space also ships with curated instructions that teach the model about
the Watchdog data model, ontology hierarchy, severity levels, and compliance
domains so it can answer follow-up questions accurately.

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

- "What is our overall compliance posture?"
- "Who has the most critical open violations?"
- "Which PII tables don't have a data steward?"
- "Show me all critical violations for gold tables"
- "What percentage of tables have data quality monitoring?"
- "Which policies are generating the most violations?"
- "Are all production jobs compliant with operational governance?"
- "Show me the trend of resolved violations by domain"
