# Watchdog Governance Posture — Lakeview Dashboard

A 5-page Lakeview dashboard that surfaces Watchdog compliance posture
data in the Databricks workspace. Designed to be imported alongside
the native Governance Hub dashboards.

## Pages

| Page | What it shows |
|------|---------------|
| **Compliance Overview** | Cross-domain posture: open violations, critical count, resources scanned. Bar charts by domain and severity. Top 10 policies. |
| **Owner Accountability** | Top 15 owners by violation count. Detail table with critical/high/medium/low breakdown per owner. |
| **Resource Compliance** | Resources with most violations, ontology class distribution, classification coverage. |
| **Access Governance** | Direct user grants (POL-A002), overprivileged grants (POL-A001), actionable remediation list. |
| **Data Quality & Monitoring** | DQM/LHM coverage pie chart, unmonitored tables list. |

## Deploy

```bash
python deploy_dashboard.py \
  --profile <profile> \
  --catalog <catalog> \
  --schema watchdog \
  --warehouse-id <warehouse_id> \
  --publish
```

## Customize

Edit `watchdog_governance_posture.json` directly or modify
dataset SQL queries to add filters, change limits, or add new pages.

The template uses `serverless_stable_s0v155_catalog.watchdog` as the
default catalog.schema — the deploy script replaces this with your target.
