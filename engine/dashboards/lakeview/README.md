# Watchdog Lakeview Dashboards

Two dashboard templates for different integration levels with the UC Governance Hub.

## 1. Watchdog Governance Posture (standalone)

`watchdog_governance_posture.json` — 5 pages, Watchdog data only.

| Page | What it shows |
|------|---------------|
| **Compliance Overview** | Cross-domain posture, violation counters, severity by domain, top policies |
| **Owner Accountability** | Top 15 owners, critical/high/medium/low breakdown |
| **Resource Compliance** | Ontology class distribution, resources with most violations |
| **Access Governance** | Direct user grants, overprivileged grants, remediation list |
| **Data Quality** | DQM/LHM coverage, unmonitored tables |

## 2. UC Governance Hub + Watchdog (unified)

`uc_governance_hub_unified.json` — 5 pages, combines **UC system tables** (information_schema, table_tags, table_privileges) with **Watchdog compliance** in a single dashboard. This is what goes alongside the Governance Hub.

| Page | UC System Tables | Watchdog Data | Cross-Join |
|------|-----------------|---------------|------------|
| **Governance Overview** | Asset counts by type | Violation counters + domain breakdown | Side-by-side |
| **Metadata & Tags** | Tag usage, undocumented tables | Ontology classification | Untagged tables WITH violations |
| **Access & Security** | Privilege distribution | Access governance violations, direct grants | Who has access to violated resources |
| **Owner Accountability** | — | Per-owner violation detail | — |
| **Data Quality** | Tables by catalog | DQ monitoring coverage, top policies | — |

The unified dashboard answers questions neither source can answer alone:
- "Which undocumented tables also have open violations?" (UC tags + Watchdog violations)
- "Who has access to resources with critical violations?" (UC privileges + Watchdog severity)
- "How does native tag coverage compare to Watchdog ontology classification?" (side-by-side)

## Deploy

```bash
# Standalone Watchdog dashboard
python deploy_dashboard.py \
  --profile <profile> --catalog <catalog> --schema watchdog \
  --warehouse-id <id> --publish

# Unified Hub + Watchdog dashboard
python deploy_dashboard.py \
  --profile <profile> --catalog <catalog> --schema watchdog \
  --warehouse-id <id> --publish \
  --template uc_governance_hub_unified.json \
  --name "UC Governance Hub + Watchdog Compliance"
```

## Integrating with the Governance Hub UI

1. Open Workspace Settings → Data Governance (the Hub entry point)
2. The Hub shows its native dashboards (Overview, Usage & Impact, Metadata & Access)
3. Place the unified Watchdog dashboard in the same workspace folder
4. Users see both native Hub dashboards and the Watchdog compliance dashboard
5. Optionally: clone a Hub dashboard and add Watchdog widgets directly into it

## Customize

Edit the JSON templates directly. Dataset SQL queries use fully-qualified table names. The deploy script replaces `serverless_stable_s0v155_catalog.watchdog` with your target catalog.schema.
