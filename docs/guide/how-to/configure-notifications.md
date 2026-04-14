# Configure Notifications

This guide covers setting up Watchdog's dual-path notification system to alert resource owners about governance violations.

## Notification Model

Watchdog uses an **owner digest** model. Rather than sending one notification per violation, it aggregates all open violations for each resource owner into a single digest. Each digest includes violation counts by severity, affected resources, and remediation guidance.

### Dual-Path Architecture

Notifications follow two paths:

| Path | Mechanism | Always Active | Purpose |
|------|-----------|---------------|---------|
| **Path 1** | Delta `notification_queue` table | Yes | Durable handoff for enterprise email systems |
| **Path 2** | Azure Communication Services (ACS) email | When configured | Direct email to owners |

Path 1 always runs. Enterprise email systems (or custom automation) consume the `notification_queue` table via Change Data Feed (CDF) or scheduled queries. Path 2 is a convenience layer for platform admins who want immediate email delivery.

## Path 1: Delta Queue (Always Active)

The `notification_queue` table receives one row per owner per notification batch. No configuration is required beyond running the notify entrypoint.

### Table Schema

| Column | Type | Description |
|--------|------|-------------|
| `notification_id` | STRING | Unique notification identifier (UUID) |
| `owner` | STRING | Resource owner (typically an email address) |
| `severity_summary` | STRING | Human-readable summary (e.g., "2 critical, 5 high") |
| `violation_count` | INT | Total violations in this digest |
| `critical_count` | INT | Count of critical violations |
| `high_count` | INT | Count of high violations |
| `medium_count` | INT | Count of medium violations |
| `low_count` | INT | Count of low violations |
| `violation_ids` | STRING | Comma-separated violation UUIDs |
| `dashboard_url` | STRING | Deep link to the compliance dashboard |
| `metastore_id` | STRING | Metastore scope (for multi-metastore deployments) |
| `created_at` | TIMESTAMP | When the notification was queued |
| `delivered_at` | TIMESTAMP | When delivery was confirmed (null if pending) |
| `delivery_channel` | STRING | How it was delivered (email, webhook, etc.) |
| `status` | STRING | pending, delivered, failed |

### Consuming the Queue

Enterprise email systems read from the queue using CDF:

```sql
-- Stream new notifications
SELECT *
FROM table_changes('platform.watchdog.notification_queue', 1)
WHERE _change_type = 'insert'
  AND status = 'pending'
```

Or with a scheduled query:

```sql
SELECT *
FROM platform.watchdog.notification_queue
WHERE status = 'pending'
ORDER BY created_at
```

## Path 2: Azure Communication Services (ACS)

### Prerequisites

1. An Azure Communication Services resource with email capability.
2. A verified sender domain in ACS.
3. The ACS connection string and sender address stored in a Databricks secret scope.

### Setup

Store the ACS credentials in the Watchdog secret scope:

```bash
# Store connection string
databricks secrets put-secret watchdog acs_connection_string \
  --string-value "endpoint=https://my-acs.communication.azure.com/;accesskey=..."

# Store sender address
databricks secrets put-secret watchdog acs_sender_address \
  --string-value "DoNotReply@notifications.example.com"
```

The Python package `azure-communication-email` must be installed in the cluster running the notify task:

```bash
pip install azure-communication-email
```

### Email Format

Each owner receives one email per notification batch. The email includes:

- Subject: `Watchdog: 7 governance violations (2 critical, 5 high)`
- Body: Violations listed by severity, each with resource name, policy ID, detail, and remediation steps.
- Dashboard link (if `--dashboard-url` is provided).

## Running Notifications

### As a Workflow Task

The `watchdog-notify` entrypoint runs as a Databricks Workflow task:

```bash
python -m watchdog.entrypoints notify \
  --catalog platform \
  --schema watchdog \
  --secret-scope watchdog \
  --dashboard-url "https://workspace.databricks.com/sql/dashboards/abc123"
```

### Entrypoint Arguments

| Argument | Required | Default | Description |
|----------|----------|---------|-------------|
| `--catalog` | Yes | | Unity Catalog name |
| `--schema` | Yes | | Schema containing Watchdog tables |
| `--secret-scope` | No | `watchdog` | Secret scope for ACS credentials |
| `--dashboard-url` | No | | Dashboard URL for deep links in notifications |

### Typical Workflow Configuration

Add the notify task after the evaluate task in the Watchdog job:

```yaml
tasks:
  - task_key: crawl
    python_wheel_task:
      entry_point: crawl
      parameters: ["--catalog", "platform", "--schema", "watchdog", "--secret-scope", "watchdog"]

  - task_key: evaluate
    depends_on: [crawl]
    python_wheel_task:
      entry_point: evaluate
      parameters: ["--catalog", "platform", "--schema", "watchdog", "--sync-policies"]

  - task_key: notify
    depends_on: [evaluate]
    python_wheel_task:
      entry_point: notify
      parameters: [
        "--catalog", "platform",
        "--schema", "watchdog",
        "--secret-scope", "watchdog",
        "--dashboard-url", "https://workspace.databricks.com/sql/dashboards/abc123"
      ]
```

## Notification Lifecycle

1. The evaluate step writes violations to the `violations` table with `notified_at = NULL`.
2. The notify step queries violations where `status = 'open'` and `notified_at IS NULL` and the owner is non-empty.
3. Violations are grouped by owner into digests.
4. Digests are written to the `notification_queue` table (Path 1).
5. If ACS is configured, emails are sent (Path 2).
6. Successfully notified violations are stamped with `notified_at = current_timestamp()` so they are not re-notified on subsequent runs.

Violations that remain open across multiple scans are only notified once. A violation must be resolved and then re-detected to trigger a second notification.
