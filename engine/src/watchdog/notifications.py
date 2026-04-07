"""Notification service — dual-path: Delta queue + Azure Communication Services.

Path 1 (always): Write un-notified violations to `notification_queue` table.
    Enterprise email systems consume this table via CDF or scheduled query.

Path 2 (optional): Send digest emails via Azure Communication Services (ACS).
    Enabled when ACS_CONNECTION_STRING is set in the secret scope.
    Sends one email per owner with their open violations grouped by severity.

The notify entrypoint runs both paths. Path 1 is the durable handoff —
Path 2 is convenience for platform admins who want immediate alerts.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import SparkSession


def ensure_notification_queue(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the notification_queue table if it doesn't exist.

    This table is the handoff point for the enterprise email pipeline.
    Each row is one notification event (one owner × one scan batch).
    CDF is enabled so downstream consumers can use streaming or incremental reads.
    """
    table = f"{catalog}.{schema}.notification_queue"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            notification_id STRING NOT NULL,
            owner STRING NOT NULL,
            severity_summary STRING,
            violation_count INT,
            critical_count INT,
            high_count INT,
            medium_count INT,
            low_count INT,
            violation_ids STRING,
            dashboard_url STRING,
            created_at TIMESTAMP NOT NULL,
            delivered_at TIMESTAMP,
            delivery_channel STRING,
            status STRING NOT NULL
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """)


@dataclass
class OwnerDigest:
    """Aggregated violation digest for one resource owner."""
    owner: str
    violations: list[dict]
    critical: int = 0
    high: int = 0
    medium: int = 0
    low: int = 0

    @property
    def total(self) -> int:
        return self.critical + self.high + self.medium + self.low

    @property
    def severity_summary(self) -> str:
        parts = []
        if self.critical:
            parts.append(f"{self.critical} critical")
        if self.high:
            parts.append(f"{self.high} high")
        if self.medium:
            parts.append(f"{self.medium} medium")
        if self.low:
            parts.append(f"{self.low} low")
        return ", ".join(parts) or "none"


def build_owner_digests(spark: SparkSession, catalog: str,
                        schema: str) -> list[OwnerDigest]:
    """Build per-owner violation digests from un-notified open violations."""
    violations_table = f"{catalog}.{schema}.violations"

    rows = spark.sql(f"""
        SELECT violation_id, resource_id, resource_name, resource_type,
               policy_id, severity, domain, detail, remediation, owner
        FROM {violations_table}
        WHERE status = 'open'
          AND notified_at IS NULL
          AND owner IS NOT NULL AND owner != ''
        ORDER BY owner,
            CASE severity
                WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                WHEN 'medium' THEN 3 WHEN 'low' THEN 4
            END
    """).collect()

    digests: dict[str, OwnerDigest] = {}
    for row in rows:
        owner = row.owner
        if owner not in digests:
            digests[owner] = OwnerDigest(owner=owner, violations=[])
        d = digests[owner]
        d.violations.append(row.asDict())
        if row.severity == "critical":
            d.critical += 1
        elif row.severity == "high":
            d.high += 1
        elif row.severity == "medium":
            d.medium += 1
        else:
            d.low += 1

    return list(digests.values())


def write_to_queue(spark: SparkSession, catalog: str, schema: str,
                   digests: list[OwnerDigest],
                   dashboard_url: str = "") -> int:
    """Write owner digests to the notification_queue table (Path 1).

    Returns number of queue entries written.
    """
    import uuid

    ensure_notification_queue(spark, catalog, schema)
    queue_table = f"{catalog}.{schema}.notification_queue"
    violations_table = f"{catalog}.{schema}.violations"
    now = datetime.now(timezone.utc)

    if not digests:
        return 0

    # Insert queue entries
    values = []
    all_violation_ids = []
    for d in digests:
        notification_id = str(uuid.uuid4())
        vids = [v["violation_id"] for v in d.violations]
        all_violation_ids.extend(vids)
        vid_str = ",".join(vids)
        values.append(
            f"('{notification_id}', '{d.owner}', '{d.severity_summary}', "
            f"{d.total}, {d.critical}, {d.high}, {d.medium}, {d.low}, "
            f"'{vid_str}', '{dashboard_url}', current_timestamp(), NULL, NULL, 'pending')"
        )

    spark.sql(f"""
        INSERT INTO {queue_table}
        (notification_id, owner, severity_summary, violation_count,
         critical_count, high_count, medium_count, low_count,
         violation_ids, dashboard_url, created_at, delivered_at,
         delivery_channel, status)
        VALUES {', '.join(values)}
    """)

    # Mark violations as notified
    if all_violation_ids:
        vid_list = ", ".join(f"'{v}'" for v in all_violation_ids)
        spark.sql(f"""
            UPDATE {violations_table}
            SET notified_at = current_timestamp()
            WHERE violation_id IN ({vid_list})
        """)

    return len(digests)


def send_acs_emails(digests: list[OwnerDigest], acs_connection_string: str,
                    sender_address: str, dashboard_url: str = "") -> int:
    """Send digest emails via Azure Communication Services (Path 2).

    Each owner gets one email with their violations grouped by severity.
    Returns the number of emails sent successfully.
    """
    try:
        from azure.communication.email import EmailClient
    except ImportError:
        print("WARNING: azure-communication-email not installed. Skipping ACS emails.")
        print("  Install with: pip install azure-communication-email")
        return 0

    client = EmailClient.from_connection_string(acs_connection_string)
    sent = 0

    for d in digests:
        subject = f"Watchdog: {d.total} governance violation{'s' if d.total != 1 else ''} ({d.severity_summary})"

        # Build plain text body
        lines = [
            f"Governance Violations for {d.owner}",
            f"{'=' * 50}",
            f"",
            f"Total: {d.total} open violations",
            f"  Critical: {d.critical}  |  High: {d.high}  |  Medium: {d.medium}  |  Low: {d.low}",
            f"",
        ]

        for v in d.violations:
            lines.append(f"[{v['severity'].upper()}] {v['resource_name']} — {v['policy_id']}")
            lines.append(f"  {v['detail']}")
            if v.get("remediation"):
                lines.append(f"  Fix: {v['remediation']}")
            lines.append("")

        if dashboard_url:
            lines.append(f"View in dashboard: {dashboard_url}")

        body = "\n".join(lines)

        message = {
            "senderAddress": sender_address,
            "recipients": {
                "to": [{"address": d.owner}],
            },
            "content": {
                "subject": subject,
                "plainText": body,
            },
        }

        try:
            poller = client.begin_send(message)
            poller.result()
            sent += 1
        except Exception as e:
            print(f"WARNING: Failed to send email to {d.owner}: {e}")

    return sent
