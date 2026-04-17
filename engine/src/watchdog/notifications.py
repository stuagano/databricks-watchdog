"""Notification service — dual-path: Delta queue + outbound delivery channels.

Path 1 (always): Write un-notified violations to `notification_queue` table.
    Enterprise email systems consume this table via CDF or scheduled query.

Path 2 (optional): Deliver digests via a configurable outbound channel:
    - Azure Communication Services email (``send_acs_emails``)
    - Generic HTTPS webhook — Slack, Teams, or any JSON consumer
      (``send_webhook_notifications``)

The notify entrypoint runs Path 1 always and dispatches Path 2 based on what
secrets are configured. Path 1 is the durable handoff; Path 2 is convenience
for platform admins who want immediate alerts.
"""

import json
from dataclasses import dataclass

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
            metastore_id STRING,
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
            "",
            f"Total: {d.total} open violations",
            f"  Critical: {d.critical}  |  High: {d.high}  |  Medium: {d.medium}  |  Low: {d.low}",
            "",
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


def build_webhook_payload(digest: "OwnerDigest", dashboard_url: str = "",
                          flavor: str = "generic") -> dict:
    """Build a JSON webhook payload for one owner's digest.

    Three payload flavors are supported so the same endpoint can be used for
    Slack incoming webhooks, Microsoft Teams office-connector webhooks, or a
    generic downstream consumer that reads the full digest verbatim:

      - ``generic``: flat JSON with the full violation list (default)
      - ``slack``:   Slack-compatible ``blocks`` layout
      - ``teams``:   Teams MessageCard (legacy connector)

    Callers that need a custom schema can build their own payload from the
    OwnerDigest and POST it directly — nothing in this module hides the
    violations list from them.
    """
    violations_summary = [
        {
            "policy_id": v.get("policy_id", ""),
            "severity": v.get("severity", ""),
            "resource": v.get("resource_name", ""),
            "detail": v.get("detail", ""),
            "remediation": v.get("remediation", ""),
        }
        for v in digest.violations
    ]
    header = (
        f"Watchdog: {digest.total} governance violation"
        f"{'s' if digest.total != 1 else ''} for {digest.owner} "
        f"({digest.severity_summary})"
    )

    if flavor == "slack":
        blocks: list[dict] = [
            {"type": "header",
             "text": {"type": "plain_text", "text": header}},
        ]
        # Slack limits blocks to 50; cap violations shown inline.
        for v in violations_summary[:10]:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*[{v['severity'].upper()}]* `{v['resource']}` — "
                        f"{v['policy_id']}\n{v['detail']}"
                        + (f"\n_Fix:_ {v['remediation']}" if v["remediation"] else "")
                    ),
                },
            })
        if len(violations_summary) > 10:
            blocks.append({
                "type": "context",
                "elements": [{"type": "mrkdwn",
                              "text": f"_…and {len(violations_summary) - 10} more._"}],
            })
        if dashboard_url:
            blocks.append({
                "type": "actions",
                "elements": [{
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Open dashboard"},
                    "url": dashboard_url,
                }],
            })
        return {"text": header, "blocks": blocks}

    if flavor == "teams":
        facts = [
            {"name": "Critical", "value": str(digest.critical)},
            {"name": "High", "value": str(digest.high)},
            {"name": "Medium", "value": str(digest.medium)},
            {"name": "Low", "value": str(digest.low)},
        ]
        card = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "summary": header,
            "title": header,
            "themeColor": "B00020" if digest.critical else "F2994A",
            "sections": [{"facts": facts, "markdown": True}],
        }
        if dashboard_url:
            card["potentialAction"] = [{
                "@type": "OpenUri",
                "name": "Open dashboard",
                "targets": [{"os": "default", "uri": dashboard_url}],
            }]
        return card

    # generic
    return {
        "owner": digest.owner,
        "total": digest.total,
        "critical": digest.critical,
        "high": digest.high,
        "medium": digest.medium,
        "low": digest.low,
        "severity_summary": digest.severity_summary,
        "dashboard_url": dashboard_url,
        "violations": violations_summary,
    }


def send_webhook_notifications(digests: list[OwnerDigest], webhook_url: str,
                                dashboard_url: str = "",
                                flavor: str = "generic",
                                timeout_seconds: float = 10.0) -> int:
    """POST a per-owner JSON digest to an HTTPS webhook (Path 2 alternative).

    Uses stdlib ``urllib.request`` so there is no extra runtime dependency on
    Databricks clusters. Returns the count of webhooks that returned 2xx.

    ``flavor`` selects the payload shape — see ``build_webhook_payload``.

    This function never raises; it logs and continues so one bad digest cannot
    block the entire run. The caller is expected to inspect the return value
    to compare against ``len(digests)`` for partial-success alerting.
    """
    import urllib.error
    import urllib.request

    if not webhook_url:
        return 0
    if not webhook_url.lower().startswith(("http://", "https://")):
        print(f"WARNING: webhook_url must be http(s): got {webhook_url!r}")
        return 0

    sent = 0
    for d in digests:
        payload = build_webhook_payload(d, dashboard_url=dashboard_url, flavor=flavor)
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                status = resp.status
                if 200 <= status < 300:
                    sent += 1
                else:
                    print(f"WARNING: webhook {d.owner} returned HTTP {status}")
        except urllib.error.HTTPError as e:
            print(f"WARNING: webhook {d.owner} HTTP error: {e.code} {e.reason}")
        except Exception as e:
            print(f"WARNING: webhook {d.owner} failed: {e}")

    return sent
