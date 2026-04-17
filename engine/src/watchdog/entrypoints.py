"""CLI entrypoints for Databricks Workflow tasks.

Each function corresponds to a task_key in watchdog_job.yml.
The evaluate and adhoc entrypoints now use the ontology-aware pipeline:
  1. Load ontology (resource classes, rule primitives)
  2. Load policies from domain-scoped YAML files
  3. Classify resources, evaluate rules, merge violations
"""

import argparse

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession


def _build_engine(spark: SparkSession, w: WorkspaceClient,
                  catalog: str, schema: str):
    """Build a PolicyEngine with all policies loaded.

    Detects whether ontology files are present and logs the operating mode:
      - Full mode: ontology classification + declarative rule engine
      - MVP mode: resource_type fallback + rule engine (no classification)

    Both modes evaluate the same policies — MVP mode uses a static
    class → resource_type map instead of tag-based classification.
    """
    import os
    from pathlib import Path

    from watchdog.ontology import OntologyEngine
    from watchdog.policy_engine import PolicyEngine
    from watchdog.policy_loader import load_delta_policies, load_yaml_policies
    from watchdog.rule_engine import RuleEngine

    # Detect ontology presence — handle serverless where __file__ may not exist
    try:
        ontology_dir = Path(__file__).parent.parent.parent / "ontologies"
    except NameError:
        # Serverless: look relative to CWD (bundle root)
        ontology_dir = Path(os.getcwd()) / "ontologies"
    has_ontology = (ontology_dir / "resource_classes.yml").exists()
    has_primitives = (ontology_dir / "rule_primitives.yml").exists()

    ontology = OntologyEngine()
    rule_engine = RuleEngine()
    yaml_policies = load_yaml_policies()
    user_policies = load_delta_policies(spark, catalog, schema)
    policies = yaml_policies + user_policies

    if has_ontology and has_primitives:
        print(f"Watchdog: full mode — ontology ({len(ontology.classes)} classes), "
              f"rule engine ({len(rule_engine.primitives)} primitives), "
              f"{len(policies)} policies ({len(yaml_policies)} YAML + {len(user_policies)} user)")
    else:
        missing = []
        if not has_ontology:
            missing.append("resource_classes.yml")
        if not has_primitives:
            missing.append("rule_primitives.yml")
        print(f"Watchdog: MVP mode — missing {', '.join(missing)}. "
              f"Using resource_type fallback. "
              f"{len(policies)} policies ({len(yaml_policies)} YAML + {len(user_policies)} user)")

    return PolicyEngine(
        spark, w, catalog, schema,
        ontology=ontology,
        rule_engine=rule_engine,
        policies=policies,
    )


def crawl():
    """Entrypoint: crawl all workspace resources and write to resource_inventory."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--secret-scope", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    # Schema evolution handled via .option("mergeSchema", "true") on writes
    w = WorkspaceClient()

    from watchdog.crawler import ResourceCrawler
    crawler = ResourceCrawler(spark, w, args.catalog, args.schema)
    results = crawler.crawl_all()

    for r in results:
        status = "OK" if not r.errors else f"ERROR: {r.errors}"
        print(f"  {r.resource_type}: {r.count} resources ({status})")


def evaluate():
    """Entrypoint: evaluate all ontology-aware policies against the latest inventory."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--sync-policies", action="store_true",
                        help="Sync YAML policy definitions to Delta before evaluating")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    # Schema evolution handled via .option("mergeSchema", "true") on writes
    w = WorkspaceClient()

    # Sync YAML policies to Delta table (idempotent)
    if args.sync_policies:
        from watchdog.policy_loader import sync_policies_to_delta
        count = sync_policies_to_delta(spark, args.catalog, args.schema)
        print(f"Synced {count} policies from YAML to Delta")

    engine = _build_engine(spark, w, args.catalog, args.schema)
    results = engine.evaluate_all()

    print(f"Ontology: {results.classes_assigned} class assignments across {results.resources_checked} resources")
    print(f"Evaluated {results.policies_run} policies")
    print(f"  Violations: {results.new_violations} new, {results.resolved} resolved")

    from watchdog.views import ensure_semantic_views
    ensure_semantic_views(spark, args.catalog, args.schema)
    print("Refreshed compliance views: v_resource_compliance, v_class_compliance, v_domain_compliance, "
          "v_tag_policy_coverage, v_data_classification_summary, v_dq_monitoring_coverage, "
          "v_compliance_trend")


def notify():
    """Entrypoint: send violation notifications to resource owners.

    Dual-path notification:
      Path 1 (always): Write digests to notification_queue for enterprise email pipeline.
      Path 2 (if configured): Send emails via Azure Communication Services.

    ACS is enabled by setting acs_connection_string and acs_sender_address
    in the secret scope.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--secret-scope", default="watchdog")
    parser.add_argument("--dashboard-url", default="",
                        help="Dashboard URL for deep links in notifications")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    w = WorkspaceClient()

    from watchdog.notifications import (
        build_owner_digests,
        send_acs_emails,
        send_webhook_notifications,
        write_to_queue,
    )

    # Build per-owner digests from un-notified violations
    digests = build_owner_digests(spark, args.catalog, args.schema)
    if not digests:
        print("No un-notified violations. Nothing to send.")
        return

    total_violations = sum(d.total for d in digests)
    print(f"Built digests for {len(digests)} owners ({total_violations} violations)")

    # Path 1: Always write to notification_queue
    queued = write_to_queue(spark, args.catalog, args.schema, digests,
                            dashboard_url=args.dashboard_url)
    print(f"Path 1 (Delta queue): {queued} entries written to notification_queue")

    def _secret(key: str) -> str | None:
        try:
            return w.dbutils.secrets.get(args.secret_scope, key)
        except Exception:
            return None

    # Path 2a: Send ACS emails if configured
    acs_conn = _secret("acs_connection_string")
    acs_sender = _secret("acs_sender_address")
    if acs_conn and acs_sender:
        sent = send_acs_emails(digests, acs_conn, acs_sender,
                               dashboard_url=args.dashboard_url)
        print(f"Path 2 (ACS email): {sent}/{len(digests)} emails sent")
    else:
        print("Path 2 (ACS email): skipped — acs_connection_string not in secret scope")

    # Path 2b: POST to webhook (Slack/Teams/generic) if configured
    webhook_url = _secret("notification_webhook_url")
    webhook_flavor = _secret("notification_webhook_flavor") or "generic"
    if webhook_url:
        sent = send_webhook_notifications(
            digests, webhook_url,
            dashboard_url=args.dashboard_url, flavor=webhook_flavor,
        )
        print(f"Path 2 ({webhook_flavor} webhook): {sent}/{len(digests)} digests posted")
    else:
        print("Path 2 (webhook): skipped — notification_webhook_url not in secret scope")


def crawl_all_metastores():
    """Crawl resources across multiple metastores.

    Reads WATCHDOG_METASTORE_IDS env var (comma-separated) or --metastore-ids arg.
    For each metastore, runs crawl_all() with the metastore_id parameter.
    All results write to the same Delta tables with metastore_id discriminator.

    Falls back to single-metastore crawl() when no metastore IDs are configured.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--secret-scope", default="watchdog")
    parser.add_argument("--metastore-ids", default="",
                        help="Comma-separated metastore IDs (overrides WATCHDOG_METASTORE_IDS)")
    args = parser.parse_args()

    from watchdog.config import WatchdogConfig
    config = WatchdogConfig()

    # CLI override takes precedence over env var
    metastore_ids = [
        m.strip() for m in args.metastore_ids.split(",") if m.strip()
    ] if args.metastore_ids else config.metastore_ids

    if not metastore_ids:
        print("No metastore IDs configured. Falling back to single-metastore crawl.")
        crawl()
        return

    spark = SparkSession.builder.getOrCreate()
    w = WorkspaceClient()

    from watchdog.crawler import ResourceCrawler

    total_resources = 0
    for metastore_id in metastore_ids:
        print(f"Scanning metastore {metastore_id}...")
        crawler = ResourceCrawler(spark, w, args.catalog, args.schema,
                                  metastore_id=metastore_id)
        results = crawler.crawl_all()

        metastore_total = 0
        for r in results:
            status = "OK" if not r.errors else f"ERROR: {r.errors}"
            print(f"  {r.resource_type}: {r.count} resources ({status})")
            metastore_total += r.count

        print(f"  Metastore {metastore_id}: {metastore_total} resources")
        total_resources += metastore_total

    print(f"Scanned {len(metastore_ids)} metastores, {total_resources} resources")


def adhoc():
    """Entrypoint: ad-hoc scan for a specific resource or full workspace."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--resource-type", default="all")
    parser.add_argument("--resource-id", default="")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    # Enable schema auto-merge for evolving table schemas (e.g., new metastore_id column)
    # Schema evolution handled via .option("mergeSchema", "true") on writes
    w = WorkspaceClient()

    from watchdog.crawler import ResourceCrawler
    crawler = ResourceCrawler(spark, w, args.catalog, args.schema)

    # Map the user-facing --resource-type value to the set of primary types the
    # crawler understands. "all" means no filter. Aliases cover common confusion
    # (e.g. plural forms and "asset" umbrellas).
    type_aliases: dict[str, set[str]] = {
        "all": set(),
        "tables": {"table"},
        "jobs": {"job"},
        "clusters": {"cluster"},
        "warehouses": {"warehouse"},
        "pipelines": {"pipeline"},
        "grants": {"grant"},
        "agents": {"agent", "agent_trace"},
        "data": {"table", "volume", "schema", "catalog"},
        "compute": {"job", "cluster", "warehouse", "pipeline"},
    }
    requested = args.resource_type.strip().lower()
    resource_types = type_aliases.get(requested, {requested} if requested != "all" else set())

    resource_id = args.resource_id.strip() or None
    if resource_types or resource_id:
        print(f"Ad-hoc scan for type={args.resource_type!r} id={args.resource_id!r}")
    results = crawler.crawl_all(
        resource_types=resource_types or None,
        resource_id=resource_id,
    )

    for r in results:
        print(f"  {r.resource_type}: {r.count} resources")

    # Sync YAML policies to Delta so views can JOIN for names/remediation
    from watchdog.policy_loader import sync_policies_to_delta
    count = sync_policies_to_delta(spark, args.catalog, args.schema)
    print(f"Synced {count} policies from YAML to Delta")

    # Run ontology-aware evaluation
    engine = _build_engine(spark, w, args.catalog, args.schema)
    eval_results = engine.evaluate_all()
    print(f"Ontology: {eval_results.classes_assigned} classifications")
    print(f"Violations: {eval_results.new_violations} new, {eval_results.resolved} resolved")

    # Refresh compliance views
    from watchdog.views import ensure_semantic_views
    ensure_semantic_views(spark, args.catalog, args.schema)
    print("Refreshed compliance views")


# ─────────────────────────────────────────────────────────────────────────────
# Remediation pipeline entrypoints
# ─────────────────────────────────────────────────────────────────────────────


def _load_agents():
    """Return the registered remediation agents.

    Keep this list explicit rather than auto-discovering so that the set of
    agents running in production is reviewable in git history.
    """
    from watchdog.remediation.agents.cluster_tagger import ClusterTaggerAgent
    from watchdog.remediation.agents.dq_monitor_scaffold import DQMonitorScaffoldAgent
    from watchdog.remediation.agents.job_owner import JobOwnerAgent
    from watchdog.remediation.agents.steward import StewardAgent

    return [
        StewardAgent(),
        ClusterTaggerAgent(),
        DQMonitorScaffoldAgent(),
        JobOwnerAgent(),
    ]


def remediate():
    """Entrypoint: dispatch open violations to remediation agents.

    Reads violations with status='open' from the violations table, dispatches
    each to the first agent whose handles[] matches its policy_id, and writes
    new proposals to remediation_proposals in status 'pending_review'.

    Idempotent — a violation that already has a proposal from the same agent
    version is skipped.
    """
    import pyspark.sql.types as T

    from watchdog.remediation.dispatcher import dispatch_remediations
    from watchdog.remediation.tables import (
        ensure_remediation_agents_table,
        ensure_remediation_proposals_table,
        register_agent,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--limit", type=int, default=500,
                        help="Max open violations to consider in one run")
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    ensure_remediation_agents_table(spark, args.catalog, args.schema)
    ensure_remediation_proposals_table(spark, args.catalog, args.schema)

    agents = _load_agents()
    for agent in agents:
        register_agent(spark, args.catalog, args.schema, agent)

    violations_table = f"{args.catalog}.{args.schema}.violations"
    proposals_table = f"{args.catalog}.{args.schema}.remediation_proposals"

    rows = spark.sql(f"""
        SELECT violation_id, policy_id, resource_id, resource_name,
               resource_type, severity, owner, status
        FROM {violations_table}
        WHERE status = 'open'
        ORDER BY severity, violation_id
        LIMIT {int(args.limit)}
    """).collect()
    violations = [r.asDict() for r in rows]

    # Load existing proposal keys for idempotency
    existing = spark.sql(f"""
        SELECT violation_id, agent_id, agent_version FROM {proposals_table}
    """).collect()
    existing_keys = {(r.violation_id, r.agent_id, r.agent_version) for r in existing}

    result = dispatch_remediations(violations, agents, existing_keys)
    proposals = result["proposals"]

    if proposals:
        proposal_schema = T.StructType([
            T.StructField("proposal_id", T.StringType(), False),
            T.StructField("violation_id", T.StringType(), False),
            T.StructField("agent_id", T.StringType(), False),
            T.StructField("agent_version", T.StringType(), False),
            T.StructField("status", T.StringType(), False),
            T.StructField("proposed_sql", T.StringType(), True),
            T.StructField("confidence", T.DoubleType(), True),
            T.StructField("context_json", T.StringType(), True),
            T.StructField("llm_prompt_hash", T.StringType(), True),
            T.StructField("citations", T.StringType(), True),
            T.StructField("created_at", T.TimestampType(), False),
        ])
        rows_tuple = [
            (
                p["proposal_id"], p["violation_id"], p["agent_id"], p["agent_version"],
                p["status"], p.get("proposed_sql", ""), float(p.get("confidence", 0.0)),
                p.get("context_json", ""), p.get("llm_prompt_hash", ""),
                p.get("citations", ""), p["created_at"],
            )
            for p in proposals
        ]
        df = spark.createDataFrame(rows_tuple, schema=proposal_schema)
        df.write.mode("append").saveAsTable(proposals_table)

    print(f"Remediate: considered {len(violations)} violations — "
          f"dispatched {result['dispatched']} proposals, "
          f"skipped {result['skipped']}, errors {result['errors']}")

    from watchdog.remediation.views import ensure_remediation_views
    ensure_remediation_views(spark, args.catalog, args.schema)


def apply_approved_remediations():
    """Entrypoint: apply all approved proposals.

    Reads proposals with status='approved', executes the proposed SQL via
    WorkspaceClient (or Spark SQL for UC statements), and records each
    application in remediation_applied. Proposal status flips to 'applied'.

    Pass --dry-run to preview what would be applied without executing SQL.
    """
    import pyspark.sql.types as T

    from watchdog.remediation.applier import apply_proposal
    from watchdog.remediation.tables import ensure_remediation_applied_table

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    ensure_remediation_applied_table(spark, args.catalog, args.schema)
    proposals_table = f"{args.catalog}.{args.schema}.remediation_proposals"
    applied_table = f"{args.catalog}.{args.schema}.remediation_applied"

    rows = spark.sql(f"""
        SELECT proposal_id, violation_id, agent_id, agent_version, status,
               proposed_sql, confidence, context_json, llm_prompt_hash,
               citations, created_at
        FROM {proposals_table}
        WHERE status = 'approved'
        ORDER BY created_at
        LIMIT {int(args.limit)}
    """).collect()

    applied_results = []
    new_statuses: list[tuple[str, str]] = []  # (proposal_id, new_status)
    errors = 0
    for row in rows:
        proposal = row.asDict()
        try:
            updated, apply_result = apply_proposal(
                proposal, pre_state="", dry_run=args.dry_run,
            )
            if not args.dry_run and apply_result["executed_sql"]:
                try:
                    spark.sql(apply_result["executed_sql"])
                except Exception as e:
                    apply_result["verify_status"] = "verification_failed"
                    apply_result["post_state"] = f"exec_error:{e}"
                    errors += 1
            applied_results.append(apply_result)
            new_statuses.append((proposal["proposal_id"], updated["status"]))
        except ValueError as e:
            print(f"Skipping proposal {proposal.get('proposal_id')}: {e}")
            errors += 1

    if applied_results:
        applied_schema = T.StructType([
            T.StructField("apply_id", T.StringType(), False),
            T.StructField("proposal_id", T.StringType(), False),
            T.StructField("executed_sql", T.StringType(), True),
            T.StructField("pre_state", T.StringType(), True),
            T.StructField("post_state", T.StringType(), True),
            T.StructField("applied_at", T.TimestampType(), False),
            T.StructField("verify_scan_id", T.StringType(), True),
            T.StructField("verify_status", T.StringType(), False),
        ])
        rows_tuple = [
            (a["apply_id"], a["proposal_id"], a.get("executed_sql", ""),
             a.get("pre_state", ""), a.get("post_state", ""),
             a["applied_at"], a.get("verify_scan_id"), a["verify_status"])
            for a in applied_results
        ]
        df = spark.createDataFrame(rows_tuple, schema=applied_schema)
        df.write.mode("append").saveAsTable(applied_table)

    # Flip proposal statuses. In dry-run we never mutate the proposal row.
    if not args.dry_run and new_statuses:
        for proposal_id, new_status in new_statuses:
            spark.sql(f"""
                UPDATE {proposals_table}
                SET status = '{new_status}'
                WHERE proposal_id = '{proposal_id}'
            """)

    mode = "dry-run" if args.dry_run else "applied"
    print(f"Remediate-apply ({mode}): {len(applied_results)} proposals, "
          f"{errors} errors")


def verify_remediations():
    """Entrypoint: verify applied proposals against the latest scan.

    Reads remediation_applied rows with verify_status='pending' and checks
    whether the corresponding violations resolved in the most recent scan.
    Sets verify_status to 'verified' or 'verification_failed' and flips
    proposal status to 'verified' when the violation is gone.
    """
    from watchdog.remediation.verifier import batch_verify

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    applied_table = f"{args.catalog}.{args.schema}.remediation_applied"
    proposals_table = f"{args.catalog}.{args.schema}.remediation_proposals"
    violations_table = f"{args.catalog}.{args.schema}.violations"

    # Pending apply results waiting for the next scan oracle
    apply_rows = spark.sql(f"""
        SELECT apply_id, proposal_id, verify_status
        FROM {applied_table}
        WHERE verify_status = 'pending'
    """).collect()
    apply_results = [r.asDict() for r in apply_rows]
    if not apply_results:
        print("Verify: no pending applies.")
        return

    # Proposal → violation lookup
    prop_rows = spark.sql(f"""
        SELECT proposal_id, violation_id FROM {proposals_table}
        WHERE proposal_id IN ({
            ", ".join(f"'{r['proposal_id']}'" for r in apply_results)
        })
    """).collect()
    proposal_violations = {r.proposal_id: r.violation_id for r in prop_rows}

    violation_ids = list(set(proposal_violations.values()))
    if not violation_ids:
        resolved_ids: set[str] = set()
    else:
        id_list = ", ".join(f"'{v}'" for v in violation_ids)
        resolved_rows = spark.sql(f"""
            SELECT violation_id FROM {violations_table}
            WHERE violation_id IN ({id_list})
              AND status = 'resolved'
        """).collect()
        resolved_ids = {r.violation_id for r in resolved_rows}

    batch = batch_verify(apply_results, resolved_ids, proposal_violations)

    # Persist verify_status updates
    for updated in batch["results"]:
        spark.sql(f"""
            UPDATE {applied_table}
            SET verify_status = '{updated['verify_status']}'
            WHERE apply_id = '{updated['apply_id']}'
        """)
        if updated["verify_status"] == "verified":
            proposal_id = updated.get("proposal_id", "")
            spark.sql(f"""
                UPDATE {proposals_table}
                SET status = 'verified'
                WHERE proposal_id = '{proposal_id}'
            """)

    print(f"Verify: {batch['verified']} verified, {batch['failed']} failed")
