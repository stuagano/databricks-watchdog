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
    from watchdog.rule_engine import RuleEngine
    from watchdog.policy_engine import PolicyEngine
    from watchdog.policy_loader import load_yaml_policies, load_delta_policies

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
    print("Refreshed semantic views: v_resource_compliance, v_class_compliance, v_domain_compliance, "
          "v_tag_policy_coverage, v_data_classification_summary, v_dq_monitoring_coverage")


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

    from watchdog.notifications import build_owner_digests, write_to_queue, send_acs_emails

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

    # Path 2: Send ACS emails if configured
    try:
        acs_conn = w.dbutils.secrets.get(args.secret_scope, "acs_connection_string")
        acs_sender = w.dbutils.secrets.get(args.secret_scope, "acs_sender_address")
    except Exception:
        acs_conn = None
        acs_sender = None

    if acs_conn and acs_sender:
        sent = send_acs_emails(digests, acs_conn, acs_sender,
                               dashboard_url=args.dashboard_url)
        print(f"Path 2 (ACS email): {sent}/{len(digests)} emails sent")
    else:
        print("Path 2 (ACS email): skipped — acs_connection_string not in secret scope")


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
    w = WorkspaceClient()

    from watchdog.crawler import ResourceCrawler
    crawler = ResourceCrawler(spark, w, args.catalog, args.schema)

    if args.resource_type == "all":
        results = crawler.crawl_all()
    else:
        print(f"Ad-hoc scan for {args.resource_type}/{args.resource_id}")
        results = crawler.crawl_all()  # TODO: filter by type/id

    for r in results:
        print(f"  {r.resource_type}: {r.count} resources")

    # Run ontology-aware evaluation
    engine = _build_engine(spark, w, args.catalog, args.schema)
    eval_results = engine.evaluate_all()
    print(f"Ontology: {eval_results.classes_assigned} classifications")
    print(f"Violations: {eval_results.new_violations} new, {eval_results.resolved} resolved")

    from watchdog.views import ensure_semantic_views
    ensure_semantic_views(spark, args.catalog, args.schema)
    print("Refreshed semantic views")
