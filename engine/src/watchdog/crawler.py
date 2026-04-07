"""Resource Crawler — enumerates all Databricks workspace resources.

Discovers resources via the Databricks SDK and Unity Catalog information_schema,
then writes a unified resource_inventory table to Delta.

Resource types crawled:
  - UC: catalogs, schemas, tables, volumes
  - Workspace: jobs, clusters, dashboards, warehouses, pipelines
  - Identity: service principals, groups (for RBAC policy evaluation)
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T


@dataclass
class CrawlResult:
    """Result of crawling a single resource type."""
    resource_type: str
    count: int
    errors: list[str] = field(default_factory=list)


# Schema for the resource_inventory table
INVENTORY_SCHEMA = T.StructType([
    T.StructField("scan_id", T.StringType(), False),
    T.StructField("resource_type", T.StringType(), False),
    T.StructField("resource_id", T.StringType(), False),
    T.StructField("resource_name", T.StringType(), True),
    T.StructField("owner", T.StringType(), True),
    T.StructField("domain", T.StringType(), True),
    T.StructField("tags", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("metadata", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("discovered_at", T.TimestampType(), False),
])


def ensure_inventory_table(spark: SparkSession, catalog: str, schema: str) -> None:
    """Create the resource_inventory table if it doesn't exist.

    Liquid clustering by (scan_id, resource_type) for efficient per-scan queries.
    Not marked appendOnly: enrichment crawlers (grants, DQM, LHM) UPDATE rows
    within the same scan to inject metadata after the initial write.
    """
    table = f"{catalog}.{schema}.resource_inventory"
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            scan_id STRING NOT NULL,
            resource_type STRING NOT NULL,
            resource_id STRING NOT NULL,
            resource_name STRING,
            owner STRING,
            domain STRING,
            tags MAP<STRING, STRING>,
            metadata MAP<STRING, STRING>,
            discovered_at TIMESTAMP NOT NULL
        )
        USING DELTA
        CLUSTER BY (scan_id, resource_type)
    """)


class ResourceCrawler:
    """Crawls Databricks workspace resources and writes to Delta."""

    def __init__(self, spark: SparkSession, w: WorkspaceClient, catalog: str, schema: str):
        self.spark = spark
        self.w = w
        self.catalog = catalog
        self.schema = schema
        self.scan_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.now = datetime.now(timezone.utc)

    @property
    def inventory_table(self) -> str:
        return f"{self.catalog}.{self.schema}.resource_inventory"

    def crawl_all(self) -> list[CrawlResult]:
        """Run all crawlers and write combined inventory to Delta."""
        results = []
        all_rows = []

        # UC resources via information_schema
        for crawler_fn in [
            self._crawl_catalogs,
            self._crawl_schemas,
            self._crawl_tables,
            self._crawl_volumes,
        ]:
            result, rows = self._safe_crawl(crawler_fn)
            results.append(result)
            all_rows.extend(rows)

        # Identity resources via SDK
        for crawler_fn in [
            self._crawl_groups,
        ]:
            result, rows = self._safe_crawl(crawler_fn)
            results.append(result)
            all_rows.extend(rows)

        # Workspace resources via SDK
        for crawler_fn in [
            self._crawl_jobs,
            self._crawl_clusters,
            self._crawl_warehouses,
            self._crawl_pipelines,
        ]:
            result, rows = self._safe_crawl(crawler_fn)
            results.append(result)
            all_rows.extend(rows)

        # DQ system table crawlers (enrich tags on table resources)
        for crawler_fn in [
            self._crawl_dqm_status,
            self._crawl_lhm_status,
        ]:
            result, rows = self._safe_crawl(crawler_fn)
            results.append(result)
            # DQ crawlers return enrichment rows, not inventory rows

        # Write to Delta
        if all_rows:
            ensure_inventory_table(self.spark, self.catalog, self.schema)
            df = self.spark.createDataFrame(all_rows, schema=INVENTORY_SCHEMA)
            df.write.mode("append").saveAsTable(self.inventory_table)

        return results

    def _safe_crawl(self, fn) -> tuple[CrawlResult, list]:
        """Execute a crawler function, catching all exceptions so one bad resource
        type never aborts the full scan. Errors are surfaced in CrawlResult.errors
        and printed by the entrypoint — the scan continues regardless."""
        try:
            rows = fn()
            resource_type = fn.__name__.replace("_crawl_", "")
            return CrawlResult(resource_type=resource_type, count=len(rows)), rows
        except Exception as e:
            resource_type = fn.__name__.replace("_crawl_", "")
            return CrawlResult(resource_type=resource_type, count=0, errors=[str(e)]), []

    def _make_row(self, resource_type: str, resource_id: str,
                  resource_name: str, owner: Optional[str] = None,
                  domain: Optional[str] = None,
                  tags: Optional[dict] = None,
                  metadata: Optional[dict] = None) -> tuple:
        """Build a single inventory row tuple matching INVENTORY_SCHEMA.

        All crawlers produce rows through this method so the scan_id and
        timestamp are stamped consistently from the same crawl_all() call.
        tags and metadata default to empty dicts — never None in Delta.
        """
        return (
            self.scan_id,
            resource_type,
            resource_id,
            resource_name,
            owner,
            domain,
            tags or {},
            metadata or {},
            self.now,
        )

    # ------------------------------------------------------------------
    # Identity resources (via SDK)
    # ------------------------------------------------------------------

    def _crawl_groups(self) -> list:
        """Crawl workspace groups to detect identity drift.

        Captures group type (workspace-local vs account-level), member count,
        and entitlements. Used by POL-S006/S007/S008 to enforce that all
        permission-holding groups are account-level (SCIM-synced from Entra ID).
        """
        rows = []
        for group in self.w.groups.list(attributes="id,displayName,meta,members,entitlements"):
            group_type = "account"
            if group.meta and group.meta.resource_type == "WorkspaceGroup":
                group_type = "workspace_local"

            member_count = len(group.members) if group.members else 0

            entitlements = []
            if group.entitlements:
                entitlements = [e.value for e in group.entitlements if e.value]

            rows.append(self._make_row(
                resource_type="group",
                resource_id=group.id,
                resource_name=group.display_name,
                metadata={
                    "group_type": group_type,
                    "member_count": str(member_count),
                    "entitlements": ",".join(entitlements),
                },
            ))
        return rows

    # ------------------------------------------------------------------
    # UC resources (via information_schema)
    # ------------------------------------------------------------------

    def _crawl_catalogs(self) -> list:
        """Crawl all UC catalogs via the SDK.

        Captures owner and UC tags. isolation_mode is stored in metadata —
        foreign catalogs (OPEN isolation) are flagged by POL-S003 as potential
        data exfiltration paths if they have broad grants.
        """
        rows = []
        for cat in self.w.catalogs.list():
            tags = dict(cat.tags) if cat.tags else {}
            rows.append(self._make_row(
                resource_type="catalog",
                resource_id=cat.name,
                resource_name=cat.name,
                owner=cat.owner,
                tags=tags,
                metadata={
                    "isolation_mode": str(cat.isolation_mode) if cat.isolation_mode else "",
                    "comment": cat.comment or "",
                },
            ))
        return rows

    def _crawl_schemas(self) -> list:
        """Crawl all schemas across all readable catalogs via the SDK.

        Iterates catalogs first; silently skips any catalog that returns a
        permission error (system catalogs, foreign catalogs we can't read).
        domain is set to the parent catalog name for domain-scoped policies.
        """
        rows = []
        for cat in self.w.catalogs.list():
            try:
                for schema in self.w.schemas.list(catalog_name=cat.name):
                    tags = dict(schema.tags) if schema.tags else {}
                    rows.append(self._make_row(
                        resource_type="schema",
                        resource_id=f"{cat.name}.{schema.name}",
                        resource_name=schema.name,
                        owner=schema.owner,
                        domain=cat.name,
                        tags=tags,
                        metadata={"comment": schema.comment or ""},
                    ))
            except Exception:
                continue  # Skip catalogs we can't read
        return rows

    def _crawl_tables(self) -> list:
        """Crawl tables via information_schema, joining table_tags for UC tag data.

        Uses a GROUP BY + collect_list join to fetch all UC tags in a single
        query rather than one SDK call per table. This is the most expensive
        crawl — information_schema.tables can be large in production workspaces.

        Tags land in the tags column (used by ontology classification).
        Table type, schema, and timestamps land in metadata (used by rules).
        """
        rows = []
        df = self.spark.sql("""
            SELECT t.table_catalog, t.table_schema, t.table_name, t.table_owner,
                   t.table_type, t.comment, t.created, t.last_altered,
                   collect_list(struct(tt.tag_name, tt.tag_value)) AS uc_tags
            FROM system.information_schema.tables t
            LEFT JOIN system.information_schema.table_tags tt
              ON  tt.catalog_name = t.table_catalog
              AND tt.schema_name  = t.table_schema
              AND tt.table_name   = t.table_name
            WHERE t.table_schema != 'information_schema'
            GROUP BY t.table_catalog, t.table_schema, t.table_name, t.table_owner,
                     t.table_type, t.comment, t.created, t.last_altered
        """)
        for row in df.collect():
            fqn = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
            tags = {r.tag_name: (r.tag_value or "") for r in row.uc_tags if r.tag_name}
            rows.append(self._make_row(
                resource_type="table",
                resource_id=fqn,
                resource_name=row.table_name,
                owner=row.table_owner,
                domain=row.table_catalog,
                tags=tags,
                metadata={
                    "table_type": row.table_type or "",
                    "schema": row.table_schema,
                    "comment": row.comment or "",
                    "created": str(row.created) if row.created else "",
                    "last_altered": str(row.last_altered) if row.last_altered else "",
                },
            ))
        return rows

    def _crawl_volumes(self) -> list:
        """Crawl UC volumes via information_schema.

        Volumes are the primary target for unstructured data policies (file
        retention, access controls). volume_type (MANAGED vs EXTERNAL) is
        stored in metadata — external volumes have different grant semantics
        and are evaluated separately by access control policies.
        """
        rows = []
        df = self.spark.sql("""
            SELECT volume_catalog, volume_schema, volume_name, volume_owner,
                   volume_type, comment
            FROM system.information_schema.volumes
        """)
        for row in df.collect():
            fqn = f"{row.volume_catalog}.{row.volume_schema}.{row.volume_name}"
            rows.append(self._make_row(
                resource_type="volume",
                resource_id=fqn,
                resource_name=row.volume_name,
                owner=row.volume_owner,
                domain=row.volume_catalog,
                metadata={
                    "volume_type": row.volume_type or "",
                    "schema": row.volume_schema,
                    "comment": row.comment or "",
                },
            ))
        return rows

    # ------------------------------------------------------------------
    # Workspace resources (via SDK)
    # ------------------------------------------------------------------

    def _crawl_jobs(self) -> list:
        """Crawl all workspace jobs via the SDK.

        Tags come from job.settings.tags (not UC tags — jobs predate UC tagging).
        schedule is stored as a string in metadata; the rule engine checks for
        timezone_id to enforce POL-06 (UTC-only scheduled jobs).
        owner is creator_user_name — jobs don't have a separate owner field.
        """
        rows = []
        for job in self.w.jobs.list():
            tags = {}
            if job.settings and job.settings.tags:
                tags = dict(job.settings.tags)
            rows.append(self._make_row(
                resource_type="job",
                resource_id=str(job.job_id),
                resource_name=job.settings.name if job.settings else "",
                owner=job.creator_user_name,
                tags=tags,
                metadata={
                    "schedule": str(job.settings.schedule) if job.settings and job.settings.schedule else "",
                    "max_concurrent_runs": str(job.settings.max_concurrent_runs) if job.settings else "",
                },
            ))
        return rows

    def _crawl_clusters(self) -> list:
        """Crawl all interactive clusters via the SDK.

        spark_version is stored in metadata for POL-07 (minimum DBR version).
        autotermination_minutes is stored for cost governance policies — clusters
        with no autotermination (value=0) are flagged as cost risks.
        Serverless clusters appear here with node_type_id=''.
        """
        rows = []
        for cluster in self.w.clusters.list():
            tags = {}
            if cluster.custom_tags:
                tags = dict(cluster.custom_tags)
            rows.append(self._make_row(
                resource_type="cluster",
                resource_id=cluster.cluster_id,
                resource_name=cluster.cluster_name,
                owner=cluster.creator_user_name,
                tags=tags,
                metadata={
                    "spark_version": cluster.spark_version or "",
                    "node_type_id": cluster.node_type_id or "",
                    "state": str(cluster.state) if cluster.state else "",
                    "autotermination_minutes": str(cluster.autotermination_minutes or ""),
                },
            ))
        return rows

    def _crawl_warehouses(self) -> list:
        """Crawl SQL warehouses via the SDK.

        Warehouse tags use a different structure than cluster tags (EndpointTagPair
        list vs plain dict) — normalised here to a flat dict. cluster_size and
        warehouse_type (CLASSIC vs PRO vs SERVERLESS) are stored for cost policies.
        """
        rows = []
        for wh in self.w.warehouses.list():
            tags = {}
            if wh.tags and wh.tags.custom_tags:
                tags = {t.key: t.value for t in wh.tags.custom_tags}
            rows.append(self._make_row(
                resource_type="warehouse",
                resource_id=wh.id,
                resource_name=wh.name,
                owner=wh.creator_name,
                tags=tags,
                metadata={
                    "cluster_size": wh.cluster_size or "",
                    "state": str(wh.state) if wh.state else "",
                    "warehouse_type": str(wh.warehouse_type) if wh.warehouse_type else "",
                },
            ))
        return rows

    def _crawl_pipelines(self) -> list:
        """Crawl Delta Live Tables pipelines via the SDK.

        Pipelines don't expose custom tags through list_pipelines() — only
        name, state, and creator. Tag-based policies don't apply to pipelines
        today; they're crawled for ownership and state visibility.
        """
        rows = []
        for pipeline in self.w.pipelines.list_pipelines():
            rows.append(self._make_row(
                resource_type="pipeline",
                resource_id=pipeline.pipeline_id,
                resource_name=pipeline.name,
                owner=pipeline.creator_user_name,
                metadata={
                    "state": str(pipeline.state) if pipeline.state else "",
                },
            ))
        return rows

    # ------------------------------------------------------------------
    # DQ system table crawlers (Phase 4)
    # ------------------------------------------------------------------

    def _crawl_dqm_status(self) -> list:
        """Crawl DQM system table and write dq_status + enrich inventory tags.

        Reads system.data_quality_monitoring.table_results to find:
        - Which tables have DQM enabled
        - Latest freshness/completeness status per table
        - Any anomalies detected

        Writes to platform.watchdog.dq_status and updates resource_inventory
        tags with dqm_enabled=true/false, dqm_freshness, dqm_completeness.
        """
        dq_table = f"{self.catalog}.{self.schema}.dq_status"

        # Ensure dq_status table exists
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {dq_table} (
                scan_id STRING NOT NULL,
                table_id STRING NOT NULL,
                source STRING NOT NULL,
                metric STRING NOT NULL,
                status STRING,
                value STRING,
                anomaly BOOLEAN,
                checked_at TIMESTAMP NOT NULL
            )
            USING DELTA
        """)

        # Query DQM system table for latest results per table
        try:
            dqm_rows = self.spark.sql("""
                SELECT
                    table_catalog, table_schema, table_name,
                    metric_name, metric_value, is_anomaly,
                    window_end AS checked_at
                FROM system.data_quality_monitoring.table_results
                WHERE window_end >= current_timestamp() - INTERVAL 7 DAY
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY table_catalog, table_schema, table_name, metric_name
                    ORDER BY window_end DESC
                ) = 1
            """).collect()
        except Exception as e:
            print(f"  DQM system table not available: {e}")
            return []

        if not dqm_rows:
            return []

        # Build dq_status rows
        dq_values = []
        # Track which tables have DQM enabled
        dqm_tables: dict[str, dict] = {}

        for row in dqm_rows:
            fqn = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
            dq_values.append((
                self.scan_id, fqn, "dqm", row.metric_name,
                "anomaly" if row.is_anomaly else "ok",
                str(row.metric_value) if row.metric_value is not None else "",
                bool(row.is_anomaly),
                row.checked_at,
            ))

            if fqn not in dqm_tables:
                dqm_tables[fqn] = {"anomalies": 0, "metrics": 0}
            dqm_tables[fqn]["metrics"] += 1
            if row.is_anomaly:
                dqm_tables[fqn]["anomalies"] += 1

        # Write dq_status
        dq_schema = T.StructType([
            T.StructField("scan_id", T.StringType(), False),
            T.StructField("table_id", T.StringType(), False),
            T.StructField("source", T.StringType(), False),
            T.StructField("metric", T.StringType(), False),
            T.StructField("status", T.StringType(), True),
            T.StructField("value", T.StringType(), True),
            T.StructField("anomaly", T.BooleanType(), True),
            T.StructField("checked_at", T.TimestampType(), False),
        ])
        df = self.spark.createDataFrame(dq_values, schema=dq_schema)
        df.write.mode("append").saveAsTable(dq_table)

        # Enrich resource_inventory tags for tables with DQM coverage
        for fqn, stats in dqm_tables.items():
            try:
                self.spark.sql(f"""
                    UPDATE {self.inventory_table}
                    SET tags = map_concat(tags, map(
                        'dqm_enabled', 'true',
                        'dqm_anomalies', '{stats["anomalies"]}',
                        'dqm_metrics_checked', '{stats["metrics"]}'
                    ))
                    WHERE resource_id = '{fqn}'
                      AND scan_id = '{self.scan_id}'
                      AND resource_type = 'table'
                """)
            except Exception:
                continue

        print(f"  DQM: {len(dqm_tables)} tables with monitoring, {len(dq_values)} metric results")
        return []  # Enrichment only, no new inventory rows

    def _crawl_lhm_status(self) -> list:
        """Crawl Lakehouse Monitoring status and enrich inventory tags.

        Detects tables with Lakehouse Monitoring (LHM) enabled by looking for
        associated profile tables in information_schema (pattern: *_profile_metrics).
        Also queries the monitor API for drift/anomaly status.
        """
        # Find tables that have LHM profile tables
        try:
            lhm_rows = self.spark.sql("""
                SELECT
                    table_catalog, table_schema, table_name
                FROM system.information_schema.tables
                WHERE table_name LIKE '%_profile_metrics'
                  AND table_schema != 'information_schema'
            """).collect()
        except Exception as e:
            print(f"  LHM detection query failed: {e}")
            return []

        if not lhm_rows:
            return []

        # Derive monitored table names (strip _profile_metrics suffix)
        monitored_tables = set()
        for row in lhm_rows:
            base_name = row.table_name.replace("_profile_metrics", "")
            fqn = f"{row.table_catalog}.{row.table_schema}.{base_name}"
            monitored_tables.add(fqn)

        # Enrich resource_inventory tags
        dq_table = f"{self.catalog}.{self.schema}.dq_status"
        for fqn in monitored_tables:
            try:
                self.spark.sql(f"""
                    UPDATE {self.inventory_table}
                    SET tags = map_concat(tags, map('lhm_enabled', 'true'))
                    WHERE resource_id = '{fqn}'
                      AND scan_id = '{self.scan_id}'
                      AND resource_type = 'table'
                """)
                # Write an LHM entry to dq_status
                self.spark.sql(f"""
                    INSERT INTO {dq_table}
                    (scan_id, table_id, source, metric, status, value, anomaly, checked_at)
                    VALUES ('{self.scan_id}', '{fqn}', 'lhm', 'monitoring_enabled',
                            'ok', 'true', false, current_timestamp())
                """)
            except Exception:
                continue

        print(f"  LHM: {len(monitored_tables)} tables with Lakehouse Monitoring")
        return []  # Enrichment only
