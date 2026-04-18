"""Resource Crawler — enumerates all Databricks workspace resources.

Discovers resources via the Databricks SDK and Unity Catalog information_schema,
then writes a unified resource_inventory table to Delta.

Resource types crawled:
  - UC: catalogs, schemas, tables, volumes, grants
  - Workspace: jobs, clusters, dashboards, warehouses, pipelines
  - Identity: service principals, groups (for RBAC policy evaluation)
  - AI Agents: deployed agents (Apps + serving endpoints), agent execution traces
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import pyspark.sql.types as T
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import SecurableType
from pyspark.sql import SparkSession

from watchdog.exceptions import CrawlError, TransientCrawlError


@dataclass
class CrawlResult:
    """Result of crawling a single resource type."""
    resource_type: str
    count: int
    errors: list[str] = field(default_factory=list)


# Schema for the resource_inventory table
INVENTORY_SCHEMA = T.StructType([
    T.StructField("scan_id", T.StringType(), False),
    T.StructField("metastore_id", T.StringType(), True),
    T.StructField("resource_type", T.StringType(), False),
    T.StructField("resource_id", T.StringType(), False),
    T.StructField("resource_name", T.StringType(), True),
    T.StructField("owner", T.StringType(), True),
    T.StructField("domain", T.StringType(), True),
    T.StructField("tags", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("metadata", T.MapType(T.StringType(), T.StringType()), True),
    T.StructField("discovered_at", T.TimestampType(), False),
])


def derive_pipeline_health(
    last_success_at: str | None,
    last_failure_at: str | None,
    failure_count_7d: int,
    now: datetime | None = None,
) -> dict:
    """Derive pipeline freshness tags from run history.

    Pure function for testability. Called by _crawl_pipeline_freshness()
    with data from system.lakeflow.pipeline_event_log.

    Returns dict of tags to merge into the pipeline's inventory row.
    """
    now = now or datetime.now(timezone.utc)
    tags = {
        "last_success_at": last_success_at or "",
        "last_failure_at": last_failure_at or "",
        "failure_count_7d": str(failure_count_7d),
    }

    if not last_success_at:
        tags["freshness_hours"] = "-1"
        tags["pipeline_health"] = "failing"
        return tags

    success_dt = datetime.fromisoformat(last_success_at.replace("Z", "+00:00"))
    hours_since = int((now - success_dt).total_seconds() // 3600)
    tags["freshness_hours"] = str(hours_since)

    # Determine health
    if last_failure_at:
        failure_dt = datetime.fromisoformat(last_failure_at.replace("Z", "+00:00"))
        if failure_dt > success_dt:
            tags["pipeline_health"] = "failing"
            return tags

    if failure_count_7d > 0:
        tags["pipeline_health"] = "degraded"
    else:
        tags["pipeline_health"] = "healthy"

    return tags


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
            metastore_id STRING,
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
        TBLPROPERTIES (
            'delta.enableChangeDataFeed' = 'true'
        )
    """)


class ResourceCrawler:
    """Crawls Databricks workspace resources and writes to Delta."""

    def __init__(self, spark: SparkSession, w: WorkspaceClient, catalog: str, schema: str,
                 metastore_id: Optional[str] = None):
        self.spark = spark
        self.w = w
        self.catalog = catalog
        self.schema = schema
        self._metastore_id_override = metastore_id
        self._cached_metastore_id: Optional[str] = None
        self.scan_id = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.now = datetime.now(timezone.utc)

    @property
    def metastore_id(self) -> str:
        """Return the metastore ID, using override if provided, else auto-detect."""
        if self._metastore_id_override:
            return self._metastore_id_override
        if self._cached_metastore_id is None:
            try:
                summary = self.w.metastores.current()
                self._cached_metastore_id = summary.metastore_id or ""
            except Exception:
                self._cached_metastore_id = ""
        return self._cached_metastore_id

    @property
    def inventory_table(self) -> str:
        return f"{self.catalog}.{self.schema}.resource_inventory"

    # Registry of primary_type → (crawler_method_name, primary_type_override)
    # Used by crawl_all() and the ad-hoc filter to pick which crawlers to run.
    _CRAWLERS: list[tuple[str, str, str | None]] = [
        ("catalog", "_crawl_catalogs", None),
        ("schema", "_crawl_schemas", None),
        ("table", "_crawl_tables", None),
        ("volume", "_crawl_volumes", None),
        ("group", "_crawl_groups", "group"),
        ("service_principal", "_crawl_service_principals", None),
        ("agent", "_crawl_agents", None),
        ("agent_trace", "_crawl_agent_traces", None),
        ("grant", "_crawl_grants", None),
        ("row_filter", "_crawl_row_filters", None),
        ("column_mask", "_crawl_column_masks", None),
        ("job", "_crawl_jobs", None),
        ("cluster", "_crawl_clusters", None),
        ("warehouse", "_crawl_warehouses", None),
        ("pipeline", "_crawl_pipelines", None),
    ]

    # DQ crawlers emit enrichment rows only — never written to inventory.
    _DQ_CRAWLERS: list[tuple[str, str]] = [
        ("dqm_status", "_crawl_dqm_status"),
        ("lhm_status", "_crawl_lhm_status"),
        ("pipeline_freshness", "_crawl_pipeline_freshness"),
    ]

    def crawl_all(self,
                  resource_types: Optional[set[str]] = None,
                  resource_id: Optional[str] = None) -> list[CrawlResult]:
        """Run all crawlers and write combined inventory to Delta.

        Args:
            resource_types: If provided, only run crawlers whose primary type is
                in the set (e.g. {"table"}). None means run everything.
            resource_id: If provided, only rows matching this resource_id are
                written to inventory. The crawler still enumerates the full
                type because the Databricks APIs do not expose per-id fetches
                uniformly — filtering happens in memory. Useful for ad-hoc scans.
        """
        results = []
        all_rows = []

        for primary_type, method_name, pt_override in self._CRAWLERS:
            if resource_types and primary_type not in resource_types:
                continue
            fn = getattr(self, method_name)
            result, rows = self._safe_crawl(fn, primary_type=pt_override)
            results.append(result)
            all_rows.extend(rows)

        # DQ crawlers enrich table tags — skip unless tables are in scope.
        if not resource_types or "table" in resource_types or "pipeline" in resource_types:
            for _pt, method_name in self._DQ_CRAWLERS:
                fn = getattr(self, method_name)
                result, _rows = self._safe_crawl(fn)
                results.append(result)

        # Post-filter by resource_id when the caller wants a single asset.
        # Row tuples match INVENTORY_SCHEMA: index 3 is resource_id.
        if resource_id:
            all_rows = [r for r in all_rows if r[3] == resource_id]

        # Write to Delta
        if all_rows:
            ensure_inventory_table(self.spark, self.catalog, self.schema)
            df = self.spark.createDataFrame(all_rows, schema=INVENTORY_SCHEMA)
            df.write.mode("append").saveAsTable(self.inventory_table)

        return results

    def _safe_crawl(self, fn, primary_type: str | None = None) -> tuple[CrawlResult, list]:
        """Execute a crawler function, catching all exceptions so one bad resource
        type never aborts the full scan. Errors are surfaced in CrawlResult.errors
        and printed by the entrypoint — the scan continues regardless.

        primary_type: the resource_type string to count as the primary resource.
        If omitted, it is derived from the function name by stripping "_crawl_".
        Pass explicitly when the derived name would not match the emitted resource_type
        (e.g. _crawl_groups emits "group", not "groups").
        """
        try:
            rows = fn()
            resource_type = fn.__name__.replace("_crawl_", "")
            pt = primary_type if primary_type is not None else resource_type
            # Count only the rows whose resource_type matches the crawler's primary
            # resource type. Crawlers like _crawl_groups() emit multiple resource
            # types (e.g. "group" + "group_member"), so using len(rows) would
            # inflate the reported count for the primary type.
            primary_count = sum(1 for r in rows if r[2] == pt)
            count = primary_count if primary_count > 0 else len(rows)
            return CrawlResult(resource_type=resource_type, count=count), rows
        except CrawlError as e:
            return CrawlResult(resource_type=e.resource_type, count=0, errors=[str(e)]), []
        except Exception as e:
            # SDK and Spark raise unstructured errors; wrap so downstream
            # retry logic can distinguish transient from permanent failures.
            resource_type = fn.__name__.replace("_crawl_", "")
            wrapped = TransientCrawlError(resource_type, str(e))
            return CrawlResult(resource_type=resource_type, count=0, errors=[str(wrapped)]), []

    def _make_row(self, resource_type: str, resource_id: str,
                  resource_name: str, owner: Optional[str] = None,
                  domain: Optional[str] = None,
                  tags: Optional[dict] = None,
                  metadata: Optional[dict] = None) -> tuple:
        """Build a single inventory row tuple matching INVENTORY_SCHEMA.

        All crawlers produce rows through this method so the scan_id and
        timestamp are stamped consistently from the same crawl_all() call.
        tags and metadata default to empty dicts — never None in Delta.
        metastore_id is stamped from the metastore_id property (override or auto-detect).
        """
        return (
            self.scan_id,
            self.metastore_id,
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

            for member in (group.members or []):
                member_value = member.value or ""
                ref = getattr(member, "ref", "") or ""
                if "ServicePrincipals" in ref:
                    member_type = "service_principal"
                elif "Groups" in ref:
                    member_type = "group"
                elif "Users" in ref:
                    member_type = "user"
                else:
                    member_type = "user"  # default

                rows.append(self._make_row(
                    resource_type="group_member",
                    resource_id=f"group_member:{group.id}:{member_value}",
                    resource_name=group.display_name,
                    metadata={
                        "group_name": group.display_name,
                        "member_value": member_value,
                        "member_type": member_type,
                    },
                ))
        return rows

    def _crawl_service_principals(self) -> list:
        """Crawl workspace service principals via the SDK.

        Captures application_id, active status, and entitlements. Used by
        identity governance policies to ensure SPs are properly managed,
        have expected entitlements, and inactive SPs are detected.
        """
        rows = []
        for sp in self.w.service_principals.list():
            entitlements = ""
            if sp.entitlements:
                entitlements = ",".join(e.value for e in sp.entitlements if e.value)

            rows.append(self._make_row(
                resource_type="service_principal",
                resource_id=f"service_principal:{sp.application_id}",
                resource_name=sp.display_name or sp.application_id,
                metadata={
                    "application_id": sp.application_id or "",
                    "active": str(sp.active) if sp.active is not None else "",
                    "entitlements": entitlements,
                },
            ))
        return rows

    # ------------------------------------------------------------------
    # AI Agent resources (via SDK + system tables)
    # ------------------------------------------------------------------

    def _crawl_agents(self) -> list:
        """Crawl deployed AI agents from Apps and serving endpoints.

        Source 1: Databricks Apps that look like agents — filtered by
        heuristic keywords (agent, mcp, assistant, bot, ai) in name or
        description. Captures app metadata, compute status, and URL.

        Source 2: Model serving endpoints — all endpoints are included
        since they commonly serve agent models. Captures endpoint state,
        creator, and creation timestamp.

        Used by AI governance policies to enforce agent registration,
        ownership, and access controls.
        """
        rows = []

        # Source 1: Databricks Apps that look like agents
        try:
            apps = self.w.apps.list()
            for app in apps:
                # Heuristic: apps with 'agent' or 'mcp' in name/description
                name = (app.name or "").lower()
                desc = (app.description or "").lower()
                if any(k in name or k in desc for k in ["agent", "mcp", "assistant", "bot", "ai"]):
                    tags = {}
                    metadata = {
                        "app_name": app.name or "",
                        "deployed_by": app.creator or "",
                        "compute_status": getattr(app, "compute_status", {}).get("state", "") if isinstance(getattr(app, "compute_status", None), dict) else str(getattr(getattr(app, "compute_status", None), "state", "")),
                        "url": app.url or "",
                        "created_at": str(getattr(app, "create_time", "")),
                    }

                    # Set governance tags from app metadata
                    if app.creator:
                        tags["agent_owner"] = app.creator
                        tags["deployed_by"] = app.creator

                    rows.append(self._make_row(
                        resource_type="agent",
                        resource_id=f"agent:app:{app.name}",
                        resource_name=app.name or "",
                        owner=app.creator,
                        domain="",
                        tags=tags,
                        metadata=metadata,
                    ))
        except Exception as e:
            print(f"  Apps crawl partial failure: {e}")

        # Source 2: Model serving endpoints (agents deployed as endpoints)
        # FMAPI endpoints (databricks-*) are Databricks-managed foundation
        # models, not customer agents. They get tagged as managed_endpoint
        # so the ontology can classify them separately and exempt them from
        # customer agent governance policies.
        _FMAPI_PREFIXES = ("databricks-",)
        try:
            endpoints = self.w.serving_endpoints.list()
            for ep in endpoints:
                tags = {}
                ep_name = ep.name or ""
                is_fmapi = any(ep_name.startswith(p) for p in _FMAPI_PREFIXES)
                metadata = {
                    "endpoint_name": ep_name,
                    "deployed_by": getattr(ep, "creator", "") or "",
                    "endpoint_state": str(getattr(getattr(ep, "state", None), "ready", "")),
                    "created_at": str(getattr(ep, "creation_timestamp", "")),
                    "is_fmapi": str(is_fmapi).lower(),
                }

                creator = getattr(ep, "creator", "") or ""
                if creator:
                    tags["agent_owner"] = creator
                    tags["deployed_by"] = creator
                tags["model_endpoint"] = ep_name

                if is_fmapi:
                    tags["managed_endpoint"] = "true"
                    tags["agent_owner"] = "databricks"
                    tags["audit_logging_enabled"] = "true"

                rows.append(self._make_row(
                    resource_type="agent",
                    resource_id=f"agent:endpoint:{ep_name}",
                    resource_name=ep_name,
                    owner=creator if not is_fmapi else "databricks",
                    domain="",
                    tags=tags,
                    metadata=metadata,
                ))
        except Exception as e:
            print(f"  Serving endpoints crawl partial failure: {e}")

        return rows

    def _crawl_agent_traces(self) -> list:
        """Crawl recent serving endpoint usage as agent_execution resources.

        Reads from system.serving.endpoint_usage joined with
        system.serving.served_entities to get per-requester usage
        aggregated by endpoint. Each (endpoint, requester) pair in the
        last 7 days becomes an agent_execution resource.

        Falls back gracefully if serving system tables don't exist.

        Used by AI governance policies to monitor agent execution patterns,
        detect anomalies, and enforce execution controls.
        """
        rows = []

        try:
            trace_query = """
                SELECT
                    se.endpoint_name,
                    se.served_entity_name,
                    se.entity_type,
                    se.task,
                    se.created_by AS endpoint_creator,
                    eu.requester,
                    COUNT(*) as request_count,
                    SUM(COALESCE(eu.input_token_count, 0)) as total_input_tokens,
                    SUM(COALESCE(eu.output_token_count, 0)) as total_output_tokens,
                    SUM(CASE WHEN eu.status_code != 200 THEN 1 ELSE 0 END) as error_count,
                    SUM(CASE WHEN eu.status_code >= 429 THEN 1 ELSE 0 END) as rate_limited_count,
                    MIN(eu.request_time) as first_request,
                    MAX(eu.request_time) as last_request
                FROM system.serving.endpoint_usage eu
                JOIN system.serving.served_entities se
                    ON eu.served_entity_id = se.served_entity_id
                WHERE eu.request_time >= date_sub(current_date(), 7)
                GROUP BY se.endpoint_name, se.served_entity_name, se.entity_type,
                         se.task, se.created_by, eu.requester
                ORDER BY request_count DESC
                LIMIT 500
            """

            result = self.spark.sql(trace_query)
            for row in result.collect():
                endpoint = row.endpoint_name or ""
                requester = row.requester or ""
                request_id = f"{endpoint}:{requester}"

                entity_type = row.entity_type or ""
                task = row.task or ""

                tags = {
                    "trace_id": request_id,
                    "execution_completed": "true" if row.error_count == 0 else "false",
                    "model_endpoint": endpoint,
                    "entity_type": entity_type,
                    "task_type": task,
                }

                # Flag high-volume users for governance review
                if row.request_count and row.request_count > 10000:
                    tags["high_volume_requester"] = "true"
                # Flag rate-limited requesters
                if row.rate_limited_count and row.rate_limited_count > 0:
                    tags["rate_limited"] = "true"

                metadata = {
                    "endpoint_name": endpoint,
                    "served_entity_name": row.served_entity_name or "",
                    "entity_type": entity_type,
                    "task": task,
                    "endpoint_creator": row.endpoint_creator or "",
                    "requester": requester,
                    "request_count": str(row.request_count or 0),
                    "total_input_tokens": str(row.total_input_tokens or 0),
                    "total_output_tokens": str(row.total_output_tokens or 0),
                    "error_count": str(row.error_count or 0),
                    "rate_limited_count": str(row.rate_limited_count or 0),
                    "first_request": str(row.first_request or ""),
                    "last_request": str(row.last_request or ""),
                    "resource_type": "agent_execution",
                }

                rows.append(self._make_row(
                    resource_type="agent_execution",
                    resource_id=f"execution:{request_id}",
                    resource_name=f"{endpoint} ({requester[:30]})",
                    owner=requester,
                    domain="",
                    tags=tags,
                    metadata=metadata,
                ))
        except Exception as e:
            # Trace tables may not exist on all workspaces
            print(f"  Agent traces not available: {e}")

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

    def _crawl_grants(self) -> list:
        """Crawl UC grants via information_schema and SDK.

        Enumerates table-level and schema-level grants from information_schema,
        plus catalog-level grants from the SDK. Used by access control policies
        to detect overly broad grants, orphaned permissions, and privilege
        escalation paths.

        Only non-inherited grants are captured (inherited_from = 'NONE' in
        information_schema). Catalog-level grants from the SDK don't have an
        inherited_from field — they are always direct.
        """
        rows = []

        # Table-level grants from information_schema (per catalog)
        for cat in self.w.catalogs.list():
            try:
                table_grants = self.spark.sql(f"""
                    SELECT grantee, table_catalog, table_schema, table_name,
                           privilege_type, is_grantable
                    FROM {cat.name}.information_schema.table_privileges
                    WHERE inherited_from = 'NONE'
                """).collect()
                for row in table_grants:
                    fqn = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
                    rows.append(self._make_row(
                        resource_type="grant",
                        resource_id=f"table:{fqn}:{row.grantee}:{row.privilege_type}",
                        resource_name=f"{row.privilege_type} on table {fqn}",
                        owner=row.grantee,
                        metadata={
                            "securable_type": "table",
                            "securable_full_name": fqn,
                            "grantee": row.grantee,
                            "privilege": row.privilege_type,
                            "grantor": "",
                            "inherited_from": "",
                        },
                    ))
            except Exception:
                continue  # Skip catalogs we can't read

            # Schema-level grants from information_schema (per catalog)
            try:
                schema_grants = self.spark.sql(f"""
                    SELECT grantee, catalog_name, schema_name,
                           privilege_type, is_grantable
                    FROM {cat.name}.information_schema.schema_privileges
                    WHERE inherited_from = 'NONE'
                """).collect()
                for row in schema_grants:
                    fqn = f"{row.catalog_name}.{row.schema_name}"
                    rows.append(self._make_row(
                        resource_type="grant",
                        resource_id=f"schema:{fqn}:{row.grantee}:{row.privilege_type}",
                        resource_name=f"{row.privilege_type} on schema {fqn}",
                        owner=row.grantee,
                        metadata={
                            "securable_type": "schema",
                            "securable_full_name": fqn,
                            "grantee": row.grantee,
                            "privilege": row.privilege_type,
                            "grantor": "",
                            "inherited_from": "",
                        },
                    ))
            except Exception:
                continue  # Skip catalogs we can't read

            # Catalog-level grants via SDK
            try:
                catalog_grants = self.w.grants.get(
                    securable_type=SecurableType.CATALOG,
                    full_name=cat.name,
                )
                if catalog_grants.privilege_assignments:
                    for assignment in catalog_grants.privilege_assignments:
                        grantee = assignment.principal or ""
                        for priv in (assignment.privileges or []):
                            privilege = priv.privilege.value if priv.privilege else ""
                            inherited = priv.inherited_from_name or ""
                            rows.append(self._make_row(
                                resource_type="grant",
                                resource_id=f"catalog:{cat.name}:{grantee}:{privilege}",
                                resource_name=f"{privilege} on catalog {cat.name}",
                                owner=grantee,
                                metadata={
                                    "securable_type": "catalog",
                                    "securable_full_name": cat.name,
                                    "grantee": grantee,
                                    "privilege": privilege,
                                    "grantor": "",
                                    "inherited_from": inherited,
                                },
                            ))
            except Exception:
                continue  # Skip catalogs we can't read grants for

        return rows

    def _crawl_row_filters(self) -> list:
        """Crawl UC row filters via information_schema.

        Row filters are security policies that restrict which rows a user can
        see in a table. Each filter is tied to a table and a filter function.
        Used by data-access governance policies to ensure row-level security
        is applied consistently and filter functions are owned and audited.
        """
        rows = []
        for cat in self.w.catalogs.list():
            try:
                filter_rows = self.spark.sql(f"""
                    SELECT table_catalog, table_schema, table_name, filter_function_name
                    FROM {cat.name}.information_schema.row_filters
                """).collect()
                for row in filter_rows:
                    table_full_name = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
                    resource_id = f"row_filter:{table_full_name}"
                    rows.append(self._make_row(
                        resource_type="row_filter",
                        resource_id=resource_id,
                        resource_name=row.table_name,
                        domain=row.table_catalog,
                        metadata={
                            "table_full_name": table_full_name,
                            "filter_function": row.filter_function_name or "",
                        },
                    ))
            except Exception:
                continue  # Skip catalogs we can't read
        return rows

    def _crawl_column_masks(self) -> list:
        """Crawl UC column masks via information_schema.

        Column masks are security policies that apply a masking function to a
        column so that sensitive data is obscured for unauthorized users. Each
        mask is tied to a specific table column and a mask function. Used by
        data-access governance policies to ensure PII and sensitive columns are
        masked, and masking functions are owned and audited.
        """
        rows = []
        for cat in self.w.catalogs.list():
            try:
                mask_rows = self.spark.sql(f"""
                    SELECT table_catalog, table_schema, table_name, column_name, mask_function_name
                    FROM {cat.name}.information_schema.column_masks
                """).collect()
                for row in mask_rows:
                    table_full_name = f"{row.table_catalog}.{row.table_schema}.{row.table_name}"
                    resource_id = f"column_mask:{table_full_name}.{row.column_name}"
                    rows.append(self._make_row(
                        resource_type="column_mask",
                        resource_id=resource_id,
                        resource_name=f"{row.table_name}.{row.column_name}",
                        domain=row.table_catalog,
                        metadata={
                            "table_full_name": table_full_name,
                            "column_name": row.column_name or "",
                            "mask_function": row.mask_function_name or "",
                        },
                    ))
            except Exception:
                continue  # Skip catalogs we can't read
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
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true'
            )
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

    def _crawl_pipeline_freshness(self) -> list:
        """Enrich pipeline inventory rows with freshness tags from system tables.

        Reads system.lakeflow.pipeline_event_log for the last 7 days,
        computes per-pipeline health metrics, and UPDATEs the existing
        pipeline rows in resource_inventory with freshness tags.

        Follows the same pattern as _crawl_dqm_status: enrichment via
        UPDATE, graceful fallback if system table isn't available.
        """
        try:
            event_rows = self.spark.sql("""
                SELECT
                    pipeline_id,
                    event_type,
                    timestamp,
                    message
                FROM system.lakeflow.pipeline_event_log
                WHERE timestamp >= current_timestamp() - INTERVAL 7 DAY
                  AND event_type IN ('create_update', 'update_progress')
            """).collect()
        except Exception as e:
            print(f"  Pipeline event log not available: {e}")
            return []

        if not event_rows:
            return []

        # Aggregate per pipeline: last success, last failure, failure count
        from collections import defaultdict
        pipeline_stats = defaultdict(lambda: {
            "last_success": None, "last_failure": None, "failure_count": 0
        })

        for row in event_rows:
            pid = row.pipeline_id
            ts = row.timestamp.isoformat() if row.timestamp else None
            msg = (row.message or "").upper()

            if row.event_type == "update_progress":
                if "COMPLETED" in msg:
                    cur = pipeline_stats[pid]["last_success"]
                    if cur is None or ts > cur:
                        pipeline_stats[pid]["last_success"] = ts
                elif "FAILED" in msg or "ERROR" in msg:
                    pipeline_stats[pid]["failure_count"] += 1
                    cur = pipeline_stats[pid]["last_failure"]
                    if cur is None or ts > cur:
                        pipeline_stats[pid]["last_failure"] = ts

        # Enrich inventory rows
        for pid, stats in pipeline_stats.items():
            health_tags = derive_pipeline_health(
                last_success_at=stats["last_success"],
                last_failure_at=stats["last_failure"],
                failure_count_7d=stats["failure_count"],
                now=self.now,
            )
            # Build SET clause for tag updates
            tag_updates = ", ".join(
                f"'{k}', '{v}'" for k, v in health_tags.items()
            )
            self.spark.sql(f"""
                UPDATE {self.inventory_table}
                SET tags = map_concat(tags, map({tag_updates}))
                WHERE scan_id = '{self.scan_id}'
                  AND resource_type = 'pipeline'
                  AND resource_id = '{pid}'
            """)

        print(f"  pipeline_freshness: enriched {len(pipeline_stats)} pipelines")
        return []  # Enrichment only, no new inventory rows
