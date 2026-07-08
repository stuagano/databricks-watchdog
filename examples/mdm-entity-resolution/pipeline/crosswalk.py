"""Persistent ID crosswalk (U5).

Turns U4's pairwise ``match.pairs`` decisions into a stable, mergeable
identity: every ``silver.source_records`` row gets assigned to a persistent
``entity_id`` that survives re-runs (id stability) and correctly collapses
when two previously-separate clusters turn out to be the same entity (a
merge), always keeping the older/lower id and marking the other superseded.

Two pure, dependency-free functions carry all the logic and are unit-tested
without Spark:

- :func:`connected_components` -- transitive closure over ``auto_match``
  pairs (union-find).
- :func:`stable_assign` -- turns a record->cluster-key map into a stable
  record->entity_id map, reusing the oldest known id per cluster from a prior
  run so ids never churn across re-runs.

:func:`assign` is the Spark/Delta-I/O wrapper (reads ``match.pairs`` +
``entity_crosswalk``, calls the two pure functions, writes
``entity_crosswalk``) and is exercised on the Databricks workspace, not by
the local pytest suite.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipeline.config import Cfg

# Matches sql/contracts.sql's entity_crosswalk DDL verbatim. `superseded_by`
# is always None on active rows (and `confidence` always None on tombstone
# rows) -- on a run with no merge tombstones (e.g. the very first run) every
# row in one of these columns is None, and Spark can't infer a type from an
# all-null column, so pass this explicit schema instead (same failure mode
# already fixed for steward.publish's part_match_reviews write).
_CROSSWALK_ROW_SCHEMA_DDL = (
    "source_record_id STRING, entity_id STRING, confidence DOUBLE, "
    "first_seen_run STRING, first_seen_ts TIMESTAMP, last_seen_run STRING, "
    "status STRING, superseded_by STRING"
)


def connected_components(pairs: list[tuple[str, str]]) -> dict[str, str]:
    """Transitive closure over an ``auto_match`` pair list via union-find.

    Returns a map of every node that appears in at least one pair to a
    deterministic cluster representative (the lexicographically smallest
    member of its cluster). Nodes that never appear in any pair are not
    invented -- they simply aren't keys in the returned dict.
    """
    parent: dict[str, str] = {}

    def find(x: str) -> str:
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[ra] = rb

    for left, right in pairs:
        union(left, right)

    groups: dict[str, list[str]] = {}
    for node in parent:
        groups.setdefault(find(node), []).append(node)

    representative: dict[str, str] = {}
    for members in groups.values():
        rep = min(members)
        for member in members:
            representative[member] = rep
    return representative


def stable_assign(clusters: dict[str, str], prior: dict[str, str]) -> dict[str, str]:
    """Assign a stable ``entity_id`` to every record in ``clusters``.

    ``clusters`` maps ``source_record_id -> cluster_key`` (e.g. the output of
    :func:`connected_components`, or any grouping). ``prior`` maps
    ``source_record_id -> entity_id`` from the previous run.

    For each cluster: if any of its members already had an ``entity_id`` in
    ``prior``, the whole cluster reuses the oldest (lexicographically
    smallest, a deterministic stand-in for "first assigned") of those ids --
    this is what makes a merge of two previously-separate clusters collapse
    onto the older id rather than minting a fresh one. Otherwise a new,
    deterministic id is minted from the cluster's smallest member id so the
    same input always produces the same id.
    """
    members_by_cluster: dict[str, list[str]] = {}
    for record_id, cluster_key in clusters.items():
        members_by_cluster.setdefault(cluster_key, []).append(record_id)

    assigned: dict[str, str] = {}
    for members in members_by_cluster.values():
        existing_ids = sorted({prior[m] for m in members if m in prior})
        entity_id = existing_ids[0] if existing_ids else f"ent-{min(members)}"
        for member in members:
            assigned[member] = entity_id
    return assigned


def assign(spark: Any, cfg: Cfg, run_id: str) -> None:
    """Read ``match.pairs`` (auto_match) + the existing ``entity_crosswalk``,
    compute clusters, assign/preserve ``entity_id`` per ``silver.source_records``
    row, and (re)write the full ``entity_crosswalk`` table.

    Every ``silver.source_records`` row gets a row here, even singletons with
    no auto_match pairs (their own cluster of one). When a merge collapses two
    previously-distinct entity ids onto one (the older id wins per
    :func:`stable_assign`), a tombstone row keyed by the now-abandoned
    ``entity_id`` is written with ``status='superseded'`` and
    ``superseded_by`` pointing at the surviving id, so any downstream
    consumer still holding the old id can resolve the redirect.
    """
    now = datetime.now(timezone.utc)

    pairs_rows = (
        spark.table(cfg.tbl("match", "pairs"))
        .filter("decision = 'auto_match'")
        .select("left_record_id", "right_record_id")
        .collect()
    )
    pairs = [(row["left_record_id"], row["right_record_id"]) for row in pairs_rows]

    silver_ids = [
        row["source_record_id"]
        for row in spark.table(cfg.tbl("silver", "source_records")).select("source_record_id").collect()
    ]

    components = connected_components(pairs)
    clusters = {record_id: components.get(record_id, record_id) for record_id in silver_ids}

    xwalk_table = cfg.tbl("mdm", "entity_crosswalk")
    prior: dict[str, str] = {}
    first_seen: dict[str, tuple[str, Any]] = {}
    try:
        existing = spark.table(xwalk_table).collect()
    except Exception:
        existing = []
    for row in existing:
        if row["status"] == "active":
            prior[row["source_record_id"]] = row["entity_id"]
        first_seen[row["source_record_id"]] = (row["first_seen_run"], row["first_seen_ts"])

    new_assignment = stable_assign(clusters, prior)

    out_rows: list[dict[str, Any]] = []
    for record_id, entity_id in new_assignment.items():
        fs_run, fs_ts = first_seen.get(record_id, (run_id, now))
        out_rows.append({
            "source_record_id": record_id,
            "entity_id": entity_id,
            "confidence": 1.0,
            "first_seen_run": fs_run,
            "first_seen_ts": fs_ts,
            "last_seen_run": run_id,
            "status": "active",
            "superseded_by": None,
        })

    # Merge tombstones: any prior entity_id that no longer covers any live
    # record has been absorbed into a surviving entity_id -- record the
    # redirect keyed by the abandoned id itself.
    surviving_ids = set(new_assignment.values())
    absorbed_by: dict[str, str] = {}
    for record_id, old_entity_id in prior.items():
        if old_entity_id in surviving_ids or old_entity_id in absorbed_by:
            continue
        if record_id in new_assignment:
            absorbed_by[old_entity_id] = new_assignment[record_id]

    for old_entity_id, winning_entity_id in absorbed_by.items():
        out_rows.append({
            "source_record_id": old_entity_id,
            "entity_id": old_entity_id,
            "confidence": None,
            "first_seen_run": first_seen.get(old_entity_id, (run_id, now))[0],
            "first_seen_ts": first_seen.get(old_entity_id, (run_id, now))[1],
            "last_seen_run": run_id,
            "status": "superseded",
            "superseded_by": winning_entity_id,
        })

    df = spark.createDataFrame(out_rows, schema=_CROSSWALK_ROW_SCHEMA_DDL)
    df.write.mode("overwrite").saveAsTable(xwalk_table)
