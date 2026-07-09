"""Vector Search blocking + batch matcher (U4) — integration unit.

Provisions (idempotently) a Databricks Vector Search endpoint plus two
Delta-Sync indexes over ``silver.source_records`` -- ``embed_func`` (commodity +
description + specs, used for commodity-scoped blocking so we never do an
O(n^2) cross-join) and ``embed_desc`` (mpn + description, kept for future
same-part precision re-ranking). For every silver record, queries the
``embed_func`` index for its top-k nearest neighbours filtered to the same
commodity, deduplicates the resulting pairs, scores each with the shared,
dependency-free :func:`pipeline.matching_core.categorize`, and writes
``match.pairs``.

This module requires a live Databricks workspace (Vector Search SDK, a real
``silver.source_records`` table, a Spark session) -- it is NOT exercised by the
local pytest suite (see ``tests/test_matching_core.py`` for the pure scoring
logic). Acceptance is the precision/recall check in ``scripts/verify_match.py``,
run via ``databricks jobs submit`` on your workspace once
``silver.source_records`` has been populated by U1-U3.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from pipeline.config import Cfg
from pipeline.matching_core import categorize

# One shared endpoint for both indexes -- cheap to provision, avoids per-run
# endpoint churn. Index/endpoint names are fixed (not config-as-data) because
# they are Vector Search infrastructure, not pipeline logic; re-pointing at a
# new catalog only requires re-running build_indexes with the new Cfg.
_ENDPOINT_NAME = "er_ref_vs_endpoint"
_EMBEDDING_MODEL = "databricks-gte-large-en"
_DEFAULT_K = 10

# Columns fetched back from a VS query -- everything matching_core.categorize
# needs (mpn/manufacturer/commodity) plus the id used to dedup pairs.
_QUERY_COLUMNS = ["source_record_id", "mpn", "manufacturer", "commodity", "description", "specs"]


def _index_name(cfg: Cfg, embed_col: str) -> str:
    return cfg.tbl("silver", f"source_records_{embed_col}_idx")


def build_indexes(cfg: Cfg, wait_timeout_seconds: int = 2700) -> dict[str, str]:
    """Create the VS endpoint + Delta-Sync indexes used for blocking, if absent,
    and block until the ``func`` index (the one :func:`run` actually queries)
    reports ONLINE.

    Returns ``{"func": <index_fqn>, "desc": <index_fqn>}``. Safe to call every
    run: endpoint/index creation is skipped when they already exist, and
    waiting on an already-ONLINE index returns immediately. A freshly created
    ``TRIGGERED`` Delta-Sync index takes real wall-clock time to provision and
    complete its first sync -- querying it before that (as this function's
    caller does right after) fails with "index is not ready", so this is not
    optional.
    """
    import datetime

    from databricks.vector_search.client import VectorSearchClient

    vsc = VectorSearchClient()

    existing_endpoints = {e.get("name") for e in vsc.list_endpoints().get("endpoints", [])}
    if _ENDPOINT_NAME not in existing_endpoints:
        vsc.create_endpoint(name=_ENDPOINT_NAME, endpoint_type="STANDARD")

    source_table = cfg.tbl("silver", "source_records")
    indexes: dict[str, str] = {}
    func_index_obj = None
    for embed_col in ("func", "desc"):
        idx_name = _index_name(cfg, embed_col)
        indexes[embed_col] = idx_name
        try:
            index_obj = vsc.get_index(endpoint_name=_ENDPOINT_NAME, index_name=idx_name)
        except Exception:
            index_obj = vsc.create_delta_sync_index(
                endpoint_name=_ENDPOINT_NAME,
                index_name=idx_name,
                source_table_name=source_table,
                pipeline_type="TRIGGERED",
                primary_key="source_record_id",
                embedding_source_column=f"embed_{embed_col}",
                embedding_model_endpoint_name=_EMBEDDING_MODEL,
            )
        if embed_col == "func":
            func_index_obj = index_obj

    func_index_obj.wait_until_ready(timeout=datetime.timedelta(seconds=wait_timeout_seconds))
    return indexes


def _query_candidates(ws: Any, index_name: str, record: dict[str, Any], k: int) -> list[dict[str, Any]]:
    """Query one VS index for the top-k neighbours of ``record``, filtered to
    the same commodity (blocking) and excluding the record itself."""
    query_text = record.get("embed_func") or record.get("embed_desc") or record.get("description", "")
    filters = {"commodity": record["commodity"]} if record.get("commodity") else None

    raw = ws.vector_search_indexes.query_index(
        index_name=index_name,
        columns=_QUERY_COLUMNS,
        query_text=query_text,
        num_results=k,
        filters_json=json.dumps(filters) if filters else None,
    )
    col_names = [c.name for c in (raw.manifest.columns or [])]
    out = []
    for row in raw.result.data_array or []:
        rec = dict(zip(col_names, row))
        if rec.get("source_record_id") == record.get("source_record_id"):
            continue
        out.append(rec)
    return out


def run(cfg: Cfg, run_id: str, spark: Any = None, ws: Any = None, k: int = _DEFAULT_K) -> None:
    """Batch-match every ``silver.source_records`` row against the commodity-scoped
    ``embed_func`` index, categorize every candidate pair via
    :func:`pipeline.matching_core.categorize`, and (re)write ``match.pairs`` for
    this ``run_id``.

    ``spark``/``ws`` default to the ambient Databricks runtime (notebook/job
    context) when omitted so this can be called as ``match.run(cfg, run_id)``;
    both are accepted as parameters so a caller (or a future test with a mocked
    ``ws``) can inject them.
    """
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
    if ws is None:
        from databricks.sdk import WorkspaceClient

        ws = WorkspaceClient()

    indexes = build_indexes(cfg)
    func_index = indexes["func"]

    records = [row.asDict() for row in spark.table(cfg.tbl("silver", "source_records")).collect()]
    by_id = {r["source_record_id"]: r for r in records}

    seen_pairs: set[tuple[str, str]] = set()
    out_rows: list[dict[str, Any]] = []
    now = datetime.now(timezone.utc)

    for left in records:
        for cand in _query_candidates(ws, func_index, left, k):
            right_id = cand.get("source_record_id")
            if not right_id:
                continue
            pair_key = tuple(sorted((left["source_record_id"], right_id)))
            if pair_key in seen_pairs:
                continue
            seen_pairs.add(pair_key)

            right = by_id.get(right_id, cand)
            result = categorize(left, right, float(cand.get("score", 0.0)))

            out_rows.append({
                "run_id": run_id,
                "left_record_id": pair_key[0],
                "right_record_id": pair_key[1],
                "score": result["confidence"],
                "category": result["category"],
                "decision": result["decision"],
                "matched_via": "func",
                "created_ts": now,
            })

    if not out_rows:
        return

    pairs_df = spark.createDataFrame(out_rows)
    pairs_df.write.mode("overwrite").saveAsTable(cfg.tbl("match", "pairs"))
