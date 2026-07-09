"""Stewardship surface (U8) — routes ``needs_review`` pairs to the human queue.

The **integration contract** with the Data Catalog app: :func:`publish` inserts
one row per ``needs_review`` :mod:`pipeline.match` pair into
``part_match_reviews`` (schema in ``docs/gold-table-design.md``), reusing the
existing catalog "Part Matches" stewardship queue — no new UI is needed.

Row shaping is pure Python (:func:`to_review_row`, stdlib only) so it is unit
tested without Spark; :func:`publish` is the Spark/Delta-I/O wrapper that reads
``match.pairs``, shapes every ``needs_review`` row, and appends them to
``part_match_reviews``. It is NOT exercised by the local pytest suite -- it
requires a live Databricks workspace with real ``match.pairs`` /
``silver.source_records`` tables (see ``pipeline.match`` for the same pattern).
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pipeline.config import Cfg

# Matches sql/contracts.sql's part_match_reviews DDL verbatim. Every freshly
# published row has reviewed_by/reviewed_ts always None (they're set later by
# a steward's approve/reject), so Spark's schema inference from the row dicts
# alone fails with CANNOT_DETERMINE_TYPE -- pass this explicit schema instead.
_REVIEW_ROW_SCHEMA_DDL = (
    "review_id STRING, source_mpn STRING, source_description STRING, "
    "matched_part_id STRING, matched_mpn STRING, category STRING, rationale STRING, "
    "confidence DOUBLE, needs_review BOOLEAN, status STRING, candidates_reviewed INT, "
    "created_ts TIMESTAMP, reviewed_by STRING, reviewed_ts TIMESTAMP"
)


def to_review_row(pair: dict[str, Any], by_id: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Shape one ``match.pairs`` row into a ``part_match_reviews`` row.

    ``pair`` is a ``match.pairs``-shaped dict (at least ``left_record_id``,
    ``right_record_id``, ``score``, ``category``). ``by_id`` maps
    ``source_record_id -> silver.source_records``-shaped dict, grounding
    ``source_mpn``/``matched_mpn`` in real records rather than inventing them.

    The left record is always treated as the incoming/source side and the
    right record as the matched/resolved side. ``matched_part_id`` prefers an
    explicit ``part_id`` on the right record, falling back to its
    ``source_record_id`` (the right record's own key) when no separate part
    identity has been assigned yet (e.g. before U5 crosswalk runs).

    Every row produced here is a ``needs_review`` row (only ``needs_review``
    pairs are routed to the steward queue -- see :func:`publish`), so
    ``needs_review`` is always ``True`` and ``status`` always starts
    ``'pending'`` for a human steward to confirm or reject.
    """
    left = by_id.get(pair["left_record_id"], {})
    right = by_id.get(pair["right_record_id"], {})

    matched_part_id = right.get("part_id") or pair["right_record_id"]
    confidence = float(pair.get("score", 0.0))
    category = pair.get("category", "")

    return {
        "review_id": str(uuid.uuid4()),
        "source_mpn": left.get("mpn"),
        "source_description": left.get("description"),
        "matched_part_id": matched_part_id,
        "matched_mpn": right.get("mpn"),
        "category": category,
        "rationale": f"{category} match, confidence {confidence:.3f}",
        "confidence": confidence,
        "needs_review": True,
        "status": "pending",
        "candidates_reviewed": 1,
        "created_ts": datetime.now(timezone.utc),
        "reviewed_by": None,
        "reviewed_ts": None,
    }


def publish(spark: Any, cfg: Cfg, run_id: str) -> None:
    """Route this run's ``needs_review`` ``match.pairs`` rows into
    ``part_match_reviews`` for the ``run_id``.

    Reads ``match.pairs`` filtered to ``decision = 'needs_review'`` and
    ``run_id``, builds a ``source_record_id -> record`` lookup from
    ``silver.source_records`` (the only place mpns/ids are grounded), shapes
    every pair with :func:`to_review_row`, and appends the resulting rows to
    ``part_match_reviews``. A no-op when there are no ``needs_review`` pairs
    for this run.
    """
    if spark is None:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

    pairs_rows = (
        spark.table(cfg.tbl("match", "pairs"))
        .filter("decision = 'needs_review'")
        .filter(f"run_id = '{run_id}'")
        .collect()
    )
    if not pairs_rows:
        return

    records = spark.table(cfg.tbl("silver", "source_records")).collect()
    by_id = {row["source_record_id"]: row.asDict() for row in records}

    out_rows = [to_review_row(row.asDict(), by_id) for row in pairs_rows]

    reviews_df = spark.createDataFrame(out_rows, schema=_REVIEW_ROW_SCHEMA_DDL)
    reviews_df.write.mode("append").saveAsTable(cfg.tbl("mdm", "part_match_reviews"))
