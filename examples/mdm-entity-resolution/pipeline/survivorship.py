"""Survivorship merge (U6).

Collapses every ``silver.source_records`` row belonging to one persistent
``entity_id`` (per U5's ``entity_crosswalk``) into a single golden record in
``gold.entities``, resolving field-level conflicts with a small,
config-driven rule set (``survivorship_rules.json`` / ``mdm.survivorship_rules``).

Two pure, dependency-free functions carry all the logic and are unit-tested
without Spark:

- :func:`pick` -- applies one strategy to one field across a set of source
  records, returning the winning value *and* the ``source_record_id`` it came
  from (so every golden field is traceable back to a real record).
- :func:`merge_entity` -- applies a full rule set (one strategy per field)
  across all of an entity's source records, assembling the golden dict plus
  a ``field_provenance`` map and ``contributing_sources`` list.

:func:`run` is the Spark/Delta-I/O wrapper (reads ``entity_crosswalk`` +
``silver.source_records`` + ``survivorship_rules``, groups by ``entity_id``,
calls the two pure functions, writes ``gold.entities``) and is exercised on
the Databricks workspace, not by the local pytest suite.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.config import Cfg

# Canonical golden fields (config/canonical_model.json's `part` product),
# excluding `mpn` which is carried through unmodified as the entity's key
# identifying attribute rather than survivorship-merged.
GOLDEN_FIELDS = ["mpn", "description", "manufacturer", "commodity", "lifecycle_status", "specs"]

DEFAULT_RULES_PATH = Path(__file__).parent.parent / "config" / "survivorship_rules.json"

# Matches sql/contracts.sql's gold.entities DDL verbatim. Without this,
# Spark's schema inference maps Python int (source_count = len(...)) to
# LongType, which Delta's schema enforcement rejects on overwrite against the
# table's declared `source_count INT` (DELTA_FAILED_TO_MERGE_FIELDS) -- the
# same class of inference mismatch already fixed for steward.publish and
# crosswalk.assign, just a type conflict instead of an all-null column.
_GOLD_ROW_SCHEMA_DDL = (
    "entity_id STRING, mpn STRING, description STRING, manufacturer STRING, "
    "commodity STRING, lifecycle_status STRING, specs STRING, source_count INT, "
    "contributing_sources ARRAY<STRING>, field_provenance MAP<STRING,STRING>, "
    "golden_ts TIMESTAMP, run_id STRING"
)


def seed_rules(spark: Any, cfg: Cfg, path: Path | str = DEFAULT_RULES_PATH) -> None:
    """Load ``config/survivorship_rules.json`` into the live ``mdm.survivorship_rules``
    table -- the config-as-data source :func:`run` actually reads at merge time.

    Idempotent (overwrite): safe to call on every pipeline run: re-running it
    after an edit to the JSON is exactly how a survivorship-rule change is
    rolled out (see README's "stamp a new domain" recipe).
    """
    rules = json.loads(Path(path).read_text())["rules"]
    spark.createDataFrame(rules).write.mode("overwrite").saveAsTable(cfg.tbl("mdm", "survivorship_rules"))


def pick(field: str, strategy: str, records: list[dict]) -> tuple[Any, str | None]:
    """Apply one survivorship ``strategy`` to ``field`` across ``records``.

    Returns ``(winning_value, winning_source_record_id)``. Records missing the
    field (``None`` or absent) are never chosen over a record that has it,
    unless every candidate is missing it -- in which case the first record's
    (missing) value is returned so callers always get *some* deterministic
    answer rather than an exception.

    Strategies:
    - ``MOST_RECENT`` -- max ``attribute_ts`` (lexicographic/ISO-comparable).
    - ``MOST_TRUSTED_SOURCE`` -- max ``source_trust``.
    - ``MOST_COMPLETE`` -- longest non-null string value for ``field``.
    - ``SOURCE_PRIORITY`` -- first record whose ``source_system`` appears
      earliest in the configured priority order (``plm`` > ``erp`` >
      ``procurement`` when no explicit order is supplied).
    - ``LONGEST`` -- longest string value for ``field`` (alias of
      ``MOST_COMPLETE`` that doesn't special-case null).
    """
    if not records:
        return None, None

    candidates = [r for r in records if r.get(field) is not None]
    pool = candidates or records

    if strategy == "MOST_RECENT":
        winner = max(pool, key=lambda r: (r.get("attribute_ts") or ""))
    elif strategy == "MOST_TRUSTED_SOURCE":
        winner = max(pool, key=lambda r: (r.get("source_trust") if r.get("source_trust") is not None else -1))
    elif strategy in ("MOST_COMPLETE", "LONGEST"):
        winner = max(pool, key=lambda r: len(str(r.get(field))) if r.get(field) is not None else -1)
    elif strategy == "SOURCE_PRIORITY":
        order = ["plm", "erp", "procurement"]

        def priority_rank(r: dict) -> int:
            source = r.get("source_system") or r["source_record_id"].split(":", 1)[0]
            return order.index(source) if source in order else len(order)

        winner = min(pool, key=priority_rank)
    else:
        raise ValueError(f"unknown survivorship strategy: {strategy!r}")

    return winner.get(field), winner.get("source_record_id")


def merge_entity(records: list[dict], rules: dict[str, str]) -> dict:
    """Apply ``rules`` (``canonical_field -> strategy``) across ``records``.

    Returns a golden dict with one key per rule field, plus:

    - ``field_provenance`` -- ``canonical_field -> winning source_record_id``.
    - ``contributing_sources`` -- every ``source_record_id`` that fed the merge,
      sorted for determinism.
    - ``source_count`` -- ``len(contributing_sources)``.
    """
    golden: dict[str, Any] = {}
    field_provenance: dict[str, str] = {}

    for field, strategy in rules.items():
        value, winning_source = pick(field, strategy, records)
        golden[field] = value
        if winning_source is not None:
            field_provenance[field] = winning_source

    contributing_sources = sorted({r["source_record_id"] for r in records})

    golden["field_provenance"] = field_provenance
    golden["contributing_sources"] = contributing_sources
    golden["source_count"] = len(contributing_sources)
    return golden


def run(spark: Any, cfg: Cfg, run_id: str) -> None:
    """Group ``silver.source_records`` by ``entity_id`` (via ``entity_crosswalk``),
    merge each group with :func:`merge_entity`, and (re)write ``gold.entities``.
    """
    now = datetime.now(timezone.utc)

    rules_rows = spark.table(cfg.tbl("mdm", "survivorship_rules")).collect()
    rules = {row["canonical_field"]: row["strategy"] for row in rules_rows}

    xwalk_rows = (
        spark.table(cfg.tbl("mdm", "entity_crosswalk"))
        .filter("status = 'active'")
        .select("source_record_id", "entity_id")
        .collect()
    )
    entity_by_record = {row["source_record_id"]: row["entity_id"] for row in xwalk_rows}

    silver_rows = [row.asDict() for row in spark.table(cfg.tbl("silver", "source_records")).collect()]

    records_by_entity: dict[str, list[dict]] = {}
    for record in silver_rows:
        entity_id = entity_by_record.get(record["source_record_id"])
        if entity_id is None:
            continue
        records_by_entity.setdefault(entity_id, []).append(record)

    out_rows: list[dict[str, Any]] = []
    for entity_id, records in records_by_entity.items():
        golden = merge_entity(records, rules)
        out_rows.append({
            "entity_id": entity_id,
            "mpn": golden.get("mpn"),
            "description": golden.get("description"),
            "manufacturer": golden.get("manufacturer"),
            "commodity": golden.get("commodity"),
            "lifecycle_status": golden.get("lifecycle_status"),
            "specs": golden.get("specs"),
            "source_count": golden["source_count"],
            "contributing_sources": golden["contributing_sources"],
            "field_provenance": golden["field_provenance"],
            "golden_ts": now,
            "run_id": run_id,
        })

    df = spark.createDataFrame(out_rows, schema=_GOLD_ROW_SCHEMA_DDL)
    df.write.mode("overwrite").saveAsTable(cfg.tbl("gold", "entities"))
