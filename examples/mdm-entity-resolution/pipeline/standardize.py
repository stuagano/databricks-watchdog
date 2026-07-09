"""Standardize Bronze -> Silver (U3).

Reads the three per-source bronze tables (erp/plm/procurement), applies the
source -> canonical mapping spec (``pipeline.mapping.load_spec``) to project
each source's raw columns onto the 6 canonical fields, and computes the
derived columns required by the ``silver.source_records`` contract
(``mpn_key``, ``source_trust``, ``embed_desc``/``embed_mfr``/``embed_func``,
``ingested_ts``). Config-as-data: this module never hardcodes a source's raw
column names -- it only ever reads them through the mapping spec.
"""

from __future__ import annotations

import re

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from pipeline.config import Cfg

# Priority order used for MOST_TRUSTED_SOURCE-style survivorship (U6) and
# recorded per-row here so downstream units never re-derive it.
_SOURCE_TRUST = {"plm": 3, "erp": 2, "procurement": 1}

_SOURCES = ("erp", "plm", "procurement")


def norm_mpn(s: str) -> str:
    """Normalize a manufacturer part number for matching/dedup.

    Strips whitespace, dashes, underscores, dots and slashes, then upper-cases.
    """
    return re.sub(r"[\s\-_./]", "", s or "").upper()


def _project_source(df: DataFrame, source: str, spec: dict[str, dict[str, str]]) -> DataFrame:
    """Select+rename ``df``'s raw columns onto the canonical field names for ``source``.

    Looks up each canonical field's raw column name in the mapping spec. Falls
    back to the canonical field name itself when the raw column named by the
    spec isn't present on ``df`` (e.g. a bronze source that already uses
    canonical-shaped column names), so this stays robust to sources with
    genuinely distinct raw schemas as well as ones that don't.
    """
    source_map = spec[source]
    select_exprs = []
    for field in ("mpn", "description", "manufacturer", "commodity", "lifecycle_status", "specs"):
        raw_col = source_map.get(field, field)
        if raw_col not in df.columns:
            raw_col = field
        select_exprs.append(F.col(raw_col).alias(field))
    for passthrough in ("source_record_id", "natural_key", "attribute_ts"):
        select_exprs.append(F.col(passthrough))
    return df.select(*select_exprs).withColumn("source_system", F.lit(source))


def to_silver(spark, cfg: Cfg, spec: dict[str, dict[str, str]], views: bool = False) -> DataFrame:
    """Build the `silver.source_records`-shaped DataFrame from the 3 bronze sources.

    When ``views`` is True, reads from temp views named ``bronze_{source}``
    (unit-test mode); otherwise reads the real bronze tables via ``cfg``.
    """
    projected = []
    for source in _SOURCES:
        if views:
            raw_df = spark.table(f"bronze_{source}")
        else:
            raw_df = spark.table(cfg.tbl("bronze", source))
        projected.append(_project_source(raw_df, source, spec))

    df = projected[0]
    for other in projected[1:]:
        df = df.unionByName(other)

    norm_mpn_udf = F.udf(norm_mpn, StringType())

    trust_map = F.create_map(*[x for src, trust in _SOURCE_TRUST.items() for x in (F.lit(src), F.lit(trust))])

    df = (
        df.withColumn("mpn_key", norm_mpn_udf(F.col("mpn")))
        .withColumn("source_trust", trust_map[F.col("source_system")])
        .withColumn("attribute_ts", F.to_timestamp(F.col("attribute_ts")))
        .withColumn("embed_desc", F.concat_ws(" ", F.col("mpn"), F.col("description")))
        .withColumn("embed_mfr", F.concat_ws(" ", F.col("manufacturer"), F.col("mpn")))
        .withColumn("embed_func", F.concat_ws(" ", F.col("commodity"), F.col("description"), F.col("specs")))
        .withColumn("ingested_ts", F.current_timestamp())
    )

    return df.select(
        "source_record_id",
        "source_system",
        "natural_key",
        "mpn",
        "mpn_key",
        "description",
        "manufacturer",
        "commodity",
        "lifecycle_status",
        "specs",
        "source_trust",
        "attribute_ts",
        "embed_desc",
        "embed_mfr",
        "embed_func",
        "ingested_ts",
    )


def run(spark, cfg: Cfg, spec: dict[str, dict[str, str]]) -> None:
    """Compute the silver DataFrame and write it to `silver.source_records`."""
    df = to_silver(spark, cfg, spec, views=False)
    df.write.mode("overwrite").saveAsTable(cfg.tbl("silver", "source_records"))
