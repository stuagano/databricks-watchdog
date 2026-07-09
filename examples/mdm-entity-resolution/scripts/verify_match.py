"""Acceptance check for the batch matcher (U4) — precision/recall of `auto_match`
pairs against the gold-truth table (`bronze.truth`, from U1).

Integration script: reads live Delta tables in the target catalog, so it needs
a Spark session in a Databricks runtime (notebook cell or job task) run AFTER
`pipeline.match.run(cfg, run_id)` has populated `match.pairs`. Not part of the
local pytest suite.

Usage (from a Databricks notebook / job task):

    from scripts.verify_match import verify
    verify(spark, cfg, run_id)

Or as a script entrypoint (assumes an ambient `spark` via `SparkSession.builder`,
e.g. inside a Databricks job task):

    python scripts/verify_match.py <run_id>
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

# Run standalone as its own spark_python_task (see run_all.py's identical
# bootstrap for why __file__ needs a sys.argv[0] fallback under that harness).
try:
    _THIS_FILE = Path(__file__)
except NameError:
    _THIS_FILE = Path(sys.argv[0])
_REPO_ROOT = _THIS_FILE.resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from pipeline.config import Cfg  # noqa: E402 (must follow the sys.path bootstrap above)

DEFAULT_MIN_PRECISION = 0.95
DEFAULT_MIN_RECALL = 0.85


def _truth_pairs(spark: Any, cfg: Cfg) -> set[tuple[str, str]]:
    """Every unordered pair of source_record_ids that the gold-truth generator
    (U1's `bronze.truth`) says refer to the same real-world part."""
    rows = spark.table(cfg.tbl("bronze", "truth")).collect()
    pairs: set[tuple[str, str]] = set()
    for row in rows:
        members = sorted(row["members"])
        for i in range(len(members)):
            for j in range(i + 1, len(members)):
                pairs.add((members[i], members[j]))
    return pairs


def verify(
    spark: Any,
    cfg: Cfg,
    run_id: str,
    min_precision: float = DEFAULT_MIN_PRECISION,
    min_recall: float = DEFAULT_MIN_RECALL,
) -> dict[str, Any]:
    """Compute precision/recall of this run's `auto_match` pairs vs `bronze.truth`
    and assert the plan's acceptance bar (P>=0.95, R>=0.85 by default).

    Returns the metrics dict on success; raises AssertionError otherwise.
    """
    truth = _truth_pairs(spark, cfg)

    auto_rows = (
        spark.table(cfg.tbl("match", "pairs"))
        .where(f"run_id = '{run_id}'")
        .where("decision = 'auto_match'")
        .select("left_record_id", "right_record_id")
        .collect()
    )
    predicted = {tuple(sorted((r["left_record_id"], r["right_record_id"]))) for r in auto_rows}

    true_positives = len(predicted & truth)
    precision = true_positives / len(predicted) if predicted else 0.0
    recall = true_positives / len(truth) if truth else 0.0

    metrics = {
        "run_id": run_id,
        "precision": precision,
        "recall": recall,
        "predicted_count": len(predicted),
        "truth_count": len(truth),
        "true_positives": true_positives,
    }
    assert precision >= min_precision, f"precision {precision:.3f} < {min_precision} ({metrics})"
    assert recall >= min_recall, f"recall {recall:.3f} < {min_recall} ({metrics})"
    return metrics


if __name__ == "__main__":
    import sys

    from pyspark.sql import SparkSession

    _run_id = sys.argv[1] if len(sys.argv) > 1 else "latest"
    _spark = SparkSession.builder.getOrCreate()
    print(verify(_spark, Cfg(), _run_id))
