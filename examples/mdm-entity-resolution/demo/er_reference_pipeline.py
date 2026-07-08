# Databricks notebook source
# MAGIC %md
# MAGIC # Fuzzy-Match Entity Resolution — Reference Pipeline
# MAGIC ### Bronze → Silver → Match → Crosswalk → Survivorship → Gold → Quality → Stewardship
# MAGIC
# MAGIC **What this is:** a narrated, cell-by-cell walk through the reference pipeline's whole
# MAGIC golden path — a Databricks-native, **batch** master-data pipeline that reconciles messy
# MAGIC records from multiple source systems into governed **golden records**, using Vector Search
# MAGIC for fuzzy matching, a persistent identity crosswalk, and config-driven survivorship.
# MAGIC
# MAGIC ```
# MAGIC [U1] synth sources ─▶ bronze.{erp,plm,procurement}_parts
# MAGIC [U2] canonical model (.json → schema + source→canonical field map)
# MAGIC [U3] standardize   ─▶ silver.source_records   (canonical-shaped + lineage + embed cols)
# MAGIC [U4] match/cluster ─▶ match.pairs             (record↔record: score, category, decision)
# MAGIC [U5] crosswalk     ─▶ mdm.entity_crosswalk    (source_record_id → stable entity_id)
# MAGIC [U6] survivorship  ─▶ gold.entities           (golden records + field_provenance)
# MAGIC [U7] quality gates  ·  [U8] steward queue  ·  [U9] orchestration (batch job)
# MAGIC ```
# MAGIC
# MAGIC **Grounding invariant.** Every id in `match.pairs`, `entity_crosswalk`, and `gold.entities`
# MAGIC traces to a real `silver.source_records.source_record_id` — nothing here is fabricated.
# MAGIC There is no LLM in this batch path; adjudication is deterministic (`pipeline.matching_core.
# MAGIC categorize`), and the same scoring core is shared with the interactive agent.
# MAGIC
# MAGIC **Everything below runs live** against real Unity Catalog Delta tables and a real Vector
# MAGIC Search index — nothing is mocked. Each cell below runs one bounded unit and `display()`s the
# MAGIC table it produced, in the same order `pipeline/run_all.py` (the U9 job entrypoint) runs them.
# MAGIC
# MAGIC To stamp this reference architecture onto a **new domain** (not just parts), see the
# MAGIC ["Stamping onto a new domain" recipe](../README.md#stamping-onto-a-new-domain) in the README —
# MAGIC every knob this notebook exercises (catalog, mapping spec, survivorship rules, quality
# MAGIC checks) is config-as-data, not hardcoded logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup — install pipeline dependencies
# MAGIC Matches `jobs/pipeline_job.yml`'s `er_ref_env` so this notebook and the scheduled batch job
# MAGIC run against the same dependency set.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch>=0.40 databricks-sdk>=0.30.0 pyyaml>=6.0 -q

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Configuration — the catalog is the only knob
# MAGIC `Cfg` is the single parameter boundary for the whole pipeline (`pipeline/config.py`) — every
# MAGIC unit composes fully-qualified table names through it, so nothing below ever hardcodes a
# MAGIC catalog or schema string. `run_id` is threaded through every stage's output table so this
# MAGIC notebook can be re-run without manual cleanup (every stage overwrites, or for the steward
# MAGIC queue appends to, its own table for this `run_id`).

# COMMAND ----------

import os
import sys
import uuid
from pathlib import Path

from pyspark.sql import functions as F

# This notebook lives at examples/mdm-entity-resolution/demo/ — a Databricks Repos checkout runs
# a notebook with its own directory as the working directory, so the parent directory is the
# example's own root that `pipeline/` and `config/` hang off of (mirrors the sys.path bootstrap
# `pipeline/run_all.py` does for the job/pytest contexts).
REPO_ROOT = Path(os.getcwd()).parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# U7's quality gates (`pipeline.quality`) import Watchdog's pure `mdm_checks` builders/interpreters
# directly -- make that importable too. `watchdog` lives at <repo_root>/engine/src/watchdog/, two
# levels up from this example's own root.
_WATCHDOG_SRC = REPO_ROOT.parent.parent / "engine" / "src"
if _WATCHDOG_SRC.is_dir() and str(_WATCHDOG_SRC) not in sys.path:
    sys.path.insert(0, str(_WATCHDOG_SRC))

from pipeline.config import Cfg  # noqa: E402 (must follow the sys.path bootstrap above)

cfg = Cfg()  # default: main — pass catalog=... to point at another workspace
run_id = f"demo-{uuid.uuid4().hex[:8]}"

print(f"catalog = {cfg.catalog}")
print(f"run_id  = {run_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## U1 — Synthetic multi-source generator
# MAGIC
# MAGIC Three independent "bronze" source systems (`erp`, `plm`, `procurement`) describing
# MAGIC overlapping electronic parts, with realistic cross-source noise: MPN case/dash formatting
# MAGIC differences, conflicting `lifecycle_status`, and differing `attribute_ts` per source (PLM is
# MAGIC always freshest). Deterministic — seeded, so `generate()` is reproducible — and a `truth`
# MAGIC table records which source records refer to the same real-world part (gold truth), so U4/U5
# MAGIC can be scored for precision/recall and id stability.
# MAGIC
# MAGIC To stamp this onto a real domain, this cell is the one you delete — replace it with your
# MAGIC real source connections writing into `bronze.{your_sources}` (see the README recipe).

# COMMAND ----------

from pipeline import gen_sources

data = gen_sources.generate()
gen_sources.write(spark, cfg, data)

print(f"erp: {len(data['erp'])} rows · plm: {len(data['plm'])} rows · "
      f"procurement: {len(data['procurement'])} rows · truth entities: {len(data['truth'])}")
display(spark.table(cfg.tbl("bronze", "erp")).orderBy("commodity").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## U2 / U3 — canonical mapping + standardize bronze → silver
# MAGIC
# MAGIC The canonical model (`config/canonical_model.json`) defines the 6 fields every golden record
# MAGIC carries. `config/mapping_spec.json` is the single declarative record of how each source's raw
# MAGIC columns map onto those canonical fields — standardize (U3) reads this spec instead of
# MAGIC hardcoding per-source column names, so a new source is a config edit, not a code change.
# MAGIC
# MAGIC The output, `silver.source_records`, is the pipeline's central contract: every downstream unit
# MAGIC (U4 matching, U6 survivorship) reads only this table, in this canonical shape.

# COMMAND ----------

from pipeline import mapping, standardize

spec = mapping.load_spec(str(REPO_ROOT / "config" / "mapping_spec.json"))
mapping_errors = mapping.validate(spec, ["erp", "plm", "procurement"])
assert not mapping_errors, f"mapping spec is missing coverage: {mapping_errors}"

standardize.run(spark, cfg, spec)

display(
    spark.table(cfg.tbl("silver", "source_records"))
    .orderBy("commodity", "source_system")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Seed the survivorship rules (config-as-data)
# MAGIC U6 reads its field-merge strategy per canonical field from the live `mdm.survivorship_rules`
# MAGIC table, not directly from JSON — `config/survivorship_rules.json` is the source of truth,
# MAGIC loaded via `survivorship.seed_rules` (the same call `pipeline.run_all.run_all` makes before
# MAGIC every U6 stage, so a rules-JSON edit rolls out on the next run/job — no separate deploy step).

# COMMAND ----------

from pipeline import survivorship

survivorship.seed_rules(spark, cfg)

display(spark.table(cfg.tbl("mdm", "survivorship_rules")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## U4 — Vector Search blocking + batch matcher
# MAGIC
# MAGIC Provisions (idempotently) a Vector Search endpoint plus a Delta-Sync index over
# MAGIC `silver.source_records.embed_func` (commodity + description + specs — the functional-similarity
# MAGIC view), so matching is commodity-scoped blocking rather than an O(n²) cross-join. For every
# MAGIC silver record, queries its top-k nearest neighbours, scores each candidate pair with the
# MAGIC shared, dependency-free `pipeline.matching_core.categorize`, and writes `match.pairs`.
# MAGIC
# MAGIC `category` is one of `EXACT | ALT_PART | FUNCTIONAL_EQUIVALENT | NO_MATCH`; `decision` is the
# MAGIC routing verdict (`auto_match | needs_review | no_match`) that U5/U8 act on.

# COMMAND ----------

from pipeline import match

match.build_indexes(cfg)  # creates the endpoint + Delta-Sync indexes on first run; a no-op after
match.run(cfg, run_id, spark=spark)

display(
    spark.table(cfg.tbl("match", "pairs"))
    .filter(f"run_id = '{run_id}'")
    .orderBy(F.desc("score"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## U8 — steward queue: routing `needs_review` pairs to a human
# MAGIC
# MAGIC `FUNCTIONAL_EQUIVALENT` matches (candidate obsolescence replacements from a *different*
# MAGIC manufacturer) and any lower-confidence match are never auto-confirmed — they're shaped into
# MAGIC `part_match_reviews` rows for a steward to approve or reject. This reuses the existing Data
# MAGIC Catalog app's stewardship queue; no new UI is needed.

# COMMAND ----------

from pipeline import steward

steward.publish(spark, cfg, run_id)

display(
    spark.table(cfg.tbl("mdm", "part_match_reviews"))
    .filter("status = 'pending'")
    .orderBy(F.desc("created_ts"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## U5 — persistent identity crosswalk
# MAGIC
# MAGIC Turns U4's pairwise `auto_match` decisions into a stable identity: every `silver.source_records`
# MAGIC row is assigned a persistent `entity_id` via union-find over `auto_match` pairs
# MAGIC (`connected_components`), reusing the oldest known id per cluster from a prior run
# MAGIC (`stable_assign`) so ids never churn across re-runs — even when a merge collapses two
# MAGIC previously-separate clusters (the older id wins; the abandoned id is tombstoned
# MAGIC `status='superseded'` with a `superseded_by` redirect).

# COMMAND ----------

from pipeline import crosswalk

crosswalk.assign(spark, cfg, run_id)

display(spark.table(cfg.tbl("mdm", "entity_crosswalk")).orderBy("entity_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## U6 — survivorship merge → golden records
# MAGIC
# MAGIC Collapses every source record belonging to one `entity_id` into a single golden record,
# MAGIC resolving field-level conflicts with the rule set seeded above (`MOST_RECENT`,
# MAGIC `MOST_TRUSTED_SOURCE`, `MOST_COMPLETE`, `SOURCE_PRIORITY`) — one strategy per canonical field,
# MAGIC not a single rule for the whole record. `field_provenance` records exactly which source record
# MAGIC won each field, so every golden value is traceable, and `contributing_sources` lists every
# MAGIC source record that fed the merge.

# COMMAND ----------

from pipeline import survivorship

survivorship.run(spark, cfg, run_id)

display(spark.table(cfg.tbl("gold", "entities")).orderBy("commodity", "entity_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## U7 — quality gates
# MAGIC
# MAGIC A thin wrapper around Watchdog's config-driven MDM checks (`config/mdm_checks.yml`): dedup,
# MAGIC reconciliation, and completeness checks run against `gold.entities`. This is the same gate the
# MAGIC batch job (U9) fails on if anything doesn't pass — a bad merge should never ship silently.

# COMMAND ----------

from pipeline import quality

issues = quality.run(spark, cfg, run_id)
for issue in issues:
    status = "PASS" if issue.get("passed") else "FAIL"
    print(f"[{status}] {issue.get('id')}: {issue.get('name')} — {issue.get('detail', '')}")

failed = [issue for issue in issues if not issue.get("passed", False)]
assert not failed, f"{len(failed)}/{len(issues)} quality checks FAILED for run_id={run_id!r}: {failed}"
print(f"\nAll {len(issues)} quality checks passed for run_id={run_id!r}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bonus: id stability across a re-run
# MAGIC
# MAGIC The whole point of a persistent crosswalk (U5) is that `entity_id`s don't churn just because
# MAGIC the pipeline ran again. Re-running the full golden path end to end (via `pipeline.run_all`, the
# MAGIC same entrypoint the batch job uses) for a **second** `run_id` should leave every active
# MAGIC `source_record_id → entity_id` mapping unchanged — this is exactly what `scripts/verify_e2e.py`
# MAGIC asserts on the workspace via two `databricks jobs submit` runs; here it's inline so you can see
# MAGIC it happen.

# COMMAND ----------

from pipeline.run_all import run_all

xwalk_before = {
    row["source_record_id"]: row["entity_id"]
    for row in spark.table(cfg.tbl("mdm", "entity_crosswalk")).filter("status = 'active'").collect()
}

run_id_2 = f"demo-{uuid.uuid4().hex[:8]}"
run_all(run_id_2, cfg=cfg, spark=spark)

xwalk_after = {
    row["source_record_id"]: row["entity_id"]
    for row in spark.table(cfg.tbl("mdm", "entity_crosswalk")).filter("status = 'active'").collect()
}

print(f"re-ran as run_id={run_id_2!r}")
print(f"entity_crosswalk stable across re-run: {xwalk_before == xwalk_after}")
assert xwalk_before == xwalk_after, "entity_crosswalk changed across an idempotent re-run (id instability)"

# COMMAND ----------

# MAGIC %md
# MAGIC ## What this proves
# MAGIC
# MAGIC | Capability | Where it ran, above |
# MAGIC |---|---|
# MAGIC | Ingest & standardize multi-source data | U1 → U3, config-driven mapping (`mapping_spec.json`) |
# MAGIC | Fuzzy match at scale | U4 — Vector Search, commodity-scoped blocking, shared scoring core |
# MAGIC | Stable persistent identity | U5 — union-find + prior-run-aware id reuse, survives merges |
# MAGIC | Config-driven survivorship | U6 — per-field strategy, full provenance, no hardcoded merge rule |
# MAGIC | Governed quality gates | U7 — dedup / reconcile / completeness against `gold.entities` |
# MAGIC | Human-in-the-loop stewardship | U8 — `needs_review` pairs routed to the existing steward queue |
# MAGIC | Idempotent batch orchestration | U9 (`pipeline/run_all.py`) — one `run_id`, safe to re-run daily |
# MAGIC
# MAGIC **Every layer runs natively on Databricks** — Unity Catalog Delta tables for governance and
# MAGIC lineage, Vector Search for matching compute, and Watchdog for quality gates. Nothing here is a
# MAGIC system alongside the lakehouse: no data movement, no second database to secure, no per-record
# MAGIC license.
# MAGIC
# MAGIC This reference pipeline reconciles electronic parts (a domain with real cross-source noise:
# MAGIC MPN formatting drift, conflicting lifecycle status, revision/packaging variants). The identical
# MAGIC pattern —
# MAGIC canonical model → standardize → match → crosswalk → survivorship → quality → stewardship —
# MAGIC stamps onto **Customer**, **Vendor/Supplier**, and any other domain that needs one trusted,
# MAGIC de-duplicated master. See the README's
# MAGIC ["Stamping onto a new domain"](../README.md#stamping-onto-a-new-domain) recipe for the concrete
# MAGIC steps.
