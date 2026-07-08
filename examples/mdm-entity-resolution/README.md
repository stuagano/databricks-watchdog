# Fuzzy-Match Entity Resolution — Reference Pipeline

A Databricks-native, **batch** reference architecture that reconciles messy records
from multiple source systems into governed **golden records** — using Vector Search
for fuzzy matching, a persistent identity crosswalk, and config-driven survivorship.
One opinionated golden path, decomposed into independently-buildable units connected
by Delta-table contracts.

This example lives inside [databricks-watchdog](../../) because its U7 quality gates
(dedup / reconcile / completeness against `gold.entities`) are a thin wrapper around
Watchdog's own pure MDM check builders/interpreters
([`engine/src/watchdog/mdm_checks.py`](../../engine/src/watchdog/mdm_checks.py)) —
this is a worked example of Watchdog's governance checks used as the quality gate
for a real downstream data pipeline, not just UC metadata policies. Verified live end
to end against a real Databricks workspace: precision 1.0 / recall 1.0 against gold
truth, all quality gates passing, and identical `entity_crosswalk` output across a
re-run (id stability).

## Golden path

```
[U1] synth sources ─▶ bronze.{erp,plm,procurement}_parts
[U2] canonical model (.json → schema + source→canonical field map)
[U3] standardize   ─▶ silver.source_records   (canonical-shaped + lineage + embed cols)
[U4] match/cluster ─▶ match.pairs             (record↔record: score, category, decision)
[U5] crosswalk     ─▶ mdm.entity_crosswalk    (source_record_id → stable entity_id)
[U6] survivorship  ─▶ gold.entities           (golden records + field_provenance)
[U7] quality gates  ·  [U8] steward queue  ·  [U9] orchestration (batch job)
[U10] reference dataset + demo notebook + docs
```

## Layout

| Path | Purpose |
|------|---------|
| `pipeline/config.py` | `Cfg` — the single catalog/schema parameter boundary; every unit composes FQNs via `Cfg.tbl` |
| `sql/contracts.sql` | The 5 seam-table `CREATE TABLE`s (+ `CREATE SCHEMA`) — created first so units build in parallel |
| `pipeline/*.py` | One module per bounded unit (U1–U9) |
| `config/*.json`, `config/*.yml` | Config-as-data: canonical model, source→canonical mapping, survivorship rules, quality checks |
| `tests/` | Pure-logic + schema-conformance tests (`pytest`, local SparkSession via `conftest.py`) |
| `jobs/` | Databricks Asset Bundle multi-task job (U9 orchestration) |
| `demo/` | Narrated reference notebook (U10) |
| `scripts/` | Integration acceptance checks run on the workspace |

## Config-as-data (the only knobs)

Nothing about a domain is hardcoded in logic. To point the pipeline at a new
workspace or stamp it onto a new domain:

- **Catalog/schema** — set `Cfg(catalog=...)` (default `main`;
  schemas are `mdm_ref_{bronze,silver,match,mdm,gold}`). Also update the catalog token
  in `sql/contracts.sql`.
- **Source→canonical mapping** — `config/mapping_spec.json` (U2).
- **Survivorship rules** — `config/survivorship_rules.json` / `mdm.survivorship_rules` (U6).
- **Quality checks** — `config/mdm_checks.yml` (U7).

## Stamping onto a new domain

The reference pipeline is deliberately built so **stamping it onto a new domain (or a new
workspace) is a config edit, never a code change**. The narrated walkthrough in
[`demo/er_reference_pipeline.py`](demo/er_reference_pipeline.py) exercises every knob below, in
order, end to end.

1. **Point at your workspace/catalog** — `Cfg(catalog="your_catalog")` (`pipeline/config.py`); also
   find/replace the catalog token in `sql/contracts.sql` and run it once to create the 5 seam
   tables + schemas.
2. **Replace the synthetic sources** — delete the `gen_sources.generate()` / `gen_sources.write()`
   call (U1) and instead land your real source systems into `bronze.{your_source_names}` tables,
   one per system, in whatever raw shape they arrive in.
3. **Edit the mapping spec** — `config/mapping_spec.json`: for each of your source systems, map its
   raw column names onto the 6 canonical fields (`mpn`, `description`, `manufacturer`, `commodity`,
   `lifecycle_status`, `specs` — or your domain's equivalents; see `config/canonical_model.json`).
   `pipeline.mapping.validate()` tells you exactly which `(source, field)` pairs are still missing.
4. **Edit the survivorship rules** — `config/survivorship_rules.json`: one strategy
   (`MOST_RECENT` / `MOST_TRUSTED_SOURCE` / `MOST_COMPLETE` / `SOURCE_PRIORITY` / `LONGEST`) per
   canonical field, reflecting which of your sources is the system of record for each field.
5. **Edit the quality checks** — `config/mdm_checks.yml`: dedup/reconcile/completeness checks
   against your `gold` table, using the same logical `schema`/`table` keys `Cfg` resolves.
6. **Re-run** — `python pipeline/run_all.py --run-id <id> --catalog <your_catalog>` locally, or
   deploy `jobs/pipeline_job.yml` as a Databricks Job for the scheduled batch run. Nothing else in
   `pipeline/*.py` needs to change — every unit only ever reads the config above.

## Table contracts

The seam tables (`silver.source_records`, `match.pairs`, `mdm.entity_crosswalk`,
`mdm.survivorship_rules`, `gold.entities`) are defined verbatim in
[`sql/contracts.sql`](sql/contracts.sql). Bronze source tables are U1's shape
(deliberately per-source) and are created by `gen_sources.write`.

**Grounding invariant:** every id in `match.pairs`, `entity_crosswalk`, and
`gold.entities` traces to a real `silver.source_records.source_record_id`. No
fabricated ids. LLM adjudication is not in the batch path.

## Config API

```python
from pipeline.config import Cfg

c = Cfg()                                   # default catalog
c = Cfg(catalog="my_catalog")               # override for a new workspace
c.tbl("silver", "source_records")           # -> 'my_catalog.mdm_ref_silver.source_records'
c.schema("gold")                            # -> 'my_catalog.mdm_ref_gold'
c.gold                                       # -> 'my_catalog.mdm_ref_gold'  (property)
```

## Testing

```bash
cd examples/mdm-entity-resolution
PYTHONPATH=../../engine/src pytest tests/ -v   # pure-logic + local-Spark schema-conformance tests
```

`PYTHONPATH` puts Watchdog's `engine/src` on the path so `pipeline.quality`'s
`from watchdog.mdm_checks import ...` resolves; `conftest.py` also adds it
automatically when it finds `../../engine/src` relative to itself.

Vector Search / Jobs / live-Delta integration checks live in `scripts/` and run on
your own Databricks workspace (`databricks jobs submit`, see `scripts/verify_match.py`
and `scripts/verify_e2e.py`'s module docstrings for the exact JSON).
