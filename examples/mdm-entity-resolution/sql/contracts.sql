-- ============================================================================
-- Contract DDL — the 5 seam tables that connect the pipeline's bounded units.
--
-- Created FIRST (before any unit) so every unit builds and tests against real
-- (empty) tables in parallel. Column definitions are copied verbatim from the
-- design spec's "Table contracts" section.
--
-- Catalog/schema follow the Cfg convention: <catalog>.mdm_ref_{bronze,silver,
-- match,mdm,gold}. Default catalog is `main`. To stamp onto another
-- workspace/domain, find/replace the catalog token below (it is the only
-- knob — see pipeline/config.py).
--
-- bronze.* tables are U1's shape (deliberately per-source) and are created by
-- Task 1 (gen_sources.write), not here.
--
-- COMMENT clauses below are real Unity Catalog metadata (visible in Catalog
-- Explorer), not just source comments -- this file is the only place that
-- documents the pipeline's data contract, so re-running it against an
-- existing table (a plain CREATE ... IF NOT EXISTS is a no-op on comments)
-- needs `ALTER TABLE ... ALTER COLUMN ... COMMENT` / `COMMENT ON TABLE` to
-- pick up a comment edit on tables that already exist.
-- ============================================================================

CREATE SCHEMA IF NOT EXISTS main.mdm_ref_bronze
  COMMENT 'Fuzzy-match ER reference pipeline (U1) -- raw per-source synthetic data (erp/plm/procurement) plus the gold-truth table used to score match precision/recall. Overwritten on every pipeline run. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';
CREATE SCHEMA IF NOT EXISTS main.mdm_ref_silver
  COMMENT 'Fuzzy-match ER reference pipeline (U3) -- standardized, canonical-shaped source records ready for matching. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';
CREATE SCHEMA IF NOT EXISTS main.mdm_ref_match
  COMMENT 'Fuzzy-match ER reference pipeline (U4) -- pairwise Vector Search match scores and decisions. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';
CREATE SCHEMA IF NOT EXISTS main.mdm_ref_mdm
  COMMENT 'Fuzzy-match ER reference pipeline (U5/U6/U8) -- persistent identity crosswalk, survivorship config, and the steward review queue. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';
CREATE SCHEMA IF NOT EXISTS main.mdm_ref_gold
  COMMENT 'Fuzzy-match ER reference pipeline (U6) -- golden Product/Part records reconciled across erp/plm/procurement. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';

-- ----------------------------------------------------------------------------
-- silver.source_records — U3 -> U4/U6 (the central contract)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.mdm_ref_silver.source_records (
  source_record_id  STRING NOT NULL COMMENT '{source_system}:{natural_key}, globally unique',
  source_system     STRING NOT NULL COMMENT 'erp | plm | procurement',
  natural_key       STRING           COMMENT 'the source system''s own id',
  mpn               STRING           COMMENT 'manufacturer part number (raw, as standardized from the source)',
  mpn_key           STRING           COMMENT 'normalized mpn: upper, strip separators -- the matching/dedup key',
  description       STRING,
  manufacturer      STRING,
  commodity         STRING           COMMENT 'part family, e.g. IC-opamp, PMT, connector -- used for VS blocking',
  lifecycle_status  STRING,
  specs             STRING,
  source_trust      INT              COMMENT 'source priority for survivorship (plm=3, erp=2, procurement=1)',
  attribute_ts      TIMESTAMP        COMMENT 'when these values were last updated (MOST_RECENT survivorship input)',
  embed_desc        STRING           COMMENT '{mpn} {description} -- kept for future same-part precision re-ranking',
  embed_mfr         STRING           COMMENT '{manufacturer} {mpn}',
  embed_func        STRING           COMMENT '{commodity} {description} {specs} -- the Vector Search blocking column',
  ingested_ts       TIMESTAMP
) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')
COMMENT 'ER reference pipeline U3 output: every source record standardized to one canonical shape (mpn/description/manufacturer/commodity/lifecycle_status/specs), ready for U4 Vector Search matching. One row per source_record_id per bronze source. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';

-- ----------------------------------------------------------------------------
-- match.pairs — U4 -> U5
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.mdm_ref_match.pairs (
  run_id           STRING NOT NULL,
  left_record_id   STRING NOT NULL COMMENT 'FK -> silver.source_records.source_record_id',
  right_record_id  STRING NOT NULL COMMENT 'FK -> silver.source_records.source_record_id',
  score            DOUBLE           COMMENT 'raw Vector Search similarity score',
  category         STRING           COMMENT 'EXACT | ALT_PART | FUNCTIONAL_EQUIVALENT | NO_MATCH -- see pipeline.matching_core.categorize',
  decision         STRING           COMMENT 'auto_match | needs_review | no_match',
  matched_via      STRING           COMMENT 'desc | mfr | func | sql_exact -- which embedding/index produced this candidate',
  created_ts       TIMESTAMP
) USING DELTA
COMMENT 'ER reference pipeline U4 output: every candidate pair the Vector Search blocking step produced, scored and categorized by pipeline.matching_core.categorize. auto_match rows feed U5 crosswalk clustering; needs_review rows are routed to the U8 steward queue. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';

-- ----------------------------------------------------------------------------
-- mdm.entity_crosswalk — U5 -> U6 (persistent identity)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.mdm_ref_mdm.entity_crosswalk (
  source_record_id  STRING NOT NULL COMMENT 'FK -> silver.source_records.source_record_id',
  entity_id         STRING NOT NULL COMMENT 'stable persistent id (e.g. ent-<smallest source_record_id in cluster>), survives re-runs',
  confidence        DOUBLE,
  first_seen_run    STRING           COMMENT 'run_id that first assigned this source_record_id an entity_id',
  first_seen_ts     TIMESTAMP,
  last_seen_run     STRING           COMMENT 'run_id of the most recent pipeline run that saw this record',
  status            STRING           COMMENT 'active | superseded (superseded on a merge -- see superseded_by)',
  superseded_by     STRING           COMMENT 'entity_id this one merged into when two clusters turned out to be the same entity; NULL when active'
) USING DELTA
COMMENT 'ER reference pipeline U5 output: the persistent identity crosswalk from raw source records to a stable golden entity_id. Recomputed every run via union-find over match.pairs auto_match pairs (pipeline.crosswalk), reusing the oldest known id per cluster so ids never churn across re-runs. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';

-- ----------------------------------------------------------------------------
-- mdm.survivorship_rules — U6 config
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.mdm_ref_mdm.survivorship_rules (
  canonical_field  STRING NOT NULL COMMENT 'description | manufacturer | lifecycle_status | ... -- a gold.entities column',
  strategy         STRING NOT NULL COMMENT 'MOST_RECENT | MOST_TRUSTED_SOURCE | MOST_COMPLETE | SOURCE_PRIORITY | LONGEST -- see pipeline.survivorship.pick',
  tiebreaker       STRING           COMMENT 'optional secondary strategy applied on a tie (documentation only -- pick() does not currently apply it)',
  notes            STRING
) USING DELTA
COMMENT 'ER reference pipeline U6 config-as-data: one field-merge strategy per canonical field, seeded from config/survivorship_rules.json by pipeline.survivorship.seed_rules before every run. Editing the JSON and re-running the pipeline is how a survivorship rule changes -- no code change needed. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';

-- ----------------------------------------------------------------------------
-- mdm.part_match_reviews — U8 output (steward queue)
--
-- Same contract as docs/gold-table-design.md's `part_match_reviews` (the
-- interactive Evaluator agent's review queue) -- the reference pipeline's
-- batch matcher (U4/U8) reuses the identical schema/queue shape within its
-- own mdm_ref_mdm schema, not the production catalog's PART_MATCH_REVIEW_TABLE
-- (which is app.yml-configurable and still a placeholder in this workspace).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.mdm_ref_mdm.part_match_reviews (
  review_id            STRING NOT NULL COMMENT 'uuid',
  source_mpn           STRING,
  source_description   STRING,
  matched_part_id      STRING           COMMENT 'NULL for NO_MATCH',
  matched_mpn          STRING,
  category             STRING           COMMENT 'EXACT | ALT_PART | FUNCTIONAL_EQUIVALENT | NO_MATCH',
  rationale            STRING,
  confidence           DOUBLE,
  needs_review         BOOLEAN,
  status               STRING           COMMENT 'pending | approved | rejected',
  candidates_reviewed  INT,
  created_ts           TIMESTAMP,
  reviewed_by          STRING           COMMENT 'set by a human steward on approve/reject',
  reviewed_ts          TIMESTAMP
) USING DELTA
COMMENT 'ER reference pipeline U8 output: needs_review match.pairs routed here for human stewardship, one row per candidate pair -- the same schema/role as the interactive agent''s review queue (docs/gold-table-design.md). A human approves or rejects each pending row. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';

-- ----------------------------------------------------------------------------
-- gold.entities — U6 output (golden records in the canonical model)
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS main.mdm_ref_gold.entities (
  entity_id             STRING NOT NULL COMMENT 'PK, FK -> mdm.entity_crosswalk.entity_id',
  mpn                   STRING,
  description           STRING,
  manufacturer          STRING,
  commodity             STRING,
  lifecycle_status      STRING,
  specs                 STRING,
  source_count          INT              COMMENT 'number of distinct source records that fed this golden record',
  contributing_sources  ARRAY<STRING>    COMMENT 'every source_record_id that fed this merge, sorted for determinism',
  field_provenance      MAP<STRING,STRING> COMMENT 'canonical_field -> winning source_record_id (audit trail for every merged value)',
  golden_ts             TIMESTAMP,
  run_id                STRING
) USING DELTA
COMMENT 'ER reference pipeline U6 output: the golden Product/Part records -- one row per stable entity_id, field-by-field merged across every contributing source per mdm.survivorship_rules (pipeline.survivorship). This is the reconciled, governed part master the whole pipeline exists to produce. Repo: stuagano/databricks-watchdog, examples/mdm-entity-resolution.';
