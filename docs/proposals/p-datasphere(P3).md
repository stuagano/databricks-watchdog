# SAP Datasphere → Databricks: Data Pipeline Proposal

**Branch:** `proposals/stuart-handoff/p-datasphere`
**Bundle:** `bundles/sap-datasphere/`
**Status:** Ready for data team implementation

---

## Summary

SAP operational data (GL actuals, cost center postings, purchase orders, material
master) needs to land in the Databricks platform for financial analytics, supply
chain reporting, and AI use cases (PCBA obsolescence scoring, Genie Space
natural-language queries).

**Recommended landing pattern (confirmed with Arniz, 2026-04-02):**
SAP Datasphere **RepFlow** writes to ADLS Gen2. Databricks picks it up via Auto Loader.
There is no direct Datasphere → Delta connector today. The Delta Sharing connector
exists via SAP BDC but requires an additional SAP license — not in scope.

RepFlow is a native Datasphere replication tool that may not carry additional cost
(confirm with SAP account team). If RepFlow is not available or adds cost, the
fallback is a new ADF pipeline owned by the data platform team (see below).

---

## Architecture

```
SAP S/4HANA / Datasphere
    │
    ├── [existing] ADF (Fabric team) → OneLake      (Power BI / Fabric — unchanged)
    │
    └── [new]      Datasphere RepFlow                ← preferred
                   (or ADF data platform team)       ← fallback
                       │
                       ▼
                   ADLS Gen2
                   stmedianraw / container: sap
                       │
                       ▼  Auto Loader (01_ingest_datasphere.py)
                   bronze.sap.*   (Delta, append)
                       │
                       ▼  02_transform_silver.py
                   silver.sap.*   (Delta, typed)
                       │
                       ▼  03_serve_gold.py + ai_query()
                   gold.sap.*     (Delta, analytics-ready)
                       │
                       ▼
                   Genie Space + PCBA Obsolescence + Financial Analytics
```

The Databricks bundle (`01_ingest_datasphere.py`) reads from ADLS via Auto Loader
using `adls_root` as a parameter. Whether RepFlow or ADF lands files there is
transparent to Databricks — only the landing path matters.

---

## What the data team needs to build (landing side)

### Storage target

| Property | Value |
|---|---|
| Storage account | `stmedianraw` (or equivalent in platform subscription) |
| Container | `sap` |
| Format | **Parquet** |
| Compression | Snappy (ADF default) |
| Path pattern | `sap/{entity}/year={YYYY}/month={MM}/day={DD}/{entity}_{timestamp}.parquet` |

The date-partitioned path lets Auto Loader pick up each day's files cleanly on
the next scheduled run. The timestamp suffix prevents file collisions on re-runs.

### Entities and connector recommendations

| Entity | ADF Connector | Load type | SAP source |
|---|---|---|---|
| `gl_items` | **SAP ODP** | Delta (incremental) | ODP extractor `0FI_GL_14` or `0FI_GL_4` |
| `cost_center_actuals` | **SAP ODP** | Delta (incremental) | ODP extractor `0CO_OM_CCA_9` |
| `purchase_orders` | **SAP Table** | Full load + watermark on `AEDAT` | Tables `EKKO` + `EKPO` (join in ADF) |
| `material_master` | **SAP Table** | Full load | Tables `MARA` + `MAKT` (join in ADF, language=EN) |

**Why ODP for GL and cost center?**
SAP ODP (Operational Data Provisioning) has built-in delta tracking — it returns
only rows that changed since the last extraction run. This is the correct mechanism
for financial posting data. The alternative (full load with watermark on BUDAT/CPUDT)
produces duplicates when postings are reversed or backdated.

**Why SAP Table for PO and materials?**
POs and materials change infrequently and the full dataset is manageable. A full
load + MERGE in Auto Loader handles updates correctly without needing ODP delta
state. Simpler to set up and debug.

### Column naming

**Use SAP technical field names** (BUKRS, BELNR, MATNR, etc.) — do not rename to
display names in ADF. The silver notebook (`02_transform_silver.py`) does all
renaming and typing. Keeping SAP names in bronze preserves lineage and makes
it possible to trace any silver column back to its SAP source field.

### ADF pipeline trigger → Databricks job

After ADF completes, trigger the Databricks pipeline job automatically:

```
ADF Pipeline (SAP extraction)
    └── On Success → Web Activity
            POST {databricks_host}/api/2.1/jobs/runs/now
            Body: { "job_id": <datasphere_pipeline_job_id> }
            Auth: Bearer {databricks_sp_token}
```

This eliminates the need to schedule the Databricks job independently. ADF
triggers it when fresh data has landed. The Databricks job does nothing if
Auto Loader finds no new files.

---

## What the Databricks bundle does (already built)

`bundles/sap-datasphere/` is deployed and ready. Once `adls_root` is configured
pointing at the ADLS path above, the pipeline runs end-to-end:

| Job | What it does |
|---|---|
| `datasphere_pipeline` | Auto Loader → bronze → silver → gold (daily) |
| `datasphere_platform_setup` | One-time: registers Watchdog policies + Ontos semantic links + Genie Space |

**Deployment sequence after ADF is ready:**

```bash
# 1. Set adls_root and deploy
databricks bundle deploy --target munich \
  --var adls_root="abfss://sap@stmedianraw.dfs.core.windows.net"

# 2. Register with platform (run once)
databricks bundle run datasphere_platform_setup --target munich

# 3. Trigger first pipeline run manually to validate
databricks bundle run datasphere_pipeline --target munich

# 4. Wire up ADF trigger → Databricks job (see above)
# 5. Unpause the ADF pipeline schedule
```

---

## Prerequisites

### SAP side (basis team)

- [ ] ODP framework enabled on S/4HANA (transaction `RODPS_REPL_TEST` to verify)
- [ ] ODP extractor `0FI_GL_14` activated (GL line items)
- [ ] ODP extractor `0CO_OM_CCA_9` activated (cost center actuals)
- [ ] RFC user created for ADF with read-only authorisation objects:
  - `S_RFC` for RFC access
  - `S_TABU_DIS` for table reads (EKKO, EKPO, MARA, MAKT)
  - `RODPS_REPL` for ODP extraction

### Azure side (data platform team)

- [ ] ADLS Gen2 storage account available in platform subscription
  (the `stmedian*` accounts created by workspace Terraform may be usable —
  check with the Azure team; alternatively create a dedicated `stmedianraw`)
- [ ] ADF instance created in platform subscription
- [ ] ADF linked service: SAP S/4HANA (RFC/HANA credentials from Key Vault)
- [ ] ADF linked service: ADLS Gen2 (managed identity preferred)
- [ ] ADF pipelines created per entity (one Copy Activity per entity)
- [ ] ADF trigger configured (daily schedule + Databricks job trigger on success)

### Databricks side (already done)

- [ ] `bundles/sap-datasphere/` deployed to munich workspace
- [ ] `adls_root` variable set in bundle config or deployment command
- [ ] Databricks SP has Storage Blob Data Reader on the ADLS container
- [ ] `datasphere_platform_setup` job run once to register with Watchdog + Ontos

---

## Open questions

1. **RepFlow cost**: Is RepFlow included in the customer's Datasphere license or an add-on?
   Confirm with SAP account team before committing to this as the landing mechanism.
   If additional cost, fall back to new ADF pipeline in the data platform subscription.

2. **ODP availability**: Is ODP enabled on the customer's S/4HANA system? If not, SAP
   Table connector with CPUDT watermark is the fallback for GL — confirm with basis.

2. **Storage account**: Should we use an existing `stmedian*` account for raw SAP
   landing, or create a dedicated `stmedianraw`? A dedicated account is cleaner
   (separate access control, no DBFS collision) but adds an Azure resource.

3. **GL history depth**: How many years of GL history does the first full load need?
   ODP initial load can be scoped by posting date — confirm the cutoff with Finance.

4. **Material master language**: `MAKT` (material descriptions) is language-dependent.
   ADF filter should be `SPRAS = 'EN'`. Confirm English is the authoritative language
   for Munich material descriptions.

5. **Cost center plan data**: `02_transform_silver.py` keeps both actuals (WRTTP=04)
   and plan (WRTTP=01) in `silver.sap.cost_objects`. Confirm whether plan data from
   the ODP extractor is in scope or actuals-only for the first iteration.

---

## Future state: Delta Sharing

Once SAP Datasphere has its Data Products feature enabled and the customer's Datasphere
team publishes data products, the ADF pipeline can be replaced by a Delta Share
registration:

```sql
CREATE CATALOG sap_datasphere
USING SHARE customer_datasphere.financial_operations;
```

The `adls_root` variable would be removed and `01_ingest_datasphere.py` would
read from the Delta Share catalog directly. Silver, gold, governance, and Genie
Space are unaffected — only the ingest step changes. The ADF pipeline would be
decommissioned.

Until that is available, this ADF → ADLS pattern is the production path.
