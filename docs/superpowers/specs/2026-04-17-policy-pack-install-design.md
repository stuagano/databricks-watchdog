# Policy Pack Install Design

**Status:** Approved
**Date:** 2026-04-17

---

## Problem

Watchdog ships four industry policy packs (healthcare, financial, defense, general) in `library/`. Activating one requires manually copying YAML files into the engine, understanding the ontology merge format, and knowing to run a sync. Customer data teams can't do this without SA help.

## Goal

A single command a customer data team can run themselves. Pack is active on the next daily scan.

```bash
scripts/install_pack.sh healthcare --target fe-stable
```

## Non-Goals

- Installing multiple packs in one command
- Real-time activation (next scan is sufficient)
- A UI or notebook interface
- Pack versioning or dependency resolution

---

## Design

### Interface

```bash
scripts/install_pack.sh <pack> --target <target>

# Examples
scripts/install_pack.sh healthcare --target fe-stable
scripts/install_pack.sh financial  --target prod
scripts/install_pack.sh defense    --target staging
scripts/install_pack.sh general    --target fe-stable
```

Exits non-zero on any failure. Safe to run twice — idempotent.

---

### Step-by-Step Execution

**1. Validate arguments**
- Pack name must be one of: `healthcare`, `financial`, `defense`, `general`
- Target must be non-empty
- Library pack directory must exist: `library/<pack>/`
- All three pack files must be present: `ontology_classes.yml`, `rule_primitives.yml`, `policies.yml`

**2. Merge ontology classes**

`scripts/_merge_pack.py merge-classes <pack>` merges `library/<pack>/ontology_classes.yml` into `engine/ontologies/resource_classes.yml`.

Merge rules:
- Reads `derived_classes` block from both files
- For each class in the pack: if it doesn't exist in the engine file, appends it
- If a class with the same name already exists with identical content, skips (idempotent)
- If a class with the same name exists with different content, exits with an error (collision — requires manual resolution)
- Writes the merged file back to `engine/ontologies/resource_classes.yml`
- Prints each class added: `  ✓ PhiAsset`, `  ✓ EphiAsset`, etc.

**3. Merge rule primitives**

`scripts/_merge_pack.py merge-primitives <pack>` merges `library/<pack>/rule_primitives.yml` into `engine/ontologies/rule_primitives.yml`.

Same merge rules as classes — append new, skip identical, error on collision.

**4. Copy policies file**

Copies `library/<pack>/policies.yml` to `engine/policies/<pack>.yml`.

If `engine/policies/<pack>.yml` already exists and is identical, skips. If it exists and differs, overwrites (policy updates in the library should propagate on reinstall).

**5. Bundle deploy**

```bash
databricks bundle deploy -t <target>
```

Pushes the updated ontology and policy files to the workspace. Required for the engine to pick up the new ontology classes and rule primitives on the next scan.

**6. Sync policies to Delta**

```bash
scripts/sync_policies.sh <target>
```

Writes the new policies into the Delta `policies` table with `origin='yaml'`. The engine reads from this table at scan time.

**7. Print summary**

```
Healthcare pack installed → fe-stable

  Ontology classes:   4 added  (PhiAsset, EphiAsset, HipaaAuditAsset, DeIdentifiedDataset)
  Rule primitives:   11 added
  Policies:          10 synced → engine/policies/healthcare.yml

Active on next scan. To scan now:
  databricks bundle run watchdog_daily_scan -t fe-stable
```

---

### Files

| Action | File | Purpose |
|--------|------|---------|
| Create | `scripts/install_pack.sh` | Main script — argument validation, step orchestration, summary output |
| Create | `scripts/_merge_pack.py` | Python helper — YAML merge for ontology classes and rule primitives |
| Modify | `engine/ontologies/resource_classes.yml` | Pack ontology classes appended |
| Modify | `engine/ontologies/rule_primitives.yml` | Pack rule primitives appended |
| Create | `engine/policies/<pack>.yml` | Pack policies copied here |
| Create | `tests/unit/test_merge_pack.py` | Unit tests for merge logic |

---

### Error Handling

| Situation | Behaviour |
|-----------|-----------|
| Unknown pack name | Exit 1 with usage message |
| Missing library file | Exit 1 naming the missing file |
| Class name collision (different content) | Exit 1 naming the colliding class, no files modified |
| Primitive name collision (different content) | Exit 1 naming the colliding primitive, no files modified |
| Bundle deploy fails | Exit 1 with databricks output, ontology files already modified locally |
| Sync policies fails | Exit 1, bundle already deployed — user can re-run `sync_policies.sh` manually |

The script does not attempt rollback on bundle deploy or sync failure — partial state is recoverable by re-running.

---

### YAML Merge Format

**`engine/ontologies/resource_classes.yml`** — pack classes are appended under `derived_classes`:

```yaml
derived_classes:
  # ... existing classes ...

  # ── Healthcare (HIPAA) ──────────────────────
  PhiAsset:
    parent: ConfidentialAsset
    ...
  EphiAsset:
    parent: PhiAsset
    ...
```

**`engine/ontologies/rule_primitives.yml`** — pack primitives are appended under `primitives`:

```yaml
primitives:
  # ... existing primitives ...

  # ── Healthcare (HIPAA) ──────────────────────
  has_phi_steward:
    ...
```

A section comment is inserted before the pack's entries so the file stays readable.

---

### Testing

**`tests/unit/test_merge_pack.py`**

- `merge_classes`: new class added to empty engine file
- `merge_classes`: new class added to engine file with existing classes
- `merge_classes`: identical class skipped (idempotent)
- `merge_classes`: colliding class (different content) raises error, file unchanged
- `merge_primitives`: new primitive added
- `merge_primitives`: identical primitive skipped
- `merge_primitives`: colliding primitive raises error, file unchanged
- `copy_policies`: policies file copied correctly
- `copy_policies`: existing identical file skipped
- `copy_policies`: existing different file overwritten
