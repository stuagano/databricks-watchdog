# p-watchdog-policies — Declarative Ontology and Starter Policy Library

**Date:** 2026-04-14 (updated 2026-05-28)
**Status:** ✅ Superseded — implemented in the standalone watchdog repo
**Branch:** `proposals/stuart-handoff/p-watchdog-policies`
**Dependencies:** `p-watchdog` deployed (provides `platform.watchdog` schema, scan pipeline, rule engine)

> **Superseded — see `~/Documents/Projects/databricks-watchdog/engine/policies/`, `engine/ontologies/rule_primitives.yml`, and `engine/ontologies/resource_classes.yml` (standalone repo `CustomerDataPlatform/watchdog`).**
>
> The declarative YAML library, rule primitives, and ontology all landed; counts diverged from this proposal:
>
> | | Proposed | Shipped |
> | --- | --- | --- |
> | Policy domains | 5 (security, cost, data_quality, regulatory, operational) | 9 (`access_governance`, `agent_governance`, `cost_governance`, `data_quality`, `drift_detection`, `medallion_governance`, `operational`, `security_governance`, `starter_policies`) — regulatory rolled into security/access; agent/drift/medallion are new |
> | Rule primitives | 26 | 16 rule types (per `docs/guide/reference/rule-types.md` and `CLAUDE.md`) — reorganized into fewer, more composable types |
> | Ontology classes | 28 | 8 base + 20+ derived (per `CLAUDE.md`) — same ballpark, different shape |
>
> Keep this file as historical record of the original policy design. Do not duplicate `policies/`, `rule_primitives.yml`, or `resource_classes.yml` into `customer-infra/bundles/watchdog/` — that would fork against the "no longer syncing into customer-infra" decision.

## Problem

The four MVP policies inlined in `p-watchdog` cover basic cloud hygiene but are too narrow for regulated industries. the customer operates under export controls, NRC compliance obligations, and radiation safety data requirements — these need policies that reflect that context, not generic cost-center tagging checks. Without an extensible library, Watchdog scans run daily but produce findings teams can't act on.

The second gap is operability: policies defined inline in Python require a code change and redeployment to modify. A declarative YAML library decouples policy authorship from deployment — compliance owners can propose rule changes via PR without touching the Python codebase.

## What this adds

A curated, version-controlled policy library across five governance domains, backed by 26 rule primitives and a 28-class resource ontology. Policy files load into `platform.watchdog.policies` at deploy time and are evaluated by the existing rule engine — no code changes required to add new rules.

### Policy domains

| Domain | File | Coverage |
|--------|------|----------|
| Security | `policies/security_governance.yml` | PII tagging, access control, sensitive column governance, export-controlled data |
| Cost | `policies/cost_governance.yml` | Cost center attribution, autotermination, orphaned compute, budget thresholds |
| Data quality | `policies/data_quality.yml` | DQM/LHM coverage, freshness SLAs, quarantine tag enforcement |
| Regulatory | `policies/regulatory.yml` | Retention obligations, NRC audit trail requirements, ITAR-tagged resource handling |
| Operational | `policies/operational_governance.yml` | Runtime versions, cluster policy compliance, job ownership, alerting coverage |

34 policies across 5 domains.

### Rule primitives (26)

Reusable check functions referenced by name in policy YAML. Primitives abstract tag inspection, metadata comparison, and cross-resource lookups so policy authors work at intent level.

| Category | Examples |
|----------|---------|
| Tag checks | `has_tag`, `tag_matches_pattern`, `tag_in_set`, `has_pii_columns` |
| Metadata | `metadata_equals`, `metadata_gte`, `metadata_in_set` |
| Cross-resource | `has_owner`, `owner_in_group`, `linked_to_cluster_policy` |
| Existence | `dqm_enabled`, `alert_configured`, `backup_enabled` |

### Ontology (28 resource classes)

Classes form an inheritance tree — a rule that applies to `DataAsset` evaluates against tables, views, and volumes without enumerating each type.

| Root class | Subclasses |
|------------|-----------|
| `DataAsset` | `ManagedTable`, `ExternalTable`, `View`, `Volume`, `StreamingTable` |
| `ComputeResource` | `InteractiveCluster`, `JobCluster`, `SQLWarehouse`, `Pipeline` |
| `WorkflowAsset` | `Job`, `Pipeline`, `Dashboard` |
| `GovernanceAsset` | `ServicePrincipal`, `UCCatalog`, `UCSchema` |

**Tag-triggered classes:** `PIIDataAsset`, `ExportControlledAsset`, `RadiationSafetyDataset` are assigned by tag detection, not resource type. A table tagged `pii=true` becomes both `ManagedTable` and `PIIDataAsset`, making it subject to PII-specific policies automatically without listing it anywhere.

## File structure

```
bundles/watchdog-bundle/
├── ontologies/
│   ├── resource_classes.yml       — 28 class definitions with inheritance and tag-based triggers
│   ├── compliance_domains.yml     — domain registry with owners and escalation paths
│   └── rule_primitives.yml        — 26 primitive definitions with parameter schemas
└── policies/
    ├── security_governance.yml    — POL-S001–S012
    ├── cost_governance.yml        — POL-C001–C008
    ├── data_quality.yml           — POL-Q001–Q007
    ├── regulatory.yml             — POL-R001–R007
    └── operational_governance.yml — POL-O001–O006
```

## Policy YAML format

```yaml
# policies/security_governance.yml (excerpt)
- policy_id: POL-S001
  name: pii-requires-steward-and-retention
  applies_to: PIIDataAsset
  domain: security
  severity: critical
  description: >
    PII data assets must have an identified data steward and a retention
    classification tag. Unowned PII is the most common source of data
    breach exposure.
  rule:
    all_of:
      - has_tag: {key: data_steward}
      - has_tag: {key: retention_class, value_in: [30d, 90d, 1y, 7y]}
```

Rules use `all_of`, `any_of`, `not`, and direct primitive references. The rule engine evaluates the full tree per resource per policy — `all_of` collects all failures rather than short-circuiting, so a single scan identifies every gap.

## How policies load

At `databricks bundle deploy`, the `policy_loader` task reads all `policies/*.yml` files and upserts them into `platform.watchdog.policies`. The `origin` field is set to `yaml`. Policies created interactively via MCP tools (`origin=user`) are preserved. Every change is appended to `policies_history` for audit trail.

To disable a policy without deleting it: set `active: false` in the YAML file and redeploy.

## Activation sequence

1. Review `policies/*.yml` — set `active: false` on any rules not applicable to your environment (e.g., rules that reference alerts you haven't configured yet).
2. Confirm `ontologies/resource_classes.yml` includes any customer-specific classification needs (e.g., `RadiationSensorData` as a tag-triggered subclass of `DataAsset`).
3. Populate `policies/regulatory.yml` with confirmed NRC retention periods and ITAR classification requirements — leave blanks rather than guessing.
4. Configure `extra_sensitive_patterns` in bundle vars with export-control identifiers.
5. Deploy via `p-watchdog` bundle — policies load automatically.
6. After first scan: `SELECT policy_id, active, origin FROM platform.watchdog.policies ORDER BY domain` to confirm all 34 loaded.

## Code-ready defaults

These items need the customer input eventually but should not block coding. Implement with the defaults below and mark each with `# TODO(compliance): confirm` in the YAML:

| Decision | Default to code with | Flag |
|----------|---------------------|------|
| NRC retention periods | 7 years for all radiation/nuclear data, 3 years for operational records | `# TODO(compliance): confirm NRC retention by facility type` |
| ITAR tag pattern | `tag_key: export_control, value: itar` | `# TODO(security): confirm ITAR tag name with security team` |
| Export control domain | Keep under `security` as `POL-S010–S012` | Revisit if > 3 export control rules emerge |
| `critical` vs `warning` split | `critical` = PII unowned, ITAR-tagged without clearance, no cost_center. `warning` = missing tags that have owners, outdated runtimes | `# TODO(compliance): validate paging thresholds` |

V4C ships code with these defaults. the customer compliance team reviews the YAML files directly — policy changes are a `git commit`, not a code change.

## TODOs (post-coding)

- [ ] Confirm NRC retention periods by facility type (NRC vs. DOE): update `regulatory.yml` retention_class values
- [ ] Get confirmed ITAR tag name from security team: update `extra_sensitive_patterns` and `POL-S010` tag key
- [ ] Decide: export control as sixth domain? — trigger if rule count exceeds 3 in `security_governance.yml`
- [ ] Validate severity thresholds after first alpha scan: tune based on actual violation volume and escalation paths
