# p-uc-isolation — Workspace Isolation & the customer Hub Publishing Path

**Date:** 2026-05-11
**Status:** Coded on branch
**Branch:** `stuart/d-uc-isolation-P2`
**Implements:** Defense-in-depth workspace isolation (UC + IAM + network) and a governed publishing path from spoke `gold` layers into the `hub_catalog` catalog.
**Resolves:** b-uc01, b-uc02, b-uc03, b-uc04, b-uc05
**Roadmap:** Phase 2 (Platform Maturity) — governance + multi-workspace data sharing on the regional metastore
**Priority:** Tier 2 — Required for DOE/HPI defense-in-depth and for the publishing model that lets spokes consume one another's curated data

## Glossary — Disambiguating Overloaded Terms

"Hub", "catalog", and "metastore" appear across orthogonal axes in this proposal. Write them precisely.

### Axis 1 — Unity Catalog hierarchy (Databricks-native)

```
Databricks Account
└── Metastore              (1 per region per account)
    └── UC Catalog         (e.g., alpha_gold, hub_catalog)
        └── Schema         (e.g., finance, supply_chain)
            └── Table / Volume / Function
```

| Term | Meaning | the customer's instance |
| --- | --- | --- |
| **Metastore** | Region-scoped container of UC identities, grants, lineage, audit | `ucm-regional` (one, in `deployment/regional-infra/main.tf:35`) |
| **UC catalog** | A securable owning schemas/tables. Top of UC's 3-level namespace. | `alpha_bronze/silver/gold`, `spoke_a_bronze/silver/gold`, …, `hub_catalog` |
| **UC Discover** | Databricks-native UI for browsing the metastore | Used at the customer as discovery layer; replaced Ontos |

### Axis 2 — Workspaces

| Workspace | Role | Network position | UC catalogs owned |
| --- | --- | --- | --- |
| `webauth` | SSO/auth only, no data | Network hub | None |
| `alpha`, `beta` | Dev / test | Spokes | `{ws}_bronze/silver/gold` |
| `spoke_a`, `spoke_b` | Production data | Spokes | `{ws}_bronze/silver/gold` |
| `sandbox`, `ml_spoke` | Specialty production | Spokes | `{ws}_bronze/silver/gold` |
| `promotion` | Publishing path only — runs `sp-publisher` jobs | Spoke (data plane) | Creates the `hub_catalog` UC catalog; bound RW to it |

### Axis 3 — Network topology (Azure VNets — separate from UC)

| Term | Meaning in the customer | Where |
| --- | --- | --- |
| **Network hub** | Shared VNet hosting webauth + central services | `vnet-dataplatform-prod-region`; webauth subnet `10.202.0.0/24` |
| **Network spoke** | Per-workspace subnets in the same shared VNet (BYO-VNet pattern) | `10.202.8.0/21` (spoke_a) … `10.202.40.0/21` (promotion) |
| **Network policy** | Databricks egress restriction (`hub_network_policy_id` vs `spoke_network_policy_id`) | `deployment/regional-infra/` |
| **NCC** | Network Connectivity Config — serverless compute → private endpoint rules | `ncc-regional-metastore`, one per region |
| **PE** | Azure Private Endpoint — classic compute → private path to storage | Per workspace, per storage account |

**Crucial:** "Network hub" (webauth + shared services) and **`hub_catalog`** (the UC catalog) are different things that share the word. They do not overlap. The `promotion` workspace is a *network* spoke even though it is the *data publishing* hub.

### Axis 4 — Governance surfaces (different layers, different code)

| Surface | What it is | Code location | Owner |
| --- | --- | --- | --- |
| **Unity Catalog** | Databricks-native grants, bindings, lineage, audit logs (`system.access.audit`) | Databricks platform | Databricks; you configure via Terraform |
| **Watchdog** | the customer's policy engine — reads `system.access.audit` + UC metadata, evaluates YAML policies, writes results to `platform.watchdog.*` Delta tables | `bundles/watchdog/`; standalone repo | the customer + Stuart |
| **Customer Catalog (FE app)** | User-facing data catalog UI — discovery, dataset detail, governance dashboard, curation requests, exception management | `bundles/customer-catalog/` | the customer |
| **UC Discover** | Databricks-native discovery UI | Databricks platform | Databricks |
| **Sigma** | End-user BI / exploration | External SaaS | the customer business users |
| **Genie** | AI/BI text-to-SQL | Databricks platform | the customer configures spaces |

### Disambiguation rules

1. **"Metastore"** = the regional singleton `ucm-regional`. Do not use this word for anything else.
2. **"Catalog"** unqualified is ambiguous. Always say:
   - **"UC catalog"** or the name itself (`hub_catalog`, `alpha_gold`) for the Databricks securable
   - **"Customer Catalog"** for the FE app at `bundles/customer-catalog/`
3. **"Hub"** unqualified is ambiguous. Always say:
   - **"network hub"** for the webauth workspace + shared VNet pattern
   - **`hub_catalog`** (italicized or code-formatted) for the UC catalog
   - **"hub-and-spoke publishing path"** for the architectural pattern (`promotion` workspace + `hub_catalog` + spoke bindings)
4. **"Workspace"** = a Databricks workspace. Includes `promotion`. Do not say "the hub workspace" when you mean either webauth (auth hub) or promotion (publishing hub) — those are distinct roles.

### Acronyms used in this document

**Azure / network plane**

| Acronym | Meaning |
| --- | --- |
| **MI** | Managed Identity — an Azure workload identity attached to a resource (e.g., a Databricks access connector). Authenticates to Azure storage as itself; no client secret, no key rotation. UC issues short-lived tokens to the MI on behalf of compute. |
| **VNet** | Virtual Network |
| **PE** | Private Endpoint — Azure resource providing a private IP for a target service inside a VNet |
| **NSG** | Network Security Group |
| **RG** | Resource Group |
| **DNS** | Domain Name System |
| **CIDR** | Classless Inter-Domain Routing (the `/21`, `/24` notation) |
| **BYO** | Bring Your Own (as in BYO VNet — customer-supplied) |
| **ADLS** | Azure Data Lake Storage (the Gen2 hierarchical filesystem on top of blob) |
| **DFS** | Data Lake Storage Gen2 endpoint (`*.dfs.core.windows.net`) |
| **abfss://** | Azure Blob File System Secure — the URI scheme for ADLS Gen2 |

**Databricks / Unity Catalog**

| Acronym | Meaning |
| --- | --- |
| **UC** | Unity Catalog (Databricks' metadata + governance layer) |
| **NCC** | Network Connectivity Config (Databricks PE-rule mechanism for serverless compute) |
| **DAB** | Databricks Asset Bundle (deployment/job-as-code format) |
| **DLT** | Delta Live Tables (declarative pipeline framework) |
| **SP** | Service Principal (Entra/Databricks identity for automation) |
| **SAT** | Security Analysis Tool (Databricks compliance scanner) |
| **D2D** | Databricks-to-Databricks (the in-account variant of Delta Sharing) |
| **RO / RW** | `BINDING_TYPE_READ_ONLY` / `BINDING_TYPE_READ_WRITE` (UC binding types) |
| **SSO** | Single Sign-On |
| **OIDC** | OpenID Connect (federation; used in `shared/github-oidc/`) |
| **CMK** | Customer-Managed Keys (workspace encryption keys you control) |
| **KV** | Key Vault (Azure) |

**Infrastructure / process**

| Acronym | Meaning |
| --- | --- |
| **TF** | Terraform |
| **CI** | Continuous Integration (in our case, GitHub Actions) |
| **IAM** | Identity and Access Management |
| **DR** | Disaster Recovery |
| **FE** | Frontend (as in "Customer Catalog FE app") |
| **WIP** | Work In Progress (commits, branches) |

**Compliance / data classification**

| Acronym | Meaning |
| --- | --- |
| **PII** | Personally Identifiable Information |
| **PHI** | Protected Health Information |
| **ITAR** | International Traffic in Arms Regulations (US export control) |
| **HIPAA** | Health Insurance Portability and Accountability Act |
| **PCI_DSS** | Payment Card Industry Data Security Standard |
| **DOE** | US Department of Energy (compliance baseline for nuclear/radiation) |
| **HPI** | High-Performance Infrastructure (in the customer's CLAUDE.md alongside DOE) |

## Architecture at a Glance

```
                METASTORE: ucm-regional  (1 per region — shared identity, audit, admin)
═══════════════════════════════════════════════════════════════════════════════════════════════

  SPOKES  (per-workspace UC catalogs, ISOLATED, bound RW to their own workspace only)

   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │  alpha  │ │  beta   │ │ spoke_a │ │  spoke_b  │ │ sandbox │ │  ml_spoke  │
   │ *_gold  │ │ *_gold  │ │ *_gold  │ │ *_gold  │ │ *_gold  │ │ *_gold  │
   └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘ └────┬────┘
        │           │           │           │           │           │
        │   bound READ_ONLY → promotion  (auto-wired via remote state)
        ▼           ▼           ▼           ▼           ▼           ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                       PROMOTION WORKSPACE                           │
   │  The only writer to hub_catalog.  DAB pipeline, run-as sp-publisher. │
   │  No users.  No catalogs of its own.  A one-way valve.               │
   └────────────────────────────────┬────────────────────────────────────┘
                                    │   WRITE  (catalog RW binding + MODIFY grant)
                                    ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                  HUB:  hub_catalog  (UC catalog, ISOLATED)           │
   │  ─────────────────────────────────────────────────────────────────  │
   │  Workspace bindings:  RW → promotion  (only)                        │
   │                       RO → alpha, beta, spoke_a, spoke_b, sandbox,    │
   │                            ml_spoke  (auto-wired via remote state)     │
   │  Grants:  publishers-region     → MODIFY / CREATE_TABLE / ...      │
   │           catalog-readers-live   → SELECT / BROWSE / READ_VOLUME    │
   └────────────────────────────────┬────────────────────────────────────┘
                                    │   READ  (any spoke user via RO binding)
                                    ▼
   ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐
   │  alpha  │ │  beta   │ │ spoke_a │ │  spoke_b  │ │ sandbox │ │  ml_spoke  │
   │  user   │ │  user   │ │  user   │ │  user   │ │  user   │ │  user   │
   └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘ └─────────┘

═══════════════════════════════════════════════════════════════════════════════════════════════
  AZURE PLANE                                  ENFORCEMENT  (4 independent attestations)

  spoke storage      hub storage               UC grants + workspace bindings (Terraform)
   (per-ws MI)        (hub MI only)            Watchdog detective scan of system.access.audit
   private,           private,                 Audit query: who wrote to hub_catalog.*
   deny-all           deny-all                 CI grant-drift assertion (terraform plan)
   PE per spoke       PE per spoke + NCC
```

The asymmetry is intentional: many readers, one writer. The promotion workspace is the choke point; `sp-publisher` is the only principal with `MODIFY`; `hub_catalog` is `RW`-bound to one workspace only. Spokes are isolated per-workspace at every layer (catalog, storage account, MI).

## The Seven Control Layers

An OSI-style view of the controls. Each layer has a distinct responsibility, its own resource type, its own failure mode, and its own bypass. **Defense in depth** comes from the next layer catching what the previous one missed: any single misconfiguration is contained.

```
                    ┌─────────────────────────────────────┐
                    │  Reviewer / Auditor                 │
                    └──────────────────┬──────────────────┘
                                       ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │ Layer 7 — Observability                                             │
   │   system.access.audit • Watchdog • CI grant-drift • SAT             │
   │   Catches misconfigs in L1–L6 after the fact                        │
   └──────────────────┬──────────────────────────────────────────────────┘
                      ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │ Layer 6 — UC Grants                                                 │
   │   databricks_grant — controls WHAT a principal may do               │
   │   Bypass: metastore admin                                           │
   └──────────────────┬──────────────────────────────────────────────────┘
                      ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │ Layer 5 — UC Securable Visibility                                   │
   │   isolation_mode + workspace_binding — WHICH workspaces see/touch   │
   │   Enforced BEFORE L6 — binding wins over grant                      │
   │   Bypass: OPEN mode; metastore admin                                │
   └──────────────────┬──────────────────────────────────────────────────┘
                      ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │ Layer 4 — Identity                                                  │
   │   Entra app + Databricks SP + group membership — WHO the principal  │
   │   Bypass: stolen credential (sp-publisher has no client_secret)     │
   └──────────────────┬──────────────────────────────────────────────────┘
                      ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │ Layer 3 — Workspace Compute                                         │
   │   Metastore assignment, cluster policy, ESC profile, network policy │
   │   Bypass: workspace admin escalation                                │
   └──────────────────┬──────────────────────────────────────────────────┘
                      ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │ Layer 2 — Azure IAM                                                 │
   │   Managed Identity + Storage Blob Data Contributor (per-MI scope)   │
   │   Bypass: Subscription Owner / Key Vault holder                     │
   └──────────────────┬──────────────────────────────────────────────────┘
                      ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │ Layer 1 — Network                                                   │
   │   VNet • PE • NCC PE rule • storage firewall deny-all • DNS         │
   │   Bypass: misconfig exposing a public IP                            │
   └──────────────────┬──────────────────────────────────────────────────┘
                      ▼
                    ┌──────────────────────────────────┐
                    │  Data: hub_catalog.finance.txns   │
                    │  (bytes in sthubregional) │
                    └──────────────────────────────────┘
```

### Per-layer control table

| L | Layer | What we put in place | What it enforces | Bypass | Compensated by |
| --- | --- | --- | --- | --- | --- |
| 1 | Network | `public_network_access_enabled = false`, deny-all firewall, PE per workspace, NCC PE rules, privatelink DNS | No public IP can reach hub storage | Misconfigured firewall exception | L2 — no MI has RBAC anyway |
| 2 | Azure IAM | Per-workspace access connector MI; `Storage Blob Data Contributor` scoped per-storage-account; hub MI is sole holder on hub storage | Only the hub MI reads hub bytes | Subscription Owner adds a role | L1 still blocks reachability; L5 still gates UC access |
| 3 | Workspace | Metastore assignment, cluster policy, `enhanced_security_compliance` (HIPAA + PCI_DSS), spoke network policy | Only an assigned workspace can run UC ops | Workspace admin escalation | L4 — promoted user still scoped by Entra group; L5 binding still applies |
| 4 | Identity | `sp-publisher` SP, `publishers-region` group (sp-publisher sole member); no client_secret | Only `sp-publisher` carries the publisher group | SP credential theft (mitigated: federation, no long-lived secret) | L5 binding scopes reach; L6 grant scopes ops |
| 5 | UC Securable Visibility | `isolation_mode = ISOLATED` on every catalog/SC/EL; `databricks_workspace_binding` RW=promotion / RO=spokes on `hub_catalog` | Non-bound workspace cannot see; RO-bound cannot write *even with* `MODIFY` grant | OPEN mode; metastore admin | L2 still blocks storage read; L7 detects |
| 6 | UC Grants | `publishers-region → MODIFY`, `catalog-readers-live → SELECT`; additive | Restricts specific privileges per principal-securable pair | Metastore admin; fat-fingered grant | L5 binding stops writes anyway; L7 catches drift |
| 7 | Observability | `system.access.audit`, Watchdog `hub_unauthorized_writes`, CI grant-drift, SAT, Customer Catalog FE dashboard | Detects failures in L1–L6 | Watchdog downtime (detection lag) | Multiple independent surfaces |

### Defense-in-depth scenarios

| If this fails… | …does this still hold? |
| --- | --- |
| **L1** Storage firewall accidentally allows public IPs | ✅ L2 — no MI has data-plane access except hub MI |
| **L2** Subscription Owner grants `Storage Blob Data Contributor` to a stray principal | ✅ L1 — that principal still needs network reach; PE rules only allow workspace VNets |
| **L3** Workspace admin escalates a spoke_a user | ✅ L4 — not in `publishers-region`; L5 RO binding still blocks writes to `hub_catalog` |
| **L4** `sp-publisher` credential stolen | ✅ L5 — RO binding on every spoke; stolen SP can only write from `promotion` (no users to phish) |
| **L5** Someone sets `hub_catalog.isolation_mode = "OPEN"` | ✅ L6 — only `publishers-region` has MODIFY; L7 detects via Watchdog policy |
| **L6** Extra `MODIFY` granted to `catalog-readers-live` | ✅ L5 — spokes are RO-bound; binding wins over grant; L7 catches the drift |
| **L7** Watchdog paused for a week | ✅ L1–L6 are *preventative*; prevention holds; CI grant-drift + audit query still scheduled |
| **Metastore admin bypass** (skips L5 + L6 by design) | ✅ L1 + L2 — admin cannot read hub bytes from non-promotion workspace; no other MI has the role, no other VNet has a PE |

The defense-in-depth claim is that **any single layer can be misconfigured and the system still does not leak**. This is the answer to "what is your blast radius if X is compromised?"

## Workspace Bindings in Detail

"Binding" is overloaded in UC vocabulary — two unrelated relationships both get called that. Disambiguating before they appear repeatedly below.

### The two "bindings"

| Relationship | Terraform resource | What it means |
| --- | --- | --- |
| **Workspace ↔ Metastore** | `databricks_metastore_assignment` | "This workspace uses this metastore." One-time per workspace, region-scoped. |
| **Catalog ↔ Workspace** | `databricks_workspace_binding` | "This *specific* catalog is reachable from these workspaces." Per-catalog. Only relevant when the catalog is `ISOLATED`. |

This proposal says "workspace binding" to mean the second — `databricks_workspace_binding`. Same model applies to storage credentials and external locations.

### The four-layer access stack

Every UC operation passes through four independent gates. All four must permit.

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: Metastore assignment                                   │
│   databricks_metastore_assignment — can ws use UC at all?       │
└──────────────────────────────┬──────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2: Catalog isolation_mode                                 │
│   databricks_catalog.isolation_mode                             │
│     OPEN     → visible to all assigned ws, bindings ignored     │
│     ISOLATED → only bound ws can see it (the customer default)        │
└──────────────────────────────┬──────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3: Workspace binding  (only when ISOLATED)                │
│   databricks_workspace_binding                                  │
│     BINDING_TYPE_READ_WRITE  → read AND write                   │
│     BINDING_TYPE_READ_ONLY   → read only, writes BLOCKED        │
│   Creating workspace is auto-bound RW                           │
└──────────────────────────────┬──────────────────────────────────┘
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│ Layer 4: Grants                                                 │
│   databricks_grant — USE_CATALOG, SELECT, MODIFY, etc.          │
└─────────────────────────────────────────────────────────────────┘
```

**Crucial design property:** Layer 3 (binding) is enforced *before* Layer 4 (grants). A workspace bound `READ_ONLY` cannot write *even if its grants say `MODIFY`*. The binding wins. This is what makes the publishing path robust to grant drift — option 5 (chosen) survives a fat-fingered grant, option 4 (binding multi-writer) does not.

### `isolation_mode` in Detail

`isolation_mode` is the attribute that decides whether bindings are enforced at all — the default-deny knob.

| Value | What it does |
| --- | --- |
| **`OPEN`** | Catalog visible from every workspace assigned to the metastore. `databricks_workspace_binding` resources are **ignored**. |
| **`ISOLATED`** | Catalog invisible to every workspace *except* those explicitly bound. The creating workspace gets an implicit RW binding. Bindings are enforced. |

**What "invisible" means concretely.** With `alpha_gold` set to `ISOLATED`:
- From **alpha**: appears in `SHOW CATALOGS`, queryable subject to grants
- From **spoke_a**: does *not* appear in `SHOW CATALOGS`; `SELECT * FROM alpha_gold.x.y` returns "catalog not found", even if spoke_a had a `SELECT` grant
- From a **metastore admin** anywhere: still visible — metastore admins bypass isolation entirely

**the customer's choice.** All the customer catalogs default to `ISOLATED` (`modules/databricks/catalog/variables.tf:74`). Per-workspace catalogs are bound RW to their owning workspace only (implicit, by creation). `hub_catalog` is RW-bound to promotion (implicit) plus explicit RO bindings to every spoke (auto-wired via `modules/databricks/hub-catalog/main.tf`).

**Three securable types, same model.** `isolation_mode` applies to:

| Securable | Allowed values |
| --- | --- |
| `databricks_catalog` | `"OPEN"` / `"ISOLATED"` |
| `databricks_storage_credential` | `"ISOLATION_MODE_OPEN"` / `"ISOLATION_MODE_ISOLATED"` |
| `databricks_external_location` | `"ISOLATION_MODE_OPEN"` / `"ISOLATION_MODE_ISOLATED"` |

Catalogs use bare strings; SC and EL use prefixed strings. Provider quirk, same behavior. the customer sets all three to ISOLATED — commit `8219d09f` brought SC and EL in line with the catalogs that were already ISOLATED.

### Truth table — concrete the customer scenarios

| Scenario | Binding | Grant | Result |
| --- | --- | --- | --- |
| Spoke_a user `SELECT` on `spoke_a_gold.*` | implicit RW (creator) | catalog-readers → SELECT | **✓ works** |
| Spoke_a user `SELECT` on `alpha_gold.*` | none | (irrelevant) | **✗ catalog invisible** |
| Spoke_a user `SELECT` on `hub_catalog.*` | RO (auto-wired) | catalog-readers-live → SELECT | **✓ works** |
| Spoke_a user `INSERT INTO hub_catalog.*` | RO | even if MODIFY... | **✗ binding blocks** |
| sp-publisher in promotion writes to `hub_catalog.*` | implicit RW (creator) | publishers-region → MODIFY | **✓ works** |
| sp-publisher run from spoke_a writes to `hub_catalog.*` | RO | publishers-region → MODIFY | **✗ binding wins** |
| Metastore admin writes from anywhere | (bypassed) | (bypassed) | **✓ admin override — see [Why Not UC Alone](#why-not-rely-on-unity-catalog-alone) in docs** |

The last row is the metastore-admin bypass that motivates the Azure-layer scoping (`Storage Blob Data Contributor` only on the hub access connector MI, never on workspace MIs).

## Problem

the customer runs multiple Databricks workspaces (`alpha`, `beta`, `spoke_a`, `spoke_b`, `sandbox`, `ml_spoke`) on a single regional Unity Catalog metastore (`ucm-regional`). Two gaps exist:

1. **Storage isolation is incomplete.** `databricks_catalog` resources are correctly `ISOLATED`, but the underlying `databricks_storage_credential` and `databricks_external_location` are metastore-wide. A principal in workspace B with `CREATE_EXTERNAL_LOCATION` can enumerate workspace A's storage credential. Auditors flag this; defense-in-depth requires the UC-layer surface to match the Azure-layer storage scoping that's already tight.

2. **No governed publishing path between spokes.** There is no controlled way to move a curated dataset from a spoke's `gold` layer into a location every spoke can read. Today the options are ungoverned (manual cross-workspace grants) or impossible (writes blocked by `ISOLATED` bindings). A "hub-and-spoke" model with one published catalog is needed, and the path that writes to it must be physically restricted — not just policy-restricted.

## Options Considered for Hub-and-Spoke Isolation

Five traditional patterns exist for letting spokes consume each other's curated data on the same metastore. the customer's DOE/HPI defense-in-depth requirement eliminates the first three; option 4 fails the "show enforcement" test; option 5 won.

| # | Pattern | UC isolation | Compute isolation | Storage isolation | Single writer | Audit attestations |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | Cross-workspace grants on OPEN catalogs | ❌ | ❌ | ❌ | ❌ | 1 (grants only) |
| 2 | Shared workspace + RLS/CLS | ❌ | ❌ | ❌ | ❌ | 1 (RLS audit) |
| 3 | Per-consumer lake replication (N copies) | ✅ | ✅ | ✅ (over) | ✅ | N (per pipeline) |
| 4 | UC workspace bindings, multi-writer | ✅ | ❌ | partial | ❌ | 2 (bindings + grants) |
| **5** | **UC bindings + promotion workspace + sp-publisher  (chosen)** | ✅ | ✅ | ✅ | ✅ | **5 independent** |

### Option 1 — Cross-workspace grants on OPEN catalogs

Each spoke makes its `gold` catalog `isolation_mode = "OPEN"`. Other spokes grant `SELECT` to their reader groups directly. Simplest model — works out of the box.

**Fatal flaw for the customer:** the metastore surface is wide open. Every workspace sees every catalog by default. A single grant misconfig is a breach with no defense in depth. Auditors get exactly one control surface (UC grants). Fails DOE/HPI requirements that demand layered controls across identity, network, and data plane.

### Option 2 — Shared workspace + Row/Column-Level Security

One workspace serves all consumers; RLS/CLS policies separate tenants at the table level. Sigma uses a variant of this for end-user reads.

**Fatal flaw for the customer:** one workspace = one blast radius for compute, identity, audit, and network simultaneously. No per-spoke VNet, no per-spoke MI, no per-spoke storage account. RLS/CLS is a data-plane control that does not substitute for compute or network isolation.

### Option 3 — Lake replication (job-per-consumer)

Each consumer workspace runs its own ETL to copy source data into its own catalogs. Full isolation by construction — every consumer has its own copy.

**Fatal flaw for the customer:** data duplicated N times, no single source of truth, drift between copies is inevitable, and N pipelines have to be maintained for one dataset. The publishing model exists specifically to avoid this.

### Option 4 — UC workspace bindings, multi-writer (no dedicated promotion workspace)

Hub catalog `ISOLATED`, RW-bound to multiple writer workspaces. `sp-publisher` runs in whichever writer happens to be the source.

**Looks similar to option 5 from the outside.** Hub catalog is isolated, bindings restrict visibility, grants restrict writes. But there is no physical choke point — anyone in any writer workspace with `MODIFY` can write to the hub. "Only the publishing path can write" becomes a grant invariant rather than an architectural fact. One bad grant in any writer workspace = breach. Auditors get two attestations (bindings + grants); both live in the same Terraform plane and can drift together.

### Option 5 — UC bindings + dedicated `promotion` workspace + `sp-publisher`  ✓ chosen

Hub catalog `ISOLATED`, RW-bound to *one* workspace (`promotion`), RO-bound to spokes. `sp-publisher` is the only principal with `MODIFY`, runs only inside `promotion`. The publishing path is an architectural fact — even if UC grants drift, the workspace binding alone stops the write outside `promotion`.

**Five independent attestations available to auditors:**
1. UC grants (Terraform, only `publishers-region` has `MODIFY`)
2. Workspace binding (Terraform, only `promotion` is `READ_WRITE`)
3. Watchdog detective scan of `system.access.audit` for any non-`sp-publisher` write
4. Audit SQL query in `docs/runbooks/audit-hub-writes.sql` classifying every hub write
5. CI grant-drift assertion (`terraform plan -detailed-exitcode`)

### Why option 5 wins for the customer specifically

- **DOE / HPI compliance** requires defense in depth across identity, network, and data-plane controls. Options 1, 2 collapse those into one surface; option 5 keeps them separate and stackable.
- **the customer's audit posture** asks: *prove that only the publishing path can write to published data*. Option 5 produces five independent answers; option 4 produces two (and they share a plane); options 1–3 do not have a publishing path concept to attest about.
- **Watchdog's policy engine** (already in production) needs surface area to scan. Option 5 gives it `system.access.audit` events tied to a single SP, distinct from every other workload on the metastore — clean signal, low false-positive rate.
- **The cost** — one additional workspace, cross-tier remote-state plumbing — is dwarfed by the audit value. Already absorbed into auto-wiring (commit `edf876a8`); no manual ID copying.

For *cross-metastore* sharing (different region, different account, external partner), see [Why Not Delta Sharing](#why-not-delta-sharing) below — that section covers the cross-boundary alternative. This section addresses same-metastore options only.

## Scope

### b-uc01 — Storage Credential & External Location Isolation

Add `isolation_mode = "ISOLATION_MODE_ISOLATED"` to `databricks_storage_credential.unity_catalog` and `databricks_external_location.catalog` in `modules/databricks/catalog/main.tf`. The creating workspace is implicitly bound, so no explicit `databricks_workspace_binding` is needed for self-access — only for cross-workspace use, which the customer does not configure today.

**Provider quirk:** catalogs use bare `ISOLATED`/`OPEN`; storage credentials and external locations use prefixed `ISOLATION_MODE_ISOLATED`/`ISOLATION_MODE_OPEN`. Both are correct.

### b-uc02 — the customer Hub Azure Stack (`deployment/regional-infra/`)

Create the regional-singleton Azure resources backing the hub catalog:

- `rg-regional-metastore-hub` resource group
- `id-databricks-regional-metastore-hub` access connector MI (the only identity with data-plane access to hub storage)
- `sthubregional` storage account: private, `default_action = "Deny"`, `public_network_access_enabled = false`, `prevent_destroy`, `private_link_access` only to the hub MI
- `hub-catalog` container
- `Storage Blob Data Contributor` role assignment scoped to hub storage, granted to the hub MI only

### b-uc03 — `sp-publisher` Service Principal & `publishers` Group (`deployment/regional-infra/`)

Single-purpose identity that owns writes to `hub_catalog`:

- Entra app registration `mi-publisher-prod` (no client_secret — auth via workspace-issued tokens once added to the promotion workspace)
- Entra service principal
- Databricks account-level service principal
- `publishers-region` Databricks group with `sp-publisher` as the sole member
- **NOT** a metastore admin

### b-uc04 — Hub UC Wiring Module (`modules/databricks/hub-catalog/`)

New module called from the promotion workspace deployment:

| Resource | Configuration |
| --- | --- |
| `databricks_storage_credential.hub` | References hub access connector MI; `isolation_mode = ISOLATION_MODE_ISOLATED` |
| `databricks_external_location.hub` | `hub_catalog` location, `abfss://hub-catalog@sthubregional.dfs.core.windows.net/`; `ISOLATION_MODE_ISOLATED` |
| `databricks_catalog.hub` | `hub_catalog` catalog; `isolation_mode = "ISOLATED"` |
| `databricks_grant` × 4 | publishers get MODIFY/CREATE_TABLE/CREATE_VOLUME/APPLY_TAG on catalog + WRITE_FILES on SC/EL; reader groups get SELECT/BROWSE/READ_VOLUME |
| `databricks_workspace_binding.spoke_readers` | `BINDING_TYPE_READ_ONLY` for each spoke workspace ID |

The promotion workspace is implicitly bound `READ_WRITE` by UC (creating workspace auto-binds).

### b-uc05 — Per-Workspace Network Plumbing (`modules/databricks/hub-pe/`)

Every workspace that consumes `hub_catalog` needs a private endpoint to the hub storage:

- `azurerm_private_endpoint.hub_dfs` + `azurerm_private_endpoint.hub_blob` in the workspace's privatelink subnet, attached to the env's blob/dfs DNS zones
- `self-approving-pe` NCC PE rules for serverless compute (dfs then blob — Azure requires exclusive access during NCC PE rule creation)

Wired into `deployment/workspace/main.tf` via `enable_hub_access = true` (default). Every spoke picks up hub PEs on its next apply.

### Promotion Workspace Configuration

`deployment/workspace/configs/live/promotion/`:

- `workspace_name = "promotion"`
- CIDR `10.202.40.0/21` (next available after `ml_spoke`)
- `create_catalog = false` (no medallion catalogs)
- `create_hub_catalog = true` (owns the hub UC wiring)
- `hub_reader_groups = ["catalog-readers-live"]`
- `spoke_workspace_ids = []` (live spokes are auto-discovered via `spoke_remote_states`; this list is only for manual cross-env additions)
- `spoke_remote_states` = map of live spokes (spoke_a, spoke_b, sandbox, ml_spoke) → tfstate config. Promotion reads each `workspace_id` output and auto-binds them RO on `hub_catalog`.

## Publishing Paths — The Four Data Flows

The architecture creates four distinct paths through the system. Each has its own controls, principals, and observability points.

### Path 1 — WRITE: spoke gold → `hub_catalog`

The publishing path proper. Source data leaves a spoke, is validated, and lands in `hub_catalog`.

```
┌──────────────────────────────────┐
│ Source spoke (e.g., spoke_a)     │
│   spoke_a_gold.finance.txns      │ ← owned + populated by spoke engineers
└─────────────┬────────────────────┘
              │ READ (cross-workspace catalog binding)
              │ spoke_a_gold bound READ_ONLY → promotion
              ▼
┌──────────────────────────────────────────────────┐
│ promotion workspace (DAB pipeline, run-as sp-publisher) │
│                                                  │
│   1. Read source via cross-workspace binding     │
│   2. Quality gates (DLT expectations)            │
│   3. Watchdog pre-publish check (hub_contract)   │
│   4. Write to hub_catalog.<domain>.<table>        │
└─────────────┬────────────────────────────────────┘
              │ WRITE (catalog binding RW)
              │ UC → hub access connector MI → storage
              │ Spoke privatelink subnet → PE → hub storage
              ▼
┌──────────────────────────────────┐
│ sthubregional /hub-catalog │ ← published bytes land here
│   hub_catalog.finance.txns        │ ← UC table referencing storage_root
└──────────────────────────────────┘
```

| Component | Role in WRITE path | Configured in |
| --- | --- | --- |
| Source spoke catalog (`{ws}_gold`) | Source of truth for working data | Existing per-workspace catalog module |
| Cross-workspace catalog binding (spoke gold → promotion `READ_ONLY`) | Allows promotion to read spoke gold | Auto-wired — `deployment/workspace/locals.tf` reads `data.terraform_remote_state.promotion.workspace_id` and adds the binding. State config in `live/common.tfvars` (`promotion_state_*`). |
| `sp-publisher` SP | Identity executing the pipeline | `deployment/regional-infra/publisher_sp.tf` |
| Promotion DAB pipeline | Validates + writes | TBD — pilot dataset still to be picked |
| Hub catalog binding (`hub_catalog` RW → promotion) | Allows the write at the UC plane | Implicit (creating workspace) — `modules/databricks/hub-catalog/` |
| Hub access connector MI | Authenticates to hub storage | `deployment/regional-infra/hub_catalog.tf` |
| Hub PE (promotion → hub storage) | Network path for compute → storage | `modules/databricks/hub-pe/` |

**Spoke-side binding is auto-wired:** each spoke reads `promotion`'s tfstate via `terraform_remote_state.promotion` and auto-generates the `gold → promotion BINDING_TYPE_READ_ONLY` entry. No manual ID copying. The remote-state config is set once in `deployment/workspace/configs/live/common.tfvars` (`promotion_state_*` variables); spokes inherit it. On the first apply (before `promotion` exists), the data source returns null and the binding is skipped — the spoke applies cleanly. Once `promotion` is deployed, re-applying any live spoke picks up the binding automatically.

### Path 2 — READ: any spoke user → `hub_catalog`

The consumption path. A user in any spoke reads published data through their own workspace.

```
┌──────────────────────────────────┐
│ Spoke user (in alpha workspace)  │
│   SELECT * FROM hub_catalog.finance.txns │
└─────────────┬────────────────────┘
              │ Query plan via metastore
              │ Catalog visibility: workspace binding RO → alpha
              ▼
┌──────────────────────────────────┐
│ Alpha compute (cluster/SQL/serverless)  │
└─────────────┬────────────────────┘
              │ UC issues short-lived token to
              │ hub access connector MI
              ▼
┌──────────────────────────────────┐
│ Alpha privatelink subnet → PE   │ ← Azure plane (classic compute)
│ Alpha NCC → PE rule              │ ← Azure plane (serverless compute)
└─────────────┬────────────────────┘
              ▼
┌──────────────────────────────────┐
│ sthubregional /hub-catalog │
│   Returns Delta files            │
└──────────────────────────────────┘
```

| Component | Role in READ path | Configured in |
| --- | --- | --- |
| Workspace binding (`hub_catalog` RO → alpha) | Makes the catalog visible to alpha users | `modules/databricks/hub-catalog/main.tf` (driven by `local.effective_spoke_workspace_ids` — auto-discovered from `spoke_remote_states` + manual `spoke_workspace_ids`) |
| UC grants (`catalog-readers-live` etc.) | Allows SELECT/BROWSE/READ_VOLUME on the catalog | Same module |
| Hub access connector MI | Authenticates to storage on the user's behalf | `deployment/regional-infra/hub_catalog.tf` |
| Hub PE on alpha VNet | Network path | `modules/databricks/hub-pe/` (every workspace gets one via `enable_hub_access = true`) |

The user's *own* workspace MI is never used for hub data plane reads — UC swaps to the hub MI transparently. This is what makes per-workspace storage isolation hold even when reading shared data.

### Path 3 — AUDIT: every UC operation → enforcement chain

Every read and write through Path 1 and Path 2 emits to `system.access.audit`. That feeds the detective layer.

```
Every UC operation
        │
        ▼
system.access.audit (auto-populated by Databricks)
        │
        ├─────► Watchdog daily scan (hub_unauthorized_writes policy)
        │           └─► exception_request opened on detection
        │
        ├─────► Customer Catalog FE (GovernanceDashboard surfaces violations)
        │
        ├─────► Weekly grant-drift SQL assertion (CI job)
        │           └─► Fails build on non-Terraform grants
        │
        └─────► SAT (Security Analysis Tool — periodic compliance scan)
```

This is the layer that lets you *prove* the WRITE path is the only path. Five independent attestations to an auditor, all derived from the same audit stream.

### Path 4 — CURATION: human → promotion request → publish

The human workflow that triggers Path 1. Lives in the Customer Catalog FE app.

```
┌──────────────────────────────────┐
│ Engineer / data steward          │
│ Browses Customer Catalog FE        │
└─────────────┬────────────────────┘
              │ Clicks "Request promotion" on
              │ spoke_a_gold.finance.txns dataset detail
              ▼
┌──────────────────────────────────┐
│ Customer Catalog backend           │
│ INSERT INTO curation_requests    │ ← migration 005_curation.sql
│   (source, target, requester,    │
│    status='pending')             │
└─────────────┬────────────────────┘
              │ Notification (Slack/Asana — TBD)
              ▼
┌──────────────────────────────────┐
│ Publishers team reviews          │
│ Customer Catalog admin UI          │ ← views/admin/
│ Approve → status='approved'      │
│ Deny → status='denied' + reason  │
└─────────────┬────────────────────┘
              │ On approve, DAB pipeline picks up
              │ approved requests on next scheduled run
              │ (or manual trigger)
              ▼
        Path 1 (WRITE)
```

| Component | Role | Where |
| --- | --- | --- |
| Customer Catalog FE (DatasetDetail) | Shows "Request promotion" button on eligible datasets | `bundles/customer-catalog/src/frontend/src/views/DatasetDetail.tsx` |
| `curation_requests` table | Persists requests + status | `bundles/customer-catalog/schema/migrations/005_curation.sql` |
| Admin UI | Review/approve/deny | `bundles/customer-catalog/src/frontend/src/views/admin/` |
| DAB pipeline | Reads approved requests, executes Path 1 | TBD — pilot dataset still to be picked |

## Network Diagrams

Four lenses on the Azure network plane underlying the publishing path.

### N1 — VNet topology (where workspaces sit)

```
   vnet-dataplatform-prod-region     (10.202.0.0/16 — single shared BYO VNet)
   │
   ├── 10.202.0.0/24       webauth     "network hub"  (SSO; auth-only)
   │
   │   ─────── spoke workspaces  (data planes, /21 each)  ───────
   │
   ├── 10.202.8.0/21       spoke_a     ┐
   ├── 10.202.16.0/21      spoke_b       │
   ├── 10.202.24.0/21      sandbox     │   each /21 split into:
   ├── 10.202.32.0/21      ml_spoke       ├──►  container /23   private compute subnet
   ├── 10.202.40.0/21      promotion   │     host      /23   public compute subnet
   │                       [NEW]       │     pe        /24   privatelink subnet
   └── (alpha + beta live in their own VNets in separate subscriptions)
```

"Network hub" (webauth) is NOT the publishing hub. They share the word but nothing else — `promotion` is a network spoke that happens to be the publishing choke point. See Glossary.

### N2 — How any workspace reaches `sthubregional` (the hub storage)

```
   CLASSIC COMPUTE  (interactive clusters, jobs)
   ─────────────────────────────────────────────

      ┌────────────────────┐     ┌──────────────────────────┐
      │ workspace cluster  │────►│ workspace's PE subnet    │
      │ (e.g. spoke_a)     │     │ (10.202.8.0/24 of its /21)│
      └────────────────────┘     └─────────────┬────────────┘
                                               │
                                               │  azurerm_private_endpoint.hub_dfs
                                               │  azurerm_private_endpoint.hub_blob
                                               │  (modules/databricks/hub-pe/)
                                               ▼
                                  ┌──────────────────────────┐
                                  │ sthubregional     │
                                  │ private, deny-all,       │
                                  │ prevent_destroy          │
                                  └──────────────────────────┘

   SERVERLESS COMPUTE  (SQL warehouse, serverless jobs, DLT serverless)
   ───────────────────────────────────────────────────────────────────

      ┌────────────────────┐     ┌──────────────────────────┐
      │ workspace          │────►│ ncc-regional-metastore       │
      │ serverless query   │     │ (NCC, region-wide pool)  │
      └────────────────────┘     └─────────────┬────────────┘
                                               │
                                               │  databricks_mws_ncc_private_endpoint_rule
                                               │  via self-approving-pe (dfs then blob —
                                               │   Azure requires exclusive access)
                                               ▼
                                  ┌──────────────────────────┐
                                  │ sthubregional     │
                                  └──────────────────────────┘
```

Both paths land at the same storage. Both PE types are created per workspace by `modules/databricks/hub-pe/`, gated by `var.enable_hub_access` (default `true`).

### N3 — DNS resolution (private path only; no public IPs ever)

```
   DNS chain when compute resolves sthubregional.dfs.core.windows.net
   ─────────────────────────────────────────────────────────────────────────

   workspace compute query
            │
            ▼
   ┌─────────────────────────────────────────────────┐
   │ Azure DNS (VNet's resolver)                     │
   │ checks privatelink override before public DNS   │
   └────────────────────────┬────────────────────────┘
                            ▼
   ┌─────────────────────────────────────────────────┐
   │ Private DNS Zone                                │
   │ privatelink.dfs.core.windows.net                │
   │ (VNet-linked, env-tier managed in live/beta;    │
   │  workspace-tier managed in alpha)               │
   └────────────────────────┬────────────────────────┘
                            ▼
   ┌─────────────────────────────────────────────────┐
   │ A record auto-created by the PE on apply:       │
   │ "sthubregional"                          │
   │   →  10.202.X.4   (PE's IP in this ws's /24)    │
   └─────────────────────────────────────────────────┘
```

TLS terminates on the PE inside the workspace's own VNet. No public IP, no public internet, no cross-VNet hop. Same shape for `*.blob.core.windows.net` via `privatelink.blob.core.windows.net`.

### N4 — Runtime query path (the whole flow for one `SELECT`)

```
   spoke_a user runs:   SELECT * FROM hub_catalog.finance.txns
   ───────────────────────────────────────────────────────────

   ┌────────────────────────────┐
   │ spoke_a user (in browser /  │
   │ notebook / external client) │
   └─────────────┬──────────────┘
                 │  query
                 ▼
   ┌────────────────────────────┐
   │ Spoke_a cluster / SQL WH    │
   │ (in spoke_a workspace,      │
   │  10.202.8.0/21)             │
   └─────────────┬──────────────┘
                 │  "can spoke_a see hub_catalog?"
                 ▼
   ┌──────────────────────────────────────────┐
   │ Unity Catalog (metastore ucm-regional-...)  │
   │  ✓ hub_catalog workspace_binding RO → rom  │
   │  ✓ catalog-readers-live has SELECT        │
   │  → issues short-lived token to            │
   │    HUB access connector MI                │
   └─────────────┬────────────────────────────┘
                 │  token + storage URL
                 ▼
   ┌──────────────────────────────────────────┐
   │ Spoke_a compute opens connection         │
   │  classic   →  via spoke_a PE  → hub stor │
   │  serverless → via NCC PE rule → hub stor │
   │  (DNS → A record → 10.202.X.4 / PE IP)   │
   └─────────────┬────────────────────────────┘
                 ▼
   ┌────────────────────────────┐
   │ sthubregional        │
   │  • hub MI authorized        │
   │  • PE network path verified │
   │  • bytes returned over TLS  │
   └─────────────┬──────────────┘
                 │  result rows
                 ▼
   ┌────────────────────────────┐
   │ Spoke_a compute → user      │
   └────────────────────────────┘
```

**Crucial:** spoke_a's *own* access connector MI is never touched in this flow. UC transparently swaps to the hub MI. That is why per-workspace storage isolation holds even when many workspaces share the hub catalog — nobody else's MI ever gets credentials for hub storage.

## Why Not Delta Sharing

Delta Sharing is the natural alternative to consider for "share data across workspaces." It is the wrong tool for this job. Here is why, and when the customer *would* use Delta Sharing.

### What Delta Sharing is for

Delta Sharing exists to cross a **trust boundary**: a metastore, a Databricks account, a region, a cloud, or an organization. The protocol issues short-lived signed URLs and serves Delta tables to a recipient that cannot see the provider's metastore directly.

It is the right tool when one of these is true:
- Provider and recipient are in **different metastores** (different regions or different accounts)
- Provider and recipient are in **different clouds** (Databricks → AWS, GCP, etc.)
- Recipient is an **external organization** (vendor, partner, regulator)
- Provider needs to **revoke independently** of recipient's UC governance

### Why same-metastore publishing doesn't qualify

All the customer workspaces share `ucm-regional` and the same Databricks account. There is no trust boundary to cross. Using Delta Sharing here would mean inventing a boundary that does not exist and paying for it.

| Concern | Native workspace binding (chosen) | Delta Sharing |
| --- | --- | --- |
| **Performance** | Direct UC compute → hub MI → storage. Same plane. | Sharing server round-trip + token exchange per query. Measurable overhead. |
| **Governance model** | UC grants apply natively. Adding a reader = `GRANT SELECT TO group`. Audited in `system.access.audit`. | Separate share/recipient model with its own grant surface. Duplicated governance, harder to audit holistically. |
| **Setup complexity** | One `databricks_workspace_binding` resource per spoke (already wired in `modules/databricks/hub-catalog/`). | Shares + recipients + activation tokens + provider/recipient SPs + share-level grants. ~5× the resources. |
| **Capability** | Supports both `READ_ONLY` and `READ_WRITE` bindings — needed for promotion's write path. | Read-only sharing. Write-back is limited. The publishing model requires a write path. |
| **Cost** | Zero additional charge inside the same metastore + region. | Sharing server compute + cross-region egress if applicable. |
| **Audit coverage** | One unified audit stream in `system.access.audit`. | Sharing audit lives in a separate surface; cross-referencing with UC events is messier. |
| **Observability via Customer Catalog FE** | Native — catalog appears alongside `{ws}_gold` etc. with consistent metadata. | Shared tables appear differently; lineage and tag flows may not carry across the share. |

Delta Sharing solves a problem the publishing path does not have. The boundary it offers comes with cost, latency, and operational surface area that buys nothing in this configuration.

### When the customer *would* use Delta Sharing

Reserve Delta Sharing for use cases that genuinely cross a boundary:

| Use case | Why Delta Sharing is the right tool |
| --- | --- |
| **Cross-region DR** (e.g., adding `westus2`) | A second region requires a second metastore. Replicating `hub_catalog` to the DR region is a Delta Sharing job. |
| **the customer subsidiary / acquisition** with its own Databricks account | Different account = different metastore. Sharing to them is D2D Delta Sharing. |
| **External vendor / partner** outside the customer's tenant | No way to give them workspace bindings; Delta Sharing's open protocol is the export path. |
| **Sigma reading via service principal** (if Sigma is external) | If Sigma cannot use a Databricks SQL warehouse and instead needs the open Delta Sharing protocol. (Today, Sigma can read via SQL warehouse — no Delta Sharing needed.) |
| **Regulator or auditor** needing a point-in-time snapshot | A frozen share is easier to reason about than granting them workspace access. |

For the customer's current single-region, single-account, all-Databricks model: **workspace bindings + `hub_catalog` is the correct primitive.** Delta Sharing stays unused until a real cross-boundary need appears.

## Enforcement & Audit

Four layers, codified in `docs/uc-isolation.md`:

1. **UC grants** — Terraform-managed; only `sp-publisher` (via `publishers-region`) has MODIFY on hub
2. **Workspace binding** — Only the `promotion` workspace has `READ_WRITE`; spokes are explicit `READ_ONLY`
3. **Watchdog detective policy** — `hub_unauthorized_writes` scans `system.access.audit` daily, opens exception_request on non-`sp-publisher` writes to `hub_catalog.*`
4. **Audit query + CI grant-drift test** — `docs/runbooks/audit-hub-writes.sql` classifies every hub write `authorized` vs `INVESTIGATE`; `terraform plan -detailed-exitcode` catches grant drift

## Dependencies

| Dependency | Status | Notes |
| --- | --- | --- |
| Regional metastore (`ucm-regional`) | ✅ Done | One per region; already in `deployment/regional-infra/` |
| Per-workspace catalog isolation (`isolation_mode`) | ✅ Done in b-uc01 | Storage credential + external location now also `ISOLATED` |
| `privatelink.blob/dfs` DNS zones | ✅ Done | Managed by subscription-base tier; hub PEs attach to them |
| Watchdog | ✅ Coded | Detective policy `hub_unauthorized_writes` extends [[p-watchdog]] |
| Existing `self-approving-pe` module | ✅ Done | Reused for NCC PE rules on hub storage |

## Related Work

This proposal is the **substrate** of the the customer governance stack. The end-user UI
that consumes it ships separately:

- **[p-customer-catalog(P3)](./p-customer-catalog(P3).md)** — the customer-branded data catalog,
  curation workflow, exception management, and ontology viewer. The catalog can only
  curate and trust what uc-isolation has made governable.
- **[p-watchdog(P1)](./p-watchdog(P1).md)** — the policy engine that classifies the
  isolated catalogs uc-isolation creates and writes the governance state the catalog reads.

Together: **uc-isolation (substrate) → watchdog (engine) → customer-catalog (UI)** — three
proposals, one stack.

## Data Replication Cost

The publishing path is where this proposal incurs real storage cost. Naming it
explicitly so it gets budgeted:

| Surface | What it copies | Where it lives | Cost driver |
| --- | --- | --- | --- |
| `hub_catalog.*` tables | Full replicas of spoke gold tables, written by `sp-publisher` DAB jobs | UC managed storage backing `hub_catalog` | sum of each published hub table's actual data size; could be GBs–TBs depending on scope |

Everything else in the governance stack is metadata-on-metadata (pointers, not bytes).
The Customer Catalog FE's Lakebase footprint is hundreds of KB to low MB for thousands
of tables. Watchdog Delta tables store violations + policies — bytes per row, not
data payloads. UC `information_schema` is native overhead.

Why the hub copy is intentional rather than queried-in-place: workspace isolation
on the compute plane means a query in `spoke_b` cannot reach `spoke_a_gold.x.y` even
with read bindings. Hub replication is the supported workaround. The cost is
the bill paid for cross-workspace consumption.

Cost lever: **publish only what's actually consumed cross-workspace** rather than
blanket-publishing all spoke gold. Demand-driven. See
[Customer Catalog architecture — Data replication & cost model](../docs/customer-catalog/architecture.md#data-replication--cost-model)
for the complete pointer-vs-copy inventory across the whole stack.

## Risks

| Risk | Mitigation |
| --- | --- |
| Metastore admin bypass | Metastore admins still ignore UC grants. `metastore-admins-region` membership must be kept tiny (platform team only); audited as an open item. Azure-layer MI scoping limits the blast radius. |
| Network plumbing rollout disruption | `enable_hub_access` defaults to `true` — re-applying any spoke creates hub PEs. Spokes are independent applies, so disruption is contained per workspace. NCC PE rule creation requires exclusive access per storage account; module sequences dfs→blob. |
| `spoke_workspace_ids` populated incorrectly | Empty list is safe (zero RO bindings created, no spokes can read but no failure). Wrong ID grants RO access to a workspace that shouldn't have it — caught by CI grant-drift test (b-uc04 layer 4 in enforcement). |
| Lifecycle `ignore_changes` on existing catalog ELs | Existing catalog module's `databricks_external_location.catalog` ignores `credential_name` and `url` (TEMPORARY for migration). Adding `isolation_mode` is NOT in the ignore list, so b-uc01 plans clean. |
| `prevent_destroy` on hub storage | Intentional safety barrier. To destroy: manually remove the lifecycle block in `deployment/regional-infra/hub_catalog.tf`. Matches the pattern on workspace storage accounts. |

## Implementation

Already committed on `stuart/d-uc-isolation-P2`:

| Commit | What |
| --- | --- |
| `8219d09f` | b-uc01 — storage credential + external location `ISOLATION_MODE_ISOLATED` |
| `04b870f3` | Doc — workspace isolation model |
| `9d826dcc` | Doc — publishing path + four-layer enforcement |
| `e52aaa95` | b-uc02 + b-uc03 — hub Azure stack + sp-publisher SP |
| `9c3aeeb0` | b-uc04 — `modules/databricks/hub-catalog/` + promotion workspace integration |
| `17a10d13` | b-uc05 — `modules/databricks/hub-pe/` + every-workspace integration |
| `ba24dbca` | Doc — reflect implemented state |

`terraform validate` passes on `deployment/regional-infra/` and `deployment/workspace/`.

## Order of Operations

First-time rollout (auto-wiring handles the cross-references via `terraform_remote_state`):

1. **`deployment/regional-infra/`** — Creates hub Azure stack (b-uc02) + sp-publisher SP (b-uc03). Idempotent if already deployed; new resources only.
2. **First `promotion` apply** (`deployment/workspace/configs/live/promotion/`) — Creates the `promotion` workspace itself + `hub_catalog` UC wiring (b-uc04). `data.terraform_remote_state.spokes` returns no IDs yet for spokes that haven't applied; no spoke bindings created on this pass. This is fine.
3. **Each spoke workspace** (`alpha`, `beta`, `spoke_a`, `spoke_b`, `sandbox`, `ml_spoke`) — Re-apply picks up `enable_hub_access = true` (lands hub PEs, b-uc05) AND `data.terraform_remote_state.promotion.workspace_id` (auto-adds `gold → promotion BINDING_TYPE_READ_ONLY` to its `catalog_workspace_bindings`). Independent applies, per-workspace schedule.
4. **Re-apply `promotion`** — `data.terraform_remote_state.spokes` now sees every live spoke's `workspace_id`. `local.effective_spoke_workspace_ids` populates and `hub_catalog` gets `BINDING_TYPE_READ_ONLY` bindings to each spoke automatically.

Rolling out b-uc01 separately:

1. Apply on `alpha` first (low-risk validation)
2. Apply on `beta` after alpha is verified
3. Apply on `live` workspaces one at a time (`spoke_a`, `spoke_b`, `sandbox`, `ml_spoke`)

## Estimated Effort

| Phase | Effort |
| --- | --- |
| b-uc01 storage isolation | ✅ Done (~30 min) |
| b-uc02 hub Azure stack | ✅ Done (~1 hr) |
| b-uc03 sp-publisher SP | ✅ Done (~30 min) |
| b-uc04 hub-catalog module | ✅ Done (~2 hr) |
| b-uc05 hub-pe module + workspace integration | ✅ Done (~1.5 hr) |
| Docs + proposal | ✅ Done (~2 hr) |
| **Code complete** | **~7.5 hr, on branch** |
| First-time apply (regional-infra + 6 spokes + promotion) | 2–3 hr (V4C) |
| Watchdog detective policy + audit runbook + grant-drift CI job | ~3 hr (follow-up) |
| Pilot promotion pipeline (first dataset → DAB) | ~4–6 hr (depends on dataset complexity) |

## Customer/Business Decisions Still Open

Implementation is complete for the live env. Remaining items are real
customer/business decisions, not deferred implementation work:

- Pick pilot promotion dataset (candidates: SAP finance views, Sigma supply-chain marts) and define its DAB pipeline
- Implement Watchdog `hub_unauthorized_writes` policy in the Watchdog repo
- Land `docs/runbooks/audit-hub-writes.sql` and the weekly grant-drift assertion job
- Audit `metastore-admins-region` membership (least-privilege review)
- Decide whether to extend `spoke_remote_states` to cross-env spokes (alpha, beta) — they would need their state container references added; today only live-env spokes are auto-wired
