# Watchdog Governance — Ontos Adapter

A pluggable governance UI module that connects the Watchdog scanner engine
to [Ontos](https://github.com/databrickslabs/ontos) (Databricks Labs
governance platform). Think of it as the **Grafana data source plugin**
for Watchdog's **Prometheus-like** scan engine.

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│  Ontos (Platform)                                             │
│                                                               │
│  ┌──────────────────────────────────────────────────────────┐│
│  │  Governance Plugin Contract (REST API)                    ││
│  │  /api/governance/violations, /policies, /exceptions, ...  ││
│  └──────────────────────────────────────────────────────────┘│
│            ▲                                                  │
│            │  React views consume this API                    │
│  ┌─────────┴────────────────────────────────────────────────┐│
│  │  Frontend: 4 pages (Dashboard, ResourceDetail,           ││
│  │            Policies, Exceptions)                          ││
│  └──────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────┘
         ▲
         │  implements
         │
┌────────┴─────────────────────────────────────────────────────┐
│  watchdog-governance package (this adapter)                    │
│                                                               │
│  GovernanceProvider protocol → WatchdogProvider (Delta SQL)    │
│  Thin FastAPI routers → dependency-injected provider           │
└──────────────────────────────────────────────────────────────┘
         ▲
         │  reads/writes
         │
┌────────┴─────────────────────────────────────────────────────┐
│  platform.watchdog.* Delta tables                             │
│  (written by the Watchdog scan engine)                        │
└──────────────────────────────────────────────────────────────┘
```

**Key design principle:** The Delta tables are the integration contract.
The adapter reads them; the engine writes them. They deploy independently.

## Quick Start

### Integrate with Ontos (3 lines)

**Backend** — in Ontos `app.py`:

```python
from watchdog_governance import register_routes as register_governance
register_governance(app)
```

**Frontend** — copy `frontend/src/` into Ontos fork, then:

```typescript
// config/features.ts
import { watchdogFeatures } from './features.watchdog'
export const features = [...existingFeatures, ...watchdogFeatures]

// app.tsx — inside /governance route children
import watchdogRoutes from './routes.watchdog'
children: [...existingGovernanceChildren, ...watchdogRoutes]
```

### Standalone mode (development)

```bash
cd ontos-adapter
pip install -e ".[watchdog,dev]"

export DATABRICKS_HOST=https://adb-xxx.azuredatabricks.net
export DATABRICKS_WAREHOUSE_HTTP_PATH=/sql/1.0/warehouses/xxx
export DATABRICKS_TOKEN=dapi...
export WATCHDOG_ONTOLOGY_DIR=../engine/ontologies

uvicorn watchdog_governance.app:app --reload
```

## GovernanceProvider Protocol

Any governance backend can implement this protocol. The default
`WatchdogProvider` reads from Delta tables.

| Domain | Methods |
|--------|---------|
| **Violations** | `violations_summary()`, `list_violations(filters)` |
| **Scans** | `list_scans(limit)`, `get_scan(scan_id)` |
| **Resources** | `list_resources(filters)`, `get_resource(resource_id)` |
| **Policies** | `list_policies(filters)`, `get_policy(id)`, `create_policy(...)`, `update_policy(...)`, `policy_history(id)` |
| **Exceptions** | `list_exceptions(filters)`, `exceptions_summary()`, `approve_exceptions(...)`, `revoke_exception(id)`, `bulk_revoke_expired()` |
| **Ontology** | `list_ontology_classes(kind)`, `get_ontology_class(name)`, `ontology_tree()`, `validate_ontology()` |

See `provider.py` for the full Protocol definition with type signatures.

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `WATCHDOG_CATALOG` | `platform` | Unity Catalog name |
| `WATCHDOG_SCHEMA` | `watchdog` | Schema within the catalog |
| `DATABRICKS_HOST` | — | Workspace URL |
| `DATABRICKS_WAREHOUSE_HTTP_PATH` | — | SQL warehouse HTTP path |
| `DATABRICKS_TOKEN` | — | PAT or OAuth token |
| `DATABRICKS_CLIENT_ID` | — | SP client ID (alternative to token) |
| `DATABRICKS_CLIENT_SECRET` | — | SP client secret (alternative to token) |
| `WATCHDOG_ONTOLOGY_DIR` | — | Path to `resource_classes.yml` directory |

## Writing a Custom Provider

Implement the `GovernanceProvider` protocol and pass it to `register_routes`:

```python
from watchdog_governance import GovernanceProvider, register_routes

class MyProvider:
    def violations_summary(self) -> ViolationSummary:
        # your implementation
        ...
    # ... implement all protocol methods

register_routes(app, provider=MyProvider())
```

Register as an entry point for auto-discovery:

```toml
[project.entry-points."watchdog_governance.providers"]
my-provider = "my_package:MyProvider"
```

## Directory Structure

```
ontos-adapter/
├── pyproject.toml                    # watchdog-governance package
├── README.md
├── src/watchdog_governance/
│   ├── __init__.py                   # exports: GovernanceProvider, register_routes
│   ├── models.py                     # Pydantic models (the data contract)
│   ├── provider.py                   # GovernanceProvider protocol
│   ├── providers/
│   │   └── watchdog.py               # WatchdogProvider (Delta SQL)
│   ├── routers/
│   │   ├── _deps.py                  # Shared dependencies (provider, current_user)
│   │   ├── violations.py             # GET violations, scans, resources
│   │   ├── policies.py               # CRUD policies + history
│   │   ├── exceptions.py             # CRUD exceptions + bulk operations
│   │   └── ontology.py               # GET class hierarchy, tree, validate
│   ├── router.py                     # Mount all routers, register_routes()
│   ├── app.py                        # Standalone FastAPI app
│   ├── ontology_export.py            # OWL/Turtle export for Ontos import
│   └── ontos_sync.py                 # Semantic link sync (Watchdog → Ontos)
└── frontend/
    ├── README.md                     # Frontend integration guide
    └── src/
        ├── config/features.watchdog.ts
        ├── i18n/en/watchdog.json
        ├── routes.watchdog.tsx
        └── views/
            ├── GovernanceDashboard.tsx
            ├── ResourceDetail.tsx
            ├── Policies.tsx
            └── Exceptions.tsx
```
