# Permissions Compiler — Watchdog Output Generator

Reference implementation showing how a **permissions-as-code** system can integrate
with Watchdog's drift detection. This is *not* the full permissions compiler — just
the Watchdog output generator and example configuration files.

## What it does

The generator reads YAML permission declarations and produces two artifacts:

| Artifact | Purpose |
|----------|---------|
| `expected_state.json` | The expected-state snapshot Watchdog evaluates against actual UC state |
| `permissions_drift_policies.yaml` | Drift detection policies using the `drift_check` rule type |

Watchdog's policy engine compares the expected state against live Unity Catalog
metadata and raises violations wherever the two diverge.

## Quick start

```bash
# Install the single dependency
pip install pyyaml

# Generate artifacts from the example declarations
python watchdog_generator.py \
  --permissions-dir ./example \
  --env alpha \
  --output-dir ./output
```

The `output/` directory will contain `expected_state.json` and
`permissions_drift_policies.yaml`, ready to be consumed by Watchdog.

## Directory layout

```
permissions-compiler/
  watchdog_generator.py          # The generator script
  example/
    domains/
      analytics.yaml             # Grant declarations per domain
    abac/
      row-filters.yaml           # Row-level security filters
      column-masks.yaml          # Column masking definitions
    teams.yaml                   # Group membership
```

## Integration with Watchdog

1. Run the generator as part of your CI/CD pipeline (or locally).
2. Copy `expected_state.json` into Watchdog's `expected_permissions/` directory.
3. Include `permissions_drift_policies.yaml` in your Watchdog policy set.
4. Watchdog's `drift_check` rule type handles the rest — comparing expected vs. actual.

See [docs/guide/how-to/drift-detection.md](../../docs/guide/how-to/drift-detection.md)
for the full drift detection pattern.
