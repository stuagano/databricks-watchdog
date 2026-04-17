#!/usr/bin/env bash
# Sync YAML policies into the Delta ``policies`` table for an existing target
# without redeploying the full bundle. Useful when policy wording, severity, or
# active flag changes and you want the update live immediately.
#
# Usage: scripts/sync_policies.sh <target>

set -euo pipefail

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
    echo "Usage: $0 <target>" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/engine"

echo "Re-running the evaluate job with --sync-policies on target=$TARGET"
databricks bundle run watchdog_daily_scan \
    -t "$TARGET" \
    --only evaluate_policies \
    --params sync_policies=true
