#!/usr/bin/env bash
# Deploy the watchdog bundle to a named target.
#
# Usage: scripts/deploy.sh <target>   (e.g. dev, staging, prod)
#
# Reads the Databricks workspace from --profile, which must already exist in
# ~/.databrickscfg. Fails fast if the target is unknown or credentials are
# missing so operators don't accidentally deploy to the wrong workspace.

set -euo pipefail

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
    echo "Usage: $0 <target>" >&2
    exit 1
fi

if ! command -v databricks >/dev/null 2>&1; then
    echo "error: databricks CLI not on PATH" >&2
    exit 2
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/engine"

echo "Deploying watchdog bundle to target=$TARGET"
databricks bundle validate -t "$TARGET"
databricks bundle deploy -t "$TARGET"
echo "Deploy complete. Next: databricks bundle run watchdog_daily_scan -t $TARGET"
