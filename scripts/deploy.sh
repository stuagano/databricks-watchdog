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

# Fail fast on unsubstituted databricks.yml placeholders so the CLI doesn't
# surface a cryptic auth/network error 30s into `bundle deploy`. Only scans
# the block for the requested target; other targets may legitimately contain
# placeholders.
BUNDLE_YML="$REPO_ROOT/engine/databricks.yml"
if [[ -f "$BUNDLE_YML" ]]; then
    # Extract lines from `  $TARGET:` up to the next top-level target (2-space
    # indent). awk treats any 2-space-indented key as a new target.
    target_block="$(awk -v t="$TARGET" '
        $0 ~ "^  "t":"                 { in_block=1; next }
        in_block && /^  [A-Za-z0-9_-]+:/ { exit }
        in_block                        { print }
    ' "$BUNDLE_YML")"
    if [[ -n "$target_block" ]] && grep -qE '<[A-Z_]+>' <<<"$target_block"; then
        offenders="$(grep -oE '<[A-Z_]+>' <<<"$target_block" | sort -u | paste -sd ', ' -)"
        echo "error: engine/databricks.yml target '$TARGET' contains unsubstituted placeholder(s): $offenders" >&2
        echo "  Edit engine/databricks.yml and replace them with real values (e.g. workspace URL, catalog)." >&2
        exit 3
    fi
fi

echo "Deploying watchdog bundle to target=$TARGET"
databricks bundle validate -t "$TARGET"
databricks bundle deploy -t "$TARGET"
echo "Deploy complete. Next: databricks bundle run watchdog_daily_scan -t $TARGET"
