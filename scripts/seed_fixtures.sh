#!/usr/bin/env bash
# Seed the watchdog e2e fixture tables in a target workspace by running the
# watchdog_e2e_test job with cleanup=false. The fixtures stay in place so you
# can run ad-hoc queries against them and exercise the dashboards.
#
# Usage: scripts/seed_fixtures.sh <target>

set -euo pipefail

TARGET="${1:-}"
if [[ -z "$TARGET" ]]; then
    echo "Usage: $0 <target>" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT/engine"

databricks bundle run watchdog_e2e_test -t "$TARGET" --params cleanup=false
