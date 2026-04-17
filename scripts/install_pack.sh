#!/usr/bin/env bash
# Install a policy pack into a Databricks Watchdog target environment.
#
# Usage: scripts/install_pack.sh <pack> --target <target>
#   pack    — one of: healthcare, financial, defense, general
#   target  — Databricks bundle target (e.g. fe-stable)

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MERGE_SCRIPT="$REPO_ROOT/scripts/_merge_pack.py"

# ── Argument parsing ────────────────────────────────────────────────────────

usage() {
    echo "Usage: $0 <pack> --target <target>" >&2
    echo "  pack    one of: healthcare, financial, defense, general" >&2
    echo "  target  Databricks bundle target (e.g. fe-stable)" >&2
    exit 1
}

PACK=""
TARGET=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)
            shift
            [[ $# -gt 0 ]] || usage
            TARGET="$1"
            shift
            ;;
        --*)
            echo "Unknown option: $1" >&2
            usage
            ;;
        *)
            if [[ -z "$PACK" ]]; then
                PACK="$1"
                shift
            else
                echo "Unexpected argument: $1" >&2
                usage
            fi
            ;;
    esac
done

[[ -n "$PACK" ]]   || { echo "Error: pack name is required." >&2; usage; }
[[ -n "$TARGET" ]] || { echo "Error: --target is required." >&2; usage; }

# ── Validation ──────────────────────────────────────────────────────────────

PACK_DIR="$REPO_ROOT/library/$PACK"

if [[ ! -d "$PACK_DIR" ]]; then
    echo "Error: library pack directory not found: $PACK_DIR" >&2
    usage
fi

for required_file in ontology_classes.yml rule_primitives.yml policies.yml; do
    if [[ ! -f "$PACK_DIR/$required_file" ]]; then
        echo "Error: missing required file: $PACK_DIR/$required_file" >&2
        exit 1
    fi
done

# ── Installation steps ──────────────────────────────────────────────────────

echo "Installing $PACK pack → $TARGET"

echo ""
echo "Ontology classes:"
python3 "$MERGE_SCRIPT" merge-classes "$PACK"

echo ""
echo "Rule primitives:"
python3 "$MERGE_SCRIPT" merge-primitives "$PACK"

echo ""
echo "Policies:"
python3 "$MERGE_SCRIPT" copy-policies "$PACK"

echo ""
echo "Deploying bundle to $TARGET..."
(cd "$REPO_ROOT/engine" && databricks bundle deploy -t "$TARGET")

echo ""
echo "Syncing policies to Delta..."
"$REPO_ROOT/scripts/sync_policies.sh" "$TARGET"

# ── Summary ─────────────────────────────────────────────────────────────────

echo ""
echo "════════════════════════════════════════════"
echo "$PACK pack installed → $TARGET"
echo ""
echo "Active on next scan. To scan now:"
echo "  databricks bundle run watchdog_daily_scan -t $TARGET"
echo "════════════════════════════════════════════"
