# Policy Pack Install — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A single command (`scripts/install_pack.sh healthcare --target fe-stable`) that installs an industry policy pack and deploys it to a Databricks workspace, ready for the next scan.

**Architecture:** A bash script orchestrates five steps: merge ontology classes, merge rule primitives, copy policies, bundle deploy, and sync to Delta. A Python helper handles YAML merging with collision detection and idempotency. Extends the existing `library/` → `engine/` file flow.

**Tech Stack:** Bash, Python 3, PyYAML, `databricks bundle deploy`, existing `scripts/sync_policies.sh`

---

### Task 1: YAML Merge Helper — `_merge_pack.py`

**Files:**
- Create: `scripts/_merge_pack.py`
- Test: `tests/unit/test_merge_pack.py`

**Context:** This Python script handles merging ontology classes and rule primitives from a library pack into the engine YAML files. It has two subcommands: `merge-classes` and `merge-primitives`. Both follow the same merge logic: append new entries, skip identical ones, error on collisions. It also has a `copy-policies` subcommand that copies the pack policies file to `engine/policies/<pack>.yml`.

The engine ontology files have this structure:
- `engine/ontologies/resource_classes.yml` has a `derived_classes:` top-level key containing a dict of class definitions
- `engine/ontologies/rule_primitives.yml` has a `primitives:` top-level key containing a dict of primitive definitions

The library pack files mirror this:
- `library/<pack>/ontology_classes.yml` has a `derived_classes:` key
- `library/<pack>/rule_primitives.yml` has a `primitives:` key
- `library/<pack>/policies.yml` is copied as-is

- [ ] **Step 1: Write failing tests for merge_classes**

```python
# tests/unit/test_merge_pack.py
"""Tests for scripts/_merge_pack.py — YAML merge logic."""

import textwrap
from pathlib import Path

import pytest
import yaml


# ── Helpers ──────────────────────────────────────────────────────────

def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(content))


def _read_yaml(path: Path) -> dict:
    return yaml.safe_load(path.read_text())


# Import after writing tests so we get a clean ImportError on first run
from scripts._merge_pack import merge_classes, merge_primitives, copy_policies


# ── merge_classes ────────────────────────────────────────────────────

class TestMergeClasses:
    def test_adds_new_classes_to_empty_engine(self, tmp_path):
        engine_file = tmp_path / "engine" / "resource_classes.yml"
        pack_file = tmp_path / "library" / "ontology_classes.yml"

        _write(engine_file, """\
            derived_classes: {}
        """)
        _write(pack_file, """\
            derived_classes:
              PhiAsset:
                parent: ConfidentialAsset
                description: "PHI data"
                classifier:
                  tag_equals:
                    data_classification: "phi"
        """)

        added, skipped = merge_classes(pack_file, engine_file, "healthcare")
        assert added == ["PhiAsset"]
        assert skipped == []

        result = _read_yaml(engine_file)
        assert "PhiAsset" in result["derived_classes"]
        assert result["derived_classes"]["PhiAsset"]["parent"] == "ConfidentialAsset"

    def test_adds_new_classes_alongside_existing(self, tmp_path):
        engine_file = tmp_path / "engine" / "resource_classes.yml"
        pack_file = tmp_path / "library" / "ontology_classes.yml"

        _write(engine_file, """\
            derived_classes:
              PiiAsset:
                parent: DataAsset
                description: "PII data"
                classifier:
                  tag_equals:
                    data_classification: "pii"
        """)
        _write(pack_file, """\
            derived_classes:
              PhiAsset:
                parent: ConfidentialAsset
                description: "PHI data"
                classifier:
                  tag_equals:
                    data_classification: "phi"
        """)

        added, skipped = merge_classes(pack_file, engine_file, "healthcare")
        assert added == ["PhiAsset"]

        result = _read_yaml(engine_file)
        assert "PiiAsset" in result["derived_classes"]
        assert "PhiAsset" in result["derived_classes"]

    def test_skips_identical_class(self, tmp_path):
        engine_file = tmp_path / "engine" / "resource_classes.yml"
        pack_file = tmp_path / "library" / "ontology_classes.yml"

        class_def = """\
            derived_classes:
              PhiAsset:
                parent: ConfidentialAsset
                description: "PHI data"
                classifier:
                  tag_equals:
                    data_classification: "phi"
        """
        _write(engine_file, class_def)
        _write(pack_file, class_def)

        added, skipped = merge_classes(pack_file, engine_file, "healthcare")
        assert added == []
        assert skipped == ["PhiAsset"]

    def test_errors_on_collision(self, tmp_path):
        engine_file = tmp_path / "engine" / "resource_classes.yml"
        pack_file = tmp_path / "library" / "ontology_classes.yml"

        _write(engine_file, """\
            derived_classes:
              PhiAsset:
                parent: DataAsset
                description: "Different definition"
                classifier:
                  tag_equals:
                    data_classification: "phi"
        """)
        _write(pack_file, """\
            derived_classes:
              PhiAsset:
                parent: ConfidentialAsset
                description: "PHI data"
                classifier:
                  tag_equals:
                    data_classification: "phi"
        """)

        original_content = engine_file.read_text()

        with pytest.raises(SystemExit):
            merge_classes(pack_file, engine_file, "healthcare")

        # File must not be modified on collision
        assert engine_file.read_text() == original_content
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && python -m pytest tests/unit/test_merge_pack.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'scripts._merge_pack'` or `ImportError`

- [ ] **Step 3: Implement merge_classes**

```python
#!/usr/bin/env python3
"""YAML merge helper for policy pack installation.

Subcommands:
  merge-classes    Merge ontology classes from a library pack into the engine
  merge-primitives Merge rule primitives from a library pack into the engine
  copy-policies    Copy policy file from library pack to engine/policies/
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import yaml


REPO_ROOT = Path(__file__).resolve().parent.parent


def _load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _dump_yaml(data: dict, path: Path, pack_name: str, section_key: str,
               new_keys: list[str]) -> None:
    """Write merged YAML back, appending new entries with a section comment."""
    # Read original file content to preserve formatting for existing entries
    original = path.read_text()

    if not new_keys:
        return

    # Build the new entries block
    pack_label = {
        "healthcare": "Healthcare (HIPAA)",
        "financial": "Financial Services",
        "defense": "Defense / ITAR",
        "general": "General Governance",
    }.get(pack_name, pack_name.title())

    lines = [f"\n  # ── {pack_label} ──────────────────────────────────"]
    for key in new_keys:
        lines.append("")
        lines.append(f"  {key}:")
        entry = data[section_key][key]
        entry_yaml = yaml.dump({key: entry}, default_flow_style=False, sort_keys=False)
        # Strip the top-level key line (we already wrote it) and indent
        for line in entry_yaml.split("\n")[1:]:
            if line.strip():
                lines.append(f"  {line}")
            elif line:
                lines.append("")

    # Remove trailing blank lines
    while lines and not lines[-1].strip():
        lines.pop()

    path.write_text(original.rstrip() + "\n" + "\n".join(lines) + "\n")


def merge_classes(pack_file: Path, engine_file: Path,
                  pack_name: str) -> tuple[list[str], list[str]]:
    """Merge derived_classes from pack into engine.

    Returns (added, skipped) class name lists.
    Exits with sys.exit(1) on collision — engine file not modified.
    """
    pack_data = _load_yaml(pack_file)
    engine_data = _load_yaml(engine_file)

    pack_classes = pack_data.get("derived_classes", {})
    engine_classes = engine_data.get("derived_classes", {})

    added = []
    skipped = []

    for name, definition in pack_classes.items():
        if name in engine_classes:
            if engine_classes[name] == definition:
                skipped.append(name)
            else:
                print(f"ERROR: Class '{name}' already exists with different "
                      f"content. Resolve manually.", file=sys.stderr)
                sys.exit(1)
        else:
            added.append(name)
            engine_classes[name] = definition

    if added:
        engine_data["derived_classes"] = engine_classes
        _dump_yaml(engine_data, engine_file, pack_name, "derived_classes", added)

    return added, skipped


def merge_primitives(pack_file: Path, engine_file: Path,
                     pack_name: str) -> tuple[list[str], list[str]]:
    """Merge primitives from pack into engine.

    Returns (added, skipped) primitive name lists.
    Exits with sys.exit(1) on collision — engine file not modified.
    """
    pack_data = _load_yaml(pack_file)
    engine_data = _load_yaml(engine_file)

    pack_prims = pack_data.get("primitives", {})
    engine_prims = engine_data.get("primitives", {})

    added = []
    skipped = []

    for name, definition in pack_prims.items():
        if name in engine_prims:
            if engine_prims[name] == definition:
                skipped.append(name)
            else:
                print(f"ERROR: Primitive '{name}' already exists with different "
                      f"content. Resolve manually.", file=sys.stderr)
                sys.exit(1)
        else:
            added.append(name)
            engine_prims[name] = definition

    if added:
        engine_data["primitives"] = engine_prims
        _dump_yaml(engine_data, engine_file, pack_name, "primitives", added)

    return added, skipped


def copy_policies(pack_file: Path, dest_file: Path) -> str:
    """Copy pack policies to engine/policies/.

    Returns: 'copied', 'skipped' (identical), or 'updated' (overwritten).
    """
    pack_content = pack_file.read_text()

    if dest_file.exists():
        existing = dest_file.read_text()
        if existing == pack_content:
            return "skipped"
        dest_file.write_text(pack_content)
        return "updated"

    dest_file.parent.mkdir(parents=True, exist_ok=True)
    dest_file.write_text(pack_content)
    return "copied"


def main():
    parser = argparse.ArgumentParser(description="Policy pack YAML merge helper")
    sub = parser.add_subparsers(dest="command", required=True)

    # merge-classes
    mc = sub.add_parser("merge-classes")
    mc.add_argument("pack", choices=["healthcare", "financial", "defense", "general"])

    # merge-primitives
    mp = sub.add_parser("merge-primitives")
    mp.add_argument("pack", choices=["healthcare", "financial", "defense", "general"])

    # copy-policies
    cp = sub.add_parser("copy-policies")
    cp.add_argument("pack", choices=["healthcare", "financial", "defense", "general"])

    args = parser.parse_args()

    pack_dir = REPO_ROOT / "library" / args.pack

    if args.command == "merge-classes":
        pack_file = pack_dir / "ontology_classes.yml"
        engine_file = REPO_ROOT / "engine" / "ontologies" / "resource_classes.yml"
        added, skipped = merge_classes(pack_file, engine_file, args.pack)
        for name in added:
            print(f"  ✓ {name}")
        for name in skipped:
            print(f"  · {name} (already present)")
        print(f"\n{len(added)} classes added, {len(skipped)} skipped")

    elif args.command == "merge-primitives":
        pack_file = pack_dir / "rule_primitives.yml"
        engine_file = REPO_ROOT / "engine" / "ontologies" / "rule_primitives.yml"
        added, skipped = merge_primitives(pack_file, engine_file, args.pack)
        for name in added:
            print(f"  ✓ {name}")
        for name in skipped:
            print(f"  · {name} (already present)")
        print(f"\n{len(added)} primitives added, {len(skipped)} skipped")

    elif args.command == "copy-policies":
        pack_file = pack_dir / "policies.yml"
        dest_file = REPO_ROOT / "engine" / "policies" / f"{args.pack}.yml"
        result = copy_policies(pack_file, dest_file)
        print(f"  policies → engine/policies/{args.pack}.yml ({result})")


if __name__ == "__main__":
    main()
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && python -m pytest tests/unit/test_merge_pack.py::TestMergeClasses -v`
Expected: 4 PASS

- [ ] **Step 5: Commit**

```bash
git add scripts/_merge_pack.py tests/unit/test_merge_pack.py
git commit -m "feat: add YAML merge helper for policy pack install"
```

---

### Task 2: Tests for merge_primitives and copy_policies

**Files:**
- Modify: `tests/unit/test_merge_pack.py`

**Context:** Same merge logic as classes, different top-level key (`primitives` instead of `derived_classes`). `copy_policies` is simpler — copy/skip/overwrite.

- [ ] **Step 1: Add tests for merge_primitives and copy_policies**

Append to `tests/unit/test_merge_pack.py`:

```python
# ── merge_primitives ─────────────────────────────────────────────────

class TestMergePrimitives:
    def test_adds_new_primitive(self, tmp_path):
        engine_file = tmp_path / "engine" / "rule_primitives.yml"
        pack_file = tmp_path / "library" / "rule_primitives.yml"

        _write(engine_file, """\
            primitives:
              has_owner:
                type: tag_exists
                keys: [owner]
        """)
        _write(pack_file, """\
            primitives:
              has_phi_steward:
                type: tag_exists
                keys: [data_steward, phi_steward]
        """)

        added, skipped = merge_primitives(pack_file, engine_file, "healthcare")
        assert added == ["has_phi_steward"]
        assert skipped == []

        result = _read_yaml(engine_file)
        assert "has_owner" in result["primitives"]
        assert "has_phi_steward" in result["primitives"]

    def test_skips_identical_primitive(self, tmp_path):
        engine_file = tmp_path / "engine" / "rule_primitives.yml"
        pack_file = tmp_path / "library" / "rule_primitives.yml"

        prim_def = """\
            primitives:
              has_phi_steward:
                type: tag_exists
                keys: [data_steward, phi_steward]
        """
        _write(engine_file, prim_def)
        _write(pack_file, prim_def)

        added, skipped = merge_primitives(pack_file, engine_file, "healthcare")
        assert added == []
        assert skipped == ["has_phi_steward"]

    def test_errors_on_collision(self, tmp_path):
        engine_file = tmp_path / "engine" / "rule_primitives.yml"
        pack_file = tmp_path / "library" / "rule_primitives.yml"

        _write(engine_file, """\
            primitives:
              has_phi_steward:
                type: tag_exists
                keys: [owner]
        """)
        _write(pack_file, """\
            primitives:
              has_phi_steward:
                type: tag_exists
                keys: [data_steward, phi_steward]
        """)

        original = engine_file.read_text()

        with pytest.raises(SystemExit):
            merge_primitives(pack_file, engine_file, "healthcare")

        assert engine_file.read_text() == original


# ── copy_policies ────────────────────────────────────────────────────

class TestCopyPolicies:
    def test_copies_new_file(self, tmp_path):
        pack_file = tmp_path / "library" / "policies.yml"
        dest_file = tmp_path / "engine" / "policies" / "healthcare.yml"

        _write(pack_file, """\
            policies:
              - id: POL-HIPAA-001
                name: "PHI must have steward"
        """)

        result = copy_policies(pack_file, dest_file)
        assert result == "copied"
        assert dest_file.exists()
        assert dest_file.read_text() == pack_file.read_text()

    def test_skips_identical_file(self, tmp_path):
        pack_file = tmp_path / "library" / "policies.yml"
        dest_file = tmp_path / "engine" / "policies" / "healthcare.yml"

        content = """\
            policies:
              - id: POL-HIPAA-001
                name: "PHI must have steward"
        """
        _write(pack_file, content)
        _write(dest_file, content)

        result = copy_policies(pack_file, dest_file)
        assert result == "skipped"

    def test_overwrites_different_file(self, tmp_path):
        pack_file = tmp_path / "library" / "policies.yml"
        dest_file = tmp_path / "engine" / "policies" / "healthcare.yml"

        _write(dest_file, """\
            policies:
              - id: POL-HIPAA-001
                name: "Old version"
        """)
        _write(pack_file, """\
            policies:
              - id: POL-HIPAA-001
                name: "Updated version"
        """)

        result = copy_policies(pack_file, dest_file)
        assert result == "updated"
        assert dest_file.read_text() == pack_file.read_text()
```

- [ ] **Step 2: Run all tests**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && python -m pytest tests/unit/test_merge_pack.py -v`
Expected: 10 PASS (4 merge_classes + 3 merge_primitives + 3 copy_policies)

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_merge_pack.py
git commit -m "test: add merge_primitives and copy_policies tests"
```

---

### Task 3: Install Script — `install_pack.sh`

**Files:**
- Create: `scripts/install_pack.sh`

**Context:** This is the user-facing command. It validates arguments, calls `_merge_pack.py` for each step, runs `databricks bundle deploy`, then runs `scripts/sync_policies.sh`. Uses existing `scripts/sync_policies.sh` for the Delta sync step.

- [ ] **Step 1: Write the install script**

```bash
#!/usr/bin/env bash
# Install an industry policy pack into the Watchdog engine and deploy.
#
# Usage: scripts/install_pack.sh <pack> --target <target>
#
# Packs: healthcare, financial, defense, general
#
# Steps:
#   1. Merge ontology classes into engine/ontologies/resource_classes.yml
#   2. Merge rule primitives into engine/ontologies/rule_primitives.yml
#   3. Copy policies to engine/policies/<pack>.yml
#   4. Bundle deploy to push files to workspace
#   5. Sync policies to Delta table
#
# Safe to run multiple times — idempotent.

set -euo pipefail

# ── Parse arguments ──────────────────────────────────────────────────

PACK=""
TARGET=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --target)
            TARGET="$2"
            shift 2
            ;;
        healthcare|financial|defense|general)
            PACK="$1"
            shift
            ;;
        *)
            echo "Usage: $0 <pack> --target <target>" >&2
            echo "Packs: healthcare, financial, defense, general" >&2
            exit 1
            ;;
    esac
done

if [[ -z "$PACK" ]]; then
    echo "Error: specify a pack name (healthcare, financial, defense, general)" >&2
    exit 1
fi

if [[ -z "$TARGET" ]]; then
    echo "Error: --target is required" >&2
    exit 1
fi

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PACK_DIR="$REPO_ROOT/library/$PACK"
MERGE_SCRIPT="$REPO_ROOT/scripts/_merge_pack.py"

# ── Validate pack exists ─────────────────────────────────────────────

for f in ontology_classes.yml rule_primitives.yml policies.yml; do
    if [[ ! -f "$PACK_DIR/$f" ]]; then
        echo "Error: missing $PACK_DIR/$f" >&2
        exit 1
    fi
done

echo "Installing $PACK pack → $TARGET"
echo ""

# ── Step 1: Merge ontology classes ───────────────────────────────────

echo "Ontology classes:"
python3 "$MERGE_SCRIPT" merge-classes "$PACK"
echo ""

# ── Step 2: Merge rule primitives ────────────────────────────────────

echo "Rule primitives:"
python3 "$MERGE_SCRIPT" merge-primitives "$PACK"
echo ""

# ── Step 3: Copy policies ────────────────────────────────────────────

echo "Policies:"
python3 "$MERGE_SCRIPT" copy-policies "$PACK"
echo ""

# ── Step 4: Bundle deploy ────────────────────────────────────────────

echo "Deploying bundle to $TARGET..."
(cd "$REPO_ROOT/engine" && databricks bundle deploy -t "$TARGET")
echo ""

# ── Step 5: Sync policies to Delta ──────────────────────────────────

echo "Syncing policies to Delta..."
"$REPO_ROOT/scripts/sync_policies.sh" "$TARGET"
echo ""

# ── Summary ──────────────────────────────────────────────────────────

echo "════════════════════════════════════════════"
echo "$PACK pack installed → $TARGET"
echo ""
echo "Active on next scan. To scan now:"
echo "  databricks bundle run watchdog_daily_scan -t $TARGET"
echo "════════════════════════════════════════════"
```

- [ ] **Step 2: Make it executable**

```bash
chmod +x scripts/install_pack.sh
```

- [ ] **Step 3: Verify it runs (dry check, no deploy)**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && scripts/install_pack.sh --help 2>&1 || true`
Expected: prints usage message

Run: `scripts/install_pack.sh bogus --target foo 2>&1 || true`
Expected: `Usage: ...` error

- [ ] **Step 4: Commit**

```bash
git add scripts/install_pack.sh
git commit -m "feat: add install_pack.sh — one-command policy pack install"
```

---

### Task 4: Ensure `scripts/` is importable for tests

**Files:**
- Create: `scripts/__init__.py`

**Context:** The test file does `from scripts._merge_pack import merge_classes, ...`. For this import to work when running pytest from the repo root, `scripts/` needs an `__init__.py` (or we add `scripts/` to `sys.path` in conftest). An `__init__.py` is the simplest approach.

- [ ] **Step 1: Create the init file**

```python
# scripts/__init__.py — makes scripts/ importable for tests
```

- [ ] **Step 2: Run full test suite to verify imports work**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && python -m pytest tests/unit/test_merge_pack.py -v`
Expected: 10 PASS

- [ ] **Step 3: Commit**

```bash
git add scripts/__init__.py
git commit -m "chore: make scripts/ importable for test suite"
```

---

### Task 5: End-to-end dry run test

**Files:**
- Modify: `tests/unit/test_merge_pack.py`

**Context:** One integration-style test that exercises all three steps in sequence against real pack files (healthcare), verifying the full merge + copy flow without deploying.

- [ ] **Step 1: Add the e2e test**

Append to `tests/unit/test_merge_pack.py`:

```python
# ── End-to-end: full pack install (local only, no deploy) ───────────

class TestFullPackInstall:
    """Exercises all three merge/copy steps against the real healthcare pack
    files, writing to tmp copies of the engine ontology files."""

    def test_healthcare_pack_installs_cleanly(self, tmp_path):
        import shutil

        repo = Path(__file__).resolve().parent.parent.parent
        pack_dir = repo / "library" / "healthcare"

        # Copy engine ontology files to tmp so we don't mutate the real ones
        engine_classes = tmp_path / "resource_classes.yml"
        engine_prims = tmp_path / "rule_primitives.yml"
        shutil.copy(repo / "engine" / "ontologies" / "resource_classes.yml", engine_classes)
        shutil.copy(repo / "engine" / "ontologies" / "rule_primitives.yml", engine_prims)

        # Step 1: merge classes
        added_c, _ = merge_classes(
            pack_dir / "ontology_classes.yml", engine_classes, "healthcare"
        )
        assert len(added_c) > 0, "Expected at least one new class"

        # Verify all pack classes landed
        result_classes = _read_yaml(engine_classes)
        pack_classes = _read_yaml(pack_dir / "ontology_classes.yml")
        for name in pack_classes["derived_classes"]:
            assert name in result_classes["derived_classes"]

        # Step 2: merge primitives
        added_p, _ = merge_primitives(
            pack_dir / "rule_primitives.yml", engine_prims, "healthcare"
        )
        assert len(added_p) > 0, "Expected at least one new primitive"

        result_prims = _read_yaml(engine_prims)
        pack_prims = _read_yaml(pack_dir / "rule_primitives.yml")
        for name in pack_prims["primitives"]:
            assert name in result_prims["primitives"]

        # Step 3: copy policies
        dest = tmp_path / "healthcare.yml"
        status = copy_policies(pack_dir / "policies.yml", dest)
        assert status == "copied"
        assert dest.exists()

        # Idempotent: run again — should all skip
        added_c2, skipped_c2 = merge_classes(
            pack_dir / "ontology_classes.yml", engine_classes, "healthcare"
        )
        assert added_c2 == []
        assert len(skipped_c2) == len(pack_classes["derived_classes"])

        added_p2, skipped_p2 = merge_primitives(
            pack_dir / "rule_primitives.yml", engine_prims, "healthcare"
        )
        assert added_p2 == []
        assert len(skipped_p2) == len(pack_prims["primitives"])

        status2 = copy_policies(pack_dir / "policies.yml", dest)
        assert status2 == "skipped"
```

- [ ] **Step 2: Run all tests**

Run: `cd /Users/stuart.gano/Documents/Projects/databricks-watchdog && python -m pytest tests/unit/test_merge_pack.py -v`
Expected: 11 PASS

- [ ] **Step 3: Commit**

```bash
git add tests/unit/test_merge_pack.py
git commit -m "test: add e2e dry-run test for healthcare pack install"
```
