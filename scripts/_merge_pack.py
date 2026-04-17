"""YAML merge helper for policy pack installation.

Merges ontology classes, rule primitives, and policies from a library pack
into the Watchdog engine's YAML files.
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent

PACK_LABELS = {
    "healthcare": "Healthcare (HIPAA)",
    "financial": "Financial Services",
    "defense": "Defense / ITAR",
    "general": "General Governance",
}


def _section_banner(label: str) -> str:
    """Return a YAML comment banner for a pack section."""
    pad = max(0, 56 - len(label))
    return f"# ── {label} {'─' * pad}"


def _load_yaml(path: Path) -> dict:
    with open(path) as f:
        return yaml.safe_load(f) or {}


def _dump_yaml(path: Path, data: dict) -> None:
    with open(path, "w") as f:
        yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)


def _append_entries_with_banner(
    engine_path: Path,
    top_key: str,
    entries: dict,
    pack_label: str,
) -> None:
    """Append new entries to an engine YAML file under *top_key* with a section banner.

    We do raw text appending so the banner comment survives (PyYAML strips comments).
    """
    with open(engine_path) as f:
        raw = f.read()

    banner = _section_banner(pack_label)
    lines = [f"\n  {banner}\n"]
    for name, body in entries.items():
        dumped = yaml.dump({name: body}, default_flow_style=False, sort_keys=False)
        # Indent every line by 2 spaces (inside the top-level key block)
        for line in dumped.splitlines():
            lines.append(f"  {line}\n")
        lines.append("\n")

    with open(engine_path, "a") as f:
        f.writelines(lines)


def merge_classes(
    pack_file: Path,
    engine_file: Path,
    pack_name: str,
) -> tuple[list[str], list[str]]:
    """Merge derived_classes from *pack_file* into *engine_file*.

    Returns (added, skipped) lists of class names.
    Exits with code 1 on collision (same name, different content).
    """
    return _merge_top_key(pack_file, engine_file, pack_name, "derived_classes")


def merge_primitives(
    pack_file: Path,
    engine_file: Path,
    pack_name: str,
) -> tuple[list[str], list[str]]:
    """Merge primitives from *pack_file* into *engine_file*.

    Returns (added, skipped) lists of class names.
    Exits with code 1 on collision (same name, different content).
    """
    return _merge_top_key(pack_file, engine_file, pack_name, "primitives")


def _merge_top_key(
    pack_file: Path,
    engine_file: Path,
    pack_name: str,
    top_key: str,
) -> tuple[list[str], list[str]]:
    pack_data = _load_yaml(pack_file)
    engine_data = _load_yaml(engine_file)

    pack_entries: dict = pack_data.get(top_key) or {}
    engine_entries: dict = engine_data.get(top_key) or {}

    added: list[str] = []
    skipped: list[str] = []
    to_add: dict = {}

    for name, body in pack_entries.items():
        if name in engine_entries:
            if engine_entries[name] == body:
                skipped.append(name)
            else:
                print(
                    f"ERROR: collision on '{name}' — pack and engine have different content",
                    file=sys.stderr,
                )
                sys.exit(1)
        else:
            added.append(name)
            to_add[name] = body

    if to_add:
        label = PACK_LABELS.get(pack_name, pack_name)
        _append_entries_with_banner(engine_file, top_key, to_add, label)

    return added, skipped


def copy_policies(pack_file: Path, dest_file: Path) -> str:
    """Copy pack policies to the engine policies directory.

    Returns "copied" if new, "skipped" if identical exists, "updated" if overwritten.
    """
    pack_content = pack_file.read_bytes()

    if dest_file.exists():
        existing = dest_file.read_bytes()
        if existing == pack_content:
            return "skipped"
        else:
            dest_file.write_bytes(pack_content)
            return "updated"
    else:
        dest_file.parent.mkdir(parents=True, exist_ok=True)
        dest_file.write_bytes(pack_content)
        return "copied"


def main() -> None:
    parser = argparse.ArgumentParser(description="Merge policy pack YAML into the engine")
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd_name in ("merge-classes", "merge-primitives", "copy-policies"):
        p = sub.add_parser(cmd_name)
        p.add_argument(
            "pack",
            choices=["healthcare", "financial", "defense", "general"],
        )

    args = parser.parse_args()
    pack = args.pack

    pack_dir = REPO_ROOT / "library" / pack
    engine_onto = REPO_ROOT / "engine" / "ontologies"

    if args.command == "merge-classes":
        added, skipped = merge_classes(
            pack_dir / "ontology_classes.yml",
            engine_onto / "resource_classes.yml",
            pack,
        )
        for name in added:
            print(f"✓ {name}")
        for name in skipped:
            print(f"· {name} (already present)")

    elif args.command == "merge-primitives":
        added, skipped = merge_primitives(
            pack_dir / "rule_primitives.yml",
            engine_onto / "rule_primitives.yml",
            pack,
        )
        for name in added:
            print(f"✓ {name}")
        for name in skipped:
            print(f"· {name} (already present)")

    elif args.command == "copy-policies":
        result = copy_policies(
            pack_dir / "policies.yml",
            REPO_ROOT / "engine" / "policies" / f"{pack}.yml",
        )
        if result == "copied":
            print(f"✓ {pack}.yml copied")
        elif result == "updated":
            print(f"✓ {pack}.yml updated")
        else:
            print(f"· {pack}.yml (already present)")


if __name__ == "__main__":
    main()
