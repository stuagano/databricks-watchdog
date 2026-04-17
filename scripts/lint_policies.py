#!/usr/bin/env python3
"""Lint every policy YAML file in the repo.

Checks the following invariants for each policy entry:
  - ``id`` is present, non-empty, and unique across all files.
  - ``name`` is present.
  - ``severity`` is one of critical/high/medium/low.
  - Either ``rule`` or ``rule.ref`` is defined (shorthand string is fine).
  - ``applies_to`` or ``resource_types`` is present (legacy form OK).

Exits non-zero on any violation. Intended for pre-commit and CI.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml

VALID_SEVERITIES = {"critical", "high", "medium", "low"}


def _iter_policy_files(root: Path):
    for sub in ("engine/policies", "library"):
        base = root / sub
        if not base.exists():
            continue
        for path in base.rglob("*.yml"):
            yield path


def lint(root: Path) -> list[str]:
    errors: list[str] = []
    seen_ids: dict[str, Path] = {}

    for path in _iter_policy_files(root):
        try:
            data = yaml.safe_load(path.read_text())
        except yaml.YAMLError as e:
            errors.append(f"{path}: YAML parse error — {e}")
            continue
        if not data or "policies" not in data:
            continue
        for i, policy in enumerate(data.get("policies") or []):
            loc = f"{path}[{i}]"
            pid = policy.get("id", "")
            if not pid:
                errors.append(f"{loc}: missing 'id'")
                continue
            if pid in seen_ids:
                errors.append(f"{loc}: duplicate id {pid!r} (also in {seen_ids[pid]})")
            else:
                seen_ids[pid] = path
            if not policy.get("name"):
                errors.append(f"{loc}: {pid} missing 'name'")
            sev = policy.get("severity", "medium")
            if sev not in VALID_SEVERITIES:
                errors.append(f"{loc}: {pid} invalid severity {sev!r}")
            rule = policy.get("rule")
            if not rule and not isinstance(rule, str):
                errors.append(f"{loc}: {pid} missing 'rule'")
            if "applies_to" not in policy and "resource_types" not in policy:
                errors.append(f"{loc}: {pid} must define 'applies_to' or 'resource_types'")

    return errors


def main() -> int:
    root = Path(__file__).resolve().parent.parent
    errors = lint(root)
    if errors:
        print("Policy lint failed:")
        for e in errors:
            print(f"  - {e}")
        return 1
    print("Policy lint passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
