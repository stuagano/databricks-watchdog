#!/usr/bin/env python3
"""Export the watchdog ontology as OWL/Turtle for Ontos import.

Writes to ``engine/ontologies/export/`` by default. Override with --output-dir.

    python scripts/export_ontology.py
    python scripts/export_ontology.py --output-dir /tmp/watchdog
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "engine" / "src"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", default=None,
                        help="Output directory (defaults to engine/ontologies/export)")
    args = parser.parse_args()

    from watchdog.ontology import OntologyEngine
    from watchdog.ontology_export import export_for_ontos

    result = export_for_ontos(OntologyEngine(), args.output_dir)
    print(f"Wrote {result['class_count']} classes to:")
    print(f"  {result['turtle_path']}")
    print(f"  {result['registry_path']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
