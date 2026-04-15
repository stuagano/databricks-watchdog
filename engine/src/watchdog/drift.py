"""Drift Detection — expected state loading and lookup building.

Loads expected state JSON files from local paths (UC volume mounts in
Databricks, local files in tests). Builds lookup structures for the
policy engine to inject into resource metadata before rule evaluation.

The rule engine's drift_check evaluator consumes the injected metadata
without knowing where it came from — keeping the rule engine pure.
"""

import json
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


def load_expected_state(file_path: str) -> dict:
    """Load expected state from a JSON file.

    Args:
        file_path: Path to the expected state JSON file. In Databricks,
            this is a UC volume mount path like
            /Volumes/{catalog}/{schema}/{volume}/expected_state.json

    Returns:
        Parsed JSON dict, or empty dict if file not found or invalid.
    """
    try:
        with open(file_path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning("Expected state file not found: %s", file_path)
        return {}
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load expected state from %s: %s", file_path, e)
        return {}


def build_expected_grants_lookup(grants: list[dict]) -> dict[str, list[dict]]:
    """Build a lookup from principal name to expected grant entries.

    Args:
        grants: List of grant entries from expected_state.json, each with
            catalog, schema, table, principal, and privileges.

    Returns:
        Dict mapping principal name to list of their expected grant entries.
    """
    lookup: dict[str, list[dict]] = defaultdict(list)
    for entry in grants:
        principal = entry.get("principal", "")
        if principal:
            lookup[principal].append(entry)
    return dict(lookup)
