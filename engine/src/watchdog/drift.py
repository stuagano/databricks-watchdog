"""Drift Detection — expected state loading and lookup building.

Loads expected state JSON files from local paths (UC volume mounts in
Databricks, local files in tests). Builds lookup structures for the
policy engine to inject into resource metadata before rule evaluation.

The rule engine's drift_check evaluator consumes the injected metadata
without knowing where it came from — keeping the rule engine pure.
"""

import json
import logging
import tarfile
from collections import defaultdict

logger = logging.getLogger(__name__)


def _load_from_bundle(path: str) -> dict:
    """Extract and parse data.json from an OPA-style .tar.gz bundle.

    Returns:
        Parsed dict from data.json, or empty dict if data.json is absent.
    """
    try:
        with tarfile.open(path, "r:gz") as tf:
            try:
                member = tf.getmember("data.json")
            except KeyError:
                logger.warning("Bundle %s has no data.json member", path)
                return {}
            f = tf.extractfile(member)
            if f is None:
                return {}
            return json.loads(f.read())
    except (tarfile.TarError, OSError) as e:
        logger.warning("Failed to open bundle %s: %s", path, e)
        return {}


def load_expected_state(file_path: str, data_path: str | None = None) -> dict:
    """Load expected state from a JSON file or OPA .tar.gz bundle.

    Args:
        file_path: Path to the expected state JSON file or .tar.gz bundle.
            In Databricks, this is a UC volume mount path like
            /Volumes/{catalog}/{schema}/{volume}/expected_state.json
        data_path: Optional single top-level key to navigate into the loaded dict.
            E.g. "permissions" returns data["permissions"]. Dot-separated paths are not supported.

    Returns:
        Parsed JSON dict (or nested sub-dict), or empty dict if file not
        found, invalid, or data_path key is missing.
    """
    try:
        if file_path.endswith(".tar.gz"):
            data = _load_from_bundle(file_path)
        else:
            with open(file_path) as f:
                data = json.load(f)
    except FileNotFoundError:
        logger.warning("Expected state file not found: %s", file_path)
        return {}
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Failed to load expected state from %s: %s", file_path, e)
        return {}

    if data_path is not None:
        data = data.get(data_path, {})
    return data


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


def build_expected_row_filters_lookup(
    row_filters: list[dict],
) -> dict[str, dict]:
    """Keyed by table_full_name → {table, function}.

    Note: the expected-state JSON uses "table" (not "table_full_name") as
    the field name for the full table identifier.
    """
    lookup = {}
    for entry in row_filters:
        key = entry.get("table", "")
        if key:
            lookup[key] = entry
    return lookup


def build_expected_column_masks_lookup(
    column_masks: list[dict],
) -> dict[str, dict]:
    """Keyed by '{table}.{column}' → {table, column, function}.

    Note: the expected-state JSON uses "table" (not "table_full_name") as
    the field name for the full table identifier.
    """
    lookup = {}
    for entry in column_masks:
        table = entry.get("table", "")
        column = entry.get("column", "")
        if table and column:
            lookup[f"{table}.{column}"] = entry
    return lookup


def build_expected_group_membership_lookup(
    group_membership: list[dict],
) -> dict[str, set[str]]:
    """Keyed by group_name → set of expected member values."""
    lookup = {}
    for entry in group_membership:
        key = entry.get("group", "")
        members = entry.get("members") or []
        if key:
            lookup[key] = set(members)
    return lookup
