"""Source -> canonical mapping spec loader/validator.

The canonical model (``config/canonical_model.json``) defines the 6 fields
every golden ``gold.entities`` row carries. Each source system (erp, plm,
procurement) names its raw columns differently; ``config/mapping_spec.json``
is the single declarative record of how each source's raw columns map onto
those canonical fields. U3 (silver normalization) reads this spec instead of
hardcoding per-source column names.
"""

import json

# The 6 canonical fields every source's mapping must cover, matching
# gold.entities / silver.source_records column names (see sql/contracts.sql).
CANONICAL_FIELDS: list[str] = [
    "mpn",
    "description",
    "manufacturer",
    "commodity",
    "lifecycle_status",
    "specs",
]


def load_spec(path: str) -> dict[str, dict[str, str]]:
    """Load the mapping spec JSON: ``{source_system: {canonical_field: raw_column}}``."""
    with open(path) as f:
        return json.load(f)


def validate(spec: dict[str, dict[str, str]], sources: list[str]) -> list[str]:
    """Return missing ``(source, field)`` coverage as human-readable error strings.

    Empty list means every source in ``sources`` maps every canonical field.
    """
    errors: list[str] = []
    for source in sources:
        if source not in spec:
            errors.append(f"missing source '{source}' in mapping spec")
            continue
        source_map = spec[source]
        for field in CANONICAL_FIELDS:
            if field not in source_map or not source_map[field]:
                errors.append(f"source '{source}' missing mapping for canonical field '{field}'")
    return errors
