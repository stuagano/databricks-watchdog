"""Pipeline configuration — the single source of truth for catalog/schema FQNs.

Config-as-data: the catalog is the only knob. Schema names follow the
`mdm_ref_{bronze,silver,match,mdm,gold}` convention and every unit composes
fully-qualified table names through :meth:`Cfg.tbl` so no unit ever hardcodes a
catalog or schema string.
"""

from dataclasses import dataclass

# Logical schema key -> physical schema name. Ordered bronze->gold (the flow).
_SCHEMAS = {
    "bronze": "mdm_ref_bronze",
    "silver": "mdm_ref_silver",
    "match": "mdm_ref_match",
    "mdm": "mdm_ref_mdm",
    "gold": "mdm_ref_gold",
}


@dataclass(frozen=True)
class Cfg:
    """Resolves logical (schema_key, table) references to concrete FQNs.

    The single parameter boundary for the whole pipeline: swap ``catalog`` (and
    optionally the data/config) to stamp the reference architecture onto a new
    workspace or domain.
    """

    catalog: str = "main"

    def schema(self, key: str) -> str:
        """Return the fully-qualified schema, e.g. ``cat.mdm_ref_silver``."""
        return f"{self.catalog}.{_SCHEMAS[key]}"

    def tbl(self, key: str, name: str) -> str:
        """Return the fully-qualified table, e.g. ``cat.mdm_ref_silver.source_records``."""
        return f"{self.schema(key)}.{name}"

    # Convenience properties — schema FQNs (per the unit interface contract).
    @property
    def bronze(self) -> str:
        return self.schema("bronze")

    @property
    def silver(self) -> str:
        return self.schema("silver")

    @property
    def match(self) -> str:
        return self.schema("match")

    @property
    def mdm(self) -> str:
        return self.schema("mdm")

    @property
    def gold(self) -> str:
        return self.schema("gold")
