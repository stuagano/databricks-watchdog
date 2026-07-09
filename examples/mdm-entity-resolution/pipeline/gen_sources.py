"""Synthetic multi-source generator (U1).

Emits three independent "bronze" source systems (erp, plm, procurement)
describing overlapping electronic parts, with realistic cross-source noise:
MPN case/dash formatting differences, conflicting `lifecycle_status`, and
differing `attribute_ts` per source (plm is always freshest, procurement
oldest). Each source also uses its own raw column names (deliberately
distinct per source, matching `config/mapping_spec.json`) so U3's
source->canonical projection has real per-source schema drift to resolve,
not just same-named passthrough columns. A `truth` table records which
source records refer to the same real-world part (gold truth) so downstream
matching (U4) and clustering (U5) can be scored for precision/recall and id
stability.

Deterministic: uses only `random.Random(seed)` — never wall-clock or
process-global randomness — so `generate(seed=N)` is reproducible.
"""

from __future__ import annotations

import random
from datetime import datetime, timedelta

from pipeline.config import Cfg

# commodity -> (candidate manufacturers, mpn prefix)
_COMMODITIES = [
    ("IC-opamp", ["Analog Devices", "Texas Instruments", "Renesas", "ON Semiconductor"], "OP"),
    ("IC-regulator", ["Texas Instruments", "STMicroelectronics", "ON Semiconductor", "Analog Devices"], "LM"),
    ("PMT", ["Hamamatsu", "Photonis", "ET Enterprises"], "R"),
    ("IC-adc", ["Analog Devices", "Texas Instruments", "Microchip"], "AD"),
    ("connector", ["TE Connectivity", "Amphenol", "Molex"], "CON"),
    ("MOSFET", ["Infineon", "ON Semiconductor", "Vishay"], "IRF"),
    ("capacitor", ["KEMET", "Vishay", "Murata", "AVX"], "C"),
]

_LIFECYCLE = ["active", "nrnd", "obsolete"]
_SOURCES = ["erp", "plm", "procurement"]
_TOTAL_PARTS = 120
_BASE_TS = datetime(2026, 1, 1)

# Per-source raw column name for each canonical field -- deliberately distinct
# per source (real legacy systems never agree on column names). Mirrors
# `config/mapping_spec.json`; kept as a literal here (not loaded from that
# file) because the bronze schema is U1's own fixed shape, not config-as-data.
_RAW_COLUMNS = {
    "erp": {
        "mpn": "part_number", "description": "part_desc", "manufacturer": "mfr_name",
        "commodity": "commodity_code", "lifecycle_status": "status", "specs": "spec_text",
    },
    "plm": {
        "mpn": "mpn", "description": "item_description", "manufacturer": "manufacturer_name",
        "commodity": "category", "lifecycle_status": "lifecycle", "specs": "technical_specs",
    },
    "procurement": {
        "mpn": "vendor_part_no", "description": "part_description", "manufacturer": "supplier_name",
        "commodity": "comm_class", "lifecycle_status": "life_status", "specs": "attributes_text",
    },
}


def _mpn(rng: random.Random, prefix: str, idx: int) -> str:
    suffix = rng.choice(["", "A", "B", "P", "-N"])
    return f"{prefix}{idx:03d}{suffix}"


def _noisy_mpn(rng: random.Random, mpn: str) -> str:
    """Return the canonical mpn with realistic per-source formatting noise."""
    dashed = f"{mpn[:-2]}-{mpn[-2:]}" if len(mpn) > 2 else mpn
    variants = [mpn, mpn.lower(), dashed, dashed.lower()]
    return rng.choice(variants)


def _attribute_ts(rng: random.Random, part_idx: int, src: str) -> str:
    """plm is always freshest, erp mid, procurement oldest (non-overlapping offsets)."""
    offset = {
        "procurement": rng.randint(0, 50),
        "erp": rng.randint(60, 150),
        "plm": rng.randint(200, 365),
    }[src]
    return (_BASE_TS + timedelta(days=part_idx + offset)).isoformat()


def generate(seed: int = 7) -> dict:
    """Generate three synthetic source systems + gold truth, seeded and deterministic."""
    rng = random.Random(seed)
    erp: list[dict] = []
    plm: list[dict] = []
    procurement: list[dict] = []
    truth: list[dict] = []

    n_commodities = len(_COMMODITIES)
    base_n, remainder = divmod(_TOTAL_PARTS, n_commodities)

    part_idx = 0
    for c_idx, (commodity, manufacturers, prefix) in enumerate(_COMMODITIES):
        n_parts = base_n + (1 if c_idx < remainder else 0)
        for _ in range(n_parts):
            part_idx += 1
            mpn = _mpn(rng, prefix, part_idx)
            manufacturer = rng.choice(manufacturers)
            base_desc = f"{commodity.replace('-', ' ')} {mpn}"
            specs = f"tol={rng.choice([1, 5, 10])}%, temp={rng.choice(['-40..85C', '-55..125C'])}"
            lifecycle = rng.choice(_LIFECYCLE)

            # weighted toward multi-source overlap so matching has real work to do
            n_sources = rng.choices([1, 2, 3], weights=[3, 4, 3])[0]
            chosen = rng.sample(_SOURCES, n_sources)

            members: list[str] = []
            for src in chosen:
                natural_key = f"{prefix}{part_idx:03d}-{src[:2]}"
                source_record_id = f"{src}:{natural_key}"

                src_lifecycle = lifecycle
                if src == "erp" and rng.random() < 0.25:
                    # ERP frequently lags a lifecycle change made upstream in PLM
                    src_lifecycle = rng.choice(_LIFECYCLE)

                description = base_desc if src == "plm" else f"{base_desc} rev{rng.randint(1, 3)}"
                canonical_values = {
                    "mpn": _noisy_mpn(rng, mpn),
                    "description": description,
                    "manufacturer": manufacturer,
                    "commodity": commodity,
                    "lifecycle_status": src_lifecycle,
                    "specs": specs,
                }
                raw_cols = _RAW_COLUMNS[src]
                row = {
                    "source_record_id": source_record_id,
                    "natural_key": natural_key,
                    "attribute_ts": _attribute_ts(rng, part_idx, src),
                    **{raw_cols[field]: value for field, value in canonical_values.items()},
                }
                if src == "erp":
                    erp.append(row)
                elif src == "plm":
                    plm.append(row)
                else:
                    procurement.append(row)
                members.append(source_record_id)

            truth.append({"entity": f"ENT-{part_idx:04d}", "members": members})

    return {"erp": erp, "plm": plm, "procurement": procurement, "truth": truth}


def write(spark, cfg: Cfg, data: dict) -> None:
    """Write the three bronze source tables + bronze.truth."""
    for name in ("erp", "plm", "procurement", "truth"):
        spark.createDataFrame(data[name]).write.mode("overwrite").saveAsTable(cfg.tbl("bronze", name))
