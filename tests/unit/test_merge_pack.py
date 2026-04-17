"""Tests for scripts._merge_pack.merge_classes."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from scripts._merge_pack import merge_classes


def _write(path: Path, content: str) -> Path:
    """Create parent dirs and write dedented content to *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(dedent(content))
    return path


def _read_yaml(path: Path) -> dict:
    """Return parsed YAML dict from *path*."""
    with open(path) as f:
        return yaml.safe_load(f) or {}


# --------------------------------------------------------------------------
# Tests
# --------------------------------------------------------------------------


def test_adds_new_classes_to_empty_engine(tmp_path: Path) -> None:
    pack = _write(
        tmp_path / "pack.yml",
        """\
        derived_classes:
          PhiAsset:
            parent: ConfidentialAsset
            description: "Contains PHI"
        """,
    )
    engine = _write(
        tmp_path / "engine.yml",
        """\
        derived_classes:
        """,
    )

    added, skipped = merge_classes(pack, engine, "healthcare")

    assert added == ["PhiAsset"]
    assert skipped == []

    result = _read_yaml(engine)
    assert "PhiAsset" in result["derived_classes"]
    assert result["derived_classes"]["PhiAsset"]["parent"] == "ConfidentialAsset"


def test_adds_new_classes_alongside_existing(tmp_path: Path) -> None:
    pack = _write(
        tmp_path / "pack.yml",
        """\
        derived_classes:
          PhiAsset:
            parent: ConfidentialAsset
            description: "Contains PHI"
        """,
    )
    engine = _write(
        tmp_path / "engine.yml",
        """\
        derived_classes:
          PiiAsset:
            parent: DataAsset
            description: "Contains PII"
        """,
    )

    added, skipped = merge_classes(pack, engine, "healthcare")

    assert added == ["PhiAsset"]
    assert skipped == []

    result = _read_yaml(engine)
    assert "PiiAsset" in result["derived_classes"]
    assert "PhiAsset" in result["derived_classes"]


def test_skips_identical_class(tmp_path: Path) -> None:
    content = """\
    derived_classes:
      PhiAsset:
        parent: ConfidentialAsset
        description: "Contains PHI"
    """
    pack = _write(tmp_path / "pack.yml", content)
    engine = _write(tmp_path / "engine.yml", content)

    added, skipped = merge_classes(pack, engine, "healthcare")

    assert added == []
    assert skipped == ["PhiAsset"]


def test_errors_on_collision(tmp_path: Path) -> None:
    pack = _write(
        tmp_path / "pack.yml",
        """\
        derived_classes:
          PhiAsset:
            parent: ConfidentialAsset
            description: "Pack version"
        """,
    )
    engine = _write(
        tmp_path / "engine.yml",
        """\
        derived_classes:
          PhiAsset:
            parent: ConfidentialAsset
            description: "Engine version — different"
        """,
    )
    engine_before = engine.read_text()

    with pytest.raises(SystemExit):
        merge_classes(pack, engine, "healthcare")

    # Engine file must not be modified on collision
    assert engine.read_text() == engine_before
