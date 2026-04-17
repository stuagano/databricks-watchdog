"""Tests for scripts._merge_pack.merge_classes, merge_primitives, copy_policies."""
from __future__ import annotations

from pathlib import Path
from textwrap import dedent

import pytest
import yaml

from scripts._merge_pack import copy_policies, merge_classes, merge_primitives


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


# --------------------------------------------------------------------------
# TestMergePrimitives
# --------------------------------------------------------------------------


class TestMergePrimitives:
    def test_adds_new_primitive(self, tmp_path: Path) -> None:
        pack = _write(
            tmp_path / "pack.yml",
            """\
            primitives:
              has_phi_steward:
                type: tag_exists
                keys: [data_steward, phi_steward]
            """,
        )
        engine = _write(
            tmp_path / "engine.yml",
            """\
            primitives:
              has_owner:
                type: tag_exists
                keys: [owner]
            """,
        )

        added, skipped = merge_primitives(pack, engine, "healthcare")

        assert added == ["has_phi_steward"]
        assert skipped == []

        result = _read_yaml(engine)
        assert "has_owner" in result["primitives"]
        assert "has_phi_steward" in result["primitives"]

    def test_skips_identical_primitive(self, tmp_path: Path) -> None:
        content = """\
        primitives:
          has_phi_steward:
            type: tag_exists
            keys: [data_steward, phi_steward]
        """
        pack = _write(tmp_path / "pack.yml", content)
        engine = _write(tmp_path / "engine.yml", content)

        added, skipped = merge_primitives(pack, engine, "healthcare")

        assert added == []
        assert skipped == ["has_phi_steward"]

    def test_errors_on_collision(self, tmp_path: Path) -> None:
        pack = _write(
            tmp_path / "pack.yml",
            """\
            primitives:
              has_phi_steward:
                type: tag_exists
                keys: [data_steward, phi_steward]
            """,
        )
        engine = _write(
            tmp_path / "engine.yml",
            """\
            primitives:
              has_phi_steward:
                type: tag_exists
                keys: [different_key]
            """,
        )
        engine_before = engine.read_text()

        with pytest.raises(SystemExit):
            merge_primitives(pack, engine, "healthcare")

        assert engine.read_text() == engine_before


# --------------------------------------------------------------------------
# TestCopyPolicies
# --------------------------------------------------------------------------


class TestCopyPolicies:
    def test_copies_new_file(self, tmp_path: Path) -> None:
        pack_file = _write(
            tmp_path / "pack" / "policies.yml",
            """\
            policies:
              - name: phi_data_policy
                description: PHI data governance policy
            """,
        )
        dest_file = tmp_path / "engine" / "policies.yml"

        result = copy_policies(pack_file, dest_file)

        assert result == "copied"
        assert dest_file.exists()
        assert dest_file.read_bytes() == pack_file.read_bytes()

    def test_skips_identical_file(self, tmp_path: Path) -> None:
        content = dedent("""\
            policies:
              - name: phi_data_policy
                description: PHI data governance policy
            """)
        pack_file = _write(tmp_path / "pack" / "policies.yml", content)
        dest_file = _write(tmp_path / "engine" / "policies.yml", content)

        result = copy_policies(pack_file, dest_file)

        assert result == "skipped"

    def test_overwrites_different_file(self, tmp_path: Path) -> None:
        pack_file = _write(
            tmp_path / "pack" / "policies.yml",
            """\
            policies:
              - name: new_policy
                description: Updated policy content
            """,
        )
        dest_file = _write(
            tmp_path / "engine" / "policies.yml",
            """\
            policies:
              - name: old_policy
                description: Old policy content
            """,
        )

        result = copy_policies(pack_file, dest_file)

        assert result == "updated"
        assert dest_file.read_bytes() == pack_file.read_bytes()
