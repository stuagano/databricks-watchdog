"""Tests for scripts._merge_pack.merge_classes, merge_primitives, copy_policies."""
from __future__ import annotations

import shutil
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


# --------------------------------------------------------------------------
# TestFullPackInstall
# --------------------------------------------------------------------------


class TestFullPackInstall:
    def test_healthcare_pack_installs_cleanly(self, tmp_path: Path) -> None:
        repo = Path(__file__).resolve().parent.parent.parent
        pack_dir = repo / "library" / "healthcare"

        engine_classes = tmp_path / "resource_classes.yml"
        engine_prims = tmp_path / "rule_primitives.yml"

        shutil.copy(repo / "engine" / "ontologies" / "resource_classes.yml", engine_classes)
        shutil.copy(repo / "engine" / "ontologies" / "rule_primitives.yml", engine_prims)

        # --- First pass: merge classes ---
        added_c, skipped_c = merge_classes(pack_dir / "ontology_classes.yml", engine_classes, "healthcare")
        assert len(added_c) > 0

        pack_classes = _read_yaml(pack_dir / "ontology_classes.yml").get("derived_classes", {})
        result_classes = _read_yaml(engine_classes).get("derived_classes", {})
        for key in pack_classes:
            assert key in result_classes, f"Pack class {key!r} not found in merged engine file"

        # --- First pass: merge primitives ---
        added_p, skipped_p = merge_primitives(pack_dir / "rule_primitives.yml", engine_prims, "healthcare")
        assert len(added_p) > 0

        pack_prims = _read_yaml(pack_dir / "rule_primitives.yml").get("primitives", {})
        result_prims = _read_yaml(engine_prims).get("primitives", {})
        for key in pack_prims:
            assert key in result_prims, f"Pack primitive {key!r} not found in merged engine file"

        # --- First pass: copy policies ---
        policy_dest = tmp_path / "healthcare.yml"
        result_copy = copy_policies(pack_dir / "policies.yml", policy_dest)
        assert result_copy == "copied"

        # --- Idempotency: second pass ---
        added_c2, skipped_c2 = merge_classes(pack_dir / "ontology_classes.yml", engine_classes, "healthcare")
        assert added_c2 == []
        assert len(skipped_c2) == len(pack_classes)

        added_p2, skipped_p2 = merge_primitives(pack_dir / "rule_primitives.yml", engine_prims, "healthcare")
        assert added_p2 == []
        assert len(skipped_p2) == len(pack_prims)

        result_copy2 = copy_policies(pack_dir / "policies.yml", policy_dest)
        assert result_copy2 == "skipped"
