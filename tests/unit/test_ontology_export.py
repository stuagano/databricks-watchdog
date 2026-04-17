"""Unit tests for watchdog.ontology_export — Turtle serialization."""

from pathlib import Path

import pytest
from watchdog.ontology import OntologyEngine
from watchdog.ontology_export import (
    _classifier_to_annotations,
    _escape_turtle,
    export_for_ontos,
    export_turtle,
)


@pytest.fixture(scope="module")
def ontology():
    return OntologyEngine()


class TestEscapeTurtle:
    def test_escapes_quotes(self):
        assert _escape_turtle('hello "world"') == 'hello \\"world\\"'

    def test_escapes_newlines(self):
        assert _escape_turtle("a\nb") == "a\\nb"

    def test_escapes_backslash(self):
        assert _escape_turtle("a\\b") == "a\\\\b"

    def test_passthrough_plain(self):
        assert _escape_turtle("plain text") == "plain text"


class TestClassifierAnnotations:
    def test_none_returns_empty(self):
        assert _classifier_to_annotations(None) == []

    def test_empty_dict_returns_empty(self):
        assert _classifier_to_annotations({}) == []

    def test_tag_equals_flattens(self):
        out = _classifier_to_annotations({"tag_equals": {"env": "prod"}})
        assert out == ["env=prod"]

    def test_tag_in_joins_values(self):
        out = _classifier_to_annotations({"tag_in": {"layer": ["bronze", "silver"]}})
        assert out == ["layer IN [bronze,silver]"]

    def test_tag_exists(self):
        out = _classifier_to_annotations({"tag_exists": ["pii"]})
        assert out == ["pii EXISTS"]

    def test_any_of_prefixes(self):
        out = _classifier_to_annotations({
            "any_of": [
                {"tag_equals": {"a": "1"}},
                {"tag_equals": {"b": "2"}},
            ]
        })
        assert "ANY:a=1" in out
        assert "ANY:b=2" in out

    def test_none_of_prefixes(self):
        out = _classifier_to_annotations({
            "none_of": [{"tag_equals": {"x": "y"}}]
        })
        assert out == ["NOT:x=y"]


class TestExportTurtle:
    def test_returns_non_empty_string(self, ontology):
        content = export_turtle(ontology)
        assert isinstance(content, str)
        assert len(content) > 0

    def test_declares_expected_prefixes(self, ontology):
        content = export_turtle(ontology)
        for prefix in ("owl:", "rdf:", "rdfs:", "skos:", "xsd:", "wd:", "wdc:", "wdp:"):
            assert f"@prefix {prefix}" in content

    def test_names_each_class(self, ontology):
        content = export_turtle(ontology)
        for cls in ontology.classes.values():
            assert f"wdc:{cls.name}" in content

    def test_classes_are_owl_classes(self, ontology):
        content = export_turtle(ontology)
        assert "rdf:type owl:Class" in content

    def test_writes_to_file_when_path_given(self, ontology, tmp_path):
        out = tmp_path / "watchdog.ttl"
        export_turtle(ontology, str(out))
        assert out.exists()
        assert out.stat().st_size > 0

    def test_file_and_return_match(self, ontology, tmp_path):
        out = tmp_path / "watchdog.ttl"
        content = export_turtle(ontology, str(out))
        assert out.read_text() == content

    def test_closing_punctuation(self, ontology):
        content = export_turtle(ontology)
        # Every class block should end with ' .' not ' ;' (last triple closes)
        # A trailing dangling ';' would mean the class isn't closed.
        assert " ;\n\n# ──" not in content


class TestExportForOntos:
    def test_creates_two_files(self, tmp_path):
        result = export_for_ontos(output_dir=str(tmp_path))
        assert Path(result["turtle_path"]).exists()
        assert Path(result["registry_path"]).exists()

    def test_class_count_matches_engine(self, tmp_path):
        ont = OntologyEngine()
        result = export_for_ontos(ont, output_dir=str(tmp_path))
        assert result["class_count"] == len(ont.classes)

    def test_registry_entry_is_yaml_like(self, tmp_path):
        result = export_for_ontos(output_dir=str(tmp_path))
        text = Path(result["registry_path"]).read_text()
        assert "id: watchdog-governance" in text
        assert "handler: simple_owl" in text
