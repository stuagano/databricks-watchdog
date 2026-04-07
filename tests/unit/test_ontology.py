"""Unit tests for OntologyEngine — tag-based resource classification.

Tests use the live ontologies/resource_classes.yml so they catch any
tag-key or class-name changes that break expected classification.

Run with: pytest tests/unit/test_ontology.py -v
"""
import pytest
from watchdog.ontology import OntologyEngine


@pytest.fixture(scope="module")
def engine(ontology_dir):
    return OntologyEngine(ontology_dir=ontology_dir)


# ── Base class matching ──────────────────────────────────────────────────────

class TestBaseClasses:
    def test_table_is_data_asset(self, engine):
        result = engine.classify("table", {}, {})
        assert "DataAsset" in result.classes

    def test_volume_is_data_asset(self, engine):
        result = engine.classify("volume", {}, {})
        assert "DataAsset" in result.classes

    def test_cluster_is_compute_asset(self, engine):
        result = engine.classify("cluster", {}, {})
        assert "ComputeAsset" in result.classes

    def test_job_is_compute_asset(self, engine):
        result = engine.classify("job", {}, {})
        assert "ComputeAsset" in result.classes

    def test_unknown_type_has_no_classes(self, engine):
        result = engine.classify("unknown_resource_type", {}, {})
        assert result.classes == []


# ── Data classification tags ─────────────────────────────────────────────────

class TestDataClassificationTags:
    def test_pii_table_classified(self, engine):
        """PiiTable requires data_classification=pii on a table resource_type."""
        result = engine.classify("table", {"data_classification": "pii"}, {})
        assert "PiiTable" in result.classes
        assert "PiiAsset" in result.classes
        assert "ConfidentialAsset" in result.classes
        assert "InternalAsset" in result.classes

    def test_pii_table_not_for_volume(self, engine):
        """PiiTable classifier requires metadata_equals resource_type=table."""
        result = engine.classify("volume", {"data_classification": "pii"}, {})
        assert "PiiTable" not in result.classes
        assert "PiiAsset" in result.classes  # PiiAsset has no resource_type constraint

    def test_confidential_classification(self, engine):
        result = engine.classify("table", {"data_classification": "confidential"}, {})
        assert "ConfidentialAsset" in result.classes
        assert "InternalAsset" in result.classes
        assert "PiiAsset" not in result.classes

    def test_gold_table_classified(self, engine):
        result = engine.classify("table", {"data_layer": "gold"}, {})
        assert "GoldTable" in result.classes
        assert "DataAsset" in result.classes

    def test_silver_table_classified(self, engine):
        result = engine.classify("table", {"data_layer": "silver"}, {})
        assert "SilverTable" in result.classes

    def test_bronze_table_classified(self, engine):
        result = engine.classify("table", {"data_layer": "bronze"}, {})
        assert "BronzeTable" in result.classes

    def test_untagged_table_only_base_class(self, engine):
        result = engine.classify("table", {}, {})
        assert result.classes == ["DataAsset"]

    def test_multiple_derived_classes_same_resource(self, engine):
        """A PII table owned by dosimetry BU gets both PiiTable and DosimetryAsset."""
        tags = {"data_classification": "pii", "business_unit": "dosimetry"}
        all_classes = engine.get_all_classes_for_resource("table", tags, {})
        assert "PiiTable" in all_classes
        assert "DosimetryAsset" in all_classes


# ── Export control classifications ───────────────────────────────────────────

class TestExportControl:
    def test_itar_classified(self, engine):
        tags = {"export_classification": "ITAR", "data_classification": "confidential"}
        all_classes = engine.get_all_classes_for_resource("table", tags, {})
        assert "ItarAsset" in all_classes
        assert "ExportControlledAsset" in all_classes
        assert "ConfidentialAsset" in all_classes

    def test_ear_classified(self, engine):
        tags = {"export_classification": "EAR", "data_classification": "restricted"}
        all_classes = engine.get_all_classes_for_resource("table", tags, {})
        assert "EarAsset" in all_classes
        assert "ExportControlledAsset" in all_classes

    def test_wrong_export_value_not_classified(self, engine):
        tags = {"export_classification": "NONE"}
        result = engine.classify("table", tags, {})
        assert "ExportControlledAsset" not in result.classes
        assert "ItarAsset" not in result.classes


# ── Compute specializations ──────────────────────────────────────────────────

class TestComputeClassifications:
    def test_interactive_cluster(self, engine):
        """Cluster without cluster_type=job is interactive."""
        result = engine.classify("cluster", {}, {})
        assert "InteractiveCluster" in result.classes

    def test_job_cluster_not_interactive(self, engine):
        """cluster_type=job tag disqualifies from InteractiveCluster."""
        result = engine.classify("cluster", {"cluster_type": "job"}, {})
        assert "InteractiveCluster" not in result.classes

    def test_production_job(self, engine):
        result = engine.classify("job", {"environment": "prod"}, {})
        assert "ProductionJob" in result.classes

    def test_critical_job(self, engine):
        tags = {"environment": "prod", "criticality": "high"}
        all_classes = engine.get_all_classes_for_resource("job", tags, {})
        assert "CriticalJob" in all_classes
        assert "ProductionJob" in all_classes

    def test_dev_cluster_is_development_compute(self, engine):
        result = engine.classify("cluster", {"environment": "dev"}, {})
        assert "DevelopmentCompute" in result.classes

    def test_production_pipeline(self, engine):
        result = engine.classify("pipeline", {"environment": "prod"}, {})
        assert "ProductionPipeline" in result.classes

    def test_unattributed_asset_no_cost_center(self, engine):
        """Cluster with no cost_center tag → UnattributedAsset."""
        result = engine.classify("cluster", {}, {})
        assert "UnattributedAsset" in result.classes

    def test_attributed_asset_has_cost_center(self, engine):
        """Cluster WITH cost_center tag → NOT UnattributedAsset."""
        result = engine.classify("cluster", {"cost_center": "CC-1234"}, {})
        assert "UnattributedAsset" not in result.classes


# ── Inheritance chains ───────────────────────────────────────────────────────

class TestAncestorChains:
    def test_pii_table_ancestor_chain(self, engine):
        chain = engine.get_ancestor_chain("PiiTable")
        assert chain == ["PiiTable", "PiiAsset", "DataAsset"]

    def test_critical_job_ancestor_chain(self, engine):
        chain = engine.get_ancestor_chain("CriticalJob")
        assert chain == ["CriticalJob", "ProductionJob", "ComputeAsset"]

    def test_itar_asset_ancestor_chain(self, engine):
        chain = engine.get_ancestor_chain("ItarAsset")
        assert chain == ["ItarAsset", "ExportControlledAsset", "ConfidentialAsset", "DataAsset"]

    def test_get_all_classes_includes_ancestors(self, engine):
        """get_all_classes expands each matched class to its full ancestor chain."""
        tags = {"data_classification": "pii"}
        all_classes = engine.get_all_classes_for_resource("table", tags, {})
        # Direct matches
        assert "PiiTable" in all_classes
        assert "PiiAsset" in all_classes
        assert "ConfidentialAsset" in all_classes
        assert "InternalAsset" in all_classes
        # Inherited base class
        assert "DataAsset" in all_classes

    def test_base_class_is_root(self, engine):
        chain = engine.get_ancestor_chain("DataAsset")
        assert chain == ["DataAsset"]  # no parent
