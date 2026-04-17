"""Ontology Export — serialize the Watchdog resource class hierarchy as OWL/Turtle.

Generates a .ttl file that Ontos (databrickslabs/ontos) can import directly
via its simple_owl handler. The export uses standard OWL/SKOS vocabulary:

  - owl:Class for each resource class
  - rdfs:subClassOf for parent relationships
  - skos:definition for descriptions
  - rdfs:label for human-readable names
  - Custom properties for classifier metadata

This bridges Watchdog's scan engine with Ontos' governance UI: Watchdog
classifies resources and writes violations to Delta, Ontos imports the
ontology for visualization and semantic linking.
"""

from datetime import datetime, timezone
from pathlib import Path

from watchdog.ontology import OntologyEngine

# OWL/Turtle namespace prefixes
PREFIXES = """\
@prefix owl:   <http://www.w3.org/2002/07/owl#> .
@prefix rdf:   <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .
@prefix skos:  <http://www.w3.org/2004/02/skos/core#> .
@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> .
@prefix wd:    <https://example.com/ontology/watchdog#> .
@prefix wdc:   <https://example.com/ontology/watchdog/class/> .
@prefix wdp:   <https://example.com/ontology/watchdog/property/> .
"""

ONTOLOGY_HEADER = """\
# ============================================================================
# Watchdog Resource Ontology
# Generated: {timestamp}
#
# This ontology defines the resource classification hierarchy used by the
# Data Platform Watchdog governance engine. Import into Ontos as a knowledge
# collection for visualization, semantic linking, and compliance dashboards.
# ============================================================================

wd:WatchdogOntology rdf:type owl:Ontology ;
    rdfs:label "Watchdog Resource Ontology" ;
    rdfs:comment "Tag-based resource classification for Databricks governance" ;
    owl:versionInfo "1.0.0" .

# ── Custom Properties ────────────────────────────────────────────────────

wdp:matchesResourceType rdf:type owl:DatatypeProperty ;
    rdfs:label "matches resource type" ;
    rdfs:comment "Resource types from the crawler that map to this base class" ;
    rdfs:domain owl:Class ;
    rdfs:range xsd:string .

wdp:classifierTag rdf:type owl:DatatypeProperty ;
    rdfs:label "classifier tag" ;
    rdfs:comment "Tag key=value that triggers classification into this class" ;
    rdfs:domain owl:Class ;
    rdfs:range xsd:string .

wdp:complianceDomain rdf:type owl:DatatypeProperty ;
    rdfs:label "compliance domain" ;
    rdfs:comment "Primary governance domain for policies targeting this class" ;
    rdfs:domain owl:Class ;
    rdfs:range xsd:string .
"""


def _escape_turtle(s: str) -> str:
    """Escape a string for Turtle literal."""
    return s.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _class_uri(name: str) -> str:
    """Convert a class name to its Turtle URI."""
    return f"wdc:{name}"


def _classifier_to_annotations(classifier: dict | None) -> list[str]:
    """Extract tag-based classifier info as annotation triples."""
    if not classifier:
        return []

    annotations = []

    def _walk(c: dict, prefix: str = "") -> None:
        if "tag_equals" in c:
            for k, v in c["tag_equals"].items():
                annotations.append(f'{prefix}{k}={v}')
        if "tag_in" in c:
            for k, vals in c["tag_in"].items():
                annotations.append(f'{prefix}{k} IN [{",".join(str(v) for v in vals)}]')
        if "tag_exists" in c:
            for k in c["tag_exists"]:
                annotations.append(f'{prefix}{k} EXISTS')
        if "all_of" in c:
            for sub in c["all_of"]:
                _walk(sub, prefix)
        if "any_of" in c:
            for sub in c["any_of"]:
                _walk(sub, prefix + "ANY:")
        if "none_of" in c:
            for sub in c["none_of"]:
                _walk(sub, prefix + "NOT:")

    _walk(classifier)
    return annotations


def export_turtle(ontology: OntologyEngine | None = None,
                  output_path: str | None = None) -> str:
    """Export the Watchdog resource ontology as OWL/Turtle.

    Args:
        ontology: OntologyEngine instance (uses default if None)
        output_path: If provided, writes to file. Otherwise returns string.

    Returns:
        The Turtle content as a string.
    """
    if ontology is None:
        ontology = OntologyEngine()

    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    lines = [PREFIXES, ONTOLOGY_HEADER.format(timestamp=now)]

    # Domain mapping for classes (based on primary governance concern)
    domain_hints = {
        "PiiAsset": "SecurityGovernance",
        "ConfidentialAsset": "SecurityGovernance",
        "GoldTable": "DataQuality",
        "SilverTable": "DataQuality",
        "BronzeTable": "DataQuality",
        "ProductionJob": "OperationalGovernance",
        "CriticalJob": "OperationalGovernance",
        "UnattributedAsset": "CostGovernance",
        "SharedCompute": "CostGovernance",
        "InteractiveCluster": "CostGovernance",
    }

    # Export each class
    for cls in ontology.classes.values():
        uri = _class_uri(cls.name)
        desc = _escape_turtle(cls.description) if cls.description else cls.name

        lines.append(f"\n# ── {cls.name} ──")
        lines.append(f"{uri} rdf:type owl:Class ;")
        lines.append(f'    rdfs:label "{cls.name}" ;')
        lines.append(f'    skos:definition "{desc}" ;')

        # Parent relationship
        if cls.parent:
            lines.append(f"    rdfs:subClassOf {_class_uri(cls.parent)} ;")
        else:
            # Root class
            lines.append("    rdfs:subClassOf owl:Thing ;")

        # Base class resource type mapping
        if cls.is_base_class():
            for rt in cls.matches_resource_types:
                lines.append(f'    wdp:matchesResourceType "{rt}" ;')

        # Classifier tag annotations
        tag_annots = _classifier_to_annotations(cls.classifier)
        for annot in tag_annots:
            lines.append(f'    wdp:classifierTag "{_escape_turtle(annot)}" ;')

        # Domain hint
        domain = domain_hints.get(cls.name)
        if domain:
            lines.append(f'    wdp:complianceDomain "{domain}" ;')

        # Close the class definition (replace last ; with .)
        last_line = lines[-1]
        if last_line.endswith(" ;"):
            lines[-1] = last_line[:-2] + " ."
        else:
            lines.append("    .")

    content = "\n".join(lines) + "\n"

    if output_path:
        Path(output_path).parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            f.write(content)

    return content


def export_for_ontos(ontology: OntologyEngine | None = None,
                     output_dir: str | None = None) -> dict:
    """Export ontology in Ontos-compatible format.

    Generates:
      1. watchdog-ontology.ttl — OWL/Turtle for Ontos semantic models import
      2. watchdog-registry-entry.yml — Entry for Ontos industry_ontologies.yaml

    Args:
        ontology: OntologyEngine instance
        output_dir: Directory to write files to

    Returns:
        Dict with file paths and metadata.
    """
    if ontology is None:
        ontology = OntologyEngine()

    if output_dir is None:
        output_dir = str(Path(__file__).parent.parent.parent / "ontologies" / "export")

    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)

    # 1. Export Turtle
    ttl_path = str(out / "watchdog-ontology.ttl")
    export_turtle(ontology, ttl_path)

    # 2. Generate Ontos registry entry
    registry_entry = f"""\
# Add this entry to Ontos' src/backend/src/data/industry_ontologies.yaml
# under the appropriate vertical (e.g., "Data Governance" or custom)

- id: watchdog-governance
  name: "Watchdog Governance Ontology"
  full_name: "Data Platform Watchdog — Resource Classification Ontology"
  description: "Tag-based resource classification hierarchy for Databricks governance. Covers data sensitivity, cost attribution, operational maturity, and data classification."
  handler: simple_owl
  license: "Internal"
  website: ""
  modules:
    - id: watchdog-core
      name: "Watchdog Core Classes"
      description: "Resource classification hierarchy: DataAsset, ComputeAsset, IdentityAsset and {len(ontology.classes)} derived classes"
      owl_url: "file://watchdog-ontology.ttl"
      maturity: release
"""

    registry_path = str(out / "watchdog-registry-entry.yml")
    with open(registry_path, "w") as f:
        f.write(registry_entry)

    return {
        "turtle_path": ttl_path,
        "registry_path": registry_path,
        "class_count": len(ontology.classes),
    }
