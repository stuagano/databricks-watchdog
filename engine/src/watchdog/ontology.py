"""Ontology Engine — tag-based resource classification with inheritance.

Loads the resource class hierarchy from ontologies/resource_classes.yml and
classifies crawled resources into one or more classes based on their tags.

Classes form an inheritance tree: a PiiTable inherits all policies that apply
to Table, which inherits from DataAsset. When a resource is classified as
PiiTable, it automatically picks up policies for PiiTable + DataAsset.

Classification is purely tag-driven — no heuristics, no schema name sniffing.
If a resource isn't classified correctly, fix its tags.
"""

import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import yaml


@dataclass
class ResourceClass:
    """A node in the resource class hierarchy."""
    name: str
    parent: Optional[str]
    description: str
    classifier: Optional[dict] = None
    matches_resource_types: list[str] = field(default_factory=list)

    def is_base_class(self) -> bool:
        return len(self.matches_resource_types) > 0


@dataclass
class ClassificationResult:
    """All classes a resource belongs to, ordered from most specific to base."""
    resource_id: str
    classes: list[str]


class OntologyEngine:
    """Loads the class hierarchy and classifies resources."""

    def __init__(self, ontology_dir: str | None = None):
        if ontology_dir is None:
            ontology_dir = str(Path(__file__).parent.parent.parent / "ontologies")
        self.ontology_dir = Path(ontology_dir)
        self.classes: dict[str, ResourceClass] = {}
        self._load()

    def _load(self) -> None:
        """Load resource classes from YAML."""
        rc_path = self.ontology_dir / "resource_classes.yml"
        if not rc_path.exists():
            return

        with open(rc_path) as f:
            data = yaml.safe_load(f)

        # Load base classes
        for name, defn in (data.get("base_classes") or {}).items():
            self.classes[name] = ResourceClass(
                name=name,
                parent=None,
                description=defn.get("description", ""),
                matches_resource_types=defn.get("matches_resource_types", []),
            )

        # Load derived classes
        for name, defn in (data.get("derived_classes") or {}).items():
            self.classes[name] = ResourceClass(
                name=name,
                parent=defn.get("parent"),
                description=defn.get("description", ""),
                classifier=defn.get("classifier"),
            )

    def classify(self, resource_type: str, tags: dict[str, str],
                 metadata: dict[str, str]) -> ClassificationResult:
        """Classify a resource into all matching classes.

        Returns classes from most specific to least specific (base).
        A resource always belongs to its base class if the resource_type matches.
        """
        matched = []

        # Check base classes first
        for cls in self.classes.values():
            if cls.is_base_class() and resource_type in cls.matches_resource_types:
                matched.append(cls.name)

        # Check derived classes — a resource can match multiple
        # BUT only if the resource belongs to the derived class's root base class
        base_set = set(matched)
        for cls in self.classes.values():
            if not cls.classifier:
                continue
            # Find root base class and ensure resource matches it
            root = self._get_root_base(cls.name)
            if root and root not in base_set:
                continue
            if self._matches_classifier(
                cls.classifier, tags, metadata, resource_type
            ):
                matched.append(cls.name)

        # Deduplicate while preserving order
        seen = set()
        unique = []
        for c in matched:
            if c not in seen:
                seen.add(c)
                unique.append(c)

        return ClassificationResult(
            resource_id="",
            classes=unique,
        )

    def get_ancestor_chain(self, class_name: str) -> list[str]:
        """Return the full inheritance chain from class_name up to root.

        Example: PiiTable -> PiiAsset -> ConfidentialAsset -> DataAsset
        """
        chain = []
        current = class_name
        visited = set()
        while current and current not in visited:
            chain.append(current)
            visited.add(current)
            cls = self.classes.get(current)
            if cls:
                current = cls.parent
            else:
                break
        return chain

    def get_all_classes_for_resource(self, resource_type: str,
                                     tags: dict[str, str],
                                     metadata: dict[str, str]) -> set[str]:
        """Get ALL classes a resource belongs to, including ancestors.

        This is what the policy engine uses: a PiiTable resource will return
        {PiiTable, PiiAsset, ConfidentialAsset, InternalAsset, DataAsset}
        so policies targeting any of those classes will apply.
        """
        result = self.classify(resource_type, tags, metadata)
        all_classes = set()
        for cls_name in result.classes:
            for ancestor in self.get_ancestor_chain(cls_name):
                all_classes.add(ancestor)
        return all_classes

    def _get_root_base(self, class_name: str) -> str | None:
        """Walk up the parent chain to find the root base class."""
        current = class_name
        visited = set()
        while current and current not in visited:
            visited.add(current)
            cls = self.classes.get(current)
            if not cls:
                return None
            if cls.is_base_class():
                return cls.name
            current = cls.parent
        return None

    # ------------------------------------------------------------------
    # Classifier evaluation
    # ------------------------------------------------------------------

    def _matches_classifier(self, classifier: dict, tags: dict[str, str],
                            metadata: dict[str, str],
                            resource_type: str) -> bool:
        """Evaluate a classifier definition against resource tags/metadata."""

        # tag_equals: { key: value }
        if "tag_equals" in classifier:
            for key, value in classifier["tag_equals"].items():
                if tags.get(key) != str(value):
                    return False
            return True

        # tag_in: { key: [v1, v2] }
        if "tag_in" in classifier:
            for key, allowed in classifier["tag_in"].items():
                if tags.get(key) not in [str(v) for v in allowed]:
                    return False
            return True

        # tag_exists: [key1, key2]
        if "tag_exists" in classifier:
            keys = classifier["tag_exists"]
            return all(k in tags for k in keys)

        # tag_matches: { key: "regex" }
        if "tag_matches" in classifier:
            for key, pattern in classifier["tag_matches"].items():
                val = tags.get(key, "")
                if not re.search(pattern, val):
                    return False
            return True

        # metadata_equals: { key: value }
        if "metadata_equals" in classifier:
            for key, value in classifier["metadata_equals"].items():
                # Special case: resource_type is a top-level field, not in metadata
                if key == "resource_type":
                    if resource_type != str(value):
                        return False
                elif metadata.get(key) != str(value):
                    return False
            return True

        # metadata_matches: { key: "regex" }
        if "metadata_matches" in classifier:
            for key, pattern in classifier["metadata_matches"].items():
                val = metadata.get(key, "")
                if not re.search(pattern, val):
                    return False
            return True

        # all_of: [ ...classifiers ]
        if "all_of" in classifier:
            return all(
                self._matches_classifier(sub, tags, metadata, resource_type)
                for sub in classifier["all_of"]
            )

        # any_of: [ ...classifiers ]
        if "any_of" in classifier:
            return any(
                self._matches_classifier(sub, tags, metadata, resource_type)
                for sub in classifier["any_of"]
            )

        # none_of: [ ...classifiers ]
        if "none_of" in classifier:
            return not any(
                self._matches_classifier(sub, tags, metadata, resource_type)
                for sub in classifier["none_of"]
            )

        return False

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    def list_classes(self) -> list[dict]:
        """Return all classes with their hierarchy info."""
        result = []
        for cls in self.classes.values():
            result.append({
                "name": cls.name,
                "parent": cls.parent,
                "description": cls.description,
                "is_base": cls.is_base_class(),
                "ancestors": self.get_ancestor_chain(cls.name),
            })
        return result

    def get_children(self, class_name: str) -> list[str]:
        """Get all direct children of a class."""
        return [
            cls.name for cls in self.classes.values()
            if cls.parent == class_name
        ]

    def get_descendants(self, class_name: str) -> set[str]:
        """Get all descendants (children, grandchildren, etc.) of a class."""
        descendants = set()
        queue = self.get_children(class_name)
        while queue:
            child = queue.pop(0)
            if child not in descendants:
                descendants.add(child)
                queue.extend(self.get_children(child))
        return descendants
