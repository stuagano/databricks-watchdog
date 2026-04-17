"""ClusterTaggerAgent — proposes missing cost-attribution tags on compute.

Deterministic agent. Handles the three common compute tag policies:
  POL-C002: cost_center on compute
  POL-C003: business_unit on any resource
  POL-C004: environment on compute

The agent inspects the violation's remediation text for a specific tag key
(e.g. 'cost_center') and suggests a value derived from the resource's existing
tags or owner domain. When no signal is available it emits a 'UNASSIGNED'
placeholder at low confidence so a reviewer is forced to intervene.
"""

import json
import re

_TAG_BY_POLICY: dict[str, str] = {
    "POL-C002": "cost_center",
    "POL-C003": "business_unit",
    "POL-C004": "environment",
}


def _infer_environment(resource_name: str) -> tuple[str, float]:
    """Guess environment from naming conventions in the resource name.

    Returns (value, confidence). Confidence is high when the signal is
    unambiguous (explicit suffix/prefix), medium for a substring match,
    and low when nothing matches — the agent emits 'dev' as a safe default.
    """
    lowered = resource_name.lower()
    for env in ("prod", "production", "staging", "stage", "test", "qa", "dev"):
        if re.search(rf"(^|[_\-/\.]){env}($|[_\-/\.])", lowered):
            canonical = {
                "production": "prod", "stage": "staging", "qa": "test",
            }.get(env, env)
            return canonical, 0.8
    return "dev", 0.3


def _infer_business_unit(owner: str) -> tuple[str, float]:
    """Derive a business unit from the owner's email domain subpart."""
    if owner and "@" in owner:
        local = owner.split("@", 1)[0]
        # Heuristic: email local parts like "alice.data-platform" → "data-platform"
        if "." in local:
            return local.rsplit(".", 1)[-1], 0.6
    return "UNASSIGNED", 0.2


class ClusterTaggerAgent:
    """Deterministic tag-filling agent for compute resources."""

    agent_id: str = "cluster-tagger-agent"
    handles: list[str] = ["POL-C002", "POL-C003", "POL-C004"]
    version: str = "1.0.0"
    model: str = ""

    def gather_context(self, violation: dict) -> dict:
        return {
            "violation": violation,
            "policy_id": violation.get("policy_id", ""),
            "resource_name": violation.get("resource_name", "unknown"),
            "resource_type": violation.get("resource_type", "cluster"),
            "owner": violation.get("owner", ""),
        }

    def propose_fix(self, context: dict) -> dict:
        policy_id = context.get("policy_id", "")
        tag_key = _TAG_BY_POLICY.get(policy_id)
        resource = context.get("resource_name", "unknown")
        resource_type = context.get("resource_type", "cluster")
        owner = context.get("owner", "")

        if tag_key is None:
            return {
                "proposed_sql": "",
                "confidence": 0.0,
                "context_json": json.dumps(context),
                "citations": "",
            }

        if tag_key == "environment":
            value, confidence = _infer_environment(resource)
        elif tag_key == "business_unit":
            value, confidence = _infer_business_unit(owner)
        else:  # cost_center — cannot be inferred without a directory lookup
            value, confidence = "UNASSIGNED", 0.2

        # Compute SQL differs between SQL-addressable resources and API-managed
        # clusters. We emit UC-style SET TAGS by default; clusters/jobs require
        # an API call performed by the applier (captured in context).
        if resource_type == "table":
            sql = f"ALTER TABLE {resource} SET TAGS ('{tag_key}' = '{value}')"
        elif resource_type == "warehouse":
            sql = (
                f"-- Warehouse tag update must be applied via Databricks API\n"
                f"-- warehouses.edit({resource}, tags={{'{tag_key}': '{value}'}})"
            )
        else:
            sql = (
                f"-- {resource_type.capitalize()} tag update must be applied via API\n"
                f"-- {resource_type}s.update({resource}, tags={{'{tag_key}': '{value}'}})"
            )

        return {
            "proposed_sql": sql,
            "confidence": confidence,
            "context_json": json.dumps(context),
            "citations": "",
        }
