"""Artifact deployer — push compiled artifacts to the workspace.

Reads artifacts from the compile manifest and dispatches each to a
target-specific deployer. Collects results (success/failure) for all
artifacts without stopping on first error.

Supported targets:
  - uc_tag_policy: UC tag policy API (create-or-update)
  - uc_abac: ALTER TABLE SET COLUMN MASK via statement execution
  - guardrails: skipped (MCP server reads from disk)
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DeployResult:
    """Outcome of deploying a single artifact."""
    artifact_id: str
    target: str
    success: bool
    error: str | None = None
    deployed_at: str | None = None
    details: str = ""


def deploy_artifacts(
    artifacts: list[dict],
    w: Any,
    spark: Any,
    catalog: str,
    schema: str,
    dry_run: bool = False,
) -> list[DeployResult]:
    """Deploy all artifacts, collecting results.

    Args:
        artifacts: List of dicts with artifact_id, target, content keys.
        w: WorkspaceClient instance.
        spark: SparkSession (needed for uc_abac resource_classifications query).
        catalog: UC catalog name.
        schema: UC schema name.
        dry_run: If True, resolve targets but skip execution.
    """
    results: list[DeployResult] = []

    for artifact in artifacts:
        target = artifact.get("target", "")
        artifact_id = artifact.get("artifact_id", "")

        try:
            if target == "guardrails":
                results.append(DeployResult(
                    artifact_id=artifact_id,
                    target=target,
                    success=True,
                    details="Skipped — guardrails artifacts deployed via disk (MCP server reads at startup).",
                ))
            elif target == "uc_tag_policy":
                results.append(_deploy_uc_tag_policy(w, artifact, dry_run=dry_run))
            elif target == "uc_abac":
                results.append(_deploy_uc_abac(
                    w, artifact, spark, catalog, schema, dry_run=dry_run,
                ))
            else:
                results.append(DeployResult(
                    artifact_id=artifact_id,
                    target=target,
                    success=False,
                    error=f"Unknown target '{target}' — no deployer registered.",
                ))
        except Exception as e:
            logger.exception(f"Deploy failed for {artifact_id}")
            results.append(DeployResult(
                artifact_id=artifact_id,
                target=target,
                success=False,
                error=str(e),
            ))

    return results


def _deploy_uc_tag_policy(w: Any, artifact: dict, dry_run: bool = False) -> DeployResult:
    """Deploy a UC tag policy via the tag policy API."""
    content = json.loads(artifact.get("content", "{}"))
    artifact_id = artifact["artifact_id"]
    tag_key = content.get("tag_key", "")
    policy_type = content.get("policy_type", "required")
    allowed_values = content.get("allowed_values")
    scope = content.get("scope")

    action = f"Create/update tag policy: tag_key={tag_key}, type={policy_type}"
    if allowed_values:
        action += f", allowed_values={allowed_values}"
    if scope:
        action += f", scope={scope}"

    if dry_run:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_tag_policy",
            success=True,
            details=f"(dry-run) {action}",
        )

    try:
        body: dict[str, Any] = {
            "name": tag_key,
            "policy_type": policy_type.upper(),
        }
        if allowed_values:
            body["allowed_values"] = allowed_values
        if scope:
            body["catalog"] = scope.get("catalog")
            body["schema"] = scope.get("schema")

        w.api_client.do("POST", "/api/2.0/unity-catalog/tag-policies", body=body)

        return DeployResult(
            artifact_id=artifact_id,
            target="uc_tag_policy",
            success=True,
            deployed_at=datetime.now(timezone.utc).isoformat(),
            details=action,
        )
    except Exception as e:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_tag_policy",
            success=False,
            error=str(e),
            details=action,
        )


def _deploy_uc_abac(
    w: Any, artifact: dict, spark: Any, catalog: str, schema: str,
    dry_run: bool = False,
) -> DeployResult:
    """Deploy a UC ABAC column mask via ALTER TABLE SET COLUMN MASK."""
    content = json.loads(artifact.get("content", "{}"))
    artifact_id = artifact["artifact_id"]
    mask_function = content.get("mask_function", "")
    applies_to = content.get("applies_to", "")

    try:
        classifications_table = f"{catalog}.{schema}.resource_classifications"
        rows = spark.sql(f"""
            SELECT DISTINCT resource_id
            FROM {classifications_table}
            WHERE class_name = '{applies_to}'
        """).collect()
        matched_tables = [r.resource_id for r in rows]
    except Exception as e:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=False,
            error=f"Failed to resolve applies_to={applies_to}: {e}",
        )

    if not matched_tables:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=True,
            details=f"No tables matched class '{applies_to}'. Nothing to deploy.",
        )

    action = (
        f"Apply column mask {mask_function} to {len(matched_tables)} table(s) "
        f"matching '{applies_to}'"
    )

    if dry_run:
        table_list = ", ".join(matched_tables[:5])
        if len(matched_tables) > 5:
            table_list += f" (+{len(matched_tables) - 5} more)"
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=True,
            details=f"(dry-run) {action}. Tables: {table_list}",
        )

    errors: list[str] = []
    applied = 0
    for table_name in matched_tables:
        try:
            info = w.tables.get(full_name=table_name)
            for col in info.columns or []:
                stmt = f"ALTER TABLE {table_name} ALTER COLUMN `{col.name}` SET MASK {mask_function}"
                spark.sql(stmt)
                applied += 1
        except Exception as e:
            errors.append(f"{table_name}: {e}")

    if errors:
        return DeployResult(
            artifact_id=artifact_id,
            target="uc_abac",
            success=False,
            deployed_at=datetime.now(timezone.utc).isoformat(),
            error=f"{len(errors)} table(s) failed: {'; '.join(errors[:3])}",
            details=f"{action}. Applied to {applied} column(s), {len(errors)} error(s).",
        )

    return DeployResult(
        artifact_id=artifact_id,
        target="uc_abac",
        success=True,
        deployed_at=datetime.now(timezone.utc).isoformat(),
        details=f"{action}. Applied to {applied} column(s).",
    )
