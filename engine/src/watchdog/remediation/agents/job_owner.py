"""JobOwnerAgent — proposes an owner for jobs flagged by POL-C001.

Heuristic: the job's creator (recorded as created_by on the Databricks Job)
is the most plausible owner. That metadata is passed through in the violation
as `owner_hint` when the crawler can capture it; otherwise the agent emits a
low-confidence proposal targeting a shared 'unassigned' principal so a human
reviewer must approve.
"""

import json


class JobOwnerAgent:
    """Deterministic owner-assignment for jobs missing an owner."""

    agent_id: str = "job-owner-agent"
    handles: list[str] = ["POL-C001"]
    version: str = "1.0.0"
    model: str = ""

    def gather_context(self, violation: dict) -> dict:
        return {
            "violation": violation,
            "resource_name": violation.get("resource_name", "unknown"),
            "resource_id": violation.get("resource_id", ""),
            "resource_type": violation.get("resource_type", "job"),
            "owner_hint": violation.get("owner_hint", ""),
        }

    def propose_fix(self, context: dict) -> dict:
        resource = context.get("resource_name", "unknown")
        resource_id = context.get("resource_id", "")
        hint = context.get("owner_hint", "").strip()
        resource_type = context.get("resource_type", "job")

        if hint:
            owner = hint
            confidence = 0.85
        else:
            owner = "platform-admin@company.com"
            confidence = 0.3

        # Jobs are managed through the Jobs API — SQL is a comment recording
        # the intended change. The applier will translate this into an API call.
        sql = (
            f"-- {resource_type.capitalize()} owner update must be applied via API\n"
            f"-- jobs.update(job_id={resource_id!r}, "
            f"new_settings={{'tags': {{'owner': '{owner}'}}}})"
        )

        return {
            "proposed_sql": sql,
            "confidence": confidence,
            "context_json": json.dumps(context),
            "citations": f"resource:{resource}",
        }
