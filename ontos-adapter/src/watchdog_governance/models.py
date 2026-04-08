"""Pydantic models — the data contract between routers and providers.

These models define the shape of data flowing through the governance API.
Routers return these types; providers produce them. Neither side depends
on the other's implementation.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, Field


# ── Violations ────────────────────────────────────────────────────────────────


class ViolationSummary(BaseModel):
    total: int
    active: int
    critical: int
    high: int
    medium: int
    low: int


class Violation(BaseModel):
    violation_id: str
    resource_id: str
    resource_name: str
    resource_type: str
    policy_id: str
    policy_name: str
    severity: Literal["critical", "high", "medium", "low"]
    domain: str
    first_seen: str
    last_seen: str
    active: bool
    scan_id: str


class ViolationFilters(BaseModel):
    active: bool = True
    severity: str | None = None
    policy_id: str | None = None
    resource_id: str | None = None
    domain: str | None = None
    limit: int = 200
    offset: int = 0


# ── Scans ─────────────────────────────────────────────────────────────────────


class ScanRun(BaseModel):
    scan_id: str
    started_at: str
    finished_at: str
    resources_scanned: int
    evaluations: int
    failures: int


class PolicyBreakdown(BaseModel):
    policy_id: str
    domain: str
    severity: str
    evaluations: int
    failures: int


class ScanDetail(ScanRun):
    policy_breakdown: list[PolicyBreakdown]


# ── Resources ─────────────────────────────────────────────────────────────────


class Resource(BaseModel):
    resource_id: str
    resource_name: str
    resource_type: str
    first_seen: str
    last_seen: str
    scan_id: str
    metadata: Any = None


class Classification(BaseModel):
    class_name: str
    class_ancestors: str | None = None
    root_class: str | None = None
    classified_at: str | None = None


class ResourceViolation(BaseModel):
    violation_id: str
    policy_id: str
    policy_name: str
    severity: str
    domain: str
    first_seen: str
    last_seen: str
    active: bool


class ResourceException(BaseModel):
    exception_id: str
    policy_id: str
    approved_by: str
    justification: str
    approved_at: str
    expires_at: str | None
    active: bool
    expiry_status: str


class ResourceDetail(Resource):
    classifications: list[Classification]
    violations: list[ResourceViolation]
    exceptions: list[ResourceException]


class ResourceFilters(BaseModel):
    resource_type: str | None = None
    scan_id: str | None = None
    limit: int = 200
    offset: int = 0


# ── Policies ──────────────────────────────────────────────────────────────────


class PolicyBase(BaseModel):
    policy_name: str
    applies_to: str = "*"
    domain: str = "User"
    severity: Literal["critical", "high", "medium", "low"] = "medium"
    description: str = ""
    remediation: str = ""
    rule_json: str = Field(..., description="JSON-serialised rule tree")
    active: bool = True


class PolicyCreate(PolicyBase):
    policy_id: str | None = None


class Policy(PolicyBase):
    policy_id: str
    origin: str
    updated_at: str | None = None


class PolicyVersion(BaseModel):
    version: int
    policy_name: str
    applies_to: str
    severity: str
    active: bool
    rule_json: str
    change_type: str
    changed_by: str
    changed_at: str


class PolicyFilters(BaseModel):
    origin: str | None = None
    active: bool | None = None


# ── Exceptions ────────────────────────────────────────────────────────────────


class ExceptionRequest(BaseModel):
    resource_id: str
    policy_ids: list[str] = Field(..., min_length=1)
    justification: str = Field(..., min_length=10)
    expires_days: int | None = Field(
        default=90,
        description="Days until expiry. Null = permanent.",
        ge=1,
        le=730,
    )


class ExceptionRecord(BaseModel):
    exception_id: str
    resource_id: str
    policy_id: str
    approved_by: str
    justification: str
    approved_at: str
    expires_at: str | None
    active: bool
    expiry_status: str


class ExceptionSummary(BaseModel):
    total: int
    active: int
    permanent: int
    expired: int
    expiring_soon: int


class ExceptionFilters(BaseModel):
    active: bool = True
    expiring_soon: bool = False
    resource_id: str | None = None


# ── Ontology ──────────────────────────────────────────────────────────────────


class OntologyClass(BaseModel):
    name: str
    kind: str  # "base" | "derived"
    parent: str | None
    description: str
    matches_resource_types: list[str]
    classifier: Any = None
    ancestry: list[str]
    children: list[str]


class OntologyTreeNode(BaseModel):
    name: str
    kind: str
    description: str
    children: list[OntologyTreeNode]


OntologyTreeNode.model_rebuild()


class OntologyTree(BaseModel):
    roots: list[OntologyTreeNode]
    total_classes: int


class ValidationResult(BaseModel):
    valid: bool
    errors: list[str]
    warnings: list[str]


# ── Grants ───────────────────────────────────────────────────────────────────


class Grant(BaseModel):
    resource_id: str
    securable_type: str
    securable_full_name: str
    grantee: str
    privilege: str
    grantor: str
    inherited_from: str


class GrantSummary(BaseModel):
    resource_id: str
    total_grants: int
    grants_by_privilege: dict[str, int]
    overprivileged_count: int
    direct_user_grant_count: int


class GrantFilters(BaseModel):
    resource_id: str | None = None
    grantee: str | None = None
    privilege: str | None = None
    securable_type: str | None = None
