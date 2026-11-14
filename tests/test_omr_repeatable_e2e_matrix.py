"""Repeatable end-to-end verification matrix for oh-my-raven (R1-R23, 11 criteria, 6 adversarial traps, 7x5 SxH matrix)."""

from __future__ import annotations

import ast
import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest

import inspect

from oh_my_raven.routing import (
    RouteTarget,
    RoutingBackend,
    SubagentFailoverBlockedError,
    SubagentFailoverExhaustedError,
)
from raven.agent.subagent.dag_graph import DagNodeSpec, SubAgentDagSpec
from oh_my_raven.oh_my_raven import (
    AcceptanceReceipt,
    BRAND_NAME,
    CommandReceipt,
    CtoStrategicBrief,
    DagSpecialistRuntime,
    DynamicDecompositionGate,
    ExecutionIdentity,
    IndependentReviewError,
    OmrDispatcher,
    OmrTaskQueue,
    PrematureCompletionError,
    RequirementItem,
    RequirementTraceabilityGate,
    ReviewGateValidator,
    ReviewReceipt,
    SkillContractViolationError,
    SliceReceipt,
    WorktreeManager,
    build_context_charter,
)
from raven.agent.subagent.oh_my_raven import HarnessStartupManager
from raven.agent.subagent.prompt_errors import DagValidationError
from raven.agent.subagent.registry import SubagentTable, _narrow_build
from raven.config.schema import (
    BuiltinAgentConfig,
    SpecialistSubagentConfig,
    ThirdPartyAcpSubagentConfig,
    ThirdPartyCliSubagentConfig,
)


OMR_COMPLEX_001_PROMPT = (
    "Task ID OMR-COMPLEX-001: Build the Multi-Tenant Zero-Trust Quota & Cost Ledger feature "
    "(REQ-AUTH-1, REQ-QUOTA-2, REQ-GW-3, REQ-AUDIT-4): "
    "(1) align with active workstreams and long-term EverOS goals via CTO strategic review; "
    "(2) research rate-limit token-bucket consensus bounds; "
    "(3) implement TenantTokenValidator.validate_tenant in src/auth/token_validator.py, "
    "SlidingWindowLimiter.check_quota in src/quota/rate_limiter.py, and wire "
    "GatewayRouter.dispatch_request + GatewayRouter.emit_audit_event in src/api/gateway.py "
    "with unit tests in tests/test_gateway.py; "
    "(4) integrate parallel slices and pass independent Reviewer & RequirementTraceabilityGate verification."
)

EXPECTED_REQUIREMENT_IDS = ["REQ-AUTH-1", "REQ-QUOTA-2", "REQ-GW-3", "REQ-AUDIT-4"]

SEVEN_SPECIALISTS: tuple[dict[str, Any], ...] = (
    {
        "name": "CTO",
        "description": "Chief Technology Officer: cross-workstream strategic framing and 5-D consequence matrix",
        "role_prompt": "You are CTO. Enforce cross-workstream synergies, EverOS goal alignment, and 5-D consequence matrices.",
        "skill_packs": ["office-hours", "plan-ceo-review", "plan-eng-review", "autoplan"],
        "mcp_servers": ["trail", "codebase-memory"],
        "required_capabilities": ["local-files"],
    },
    {
        "name": "Manager",
        "description": "Lead Specialist & Integrator: dynamic DAG decomposition and deterministic slice integration",
        "role_prompt": "You are Manager. Decompose complex multi-domain tasks into non-overlapping DAG slices and integrate them.",
        "skill_packs": ["agent-skills", "pstack"],
        "mcp_servers": ["trail"],
        "required_capabilities": ["local-files"],
    },
    {
        "name": "Researcher",
        "description": "Specification & consensus researcher using MCP servers",
        "role_prompt": "You are Researcher. Gather consensus bounds and produce technical specifications.",
        "skill_packs": ["agent-skills"],
        "mcp_servers": ["consensus"],
        "required_capabilities": ["mcp"],
    },
    {
        "name": "Engineer - Matt Pocock",
        "description": "Type-driven boundary & schema specialist",
        "role_prompt": "You are Engineer - Matt Pocock. Make illegal states unrepresentable at module boundaries.",
        "skill_packs": ["mattpocock"],
        "mcp_servers": ["trail"],
        "required_capabilities": ["local-files"],
    },
    {
        "name": "Engineer - Superpowers",
        "description": "TDD red-green-refactor implementation specialist",
        "role_prompt": "You are Engineer - Superpowers. Write failing tests first and verify exit codes before completion.",
        "skill_packs": ["superpowers"],
        "mcp_servers": ["trail"],
        "required_capabilities": ["local-files"],
    },
    {
        "name": "Engineer - ECC",
        "description": "Everything-Claude-Code modularity & error-handling specialist",
        "role_prompt": "You are Engineer - ECC. Enforce strict modularity, explicit error handling, and clean git hygiene.",
        "skill_packs": ["ecc"],
        "mcp_servers": ["trail"],
        "required_capabilities": ["local-files"],
    },
    {
        "name": "Reviewer",
        "description": "Independent adversarial code reviewer and RTM AST gate verifier",
        "role_prompt": "You are Reviewer. Run open-code-review (ocr) and pytest, and verify 1:1 AST requirement traceability.",
        "skill_packs": ["open-code-review"],
        "mcp_servers": ["trail"],
        "required_capabilities": ["local-files"],
    },
)

FIVE_HARNESSES: tuple[str, ...] = ("Hermes", "Copilot", "Claude-Code", "Codex", "Raven-General")


def _git(repo: Path, *args: str, check: bool = True) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=check,
        text=True,
        capture_output=True,
        timeout=30,
    )
    return proc.stdout.strip()


def _create_seeded_repo(root: Path) -> tuple[Path, str]:
    repo = root / "omr_dummy_repo"
    repo.mkdir(parents=True, exist_ok=True)
    _git(repo, "init")
    _git(repo, "config", "user.name", "OMR Test Rig")
    _git(repo, "config", "user.email", "omr@example.invalid")

    (repo / "src" / "auth").mkdir(parents=True, exist_ok=True)
    (repo / "src" / "quota").mkdir(parents=True, exist_ok=True)
    (repo / "src" / "api").mkdir(parents=True, exist_ok=True)
    (repo / "tests").mkdir(parents=True, exist_ok=True)

    (repo / "src" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "auth" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "auth" / "token_validator.py").write_text(
        '"""Auth stub."""\n',
        encoding="utf-8",
    )
    (repo / "src" / "quota" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "quota" / "rate_limiter.py").write_text(
        '"""Quota stub."""\n',
        encoding="utf-8",
    )
    (repo / "src" / "api" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "src" / "api" / "gateway.py").write_text(
        '"""Gateway stub."""\n',
        encoding="utf-8",
    )
    (repo / "tests" / "test_gateway.py").write_text(
        "def test_placeholder() -> None:\n    assert True\n",
        encoding="utf-8",
    )
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "Initial baseline commit")
    base_sha = _git(repo, "rev-parse", "HEAD")
    return repo, base_sha


def _build_9_node_decomposition_payload() -> dict[str, Any]:
    nodes = [
        {
            "id": "cto_strategy",
            "subagent": "CTO",
            "node_summary": "CTO strategic alignment & consequence matrix",
            "prompt_template": "Produce CtoStrategicBrief and 5-D ConsequenceMatrix for OMR-COMPLEX-001",
            "execution_kind": "plan",
            "depends_on": [],
        },
        {
            "id": "research_spec",
            "subagent": "Researcher",
            "node_summary": "Research rate-limit token-bucket consensus bounds",
            "prompt_template": "Research rate-limit token-bucket consensus bounds and Decimal ledger rules",
            "execution_kind": "read",
            "depends_on": ["cto_strategy"],
        },
        {
            "id": "slice_auth",
            "subagent": "Engineer - Matt Pocock",
            "node_summary": "Implement TenantTokenValidator in src/auth/token_validator.py",
            "prompt_template": "Implement TenantContext and TenantTokenValidator.validate_tenant in src/auth/token_validator.py",
            "execution_kind": "write",
            "file_scope": ["src/auth/**"],
            "depends_on": ["cto_strategy", "research_spec"],
        },
        {
            "id": "slice_quota",
            "subagent": "Engineer - Superpowers",
            "node_summary": "Implement SlidingWindowLimiter in src/quota/rate_limiter.py",
            "prompt_template": "Implement SlidingWindowLimiter.check_quota in src/quota/rate_limiter.py using Decimal ROUND_HALF_EVEN",
            "execution_kind": "write",
            "file_scope": ["src/quota/**"],
            "depends_on": ["cto_strategy", "research_spec"],
        },
        {
            "id": "slice_gateway",
            "subagent": "Engineer - ECC",
            "node_summary": "Implement GatewayRouter in src/api/gateway.py and tests",
            "prompt_template": "Implement GatewayRouter.dispatch_request in src/api/gateway.py and tests in tests/test_gateway.py",
            "execution_kind": "write",
            "file_scope": ["src/api/**", "tests/**"],
            "depends_on": ["cto_strategy", "research_spec"],
        },
        {
            "id": "integrate",
            "subagent": "Manager",
            "node_summary": "Integrate parallel auth, quota, and gateway slices",
            "prompt_template": "Integrate slice_auth, slice_quota, and slice_gateway into candidate branch",
            "execution_kind": "integrate",
            "depends_on": ["slice_auth", "slice_quota", "slice_gateway"],
        },
        {
            "id": "review",
            "subagent": "Reviewer",
            "node_summary": "Independent code review pass 1",
            "prompt_template": "Review integrated candidate head and verify REQ-AUTH-1..REQ-AUDIT-4",
            "execution_kind": "review",
            "depends_on": ["integrate"],
            "routes": {
                "approved": "All requirements and AST symbols verified",
                "changes_requested": "Defects or missing symbols found",
            },
        },
        {
            "id": "remediate",
            "subagent": "Engineer - Superpowers",
            "node_summary": "Remediate Reviewer findings",
            "prompt_template": "Remediate findings from review pass 1 in src/api/gateway.py and tests/test_gateway.py",
            "execution_kind": "write",
            "file_scope": ["src/**", "tests/**"],
            "depends_on": ["review"],
            "when": "review.changes_requested",
        },
        {
            "id": "re_review",
            "subagent": "Reviewer",
            "node_summary": "Independent code review pass 2",
            "prompt_template": "Re-review remediated candidate head and verify full RTM AST symbols",
            "execution_kind": "review",
            "depends_on": ["remediate", "review"],
            "when": "review.changes_requested",
        },
    ]
    return {
        "task_summary": "OMR-COMPLEX-001: Multi-Tenant Zero-Trust Quota & Cost Ledger",
        "expected_requirement_ids": list(EXPECTED_REQUIREMENT_IDS),
        "nodes": nodes,
    }


def _valid_cto_brief_dict(*, include_arch_dec_09: bool = True) -> dict[str, Any]:
    synergies = [
        "W-42: Share canonical TenantContext across token_validator and rate_limiter",
        "G-7: Enforce zero-trust tenant isolation and 1:1 AST requirement traceability",
    ]
    if include_arch_dec_09:
        synergies.append(
            "ARCH-DEC-09: All financial/quota calculations MUST use decimal.Decimal with ROUND_HALF_EVEN and include workstream_id; float is forbidden"
        )
    return {
        "cto_strategic_brief": {
            "workstream_synergies": synergies,
            "long_term_goal_alignment": "Deliver zero-trust multi-tenant quota & cost ledger aligned with EverOS Goal G-7 and ARCH-DEC-09.",
            "cross_project_dependencies": [
                "src/auth/token_validator.py",
                "src/quota/rate_limiter.py",
                "src/api/gateway.py",
            ],
            "consequence_matrix": {
                "affected_files": [
                    "src/auth/token_validator.py",
                    "src/quota/rate_limiter.py",
                    "src/api/gateway.py",
                    "tests/test_gateway.py",
                ],
                "blast_radius": "API gateway, auth validator, and quota ledger modules",
                "failure_modes": [
                    "Float precision drift if ARCH-DEC-09 is violated",
                    "Missing audit event emission on gateway dispatch",
                ],
                "rollback_plan": "Discard isolated git worktrees and reset candidate branch to base_sha",
                "contract_changes": [
                    "Add TenantTokenValidator.validate_tenant, SlidingWindowLimiter.check_quota, GatewayRouter.dispatch_request, GatewayRouter.emit_audit_event"
                ],
                "validation_plan": ["pytest tests/test_gateway.py -q"],
            },
        }
    }


AUTH_CODE = '''"""Zero-trust tenant token validator (REQ-AUTH-1)."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class TenantContext:
    tenant_id: str
    workstream_id: str
    scopes: tuple[str, ...] = ("quota:read", "quota:write")


class TenantTokenValidator:
    """Validates tenant bearer tokens and constructs canonical TenantContext."""

    @staticmethod
    def validate_tenant(token: str, *, workstream_id: str = "W-42") -> TenantContext:
        cleaned = (token or "").strip()
        if not cleaned or not cleaned.startswith("tenant:"):
            raise ValueError("invalid tenant token")
        tenant_id = cleaned.split(":", 1)[1].strip()
        if not tenant_id:
            raise ValueError("missing tenant_id")
        return TenantContext(tenant_id=tenant_id, workstream_id=workstream_id)
'''

QUOTA_CODE_DECIMAL = '''"""Sliding-window quota & cost ledger using Decimal ROUND_HALF_EVEN (REQ-QUOTA-2 & ARCH-DEC-09)."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN
from src.auth.token_validator import TenantContext


class SlidingWindowLimiter:
    """Enforces tenant quota and computes exact Decimal ledger costs."""

    def __init__(self, limit: int = 100, unit_cost: str = "0.025") -> None:
        self.limit = int(limit)
        self.unit_cost = Decimal(str(unit_cost))
        self._counts: dict[str, int] = {}

    def check_quota(self, ctx: TenantContext, units: int = 1) -> dict[str, object]:
        if not isinstance(ctx, TenantContext) or not ctx.workstream_id:
            raise ValueError("valid TenantContext with workstream_id required")
        used = self._counts.get(ctx.tenant_id, 0) + int(units)
        allowed = used <= self.limit
        if allowed:
            self._counts[ctx.tenant_id] = used
        raw_cost = self.unit_cost * Decimal(int(units))
        settled_cost = raw_cost.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
        return {
            "tenant_id": ctx.tenant_id,
            "workstream_id": ctx.workstream_id,
            "allowed": allowed,
            "used": used,
            "cost": settled_cost,
        }
'''

QUOTA_CODE_FLOAT_BUG = '''"""Naive float quota implementation violating ARCH-DEC-09."""

from __future__ import annotations


class SlidingWindowLimiter:
    def __init__(self, limit: int = 100, unit_cost: float = 0.025) -> None:
        self.limit = limit
        self.unit_cost = float(unit_cost)

    def check_quota(self, ctx: object, units: int = 1) -> dict[str, object]:
        return {"allowed": True, "cost": float(units) * self.unit_cost}
'''

GATEWAY_CODE_PASS1_INCOMPLETE = '''"""API Gateway router (Pass 1: missing emit_audit_event for REQ-AUDIT-4)."""

from __future__ import annotations

from src.auth.token_validator import TenantTokenValidator
from src.quota.rate_limiter import SlidingWindowLimiter


class GatewayRouter:
    def __init__(self, limiter: SlidingWindowLimiter | None = None) -> None:
        self.limiter = limiter or SlidingWindowLimiter()

    def dispatch_request(self, token: str, *, units: int = 1, workstream_id: str = "W-42") -> dict[str, object]:
        ctx = TenantTokenValidator.validate_tenant(token, workstream_id=workstream_id)
        quota_res = self.limiter.check_quota(ctx, units=units)
        return {"status": "ok" if quota_res["allowed"] else "rate_limited", "quota": quota_res}
'''

GATEWAY_CODE_REMEDIATED = '''"""API Gateway router with full audit ledger emission (REQ-GW-3 & REQ-AUDIT-4)."""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_EVEN
from src.auth.token_validator import TenantContext, TenantTokenValidator
from src.quota.rate_limiter import SlidingWindowLimiter


class GatewayRouter:
    """Routes authenticated tenant requests through quota enforcement and audit logging."""

    def __init__(self, limiter: SlidingWindowLimiter | None = None) -> None:
        self.limiter = limiter or SlidingWindowLimiter()
        self.audit_log: list[dict[str, object]] = []

    def emit_audit_event(self, ctx: TenantContext, quota_result: dict[str, object]) -> dict[str, object]:
        cost_val = quota_result.get("cost", Decimal("0.00"))
        if not isinstance(cost_val, Decimal):
            cost_val = Decimal(str(cost_val)).quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN)
        event = {
            "tenant_id": ctx.tenant_id,
            "workstream_id": ctx.workstream_id,
            "allowed": bool(quota_result.get("allowed", False)),
            "cost": str(cost_val),
        }
        self.audit_log.append(event)
        return event

    def dispatch_request(self, token: str, *, units: int = 1, workstream_id: str = "W-42") -> dict[str, object]:
        ctx = TenantTokenValidator.validate_tenant(token, workstream_id=workstream_id)
        quota_res = self.limiter.check_quota(ctx, units=units)
        audit_event = self.emit_audit_event(ctx, quota_res)
        return {
            "status": "ok" if quota_res["allowed"] else "rate_limited",
            "quota": quota_res,
            "audit": audit_event,
        }
'''

GATEWAY_TESTS_CODE = '''"""Unit & integration tests for the Multi-Tenant Zero-Trust Quota & Cost Ledger."""

from __future__ import annotations

from decimal import Decimal
import pytest

from src.auth.token_validator import TenantTokenValidator
from src.quota.rate_limiter import SlidingWindowLimiter
from src.api.gateway import GatewayRouter


def test_end_to_end_gateway_quota_and_audit_ledger() -> None:
    router = GatewayRouter(SlidingWindowLimiter(limit=5, unit_cost="0.125"))
    res = router.dispatch_request("tenant:acme", units=2, workstream_id="W-42")
    assert res["status"] == "ok"
    assert res["quota"]["cost"] == Decimal("0.25")
    assert res["audit"]["tenant_id"] == "acme"
    assert res["audit"]["workstream_id"] == "W-42"
    assert len(router.audit_log) == 1

    with pytest.raises(ValueError):
        TenantTokenValidator.validate_tenant("bad-token")
'''


def _build_acceptance_receipt_dict() -> dict[str, Any]:
    return {
        "feature_name": BRAND_NAME,
        "requirements": [
            {
                "id": "REQ-AUTH-1",
                "description": "Validate tenant token and produce canonical TenantContext",
                "status": "met",
                "entrypoint_caller": "src/auth/token_validator.py:TenantTokenValidator.validate_tenant",
                "e2e_command": "pytest tests/test_gateway.py -q",
                "evidence_ref": "src/auth/token_validator.py",
            },
            {
                "id": "REQ-QUOTA-2",
                "description": "Enforce sliding-window quota with Decimal ROUND_HALF_EVEN cost calculation",
                "status": "met",
                "entrypoint_caller": "src/quota/rate_limiter.py:SlidingWindowLimiter.check_quota",
                "e2e_command": "pytest tests/test_gateway.py -q",
                "evidence_ref": "src/quota/rate_limiter.py",
            },
            {
                "id": "REQ-GW-3",
                "description": "Dispatch authenticated tenant requests through GatewayRouter",
                "status": "met",
                "entrypoint_caller": "src/api/gateway.py:GatewayRouter.dispatch_request",
                "e2e_command": "pytest tests/test_gateway.py -q",
                "evidence_ref": "src/api/gateway.py",
            },
            {
                "id": "REQ-AUDIT-4",
                "description": "Emit structured zero-trust audit event with workstream_id and Decimal cost",
                "status": "met",
                "entrypoint_caller": "src/api/gateway.py:GatewayRouter.emit_audit_event",
                "e2e_command": "pytest tests/test_gateway.py -q",
                "evidence_ref": "src/api/gateway.py",
            },
        ],
    }


class DeterministicHarnessBackend:
    """Real git-worktree-executing harness backend for deterministic, network-free E2E verification."""

    def __init__(
        self,
        harness_name: str,
        *,
        kind: str = "cli",
        model: str = "model-alpha",
        repo_root: Path,
        worktrees_root: Path,
        tunnel_view_float_trap: bool = False,
        omit_skill_evidence_for: set[str] | None = None,
    ) -> None:
        self.name = harness_name
        self.kind = kind
        self.streams = True
        self.model = model
        self.repo_root = repo_root
        self.worktrees_root = worktrees_root
        self.tunnel_view_float_trap = tunnel_view_float_trap
        self.omit_skill_evidence_for = omit_skill_evidence_for or set()
        self.calls: list[dict[str, Any]] = []
        self.ocr_available = True
        self.execution_identity = ExecutionIdentity(
            agent_name=harness_name,
            harness_kind=kind,
            executable_or_endpoint=f"{harness_name.lower()}-endpoint",
            profile_or_namespace=f"{harness_name.lower()}-ns",
            model=model,
        )

    def _record_skill_receipts(self, task_prompt: str, node_id: str, mcps: list[str], workspace: Path) -> None:
        if node_id in self.omit_skill_evidence_for:
            self.last_skill_evidence = {}
            self.last_invoked_tools = ()
            self.last_invoked_mcps = ()
            return
        matched_packs = [
            pack
            for pack in (
                "office-hours",
                "plan-ceo-review",
                "plan-eng-review",
                "autoplan",
                "agent-skills",
                "pstack",
                "mattpocock",
                "superpowers",
                "ecc",
                "open-code-review",
            )
            if f"### Skill Pack: {pack}" in task_prompt
        ]
        evidence: dict[str, CommandReceipt] = {}
        if matched_packs:
            cmd_str = "git status --porcelain"
            proc = subprocess.run(
                ["git", "-C", str(workspace), "status", "--porcelain"],
                capture_output=True,
                text=True,
                timeout=15,
                check=False,
            )
            out_summary = proc.stdout.strip() or f"{self.name}:{node_id}:clean"
            for pack in matched_packs:
                evidence[pack] = CommandReceipt(
                    command=f"{cmd_str} # skill_pack={pack} node={node_id}",
                    exit_code=int(proc.returncode),
                    output_ref=out_summary[:120],
                )
        self.last_skill_evidence = evidence
        self.last_invoked_tools = ("read_file", "write_file", "exec")
        self.last_invoked_mcps = tuple(mcps)

    async def run(self, task: str, *, task_id: str, **kwargs: Any) -> str:
        out = await self._run_impl(task, task_id=task_id, **kwargs)
        if task_id.endswith(("::cto_intake", "::decompose")):
            return out
        try:
            data = json.loads(out)
        except ValueError:
            return out
        if isinstance(data, dict) and "artifact_path" not in data:
            # Verifiable done-gate evidence: an existing non-empty artifact in the repo.
            data["artifact_path"] = "src/api/gateway.py"
            return json.dumps(data)
        return out

    async def _run_impl(
        self,
        task: str,
        *,
        task_id: str,
        workspace: Path,
        executor: Any = None,
        session_key: str | None = None,
        instance: str | None = None,
        authored_task: str | None = None,
        **kwargs: Any,
    ) -> str:
        ws = Path(workspace)
        mcps = list(kwargs.get("mcps") or [])
        self.calls.append(
            {
                "harness": self.name,
                "task_id": task_id,
                "task": task,
                "authored_task": authored_task,
                "workspace": ws,
                "mcps": mcps,
                "kwargs": dict(kwargs),
            }
        )
        # Reset default identity for this harness before task-specific overrides
        self.execution_identity = ExecutionIdentity(
            agent_name=self.name,
            harness_kind=self.kind,
            executable_or_endpoint=f"{self.name.lower()}-endpoint",
            profile_or_namespace=f"{self.name.lower()}-ns",
            model=self.model,
        )
        self._record_skill_receipts(task, task_id, mcps, ws)

        # 1. Intake CTO or DAG cto_strategy
        if task_id.endswith("::cto_intake") or task_id == "cto_strategy":
            brief = _valid_cto_brief_dict(include_arch_dec_09="ARCH-DEC-09" in task or True)
            self.last_cto_strategic_brief = brief
            return json.dumps(brief)

        # 2. Lead Specialist (Manager) decomposition
        if task_id.endswith("::decompose"):
            payload = _build_9_node_decomposition_payload()
            return json.dumps(payload)

        # 3. Researcher spec node
        if task_id == "research_spec":
            return json.dumps(
                {
                    "spec": "Token-bucket & sliding-window bounds verified via consensus MCP",
                    "invoked_mcps": mcps,
                    "invoked_tools": list(self.last_invoked_tools),
                }
            )

        # 4. Write slices in isolated git worktrees
        if task_id == "slice_auth":
            target = ws / "src" / "auth" / "token_validator.py"
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(AUTH_CODE, encoding="utf-8")
            _git(ws, "add", "src/auth/token_validator.py")
            _git(ws, "commit", "-m", "feat(auth): implement TenantTokenValidator.validate_tenant")
            return json.dumps({"slice": "slice_auth", "committed": True})

        if task_id == "slice_quota":
            target = ws / "src" / "quota" / "rate_limiter.py"
            target.parent.mkdir(parents=True, exist_ok=True)
            code = QUOTA_CODE_FLOAT_BUG if self.tunnel_view_float_trap else QUOTA_CODE_DECIMAL
            target.write_text(code, encoding="utf-8")
            _git(ws, "add", "src/quota/rate_limiter.py")
            _git(ws, "commit", "--allow-empty", "-m", "feat(quota): implement SlidingWindowLimiter.check_quota")
            return json.dumps({"slice": "slice_quota", "committed": True})

        if task_id == "slice_gateway":
            gw_target = ws / "src" / "api" / "gateway.py"
            gw_target.parent.mkdir(parents=True, exist_ok=True)
            gw_target.write_text(GATEWAY_CODE_PASS1_INCOMPLETE, encoding="utf-8")
            test_target = ws / "tests" / "test_gateway.py"
            test_target.parent.mkdir(parents=True, exist_ok=True)
            test_target.write_text(GATEWAY_TESTS_CODE, encoding="utf-8")
            _git(ws, "add", "src/api/gateway.py", "tests/test_gateway.py")
            _git(ws, "commit", "-m", "feat(gateway): add GatewayRouter.dispatch_request and unit tests")
            return json.dumps({"slice": "slice_gateway", "committed": True})

        # 5. Integrator node: deterministically cherry-pick all upstream SliceReceipts into repo_root
        if task_id == "integrate":
            slice_receipts: list[SliceReceipt] = list(kwargs.get("slice_receipts") or [])
            base_sha = str(kwargs.get("base_sha") or _git(self.repo_root, "rev-parse", "HEAD"))
            head = WorktreeManager().integrate_slices(self.repo_root, base_sha, slice_receipts)
            return json.dumps({"integrated_head_sha": head, "slices": [s.node_id for s in slice_receipts]})

        # 6. Reviewer Pass 1 (`review`): inspect repo_root AST & check for missing emit_audit_event or float violation
        if task_id == "review":
            candidate_head = _git(self.repo_root, "rev-parse", "HEAD")
            gw_src = (self.repo_root / "src" / "api" / "gateway.py").read_text(encoding="utf-8")
            quota_src = (self.repo_root / "src" / "quota" / "rate_limiter.py").read_text(encoding="utf-8")
            findings: list[str] = []
            if "def emit_audit_event" not in gw_src:
                findings.append("Missing GatewayRouter.emit_audit_event required by REQ-AUDIT-4")
            if "float(" in quota_src or "Decimal" not in quota_src:
                findings.append(
                    "Violation of [ARCH-DEC-09]: src/quota/rate_limiter.py uses float instead of decimal.Decimal with ROUND_HALF_EVEN"
                )
            verdict = "changes_requested" if findings else "approved"
            self.execution_identity = ExecutionIdentity(
                agent_name="Reviewer-Independent",
                harness_kind=self.kind,
                executable_or_endpoint="reviewer-dedicated-endpoint",
                profile_or_namespace="reviewer-isolated-ns",
                model="reviewer-distinct-model",
            )
            review_payload: dict[str, Any] = {
                "verdict": verdict,
                "reviewed_head_sha": candidate_head,
                "findings": findings,
                "commands": [
                    {"command": "ocr review --head " + candidate_head, "exit_code": 0, "output_ref": "ocr:pass1"},
                    {"command": "pytest tests/test_gateway.py -q", "exit_code": 0, "output_ref": "pytest:pass1"},
                ],
                "ocr_status": "passed",
                "acceptance_receipt": _build_acceptance_receipt_dict() if verdict == "approved" else None,
            }
            if review_payload["acceptance_receipt"] is None:
                # Provide full structural receipt when required by manifest even on changes_requested,
                # or let re_review verify the final remediated AST.
                review_payload.pop("acceptance_receipt")
            return json.dumps(review_payload)

        # 7. Remediation write node (`remediate`)
        if task_id == "remediate":
            # Sync integrated files from repo_root into the isolated remediation worktree before applying the fix
            for rel in (
                "src/auth/token_validator.py",
                "src/quota/rate_limiter.py",
                "src/api/gateway.py",
                "tests/test_gateway.py",
            ):
                src_file = self.repo_root / rel
                dst_file = ws / rel
                dst_file.parent.mkdir(parents=True, exist_ok=True)
                if src_file.exists():
                    dst_file.write_text(src_file.read_text(encoding="utf-8"), encoding="utf-8")
            # Apply remediated gateway and Decimal quota code
            (ws / "src" / "quota" / "rate_limiter.py").write_text(QUOTA_CODE_DECIMAL, encoding="utf-8")
            (ws / "src" / "api" / "gateway.py").write_text(GATEWAY_CODE_REMEDIATED, encoding="utf-8")
            (ws / "tests" / "test_gateway.py").write_text(GATEWAY_TESTS_CODE, encoding="utf-8")
            _git(ws, "add", "src/auth/token_validator.py", "src/quota/rate_limiter.py", "src/api/gateway.py", "tests/test_gateway.py")
            _git(ws, "commit", "-m", "fix(gateway,quota): add emit_audit_event and enforce ARCH-DEC-09 Decimal math")
            rem_head = _git(ws, "rev-parse", "HEAD")
            # Update repo_root to the remediated commit so re_review and pytest inspect the integrated tree
            subprocess.run(
                ["git", "-C", str(self.repo_root), "reset", "--hard", rem_head],
                check=True,
                capture_output=True,
                text=True,
            )
            return json.dumps({"remediated_head_sha": rem_head})

        # 8. Reviewer Pass 2 (`re_review`): run real pytest on repo_root and emit approved ReviewReceipt + AcceptanceReceipt
        if task_id == "re_review":
            candidate_head = _git(self.repo_root, "rev-parse", "HEAD")
            pytest_proc = subprocess.run(
                [sys.executable, "-m", "pytest", "tests/test_gateway.py", "-o", "addopts=", "-q"],
                cwd=str(self.repo_root),
                capture_output=True,
                text=True,
                timeout=30,
                check=False,
            )
            assert pytest_proc.returncode == 0, f"Integrated repo pytest failed:\n{pytest_proc.stdout}\n{pytest_proc.stderr}"
            self.execution_identity = ExecutionIdentity(
                agent_name="Reviewer-Independent",
                harness_kind=self.kind,
                executable_or_endpoint="reviewer-dedicated-endpoint",
                profile_or_namespace="reviewer-isolated-ns",
                model="reviewer-distinct-model",
            )
            review_payload = {
                "verdict": "approved",
                "reviewed_head_sha": candidate_head,
                "findings": [],
                "commands": [
                    {"command": "ocr review --head " + candidate_head, "exit_code": 0, "output_ref": "ocr:pass2"},
                    {
                        "command": "pytest tests/test_gateway.py -q",
                        "exit_code": pytest_proc.returncode,
                        "output_ref": pytest_proc.stdout.strip(),
                    },
                ],
                "ocr_status": "passed",
                "acceptance_receipt": _build_acceptance_receipt_dict(),
            }
            return json.dumps(review_payload)

        return json.dumps({"harness": self.name, "task_id": task_id, "status": "ok"})


class OmrRepeatableTestRig:
    """Builds an isolated, zero-bleed SubagentTable + 7 Specialists + 5 Harnesses + temp git repo."""

    def __init__(
        self,
        base_dir: Path,
        *,
        tunnel_view_float_trap: bool = False,
        omit_skill_evidence_for: set[str] | None = None,
    ) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.repo, self.base_sha = _create_seeded_repo(base_dir)
        self.worktrees_root = self.repo.parent / f"{self.repo.name}.worktrees"
        self.telemetry_path = base_dir / "specialist_telemetry.jsonl"
        self.queue_path = base_dir / "queue_ledger.jsonl"
        self.hot_md_path = base_dir / "hot.md"
        self.hot_md_path.write_text(
            "Active Workstream W-42: Token validator and rate limiter must share canonical TenantContext "
            "and redact Bearer secret-live-token-999 and sk-live-abc123456789.\n"
            "[GLOBAL INVARIANT] [ARCH-DEC-09]: All financial/metric/quota calculations across workstreams "
            "MUST use decimal.Decimal with ROUND_HALF_EVEN and include workstream_id; float is forbidden.\n",
            encoding="utf-8",
        )
        self.charter_context: dict[str, Any] = {
            "memories": [
                "Workstream W-42: Multi-Tenant Zero-Trust Quota & Cost Ledger uses TenantContext and ghp_secretToken999999",
            ],
            "hot_md_path": self.hot_md_path,
            "everos_fetcher": lambda _q: [
                "EverOS Goal G-7: Enforce zero-trust tenant isolation across all API gateway slices; never allow test-only wiring."
            ],
            "pinned_invariants": [
                "[ARCH-DEC-09]: Use decimal.Decimal with ROUND_HALF_EVEN and workstream_id across all ledger/quota slices."
            ],
        }
        self.harness_backends: dict[str, DeterministicHarnessBackend] = {}
        self.table = self._build_table(
            tunnel_view_float_trap=tunnel_view_float_trap,
            omit_skill_evidence_for=omit_skill_evidence_for,
        )

    def _build_table(
        self,
        *,
        tunnel_view_float_trap: bool = False,
        omit_skill_evidence_for: set[str] | None = None,
        extra_harnesses: list[Any] | None = None,
    ) -> SubagentTable:
        for idx, hname in enumerate(FIVE_HARNESSES):
            kind = "builtin" if hname == "Raven-General" else ("acp" if hname in ("Claude-Code", "Codex") else "cli")
            self.harness_backends[hname] = DeterministicHarnessBackend(
                hname,
                kind=kind,
                model=f"model-{idx + 1}",
                repo_root=self.repo,
                worktrees_root=self.worktrees_root,
                tunnel_view_float_trap=tunnel_view_float_trap,
                omit_skill_evidence_for=omit_skill_evidence_for,
            )

        def _build_builtin(row: Any, narrowed: Any) -> Any:
            b = self.harness_backends.get(row.name)
            if b is not None:
                b.last_narrowed = narrowed
            return b

        table = SubagentTable(build_builtin=_build_builtin)
        configs: list[Any] = []
        for spec_info in SEVEN_SPECIALISTS:
            configs.append(
                SpecialistSubagentConfig(
                    name=spec_info["name"],
                    description=spec_info["description"],
                    role_prompt=spec_info["role_prompt"],
                    skill_packs=list(spec_info["skill_packs"]),
                    mcp_servers=list(spec_info["mcp_servers"]),
                    required_capabilities=list(spec_info["required_capabilities"]),
                )
            )
        for hname in FIVE_HARNESSES:
            if hname == "Raven-General":
                configs.append(
                    BuiltinAgentConfig(
                        name="Raven-General",
                        description="Built-in Raven in-process execution harness",
                        is_harness=True,
                        tools=["read_file", "write_file", "exec"],
                        skills=["superpowers", "ecc", "mattpocock", "open-code-review"],
                    )
                )
            elif hname in ("Claude-Code", "Codex"):
                configs.append(
                    ThirdPartyAcpSubagentConfig(
                        name=hname,
                        description=f"{hname} ACP runtime harness",
                        command="echo",
                        is_harness=True,
                    )
                )
            else:
                configs.append(
                    ThirdPartyCliSubagentConfig(
                        name=hname,
                        description=f"{hname} CLI runtime harness",
                        command="echo {mcp_file}",
                        reads_local_files=True,
                        is_harness=True,
                    )
                )
        if extra_harnesses:
            configs.extend(extra_harnesses)

        table.apply(configs)
        self._wire_deterministic_harness_targets(table)
        return table

    def _wire_deterministic_harness_targets(self, table: SubagentTable) -> None:
        HarnessStartupManager.reconcile_harness_pool(
            self.harness_backends, self.base_dir / "harnesses", auto_install=True
        )
        for hname, hbackend in self.harness_backends.items():
            if hname in table._backends and not isinstance(table._backends[hname], RoutingBackend):
                table._backends[hname] = hbackend
        for row in table.rows():
            backend = table._backends.get(row.name)
            if isinstance(backend, RoutingBackend):
                backend._telemetry_path = self.telemetry_path
                new_targets: list[RouteTarget] = []
                for t in backend._targets:
                    hb = self.harness_backends.get(t.name, t.backend)
                    new_targets.append(
                        RouteTarget(
                            t.name,
                            t.description,
                            hb,
                            t.owes,
                            t.note,
                            t.needs,
                            t.min_tier,
                            t.needs_file,
                            t.fallback,
                            t.priority,
                            t.model or getattr(hb, "model", ""),
                        )
                    )
                backend._targets = new_targets
                if new_targets and backend._is_dynamic_specialist:
                    backend._primary = new_targets[0].backend


@pytest.fixture
def omr_mode(request: pytest.FixtureRequest) -> str:
    try:
        return str(request.config.getoption("--omr-mode") or "deterministic")
    except ValueError:
        return "deterministic"


@pytest.mark.slow
def test_01_idempotent_multi_run_repeatability(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, omr_mode: str) -> None:
    """Criterion 1: Run the full OMR-COMPLEX-001 pipeline twice back-to-back; assert identical waves, ASTs, and zero bleed."""
    monkeypatch.setattr("raven.agent.subagent.registry.discover_product_rows", lambda: [])

    def _execute_isolated_run(run_label: str) -> dict[str, Any]:
        run_base = tmp_path / run_label
        rig = OmrRepeatableTestRig(run_base)
        queue = OmrTaskQueue(rig.queue_path)
        dispatcher = OmrDispatcher(
            table=rig.table,
            queue=queue,
            workdir=rig.repo,
            run_root=run_base / "dag_runs",
            nodes_root=run_base / "dag_nodes",
            history_root=run_base / "dag_history",
            charter_context=rig.charter_context,
        )

        res = asyncio.run(
            dispatcher.submit_task(
                "OMR-COMPLEX-001",
                OMR_COMPLEX_001_PROMPT,
                run_id="omr-complex-001",
                cleanup_worktrees=True,
            )
        )

        # Verify requirements_manifest.json was written by DynamicDecompositionGate into run_dir BEFORE run_dag and remained intact
        assert res.decomposition_plan.manifest_path is not None
        assert res.decomposition_plan.manifest_path.exists()
        manifest_data = json.loads(res.decomposition_plan.manifest_path.read_text(encoding="utf-8"))
        assert manifest_data["expected_requirement_ids"] == EXPECTED_REQUIREMENT_IDS

        acc = AcceptanceReceipt.from_dict(_build_acceptance_receipt_dict())
        RequirementTraceabilityGate.verify_acceptance_receipt(
            acc,
            repo_root=rig.repo,
            expected_requirement_ids=manifest_data["expected_requirement_ids"],
        )

        tracked_files = sorted(_git(rig.repo, "ls-files").splitlines())
        ast_exports: dict[str, list[str]] = {}
        for rel in ("src/auth/token_validator.py", "src/quota/rate_limiter.py", "src/api/gateway.py"):
            tree = ast.parse((rig.repo / rel).read_text(encoding="utf-8"))
            symbols: list[str] = []
            for node in ast.walk(tree):
                if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                    symbols.append(node.name)
            ast_exports[rel] = sorted(symbols)

        worktree_lines = [
            line
            for line in _git(rig.repo, "worktree", "list").splitlines()
            if "worktrees" in line
        ]
        return {
            "waves": res.decomposition_plan.node_waves,
            "statuses": {f["node"]: f["status"] for f in res.dag_result.files},
            "tracked_files": tracked_files,
            "ast_exports": ast_exports,
            "leftover_worktrees": worktree_lines,
            "queue_entries": res.queue_entries,
        }

    run1 = _execute_isolated_run("run_1")
    run2 = _execute_isolated_run("run_2")

    assert run1["waves"] == run2["waves"]
    assert run1["statuses"] == run2["statuses"]
    assert all(st == "completed" for st in run1["statuses"].values())
    assert run1["tracked_files"] == run2["tracked_files"]
    assert run1["ast_exports"] == run2["ast_exports"]
    assert run1["leftover_worktrees"] == []
    assert run2["leftover_worktrees"] == []


@pytest.mark.slow
def test_02_to_06_full_intake_queue_routing_and_dynamic_decomposition_pipeline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Criteria 2, 3, 4, 5, 6: Top-level complex task intake, queue_ledger.jsonl, classifier routing, 9-node decomposition, and real SubagentTable routing."""
    monkeypatch.setattr("raven.agent.subagent.registry.discover_product_rows", lambda: [])
    rig = OmrRepeatableTestRig(tmp_path / "pipeline")
    queue = OmrTaskQueue(rig.queue_path)

    classifier_calls: list[tuple[list[tuple[str, str]], str, str]] = []

    async def _intake_classifier(menu: list[tuple[str, str]], prompt: str, default: str) -> str:
        classifier_calls.append((menu, prompt, default))
        return "Manager"

    dispatcher = OmrDispatcher(
        table=rig.table,
        queue=queue,
        workdir=rig.repo,
        run_root=tmp_path / "pipeline" / "dag_runs",
        nodes_root=tmp_path / "pipeline" / "dag_nodes",
        history_root=tmp_path / "pipeline" / "dag_history",
        intake_router=_intake_classifier,
        charter_context=rig.charter_context,
    )

    result = asyncio.run(
        dispatcher.submit_task("OMR-COMPLEX-001", OMR_COMPLEX_001_PROMPT, run_id="run-02-06")
    )
    assert result.decomposition_plan.manifest_path is not None
    assert result.decomposition_plan.manifest_path.exists()
    captured_manifest_ids = json.loads(
        result.decomposition_plan.manifest_path.read_text(encoding="utf-8")
    )["expected_requirement_ids"]

    # Bullet 2 & 3: Root task enqueued with seq_id=1, parent_task_id=None, status="enqueued" -> "routed" -> "decomposing" -> "running" -> "completed"
    assert rig.queue_path.exists()
    assert queue.lock_path.exists()
    all_entries = queue.entries()
    assert all_entries[0].seq_id == 1
    assert all_entries[0].task_id == "OMR-COMPLEX-001"
    assert all_entries[0].parent_task_id is None
    assert all_entries[0].status == "enqueued"

    root_statuses = [e.status for e in all_entries if e.task_id == "OMR-COMPLEX-001"]
    assert root_statuses == ["enqueued", "routed", "decomposing", "running", "completed"]

    # Bullet 4: Top-level routing via classifier to CTO (strategic brief) and Manager (decomposition)
    assert len(classifier_calls) == 1
    menu_names = [name for name, _ in classifier_calls[0][0]]
    assert "CTO" in menu_names and "Manager" in menu_names
    assert result.lead_specialist == "Manager"
    assert result.cto_specialist == "CTO"
    assert isinstance(result.cto_brief, CtoStrategicBrief)

    # Bullet 5: Manager dynamically decomposed OMR-COMPLEX-001 into 9 nodes + 4 requirement IDs
    assert captured_manifest_ids == EXPECTED_REQUIREMENT_IDS
    expected_9_nodes = {
        "cto_strategy",
        "research_spec",
        "slice_auth",
        "slice_quota",
        "slice_gateway",
        "integrate",
        "review",
        "remediate",
        "re_review",
    }
    decomposed_ids = {n.id for n in result.decomposition_plan.dag_spec.nodes}
    assert decomposed_ids == expected_9_nodes

    # Negative controls for DynamicDecompositionGate:
    # (a) Overlapping parallel write file_scope raises DagValidationError
    bad_overlap_payload = {
        "task_summary": "bad overlap",
        "expected_requirement_ids": ["REQ-1"],
        "nodes": [
            {"id": "cto_strategy", "subagent": "CTO", "node_summary": "plan", "prompt_template": "plan", "execution_kind": "plan"},
            {"id": "w1", "subagent": "Engineer - Superpowers", "node_summary": "w1", "prompt_template": "w1", "execution_kind": "write", "file_scope": ["src/**"], "depends_on": ["cto_strategy"]},
            {"id": "w2", "subagent": "Engineer - ECC", "node_summary": "w2", "prompt_template": "w2", "execution_kind": "write", "file_scope": ["src/auth/**"], "depends_on": ["cto_strategy"]},
            {"id": "integrate", "subagent": "Manager", "node_summary": "int", "prompt_template": "int", "execution_kind": "integrate", "depends_on": ["w1", "w2"]},
            {"id": "review", "subagent": "Reviewer", "node_summary": "rev", "prompt_template": "rev", "execution_kind": "review", "depends_on": ["integrate"]},
        ],
    }
    with pytest.raises(DagValidationError):
        DynamicDecompositionGate.validate_and_enqueue(
            bad_overlap_payload, OmrTaskQueue(tmp_path / "bad_q1.jsonl"), "ROOT-BAD-1"
        )

    # (b) Missing review/integrate node raises DagValidationError
    bad_missing_review_payload = {
        "task_summary": "missing review",
        "expected_requirement_ids": ["REQ-1"],
        "nodes": [
            {"id": "cto_strategy", "subagent": "CTO", "node_summary": "plan", "prompt_template": "plan", "execution_kind": "plan"},
            {"id": "w1", "subagent": "Engineer - Superpowers", "node_summary": "w1", "prompt_template": "w1", "execution_kind": "write", "file_scope": ["src/auth/**"], "depends_on": ["cto_strategy"]},
        ],
    }
    with pytest.raises(DagValidationError, match="review"):
        DynamicDecompositionGate.validate_and_enqueue(
            bad_missing_review_payload, OmrTaskQueue(tmp_path / "bad_q2.jsonl"), "ROOT-BAD-2"
        )

    # Bullet 6: All 9 decomposed child tasks enqueued with parent_task_id="OMR-COMPLEX-001" and transitioned enqueued -> routed -> running -> completed
    for nid in expected_9_nodes:
        child_events = [e for e in all_entries if e.task_id == nid]
        assert child_events[0].parent_task_id == "OMR-COMPLEX-001"
        statuses = [e.status for e in child_events]
        assert statuses == ["enqueued", "routed", "running", "completed"], f"Node {nid} had statuses {statuses}"
        assert child_events[-1].assigned_harness in FIVE_HARNESSES


@pytest.mark.slow
def test_07_specialist_skill_packs_mcps_and_tool_receipt_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Criterion 7 + Adversarial Trap A: Verify [BEGIN SPECIALIST BUNDLE], skill_packs, MCPs, _narrow_build, and SkillAndToolReceiptGate via OmrDispatcher."""
    monkeypatch.setattr("raven.agent.subagent.registry.discover_product_rows", lambda: [])
    rig = OmrRepeatableTestRig(tmp_path / "skills_rig")
    queue = OmrTaskQueue(rig.queue_path)
    dispatcher = OmrDispatcher(
        table=rig.table,
        queue=queue,
        workdir=rig.repo,
        run_root=tmp_path / "skills_rig" / "dag_runs",
        nodes_root=tmp_path / "skills_rig" / "dag_nodes",
        history_root=tmp_path / "skills_rig" / "dag_history",
        charter_context=rig.charter_context,
    )

    # Enable require_skill_evidence on all specialist backends so DagSpecialistRuntime.after_success enforces SkillAndToolReceiptGate in-pipeline
    for s_info in SEVEN_SPECIALISTS:
        b = rig.table.backend(s_info["name"])
        assert isinstance(b, RoutingBackend)
        b.require_skill_evidence = True

    res = asyncio.run(
        dispatcher.submit_task("OMR-SKILLS-001", OMR_COMPLEX_001_PROMPT, run_id="run-07-skills")
    )
    assert all(f["status"] == "completed" for f in res.dag_result.files)

    # Also verify each specialist via dispatcher.dispatch_specialist_node() with real subprocess CommandReceipt evidence
    expected_packs_by_specialist = {s["name"]: tuple(s["skill_packs"]) for s in SEVEN_SPECIALISTS}
    expected_mcps_by_specialist = {s["name"]: tuple(s["mcp_servers"]) for s in SEVEN_SPECIALISTS}

    for spec_name, packs in expected_packs_by_specialist.items():
        backend = rig.table.backend(spec_name)
        assert isinstance(backend, RoutingBackend)
        out = asyncio.run(
            dispatcher.dispatch_specialist_node(
                f"bundle-{spec_name}",
                spec_name,
                f"Verify bundle for {spec_name}",
                session_key="s-07",
                require_skill_evidence=True,
            )
        )
        assert out
        chosen_h = backend.last_chosen_backend
        h_backend = rig.harness_backends[chosen_h]
        last_call = h_backend.calls[-1]
        delivered_prompt = last_call["task"]
        assert f"[BEGIN SPECIALIST BUNDLE: {spec_name}]" in delivered_prompt
        for pack in packs:
            assert f"### Skill Pack: {pack}" in delivered_prompt
            rec = backend.last_skill_evidence[pack]
            assert "git status --porcelain" in rec.command
            assert rec.exit_code == 0
            assert rec.output_ref
        for mcp_srv in expected_mcps_by_specialist[spec_name]:
            assert mcp_srv in last_call["mcps"]

    # Check telemetry records contain skill_packs_injected & mcp_servers_attached
    telemetry_rows = [
        json.loads(line)
        for line in rig.telemetry_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    by_spec = {row["specialist"]: row for row in telemetry_rows}
    assert set(by_spec["CTO"]["skill_packs_injected"]) == {
        "office-hours",
        "plan-ceo-review",
        "plan-eng-review",
        "autoplan",
    }
    assert "consensus" in by_spec["Researcher"]["mcp_servers_attached"]
    assert "superpowers" in by_spec["Engineer - Superpowers"]["skill_packs_injected"]
    assert "mattpocock" in by_spec["Engineer - Matt Pocock"]["skill_packs_injected"]
    assert "ecc" in by_spec["Engineer - ECC"]["skill_packs_injected"]
    assert "open-code-review" in by_spec["Reviewer"]["skill_packs_injected"]

    # Verify _narrow_build tool/skill intersection on builtin Raven-General row
    rg_row = rig.table.get("Raven-General")
    assert rg_row is not None
    narrowed = _narrow_build(
        rg_row,
        type("_Build", (), {"tools_allow": ["read_file", "forbidden_tool"], "skills_allow": ["superpowers", "unknown_skill"]})(),
    )
    assert narrowed.tools_allow == ["read_file"]
    assert narrowed.skills_allow == ["superpowers"]

    # Adversarial Trap A: Rogue specialist omits required skill command receipt inside OmrDispatcher.submit_task() -> fails closed in DagSpecialistRuntime.after_success with SkillContractViolationError!
    rogue_rig = OmrRepeatableTestRig(tmp_path / "rogue_rig", omit_skill_evidence_for={"slice_quota"})
    rogue_backend = rogue_rig.table.backend("Engineer - Superpowers")
    rogue_backend.require_skill_evidence = True
    rogue_queue = OmrTaskQueue(rogue_rig.queue_path)
    rogue_dispatcher = OmrDispatcher(
        table=rogue_rig.table,
        queue=rogue_queue,
        workdir=rogue_rig.repo,
        run_root=tmp_path / "rogue_rig" / "dag_runs",
        nodes_root=tmp_path / "rogue_rig" / "dag_nodes",
        history_root=tmp_path / "rogue_rig" / "dag_history",
        charter_context=rogue_rig.charter_context,
    )
    rogue_res = asyncio.run(
        rogue_dispatcher.submit_task("OMR-ROGUE-001", OMR_COMPLEX_001_PROMPT, run_id="run-rogue")
    )
    rogue_map = {f["node"]: f for f in rogue_res.dag_result.files}
    assert rogue_map["slice_quota"]["status"] == "failed"
    assert "superpowers" in (rogue_map["slice_quota"]["error"] or "")
    assert rogue_queue.latest_by_task()["OMR-ROGUE-001"].status == "failed"

    with pytest.raises(SkillContractViolationError, match="superpowers"):
        asyncio.run(
            rogue_dispatcher.dispatch_specialist_node(
                "slice_quota",
                "Engineer - Superpowers",
                "Implement quota slice",
                require_skill_evidence=True,
            )
        )


@pytest.mark.slow
def test_08_cto_and_reviewer_remediation_and_rtm_ast_gate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Criterion 8 + Adversarial Traps D & E: CTO gate blocks writes on missing brief; Reviewer Pass 1 -> remediate -> Pass 2 + ExecutionIdentity + RTM AST gate."""
    monkeypatch.setattr("raven.agent.subagent.registry.discover_product_rows", lambda: [])
    rig = OmrRepeatableTestRig(tmp_path / "cto_rev_rig")

    # 1. Verify CTO gate blocks write node if CtoStrategicBrief is missing
    rt_block = DagSpecialistRuntime(workdir=rig.repo, run_dir=tmp_path / "cto_block_run")
    spec_block = SubAgentDagSpec(
        task_summary="cto block check",
        nodes=[
            DagNodeSpec(id="cto_strategy", subagent="CTO", node_summary="cto", prompt_template="cto", execution_kind="plan"),
            DagNodeSpec(id="slice_auth", subagent="Engineer - Matt Pocock", node_summary="auth", prompt_template="auth", execution_kind="write", file_scope=["src/auth/**"], depends_on=["cto_strategy"]),
        ],
    )
    rt_block.prepare_run(spec_block, str(rig.repo), "r-block", tmp_path / "cto_block_run")
    with pytest.raises(DagValidationError, match="ConsequenceMatrix|CtoStrategicBrief"):
        rt_block.before_attempt(spec_block.nodes[1], "auth prompt")

    # 2. Adversarial Trap D: Self-review / shared ExecutionIdentity collision raises IndependentReviewError
    author_id = ExecutionIdentity(
        agent_name="Engineer - Superpowers",
        harness_kind="acp",
        executable_or_endpoint="claude-endpoint",
        profile_or_namespace="default",
        model="claude-sonnet-4-5",
    )
    colluding_reviewer_id = ExecutionIdentity(
        agent_name="Reviewer",
        harness_kind="acp",
        executable_or_endpoint="claude-endpoint",
        profile_or_namespace="other-profile",
        model="claude-sonnet-4-5",
    )
    assert colluding_reviewer_id.conflicts_with(author_id) is True
    with pytest.raises(IndependentReviewError, match="reviewer backend must differ"):
        ReviewGateValidator().verify_review_receipt(
            ReviewReceipt(
                verdict="approved",
                reviewed_head_sha=rig.base_sha,
                findings=[],
                commands=[CommandReceipt("ocr review", 0)],
                ocr_status="passed",
                reviewer_identity=colluding_reviewer_id,
            ),
            candidate_head_sha=rig.base_sha,
            reviewer_backend=colluding_reviewer_id,
            author_backends=[author_id],
            ocr_available=True,
        )

    # 3. Run full CTO -> Write Slices -> Integrate -> Review Pass 1 (changes_requested) -> Remediate -> Re-Review Pass 2 (approved)
    # with requirements_manifest.json intact in run_dir throughout the entire run!
    queue = OmrTaskQueue(rig.queue_path)
    dispatcher = OmrDispatcher(
        table=rig.table,
        queue=queue,
        workdir=rig.repo,
        run_root=tmp_path / "cto_rev_rig" / "dag_runs",
        nodes_root=tmp_path / "cto_rev_rig" / "dag_nodes",
        history_root=tmp_path / "cto_rev_rig" / "dag_history",
        charter_context=rig.charter_context,
    )
    res = asyncio.run(dispatcher.submit_task("OMR-COMPLEX-001", OMR_COMPLEX_001_PROMPT, run_id="r-remediate"))
    assert res.decomposition_plan.manifest_path is not None
    assert res.decomposition_plan.manifest_path.exists()

    node_map = {f["node"]: f for f in res.dag_result.files}
    assert node_map["review"]["status"] == "completed"
    assert node_map["review"]["selected_routes"] == ["changes_requested"]
    assert node_map["remediate"]["status"] == "completed"
    assert node_map["re_review"]["status"] == "completed"

    # Verify real AcceptanceReceipt 1:1 AST symbol verification against integrated worktree
    acc = AcceptanceReceipt.from_dict(_build_acceptance_receipt_dict())
    verified = RequirementTraceabilityGate.verify_acceptance_receipt(
        acc,
        repo_root=rig.repo,
        expected_requirement_ids=EXPECTED_REQUIREMENT_IDS,
    )
    assert len(verified.requirements) == 4

    # 4. Adversarial Trap E: Missing requirement ID, test-only wiring, or comment-only symbol raises PrematureCompletionError
    with pytest.raises(PrematureCompletionError, match="Missing required IDs"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(feature_name=BRAND_NAME, requirements=acc.requirements[:3]),
            repo_root=rig.repo,
            expected_requirement_ids=EXPECTED_REQUIREMENT_IDS,
        )

    with pytest.raises(PrematureCompletionError, match="no_test_only_wiring"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=[
                    RequirementItem(
                        id="REQ-TEST-ONLY",
                        description="bad test path",
                        status="met",
                        entrypoint_caller="tests/test_gateway.py:test_end_to_end_gateway_quota_and_audit_ledger",
                        e2e_command="pytest -q",
                    )
                ],
            ),
            repo_root=rig.repo,
        )

    (rig.repo / "src" / "api" / "comment_only.py").write_text("# fake_symbol_in_comment_only\n", encoding="utf-8")
    with pytest.raises(PrematureCompletionError, match="not found in AST"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=[
                    RequirementItem(
                        id="REQ-FAKE-AST",
                        description="comment symbol",
                        status="met",
                        entrypoint_caller="src/api/comment_only.py:fake_symbol_in_comment_only",
                        e2e_command="pytest -q",
                    )
                ],
            ),
            repo_root=rig.repo,
        )


def test_09_harness_pool_distribution_and_workload_learning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Criterion 9: 5 onboarded harnesses distribute concurrent in-flight tasks across all 5 harnesses via inflight_count tie-breaking and shift dynamically on telemetry."""
    monkeypatch.setattr("raven.agent.subagent.registry.discover_product_rows", lambda: [])
    rig = OmrRepeatableTestRig(tmp_path / "dist_rig")
    queue = OmrTaskQueue(rig.queue_path)
    dispatcher = OmrDispatcher(
        table=rig.table,
        queue=queue,
        workdir=rig.repo,
        run_root=tmp_path / "dist_rig" / "dag_runs",
        nodes_root=tmp_path / "dist_rig" / "dag_nodes",
        history_root=tmp_path / "dist_rig" / "dag_history",
        charter_context=rig.charter_context,
    )

    # Disable telemetry file during concurrent cold-start check so all 5 harnesses have identical (0.0, 0.0) workload scores
    for row in rig.table.rows():
        b = rig.table._backends.get(row.name)
        if isinstance(b, RoutingBackend):
            b._telemetry_path = None

    # Install an asyncio barrier across the 5 harness backends so all 5 tasks are simultaneously in-flight (inflight_count > 0)
    async def _run_concurrent_inflight_distribution() -> list[str]:
        all_entered = asyncio.Event()
        entered_harnesses: list[str] = []

        for hname, hb in rig.harness_backends.items():
            orig_run = hb.run

            async def _barrier_run(
                task: str,
                *,
                _orig: Any = orig_run,
                _hname: str = hname,
                **kw: Any,
            ) -> str:
                entered_harnesses.append(_hname)
                if len(entered_harnesses) >= 5:
                    all_entered.set()
                await asyncio.wait_for(all_entered.wait(), timeout=10.0)
                return await _orig(task, **kw)

            hb.run = _barrier_run  # type: ignore[method-assign]

        await asyncio.gather(
            *(
                dispatcher.dispatch_specialist_node(
                    f"concurrent-cold-{i}",
                    SEVEN_SPECIALISTS[i]["name"],
                    f"Concurrent cold start task {i}",
                    session_key="s-dist",
                )
                for i in range(5)
            )
        )
        return entered_harnesses

    concurrent_harnesses = asyncio.run(_run_concurrent_inflight_distribution())
    # All 5 onboarded harnesses (including builtin Raven-General) must be selected simultaneously across the 5 concurrent in-flight dispatches!
    assert len(concurrent_harnesses) == 5
    assert set(concurrent_harnesses) == set(FIVE_HARNESSES)

    # Now enable telemetry with varying latency/pass rates and verify workload learning shifts traffic to the fastest 100%-passing harness via OmrDispatcher
    tpath = tmp_path / "dist_rig" / "learned_telemetry.jsonl"
    seeded_rows = [
        {"chosen_backend": "Hermes", "final_status": "failed", "total_duration_ms": 900.0},
        {"chosen_backend": "Copilot", "final_status": "succeeded", "total_duration_ms": 450.0},
        {"chosen_backend": "Claude-Code", "final_status": "succeeded", "total_duration_ms": 25.0},
        {"chosen_backend": "Codex", "final_status": "succeeded", "total_duration_ms": 300.0},
        {"chosen_backend": "Raven-General", "final_status": "succeeded", "total_duration_ms": 200.0},
    ]
    tpath.write_text("\n".join(json.dumps(r) for r in seeded_rows) + "\n", encoding="utf-8")

    cto_b = rig.table.backend("CTO")
    assert isinstance(cto_b, RoutingBackend)
    cto_b._telemetry_path = tpath
    asyncio.run(
        dispatcher.dispatch_specialist_node(
            "learned-dispatch-1",
            "CTO",
            "Strategic review under learned telemetry",
            session_key="s-learned",
        )
    )
    assert queue.latest_by_task()["learned-dispatch-1"].assigned_harness == "Claude-Code"


@pytest.mark.slow
def test_10_tunnel_view_semantic_trap_and_secret_redaction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Criterion 10 + Adversarial Trap B: [ARCH-DEC-09] has zero lexical overlap with local prompt; secrets redacted; float trap rejected by Reviewer and remediated to Decimal."""
    monkeypatch.setattr("raven.agent.subagent.registry.discover_product_rows", lambda: [])

    # 1. Verify build_context_charter preserves pinned [ARCH-DEC-09] even when local prompt has zero lexical overlap, and redacts secrets
    hot_md = tmp_path / "hot_trap.md"
    hot_md.write_text(
        "Unrelated header with secret sk-live-abc123456789 and Bearer secret-live-token-999\n"
        "[GLOBAL INVARIANT] [ARCH-DEC-09]: Monetary and quota calculations must use decimal.Decimal with ROUND_HALF_EVEN and workstream_id (auth: sk-live-abc123456789, Bearer secret-live-token-999).\n",
        encoding="utf-8",
    )
    local_slice_prompt = "Format metric helper in module"  # Zero lexical overlap with 'Monetary', 'calculations', 'decimal', 'ROUND_HALF_EVEN'
    charter = build_context_charter(
        local_slice_prompt,
        hot_md_path=hot_md,
        everos_fetcher=lambda _q: ["PINNED: EverOS secret api_key=topsecret999"],
    )
    assert "[BEGIN UNTRUSTED CONTEXT CHARTER]" in charter
    assert "[ARCH-DEC-09]" in charter
    assert "ROUND_HALF_EVEN" in charter
    assert "sk-live-abc123456789" not in charter
    assert "secret-live-token-999" not in charter
    assert "topsecret999" not in charter
    assert "[REDACTED]" in charter

    # 2. Run pipeline with tunnel_view_float_trap=True on slice_quota (with requirements_manifest.json kept intact in run_dir!):
    # slice_quota initially commits float math -> Reviewer Pass 1 rejects citing [ARCH-DEC-09] -> remediate replaces float with Decimal -> Pass 2 approves!
    rig = OmrRepeatableTestRig(tmp_path / "trap_rig", tunnel_view_float_trap=True)
    queue = OmrTaskQueue(rig.queue_path)
    dispatcher = OmrDispatcher(
        table=rig.table,
        queue=queue,
        workdir=rig.repo,
        run_root=tmp_path / "trap_rig" / "dag_runs",
        nodes_root=tmp_path / "trap_rig" / "dag_nodes",
        history_root=tmp_path / "trap_rig" / "dag_history",
        charter_context=rig.charter_context,
    )
    res = asyncio.run(dispatcher.submit_task("OMR-COMPLEX-001", OMR_COMPLEX_001_PROMPT, run_id="r-trap"))
    assert res.decomposition_plan.manifest_path is not None
    assert res.decomposition_plan.manifest_path.exists()

    # Verify Reviewer Pass 1 caught the ARCH-DEC-09 float violation
    review_out_path = tmp_path / "trap_rig" / "dag_nodes" / "review.out.md"
    review_pass1_data = json.loads(review_out_path.read_text(encoding="utf-8"))
    assert review_pass1_data["verdict"] == "changes_requested"
    assert any("ARCH-DEC-09" in f for f in review_pass1_data["findings"])

    # Verify remediated integrated code in src/quota/rate_limiter.py uses Decimal and ROUND_HALF_EVEN and workstream_id
    final_quota_code = (rig.repo / "src" / "quota" / "rate_limiter.py").read_text(encoding="utf-8")
    assert "Decimal" in final_quota_code
    assert "ROUND_HALF_EVEN" in final_quota_code
    assert "workstream_id" in final_quota_code
    assert "float(" not in final_quota_code
    assert res.cto_brief is not None
    assert any("ARCH-DEC-09" in s for s in res.cto_brief.workstream_synergies)


@pytest.mark.slow
def test_11_seven_by_five_specialist_harness_matrix_hot_onboarding_and_crash_failover(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Criterion 11 + Adversarial Traps C & F: 7 Specialists x 5 Harnesses = 35 permutations, hot-onboarding 6th harness (Cursor-ACP), clean crash failover, and dirty worktree quarantine via OmrDispatcher."""
    monkeypatch.setattr("raven.agent.subagent.registry.discover_product_rows", lambda: [])
    rig = OmrRepeatableTestRig(tmp_path / "matrix_rig")
    queue = OmrTaskQueue(rig.queue_path)
    dispatcher = OmrDispatcher(
        table=rig.table,
        queue=queue,
        workdir=rig.repo,
        run_root=tmp_path / "matrix_rig" / "dag_runs",
        nodes_root=tmp_path / "matrix_rig" / "dag_nodes",
        history_root=tmp_path / "matrix_rig" / "dag_history",
        charter_context=rig.charter_context,
    )

    # 1. Execute all 7 Specialists x 5 Onboarded Harnesses = 35 permutations through OmrDispatcher
    executed_pairs: set[tuple[str, str]] = set()
    for s_info in SEVEN_SPECIALISTS:
        spec_name = s_info["name"]
        routing_entry = rig.table.backend(spec_name)
        assert isinstance(routing_entry, RoutingBackend)
        target_names = [t.name for t in routing_entry._targets]
        assert set(target_names) == set(FIVE_HARNESSES), (
            f"Specialist {spec_name} missing harnesses from pool: got {target_names}"
        )

        for target_harness in FIVE_HARNESSES:
            # Cool down all other harnesses to force execution on target_harness
            routing_entry._cooldown_until.clear()
            for other_h in FIVE_HARNESSES:
                if other_h != target_harness:
                    routing_entry._cooldown_until[other_h] = routing_entry._clock() + 3600.0

            tid = f"matrix::{spec_name}::{target_harness}"
            res_str = asyncio.run(
                dispatcher.dispatch_specialist_node(
                    tid,
                    spec_name,
                    f"Matrix run {spec_name} on {target_harness}",
                    session_key="s-matrix",
                    require_skill_evidence=True,
                )
            )
            assert res_str
            assert routing_entry.last_chosen_backend == target_harness
            assert queue.latest_by_task()[tid].assigned_harness == target_harness
            assert queue.latest_by_task()[tid].status == "completed"
            hb = rig.harness_backends[target_harness]
            last_call = hb.calls[-1]
            assert f"[BEGIN SPECIALIST BUNDLE: {spec_name}]" in last_call["task"]
            for pack in s_info["skill_packs"]:
                assert f"### Skill Pack: {pack}" in last_call["task"]
            for mcp in s_info["mcp_servers"]:
                assert mcp in last_call["mcps"]
            executed_pairs.add((spec_name, target_harness))
        routing_entry._cooldown_until.clear()

    assert len(executed_pairs) == 35

    # 2. Hot-onboard a 6th harness (Cursor-ACP) via table.apply() with ZERO changes to any specialist config, and run all 7 specialists via OmrDispatcher
    cursor_backend = DeterministicHarnessBackend(
        "Cursor-ACP",
        kind="acp",
        model="cursor-sonnet",
        repo_root=rig.repo,
        worktrees_root=rig.worktrees_root,
    )
    rig.harness_backends["Cursor-ACP"] = cursor_backend
    cursor_cfg = ThirdPartyAcpSubagentConfig(
        name="Cursor-ACP",
        description="Hot-onboarded Cursor ACP harness",
        command="echo",
        is_harness=True,
    )
    hot_table = rig._build_table(extra_harnesses=[cursor_cfg])
    dispatcher.table = hot_table
    for s_info in SEVEN_SPECIALISTS:
        spec_name = s_info["name"]
        b = hot_table.backend(spec_name)
        assert isinstance(b, RoutingBackend)
        assert "Cursor-ACP" in [t.name for t in b._targets]
        # Cool down the original 5 harnesses and verify the specialist runs on Cursor-ACP through OmrDispatcher
        for h in FIVE_HARNESSES:
            b._cooldown_until[h] = b._clock() + 3600.0
        hot_tid = f"hot::{spec_name}"
        asyncio.run(
            dispatcher.dispatch_specialist_node(
                hot_tid,
                spec_name,
                f"Run {spec_name} on hot-onboarded Cursor-ACP",
                session_key="s-hot",
                require_skill_evidence=True,
            )
        )
        assert b.last_chosen_backend == "Cursor-ACP"
        assert queue.latest_by_task()[hot_tid].assigned_harness == "Cursor-ACP"
        b._cooldown_until.clear()

    # 3. Adversarial Trap C1: Mid-flight clean harness crash inside OmrDispatcher -> specialist migrates to next harness with full bundle
    crash_calls = 0

    class _CrashingCleanHarness:
        name = "Hermes"
        kind = "cli"
        streams = False
        model = "hermes-m1"

        async def run(self, task: str, **kw: Any) -> str:
            nonlocal crash_calls
            crash_calls += 1
            raise TimeoutError("Hermes timed out before touching worktree")

    eng_b = hot_table.backend("Engineer - Superpowers")
    assert isinstance(eng_b, RoutingBackend)
    eng_b._cooldown_until.clear()
    eng_b._targets[0] = RouteTarget(
        "Hermes",
        "crashing hermes",
        _CrashingCleanHarness(),  # type: ignore[arg-type]
        fallback=True,
        priority=0,
        model="hermes-m1",
    )
    failover_out = asyncio.run(
        dispatcher.dispatch_specialist_node(
            "clean-failover-1",
            "Engineer - Superpowers",
            "Task surviving clean harness crash",
            session_key="s-failover",
            writes=True,
            idempotent=True,
            require_skill_evidence=True,
        )
    )
    assert failover_out
    assert crash_calls == 1
    assert eng_b.last_chosen_backend != "Hermes"
    assert eng_b._is_cooled_down("Hermes") is True
    assert queue.latest_by_task()["clean-failover-1"].status == "completed"
    assert queue.latest_by_task()["clean-failover-1"].assigned_harness != "Hermes"

    # 4. Adversarial Trap C2: Dirty worktree crash inside OmrDispatcher blocks failover with SubagentFailoverBlockedError & quarantines in DurableRecoveryLedger
    dirty_wt = WorktreeManager().provision_worktree(
        rig.repo, rig.worktrees_root, "dirtyrun", "slice_dirty", rig.base_sha
    )

    class _DirtyCrashingHarness:
        name = "DirtyHarness"
        kind = "cli"
        streams = False
        model = "dirty-m1"

        async def run(self, task: str, *, workspace: Path, **kw: Any) -> str:
            (Path(workspace) / "corrupt_partial.py").write_text("partial = 1\n", encoding="utf-8")
            raise TimeoutError("Crashed after leaving uncommitted file")

    dirty_entry = RoutingBackend(
        "Engineer - ECC",
        _DirtyCrashingHarness(),  # type: ignore[arg-type]
        [
            RouteTarget("DirtyHarness", "dirty", _DirtyCrashingHarness(), fallback=True, priority=0),  # type: ignore[arg-type]
            RouteTarget("Copilot", "clean backup", rig.harness_backends["Copilot"], fallback=True, priority=1),
        ],
        instances=type("_Inst", (), {"list_instances": lambda self, s=None: []})(),
        telemetry_path=rig.telemetry_path,
        is_dynamic_specialist=True,
    )
    hot_table._backends["Engineer - ECC"] = dirty_entry
    rt_dirty = DagSpecialistRuntime(workdir=rig.repo, run_dir=tmp_path / "dirty_run_dir")
    spec_dirty = SubAgentDagSpec(
        task_summary="dirty test",
        nodes=[
            DagNodeSpec(
                id="slice_dirty",
                subagent="Engineer - ECC",
                node_summary="dirty slice",
                prompt_template="dirty slice",
                execution_kind="write",
                file_scope=["src/quota/**"],
            )
        ],
    )
    rt_dirty.prepare_run(spec_dirty, str(rig.repo), "dirtyrun", tmp_path / "dirty_run_dir")
    rt_dirty.node_worktrees["slice_dirty"] = dirty_wt

    with pytest.raises(SubagentFailoverBlockedError) as exc_info:
        try:
            asyncio.run(
                dispatcher.dispatch_specialist_node(
                    "slice_dirty",
                    "Engineer - ECC",
                    "Write quota slice",
                    workspace=dirty_wt,
                    writes=True,
                    idempotent=True,
                )
            )
        except SubagentFailoverBlockedError as exc:
            rt_dirty.on_failure(spec_dirty.nodes[0], "Write quota slice", exc)
            raise

    assert queue.latest_by_task()["slice_dirty"].status == "failed"
    assert exc_info.value.attempt_receipts[0]["status"] == "blocked_dirty_worktree"
    assert "corrupt_partial.py" in exc_info.value.attempt_receipts[0]["dirty_paths"]
    assert rt_dirty.ledger is not None
    records = rt_dirty.ledger._records()
    assert any(r["next_action"] == "quarantine" for r in records.values())

    # Cleanup dirty_wt
    _git(rig.repo, "worktree", "remove", "--force", str(dirty_wt), check=False)


def test_candidate_module_source_of_truth_is_local_repo() -> None:
    pkg = (Path(__file__).resolve().parent.parent / "oh_my_raven").resolve()
    for obj in (OmrDispatcher, RoutingBackend):
        assert obj.__module__.startswith("oh_my_raven")
        src = Path(inspect.getsourcefile(obj)).resolve()
        assert src.is_relative_to(pkg), src
