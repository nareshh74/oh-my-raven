"""oh-my-raven: Harness-agnostic specialist orchestration, worktree isolation, review gates, and requirement traceability."""

from __future__ import annotations

import ast
import asyncio
import fnmatch
import hashlib
import json
import os
import re
import shutil
import subprocess
import tempfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Literal, Sequence

from loguru import logger

from raven.agent.subagent.dag_graph import DagNodeSpec
from raven.agent.subagent.prompt_errors import DagValidationError
from raven.contracts.subagent_backend import SubagentBackend, SubagentNoAnswerError
from .gate import LessonsStore, TriageDecision, classify_triage_tier, escalate_triage_tier, evaluate_council_consensus, GatePolicy, has_verifiable_evidence
from .routing import RouteTarget, RoutingBackend

BRAND_NAME = "oh-my-raven"


class SpecialistPlaceholderBackend(SubagentBackend):
    """Logical placeholder backend for a harness-free Specialist (kind='specialist').

    Real execution runs on the Dynamic Runtime Harness Pool through `RoutingBackend`.
    If no harness is available in the pool, this raises `SubagentNoAnswerError`.
    """

    kind = "specialist"
    streams = True

    def __init__(self, name: str) -> None:
        self.name = name

    async def run(self, task: str, **kwargs: Any) -> str:
        raise SubagentNoAnswerError(
            f"Specialist {self.name!r} is harness-free and no runtime harness is available in the Dynamic Harness Pool"
        )


@dataclass(frozen=True)
class SpecialistDefinition:
    """Portable, harness-agnostic Specialist definition (role + skill_packs + mcp_servers + capabilities)."""

    name: str
    role_prompt: str = ""
    soul_path: str = ""
    skill_packs: tuple[str, ...] = ()
    skill_dirs: tuple[str, ...] = ()
    mcp_servers: tuple[str, ...] = ()
    required_capabilities: tuple[str, ...] = ()
    is_harness: bool = False

    @classmethod
    def from_obj(cls, obj: Any) -> "SpecialistDefinition":
        if isinstance(obj, SpecialistDefinition):
            return obj
        read = obj.get if isinstance(obj, dict) else lambda k, d=None: getattr(obj, k, d)
        name = str(read("name", "") or "")
        kind = str(read("kind", "") or "")
        role_prompt = str(read("role_prompt", "") or read("rolePrompt", "") or "")
        soul_path = str(read("soul_path", "") or read("soulPath", "") or "")
        skill_packs = tuple(str(s) for s in (read("skill_packs", None) or read("skillPacks", None) or ()) if str(s))
        skill_dirs = tuple(str(s) for s in (read("skill_dirs", None) or read("skillDirs", None) or ()) if str(s))
        mcp_servers = tuple(
            str(m)
            for m in (
                read("mcp_servers", None)
                or read("mcpServers", None)
                or read("mcps", None)
                or ()
            )
            if str(m)
        )
        req_caps = tuple(
            str(c)
            for c in (read("required_capabilities", None) or read("requiredCapabilities", None) or ())
            if str(c)
        )
        is_harness_raw = read("is_harness", None)
        if is_harness_raw is None:
            is_harness_raw = read("isHarness", None)
        is_harness = False if kind == "specialist" else (True if is_harness_raw is None else bool(is_harness_raw))
        return cls(
            name=name,
            role_prompt=role_prompt,
            soul_path=soul_path,
            skill_packs=skill_packs,
            skill_dirs=skill_dirs,
            mcp_servers=mcp_servers,
            required_capabilities=req_caps,
            is_harness=is_harness,
        )

    @property
    def has_specialist_bundle(self) -> bool:
        return bool(self.role_prompt.strip() or self.soul_path.strip() or self.skill_packs or self.mcp_servers)


_PORTABLE_SKILL_PACK_LIBRARY: dict[str, str] = {
    "superpowers": (
        "Methodology [superpowers]: Follow disciplined TDD & verification-before-completion. "
        "1) Reproduce or write failing test first; 2) Implement minimal surgical code to pass; "
        "3) Run verification commands and inspect real exit codes before claiming completion."
    ),
    "ecc": (
        "Methodology [ecc]: Everything-Claude-Code engineering rigor. "
        "Enforce strict modularity, explicit error handling, zero silent failures, and clean git hygiene."
    ),
    "mattpocock": (
        "Methodology [mattpocock]: Type-driven design & total-typescript style precision. "
        "Make illegal states unrepresentable, narrow boundary types, and validate schemas at the edge."
    ),
    "agent-skills": (
        "Methodology [agent-skills]: Structured agent workflow decomposition, explicit tool verification, "
        "and deterministic state transitions."
    ),
    "pstack": (
        "Methodology [pstack]: Full-stack production readiness, end-to-end wiring checks, "
        "and latency/resource awareness."
    ),
    "open-code-review": (
        "Methodology [open-code-review]: Multi-perspective adversarial code review (security, correctness, "
        "blast radius, test coverage, and requirement traceability)."
    ),
}


class SkillPackResolver:
    """Resolves portable Specialist role prompts, skill_packs, and MCP server hints for any runtime harness."""

    DEFAULT_SKILL_SEARCH_ROOTS: tuple[Path, ...] = (
        Path.home() / ".raven" / "workspace" / "skills",
        Path.home() / "AppData" / "Local" / "hermes" / "profiles",
    )

    @classmethod
    def resolve_soul_text(cls, specialist: SpecialistDefinition) -> str:
        parts: list[str] = []
        if specialist.role_prompt.strip():
            parts.append(specialist.role_prompt.strip())
        if specialist.soul_path.strip():
            spath = Path(specialist.soul_path).expanduser()
            if spath.is_file():
                try:
                    soul_text = spath.read_text(encoding="utf-8", errors="replace").strip()
                except OSError as exc:
                    logger.debug("Unable to read specialist soul file {}: {}", spath, exc)
                    soul_text = ""
                if soul_text:
                    parts.append(soul_text)
        return "\n\n".join(p for p in parts if p)

    @classmethod
    def resolve_skill_pack_text(
        cls,
        pack_name: str,
        *,
        extra_dirs: Sequence[str | Path] = (),
        max_chars_per_pack: int = 1500,
    ) -> str:
        norm = pack_name.strip().lower()
        if not norm:
            return ""
        search_paths: list[Path] = [Path(d).expanduser() for d in extra_dirs if str(d).strip()]
        for root in cls.DEFAULT_SKILL_SEARCH_ROOTS:
            if root.exists():
                search_paths.append(root)
        for base in search_paths:
            candidates = [
                base / pack_name / "SKILL.md",
                base / norm / "SKILL.md",
                base / f"{norm}.md",
            ]
            if base.name == "profiles" and base.is_dir():
                for prof_skill in base.glob(f"*{norm}*/skills/**/SKILL.md"):
                    candidates.append(prof_skill)
            for cand in candidates:
                if cand.is_file():
                    try:
                        raw = cand.read_text(encoding="utf-8", errors="replace").strip()
                        if raw:
                            return f"### Skill Pack: {pack_name}\n{_SECRET.sub('[REDACTED]', raw)[:max_chars_per_pack]}"
                    except OSError as exc:
                        logger.debug("Skipping unreadable skill pack file {}: {}", cand, exc)
                        continue
        builtin_text = _PORTABLE_SKILL_PACK_LIBRARY.get(norm)
        if builtin_text:
            return f"### Skill Pack: {pack_name}\n{builtin_text}"
        return f"### Skill Pack: {pack_name}\nApply portable '{pack_name}' methodology and verification standards."

    @classmethod
    def render_specialist_bundle(
        cls,
        specialist: SpecialistDefinition | Any,
        task_prompt: str,
    ) -> str:
        spec = SpecialistDefinition.from_obj(specialist)
        if not spec.has_specialist_bundle:
            return task_prompt
        if "[BEGIN SPECIALIST BUNDLE:" in task_prompt:
            return task_prompt
        sections: list[str] = [f"[BEGIN SPECIALIST BUNDLE: {spec.name or 'Specialist'}]"]
        soul = cls.resolve_soul_text(spec)
        if soul:
            sections.append(f"## Specialist Role & Charter\n{_SECRET.sub('[REDACTED]', soul)}")
        if spec.skill_packs:
            pack_blocks = [
                cls.resolve_skill_pack_text(pack, extra_dirs=spec.skill_dirs)
                for pack in spec.skill_packs
                if str(pack).strip()
            ]
            if pack_blocks:
                sections.append("## Portable Specialist Skill Packs\n" + "\n\n".join(pack_blocks))
        if spec.mcp_servers:
            sections.append(
                "## Attached Specialist MCP Servers\n"
                f"Active MCP servers for this specialist: {', '.join(spec.mcp_servers)}"
            )
        sections.append(f"[END SPECIALIST BUNDLE: {spec.name or 'Specialist'}]")
        bundle = "\n\n".join(sections)
        return f"{bundle}\n\n{task_prompt}"


SkillPackInjector = SkillPackResolver


class DirtyBaselineError(RuntimeError):
    pass


class ScopeViolationError(RuntimeError):
    pass


class IntegrationConflictError(RuntimeError):
    pass


class IndependentReviewError(RuntimeError):
    pass


class StaleReviewError(RuntimeError):
    pass


class ReviewCycleLimitError(RuntimeError):
    pass


class PrematureCompletionError(RuntimeError):
    """Raised when an AcceptanceReceipt fails the RequirementTraceabilityGate."""


class SkillContractViolationError(RuntimeError):
    """Raised when a specialist omits required skill/tool/MCP execution evidence."""


@dataclass(frozen=True)
class CommandReceipt:
    command: str
    exit_code: int
    output_ref: str = ""


@dataclass(frozen=True)
class ExecutionIdentity:
    agent_name: str
    harness_kind: str = ""
    executable_or_endpoint: str = ""
    profile_or_namespace: str = ""
    model: str = ""

    def conflicts_with(self, other: "ExecutionIdentity") -> bool:
        return (
            self.agent_name == other.agent_name
            or (
                self.harness_kind
                and self.harness_kind == other.harness_kind
                and self.executable_or_endpoint
                and self.executable_or_endpoint == other.executable_or_endpoint
                and self.profile_or_namespace == other.profile_or_namespace
            )
            or (
                self.model
                and self.model == other.model
                and self.executable_or_endpoint
                and self.executable_or_endpoint == other.executable_or_endpoint
            )
        )


@dataclass(frozen=True)
class SliceReceipt:
    node_id: str
    head_sha: str
    base_sha: str = ""
    worktree_path: str = ""
    changed_paths: tuple[str, ...] = ()
    committed: bool = True
    tests: tuple[CommandReceipt, ...] = ()
    invoked_tools: tuple[str, ...] = ()
    invoked_mcps: tuple[str, ...] = ()
    skill_packs_injected: tuple[str, ...] = ()
    skill_evidence: dict[str, CommandReceipt] = field(default_factory=dict)


class SkillAndToolReceiptGate:
    """Fail-closed gate verifying a specialist actually invoked its required skills, tools, and MCP servers."""

    @classmethod
    def extract_evidence(
        cls,
        agent_backend: Any,
        result: str = "",
        *,
        spec_def: SpecialistDefinition | None = None,
    ) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...], dict[str, CommandReceipt]]:
        invoked_tools_raw = getattr(agent_backend, "last_invoked_tools", None)
        invoked_mcps_raw = getattr(agent_backend, "last_invoked_mcps", None)
        skill_ev_raw = getattr(agent_backend, "last_skill_evidence", None)
        if result and (invoked_tools_raw is None or invoked_mcps_raw is None or skill_ev_raw is None):
            try:
                loaded = json.loads(result)
            except (TypeError, ValueError):
                loaded = None
            if isinstance(loaded, dict):
                if invoked_tools_raw is None:
                    invoked_tools_raw = getattr(loaded, "get")("invoked_tools") or ()
                if invoked_mcps_raw is None:
                    invoked_mcps_raw = getattr(loaded, "get")("invoked_mcps") or ()
                if skill_ev_raw is None:
                    skill_ev_raw = getattr(loaded, "get")("skill_evidence") or {}

        invoked_tools = tuple(str(t) for t in (invoked_tools_raw or ()) if str(t))
        invoked_mcps = tuple(str(m) for m in (invoked_mcps_raw or ()) if str(m))
        skill_packs = tuple(spec_def.skill_packs) if spec_def is not None else ()
        skill_evidence: dict[str, CommandReceipt] = {}
        if isinstance(skill_ev_raw, dict):
            for k, v in skill_ev_raw.items():
                if isinstance(v, CommandReceipt):
                    skill_evidence[str(k)] = v
                elif isinstance(v, dict):
                    skill_evidence[str(k)] = CommandReceipt(
                        command=str(v.get("command", "")),
                        exit_code=int(v.get("exit_code", 0)),
                        output_ref=str(v.get("output_ref", "")),
                    )
                elif isinstance(v, str):
                    skill_evidence[str(k)] = CommandReceipt(command=v, exit_code=0)
        return invoked_tools, invoked_mcps, skill_packs, skill_evidence

    @classmethod
    def verify_specialist_execution(
        cls,
        node: Any,
        agent_backend: Any,
        result: str = "",
        *,
        delivered_prompt: str = "",
        require_skill_evidence: bool = False,
        required_skill_packs: Sequence[str] = (),
        required_tools: Sequence[str] = (),
        required_mcps: Sequence[str] = (),
    ) -> SliceReceipt | dict[str, Any]:
        spec_def_raw = getattr(agent_backend, "specialist_definition", None) or getattr(
            node, "specialist_definition", None
        )
        spec_def = SpecialistDefinition.from_obj(spec_def_raw) if spec_def_raw is not None else None
        invoked_tools, invoked_mcps, skill_packs, skill_evidence = cls.extract_evidence(
            agent_backend, result, spec_def=spec_def
        )
        effective_require = (
            require_skill_evidence
            or bool(getattr(agent_backend, "require_skill_evidence", False))
            or bool(getattr(node, "require_skill_evidence", False))
            or bool(required_skill_packs)
            or bool(required_tools)
            or bool(required_mcps)
        )
        if not effective_require:
            return {
                "invoked_tools": invoked_tools,
                "invoked_mcps": invoked_mcps,
                "skill_packs_injected": skill_packs,
                "skill_evidence": skill_evidence,
            }

        expected_packs = tuple(required_skill_packs) or (spec_def.skill_packs if spec_def is not None else ())
        expected_mcps = tuple(required_mcps) or (spec_def.mcp_servers if spec_def is not None else ())
        expected_tools = tuple(required_tools) or tuple(getattr(agent_backend, "required_tools", ()) or ())

        if delivered_prompt and spec_def is not None and spec_def.has_specialist_bundle:
            if "[BEGIN SPECIALIST BUNDLE:" not in delivered_prompt:
                raise SkillContractViolationError(
                    f"Node '{getattr(node, 'id', '')}' missing [BEGIN SPECIALIST BUNDLE] in delivered prompt"
                )

        for pack in expected_packs:
            rec = skill_evidence.get(pack)
            if rec is None or not rec.command.strip() or rec.exit_code != 0:
                raise SkillContractViolationError(
                    f"Specialist on node '{getattr(node, 'id', '')}' failed closed: missing or non-zero CommandReceipt for required skill pack '{pack}'"
                )

        for mcp in expected_mcps:
            if mcp not in invoked_mcps:
                raise SkillContractViolationError(
                    f"Specialist on node '{getattr(node, 'id', '')}' failed closed: required MCP server '{mcp}' was not invoked (invoked_mcps={invoked_mcps})"
                )

        for tool in expected_tools:
            if tool not in invoked_tools:
                raise SkillContractViolationError(
                    f"Specialist on node '{getattr(node, 'id', '')}' failed closed: required tool '{tool}' was not invoked (invoked_tools={invoked_tools})"
                )

        if not skill_evidence and not invoked_tools and not invoked_mcps:
            raise SkillContractViolationError(
                f"Specialist on node '{getattr(node, 'id', '')}' failed closed: require_skill_evidence=True but no skill_evidence, invoked_tools, or invoked_mcps were recorded"
            )

        return {
            "invoked_tools": invoked_tools,
            "invoked_mcps": invoked_mcps,
            "skill_packs_injected": skill_packs,
            "skill_evidence": skill_evidence,
        }


@dataclass(frozen=True)
class RequirementItem:
    id: str
    description: str
    status: Literal["met", "unmet", "deferred"] = "met"
    entrypoint_caller: str = ""
    e2e_command: str = ""
    evidence_ref: str = ""


@dataclass(frozen=True)
class AcceptanceReceipt:
    feature_name: str
    requirements: list[RequirementItem]
    unmet_requirements: list[str] = field(default_factory=list)
    deferred_requirements: list[str] = field(default_factory=list)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "AcceptanceReceipt":
        reqs = [
            item if isinstance(item, RequirementItem) else RequirementItem(**item)
            for item in data.get("requirements", [])
        ]
        return cls(
            feature_name=str(data.get("feature_name", BRAND_NAME)),
            requirements=reqs,
            unmet_requirements=list(data.get("unmet_requirements", [])),
            deferred_requirements=list(data.get("deferred_requirements", [])),
        )


class RequirementTraceabilityGate:
    """Fail-closed gate verifying every requirement is met and wired into a production entrypoint."""

    _FORBIDDEN_PATH_SEGMENTS = frozenset({"tests", "test", "scripts", "conftest.py"})

    @classmethod
    def _is_test_or_script_path(cls, normalized_file: str) -> bool:
        segments = [seg for seg in normalized_file.split("/") if seg]
        if not segments:
            return True
        if any(seg in cls._FORBIDDEN_PATH_SEGMENTS for seg in segments):
            return True
        basename = segments[-1]
        if basename.startswith("test_") or basename.endswith("_test.py"):
            return True
        return False

    @staticmethod
    def _symbol_exists_in_ast(source: str, symbol_part: str) -> bool:
        try:
            tree = ast.parse(source)
        except SyntaxError:
            return False

        parts = [tok.strip() for tok in symbol_part.split(".") if tok.strip()]
        if not parts:
            return False

        defined_names: set[str] = set()
        class_methods: dict[str, set[str]] = {}
        referenced_names: set[str] = set()
        referenced_attrs: set[tuple[str, str]] = set()

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                defined_names.add(node.name)
            elif isinstance(node, ast.ClassDef):
                defined_names.add(node.name)
                methods = class_methods.setdefault(node.name, set())
                for item in node.body:
                    if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        methods.add(item.name)
                    elif isinstance(item, ast.Assign):
                        for target in item.targets:
                            if isinstance(target, ast.Name):
                                methods.add(target.id)
                    elif isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                        methods.add(item.target.id)
            elif isinstance(node, ast.Name):
                referenced_names.add(node.id)
            elif isinstance(node, ast.Attribute):
                referenced_names.add(node.attr)
                if isinstance(node.value, ast.Name):
                    referenced_attrs.add((node.value.id, node.attr))
                elif isinstance(node.value, ast.Call) and isinstance(node.value.func, ast.Name):
                    referenced_attrs.add((node.value.func.id, node.attr))
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    defined_names.add(alias.asname or alias.name)

        if len(parts) == 1:
            target_sym = parts[0]
            return target_sym in defined_names or target_sym in referenced_names

        owner, member = parts[0], parts[-1]
        if owner in class_methods and member in class_methods[owner]:
            return True
        if (owner, member) in referenced_attrs:
            return True
        return all(part in defined_names or part in referenced_names for part in parts)

    @classmethod
    def verify_acceptance_receipt(
        cls,
        receipt: AcceptanceReceipt,
        repo_root: Path | None = None,
        expected_requirement_ids: Sequence[str] | None = None,
    ) -> AcceptanceReceipt:
        if not receipt.requirements:
            raise PrematureCompletionError("acceptance receipt has no requirements")
        if receipt.unmet_requirements:
            raise PrematureCompletionError(
                f"acceptance receipt has unmet requirements: {', '.join(receipt.unmet_requirements)}"
            )
        if receipt.deferred_requirements:
            raise PrematureCompletionError(
                f"acceptance receipt has deferred requirements: {', '.join(receipt.deferred_requirements)}"
            )
        seen_ids: set[str] = set()
        dup_ids: set[str] = set()
        for item in receipt.requirements:
            if item.id in seen_ids:
                dup_ids.add(item.id)
            seen_ids.add(item.id)
        if dup_ids:
            raise PrematureCompletionError(
                f"Duplicate requirement IDs in AcceptanceReceipt: {sorted(dup_ids)}"
            )
        if expected_requirement_ids is not None:
            expected_list = [str(x).strip() for x in expected_requirement_ids if str(x).strip()]
            seen_expected: set[str] = set()
            dup_expected: set[str] = set()
            for req_id in expected_list:
                if req_id in seen_expected:
                    dup_expected.add(req_id)
                seen_expected.add(req_id)
            if dup_expected:
                raise PrematureCompletionError(
                    f"Duplicate expected_requirement_ids: {sorted(dup_expected)}"
                )
            expected_set = seen_expected
            missing = expected_set - seen_ids
            if missing:
                raise PrematureCompletionError(
                    f"Missing required IDs from AcceptanceReceipt: {sorted(missing)}"
                )
            extra = seen_ids - expected_set
            if extra:
                raise PrematureCompletionError(
                    f"Unexpected extra IDs in AcceptanceReceipt: {sorted(extra)}"
                )
        for item in receipt.requirements:
            if item.status != "met":
                raise PrematureCompletionError(
                    f"requirement {item.id!r} has non-met status {item.status!r}"
                )
            caller = item.entrypoint_caller.strip()
            if not caller:
                raise PrematureCompletionError(
                    f"requirement {item.id!r} is missing entrypoint_caller"
                )
            if not item.e2e_command.strip():
                raise PrematureCompletionError(
                    f"requirement {item.id!r} is missing e2e_command"
                )
            file_part, sep, symbol_part = caller.partition(":")
            normalized_file = file_part.replace("\\", "/").strip().lstrip("./")
            if cls._is_test_or_script_path(normalized_file):
                raise PrematureCompletionError(
                    f"requirement {item.id!r} violates no_test_only_wiring: entrypoint_caller {caller!r} points to test/script path"
                )
            if repo_root is not None:
                candidate = Path(repo_root) / normalized_file
                if not candidate.exists() and not normalized_file.endswith(".py"):
                    module_candidate = Path(repo_root) / (normalized_file.replace(".", "/") + ".py")
                    if module_candidate.exists():
                        candidate = module_candidate
                if not candidate.exists() or not candidate.is_file():
                    raise PrematureCompletionError(
                        f"requirement {item.id!r} entrypoint_caller file does not exist under repo_root: {normalized_file!r}"
                    )
                if sep and symbol_part.strip():
                    content = candidate.read_text(encoding="utf-8", errors="replace")
                    if not cls._symbol_exists_in_ast(content, symbol_part.strip()):
                        raise PrematureCompletionError(
                            f"requirement {item.id!r} symbol {symbol_part!r} not found in AST of {normalized_file!r}"
                        )
        return receipt


@dataclass(frozen=True)
class ReviewReceipt:
    verdict: str
    reviewed_head_sha: str
    findings: list[str]
    commands: Sequence[str | CommandReceipt]
    ocr_status: str = "unavailable"
    reviewer_identity: ExecutionIdentity | None = None
    author_identities: tuple[ExecutionIdentity, ...] = ()
    acceptance_receipt: AcceptanceReceipt | None = None


@dataclass(frozen=True)
class ConsequenceEntry:
    decision: str
    second_order_effect: str
    rollback_plan: str
    blast_radius: str
    affected_files: str = ""
    failure_modes: str = ""
    contract_changes: str = ""
    validation_plan: str = ""


@dataclass(frozen=True)
class ConsequenceMatrix:
    affected_files: Sequence[str] | str = ()
    blast_radius: str = ""
    failure_modes: Sequence[str] | str = ()
    rollback_plan: str = ""
    contract_changes: Sequence[str] | str = ()
    validation_plan: Sequence[str] | str = ()
    entries: Sequence[ConsequenceEntry] = ()

    @classmethod
    def from_payload(cls, payload: Any) -> "ConsequenceMatrix":
        if isinstance(payload, ConsequenceMatrix):
            return payload
        if isinstance(payload, str):
            data = json.loads(payload)
        elif isinstance(payload, dict):
            data = payload
        elif isinstance(payload, (list, tuple)):
            entries = [
                item if isinstance(item, ConsequenceEntry) else ConsequenceEntry(**item)
                for item in payload
            ]
            return cls(entries=tuple(entries))
        else:
            raise ValueError("invalid ConsequenceMatrix payload")
        if isinstance(data, dict) and "consequence_matrix" in data and isinstance(data["consequence_matrix"], dict):
            data = data["consequence_matrix"]
        if not isinstance(data, dict):
            raise ValueError("ConsequenceMatrix payload must be a JSON object")
        raw_entries = data.get("entries") or ()
        entries = tuple(
            item if isinstance(item, ConsequenceEntry) else ConsequenceEntry(**item)
            for item in raw_entries
        )
        return cls(
            affected_files=data.get("affected_files", ()),
            blast_radius=str(data.get("blast_radius", "")),
            failure_modes=data.get("failure_modes", ()),
            rollback_plan=str(data.get("rollback_plan", "")),
            contract_changes=data.get("contract_changes", ()),
            validation_plan=data.get("validation_plan", ()),
            entries=entries,
        )

    def validate(self) -> "ConsequenceMatrix":
        def _non_empty(val: Any) -> bool:
            if isinstance(val, str):
                return bool(val.strip())
            if isinstance(val, (list, tuple, set)):
                return any(str(x).strip() for x in val)
            return bool(val)

        if self.entries:
            validate_consequence_matrix(self.entries)
            if not (
                _non_empty(self.affected_files)
                or any(_non_empty(e.affected_files) or _non_empty(e.decision) for e in self.entries)
            ):
                raise ValueError("ConsequenceMatrix requires affected_files or valid entries")
            return self

        has_five_dims = (
            _non_empty(self.affected_files)
            and _non_empty(self.blast_radius)
            and _non_empty(self.failure_modes)
            and _non_empty(self.rollback_plan)
            and (_non_empty(self.contract_changes) or _non_empty(self.validation_plan))
        )
        if not has_five_dims:
            raise ValueError(
                "ConsequenceMatrix requires 5 non-empty dimensions: affected_files, blast_radius, failure_modes, rollback_plan, and contract_changes/validation_plan"
            )
        return self


@dataclass(frozen=True)
class CtoStrategicBrief:
    """Active CTO strategic alignment & cross-workstream synergy brief (fixing tunnel view)."""

    workstream_synergies: tuple[str, ...] = ()
    long_term_goal_alignment: str = ""
    cross_project_dependencies: tuple[str, ...] = ()
    consequence_matrix: ConsequenceMatrix = field(default_factory=ConsequenceMatrix)

    @classmethod
    def from_payload(cls, payload: Any) -> "CtoStrategicBrief":
        if isinstance(payload, CtoStrategicBrief):
            return payload
        if isinstance(payload, str):
            raw_obj = json.loads(payload)
        elif isinstance(payload, dict):
            raw_obj = payload
        else:
            raise ValueError("invalid CtoStrategicBrief payload")
        if not isinstance(raw_obj, dict):
            raise ValueError("CtoStrategicBrief payload must be a JSON object")
        nested = raw_obj.get("cto_strategic_brief")
        source = dict(nested) if isinstance(nested, dict) else dict(raw_obj)
        if "consequence_matrix" not in source and "consequence_matrix" in raw_obj:
            source["consequence_matrix"] = raw_obj["consequence_matrix"]
        synergies_raw = source["workstream_synergies"] if "workstream_synergies" in source else ()
        if isinstance(synergies_raw, str):
            synergies = (synergies_raw.strip(),) if synergies_raw.strip() else ()
        else:
            synergies = tuple(str(s).strip() for s in (synergies_raw or ()) if str(s).strip())
        deps_raw = source["cross_project_dependencies"] if "cross_project_dependencies" in source else ()
        if isinstance(deps_raw, str):
            deps = (deps_raw.strip(),) if deps_raw.strip() else ()
        else:
            deps = tuple(str(d).strip() for d in (deps_raw or ()) if str(d).strip())
        alignment_val = source["long_term_goal_alignment"] if "long_term_goal_alignment" in source else ""
        alignment = str(alignment_val or "").strip()
        matrix_payload = source["consequence_matrix"] if "consequence_matrix" in source else source
        matrix = ConsequenceMatrix.from_payload(matrix_payload)
        return cls(
            workstream_synergies=synergies,
            long_term_goal_alignment=alignment,
            cross_project_dependencies=deps,
            consequence_matrix=matrix,
        )

    def validate(self, *, require_synergies: bool = True) -> "CtoStrategicBrief":
        self.consequence_matrix.validate()
        if require_synergies:
            if not self.long_term_goal_alignment.strip():
                raise ValueError("CtoStrategicBrief requires non-empty long_term_goal_alignment")
            if not self.workstream_synergies or not any(s.strip() for s in self.workstream_synergies):
                raise ValueError("CtoStrategicBrief requires non-empty workstream_synergies")
        return self

    def render_guidance_block(self) -> str:
        synergies = "; ".join(self.workstream_synergies) if self.workstream_synergies else "none"
        deps = "; ".join(self.cross_project_dependencies) if self.cross_project_dependencies else "none"
        return (
            "[BEGIN CTO STRATEGIC BRIEF]\n"
            f"Long-Term Goal Alignment: {_SECRET.sub('[REDACTED]', self.long_term_goal_alignment)}\n"
            f"Cross-Workstream Synergies: {_SECRET.sub('[REDACTED]', synergies)}\n"
            f"Cross-Project Dependencies: {_SECRET.sub('[REDACTED]', deps)}\n"
            f"Blast Radius: {_SECRET.sub('[REDACTED]', self.consequence_matrix.blast_radius)}\n"
            f"Rollback Plan: {_SECRET.sub('[REDACTED]', self.consequence_matrix.rollback_plan)}\n"
            "[END CTO STRATEGIC BRIEF]"
        )


class CtoAlignmentGate:
    """Enforces active CTO strategic alignment and cross-workstream synergy on CTO plan nodes."""

    @staticmethod
    def is_cto_node(node: Any) -> bool:
        if getattr(node, "execution_kind", None) != "plan":
            return False
        sub = str(getattr(node, "subagent", "") or "").strip().lower()
        nid = str(getattr(node, "id", "") or "").strip().lower()
        sub_tokens = [tok for tok in re.split(r"[^a-z0-9]+", sub) if tok]
        nid_tokens = [tok for tok in re.split(r"[^a-z0-9]+", nid) if tok]
        return "cto" in sub_tokens or "cto" in nid_tokens

    @classmethod
    def verify_cto_brief(
        cls,
        payload: Any,
        *,
        require_cto_alignment: bool = True,
    ) -> CtoStrategicBrief:
        brief = CtoStrategicBrief.from_payload(payload)
        return brief.validate(require_synergies=require_cto_alignment)


def _normalize(path: str) -> str:
    return path.replace("\\", "/").strip().strip("/")


def path_matches_scope(rel_path: str, file_scope: Sequence[str]) -> bool:
    path = _normalize(rel_path)
    for scope in file_scope:
        pattern = _normalize(scope)
        if pattern.endswith("/**"):
            prefix = pattern[:-3]
            if path == prefix or path.startswith(prefix + "/"):
                return True
        if path == pattern or path.startswith(pattern + "/") or fnmatch.fnmatchcase(path, pattern):
            return True
    return False


def scopes_overlap(scope_a: Sequence[str], scope_b: Sequence[str]) -> tuple[bool, str | None]:
    for left in scope_a:
        for right in scope_b:
            a, b = _normalize(left), _normalize(right)
            if not a or not b:
                continue
            a_prefix = a[:-3] if a.endswith("/**") else a
            b_prefix = b[:-3] if b.endswith("/**") else b
            if (
                a == b
                or a_prefix.startswith(b_prefix + "/")
                or b_prefix.startswith(a_prefix + "/")
                or fnmatch.fnmatchcase(a_prefix, b)
                or fnmatch.fnmatchcase(b_prefix, a)
            ):
                return True, f"{left} <-> {right}"
    return False, None


def _git(repo: Path, *args: str, check: bool = True, timeout: int = 60) -> subprocess.CompletedProcess[str]:
    git_bin = shutil.which("git") or "git"
    return subprocess.run(
        [git_bin, "-C", str(repo), *args],
        check=check,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def _nul_paths(value: str, *, status: bool = False) -> list[str]:
    entries, paths, index = value.split("\0"), [], 0
    while index < len(entries):
        entry = entries[index]
        if not entry:
            index += 1
            continue
        if status:
            code, path = entry[:2], entry[3:]
            paths.append(path)
            if "R" in code or "C" in code:
                index += 1
                if index < len(entries) and entries[index]:
                    paths.append(entries[index])
        else:
            paths.append(entry)
        index += 1
    return paths


class WorktreeManager:
    def verify_clean_baseline(self, worktree_path: Path, expected_head_sha: str | None = None) -> None:
        if _git(worktree_path, "status", "--porcelain=v1", "-z", "-uall").stdout:
            raise DirtyBaselineError(f"baseline repository is dirty: {worktree_path}")
        if expected_head_sha and _git(worktree_path, "rev-parse", "HEAD").stdout.strip() != expected_head_sha:
            raise DirtyBaselineError(f"HEAD differs from expected baseline: {worktree_path}")

    def provision_worktree(
        self,
        repo_root: Path,
        worktrees_root: Path,
        run_id: str,
        node_id: str,
        base_sha: str,
        *,
        require_clean_source: bool = False,
    ) -> Path:
        if require_clean_source:
            self.verify_clean_baseline(repo_root)
        commit_sha = _git(repo_root, "rev-parse", "--verify", f"{base_sha}^{{commit}}").stdout.strip()
        path = worktrees_root / f"{run_id}-{node_id}"
        if path.exists():
            raise DirtyBaselineError(f"worktree already exists: {path}")
        worktrees_root.mkdir(parents=True, exist_ok=True)
        _git(repo_root, "worktree", "add", "--detach", str(path), commit_sha)
        self.verify_clean_baseline(path, commit_sha)
        return path

    def verify_worktree_scope(self, worktree_path: Path, base_sha: str, file_scope: Sequence[str]) -> list[str]:
        commit_sha = _git(worktree_path, "rev-parse", "--verify", f"{base_sha}^{{commit}}").stdout.strip()
        if _git(worktree_path, "merge-base", "--is-ancestor", commit_sha, "HEAD", check=False).returncode:
            raise ScopeViolationError(f"HEAD does not descend from {base_sha}")
        paths = sorted(
            set(
                _nul_paths(_git(worktree_path, "diff", "--name-only", "-z", f"{commit_sha}..HEAD").stdout)
                + _nul_paths(_git(worktree_path, "status", "--porcelain=v1", "-z", "-uall").stdout, status=True)
            )
        )
        outside = [path for path in paths if not path_matches_scope(path, file_scope)]
        if outside:
            raise ScopeViolationError(f"out-of-scope paths: {', '.join(outside)}")
        return paths

    def build_slice_receipt(
        self,
        worktree_path: Path,
        node_id: str,
        base_sha: str,
        file_scope: Sequence[str],
        tests: Sequence[CommandReceipt] = (),
        *,
        invoked_tools: Sequence[str] = (),
        invoked_mcps: Sequence[str] = (),
        skill_packs_injected: Sequence[str] = (),
        skill_evidence: dict[str, CommandReceipt] | None = None,
    ) -> SliceReceipt:
        paths = self.verify_worktree_scope(worktree_path, base_sha, file_scope)
        self.verify_clean_baseline(worktree_path)
        return SliceReceipt(
            node_id,
            _git(worktree_path, "rev-parse", "HEAD").stdout.strip(),
            base_sha,
            str(worktree_path),
            tuple(paths),
            True,
            tuple(tests),
            tuple(str(t) for t in invoked_tools if str(t)),
            tuple(str(m) for m in invoked_mcps if str(m)),
            tuple(str(s) for s in skill_packs_injected if str(s)),
            dict(skill_evidence or {}),
        )

    def integrate_slices(
        self,
        integration_worktree: Path,
        base_sha: str | Path,
        slices: Sequence[SliceReceipt] | str,
        *legacy: object,
    ) -> str | tuple[Path, str]:
        legacy_call = len(legacy) == 2
        if legacy_call:
            repo_root = integration_worktree
            worktrees_root = base_sha
            run_id = slices
            actual_base_sha, actual_slices = legacy
            if (
                not isinstance(worktrees_root, Path)
                or not isinstance(run_id, str)
                or not isinstance(actual_base_sha, str)
                or not isinstance(actual_slices, Sequence)
            ):
                raise TypeError("legacy integrate_slices expects (repo_root, worktrees_root, run_id, base_sha, slices)")
            integration_worktree = self.provision_worktree(
                repo_root, worktrees_root, run_id, "integration", actual_base_sha
            )
            base_sha, slices = actual_base_sha, actual_slices
        elif legacy:
            raise TypeError("integrate_slices expects (integration_worktree, base_sha, slices)")
        if not isinstance(base_sha, str) or isinstance(slices, str):
            raise TypeError("integrate_slices expects (integration_worktree, base_sha, slices)")
        base = _git(integration_worktree, "rev-parse", "--verify", f"{base_sha}^{{commit}}").stdout.strip()
        self.verify_clean_baseline(integration_worktree, base)
        for receipt in sorted(slices, key=lambda item: item.node_id):
            if _git(integration_worktree, "merge-base", "--is-ancestor", base, receipt.head_sha, check=False).returncode:
                raise IntegrationConflictError(f"slice {receipt.node_id} does not descend from {base_sha}")
            if _git(integration_worktree, "cherry-pick", receipt.head_sha, check=False).returncode:
                conflicts = _nul_paths(
                    _git(integration_worktree, "diff", "--name-only", "-z", "--diff-filter=U", check=False).stdout
                )
                _git(integration_worktree, "cherry-pick", "--abort", check=False)
                raise IntegrationConflictError(f"cherry-pick conflict: {', '.join(conflicts) or receipt.node_id}")
        head = _git(integration_worktree, "rev-parse", "HEAD").stdout.strip()
        return (integration_worktree, head) if legacy_call else head

    def cleanup_worktree(self, repo_root: Path, worktree_path: Path) -> None:
        _git(repo_root, "worktree", "remove", str(worktree_path))


def _write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        json.dump(data, handle, sort_keys=True)
        temporary = handle.name
    os.replace(temporary, path)


class DurableRecoveryLedger:
    def __init__(self, ledger_path: Path) -> None:
        self.ledger_path = ledger_path

    def _records(self) -> dict[str, Any]:
        return json.loads(self.ledger_path.read_text(encoding="utf-8")) if self.ledger_path.exists() else {}

    def record_attempt(
        self,
        *,
        run_id: str,
        node_id: str,
        input_hash: str,
        backend: str,
        failure_class: str,
        worktree_path: str = "",
        committed_sha: str = "",
        dirty_paths: Sequence[str] = (),
        max_attempts: int = 2,
    ) -> dict[str, Any]:
        records, key = self._records(), f"{run_id}:{node_id}:{input_hash}"
        record = records.get(key, {"attempt_count": 0})
        record.update(
            backend=backend,
            failure_class=failure_class,
            worktree_path=worktree_path,
            committed_sha=committed_sha,
            dirty_paths=list(dirty_paths),
        )
        if failure_class == "CancelledError":
            record["next_action"] = "cancelled"
        else:
            record["attempt_count"] += 1
            record["next_action"] = (
                "quarantine" if dirty_paths else "escalate" if record["attempt_count"] >= max_attempts else "retry"
            )
        records[key] = record
        _write_json(self.ledger_path, records)
        return record

    def reconcile(
        self,
        *,
        run_id: str,
        node_id: str,
        input_hash: str,
        worktree_path: Path | None = None,
        base_sha: str | None = None,
    ) -> dict[str, Any]:
        records, key = self._records(), f"{run_id}:{node_id}:{input_hash}"
        record = records.get(key, {"attempt_count": 0, "next_action": "retry"})
        if worktree_path and (
            _git(worktree_path, "status", "--porcelain=v1", "-z", "-uall").stdout
            or base_sha
            and _git(worktree_path, "rev-parse", "HEAD").stdout.strip() != base_sha
        ):
            record["next_action"] = "quarantine"
            records[key] = record
            _write_json(self.ledger_path, records)
        return record


class ReviewCycleTracker:
    def __init__(self, state_path: Path, max_cycles: int = 2) -> None:
        self.state_path, self.max_cycles = state_path, max_cycles

    def record_verdict(self, run_id: str, candidate_head_sha: str, verdict: str) -> int:
        state = json.loads(self.state_path.read_text(encoding="utf-8")) if self.state_path.exists() else {}
        key = f"{run_id}:{candidate_head_sha}"
        state[key] = state.get(key, 0) + (verdict == "changes_requested")
        _write_json(self.state_path, state)
        if verdict == "changes_requested" and state[key] > self.max_cycles:
            raise ReviewCycleLimitError("review remediation cycle limit reached")
        return state[key]


class ReviewGateValidator:
    def validate_workflow_structure(self, nodes: Sequence[DagNodeSpec]) -> list[str]:
        by_id = {node.id: node for node in nodes}

        def downstream(start: str, target: str) -> bool:
            seen: set[str] = set()
            stack = [target]
            while stack:
                node_id = stack.pop()
                if node_id in seen:
                    continue
                seen.add(node_id)
                if start in by_id[node_id].depends_on:
                    return True
                stack.extend(by_id[node_id].depends_on)
            return False

        writes = [node for node in nodes if node.execution_kind == "write"]
        if not writes:
            return []
        plans = [node for node in nodes if node.execution_kind == "plan"]
        reviews = [node for node in nodes if node.execution_kind == "review"]
        errors: list[str] = []
        if plans and not all(any(downstream(plan.id, write.id) for plan in plans) for write in writes):
            errors.append("plan workflow requires write nodes to depend on an upstream plan node")
        if not reviews or not any(all(downstream(write.id, review.id) for write in writes) for review in reviews):
            errors.append("write workflow must have a downstream review node")
        parallel = [
            left
            for left in writes
            if any(
                left is not right and not downstream(left.id, right.id) and not downstream(right.id, left.id)
                for right in writes
            )
        ]
        if parallel:
            integrations = [node for node in nodes if node.execution_kind == "integrate"]
            if not any(all(downstream(write.id, integration.id) for write in parallel) for integration in integrations):
                errors.append("parallel write workflow must have a downstream integrate node")
        return errors

    def verify_review_receipt(
        self,
        receipt: ReviewReceipt,
        *,
        candidate_head_sha: str,
        reviewer_backend: str | ExecutionIdentity,
        author_backends: Sequence[str | ExecutionIdentity],
        cycle: int = 0,
        max_cycles: int = 2,
        ocr_available: bool = False,
        cycle_tracker: ReviewCycleTracker | None = None,
        run_id: str = "default",
        require_acceptance_receipt: bool = False,
        expected_requirement_ids: Sequence[str] | None = None,
        repo_root: Path | None = None,
    ) -> str:
        reviewer = receipt.reviewer_identity or (
            reviewer_backend if isinstance(reviewer_backend, ExecutionIdentity) else ExecutionIdentity(reviewer_backend)
        )
        authors = [
            (item if isinstance(item, ExecutionIdentity) else ExecutionIdentity(item)) for item in author_backends
        ] + list(receipt.author_identities)
        if any(reviewer.conflicts_with(author) for author in authors):
            raise IndependentReviewError("reviewer backend must differ from every author backend")
        if receipt.reviewed_head_sha != candidate_head_sha:
            raise StaleReviewError("review receipt does not match candidate head")
        if not receipt.commands:
            raise IndependentReviewError("review receipt requires command evidence")
        if receipt.verdict not in {"approved", "changes_requested"}:
            raise ValueError("invalid review verdict")
        ocr_ok = any(
            "ocr" in (item.command if isinstance(item, CommandReceipt) else item).lower()
            and (not isinstance(item, CommandReceipt) or item.exit_code == 0)
            for item in receipt.commands
        )
        if ocr_available and (receipt.ocr_status != "passed" or not ocr_ok):
            raise IndependentReviewError("mandatory ocr review evidence missing or failed")
        if not ocr_available and receipt.ocr_status not in {"unavailable", "passed"} and receipt.verdict == "approved":
            raise IndependentReviewError("mandatory ocr review evidence missing or failed")
        must_verify_acceptance = (
            require_acceptance_receipt
            or bool(expected_requirement_ids)
            or receipt.acceptance_receipt is not None
        )
        if must_verify_acceptance and receipt.acceptance_receipt is None:
            raise PrematureCompletionError("review receipt requires a valid acceptance_receipt")
        if receipt.acceptance_receipt is not None:
            RequirementTraceabilityGate.verify_acceptance_receipt(
                receipt.acceptance_receipt,
                repo_root=repo_root,
                expected_requirement_ids=expected_requirement_ids,
            )
        if cycle_tracker:
            cycle_tracker.record_verdict(run_id, candidate_head_sha, receipt.verdict)
        elif receipt.verdict == "changes_requested" and cycle >= max_cycles:
            raise ReviewCycleLimitError("review remediation cycle limit reached")
        return receipt.verdict


def _build_specialist_nodes() -> list[DagNodeSpec]:
    return [
        DagNodeSpec(
            id="cto_strategy",
            subagent="CTO",
            node_summary="CTO cross-workstream strategic alignment and consequence matrix",
            prompt_template="Analyze cross-workstream synergies, long-term goal alignment, and 5-dimension consequence matrix",
            execution_kind="plan",
        ),
        DagNodeSpec(
            id="slice_a",
            subagent="specialist",
            node_summary="implement first slice",
            prompt_template="implement first slice",
            execution_kind="write",
            file_scope=["raven/agent/subagent/**"],
            depends_on=["cto_strategy"],
        ),
        DagNodeSpec(
            id="slice_b",
            subagent="specialist",
            node_summary="implement second slice",
            prompt_template="implement second slice",
            execution_kind="write",
            file_scope=["tests/**"],
            depends_on=["cto_strategy"],
        ),
        DagNodeSpec(
            id="integrate",
            subagent="specialist",
            node_summary="integrate slices",
            prompt_template="integrate slices",
            execution_kind="integrate",
            depends_on=["slice_a", "slice_b"],
        ),
        DagNodeSpec(
            id="review",
            subagent="reviewer",
            node_summary="review candidate",
            prompt_template="review candidate",
            execution_kind="review",
            depends_on=["integrate"],
            routes={"approved": "candidate accepted", "changes_requested": "candidate needs changes"},
        ),
        DagNodeSpec(
            id="remediate",
            subagent="specialist",
            node_summary="remediate review",
            prompt_template="remediate review",
            execution_kind="write",
            depends_on=["review"],
            when="review.changes_requested",
            file_scope=["raven/agent/subagent/**", "tests/**"],
        ),
        DagNodeSpec(
            id="re_review",
            subagent="reviewer",
            node_summary="review remediation",
            prompt_template="review remediation",
            execution_kind="review",
            depends_on=["remediate", "review"],
            when="review.changes_requested",
        ),
    ]


def build_oh_my_raven_playbook() -> Any:
    from raven.playbook.types import PlaybookSpec, Triggers

    return PlaybookSpec(
        name=BRAND_NAME,
        description="oh-my-raven: Deliver a reviewed feature through isolated specialist slices and fail-closed gates.",
        task_summary="deliver reviewed feature via oh-my-raven",
        version=2,
        mode="dag",
        triggers=Triggers(keywords=["oh-my-raven", "feature delivery", "specialist orchestration"]),
        nodes=_build_specialist_nodes(),
    )


def build_autonomous_feature_delivery_playbook() -> Any:
    from raven.playbook.types import PlaybookSpec, Triggers

    return PlaybookSpec(
        name="autonomous-feature-delivery",
        description="Deliver a reviewed feature through isolated specialist slices.",
        task_summary="deliver reviewed feature",
        version=2,
        mode="dag",
        triggers=Triggers(keywords=["feature delivery"]),
        nodes=_build_specialist_nodes(),
    )


_SECRET = re.compile(r"sk-[A-Za-z0-9_-]+|ghp_[A-Za-z0-9]+|Bearer\s+\S+|api[_-]?key\s*[:=]\s*\S+", re.IGNORECASE)
_PINNED_INVARIANT_MARKER = re.compile(r"(\[GLOBAL INVARIANT\]|\[ARCH-DEC-[^\]]*\]|\bPINNED\s*:)", re.IGNORECASE)


def build_context_charter(
    task: str,
    *,
    memories: Sequence[str] = (),
    hot_md_path: Path | None = None,
    everos_fetcher: Callable[[str], Sequence[str]] | None = None,
    pinned_invariants: Sequence[str] = (),
    max_chars: int = 2000,
) -> str:
    items = list(memories)
    if everos_fetcher is not None:
        try:
            fetched = tuple(everos_fetcher(task))
        except Exception as exc:
            logger.debug("Context charter everos_fetcher skipped: {}", exc)
            fetched = ()
        items.extend(fetched)
    if hot_md_path is not None:
        try:
            hot_text = Path(hot_md_path).read_text(encoding="utf-8")
        except OSError as exc:
            logger.debug("Context charter hot_md_path unreadable ({}): {}", hot_md_path, exc)
            hot_text = ""
        if hot_text:
            items.append(hot_text)
    terms = {term.lower() for term in re.findall(r"[A-Za-z0-9_]+", task) if len(term) > 2}
    relevant: list[str] = []
    seen_lines: set[str] = set()

    def _add_item(text: str) -> None:
        cleaned = text.strip()
        if cleaned and cleaned not in seen_lines:
            seen_lines.add(cleaned)
            relevant.append(cleaned)

    for inv in pinned_invariants or ():
        if str(inv).strip():
            _add_item(str(inv))

    for item in items:
        for line in str(item).splitlines():
            if _PINNED_INVARIANT_MARKER.search(line):
                _add_item(line)
        if terms and terms.intersection(re.findall(r"[A-Za-z0-9_]+", str(item).lower())):
            _add_item(str(item))

    if not relevant:
        return ""
    body = _SECRET.sub("[REDACTED]", "\n".join(relevant))[:max_chars]
    return f"[BEGIN UNTRUSTED CONTEXT CHARTER]\n{body}\n[END UNTRUSTED CONTEXT CHARTER]"


build_holistic_charter = build_context_charter


def validate_consequence_matrix(entries: Sequence[ConsequenceEntry]) -> None:
    if not entries:
        raise ValueError("consequence matrix entries cannot be empty")
    for entry in entries:
        if not all((entry.decision, entry.second_order_effect, entry.rollback_plan, entry.blast_radius)):
            raise ValueError(
                "consequence matrix entries require decision, second_order_effect, rollback_plan, and blast_radius"
            )


_SPECIAL_KINDS = ("write", "integrate", "review", "plan")


class RecoveryBlockedError(RuntimeError):
    """The recovery ledger refused a retry; not itself a new attempt."""


class DagSpecialistRuntime:
    """Per-run bridge between the DAG runner and the oh-my-raven specialist contracts.

    Inert unless the graph holds a write, integrate, review, or plan node, or charter_context is configured.
    """

    def __init__(
        self,
        *,
        workdir: str | Path = "",
        run_dir: str | Path | None = None,
        max_review_cycles: int = 3,
        require_skill_evidence: bool = False,
        charter_context: dict[str, Any] | None = None,
    ) -> None:
        self.enabled = False
        self.workdir = str(workdir) if workdir else ""
        self.run_dir: Path | None = Path(run_dir) if run_dir is not None else None
        self.run_id = ""
        self.max_review_cycles = max_review_cycles
        self.require_skill_evidence = bool(require_skill_evidence)
        self.charter_context: dict[str, Any] | None = dict(charter_context) if charter_context else None
        self.base_sha: str | None = None
        self.ledger: DurableRecoveryLedger | None = None
        self.cycle_tracker: ReviewCycleTracker | None = (
            ReviewCycleTracker(Path(run_dir) / "review_cycles.json", max_cycles=max_review_cycles)
            if run_dir is not None
            else None
        )
        self.slice_receipts: dict[str, SliceReceipt] = {}
        self.validated_plans: dict[str, ConsequenceMatrix] = {}
        self.consequence_matrices: dict[str, ConsequenceMatrix] = self.validated_plans
        self.cto_briefs: dict[str, CtoStrategicBrief] = {}
        self.require_cto_alignment: bool = False
        self.by_id: dict[str, Any] = {}
        self.expected_requirement_ids: list[str] | None = None
        self.require_acceptance_receipt: bool = False
        self.node_worktrees: dict[str, Path] = {}
        self.node_backends: dict[str, Any] = {}

    @staticmethod
    def _load_requirements_manifest(candidate: Path) -> list[str]:
        try:
            manifest_data = json.loads(candidate.read_text(encoding="utf-8"))
        except Exception as exc:
            raise PrematureCompletionError(
                f"Invalid requirements_manifest.json at {candidate}: must be JSON object with non-empty 'expected_requirement_ids' list"
            ) from exc
        if not isinstance(manifest_data, dict):
            raise PrematureCompletionError(
                f"Invalid requirements_manifest.json at {candidate}: must be JSON object with non-empty 'expected_requirement_ids' list"
            )
        raw_ids = manifest_data.get("expected_requirement_ids")
        if (
            not isinstance(raw_ids, list)
            or not raw_ids
            or not all(isinstance(x, str) and x.strip() for x in raw_ids)
        ):
            raise PrematureCompletionError(
                f"Invalid requirements_manifest.json at {candidate}: must be JSON object with non-empty 'expected_requirement_ids' list"
            )
        return [x.strip() for x in raw_ids]

    @classmethod
    def from_run(
        cls,
        spec: Any,
        workdir: str,
        store: Any,
        node_backends: dict[str, Any] | None = None,
        *,
        max_review_cycles: int | None = None,
        charter_context: dict[str, Any] | None = None,
    ) -> "DagSpecialistRuntime":
        if max_review_cycles is None and node_backends:
            for n in getattr(spec, "nodes", ()):
                if getattr(n, "execution_kind", None) == "review":
                    mc = getattr(node_backends.get(n.id), "max_review_cycles", None)
                    if isinstance(mc, int):
                        max_review_cycles = mc
                        break
        if charter_context is None and node_backends:
            for backend in node_backends.values():
                ctx = getattr(backend, "charter_context", None)
                if isinstance(ctx, dict) and ctx:
                    charter_context = ctx
                    break
        require_skill_ev = any(
            bool(getattr(b, "require_skill_evidence", False)) for b in (node_backends or {}).values()
        )
        rt = cls(
            workdir=workdir,
            run_dir=Path(store.run_dir),
            max_review_cycles=max_review_cycles if max_review_cycles is not None else 3,
            require_skill_evidence=require_skill_ev,
            charter_context=charter_context,
        )
        rt.node_backends = dict(node_backends or {})
        rt.prepare_run(
            spec,
            workdir,
            store.run_id,
            Path(store.run_dir),
            max_review_cycles=rt.max_review_cycles,
            charter_context=charter_context,
        )
        return rt

    def prepare_run(
        self,
        spec: Any,
        workdir: str,
        run_id: str,
        run_dir_path: Path,
        *,
        max_review_cycles: int | None = None,
        charter_context: dict[str, Any] | None = None,
    ) -> None:
        self.workdir, self.run_id, self.run_dir = workdir, run_id, Path(run_dir_path)
        self.by_id = {n.id: n for n in getattr(spec, "nodes", ())}
        if max_review_cycles is not None:
            self.max_review_cycles = max_review_cycles
        if charter_context is not None:
            self.charter_context = dict(charter_context)
        elif self.charter_context is None:
            charter_file = self.run_dir / "charter_context.json"
            if charter_file.exists():
                try:
                    loaded = json.loads(charter_file.read_text(encoding="utf-8"))
                except (OSError, ValueError) as exc:
                    logger.debug("Skipping invalid charter_context.json at {}: {}", charter_file, exc)
                    loaded = None
                if isinstance(loaded, dict):
                    self.charter_context = loaded
        for candidate_dir in (self.run_dir, Path(workdir) if workdir else None):
            if candidate_dir is None:
                continue
            manifest_file = Path(candidate_dir) / "requirements_manifest.json"
            if manifest_file.exists():
                self.require_acceptance_receipt = True
                self.expected_requirement_ids = self._load_requirements_manifest(manifest_file)
                break
        self.enabled = (
            any(n.execution_kind in _SPECIAL_KINDS for n in spec.nodes)
            or bool(self.charter_context)
            or self.expected_requirement_ids is not None
            or self.require_skill_evidence
            or any(
                bool(getattr(self.node_backends.get(n.id), "require_skill_evidence", False))
                or bool(getattr(self.node_backends.get(n.id), "specialist_definition", None))
                for n in spec.nodes
            )
        )
        if not self.enabled:
            return
        self.ledger = DurableRecoveryLedger(run_dir_path / "recovery_ledger.json")
        self.cycle_tracker = ReviewCycleTracker(
            run_dir_path / "review_cycles.json", max_cycles=self.max_review_cycles
        )
        if (Path(workdir) / ".git").exists():
            self.base_sha = _git(Path(workdir), "rev-parse", "HEAD").stdout.strip()

    def _worktrees_root(self) -> Path:
        root = Path(self.workdir)
        return root.parent / f"{root.name}.worktrees"

    @staticmethod
    def _hash(prompt: str) -> str:
        return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]

    def _resolve_charter_sources(self, node: Any) -> dict[str, Any] | None:
        if self.charter_context:
            return self.charter_context
        backend = self.node_backends.get(getattr(node, "id", ""))
        backend_ctx = getattr(backend, "charter_context", None)
        if isinstance(backend_ctx, dict) and backend_ctx:
            return backend_ctx
        for candidate_dir in (self.run_dir, Path(self.workdir) if self.workdir else None):
            if candidate_dir is None:
                continue
            charter_file = Path(candidate_dir) / "charter_context.json"
            if charter_file.exists():
                try:
                    loaded = json.loads(charter_file.read_text(encoding="utf-8"))
                except (OSError, ValueError) as exc:
                    logger.debug("Skipping invalid charter_context.json at {}: {}", charter_file, exc)
                    loaded = None
                if isinstance(loaded, dict) and loaded:
                    return loaded
        return None

    def _upstream_cto_briefs(self, node: Any) -> list[CtoStrategicBrief]:
        seen: set[str] = set()
        stack = list(getattr(node, "depends_on", ()))
        found_ids: list[str] = []
        while stack:
            dep_id = stack.pop()
            if dep_id in seen:
                continue
            seen.add(dep_id)
            if dep_id in self.cto_briefs:
                found_ids.append(dep_id)
            dep_node = self.by_id.get(dep_id)
            if dep_node is not None:
                stack.extend(getattr(dep_node, "depends_on", ()))
        if not found_ids and self.cto_briefs and getattr(node, "execution_kind", None) in ("write", "integrate", "review"):
            found_ids = list(self.cto_briefs.keys())
        return [self.cto_briefs[nid] for nid in sorted(set(found_ids)) if nid in self.cto_briefs]

    def inject_charter(self, node: Any, prompt: str) -> str:
        out = prompt
        backend = self.node_backends.get(getattr(node, "id", ""))
        spec_def = getattr(backend, "specialist_definition", None) or getattr(node, "specialist_definition", None)
        if spec_def is not None and "[BEGIN SPECIALIST BUNDLE:" not in out:
            out = SkillPackResolver.render_specialist_bundle(spec_def, out)
        if getattr(node, "execution_kind", None) in ("write", "integrate", "review") and "[BEGIN CTO STRATEGIC BRIEF]" not in out:
            briefs = self._upstream_cto_briefs(node)
            if briefs:
                brief_block = "\n\n".join(b.render_guidance_block() for b in briefs)
                out = f"{brief_block}\n\n{out}"
        if "[BEGIN UNTRUSTED CONTEXT CHARTER]" in out:
            return out
        ctx = self._resolve_charter_sources(node)
        if not ctx:
            return out
        memories = ctx.get("memories") or ()
        pinned_invariants = ctx.get("pinned_invariants") or ()
        hot_md_path = ctx.get("hot_md_path")
        if isinstance(hot_md_path, str) and hot_md_path:
            hot_md_path = Path(hot_md_path)
        everos_fetcher = ctx.get("everos_fetcher")
        max_chars = int(ctx.get("max_chars", 2000))
        query = f"{getattr(node, 'node_summary', '')} {prompt}".strip()
        charter = build_context_charter(
            query,
            memories=memories,
            hot_md_path=hot_md_path if isinstance(hot_md_path, Path) else None,
            everos_fetcher=everos_fetcher if callable(everos_fetcher) else None,
            pinned_invariants=pinned_invariants,
            max_chars=max_chars,
        )
        if not charter:
            return out
        return f"{charter}\n\n{out}"

    render_prompt_with_charter = inject_charter

    def before_attempt(self, node: Any, prompt: str) -> tuple[Path, dict[str, Any]]:
        base = Path(self.workdir)
        charter_prompt = self.inject_charter(node, prompt)
        meta: dict[str, Any] = {}
        if charter_prompt != prompt:
            meta["charter"] = charter_prompt
        if not self.enabled:
            return base, meta
        if node.execution_kind == "write":
            stack = list(getattr(node, "depends_on", ()))
            seen_deps: set[str] = set()
            while stack:
                dep_id = stack.pop()
                if dep_id in seen_deps:
                    continue
                seen_deps.add(dep_id)
                dep_node = self.by_id.get(dep_id)
                if dep_node is None:
                    continue
                if getattr(dep_node, "execution_kind", None) == "plan":
                    if dep_id not in self.validated_plans:
                        raise DagValidationError(
                            f"Write node '{node.id}' cannot execute before upstream plan node '{dep_id}' has a validated ConsequenceMatrix"
                        )
                    if (CtoAlignmentGate.is_cto_node(dep_node) or self.require_cto_alignment) and dep_id not in self.cto_briefs:
                        raise DagValidationError(
                            f"Write node '{node.id}' cannot execute before upstream CTO plan node '{dep_id}' has a validated CtoStrategicBrief"
                        )
                stack.extend(getattr(dep_node, "depends_on", ()))
            wt_path = self.node_worktrees.get(node.id) or self._worktrees_root() / f"{self.run_id}-{node.id}"
            rec = self.ledger.reconcile(
                run_id=self.run_id,
                node_id=node.id,
                input_hash=self._hash(prompt),
                worktree_path=wt_path if wt_path.exists() else None,
                base_sha=self.base_sha,
            )
            if rec["next_action"] in ("quarantine", "escalate"):
                raise RecoveryBlockedError(f"recovery ledger blocked retry for {node.id}: {rec['next_action']}")
            if not self.base_sha:
                return base, meta
            if not wt_path.exists():
                wt_path = WorktreeManager().provision_worktree(
                    base, self._worktrees_root(), self.run_id, node.id, self.base_sha
                )
            self.node_worktrees[node.id] = wt_path
            return wt_path, {**meta, "base_sha": self.base_sha}
        if node.execution_kind == "integrate":
            receipts = [self.slice_receipts[d] for d in node.depends_on if d in self.slice_receipts]
            return base, {**meta, "base_sha": self.base_sha, "slice_receipts": receipts}
        return base, meta

    @staticmethod
    def _authors(
        node: Any, by_id: dict[str, Any], node_backends: dict[str, Any]
    ) -> tuple[list[ExecutionIdentity], list[str]]:
        seen: set[str] = set()
        stack = list(node.depends_on)
        identities: list[ExecutionIdentity] = []
        writers: list[str] = []
        while stack:
            dep = stack.pop()
            if dep in seen or dep not in by_id:
                continue
            seen.add(dep)
            up = by_id[dep]
            stack.extend(up.depends_on)
            if up.execution_kind in ("write", "integrate"):
                writers.append(dep)
                backend = node_backends.get(dep)
                identities.append(
                    getattr(backend, "execution_identity", None) or ExecutionIdentity(agent_name=up.subagent)
                )
        return identities, writers

    @staticmethod
    def _receipt_from(agent_backend: Any, result: str) -> ReviewReceipt | None:
        found = getattr(agent_backend, "last_review_receipt", None)
        if isinstance(found, ReviewReceipt):
            return found
        try:
            data = json.loads(result)
        except (TypeError, ValueError):
            return None
        if not isinstance(data, dict) or "verdict" not in data or "reviewed_head_sha" not in data:
            return None
        commands = [CommandReceipt(**c) if isinstance(c, dict) else c for c in data.get("commands", [])]
        acc_raw = data.get("acceptance_receipt")
        acc = (
            acc_raw
            if isinstance(acc_raw, AcceptanceReceipt)
            else AcceptanceReceipt.from_dict(acc_raw)
            if isinstance(acc_raw, dict)
            else None
        )
        return ReviewReceipt(
            verdict=data["verdict"],
            reviewed_head_sha=data["reviewed_head_sha"],
            findings=list(data.get("findings", [])),
            commands=commands,
            ocr_status=data.get("ocr_status", "unavailable"),
            acceptance_receipt=acc,
        )

    @staticmethod
    def _matrix_from(agent_backend: Any, result: str) -> ConsequenceMatrix | None:
        found = getattr(agent_backend, "last_consequence_matrix", None)
        if found is not None:
            try:
                return ConsequenceMatrix.from_payload(found).validate()
            except Exception:
                return None
        try:
            return ConsequenceMatrix.from_payload(result).validate()
        except Exception:
            return None

    @staticmethod
    def _cto_brief_from(
        agent_backend: Any,
        result: str,
        *,
        require_cto_alignment: bool = True,
    ) -> CtoStrategicBrief | None:
        found = getattr(agent_backend, "last_cto_strategic_brief", None) or getattr(
            agent_backend, "last_cto_brief", None
        )
        if found is not None:
            try:
                return CtoAlignmentGate.verify_cto_brief(found, require_cto_alignment=require_cto_alignment)
            except Exception:
                return None
        try:
            return CtoAlignmentGate.verify_cto_brief(result, require_cto_alignment=require_cto_alignment)
        except Exception:
            return None

    def after_success(
        self,
        node: Any,
        result: str,
        agent_backend: Any,
        by_id: dict[str, Any],
        node_backends: dict[str, Any],
    ) -> None:
        if not self.enabled:
            return
        evidence_info = SkillAndToolReceiptGate.verify_specialist_execution(
            node,
            agent_backend,
            result,
            require_skill_evidence=(
                bool(getattr(self, "require_skill_evidence", False))
                or bool(getattr(agent_backend, "require_skill_evidence", False))
                or bool(getattr(node, "require_skill_evidence", False))
            ),
        )
        ev_dict = evidence_info if isinstance(evidence_info, dict) else {}
        if node.execution_kind == "write" and node.id in self.node_worktrees:
            skill_ev: dict[str, CommandReceipt] = dict(ev_dict.get("skill_evidence") or {})
            extra_tests = tuple(skill_ev.values())
            self.slice_receipts[node.id] = WorktreeManager().build_slice_receipt(
                self.node_worktrees[node.id],
                node.id,
                self.base_sha,
                node.file_scope,
                tests=extra_tests,
                invoked_tools=tuple(ev_dict.get("invoked_tools") or ()),
                invoked_mcps=tuple(ev_dict.get("invoked_mcps") or ()),
                skill_packs_injected=tuple(ev_dict.get("skill_packs_injected") or ()),
                skill_evidence=skill_ev,
            )
        elif node.execution_kind == "integrate":
            integ_sha = ""
            if result:
                try:
                    parsed_int = json.loads(result)
                except (TypeError, ValueError):
                    parsed_int = None
                if isinstance(parsed_int, dict) and parsed_int.get("integrated_head_sha"):
                    integ_sha = str(parsed_int["integrated_head_sha"]).strip()
            if not integ_sha and self.workdir and (Path(self.workdir) / ".git").exists():
                integ_sha = _git(Path(self.workdir), "rev-parse", "HEAD").stdout.strip()
            if integ_sha and (not self.base_sha or integ_sha != self.base_sha):
                skill_ev_int: dict[str, CommandReceipt] = dict(ev_dict.get("skill_evidence") or {})
                self.slice_receipts[node.id] = SliceReceipt(
                    node_id=node.id,
                    head_sha=integ_sha,
                    base_sha=self.base_sha or "",
                    worktree_path=str(self.workdir),
                    changed_paths=(),
                    committed=True,
                    tests=tuple(skill_ev_int.values()),
                    invoked_tools=tuple(ev_dict.get("invoked_tools") or ()),
                    invoked_mcps=tuple(ev_dict.get("invoked_mcps") or ()),
                    skill_packs_injected=tuple(ev_dict.get("skill_packs_injected") or ()),
                    skill_evidence=skill_ev_int,
                )
        elif node.execution_kind == "plan":
            is_cto = (
                CtoAlignmentGate.is_cto_node(node)
                or self.require_cto_alignment
                or bool(getattr(agent_backend, "require_cto_alignment", False))
            )
            if is_cto:
                brief = self._cto_brief_from(agent_backend, result, require_cto_alignment=True)
                if brief is None:
                    raise DagValidationError(
                        f"CTO plan node '{node.id}' completed without a valid CtoStrategicBrief (requires workstream_synergies, long_term_goal_alignment, and 5-dimension ConsequenceMatrix)"
                    )
                self.cto_briefs[node.id] = brief
                self.validated_plans[node.id] = brief.consequence_matrix
            else:
                brief = self._cto_brief_from(agent_backend, result, require_cto_alignment=True)
                if brief is not None:
                    self.cto_briefs[node.id] = brief
                    self.validated_plans[node.id] = brief.consequence_matrix
                else:
                    matrix = self._matrix_from(agent_backend, result)
                    if matrix is None:
                        raise DagValidationError(f"Plan node '{node.id}' completed without a valid ConsequenceMatrix")
                    self.validated_plans[node.id] = matrix
        elif node.execution_kind == "review":
            receipt = self._receipt_from(agent_backend, result)
            if receipt is None:
                raise DagValidationError(f"Review node '{node.id}' completed without a valid ReviewReceipt")
            backend_max_cycles = getattr(agent_backend, "max_review_cycles", None)
            if isinstance(backend_max_cycles, int) and self.cycle_tracker is not None:
                self.cycle_tracker.max_cycles = backend_max_cycles
            authors, writers = self._authors(node, by_id, node_backends)
            direct_heads = [
                self.slice_receipts[d].head_sha
                for d in getattr(node, "depends_on", ())
                if d in self.slice_receipts
            ]
            heads = direct_heads or [self.slice_receipts[w].head_sha for w in writers if w in self.slice_receipts]
            manifest_expected_ids = self.expected_requirement_ids
            if manifest_expected_ids is None:
                for candidate_dir in (self.run_dir, Path(self.workdir) if self.workdir else None):
                    if candidate_dir is None:
                        continue
                    manifest_file = Path(candidate_dir) / "requirements_manifest.json"
                    if manifest_file.exists():
                        self.require_acceptance_receipt = True
                        manifest_expected_ids = self._load_requirements_manifest(manifest_file)
                        self.expected_requirement_ids = manifest_expected_ids
                        break
            backend_expected_ids = getattr(agent_backend, "expected_requirement_ids", None)
            if manifest_expected_ids is not None:
                if backend_expected_ids is not None and list(backend_expected_ids) != list(manifest_expected_ids):
                    raise PrematureCompletionError(
                        "Backend expected_requirement_ids cannot override or narrow requirements_manifest.json"
                    )
                expected_ids = manifest_expected_ids
            else:
                expected_ids = backend_expected_ids
            require_acc = (
                receipt.verdict == "approved"
                and (
                    self.require_acceptance_receipt
                    or bool(getattr(agent_backend, "require_acceptance_receipt", False))
                    or expected_ids is not None
                )
            ) or receipt.acceptance_receipt is not None
            ReviewGateValidator().verify_review_receipt(
                receipt,
                candidate_head_sha=heads[0] if direct_heads else (heads[-1] if heads else (self.base_sha or receipt.reviewed_head_sha)),
                reviewer_backend=getattr(agent_backend, "execution_identity", None)
                or ExecutionIdentity(agent_name=node.subagent),
                author_backends=authors,
                max_cycles=backend_max_cycles if isinstance(backend_max_cycles, int) else self.max_review_cycles,
                cycle_tracker=self.cycle_tracker,
                run_id=self.run_id,
                ocr_available=bool(getattr(agent_backend, "ocr_available", False)),
                require_acceptance_receipt=require_acc,
                expected_requirement_ids=expected_ids if require_acc else None,
                repo_root=Path(self.workdir) if self.workdir else None,
            )

    def on_failure(self, node: Any, prompt: str, exc: BaseException) -> None:
        if not self.enabled or isinstance(exc, (asyncio.CancelledError, RecoveryBlockedError)):
            return
        if self.ledger is not None:
            dirty_paths: list[str] = []
            for r in getattr(exc, "attempt_receipts", ()) or ():
                if isinstance(r, dict) and r.get("dirty_paths"):
                    dirty_paths.extend(str(p) for p in r["dirty_paths"])
            wt_path = self.node_worktrees.get(node.id)
            if not dirty_paths and wt_path is not None and wt_path.exists():
                try:
                    dirty_paths = _nul_paths(
                        _git(wt_path, "status", "--porcelain=v1", "-z", "-uall", check=False).stdout,
                        status=True,
                    )
                except Exception:
                    dirty_paths = []
            self.ledger.record_attempt(
                run_id=self.run_id,
                node_id=node.id,
                input_hash=self._hash(prompt),
                backend=node.subagent,
                failure_class=type(exc).__name__,
                worktree_path=str(wt_path or ""),
                dirty_paths=dirty_paths,
            )

    async def persist(self, store: Any) -> None:
        if self.enabled and self.ledger is not None and self.ledger.ledger_path.exists():
            await store.write_text(
                store.run_dir + "/recovery_ledger.json", self.ledger.ledger_path.read_text(encoding="utf-8")
            )


QueueStatus = Literal["enqueued", "routed", "decomposing", "running", "completed", "incomplete", "failed"]


@dataclass(frozen=True)
class OmrQueueEntry:
    """Durable record in ``queue_ledger.jsonl`` tracking top-level and decomposed child tasks."""

    seq_id: int
    task_id: str
    parent_task_id: str | None
    prompt: str
    assigned_specialist: str = ""
    assigned_harness: str = ""
    wave: int = 0
    status: QueueStatus = "enqueued"
    timestamp: str = ""
    error: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "seq_id": int(self.seq_id),
            "task_id": str(self.task_id),
            "parent_task_id": self.parent_task_id,
            "prompt": _SECRET.sub("[REDACTED]", str(self.prompt)),
            "assigned_specialist": str(self.assigned_specialist),
            "assigned_harness": str(self.assigned_harness),
            "wave": int(self.wave),
            "status": str(self.status),
            "timestamp": str(self.timestamp),
            "error": str(self.error),
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "OmrQueueEntry":
        return cls(
            seq_id=int(data.get("seq_id", 0)),
            task_id=str(data.get("task_id", "")),
            parent_task_id=data.get("parent_task_id"),
            prompt=str(data.get("prompt", "")),
            assigned_specialist=str(data.get("assigned_specialist", "")),
            assigned_harness=str(data.get("assigned_harness", "")),
            wave=int(data.get("wave", 0)),
            status=data.get("status", "enqueued"),
            timestamp=str(data.get("timestamp", "")),
            error=str(data.get("error", "")),
        )


class OmrTaskQueue:
    """Durable JSONL-backed task queue (``queue_ledger.jsonl``) with atomic file-locked appends."""

    def __init__(self, ledger_path: Path | str) -> None:
        self.ledger_path = Path(ledger_path)

    @property
    def lock_path(self) -> Path:
        return self.ledger_path.with_name(f"{self.ledger_path.name}.lock")

    def _acquire_file_lock(self, lock_fh: Any) -> None:
        if os.name == "nt":
            import msvcrt

            lock_fh.seek(0, os.SEEK_END)
            if lock_fh.tell() == 0:
                lock_fh.write(b"\0")
                lock_fh.flush()
            lock_fh.seek(0)
            msvcrt.locking(lock_fh.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)

    def _release_file_lock(self, lock_fh: Any) -> None:
        if os.name == "nt":
            import msvcrt

            lock_fh.seek(0)
            msvcrt.locking(lock_fh.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    def _now_iso(self) -> str:
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    def _append_entry_unlocked(self, entry: OmrQueueEntry) -> OmrQueueEntry:
        line = (json.dumps(entry.to_dict(), ensure_ascii=True, separators=(",", ":")) + "\n").encode("utf-8")
        with self.ledger_path.open("ab") as fh:
            fh.write(line)
            fh.flush()
        return entry

    def _read_entries_unlocked(self) -> list[OmrQueueEntry]:
        if not self.ledger_path.exists():
            return []
        entries: list[OmrQueueEntry] = []
        for raw_line in self.ledger_path.read_text(encoding="utf-8", errors="replace").splitlines():
            stripped = raw_line.strip()
            if not stripped:
                continue
            try:
                data = json.loads(stripped)
            except ValueError:
                data = None
            if isinstance(data, dict):
                entries.append(OmrQueueEntry.from_dict(data))
        return entries

    def entries(self) -> list[OmrQueueEntry]:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_fh:
            self._acquire_file_lock(lock_fh)
            try:
                return self._read_entries_unlocked()
            finally:
                self._release_file_lock(lock_fh)

    def latest_by_task(self) -> dict[str, OmrQueueEntry]:
        latest: dict[str, OmrQueueEntry] = {}
        for item in self.entries():
            latest[item.task_id] = item
        return latest

    def enqueue(
        self,
        task_id: str,
        prompt: str,
        *,
        parent_task_id: str | None = None,
        assigned_specialist: str = "",
        assigned_harness: str = "",
        wave: int = 0,
        status: QueueStatus = "enqueued",
    ) -> OmrQueueEntry:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_fh:
            self._acquire_file_lock(lock_fh)
            try:
                existing = self._read_entries_unlocked()
                next_seq = (max((e.seq_id for e in existing), default=0)) + 1
                entry = OmrQueueEntry(
                    seq_id=next_seq,
                    task_id=str(task_id),
                    parent_task_id=parent_task_id,
                    prompt=str(prompt),
                    assigned_specialist=str(assigned_specialist),
                    assigned_harness=str(assigned_harness),
                    wave=int(wave),
                    status=status,
                    timestamp=self._now_iso(),
                )
                return self._append_entry_unlocked(entry)
            finally:
                self._release_file_lock(lock_fh)

    def transition(
        self,
        task_id: str,
        status: QueueStatus,
        *,
        assigned_specialist: str | None = None,
        assigned_harness: str | None = None,
        wave: int | None = None,
        error: str = "",
    ) -> OmrQueueEntry:
        self.ledger_path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_fh:
            self._acquire_file_lock(lock_fh)
            try:
                existing = self._read_entries_unlocked()
                next_seq = (max((e.seq_id for e in existing), default=0)) + 1
                prev = next((e for e in reversed(existing) if e.task_id == task_id), None)
                entry = OmrQueueEntry(
                    seq_id=next_seq,
                    task_id=str(task_id),
                    parent_task_id=prev.parent_task_id if prev is not None else None,
                    prompt=prev.prompt if prev is not None else "",
                    assigned_specialist=(
                        str(assigned_specialist)
                        if assigned_specialist is not None
                        else (prev.assigned_specialist if prev is not None else "")
                    ),
                    assigned_harness=(
                        str(assigned_harness)
                        if assigned_harness is not None
                        else (prev.assigned_harness if prev is not None else "")
                    ),
                    wave=int(wave) if wave is not None else (prev.wave if prev is not None else 0),
                    status=status,
                    timestamp=self._now_iso(),
                    error=str(error),
                )
                return self._append_entry_unlocked(entry)
            finally:
                self._release_file_lock(lock_fh)


@dataclass(frozen=True)
class DecompositionPlan:
    """Validated dynamic task decomposition produced by the Lead Specialist (Manager)."""

    dag_spec: Any
    expected_requirement_ids: tuple[str, ...]
    node_waves: dict[str, int]
    manifest_path: Path | None = None


class DynamicDecompositionGate:
    """Validates a Lead Specialist's dynamic DAG decomposition and enqueues child tasks into ``OmrTaskQueue``."""

    @staticmethod
    def compute_topological_waves(nodes: Sequence[DagNodeSpec]) -> dict[str, int]:
        by_id = {n.id: n for n in nodes}
        memo: dict[str, int] = {}
        visiting: set[str] = set()

        def _wave(nid: str) -> int:
            if nid in memo:
                return memo[nid]
            if nid in visiting:
                raise DagValidationError(f"cycle detected involving node {nid!r}")
            visiting.add(nid)
            node = by_id.get(nid)
            deps = [d for d in (getattr(node, "depends_on", ()) if node is not None else ()) if d in by_id]
            w = 0 if not deps else 1 + max(_wave(d) for d in deps)
            visiting.remove(nid)
            memo[nid] = w
            return w

        for n in nodes:
            _wave(n.id)
        return memo

    @classmethod
    def parse_decomposition(cls, plan_payload: Any, *, default_summary: str = "decomposed oh-my-raven workflow") -> tuple[Any, list[str]]:
        from raven.agent.subagent.dag_graph import SubAgentDagSpec, parse_dag_spec

        if isinstance(plan_payload, SubAgentDagSpec):
            return plan_payload, []
        if isinstance(plan_payload, str):
            try:
                loaded_payload = json.loads(plan_payload)
            except ValueError as exc:
                raise DagValidationError(f"Decomposition payload is not valid JSON: {exc}") from exc
        elif isinstance(plan_payload, dict):
            loaded_payload = dict(plan_payload)
        else:
            raise DagValidationError(f"Unsupported decomposition payload type: {type(plan_payload).__name__}")

        if not isinstance(loaded_payload, dict):
            raise DagValidationError("Decomposition payload must be a JSON object")

        read_field = getattr(loaded_payload, "get")
        req_ids_raw = read_field("expected_requirement_ids")
        manifest_obj = read_field("requirements_manifest")
        if req_ids_raw is None and isinstance(manifest_obj, dict):
            req_ids_raw = manifest_obj.get("expected_requirement_ids")
        expected_ids = [str(x).strip() for x in (req_ids_raw or ()) if str(x).strip()]

        nested_dag = read_field("dag_spec")
        confirm_val = read_field("confirm")
        dag_raw = nested_dag if isinstance(nested_dag, dict) else {
            "task_summary": read_field("task_summary") or default_summary,
            "nodes": read_field("nodes") or [],
            **({"confirm": confirm_val} if confirm_val is not None else {}),
        }
        dag_spec = parse_dag_spec(dag_raw)
        return dag_spec, expected_ids

    @classmethod
    def validate_and_enqueue(
        cls,
        plan_payload: Any,
        queue: OmrTaskQueue,
        parent_task_id: str,
        run_dir: Path | str | None = None,
        *,
        workdir: Path | str | None = None,
        require_cto_and_review: bool = True,
    ) -> DecompositionPlan:
        from raven.agent.subagent.dag_graph import validate_and_order

        dag_spec, expected_ids = cls.parse_decomposition(plan_payload)
        roots = (str(workdir),) if workdir else ()
        validate_and_order(dag_spec, roots)

        struct_errors = ReviewGateValidator().validate_workflow_structure(dag_spec.nodes)
        if struct_errors:
            raise DagValidationError("; ".join(struct_errors))

        if require_cto_and_review:
            has_cto = any(CtoAlignmentGate.is_cto_node(n) for n in dag_spec.nodes)
            has_integrate = any(n.execution_kind == "integrate" for n in dag_spec.nodes)
            has_review = any(n.execution_kind == "review" for n in dag_spec.nodes)
            if not (has_cto and has_integrate and has_review):
                missing_parts = [
                    name
                    for name, ok in (("cto/plan", has_cto), ("integrate", has_integrate), ("review", has_review))
                    if not ok
                ]
                raise DagValidationError(
                    f"Decomposed workflow missing mandatory oh-my-raven governance nodes: {', '.join(missing_parts)}"
                )

        manifest_path: Path | None = None
        if expected_ids and run_dir is not None:
            mpath = Path(run_dir) / "requirements_manifest.json"
            _write_json(mpath, {"expected_requirement_ids": expected_ids})
            manifest_path = mpath

        waves = cls.compute_topological_waves(dag_spec.nodes)
        ordered_nodes = sorted(dag_spec.nodes, key=lambda n: (waves.get(n.id, 0), n.id))
        for node in ordered_nodes:
            queue.enqueue(
                node.id,
                node.prompt_template,
                parent_task_id=parent_task_id,
                assigned_specialist=node.subagent,
                wave=waves.get(node.id, 0),
                status="enqueued",
            )

        return DecompositionPlan(
            dag_spec=dag_spec,
            expected_requirement_ids=tuple(expected_ids),
            node_waves=waves,
            manifest_path=manifest_path,
        )


@dataclass
class OmrDispatchResult:
    """End-to-end result of submitting a top-level complex task to ``OmrDispatcher``."""

    task_id: str
    lead_specialist: str
    cto_specialist: str
    cto_brief: CtoStrategicBrief | None
    decomposition_plan: DecompositionPlan
    dag_result: Any
    queue_entries: list[OmrQueueEntry]
    integrated_head_sha: str = ""


class OmrDispatcher:
    """Top-level oh-my-raven intake dispatcher: enqueues raw tasks, routes to CTO + Manager, decomposes, and runs the DAG."""

    def __init__(
        self,
        *,
        table: Any,
        queue: OmrTaskQueue,
        workdir: Path | str,
        run_root: Path | str,
        nodes_root: Path | str,
        history_root: Path | str,
        intake_router: Callable[[list[tuple[str, str]], str, str], Any] | None = None,
        charter_context: dict[str, Any] | None = None,
        gate_policy_path: Path | str | None = None,
    ) -> None:
        self.table = table
        self.queue = queue
        self.workdir = Path(workdir)
        self.run_root = Path(run_root)
        self.nodes_root = Path(nodes_root)
        self.history_root = Path(history_root)
        self.intake_router = intake_router
        self.charter_context = dict(charter_context) if charter_context else None
        self.gate_policy = GatePolicy.load_default(gate_policy_path)
        self.gate_log_path = self.run_root / "gate_log.json"
        self.lessons_store = LessonsStore(self.history_root / "lessons.jsonl")

    def onboard_harness(
        self,
        name: str,
        backend: Any,
        weight: float = 1.0,
        max_failovers: int | None = None,
    ) -> int:
        """Add a runtime harness to every routed specialist without profile setup."""
        del weight  # Reserved for table implementations with weighted pools.
        backends = getattr(self.table, "_backends", None) or getattr(self.table, "_b", {})
        updated = 0
        for routed in backends.values():
            if not isinstance(routed, RoutingBackend):
                continue
            if any(target.name == name for target in routed._targets):
                continue
            routed._targets.append(RouteTarget(name, name, backend))
            if max_failovers is not None:
                routed._max_failovers = max(0, int(max_failovers))
                routed._max_attempts = max(routed._max_attempts, 1 + routed._max_failovers)
            updated += 1
        self.decide_gate("technical_decision", f"onboard_harness:{name}")
        return updated

    def onboard_specialist(
        self,
        name: str,
        targets: list[RouteTarget] | Any,
        max_failovers: int = 2,
        max_attempts: int | None = None,
        replace: bool = False,
    ) -> RoutingBackend:
        """Register a profile-free specialist over an existing runtime harness pool."""
        raw_targets = list(targets) if isinstance(targets, (list, tuple)) else [RouteTarget(name, name, targets)]
        route_targets: list[RouteTarget] = []
        seen_incoming: set[str] = set()
        for target in raw_targets:
            if target.name not in seen_incoming:
                seen_incoming.add(target.name)
                route_targets.append(target)
        if not route_targets:
            raise ValueError("A dynamic specialist requires at least one harness target")
        existing = None
        getter = getattr(self.table, "backend", None)
        if callable(getter):
            try:
                candidate = getter(name)
            except (KeyError, ValueError, LookupError):
                candidate = None
            if isinstance(candidate, RoutingBackend):
                existing = candidate
        if existing is not None and not replace:
            target_names = {target.name for target in existing._targets}
            for target in route_targets:
                if target.name not in target_names:
                    target_names.add(target.name)
                    existing._targets.append(target)
            self.decide_gate("technical_decision", f"onboard_specialist:{name}")
            return existing
        instances = next(
            (routed._instances for routed in getattr(self.table, "_backends", {}).values() if isinstance(routed, RoutingBackend)),
            None,
        )
        if instances is None:
            instances = next(
                (routed._instances for routed in getattr(self.table, "_b", {}).values() if isinstance(routed, RoutingBackend)),
                None,
            )
        if instances is None:
            instances = type("_RuntimeInstances", (), {"list_instances": lambda self, kind=None: []})()
        routed = RoutingBackend(
            name,
            SpecialistPlaceholderBackend(name),
            route_targets,
            instances=instances,
            max_failovers=max_failovers,
            max_attempts=max_attempts,
            specialist_definition=SpecialistDefinition(name=name),
            is_dynamic_specialist=True,
        )
        table_backends = getattr(self.table, "_backends", None)
        if isinstance(table_backends, dict):
            table_backends[name] = routed
            rows = getattr(self.table, "_rows", None)
            order = getattr(self.table, "_order", None)
            if isinstance(rows, dict):
                rows[name] = type("DynamicSpecialistRow", (), {
                    "name": name, "description": name, "kind": "specialist", "enabled": True,
                    "hidden": False, "is_harness": False,
                })()
                if isinstance(order, list) and name not in order:
                    order.append(name)
        else:
            simple_backends = getattr(self.table, "_b", None)
            if isinstance(simple_backends, dict):
                simple_backends[name] = routed
            elif callable(getattr(self.table, "onboard", None)):
                self.table.onboard(name, backend=routed)
            else:
                raise TypeError("table does not support runtime registration")
        self.decide_gate("technical_decision", f"onboard_specialist:{name}")
        return routed

    def triage_task(self, prompt: str, file_count_hint: int | None = None) -> TriageDecision:
        return classify_triage_tier(prompt, file_count_hint)

    def record_lesson(self, trigger: str, rule: str, source_task_id: str) -> None:
        self.lessons_store.record_lesson(trigger, rule, source_task_id)

    def run_council_consensus(self, task_id: str, votes: list[dict[str, Any]]) -> dict[str, Any]:
        verdict = evaluate_council_consensus(votes)
        payload = verdict.model_dump()
        receipt_path = self.run_root / "council" / f"{task_id}.json"
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        _write_json(receipt_path, payload)
        if verdict.merged_gaps:
            self.decide_gate("plan_gap_fill", f"council_gaps:{task_id}:{len(verdict.merged_gaps)}")
            for gap in verdict.merged_gaps:
                self.lessons_store.record_lesson("council_gap", gap, task_id)
        return payload

    def execute_external_action(self, gate: str, detail: str, action_fn: Callable[[], Any]) -> Any:
        self.require_gate(gate, detail)
        return action_fn()

    def git_push(self, detail: str, action_fn: Callable[[], Any]) -> Any:
        return self.execute_external_action("push", detail, action_fn)

    def publish_pr(self, detail: str, action_fn: Callable[[], Any]) -> Any:
        return self.execute_external_action("publish", detail, action_fn)

    def deploy(self, detail: str, action_fn: Callable[[], Any]) -> Any:
        return self.execute_external_action("deploy", detail, action_fn)

    def run_ambiguity_experiment(self, task_id: str, question: str, probe_fn: Callable[[], dict[str, Any]] | None = None) -> dict[str, Any]:
        if probe_fn is not None:
            finding = probe_fn()
            resolved = bool(finding.get("resolved"))
            receipt = {"task_id": task_id, "question": question, "resolved": resolved, "finding": finding, "verified": True, "exit_code": 0}
            receipt_path = self.run_root / "experiments" / f"{task_id}.json"
            receipt_path.parent.mkdir(parents=True, exist_ok=True)
            _write_json(receipt_path, receipt)
            if resolved:
                self.decide_gate("technical_decision", f"experiment_resolved:{task_id}")
                return {"proceed": True, "resolved_by": "experiment", "receipt_path": str(receipt_path)}
        return {"proceed": self.decide_gate("ambiguous_product_intent", question)}

    async def submit_trivial_task(self, task_id: str, prompt: str, specialist: str | None = None) -> Any:
        chosen = specialist or await self.route_intake_specialist(prompt, default="Manager")
        try:
            result = await self.dispatch_specialist_node(task_id, chosen, prompt)
            if self.completion_status(str(result)) == "completed":
                return {"status": "completed", "triage_tier": "trivial", "escalated": False, "result": result}
        except BaseException:
            pass
        decision = escalate_triage_tier("trivial", "trivial specialist run failed or was incomplete")
        self.decide_gate("technical_decision", f"triage_escalation:trivial->{decision.tier}")
        result = await self.submit_task(task_id, prompt, lead_specialist=chosen)
        status = self.queue.latest_by_task()[task_id].status
        return {"status": status, "triage_tier": decision.tier, "escalated": True, "result": result}

    def completion_status(self, result: str) -> QueueStatus:
        """Return completed only when result contains evidence we can verify."""
        return "completed" if has_verifiable_evidence(result, self.workdir) else "incomplete"

    def decide_gate(self, gate: str, detail: str = "") -> bool:
        """Record a gate decision; external/irreversible callers must use ``require_gate``."""
        proceed = self.gate_policy.decide(gate, detail)
        if self.gate_policy.log:
            self.gate_log_path.parent.mkdir(parents=True, exist_ok=True)
            self.gate_policy.persist_log(self.gate_log_path)
        return proceed

    def require_gate(self, gate: str, detail: str = "") -> bool:
        """Require approval before push, publish, deploy, spend, or delete_remote actions."""
        if not self.decide_gate(gate, detail):
            raise PermissionError(f"Gate {gate!r} requires human approval ({detail})")
        return True

    async def route_intake_specialist(self, prompt: str, *, default: str = "Manager") -> str:
        menu = [(row.name, row.description) for row in self.table.enabled() if not row.is_harness or row.kind == "specialist"]
        if not menu:
            menu = [(row.name, row.description) for row in self.table.enabled()]
        if self.intake_router is not None and menu:
            ans = self.intake_router(menu, prompt, default)
            if asyncio.iscoroutine(ans) or isinstance(ans, asyncio.Future):
                ans = await ans
            if isinstance(ans, str) and any(name == ans for name, _ in menu):
                return ans
        self.decide_gate("specialist_fallback", f"default={default}")
        for name, _ in menu:
            if name.lower() == default.lower():
                return name
        return default

    def _build_framed_prompt(self, prompt: str) -> str:
        if not self.charter_context:
            return prompt
        hot_md = self.charter_context.get("hot_md_path")
        charter_block = build_context_charter(
            prompt,
            memories=self.charter_context.get("memories") or (),
            hot_md_path=Path(hot_md) if hot_md else None,
            everos_fetcher=self.charter_context.get("everos_fetcher"),
            pinned_invariants=self.charter_context.get("pinned_invariants") or (),
            max_chars=int(self.charter_context.get("max_chars", 2000)),
        )
        return f"{charter_block}\n\n{prompt}" if charter_block else prompt

    def _cleanup_run_worktrees(self, effective_run_id: str) -> None:
        if not (self.workdir / ".git").exists():
            return
        wt_root = self.workdir.parent / f"{self.workdir.name}.worktrees"
        if not wt_root.exists():
            return
        for wt_child in list(wt_root.iterdir()):
            if wt_child.is_dir() and wt_child.name.startswith(f"{effective_run_id}-"):
                _git(self.workdir, "worktree", "remove", "--force", str(wt_child), check=False)
        _git(self.workdir, "worktree", "prune", check=False)
        if wt_root.exists() and not any(wt_root.iterdir()):
            shutil.rmtree(wt_root, ignore_errors=True)

    @staticmethod
    async def _default_dag_judge(
        *,
        node: DagNodeSpec,
        store: Any,
        output: str,
        error: str,
        crashed: bool,
        output_limited: bool = False,
    ) -> Any:
        from raven.agent.subagent.dag_verdict import Verdict

        if crashed:
            return Verdict(accomplished=False, what_is_missing=error or "node crashed")
        if node.routes:
            selected: list[str] = []
            try:
                parsed = json.loads(output)
            except (TypeError, ValueError):
                parsed = None
            if isinstance(parsed, dict) and parsed.get("verdict") in node.routes:
                selected = [str(parsed["verdict"])]
            if not selected:
                for rname in node.routes:
                    if rname in output:
                        selected = [rname]
                        break
            if not selected:
                selected = [next(iter(node.routes))]
            return Verdict(accomplished=True, routes=selected, route_evidence=output[:200])
        return Verdict(accomplished=True)

    async def _execute_decomposed_dag(
        self,
        plan: DecompositionPlan,
        *,
        effective_run_id: str,
        session_key: str | None,
        sandbox_factory: Callable[[Path], Any] | None,
        judge_node: Callable[..., Any] | None,
    ) -> Any:
        from raven.agent.subagent.dag_runner import run_dag
        from raven.agent.subagent.prompt_backend import LocalFileBackend

        node_waves = plan.node_waves
        by_id = {n.id: n for n in plan.dag_spec.nodes}
        node_outputs: dict[str, str] = {}
        for node in plan.dag_spec.nodes:
            self.queue.transition(
                node.id,
                "routed",
                assigned_specialist=node.subagent,
                wave=node_waves.get(node.id, 0),
            )

        def _resolve_from_table(node: DagNodeSpec) -> Any:
            resolved = self.table.backend(node.subagent, build=node)
            if resolved is not None and self.charter_context:
                setattr(resolved, "charter_context", self.charter_context)
            if resolved is None:
                return None

            class _CapturingBackend:
                def __getattr__(self, name: str) -> Any:
                    return getattr(resolved, name)

                async def run(self, *args: Any, **kwargs: Any) -> Any:
                    result = await resolved.run(*args, **kwargs)
                    node_outputs[node.id] = str(result)
                    return result

            return _CapturingBackend()

        async def _on_node_start(nid: str) -> None:
            node_obj = by_id.get(nid)
            self.queue.transition(
                nid,
                "running",
                assigned_specialist=node_obj.subagent if node_obj else "",
                wave=node_waves.get(nid, 0),
            )

        async def _progress_pub(event_name: str, payload: dict[str, Any]) -> None:
            if event_name == "dag_node_updated":
                nid = str(payload.get("node") or "")
                st = str(payload.get("status") or "")
                if nid in by_id and st in ("completed", "failed"):
                    node_obj = by_id[nid]
                    b_obj = self.table.backend(node_obj.subagent)
                    h_name = str(getattr(b_obj, "last_chosen_backend", "") or "")
                    self.queue.transition(
                        nid,
                        self.completion_status(str(
                            payload.get("output") or payload.get("result") or payload.get("summary")
                            or node_outputs.get(nid, "")
                        )) if st == "completed" else "failed",
                        assigned_specialist=node_obj.subagent,
                        assigned_harness=h_name,
                        wave=node_waves.get(nid, 0),
                    )

        result = await run_dag(
            plan.dag_spec,
            resolve=_resolve_from_table,
            backend=LocalFileBackend(),
            workdir=str(self.workdir),
            run_root=str(self.run_root),
            nodes_root=str(self.nodes_root),
            history_root=str(self.history_root),
            run_id=effective_run_id,
            session_key=session_key,
            sandbox_factory=sandbox_factory,
            on_node_start=_on_node_start,
            progress_publisher=_progress_pub,
            judge_node=judge_node or self._default_dag_judge,
        )
        for file_entry in result.files:
            nid = str(file_entry.get("node") or "")
            if file_entry.get("status") != "completed" or nid not in by_id:
                continue
            output = file_entry.get("output") or file_entry.get("result") or file_entry.get("summary")
            output = output or node_outputs.get(nid, "")
            node_obj = by_id[nid]
            backend_obj = self.table.backend(node_obj.subagent)
            final_status = self.completion_status(str(output or ""))
            prev = self.queue.latest_by_task().get(nid)
            if prev is not None and prev.status == final_status:
                continue
            self.queue.transition(
                nid,
                final_status,
                assigned_specialist=node_obj.subagent,
                assigned_harness=str(getattr(backend_obj, "last_chosen_backend", "") or ""),
                wave=node_waves.get(nid, 0),
            )
        return result

    async def dispatch_specialist_node(
        self,
        task_id: str,
        specialist_name: str,
        prompt: str,
        *,
        parent_task_id: str | None = None,
        workspace: Path | str | None = None,
        session_key: str | None = "omr-session",
        writes: bool = False,
        idempotent: bool = True,
        require_skill_evidence: bool = False,
        **kwargs: Any,
    ) -> str:
        ws = Path(workspace) if workspace is not None else self.workdir
        self.queue.enqueue(
            task_id,
            prompt,
            parent_task_id=parent_task_id,
            assigned_specialist=specialist_name,
            wave=0,
            status="enqueued",
        )
        backend = self.table.backend(specialist_name)
        if backend is None:
            self.queue.transition(task_id, "failed", assigned_specialist=specialist_name, error="unknown specialist")
            raise DagValidationError(f"Specialist {specialist_name!r} not found in SubagentTable")
        self.queue.transition(task_id, "routed", assigned_specialist=specialist_name)
        self.queue.transition(task_id, "running", assigned_specialist=specialist_name)
        lessons = self.lessons_store.matching_lessons(prompt)
        lessons_block = "\n".join(f"- {lesson['rule']}" for lesson in lessons)
        lesson_prompt = f"{prompt}\n\n[Active Lessons]\n{lessons_block}" if lessons else prompt
        framed_prompt = self._build_framed_prompt(lesson_prompt)
        try:
            result = await backend.run(
                framed_prompt,
                task_id=task_id,
                workspace=ws,
                executor=None,
                session_key=session_key,
                authored_task=prompt,
                writes=writes,
                idempotent=idempotent,
                **kwargs,
            )
            chosen_harness = str(getattr(backend, "last_chosen_backend", "") or getattr(backend, "name", "") or "")
            node_stub = DagNodeSpec(
                id=re.sub(r"[^A-Za-z0-9_-]", "_", task_id)[:60] or "node",
                subagent=specialist_name,
                node_summary=task_id[:40] or "task",
                prompt_template=prompt,
                execution_kind="write" if writes else "read",
                file_scope=["**"] if writes else [],
            )
            SkillAndToolReceiptGate.verify_specialist_execution(
                node_stub,
                backend,
                result,
                require_skill_evidence=require_skill_evidence or bool(getattr(backend, "require_skill_evidence", False)),
            )
            self.queue.transition(
                task_id,
                self.completion_status(str(result)),
                assigned_specialist=specialist_name,
                assigned_harness=chosen_harness,
            )
            if self.completion_status(str(result)) == "incomplete":
                self.record_lesson(prompt, "Provide verifiable done-gate evidence before completion.", task_id)
            return result
        except BaseException as exc:
            chosen_harness = str(getattr(backend, "last_chosen_backend", "") or getattr(backend, "name", "") or "")
            self.queue.transition(
                task_id,
                "failed",
                assigned_specialist=specialist_name,
                assigned_harness=chosen_harness,
                error=str(exc),
            )
            raise

    async def submit_task(
        self,
        task_id: str,
        prompt: str,
        *,
        run_id: str | None = None,
        session_key: str | None = "omr-session",
        sandbox_factory: Callable[[Path], Any] | None = None,
        judge_node: Callable[..., Any] | None = None,
        cto_specialist: str = "CTO",
        lead_specialist: str | None = None,
        cleanup_worktrees: bool = True,
    ) -> OmrDispatchResult:
        effective_run_id = run_id or f"run-{task_id.lower()}"
        run_dir = self.run_root / effective_run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        if self.charter_context:
            charter_serializable = {
                k: (str(v) if isinstance(v, Path) else v)
                for k, v in self.charter_context.items()
                if k != "everos_fetcher"
            }
            _write_json(run_dir / "charter_context.json", charter_serializable)

        self.queue.enqueue(task_id, prompt, parent_task_id=None, status="enqueued")
        try:
            chosen_lead = lead_specialist or await self.route_intake_specialist(prompt, default="Manager")
            self.queue.transition(task_id, "routed", assigned_specialist=chosen_lead)
            framed_prompt = self._build_framed_prompt(prompt)

            cto_backend = self.table.backend(cto_specialist)
            intake_cto_brief: CtoStrategicBrief | None = None
            if cto_backend is not None:
                cto_out = await cto_backend.run(
                    framed_prompt,
                    task_id=f"{task_id}::cto_intake",
                    workspace=self.workdir,
                    executor=None,
                    session_key=session_key,
                    authored_task=prompt,
                )
                intake_cto_brief = DagSpecialistRuntime._cto_brief_from(
                    cto_backend, cto_out, require_cto_alignment=True
                )

            manager_backend = self.table.backend(chosen_lead)
            if manager_backend is None:
                raise DagValidationError(f"Lead specialist {chosen_lead!r} not found in SubagentTable")
            manager_input = (
                f"{intake_cto_brief.render_guidance_block()}\n\n{framed_prompt}"
                if intake_cto_brief is not None
                else framed_prompt
            )
            decomp_out = await manager_backend.run(
                manager_input,
                task_id=f"{task_id}::decompose",
                workspace=self.workdir,
                executor=None,
                session_key=session_key,
                authored_task=prompt,
            )
            chosen_mgr_harness = str(getattr(manager_backend, "last_chosen_backend", "") or "")
            self.queue.transition(
                task_id,
                "decomposing",
                assigned_specialist=chosen_lead,
                assigned_harness=chosen_mgr_harness,
            )

            plan = DynamicDecompositionGate.validate_and_enqueue(
                decomp_out,
                self.queue,
                parent_task_id=task_id,
                run_dir=run_dir,
                workdir=self.workdir,
            )
            self.decide_gate("technical_decision", f"decompose:{task_id}")
            self.queue.transition(
                task_id,
                "running",
                assigned_specialist=chosen_lead,
                assigned_harness=chosen_mgr_harness,
            )

            dag_res = await self._execute_decomposed_dag(
                plan,
                effective_run_id=effective_run_id,
                session_key=session_key,
                sandbox_factory=sandbox_factory,
                judge_node=judge_node,
            )
            any_failed = any(f.get("status") == "failed" for f in dag_res.files)
            if cleanup_worktrees:
                self._cleanup_run_worktrees(effective_run_id)

            latest = self.queue.latest_by_task()
            children_done = all(
                e.status == "completed" for e in latest.values() if e.parent_task_id == task_id
            )
            self.queue.transition(
                task_id,
                "failed" if any_failed else "completed" if children_done else "incomplete",
                assigned_specialist=chosen_lead,
                assigned_harness=chosen_mgr_harness,
            )
            head_sha = (
                _git(self.workdir, "rev-parse", "HEAD").stdout.strip()
                if (self.workdir / ".git").exists()
                else ""
            )
            return OmrDispatchResult(
                task_id=task_id,
                lead_specialist=chosen_lead,
                cto_specialist=cto_specialist,
                cto_brief=intake_cto_brief,
                decomposition_plan=plan,
                dag_result=dag_res,
                queue_entries=self.queue.entries(),
                integrated_head_sha=head_sha,
            )
        except BaseException as exc:
            self.queue.transition(task_id, "failed", error=str(exc))
            raise


__all__ = [
    "AcceptanceReceipt",
    "BRAND_NAME",
    "CommandReceipt",
    "ConsequenceEntry",
    "ConsequenceMatrix",
    "CtoAlignmentGate",
    "CtoStrategicBrief",
    "DagSpecialistRuntime",
    "DecompositionPlan",
    "DirtyBaselineError",
    "DurableRecoveryLedger",
    "DynamicDecompositionGate",
    "ExecutionIdentity",
    "IndependentReviewError",
    "IntegrationConflictError",
    "OmrDispatchResult",
    "OmrDispatcher",
    "OmrQueueEntry",
    "OmrTaskQueue",
    "TriageDecision",
    "classify_triage_tier",
    "escalate_triage_tier",
    "LessonsStore",
    "PrematureCompletionError",
    "QueueStatus",
    "RecoveryBlockedError",
    "RequirementItem",
    "RequirementTraceabilityGate",
    "ReviewCycleLimitError",
    "ReviewCycleTracker",
    "ReviewGateValidator",
    "ReviewReceipt",
    "ScopeViolationError",
    "SkillAndToolReceiptGate",
    "SkillContractViolationError",
    "SkillPackInjector",
    "SkillPackResolver",
    "SliceReceipt",
    "SpecialistDefinition",
    "SpecialistPlaceholderBackend",
    "StaleReviewError",
    "WorktreeManager",
    "build_autonomous_feature_delivery_playbook",
    "build_context_charter",
    "build_holistic_charter",
    "build_oh_my_raven_playbook",
    "path_matches_scope",
    "scopes_overlap",
    "validate_consequence_matrix",
]
