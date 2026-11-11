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


def build_context_charter(
    task: str,
    *,
    memories: Sequence[str] = (),
    hot_md_path: Path | None = None,
    everos_fetcher: Callable[[str], Sequence[str]] | None = None,
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
    relevant = [item for item in items if terms and terms.intersection(re.findall(r"[A-Za-z0-9_]+", item.lower()))]
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
        charter_context: dict[str, Any] | None = None,
    ) -> None:
        self.enabled = False
        self.workdir = str(workdir) if workdir else ""
        self.run_dir: Path | None = Path(run_dir) if run_dir is not None else None
        self.run_id = ""
        self.max_review_cycles = max_review_cycles
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
        rt = cls(
            workdir=workdir,
            run_dir=Path(store.run_dir),
            max_review_cycles=max_review_cycles if max_review_cycles is not None else 3,
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
        if node.execution_kind == "write" and node.id in self.node_worktrees:
            self.slice_receipts[node.id] = WorktreeManager().build_slice_receipt(
                self.node_worktrees[node.id], node.id, self.base_sha, node.file_scope
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
            heads = [self.slice_receipts[w].head_sha for w in writers if w in self.slice_receipts]
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
                self.require_acceptance_receipt
                or bool(getattr(agent_backend, "require_acceptance_receipt", False))
                or expected_ids is not None
                or receipt.acceptance_receipt is not None
            )
            ReviewGateValidator().verify_review_receipt(
                receipt,
                candidate_head_sha=heads[-1] if heads else (self.base_sha or receipt.reviewed_head_sha),
                reviewer_backend=getattr(agent_backend, "execution_identity", None)
                or ExecutionIdentity(agent_name=node.subagent),
                author_backends=authors,
                max_cycles=backend_max_cycles if isinstance(backend_max_cycles, int) else self.max_review_cycles,
                cycle_tracker=self.cycle_tracker,
                run_id=self.run_id,
                ocr_available=bool(getattr(agent_backend, "ocr_available", False)),
                require_acceptance_receipt=require_acc,
                expected_requirement_ids=expected_ids,
                repo_root=Path(self.workdir) if self.workdir else None,
            )

    def on_failure(self, node: Any, prompt: str, exc: BaseException) -> None:
        if not self.enabled or isinstance(exc, (asyncio.CancelledError, RecoveryBlockedError)):
            return
        if self.ledger is not None:
            self.ledger.record_attempt(
                run_id=self.run_id,
                node_id=node.id,
                input_hash=self._hash(prompt),
                backend=node.subagent,
                failure_class=type(exc).__name__,
                worktree_path=str(self.node_worktrees.get(node.id, "")),
            )

    async def persist(self, store: Any) -> None:
        if self.enabled and self.ledger is not None and self.ledger.ledger_path.exists():
            await store.write_text(
                store.run_dir + "/recovery_ledger.json", self.ledger.ledger_path.read_text(encoding="utf-8")
            )


__all__ = [
    "AcceptanceReceipt",
    "BRAND_NAME",
    "CommandReceipt",
    "ConsequenceEntry",
    "ConsequenceMatrix",
    "CtoAlignmentGate",
    "CtoStrategicBrief",
    "DagSpecialistRuntime",
    "DirtyBaselineError",
    "DurableRecoveryLedger",
    "ExecutionIdentity",
    "IndependentReviewError",
    "IntegrationConflictError",
    "PrematureCompletionError",
    "RecoveryBlockedError",
    "RequirementItem",
    "RequirementTraceabilityGate",
    "ReviewCycleLimitError",
    "ReviewCycleTracker",
    "ReviewGateValidator",
    "ReviewReceipt",
    "ScopeViolationError",
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
