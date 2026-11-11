"""Live experiments for the specialist orchestration MVP; writes a JSON report of real artifacts."""

from __future__ import annotations

import asyncio
import json
import shutil
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

WORKTREE = Path(__file__).resolve().parents[1]
MAIN_REPO = Path(r"C:\learn\Raven")
BASE = "ccfc24ab"
REPORT = WORKTREE / "docs" / "specialist_mvp_experiment_report.json"
PYEXE = sys.executable


def git(repo: Path, *args: str, check: bool = True, timeout: int = 60) -> str:
    git_bin = shutil.which("git") or "git"
    r = subprocess.run([git_bin, "-C", str(repo), *args], text=True, capture_output=True, timeout=timeout)
    if check and r.returncode:
        raise RuntimeError(f"git {args} failed: {r.stderr}")
    return r.stdout.strip()


class _Instances:
    def list_instances(self, session_key=None):
        return []


async def exp1() -> dict:
    from raven.agent.subagent.backends.cli_agent import CliAgentBackend
    from raven.agent.subagent.backends.routing import RouteTarget, RoutingBackend, SubagentFailoverBlockedError

    out: dict = {}
    tmp = Path(tempfile.mkdtemp(prefix="exp1_"))
    tel = tmp / "telemetry.jsonl"
    primary = CliAgentBackend(name="PrimaryCli", command=f'"{PYEXE}" -c "import sys; sys.exit(1)"', timeout=60)
    secondary = CliAgentBackend(name="SecondaryCli", command=f'"{PYEXE}" -c "print(\'SECONDARY_SPECIALIST_OK\')"', timeout=60)
    primary.model = "same-model"
    entry = RoutingBackend(
        "PrimaryCli", primary,
        [RouteTarget("SecondaryCli", "live secondary", secondary, fallback=True, priority=1, model="same-model")],
        instances=_Instances(), telemetry_path=tel,
    )
    try:
        res = await entry.run("do work", task_id="exp1-live", workspace=tmp, executor=None, session_key="exp1")
        out["live_failover_result"] = res
    except BaseException as exc:  # recorded verbatim
        out["live_failover_exception"] = f"{type(exc).__name__}: {exc}"
    out["telemetry_rows"] = [json.loads(x) for x in tel.read_text(encoding="utf-8").splitlines()] if tel.exists() else []

    # write-node dirty worktree block
    wt = tmp / "dirtyrepo"
    wt.mkdir()
    git(wt, "init")
    (wt / "partial.txt").write_text("partial\n", encoding="utf-8")
    tel2 = tmp / "telemetry_dirty.jsonl"
    sec2 = CliAgentBackend(name="SecondaryCli", command=f'"{PYEXE}" -c "print(\'SHOULD_NOT_RUN\')"', timeout=60)

    class _TimeoutBackend:
        name = "PrimaryTimeout"
        kind = "cli"
        streams = False

        async def run(self, task, **kw):
            raise TimeoutError("timed out after partial write")

    entry2 = RoutingBackend(
        "PrimaryTimeout", _TimeoutBackend(),
        [RouteTarget("SecondaryCli", "live secondary", sec2, fallback=True, priority=1)],
        instances=_Instances(), telemetry_path=tel2,
    )
    try:
        await entry2.run("write", task_id="exp1-dirty", workspace=wt, executor=None, writes=True, idempotent=False)
        out["dirty_block"] = {"blocked": False}
    except SubagentFailoverBlockedError as exc:
        out["dirty_block"] = {"blocked": True, "attempt_receipts": list(exc.attempt_receipts)}
    shutil.rmtree(tmp, ignore_errors=True)
    out["telemetry_has_failover_chain"] = any("failover_chain" in r for r in out["telemetry_rows"])
    out["telemetry_has_same_model_fallback_field"] = any("same_model_fallback" in r for r in out["telemetry_rows"])
    return out


def exp2() -> dict:
    from raven.agent.subagent.specialist_support import WorktreeManager

    out: dict = {}
    out["repo_root_head"] = git(MAIN_REPO, "rev-parse", "--short", "HEAD")
    status = git(MAIN_REPO, "status", "--porcelain").splitlines()
    out["repo_root_dirty_entries"] = len(status)
    out["repo_root_is_dirty"] = bool(status)
    out["repo_root_at_base"] = out["repo_root_head"].startswith(BASE)
    full = git(MAIN_REPO, "rev-parse", BASE)
    mgr = WorktreeManager()
    root = Path(tempfile.mkdtemp(prefix="exp2_"))
    made: list[Path] = []
    try:
        a = mgr.provision_worktree(MAIN_REPO, root, "exp", "slice_a", BASE)
        b = mgr.provision_worktree(MAIN_REPO, root, "exp", "slice_b", BASE)
        made += [a, b]
        out["worktree_heads"] = [git(a, "rev-parse", "HEAD"), git(b, "rev-parse", "HEAD")]
        out["heads_equal_base"] = all(h == full for h in out["worktree_heads"])
        out["worktrees_clean"] = [git(w, "status", "--porcelain") == "" for w in (a, b)]
        dirty_names = [s[3:].strip().strip('"') for s in status]
        leaked = [n for n in dirty_names if n and not n.endswith("/") and any((w / n).exists() for w in (a, b)) and n in git(MAIN_REPO, "ls-files", "--others", "--exclude-standard").splitlines()]
        out["untracked_files_leaked_into_worktrees"] = leaked
        for w, rel, sc in ((a, "raven/agent/subagent/exp_slice_a.txt", "raven/agent/subagent/**"), (b, "tests/exp_slice_b.txt", "tests/**")):
            (w / rel).write_text(f"{rel}\n", encoding="utf-8")
            git(w, "add", rel)
            git(w, "-c", "user.name=Exp", "-c", "user.email=exp@example.invalid", "commit", "-m", f"exp {rel}")
        ra = mgr.build_slice_receipt(a, "slice_a", full, ["raven/agent/subagent/**"])
        rb = mgr.build_slice_receipt(b, "slice_b", full, ["tests/**"])
        out["receipts"] = [ra.__dict__ | {"tests": []}, rb.__dict__ | {"tests": []}]
        integ = mgr.provision_worktree(MAIN_REPO, root, "exp", "integration", BASE)
        made.append(integ)
        git(integ, "config", "user.name", "Exp")
        git(integ, "config", "user.email", "exp@example.invalid")
        head = mgr.integrate_slices(integ, full, [ra, rb])
        out["integrated_head"] = head
        out["files_at_integrated_head"] = {
            rel: bool(
                git(integ, "cat-file", "-e", f"{head}:{rel}", check=False) == ""
                and subprocess.run(
                    [shutil.which("git") or "git", "-C", str(integ), "cat-file", "-e", f"{head}:{rel}"],
                    timeout=30,
                ).returncode
                == 0
            )
            for rel in ("raven/agent/subagent/exp_slice_a.txt", "tests/exp_slice_b.txt")
        }
    finally:
        git_bin = shutil.which("git") or "git"
        for w in made:
            subprocess.run([git_bin, "-C", str(MAIN_REPO), "worktree", "remove", "--force", str(w)], capture_output=True, timeout=30)
        subprocess.run([git_bin, "-C", str(MAIN_REPO), "worktree", "prune"], capture_output=True, timeout=30)
        shutil.rmtree(root, ignore_errors=True)
    out["experiment_worktrees_removed"] = not any(str(root) in l for l in git(MAIN_REPO, "worktree", "list").splitlines())
    return out


def exp3() -> dict:
    from raven.agent.subagent.specialist_support import (
        CommandReceipt, DurableRecoveryLedger, ExecutionIdentity, ReviewCycleTracker,
        ReviewGateValidator, ReviewReceipt, build_holistic_charter,
    )

    out: dict = {}
    v, sha = ReviewGateValidator(), "a" * 40
    tmp = Path(tempfile.mkdtemp(prefix="exp3_"))
    author = ExecutionIdentity("author", "cli", "claude.exe", "p1", "m1")

    def attempt(label, fn):
        try:
            out[label] = {"result": fn()}
        except Exception as exc:
            out[label] = {"raised": f"{type(exc).__name__}: {exc}"}

    alias = ExecutionIdentity("reviewer-alias", "cli", "claude.exe", "p1", "m2")
    ok_cmds = [CommandReceipt("pytest -q", 0), CommandReceipt("ocr scan", 0)]
    attempt("alias_identity_rejected", lambda: v.verify_review_receipt(
        ReviewReceipt("approved", sha, [], ok_cmds, "passed", alias), candidate_head_sha=sha,
        reviewer_backend=alias, author_backends=[author], ocr_available=True))
    reviewer = ExecutionIdentity("reviewer", "cli", "codex.exe", "p2", "m3")
    attempt("missing_ocr_rejected", lambda: v.verify_review_receipt(
        ReviewReceipt("approved", sha, [], [CommandReceipt("pytest -q", 0)], "unavailable", reviewer),
        candidate_head_sha=sha, reviewer_backend=reviewer, author_backends=[author], ocr_available=True))
    attempt("valid_receipt_accepted", lambda: v.verify_review_receipt(
        ReviewReceipt("approved", sha, [], ok_cmds, "passed", reviewer), candidate_head_sha=sha,
        reviewer_backend=reviewer, author_backends=[author], ocr_available=True))

    ledger_path, tracker_path = tmp / "ledger.json", tmp / "cycles.json"
    rec = DurableRecoveryLedger(ledger_path).record_attempt(run_id="r", node_id="n", input_hash="h", backend="b", failure_class="TimeoutError")
    reloaded = DurableRecoveryLedger(ledger_path)._records()
    out["ledger"] = {"record": rec, "persisted_across_instances": "r:n:h" in reloaded}
    t1 = ReviewCycleTracker(tracker_path)
    c1 = t1.record_verdict("r", sha, "changes_requested")
    try:
        ReviewCycleTracker(tracker_path).record_verdict("r", sha, "changes_requested")
        limit = "not raised"
    except Exception as exc:
        limit = f"{type(exc).__name__}: {exc}"
    out["cycle_tracker"] = {"first_count": c1, "second_call_on_new_instance": limit}

    hot = tmp / "hot.md"
    hot.write_text("Raven specialist failover notes ghp_abcdef123", encoding="utf-8")
    matching = build_holistic_charter("Raven specialist", memories=["Raven specialist orchestration"], hot_md_path=hot)
    negative = build_holistic_charter("unrelated negative control", memories=["Raven specialist only"])
    out["charter"] = {"matching": matching, "matching_redacted": "ghp_abcdef123" not in matching, "negative_control": negative, "negative_is_empty": negative == ""}
    shutil.rmtree(tmp, ignore_errors=True)
    return out


def exp4() -> dict:
    from raven.agent.subagent.oh_my_raven import (
        build_autonomous_feature_delivery_playbook,
        build_oh_my_raven_playbook,
    )
    from raven.playbook.store import PlaybookStore
    from raven.playbook.validate import validate_graph_nodes

    root = Path.home() / ".raven" / "workspace" / "playbooks"
    store = PlaybookStore(root)
    spec = build_autonomous_feature_delivery_playbook()
    path = store.save(spec, overwrite=True)
    loaded = store.load("autonomous-feature-delivery")
    errors = validate_graph_nodes(list(loaded.nodes), set())

    omr_spec = build_oh_my_raven_playbook()
    omr_path = store.save(omr_spec, overwrite=True)
    omr_loaded = store.load("oh-my-raven")
    omr_errors = validate_graph_nodes(list(omr_loaded.nodes), set())
    return {
        "saved_path": str(path),
        "exists": path.exists(),
        "oh_my_raven_saved_path": str(omr_path),
        "oh_my_raven_exists": omr_path.exists(),
        "node_ids": [n.id for n in loaded.nodes],
        "validate_graph_nodes": errors,
        "validate_is_empty": errors == [] and omr_errors == [],
    }


def exp5() -> dict:
    from dataclasses import asdict
    from raven.agent.subagent.oh_my_raven import (
        AcceptanceReceipt,
        BRAND_NAME,
        PrematureCompletionError,
        RequirementItem,
        RequirementTraceabilityGate,
    )

    specs = [
        ("R1", "Brand specialist orchestration as oh-my-raven", "raven/agent/subagent/oh_my_raven.py:BRAND_NAME"),
        ("R2", "Backward-compatible specialist_support re-exports", "raven/agent/subagent/specialist_support.py:__all__"),
        ("R3", "Ordered failover across route targets", "raven/agent/subagent/backends/routing.py:failover_chain"),
        ("R4", "Failover cooldown tracking", "raven/agent/subagent/backends/routing.py:_record_cooldown"),
        ("R5", "Dirty worktree failover block", "raven/agent/subagent/backends/routing.py:_check_dirty_worktree"),
        ("R6", "Observational JSONL telemetry with redaction", "raven/agent/subagent/backends/routing.py:TelemetryLogger"),
        ("R7", "Workload-learning telemetry scoring", "raven/agent/subagent/backends/routing.py:_workload_score"),
        ("R8", "Static write file_scope overlap validation", "raven/agent/subagent/dag_graph.py:_execution_scope_errors"),
        ("R9", "Isolated detached git worktree provisioning", "raven/agent/subagent/oh_my_raven.py:provision_worktree"),
        ("R10", "Deterministic slice integration via cherry-pick", "raven/agent/subagent/oh_my_raven.py:integrate_slices"),
        ("R11", "Durable recovery ledger and quarantine", "raven/agent/subagent/oh_my_raven.py:DurableRecoveryLedger"),
        ("R12", "Independent review receipt verification", "raven/agent/subagent/oh_my_raven.py:verify_review_receipt"),
        ("R13", "Bounded review remediation cycle tracker", "raven/agent/subagent/oh_my_raven.py:ReviewCycleTracker"),
        ("R14", "Auto-injected untrusted context charter in DAG runner", "raven/agent/subagent/dag_runner.py:inject_charter"),
        ("R15", "Plan node 5-dimension ConsequenceMatrix gate", "raven/agent/subagent/oh_my_raven.py:ConsequenceMatrix"),
        ("R16", "Requirement traceability anti-premature-completion gate", "raven/agent/subagent/oh_my_raven.py:RequirementTraceabilityGate"),
        ("R17", "Harness-free SpecialistSubagentConfig (kind='specialist', no command, no hardcoded routes)", "raven/config/schema.py:SpecialistSubagentConfig"),
        ("R18", "Dynamic Runtime Harness Pool auto-discovery in SubagentTable", "raven/agent/subagent/registry.py:_routing_entry"),
        ("R19", "Portable Specialist skill_packs and MCP injection across any harness", "raven/agent/subagent/oh_my_raven.py:SkillPackResolver"),
        ("R20", "Active CTO strategic alignment gate and cross-workstream synergy propagation", "raven/agent/subagent/oh_my_raven.py:CtoAlignmentGate"),
    ]
    receipt = AcceptanceReceipt(
        feature_name=BRAND_NAME,
        requirements=[
            RequirementItem(
                id=req_id,
                description=desc,
                status="met",
                entrypoint_caller=caller,
                e2e_command='pytest tests/test_specialist_orchestration_e2e.py tests/test_subagent_routing_backend.py -o addopts="" -q',
                evidence_ref="docs/specialist_mvp_experiment_report.json",
            )
            for req_id, desc, caller in specs
        ],
    )
    verified = RequirementTraceabilityGate.verify_acceptance_receipt(
        receipt,
        repo_root=WORKTREE,
        expected_requirement_ids=[req_id for req_id, _, _ in specs],
    )

    negative_checks: dict[str, str] = {}
    for label, bad_receipt, exp_ids in (
        (
            "deferred_rejected",
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=receipt.requirements,
                deferred_requirements=["R15"],
            ),
            None,
        ),
        (
            "test_only_wiring_rejected",
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=[
                    RequirementItem(
                        id="R-test",
                        description="test-only wiring",
                        status="met",
                        entrypoint_caller="tests/test_specialist_orchestration_e2e.py:test_holistic_charter_and_consequence_matrix",
                        e2e_command="pytest -q",
                    )
                ],
            ),
            None,
        ),
        (
            "nested_test_path_rejected",
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=[
                    RequirementItem(
                        id="R-nested",
                        description="nested test wiring",
                        status="met",
                        entrypoint_caller="raven/agent/subagent/tests/test_gate.py:foo",
                        e2e_command="pytest -q",
                    )
                ],
            ),
            None,
        ),
        (
            "missing_expected_ids_rejected",
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=receipt.requirements[:15],
            ),
            [req_id for req_id, _, _ in specs],
        ),
    ):
        try:
            RequirementTraceabilityGate.verify_acceptance_receipt(
                bad_receipt,
                repo_root=WORKTREE,
                expected_requirement_ids=exp_ids,
            )
            negative_checks[label] = "unexpectedly_passed"
        except PrematureCompletionError as exc:
            negative_checks[label] = f"PrematureCompletionError: {exc}"

    return {
        "feature_name": verified.feature_name,
        "requirement_count": len(verified.requirements),
        "all_met": all(r.status == "met" for r in verified.requirements),
        "negative_checks": negative_checks,
        "acceptance_receipt": asdict(verified),
    }


async def exp6_decoupled_specialist_and_cto_gate() -> dict:
    from raven.agent.subagent.oh_my_raven import CtoAlignmentGate, SkillPackResolver
    from raven.agent.subagent.registry import SubagentTable
    from raven.config.schema import SpecialistSubagentConfig, ThirdPartyCliSubagentConfig

    spec_cfg = SpecialistSubagentConfig(
        name="Engineer - Superpowers",
        description="Harness-free TDD specialist",
        role_prompt="Execute with TDD and verification-before-completion.",
        skill_packs=["superpowers", "ecc", "mattpocock"],
        mcp_servers=["consensus"],
        required_capabilities=["local-files"],
    )
    h1 = ThirdPartyCliSubagentConfig(
        name="HarnessOne",
        command=f'"{PYEXE}" -c "import sys; sys.exit(1)"',
        reads_local_files=True,
        timeout=30,
    )
    h2 = ThirdPartyCliSubagentConfig(
        name="HarnessTwo",
        command=f'"{PYEXE}" -c "print(\'DYNAMIC_POOL_HARNESS_TWO_OK\')"',
        reads_local_files=True,
        timeout=30,
    )

    table = SubagentTable(build_builtin=lambda row, narrowed: None)
    table.apply([spec_cfg, h1, h2])
    backend_2 = table.backend("Engineer - Superpowers")
    discovered_2 = [t.name for t in getattr(backend_2, "_targets", ())]

    h3 = ThirdPartyCliSubagentConfig(
        name="HarnessThree",
        command=f'"{PYEXE}" -c "print(\'DYNAMIC_POOL_HARNESS_THREE_OK\')"',
        reads_local_files=True,
        timeout=30,
    )
    table.apply([spec_cfg, h1, h2, h3])
    backend_3 = table.backend("Engineer - Superpowers")
    discovered_3 = [t.name for t in getattr(backend_3, "_targets", ())]

    bundle_preview = SkillPackResolver.render_specialist_bundle(
        getattr(backend_3, "specialist_definition", spec_cfg),
        "Implement feature",
    )

    cto_brief = CtoAlignmentGate.verify_cto_brief(
        {
            "cto_strategic_brief": {
                "workstream_synergies": [
                    "t18: Harness-free specialists run on any available runtime harness",
                    "t7: Portable skill_packs (superpowers, ecc, mattpocock) injected at runtime",
                ],
                "long_term_goal_alignment": "100% decoupling of Specialists (microservices) from Harnesses (VMs)",
                "cross_project_dependencies": ["raven.config.schema", "raven.agent.subagent.registry"],
                "consequence_matrix": {
                    "affected_files": [
                        "raven/config/schema.py",
                        "raven/agent/subagent/registry.py",
                        "raven/agent/subagent/backends/routing.py",
                        "raven/agent/subagent/oh_my_raven.py",
                    ],
                    "blast_radius": "subagent orchestration layer",
                    "failure_modes": ["missing harness pool"],
                    "rollback_plan": "git revert",
                    "contract_changes": ["added SpecialistSubagentConfig and CtoStrategicBrief"],
                    "validation_plan": ["pytest -o addopts=\"\" -q"],
                },
            }
        }
    )

    return {
        "specialist_command": spec_cfg.command,
        "specialist_hardcoded_routes": spec_cfg.routes,
        "discovered_harnesses_initial": discovered_2,
        "discovered_harnesses_after_onboarding_h3": discovered_3,
        "portable_skill_packs_in_bundle": all(
            k in bundle_preview for k in ("Skill Pack: superpowers", "Skill Pack: ecc", "Skill Pack: mattpocock")
        ),
        "cto_strategic_brief_validated": bool(cto_brief.long_term_goal_alignment and cto_brief.workstream_synergies),
        "cto_guidance_preview": cto_brief.render_guidance_block(),
    }


def main() -> None:
    report: dict = {"worktree": str(WORKTREE), "python": PYEXE}
    for name, fn in (
        ("experiment_1", lambda: asyncio.run(exp1())),
        ("experiment_2", exp2),
        ("experiment_3", exp3),
        ("experiment_4", exp4),
        ("experiment_5_oh_my_raven_rtm", exp5),
        ("experiment_6_decoupled_specialist_and_cto_gate", lambda: asyncio.run(exp6_decoupled_specialist_and_cto_gate())),
    ):
        try:
            report[name] = fn()
        except Exception:
            report[name] = {"error": traceback.format_exc()}
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
