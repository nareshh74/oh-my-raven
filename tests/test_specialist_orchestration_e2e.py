"""Slice 2 and 3 orchestration contracts over real temporary Git repositories."""

from __future__ import annotations

import asyncio
import json
import subprocess
from pathlib import Path

import pytest

from raven.agent.subagent.dag_graph import DagNodeSpec, SubAgentDagSpec, validate_and_order
from raven.agent.subagent.prompt_errors import DagValidationError
import raven.agent.subagent.oh_my_raven as _raven_omr_mod
import raven.agent.subagent.backends.routing as _raven_routing_mod
import oh_my_raven.oh_my_raven as _standalone_omr_mod
import oh_my_raven.routing as _standalone_routing_mod

for _k, _v in vars(_raven_omr_mod).items():
    if not _k.startswith("__"):
        setattr(_standalone_omr_mod, _k, _v)
for _k, _v in vars(_raven_routing_mod).items():
    if not _k.startswith("__"):
        setattr(_standalone_routing_mod, _k, _v)

from oh_my_raven.oh_my_raven import (
    AcceptanceReceipt,
    BRAND_NAME,
    CommandReceipt,
    ConsequenceEntry,
    DagSpecialistRuntime,
    DirtyBaselineError,
    DurableRecoveryLedger,
    ExecutionIdentity,
    IndependentReviewError,
    IntegrationConflictError,
    PrematureCompletionError,
    RequirementItem,
    RequirementTraceabilityGate,
    ReviewCycleLimitError,
    ReviewCycleTracker,
    ReviewGateValidator,
    ReviewReceipt,
    ScopeViolationError,
    SliceReceipt,
    StaleReviewError,
    WorktreeManager,
    build_context_charter,
    build_holistic_charter,
    build_oh_my_raven_playbook,
    validate_consequence_matrix,
)


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, text=True, capture_output=True
    ).stdout.strip()


def _repo(tmp_path: Path) -> tuple[Path, str]:
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init")
    _git(repo, "config", "user.email", "test@example.invalid")
    _git(repo, "config", "user.name", "Test User")
    (repo / "README.md").write_text("base\n", encoding="utf-8")
    _git(repo, "add", "README.md")
    _git(repo, "commit", "-m", "base")
    return repo, _git(repo, "rev-parse", "HEAD")


def _node(node_id: str, *, kind: str = "read", scope: list[str] | None = None, deps: list[str] | None = None) -> DagNodeSpec:
    return DagNodeSpec(
        id=node_id,
        subagent="specialist",
        node_summary=node_id,
        prompt_template=node_id,
        execution_kind=kind,
        file_scope=scope or [],
        depends_on=deps or [],
    )


def test_write_scope_validation_rejects_concurrent_overlap_and_bad_scopes() -> None:
    spec = SubAgentDagSpec(
        task_summary="scope validation",
        nodes=[_node("one", kind="write", scope=["raven/a.py"]), _node("two", kind="write", scope=["raven/**"])],
    )
    with pytest.raises(DagValidationError, match="one.*two|two.*one"):
        validate_and_order(spec)

    for scope in ([], [""], ["/absolute.py"], ["../escape.py"]):
        invalid = SubAgentDagSpec(task_summary="invalid", nodes=[_node("write", kind="write", scope=scope)])
        with pytest.raises(DagValidationError):
            validate_and_order(invalid)


def test_write_scope_validation_allows_ordered_or_read_overlap() -> None:
    ordered = SubAgentDagSpec(
        task_summary="ordered",
        nodes=[_node("one", kind="write", scope=["raven/**"]), _node("two", kind="write", scope=["raven/a.py"], deps=["one"])],
    )
    assert validate_and_order(ordered) == ["one", "two"]
    read_overlap = SubAgentDagSpec(
        task_summary="read overlap",
        nodes=[_node("read", scope=["raven/a.py"]), _node("write", kind="write", scope=["raven/a.py"])],
    )
    assert validate_and_order(read_overlap) == ["read", "write"]


@pytest.mark.slow
def test_worktrees_are_isolated_and_scope_checked(tmp_path: Path) -> None:
    repo, base_sha = _repo(tmp_path)
    manager = WorktreeManager()
    root = tmp_path / "worktrees"
    first = manager.provision_worktree(repo, root, "run", "one", base_sha)
    second = manager.provision_worktree(repo, root, "run", "two", base_sha)
    assert first != second
    assert _git(first, "rev-parse", "HEAD") == _git(second, "rev-parse", "HEAD") == base_sha

    (first / "allowed.py").write_text("ok\n", encoding="utf-8")
    assert manager.verify_worktree_scope(first, base_sha, ["allowed.py"]) == ["allowed.py"]
    (first / "outside.py").write_text("no\n", encoding="utf-8")
    with pytest.raises(ScopeViolationError, match="outside.py"):
        manager.verify_worktree_scope(first, base_sha, ["allowed.py"])


@pytest.mark.slow
def test_dirty_baseline_is_rejected(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    (repo / "dirty.txt").write_text("dirty\n", encoding="utf-8")
    with pytest.raises(DirtyBaselineError):
        WorktreeManager().verify_clean_baseline(repo)


@pytest.mark.slow
def test_integration_merges_slices_and_detects_conflicts(tmp_path: Path) -> None:
    repo, base_sha = _repo(tmp_path)
    manager = WorktreeManager()
    root = tmp_path / "worktrees"
    first = manager.provision_worktree(repo, root, "slices", "one", base_sha)
    (first / "one.txt").write_text("one\n", encoding="utf-8")
    _git(first, "add", "one.txt")
    _git(first, "commit", "-m", "one")
    second = manager.provision_worktree(repo, root, "slices", "two", base_sha)
    (second / "two.txt").write_text("two\n", encoding="utf-8")
    _git(second, "add", "two.txt")
    _git(second, "commit", "-m", "two")
    integration, head = manager.integrate_slices(
        repo, root, "integrated", base_sha,
        [SliceReceipt("two", _git(second, "rev-parse", "HEAD")), SliceReceipt("one", _git(first, "rev-parse", "HEAD"))],
    )
    assert (integration / "one.txt").exists() and (integration / "two.txt").exists()
    assert head == _git(integration, "rev-parse", "HEAD")

    conflict_a = manager.provision_worktree(repo, root, "conflict", "a", base_sha)
    (conflict_a / "README.md").write_text("a\n", encoding="utf-8")
    _git(conflict_a, "commit", "-am", "a")
    conflict_b = manager.provision_worktree(repo, root, "conflict", "b", base_sha)
    (conflict_b / "README.md").write_text("b\n", encoding="utf-8")
    _git(conflict_b, "commit", "-am", "b")
    with pytest.raises(IntegrationConflictError):
        manager.integrate_slices(
            repo, root, "conflicted", base_sha,
            [SliceReceipt("a", _git(conflict_a, "rev-parse", "HEAD")), SliceReceipt("b", _git(conflict_b, "rev-parse", "HEAD"))],
        )


def test_reviewer_gate_structure_receipts_and_cycle_limit() -> None:
    nodes = [_node("write", kind="write", scope=["a.py"]), _node("review", kind="review", deps=["write"])]
    assert ReviewGateValidator().validate_workflow_structure(nodes) == []
    receipt = ReviewReceipt(verdict="approved", reviewed_head_sha="abc", findings=[], commands=["pytest"])
    assert ReviewGateValidator().verify_review_receipt(receipt, candidate_head_sha="abc", reviewer_backend="reviewer", author_backends=["author"], cycle=0) == "approved"
    with pytest.raises(IndependentReviewError):
        ReviewGateValidator().verify_review_receipt(receipt, candidate_head_sha="abc", reviewer_backend="author", author_backends=["author"], cycle=0)
    with pytest.raises(StaleReviewError):
        ReviewGateValidator().verify_review_receipt(receipt, candidate_head_sha="new", reviewer_backend="reviewer", author_backends=["author"], cycle=0)
    changes = ReviewReceipt(verdict="changes_requested", reviewed_head_sha="abc", findings=["fix"], commands=["pytest"])
    assert ReviewGateValidator().verify_review_receipt(changes, candidate_head_sha="abc", reviewer_backend="reviewer", author_backends=["author"], cycle=1) == "changes_requested"
    with pytest.raises(ReviewCycleLimitError):
        ReviewGateValidator().verify_review_receipt(changes, candidate_head_sha="abc", reviewer_backend="reviewer", author_backends=["author"], cycle=2)


def test_holistic_charter_and_consequence_matrix(tmp_path: Path) -> None:
    hot = tmp_path / "hot.md"
    hot.write_text("Raven specialist work uses sk-secret and Bearer token\n", encoding="utf-8")
    charter = build_holistic_charter("Raven specialist", memories=["Raven specialist uses ghp_abcdef"], hot_md_path=hot, max_chars=500)
    assert charter.startswith("[BEGIN UNTRUSTED CONTEXT CHARTER]\n")
    assert charter.endswith("\n[END UNTRUSTED CONTEXT CHARTER]")
    assert "sk-secret" not in charter and "ghp_abcdef" not in charter and "Bearer token" not in charter
    assert build_holistic_charter("unrelated negative control", memories=["Raven specialist only"]) == ""
    assert build_holistic_charter("task", hot_md_path=tmp_path / "missing.md") == ""
    validate_consequence_matrix([ConsequenceEntry("change", "effect", "rollback", "local")])
    with pytest.raises(ValueError):
        validate_consequence_matrix([ConsequenceEntry("", "effect", "rollback", "local")])


class _Sandbox:
    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class _Backend:
    def __init__(
        self,
        fail: bool = False,
        commit: str | None = None,
        reply: str | None = None,
        receipt: ReviewReceipt | None = None,
        max_review_cycles: int | None = None,
        agent_name: str | None = None,
    ) -> None:
        self.fail, self.commit, self.reply, self.calls = fail, commit, reply, []
        if receipt is not None:
            self.last_review_receipt = receipt
        if max_review_cycles is not None:
            self.max_review_cycles = max_review_cycles
        if agent_name is not None:
            self.execution_identity = ExecutionIdentity(agent_name=agent_name)

    async def run(self, task, *, task_id, workspace, executor=None, session_key=None, instance=None, mode=None, **kw):
        self.calls.append({"task": task, "workspace": Path(workspace), "executor": executor, **kw})
        if self.fail:
            raise RuntimeError("boom")
        if self.commit:
            (Path(workspace) / self.commit).write_text(task_id, encoding="utf-8")
            _git(Path(workspace), "add", self.commit)
            _git(Path(workspace), "-c", "user.email=t@e.invalid", "-c", "user.name=T", "commit", "-m", task_id)
        return self.reply if self.reply is not None else f"done {task_id}"


async def _run(spec, repo, tmp_path, backends, run_id="r1", reg="hist", sandbox_factory=None):
    from raven.agent.subagent.dag_runner import run_dag
    from raven.agent.subagent.prompt_backend import LocalFileBackend

    return await run_dag(
        spec,
        resolve=lambda node: backends[node.id],
        backend=LocalFileBackend(),
        workdir=str(repo),
        run_root=str(tmp_path / reg / "dag"),
        nodes_root=str(tmp_path / reg / "nodes"),
        history_root=str(tmp_path / reg),
        run_id=run_id,
        sandbox_factory=sandbox_factory,
    )


@pytest.mark.slow
def test_dag_runner_persists_recovery_ledger_and_blocks_quarantined_retry(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    spec = SubAgentDagSpec(task_summary="t", nodes=[_node("w", kind="write", scope=["a.txt"])])
    first = _Backend(fail=True)
    asyncio.run(_run(spec, repo, tmp_path, {"w": first}))
    ledgers = list((tmp_path / "hist").rglob("recovery_ledger.json"))
    assert ledgers, "ledger not persisted"
    record = next(iter(json.loads(ledgers[0].read_text(encoding="utf-8")).values()))
    assert record["attempt_count"] >= 1 and len(first.calls) >= 1
    wt = tmp_path / "repo.worktrees" / "r1-w"
    (wt / "dirty.txt").write_text("x", encoding="utf-8")
    second = _Backend()
    asyncio.run(_run(spec, repo, tmp_path, {"w": second}, run_id="r1", reg="hist2"))
    assert second.calls == []
    second_ledger = next((tmp_path / "hist2").rglob("recovery_ledger.json"))
    assert "quarantine" in second_ledger.read_text(encoding="utf-8")

    ledger = DurableRecoveryLedger(tmp_path / "l.json")
    rt = DagSpecialistRuntime()
    rt.prepare_run(spec, str(repo), "rc", tmp_path)
    rt.on_failure(spec.nodes[0], "p", asyncio.CancelledError())
    assert not (tmp_path / "recovery_ledger.json").exists()
    assert ledger._records() == {}


@pytest.mark.slow
def test_dag_runner_provisions_isolated_worktrees_and_passes_slice_receipts_to_integrate(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    spec = SubAgentDagSpec(
        task_summary="t",
        nodes=[
            _node("slice_a", kind="write", scope=["a.txt"]),
            _node("slice_b", kind="write", scope=["b.txt"]),
            _node("integrate", kind="integrate", deps=["slice_a", "slice_b"]),
        ],
    )
    backends = {"slice_a": _Backend(commit="a.txt"), "slice_b": _Backend(commit="b.txt"), "integrate": _Backend()}
    asyncio.run(_run(spec, repo, tmp_path, backends, sandbox_factory=_Sandbox))
    wa, wb = (backends[k].calls[0]["workspace"] for k in ("slice_a", "slice_b"))
    ea, eb = (backends[k].calls[0]["executor"].root for k in ("slice_a", "slice_b"))
    assert wa != wb and repo not in (wa, wb)
    assert ea == wa and eb == wb and repo not in (ea, eb)
    assert _git(wa, "rev-parse", "HEAD") != base and _git(wa, "rev-parse", "--abbrev-ref", "HEAD") == "HEAD"
    got = backends["integrate"].calls[0]
    assert got["workspace"] == repo
    assert [r.node_id for r in got["slice_receipts"]] == ["slice_a", "slice_b"]
    assert all(isinstance(r, SliceReceipt) and r.base_sha == base for r in got["slice_receipts"])


@pytest.mark.slow
def test_dag_runner_review_fails_closed_without_receipt_and_enforces_max_review_cycles(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    review_spec = SubAgentDagSpec(
        task_summary="t",
        nodes=[
            DagNodeSpec(
                id="review",
                subagent="reviewer",
                node_summary="r",
                prompt_template="r",
                execution_kind="review",
            ),
            DagNodeSpec(
                id="re_review",
                subagent="reviewer",
                node_summary="r2",
                prompt_template="r2",
                execution_kind="review",
                depends_on=["review"],
            ),
        ],
    )
    prose = _Backend(reply="Looks good to me!", agent_name="reviewer")
    res_prose = asyncio.run(_run(review_spec, repo, tmp_path, {"review": prose, "re_review": prose}, run_id="r-prose", reg="h-prose"))
    prose_nodes = {f["node"]: f for f in res_prose.files}
    assert prose_nodes["review"]["status"] == "failed"
    assert "completed without a valid ReviewReceipt" in (prose_nodes["review"]["error"] or "")

    changes_json = json.dumps({"verdict": "changes_requested", "reviewed_head_sha": base, "findings": ["fix"], "commands": ["pytest"]})
    capped = _Backend(reply=changes_json, receipt=ReviewReceipt(verdict="changes_requested", reviewed_head_sha=base, findings=["fix"], commands=["pytest"]), max_review_cycles=1, agent_name="reviewer")
    res_capped = asyncio.run(_run(review_spec, repo, tmp_path, {"review": capped, "re_review": capped}, run_id="r-cap", reg="h-cap"))
    capped_nodes = {f["node"]: f for f in res_capped.files}
    assert capped_nodes["review"]["status"] == "completed"
    assert capped_nodes["re_review"]["status"] == "failed"
    assert "review remediation cycle limit reached" in (capped_nodes["re_review"]["error"] or "")
    rt = DagSpecialistRuntime(workdir=repo, run_dir=tmp_path / "rt", max_review_cycles=1)
    rt.prepare_run(review_spec, str(repo), "r-direct", tmp_path / "rt", max_review_cycles=1)
    rt.after_success(review_spec.nodes[0], changes_json, capped, {"review": review_spec.nodes[0]}, {"review": capped})
    with pytest.raises(ReviewCycleLimitError):
        rt.after_success(review_spec.nodes[1], changes_json, capped, {"review": review_spec.nodes[0], "re_review": review_spec.nodes[1]}, {"review": capped, "re_review": capped})


def test_review_gate_ocr_and_persisted_cycle_tracker_matrix(tmp_path: Path) -> None:
    gate = ReviewGateValidator()

    def verify(receipt: ReviewReceipt, ocr: bool) -> str:
        return gate.verify_review_receipt(receipt, candidate_head_sha="h", reviewer_backend="r", author_backends=["a"], ocr_available=ocr)

    missing = ReviewReceipt("approved", "h", [], ["pytest"], ocr_status="passed")
    with pytest.raises(IndependentReviewError):
        verify(missing, True)
    failed = ReviewReceipt("approved", "h", [], [CommandReceipt("ocr review", 1)], ocr_status="passed")
    with pytest.raises(IndependentReviewError):
        verify(failed, True)
    audit = ReviewReceipt("approved", "h", [], [CommandReceipt("manual audit", 0)], ocr_status="unavailable")
    assert verify(audit, False) == "approved"

    path = tmp_path / "cycles.json"
    assert ReviewCycleTracker(path, max_cycles=1).record_verdict("run", "h", "changes_requested") == 1
    with pytest.raises(ReviewCycleLimitError):
        ReviewCycleTracker(path, max_cycles=1).record_verdict("run", "h", "changes_requested")


def _build_16_item_acceptance_receipt() -> AcceptanceReceipt:
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
    return AcceptanceReceipt(
        feature_name=BRAND_NAME,
        requirements=[
            RequirementItem(
                id=req_id,
                description=desc,
                status="met",
                entrypoint_caller=caller,
                e2e_command="pytest tests/test_specialist_orchestration_e2e.py tests/test_subagent_routing_backend.py -o addopts=\"\" -q",
                evidence_ref="docs/specialist_mvp_experiment_report.json",
            )
            for req_id, desc, caller in specs
        ],
    )


def test_requirement_traceability_gate_rejects_premature_completion_and_accepts_valid_16_item_receipt() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    if not (repo_root / "raven").exists():
        import raven
        repo_root = Path(raven.__file__).resolve().parents[1]
    valid_receipt = _build_16_item_acceptance_receipt()
    assert len(valid_receipt.requirements) == 20
    assert RequirementTraceabilityGate.verify_acceptance_receipt(valid_receipt, repo_root=repo_root) is valid_receipt

    with pytest.raises(PrematureCompletionError, match="no requirements"):
        RequirementTraceabilityGate.verify_acceptance_receipt(AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[]))

    with pytest.raises(PrematureCompletionError, match="deferred requirements"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=valid_receipt.requirements,
                deferred_requirements=["R15"],
            )
        )

    with pytest.raises(PrematureCompletionError, match="unmet requirements"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(
                feature_name=BRAND_NAME,
                requirements=valid_receipt.requirements,
                unmet_requirements=["R14"],
            )
        )

    deferred_item = RequirementItem(
        id="R-def",
        description="deferred item",
        status="deferred",
        entrypoint_caller="raven/agent/subagent/oh_my_raven.py:BRAND_NAME",
        e2e_command="pytest -q",
    )
    with pytest.raises(PrematureCompletionError, match="non-met status"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[deferred_item])
        )

    test_only_item = RequirementItem(
        id="R-test",
        description="wired only in tests",
        status="met",
        entrypoint_caller="tests/test_specialist_orchestration_e2e.py:test_holistic_charter_and_consequence_matrix",
        e2e_command="pytest -q",
    )
    with pytest.raises(PrematureCompletionError, match="no_test_only_wiring"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[test_only_item]),
            repo_root=repo_root,
        )

    review_with_acc = ReviewReceipt(
        verdict="approved",
        reviewed_head_sha="abc",
        findings=[],
        commands=["pytest"],
        acceptance_receipt=valid_receipt,
    )
    assert (
        ReviewGateValidator().verify_review_receipt(
            review_with_acc,
            candidate_head_sha="abc",
            reviewer_backend="reviewer",
            author_backends=["author"],
            require_acceptance_receipt=True,
            repo_root=repo_root,
        )
        == "approved"
    )


@pytest.mark.slow
def test_dag_runner_plan_node_enforces_consequence_matrix_and_injects_context_charter(tmp_path: Path) -> None:
    repo, _ = _repo(tmp_path)
    (repo / "charter_context.json").write_text(
        json.dumps({"memories": ["plan_node and write_after_plan use sk-secret-token and oh_my_raven charter"]}),
        encoding="utf-8",
    )
    spec = SubAgentDagSpec(
        task_summary="plan and write",
        nodes=[
            _node("plan_node", kind="plan"),
            _node("write_after_plan", kind="write", scope=["planned.txt"], deps=["plan_node"]),
        ],
    )

    bad_plan = _Backend(reply="just some prose without matrix")
    write_backend = _Backend(commit="planned.txt")
    res_bad = asyncio.run(
        _run(spec, repo, tmp_path, {"plan_node": bad_plan, "write_after_plan": write_backend}, run_id="r-bad-plan", reg="h-bad-plan")
    )
    bad_files = {f["node"]: f for f in res_bad.files}
    assert bad_files["plan_node"]["status"] == "failed"
    assert "completed without a valid ConsequenceMatrix" in (bad_files["plan_node"]["error"] or "")
    assert bad_files["write_after_plan"]["status"] == "skipped"
    assert write_backend.calls == []

    valid_matrix_json = json.dumps(
        {
            "consequence_matrix": {
                "affected_files": ["planned.txt"],
                "blast_radius": "isolated slice",
                "failure_modes": ["bad plan format"],
                "rollback_plan": "git worktree remove",
                "contract_changes": ["added plan node"],
                "validation_plan": ["pytest -q"],
            }
        }
    )
    good_plan = _Backend(reply=valid_matrix_json)
    good_write = _Backend(commit="planned.txt")
    res_good = asyncio.run(
        _run(spec, repo, tmp_path, {"plan_node": good_plan, "write_after_plan": good_write}, run_id="r-good-plan", reg="h-good-plan")
    )
    good_files = {f["node"]: f for f in res_good.files}
    assert good_files["plan_node"]["status"] == "completed"
    assert good_files["write_after_plan"]["status"] == "completed"
    assert len(good_write.calls) == 1
    dispatched_task = good_plan.calls[0].get("task") or ""
    assert "[BEGIN UNTRUSTED CONTEXT CHARTER]" in dispatched_task
    assert "[REDACTED]" in dispatched_task and "sk-secret-token" not in dispatched_task
    rt = DagSpecialistRuntime(workdir=repo, charter_context={"memories": ["plan_node uses sk-secret-token"]})
    injected = rt.inject_charter(spec.nodes[0], "plan_node task")
    assert injected.startswith("[BEGIN UNTRUSTED CONTEXT CHARTER]\n")
    assert "[REDACTED]" in injected and "sk-secret-token" not in injected
    assert build_oh_my_raven_playbook().name == BRAND_NAME
    assert build_context_charter is build_holistic_charter

    unvalidated_rt = DagSpecialistRuntime(workdir=repo)
    unvalidated_rt.prepare_run(spec, str(repo), "r-unval", tmp_path / "unval")
    with pytest.raises(DagValidationError, match="cannot execute before upstream plan node"):
        unvalidated_rt.before_attempt(spec.nodes[1], "write task")

    plan_plus_write_no_dep = [
        _node("plan_node", kind="plan"),
        _node("write_unlinked", kind="write", scope=["a.py"]),
        _node("review", kind="review", deps=["write_unlinked"]),
    ]
    assert "plan workflow requires write nodes to depend on an upstream plan node" in ReviewGateValidator().validate_workflow_structure(plan_plus_write_no_dep)


def test_rtm_gate_nested_test_paths_ast_symbols_and_expected_ids(tmp_path: Path) -> None:
    for nested_caller in (
        "raven/agent/subagent/tests/test_gate.py:foo",
        "raven/agent/subagent/test/helper.py:foo",
        "raven/agent/subagent/conftest.py:foo",
        "raven/agent/subagent/gate_test.py:foo",
    ):
        nested_item = RequirementItem(
            id="R-nested",
            description="nested test caller",
            status="met",
            entrypoint_caller=nested_caller,
            e2e_command="pytest -q",
        )
        with pytest.raises(PrematureCompletionError, match="no_test_only_wiring"):
            RequirementTraceabilityGate.verify_acceptance_receipt(
                AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[nested_item])
            )

    prod_dir = tmp_path / "raven" / "pkg"
    prod_dir.mkdir(parents=True)
    mod_file = prod_dir / "module.py"
    mod_file.write_text(
        "# symbol_in_comment_only\n"
        "LITERAL = 'symbol_in_string_only'\n"
        "class MyService:\n"
        "    def real_method(self):\n"
        "        return 1\n",
        encoding="utf-8",
    )

    for fake_sym in ("symbol_in_comment_only", "symbol_in_string_only", "MyService.missing_method"):
        comment_item = RequirementItem(
            id="R-ast",
            description="comment or string only",
            status="met",
            entrypoint_caller=f"raven/pkg/module.py:{fake_sym}",
            e2e_command="pytest -q",
        )
        with pytest.raises(PrematureCompletionError, match="not found in AST"):
            RequirementTraceabilityGate.verify_acceptance_receipt(
                AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[comment_item]),
                repo_root=tmp_path,
            )

    real_item = RequirementItem(
        id="REQ-1",
        description="real method in AST",
        status="met",
        entrypoint_caller="raven/pkg/module.py:MyService.real_method",
        e2e_command="pytest -q",
    )
    receipt_req1 = AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[real_item])
    assert (
        RequirementTraceabilityGate.verify_acceptance_receipt(
            receipt_req1,
            repo_root=tmp_path,
            expected_requirement_ids=["REQ-1"],
        )
        is receipt_req1
    )

    with pytest.raises(PrematureCompletionError, match=r"Missing required IDs from AcceptanceReceipt: \['REQ-2'\]"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            receipt_req1,
            repo_root=tmp_path,
            expected_requirement_ids=["REQ-1", "REQ-2"],
        )

    with pytest.raises(PrematureCompletionError, match=r"Duplicate requirement IDs in AcceptanceReceipt: \['REQ-1'\]"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[real_item, real_item]),
            repo_root=tmp_path,
        )

    with pytest.raises(PrematureCompletionError, match=r"Duplicate expected_requirement_ids: \['REQ-1'\]"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            receipt_req1,
            repo_root=tmp_path,
            expected_requirement_ids=["REQ-1", "REQ-1"],
        )

    extra_item = RequirementItem(
        id="REQ-EXTRA",
        description="extra method in AST",
        status="met",
        entrypoint_caller="raven/pkg/module.py:MyService.real_method",
        e2e_command="pytest -q",
    )
    req2_item = RequirementItem(
        id="REQ-2",
        description="second method in AST",
        status="met",
        entrypoint_caller="raven/pkg/module.py:MyService.real_method",
        e2e_command="pytest -q",
    )
    with pytest.raises(PrematureCompletionError, match=r"Unexpected extra IDs in AcceptanceReceipt: \['REQ-EXTRA'\]"):
        RequirementTraceabilityGate.verify_acceptance_receipt(
            AcceptanceReceipt(feature_name=BRAND_NAME, requirements=[real_item, req2_item, extra_item]),
            repo_root=tmp_path,
            expected_requirement_ids=["REQ-1", "REQ-2"],
        )


@pytest.mark.slow
def test_requirements_manifest_forces_acceptance_receipt_and_expected_ids_in_run_dag(tmp_path: Path) -> None:
    repo, base = _repo(tmp_path)
    (repo / "raven").mkdir()
    (repo / "raven" / "prod.py").write_text("def prod_entry():\n    return True\n", encoding="utf-8")
    (repo / "requirements_manifest.json").write_text(
        json.dumps({"expected_requirement_ids": ["REQ-1", "REQ-2"]}),
        encoding="utf-8",
    )

    review_spec = SubAgentDagSpec(
        task_summary="manifest review gate",
        nodes=[
            DagNodeSpec(
                id="review",
                subagent="reviewer",
                node_summary="review with manifest",
                prompt_template="review",
                execution_kind="review",
            )
        ],
    )

    no_acc_json = json.dumps(
        {
            "verdict": "approved",
            "reviewed_head_sha": base,
            "findings": [],
            "commands": ["pytest"],
        }
    )
    b_no_acc = _Backend(reply=no_acc_json, agent_name="reviewer")
    res_no_acc = asyncio.run(_run(review_spec, repo, tmp_path, {"review": b_no_acc}, run_id="r-no-acc", reg="h-no-acc"))
    assert {f["node"]: f for f in res_no_acc.files}["review"]["status"] == "failed"
    assert "requires a valid acceptance_receipt" in ({f["node"]: f for f in res_no_acc.files}["review"]["error"] or "")

    partial_acc_json = json.dumps(
        {
            "verdict": "approved",
            "reviewed_head_sha": base,
            "findings": [],
            "commands": ["pytest"],
            "acceptance_receipt": {
                "feature_name": BRAND_NAME,
                "requirements": [
                    {
                        "id": "REQ-1",
                        "description": "first",
                        "status": "met",
                        "entrypoint_caller": "raven/prod.py:prod_entry",
                        "e2e_command": "pytest -q",
                    }
                ],
            },
        }
    )
    b_partial = _Backend(reply=partial_acc_json, agent_name="reviewer")
    res_partial = asyncio.run(_run(review_spec, repo, tmp_path, {"review": b_partial}, run_id="r-partial", reg="h-partial"))
    assert {f["node"]: f for f in res_partial.files}["review"]["status"] == "failed"
    assert "Missing required IDs from AcceptanceReceipt: ['REQ-2']" in (
        {f["node"]: f for f in res_partial.files}["review"]["error"] or ""
    )

    full_acc_json = json.dumps(
        {
            "verdict": "approved",
            "reviewed_head_sha": base,
            "findings": [],
            "commands": ["pytest"],
            "acceptance_receipt": {
                "feature_name": BRAND_NAME,
                "requirements": [
                    {
                        "id": "REQ-1",
                        "description": "first",
                        "status": "met",
                        "entrypoint_caller": "raven/prod.py:prod_entry",
                        "e2e_command": "pytest -q",
                    },
                    {
                        "id": "REQ-2",
                        "description": "second",
                        "status": "met",
                        "entrypoint_caller": "raven/prod.py:prod_entry",
                        "e2e_command": "pytest -q",
                    },
                ],
            },
        }
    )
    b_narrow = _Backend(reply=full_acc_json, agent_name="reviewer")
    b_narrow.expected_requirement_ids = ["REQ-1"]
    res_narrow = asyncio.run(_run(review_spec, repo, tmp_path, {"review": b_narrow}, run_id="r-narrow", reg="h-narrow"))
    assert {f["node"]: f for f in res_narrow.files}["review"]["status"] == "failed"
    assert "Backend expected_requirement_ids cannot override or narrow requirements_manifest.json" in (
        {f["node"]: f for f in res_narrow.files}["review"]["error"] or ""
    )
    rt_narrow = DagSpecialistRuntime(workdir=repo, run_dir=tmp_path / "rt-narrow")
    rt_narrow.prepare_run(review_spec, str(repo), "r-narrow-direct", tmp_path / "rt-narrow")
    with pytest.raises(
        PrematureCompletionError,
        match="Backend expected_requirement_ids cannot override or narrow requirements_manifest.json",
    ):
        rt_narrow.after_success(
            review_spec.nodes[0],
            full_acc_json,
            b_narrow,
            {"review": review_spec.nodes[0]},
            {"review": b_narrow},
        )

    b_full = _Backend(reply=full_acc_json, agent_name="reviewer")
    res_full = asyncio.run(_run(review_spec, repo, tmp_path, {"review": b_full}, run_id="r-full", reg="h-full"))
    assert {f["node"]: f for f in res_full.files}["review"]["status"] == "completed"

    for bad_manifest in ("{bad json", "{}", json.dumps({"expected_requirement_ids": []})):
        (repo / "requirements_manifest.json").write_text(bad_manifest, encoding="utf-8")
        with pytest.raises(PrematureCompletionError, match="Invalid requirements_manifest.json"):
            asyncio.run(
                _run(
                    review_spec,
                    repo,
                    tmp_path,
                    {"review": _Backend(reply=full_acc_json, agent_name="reviewer")},
                    run_id=f"r-bad-{abs(hash(bad_manifest))}",
                    reg=f"h-bad-{abs(hash(bad_manifest))}",
                )
            )


@pytest.mark.slow
def test_active_cto_strategic_alignment_gate_and_cross_workstream_synergy_propagation(tmp_path: Path) -> None:
    from raven.agent.subagent.oh_my_raven import (
        CtoAlignmentGate,
        CtoStrategicBrief,
        SkillPackResolver,
        SpecialistDefinition,
    )

    pb = build_oh_my_raven_playbook()
    assert pb.nodes[0].id == "cto_strategy"
    assert pb.nodes[0].subagent == "CTO"
    assert pb.nodes[0].execution_kind == "plan"
    assert "cto_strategy" in pb.nodes[1].depends_on
    assert "cto_strategy" in pb.nodes[2].depends_on

    repo, _ = _repo(tmp_path)
    (repo / "charter_context.json").write_text(
        json.dumps(
            {
                "memories": [
                    "cto_strategy open thread t18: decouple specialists from harnesses and align with EverOS long-term vision"
                ]
            }
        ),
        encoding="utf-8",
    )

    spec = SubAgentDagSpec(
        task_summary="cto strategic alignment and write slice",
        nodes=[
            DagNodeSpec(
                id="cto_strategy",
                subagent="CTO",
                node_summary="cto_strategy cross-workstream alignment",
                prompt_template="cto_strategy plan",
                execution_kind="plan",
            ),
            DagNodeSpec(
                id="slice_a",
                subagent="specialist",
                node_summary="implement slice_a",
                prompt_template="implement slice_a",
                execution_kind="write",
                file_scope=["slice_a.txt"],
                depends_on=["cto_strategy"],
            ),
        ],
    )

    # 1) CTO node returning only ConsequenceMatrix without workstream_synergies / long_term_goal_alignment fails closed!
    matrix_only_json = json.dumps(
        {
            "consequence_matrix": {
                "affected_files": ["slice_a.txt"],
                "blast_radius": "local",
                "failure_modes": ["none"],
                "rollback_plan": "git revert",
                "contract_changes": ["none"],
                "validation_plan": ["pytest -q"],
            }
        }
    )
    bad_cto = _Backend(reply=matrix_only_json, agent_name="CTO")
    write_slice = _Backend(commit="slice_a.txt", agent_name="specialist")
    res_missing_synergy = asyncio.run(
        _run(
            spec,
            repo,
            tmp_path,
            {"cto_strategy": bad_cto, "slice_a": write_slice},
            run_id="r-cto-bad",
            reg="h-cto-bad",
        )
    )
    bad_map = {f["node"]: f for f in res_missing_synergy.files}
    assert bad_map["cto_strategy"]["status"] == "failed"
    assert "completed without a valid CtoStrategicBrief" in (bad_map["cto_strategy"]["error"] or "")
    assert bad_map["slice_a"]["status"] == "skipped"
    assert write_slice.calls == []

    # 2) Valid CtoStrategicBrief passes gate and propagates into downstream write node prompt!
    valid_cto_json = json.dumps(
        {
            "cto_strategic_brief": {
                "workstream_synergies": [
                    "t18: Unify harness-free specialist pool across Raven-Code, Claude Code, Codex, and Hermes",
                    "t7: Share portable skill_packs (superpowers, ecc, mattpocock) across all runtime harnesses",
                ],
                "long_term_goal_alignment": "100% decoupling of logical Specialists (microservices) from Harnesses (VMs)",
                "cross_project_dependencies": ["raven.config.schema", "raven.agent.subagent.registry"],
                "consequence_matrix": {
                    "affected_files": ["slice_a.txt"],
                    "blast_radius": "subagent orchestration layer",
                    "failure_modes": ["missing harness pool"],
                    "rollback_plan": "git worktree remove",
                    "contract_changes": ["added kind='specialist' and CtoStrategicBrief"],
                    "validation_plan": ["pytest -o addopts=\"\" -q"],
                },
            }
        }
    )
    good_cto = _Backend(reply=valid_cto_json, agent_name="CTO")
    good_slice = _Backend(commit="slice_a.txt", agent_name="specialist")
    good_slice.specialist_definition = SpecialistDefinition(
        name="Engineer - Superpowers",
        role_prompt="Execute with TDD discipline.",
        skill_packs=("superpowers", "ecc"),
        mcp_servers=("consensus",),
    )

    res_good_cto = asyncio.run(
        _run(
            spec,
            repo,
            tmp_path,
            {"cto_strategy": good_cto, "slice_a": good_slice},
            run_id="r-cto-good",
            reg="h-cto-good",
        )
    )
    good_map = {f["node"]: f for f in res_good_cto.files}
    assert good_map["cto_strategy"]["status"] == "completed"
    assert good_map["slice_a"]["status"] == "completed"
    assert len(good_slice.calls) == 1

    # Verify CTO received the cross-workstream context charter AND downstream slice_a received both the CTO Strategic Brief and portable Specialist Bundle
    cto_prompt = good_cto.calls[0]["task"]
    assert "[BEGIN UNTRUSTED CONTEXT CHARTER]" in cto_prompt
    assert "open thread t18" in cto_prompt

    slice_prompt = good_slice.calls[0]["task"]
    assert "[BEGIN CTO STRATEGIC BRIEF]" in slice_prompt
    assert "100% decoupling of logical Specialists (microservices) from Harnesses (VMs)" in slice_prompt
    assert "t18: Unify harness-free specialist pool" in slice_prompt
    assert "[BEGIN SPECIALIST BUNDLE: Engineer - Superpowers]" in slice_prompt
    assert "Skill Pack: superpowers" in slice_prompt
    assert "Skill Pack: ecc" in slice_prompt

    brief_obj = CtoAlignmentGate.verify_cto_brief(valid_cto_json)
    assert isinstance(brief_obj, CtoStrategicBrief)
    assert len(brief_obj.workstream_synergies) == 2
    rendered_bundle = SkillPackResolver.render_specialist_bundle(good_slice.specialist_definition, "do task")
    assert "Attached Specialist MCP Servers" in rendered_bundle
    assert "consensus" in rendered_bundle

    # Regression 3: Word-boundary matching in CtoAlignmentGate.is_cto_node()
    reactor_node = DagNodeSpec(
        id="reactor_analysis",
        subagent="planner",
        node_summary="reactor analysis",
        prompt_template="reactor analysis",
        execution_kind="plan",
    )
    director_node = DagNodeSpec(
        id="director_plan",
        subagent="factory_planner",
        node_summary="director plan",
        prompt_template="director plan",
        execution_kind="plan",
    )
    cto_node = DagNodeSpec(
        id="cto_strategy",
        subagent="CTO",
        node_summary="cto strategy",
        prompt_template="cto strategy",
        execution_kind="plan",
    )
    assert CtoAlignmentGate.is_cto_node(reactor_node) is False
    assert CtoAlignmentGate.is_cto_node(director_node) is False
    assert CtoAlignmentGate.is_cto_node(cto_node) is True
    import oh_my_raven
    from oh_my_raven import specialist_support as legacy_specialist_support
    assert oh_my_raven.BRAND_NAME == "oh-my-raven"
    assert legacy_specialist_support.BRAND_NAME == "oh-my-raven"

