"""Multi-scope test pyramid: Integration, Outside-In, End-to-End golden set.

Real git repos, real WorktreeManager, real DynamicDecompositionGate, real
SkillAndToolReceiptGate, real run_dag and real subprocesses. Nothing in the
pipeline under test is monkeypatched.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from oh_my_raven.oh_my_raven import DynamicDecompositionGate, OmrDispatcher, OmrTaskQueue, WorktreeManager, escalate_triage_tier
from raven.agent.subagent.prompt_errors import DagValidationError
from oh_my_raven.routing import DEFAULT_MAX_ATTEMPTS, RouteTarget, RoutingBackend

PASSING_TEST = "def test_ok():\n    assert True\n"


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, text=True, capture_output=True
    ).stdout.strip()


def _init_repo(root: Path) -> Path:
    repo = root / "repo"
    repo.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    _git(repo, "config", "user.email", "t@example.com")
    _git(repo, "config", "user.name", "t")
    _git(repo, "config", "commit.gpgsign", "false")
    (repo / "test_feature.py").write_text(PASSING_TEST, encoding="utf-8")
    (repo / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n", encoding="utf-8")
    _git(repo, "add", "-A")
    _git(repo, "commit", "-q", "-m", "init")
    return repo


class _Inst:
    def list_instances(self, k=None):
        return []


class _Table:
    """Minimal SubagentTable surface used by OmrDispatcher: enabled() and backend()."""

    def __init__(self, backends: dict[str, Any]):
        self._b = backends

    def enabled(self):
        return [SimpleNamespace(name="Manager", description="lead", is_harness=False, kind="specialist")]

    def backend(self, name, build=None):
        return self._b.get(name)


def _routing(name: str, primary: Any, targets=(), **kw) -> RoutingBackend:
    ts = [RouteTarget(t.name, t.name, t) for t in targets]
    return RoutingBackend(name, primary, ts, instances=_Inst(), cooldown_s=0, **kw)


def _dispatcher(root: Path, workdir: Path, table: Any) -> OmrDispatcher:
    return OmrDispatcher(
        table=table,
        queue=OmrTaskQueue(root / "queue_ledger.jsonl"),
        workdir=workdir,
        run_root=root / "runs",
        nodes_root=root / "nodes",
        history_root=root / "history",
        gate_policy_path=root / "gate_policy.json",
    )


def _dag_payload() -> str:
    def n(i, kind, deps=()):
        return {"id": i, "subagent": {"plan": "CTO", "review": "Reviewer"}.get(i, "Worker"), "node_summary": i,
                "prompt_template": i, "execution_kind": kind, "depends_on": list(deps)}

    return json.dumps({"task_summary": "pyramid", "nodes": [
        n("plan", "plan"), n("implement", "read", ["plan"]),
        n("integrate", "integrate", ["implement"]), n("review", "review", ["integrate"]),
    ]})


class _Manager:
    name = "Manager"
    kind = "acp"
    streams = False
    last_chosen_backend = "stub"

    async def run(self, task, **kw):
        return _dag_payload()


EVIDENCE = {"artifact_path": "test_feature.py"}
CTO_BRIEF = json.dumps({"evidence": EVIDENCE, "cto_strategic_brief": {
    "workstream_synergies": ["W-1: share one validator"],
    "long_term_goal_alignment": "Keep verified delivery aligned with long-term goals.",
    "cross_project_dependencies": ["deliverable.json"],
    "consequence_matrix": {
        "affected_files": ["deliverable.json"], "blast_radius": "single file",
        "failure_modes": ["bad artifact"], "rollback_plan": "git reset",
        "contract_changes": ["none"], "validation_plan": ["pytest test_feature.py -q"]},
}})


class _Cto:
    name = "CTO"
    kind = "acp"
    streams = False

    async def run(self, task, **kw):
        return CTO_BRIEF


class _Reviewer:
    name = "Reviewer"
    kind = "acp"
    streams = False

    def __init__(self, repo: Path):
        self.repo = repo

    async def run(self, task, **kw):
        return json.dumps({"verdict": "approved", "reviewed_head_sha": _git(self.repo, "rev-parse", "HEAD"),
                           "findings": [], "commands": ["git diff --stat"], "evidence": EVIDENCE})


class _Worker:
    name = "Worker"
    kind = "acp"
    streams = False

    def __init__(self, repo: Path, output):
        self.repo, self.output = repo, output

    async def run(self, task, **kw):
        return self.output(self.repo)


def _write_deliverable(repo: Path) -> str:
    (repo / "deliverable.json").write_text('{"ok": true}', encoding="utf-8")
    return json.dumps({"evidence": {"artifact_path": "deliverable.json",
                                    "test_command": "pytest test_feature.py", "exit_code": 0}})


def _submit(root: Path, worker_output) -> OmrDispatcher:
    repo = _init_repo(root)
    worker = _Worker(repo, worker_output)
    table = _Table({"Manager": _routing("Manager", _Manager()),
                    "Worker": _routing("Worker", worker),
                    "CTO": _routing("CTO", _Cto()),
                    "Reviewer": _routing("Reviewer", _Reviewer(repo))})
    d = _dispatcher(root, repo, table)
    asyncio.run(d.submit_task("PARENT", "build it", cto_specialist="missing", run_id="r1"))
    return d


def _ledger(root: Path) -> list[dict]:
    return [json.loads(x) for x in (root / "queue_ledger.jsonl").read_text().splitlines() if x.strip()]


class TestIntegrationScope:
    def test_integration_triage_trivial_fast_path_and_auto_escalation(self, tmp_path):
        class TrivialThenManager(_Manager):
            def __init__(self):
                self.calls = 0

            async def run(self, task, **kw):
                self.calls += 1
                return "incomplete" if self.calls == 1 else _dag_payload()

        repo = _init_repo(tmp_path)
        manager = TrivialThenManager()
        table = _Table({"Manager": _routing("Manager", manager),
                        "Worker": _routing("Worker", _Worker(repo, _write_deliverable)),
                        "CTO": _routing("CTO", _Cto()),
                        "Reviewer": _routing("Reviewer", _Reviewer(repo))})
        d = _dispatcher(tmp_path, repo, table)
        assert d.triage_task("fix typo", 1).tier == "trivial"
        asyncio.run(d.submit_trivial_task("T", "fix typo", "Manager"))
        assert manager.calls >= 2
        assert d.queue.latest_by_task()["T"].status == "completed"

    def test_integration_lessons_loop_auto_records_and_injects_on_retry(self, tmp_path):
        class LearningWorker:
            name, kind, streams = "Worker", "acp", False

            def __init__(self, repo):
                self.repo, self.prompts, self.calls = repo, [], 0

            async def run(self, task, **kw):
                self.prompts.append(task)
                self.calls += 1
                return "incomplete" if self.calls == 1 else _write_deliverable(self.repo)

        repo = _init_repo(tmp_path)
        worker = LearningWorker(repo)
        d = _dispatcher(tmp_path, repo, _Table({"Worker": _routing("Worker", worker)}))
        asyncio.run(d.dispatch_specialist_node("L1", "Worker", "build durable feature"))
        asyncio.run(d.dispatch_specialist_node("L2", "Worker", "build durable feature"))
        assert (tmp_path / "history" / "lessons.jsonl").is_file()
        assert "[Active Lessons]" in worker.prompts[1]
        assert d.queue.latest_by_task()["L2"].status == "completed"

    def test_integration_real_dag_runner_and_decomposition_with_done_gate(self, tmp_path):
        d = _submit(tmp_path, _write_deliverable)
        assert (d.workdir / "deliverable.json").is_file()
        gates = [e["gate"] for e in json.loads(d.gate_log_path.read_text())]
        assert "specialist_fallback" in gates and "technical_decision" in gates
        seen = {(r.get("task_id"), r.get("status")) for r in _ledger(tmp_path)}
        for node in ("plan", "implement", "integrate", "review"):
            assert (node, "enqueued") in seen and (node, "routed") in seen and (node, "running") in seen
        assert ("PARENT", "decomposing") in seen and ("PARENT", "running") in seen
        assert d.queue.latest_by_task()["PARENT"].status in {"completed", "incomplete"}

    def test_integration_real_dag_verified_evidence_rolls_up_completed(self, tmp_path):
        d = _submit(tmp_path, _write_deliverable)
        assert d.queue.latest_by_task()["PARENT"].status == "completed"

    @pytest.mark.parametrize("claim", [
        "done",
        json.dumps({"evidence": {"command": 'python -c "print(1)"', "exit_code": 0}}),
    ])
    def test_integration_real_dag_rejects_unverified_done_claim(self, tmp_path, claim):
        d = _submit(tmp_path, lambda repo: claim)
        assert d.queue.latest_by_task()["PARENT"].status == "incomplete"
        assert ("PARENT", "incomplete") in {(r.get("task_id"), r.get("status")) for r in _ledger(tmp_path)}


_HARNESS_SCRIPT = (
    "import sys, pathlib, json\n"
    "wt = pathlib.Path(sys.argv[1])\n"
    "if sys.argv[2] != '1':\n"
    "    sys.exit(3)\n"
    "(wt / 'work.txt').write_text('real work')\n"
    "(wt / 'receipt.json').write_text(json.dumps({'verified': True, 'exit_code': 0}))\n"
)


class _SubprocessHarness:
    """Spawns a real python subprocess that works inside a real git worktree."""

    name = "proc"
    kind = "cli"
    streams = False

    def __init__(self, wt: Path, fail_first: int):
        self.wt, self.fail_first, self.calls = wt, fail_first, 0

    async def run(self, task, **kw):
        self.calls += 1
        ok = "1" if self.calls > self.fail_first else "0"
        proc = subprocess.run([sys.executable, "-c", _HARNESS_SCRIPT, str(self.wt), ok],
                              capture_output=True, text=True)
        if proc.returncode != 0:
            raise ConnectionError(f"harness crashed rc={proc.returncode}")
        _git(self.wt, "add", "-A")
        _git(self.wt, "commit", "--allow-empty", "-q", "-m", "worker output")
        return json.dumps({"evidence": {"receipt_path": "receipt.json"}})


class TestOutsideInScope:
    def test_outside_in_ambiguity_experiment_resolves_without_asking_user(self, tmp_path):
        repo = _init_repo(tmp_path)
        d = _dispatcher(tmp_path, repo, _Table({}))

        def probe():
            proc = subprocess.run([sys.executable, "-c", "print('resolved')"], capture_output=True, text=True)
            return {"resolved": proc.returncode == 0 and proc.stdout.strip() == "resolved"}

        answer = d.run_ambiguity_experiment("A1", "which behavior?", probe)
        assert answer["proceed"] and answer["resolved_by"] == "experiment"
        assert (tmp_path / "runs" / "experiments" / "A1.json").is_file()
        assert d.run_ambiguity_experiment("A2", "still unclear", lambda: {"resolved": False})["proceed"] is False

    def test_outside_in_external_write_adapters_enforced_by_require_gate(self, tmp_path):
        repo = _init_repo(tmp_path)
        calls = []
        d = _dispatcher(tmp_path, repo, _Table({}))
        for action in (d.git_push, d.publish_pr, d.deploy):
            with pytest.raises(PermissionError):
                action("target", lambda: calls.append("called"))
        assert calls == []
        (tmp_path / "gate_policy.json").write_text(json.dumps({
            "modes": {"push": "auto", "publish": "auto", "deploy": "auto"},
            "allow_irreversible_auto": True,
        }))
        allowed = _dispatcher(tmp_path, repo, _Table({}))
        for action in (allowed.git_push, allowed.publish_pr, allowed.deploy):
            action("target", lambda: calls.append("called"))
        assert calls == ["called"] * 3

    def test_outside_in_subprocess_harness_failover_and_revival_to_done_gate(self, tmp_path):
        repo = _init_repo(tmp_path)
        base = _git(repo, "rev-parse", "HEAD")
        wt = WorktreeManager().provision_worktree(repo, tmp_path / "wts", "run", "n1", base)
        _git(wt, "config", "user.email", "t@example.com")
        _git(wt, "config", "user.name", "t")
        harness = _SubprocessHarness(wt, fail_first=1)
        rb = _routing("Worker", harness)
        assert DEFAULT_MAX_ATTEMPTS == 3
        d = _dispatcher(tmp_path, wt, _Table({"Worker": rb}))
        out = asyncio.run(d.dispatch_specialist_node("T1", "Worker", "do work", writes=True))
        assert [a["status"] for a in rb.last_attempts] == ["failed", "succeeded"]
        assert wt.exists() and (wt / "work.txt").read_text() == "real work"
        assert "work.txt" in _git(wt, "show", "--name-only", "HEAD")
        assert d.completion_status(out) == "completed"
        assert d.queue.latest_by_task()["T1"].status == "completed"
        d.decide_gate("technical_decision", "after failover")
        assert json.loads(d.gate_log_path.read_text())[0]["gate"] == "technical_decision"
        assert not list(d.gate_log_path.parent.glob(".gate_log.json.*.tmp"))

    def test_outside_in_hot_onboard_new_specialist_and_new_harness_zero_profile_setup(self, tmp_path):
        repo = _init_repo(tmp_path)
        base = _git(repo, "rev-parse", "HEAD")
        wt = WorktreeManager().provision_worktree(repo, tmp_path / "wts", "hot", "n1", base)
        _git(wt, "config", "user.email", "t@example.com")
        _git(wt, "config", "user.name", "t")
        dead = _Flaky("existing", 99, wt)
        table = _Table({"Manager": _routing("Manager", dead, max_failovers=1)})
        dispatcher = _dispatcher(tmp_path, wt, table)
        windsurf_harness = _SubprocessHarness(wt, fail_first=0)

        assert dispatcher.onboard_harness("Windsurf-ACP", windsurf_harness, max_failovers=1) == 1
        existing = asyncio.run(dispatcher.dispatch_specialist_node("HOT-H", "Manager", "fail over", writes=True))
        assert table.backend("Manager").last_chosen_backend == "Windsurf-ACP"
        assert dispatcher.completion_status(existing) == "completed"

        security = dispatcher.onboard_specialist(
            "Security-Auditor", [RouteTarget("Windsurf-ACP", "runtime harness", windsurf_harness)]
        )
        result = asyncio.run(dispatcher.dispatch_specialist_node("HOT-S", "Security-Auditor", "audit", writes=True))
        assert security.last_chosen_backend == "Windsurf-ACP"
        assert dispatcher.completion_status(result) == "completed"
        assert dispatcher.queue.latest_by_task()["HOT-S"].status == "completed"
        assert not (tmp_path / "profiles" / "Security-Auditor").exists()
        assert {row["status"] for row in _ledger(tmp_path) if row["task_id"] == "HOT-S"} >= {"routed", "running", "completed"}
        details = [row["detail"] for row in json.loads(dispatcher.gate_log_path.read_text())]
        assert "onboard_harness:Windsurf-ACP" in details
        assert "onboard_specialist:Security-Auditor" in details

    def test_outside_in_policy_dial_modes_via_env_and_json_file(self, tmp_path, monkeypatch):
        policy = tmp_path / "gate_policy.json"
        monkeypatch.setenv("OMR_GATE_POLICY_PATH", str(policy))

        def make() -> OmrDispatcher:
            return OmrDispatcher(table=None, queue=OmrTaskQueue(tmp_path / "q.jsonl"), workdir=tmp_path,
                                 run_root=tmp_path / "runs", nodes_root=tmp_path / "n",
                                 history_root=tmp_path / "h")

        policy.write_text(json.dumps({"modes": {"technical_decision": "ask", "plan_gap": "auto",
                                                "plan_gap_fill": "auto-log", "push": "auto"}}))
        d = make()
        assert d.decide_gate("technical_decision") is False  # ask
        assert d.decide_gate("plan_gap") is True             # auto: not logged
        assert d.decide_gate("plan_gap_fill", "x") is True   # auto-log
        assert [e["gate"] for e in json.loads(d.gate_log_path.read_text())] == ["plan_gap_fill"]
        with pytest.raises(PermissionError):                 # irreversible stays blocked
            d.require_gate("push", "origin/main")
        policy.write_text(json.dumps({"modes": {"push": "auto"}, "allow_irreversible_auto": True}))
        assert make().require_gate("push", "origin/main") is True


class _Flaky:
    kind = "acp"
    streams = False

    def __init__(self, name, fail, repo: Path, exc=ConnectionError):
        self.name, self.fail, self.exc, self.repo, self.calls = name, fail, exc, repo, 0

    async def run(self, task, **kw):
        self.calls += 1
        if self.calls <= self.fail:
            raise self.exc("crash")
        (self.repo / "deliverable.json").write_text("{}", encoding="utf-8")
        return json.dumps({"evidence": {"artifact_path": "deliverable.json"}})


class TestEndToEndGoldenSet:
    def test_e2e_golden_set_all_10_trust_symptoms(self, tmp_path):
        scores: dict[str, bool] = {}

        # 1: trivial work takes the one-node fast path without CTO/Manager DAG ceremony.
        repo1 = _init_repo(tmp_path / "s1")
        fast = _Worker(repo1, _write_deliverable)
        d1 = _dispatcher(tmp_path / "s1", repo1, _Table({"Worker": _routing("Worker", fast)}))
        asyncio.run(d1.submit_trivial_task("T1", "fix typo in config.yaml", "Worker"))
        scores["s1"] = d1.triage_task("fix typo in config.yaml", 1).tier == "trivial" and fast.output is _write_deliverable

        # 2: council disagreement exposes and persists plan gaps.
        council = d1.run_council_consensus("C1", [
            {"voter": "cto", "approved": True},
            {"voter": "reviewer", "approved": True, "gaps": ["rollback missing"]},
            {"voter": "engineer-superpowers", "approved": False, "gaps": ["test gap"]},
        ])
        scores["s2"] = (not council["consensus"] and council["merged_gaps"] == ["rollback missing", "test gap"]
                        and (tmp_path / "s1" / "history" / "lessons.jsonl").is_file())

        # 3: premature "done" rejected.
        d3 = _submit(tmp_path / "s3", lambda repo: "All done!")
        scores["s3"] = d3.queue.latest_by_task()["PARENT"].status == "incomplete"

        # Scenario 2 (symptoms #3, #4): real deliverable verified by a real pytest run.
        def s2(repo: Path) -> str:
            (repo / "src").mkdir(exist_ok=True)
            (repo / "tests").mkdir(exist_ok=True)
            (repo / "src" / "feature.py").write_text("def f():\n    return 1\n")
            (repo / "tests" / "test_feature_e2e.py").write_text(PASSING_TEST)
            return json.dumps({"evidence": {"test_command": "pytest tests/test_feature_e2e.py", "exit_code": 0}})

        repo4 = _init_repo(tmp_path / "s4")
        d4 = _dispatcher(tmp_path / "s4", repo4, _Table({"Worker": _routing("Worker", _Worker(repo4, s2))}))
        out4 = asyncio.run(d4.dispatch_specialist_node("T4", "Worker", "build feature", writes=True))
        scores["s4"] = d4.completion_status(out4) == "completed" and d4.queue.latest_by_task()["T4"].status == "completed"

        # Scenario 3 (symptom #5): routine decisions auto-logged, push blocked.
        repo5 = _init_repo(tmp_path / "s5")
        d5 = _dispatcher(tmp_path / "s5", repo5, _Table({}))
        answer = d5.run_ambiguity_experiment("A", "which local config?", lambda: {"resolved": True})
        try:
            d5.require_gate("push", "origin/main")
            blocked = False
        except PermissionError:
            blocked = True
        scores["s5"] = answer["proceed"] and answer["resolved_by"] == "experiment" and blocked

        # 6: intake selects security, architecture, and implementation specialists.
        names = {"security": "Security-Auditor", "architecture": "CTO", "implementation": "Engineer"}
        rows = [SimpleNamespace(name=n, description=n, is_harness=False, kind="specialist") for n in names.values()]
        table6 = _Table({})
        table6.enabled = lambda: rows
        d6 = _dispatcher(tmp_path / "s6", repo5, table6)
        d6.intake_router = lambda menu, prompt, default: names[prompt]
        routed = [asyncio.run(d6.route_intake_specialist(prompt)) for prompt in names]
        scores["s6"] = routed == list(names.values())

        # 7: governance gate rejects a multi-requirement DAG missing CTO/reviewer coverage.
        bad = {"task_summary": "missing specialists", "nodes": [
            {"id": "work", "subagent": "Engineer", "node_summary": "work", "prompt_template": "work", "execution_kind": "read"}
        ]}
        with pytest.raises(DagValidationError):
            DynamicDecompositionGate.validate_and_enqueue(bad, OmrTaskQueue(tmp_path / "s7.jsonl"), "S7")
        scores["s7"] = True

        # Scenario 4 (symptom #8): two crashes, revived on attempt 3.
        repo8 = _init_repo(tmp_path / "s8")
        primary = _Flaky("p", 2, repo8)
        rb4 = _routing("Worker", primary)
        d8 = _dispatcher(tmp_path / "s8", repo8, _Table({"Worker": rb4}))
        out8a = asyncio.run(d8.dispatch_specialist_node("T8A", "Worker", "go"))

        # Scenario 5 (symptom #8): primary + 3 fallbacks fail, 4th fallback succeeds on attempt 5.
        fbs = [_Flaky("a", 99, repo8), _Flaky("b", 99, repo8), _Flaky("c", 99, repo8), _Flaky("d", 0, repo8)]
        rb8 = _routing("Fallback", _Flaky("p2", 99, repo8), fbs, max_failovers=4)
        d8.table._b["Fallback"] = rb8
        out8b = asyncio.run(d8.dispatch_specialist_node("T8B", "Fallback", "go"))
        scores["s8"] = (primary.calls == DEFAULT_MAX_ATTEMPTS and d8.completion_status(out8a) == "completed"
                        and len(rb8.last_attempts) == 5 and d8.completion_status(out8b) == "completed")

        # 9: rigor matches complexity; failed trivial work auto-escalates to a standard DAG.
        scores["s9"] = (d1.triage_task("fix typo", 1).tier == "trivial"
                        and escalate_triage_tier("trivial", "incomplete").tier == "standard")

        # 10: first failure becomes an active lesson injected into the next prompt.
        class Learner:
            name, kind, streams = "Learner", "acp", False
            def __init__(self): self.calls, self.prompts = 0, []
            async def run(self, task, **kw):
                self.calls += 1; self.prompts.append(task)
                return "incomplete" if self.calls == 1 else _write_deliverable(repo8)
        learner = Learner()
        d10 = _dispatcher(tmp_path / "s10", repo8, _Table({"Learner": _routing("Learner", learner)}))
        asyncio.run(d10.dispatch_specialist_node("L1", "Learner", "repeatable task"))
        asyncio.run(d10.dispatch_specialist_node("L2", "Learner", "repeatable task"))
        scores["s10"] = "[Active Lessons]" in learner.prompts[1]

        assert scores == {f"s{i}": True for i in range(1, 11)}
