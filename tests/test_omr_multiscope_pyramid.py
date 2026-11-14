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

from oh_my_raven.oh_my_raven import DynamicDecompositionGate, OmrDispatcher, OmrTaskQueue, WorktreeManager
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

    def test_outside_in_per_specialist_folder_subprocess_and_skill_mcp_failover(self, tmp_path):
        repo = _init_repo(tmp_path)
        base = _git(repo, "rev-parse", "HEAD")
        wt = WorktreeManager().provision_worktree(repo, tmp_path / "wts", "run-spec-dir", "slice-1", base)
        _git(wt, "config", "user.email", "t@example.com")
        _git(wt, "config", "user.name", "t")
        pack = tmp_path / "ext_skills" / "superpowers"
        (pack / "scripts").mkdir(parents=True)
        (pack / "SKILL.md").write_text("# Superpowers", encoding="utf-8")
        (pack / "scripts" / "verify_tdd.py").write_text("print('SUPERPOWERS_SCRIPT_VERIFIED')\n", encoding="utf-8")
        seen: dict[str, Any] = {}

        class _Claude:
            name, kind, streams = "Claude-Code", "cli", False

            async def run(self, task, **kw):
                seen["claude_args"] = list(kw["session_cli_args"])
                seen["stale_cleared"] = (not getattr(self, "last_skill_evidence", None)
                                         and not getattr(self, "last_invoked_mcps", None))
                return json.dumps({"invoked_mcps": [], "skill_evidence": {}})

        class _Copilot:
            name, kind, streams = "Copilot-CLI", "cli", False

            async def run(self, task, **kw):
                args = list(kw["session_cli_args"])
                assert "--add-dir" in args and "--enable-mcp-server" in args
                assert Path(kw["agents_md_path"]).read_text(encoding="utf-8")
                script = Path(kw["specialist_dir"]) / "skills" / "superpowers" / "scripts" / "verify_tdd.py"
                proc = subprocess.run([sys.executable, str(script)], capture_output=True, text=True, check=True)
                seen["script"] = script
                (wt / "verified_feature.py").write_text("X = 1\n", encoding="utf-8")
                _git(wt, "add", "-A")
                _git(wt, "commit", "-q", "-m", "verified feature")
                return json.dumps({
                    "evidence": {"artifact_path": "verified_feature.py"},
                    "invoked_mcps": ["codebase-memory", "custom-audit-mcp"], "invoked_tools": ["read_file"],
                    "skill_evidence": {"superpowers": {"command": "python verify_tdd.py", "exit_code": 0,
                                                       "output_ref": proc.stdout.strip()}},
                })

        flaky, good = _Claude(), _Copilot()
        flaky.last_skill_evidence = {"superpowers": {"command": "stale", "exit_code": 0, "output_ref": "stale"}}
        flaky.last_invoked_mcps = ("codebase-memory",)
        dispatcher = _dispatcher(tmp_path, wt, _Table({}))
        tdd_spec = dispatcher.onboard_specialist(
            "TDD-Implementer",
            [RouteTarget("Claude-Code", "primary", flaky), RouteTarget("Copilot-CLI", "fallback", good)],
            specialist_definition={"name": "TDD-Implementer", "role_prompt": "Strict TDD specialist.",
                                   "skill_packs": ["superpowers"], "skill_dirs": [str(pack.parent)],
                                   "mcp_servers": ["codebase-memory", "custom-audit-mcp"], "disabled_mcp_servers": ["github"],
                                   "strict_mcp_scope": True,
                                   "startup_skills": ["andrej-karpathy-skill", "autonomous-delivery"],
                                   "startup_mcp_servers": ["everos"]},
            require_skill_evidence=True,
        )
        sdir = Path(tdd_spec.specialist_definition.specialist_dir)
        assert not (sdir / "skills" / "andrej-karpathy-skill").exists()
        assert (sdir / "skills" / "superpowers" / "SKILL.md").is_file()
        asyncio.run(dispatcher.dispatch_specialist_node(
            "SPEC-DIR-1", "TDD-Implementer", "Implement feature with superpowers", workspace=wt, writes=True))
        assert "--add-dir" in seen["claude_args"] and "--strict-mcp-config" in seen["claude_args"]
        assert seen["stale_cleared"] is True
        assert tdd_spec.last_attempts[0]["error_class"] == "SkillContractViolationError"
        assert tdd_spec.last_chosen_backend == "Copilot-CLI"
        assert seen["script"].is_file() and wt not in seen["script"].parents
        assert _git(wt, "status", "--porcelain", "-uall") == ""
        assert not (wt / "AGENTS.md").exists() and not (wt / "mcp.json").exists()

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
        duplicate = dispatcher.onboard_specialist(
            "Security-Auditor", [RouteTarget("Windsurf-ACP", "duplicate", windsurf_harness),
                                 RouteTarget("Second-Harness", "new", windsurf_harness)]
        )
        assert duplicate is security
        assert [target.name for target in security._targets] == ["Windsurf-ACP", "Second-Harness"]
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
        def write_config(repo: Path) -> str:
            (repo / "cfg.txt").write_text("timeout=30\n", encoding="utf-8")
            return json.dumps({"artifact_path": "cfg.txt"})

        fast = _Worker(repo1, write_config)
        d1 = _dispatcher(tmp_path / "s1", repo1, _Table({"backend-developer": _routing("backend-developer", fast)}))
        trivial_res = asyncio.run(d1.submit_trivial_task("golden-s1", "Update timeout in config.yaml", "backend-developer"))
        trivial_entry = d1.queue.latest_by_task()["golden-s1"]
        scores["s1"] = (trivial_res["status"] == "completed"
                        and trivial_res["triage_tier"] == "trivial"
                        and trivial_res.get("escalated") is False
                        and (repo1 / "cfg.txt").is_file()
                        and trivial_entry.status == "completed"
                        and trivial_entry.assigned_specialist == "backend-developer")

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

        # 9: incomplete trivial work auto-escalates through standard DAG orchestration.
        repo9 = _init_repo(tmp_path / "s9")

        class IncompleteThenManager(_Manager):
            name = "backend-developer"
            def __init__(self): self.calls = 0
            async def run(self, task, **kw):
                self.calls += 1
                return "Needs full multi-node architecture" if self.calls == 1 else _dag_payload()

        def write_standard(repo: Path) -> str:
            (repo / "s9_standard.txt").write_text("standard path\n", encoding="utf-8")
            return json.dumps({"artifact_path": "s9_standard.txt"})

        d9 = _dispatcher(tmp_path / "s9", repo9, _Table({
            "backend-developer": _routing("backend-developer", IncompleteThenManager()),
            "Worker": _routing("Worker", _Worker(repo9, write_standard)),
            "CTO": _routing("CTO", _Cto()),
            "Reviewer": _routing("Reviewer", _Reviewer(repo9)),
        }))
        s9_result = asyncio.run(d9.submit_trivial_task("golden-s9", "Design and implement architecture", "backend-developer"))
        scores["s9"] = (s9_result["triage_tier"] == "standard"
                        and s9_result["escalated"] is True
                        and s9_result["status"] == "completed"
                        and (repo9 / "s9_standard.txt").is_file())

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


class _ModelHarness:
    """Records per-call session args; optionally raises."""

    kind, streams = "cli", False

    def __init__(self, name, models, exc=None):
        self.name, self.supported_models, self.exc, self.args, self.calls = name, tuple(models), exc, [], 0

    async def run(self, task, **kw):
        self.calls += 1
        self.args.append(list(kw.get("session_cli_args") or ()))
        if self.exc is not None:
            raise self.exc
        return "done"


def _read_telemetry(path: Path) -> list[dict]:
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


class TestR24ModelConfig:
    def _rb(self, tmp_path, h1, h2, cfg):
        from oh_my_raven.oh_my_raven import SpecialistDefinition, SpecialistFolderManager
        spec = SpecialistFolderManager.ensure_specialist_dir(
            SpecialistDefinition(name="Modeler", model_config=cfg), tmp_path / "specialists")
        tel = tmp_path / "specialist_telemetry.jsonl"
        rb = RoutingBackend(h1.name, h1, [RouteTarget(h2.name, h2.name, h2)],
                            instances=_Inst(), cooldown_s=0, specialist_definition=spec, telemetry_path=tel)
        return rb, tel

    @staticmethod
    def _cfg():
        from oh_my_raven.oh_my_raven import SpecialistModelConfig
        return SpecialistModelConfig(preferred_models=("claude-sonnet-4-6",), fallback_models=("gpt-5.4",),
                                     reasoning_effort="high")

    def test_r24_catalog_aware_model_failover_and_reviewer_family_separation(self, tmp_path):
        from oh_my_raven.oh_my_raven import (
            CommandReceipt, ExecutionIdentity, IndependentReviewError, ReviewGateValidator, ReviewReceipt,
            SpecialistModelConfig, SessionHarnessFlagBuilder, SpecialistDefinition,
        )
        from oh_my_raven.routing import SubagentRetryableError

        # A: catalog/availability failover on unsupported model
        a_dir = tmp_path / "a"
        a_dir.mkdir()
        h1, h2 = _ModelHarness("Claude-Code", ("claude-3-haiku",)), _ModelHarness("Codex-CLI", ("gpt-5.4",))
        rb, tel = self._rb(a_dir, h1, h2, self._cfg())
        asyncio.run(rb.run("task", task_id="1", workspace=Path("."), executor=None))
        assert h1.calls == 0 and h2.calls == 1
        assert "-m" in h2.args[0] and "gpt-5.4" in h2.args[0] and 'model_reasoning_effort="high"' in h2.args[0]
        assert rb.last_resolved_model == "gpt-5.4"
        rec = _read_telemetry(tel)[-1]
        assert (rec["resolved_model"], rec["canonical_model"], rec["reasoning_effort"]) == ("gpt-5.4", "gpt-5.4", "high")
        assert rec["model_fallback_used"] is True and rec["attempt_count"] == 2

        # B: in-loop quota failover on preferred model
        b_dir = tmp_path / "b"
        b_dir.mkdir()
        h1 = _ModelHarness("Claude-Code", ("claude-sonnet-4-6",),
                           SubagentRetryableError("429 rate limit exceeded for model claude-sonnet-4-6"))
        h2 = _ModelHarness("Codex-CLI", ("claude-sonnet-4-6", "gpt-5.4"))
        rb, tel = self._rb(b_dir, h1, h2, self._cfg())
        asyncio.run(rb.run("task", task_id="1", workspace=Path("."), executor=None))
        assert h1.calls == 1 and h2.calls == 1
        assert "claude-sonnet-4-6" in h1.args[0] and "gpt-5.4" in h2.args[0]
        assert rb.last_resolved_model == "gpt-5.4"
        assert _read_telemetry(tel)[-1]["model_fallback_used"] is True

        # C: reviewer cross-family enforcement
        author = ExecutionIdentity(agent_name="Engineer", harness_kind="claude", executable_or_endpoint="claude",
                                   model="custom/gh/claude-sonnet-4.6")
        reviewer_cfg = SpecialistModelConfig(preferred_models=("claude-sonnet-4-6", "gpt-5.4"),
                                             require_distinct_model_family_from_author=True)
        res = reviewer_cfg.resolve_for_candidate("Copilot-CLI", harness_family="copilot",
                                                 author_models=("custom/gh/claude-sonnet-4.6",))
        assert res["canonical_model"] == "gpt-5.4" and res["model_family"] == "gpt"
        assert any("author_family_conflict:claude" in r for r in res["rejection_reasons"])
        bundle = SessionHarnessFlagBuilder.build_for_candidate(
            SpecialistDefinition(name="Reviewer", model_config=reviewer_cfg), "Copilot-CLI",
            author_models=("custom/gh/claude-sonnet-4.6",))
        assert bundle["resolved_model_family"] == "gpt"
        same_family = ExecutionIdentity(agent_name="Reviewer", harness_kind="copilot",
                                        executable_or_endpoint="Copilot-CLI", model="custom/gh/claude-sonnet-4.6",
                                        require_distinct_model_family_from_author=True)
        with pytest.raises(IndependentReviewError):
            ReviewGateValidator().verify_review_receipt(
                ReviewReceipt(verdict="approved", reviewed_head_sha="abc", findings=[],
                              commands=[CommandReceipt("pytest", 0)], reviewer_identity=same_family),
                candidate_head_sha="abc", reviewer_backend=same_family, author_backends=[author])

    def test_r24_identity_preserved_nonquota_contract_error_and_reraise(self, tmp_path):
        from oh_my_raven.oh_my_raven import ExecutionIdentity, SkillContractViolationError

        # (a) pre-configured backend identity preserved + enriched; per-attempt reset
        class _IdHarness(_ModelHarness):
            def __init__(self, *a, **k):
                super().__init__(*a, **k)
                self.execution_identity = ExecutionIdentity(
                    agent_name="X", harness_kind="cli", executable_or_endpoint="x", model="stale")

        a_dir = tmp_path / "a"
        a_dir.mkdir()
        h1, h2 = _IdHarness("Claude-Code", ("claude-sonnet-4-6",)), _ModelHarness("Codex-CLI", ("gpt-5.4",))
        rb, _ = self._rb(a_dir, h1, h2, self._cfg())
        asyncio.run(rb.run("t", task_id="1", workspace=Path("."), executor=None))
        assert rb.execution_identity.agent_name == "X"
        assert rb.execution_identity.model == "claude-sonnet-4-6"
        assert rb.last_resolved_model == "claude-sonnet-4-6"
        h1.supported_models = ("claude-3-haiku",)
        asyncio.run(rb.run("t", task_id="2", workspace=Path("."), executor=None))
        assert rb.last_resolved_model == "gpt-5.4"
        assert getattr(rb, "execution_identity", None) is None or rb.execution_identity.model == "gpt-5.4"

        # (b) non-quota contract error mentioning "model" does not burn preferred model
        b_dir = tmp_path / "b"
        b_dir.mkdir()
        h1 = _ModelHarness("Claude-Code", ("claude-sonnet-4-6", "gpt-5.4"),
                           SkillContractViolationError("missing skill evidence for model_validator pack"))
        h2 = _ModelHarness("Codex-CLI", ("claude-sonnet-4-6", "gpt-5.4"))
        rb, _ = self._rb(b_dir, h1, h2, self._cfg())
        asyncio.run(rb.run("t", task_id="1", workspace=Path("."), executor=None))
        assert "claude-sonnet-4-6" in h2.args[0] and "gpt-5.4" not in h2.args[0]

        # (c) all fail -> SkillContractViolationError re-raised
        c_dir = tmp_path / "c"
        c_dir.mkdir()
        e = SkillContractViolationError("missing skill evidence")
        h1, h2 = _ModelHarness("Claude-Code", ("claude-sonnet-4-6",), e), _ModelHarness("Codex-CLI", ("gpt-5.4",), e)
        rb, _ = self._rb(c_dir, h1, h2, self._cfg())
        with pytest.raises(SkillContractViolationError):
            asyncio.run(rb.run("t", task_id="1", workspace=Path("."), executor=None))
