import json

import pytest

from oh_my_raven.oh_my_raven import OmrDispatcher, OmrTaskQueue


def dispatcher(tmp_path, policy=None):
    return OmrDispatcher(
        table=None,
        queue=OmrTaskQueue(tmp_path / "queue.jsonl"),
        workdir=tmp_path,
        run_root=tmp_path / "runs",
        nodes_root=tmp_path / "nodes",
        history_root=tmp_path / "history",
        gate_policy_path=policy,
    )


def test_done_requires_successful_test_evidence(tmp_path):
    d = dispatcher(tmp_path)
    assert d.completion_status(json.dumps({"evidence": {"test_command": 'python -c "assert True"', "exit_code": 0}})) == "completed"
    assert d.completion_status("done") == "incomplete"
    assert d.completion_status(json.dumps({"test_command": "pytest", "exit_code": 1})) == "incomplete"


def test_existing_artifact_is_evidence(tmp_path):
    (tmp_path / "report.txt").write_text("ok")
    d = dispatcher(tmp_path)
    assert d.completion_status(json.dumps({"artifact_path": "report.txt"})) == "completed"
    assert d.completion_status(json.dumps({"artifact_path": "missing.txt"})) == "incomplete"


def test_artifact_path_traversal_and_absolute_rejected(tmp_path):
    outside = tmp_path.parent / "outside.txt"
    outside.write_text("no")
    d = dispatcher(tmp_path)
    assert d.completion_status(json.dumps({"artifact_path": "../outside.txt"})) == "incomplete"
    assert d.completion_status(json.dumps({"artifact_path": str(outside.resolve())})) == "incomplete"


def test_policy_auto_logs_decisions_and_asks_for_intent_and_irreversible(tmp_path):
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"modes": {"plan_gap": "auto-log"}}))
    d = dispatcher(tmp_path, policy)
    assert d.decide_gate("plan_gap", "fill missing step")
    assert d.decide_gate("technical_decision", "choose stdlib")
    assert not d.decide_gate("ambiguous_product_intent")
    assert not d.decide_gate("push")
    assert [x["gate"] for x in json.loads(d.gate_log_path.read_text())] == ["plan_gap", "technical_decision"]


def _run_node(tmp_path, output):
    import asyncio
    from types import SimpleNamespace

    class B:
        name = "b"
        last_chosen_backend = "b"

        async def run(self, *a, **k):
            return output

    d = dispatcher(tmp_path)
    d.table = SimpleNamespace(backend=lambda n: B())
    from oh_my_raven import oh_my_raven as m


    asyncio.run(d.dispatch_specialist_node("t1", "S", "p"))
    return d.queue.latest_by_task()["t1"].status


def test_single_task_without_evidence_is_incomplete(tmp_path, monkeypatch):
    from oh_my_raven import oh_my_raven as m

    monkeypatch.setattr(m.SkillAndToolReceiptGate, "verify_specialist_execution", staticmethod(lambda *a, **k: None))
    assert _run_node(tmp_path, "done") == "incomplete"
    good = json.dumps({"evidence": {"command": 'python -c "assert True"', "exit_code": 0}})
    assert _run_node(tmp_path / "g", good) == "completed"


def _submit_with_output(tmp_path, monkeypatch, output):
    import asyncio
    from types import SimpleNamespace
    from oh_my_raven import oh_my_raven as m

    class Manager:
        last_chosen_backend = "stub"
        async def run(self, *args, **kwargs):
            return "plan"

    manager = Manager()
    table = SimpleNamespace(
        enabled=lambda: [SimpleNamespace(name="Manager", description="lead", is_harness=False, kind="specialist")],
        backend=lambda name, build=None: manager,
    )
    d = dispatcher(tmp_path)
    d.table = table
    node = SimpleNamespace(id="child", subagent="Worker")
    plan = SimpleNamespace(dag_spec=SimpleNamespace(nodes=[node]), node_waves={"child": 0})

    def validate(_payload, queue, parent_task_id, **kwargs):
        queue.enqueue("child", "work", parent_task_id=parent_task_id)
        return plan

    async def run_dag(*args, **kwargs):
        await kwargs["on_node_start"]("child")
        await kwargs["progress_publisher"](
            "dag_node_updated", {"node": "child", "status": "completed", "output": output}
        )
        return SimpleNamespace(files=[{"status": "completed", "output": output}])

    monkeypatch.setattr(m.DynamicDecompositionGate, "validate_and_enqueue", validate)
    monkeypatch.setattr("raven.agent.subagent.dag_runner.run_dag", run_dag)
    asyncio.run(d.submit_task("parent", "do it", cto_specialist="missing"))
    return d


def test_dag_rollup_without_evidence_is_incomplete(tmp_path, monkeypatch):
    d = _submit_with_output(tmp_path / "bad", monkeypatch, "done")
    assert d.queue.latest_by_task()["parent"].status == "incomplete"
    evidence = json.dumps({"evidence": {"command": 'python -c "assert True"', "exit_code": 0}})
    d = _submit_with_output(tmp_path / "good", monkeypatch, evidence)
    assert d.queue.latest_by_task()["parent"].status == "completed"


def test_submit_task_consults_gate_policy_and_require_gate_blocks_irreversible(tmp_path, monkeypatch):
    d = _submit_with_output(
        tmp_path,
        monkeypatch,
        json.dumps({"evidence": {"command": 'python -c "assert True"', "exit_code": 0}}),
    )
    gates = [entry["gate"] for entry in json.loads(d.gate_log_path.read_text())]
    assert gates == ["specialist_fallback", "technical_decision"]
    with pytest.raises(PermissionError, match="requires human approval"):
        d.require_gate("push", "origin/main")


@pytest.mark.parametrize("command", [
    "pytest test_missing_file.py",
    'python -c "import sys; sys.exit(1)"',
    "nonexistent_binary_xyz",
])
def test_fabricated_failing_or_nonexistent_command_claim_is_rejected(tmp_path, command):
    d = dispatcher(tmp_path)
    claim = json.dumps({"evidence": {"command": command, "exit_code": 0}})
    assert d.completion_status(claim) == "incomplete"
