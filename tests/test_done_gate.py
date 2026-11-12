import json

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
    assert d.completion_status(json.dumps({"evidence": {"test_command": "pytest", "exit_code": 0}})) == "completed"
    assert d.completion_status("done") == "incomplete"
    assert d.completion_status(json.dumps({"test_command": "pytest", "exit_code": 1})) == "incomplete"


def test_existing_artifact_is_evidence(tmp_path):
    (tmp_path / "report.txt").write_text("ok")
    d = dispatcher(tmp_path)
    assert d.completion_status(json.dumps({"artifact_path": "report.txt"})) == "completed"
    assert d.completion_status(json.dumps({"artifact_path": "missing.txt"})) == "incomplete"


def test_policy_auto_logs_decisions_and_asks_for_intent_and_irreversible(tmp_path):
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"modes": {"plan_gap": "auto-log"}}))
    d = dispatcher(tmp_path, policy)
    assert d.decide_gate("plan_gap", "fill missing step")
    assert d.decide_gate("technical_decision", "choose stdlib")
    assert not d.decide_gate("ambiguous_product_intent")
    assert not d.decide_gate("push")
    assert [x["gate"] for x in json.loads(d.gate_log_path.read_text())] == ["plan_gap", "technical_decision"]
