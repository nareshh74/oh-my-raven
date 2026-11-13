import json
from concurrent.futures import ThreadPoolExecutor

import pytest

from oh_my_raven.gate import CouncilVote, LessonsStore, GatePolicy, classify_triage_tier, escalate_triage_tier, evaluate_council_consensus
from oh_my_raven.oh_my_raven import OmrDispatcher


def _persist_entries(path, worker):
    for item in range(5):
        policy = GatePolicy()
        policy.decide("technical_decision", f"{worker}:{item}")
        policy.persist_log(path)


def test_defaults():
    p = GatePolicy()
    assert p.decide("technical_decision", "pick lib") and p.log
    assert not p.decide("push")
    assert not p.decide("unknown_gate")


def test_irreversible_cannot_be_downgraded_without_optin(tmp_path):
    f = tmp_path / "g.json"
    f.write_text(json.dumps({"modes": {"push": "auto"}}))
    assert not GatePolicy.load(f).decide("push")
    f.write_text(json.dumps({"modes": {"push": "auto"}, "allow_irreversible_auto": True}))
    assert GatePolicy.load(f).decide("push")


def test_invalid_mode(tmp_path):
    f = tmp_path / "g.json"
    f.write_text(json.dumps({"modes": {"x": "yolo"}}))
    with pytest.raises(ValueError):
        GatePolicy.load(f)


def test_persist_log_preserves_entries_from_prior_policy(tmp_path):
    path = tmp_path / "gate_log.json"
    first, second = GatePolicy(), GatePolicy()
    first.decide("technical_decision", "first")
    first.persist_log(path)
    second.decide("plan_gap", "second")
    second.persist_log(path)
    assert [entry["detail"] for entry in json.loads(path.read_text())] == ["first", "second"]


def test_malformed_string_irreversible_auto_does_not_enable_auto(tmp_path):
    f = tmp_path / "g.json"
    f.write_text(json.dumps({"modes": {"push": "auto"}, "allow_irreversible_auto": "false"}))
    assert not GatePolicy.load(f).decide("push")
    p = GatePolicy(allow_irreversible_auto="false")
    p.modes["push"] = "auto"
    assert not p.decide("push")


def test_persist_log_atomic_replace_and_corrupt_existing_recovery(tmp_path, monkeypatch):
    import os

    path = tmp_path / "gate_log.json"
    path.write_text("{corrupt", encoding="utf-8")
    calls = []
    real_replace = os.replace

    def spy(src, dst):
        calls.append((str(src), str(dst)))
        return real_replace(src, dst)

    monkeypatch.setattr(os, "replace", spy)
    pol = GatePolicy()
    pol.decide("technical_decision", "x")
    pol.persist_log(path)
    assert len(calls) == 1 and calls[0][1] == str(path)
    assert json.loads(path.read_text(encoding="utf-8")) == [
        {"gate": "technical_decision", "detail": "x"}
    ]
    assert list(tmp_path.glob("*.tmp")) == [] and list(tmp_path.glob(".*.tmp")) == []


def test_persist_log_read_oserror_propagates_without_overwriting(tmp_path, monkeypatch):
    import os
    from pathlib import Path

    log_path = tmp_path / "gate_log.json"
    original = [{"gate": "prior", "mode": "auto", "allowed": True, "reason": "ok"}]
    log_path.write_text(json.dumps(original), encoding="utf-8")
    real_read = Path.read_text

    def fake_read(self, *a, **k):
        if self == log_path:
            raise PermissionError("locked")
        return real_read(self, *a, **k)

    replaced = []
    monkeypatch.setattr(Path, "read_text", fake_read)
    monkeypatch.setattr(os, "replace", lambda *a: replaced.append(a))
    gp = GatePolicy()
    gp.decide("technical_decision", "x")
    with pytest.raises(PermissionError):
        gp.persist_log(log_path)
    assert replaced == []
    monkeypatch.undo()
    assert json.loads(log_path.read_text(encoding="utf-8")) == original


def test_persist_log_replace_oserror_keeps_memory_log_and_no_tmp(tmp_path, monkeypatch):
    import os

    log_path = tmp_path / "gate_log.json"
    gp = GatePolicy()
    gp.decide("technical_decision", "x")

    def boom(*a):
        raise OSError("replace failed")

    monkeypatch.setattr(os, "replace", boom)
    with pytest.raises(OSError):
        gp.persist_log(log_path)
    assert gp.log == [{"gate": "technical_decision", "detail": "x"}]
    assert list(tmp_path.glob(".*.tmp")) == [] and not log_path.exists()


def test_persist_log_concurrent_writers_preserve_all_entries(tmp_path):
    path = tmp_path / "gate_log.json"
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda worker: _persist_entries(path, worker), range(4)))
    assert len(json.loads(path.read_text(encoding="utf-8"))) == 20


def test_triage_and_escalation():
    assert classify_triage_tier("fix typo", 1).tier == "trivial"
    assert classify_triage_tier("not sure which approach?").tier == "ambiguous-novel"
    decision = escalate_triage_tier("trivial", "failed")
    assert decision.tier == "standard" and decision.escalated_from == "trivial"


def test_lessons_store_matches_trigger(tmp_path):
    store = LessonsStore(tmp_path / "lessons.jsonl")
    store.record_lesson("pytest evidence", "Run focused tests", "task-1")
    assert store.matching_lessons("add pytest evidence")[0]["rule"] == "Run focused tests"


def test_gate_emits_opentelemetry_span(monkeypatch):
    seen = []
    class Span:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def set_attribute(self, key, value): seen.append((key, value))
    monkeypatch.setattr("oh_my_raven.gate._TRACER.start_as_current_span", lambda name: Span())
    GatePolicy().decide("technical_decision", "pick")
    assert ("omr.gate.name", "technical_decision") in seen


def test_ambiguity_experiment_resolves_before_ask(tmp_path):
    dispatcher = object.__new__(OmrDispatcher)
    dispatcher.run_root = tmp_path
    dispatcher.gate_policy = GatePolicy()
    dispatcher.gate_log_path = tmp_path / "gate_log.json"
    result = dispatcher.run_ambiguity_experiment("t1", "which?", lambda: {"resolved": True, "answer": "a"})
    assert result["proceed"] is True and result["resolved_by"] == "experiment"
    assert json.loads((tmp_path / "experiments" / "t1.json").read_text())["verified"] is True


def test_execute_external_action_blocks_before_side_effect():
    dispatcher = object.__new__(OmrDispatcher)
    dispatcher.gate_policy = GatePolicy()
    dispatcher.gate_log_path = __import__("pathlib").Path("unused.json")
    called = []
    with pytest.raises(PermissionError):
        dispatcher.execute_external_action("push", "origin", lambda: called.append(True))
    assert called == []


def test_council_consensus_requires_ratio_and_no_gaps(monkeypatch):
    attributes = []
    class Span:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def set_attribute(self, key, value): attributes.append((key, value))
    monkeypatch.setattr("oh_my_raven.gate._TRACER.start_as_current_span", lambda name: Span())
    verdict = evaluate_council_consensus([
        CouncilVote(voter="cto", approved=True),
        {"voter": "reviewer", "approved": True, "gaps": ["add rollback", "add rollback"]},
        {"voter": "engineer-superpowers", "approved": False, "gaps": ["test failure"]},
    ])
    assert not verdict.consensus
    assert verdict.merged_gaps == ["add rollback", "test failure"]
    assert ("omr.council.approved_count", 2) in attributes
    assert evaluate_council_consensus([
        {"voter": "cto", "approved": True}, {"voter": "reviewer", "approved": True}
    ]).consensus


def test_dispatcher_council_persists_and_records_gaps(tmp_path):
    dispatcher = object.__new__(OmrDispatcher)
    dispatcher.run_root = tmp_path / "runs"
    dispatcher.gate_policy = GatePolicy()
    dispatcher.gate_log_path = dispatcher.run_root / "gate_log.json"
    dispatcher.lessons_store = LessonsStore(tmp_path / "history" / "lessons.jsonl")
    result = dispatcher.run_council_consensus("task-1", [
        {"voter": "cto", "approved": True, "gaps": ["prove rollback"]},
        {"voter": "reviewer", "approved": True, "gaps": ["prove rollback"]},
    ])
    assert result["consensus"] is False
    assert json.loads((dispatcher.run_root / "council" / "task-1.json").read_text()) == result
    assert json.loads((dispatcher.run_root / "gate_log.json").read_text())[0]["detail"] == "council_gaps:task-1:1"
    lesson = json.loads(dispatcher.lessons_store.path.read_text().splitlines()[0])
    assert lesson == {"trigger": "council_gap", "rule": "prove rollback", "source_task_id": "task-1", "active": True}


def test_onboard_specialist_merges_via_public_table_backend_and_deduplicates_targets(tmp_path):
    from types import MappingProxyType, SimpleNamespace
    from oh_my_raven.oh_my_raven import OmrTaskQueue
    from oh_my_raven.routing import RouteTarget, RoutingBackend

    decoy = RoutingBackend("Sec-Audit", SimpleNamespace(name="X"), [RouteTarget("X", "X", SimpleNamespace(name="X"))], instances=None)

    class Table:
        def __init__(self):
            self.registry = {}
            self._backends = MappingProxyType({"Sec-Audit": decoy})  # read-only decoy: registration goes via onboard()

        def backend(self, name):
            return self.registry[name]

        def onboard(self, cfg, backend=None):
            self.registry[cfg] = backend

    table = Table()
    d = OmrDispatcher(
        table=table, queue=OmrTaskQueue(tmp_path / "q.jsonl"), workdir=tmp_path,
        run_root=tmp_path / "runs", nodes_root=tmp_path / "nodes",
        history_root=tmp_path / "history", gate_policy_path=None,
    )
    h1, h2 = SimpleNamespace(name="H1"), SimpleNamespace(name="H2")
    rb = d.onboard_specialist("Sec-Audit", [RouteTarget("H1", "H1", h1), RouteTarget("H1", "H1", h1)])
    assert isinstance(rb, RoutingBackend)
    assert [t.name for t in rb._targets] == ["H1"]
    merged = d.onboard_specialist(
        "Sec-Audit", [RouteTarget("H1", "H1", h1), RouteTarget("H2", "H2", h2)], replace=False
    )
    assert merged is rb
    assert [t.name for t in rb._targets] == ["H1", "H2"]
    assert rb is not decoy and [t.name for t in decoy._targets] == ["X"]
