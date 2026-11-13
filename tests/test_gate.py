import json

import pytest

from oh_my_raven.gate import GatePolicy


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
