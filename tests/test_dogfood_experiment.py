"""Runs the live dogfood experiment and asserts its monitoring receipts."""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dogfood_omr_workload_experiment import run_dogfood_experiment  # noqa: E402


def test_dogfood_experiment(tmp_path):
    s = run_dogfood_experiment(tmp_path)
    assert len(s["workloads"]) == 6
    assert all(w["ok"] for w in s["workloads"].values()), s["workloads"]
    assert s["all_ok"]
    names = s["spans_by_name"]
    assert s["spans_total"] >= 6
    assert names.get("omr.routing.run") and names.get("omr.council.consensus")
    assert any(n.startswith("omr.gate.") for n in names)
    assert json.loads((tmp_path / "runs" / "gate_log.json").read_text())
    assert (tmp_path / "history" / "lessons.jsonl").read_text().strip()
    assert s["workloads"]["W1_trivial_fast_path"]["latency_ms"] < 2000
