"""Runs the live dogfood experiment and asserts its monitoring receipts."""

import json
import sys
from pathlib import Path

from opentelemetry import trace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from dogfood_omr_workload_experiment import run_dogfood_experiment  # noqa: E402


def _check(s, tmp_path):
    assert len(s["workloads"]) == 7
    w7 = s["workloads"]["W7_per_specialist_folder_agents_md_and_session_flag_failover"]
    for k in ("ok", "claude_flags_ok", "codex_flags_ok", "agents_md_loaded", "worktree_clean",
              "bucket1_reconciled", "gstack_in_bucket2", "model_config_ok"):
        assert w7[k], k
    assert all(w["ok"] for w in s["workloads"].values()), s["workloads"]
    assert s["all_ok"]
    n = s["spans_by_name"]
    assert n["omr.routing.run"] >= 4
    assert n["omr.council.consensus"] == 2
    assert n["omr.gate.push"] >= 1
    assert n["omr.gate.technical_decision"] >= 2
    assert s["routing_attempt_events"] >= 5
    assert s["lessons"] >= 2
    assert s["queue_ledger_rows"] >= 16
    gate_log = json.dumps(json.loads((tmp_path / "runs" / "gate_log.json").read_text()))
    assert "onboard_specialist:Security-Auditor" in gate_log and "onboard_harness:Windsurf-ACP" in gate_log
    assert (tmp_path / "history" / "lessons.jsonl").read_text().strip()
    assert s["workloads"]["W1_trivial_fast_path"]["latency_ms"] < 2000


def test_dogfood_experiment(tmp_path):
    _check(run_dogfood_experiment(tmp_path), tmp_path)


def test_dogfood_experiment_span_capture_survives_preconfigured_global_tracer_provider(tmp_path):
    class Dummy(trace.TracerProvider):
        def get_tracer(self, *a, **k):
            return trace.NoOpTracer()

    trace.set_tracer_provider(Dummy())  # no-op if a provider is already set; capture must not care
    base = run_dogfood_experiment(tmp_path / "a")
    again = run_dogfood_experiment(tmp_path / "b")
    assert base["spans_total"] > 0 and base["spans_by_name"] == again["spans_by_name"]
    _check(again, tmp_path / "b")
