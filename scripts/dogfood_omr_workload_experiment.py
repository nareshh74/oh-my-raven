"""Live dogfood experiment: 6 realistic workloads through oh_my_raven with real git, subprocesses and OTel spans.

Usage: python scripts/dogfood_omr_workload_experiment.py [--write-report]
Spans use the OTel SDK when installed, otherwise a tiny recording TracerProvider built on opentelemetry-api.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import subprocess
import sys
import tempfile
import time
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "tests"))
import conftest  # noqa: E402,F401  (aliases raven.agent.subagent.* to this repo's oh_my_raven)
from opentelemetry import trace as otel_trace  # noqa: E402

from oh_my_raven.oh_my_raven import OmrDispatcher, OmrTaskQueue, WorktreeManager  # noqa: E402
from oh_my_raven.routing import RouteTarget, RoutingBackend  # noqa: E402

REPORT = ROOT / "docs" / "DOGFOOD_EXPERIMENT_REPORT.md"
PASSING_TEST = "from feature import add\n\n\ndef test_add():\n    assert add(1, 2) == 3\n"


# ---------------------------------------------------------------- span capture
class _Span(otel_trace.NonRecordingSpan):
    def __init__(self, name: str, sink: list) -> None:
        super().__init__(otel_trace.INVALID_SPAN_CONTEXT)
        self.name, self.attributes, self.events = name, {}, []
        sink.append(self)

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def add_event(self, name, attributes=None, timestamp=None):
        self.events.append({"name": name, **(attributes or {})})


class _Tracer(otel_trace.Tracer):
    def __init__(self, sink: list) -> None:
        self.sink = sink

    def start_span(self, name, *a, **k):
        return _Span(name, self.sink)

    @contextlib.contextmanager
    def start_as_current_span(self, name, *a, **k):
        yield _Span(name, self.sink)


class _Provider(otel_trace.TracerProvider):
    def __init__(self, sink: list) -> None:
        self.sink = sink

    def get_tracer(self, *a, **k):
        return _Tracer(self.sink)


_CAPTURE: tuple[Any, str] | None = None


def install_span_capture() -> tuple[Any, str]:
    """Install once per process (OTel allows a single global provider)."""
    global _CAPTURE
    if _CAPTURE:
        return _CAPTURE
    try:
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        otel_trace.set_tracer_provider(provider)
        _CAPTURE = (exporter, "opentelemetry-sdk InMemorySpanExporter")
    except ImportError:
        sink: list = []
        otel_trace.set_tracer_provider(_Provider(sink))
        _CAPTURE = (sink, "custom recording TracerProvider (opentelemetry-sdk not installed)")
    return _CAPTURE


def _span_rows(cap) -> list[dict[str, Any]]:
    if isinstance(cap, list):
        return [{"name": s.name, "attributes": dict(s.attributes), "events": list(s.events)} for s in cap]
    return [{"name": s.name, "attributes": dict(s.attributes or {}),
             "events": [{"name": e.name, **dict(e.attributes or {})} for e in s.events]} for s in cap.get_finished_spans()]


# ---------------------------------------------------------------- helpers
def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-C", str(repo), *args], check=True, text=True, capture_output=True).stdout.strip()


def init_repo(root: Path) -> Path:
    repo = root / "repo"
    repo.mkdir(parents=True)
    subprocess.run(["git", "init", "-q", str(repo)], check=True)
    for k, v in (("user.email", "dogfood@example.com"), ("user.name", "dogfood"), ("commit.gpgsign", "false")):
        git(repo, "config", k, v)
    (repo / "pyproject.toml").write_text('[project]\nname = "demo"\n', encoding="utf-8")
    (repo / ".gitignore").write_text("__pycache__/\n.pytest_cache/\n", encoding="utf-8")
    git(repo, "add", "-A")
    git(repo, "commit", "-q", "-m", "init")
    return repo


class Stub:
    kind, streams = "cli", False

    def __init__(self, name: str, fn) -> None:
        self.name, self.fn, self.calls, self.prompts = name, fn, 0, []

    async def run(self, task, **kw):
        self.calls += 1
        self.prompts.append(task)
        return self.fn(self, task, kw)


class Table:
    def __init__(self) -> None:
        self.backends: dict[str, Any] = {}

    def enabled(self):
        return [SimpleNamespace(name="Manager", description="lead", is_harness=False, kind="specialist")]

    def backend(self, name, build=None):
        return self.backends.get(name)

    def onboard(self, name: str, backend: Any) -> None:  # hot-onboard at runtime
        self.backends[name] = backend


class Inst:
    def list_instances(self, k=None):
        return []


def routing(name, primary, targets, telemetry, **kw) -> RoutingBackend:
    return RoutingBackend(name, primary, [RouteTarget(t.name, t.name, t) for t in targets], instances=Inst(),
                          cooldown_s=0, telemetry_path=telemetry, **kw)


def jsonl(path: Path) -> list[dict]:
    if not path.exists():
        return []
    return [json.loads(x) for x in path.read_text(encoding="utf-8").splitlines() if x.strip()]


def _wt(repo: Path, tmp: Path, run: str) -> Path:
    wt = WorktreeManager().provision_worktree(repo, tmp / "wts", run, "n1", git(repo, "rev-parse", "HEAD"))
    git(wt, "config", "user.email", "dogfood@example.com")
    git(wt, "config", "user.name", "dogfood")
    return wt


# ---------------------------------------------------------------- experiment
def run_dogfood_experiment(tmp: Path) -> dict[str, Any]:
    tmp = Path(tmp)
    cap, cap_kind = install_span_capture()
    span_base = len(_span_rows(cap))
    repo = init_repo(tmp)
    telemetry = tmp / "specialist_telemetry.jsonl"
    table = Table()
    d = OmrDispatcher(table=table, queue=OmrTaskQueue(tmp / "queue_ledger.jsonl"), workdir=repo, run_root=tmp / "runs",
                      nodes_root=tmp / "nodes", history_root=tmp / "history", gate_policy_path=tmp / "gate_policy.json")
    results: dict[str, dict[str, Any]] = {}

    def timed(key, fn):
        t0 = time.perf_counter()
        info = fn()
        info["latency_ms"] = round((time.perf_counter() - t0) * 1000, 2)
        results[key] = info

    def w1():
        def work(s, task, kw):
            (repo / "config.json").write_text('{"timeout": 30}', encoding="utf-8")
            return json.dumps({"evidence": {"artifact_path": "config.json"}})
        worker = Stub("Worker", work)
        table.onboard("Worker", routing("Worker", worker, [], telemetry))
        prompt = "Update timeout=30 in config.json"
        tier = d.triage_task(prompt, 1).tier
        asyncio.run(d.dispatch_specialist_node("W1", "Worker", prompt))
        status = d.queue.latest_by_task()["W1"].status
        return {"ok": tier == "trivial" and status == "completed" and worker.calls == 1,
                "tier": tier, "status": status, "specialist_turns": worker.calls}
    timed("W1_trivial_fast_path", w1)

    def w2():
        gap = "missing edge-case test for empty input"
        bad = [{"voter": "cto", "approved": True}, {"voter": "reviewer", "approved": False, "gaps": [gap]},
               {"voter": "engineer-superpowers", "approved": True, "gaps": [gap]}]
        good = [{"voter": v, "approved": True} for v in ("cto", "reviewer", "engineer-superpowers")]
        first, second = d.run_council_consensus("W2", bad), d.run_council_consensus("W2b", good)
        return {"ok": first["consensus"] is False and second["consensus"] is True and first["merged_gaps"] == [gap],
                "first_consensus": first["consensus"], "second_consensus": second["consensus"], "gaps": first["merged_gaps"]}
    timed("W2_council_plan_review", w2)

    def w3():
        def probe():
            fmt = "toml" if (repo / "pyproject.toml").is_file() else "unknown"
            r = subprocess.run([sys.executable, "-c", f"print({fmt!r})"], capture_output=True, text=True)
            return {"resolved": fmt == "toml" and r.stdout.strip() == "toml", "config_format": fmt}
        ans = d.run_ambiguity_experiment("W3", "which config format does this repo use?", probe)
        return {"ok": ans["proceed"] and ans["resolved_by"] == "experiment",
                "proceed": ans["proceed"], "resolved_by": ans["resolved_by"]}
    timed("W3_ambiguity_experiment", w3)

    def w4():
        wt = _wt(repo, tmp, "w4")

        def work(s, task, kw):
            if s.calls == 1:
                return "Code changes done, testing not complete"
            (wt / "feature.py").write_text("def add(a, b):\n    return a + b\n", encoding="utf-8")
            (wt / "test_feature.py").write_text(PASSING_TEST, encoding="utf-8")
            git(wt, "add", "-A")
            git(wt, "commit", "-q", "-m", "feature + test")
            return json.dumps({"evidence": {"test_command": "pytest test_feature.py", "exit_code": 0}})
        worker = Stub("Coder", work)
        table.onboard("Coder", routing("Coder", worker, [], telemetry))
        prompt = "implement add feature with tests"
        d.workdir = wt  # done-gate verifies evidence (real pytest) inside the worktree
        try:
            asyncio.run(d.dispatch_specialist_node("W4a", "Coder", prompt, workspace=wt, writes=True))
            s1 = d.queue.latest_by_task()["W4a"].status
            asyncio.run(d.dispatch_specialist_node("W4b", "Coder", prompt, workspace=wt, writes=True))
            s2 = d.queue.latest_by_task()["W4b"].status
        finally:
            d.workdir = repo
        injected = "[Active Lessons]" in worker.prompts[1]
        return {"ok": s1 == "incomplete" and s2 == "completed" and injected, "first_status": s1, "retry_status": s2,
                "lessons_injected": injected}
    timed("W4_premature_completion_lesson_loop", w4)

    def w5():
        wt = _wt(repo, tmp, "w5")

        def crash(s, task, kw):
            raise TimeoutError("harness unresponsive")

        def windsurf(s, task, kw):
            script = ("import sys,pathlib,json;wt=pathlib.Path(sys.argv[1]);(wt/'audit.txt').write_text('no findings');"
                      "(wt/'audit_receipt.json').write_text(json.dumps({'verified':True,'exit_code':0}))")
            subprocess.run([sys.executable, "-c", script, str(wt)], check=True)
            git(wt, "add", "-A")
            git(wt, "commit", "-q", "-m", "security audit")
            return json.dumps({"evidence": {"receipt_path": "audit_receipt.json"}})
        claude = Stub("Claude-Code", crash)
        windsurf_h = Stub("Windsurf-ACP", windsurf)  # hot-onboarded 6th harness
        auditor = routing("Security-Auditor", claude, [claude, windsurf_h], telemetry, max_attempts=4)  # hot-onboarded 8th specialist
        table.onboard("Security-Auditor", auditor)
        d.workdir = wt
        try:
            asyncio.run(d.dispatch_specialist_node("W5", "Security-Auditor", "audit dependencies", workspace=wt, writes=True))
        finally:
            d.workdir = repo
        statuses = [a["status"] for a in auditor.last_attempts]
        committed = "audit.txt" in git(wt, "show", "--name-only", "HEAD")
        status = d.queue.latest_by_task()["W5"].status
        chosen = str(auditor.last_chosen_backend)
        return {"ok": status == "completed" and committed and statuses.count("failed") >= 2 and chosen == "Windsurf-ACP",
                "attempts": statuses, "claude_calls": claude.calls, "chosen_backend": chosen,
                "committed": committed, "status": status}
    timed("W5_crash_revival_failover_hot_onboard", w5)

    def w6():
        pushed: list[str] = []
        try:
            d.git_push("origin/main", lambda: pushed.append("pushed"))
            blocked = False
        except PermissionError:
            blocked = True
        return {"ok": blocked and not pushed, "blocked": blocked, "callback_ran": bool(pushed)}
    timed("W6_irreversible_action_blocked", w6)

    spans = _span_rows(cap)[span_base:]
    by_name = Counter(s["name"] for s in spans)
    attempt_events = sum(1 for s in spans for e in s["events"] if e["name"] == "omr.routing.attempt")
    gate_log = json.loads((tmp / "runs" / "gate_log.json").read_text(encoding="utf-8"))
    queue_rows = jsonl(tmp / "queue_ledger.jsonl")
    return {
        "span_capture": cap_kind,
        "workloads": results,
        "all_ok": len(results) == 6 and all(r["ok"] for r in results.values()),
        "spans_total": len(spans),
        "spans_by_name": dict(sorted(by_name.items())),
        "routing_attempt_events": attempt_events,
        "gate_log_entries": len(gate_log),
        "gate_log_gates": dict(Counter(e["gate"] for e in gate_log)),
        "queue_ledger_rows": len(queue_rows),
        "queue_transitions": dict(Counter(r.get("status") for r in queue_rows)),
        "telemetry_rows": len(jsonl(telemetry)),
        "lessons": len(jsonl(tmp / "history" / "lessons.jsonl")),
    }


def render_report(s: dict[str, Any]) -> str:
    lines = ["# Dogfood Experiment Report", "",
             "Generated by `scripts/dogfood_omr_workload_experiment.py --write-report` "
             "(real temporary git repo, real subprocess workers, real pytest).",
             f"Span capture: {s['span_capture']}.", "", "## Workloads", "",
             "| Workload | OK | Latency (ms) | Detail |", "|---|---|---|---|"]
    for k, r in s["workloads"].items():
        detail = ", ".join(f"{a}={b}" for a, b in r.items() if a not in ("ok", "latency_ms"))
        lines.append(f"| {k} | {r['ok']} | {r['latency_ms']} | {detail} |")
    lines += ["", f"All 6 workloads OK: **{s['all_ok']}**", "", "## Monitoring receipts", "",
              f"- OTel spans captured: {s['spans_total']} (`{json.dumps(s['spans_by_name'])}`)",
              f"- `omr.routing.attempt` events: {s['routing_attempt_events']}",
              f"- `gate_log.json` entries: {s['gate_log_entries']} (`{json.dumps(s['gate_log_gates'])}`)",
              f"- `queue_ledger.jsonl` rows: {s['queue_ledger_rows']} (`{json.dumps(s['queue_transitions'])}`)",
              f"- `specialist_telemetry.jsonl` rows: {s['telemetry_rows']}",
              f"- `lessons.jsonl` rows: {s['lessons']}", ""]
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write-report", action="store_true")
    args = ap.parse_args()
    with tempfile.TemporaryDirectory(prefix="omr-dogfood-") as td:
        summary = run_dogfood_experiment(Path(td))
    print(json.dumps(summary, indent=2))
    if args.write_report:
        REPORT.write_text(render_report(summary), encoding="utf-8")
    return 0 if summary["all_ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
