"""Configurable trust gate: per-gate mode ask | auto-log | auto.

Irreversible external writes default to ``ask`` and can never be downgraded
unless explicitly opted in via ``allow_irreversible_auto``.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path

MODES = ("ask", "auto-log", "auto")
IRREVERSIBLE = frozenset({"push", "publish", "spend", "deploy", "delete_remote"})
DEFAULTS = {
    "technical_decision": "auto-log",
    "plan_gap": "auto-log",
    "plan_gap_fill": "auto-log",
    "specialist_fallback": "auto-log",
    "ambiguous_product_intent": "ask",
    **{g: "ask" for g in IRREVERSIBLE},
}
DEFAULT_POLICY_ENV = "OMR_GATE_POLICY_PATH"


@dataclass
class GatePolicy:
    modes: dict[str, str] = field(default_factory=lambda: dict(DEFAULTS))
    allow_irreversible_auto: bool = False
    log: list[dict] = field(default_factory=list)

    @classmethod
    def load(cls, path: str | Path) -> "GatePolicy":
        p = Path(path)
        data = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        pol = cls(allow_irreversible_auto=bool(data.get("allow_irreversible_auto", False)))
        for gate, mode in data.get("modes", {}).items():
            if mode not in MODES:
                raise ValueError(f"invalid mode {mode!r} for gate {gate!r}")
            pol.modes[gate] = mode
        return pol

    def mode(self, gate: str) -> str:
        m = self.modes.get(gate, "ask")  # unknown gates are conservative
        if gate in IRREVERSIBLE and m != "ask" and not self.allow_irreversible_auto:
            return "ask"
        return m

    def decide(self, gate: str, detail: str = "") -> bool:
        """True = proceed autonomously; False = must ask the human."""
        m = self.mode(gate)
        if m == "auto-log":
            self.log.append({"gate": gate, "detail": detail})
        return m != "ask"

    @classmethod
    def load_default(cls, path: str | Path | None = None) -> "GatePolicy":
        return cls.load(path or os.environ.get(DEFAULT_POLICY_ENV, "gate_policy.json"))

    def persist_log(self, path: str | Path) -> None:
        Path(path).write_text(json.dumps(self.log, indent=2) + "\n", encoding="utf-8")


def has_verifiable_evidence(result: str, repo_root: str | Path) -> bool:
    """Accept a successful command receipt or an existing artifact."""
    try:
        data = json.loads(result)
    except (TypeError, ValueError):
        return False
    if not isinstance(data, dict):
        return False
    evidence = data.get("evidence", data)
    if not isinstance(evidence, dict):
        return False
    command = evidence.get("test_command") or evidence.get("command")
    exit_code = evidence.get("exit_code")
    if isinstance(command, str) and command.strip() and exit_code == 0:
        return True
    artifact = evidence.get("artifact_path")
    if isinstance(artifact, str) and artifact.strip():
        candidate = Path(artifact)
        if not candidate.is_absolute():
            candidate = Path(repo_root) / candidate
        return candidate.exists()
    return False
