"""Configurable trust gate: per-gate mode ask | auto-log | auto.

Irreversible external writes default to ``ask`` and can never be downgraded
unless explicitly opted in via ``allow_irreversible_auto``.
"""
from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
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
        p = Path(path)
        existing = json.loads(p.read_text(encoding="utf-8")) if p.exists() else []
        if not isinstance(existing, list):
            existing = []
        p.write_text(json.dumps(existing + self.log, indent=2) + "\n", encoding="utf-8")
        self.log.clear()


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
    root = Path(repo_root).resolve()

    def confined_file(value: object) -> Path | None:
        if not isinstance(value, str) or not value.strip():
            return None
        path = Path(value)
        if path.is_absolute():
            return None
        candidate = (root / path).resolve()
        return candidate if candidate.is_relative_to(root) and candidate.is_file() and candidate.stat().st_size > 0 else None

    receipt = confined_file(evidence.get("receipt_path"))
    artifact = confined_file(evidence.get("artifact_path"))
    if receipt:
        try:
            receipt_data = json.loads(receipt.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            receipt_data = None
        if receipt.suffix.lower() == ".json":
            return isinstance(receipt_data, dict) and receipt_data.get("verified") is True and receipt_data.get("exit_code") == 0
        return True
    if artifact:
        return True

    command = evidence.get("test_command") or evidence.get("command")
    exit_code = evidence.get("exit_code")
    if not isinstance(command, str) or not command.strip() or isinstance(exit_code, bool) or exit_code != 0:
        return False
    try:
        args = shlex.split(command)
        if args and args[0].lower() in {"pytest", "pytest.exe"}:
            args = [sys.executable, "-m", "pytest", *args[1:]]
        elif args and args[0].lower() in {"python", "python.exe"}:
            args[0] = sys.executable
        proc = subprocess.run(args, cwd=str(root), capture_output=True, text=True, timeout=15, check=False)
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0
