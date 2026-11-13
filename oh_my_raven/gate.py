"""Configurable trust gate: per-gate mode ask | auto-log | auto.

Irreversible external writes default to ``ask`` and can never be downgraded
unless explicitly opted in via ``allow_irreversible_auto``.
"""
from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
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
        pol = cls(allow_irreversible_auto=data.get("allow_irreversible_auto") is True)
        for gate, mode in data.get("modes", {}).items():
            if mode not in MODES:
                raise ValueError(f"invalid mode {mode!r} for gate {gate!r}")
            pol.modes[gate] = mode
        return pol

    def mode(self, gate: str) -> str:
        m = self.modes.get(gate, "ask")  # unknown gates are conservative
        if gate in IRREVERSIBLE and m != "ask" and self.allow_irreversible_auto is not True:
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

_TEST_PATH_RE = re.compile(r"^tests?/[A-Za-z0-9_./-]+$")
_TEST_FILE_RE = re.compile(r"^test_[A-Za-z0-9_.-]+\.py$")


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
    try:
        root = Path(repo_root).resolve()
    except (OSError, RuntimeError, ValueError):
        return False

    def confined_file(value: object) -> Path | None:
        if not isinstance(value, str) or not value.strip():
            return None
        try:
            path = Path(value)
            if path.is_absolute():
                return None
            candidate = (root / path).resolve()
            ok = candidate.is_relative_to(root) and candidate.is_file() and candidate.stat().st_size > 0
        except (OSError, RuntimeError, ValueError):
            return None
        return candidate if ok else None

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
    # Never run worker-supplied programs: only a strict pytest allowlist.
    try:
        parts = shlex.split(command.strip())
    except ValueError:
        return False
    if not parts or parts[0] != "pytest":
        return False
    safe_args: list[str] = []
    for arg in parts[1:]:
        if arg in ("-q", "-v"):
            continue
        if arg.startswith("-") or not (_TEST_PATH_RE.match(arg) or _TEST_FILE_RE.match(arg)):
            return False
        if confined_file(arg) is None:
            return False
        safe_args.append(arg)
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "pytest", "-q", *safe_args],
            cwd=str(root), shell=False, capture_output=True, text=True, timeout=15, check=False,
        )
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return False
    return proc.returncode == 0
