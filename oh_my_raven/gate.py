"""Configurable trust gate: per-gate mode ask | auto-log | auto.

Irreversible external writes default to ``ask`` and can never be downgraded
unless explicitly opted in via ``allow_irreversible_auto``.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

MODES = ("ask", "auto-log", "auto")
IRREVERSIBLE = frozenset({"push", "publish", "spend", "deploy", "delete_remote"})
DEFAULTS = {
    "technical_decision": "auto-log",
    "plan_gap_fill": "auto-log",
    "specialist_fallback": "auto-log",
    "ambiguous_product_intent": "ask",
    **{g: "ask" for g in IRREVERSIBLE},
}


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
