"""Shared pytest configuration and CLI options for oh-my-raven."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Ensure the specialist-mvp worktree precedes the editable main-repo install in sys.path
_CANDIDATES = (
    Path(__file__).resolve().parent.parent,
    Path(r"C:\learn\Raven.worktrees\specialist-mvp"),
)
for _cand in reversed(_CANDIDATES):
    if _cand.exists():
        if str(_cand) in sys.path:
            sys.path.remove(str(_cand))
        sys.path.insert(0, str(_cand))

# Single source of truth: any raven.agent.subagent.* import resolves to this repo's oh_my_raven.
import oh_my_raven.routing as _omr_routing_mod  # noqa: E402

# Routing must be aliased BEFORE raven.agent.subagent (registry) is first imported.
sys.modules["raven.agent.subagent.backends.routing"] = _omr_routing_mod
import oh_my_raven.oh_my_raven as _omr_mod  # noqa: E402

sys.modules["raven.agent.subagent.oh_my_raven"] = _omr_mod

# sys.modules alone is not enough: parents/registry may already hold the old objects.
import raven.agent.subagent as _pkg  # noqa: E402
import raven.agent.subagent.backends as _backends_pkg  # noqa: E402
import raven.agent.subagent.registry as _registry  # noqa: E402

_pkg.oh_my_raven = _omr_mod
_backends_pkg.routing = _omr_routing_mod
_registry.RoutingBackend = _omr_routing_mod.RoutingBackend
_registry.RouteTarget = _omr_routing_mod.RouteTarget


def pytest_addoption(parser: pytest.Parser) -> None:
    group = parser.getgroup("oh-my-raven")
    try:
        group.addoption(
            "--omr-mode",
            default="deterministic",
            choices=("deterministic", "live"),
            help="execution mode for oh-my-raven repeatable E2E matrix (deterministic|live)",
        )
    except ValueError:
        pass
