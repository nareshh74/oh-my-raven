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
