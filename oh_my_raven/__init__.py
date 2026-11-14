"""oh-my-raven: Harness-agnostic specialist orchestration plugin for Raven."""

from __future__ import annotations

from oh_my_raven.oh_my_raven import *  # noqa: F401,F403
from oh_my_raven.oh_my_raven import __all__ as _omr_all
from oh_my_raven.oh_my_raven import SpecialistModelConfig, infer_model_family
from oh_my_raven.routing import (
    HOST_PREFIX,
    Picked,
    RouteTarget,
    Router,
    RoutingBackend,
    SubagentFailoverBlockedError,
    SubagentFailoverExhaustedError,
    SubagentRetryableError,
    TargetReady,
    TelemetryLogger,
    handed_files,
)

__version__ = "0.1.0"
__all__ = list(_omr_all) + [
    "HOST_PREFIX",
    "Picked",
    "RouteTarget",
    "Router",
    "RoutingBackend",
    "SubagentFailoverBlockedError",
    "SubagentFailoverExhaustedError",
    "SubagentRetryableError",
    "TargetReady",
    "TelemetryLogger",
    "__version__",
    "handed_files",
]

