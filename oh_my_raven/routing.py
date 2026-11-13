"""One product entry fronting several implementations: the route is picked on ``run``.

A row declaring ``routes`` is the only row the dispatching model sees for the
work its targets do (they ship ``hidden``). Every caller that dispatches to the
row -- ``spawn``, a DAG node and its retries, a WebUI direct chat -- resolves
one backend by name and calls ``run`` on it, so this is the one seat where the
task text and the implementation meet, and the gate lives here rather than once
per caller.

``run`` reads the task as the dispatching model wrote it (``authored_task``;
the rendered ``task`` only where a caller has no other text): a lane may have
inlined whole files into the rendered task, and their contents are neither
routing evidence nor something to send through the host model again. It picks
in this order:

1. a reused ``instance`` handle continues the conversation it was opened on --
   the implementation whose transport already bound the handle in this
   session wins, and nothing is classified;
2. otherwise the host's classifier (set by the manager, which holds the host
   model) picks between the targets and this row's own implementation, reading
   the targets' roster lines only: the fronting row's line is written for the
   dispatching model and argues the opposite case;
3. no classifier, no answer, or an answer outside the set keeps the task on the
   row's own implementation -- a route can redirect, never lose, a task.

Step 2's answer is read twice. It says which implementation the work is for,
and it is also the only thing on this seat that knows *what kind of work it
is*: the classifier answering a target is the entry learning that this task is
the target's specialty. ``_target_open`` then decides whether that specialty's
own lane may run here -- whether the deployment holds the credentials the
target's pipeline spends, whether the dispatch hands over the file the target
builds on, and the tier this dispatch runs at. When it may not, the task stays on the
row's own implementation and carries the closed route's own note with it, so the
lane that ends up building it is told what the other lane would have produced,
and how to produce it here.

That note is declared per route and never written in this module. The gate is
one piece of code serving every row that declares a route, and the only row
declaring one today fronts a deck engine: a sentence about decks compiled into
the gate would be appended, the day a second row routes, to requests that are
not decks. A route declaring no note hands the task over exactly as it arrived.

The wrapper is the row's backend as far as the host is concerned: binders and
config pushes are forwarded to every implementation (a target's own row is on
the table but nothing addresses it, so it would otherwise never be bound), and
every other attribute reads off the row's own implementation.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from collections.abc import Awaitable, Callable, Iterable, Sequence
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator, NamedTuple

from loguru import logger
from opentelemetry import trace as otel_trace

_TRACER = otel_trace.get_tracer("oh_my_raven")

from raven.agent.subagent.attachments import turn_attachments_in_force
from raven.agent.subagent.backends.base import optional_keyword
from raven.agent.subagent.backends.cli_agent import CliAgentExitError, CliAgentTimeoutError
from raven.agent.subagent.mode_tiers import turn_tier_in_force
from raven.config.schema import TIER_LADDER
from raven.contracts.subagent_backend import SubagentActionAbortedError, SubagentBackend, SubagentNoAnswerError

#: Default bound on total attempts (revival of a transient-failing harness).
DEFAULT_MAX_ATTEMPTS: int = 3

#: ``(menu, task, default) -> chosen name or None``; ``menu`` is ``[(name, description), ...]``.
Router = Callable[[list[tuple[str, str]], str, str], Awaitable[str | None]]

#: ``(target, needs) -> bool``: whether the routed target holds what the route
#: declared its pipeline spends. Injected at the table (see ``AgentRegistry``)
#: rather than read from a module-level config, so what a process routes on is
#: what that process was built with.
#:
#: Both arguments are the question: the target names *whose* credentials are
#: being asked about -- a lane configured from its own product folder, not the
#: host loop -- and ``needs`` is the route's own declaration of what to ask for.
#: A route declaring nothing is never probed.
TargetReady = Callable[[str, Sequence[str]], bool]

#: Marks a route's note as the host speaking rather than the dispatching model.
#:
#: The lane reads one block of task text, and what arrived from the gate is a
#: requirement on the answer rather than part of the request being answered.
HOST_PREFIX = "\n\n[host] "


class SubagentRetryableError(RuntimeError):
    """Explicit adapter-level retryable failure eligible for ordered failover."""


class SubagentFailoverBlockedError(RuntimeError):
    """Failover blocked because a non-idempotent write attempt left dirty state."""

    def __init__(self, message: str, *, attempt_receipts: Sequence[dict[str, Any]] = ()) -> None:
        super().__init__(message)
        self.attempt_receipts: list[dict[str, Any]] = list(attempt_receipts)


class SubagentFailoverExhaustedError(RuntimeError):
    """All eligible failover targets failed."""

    def __init__(self, message: str, *, attempts: Sequence[dict[str, Any]] = ()) -> None:
        super().__init__(message)
        self.attempts: list[dict[str, Any]] = list(attempts)


_NON_RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    SubagentActionAbortedError,
    asyncio.CancelledError,
    ValueError,
    PermissionError,
)

_RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    TimeoutError,
    ConnectionError,
    OSError,
    SubagentNoAnswerError,
    SubagentRetryableError,
    CliAgentExitError,
    CliAgentTimeoutError,
)

_SECRET_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"sk-[A-Za-z0-9_-]{6,}"),
    re.compile(r"ghp_[A-Za-z0-9]{6,}"),
    re.compile(r"(?i)Bearer\s+[A-Za-z0-9._\-+/=]+"),
    re.compile(r"(?i)(api_key\s*[:=]\s*['\"]?)([^\s,'\"};]+)"),
)


def _redact_secrets(value: Any) -> Any:
    if isinstance(value, str):
        text = value
        for pat in _SECRET_PATTERNS[:3]:
            text = pat.sub("[REDACTED]", text)
        text = _SECRET_PATTERNS[3].sub(r"\1[REDACTED]", text)
        return text
    if isinstance(value, dict):
        return {k: _redact_secrets(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_redact_secrets(v) for v in value]
    return value


class TelemetryLogger:
    """Process-safe observational JSONL logger for specialist routing runs."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)

    @property
    def lock_path(self) -> Path:
        return self.path.with_name(f"{self.path.name}.lock")

    @contextmanager
    def _file_lock(self) -> Iterator[None]:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.lock_path.open("a+b") as lock_fh:
            if os.name == "nt":
                import msvcrt

                lock_fh.seek(0, os.SEEK_END)
                if lock_fh.tell() == 0:
                    lock_fh.write(b"\0")
                    lock_fh.flush()
                lock_fh.seek(0)
                msvcrt.locking(lock_fh.fileno(), msvcrt.LK_LOCK, 1)
                try:
                    yield
                finally:
                    lock_fh.seek(0)
                    msvcrt.locking(lock_fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl

                fcntl.flock(lock_fh.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_fh.fileno(), fcntl.LOCK_UN)

    def _recover_corrupt_tail_unlocked(self) -> bool:
        if not self.path.exists():
            return False
        raw = self.path.read_bytes()
        if not raw:
            return False
        lines = raw.splitlines(keepends=True)
        valid_end = len(lines)
        while valid_end > 0:
            candidate = lines[valid_end - 1]
            stripped = candidate.strip()
            if not stripped:
                valid_end -= 1
                continue
            try:
                if not candidate.endswith(b"\n"):
                    raise ValueError("unterminated line")
                parsed = json.loads(stripped.decode("utf-8"))
                if not isinstance(parsed, dict):
                    raise ValueError("not a JSON object")
                break
            except Exception:  # noqa: BLE001
                valid_end -= 1
        if valid_end == len(lines):
            return False
        kept = b"".join(lines[:valid_end])
        with self.path.open("wb") as fh:
            fh.write(kept)
            fh.flush()
        return True

    def recover_corrupt_tail(self) -> bool:
        try:
            with self._file_lock():
                return self._recover_corrupt_tail_unlocked()
        except Exception as exc:  # noqa: BLE001
            logger.warning("specialist telemetry tail recovery failed for {}: {}", self.path, exc)
            return False

    def write_record(self, record: dict[str, Any]) -> None:
        try:
            sanitized = _redact_secrets(record)
            line = (json.dumps(sanitized, ensure_ascii=True, separators=(",", ":")) + "\n").encode("utf-8")
            with self._file_lock():
                self._recover_corrupt_tail_unlocked()
                with self.path.open("ab") as fh:
                    fh.write(line)
                    fh.flush()
        except Exception as exc:  # noqa: BLE001
            logger.warning("specialist telemetry write failed for {}: {}", self.path, exc)

    def record_routing_decision(self, record: dict[str, Any]) -> None:
        with _TRACER.start_as_current_span("omr.routing.run") as span:
            span.set_attribute("omr.specialist", str(record.get("specialist") or record.get("specialist_name") or ""))
            attempts = record.get("attempts") or record.get("failover_chain") or []
            span.set_attribute("omr.attempts", len(attempts))
            span.set_attribute("omr.chosen_backend", str(record.get("chosen_backend") or ""))
            for attempt in attempts:
                span.add_event("omr.routing.attempt", attributes={k: v for k, v in attempt.items() if isinstance(v, (str, bool, int, float))})
            self.write_record(record)



def handed_files(*texts: str | None, media: Iterable[Any] = ()) -> tuple[str, ...]:
    """The files this dispatch hands the implementation, by path.

    Only files the user attached count, and they are read from where the host
    holds them rather than from the task text. A direct chat carries them as
    ``media`` (``Media`` rows, or plain paths): every one of those is handed
    over, since the transport puts them beside the text. A spawn or a DAG node
    carries none -- the dispatching model passes a file on by writing its path
    into the task -- so there the turn's own attachments are read
    (:func:`turn_attachments_in_force`) and one counts when the task names it.

    Read this way rather than by scanning the text for anything that looks like
    a file, because the text does not settle the question: every deck brief the
    dispatching model writes names the deck's destination in the same spelling
    as a template (``.../tmp/tui/history.pptx``), and one that mentions a stray
    file in the working directory names that too. Neither was handed over.
    Conversely an attachment the user told the model to ignore is not named in
    the task, and stays where it is.
    """
    given = [path for path in (_path_of(item) for item in media or ()) if path]
    if given:
        return tuple(given)
    handed: list[str] = []
    for item in turn_attachments_in_force():
        path = _path_of(item)
        if not path:
            continue
        # The path or its name, bounded the way a file name is on both sides:
        # ``brand.pptx`` in ``brand.pptx.bak`` or in ``new_brand.pptx`` is a
        # different file, while ``brand.pptx.`` at the end of a sentence is
        # this one -- a dot closes the name unless it opens a longer one.
        named = re.compile(r"(?<![\w.-])" + re.escape(Path(path).name) + r"(?![\w-])(?!\.[\w-])")
        if any(text and named.search(text) for text in texts):
            handed.append(path)
    return tuple(handed)


def _path_of(item: Any) -> str:
    """A ``Media`` row's path, or a plain string path; anything else is not a file."""
    path = getattr(item, "path", item)
    return path if isinstance(path, str) else ""


class RouteTarget(NamedTuple):
    """One candidate target as the entry holds it.

    ``name`` and ``description`` are the roster line the classifier picks from;
    ``owes`` and ``note`` are the declaring row's own words for the case where
    the gate keeps the work here, and ``needs``, ``min_tier`` and ``needs_file``
    are what put a route under the gate at all. A plain ``(name, description,
    backend)`` still builds one, with every declared field saying nothing --
    which is what a route that declares nothing means, and such a route is
    neither probed nor tiered nor asked for a file.
    """

    name: str
    description: str
    backend: SubagentBackend
    owes: str = ""
    note: str = ""
    needs: tuple[str, ...] = ()
    min_tier: str = ""
    needs_file: str = ""
    fallback: bool = False
    priority: int = 0
    model: str = ""

    def hand_off(self) -> str:
        """What to append to a task classified for this route that stays here."""
        text = self.note.strip()
        return f"{HOST_PREFIX}{text}" if text else ""


class Picked(NamedTuple):
    """Which implementation runs this task, and what the entry owes it.

    A tuple because ``pick`` has always answered ``(name, backend)`` and callers
    index it that way; ``note`` is the third thing the seat now knows and had
    nowhere to put -- see the module docstring for why it is the classifier's
    answer, not the task text, that produces it.
    """

    name: str
    backend: SubagentBackend
    note: str = ""


_BROADCAST = (
    "bind_session_dir",
    "bind_event_sink",
    "bind_caps_listener",
    "bind_unprompted_announcer",
    "set_mcp_source",
    "repoint_pooled_resident",
)


class RoutingBackend(SubagentBackend):
    def __init__(
        self,
        name: str,
        primary: SubagentBackend,
        targets: Sequence[tuple[str, str, SubagentBackend] | RouteTarget],
        *,
        instances: Any,
        target_ready: TargetReady | None = None,
        max_failovers: int = 2,
        max_attempts: int | None = None,
        cooldown_s: float = 60.0,
        max_review_cycles: int = 3,
        clock: Callable[[], float] = time.monotonic,
        telemetry_path: Path | None = None,
        worktree_dirty_check: Callable[[Path], list[str]] | None = None,
        specialist_definition: Any = None,
        is_dynamic_specialist: bool = False,
        pool_load_state: dict[str, dict[str, int]] | None = None,
    ) -> None:
        """``targets`` are ``(name, description, backend)`` in route order.

        ``target_ready`` answers whether the deployment holds what a target's own
        pipeline spends; ``None`` is a process that never told the table, and
        reads as ready -- the pre-gate behaviour.
        """
        self.name = name
        self._primary = primary
        self._own = primary
        self._targets = [t if isinstance(t, RouteTarget) else RouteTarget(*t) for t in targets]
        self._instances = instances
        self._target_ready = target_ready
        self._router: Router | None = None
        self._max_failovers = max(0, int(max_failovers))
        # Conductor revival: when set, the chain is cycled (dead implementors are
        # re-spawned) until exactly this many attempts have been made.
        # Precedence: an explicit ``max_attempts`` is an exact total-attempt cap
        # (truncates or cycles the chain). When omitted, the cap is
        # max(DEFAULT_MAX_ATTEMPTS, 1 + max_failovers), so a configured
        # ``max_failovers`` is never truncated and short chains still revive.
        self._max_attempts = (
            max(1, int(max_attempts))
            if max_attempts is not None
            else max(DEFAULT_MAX_ATTEMPTS, 1 + self._max_failovers)
        )
        self.last_attempts: list[dict[str, Any]] = []
        self._cooldown_s = float(cooldown_s)
        self.max_review_cycles = max(1, int(max_review_cycles))
        self._clock = clock
        self._telemetry_path = Path(telemetry_path) if telemetry_path is not None else None
        self._worktree_dirty_check = worktree_dirty_check
        self._cooldown_until: dict[str, float] = {}
        self._instance_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._session_dir_resolver: Callable[..., Any] | None = None
        self.specialist_definition = specialist_definition
        self._is_dynamic_specialist = bool(is_dynamic_specialist)
        if pool_load_state is not None:
            self._dispatch_counts: dict[str, int] = pool_load_state.setdefault("dispatch_counts", {})
            self._inflight_counts: dict[str, int] = pool_load_state.setdefault("inflight_counts", {})
        else:
            self._dispatch_counts = {}
            self._inflight_counts = {}

    @property
    def streams(self) -> bool:  # type: ignore[override]
        return bool(getattr(self._primary, "streams", False))

    def set_router(self, router: Router | None) -> None:
        self._router = router

    def set_target_ready(self, target_ready: TargetReady | None) -> None:
        self._target_ready = target_ready

    def implementations(self) -> list[SubagentBackend]:
        seen: list[SubagentBackend] = []
        for b in [self._primary, *(target.backend for target in self._targets)]:
            if all(b is not existing for existing in seen):
                seen.append(b)
        return seen

    def __getattr__(self, attr: str) -> Any:
        if attr.startswith("_"):
            raise AttributeError(attr)
        if attr in _BROADCAST:
            return self._broadcast(attr)
        return getattr(self._primary, attr)

    def _broadcast(self, attr: str) -> Callable[..., None]:
        def call(*args: Any, **kwargs: Any) -> None:
            if attr == "bind_session_dir" and args and callable(args[0]):
                self._session_dir_resolver = args[0]
            for backend in self.implementations():
                member = getattr(backend, attr, None)
                if callable(member):
                    member(*args, **kwargs)

        return call

    def _is_cooled_down(self, target_name: str) -> bool:
        until = self._cooldown_until.get(target_name)
        if until is None:
            return False
        if self._clock() < until:
            return True
        self._cooldown_until.pop(target_name, None)
        return False

    def _record_cooldown(self, target_name: str) -> None:
        if self._cooldown_s > 0:
            self._cooldown_until[target_name] = self._clock() + self._cooldown_s

    def _instance_lock(self, session_key: str | None, instance: str) -> asyncio.Lock:
        key = (session_key or "default", instance)
        lock = self._instance_locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._instance_locks[key] = lock
        return lock

    async def _unbind_failed_instance(self, session_key: str | None, agent: str, instance: str | None) -> None:
        if not instance or self._instances is None:
            return
        skey = session_key or "default"
        async with self._instance_lock(skey, instance):
            unbind_fn = getattr(self._instances, "unbind", None)
            if callable(unbind_fn):
                try:
                    res = unbind_fn(skey, agent, instance)
                    if isinstance(res, Awaitable):
                        await res
                    return
                except Exception as exc:  # noqa: BLE001
                    logger.warning("{!r}: failed to unbind instance {!r} on {!r}: {}", self.name, instance, agent, exc)
            rows = getattr(self._instances, "rows", None)
            if isinstance(rows, list):
                for row in rows:
                    if isinstance(row, dict) and row.get("sessionKey") == skey and row.get("agent") == agent and row.get("handle") == instance:
                        row["agentId"] = None

    def _target_open(self, target: RouteTarget, mode: str | None, handed: Sequence[str] = ()) -> str:
        """``""`` when ``target`` may run this dispatch, else why it may not.

        Three conditions, none of them a judgement the classifier could make:
        two are about the deployment and the third is a fact about what the
        dispatch carries. All three read off the *route's own declaration*, so a
        route that declares none is returned unconditionally open. ``routes`` is
        a general facility: a row routing for reasons of its own would otherwise
        be closed by a credential its target never spends, a file it never asked
        for and a tier it never named, having opted into nothing.

        The credentials come first because they are the flat answer: a target
        whose pipeline cannot buy a picture builds the same deck at every tier.
        The file comes second: a target that builds on one the user supplies
        has nothing to build on without it, at any tier. The tier comes last,
        and where it is read from depends on what kind of dispatch this is; see
        :meth:`_tier_in_force`.
        """
        if target.needs and self._target_ready is not None:
            try:
                ready = bool(self._target_ready(target.name, target.needs))
            except Exception as exc:  # noqa: BLE001 - a readiness probe must not lose the task
                logger.warning(
                    "{!r}: could not read what {!r} needs; treating as ready: {}", self.name, target.name, exc
                )
                ready = True
            if not ready:
                return "a credential its lane spends is not configured here"
        if target.needs_file:
            suffix = target.needs_file.lower()
            if not any(name.lower().endswith(suffix) for name in handed):
                return f"no {target.needs_file} the user attached was handed over"
        if not target.min_tier:
            return ""
        where, tier = self._tier_in_force(mode)
        if not tier:
            # Nothing declared one. Three separate facts land here -- a turn that
            # began with no tier, a dispatch outside any turn that names no mode,
            # and a deployment that sets neither -- and none of them is a low
            # tier. Reading the absence as one would close the target on a
            # deployment that never asked for tiers at all.
            return ""
        if tier not in TIER_LADDER or target.min_tier not in TIER_LADDER:
            # Another vocabulary. ``resolve_tier`` declines to guess across one
            # rather than pick a nearest rung it cannot order, and so does this.
            return ""
        if TIER_LADDER.index(tier) < TIER_LADDER.index(target.min_tier):
            return f"{where} runs at {tier!r}, below {target.min_tier!r}"
        return ""

    @staticmethod
    def _tier_in_force(mode: str | None) -> tuple[str, str]:
        """Where this dispatch's tier comes from, and what it is.

        Two lanes reach this seat and they carry the same fact in two places. A
        spawn, a DAG node and their retries run inside a host turn, and the tier
        is the turn's own frozen snapshot -- frozen for the reason ``turn_tier``
        freezes it, so a switch arriving mid-turn belongs to the next turn and a
        dispatch late in this one cannot pick up a tier the turn never started
        under. A direct chat has no turn scope at all, and its tier is the
        ``mode`` the manager resolved for the dispatch (the instance's own
        override, else the session's standing tier).

        The absence of a turn is therefore not the absence of a tier, which is
        what this used to read it as: every direct chat -- the surface the user
        actually talks to a row through -- reached the target at every tier, and
        no setting on the page could exercise the other lane.
        """
        turn = turn_tier_in_force()
        if turn is None:
            return "this chat", (mode or "").strip()
        return "the turn", turn.strip()

    async def pick(
        self,
        task: str,
        *,
        session_key: str | None,
        instance: str | None,
        mode: str | None = None,
        handed: Sequence[str] = (),
    ) -> Picked:
        """What runs this task; see the module docstring for the order.

        ``handed`` is what the dispatch carries by file name (see
        :func:`handed_files`); a route declaring ``needs_file`` opens on it.
        """
        if instance and (bound := await self._bound(session_key, instance)) is not None:
            if not self._is_cooled_down(bound.name):
                return bound
        if self._router is not None and self._targets:
            menu = [(target.name, target.description) for target in self._targets]
            try:
                answer = await self._router(menu, task, self.name)
            except Exception as exc:  # noqa: BLE001 - a routing failure must not lose the task
                logger.warning("{!r}: the route classifier failed; running here: {}", self.name, exc)
                answer = None
            for target in self._targets:
                if answer == target.name:
                    if self._is_cooled_down(target.name):
                        logger.info("{!r}: classified for {!r}, but target is in cooldown; running here", self.name, target.name)
                        return Picked(self.name, self._primary, target.hand_off())
                    if closed := self._target_open(target, mode, handed):
                        logger.info(
                            "{!r}: classified for {!r}, but {}; running here{}",
                            self.name,
                            target.name,
                            closed,
                            f" and owing a {target.owes}" if target.owes else "",
                        )
                        return Picked(self.name, self._primary, target.hand_off())
                    logger.info("{!r}: classified for {!r}; routing there", self.name, target.name)
                    return Picked(target.name, target.backend)
            if answer is not None and answer != self.name:
                logger.warning(
                    "{!r}: the route classifier answered {!r}, not a candidate; running here", self.name, answer
                )
        if self._is_dynamic_specialist and self._targets:
            stats = self._load_telemetry_stats(session_key)
            ordered = sorted(
                enumerate(self._targets),
                key=lambda item: self._target_rank_key(
                    item[0], item[1], session_key=session_key, stats=stats
                ),
            )
            for _, target in ordered:
                if self._is_cooled_down(target.name):
                    continue
                if self._target_open(target, mode, handed):
                    continue
                return Picked(target.name, target.backend, target.hand_off())
            return Picked(ordered[0][1].name, ordered[0][1].backend, ordered[0][1].hand_off())
        return Picked(self.name, self._primary)

    def _load_telemetry_stats(self, session_key: str | None = None) -> dict[str, dict[str, float]]:
        tpath = self._resolve_telemetry_path(session_key)
        if tpath is None or not tpath.exists():
            return {}
        counts: dict[str, list[tuple[bool, float]]] = {}
        try:
            for line in tpath.read_text(encoding="utf-8", errors="replace").splitlines():
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    row = json.loads(stripped)
                except ValueError as exc:
                    logger.debug("{!r}: skipping malformed telemetry line: {}", self.name, exc)
                    continue
                if not isinstance(row, dict):
                    continue
                chain = row.get("failover_chain")
                if isinstance(chain, list) and chain:
                    for item in chain:
                        if not isinstance(item, dict):
                            continue
                        bname = str(item.get("backend") or "")
                        if not bname:
                            continue
                        ok = item.get("status") == "succeeded"
                        dur = float(item.get("duration_ms") or 0.0)
                        counts.setdefault(bname, []).append((ok, max(0.0, dur)))
                else:
                    bname = str(row.get("chosen_backend") or "")
                    if bname:
                        ok = row.get("final_status") == "succeeded"
                        dur = float(row.get("total_duration_ms") or 0.0)
                        counts.setdefault(bname, []).append((ok, max(0.0, dur)))
        except Exception as exc:  # noqa: BLE001
            logger.warning("{!r}: failed to load telemetry stats from {}: {}", self.name, tpath, exc)
            return {}

        stats: dict[str, dict[str, float]] = {}
        for bname, samples in counts.items():
            total = len(samples)
            if total == 0:
                continue
            successes = sum(1 for ok, _ in samples if ok)
            avg_lat = sum(dur for _, dur in samples) / total
            stats[bname] = {
                "runs": float(total),
                "success_rate": successes / total,
                "avg_latency_ms": avg_lat,
            }
        return stats

    def _workload_score(
        self,
        backend_name: str,
        *,
        session_key: str | None = None,
        stats: dict[str, dict[str, float]] | None = None,
    ) -> tuple[float, float]:
        resolved = stats if stats is not None else self._load_telemetry_stats(session_key)
        entry = resolved.get(backend_name)
        if not entry:
            return (0.0, 0.0)
        return (float(entry.get("success_rate", 0.0)), -float(entry.get("avg_latency_ms", 0.0)))

    def _load_tiebreak_key(self, backend_name: str) -> tuple[int, int]:
        return (
            int(self._inflight_counts.get(backend_name, 0)),
            int(self._dispatch_counts.get(backend_name, 0)),
        )

    def _target_rank_key(
        self,
        declaration_index: int,
        target: RouteTarget,
        *,
        session_key: str | None = None,
        stats: dict[str, dict[str, float]] | None = None,
    ) -> tuple[int, float, float, int, int, int]:
        score = self._workload_score(target.name, session_key=session_key, stats=stats)
        inflight, dispatches = self._load_tiebreak_key(target.name)
        return (
            target.priority,
            -score[0],
            -score[1],
            inflight,
            dispatches,
            declaration_index,
        )

    def _iter_candidates(
        self,
        picked: Picked,
        *,
        mode: str | None = None,
        handed: Sequence[str] = (),
        session_key: str | None = None,
    ) -> list[Picked]:
        """Ordered candidate chain starting with ``picked`` and up to ``max_failovers`` fallbacks."""
        chain: list[Picked] = [picked]
        tried: set[str] = {picked.name}
        stats = self._load_telemetry_stats(session_key)
        ordered_targets = sorted(
            enumerate(self._targets),
            key=lambda item: self._target_rank_key(
                item[0], item[1], session_key=session_key, stats=stats
            ),
        )
        for _, target in ordered_targets:
            if target.name in tried:
                continue
            if self._is_cooled_down(target.name):
                continue
            if self._target_open(target, mode, handed):
                continue
            tried.add(target.name)
            chain.append(Picked(target.name, target.backend, target.hand_off()))
        if not self._is_dynamic_specialist and self.name not in tried and not self._is_cooled_down(self.name):
            tried.add(self.name)
            chain.append(Picked(self.name, self._own, picked.note))
        return chain[: 1 + self._max_failovers]

    def failover_chain(
        self,
        picked: Picked,
        *,
        mode: str | None = None,
        handed: Sequence[str] = (),
        session_key: str | None = None,
    ) -> list[Picked]:
        """Ordered candidate chain starting with ``picked`` and up to ``max_failovers`` fallbacks."""
        return self._iter_candidates(picked, mode=mode, handed=handed, session_key=session_key)

    def _model_for(self, backend_name: str, backend: SubagentBackend, kwargs: dict[str, Any]) -> str:
        for target in self._targets:
            if target.name == backend_name and target.model:
                return str(target.model)
        if kwargs.get("model"):
            return str(kwargs["model"])
        return str(getattr(backend, "model", "") or "")

    def _check_dirty_worktree(self, workspace: Path) -> list[str]:
        if self._worktree_dirty_check is not None:
            try:
                return [str(p) for p in (self._worktree_dirty_check(workspace) or [])]
            except Exception as exc:  # noqa: BLE001
                logger.warning("{!r}: worktree_dirty_check failed on {}: {}", self.name, workspace, exc)
                return ["<dirty_check_failed>"]
        if not workspace or not (workspace / ".git").exists():
            return []
        try:
            git_bin = shutil.which("git") or "git"
            proc = subprocess.run(
                [git_bin, "-C", str(workspace), "status", "--porcelain", "-uall"],
                capture_output=True,
                text=True,
                timeout=10,
                check=False,
            )
            if proc.returncode != 0:
                return ["<dirty_check_failed>"]
            return [line[3:].strip() for line in proc.stdout.splitlines() if line.strip()]
        except Exception:  # noqa: BLE001
            return ["<dirty_check_failed>"]

    async def _cleanup_failed_attempt(self, backend: Any, task_id: str, workspace: Path) -> bool:
        cleaned = False
        cleanup_fn = getattr(backend, "cleanup_attempt", None)
        if callable(cleanup_fn):
            try:
                res = cleanup_fn(task_id, workspace)
                if isinstance(res, Awaitable):
                    res = await res
                cleaned = bool(res) if res is not None else True
            except Exception as exc:  # noqa: BLE001
                logger.warning("{!r}: cleanup_attempt failed for {}: {}", self.name, task_id, exc)
        cancel_fn = getattr(backend, "cancel", None)
        if callable(cancel_fn):
            try:
                res = cancel_fn(task_id)
                if isinstance(res, Awaitable):
                    res = await res
                cleaned = True if res is None else (cleaned or bool(res))
            except Exception as exc:  # noqa: BLE001
                logger.warning("{!r}: cancel failed for {}: {}", self.name, task_id, exc)
        return cleaned

    def _resolve_telemetry_path(self, session_key: str | None) -> Path | None:
        if self._telemetry_path is not None:
            return self._telemetry_path
        if self._session_dir_resolver is not None and session_key:
            try:
                sdir = self._session_dir_resolver(session_key)
                if sdir:
                    return Path(sdir) / "specialist_telemetry.jsonl"
            except Exception:  # noqa: BLE001
                return None
        return None

    def _emit_telemetry(
        self,
        *,
        session_key: str | None,
        task_id: str,
        workspace: Path,
        chosen_backend: str,
        model: str,
        failover_chain_records: list[dict[str, Any]],
        final_status: str,
        primary_model: str = "",
        total_duration_ms: float,
        skill_packs_injected: Sequence[str] = (),
        mcp_servers_attached: Sequence[str] = (),
        context_charter_injected: bool = False,
        cto_brief_injected: bool = False,
        specialist_dir: str = "",
        agents_md_path: str = "",
        session_cli_args: Sequence[str] = (),
        mcps_enabled: Sequence[str] = (),
        mcps_disabled: Sequence[str] = (),
        plugins_enabled: Sequence[str] = (),
    ) -> None:
        tpath = self._resolve_telemetry_path(session_key)
        if tpath is None:
            return
        record = {
            "schema_version": 1,
            "event_id": str(uuid.uuid4()),
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "task_id": str(task_id),
            "specialist": self.name,
            "attempt_count": len(failover_chain_records),
            "chosen_backend": chosen_backend,
            "model": model,
            "failover_chain": failover_chain_records,
            "same_model_fallback": bool(primary_model) and primary_model == model and chosen_backend != self.name,
            "final_status": final_status,
            "total_duration_ms": round(max(0.0, total_duration_ms), 3),
            "worktree_path": str(workspace) if workspace is not None else "",
            "skill_packs_injected": [str(s) for s in skill_packs_injected if str(s)],
            "mcp_servers_attached": [str(m) for m in mcp_servers_attached if str(m)],
            "context_charter_injected": bool(context_charter_injected),
            "cto_brief_injected": bool(cto_brief_injected),
            "specialist_dir": specialist_dir,
            "agents_md_path": agents_md_path,
            "session_cli_args": list(session_cli_args),
            "mcps_enabled": list(mcps_enabled),
            "mcps_disabled": list(mcps_disabled),
            "plugins_enabled": list(plugins_enabled),
        }
        TelemetryLogger(tpath).write_record(record)

    async def _bound(self, session_key: str | None, instance: str) -> Picked | None:
        """The implementation whose transport already holds this handle, if any.

        Read off the instance registry rows the transports write when a session
        is bound (``agentId`` set). The manager's own status row for the handle
        is written under this row's name before the first run and carries no
        session, so it is not evidence; a bound session under this row's name is.
        """
        try:
            records = self._instances.list_instances(session_key or "default")
        except Exception as exc:  # noqa: BLE001 - the registry is best-effort storage
            logger.warning("{!r}: could not read the instance registry: {}", self.name, exc)
            return None
        bound = {r.get("agent") for r in records if r.get("handle") == instance and r.get("agentId")}
        for target in self._targets:
            if target.name in bound:
                return Picked(target.name, target.backend)
        if self.name in bound:
            return Picked(self.name, self._primary)
        return None

    def _forward_candidate_attributes(self, candidate_name: str, backend: Any) -> None:
        self.last_chosen_backend = candidate_name
        self.last_chosen_backend_obj = backend
        for receipt_attr in (
            "last_review_receipt",
            "last_cto_strategic_brief",
            "last_cto_brief",
            "last_consequence_matrix",
            "last_skill_evidence",
            "last_invoked_tools",
            "last_invoked_mcps",
            "execution_identity",
            "ocr_available",
            "expected_requirement_ids",
            "require_acceptance_receipt",
        ):
            if hasattr(backend, receipt_attr):
                val = getattr(backend, receipt_attr, None)
                if val is not None:
                    setattr(self, receipt_attr, val)

    def _prepare_specialist_bundle(
        self, base_task: str, kwargs: dict[str, Any], call_kwargs: dict[str, Any]
    ) -> tuple[str, tuple[str, ...], list[str]]:
        skill_packs_injected: tuple[str, ...] = ()
        mcp_servers_attached: list[str] = list(kwargs.get("mcps") or [])
        if self.specialist_definition is not None:
            from raven.agent.subagent.oh_my_raven import SkillPackResolver, SpecialistDefinition

            spec_def = SpecialistDefinition.from_obj(self.specialist_definition)
            self._active_spec_def = spec_def
            if spec_def.has_specialist_bundle:
                base_task = SkillPackResolver.render_specialist_bundle(spec_def, base_task)
                skill_packs_injected = tuple(spec_def.skill_packs)
                if spec_def.mcp_servers:
                    existing_mcps = list(kwargs.get("mcps") or [])
                    for srv in spec_def.mcp_servers:
                        if srv not in existing_mcps:
                            existing_mcps.append(srv)
                    call_kwargs["mcps"] = existing_mcps
                    mcp_servers_attached = list(existing_mcps)
        return base_task, skill_packs_injected, mcp_servers_attached

    def _verify_write_failover_safety(
        self,
        *,
        candidate: Picked,
        cleaned: bool,
        idempotent: bool,
        error_class: str,
        duration_ms: float,
        attempt_records: list[dict[str, Any]],
        session_key: str | None,
        task_id: str,
        workspace: Path,
        call_kwargs: dict[str, Any],
        first_model: str,
        run_start: float,
        telemetry_extra: dict[str, Any],
        exc: BaseException,
    ) -> None:
        has_cleanup_hook = callable(getattr(candidate.backend, "cleanup_attempt", None)) or callable(
            getattr(candidate.backend, "cancel", None)
        )
        if has_cleanup_hook and not cleaned:
            receipt = {
                "backend": candidate.name,
                "status": "blocked_unverified_cleanup",
                "error_class": error_class,
                "duration_ms": duration_ms,
                "dirty_paths": [],
            }
            attempt_records.append(receipt)
            self._emit_telemetry(
                session_key=session_key,
                task_id=task_id,
                workspace=workspace,
                chosen_backend=candidate.name,
                model=self._model_for(candidate.name, candidate.backend, call_kwargs),
                primary_model=first_model,
                failover_chain_records=attempt_records,
                final_status="blocked",
                total_duration_ms=(self._clock() - run_start) * 1000.0,
                **telemetry_extra,
            )
            raise SubagentFailoverBlockedError(
                f"{self.name!r}: failover blocked after {candidate.name!r} failed with {error_class} and cleanup was unverified",
                attempt_receipts=attempt_records,
            ) from exc
        dirty_paths = self._check_dirty_worktree(workspace)
        if dirty_paths or not idempotent:
            receipt = {
                "backend": candidate.name,
                "status": "blocked_dirty_worktree",
                "error_class": error_class,
                "duration_ms": duration_ms,
                "dirty_paths": dirty_paths,
            }
            attempt_records.append(receipt)
            self._emit_telemetry(
                session_key=session_key,
                task_id=task_id,
                workspace=workspace,
                chosen_backend=candidate.name,
                model=self._model_for(candidate.name, candidate.backend, call_kwargs),
                primary_model=first_model,
                failover_chain_records=attempt_records,
                final_status="blocked",
                total_duration_ms=(self._clock() - run_start) * 1000.0,
                **telemetry_extra,
            )
            raise SubagentFailoverBlockedError(
                f"{self.name!r}: failover blocked after {candidate.name!r} failed with {error_class} and left dirty paths {dirty_paths!r}",
                attempt_receipts=attempt_records,
            ) from exc

    async def _handle_attempt_exception(
        self,
        *,
        idx: int,
        chain: list[Picked],
        candidate: Picked,
        exc: BaseException,
        attempt_start: float,
        run_start: float,
        writes: bool,
        idempotent: bool,
        attempt_records: list[dict[str, Any]],
        session_key: str | None,
        instance: str | None,
        task_id: str,
        workspace: Path,
        call_kwargs: dict[str, Any],
        first_model: str,
        telemetry_extra: dict[str, Any],
    ) -> None:
        duration_ms = round(max(0.0, (self._clock() - attempt_start) * 1000.0), 3)
        error_class = type(exc).__name__
        session_attempt = getattr(self, "last_session_flags", {})
        session_fields = ({"harness_family": session_attempt.get("harness_family", ""), "specialist_dir": session_attempt.get("specialist_dir", "")} if session_attempt else {})
        if isinstance(exc, _NON_RETRYABLE_ERRORS) or not isinstance(exc, _RETRYABLE_ERRORS):
            attempt_records.append(
                {"backend": candidate.name, "status": "aborted", "error_class": error_class, "duration_ms": duration_ms, **session_fields}
            )
            self._emit_telemetry(
                session_key=session_key,
                task_id=task_id,
                workspace=workspace,
                chosen_backend=candidate.name,
                model=self._model_for(candidate.name, candidate.backend, call_kwargs),
                primary_model=first_model,
                failover_chain_records=attempt_records,
                final_status="failed",
                total_duration_ms=(self._clock() - run_start) * 1000.0,
                **telemetry_extra,
            )
            raise exc
        cleaned = await self._cleanup_failed_attempt(candidate.backend, task_id, workspace)
        if writes:
            self._verify_write_failover_safety(
                candidate=candidate,
                cleaned=cleaned,
                idempotent=idempotent,
                error_class=error_class,
                duration_ms=duration_ms,
                attempt_records=attempt_records,
                session_key=session_key,
                task_id=task_id,
                workspace=workspace,
                call_kwargs=call_kwargs,
                first_model=first_model,
                run_start=run_start,
                telemetry_extra=telemetry_extra,
                exc=exc,
            )
        attempt_records.append(
            {"backend": candidate.name, "status": "failed", "error_class": error_class, "duration_ms": duration_ms, **session_fields}
        )
        self._record_cooldown(candidate.name)
        await self._unbind_failed_instance(session_key, candidate.name, instance)
        if len(chain) == 1:
            raise exc
        if idx + 1 < len(chain):
            logger.warning(
                "{!r}: attempt {} on {!r} failed with {}; failing over to {!r}",
                self.name,
                idx + 1,
                candidate.name,
                error_class,
                chain[idx + 1].name,
            )

    async def _run(
        self,
        task: str,
        *,
        task_id: str,
        workspace: Path,
        executor: Any,
        session_key: str | None = None,
        instance: str | None = None,
        authored_task: str | None = None,
        **kwargs: Any,
    ) -> str:
        base_task = task
        base_authored_task = authored_task
        mode = kwargs.get("mode")
        media_items = kwargs.get("media") or ()
        writes = bool(kwargs.get("writes", False))
        idempotent = bool(kwargs.get("idempotent", True))
        call_kwargs = {k: v for k, v in kwargs.items() if k not in ("writes", "idempotent")}
        base_task, skill_packs_injected, mcp_servers_attached = self._prepare_specialist_bundle(
            base_task, kwargs, call_kwargs
        )
        handed = handed_files(base_task, base_authored_task, media=media_items)
        picked = await self.pick(
            base_authored_task or task,
            session_key=session_key,
            instance=instance,
            mode=mode,
            handed=handed,
        )
        chain = self.failover_chain(picked, mode=mode, handed=handed, session_key=session_key)
        if self._max_attempts is not None:
            base_chain = chain[: max(1, min(len(chain), self._max_attempts))]
            chain = [base_chain[i % len(base_chain)] for i in range(self._max_attempts)]
        first_model = self._model_for(chain[0].name, chain[0].backend, call_kwargs)
        run_start = self._clock()
        attempt_records: list[dict[str, Any]] = []
        self.last_attempts = attempt_records
        last_exc: BaseException | None = None
        telemetry_extra = {
            "skill_packs_injected": skill_packs_injected,
            "mcp_servers_attached": mcp_servers_attached,
            "context_charter_injected": "[BEGIN UNTRUSTED CONTEXT CHARTER]" in base_task,
            "cto_brief_injected": "[BEGIN CTO STRATEGIC BRIEF]" in base_task,
        }

        for idx, candidate in enumerate(chain):
            session_bundle: dict[str, Any] = {}
            if self.specialist_definition is not None:
                from raven.agent.subagent.oh_my_raven import SessionHarnessFlagBuilder
                session_bundle = SessionHarnessFlagBuilder.build_for_candidate(self.specialist_definition, candidate.name, backend=candidate.backend)
                spec = getattr(self, "_active_spec_def", None)
                if spec and (spec.specialist_dir or spec.agents_md_path or spec.skill_packs or spec.mcp_servers or spec.disabled_mcp_servers or spec.plugins):
                    self.last_session_flags = session_bundle
            telemetry_extra.update({k: session_bundle.get(k, ()) for k in ("specialist_dir", "agents_md_path", "session_cli_args", "mcps_enabled", "mcps_disabled", "plugins_enabled")})
            session_kwargs: dict[str, Any] = {}
            for key in ("specialist_dir", "agents_md_path", "session_cli_args", "session_mcp_config"):
                if session_bundle.get(key):
                    session_kwargs.update(optional_keyword(candidate.backend, key, session_bundle[key]))
            attempt_task = f"{base_task}{candidate.note}" if candidate.note else base_task
            attempt_authored = (
                f"{base_authored_task}{candidate.note}"
                if (base_authored_task is not None and candidate.note)
                else base_authored_task
            )
            attempt_start = self._clock()
            self._inflight_counts[candidate.name] = int(self._inflight_counts.get(candidate.name, 0)) + 1
            self._dispatch_counts[candidate.name] = int(self._dispatch_counts.get(candidate.name, 0)) + 1
            try:
                for attr in ("last_skill_evidence", "last_invoked_tools", "last_invoked_mcps"):
                    if hasattr(self, attr): delattr(self, attr)
                result = await candidate.backend.run(
                    attempt_task,
                    task_id=task_id,
                    workspace=workspace,
                    executor=executor,
                    session_key=session_key,
                    instance=instance,
                    **optional_keyword(candidate.backend, "authored_task", attempt_authored),
                    **session_kwargs,
                    **call_kwargs,
                )
                self._forward_candidate_attributes(candidate.name, candidate.backend)
                require_ev = bool(call_kwargs.get("require_skill_evidence", False) or getattr(self, "require_skill_evidence", False) or getattr(candidate.backend, "require_skill_evidence", False))
                if require_ev or call_kwargs.get("required_skill_packs") or call_kwargs.get("required_tools") or call_kwargs.get("required_mcps"):
                    from raven.agent.subagent.oh_my_raven import SkillAndToolReceiptGate
                    SkillAndToolReceiptGate.verify_specialist_execution(self, candidate.backend, result, delivered_prompt=attempt_task, require_skill_evidence=require_ev, required_skill_packs=call_kwargs.get("required_skill_packs") or (), required_tools=call_kwargs.get("required_tools") or (), required_mcps=call_kwargs.get("required_mcps") or ())
                duration_ms = round(max(0.0, (self._clock() - attempt_start) * 1000.0), 3)
                attempt_records.append({"backend": candidate.name, "status": "succeeded", "error_class": None, "duration_ms": duration_ms, **({"harness_family": session_bundle["harness_family"], "specialist_dir": session_bundle["specialist_dir"]} if session_bundle else {})})
                self._emit_telemetry(
                    session_key=session_key,
                    task_id=task_id,
                    workspace=workspace,
                    chosen_backend=candidate.name,
                    model=self._model_for(candidate.name, candidate.backend, call_kwargs),
                    primary_model=first_model,
                    failover_chain_records=attempt_records,
                    final_status="succeeded",
                    total_duration_ms=(self._clock() - run_start) * 1000.0,
                    **telemetry_extra,
                )
                return result
            except BaseException as exc:
                last_exc = exc
                await self._handle_attempt_exception(
                    idx=idx,
                    chain=chain,
                    candidate=candidate,
                    exc=exc,
                    attempt_start=attempt_start,
                    run_start=run_start,
                    writes=writes,
                    idempotent=idempotent,
                    attempt_records=attempt_records,
                    session_key=session_key,
                    instance=instance,
                    task_id=task_id,
                    workspace=workspace,
                    call_kwargs=call_kwargs,
                    first_model=first_model,
                    telemetry_extra=telemetry_extra,
                )
            finally:
                cur_inf = int(self._inflight_counts.get(candidate.name, 0))
                if cur_inf <= 1:
                    self._inflight_counts.pop(candidate.name, None)
                else:
                    self._inflight_counts[candidate.name] = cur_inf - 1

        last_candidate = chain[-1]
        self._emit_telemetry(
            session_key=session_key,
            task_id=task_id,
            workspace=workspace,
            chosen_backend=last_candidate.name,
            model=self._model_for(last_candidate.name, last_candidate.backend, call_kwargs),
            primary_model=first_model,
            failover_chain_records=attempt_records,
            final_status="exhausted",
            total_duration_ms=(self._clock() - run_start) * 1000.0,
            **telemetry_extra,
        )
        summary = ", ".join(f"{r['backend']}:{r['error_class']}" for r in attempt_records)
        raise SubagentFailoverExhaustedError(
            f"{self.name!r}: all {len(attempt_records)} failover attempts failed ({summary})",
            attempts=attempt_records,
        ) from last_exc

    async def run(self, task: str, **kwargs: Any) -> str:
        with _TRACER.start_as_current_span("omr.routing.run") as span:
            try:
                return await self._run(task, **kwargs)
            finally:
                span.set_attribute("omr.specialist", self.name)
                span.set_attribute("omr.attempts", len(self.last_attempts))
                span.set_attribute("omr.chosen_backend", str(getattr(self, "last_chosen_backend", "") or ""))
                for attempt in self.last_attempts:
                    span.add_event("omr.routing.attempt", attributes={k: v for k, v in attempt.items() if isinstance(v, (str, bool, int, float))})


__all__ = [
    "DEFAULT_MAX_ATTEMPTS",
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
    "handed_files",
]
