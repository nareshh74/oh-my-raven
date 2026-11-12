import asyncio
from pathlib import Path

import pytest

import oh_my_raven.routing as mod
from oh_my_raven.routing import RouteTarget, RoutingBackend


class Fake:
    kind = "acp"
    streams = False

    def __init__(self, name, fail=0, exc=TimeoutError):
        self.name, self.fail, self.exc, self.calls = name, fail, exc, 0

    async def run(self, task, **kw):
        self.calls += 1
        if self.calls <= self.fail:
            raise self.exc("boom")
        return f"{self.name} ok"


class Inst:
    def list_instances(self, k=None):
        return []


def mk(primary, targets=(), **kw):
    ts = [RouteTarget(t.name, t.name, t) for t in targets]
    return RoutingBackend("spec", primary, ts, instances=Inst(), cooldown_s=0, **kw)


def run(rb):
    return asyncio.run(rb.run("t", task_id="1", workspace=Path("."), executor=None))


def test_failover_to_next_harness_records_attempts():
    a, b = Fake("a", fail=9), Fake("b")
    rb = mk(a, [b], max_failovers=2)
    assert run(rb) == "b ok"
    assert [(x["backend"], x["status"]) for x in rb.last_attempts] == [("spec", "failed"), ("b", "succeeded")]
    assert rb.last_attempts[0]["error_class"] == "TimeoutError"


def test_revival_retries_same_harness_bounded():
    a = Fake("a", fail=2, exc=ConnectionError)
    rb = mk(a, max_attempts=3)
    assert run(rb) == "a ok"
    assert a.calls == 3 and len(rb.last_attempts) == 3


def test_exhaustion_surfaces_attempt_log():
    a = Fake("a", fail=99)
    rb = mk(a, max_attempts=3)
    with pytest.raises(mod.SubagentFailoverExhaustedError) as ei:
        run(rb)
    assert a.calls == 3
    assert len(ei.value.attempts) == 3 and all(x["status"] == "failed" for x in ei.value.attempts)
