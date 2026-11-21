"""Waiting in tests without asserting on time.

Every red CI run this project had was a test asserting on elapsed time or on "has it happened yet" after a fixed sleep.
Use these instead (test_timing_guard.py fails on the old patterns):

- wait_until(pred, deadline, what): poll until pred is truthy and return its value, or fail naming what it waited for.
  The deadline is a generous ceiling, not a measurement: a slow machine only waits longer, it never fails early.
- holds_for(pred, seconds, what): for absence checks (a refused signal killed nothing, a blocked call is still
  blocked): pred must stay truthy for the whole window. Slowness can only make it pass, never fail, so it is no race.

The async forms (await_until, aholds_for) do the same inside anyio tests.
"""

import time

EVERY = 0.05


def wait_until(pred, deadline=30, what="the condition", every=EVERY):
    """pred's first truthy value, polled until deadline seconds have passed; an AssertionError naming what it waited
    for (and pred's last value) otherwise."""
    end = time.monotonic() + deadline
    while True:
        last = pred()
        if last:
            return last
        if time.monotonic() >= end:
            raise AssertionError(f"timed out after {deadline} s waiting for {what} (last: {last!r})")
        time.sleep(every)


def holds_for(pred, seconds, what="the condition", every=EVERY):
    """Fail as soon as pred is falsy during the next seconds; return None when it held throughout."""
    end = time.monotonic() + seconds
    while True:
        if not (last := pred()):
            raise AssertionError(f"{what} stopped holding (got {last!r})")
        if time.monotonic() >= end:
            return
        time.sleep(every)


async def await_until(pred, deadline=30, what="the condition", every=EVERY):
    """wait_until for anyio tests; pred may be a coroutine function."""
    import inspect

    import anyio

    end = time.monotonic() + deadline
    while True:
        last = pred()
        if inspect.isawaitable(last):
            last = await last
        if last:
            return last
        if time.monotonic() >= end:
            raise AssertionError(f"timed out after {deadline} s waiting for {what} (last: {last!r})")
        await anyio.sleep(every)


async def aholds_for(pred, seconds, what="the condition", every=EVERY):
    """holds_for for anyio tests; pred may be a coroutine function."""
    import inspect

    import anyio

    end = time.monotonic() + seconds
    while True:
        last = pred()
        if inspect.isawaitable(last):
            last = await last
        if not last:
            raise AssertionError(f"{what} stopped holding (got {last!r})")
        if time.monotonic() >= end:
            return
        await anyio.sleep(every)
