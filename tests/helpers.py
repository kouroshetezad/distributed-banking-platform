"""Tiny assertion helpers so the tests double as the experiment runner.

Every test is a plain ``test_*`` function that returns ``(passed, message)`` so
``run_tests.py`` can print one verdict per experiment and keep going after a
failure.  Nothing here depends on pytest, so the suite runs anywhere Python does.
"""

from __future__ import annotations

import threading
import time
import traceback
from typing import Any, Callable, Dict, List, Optional


class CheckFailed(AssertionError):
    pass


def check(condition: Any, message: str) -> None:
    if not condition:
        raise CheckFailed(message)


def check_eq(actual: Any, expected: Any, what: str) -> None:
    if actual != expected:
        raise CheckFailed(f"{what}: expected {expected!r}, got {actual!r}")


def run_case(name: str, fn: Callable[[], Optional[str]]) -> Dict[str, Any]:
    """Run one test function and capture a pass/fail verdict."""
    started = time.time()
    try:
        note = fn() or ""
        return {"name": name, "passed": True, "note": note,
                "seconds": round(time.time() - started, 3)}
    except Exception as exc:
        detail = f"{type(exc).__name__}: {exc}"
        if not isinstance(exc, CheckFailed):
            detail += "\n" + traceback.format_exc()
        return {"name": name, "passed": False, "note": detail,
                "seconds": round(time.time() - started, 3)}


class TrafficGenerator:
    """Background banking traffic, used by the failover and migration tests."""

    def __init__(self, client, accounts: List[str], amount: float = 1.0,
                 kinds: Optional[List[str]] = None, delay: float = 0.002):
        self.client = client
        self.accounts = accounts
        self.amount = amount
        self.kinds = kinds or ["deposit"]
        self.delay = delay
        self.stop = threading.Event()
        self.threads: List[threading.Thread] = []
        self.ok = 0
        self.errors: Dict[str, int] = {}
        self.pending: List[str] = []
        self._lock = threading.Lock()
        self._index = 0
        self.confirmed_cents = 0

    def _next(self) -> Any:
        with self._lock:
            value = self._index
            self._index += 1
        return value

    def _record_ok(self, cents: int) -> None:
        with self._lock:
            self.ok += 1
            self.confirmed_cents += cents

    def _record_error(self, code: str) -> None:
        with self._lock:
            self.errors[code] = self.errors.get(code, 0) + 1

    def _worker(self) -> None:
        while not self.stop.is_set():
            index = self._next()
            account = self.accounts[index % len(self.accounts)]
            kind = self.kinds[index % len(self.kinds)]
            try:
                if kind == "withdraw":
                    reply = self.client.withdraw(account, self.amount)
                else:
                    reply = self.client.deposit(account, self.amount)
                status = reply.get("status")
                if status == "success":
                    self._record_ok(int(round(self.amount * 100)))
                elif status == "pending":
                    with self._lock:
                        self.pending.append(reply.get("txn_id", ""))
                else:
                    self._record_error(reply.get("error", "unknown"))
            except Exception as exc:
                self._record_error(type(exc).__name__)
            if self.delay:
                time.sleep(self.delay)

    def start(self, workers: int = 4) -> "TrafficGenerator":
        for index in range(workers):
            thread = threading.Thread(target=self._worker, name=f"traffic-{index}",
                                      daemon=True)
            thread.start()
            self.threads.append(thread)
        return self

    def stop_and_join(self, timeout: float = 10.0) -> None:
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=timeout)

    def __enter__(self) -> "TrafficGenerator":
        return self.start()

    def __exit__(self, *exc) -> None:
        self.stop_and_join()

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            return {"ok": self.ok, "errors": dict(self.errors),
                    "pending": len(self.pending),
                    "confirmed_cents": self.confirmed_cents}


def settle(seconds: float = 0.5) -> None:
    time.sleep(seconds)
