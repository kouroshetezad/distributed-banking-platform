"""Base scaffolding for every HTTP service of the platform.

These services are all small: they take a JSON request, run a function inside a
SQLite transaction, and answer with JSON.  So instead of a web framework they
share one thread-per-connection server built on :mod:`http.server`.

What this module provides:

* a uniform JSON request/response envelope with an ``X-Request-Id`` echoed on
  every reply and on the matching log line;
* an :class:`AppError` type that maps a service-level rejection to the right
  HTTP status code;
* ``/health`` and ``/whoami`` built in, because the gateway's health detection
  and the report's replica-identification requirement both need them;
* graceful shutdown so the load experiments can stop a container mid-flight.

Money is **never** carried as a float in the wire format.  Wherever a rounded
amount matters (account balances) it travels as ``*_cents``.  See
:mod:`.http_api` for the helpers that accept either a decimal amount from a
human client or an exact cent value from an internal peer.
"""

from __future__ import annotations

import json
import logging
import os
import socket
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable, Dict, Optional, Tuple
from urllib.parse import parse_qs

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s %(message)s"
LOGGER = logging.getLogger("bank.http")


class AppError(Exception):
    """A request that should be reported to the caller with a status code."""

    def __init__(self, code: str, message: str, status: int = 422, **extra: Any):
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status
        self.extra = extra

    def as_body(self) -> Dict[str, Any]:
        body = {"status": "error", "error": self.code, "message": self.message}
        body.update(self.extra)
        return body


class NotReady(AppError):
    def __init__(self, message: str):
        super().__init__("not_ready", message, status=503)


def configure_logging(name: str, level: Optional[str] = None) -> logging.Logger:
    """Configure root logging once, with a per-service logger name."""
    if not logging.getLogger().handlers:
        logging.basicConfig(
            level=os.environ.get("LOG_LEVEL", level or "INFO").upper(),
            format=LOG_FORMAT,
        )
    return logging.getLogger(name)


class _Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MiniBank/2.0"


    def log_message(self, fmt, *args):
        return

    def _request_id(self) -> str:
        return self.headers.get("X-Request-Id") or uuid.uuid4().hex[:12]

    def _read_json(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise AppError(
                "malformed_json", f"request body is not valid JSON: {exc}", status=400
            ) from exc
        if not isinstance(body, dict):
            raise AppError("malformed_json", "request body must be a JSON object", status=400)
        return body

    def _send_json(self, status: int, body: Dict[str, Any], request_id: str) -> None:
        try:
            payload = json.dumps(body).encode("utf-8")
        except (TypeError, ValueError):
            LOGGER.exception("response body is not JSON-serialisable (rid=%s)", request_id)
            status = 500
            payload = json.dumps({
                "status": "error", "error": "unserialisable_response",
                "message": "the service produced a response that could not be encoded",
            }).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Request-Id", request_id)
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(payload)

    def _dispatch(self, method: str) -> None:
        request_id = self._request_id()
        path = self.path.split("?", 1)[0]
        try:
            if path == "/health":
                self._send_json(200, {"status": "ok", "service": self.server.service_name,
                                      "instance": self.server.instance_id}, request_id)
                return
            if path == "/whoami":
                self._send_json(200, self.server.describe(), request_id)
                return
            body = self._read_json() if method == "POST" else {}
            if method == "GET" and "?" in self.path:
                body = {k: v[0] for k, v in parse_qs(self.path.split("?", 1)[1]).items()}
            result = self.server.app(method, path, body, request_id)
            if result is None:
                raise AppError("unknown_route", f"no route for {method} {path}", status=404)
            status, payload = result
            self._send_json(status, payload, request_id)
        except AppError as exc:
            self.server.log_rejection(method, path, request_id, exc)
            self._send_json(exc.status, exc.as_body(), request_id)
        except BrokenPipeError:
            return
        except Exception:
            LOGGER.exception("unhandled error on %s %s (rid=%s)", method, path, request_id)
            self._send_json(500, {"status": "error", "error": "internal_error",
                                  "message": "internal server error"}, request_id)

    def do_GET(self):
        self._dispatch("GET")

    def do_POST(self):
        self._dispatch("POST")

    def handle_one_request(self):
        self.close_connection = True
        try:
            super().handle_one_request()
        finally:
            try:
                self.server.release_thread_resources()
            except Exception:
                pass


class JsonService(ThreadingHTTPServer):
    """Thread-per-connection JSON server with a pluggable ``app`` callable.

    ``app(method, path, body, request_id)`` returns ``(http_status, body)`` or
    ``None`` when the route is unknown, and may raise :class:`AppError`.
    """

    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, host: str, port: int, name: str, app: Callable):
        self.service_name = name
        self.instance_id = os.environ.get("INSTANCE_ID") or socket.gethostname()
        self.app = app
        self.logger = configure_logging(f"bank.{name}")
        super().__init__((host, port), _Handler)
        self._shutdown = threading.Event()


    def describe(self) -> Dict[str, Any]:
        return {"service": self.service_name, "instance": self.instance_id}

    def log_rejection(self, method, path, request_id, exc: AppError) -> None:
        self.logger.info(
            "reject %s %s rid=%s code=%s status=%d", method, path, request_id,
            exc.code, exc.status,
        )

    def url(self) -> str:
        host, port = self.server_address[:2]
        return f"http://{host}:{port}"

    def shutdown_async(self) -> None:
        self._shutdown.set()
        self.shutdown()

    def close(self) -> None:
        """Release sockets and any storage this service owns.

        Called by the local harness between stacks.  Without it a process that
        builds many stacks in a row leaks a file descriptor per SQLite connection
        per thread, and eventually hits the process file-descriptor limit - which
        is what ``[Errno 24] Too many open files`` means.  Subclasses that own a
        store or a registry override this and call ``super().close()``.
        """
        try:
            self.server_close()
        except Exception:
            pass

    def release_thread_resources(self) -> None:
        """Hand back the current thread's storage connection after a request.

        Overridden by services that own a :class:`Registry` or a ``ShardStore``.
        The default is a no-op so a service with no storage is unaffected.
        """
        return

    def start_background(self) -> threading.Thread:
        thread = threading.Thread(target=self.serve_forever, name=f"{self.service_name}-http",
                                  daemon=True)
        thread.start()
        return thread


def run_service(service: JsonService, logger: Optional[logging.Logger] = None) -> None:
    """Serve until SIGINT/SIGTERM; used by the service entry points."""
    import signal

    log = logger or service.logger
    stop = threading.Event()

    def _stop(signum, _frame):
        log.info("received signal %s, shutting down", signum)
        stop.set()
        threading.Thread(target=service.shutdown, daemon=True).start()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _stop)
        except (ValueError, OSError):
            pass

    log.info("%s listening on %s (instance=%s)", service.service_name,
             service.url(), service.instance_id)
    try:
        service.serve_forever()
    finally:
        service.server_close()
        log.info("%s stopped", service.service_name)
