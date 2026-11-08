"""Where a participant lives on the network, and how to reach it.

Two kinds of peer exist in this platform:

* **replicas** run inside the compose network and are reached by service name
  (``shard-a-1:9000``);
* **clients** and the **test harness** run on the host and reach the same
  replicas through the published port (``localhost:9001``).

The same container therefore has to expose *two* addresses for itself: one for
its peers, one for whoever is outside.  Rather than duplicating every setting,
each service is configured with a single ``*_ADVERTISE`` variable that may carry
two URLs separated by a comma::

    SHARD_A_ADVERTISE=http://shard-a-1:9000,http://localhost:9001

Everything here is deliberately dependency-free so it can be imported by the
server, the gateway, the coordinator and the tests alike.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request


class PeerError(Exception):
    """A participant could not be reached, or answered something unusable."""

    def __init__(self, message, *, unreachable=False, status=None, body=None):
        super().__init__(message)
        self.message = message
        self.unreachable = unreachable
        self.status = status
        self.body = body


def split_csv(value):
    """Split a comma separated environment variable, ignoring blanks."""
    if not value:
        return []
    return [part.strip() for part in str(value).split(",") if part.strip()]


def normalize_url(url):
    url = str(url).strip().rstrip("/")
    if not url.startswith("http://") and not url.startswith("https://"):
        url = "http://" + url
    return url


def url_for(url, path):
    if not path.startswith("/"):
        path = "/" + path
    return normalize_url(url) + path


def http_json(url, payload=None, *, method="POST", timeout=10.0, headers=None):
    """POST (or GET) JSON and return the decoded body.

    Raises :class:`PeerError` for anything that is not a well-formed JSON reply.
    ``PeerError.unreachable`` is set when the failure is a connectivity problem,
    which is what the callers use to decide that a participant is *down* rather
    than *angry*.
    """
    data = None
    request_headers = {"Accept": "application/json", "Connection": "close"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        request_headers["Content-Type"] = "application/json"
    request_headers.update(headers or {})

    request_headers["Connection"] = "close"
    req = urllib.request.Request(
        normalize_url(url), data=data, headers=request_headers, method=method
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as exc:
        try:
            raw = exc.read()
        finally:
            exc.close()
        status = exc.code
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise PeerError(f"{url} unreachable: {exc}", unreachable=True) from exc

    text = raw.decode("utf-8", "replace") if raw else ""
    try:
        body = json.loads(text) if text else {}
    except json.JSONDecodeError as exc:
        raise PeerError(
            f"{url} returned non-JSON (HTTP {status}): {text[:200]!r}",
            status=status,
            body=text,
        ) from exc

    if isinstance(body, dict):
        body.setdefault("_http_status", status)
    return body


def env(name, default=None):
    value = os.environ.get(name)
    return default if value is None or value == "" else value


def env_int(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def env_float(name, default):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}
