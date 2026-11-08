"""``platform`` - shared plumbing for the distributed banking platform.

Importing the package is cheap and side-effect free; every service imports only
what it needs.  See ``docs/ARCHITECTURE.md`` for the full design and
``platform/storage/`` for the per-component durable state machines.
"""

__all__ = ["targets", "http_service", "http_api"]
__version__ = "2.0.0"
