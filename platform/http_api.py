"""Amounts, account names and the shared request/response envelope.

Money rules (kept from Assignment 1 and extended for sharding):

* inside the platform, balances are **integer cents**;
* a human client may send ``{"amount": 12.5}`` and it is converted exactly once,
  at the edge;
* two services that are peers talk in cents so that a value never has to survive
  a float round-trip.

The envelope is intentionally request-id centric.  Every write carries a
``request_id``; services remember the ids they have already applied so that a
retry after a timeout is applied exactly once.
"""

from __future__ import annotations

import uuid
from typing import Any, Dict, Optional

MAX_AMOUNT = 1_000_000_000


class ValidationError(Exception):
    def __init__(self, code: str, message: str, **extra: Any):
        super().__init__(message)
        self.code = code
        self.message = message
        self.extra = extra


def to_cents(amount: Any, *, allow_zero: bool = False) -> int:
    """Convert a decimal amount (or an int already in cents) to integer cents.

    Accepts int/float and numeric strings.  Rejects booleans, NaN/inf, more than
    two decimal places, negatives and (unless ``allow_zero``) zero.
    """
    if isinstance(amount, bool):
        raise ValidationError("invalid_amount", "amount must be a number")
    if isinstance(amount, str):
        try:
            amount = float(amount)
        except ValueError:
            raise ValidationError("invalid_amount", f"amount is not a number: {amount!r}") from None
    if not isinstance(amount, (int, float)):
        raise ValidationError("invalid_amount", "amount must be a number")
    if amount != amount or amount in (float("inf"), float("-inf")):
        raise ValidationError("invalid_amount", "amount must be a finite number")
    if amount > MAX_AMOUNT:
        raise ValidationError("invalid_amount", f"amount must not exceed {MAX_AMOUNT}")
    scaled = amount * 100
    if abs(scaled - round(scaled)) > 1e-6:
        raise ValidationError("invalid_amount", "amount may have at most 2 decimal places")
    cents = round(scaled)
    if cents < 0 or (cents == 0 and not allow_zero):
        raise ValidationError("invalid_amount", "amount must be greater than zero")
    return cents


def cents_from_body(body: Dict[str, Any], field: str = "amount") -> int:
    """Read an exact cent value from an internal peer.

    ``*_cents`` keys are trusted as an internal representation; ``amount`` (a
    decimal) is accepted too so the same endpoint works for a curl experiment.
    """
    cents_field = f"{field}_cents"
    if cents_field in body and body[cents_field] is not None:
        value = body[cents_field]
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValidationError("invalid_amount", f"{cents_field} must be an integer")
        if value <= 0:
            raise ValidationError("invalid_amount", f"{cents_field} must be greater than zero")
        return value
    if field not in body or body[field] is None:
        raise ValidationError("missing_field", f"{field} is required")
    return to_cents(body[field])


def require_account(value: Any, field: str = "account") -> str:
    if value is None or value == "":
        raise ValidationError("missing_field", f"{field} is required")
    if not isinstance(value, str):
        raise ValidationError("invalid_account", f"{field} must be a string")
    account = value.strip().upper()
    if not account:
        raise ValidationError("missing_field", f"{field} is required")
    return account


def require_account_range(value: Any) -> str:
    """Validate a ``A0001-A1000`` style range used by the shard map."""
    if not isinstance(value, str) or "-" not in value:
        raise ValidationError("invalid_range", f"range must look like A0001-A1000, got {value!r}")
    low, _, high = value.partition("-")
    low, high = low.strip().upper(), high.strip().upper()
    if not low or not high:
        raise ValidationError("invalid_range", f"range must look like A0001-A1000, got {value!r}")
    return f"{low}-{high}"


def new_request_id(prefix: str = "req") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:16]}"


def error_body(code: str, message: str, **extra: Any) -> Dict[str, Any]:
    body = {"status": "error", "error": code, "message": message}
    body.update(extra)
    return body


def ok_body(**fields: Any) -> Dict[str, Any]:
    body = {"status": "success"}
    body.update(fields)
    return body


def coins(balance_cents: int) -> Dict[str, Any]:
    """A balance in both representations, so no caller has to divide by 100."""
    return {"balance_cents": int(balance_cents), "balance": round(balance_cents / 100.0, 2)}
