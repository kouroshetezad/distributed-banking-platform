"""Banking client.

The client is deliberately ignorant of shards and replicas: it knows one URL,
the gateway, and nothing else.  It does not even know whether a transfer was
local or cross-shard - the gateway and coordinator decide that, and the client
just reports what came back.

What the client *does* do carefully is **retries**.  Every write gets a
``request_id`` generated once, and the same id is reused on every retry.  That is
what turns an ambiguous failure ("the reply never arrived - did the money move?")
into a safe question: if the platform already applied the request, it replays the
stored answer instead of applying it twice.  ``--query-txn`` asks about an
outcome the client genuinely does not know.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import uuid
from typing import Any, Dict, Optional

from .targets import (
    PeerError,
    env,
    env_int,
    http_json,
    split_csv,
    url_for,
)


class ClientError(Exception):
    pass


class BankClient:
    def __init__(self, gateway: str, timeout: float = 10.0, retries: int = 3,
                 verbose: bool = False):
        self.gateway = gateway.rstrip("/")
        self.timeout = timeout
        self.retries = retries
        self.verbose = verbose


    def _call(self, path: str, payload: Optional[Dict[str, Any]] = None,
              *, method: str = "POST") -> Dict[str, Any]:
        url = url_for(self.gateway, path)
        last: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                body = http_json(url, payload if method == "POST" else None,
                                 method=method, timeout=self.timeout)
                if self.verbose:
                    print(f"  <- HTTP {body.get('_http_status')} {json.dumps(body)[:300]}",
                          file=sys.stderr)
                return body
            except PeerError as exc:
                last = exc
                if attempt < self.retries:
                    if self.verbose:
                        print(f"  .. attempt {attempt} failed ({exc}), retrying", file=sys.stderr)
                    time.sleep(0.4 * attempt)
        raise ClientError(f"gateway unreachable after {self.retries} attempts: {last}")

    @staticmethod
    def request_id(prefix: str = "cli") -> str:
        return f"{prefix}-{uuid.uuid4().hex[:16]}"


    def balance(self, account: str) -> Dict[str, Any]:
        return self._call(f"/account?account={account}", method="GET")

    def deposit(self, account: str, amount: float,
                request_id: Optional[str] = None) -> Dict[str, Any]:
        return self._call("/deposit", {"account": account, "amount": amount,
                                       "request_id": request_id or self.request_id("dep")})

    def withdraw(self, account: str, amount: float,
                 request_id: Optional[str] = None) -> Dict[str, Any]:
        return self._call("/withdraw", {"account": account, "amount": amount,
                                        "request_id": request_id or self.request_id("wdr")})

    def transfer(self, src: str, dst: str, amount: float,
                 request_id: Optional[str] = None) -> Dict[str, Any]:
        return self._call("/transfer", {"account": src, "to": dst, "amount": amount,
                                        "request_id": request_id or self.request_id("xfr")})

    def txn_status(self, txn_id: str) -> Dict[str, Any]:
        return self._call(f"/txn?txn_id={txn_id}", method="GET")

    def shard_map(self) -> Dict[str, Any]:
        return self._call("/shard-map", method="GET")

    def clusters(self) -> Dict[str, Any]:
        return self._call("/clusters", method="GET")




def _money(cents: Any) -> str:
    try:
        return f"{int(cents) / 100:.2f}"
    except (TypeError, ValueError):
        return str(cents)


def render(body: Dict[str, Any]) -> str:
    """A short human-readable summary; still print the raw JSON with --json."""
    if not isinstance(body, dict):
        return str(body)
    status = body.get("status")
    http = body.get("_http_status")
    if status == "pending":
        return (f"PENDING   txn {body.get('txn_id')} decision={body.get('decision')} "
                f"awaiting={body.get('awaiting')} - query it later with --query-txn")
    if status == "error" or (http and http >= 400):
        code = body.get("error", "error")
        message = body.get("message", "")
        extra = ""
        if body.get("balance_cents") is not None:
            extra = f" (balance {_money(body['balance_cents'])})"
        elif body.get("balance") is not None:
            extra = f" (balance {body['balance']})"
        return f"ERROR     {code}: {message}{extra}"

    action = body.get("action")
    if action == "balance":
        return (f"BALANCE   {body.get('account')} = {_money(body.get('balance_cents'))} "
                f"[shard {body.get('shard')} via {body.get('served_by', body.get('instance'))}]")
    if action in ("deposit", "withdraw"):
        return (f"{action.upper():<9} {body.get('account')} -> "
                f"{_money(body.get('balance_cents'))} "
                f"[shard {body.get('shard')} via {body.get('served_by', body.get('instance'))}]")
    if action == "transfer" or body.get("mode"):
        mode = body.get("mode", "transfer")
        head = f"TRANSFER  {body.get('account')} -> {body.get('to')} "
        if body.get("amount_cents") is not None:
            head += f"{_money(body['amount_cents'])} "
        head += f"[{mode}]"
        if body.get("txn_id"):
            head += f" txn={body['txn_id']}"
        if body.get("source_balance_cents") is not None:
            head += (f"  {body.get('account')}={_money(body['source_balance_cents'])}"
                     f" {body.get('to')}={_money(body.get('target_balance_cents'))}")
        elif body.get("balance_cents") is not None:
            head += (f"  {body.get('account')}={_money(body['balance_cents'])}"
                     f" {body.get('to')}={_money(body.get('to_balance_cents'))}")
        return head
    if body.get("coordinated") is False and body.get("mode") == "same_shard":
        return body.get("message", "not coordinated")
    if "ranges" in body:
        ranges = ", ".join(f"{r['shard']}:{r['range']}" for r in body["ranges"])
        return f"SHARD MAP epoch={body.get('epoch')} {ranges}"
    return json.dumps(body)


def _interactive(client: BankClient) -> int:
    print("Interactive banking client. Commands:")
    print("  balance <acct> | deposit <acct> <amt> | withdraw <acct> <amt>")
    print("  transfer <from> <to> <amt> | shardmap | clusters | txn <id> | quit")
    while True:
        try:
            line = input("bank> ").strip()
        except (EOFError, KeyboardInterrupt):
            print()
            return 0
        if not line:
            continue
        parts = line.split()
        command, rest = parts[0].lower(), parts[1:]
        try:
            if command in ("quit", "exit"):
                return 0
            elif command == "balance" and len(rest) == 1:
                print(render(client.balance(rest[0])))
            elif command == "deposit" and len(rest) == 2:
                print(render(client.deposit(rest[0], float(rest[1]))))
            elif command == "withdraw" and len(rest) == 2:
                print(render(client.withdraw(rest[0], float(rest[1]))))
            elif command == "transfer" and len(rest) == 3:
                print(render(client.transfer(rest[0], rest[1], float(rest[2]))))
            elif command == "shardmap":
                print(json.dumps(client.shard_map(), indent=2))
            elif command == "clusters":
                print(json.dumps(client.clusters(), indent=2))
            elif command == "txn" and len(rest) == 1:
                print(json.dumps(client.txn_status(rest[0]), indent=2))
            else:
                print("unknown or malformed command")
        except ClientError as exc:
            print(f"ERROR     {exc}", file=sys.stderr)


def main(argv: Optional[str] = None) -> int:
    parser = argparse.ArgumentParser(description="Distributed banking client (talks to the gateway)")
    parser.add_argument("command", nargs="?",
                        help="balance | deposit | withdraw | transfer | shardmap | clusters")
    parser.add_argument("args", nargs="*")
    parser.add_argument("--gateway", default=os.environ.get("GATEWAY_URL") or
                        f"http://{env('GATEWAY_HOST', 'localhost')}:{env_int('GATEWAY_PORT', 8000)}")
    parser.add_argument("--timeout", type=float, default=10.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--request-id", default=None,
                        help="reuse a specific request id (for retry experiments)")
    parser.add_argument("--query-txn", default=None,
                        help="ask the coordinator about a transaction id")
    parser.add_argument("--json", action="store_true", help="print the raw JSON reply")
    parser.add_argument("--interactive", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args(argv)

    client = BankClient(args.gateway, args.timeout, args.retries, args.verbose)

    try:
        if args.query_txn:
            body = client.txn_status(args.query_txn)
        elif args.interactive or not args.command:
            return _interactive(client)
        elif args.command == "balance" and len(args.args) == 1:
            body = client.balance(args.args[0])
        elif args.command == "deposit" and len(args.args) == 2:
            body = client.deposit(args.args[0], float(args.args[1]), args.request_id)
        elif args.command == "withdraw" and len(args.args) == 2:
            body = client.withdraw(args.args[0], float(args.args[1]), args.request_id)
        elif args.command == "transfer" and len(args.args) == 3:
            body = client.transfer(args.args[0], args.args[1], float(args.args[2]),
                                   args.request_id)
        elif args.command == "shardmap":
            body = client.shard_map()
        elif args.command == "clusters":
            body = client.clusters()
        else:
            parser.error(f"unknown command or wrong argument count: {args.command} {args.args}")
            return 2
    except ClientError as exc:
        print(f"ERROR     {exc}", file=sys.stderr)
        return 1

    print(json.dumps(body, indent=2) if args.json else render(body))
    http = body.get("_http_status", 200) if isinstance(body, dict) else 200
    return 0 if http < 400 and body.get("status") != "error" else 1


if __name__ == "__main__":
    raise SystemExit(main())
