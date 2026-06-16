"""Minimal example: insert an outbox row from Python (psycopg 3).

Demonstrates the SQL-only integration path documented in
docs/protocol/insert.sql. No outbox-specific SDK; psycopg is the only
runtime dependency.

Run against the lib's test compose stack:

    DATABASE_URL='postgres://outbox:outbox@localhost:5434/outbox' python3 insert.py

Requires Python 3.11+ and the `uuid7` and `psycopg[binary]` packages.
"""
import json
import os

import psycopg

# UUIDv7 is the recommended event_id format. Python 3.14 ships uuid.uuid7()
# in stdlib; on earlier versions, use the `uuid7` package or roll your own.
try:
    from uuid import uuid7  # type: ignore[attr-defined]  # Python 3.14+
except ImportError:
    from uuid7 import uuid7  # third-party `uuid7` package


INSERT_SQL = """
INSERT INTO outbox.messages
    (event_id, address, data, headers, ordering_key, retry_limit)
VALUES
    (%s, %s, %s, %s, %s, %s)
"""


def emit_event(
    conn: psycopg.Connection,
    *,
    address: str,
    payload: bytes,
    ordering_key: str = "",
    headers: dict[str, str] | None = None,
    retry_limit: int = 5,
) -> str:
    """Insert an outbox row inside the caller's transaction.

    The caller is responsible for the transaction boundary. Typical usage:

        with conn.transaction():
            cur.execute("UPDATE payments SET ...")
            emit_event(conn, address="payments.completed.v1", payload=...)

    Returns the event_id (UUIDv7) so the caller can log/correlate it.
    """
    event_id = str(uuid7())
    conn.execute(
        INSERT_SQL,
        (
            event_id,
            address,
            payload,
            json.dumps(headers or {}),
            ordering_key,
            retry_limit,
        ),
    )
    return event_id


def main():
    url = os.environ["DATABASE_URL"]
    with psycopg.connect(url, autocommit=False) as conn:
        with conn.transaction():
            # In a real service, your domain write goes here in the same tx:
            #   conn.execute("UPDATE payments SET status='completed' WHERE id=%s", (payment_id,))

            event_id = emit_event(
                conn,
                address="example.smoke.v1",
                payload=b'{"hello": "world"}',
                ordering_key="example-key",
                headers={"content-type": "application/json"},
            )
            print(f"emitted event_id={event_id}")


if __name__ == "__main__":
    main()
