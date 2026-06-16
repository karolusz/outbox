INSERT INTO outbox.messages (id, data, headers, address, ordering_key, retry_limit)
OVERRIDING SYSTEM VALUE
VALUES
    (777, decode('48656c6c6f', 'hex'), '{}', 'address.not.in.book.v1', 'k1', 5);
