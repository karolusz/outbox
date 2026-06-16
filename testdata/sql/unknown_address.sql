INSERT INTO outbox_events (id, data, attributes, topic, ordering_key, event_type, retry_limit)
OVERRIDING SYSTEM VALUE
VALUES
    (777, decode('48656c6c6f', 'hex'), '{}', 'address.not.in.book.v1', 'k1', 'evt.unknown', 5);
