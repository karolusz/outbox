INSERT INTO outbox_events (id, data, attributes, topic, ordering_key, event_type, retry_limit)
OVERRIDING SYSTEM VALUE
VALUES
    (100, decode('48656c6c6f20576f726c64', 'hex'), '{"foo":"bar"}', 'my_topic', 'key1', 'user.created', 2),
    (101, decode('5465737420646174612033', 'hex'), '{"priority":"high"}', 'my_topic', 'key4', 'order.shipped', 2);
