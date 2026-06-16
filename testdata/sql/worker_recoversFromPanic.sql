INSERT INTO outbox.messages (id, data, headers, address, ordering_key, retry_limit)
OVERRIDING SYSTEM VALUE
VALUES
    (999, decode('48656c6c6f20576f726c64', 'hex'), '{"foo":"bar"}', 'my_topic', 'key1', 2),
    (42, decode('5465737420646174612033', 'hex'), '{"priority":"high"}', 'my_topic', 'key4', 2);
