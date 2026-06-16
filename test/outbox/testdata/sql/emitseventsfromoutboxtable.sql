INSERT INTO outbox.messages (data, headers, address, ordering_key, retry_limit)
VALUES
    (decode('48656c6c6f20576f726c64', 'hex'), '{"foo":"bar"}', 'my_topic', 'key1', 2),
    (decode('5465737420646174612031', 'hex'), '{"baz":"qux"}', 'my_topic', 'key2', 2),
    (decode('5465737420646174612032', 'hex'), '{}', 'my_topic', 'key3', 2),
    (decode('5465737420646174612033', 'hex'), '{"priority":"high"}', 'my_topic', 'key4', 2);
