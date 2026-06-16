# Address book YAML format

The address book is a YAML file that tells the relay where each logical address goes — which broker plugin to use, which broker-specific target name to publish to, and the plugin's per-instance configuration.

The same file is loaded by:

- the relay binary at startup (`cmd/outbox-relay` reads it from `--addressbook` path), and
- (optionally) producers that want to pre-validate addresses at API boundaries before they reach Postgres.

## Schema

```yaml
version: 1                            # Required. Schema version.
                                      # The relay refuses to start if missing
                                      # or unsupported.

publishers:                            # Required. List of plugin instances.
  - name: pubsub-prod                  # Required. Unique key in this file;
                                       # referenced by addresses[].publisher.
    plugin: gcppubsub                  # Required. Plugin name (the name the
                                       # plugin package registered with).
    config:                            # Plugin-specific configuration.
      project: my-gcp-project          # Each plugin documents its own
      enable_message_ordering: true    # config keys.

  - name: pubsub-staging
    plugin: gcppubsub
    config:
      project: my-gcp-staging

  - name: in-memory-fake
    plugin: fake                       # The lib-shipped fake plugin takes
    config: {}                         # no config; the block can be empty
                                       # or omitted entirely.

addresses:                             # Required. List of logical addresses.
  - name: payments.completed.v1        # Required. Logical address (free-form
                                       # string; producers write this verbatim
                                       # into the address column).
    publisher: pubsub-prod             # Required. Must match a publishers[].name
                                       # in this file. The relay validates
                                       # this at startup.
    target: payments-prod-topic        # Required. Broker-specific target
                                       # passed to the publisher's Publish
                                       # method. For Pub/Sub: topic name.
                                       # For Kafka: topic name. Semantics
                                       # are publisher-defined.

  - name: mandates.created.v1
    publisher: pubsub-prod
    target: mandates-prod-topic

  - name: dev.scratch.v1
    publisher: in-memory-fake
    target: scratch
```

## Validation rules

The loader (`yamlconfig.LoadAddressBook` in the Go SDK; same logic backs the relay binary) enforces:

- `version` must equal `1` (the current supported schema version).
- Every entry in `publishers` must have a unique `name`.
- Every entry in `addresses` must have a unique `name`.
- Every `addresses[].publisher` must reference an existing `publishers[].name`.
- Every `addresses[].target` must be non-empty.
- Every plugin named in `publishers[].plugin` must be registered with the relay (via blank-import — see the README).

Errors are aggregated: a single load attempt reports every problem the loader finds, not just the first one. Adopters see "fix-recompile-repeat" in a tight loop only if they want to ignore the bundled report.

## Plugin config decoding

Each plugin defines its own `config:` schema. The loader passes the YAML node beneath `config:` to the plugin's factory via a decoder closure; the plugin populates its Config struct as it sees fit.

For lib-shipped plugins:

### `gcppubsub`

```yaml
config:
  project: my-gcp-project            # Required. GCP project ID.
  enable_message_ordering: true      # Optional. Default false. Set true if
                                     # any address using this publisher
                                     # relies on ordering_key.
  publish_timeout: 30s               # Optional. Default 10s. Per-publish
                                     # timeout (Go duration string).
  credentials:                       # Optional. Defaults to Application
                                     # Default Credentials (ADC).
    type: adc                        # adc | file | env
    file: /path/to/key.json          # Only with type=file.
    env_var: GOOGLE_APPLICATION_CREDENTIALS_JSON  # Only with type=env.
                                                  # The env var's value
                                                  # is the JSON key contents.
```

### `fake`

No configuration. The `config:` block can be empty or omitted entirely.

```yaml
- name: in-memory-fake
  plugin: fake
  # config block can be omitted
```

The fake publisher records messages in memory. Useful for tests and "soft launch" setups where the YAML names the address but you don't want to publish to a real broker yet.

## Where the file lives

Operationally, mount the file as a ConfigMap (k8s) or bind-mount it into the relay container (Docker). The relay binary reads it once at startup; there is no hot-reload in v1.

For the lib-shipped `cmd/outbox-relay`, the default path is `/etc/outbox/addressbook.yaml`. Override via `--addressbook=/path/to/file.yaml`.

## CloudEvents binding modes

`data` is opaque bytes; the lib doesn't model CloudEvents at the schema level. Adopters who want CloudEvents have two natural paths through our envelope, both supported by the CloudEvents v1.0 spec.

### Structured mode

Serialize the entire CloudEvent (context attributes + data) as a JSON object and place it in `data`. `headers` carries only the content-type hint.

Producer-side example (Python, `cloudevents` library):

```python
from cloudevents.http import CloudEvent
from cloudevents.conversion import to_json

event = CloudEvent(
    attributes={
        "type": "com.example.payments.completed.v1",
        "source": "/payments-service",
        "id": str(uuid7()),
        "time": datetime.now(UTC).isoformat(),
    },
    data={"payment_id": "abc", "amount": 1000},
)

conn.execute(INSERT_SQL, (
    event["id"],                                              # event_id
    "payments.completed.v1",                                  # address
    to_json(event),                                           # data (bytes)
    json.dumps({"content-type": "application/cloudevents+json"}),  # headers
    payment.id,                                               # ordering_key
    5,                                                        # retry_limit
))
```

Easy to produce, opaque to the publisher, consumer parses one JSON blob.

### Binary mode

Put only the inner data payload in `data`, and split CloudEvent context attributes into `headers` with the standard `ce-` prefix:

```python
event_id = str(uuid7())
conn.execute(INSERT_SQL, (
    event_id,
    "payments.completed.v1",
    json.dumps({"payment_id": "abc", "amount": 1000}).encode(),  # data
    json.dumps({                                                 # headers
        "ce-specversion": "1.0",
        "ce-id":          event_id,
        "ce-type":        "com.example.payments.completed.v1",
        "ce-source":      "/payments-service",
        "ce-time":        datetime.now(UTC).isoformat(),
        "content-type":   "application/json",
    }),
    payment.id,
    5,
))
```

Better for broker-side filtering (Kafka stream processors, Pub/Sub message-attribute filters) because consumers can route on individual headers without parsing the body.

### Which to use

Document neither as default. The choice depends on your consumers' needs.

| Mode | Strength | Weakness |
|---|---|---|
| Structured | Easy to produce, easy to consume, opaque pipeline | Broker-side filtering needs body parse |
| Binary | Broker-side filtering on headers, natural for Kafka/HTTP consumers | More producer-side wiring |

Both work through our envelope without any schema changes. The lib doesn't care which one you pick.
