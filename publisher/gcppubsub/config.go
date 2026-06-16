package gcppubsub

import "time"

// Config is the YAML-visible configuration for the gcppubsub plugin.
// Adopters describe their Pub/Sub setup in their address-book YAML; the
// plugin's factory unmarshals the publisher's `config:` block into this
// struct.
//
// The same struct is also accepted by NewFromConfig for adopters using
// the in-Go construction path.
type Config struct {
	// Project is the GCP project ID. Required.
	Project string `yaml:"project"`

	// EnableMessageOrdering opts each topic into ordered delivery. Honoured
	// only if the destination topic was created with ordering support.
	EnableMessageOrdering bool `yaml:"enable_message_ordering"`

	// PublishTimeout caps how long a single Publish call blocks waiting
	// for the broker ack. Zero means the SDK default applies.
	PublishTimeout time.Duration `yaml:"publish_timeout"`

	// Credentials describes how the plugin obtains GCP credentials. If
	// omitted entirely, the plugin uses Application Default Credentials
	// (workload identity, GOOGLE_APPLICATION_CREDENTIALS env var, etc.).
	Credentials Credentials `yaml:"credentials"`
}

// Credentials describes where the gcppubsub plugin should source GCP
// credentials. Only one of Path or EnvVar is read, depending on Type.
type Credentials struct {
	// Type selects the credential source. Valid values:
	//
	//   "" or "adc"  — Application Default Credentials. The default.
	//   "file"       — Service-account JSON at Path.
	//   "env"        — Service-account JSON in the env var named by EnvVar.
	//
	// Complex flows (Vault-fetched, runtime-refreshed) are not supported
	// in YAML. Use the Go-injected publisher path instead.
	Type string `yaml:"type"`

	// Path is the filesystem path to a service-account JSON file.
	// Used when Type == "file".
	Path string `yaml:"path,omitempty"`

	// EnvVar is the name of an environment variable holding service-account
	// JSON. Used when Type == "env".
	EnvVar string `yaml:"env_var,omitempty"`
}
