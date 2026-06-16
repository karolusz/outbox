package gcppubsub

import (
	"fmt"
	"os"

	"google.golang.org/api/option"
)

// resolveCredentials converts a Credentials struct into the
// []option.ClientOption slice the GCP Pub/Sub SDK accepts. Returns nil
// (no options) for ADC — the SDK's default credential chain handles it.
//
// Error messages name the YAML field the operator should look at.
func resolveCredentials(c Credentials) ([]option.ClientOption, error) {
	switch c.Type {
	case "", "adc":
		// Application Default Credentials. The SDK consults
		// GOOGLE_APPLICATION_CREDENTIALS, gcloud auth state, GCE/GKE
		// metadata server, etc. No explicit option needed.
		return nil, nil

	case "file":
		if c.Path == "" {
			return nil, fmt.Errorf("gcppubsub: credentials.type=file requires credentials.path")
		}
		return []option.ClientOption{option.WithCredentialsFile(c.Path)}, nil

	case "env":
		if c.EnvVar == "" {
			return nil, fmt.Errorf("gcppubsub: credentials.type=env requires credentials.env_var")
		}
		jsonBytes := os.Getenv(c.EnvVar)
		if jsonBytes == "" {
			return nil, fmt.Errorf("gcppubsub: credentials.type=env: env var %q is unset or empty", c.EnvVar)
		}
		return []option.ClientOption{option.WithCredentialsJSON([]byte(jsonBytes))}, nil

	default:
		return nil, fmt.Errorf("gcppubsub: unknown credentials.type %q (expected adc, file, or env)", c.Type)
	}
}
