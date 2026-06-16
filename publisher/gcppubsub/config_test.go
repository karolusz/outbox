//go:build testing

package gcppubsub

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfig_DecodesYAML_AllFields(t *testing.T) {
	in := []byte(`
project: anyfin-prod
enable_message_ordering: true
publish_timeout: 10s
credentials:
  type: file
  path: /secrets/sa.json
`)
	var cfg Config
	require.NoError(t, yaml.Unmarshal(in, &cfg))

	assert.Equal(t, "anyfin-prod", cfg.Project)
	assert.True(t, cfg.EnableMessageOrdering)
	assert.Equal(t, "10s", cfg.PublishTimeout.String())
	assert.Equal(t, "file", cfg.Credentials.Type)
	assert.Equal(t, "/secrets/sa.json", cfg.Credentials.Path)
}

func TestConfig_DecodesYAML_MinimalConfig_DefaultsToADC(t *testing.T) {
	// Only Project specified; credentials block omitted entirely.
	// resolveCredentials should treat this as ADC (default).
	in := []byte(`
project: anyfin-prod
`)
	var cfg Config
	require.NoError(t, yaml.Unmarshal(in, &cfg))

	assert.Equal(t, "anyfin-prod", cfg.Project)
	assert.Equal(t, "", cfg.Credentials.Type, "missing credentials block should leave Type empty (treated as ADC)")
}

func TestResolveCredentials_ADC_Default(t *testing.T) {
	// Type is "" — equivalent to ADC.
	opts, err := resolveCredentials(Credentials{})
	require.NoError(t, err)
	assert.Nil(t, opts, "ADC should produce no explicit ClientOption")
}

func TestResolveCredentials_ADC_Explicit(t *testing.T) {
	opts, err := resolveCredentials(Credentials{Type: "adc"})
	require.NoError(t, err)
	assert.Nil(t, opts)
}

func TestResolveCredentials_File(t *testing.T) {
	opts, err := resolveCredentials(Credentials{Type: "file", Path: "/secrets/sa.json"})
	require.NoError(t, err)
	require.Len(t, opts, 1, "file credentials should produce one ClientOption")
}

func TestResolveCredentials_File_MissingPath_Errors(t *testing.T) {
	_, err := resolveCredentials(Credentials{Type: "file"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentials.path")
}

func TestResolveCredentials_Env(t *testing.T) {
	const envName = "TEST_GCP_CREDS_JSON"
	t.Setenv(envName, `{"type":"service_account","project_id":"test"}`)

	opts, err := resolveCredentials(Credentials{Type: "env", EnvVar: envName})
	require.NoError(t, err)
	require.Len(t, opts, 1, "env credentials should produce one ClientOption")
}

func TestResolveCredentials_Env_MissingVar_Errors(t *testing.T) {
	_, err := resolveCredentials(Credentials{Type: "env"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "credentials.env_var")
}

func TestResolveCredentials_Env_VarUnset_Errors(t *testing.T) {
	const envName = "TEST_GCP_CREDS_UNSET_FOR_TEST"
	// Ensure the var is unset for this test.
	_ = os.Unsetenv(envName)

	_, err := resolveCredentials(Credentials{Type: "env", EnvVar: envName})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unset or empty")
}

func TestResolveCredentials_Env_VarEmpty_Errors(t *testing.T) {
	const envName = "TEST_GCP_CREDS_EMPTY_FOR_TEST"
	t.Setenv(envName, "")

	_, err := resolveCredentials(Credentials{Type: "env", EnvVar: envName})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unset or empty")
}

func TestResolveCredentials_UnknownType_Errors(t *testing.T) {
	_, err := resolveCredentials(Credentials{Type: "vault"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), `unknown credentials.type "vault"`)
}

func TestNewFromConfig_EmptyProject_Errors(t *testing.T) {
	_, err := NewFromConfig(t.Context(), Config{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "project is required")
}
