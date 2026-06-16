//go:build testing

package outbox

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetPluginRegistry swaps in a fresh registry for test isolation.
// Test-only; the testing build tag keeps it out of production binaries.
// Tests that register plugins should defer this so they leave the
// registry clean for subsequent tests.
func resetPluginRegistry() {
	globalRegistry = newPluginRegistry()
}

func noopFactory(ctx context.Context, decode ConfigDecoder) (Publisher, error) {
	return nil, nil
}

func TestRegisterPlugin_Stores(t *testing.T) {
	t.Cleanup(resetPluginRegistry)
	resetPluginRegistry()

	RegisterPlugin("test-plugin-stores", noopFactory)

	f, ok := lookupPlugin("test-plugin-stores")
	require.True(t, ok, "registered factory should be retrievable via lookupPlugin")
	require.NotNil(t, f)
}

func TestRegisterPlugin_DuplicatePanics(t *testing.T) {
	t.Cleanup(resetPluginRegistry)
	resetPluginRegistry()

	RegisterPlugin("dup-test", noopFactory)

	require.PanicsWithValue(t,
		`outbox: plugin "dup-test" already registered`,
		func() { RegisterPlugin("dup-test", noopFactory) },
		"second registration of the same name should panic with a clear message",
	)
}

func TestRegisterPlugin_EmptyNamePanics(t *testing.T) {
	require.PanicsWithValue(t,
		"outbox: RegisterPlugin called with empty name",
		func() { RegisterPlugin("", noopFactory) },
	)
}

func TestRegisterPlugin_NilFactoryPanics(t *testing.T) {
	require.PanicsWithValue(t,
		`outbox: RegisterPlugin("nilcheck") called with nil factory`,
		func() { RegisterPlugin("nilcheck", nil) },
	)
}

func TestRegisteredPlugins_Lists_Sorted(t *testing.T) {
	t.Cleanup(resetPluginRegistry)
	resetPluginRegistry()

	// Register in reverse alphabetical order to confirm the listing is sorted,
	// not insertion-ordered.
	RegisterPlugin("zebra", noopFactory)
	RegisterPlugin("apple", noopFactory)
	RegisterPlugin("mango", noopFactory)

	names := RegisteredPlugins()
	assert.Equal(t, []string{"apple", "mango", "zebra"}, names)
}

func TestRegisteredPlugins_EmptyByDefault(t *testing.T) {
	t.Cleanup(resetPluginRegistry)
	resetPluginRegistry()

	names := RegisteredPlugins()
	assert.Empty(t, names)
}

// TestFactoryIsActuallyCalled exercises the round-trip: register a factory,
// retrieve it via lookupPlugin, call it, observe the result. Catches
// regressions where the registry would store something but lookup returns
// a wrong instance.
func TestFactoryIsActuallyCalled(t *testing.T) {
	t.Cleanup(resetPluginRegistry)
	resetPluginRegistry()

	sentinelErr := errors.New("sentinel from factory")
	type cfg struct {
		Name string `yaml:"name"`
	}
	RegisterPlugin("sentinel", func(ctx context.Context, decode ConfigDecoder) (Publisher, error) {
		var c cfg
		if err := decode(&c); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		return nil, fmt.Errorf("decoded name=%q: %w", c.Name, sentinelErr)
	})

	f, ok := lookupPlugin("sentinel")
	require.True(t, ok)

	// Direct invocation with a hand-built decoder that fills in a known
	// value. Confirms the factory receives a real decoder, can decode
	// through it, and returns errors that flow back to the caller.
	decode := func(v any) error {
		dst, ok := v.(*cfg)
		if !ok {
			return errors.New("test decoder only supports *cfg")
		}
		dst.Name = "sentinel-test"
		return nil
	}
	_, err := f(context.Background(), decode)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinelErr, "round-trip should preserve the factory's error")
	assert.Contains(t, err.Error(), `decoded name="sentinel-test"`)
}
