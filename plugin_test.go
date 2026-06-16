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

func noopFactory(ctx context.Context, raw []byte) (Publisher, error) {
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
	RegisterPlugin("sentinel", func(ctx context.Context, raw []byte) (Publisher, error) {
		return nil, fmt.Errorf("got %d config bytes: %w", len(raw), sentinelErr)
	})

	f, ok := lookupPlugin("sentinel")
	require.True(t, ok)

	rawConfig := []byte("some-config-bytes")
	_, err := f(context.Background(), rawConfig)
	require.Error(t, err)
	require.ErrorIs(t, err, sentinelErr, "round-trip should preserve the factory's error")
	assert.Contains(t, err.Error(), fmt.Sprintf("got %d config bytes", len(rawConfig)))
}
