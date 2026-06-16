//go:build testing

package publisher

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func noopFactory(ctx context.Context, decode ConfigDecoder) (Publisher, error) {
	return nil, nil
}

func TestRegister_Stores(t *testing.T) {
	t.Cleanup(ResetForTests)
	ResetForTests()

	Register("test-plugin-stores", noopFactory)

	f, ok := Lookup("test-plugin-stores")
	require.True(t, ok, "registered factory should be retrievable via Lookup")
	require.NotNil(t, f)
}

func TestRegister_DuplicatePanics(t *testing.T) {
	t.Cleanup(ResetForTests)
	ResetForTests()

	Register("dup-test", noopFactory)

	require.PanicsWithValue(t,
		`outbox: plugin "dup-test" already registered`,
		func() { Register("dup-test", noopFactory) },
		"second registration of the same name should panic with a clear message",
	)
}

func TestRegister_EmptyNamePanics(t *testing.T) {
	require.PanicsWithValue(t,
		"outbox: Register called with empty name",
		func() { Register("", noopFactory) },
	)
}

func TestRegister_NilFactoryPanics(t *testing.T) {
	require.PanicsWithValue(t,
		`outbox: Register("nilcheck") called with nil factory`,
		func() { Register("nilcheck", nil) },
	)
}

func TestNames_Lists_Sorted(t *testing.T) {
	t.Cleanup(ResetForTests)
	ResetForTests()

	// Register in reverse alphabetical order to confirm the listing is sorted,
	// not insertion-ordered.
	Register("zebra", noopFactory)
	Register("apple", noopFactory)
	Register("mango", noopFactory)

	names := Names()
	assert.Equal(t, []string{"apple", "mango", "zebra"}, names)
}

func TestNames_EmptyByDefault(t *testing.T) {
	t.Cleanup(ResetForTests)
	ResetForTests()

	names := Names()
	assert.Empty(t, names)
}

// TestFactoryIsActuallyCalled exercises the round-trip: register a factory,
// retrieve it via Lookup, call it, observe the result. Catches regressions
// where the registry would store something but lookup returns a wrong
// instance.
func TestFactoryIsActuallyCalled(t *testing.T) {
	t.Cleanup(ResetForTests)
	ResetForTests()

	sentinelErr := errors.New("sentinel from factory")
	type cfg struct {
		Name string `yaml:"name"`
	}
	Register("sentinel", func(ctx context.Context, decode ConfigDecoder) (Publisher, error) {
		var c cfg
		if err := decode(&c); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		return nil, fmt.Errorf("decoded name=%q: %w", c.Name, sentinelErr)
	})

	f, ok := Lookup("sentinel")
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
