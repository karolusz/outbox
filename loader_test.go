//go:build testing

package outbox

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loaderTestSetup blank-imports the lib-shipped plugins so the registry is
// populated before each test that needs them. Tests that mutate the
// registry should defer resetPluginRegistry().
//
// We register a fake factory here directly rather than blank-import the
// publisher/fake package because importing it would create a circular
// dependency (publisher/fake → outbox → loader → ... back to fake).
// The loader tests need plugins registered; the trick is to register
// minimal stub factories from within the outbox package itself.
func loaderTestSetup(t *testing.T) {
	t.Helper()
	resetPluginRegistry()
	// Register a no-op "fake" factory and a "gcppubsub" factory that
	// validates the config has a project. These cover the loader-test
	// scenarios without depending on the real plugin packages.
	RegisterPlugin("fake", func(ctx context.Context, raw []byte) (Publisher, error) {
		return loaderTestPublisher{name: "fake"}, nil
	})
	RegisterPlugin("gcppubsub", func(ctx context.Context, raw []byte) (Publisher, error) {
		// Look for "project:" in the raw bytes — minimal validation that
		// matches what the real plugin does at the boundary.
		if len(raw) == 0 {
			return nil, errors.New("gcppubsub: project is required")
		}
		// We don't actually parse YAML here; just confirm bytes were
		// passed through. For tests that need real parsing, use the
		// real publisher/gcppubsub package and verify in the
		// blankimporttest package.
		return loaderTestPublisher{name: "gcppubsub"}, nil
	})
	t.Cleanup(resetPluginRegistry)
}

type loaderTestPublisher struct{ name string }

func (p loaderTestPublisher) Publish(ctx context.Context, target string, msg *Message) error {
	return nil
}
func (p loaderTestPublisher) Close(ctx context.Context) error { return nil }

func fixture(name string) string {
	return filepath.Join("testdata", "loader", name)
}

func TestLoadAddressBook_HappyPath(t *testing.T) {
	loaderTestSetup(t)

	book, err := LoadAddressBook(t.Context(), fixture("happy.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)

	pub, target, err := book.Resolve("event.alpha.v1")
	require.NoError(t, err)
	require.NotNil(t, pub)
	assert.Equal(t, "topic-alpha", target)

	pub, target, err = book.Resolve("event.gamma.v1")
	require.NoError(t, err)
	require.NotNil(t, pub)
	assert.Equal(t, "topic-gamma", target)
}

func TestLoadAddressBook_FileNotFound(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("does_not_exist.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "read address book")
}

func TestLoadAddressBook_MalformedYAML(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("malformed.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse address book")
}

func TestLoadAddressBook_MissingVersion(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("missing_version.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing or zero `version:`")
}

func TestLoadAddressBook_UnsupportedVersion(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("unsupported_version.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported version 99")
}

func TestLoadAddressBook_UnknownPlugin(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("unknown_plugin.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `references plugin "kafka"`)
	assert.Contains(t, err.Error(), "blank-import")
}

func TestLoadAddressBook_DanglingPublisherRef(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("dangling_publisher_ref.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `references unregistered publisher "nonexistent"`)
}

func TestLoadAddressBook_DuplicatePublisher(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("duplicate_publisher.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `publisher key "fake-a" registered 2 times`)
}

func TestLoadAddressBook_DuplicateAddress(t *testing.T) {
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("duplicate_address.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `address "event.alpha.v1" registered 2 times`)
}

func TestLoadAddressBook_MissingConfigBlock_ZeroValueDecoded(t *testing.T) {
	// fake plugin tolerates an empty config; loading should succeed.
	loaderTestSetup(t)

	book, err := LoadAddressBook(t.Context(), fixture("missing_config_block.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)
}

func TestLoadAddressBook_GcppubsubConfigBytesPassedThrough(t *testing.T) {
	// The loader marshals the config Node back to bytes and hands them
	// to the factory. Our test gcppubsub factory checks that the bytes
	// are non-empty — proves end-to-end byte flow works.
	loaderTestSetup(t)

	book, err := LoadAddressBook(t.Context(), fixture("gcppubsub_with_config.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)
}

func TestLoadAddressBook_UserOptOverlap_StillErrors(t *testing.T) {
	// User-supplied opt registers the same publisher name as YAML.
	// v0.2 does not support override semantics; this is reported as a
	// duplicate.
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("dup_with_user_opt.yaml"),
		WithPublisher("fake-a", loaderTestPublisher{name: "user-supplied"}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `publisher key "fake-a" registered 2 times`)
}

func TestLoadAddressBook_UserOptAddsToYAML(t *testing.T) {
	// User-supplied opt registers a publisher and a route NOT in the
	// YAML — should be appended cleanly.
	loaderTestSetup(t)

	book, err := LoadAddressBook(t.Context(), fixture("happy.yaml"),
		WithPublisher("extra", loaderTestPublisher{name: "extra"}),
		WithRoute("event.extra.v1", Route{Publisher: "extra", Target: "extra-target"}),
	)
	require.NoError(t, err)

	pub, target, err := book.Resolve("event.extra.v1")
	require.NoError(t, err)
	require.NotNil(t, pub)
	assert.Equal(t, "extra-target", target)
}

func TestLoadAddressBookValidateOnly_HappyPath(t *testing.T) {
	// The validate-only loader does NOT need plugins to be registered
	// because it doesn't instantiate publishers. Don't call
	// loaderTestSetup — fresh registry, but loader should still succeed.
	resetPluginRegistry()
	t.Cleanup(resetPluginRegistry)

	book, err := LoadAddressBookValidateOnly(fixture("happy.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)

	assert.True(t, book.Has("event.alpha.v1"))
	assert.False(t, book.Has("nonexistent"))
	assert.NoError(t, book.Validate("event.beta.v1"))
	assert.ErrorIs(t, book.Validate("nonexistent"), ErrUnknownAddress)
}

func TestLoadAddressBookValidateOnly_PluginNotRegistered_StillSucceeds(t *testing.T) {
	// The whole point of validate-only is that producer binaries can
	// validate addresses without depending on plugin packages. The
	// unknown plugin in the YAML should NOT cause a failure here.
	resetPluginRegistry()
	t.Cleanup(resetPluginRegistry)

	book, err := LoadAddressBookValidateOnly(fixture("unknown_plugin.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)
	assert.True(t, book.Has("event.alpha.v1"))
}

func TestLoadAddressBookValidateOnly_ResolveReturnsStub(t *testing.T) {
	// Validate-only books return the stub publisher from Resolve. Its
	// Publish errors clearly so adopters know they cannot use a
	// validate-only book for delivery.
	resetPluginRegistry()
	t.Cleanup(resetPluginRegistry)

	book, err := LoadAddressBookValidateOnly(fixture("happy.yaml"))
	require.NoError(t, err)

	pub, target, err := book.Resolve("event.alpha.v1")
	require.NoError(t, err)
	require.NotNil(t, pub)
	assert.Equal(t, "topic-alpha", target)

	// Attempting to publish through the stub should error with a clear
	// "validate-only" message.
	err = pub.Publish(t.Context(), target, &Message{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "validate-only")
}

func TestLoadAddressBookValidateOnly_FileNotFound(t *testing.T) {
	_, err := LoadAddressBookValidateOnly(fixture("does_not_exist.yaml"))
	require.Error(t, err)
}

func TestLoadAddressBookValidateOnly_MalformedYAML(t *testing.T) {
	_, err := LoadAddressBookValidateOnly(fixture("malformed.yaml"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parse address book")
}
