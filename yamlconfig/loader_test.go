//go:build testing

package yamlconfig

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/karolusz/outbox"
	"github.com/karolusz/outbox/publisher"
)

// loaderTestSetup populates the plugin registry with stub factories the
// loader tests reference by name. We register minimal stubs here rather
// than blank-import the real publisher/fake and publisher/gcppubsub
// packages because we want the loader's behaviour exercised in isolation
// from those plugins' specifics.
func loaderTestSetup(t *testing.T) {
	t.Helper()
	publisher.ResetForTests()
	publisher.Register("fake", func(ctx context.Context, decode publisher.ConfigDecoder) (publisher.Publisher, error) {
		return loaderTestPublisher{name: "fake"}, nil
	})
	publisher.Register("gcppubsub", func(ctx context.Context, decode publisher.ConfigDecoder) (publisher.Publisher, error) {
		// Decode into a struct that mirrors the real plugin's minimal
		// requirement — Project must be present. Confirms the decoder
		// closure reaches plugin code with the right node attached.
		var cfg struct {
			Project string `yaml:"project"`
		}
		if err := decode(&cfg); err != nil {
			return nil, fmt.Errorf("gcppubsub: parse config: %w", err)
		}
		if cfg.Project == "" {
			return nil, errors.New("gcppubsub: project is required")
		}
		return loaderTestPublisher{name: "gcppubsub"}, nil
	})
	t.Cleanup(publisher.ResetForTests)
}

type loaderTestPublisher struct{ name string }

func (p loaderTestPublisher) Publish(ctx context.Context, target string, msg *publisher.Message) error {
	return nil
}
func (p loaderTestPublisher) Close(ctx context.Context) error { return nil }

func fixture(name string) string {
	return filepath.Join("testdata", name)
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

func TestLoadAddressBook_GcppubsubConfigPassedThrough(t *testing.T) {
	// The loader builds a decoder closure over the parsed yaml.Node and
	// hands it to the factory. The test gcppubsub factory above requires
	// a non-empty Project field — proves end-to-end decoder flow works.
	loaderTestSetup(t)

	book, err := LoadAddressBook(t.Context(), fixture("gcppubsub_with_config.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)
}

func TestLoadAddressBook_UserOptOverlap_StillErrors(t *testing.T) {
	// User-supplied opt registers the same publisher name as YAML.
	// Override semantics are not supported; this is reported as a
	// duplicate.
	loaderTestSetup(t)

	_, err := LoadAddressBook(t.Context(), fixture("dup_with_user_opt.yaml"),
		outbox.WithPublisher("fake-a", loaderTestPublisher{name: "user-supplied"}),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), `publisher key "fake-a" registered 2 times`)
}

func TestLoadAddressBook_UserOptAddsToYAML(t *testing.T) {
	// User-supplied opt registers a publisher and a route NOT in the
	// YAML — should be appended cleanly.
	loaderTestSetup(t)

	book, err := LoadAddressBook(t.Context(), fixture("happy.yaml"),
		outbox.WithPublisher("extra", loaderTestPublisher{name: "extra"}),
		outbox.WithRoute("event.extra.v1", outbox.Route{Publisher: "extra", Target: "extra-target"}),
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
	publisher.ResetForTests()
	t.Cleanup(publisher.ResetForTests)

	book, err := LoadAddressBookValidateOnly(fixture("happy.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)

	assert.True(t, book.Has("event.alpha.v1"))
	assert.False(t, book.Has("nonexistent"))
	assert.NoError(t, book.Validate("event.beta.v1"))
	assert.ErrorIs(t, book.Validate("nonexistent"), outbox.ErrUnknownAddress)
}

func TestLoadAddressBookValidateOnly_PluginNotRegistered_StillSucceeds(t *testing.T) {
	// The whole point of validate-only is that producer binaries can
	// validate addresses without depending on plugin packages. The
	// unknown plugin in the YAML should NOT cause a failure here.
	publisher.ResetForTests()
	t.Cleanup(publisher.ResetForTests)

	book, err := LoadAddressBookValidateOnly(fixture("unknown_plugin.yaml"))
	require.NoError(t, err)
	require.NotNil(t, book)
	assert.True(t, book.Has("event.alpha.v1"))
}

func TestLoadAddressBookValidateOnly_ResolveReturnsStub(t *testing.T) {
	// Validate-only books return the stub publisher from Resolve. Its
	// Publish errors clearly so adopters know they cannot use a
	// validate-only book for delivery.
	publisher.ResetForTests()
	t.Cleanup(publisher.ResetForTests)

	book, err := LoadAddressBookValidateOnly(fixture("happy.yaml"))
	require.NoError(t, err)

	pub, target, err := book.Resolve("event.alpha.v1")
	require.NoError(t, err)
	require.NotNil(t, pub)
	assert.Equal(t, "topic-alpha", target)

	// Attempting to publish through the stub should error with a clear
	// "validate-only" message.
	err = pub.Publish(t.Context(), target, &publisher.Message{})
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
