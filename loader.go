package outbox

import (
	"context"
	"errors"
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// yamlConfig is the wire shape of the address-book YAML file. The
// loader parses bytes into this shape, then walks publishers and
// addresses to populate an AddressBook.
type yamlConfig struct {
	Version    int             `yaml:"version"`
	Publishers []yamlPublisher `yaml:"publishers"`
	Addresses  []yamlAddress   `yaml:"addresses"`
}

// yamlPublisher captures a publisher entry. The Config field is kept as
// a yaml.Node so we can re-marshal just that block and hand it to the
// plugin's factory, which decodes it into a plugin-specific Config
// struct without the loader needing to know plugin internals.
type yamlPublisher struct {
	Name   string    `yaml:"name"`
	Plugin string    `yaml:"plugin"`
	Config yaml.Node `yaml:"config"`
}

// yamlAddress captures an address entry.
type yamlAddress struct {
	Name      string `yaml:"name"`
	Publisher string `yaml:"publisher"`
	Target    string `yaml:"target"`
}

const addressBookSchemaVersion = 1

// LoadAddressBook reads a YAML address-book file, instantiates publishers
// by looking up their plugin factories in the global registry, builds
// routes, validates the whole graph, and returns a ready-to-use
// AddressBook.
//
// Additional Go-side options (typically WithPublisher / WithRoute) are
// applied alongside the YAML-derived options. They must use keys disjoint
// from anything in the YAML — duplicates between YAML and Go opts are
// reported as errors, same as duplicates within YAML. Adopters who need
// to inject a publisher that YAML cannot describe (e.g. Vault-fetched
// credentials) should give it a name that does not appear in the YAML.
//
// Plugins must be registered before this is called — adopters typically
// blank-import the plugin packages. If the YAML references a plugin not
// in the registry, the error message recommends checking blank imports.
func LoadAddressBook(ctx context.Context, path string, opts ...AddressBookOption) (*AddressBook, error) {
	cfg, err := readYAMLConfig(path)
	if err != nil {
		return nil, err
	}

	yamlOpts, instantiationErrors := buildOptsFromYAML(ctx, cfg)

	// Combine YAML-derived opts with user-supplied opts, then run the
	// standard NewAddressBook validation. We collect instantiation errors
	// separately because they happen BEFORE the validation phase and
	// would otherwise be masked by downstream "unregistered publisher"
	// complaints.
	allOpts := make([]AddressBookOption, 0, len(yamlOpts)+len(opts))
	allOpts = append(allOpts, yamlOpts...)
	allOpts = append(allOpts, opts...)

	book, buildErr := NewAddressBook(allOpts...)

	switch {
	case len(instantiationErrors) > 0 && buildErr != nil:
		// Both classes of error fired. Report instantiation first
		// (root cause) followed by the validation errors that depend
		// on it.
		return nil, fmt.Errorf(
			"address book %s: failed to load:\n  plugin instantiation:\n    - %w\n  validation:\n    - %w",
			path,
			errors.Join(instantiationErrors...),
			buildErr,
		)
	case len(instantiationErrors) > 0:
		return nil, fmt.Errorf(
			"address book %s: plugin instantiation failed:\n  - %w",
			path,
			errors.Join(instantiationErrors...),
		)
	case buildErr != nil:
		return nil, fmt.Errorf("address book %s: %w", path, buildErr)
	}

	return book, nil
}

// LoadAddressBookValidateOnly parses a YAML address-book file for
// validation purposes only — it does NOT instantiate publishers. The
// returned AddressBook supports Has and Validate (for producer-side
// address pre-checks) but Resolve will return a publisher that errors on
// Publish.
//
// Useful in producer binaries that want to validate addresses at API
// boundaries but do not need to publish themselves — they avoid pulling
// in transitive dependencies from plugin packages (Pub/Sub SDK, etc.).
func LoadAddressBookValidateOnly(path string) (*AddressBook, error) {
	cfg, err := readYAMLConfig(path)
	if err != nil {
		return nil, err
	}

	opts := make([]AddressBookOption, 0, len(cfg.Publishers)+len(cfg.Addresses))
	stub := validateOnlyPublisher{}
	for _, p := range cfg.Publishers {
		// Skip blank-name entries — the duplicate-route validation in
		// NewAddressBook will surface them via the routes that reference
		// them. We only need the name slot here.
		if p.Name == "" {
			continue
		}
		opts = append(opts, WithPublisher(p.Name, stub))
	}
	for _, a := range cfg.Addresses {
		opts = append(opts, WithRoute(a.Name, Route{Publisher: a.Publisher, Target: a.Target}))
	}

	book, err := NewAddressBook(opts...)
	if err != nil {
		return nil, fmt.Errorf("address book %s: %w", path, err)
	}
	return book, nil
}

// validateOnlyPublisher is the placeholder Publisher used by
// LoadAddressBookValidateOnly. Its Publish errors clearly if anything
// tries to deliver through a validate-only book.
type validateOnlyPublisher struct{}

func (validateOnlyPublisher) Publish(ctx context.Context, target string, msg *Message) error {
	return errors.New("outbox: this AddressBook was loaded validate-only; Publish is unavailable")
}

func (validateOnlyPublisher) Close(ctx context.Context) error { return nil }

// readYAMLConfig reads and parses the file, validates schema version.
func readYAMLConfig(path string) (*yamlConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("outbox: read address book %s: %w", path, err)
	}

	var cfg yamlConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("outbox: parse address book %s: %w", path, err)
	}

	if cfg.Version != addressBookSchemaVersion {
		if cfg.Version == 0 {
			return nil, fmt.Errorf("outbox: address book %s: missing or zero `version:` field (expected %d)", path, addressBookSchemaVersion)
		}
		return nil, fmt.Errorf("outbox: address book %s: unsupported version %d (this build supports version %d)", path, cfg.Version, addressBookSchemaVersion)
	}

	return &cfg, nil
}

// buildOptsFromYAML walks the YAML config, instantiating publishers via
// the plugin registry and turning addresses into WithRoute options.
// Instantiation errors are accumulated and returned separately from the
// opts slice so the caller can decide how to surface them alongside the
// later validation errors.
func buildOptsFromYAML(ctx context.Context, cfg *yamlConfig) ([]AddressBookOption, []error) {
	opts := make([]AddressBookOption, 0, len(cfg.Publishers)+len(cfg.Addresses))
	var instantiationErrors []error

	for _, p := range cfg.Publishers {
		if p.Name == "" {
			instantiationErrors = append(instantiationErrors, errors.New("publisher entry has empty name"))
			continue
		}
		if p.Plugin == "" {
			instantiationErrors = append(instantiationErrors, fmt.Errorf("publisher %q has empty plugin", p.Name))
			continue
		}

		factory, ok := lookupPlugin(p.Plugin)
		if !ok {
			instantiationErrors = append(instantiationErrors,
				fmt.Errorf("publisher %q references plugin %q which is not registered (did you forget to blank-import the plugin package?)", p.Name, p.Plugin))
			continue
		}

		// Build a decoder closure over the parsed yaml.Node. The plugin's
		// factory calls decode(&cfg) once into its plugin-specific Config
		// struct; no re-serialisation through bytes.
		configNode := p.Config // capture in this iteration's scope
		decode := func(v any) error {
			if configNode.Kind == 0 {
				// `config:` block omitted entirely. Leave dest at zero
				// value — same behaviour as decoding an empty document.
				return nil
			}
			return configNode.Decode(v)
		}

		pub, err := factory(ctx, decode)
		if err != nil {
			instantiationErrors = append(instantiationErrors,
				fmt.Errorf("publisher %q (plugin %s): %w", p.Name, p.Plugin, err))
			continue
		}

		opts = append(opts, WithPublisher(p.Name, pub))
	}

	for _, a := range cfg.Addresses {
		opts = append(opts, WithRoute(a.Name, Route{Publisher: a.Publisher, Target: a.Target}))
	}

	return opts, instantiationErrors
}

