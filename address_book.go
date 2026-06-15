package outbox

import (
	"errors"
	"fmt"
)

// Route describes where a logical address gets delivered.
//
// Publisher is the key of a registered Publisher instance (see
// WithPublisher). Target is the broker-specific destination name passed to
// that Publisher's Publish method — e.g. a GCP Pub/Sub topic, a Kafka
// topic, or an SQS queue name. What "target" means is the Publisher's
// concern; the outbox treats it as an opaque string.
type Route struct {
	Publisher string
	Target    string
}

// AddressBook is the routing table between producer-visible logical
// addresses and concrete (Publisher, target) pairs. Constructed once at
// relay startup via NewAddressBook (or LoadAddressBook, which is layered
// on top in a separate file). Immutable after construction.
//
// AddressBook is safe for concurrent reads. There is no public write API
// after NewAddressBook returns.
type AddressBook struct {
	routes     map[string]Route
	publishers map[string]Publisher
}

// addressBookConfig is the internal accumulator passed through options.
// We track duplicate registrations as counts so NewAddressBook can report
// all of them in a single aggregated error.
type addressBookConfig struct {
	routes        map[string]Route
	publishers    map[string]Publisher
	routeOrder    []string // for stable error ordering
	pubOrder      []string
	routeCount    map[string]int
	pubCount      map[string]int
}

// AddressBookOption configures a new AddressBook. Apply via NewAddressBook.
type AddressBookOption func(*addressBookConfig)

// WithPublisher registers a Publisher instance under a stable key. The key
// is referenced by Route.Publisher (in WithRoute or in a loaded YAML) to
// associate routes with publisher backends. Multiple routes can share a
// single publisher instance — recommended when several addresses publish
// to the same broker (e.g. several topics in one GCP project).
//
// Duplicate keys are reported as an error by NewAddressBook.
func WithPublisher(key string, p Publisher) AddressBookOption {
	return func(c *addressBookConfig) {
		if _, seen := c.publishers[key]; !seen {
			c.pubOrder = append(c.pubOrder, key)
		}
		c.publishers[key] = p
		c.pubCount[key]++
	}
}

// WithRoute registers a logical address and the (publisher, target) pair
// it resolves to. The route's Publisher field must match a key registered
// via WithPublisher; otherwise NewAddressBook returns an error.
//
// Duplicate addresses are reported as an error by NewAddressBook.
func WithRoute(address string, route Route) AddressBookOption {
	return func(c *addressBookConfig) {
		if _, seen := c.routes[address]; !seen {
			c.routeOrder = append(c.routeOrder, address)
		}
		c.routes[address] = route
		c.routeCount[address]++
	}
}

// NewAddressBook constructs an AddressBook from the given options. All
// validation problems are aggregated into a single returned error so the
// adopter sees every issue in one shot rather than fix-recompile-repeat.
//
// Validates:
//   - At least one route is registered (empty book is treated as
//     misconfiguration).
//   - No address is registered more than once.
//   - No publisher key is registered more than once.
//   - Every Route.Publisher references a registered publisher.
//   - No Route.Target is empty.
//
// A registered publisher with no route referencing it is permitted (the
// adopter may be staging it for a future route).
func NewAddressBook(opts ...AddressBookOption) (*AddressBook, error) {
	cfg := &addressBookConfig{
		routes:     make(map[string]Route),
		publishers: make(map[string]Publisher),
		routeCount: make(map[string]int),
		pubCount:   make(map[string]int),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	var problems []error

	if len(cfg.routes) == 0 {
		problems = append(problems, errors.New("no routes registered"))
	}

	for _, key := range cfg.pubOrder {
		if cfg.pubCount[key] > 1 {
			problems = append(problems, fmt.Errorf("publisher key %q registered %d times", key, cfg.pubCount[key]))
		}
	}
	for _, addr := range cfg.routeOrder {
		if cfg.routeCount[addr] > 1 {
			problems = append(problems, fmt.Errorf("address %q registered %d times", addr, cfg.routeCount[addr]))
		}
	}

	for _, addr := range cfg.routeOrder {
		route := cfg.routes[addr]
		if route.Publisher == "" {
			problems = append(problems, fmt.Errorf("route %q has empty Publisher reference", addr))
		} else if _, ok := cfg.publishers[route.Publisher]; !ok {
			problems = append(problems, fmt.Errorf("route %q references unregistered publisher %q", addr, route.Publisher))
		}
		if route.Target == "" {
			problems = append(problems, fmt.Errorf("route %q has empty Target", addr))
		}
	}

	if len(problems) > 0 {
		return nil, fmt.Errorf("address book construction failed:\n  - %w", errors.Join(problems...))
	}

	return &AddressBook{
		routes:     cfg.routes,
		publishers: cfg.publishers,
	}, nil
}

// Resolve looks up an address and returns its Publisher instance and the
// broker target the publisher should write to. Returns an error wrapping
// ErrUnknownAddress when the address is not registered.
func (b *AddressBook) Resolve(address string) (Publisher, string, error) {
	route, ok := b.routes[address]
	if !ok {
		return nil, "", fmt.Errorf("%w: %q", ErrUnknownAddress, address)
	}
	pub, ok := b.publishers[route.Publisher]
	if !ok {
		// Defensive: NewAddressBook validates this is impossible. If it
		// fires, the AddressBook was constructed by bypassing the
		// public API.
		return nil, "", fmt.Errorf("address %q references publisher key %q which is not registered", address, route.Publisher)
	}
	return pub, route.Target, nil
}

// Has reports whether the given address is registered.
func (b *AddressBook) Has(address string) bool {
	_, ok := b.routes[address]
	return ok
}

// Validate returns nil if address is registered, otherwise an error
// wrapping ErrUnknownAddress. Useful at producer API boundaries to reject
// unknown addresses early — for example, a REST handler can call this
// before inserting an outbox row, returning a 400 to the caller instead
// of letting the row reach the relay and stall there.
func (b *AddressBook) Validate(address string) error {
	if !b.Has(address) {
		return fmt.Errorf("%w: %q", ErrUnknownAddress, address)
	}
	return nil
}
