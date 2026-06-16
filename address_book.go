package outbox

import (
	"errors"
	"fmt"

	"github.com/karolusz/outbox/publisher"
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
// relay startup via NewAddressBook or LoadAddressBook. Immutable after
// construction.
//
// AddressBook is safe for concurrent reads. There is no public write API
// after NewAddressBook returns.
//
// A book constructed via SinglePublisherAddressBook behaves differently:
// every Resolve returns the configured publisher with target equal to the
// address. This is a v0.1 migration aid; the routes/publishers maps are
// empty in that mode.
type AddressBook struct {
	routes     map[string]Route
	publishers map[string]publisher.Publisher

	// passthrough, when non-nil, makes Resolve return (passthrough, address, nil)
	// for any address. Set only by SinglePublisherAddressBook.
	passthrough publisher.Publisher
}

// addressBookConfig is the internal accumulator passed through options.
// We track duplicate registrations as counts so NewAddressBook can report
// all of them in a single aggregated error.
type addressBookConfig struct {
	routes     map[string]Route
	publishers map[string]publisher.Publisher
	routeOrder []string // for stable error ordering
	pubOrder   []string
	routeCount map[string]int
	pubCount   map[string]int
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
func WithPublisher(key string, p publisher.Publisher) AddressBookOption {
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
		publishers: make(map[string]publisher.Publisher),
		routeCount: make(map[string]int),
		pubCount:   make(map[string]int),
	}
	for _, opt := range opts {
		opt(cfg)
	}

	var problems []error
	problems = append(problems, validateAtLeastOneRoute(cfg)...)
	problems = append(problems, validateUniquePublisherKeys(cfg)...)
	problems = append(problems, validateUniqueAddresses(cfg)...)
	problems = append(problems, validateRoutePublishers(cfg)...)
	problems = append(problems, validateRouteTargets(cfg)...)

	if len(problems) > 0 {
		return nil, fmt.Errorf("address book construction failed:\n  - %w", errors.Join(problems...))
	}

	return &AddressBook{
		routes:     cfg.routes,
		publishers: cfg.publishers,
	}, nil
}

// validateAtLeastOneRoute reports an error if no routes were registered.
// An empty book is treated as misconfiguration — adopters typically intend
// at least one address; an empty constructor call is almost always a bug.
func validateAtLeastOneRoute(cfg *addressBookConfig) []error {
	if len(cfg.routes) == 0 {
		return []error{errors.New("no routes registered")}
	}
	return nil
}

// validateUniquePublisherKeys reports one error per publisher key that was
// passed to WithPublisher more than once.
func validateUniquePublisherKeys(cfg *addressBookConfig) []error {
	var out []error
	for _, key := range cfg.pubOrder {
		if cfg.pubCount[key] > 1 {
			out = append(out, fmt.Errorf("publisher key %q registered %d times", key, cfg.pubCount[key]))
		}
	}
	return out
}

// validateUniqueAddresses reports one error per address that was passed to
// WithRoute more than once.
func validateUniqueAddresses(cfg *addressBookConfig) []error {
	var out []error
	for _, addr := range cfg.routeOrder {
		if cfg.routeCount[addr] > 1 {
			out = append(out, fmt.Errorf("address %q registered %d times", addr, cfg.routeCount[addr]))
		}
	}
	return out
}

// validateRoutePublishers reports routes whose Publisher field is empty or
// references a publisher key that was not registered. The two checks live
// in one function because they are alternatives — an empty Publisher
// short-circuits the "unregistered" check (no need to tell the operator
// the empty string is unregistered).
func validateRoutePublishers(cfg *addressBookConfig) []error {
	var out []error
	for _, addr := range cfg.routeOrder {
		route := cfg.routes[addr]
		if route.Publisher == "" {
			out = append(out, fmt.Errorf("route %q has empty Publisher reference", addr))
			continue
		}
		if _, ok := cfg.publishers[route.Publisher]; !ok {
			out = append(out, fmt.Errorf("route %q references unregistered publisher %q", addr, route.Publisher))
		}
	}
	return out
}

// validateRouteTargets reports routes whose Target field is empty.
func validateRouteTargets(cfg *addressBookConfig) []error {
	var out []error
	for _, addr := range cfg.routeOrder {
		if cfg.routes[addr].Target == "" {
			out = append(out, fmt.Errorf("route %q has empty Target", addr))
		}
	}
	return out
}

// Resolve looks up an address and returns its Publisher instance and the
// broker target the publisher should write to. Returns an error wrapping
// ErrUnknownAddress when the address is not registered.
//
// In a single-publisher book (see SinglePublisherAddressBook), every
// address resolves to the configured publisher with target equal to the
// address. The address is otherwise opaque to the book.
func (b *AddressBook) Resolve(address string) (publisher.Publisher, string, error) {
	if b.passthrough != nil {
		return b.passthrough, address, nil
	}
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

// Has reports whether the given address is registered. In a single-
// publisher book, every address is considered registered.
func (b *AddressBook) Has(address string) bool {
	if b.passthrough != nil {
		return true
	}
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

// SinglePublisherAddressBook is a v0.1 migration aid that returns an
// AddressBook routing every address to the supplied Publisher, with
// target equal to the address itself. The book has no validated routes;
// Has returns true for every address.
//
// Use this when migrating from a v0.1-style setup where a single
// Publisher served every message and msg.Address was the broker target
// verbatim. For new setups, prefer NewAddressBook or LoadAddressBook so
// addresses and targets are explicitly mapped — that decoupling is the
// whole point of the address book.
func SinglePublisherAddressBook(p publisher.Publisher) *AddressBook {
	return &AddressBook{passthrough: p}
}
