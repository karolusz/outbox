package publisher

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// ConfigDecoder unmarshals a plugin's config block into a destination
// struct. Plugin factories use this to populate their plugin-specific
// Config type without knowing how the config was loaded; today the YAML
// loader provides a closure over yaml.Node.Decode.
type ConfigDecoder func(v any) error

// Factory builds a Publisher from a decoder over the plugin's config
// block. Plugin authors register their factory via Register; the
// factory calls decode(&cfg) into a plugin-specific Config struct,
// validates it, and returns a constructed Publisher.
type Factory func(ctx context.Context, decode ConfigDecoder) (Publisher, error)

// registry holds registered plugin factories. The lock and map live
// together so callers cannot accidentally bypass the lock. No
// unregister; plugins live for process lifetime.
type registry struct {
	mu      sync.RWMutex
	plugins map[string]Factory
}

func newRegistry() *registry {
	return &registry{plugins: make(map[string]Factory)}
}

// register associates a plugin name with its factory. Panics on duplicate
// names. Input validation (empty name, nil factory) is the public Register
// function's job; this method assumes well-formed input.
func (r *registry) register(name string, factory Factory) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.plugins[name]; exists {
		panic(fmt.Sprintf("outbox: plugin %q already registered", name))
	}
	r.plugins[name] = factory
}

// list returns the registered plugin names sorted alphabetically.
func (r *registry) list() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.plugins))
	for name := range r.plugins {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// lookup retrieves a factory by name.
func (r *registry) lookup(name string) (Factory, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.plugins[name]
	return f, ok
}

// globalRegistry is the process-wide plugin registry. The pattern mirrors
// database/sql.Register: blank-import the plugin package, its init()
// registers a factory, the lib looks up the factory at AddressBook
// construction time.
var globalRegistry = newRegistry()

// Register associates a plugin name with its factory. Typically called
// from a plugin package's init(); adopters blank-import the package to
// trigger registration. Duplicate names panic — a startup-time error
// caught at program boot, not at first config load.
func Register(name string, factory Factory) {
	if name == "" {
		panic("outbox: Register called with empty name")
	}
	if factory == nil {
		panic(fmt.Sprintf("outbox: Register(%q) called with nil factory", name))
	}
	globalRegistry.register(name, factory)
}

// Names returns the names of all registered plugins, sorted
// alphabetically. Useful for diagnostics and confirming a blank-import
// side-effect ran.
func Names() []string {
	return globalRegistry.list()
}

// Lookup retrieves a factory by name. The YAML loader calls this to
// instantiate publishers; adopters typically don't call it directly.
func Lookup(name string) (Factory, bool) {
	return globalRegistry.lookup(name)
}
