package publisher

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// ConfigDecoder unmarshals a plugin's config block into a destination
// struct. Plugin factories use this to populate their plugin-specific
// Config type without knowing how the config arrives — YAML today (the
// loader builds a closure over yaml.Node.Decode), potentially JSON, TOML,
// or programmatic construction later. Plugins keep their `yaml:"name"`
// struct tags; yaml.Node honours them during decode, and any future
// format-aware decoder can do the same.
//
// Implementations may be called more than once with different destination
// types; nothing about ConfigDecoder requires single-use semantics.
type ConfigDecoder func(v any) error

// Factory builds a Publisher from a decoder over the plugin's config block.
//
// Plugin authors register a factory via Register so the YAML loader (and
// adopters using the in-Go construction path) can instantiate the plugin's
// Publisher without knowing the plugin's internals. The factory calls
// decode(&cfg) once into a plugin-specific Config struct, validates it,
// and returns a constructed Publisher (or an error).
type Factory func(ctx context.Context, decode ConfigDecoder) (Publisher, error)

// registry holds registered plugin factories. The lock and the map are
// kept together as one type so callers cannot accidentally read or write
// the map outside the lock. There is no unregister and no clear at the
// type level; plugins live for process lifetime.
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

// Register associates a plugin name with its factory function. Typically
// called from a plugin package's init(), with adopters blank-importing the
// plugin package to trigger registration.
//
// Plugin names must be unique within a process. Registering the same name
// twice panics — this is a startup-time error and a panic from init()
// surfaces the bug at program boot rather than at first config load. Name
// collisions are almost always configuration mistakes worth crashing on;
// adopters who want to override a lib-shipped plugin should register under
// a different name.
func Register(name string, factory Factory) {
	if name == "" {
		panic("outbox: Register called with empty name")
	}
	if factory == nil {
		panic(fmt.Sprintf("outbox: Register(%q) called with nil factory", name))
	}
	globalRegistry.register(name, factory)
}

// Names returns the names of all currently-registered plugins, sorted
// alphabetically. Useful for diagnostics, "list available plugins" output,
// and confirming a blank-import side-effect actually ran.
func Names() []string {
	return globalRegistry.list()
}

// Lookup retrieves a factory by name from the global registry. The YAML
// loader (and any future loader format) calls this to instantiate
// publishers; adopters typically do not call it directly.
func Lookup(name string) (Factory, bool) {
	return globalRegistry.lookup(name)
}
