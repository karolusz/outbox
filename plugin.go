package outbox

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// PluginFactory builds a Publisher from raw YAML config bytes.
//
// Plugin authors register a factory via RegisterPlugin so the YAML loader
// (and adopters using the in-Go construction path) can instantiate the
// plugin's Publisher without knowing the plugin's internals. The rawConfig
// slice is the YAML `config:` block beneath the plugin's entry in the
// address book; the factory is responsible for unmarshalling it into a
// plugin-specific struct, validating it, and returning a constructed
// Publisher (or an error).
type PluginFactory func(ctx context.Context, rawConfig []byte) (Publisher, error)

// pluginRegistry is the package-level singleton holding all registered
// plugins. The pattern mirrors database/sql.Register: blank-import the
// plugin package, its init() registers a factory, the lib looks up the
// factory at AddressBook construction time. There is no Unregister and
// no clear; plugins live for process lifetime.
var (
	pluginRegistryMu sync.RWMutex
	pluginRegistry   = make(map[string]PluginFactory)
)

// RegisterPlugin associates a plugin name with its factory function.
// Typically called from a plugin package's init(), with adopters blank-
// importing the plugin package to trigger registration.
//
// Plugin names must be unique within a process. Registering the same
// name twice panics — this is a startup-time error and a panic from init()
// surfaces the bug at program boot rather than at first config load.
// Name collisions are almost always configuration mistakes worth crashing
// on; adopters who want to override a lib-shipped plugin should register
// under a different name.
func RegisterPlugin(name string, factory PluginFactory) {
	if name == "" {
		panic("outbox: RegisterPlugin called with empty name")
	}
	if factory == nil {
		panic(fmt.Sprintf("outbox: RegisterPlugin(%q) called with nil factory", name))
	}
	pluginRegistryMu.Lock()
	defer pluginRegistryMu.Unlock()
	if _, exists := pluginRegistry[name]; exists {
		panic(fmt.Sprintf("outbox: plugin %q already registered", name))
	}
	pluginRegistry[name] = factory
}

// RegisteredPlugins returns the names of all currently-registered plugins,
// sorted alphabetically. Useful for diagnostics, "list available plugins"
// output, and confirming a blank-import side-effect actually ran.
func RegisteredPlugins() []string {
	pluginRegistryMu.RLock()
	defer pluginRegistryMu.RUnlock()
	names := make([]string, 0, len(pluginRegistry))
	for name := range pluginRegistry {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// lookupPlugin retrieves a factory by name. Used internally by the YAML
// loader; unexported because adopters should reach for RegisteredPlugins
// for diagnostics rather than handle factories directly.
func lookupPlugin(name string) (PluginFactory, bool) {
	pluginRegistryMu.RLock()
	defer pluginRegistryMu.RUnlock()
	f, ok := pluginRegistry[name]
	return f, ok
}
