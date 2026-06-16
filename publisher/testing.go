//go:build testing

package publisher

// ResetForTests swaps in a fresh registry for test isolation. Available
// only under the "testing" build tag so production binaries never expose
// it. Both this package's own tests and cross-package tests (e.g.
// yamlconfig) use it to start each test with an empty registry.
func ResetForTests() {
	globalRegistry = newRegistry()
}
