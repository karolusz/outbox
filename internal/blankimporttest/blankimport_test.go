//go:build testing

// Package blankimporttest verifies that lib-shipped plugin packages
// register themselves with the publisher registry via init() side effects
// when blank-imported.
//
// This test lives in its own package because the plugin registry is a
// package-level singleton in outbox/publisher; tests inside the publisher
// package reset it for isolation. A separate test binary that blank-
// imports the plugins and observes the registry catches regressions in
// the registration mechanism itself — the kind of bug that would silently
// make a plugin invisible to adopters following the documented pattern.
package blankimporttest

import (
	"slices"
	"testing"

	"github.com/karolusz/outbox/publisher"

	// Blank-imported for their side effects (init() registers the plugin).
	// If either import is removed, the corresponding assertion below fails,
	// which is the exact behaviour we want to catch.
	_ "github.com/karolusz/outbox/publisher/fake"
	_ "github.com/karolusz/outbox/publisher/gcppubsub"
)

func TestBlankImport_RegistersGcppubsub(t *testing.T) {
	names := publisher.Names()
	if !slices.Contains(names, "gcppubsub") {
		t.Fatalf("expected gcppubsub to be registered via blank import; got: %v", names)
	}
}

func TestBlankImport_RegistersFake(t *testing.T) {
	names := publisher.Names()
	if !slices.Contains(names, "fake") {
		t.Fatalf("expected fake to be registered via blank import; got: %v", names)
	}
}
