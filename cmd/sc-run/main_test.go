package main

import (
	"crypto/x509"
	"testing"
)

// sc-run runs in images without CA certificates, so the embedded roots must be
// registered as the system fallback. SetFallbackRoots panics when called twice,
// which is how the test detects the registration made by the blank import.
func TestEmbeddedFallbackRootsAreRegistered(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("no fallback roots registered: HTTPS fails in images without ca-certificates")
		}
	}()
	x509.SetFallbackRoots(x509.NewCertPool())
}
