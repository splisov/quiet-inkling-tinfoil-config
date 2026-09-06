//go:build audioonly

package main

import (
	"crypto/ed25519"
	"net/http"
	"quietinkling/private-inference/attested"
	"quietinkling/private-inference/gateway"
)

// Public STT workloads cannot acquire a text route through a policy update.
func registerText(_ *http.ServeMux, policy *attested.Policy, _ ed25519.PublicKey, _ gateway.Claim, _ gateway.Complete) (int, error) {
	if _, e := policy.Endpoint("textIngress"); e == nil {
		return 0, attested.ErrUnavailable
	}
	return 0, nil
}
