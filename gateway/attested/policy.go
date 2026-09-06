// Package attested binds owner-approved release policy to the maintained Tinfoil verifier.
package attested

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"
)

var ErrUnavailable = errors.New("verified private inference unavailable")

type Endpoint struct {
	Role       string `json:"role"`
	Host       string `json:"host"`
	Repo       string `json:"repo"`
	Digest     string `json:"digest"`
	Model      string `json:"model,omitempty"`
	Evaluation string `json:"evaluation,omitempty"`
}

type Policy struct {
	Version   int64      `json:"version"`
	IssuedAt  int64      `json:"issued_at"`
	ExpiresAt int64      `json:"expires_at"`
	Endpoints []Endpoint `json:"endpoints"`
}

var releaseDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)
var repositoryName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,99}/[A-Za-z0-9][A-Za-z0-9._-]{0,99}$`)
var policyRole = regexp.MustCompile(`^(audioIngress|audioModel|textIngress|textCard[01]|textReading[01])$`)
var modelIdentifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9:._/-]{0,199}$`)

func VerifyPolicy(payload, signature, publicKey string, floor int64, now time.Time) (*Policy, error) {
	if len(payload) > 32768 || len(signature) > 128 {
		return nil, ErrUnavailable
	}
	key, e := base64.RawURLEncoding.DecodeString(publicKey)
	if e != nil || len(key) != ed25519.PublicKeySize {
		return nil, ErrUnavailable
	}
	sig, e := base64.RawURLEncoding.DecodeString(signature)
	if e != nil || !ed25519.Verify(key, []byte("quiet-inkling-policy-v1."+payload), sig) {
		return nil, ErrUnavailable
	}
	raw, e := base64.RawURLEncoding.DecodeString(payload)
	if e != nil {
		return nil, ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	var p Policy
	if decoder.Decode(&p) != nil || decoder.Decode(new(any)) != io.EOF {
		return nil, ErrUnavailable
	}
	if p.Version < 1 || p.Version < floor || p.IssuedAt > now.Unix()+60 || p.ExpiresAt <= now.Unix() || p.ExpiresAt-p.IssuedAt > 86400 || p.ExpiresAt <= p.IssuedAt || len(p.Endpoints) < 1 || len(p.Endpoints) > 12 {
		return nil, ErrUnavailable
	}
	roles := map[string]bool{}
	for _, endpoint := range p.Endpoints {
		u, e := url.Parse("https://" + endpoint.Host)
		if e != nil || u.Host != endpoint.Host || u.User != nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" || !strings.Contains(u.Hostname(), ".") || !repositoryName.MatchString(endpoint.Repo) || !releaseDigest.MatchString(endpoint.Digest) || !policyRole.MatchString(endpoint.Role) || roles[endpoint.Role] {
			return nil, ErrUnavailable
		}
		if strings.HasSuffix(endpoint.Role, "Ingress") {
			if endpoint.Model != "" || endpoint.Evaluation != "" {
				return nil, ErrUnavailable
			}
		} else if !modelIdentifier.MatchString(endpoint.Model) || !releaseDigest.MatchString(endpoint.Evaluation) {
			return nil, ErrUnavailable
		}
		roles[endpoint.Role] = true
	}
	return &p, nil
}

func (p *Policy) Endpoint(role string) (Endpoint, error) {
	for _, e := range p.Endpoints {
		if e.Role == role {
			return e, nil
		}
	}
	return Endpoint{}, ErrUnavailable
}
