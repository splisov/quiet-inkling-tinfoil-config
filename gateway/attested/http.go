package attested

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"time"

	tf "github.com/tinfoilsh/tinfoil-go/verifier/client"
)

type Evidence struct {
	Host, Repo, Digest, Key string
	Verified                bool
}

func approve(endpoint Endpoint, evidence Evidence) error {
	if !evidence.Verified || evidence.Key == "" || evidence.Host != endpoint.Host || evidence.Repo != endpoint.Repo || evidence.Digest != endpoint.Digest {
		return ErrUnavailable
	}
	return nil
}

type boundedTransport struct {
	transport http.RoundTripper
	host      string
	expires   time.Time
}

func (b boundedTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.Host != b.host || r.URL.User != nil || time.Now().After(b.expires) {
		return nil, ErrUnavailable
	}
	return b.transport.RoundTrip(r)
}

// VerifyHTTP never returns a transport until both attestation and exact owner
// approval succeed. There is no automatic re-verification/retry after dispatch.
type Recipient struct {
	Client    *http.Client
	Evidence  string
	HPKEKey   string
	ExpiresAt int64
}

func VerifyHTTP(p *Policy, role string) (*http.Client, string, error) {
	recipient, err := VerifyRecipient(p, role)
	if err != nil {
		return nil, "", err
	}
	return recipient.Client, recipient.Evidence, nil
}

// Both keys come from the same hardware/code verification and owner approval.
func VerifyRecipient(p *Policy, role string) (*Recipient, error) {
	if p == nil || time.Now().Unix() >= p.ExpiresAt {
		return nil, ErrUnavailable
	}
	endpoint, e := p.Endpoint(role)
	if e != nil {
		return nil, e
	}
	verifier := tf.NewSecureClient(endpoint.Host, endpoint.Repo)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	truth, e := boundedVerification(ctx, verifier.Verify)
	if e != nil {
		return nil, ErrUnavailable
	}
	document := verifier.VerificationDocument()
	if document == nil || approve(endpoint, Evidence{truth.EnclaveHost, truth.ConfigRepo, truth.Digest, truth.TLSPublicKey, document.SecurityVerified}) != nil {
		return nil, ErrUnavailable
	}
	hpke, e := hex.DecodeString(truth.HPKEPublicKey)
	if e != nil || len(hpke) != 32 {
		return nil, ErrUnavailable
	}
	client, e := verifier.HTTPClient()
	if e != nil {
		return nil, ErrUnavailable
	}
	expiry := time.Now().Add(5 * time.Minute)
	if policyExpiry := time.Unix(p.ExpiresAt, 0); policyExpiry.Before(expiry) {
		expiry = policyExpiry
	}
	client.Transport = boundedTransport{client.Transport, endpoint.Host, expiry}
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return ErrUnavailable }
	// Request contexts impose operation-specific deadlines, including websocket lifetime.
	raw, _ := json.Marshal(document)
	return &Recipient{Client: client, Evidence: string(raw), HPKEKey: truth.HPKEPublicKey, ExpiresAt: expiry.Unix()}, nil
}

func RequestContext(parent context.Context, expires int64) (context.Context, context.CancelFunc) {
	deadline := time.Now().Add(60 * time.Second)
	if expiry := time.Unix(expires, 0); expiry.Before(deadline) {
		deadline = expiry
	}
	return context.WithDeadline(parent, deadline)
}
