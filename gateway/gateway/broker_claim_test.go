package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const syntheticBrokerSecret = "synthetic-broker-secret-000000000000"

func continuousClaimFixture(t *testing.T, claim Claim) (http.Handler, string, *atomic.Int32, *atomic.Int32) {
	return continuousClaimFixtureWithLease(t, claim, 60)
}

func continuousClaimFixtureWithLease(t *testing.T, claim Claim, leaseSeconds int64) (http.Handler, string, *atomic.Int32, *atomic.Int32) {
	return continuousClaimFixtureWithAuthority(t, claim, leaseSeconds, leaseSeconds)
}

func continuousClaimFixtureWithAuthority(t *testing.T, claim Claim, leaseSeconds, capabilitySeconds int64) (http.Handler, string, *atomic.Int32, *atomic.Int32) {
	return continuousClaimFixtureWithPeriods(t, claim, leaseSeconds, capabilitySeconds, 300)
}

func continuousClaimFixtureWithPeriods(t *testing.T, claim Claim, leaseSeconds, capabilitySeconds, policySeconds int64) (http.Handler, string, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	capability := Capability{ID: "11111111-1111-4111-8111-111111111111", AttemptID: "22222222-2222-4222-8222-222222222222",
		Audience: "ingress.example", Operation: "audio", ExpiresAt: now + capabilitySeconds, LeaseExpiresAt: now + leaseSeconds,
		ProtocolVersion: 2, MaxAudioBytes: 960000}
	raw, err := json.Marshal(capability)
	if err != nil {
		t.Fatal(err)
	}
	payload := base64.RawURLEncoding.EncodeToString(raw)
	token := payload + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte("quiet-inkling-capability-v1."+payload)))
	dials, completions := new(atomic.Int32), new(atomic.Int32)
	handler := audioHandlerWithRenewal("ingress.example", public, claim,
		func(context.Context, string, bool) error { completions.Add(1); return nil },
		func(context.Context) (*websocket.Conn, error) {
			dials.Add(1)
			// The control reaches the real boundary without any provider socket.
			return nil, errors.New("synthetic upstream dial stop")
		}, `{"type":"receipt"}`, func(context.Context, string, int, int) (AudioLease, error) {
			t.Error("admission fixture unexpectedly renewed")
			return AudioLease{}, errors.New("synthetic renewal stop")
		}, now+policySeconds)
	return handler, token, dials, completions
}

func runContinuousClaim(handler http.Handler, token string, ctx context.Context) *httptest.ResponseRecorder {
	request := httptest.NewRequest("GET", "https://ingress.example/v2/audio", nil).WithContext(ctx)
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

// Faithful broker HTTP results compose with the actual Go handler/callback.
// Actual proxy fixtures separately establish custody/DO clock fencing.
func TestContinuousClaimRequiresExactBrokerAcknowledgementBeforeUpstreamDial(t *testing.T) {
	positive := `{"allowed":true}`
	cases := []struct {
		name   string
		status int
		body   string
		allow  bool
	}{
		{"expired_after_custody", 409, `{"allowed":false}`, false},
		{"unknown_dispatch_selection", 503, `{"error":"allowance_dispatch_unavailable"}`, false},
		{"empty_200", 200, "", false},
		{"missing_allowed", 200, `{}`, false},
		{"negative_200", 200, `{"allowed":false}`, false},
		{"null_allowed", 200, `{"allowed":null}`, false},
		{"string_allowed", 200, `{"allowed":"true"}`, false},
		{"numeric_allowed", 200, `{"allowed":1}`, false},
		{"unknown_field", 200, `{"allowed":true,"secret":"synthetic-private-body"}`, false},
		{"wrong_key_case", 200, `{"Allowed":true}`, false},
		{"duplicate_positive", 200, `{"allowed":true,"allowed":true}`, false},
		{"duplicate_negative_then_positive", 200, `{"allowed":false,"allowed":true}`, false},
		{"array", 200, `[true]`, false},
		{"boolean", 200, `true`, false},
		{"truncated", 200, `{"allowed":true`, false},
		{"trailing_json", 200, positive + `{}`, false},
		{"trailing_text", 200, positive + " trailing", false},
		{"oversized_whitespace", 200, positive + strings.Repeat(" ", brokerClaimMaximumBytes+1-len(positive)), false},
		{"invalid_utf8", 200, positive + string([]byte{0xff}), false},
		{"valid_positive_control", 200, positive, true},
		{"exact_maximum_bytes", 200, strings.Repeat(" ", brokerClaimMaximumBytes-len(positive)) + positive, true},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			var callbacks atomic.Int32
			var expectedToken atomic.Value
			broker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				callbacks.Add(1)
				if r.Method != "POST" || r.URL.Path != "/internal/private/claim" || r.Header.Get("Authorization") != "Bearer "+syntheticBrokerSecret ||
					r.Header.Get("Content-Type") != "application/json" || r.Header.Get("Cache-Control") != "no-store" {
					t.Error("wrong fixed metadata endpoint/authority")
				}
				var fields map[string]any
				decoder := json.NewDecoder(r.Body)
				if decoder.Decode(&fields) != nil || decoder.Decode(new(any)) != io.EOF || len(fields) != 1 || fields["capability"] != expectedToken.Load() {
					t.Error("changed original capability or extra disclosure")
				}
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer broker.Close()
			handler, capability, dials, completions := continuousClaimFixture(t, NewBrokerClaim(broker.Client(), broker.URL, syntheticBrokerSecret))
			expectedToken.Store(capability)
			response := runContinuousClaim(handler, capability, context.Background())
			if callbacks.Load() != 1 {
				t.Fatal("claim skipped or retried", callbacks.Load())
			}
			wantStatus, wantDials := http.StatusConflict, int32(0)
			if test.allow {
				wantStatus, wantDials = http.StatusServiceUnavailable, 1
			}
			if response.Code != wantStatus || dials.Load() != wantDials || completions.Load() != wantDials || response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("claim boundary: status=%d dial=%d complete=%d", response.Code, dials.Load(), completions.Load())
			}
			if strings.Contains(response.Body.String(), "receipt") || strings.Contains(response.Body.String(), "synthetic-private-body") {
				t.Fatal("unverified admission exposed receipt or broker prose")
			}
		})
	}
}

func TestContinuousClaimTimeoutAndLateAcknowledgementCannotResumeDial(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			started, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var callbacks atomic.Int32
			broker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				callbacks.Add(1)
				if phase == "body" {
					w.WriteHeader(200)
					_, _ = w.Write([]byte(`{"allowed":`))
					w.(http.Flusher).Flush()
				}
				close(started)
				<-release
				if phase == "body" {
					_, _ = w.Write([]byte(`true}`))
				} else {
					_, _ = w.Write([]byte(`{"allowed":true}`))
				}
				close(finished)
			}))
			defer broker.Close()
			// A short disposable deadline exercises the real HTTP wait; the production
			// default remains three seconds.
			client := broker.Client()
			client.Timeout = 500 * time.Millisecond
			handler, token, dials, completions := continuousClaimFixture(t, NewBrokerClaim(client, broker.URL, syntheticBrokerSecret))
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { done <- runContinuousClaim(handler, token, context.Background()) }()
			select {
			case <-started:
			case <-time.After(3 * time.Second):
				close(release)
				t.Fatal("callback did not start")
			}
			var response *httptest.ResponseRecorder
			select {
			case response = <-done:
			case <-time.After(2 * time.Second):
				close(release)
				t.Fatal("claim waited for policy horizon")
			}
			if response.Code != 409 || dials.Load() != 0 || completions.Load() != 0 || callbacks.Load() != 1 {
				close(release)
				t.Fatal("unknown claim reached upstream or retried")
			}
			close(release)
			<-finished
			if dials.Load() != 0 || completions.Load() != 0 {
				t.Fatal("late acknowledgement resumed paid work")
			}
		})
	}
}

func TestContinuousClaimRejectsRedirectWithoutSecondBrokerRequest(t *testing.T) {
	var callbacks atomic.Int32
	broker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		callbacks.Add(1)
		http.Redirect(w, r, "/other-claim", 307)
	}))
	defer broker.Close()
	handler, token, dials, completions := continuousClaimFixture(t, NewBrokerClaim(broker.Client(), broker.URL, syntheticBrokerSecret))
	response := runContinuousClaim(handler, token, context.Background())
	if response.Code != 409 || callbacks.Load() != 1 || dials.Load() != 0 || completions.Load() != 0 {
		t.Fatal("redirect escaped fixed endpoint or reached upstream")
	}
}

type claimRoundTripper func(*http.Request) (*http.Response, error)

func (transport claimRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return transport(request)
}

type closeFencedClaimBody struct {
	io.Reader
	close func() error
}

func (body closeFencedClaimBody) Close() error { return body.close() }

func TestContinuousClaimRechecksContextAndBodyReleaseBeforeDial(t *testing.T) {
	for _, phase := range []string{"transport_cancels", "close_cancels", "close_fails"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var closes atomic.Int32
			client := &http.Client{Transport: claimRoundTripper(func(request *http.Request) (*http.Response, error) {
				if phase == "transport_cancels" {
					cancel()
				} // Ignores the cancelled context and returns200.
				return &http.Response{StatusCode: 200, Header: make(http.Header), Request: request,
					Body: closeFencedClaimBody{Reader: strings.NewReader(`{"allowed":true}`), close: func() error {
						closes.Add(1)
						if phase == "close_cancels" {
							cancel()
						}
						if phase == "close_fails" {
							return errors.New("synthetic close uncertainty")
						}
						return nil
					}}}, nil
			})}
			handler, token, dials, completions := continuousClaimFixture(t, NewBrokerClaim(client, "https://broker.example.invalid", syntheticBrokerSecret))
			response := runContinuousClaim(handler, token, ctx)
			if response.Code != 409 || dials.Load() != 0 || completions.Load() != 0 || closes.Load() != 1 {
				t.Fatalf("late/unreleased claim: %d dial=%d complete=%d close=%d", response.Code, dials.Load(), completions.Load(), closes.Load())
			}
		})
	}
}

func TestBrokerClaimInvalidFixedConfigurationOrCancelledContextDoesNotSend(t *testing.T) {
	var sends atomic.Int32
	client := &http.Client{Transport: claimRoundTripper(func(*http.Request) (*http.Response, error) { sends.Add(1); return nil, errors.New("must not send") })}
	for _, origin := range []string{"http://broker.example", "https://broker.example/path", "https://user@broker.example", "https://broker.example?", "https://broker.example#fragment", "https://broker.example#"} {
		if NewBrokerClaim(client, origin, syntheticBrokerSecret)(context.Background(), "synthetic-capability") == nil {
			t.Fatal("invalid origin", origin)
		}
	}
	for _, token := range []string{"", strings.Repeat("x", 4097)} {
		if NewBrokerClaim(client, "https://broker.example", syntheticBrokerSecret)(context.Background(), token) == nil {
			t.Fatal("unbounded capability")
		}
	}
	if NewBrokerClaim(nil, "https://broker.example", syntheticBrokerSecret)(context.Background(), "synthetic-capability") == nil ||
		NewBrokerClaim(client, "https://broker.example", "short")(context.Background(), "synthetic-capability") == nil {
		t.Fatal("missing client/authority")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if NewBrokerClaim(client, "https://broker.example", syntheticBrokerSecret)(ctx, "synthetic-capability") == nil || sends.Load() != 0 {
		t.Fatal("cancelled/invalid claim dispatched")
	}
}
