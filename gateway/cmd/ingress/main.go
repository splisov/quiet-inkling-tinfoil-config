// ingress must run wholly inside the measured workload, behind its attested TLS
// listener. Never deploy this plaintext listener on an ordinary application host.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"quietinkling/private-inference/attested"
	"quietinkling/private-inference/gateway"
	"syscall"
	"time"
)

// Public trust configuration is embedded in the measured binary. Runtime
// environment variables cannot replace the owner, issuer or admission origin.
var policyRoot, capabilityPublicKey, admissionURL string

func main() {
	if len(os.Args) == 2 && os.Args[1] == "public-config" {
		if validateTrust(policyRoot, capabilityPublicKey, admissionURL) != nil {
			os.Exit(1)
		}
		_ = json.NewEncoder(os.Stdout).Encode(map[string]string{"policy_root": policyRoot, "capability_public_key": capabilityPublicKey, "admission_url": admissionURL})
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		client := &http.Client{Timeout: time.Second}
		response, e := client.Get("http://127.0.0.1:8080/healthz")
		if e != nil {
			os.Exit(1)
		}
		response.Body.Close()
		if response.StatusCode != 200 {
			os.Exit(1)
		}
		return
	}
	if run() != nil { // Deliberately no request, credential, body or SDK error logging.
		_, _ = os.Stderr.WriteString("private ingress unavailable\n")
		os.Exit(1)
	}
}
func run() error {
	if e := validateTrust(policyRoot, capabilityPublicKey, admissionURL); e != nil {
		return e
	}
	raw := []byte(os.Getenv("POLICY_ENVELOPE"))
	if len(raw) == 0 {
		var e error
		raw, e = os.ReadFile(os.Getenv("POLICY_FILE"))
		if e != nil {
			return e
		}
	}
	if len(raw) > 48*1024 {
		return attested.ErrUnavailable
	}
	var envelope struct {
		Payload   string `json:"payload"`
		Signature string `json:"signature"`
	}
	if json.Unmarshal(raw, &envelope) != nil {
		return attested.ErrUnavailable
	}
	policy, e := attested.VerifyPolicy(envelope.Payload, envelope.Signature, policyRoot, 1, time.Now())
	if e != nil {
		return e
	}
	key, e := base64.RawURLEncoding.DecodeString(capabilityPublicKey)
	if e != nil || len(key) != ed25519.PublicKeySize {
		return attested.ErrUnavailable
	}
	broker, e := url.Parse(admissionURL)
	secret := os.Getenv("INGRESS_SECRET")
	if e != nil || broker.Scheme != "https" || broker.Host == "" || broker.User != nil || broker.RawQuery != "" || broker.Fragment != "" || (broker.Path != "" && broker.Path != "/") || len(secret) < 32 {
		return attested.ErrUnavailable
	}
	client := &http.Client{Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect refused") }}
	call := func(ctx context.Context, action, token string, benefit bool) error {
		fields := map[string]any{"capability": token}
		if action == "complete" {
			fields["benefit"] = benefit
		}
		body, _ := json.Marshal(fields)
		req, e := http.NewRequestWithContext(ctx, "POST", broker.Scheme+"://"+broker.Host+"/internal/private/"+action, bytes.NewReader(body))
		if e != nil {
			return e
		}
		req.Header.Set("Authorization", "Bearer "+secret)
		req.Header.Set("Content-Type", "application/json")
		resp, e := client.Do(req)
		if e != nil {
			return attested.ErrUnavailable
		}
		defer resp.Body.Close()
		if resp.StatusCode != 200 {
			return attested.ErrUnavailable
		}
		return nil
	}
	claim := func(ctx context.Context, token string) error { return call(ctx, "claim", token, false) }
	complete := func(ctx context.Context, token string, benefit bool) error {
		return call(ctx, "complete", token, benefit)
	}
	// Audio metadata waits have independent finite caps. Legacy text and all
	// completion calls keep the original three-second client above.
	audioClaimClient := &http.Client{Timeout: 30 * time.Second, CheckRedirect: client.CheckRedirect}
	audioRenewClient := &http.Client{Timeout: 60 * time.Second, CheckRedirect: client.CheckRedirect}
	audioClaim := gateway.NewBrokerClaim(audioClaimClient, broker.Scheme+"://"+broker.Host, secret)
	audioRenew := gateway.NewBrokerAudioRenew(audioRenewClient, broker.Scheme+"://"+broker.Host, secret)
	mux := http.NewServeMux()
	enabled := 0
	if _, e := policy.Endpoint("audioIngress"); e == nil {
		audio, e := gateway.NewAudioHandler(policy, ed25519.PublicKey(key), os.Getenv("TINFOIL_API_KEY"), audioClaim, complete, audioRenew)
		if e != nil {
			return e
		}
		mux.Handle("/v1/audio", audio)
		mux.Handle("/v2/audio", audio)
		enabled++
	}
	textEnabled, e := registerText(mux, policy, ed25519.PublicKey(key), claim, complete)
	if e != nil {
		return e
	}
	enabled += textEnabled
	if enabled == 0 {
		return attested.ErrUnavailable
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != "GET" || time.Now().Unix() >= policy.ExpiresAt {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(200)
	})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 3 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 8192,
		ErrorLog: log.New(io.Discard, "", 0), BaseContext: func(net.Listener) context.Context { return ctx }}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	e = server.ListenAndServe()
	if errors.Is(e, http.ErrServerClosed) {
		return nil
	}
	return e
}

func validateTrust(root, issuer, admission string) error {
	for _, value := range []string{root, issuer} {
		raw, e := base64.RawURLEncoding.DecodeString(value)
		if e != nil || len(raw) != ed25519.PublicKeySize {
			return attested.ErrUnavailable
		}
	}
	broker, e := url.Parse(admission)
	if e != nil || broker.Scheme != "https" || broker.Host == "" || broker.User != nil || broker.RawQuery != "" || broker.Fragment != "" || broker.Path != "" {
		return attested.ErrUnavailable
	}
	return nil
}
