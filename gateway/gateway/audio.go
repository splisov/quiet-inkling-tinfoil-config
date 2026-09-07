package gateway

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"quietinkling/private-inference/attested"
)

type Claim func(context.Context, string) error
type Complete func(context.Context, string, bool) error

type audioDial func(context.Context) (*websocket.Conn, error)

// NewAudioHandler only uses a verifier-established downstream transport and the
// model in the owner policy. No caller-supplied URL, prompt or model is accepted.
func NewAudioHandler(policy *attested.Policy, key ed25519.PublicKey, providerKey string, claim Claim, complete Complete, renew RenewAudio) (http.Handler, error) {
	ingress, e := policy.Endpoint("audioIngress")
	if e != nil {
		return nil, e
	}
	upstream, e := policy.Endpoint("audioModel")
	if e != nil || upstream.Model == "" || providerKey == "" || claim == nil || complete == nil || renew == nil {
		return nil, attested.ErrUnavailable
	}
	var verification sync.Mutex
	var cached *http.Client
	var verifiedUntil time.Time
	dial := func(ctx context.Context) (*websocket.Conn, error) {
		verification.Lock()
		if cached == nil || time.Now().After(verifiedUntil) || time.Now().Unix() >= policy.ExpiresAt {
			client, _, err := attested.VerifyHTTP(policy, "audioModel")
			if err != nil {
				verification.Unlock()
				return nil, err
			}
			cached = client
			verifiedUntil = time.Now().Add(4 * time.Minute)
		}
		client := cached
		verification.Unlock()
		if ctx.Err() != nil {
			return nil, attested.ErrUnavailable
		}
		handshake, handshakeDone := context.WithTimeout(ctx, 15*time.Second)
		defer handshakeDone()
		conn, _, e := websocket.Dial(handshake, "wss://"+upstream.Host+"/v1/realtime?model="+url.QueryEscape(upstream.Model), &websocket.DialOptions{
			HTTPClient: client, HTTPHeader: http.Header{"Authorization": []string{"Bearer " + providerKey}}, CompressionMode: websocket.CompressionDisabled,
		})
		if e != nil {
			verification.Lock()
			if cached == client {
				cached = nil
			}
			verification.Unlock()
			return nil, e
		}
		if e = establishAudioSession(ctx, conn, upstream.Model); e != nil {
			conn.CloseNow()
			return nil, e
		}
		return conn, nil
	}
	receipt, _ := json.Marshal(map[string]any{"type": "receipt", "policy_version": policy.Version,
		"ingress_digest": ingress.Digest, "model_digest": upstream.Digest})
	return audioHandlerWithRenewal(ingress.Host, key, claim, complete, dial, string(receipt), renew, policy.ExpiresAt), nil
}

// Matches the vendor's native vLLM handshake. In particular, model is flat and
// a non-final readiness commit starts streaming before the utterance's final
// commit. See confidential-realtime-models a422323/router/openai_compat.go.
func establishAudioSession(ctx context.Context, conn *websocket.Conn, model string) error {
	conn.SetReadLimit(48 * 1024)
	handshake, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	kind, raw, e := conn.Read(handshake)
	var first struct {
		Type string `json:"type"`
	}
	if e != nil || kind != websocket.MessageText || json.Unmarshal(raw, &first) != nil || first.Type != "session.created" {
		return attested.ErrUnavailable
	}
	session, _ := json.Marshal(map[string]string{"type": "session.update", "model": model})
	if e = writeAudioMessage(handshake, conn, websocket.MessageText, session); e != nil {
		return e
	}
	return writeAudioMessage(handshake, conn, websocket.MessageText, []byte(`{"type":"input_audio_buffer.commit"}`))
}

func audioHandler(host string, key ed25519.PublicKey, claim Claim, complete Complete, dial audioDial, receipt string) http.Handler {
	return audioHandlerWithRenewal(host, key, claim, complete, dial, receipt, nil, 0)
}
func audioHandlerWithRenewal(host string, key ed25519.PublicKey, claim Claim, complete Complete, dial audioDial, receipt string, renew RenewAudio, policyExpires int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != "GET" || (r.URL.Path != "/v1/audio" && r.URL.Path != "/v2/audio") || r.URL.RawQuery != "" {
			http.Error(w, "not found", 404)
			return
		}
		token := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		capability, e := VerifyCapability(token, key, host, "audio", time.Now())
		if e != nil {
			http.Error(w, "unavailable", 403)
			return
		}
		continuous := r.URL.Path == "/v2/audio"
		if continuous != (capability.ProtocolVersion == 2) || (continuous && (renew == nil || policyExpires <= time.Now().Unix()+30 || capability.LeaseExpiresAt <= time.Now().Unix()+30)) {
			http.Error(w, "unsupported audio protocol", 403)
			return
		}
		ctx, cancel := attested.RequestContext(r.Context(), capability.ExpiresAt)
		var authority *audioAuthority
		if continuous {
			cancel()
			ctx, cancel = context.WithDeadline(r.Context(), time.Unix(policyExpires, 0))
			authority = &audioAuthority{limit: capability.MaxAudioBytes, expires: capability.LeaseExpiresAt}
		}
		defer cancel()
		// Durable admission consumes this ID before any paid upstream connection.
		if claim(ctx, token) != nil {
			http.Error(w, "already consumed or unavailable", 409)
			return
		}
		successful := false
		defer func() {
			cleanup, done := context.WithTimeout(context.Background(), 3*time.Second)
			defer done()
			_ = complete(cleanup, token, successful)
		}()
		upstream, e := dial(ctx)
		if e != nil {
			http.Error(w, "unavailable", 503)
			return
		}
		defer upstream.CloseNow()
		upstream.SetReadLimit(48 * 1024)
		if continuous {
			upstream.SetReadLimit(256 * 1024)
		}
		downstream, e := websocket.Accept(w, r, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if e != nil {
			return
		}
		defer downstream.CloseNow()
		downstream.SetReadLimit(4096)
		wireReceipt := receipt
		if continuous {
			var fields map[string]any
			if json.Unmarshal([]byte(receipt), &fields) != nil {
				return
			}
			fields["protocol_version"] = 2
			raw, _ := json.Marshal(fields)
			wireReceipt = string(raw)
		}
		if wireReceipt != "" {
			if writeAudioMessage(ctx, downstream, websocket.MessageText, []byte(wireReceipt)) != nil {
				return
			}
		}
		// One model-scoped session per connection, established by the verified dialer.
		committed := new(atomic.Bool)
		failures := make(chan error, 3)
		loops := 2
		if authority != nil {
			loops++
			go func() {
				failures <- authority.maintain(ctx, token, renew, func() error { return writeEvent(ctx, downstream, "notice", "authorization_ending") })
			}()
		}
		go func() {
			failures <- pumpAudioWithAuthority(ctx, downstream, upstream, capability.MaxAudioBytes, committed, authority)
		}()
		go func() { failures <- pumpText(ctx, upstream, downstream, committed) }()
		first := <-failures
		successful = first == nil
		if first != nil {
			writeEvent(ctx, downstream, "error", "")
		}
		cancel()
		upstream.CloseNow()
		downstream.CloseNow()
		// Joining both loops bounds memory lifetime and prevents a runaway writer.
		for i := 1; i < loops; i++ {
			<-failures
		}
	})
}

func pumpAudio(ctx context.Context, from, to *websocket.Conn, limit int, committed *atomic.Bool) error {
	return pumpAudioWithAuthority(ctx, from, to, limit, committed, nil)
}
func pumpAudioWithAuthority(ctx context.Context, from, to *websocket.Conn, limit int, committed *atomic.Bool, authority *audioAuthority) error {
	bytes := 0
	finished := false
	for {
		// An idle client cannot hold a paid GPU connection indefinitely with
		// zero-byte lease heartbeats. Pause finishes; Resume is a deliberate new arm.
		readContext, readDone := context.WithTimeout(ctx, 10*time.Second)
		kind, payload, e := from.Read(readContext)
		readDone()
		if e != nil {
			return e
		}
		if finished {
			return errors.New("audio after commit")
		}
		if kind == websocket.MessageBinary {
			if len(payload) == 0 || len(payload) > 3200 || len(payload)%2 != 0 || (authority == nil && bytes+len(payload) > limit) {
				clear(payload)
				return errors.New("audio limit")
			}
			if authority != nil && !authority.consume(len(payload), time.Now().Unix()) {
				clear(payload)
				return errors.New("audio lease exhausted")
			}
			bytes += len(payload)
			frame := map[string]any{"type": "input_audio_buffer.append", "audio": base64.StdEncoding.EncodeToString(payload)}
			clear(payload)
			raw, _ := json.Marshal(frame)
			e = writeAudioMessage(ctx, to, websocket.MessageText, raw)
			clear(raw)
			if e != nil {
				return e
			}
		} else {
			var event struct {
				Type string `json:"type"`
			}
			decoder := json.NewDecoder(strings.NewReader(string(payload)))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&event) != nil || decoder.Decode(new(any)) != io.EOF || event.Type != "finish" || bytes == 0 {
				return errors.New("invalid commit")
			}
			finished = true
			committed.Store(true)
			if e := writeAudioMessage(ctx, to, websocket.MessageText, []byte(`{"type":"input_audio_buffer.commit","final":true}`)); e != nil {
				return e
			}
			// Continue reading until the response loop accepts a final or cancellation.
		}
	}
}

func pumpText(ctx context.Context, from, to *websocket.Conn, committed *atomic.Bool) error {
	text := ""
	events := 0
	for {
		kind, raw, e := from.Read(ctx)
		if e != nil {
			return e
		}
		if kind != websocket.MessageText {
			return errors.New("invalid response")
		}
		events++
		if events > 100000 {
			return errors.New("event limit")
		}
		var event struct {
			Type  string `json:"type"`
			Delta string `json:"delta"`
			Text  string `json:"text"`
		}
		if json.Unmarshal(raw, &event) != nil {
			return errors.New("invalid response")
		}
		switch event.Type {
		case "session.created", "session.updated", "input_audio_buffer.committed":
		case "transcription.delta":
			text += event.Delta
			if len(text) > 192000 {
				return errors.New("text limit")
			}
			if e := writeEvent(ctx, to, "partial", text); e != nil {
				return e
			}
		case "transcription.done":
			if !committed.Load() {
				return errors.New("final before commit")
			}
			if event.Text != "" {
				text = event.Text
			}
			if strings.TrimSpace(text) == "" || len(text) > 192000 {
				return errors.New("empty final")
			}
			return writeEvent(ctx, to, "final", text)
		default:
			return errors.New("unexpected response")
		}
	}
}
func writeEvent(ctx context.Context, conn *websocket.Conn, kind, text string) error {
	raw, _ := json.Marshal(map[string]string{"type": kind, "text": text})
	return writeAudioMessage(ctx, conn, websocket.MessageText, raw)
}

// A stalled peer cannot hold a lease, cancellation, or finalization open until
// policy expiry. coder/websocket closes the connection when this deadline fires.
func writeAudioMessage(parent context.Context, conn *websocket.Conn, kind websocket.MessageType, raw []byte) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	return conn.Write(ctx, kind, raw)
}
