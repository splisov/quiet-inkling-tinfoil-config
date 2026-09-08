package gateway

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"github.com/coder/websocket"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAudioCapabilityCannotChangeOperationOrExtendItsLimit(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Unix(1800000000, 0)
	cap := Capability{ID: "12345678-1234-1234-1234-123456789abc", Audience: "ingress.example", Operation: "audio", ExpiresAt: now.Unix() + 60, MaxAudioBytes: 960000}
	raw, _ := json.Marshal(cap)
	p := base64.RawURLEncoding.EncodeToString(raw)
	token := p + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte("quiet-inkling-capability-v1."+p)))
	if _, e := VerifyCapability(token, pub, "ingress.example", "audio", now); e != nil {
		t.Fatal(e)
	}
	if _, e := VerifyCapability(token, pub, "ingress.example", "text", now); e == nil {
		t.Fatal("audio capability used for text")
	}
	if _, e := VerifyCapability(token, pub, "ingress.example", "audio", now.Add(time.Minute)); e == nil {
		t.Fatal("expired capability accepted")
	}
}

func TestAudioSessionForwardsBoundedPCMAndOneFinal(t *testing.T) {
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"session.created"}`))
		_, setup, e := c.Read(r.Context())
		if e != nil || string(setup) != `{"model":"approved-audio","type":"session.update"}` {
			t.Errorf("incorrect native session update: %s", setup)
			return
		}
		_, ready, e := c.Read(r.Context())
		if e != nil || string(ready) != `{"type":"input_audio_buffer.commit"}` {
			t.Errorf("missing readiness commit: %s", ready)
			return
		}
		for {
			_, raw, e := c.Read(r.Context())
			if e != nil {
				return
			}
			var event map[string]any
			_ = json.Unmarshal(raw, &event)
			if event["type"] == "input_audio_buffer.commit" {
				_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"transcription.delta","delta":"Two of Disks"}`))
				_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"transcription.done"}`))
				_, _, _ = c.Read(r.Context())
				return
			}
		}
	}))
	defer provider.Close()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	calls := 0
	claim := func(context.Context, string) error { calls++; return nil }
	dial := func(ctx context.Context) (*websocket.Conn, error) {
		c, _, e := websocket.Dial(ctx, "ws"+strings.TrimPrefix(provider.URL, "http"), &websocket.DialOptions{HTTPClient: provider.Client()})
		if e == nil {
			e = establishAudioSession(ctx, c, "approved-audio")
		}
		return c, e
	}
	ingress := httptest.NewServer(audioHandler("ingress.example", pub, claim, func(context.Context, string, bool) error { return nil }, dial, ""))
	defer ingress.Close()
	cap := Capability{ID: "12345678-1234-1234-1234-123456789abc", Audience: "ingress.example", Operation: "audio", ExpiresAt: time.Now().Unix() + 60, MaxAudioBytes: 3200}
	raw, _ := json.Marshal(cap)
	p := base64.RawURLEncoding.EncodeToString(raw)
	token := p + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte("quiet-inkling-capability-v1."+p)))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	c, _, e := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ingress.URL, "http")+"/v1/audio", &websocket.DialOptions{HTTPClient: ingress.Client(), HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	if e = c.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); e != nil {
		t.Fatal(e)
	}
	if e = c.Write(ctx, websocket.MessageText, []byte(`{"type":"finish"}`)); e != nil {
		t.Fatal(e)
	}
	_, partial, e := c.Read(ctx)
	if e != nil || !strings.Contains(string(partial), `"partial"`) {
		t.Fatalf("partial: %s %v", partial, e)
	}
	_, final, e := c.Read(ctx)
	if e != nil || !strings.Contains(string(final), `"final"`) {
		t.Fatalf("final: %s %v", final, e)
	}
	if calls != 1 {
		t.Fatal("not exactly one admission")
	}
}

func TestContinuousAudioRenewsSameConnectionAcrossOldByteLimit(t *testing.T) {
	var dials atomic.Int32
	var claims atomic.Int32
	var renewals atomic.Int32
	renewed := make(chan struct{})
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		dials.Add(1)
		c, e := websocket.Accept(w, r, nil)
		if e != nil {
			return
		}
		defer c.CloseNow()
		total := 0
		for {
			_, raw, e := c.Read(r.Context())
			if e != nil {
				return
			}
			var event struct {
				Type  string `json:"type"`
				Audio string `json:"audio"`
			}
			if json.Unmarshal(raw, &event) != nil {
				return
			}
			if event.Type == "input_audio_buffer.append" {
				pcm, _ := base64.StdEncoding.DecodeString(event.Audio)
				total += len(pcm)
				clear(pcm)
			}
			if event.Type == "input_audio_buffer.commit" {
				if total != 1024000 {
					t.Errorf("lost audio: %d", total)
				}
				_ = c.Write(r.Context(), websocket.MessageText, []byte(`{"type":"transcription.done","text":"synthetic continuous final"}`))
				return
			}
		}
	}))
	defer provider.Close()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	dial := func(ctx context.Context) (*websocket.Conn, error) {
		c, _, e := websocket.Dial(ctx, "ws"+strings.TrimPrefix(provider.URL, "http"), &websocket.DialOptions{HTTPClient: provider.Client()})
		return c, e
	}
	renew := func(_ context.Context, _ string, sequence, seconds int) (AudioLease, error) {
		renewals.Add(1)
		if sequence != 1 || seconds != 60 {
			t.Errorf("unexpected renewal %d %d", sequence, seconds)
		}
		close(renewed)
		return AudioLease{true, sequence, time.Now().Unix() + 60, seconds * 32000}, nil
	}
	ingress := httptest.NewServer(audioHandlerWithRenewal("ingress.example", pub, func(context.Context, string) error { claims.Add(1); return nil }, func(context.Context, string, bool) error { return nil }, dial, `{"type":"receipt"}`, renew, time.Now().Unix()+120))
	defer ingress.Close()
	cap := Capability{ID: "12345678-1234-1234-1234-123456789abc", Audience: "ingress.example", Operation: "audio", ExpiresAt: time.Now().Unix() + 60, LeaseExpiresAt: time.Now().Unix() + 60, ProtocolVersion: 2, MaxAudioBytes: 960000}
	raw, _ := json.Marshal(cap)
	p := base64.RawURLEncoding.EncodeToString(raw)
	token := p + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte("quiet-inkling-capability-v1."+p)))
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	c, _, e := websocket.Dial(ctx, "ws"+strings.TrimPrefix(ingress.URL, "http")+"/v2/audio", &websocket.DialOptions{HTTPClient: ingress.Client(), HTTPHeader: http.Header{"Authorization": []string{"Bearer " + token}}})
	if e != nil {
		t.Fatal(e)
	}
	defer c.CloseNow()
	_, receipt, e := c.Read(ctx)
	if e != nil || !strings.Contains(string(receipt), `"protocol_version":2`) {
		t.Fatalf("receipt %s %v", receipt, e)
	}
	if e = c.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); e != nil {
		t.Fatal(e)
	}
	frames := 1
	heartbeat := time.NewTicker(time.Second)
	defer heartbeat.Stop()
waitRenewal:
	for {
		select {
		case <-renewed:
			break waitRenewal
		case <-heartbeat.C:
			if e = c.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); e != nil {
				t.Fatal(e)
			}
			frames++
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// Renew response has reached the handler before more than the old allowance is sent.
	for i := frames; i < 320; i++ {
		if e = c.Write(ctx, websocket.MessageBinary, make([]byte, 3200)); e != nil {
			t.Fatal(e)
		}
	}
	if e = c.Write(ctx, websocket.MessageText, []byte(`{"type":"finish"}`)); e != nil {
		t.Fatal(e)
	}
	_, final, e := c.Read(ctx)
	if e != nil || !strings.Contains(string(final), `"final"`) {
		t.Fatalf("final %s %v", final, e)
	}
	if dials.Load() != 1 || claims.Load() != 1 || renewals.Load() != 1 {
		t.Fatalf("execution restarted: %d %d %d", dials.Load(), claims.Load(), renewals.Load())
	}
}

func TestBlockedAudioEventWriteHasItsOwnDeadline(t *testing.T) {
	done := make(chan struct{})
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		<-done
	}))
	defer peer.Close()
	defer close(done)
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(peer.URL, "http"), &websocket.DialOptions{HTTPClient: peer.Client()})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.CloseNow()
	started := time.Now()
	for i := 0; i < 100 && err == nil; i++ {
		err = writeEvent(ctx, conn, "partial", strings.Repeat("x", 192000))
	}
	if err == nil {
		t.Fatal("test failed to saturate socket")
	}
	if time.Since(started) > 8*time.Second {
		t.Fatal("write waited for policy expiry")
	}
}

func TestContinuousAdmissionAllowsBoundedBrokerClockLead(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	const brokerNow int64 = 1800000000
	cap := Capability{ID: "12345678-1234-1234-1234-123456789abc", Audience: "ingress.example", Operation: "audio", ExpiresAt: brokerNow + 60, LeaseExpiresAt: brokerNow + 60, ProtocolVersion: 2, MaxAudioBytes: 960000}
	raw, _ := json.Marshal(cap)
	payload := base64.RawURLEncoding.EncodeToString(raw)
	token := payload + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(priv, []byte("quiet-inkling-capability-v1."+payload)))
	for skew := int64(-5); skew <= 5; skew++ {
		if _, err := VerifyCapability(token, pub, "ingress.example", "audio", time.Unix(brokerNow-skew, 0)); err != nil {
			t.Fatalf("admission skew %d rejected: %v", skew, err)
		}
	}
	if _, err := VerifyCapability(token, pub, "ingress.example", "audio", time.Unix(brokerNow-6, 0)); err == nil {
		t.Fatal("out-of-bound future lease accepted")
	}
}
