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
