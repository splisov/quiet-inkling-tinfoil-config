package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestAudioRenewSupportsBoundedFreshReadsBeyondLegacyBudget(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "POST" || r.URL.Path != "/internal/private/renew" {
			t.Error("changed fixed renewal route")
		}
		select {
		case <-time.After(3100 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"allowed":true,"sequence":1,"lease_expires_at":1080,"max_audio_bytes":1920000}`))
	}))
	defer server.Close()
	lease, err := NewBrokerAudioRenew(server.Client(), server.URL, syntheticBrokerSecret)(context.Background(), "synthetic-original", 1, 60)
	if err != nil || !lease.Allowed || lease.Sequence != 1 || lease.MaxAudioBytes != 1920000 || calls.Load() != 1 {
		t.Fatal("bounded renewal failed or retried")
	}
}

func TestAudioRenewDiscardsLateHeadersAndStreamedBodyWithoutRetry(t *testing.T) {
	for _, phase := range []string{"headers", "body"} {
		t.Run(phase, func(t *testing.T) {
			release := make(chan struct{})
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if phase == "body" {
					_, _ = w.Write([]byte(`{"allowed":`))
					w.(http.Flusher).Flush()
				}
				<-release
				if phase == "body" {
					_, _ = w.Write([]byte(`true,"sequence":1,"lease_expires_at":1080,"max_audio_bytes":1920000}`))
				} else {
					_, _ = w.Write([]byte(`{"allowed":true,"sequence":1,"lease_expires_at":1080,"max_audio_bytes":1920000}`))
				}
			}))
			defer server.Close()
			client := server.Client()
			client.Timeout = 80 * time.Millisecond
			lease, err := NewBrokerAudioRenew(client, server.URL, syntheticBrokerSecret)(context.Background(), "synthetic-original", 1, 60)
			close(release)
			if err == nil || lease.Allowed || calls.Load() != 1 {
				t.Fatal("late renewal published authority or retried")
			}
		})
	}
}

func TestAudioRenewRejectsRedirectAndOversizedBody(t *testing.T) {
	for _, mode := range []string{"redirect", "oversized"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if mode == "redirect" {
					http.Redirect(w, r, "/other", 307)
				} else {
					_, _ = io.WriteString(w, `{"allowed":true}`+strings.Repeat(" ", 4096))
				}
			}))
			defer server.Close()
			lease, err := NewBrokerAudioRenew(server.Client(), server.URL, syntheticBrokerSecret)(context.Background(), "synthetic-original", 1, 60)
			if err == nil || lease.Allowed || calls.Load() != 1 {
				t.Fatal("unbounded renewal or redirected retry")
			}
		})
	}
}

func TestAudioRenewRechecksCancellationAndBodyReleaseBeforePublishingLease(t *testing.T) {
	for _, phase := range []string{"transport_cancels", "close_cancels", "close_fails"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var closes, sends atomic.Int32
			client := &http.Client{Transport: claimRoundTripper(func(request *http.Request) (*http.Response, error) {
				sends.Add(1)
				if phase == "transport_cancels" {
					cancel()
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Request: request,
					Body: closeFencedClaimBody{Reader: strings.NewReader(`{"allowed":true,"sequence":1,"lease_expires_at":1080,"max_audio_bytes":1920000}`), close: func() error {
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
			lease, err := NewBrokerAudioRenew(client, "https://broker.example.invalid", syntheticBrokerSecret)(ctx, "synthetic-original", 1, 60)
			if err == nil || lease.Allowed || lease.MaxAudioBytes != 0 || sends.Load() != 1 || closes.Load() != 1 {
				t.Fatal("late or unreleased renewal published authority or retried")
			}
		})
	}
}
