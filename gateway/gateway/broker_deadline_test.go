package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAudioClaimAdmitsBoundedMetadataWaitBeyondLegacyThreeSeconds(t *testing.T) {
	var calls atomic.Int32
	broker := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-time.After(3100 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write([]byte(`{"allowed":true}`))
	}))
	defer broker.Close()
	handler, token, dials, completions := continuousClaimFixture(t, NewBrokerClaim(broker.Client(), broker.URL, syntheticBrokerSecret))
	response := runContinuousClaim(handler, token, context.Background())
	if response.Code != http.StatusServiceUnavailable || dials.Load() != 1 || completions.Load() != 1 || calls.Load() != 1 {
		t.Fatalf("bounded claim was not selected exactly once: status=%d dials=%d completions=%d calls=%d", response.Code, dials.Load(), completions.Load(), calls.Load())
	}
}

func TestAudioStartupUsesInitialAuthorityDeadlineBeforeAnyProviderDial(t *testing.T) {
	var observed time.Time
	claim := func(ctx context.Context, _ string) error { observed, _ = ctx.Deadline(); return context.Canceled }
	handler, token, dials, _ := continuousClaimFixture(t, claim)
	response := runContinuousClaim(handler, token, context.Background())
	// Fixture signs a 60s lease; its policy lifetime is 300s. Startup must use
	// the earlier initial lease minus the unchanged 5s clock-skew envelope.
	remaining := time.Until(observed)
	if response.Code != 409 || dials.Load() != 0 || remaining < 53*time.Second || remaining > 56*time.Second {
		t.Fatalf("startup escaped initial authority: status=%d dials=%d remaining=%s", response.Code, dials.Load(), remaining)
	}
}

func TestAudioStartupRejectsClaimAcknowledgementAfterParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handler, token, dials, _ := continuousClaimFixture(t, func(context.Context, string) error { cancel(); return nil })
	response := runContinuousClaim(handler, token, ctx)
	if response.Code != 409 || dials.Load() != 0 {
		t.Fatalf("late claim reached provider: status=%d dials=%d", response.Code, dials.Load())
	}
}

func TestAudioStartupRefusesAlreadyEndingAuthorityBeforeProviderDial(t *testing.T) {
	handler, token, dials, _ := continuousClaimFixtureWithLease(t, func(context.Context, string) error { time.Sleep(7 * time.Second); return nil }, 36)
	response := runContinuousClaim(handler, token, context.Background())
	if response.Code != 409 || dials.Load() != 0 {
		t.Fatalf("ending startup reached provider: status=%d dials=%d", response.Code, dials.Load())
	}
}

func TestAudioStartupCannotOutliveOriginalCapability(t *testing.T) {
	var observed time.Time
	handler, token, dials, _ := continuousClaimFixtureWithAuthority(t, func(ctx context.Context, _ string) error {
		observed, _ = ctx.Deadline()
		time.Sleep(4100 * time.Millisecond)
		return nil
	}, 60, 3)
	response := runContinuousClaim(handler, token, context.Background())
	if response.Code != 409 || dials.Load() != 0 || time.Now().Before(observed) {
		t.Fatalf("capability-expired claim reached provider: status=%d dials=%d", response.Code, dials.Load())
	}
}

func TestAudioStartupRefusesAlreadyEndingPolicyBeforeProviderDial(t *testing.T) {
	handler, token, dials, _ := continuousClaimFixtureWithPeriods(t, func(context.Context, string) error { time.Sleep(7 * time.Second); return nil }, 60, 60, 36)
	response := runContinuousClaim(handler, token, context.Background())
	if response.Code != 409 || dials.Load() != 0 {
		t.Fatalf("ending policy reached provider: status=%d dials=%d", response.Code, dials.Load())
	}
}
