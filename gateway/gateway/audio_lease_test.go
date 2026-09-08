package gateway

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestAudioLeaseReservesAheadWithoutChargingIdleHeartbeats(t *testing.T) {
	a := &audioAuthority{limit: 960000, expires: 1060}
	seq, seconds := a.request()
	if seq != 1 || seconds != 30 {
		t.Fatalf("idle reservation %d %d", seq, seconds)
	}
	if !a.consume(32000, 1001) {
		t.Fatal("initial authority denied")
	}
	seq, seconds = a.request()
	if seconds != 60 {
		t.Fatalf("no capture headroom: %d", seconds)
	}
	if err := a.accept(AudioLease{true, seq, 1080, 1920000}, seq, seconds, 1020); err != nil {
		t.Fatal(err)
	}
	// Crossing the old 30-second boundary consumes the existing connection's
	// increased cumulative allowance, not a new request or reset byte counter.
	if !a.consume(960000, 1021) || a.bytes != 992000 {
		t.Fatal("continuous bytes lost")
	}
	if a.consume(1920000, 1022) {
		t.Fatal("overspend")
	}
	if a.consume(2, 1075) {
		t.Fatal("expired lease")
	}
}
func TestAudioLeaseRejectsRollbackReplayAndLateRenewal(t *testing.T) {
	cases := []AudioLease{{true, 1, -1 << 63, 1920000}, {true, 0, 1080, 1920000}, {true, 1, 1086, 1920000}, {true, 1, 1080, 960000}, {false, 1, 1080, 1920000}}
	for _, lease := range cases {
		a := &audioAuthority{limit: 960000, expires: 1060}
		if a.accept(lease, 1, 60, 1020) == nil {
			t.Fatalf("accepted %+v", lease)
		}
	}
	a := &audioAuthority{limit: 960000, expires: 1020}
	if a.accept(AudioLease{true, 1, 1080, 1920000}, 1, 60, 1020) == nil {
		t.Fatal("resurrected expired execution")
	}
}

func TestFailedRenewalWarnsBeforePriorAuthorityEnds(t *testing.T) {
	a := &audioAuthority{limit: 960000, expires: time.Now().Unix() + 25}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	warned := false
	err := a.maintain(ctx, "original", func(context.Context, string, int, int) (AudioLease, error) { return AudioLease{}, errors.New("denied") }, func() error { warned = true; cancel(); return nil })
	if !warned || err == nil || a.limit != 960000 || a.sequence != 0 {
		t.Fatal("denial extended authority or failed to warn")
	}
}

func TestAudioLeaseClockSkewNeverExtendsBrokerAuthority(t *testing.T) {
	const brokerNow int64 = 1020
	for skew := int64(-5); skew <= 5; skew++ {
		enclaveNow := brokerNow - skew
		a := &audioAuthority{limit: 960000, expires: 1060 - audioClockSkewSeconds}
		if err := a.accept(AudioLease{true, 1, brokerNow + 60, 1920000}, 1, 60, enclaveNow); err != nil {
			t.Fatalf("skew %d rejected: %v", skew, err)
		}
		// At broker expiry, the enclave clock equals broker time minus skew. The
		// local execution deadline must already have passed, never extend the lease.
		if a.consume(2, brokerNow+60-skew) {
			t.Fatalf("skew %d outlived broker", skew)
		}
		if a.expires != brokerNow+60-audioClockSkewSeconds {
			t.Fatal("missing conservative margin")
		}
	}
	a := &audioAuthority{limit: 960000, expires: 1060}
	if a.accept(AudioLease{true, 1, brokerNow + 60, 1920000}, 1, 60, brokerNow-6) == nil {
		t.Fatal("future clock skew outside bound accepted")
	}
}
