package gateway

import (
	"context"
	"testing"
	"time"
)

func TestAudioGrowthWakeSerializesAndDoesNotRetryStaleNotification(t *testing.T) {
	a := &audioAuthority{limit: 960000, expires: time.Now().Unix() + 60, growth: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered, release, selected, done := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan error, 1)
	calls := 0
	go func() {
		done <- a.maintain(ctx, "original", func(call context.Context, _ string, sequence, seconds int) (AudioLease, error) {
			calls++
			if calls != 1 || sequence != 1 || seconds != 60 {
				t.Error("growth replayed or changed cumulative allowance")
			}
			deadline, _ := call.Deadline()
			a.mu.Lock()
			prior := a.expires
			a.mu.Unlock()
			if deadline.After(time.Unix(prior, 0).Add(-audioMessageTimeout)) {
				t.Error("renewal borrowed notice margin")
			}
			close(entered)
			<-release
			defer close(selected)
			return AudioLease{true, sequence, time.Now().Unix() + 60, seconds * 32000}, nil
		}, func() error { return nil })
	}()
	if !a.consume(640, time.Now().Unix()) {
		t.Fatal("first bounded PCM denied")
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		cancel()
		t.Fatal("first PCM did not wake renewal")
	}
	// This queues an advisory wake while the first request is still pending.
	if !a.consume(640, time.Now().Unix()) {
		t.Fatal("prior paid audio denied")
	}
	close(release)
	<-selected
	time.Sleep(80 * time.Millisecond)
	cancel()
	<-done
	if calls != 1 || a.sequence != 1 || a.limit != 1920000 {
		t.Fatal("queued wake retried selected authority")
	}
}

func TestAudioGrowthForecastRetainsSessionCeilingAndDoesNotGrowOnIdle(t *testing.T) {
	a := &audioAuthority{limit: 960000, expires: time.Now().Unix() + 60, growth: make(chan struct{}, 1)}
	if !a.consume(0, time.Now().Unix()) {
		t.Fatal("idle accounting denied")
	}
	select {
	case <-a.growth:
		t.Fatal("idle grew byte authority")
	default:
	}
	a.bytes = 1490 * audioBytesPerSecond
	a.limit = audioSessionMaxBytes
	if !a.consume(640, time.Now().Unix()) {
		t.Fatal("within-cap tail denied")
	}
	_, seconds := a.request()
	if seconds != 1500 {
		t.Fatal("forecast exceeded session ceiling")
	}
	select {
	case <-a.growth:
		t.Fatal("maximum session kept requesting growth")
	default:
	}
}

func TestAudioLateRenewalCannotPublishAfterParentCancellation(t *testing.T) {
	a := &audioAuthority{limit: 960000, expires: time.Now().Unix() + 60, growth: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	notified := false
	if !a.consume(640, time.Now().Unix()) {
		t.Fatal("first PCM denied")
	}
	_ = a.maintain(ctx, "original", func(context.Context, string, int, int) (AudioLease, error) {
		cancel()
		return AudioLease{true, 1, time.Now().Unix() + 60, 1920000}, nil
	}, func() error { notified = true; return context.Canceled })
	if !notified || a.sequence != 0 || a.limit != 960000 {
		t.Fatal("late reply expanded prior authority")
	}
}

func TestAudioExpiredRenewalDoesNotAttemptFailureNotification(t *testing.T) {
	a := &audioAuthority{limit: 960000, expires: time.Now().Unix() + 60, growth: make(chan struct{}, 1)}
	if !a.consume(640, time.Now().Unix()) {
		t.Fatal("first PCM denied")
	}
	notified := false
	_ = a.maintain(context.Background(), "original", func(context.Context, string, int, int) (AudioLease, error) {
		a.mu.Lock()
		a.expires = time.Now().Unix()
		a.mu.Unlock()
		return AudioLease{true, 1, time.Now().Unix() + 60, 1920000}, nil
	}, func() error { notified = true; return nil })
	if notified || a.sequence != 0 || a.limit != 960000 {
		t.Fatal("expired authority wrote notice or expanded")
	}
}
