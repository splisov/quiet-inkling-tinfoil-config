package gateway

import (
	"context"
	"errors"
	"sync"
	"time"
)

// AudioLease is metadata only. A renewal extends authority for the SAME paid
// connection. Neither retries nor heartbeats open a new provider connection.
type AudioLease struct {
	Allowed       bool  `json:"allowed"`
	Sequence      int   `json:"sequence"`
	ExpiresAt     int64 `json:"lease_expires_at"`
	MaxAudioBytes int   `json:"max_audio_bytes"`
}
type RenewAudio func(context.Context, string, int, int) (AudioLease, error)

// Broker and enclave clocks can differ. Never use the tolerance to extend paid
// authority: subtract the whole envelope from the broker's absolute expiry.
// If enclave time lags broker time by at most this margin, the resulting local
// deadline occurs no later than the broker's own concurrency expiry.
const audioClockSkewSeconds int64 = 5

const audioBytesPerSecond = 32000
const audioSessionMaxBytes = 1500 * audioBytesPerSecond
const brokerAudioRenewTimeout = 60 * time.Second
const audioMessageTimeout = 3 * time.Second
const audioEndingHorizonSeconds int64 = 25

type audioAuthority struct {
	mu                     sync.Mutex
	bytes, limit, sequence int
	expires                int64
	growth                 chan struct{}
}

func (a *audioAuthority) consume(n int, now int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if now >= a.expires || a.bytes+n > a.limit {
		return false
	}
	a.bytes += n
	if a.growth != nil && audioReservationSeconds(a.bytes, a.limit)*audioBytesPerSecond > a.limit {
		select {
		case a.growth <- struct{}{}:
		default:
		}
	}
	return true
}
func (a *audioAuthority) request() (int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Reserve 30 seconds ahead of captured audio, rounding up to whole quanta.
	return a.sequence + 1, audioReservationSeconds(a.bytes, a.limit)
}
func audioReservationSeconds(bytes, limit int) int {
	seconds := ((bytes + 30*audioBytesPerSecond + 30*audioBytesPerSecond - 1) / (30 * audioBytesPerSecond)) * 30
	if seconds > 1500 {
		seconds = 1500
	}
	if seconds*audioBytesPerSecond < limit {
		seconds = limit / audioBytesPerSecond
	}
	return seconds
}
func (a *audioAuthority) paidContext(parent context.Context) (context.Context, context.CancelFunc) {
	a.mu.Lock()
	expires := a.expires
	a.mu.Unlock()
	return context.WithDeadline(parent, time.Unix(expires, 0))
}
func (a *audioAuthority) startupHeadroom(parent context.Context, seconds int64) bool {
	a.mu.Lock()
	expires := time.Unix(a.expires, 0)
	a.mu.Unlock()
	if deadline, bounded := parent.Deadline(); bounded && deadline.Before(expires) {
		expires = deadline
	}
	return parent.Err() == nil && time.Until(expires) > time.Duration(seconds)*time.Second
}
func (a *audioAuthority) accept(lease AudioLease, sequence, seconds int, now int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !lease.Allowed || sequence != a.sequence+1 || lease.Sequence != sequence || lease.ExpiresAt <= now+audioClockSkewSeconds || lease.ExpiresAt > now+60+audioClockSkewSeconds || lease.MaxAudioBytes != seconds*audioBytesPerSecond || lease.MaxAudioBytes < a.limit || lease.MaxAudioBytes > audioSessionMaxBytes || now >= a.expires {
		return errors.New("invalid audio lease")
	}
	a.sequence = sequence
	a.expires = lease.ExpiresAt - audioClockSkewSeconds
	a.limit = lease.MaxAudioBytes
	return nil
}
func (a *audioAuthority) maintain(ctx context.Context, token string, renew RenewAudio, notify func() error) error {
	a.mu.Lock()
	if a.growth == nil {
		a.growth = make(chan struct{}, 1)
	}
	growth := a.growth
	first := time.Until(time.Unix(a.expires-audioEndingHorizonSeconds, 0))
	if audioReservationSeconds(a.bytes, a.limit)*audioBytesPerSecond > a.limit {
		first = 0
	}
	a.mu.Unlock()
	if first > 20*time.Second {
		first = 20 * time.Second
	}
	if first < 0 {
		first = 0
	}
	ticker := time.NewTimer(first)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		case <-growth:
			a.mu.Lock()
			needed := audioReservationSeconds(a.bytes, a.limit)*audioBytesPerSecond > a.limit
			a.mu.Unlock()
			if !needed {
				continue
			} // A selected reply can make a queued wake stale.
		}
		ticker.Reset(20 * time.Second)
		sequence, seconds := a.request()
		// The nominal metadata budget cannot borrow from the prospective lease.
		// Reserve the existing event-write budget inside prior paid authority.
		a.mu.Lock()
		priorExpiry := a.expires
		a.mu.Unlock()
		deadline := time.Now().Add(brokerAudioRenewTimeout)
		latest := time.Unix(priorExpiry, 0).Add(-audioMessageTimeout)
		if latest.Before(deadline) {
			deadline = latest
		}
		call, cancel := context.WithDeadline(ctx, deadline)
		lease, err := renew(call, token, sequence, seconds)
		timely := audioContextCurrent(call)
		cancel()
		if err == nil && !timely {
			err = errors.New("late audio lease")
		}
		if err == nil {
			err = a.accept(lease, sequence, seconds, time.Now().Unix())
		}
		a.mu.Lock()
		ending := a.bytes >= 1470*audioBytesPerSecond || a.expires-time.Now().Unix() <= audioEndingHorizonSeconds
		expires := a.expires
		a.mu.Unlock()
		if err != nil || ending {
			// Stop gracefully while there is still previously paid authority to drain
			// captured frames and obtain a final. Failure to finish still expires closed.
			if time.Now().Unix() >= expires {
				return errors.New("audio lease expired")
			}
			if notify() != nil {
				return errors.New("audio authorization ending")
			}
			timer := time.NewTimer(time.Until(time.Unix(expires, 0)))
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-timer.C:
				return errors.New("audio lease expired")
			}
		}
	}
}

func audioContextCurrent(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}
	deadline, bounded := ctx.Deadline()
	return bounded && time.Now().Before(deadline)
}
