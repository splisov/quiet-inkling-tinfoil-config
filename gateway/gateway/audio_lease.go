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

const audioBytesPerSecond = 32000
const audioSessionMaxBytes = 1500 * audioBytesPerSecond

type audioAuthority struct {
	mu                     sync.Mutex
	bytes, limit, sequence int
	expires                int64
}

func (a *audioAuthority) consume(n int, now int64) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	if now >= a.expires || a.bytes+n > a.limit {
		return false
	}
	a.bytes += n
	return true
}
func (a *audioAuthority) request() (int, int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	// Reserve 30 seconds ahead of captured audio, rounding up to whole quanta.
	seconds := ((a.bytes + 30*audioBytesPerSecond + 30*audioBytesPerSecond - 1) / (30 * audioBytesPerSecond)) * 30
	if seconds > 1500 {
		seconds = 1500
	}
	if seconds*audioBytesPerSecond < a.limit {
		seconds = a.limit / audioBytesPerSecond
	}
	return a.sequence + 1, seconds
}
func (a *audioAuthority) accept(lease AudioLease, sequence, seconds int, now int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !lease.Allowed || sequence != a.sequence+1 || lease.Sequence != sequence || lease.ExpiresAt <= now || lease.ExpiresAt > now+60 || lease.MaxAudioBytes != seconds*audioBytesPerSecond || lease.MaxAudioBytes < a.limit || lease.MaxAudioBytes > audioSessionMaxBytes || now >= a.expires {
		return errors.New("invalid audio lease")
	}
	a.sequence = sequence
	a.expires = lease.ExpiresAt
	a.limit = lease.MaxAudioBytes
	return nil
}
func (a *audioAuthority) maintain(ctx context.Context, token string, renew RenewAudio, notify func() error) error {
	a.mu.Lock()
	first := time.Until(time.Unix(a.expires-25, 0))
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
			ticker.Reset(20 * time.Second)
			sequence, seconds := a.request()
			// Bounded call finishes well inside the preceding 60-second lease. Any
			// ambiguous result fails closed; never grant ourselves its possible coverage.
			call, cancel := context.WithTimeout(ctx, 3*time.Second)
			lease, err := renew(call, token, sequence, seconds)
			cancel()
			if err == nil {
				err = a.accept(lease, sequence, seconds, time.Now().Unix())
			}
			a.mu.Lock()
			ending := a.bytes >= 1470*audioBytesPerSecond || a.expires-time.Now().Unix() <= 25
			expires := a.expires
			a.mu.Unlock()
			if err != nil || ending {
				// Stop gracefully while there is still previously paid authority to drain
				// captured frames and obtain a final. Failure to finish still expires closed.
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
}
