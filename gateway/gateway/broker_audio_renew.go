package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"unicode/utf8"

	"quietinkling/private-inference/attested"
)

// NewBrokerAudioRenew requests metadata for the existing connection only.
// Its caller must also impose the preceding lease's conservative deadline.
func NewBrokerAudioRenew(client *http.Client, origin, secret string) RenewAudio {
	broker, err := url.Parse(origin)
	valid := err == nil && broker.Scheme == "https" && broker.Host != "" && broker.User == nil &&
		broker.Path == "" && broker.RawQuery == "" && !broker.ForceQuery && !strings.Contains(origin, "#") && len(secret) >= 32 && client != nil
	var bounded http.Client
	if client != nil {
		bounded = *client
		if bounded.Timeout <= 0 || bounded.Timeout > brokerAudioRenewTimeout {
			bounded.Timeout = brokerAudioRenewTimeout
		}
		bounded.CheckRedirect = func(*http.Request, []*http.Request) error { return attested.ErrUnavailable }
	}
	return func(ctx context.Context, token string, sequence, seconds int) (lease AudioLease, failure error) {
		if !valid || len(token) == 0 || len(token) > 4096 || ctx.Err() != nil || sequence < 1 || seconds < 30 || seconds > 1500 || seconds%30 != 0 {
			return lease, attested.ErrUnavailable
		}
		call, cancel := context.WithTimeout(ctx, brokerAudioRenewTimeout)
		defer cancel()
		body, err := json.Marshal(map[string]any{"capability": token, "sequence": sequence, "audio_seconds": seconds})
		if err != nil {
			return lease, attested.ErrUnavailable
		}
		request, err := http.NewRequestWithContext(call, "POST", origin+"/internal/private/renew", bytes.NewReader(body))
		if err != nil {
			return lease, attested.ErrUnavailable
		}
		request.Header.Set("Authorization", "Bearer "+secret)
		request.Header.Set("Content-Type", "application/json")
		response, err := bounded.Do(request)
		if err != nil {
			return lease, attested.ErrUnavailable
		}
		defer func() {
			if response.Body.Close() != nil || !audioContextCurrent(call) {
				lease = AudioLease{}
				failure = attested.ErrUnavailable
			}
		}()
		if response.StatusCode != http.StatusOK || !audioContextCurrent(call) {
			return lease, attested.ErrUnavailable
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, 4097))
		if err != nil || len(raw) > 4096 || !utf8.Valid(raw) || !audioContextCurrent(call) {
			return lease, attested.ErrUnavailable
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		if decoder.Decode(&lease) != nil || decoder.Decode(new(any)) != io.EOF || !audioContextCurrent(call) {
			return AudioLease{}, attested.ErrUnavailable
		}
		return lease, nil
	}
}
