package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"quietinkling/private-inference/attested"
)

const brokerClaimMaximumBytes = 1024
const brokerClaimTimeout = 30 * time.Second

// NewBrokerClaim consumes only the original capability at the fixed broker.
// A bounded, positive acknowledgement precedes any upstream dial. Uncertainty
// does not release, replace or retry the possibly selected claim.
func NewBrokerClaim(client *http.Client, origin, secret string) Claim {
	broker, err := url.Parse(origin)
	valid := err == nil && broker.Scheme == "https" && broker.Host != "" && broker.User == nil &&
		broker.Path == "" && broker.RawQuery == "" && !broker.ForceQuery && !strings.Contains(origin, "#") && len(secret) >= 32 && client != nil
	var bounded http.Client
	if client != nil {
		bounded = *client
		if bounded.Timeout <= 0 || bounded.Timeout > brokerClaimTimeout {
			bounded.Timeout = brokerClaimTimeout
		}
		bounded.CheckRedirect = func(*http.Request, []*http.Request) error { return attested.ErrUnavailable }
	}
	return func(ctx context.Context, token string) (failure error) {
		if !valid || len(token) == 0 || len(token) > 4096 || ctx.Err() != nil {
			return attested.ErrUnavailable
		}
		call, cancel := context.WithTimeout(ctx, brokerClaimTimeout)
		defer cancel()
		body, err := json.Marshal(map[string]string{"capability": token})
		if err != nil {
			return attested.ErrUnavailable
		}
		request, err := http.NewRequestWithContext(call, "POST", origin+"/internal/private/claim", bytes.NewReader(body))
		if err != nil {
			return attested.ErrUnavailable
		}
		request.Header.Set("Authorization", "Bearer "+secret)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
		request.Header.Set("Cache-Control", "no-store")
		response, err := bounded.Do(request)
		if err != nil {
			return attested.ErrUnavailable
		}
		defer func() {
			if response.Body.Close() != nil || call.Err() != nil {
				failure = attested.ErrUnavailable
			}
		}()
		if response.StatusCode != http.StatusOK || call.Err() != nil {
			return attested.ErrUnavailable
		}
		raw, err := io.ReadAll(io.LimitReader(response.Body, brokerClaimMaximumBytes+1))
		if err != nil || len(raw) > brokerClaimMaximumBytes || !utf8.Valid(raw) || call.Err() != nil {
			return attested.ErrUnavailable
		}
		// Tokens make the one-field object exact, including duplicate-key rejection.
		decoder := json.NewDecoder(bytes.NewReader(raw))
		opening, err := decoder.Token()
		if err != nil || opening != json.Delim('{') {
			return attested.ErrUnavailable
		}
		key, err := decoder.Token()
		if err != nil || key != "allowed" {
			return attested.ErrUnavailable
		}
		var allowed bool
		if decoder.Decode(&allowed) != nil || !allowed {
			return attested.ErrUnavailable
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') || decoder.Decode(new(any)) != io.EOF || call.Err() != nil {
			return attested.ErrUnavailable
		}
		return nil
	}
}
