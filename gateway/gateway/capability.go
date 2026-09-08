package gateway

import (
	"bytes"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"io"
	"quietinkling/private-inference/attested"
	"strings"
	"time"
)

type Capability struct {
	ProtocolVersion int    `json:"protocol_version,omitempty"`
	LeaseExpiresAt  int64  `json:"lease_expires_at,omitempty"`
	LeaseSequence   int    `json:"lease_sequence,omitempty"`
	ID              string `json:"id"`
	AttemptID       string `json:"attempt_id,omitempty"`
	Audience        string `json:"audience"`
	Operation       string `json:"operation"`
	ExpiresAt       int64  `json:"expires_at"`
	MaxAudioBytes   int    `json:"max_audio_bytes"`
}

func VerifyCapability(token string, key ed25519.PublicKey, audience, operation string, now time.Time) (Capability, error) {
	var cap Capability
	parts := strings.Split(token, ".")
	if len(key) != ed25519.PublicKeySize || len(token) > 4096 || len(parts) != 2 {
		return cap, attested.ErrUnavailable
	}
	signature, e := base64.RawURLEncoding.DecodeString(parts[1])
	if e != nil || !ed25519.Verify(key, []byte("quiet-inkling-capability-v1."+parts[0]), signature) {
		return cap, attested.ErrUnavailable
	}
	raw, e := base64.RawURLEncoding.DecodeString(parts[0])
	if e != nil {
		return cap, attested.ErrUnavailable
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&cap) != nil || decoder.Decode(new(any)) != io.EOF || len(cap.ID) != 36 || cap.Audience != audience || cap.Operation != operation || cap.ExpiresAt <= now.Unix() || cap.ExpiresAt > now.Unix()+90 {
		return cap, attested.ErrUnavailable
	}
	if cap.ProtocolVersion == 0 && (cap.LeaseExpiresAt != 0 || cap.LeaseSequence != 0) {
		return cap, attested.ErrUnavailable
	}
	if cap.ProtocolVersion != 0 && (operation != "audio" || cap.ProtocolVersion != 2 || cap.LeaseSequence != 0 || cap.LeaseExpiresAt < cap.ExpiresAt || cap.LeaseExpiresAt > now.Unix()+60+audioClockSkewSeconds) {
		return cap, attested.ErrUnavailable
	}
	if operation == "audio" && (cap.MaxAudioBytes < 2 || cap.MaxAudioBytes > 960000) {
		return cap, attested.ErrUnavailable
	}
	if operation == "text" && !validTextCapability(cap) {
		return cap, attested.ErrUnavailable
	}
	return cap, nil
}
