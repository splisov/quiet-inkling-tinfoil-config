package attested

import (
	"context"
	"net"
	"net/http"
	"time"
)

// The maintained SDK's util.Get uses DefaultClient. This Go runtime is owned by
// the binding/ingress; configure it once at startup, never during a request.
// Keep DefaultTransport's concrete type: the SDK clones it for pinned TLS.
func init() {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSHandshakeTimeout = 8 * time.Second
	transport.ResponseHeaderTimeout = 8 * time.Second
	transport.MaxResponseHeaderBytes = 32 * 1024
	transport.Proxy = nil
	http.DefaultTransport = transport
	http.DefaultClient = &http.Client{Transport: metadataTransport{transport}, Timeout: 10 * time.Second,
		CheckRedirect: func(r *http.Request, via []*http.Request) error {
			if r.URL.Scheme != "https" || len(via) >= 3 {
				return ErrUnavailable
			}
			return nil
		}}
}

type metadataTransport struct{ transport http.RoundTripper }

func (m metadataTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.URL.Scheme != "https" || r.URL.User != nil {
		return nil, ErrUnavailable
	}
	response, e := m.transport.RoundTrip(r)
	if e != nil {
		return nil, e
	}
	response.Body = http.MaxBytesReader(nil, response.Body, 8*1024*1024)
	return response, nil
}

var verificationSlots = make(chan struct{}, 4)

// The SDK has no caller context. Bound caller latency and outstanding work;
// a timed-out verification never returns a recipient or transmits content later.
func boundedVerification[T any](ctx context.Context, run func() (T, error)) (T, error) {
	var zero T
	select {
	case verificationSlots <- struct{}{}:
	default:
		return zero, ErrUnavailable
	}
	type result struct {
		value T
		err   error
	}
	done := make(chan result, 1)
	go func() {
		defer func() { <-verificationSlots }()
		output := result{err: ErrUnavailable}
		defer func() {
			if recover() != nil {
				output = result{err: ErrUnavailable}
			}
			done <- output
		}()
		output.value, output.err = run()
	}()
	select {
	case value := <-done:
		return value.value, value.err
	case <-ctx.Done():
		return zero, ErrUnavailable
	}
}
