//go:build audioonly

package main

import (
	"net/http"
	"net/http/httptest"
	"quietinkling/private-inference/attested"
	"testing"
)

func TestAudioBuildCannotEnableTextThroughPolicy(t *testing.T) {
	mux := http.NewServeMux()
	p := &attested.Policy{Endpoints: []attested.Endpoint{{Role: "textIngress"}}}
	if _, e := registerText(mux, p, nil, nil, nil); e == nil {
		t.Fatal("text policy accepted")
	}
	p.Endpoints = []attested.Endpoint{{Role: "audioIngress"}}
	if n, e := registerText(mux, p, nil, nil, nil); e != nil || n != 0 {
		t.Fatal("audio policy rejected")
	}
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("POST", "/v1/interpretations", nil))
	if w.Code != 404 {
		t.Fatal("text route registered")
	}
}
