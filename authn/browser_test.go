package authn

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBrowserGuardUnsafeSelectedCookie(t *testing.T) {
	guard, err := NewBrowserGuard([]string{"https://app.example:8443"}, false)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name, origin, site string
		want               bool
	}{
		{"same origin", "https://app.example:8443", "same-origin", true},
		{"missing", "", "", false}, {"null", "null", "", false},
		{"foreign", "https://evil.example", "", false},
		{"suffix", "https://app.example:8443.evil.example", "", false},
		{"cross site", "https://app.example:8443", "cross-site", false},
		{"combined", "https://app.example:8443, https://evil.example", "", false},
		{"path", "https://app.example:8443/", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "https://internal/jobs/id/cancel", nil)
			r.Header.Set("Origin", tc.origin)
			if tc.site != "" {
				r.Header.Set("Sec-Fetch-Site", tc.site)
			}
			if got := guard.Check(r, TransportCookie) == nil; got != tc.want {
				t.Fatalf("allowed=%v want=%v", got, tc.want)
			}
		})
	}
	r := httptest.NewRequest(http.MethodPost, "https://internal/", nil)
	r.Header["Origin"] = []string{"https://app.example:8443", "https://app.example:8443"}
	if guard.Check(r, TransportCookie) == nil {
		t.Fatal("duplicate origin accepted")
	}
	r.Header.Set("Origin", "https://app.example:8443")
	r.Header["Sec-Fetch-Site"] = []string{"same-origin", "same-origin"}
	if guard.Check(r, TransportCookie) == nil {
		t.Fatal("duplicate metadata accepted")
	}
	if guard.Check(r, TransportBearer) != nil {
		t.Fatal("bearer requires browser metadata")
	}
	r.Method = http.MethodGet
	if guard.Check(r, TransportCookie) != nil {
		t.Fatal("safe method blocked")
	}
}

func TestBrowserGuardConfiguration(t *testing.T) {
	for _, origins := range [][]string{nil, {"*"}, {"null"}, {"http://app.example"}, {"https://app.example/"}, {"https://app.example?q=x"}, {"https://user@app.example"}, {strings.Repeat("x", 2049)}, {"https://APP.example"}, {"https://app.example", "https://app.example"}} {
		if _, err := NewBrowserGuard(origins, false); err == nil {
			t.Fatalf("accepted invalid origins %#v", origins)
		}
	}
	if _, err := NewBrowserGuard([]string{"http://127.0.0.1:8080"}, true); err != nil {
		t.Fatal(err)
	}
	if _, err := NewBrowserGuard([]string{"http://app.example"}, true); err == nil {
		t.Fatal("dev transport accepts nonloopback HTTP")
	}
}

func TestDeclaredMedia(t *testing.T) {
	for _, tc := range []struct {
		values []string
		want   bool
	}{
		{[]string{"application/json"}, true}, {[]string{"application/json; charset=utf-8"}, true},
		{nil, false}, {[]string{"text/plain"}, false}, {[]string{"application/x-www-form-urlencoded"}, false},
		{[]string{"application/json", "application/json"}, false}, {[]string{"application/json, text/plain"}, false},
		{[]string{"application/json; boundary=x"}, false}, {[]string{"application/json; charset=latin1"}, false},
	} {
		r := httptest.NewRequest(http.MethodPost, "https://app.example", nil)
		r.Header["Content-Type"] = tc.values
		if got := RequireMediaType(r, "application/json") == nil; got != tc.want {
			t.Fatalf("media %#v allowed=%v", tc.values, got)
		}
	}
}

func TestSelectedTransport(t *testing.T) {
	for _, tc := range []struct {
		headers http.Header
		want    Transport
	}{
		{http.Header{"Authorization": {"Bearer secret"}}, TransportBearer},
		{http.Header{"Cookie": {"ory_kratos_session=secret"}}, TransportCookie},
	} {
		r := httptest.NewRequest(http.MethodPost, "https://app.example", nil)
		r.Header = tc.headers
		in, transport, err := SelectCredentials(r, DefaultSessionCookieName)
		if err != nil || transport != tc.want || in.Validate() != nil {
			t.Fatalf("selection %v %v", transport, err)
		}
	}
	r := httptest.NewRequest(http.MethodPost, "https://app.example", nil)
	r.Header.Set("Authorization", "Bearer secret")
	r.Header.Set("Cookie", "ory_kratos_session=other")
	if _, _, err := SelectCredentials(r, DefaultSessionCookieName); err == nil {
		t.Fatal("mixed transport accepted")
	}
}
