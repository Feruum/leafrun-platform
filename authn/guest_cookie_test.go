package authn

import (
	"context"
	"encoding/json"
	"fmt"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestGuestCookieSelectorAndResolverAgree(t *testing.T) {
	names := CookieNames{Human: "human_session", Guest: "guest_session"}
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var in accessv1.ResolveRequest
		if json.NewDecoder(r.Body).Decode(&in) != nil || in.SessionCookie != "opaque-guest" || in.SessionCookieKind != accessv1.CookieKindGuest || in.Bearer != "" || r.Header.Get("Cookie") != "" || r.Header.Get("Authorization") != "Bearer fixed-service" {
			t.Error("selected kind or service separation lost")
		}
		fmt.Fprint(w, `{"identity":{"principal":{"subject":"grant","domain":"guest"},"authority":{"kind":"guest_session","id":"guest-id","version":1}},"expiresAt":"2099-01-01T00:00:00Z"}`)
	}))
	defer s.Close()
	c, err := NewClientWithCookieNames(s.URL, "fixed-service", names, nil)
	if err != nil {
		t.Fatal(err)
	}
	g, _ := NewBrowserGuard([]string{"https://app.example"}, false)
	r := httptest.NewRequest("POST", "https://app.example/v1/action", nil)
	r.Header.Set("Cookie", "tracking=ignored; guest_session=opaque-guest")
	r.Header.Set("Origin", "https://app.example")
	selected, transport, err := SelectCredentialsWithCookieNames(r, names)
	if err != nil || selected.SessionCookieKind != accessv1.CookieKindGuest || transport != TransportCookie {
		t.Fatal("guest preflight selection failed")
	}
	id, err := c.AuthenticateGuarded(context.Background(), r, g)
	if err != nil || id.Authority.ID != "guest-id" {
		t.Fatalf("guest resolver: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatal("guest resolved more than once")
	}
	for _, header := range []http.Header{
		{"Cookie": {"human_session=human; guest_session=opaque-guest"}}, {"Authorization": {"Bearer caller"}, "Cookie": {"guest_session=opaque-guest"}},
		{"Cookie": {"guest_session=one; guest_session=two"}}, {"Cookie": {"guest_session=one"}, "cookie": {"guest_session=two"}},
		{"Cookie": {"guest_session"}}, {"Cookie": {"guest_session="}}, {"Cookie": {"guest_session =x"}}, {"Cookie": {"guest_session=\"x\""}},
		{"Cookie": {"guest_session=" + strings.Repeat("x", 4097)}}, {"Cookie": {"unrelated=" + strings.Repeat("x", 32<<10)}},
		{"Cookie": {"guest_session=fixed-service"}}, {"Cookie": {"guest_session=opaque-guest"}},
	} {
		r := httptest.NewRequest("POST", "https://app.example/v1/action", nil)
		r.Header = header
		if _, err := c.AuthenticateGuarded(context.Background(), r, g); err == nil {
			t.Fatal("invalid/unguarded guest accepted")
		}
	}
	if calls.Load() != 1 {
		t.Fatal("invalid guest candidate reached dependency")
	}
	for _, names := range []CookieNames{{Human: "same", Guest: "same"}, {Human: "human", Guest: "bad name"}, {Human: "", Guest: "guest"}, {Human: "human", Guest: strings.Repeat("a", 129)}} {
		if _, err := NewClientWithCookieNames(s.URL, "fixed-service", names, nil); err == nil {
			t.Fatal("invalid cookie names accepted")
		}
	}
}

func TestSelectedCookieRejectsNonASCIICookieBytes(t *testing.T) {
	r := httptest.NewRequest("GET", "http://public.example/v1/", nil)
	r.Header.Set("Cookie", "guest_session=é")
	if _, _, err := SelectCredentialsWithCookieNames(r, CookieNames{Human: "human_session", Guest: "guest_session"}); err == nil {
		t.Fatal("non-ASCII selected cookie accepted")
	}
}
