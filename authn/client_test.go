package authn

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func identityFixture() identityv1.Identity {
	return identityv1.Identity{Principal: identityv1.Principal{Subject: "alice", Domain: "user"}, Authority: identityv1.AuthorityRef{Kind: identityv1.KindKratosSession, ID: "session-1", Version: 1}}
}
func TestAdapterForwardsOnlySelectedCallerCredential(t *testing.T) {
	for _, tc := range []struct {
		auth, cookie string
		want         accessv1.ResolveRequest
	}{{"Bearer caller-secret", "tracking=ignore", accessv1.ResolveRequest{Bearer: "caller-secret"}}, {"", "tracking=ignore; ory_kratos_session=session-secret", accessv1.ResolveRequest{SessionCookie: "session-secret"}}} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer service-secret" || r.Header.Get("Cookie") != "" || r.URL.Path != "/internal/v1/auth/resolve" {
				t.Error("credential separation/path failed")
			}
			var in accessv1.ResolveRequest
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in != tc.want {
				t.Errorf("forwarded %#v %v", in, err)
			}
			fmt.Fprint(w, `{"identity":{"principal":{"subject":"alice","domain":"user"},"authority":{"kind":"kratos_session","id":"session-1","version":1}},"expiresAt":"2099-01-01T00:00:00Z"}`)
		}))
		c, err := NewClient(s.URL, "service-secret", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		var _ Authenticator = c
		var _ Checker = c
		r := httptest.NewRequest("GET", "http://public.example/", nil)
		if tc.auth != "" {
			r.Header.Set("Authorization", tc.auth)
		}
		r.Header.Set("Cookie", tc.cookie)
		// Caller controlled identity headers and body never become the actor.
		r.Header.Set("X-Principal-Subject", "mallory")
		got, err := c.Authenticate(context.Background(), r)
		s.Close()
		if err != nil || got != identityFixture() {
			t.Fatalf("identity: %#v %v", got, err)
		}
	}
}
func TestAdapterRejectsAmbiguityAndServiceCredentials(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer s.Close()
	c, _ := NewClient(s.URL, "service-secret", "", nil)
	for _, headers := range []http.Header{
		{}, {"Authorization": {""}}, {"Authorization": {"Bearer a", "Bearer b"}},
		{"Authorization": {"Bearer a"}, "authorization": {"Bearer b"}},
		{"Authorization": {"Bearer a, Bearer b"}}, {"Authorization": {"Basic a"}},
		{"Authorization": {"Bearer  a"}}, {"Authorization": {"Bearer a\n"}},
		{"Authorization": {"Bearer a"}, "Cookie": {"ory_kratos_session=b"}},
		{"Cookie": {"ory_kratos_session=a; ory_kratos_session=b"}},
		{"Cookie": {"ory_kratos_session=a", "ory_kratos_session=b"}},
		{"Cookie": {"ory_kratos_session="}}, {"Cookie": {"ory_kratos_session"}},
		{"Cookie": {"ory_kratos_session=\"a\""}}, {"Cookie": {"ory_kratos_session=a,b"}},
		{"Cookie": {"ory_kratos_session=a "}},
		{"Authorization": {"Bearer service-secret"}}, {"Cookie": {"ory_kratos_session=service-secret"}},
	} {
		r := httptest.NewRequest("GET", "http://public.example/", nil)
		r.Header = headers
		out, err := c.Authenticate(context.Background(), r)
		if !errors.Is(err, accessv1.ErrUnauthenticated) || out != (identityv1.Identity{}) {
			t.Fatalf("accepted ambiguous request %v %#v", err, out)
		}
		if strings.Contains(err.Error(), "service-secret") {
			t.Fatal("secret echoed")
		}
	}
	if calls.Load() != 0 {
		t.Fatal("invalid caller sent to Access")
	}
	if _, err := c.Authenticate(context.Background(), nil); !errors.Is(err, accessv1.ErrUnauthenticated) {
		t.Fatal(err)
	}
}
func TestConfiguredCookieAndCheck(t *testing.T) {
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/checks/access" {
			t.Error("wrong path")
		}
		fmt.Fprint(w, `{"allowed":true}`)
	}))
	defer s.Close()
	c, _ := NewClient(s.URL, "service-secret", "custom_session", nil)
	r := httptest.NewRequest("GET", "http://public.example/", nil)
	r.Header.Set("Cookie", "ory_kratos_session=x")
	if _, err := c.Authenticate(context.Background(), r); !errors.Is(err, accessv1.ErrUnauthenticated) {
		t.Fatal("unconfigured cookie accepted")
	}
	allowed, err := c.Check(context.Background(), accessv1.CheckRequest{Principal: identityv1.Principal{Subject: "owner-service", Domain: "service"}, Authority: &identityv1.AuthorityRef{Kind: identityv1.KindService, ID: "service-1", Version: 1}, Operation: "health"})
	if err != nil || !allowed {
		t.Fatalf("check: %v %v", allowed, err)
	}
	if _, err := NewClient(s.URL, "service-secret", "bad name", nil); err == nil {
		t.Fatal("bad cookie name accepted")
	}
}
func TestIdentityContextValidatesDurableReference(t *testing.T) {
	ctx := context.Background()
	if _, ok := IdentityFromContext(ctx); ok {
		t.Fatal("unexpected identity")
	}
	id := identityFixture()
	restored, err := WithIdentity(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := IdentityFromContext(restored)
	if !ok || got != id {
		t.Fatal("durable identity not restored")
	}
	id.Authority.Version = 0
	if _, err := WithIdentity(ctx, id); err == nil {
		t.Fatal("invalid identity stored")
	}
}

func TestGuardedAuthenticationRejectsCookieBeforeAccess(t *testing.T) {
	var calls atomic.Int32
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer s.Close()
	c, _ := NewClient(s.URL, "service-secret", "", nil)
	guard, _ := NewBrowserGuard([]string{"https://app.example"}, false)
	r := httptest.NewRequest(http.MethodPost, "https://internal/", nil)
	r.Header.Set("Cookie", "ory_kratos_session=cookie-secret")
	if _, err := c.AuthenticateGuarded(r.Context(), r, guard); !errors.Is(err, accessv1.ErrDenied) {
		t.Fatalf("missing origin: %v", err)
	}
	if calls.Load() != 0 {
		t.Fatal("denied cookie sent to Access")
	}
}

func TestRemoteServiceAndDevIdentityCannotAuthenticatePublicCaller(t *testing.T) {
	for _, tc := range []struct{ kind, domain string }{{"service", "service"}, {"service", "user"}, {"dev", "user"}, {"kratos_session", "service"}} {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintf(w, `{"identity":{"principal":{"subject":"other-node","domain":"%s"},"authority":{"kind":"%s","id":"authority-1","version":1}},"expiresAt":"2099-01-01T00:00:00Z"}`, tc.domain, tc.kind)
		}))
		c, _ := NewClient(s.URL, "local-service-secret", "", nil)
		r := httptest.NewRequest("GET", "http://public.example/", nil)
		r.Header.Set("Authorization", "Bearer other-node-service-secret")
		id, err := c.Authenticate(context.Background(), r)
		s.Close()
		if !errors.Is(err, accessv1.ErrUnauthenticated) || id != (identityv1.Identity{}) {
			t.Fatalf("public service identity accepted: %#v %v", id, err)
		}
	}
}
