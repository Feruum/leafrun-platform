package authn

import (
	"context"
	"net/http"
	"strings"

	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	storagev1 "github.com/leafrun/leafrun-contracts/storage/v1"
)

const DefaultSessionCookieName = "ory_kratos_session"

type Client struct {
	access                    *accessv1.Client
	serviceBearer, cookieName string
	cookieNames               CookieNames
}

func NewClient(baseURL, serviceBearer, cookieName string, transport *http.Client) (*Client, error) {
	if cookieName == "" {
		cookieName = DefaultSessionCookieName
	}
	return NewClientWithCookieNames(baseURL, serviceBearer, CookieNames{Human: cookieName}, transport)
}
func NewClientWithCookieNames(baseURL, serviceBearer string, names CookieNames, transport *http.Client) (*Client, error) {
	if err := names.Validate(); err != nil {
		return nil, err
	}
	access, err := accessv1.NewClient(baseURL, serviceBearer, transport)
	if err != nil {
		return nil, err
	}
	return &Client{access: access, serviceBearer: serviceBearer, cookieName: names.Human, cookieNames: names}, nil
}

func (c *Client) Authenticate(ctx context.Context, r *http.Request) (identityv1.Identity, error) {
	if r == nil {
		return identityv1.Identity{}, accessv1.ErrUnauthenticated
	}
	in, err := c.credentials(r)
	if err != nil {
		return identityv1.Identity{}, err
	}
	resolved, err := c.access.Resolve(ctx, in)
	if err != nil {
		return identityv1.Identity{}, err
	}
	if resolved.Identity.Authority.Kind == identityv1.KindService || resolved.Identity.Authority.Kind == identityv1.KindDev || strings.EqualFold(resolved.Identity.Principal.Domain, "service") {
		return identityv1.Identity{}, accessv1.ErrUnauthenticated
	}
	return resolved.Identity, nil
}
func (c *Client) Check(ctx context.Context, in accessv1.CheckRequest) (bool, error) {
	return c.access.Check(ctx, in)
}

func (c *Client) CheckScope(ctx context.Context, in accessv1.CheckRequest) (accessv1.CheckResponse, error) {
	return c.access.CheckScope(ctx, in)
}

// AuthenticateGuarded protects unsafe cookie requests before resolving identity.
func (c *Client) AuthenticateGuarded(ctx context.Context, r *http.Request, guard *BrowserGuard) (identityv1.Identity, error) {
	_, transport, err := SelectCredentialsWithCookieNames(r, c.cookieNames)
	if err != nil {
		return identityv1.Identity{}, err
	}
	if err = guard.Check(r, transport); err != nil {
		return identityv1.Identity{}, accessv1.ErrDenied
	}
	return c.Authenticate(ctx, r)
}

// CheckStorage adapts the unchanged content scope to the Access policy scope.
// A public actor always needs its server-derived current authority. Restoring
// a different actor's identity in context cannot authorize this check.
func (c *Client) CheckStorage(ctx context.Context, in storagev1.AccessCheck) (bool, error) {
	if id, ok := IdentityFromContext(ctx); ok {
		if id.Principal != in.Principal || in.Authority != nil && *in.Authority != id.Authority {
			return false, accessv1.ErrUnauthenticated
		}
		ref := id.Authority
		in.Authority = &ref
	}
	if in.Principal.Domain != "service" && in.Authority == nil {
		return false, accessv1.ErrUnauthenticated
	}
	return c.Check(ctx, accessv1.FromStorageAccessCheck(in))
}
func (c *Client) credentials(r *http.Request) (accessv1.ResolveRequest, error) {
	in, err := CredentialsWithCookieNames(r, c.cookieNames)
	if err != nil || in.Bearer == c.serviceBearer || in.SessionCookie == c.serviceBearer {
		return accessv1.ResolveRequest{}, accessv1.ErrUnauthenticated
	}
	return in, nil
}

// Credentials extracts exactly one caller credential using strict raw headers.
// It applies no service credential policy; callers must reject their own fixed
// internal credentials. No dependency requests are made by this helper.
func Credentials(r *http.Request, cookieName string) (accessv1.ResolveRequest, error) {
	return CredentialsWithCookieNames(r, CookieNames{Human: cookieName})
}
func CredentialsWithCookieNames(r *http.Request, names CookieNames) (accessv1.ResolveRequest, error) {
	invalid := func() (accessv1.ResolveRequest, error) { return accessv1.ResolveRequest{}, accessv1.ErrUnauthenticated }
	if r == nil || names.Validate() != nil {
		return invalid()
	}
	var in accessv1.ResolveRequest
	authorization := headerValues(r.Header, "Authorization")
	if len(authorization) > 1 {
		return invalid()
	}
	if len(authorization) == 1 {
		scheme, value, ok := strings.Cut(authorization[0], " ")
		if !ok || !strings.EqualFold(scheme, "Bearer") || !accessv1.ValidCredential(value) {
			return invalid()
		}
		in.Bearer = value
	}
	// Parse raw cookie pairs so duplicate/empty/malformed allowlisted cookies do
	// not disappear through net/http's forgiving Cookie parser. Ignore other names.
	selected := 0
	total := 0
	for _, line := range headerValues(r.Header, "Cookie") {
		total += len(line)
		if total > 32<<10 {
			return invalid()
		}
		for _, part := range strings.Split(line, ";") {
			name, value, ok := strings.Cut(strings.TrimLeft(part, " \t"), "=")
			trimmed := strings.TrimSpace(name)
			if trimmed != names.Human && (names.Guest == "" || trimmed != names.Guest) {
				continue
			}
			selected++
			if selected > 1 || !ok || name != trimmed || !accessv1.ValidCredential(value) || (&http.Cookie{Name: name, Value: value}).Valid() != nil || strings.ContainsAny(value, "\"\\") {
				return invalid()
			}
			in.SessionCookie = value
			if trimmed == names.Guest {
				in.SessionCookieKind = accessv1.CookieKindGuest
			} else if names.Guest != "" {
				in.SessionCookieKind = accessv1.CookieKindKratos
			}
		}
	}
	if in.Validate() != nil {
		return invalid()
	}
	return in, nil
}

// Header map keys are compared case-insensitively even for synthetic requests
// whose keys have not been canonicalized by net/http.
func headerValues(header http.Header, name string) []string {
	var values []string
	for key, entries := range header {
		if strings.EqualFold(key, name) {
			values = append(values, entries...)
		}
	}
	return values
}
