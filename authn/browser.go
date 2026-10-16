package authn

import (
	"errors"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"

	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
)

// Transport records how this request supplied its selected credential. It is
// request-local metadata, never derived from identity kind or persisted work.
type Transport string

const (
	TransportBearer Transport = "bearer"
	TransportCookie Transport = "cookie"
)

func SelectCredentials(r *http.Request, cookieName string) (accessv1.ResolveRequest, Transport, error) {
	return SelectCredentialsWithCookieNames(r, CookieNames{Human: cookieName})
}
func SelectCredentialsWithCookieNames(r *http.Request, names CookieNames) (accessv1.ResolveRequest, Transport, error) {
	in, err := CredentialsWithCookieNames(r, names)
	if err != nil {
		return accessv1.ResolveRequest{}, "", err
	}
	if in.SessionCookie != "" {
		return in, TransportCookie, nil
	}
	return in, TransportBearer, nil
}

// BrowserGuard permits unsafe cookie requests only from explicitly configured
// serialized origins. It does not grant identity, policy rights, or CORS access.
type BrowserGuard struct{ origins map[string]struct{} }

func NewBrowserGuard(origins []string, allowLoopbackHTTP bool) (*BrowserGuard, error) {
	if len(origins) == 0 || len(origins) > 16 {
		return nil, errors.New("explicit bounded browser origins required")
	}
	g := &BrowserGuard{origins: make(map[string]struct{}, len(origins))}
	for _, origin := range origins {
		u, err := url.Parse(origin)
		if err != nil || len(origin) > 2048 || u.Host == "" || u.User != nil || u.Path != "" || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || strings.ToLower(u.Host) != u.Host || u.String() != origin || strings.ContainsAny(origin, " \t\r\n,\\") {
			return nil, errors.New("invalid browser origin")
		}
		if u.Scheme != "https" {
			ip := net.ParseIP(u.Hostname())
			if !allowLoopbackHTTP || u.Scheme != "http" || (u.Hostname() != "localhost" && (ip == nil || !ip.IsLoopback())) {
				return nil, errors.New("browser origins require HTTPS")
			}
		}
		if port := u.Port(); port != "" {
			if _, err := net.LookupPort("tcp", port); err != nil {
				return nil, errors.New("invalid browser origin port")
			}
		}
		if _, exists := g.origins[origin]; exists {
			return nil, errors.New("duplicate browser origin")
		}
		g.origins[origin] = struct{}{}
	}
	return g, nil
}

func (g *BrowserGuard) Check(r *http.Request, transport Transport) error {
	if r == nil {
		return errors.New("invalid browser request")
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return nil
	}
	if transport == TransportBearer {
		return nil
	}
	if transport != TransportCookie || g == nil {
		return errors.New("browser request denied")
	}
	origins := headerValues(r.Header, "Origin")
	if len(origins) != 1 || len(origins[0]) > 2048 {
		return errors.New("browser origin required")
	}
	if _, ok := g.origins[origins[0]]; !ok {
		return errors.New("browser origin denied")
	}
	sites := headerValues(r.Header, "Sec-Fetch-Site")
	if len(sites) > 1 {
		return errors.New("ambiguous browser metadata")
	}
	if len(sites) == 1 && sites[0] != "same-origin" && sites[0] != "same-site" {
		return errors.New("browser metadata denied")
	}
	return nil
}

// RequireMediaType applies a route's declared media before decoding or writes.
// Only JSON permits an optional UTF-8 charset; binary media has no parameters.
func RequireMediaType(r *http.Request, declared string) error {
	if r == nil {
		return errors.New("declared content type required")
	}
	values := headerValues(r.Header, "Content-Type")
	if len(values) != 1 || len(values[0]) > 256 {
		return errors.New("declared content type required")
	}
	media, params, err := mime.ParseMediaType(values[0])
	if err != nil || media != declared {
		return errors.New("unsupported content type")
	}
	if len(params) > 0 && (declared != "application/json" || len(params) != 1 || !strings.EqualFold(params["charset"], "utf-8")) {
		return errors.New("unsupported content type parameters")
	}
	return nil
}
