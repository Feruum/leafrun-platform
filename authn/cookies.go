package authn

import (
	"errors"
	"net/http"
)

const DefaultGuestCookieName = "leafrun_guest_session"

// CookieNames is server configuration, never client-selected provider metadata.
type CookieNames struct{ Human, Guest string }

func (n CookieNames) Validate() error {
	if n.Human == "" || len(n.Human) > 128 || (&http.Cookie{Name: n.Human, Value: "x"}).Valid() != nil {
		return errors.New("invalid human cookie name")
	}
	if n.Guest != "" && (len(n.Guest) > 128 || n.Guest == n.Human || (&http.Cookie{Name: n.Guest, Value: "x"}).Valid() != nil) {
		return errors.New("invalid guest cookie name")
	}
	return nil
}

// ConfiguredCookieNames applies deployment defaults once, consistently for preflight and resolver.
func ConfiguredCookieNames(human, guest string) CookieNames {
	if human == "" {
		human = DefaultSessionCookieName
	}
	if guest == "" {
		guest = DefaultGuestCookieName
	}
	return CookieNames{Human: human, Guest: guest}
}
