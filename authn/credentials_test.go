package authn

import (
	"net/http/httptest"
	"testing"
)

func TestExportedCredentialParserHasNoServicePolicy(t *testing.T) {
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Authorization", "Bearer service-value")
	in, err := Credentials(r, DefaultSessionCookieName)
	if err != nil || in.Bearer != "service-value" {
		t.Fatalf("technical parser applied caller policy: %+v %v", in, err)
	}
	if _, err = Credentials(r, "bad name"); err == nil {
		t.Fatal("bad cookie name accepted")
	}
}
