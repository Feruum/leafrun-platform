// Package authn supplies technical Access authentication and context propagation.
// Permission policy and identity provenance remain with the owning Access server.
package authn

import (
	"context"
	accessv1 "github.com/leafrun/leafrun-contracts/access/v1"
	identityv1 "github.com/leafrun/leafrun-contracts/identity/v1"
	"net/http"
)

type Authenticator interface {
	Authenticate(context.Context, *http.Request) (identityv1.Identity, error)
}
type Checker interface {
	Check(context.Context, accessv1.CheckRequest) (bool, error)
}
type identityKey struct{}

// WithIdentity restores only a validated identity obtained from an authenticated
// owner boundary or durable server record. Public headers/body are never trusted.
// The immutable value contains no bearer token or session cookie.
func WithIdentity(ctx context.Context, id identityv1.Identity) (context.Context, error) {
	if err := id.Validate(); err != nil {
		return ctx, err
	}
	return context.WithValue(ctx, identityKey{}, id), nil
}
func IdentityFromContext(ctx context.Context) (identityv1.Identity, bool) {
	id, ok := ctx.Value(identityKey{}).(identityv1.Identity)
	if !ok || id.Validate() != nil {
		return identityv1.Identity{}, false
	}
	return id, true
}

// WithoutIdentity keeps cancellation/deadline/trace values while deliberately
// removing initiating-user authority from fixed-service cleanup operations.
func WithoutIdentity(ctx context.Context) context.Context {
	return context.WithValue(ctx,identityKey{},identityv1.Identity{})
}
