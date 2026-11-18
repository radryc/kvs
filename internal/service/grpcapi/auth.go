package grpcapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
)

const anonymousPrincipal = "anonymous"

// principalContextKey carries the authenticated principal through the request
// context.
type principalContextKey struct{}

// PrincipalFromContext returns the authenticated principal, or "anonymous"
// when authentication is disabled.
func PrincipalFromContext(ctx context.Context) string {
	if ctx != nil {
		if principal, ok := ctx.Value(principalContextKey{}).(string); ok && principal != "" {
			return principal
		}
	}
	return anonymousPrincipal
}

func withPrincipal(ctx context.Context, principal string) context.Context {
	if principal == "" {
		principal = anonymousPrincipal
	}
	return context.WithValue(ctx, principalContextKey{}, principal)
}

// principalForToken derives a stable, non-reversible principal identifier from
// the shared bearer token so audit records never contain the token itself.
func principalForToken(token string) string {
	digest := sha256.Sum256([]byte(token))
	return "bearer:" + hex.EncodeToString(digest[:8])
}
