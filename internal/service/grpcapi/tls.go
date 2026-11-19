package grpcapi

import (
	"fmt"

	"github.com/radryc/tlsx"
	"google.golang.org/grpc"
)

// TLSOptions returns the gRPC server options that serve TLS or mTLS using the
// shared STRATA_TLS_* contract. It returns no options when STRATA_TLS_MODE is
// off, which keeps the plaintext default of embedded deployments unchanged.
//
// The options are additive: callers attach their auth interceptors as usual.
func TLSOptions() ([]grpc.ServerOption, error) {
	cfg, err := tlsx.LoadEnv()
	if err != nil {
		return nil, err
	}
	if cfg.Mode == tlsx.ModeOff {
		return nil, nil
	}
	creds, err := tlsx.ServerCredentials(cfg.ServerConfig())
	if err != nil {
		return nil, fmt.Errorf("grpcapi: strata tls: %w", err)
	}
	return []grpc.ServerOption{grpc.Creds(creds)}, nil
}
