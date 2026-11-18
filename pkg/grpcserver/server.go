package grpcserver

import (
	"log/slog"

	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	grpcapi "github.com/radryc/kvs/internal/service/grpcapi"
	"github.com/radryc/kvs/pkg/kvsapi"
	"google.golang.org/grpc"
)

type Config struct {
	ChunkSize int
	// AuthToken is the bearer token required by NewWithAuth. When empty,
	// NewWithAuth falls back to KVS_AUTH_TOKEN.
	AuthToken string
	// AuditLogPath enables a JSONL audit trail. When empty, KVS_AUDIT_LOG is
	// consulted, otherwise audit records are written to stderr as JSON.
	AuditLogPath string
	// AuditLogger overrides the audit destination (tests and embedding).
	AuditLogger *slog.Logger
}

func apiConfig(cfg Config) grpcapi.Config {
	return grpcapi.Config{
		ChunkSize:    cfg.ChunkSize,
		AuthToken:    cfg.AuthToken,
		AuditLogPath: cfg.AuditLogPath,
		AuditLogger:  cfg.AuditLogger,
	}
}

// New returns an unauthenticated server for tests and callers that provide
// their own authorization layer. Production entry points should use
// NewWithAuth.
func New(store kvsapi.Store, cfg Config) kvsv1.KVStoreServer {
	return grpcapi.NewServer(store, apiConfig(cfg))
}

// NewWithAuth returns a KVStore service that requires a bearer token on every
// RPC (health checks excepted), together with the gRPC server options that
// enforce it:
//
//	service, opts, err := grpcserver.NewWithAuth(store, cfg)
//	srv := grpc.NewServer(opts...)
//	kvsv1.RegisterKVStoreServer(srv, service)
func NewWithAuth(store kvsapi.Store, cfg Config) (kvsv1.KVStoreServer, []grpc.ServerOption, error) {
	server, err := grpcapi.NewServerWithAuth(store, apiConfig(cfg))
	if err != nil {
		return nil, nil, err
	}
	options := []grpc.ServerOption{
		grpc.ChainUnaryInterceptor(server.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(server.StreamInterceptor()),
	}
	return server, options, nil
}

// Register installs an unauthenticated KVStore service. Use NewWithAuth and a
// grpc.NewServer configured with its options when authentication is required.
func Register(server grpc.ServiceRegistrar, store kvsapi.Store, cfg Config) {
	if server == nil || store == nil {
		return
	}
	kvsv1.RegisterKVStoreServer(server, New(store, cfg))
}
