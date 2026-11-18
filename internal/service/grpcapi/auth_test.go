package grpcapi

import (
	"context"
	"errors"
	"net"
	"testing"

	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	"github.com/radryc/kvs/internal/store/local"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func authContext(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

// newAuthTestClient starts a bufconn server created with NewServerWithAuth and
// returns a client. The cleanup closes everything.
func newAuthTestClient(t *testing.T, cfg Config) (kvsv1.KVStoreClient, func()) {
	t.Helper()
	listener := bufconn.Listen(bufSize)
	baseStore, err := local.Open(local.Config{DataDir: t.TempDir(), MaxHotVersions: 2})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	server, err := NewServerWithAuth(baseStore, cfg)
	if err != nil {
		_ = baseStore.Close()
		_ = listener.Close()
		t.Fatalf("NewServerWithAuth: %v", err)
	}

	grpcServer := grpc.NewServer(
		grpc.ChainUnaryInterceptor(server.UnaryInterceptor()),
		grpc.ChainStreamInterceptor(server.StreamInterceptor()),
	)
	kvsv1.RegisterKVStoreServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(listener) }()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		_ = baseStore.Close()
		_ = listener.Close()
		t.Fatalf("dial bufconn: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = baseStore.Close()
		_ = listener.Close()
	}
	return kvsv1.NewKVStoreClient(conn), cleanup
}

func TestNewServerWithAuthRequiresToken(t *testing.T) {
	store, err := local.Open(local.Config{DataDir: t.TempDir(), MaxHotVersions: 2})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	if _, err := NewServerWithAuth(store, Config{}); !errors.Is(err, ErrAuthTokenRequired) {
		t.Fatalf("NewServerWithAuth() error = %v, want ErrAuthTokenRequired", err)
	}

	t.Setenv(AuthTokenEnv, "env-token")
	server, err := NewServerWithAuth(store, Config{})
	if err != nil {
		t.Fatalf("NewServerWithAuth with %s: %v", AuthTokenEnv, err)
	}
	if server.authToken != "env-token" {
		t.Fatalf("auth token = %q, want env-token", server.authToken)
	}
}

func TestUnaryAuthInterceptor(t *testing.T) {
	const secret = "grpc-secret"
	interceptor := UnaryAuthInterceptor(secret)
	info := &grpc.UnaryServerInfo{FullMethod: "/kvs.v1.KVStore/ReadFile"}
	handler := func(ctx context.Context, req any) (any, error) {
		return PrincipalFromContext(ctx), nil
	}

	if _, err := interceptor(context.Background(), nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token: code = %v, want Unauthenticated", status.Code(err))
	}

	badCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer wrong"))
	if _, err := interceptor(badCtx, nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token: code = %v, want Unauthenticated", status.Code(err))
	}

	// A non-Bearer scheme must not be accepted.
	basicCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Basic "+secret))
	if _, err := interceptor(basicCtx, nil, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("basic auth: code = %v, want Unauthenticated", status.Code(err))
	}

	goodCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "bearer "+secret))
	principal, err := interceptor(goodCtx, nil, info, handler)
	if err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if principal == anonymousPrincipal || principal == "" {
		t.Fatalf("principal = %q, want an authenticated principal", principal)
	}

	// Health checks stay reachable for probes.
	healthInfo := &grpc.UnaryServerInfo{FullMethod: "/grpc.health.v1.Health/Check"}
	if _, err := interceptor(context.Background(), nil, healthInfo, handler); err != nil {
		t.Fatalf("health check without token: %v", err)
	}

	// An empty configured token keeps the historical unauthenticated behaviour.
	open := UnaryAuthInterceptor("")
	if got, err := open(context.Background(), nil, info, handler); err != nil || got != anonymousPrincipal {
		t.Fatalf("unauthenticated mode: got=%v err=%v", got, err)
	}
}

type fakeServerStream struct {
	ctx context.Context
}

func (s *fakeServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *fakeServerStream) SendHeader(metadata.MD) error { return nil }
func (s *fakeServerStream) SetTrailer(metadata.MD)       {}
func (s *fakeServerStream) Context() context.Context     { return s.ctx }
func (s *fakeServerStream) SendMsg(any) error            { return nil }
func (s *fakeServerStream) RecvMsg(any) error            { return nil }

func TestStreamAuthInterceptor(t *testing.T) {
	const secret = "grpc-secret"
	interceptor := StreamAuthInterceptor(secret)
	info := &grpc.StreamServerInfo{FullMethod: "/kvs.v1.KVStore/ReadFileStream"}

	var principal string
	handler := func(srv any, stream grpc.ServerStream) error {
		principal = PrincipalFromContext(stream.Context())
		return nil
	}

	if err := interceptor(nil, &fakeServerStream{ctx: context.Background()}, info, handler); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("missing token: code = %v, want Unauthenticated", status.Code(err))
	}

	goodCtx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+secret))
	if err := interceptor(nil, &fakeServerStream{ctx: goodCtx}, info, handler); err != nil {
		t.Fatalf("valid token: %v", err)
	}
	if principal == "" || principal == anonymousPrincipal {
		t.Fatalf("principal = %q, want an authenticated principal", principal)
	}
}

func TestServerRejectsUnauthenticatedRPCs(t *testing.T) {
	const token = "rpc-secret"
	client, cleanup := newAuthTestClient(t, Config{AuthToken: token})
	defer cleanup()

	ctx := authContext(context.Background(), token)
	if _, err := client.UpsertFiles(ctx, &kvsv1.UpsertFilesRequest{
		Writes: []*kvsv1.PathWrite{{LogicalPath: "/auth/demo.yaml", Content: []byte("v1")}},
	}); err != nil {
		t.Fatalf("authenticated UpsertFiles: %v", err)
	}

	if _, err := client.ReadFile(context.Background(), &kvsv1.ReadFileRequest{LogicalPath: "/auth/demo.yaml"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated ReadFile: code = %v, want Unauthenticated", status.Code(err))
	}
	if _, err := client.ReadFile(authContext(context.Background(), "wrong"), &kvsv1.ReadFileRequest{LogicalPath: "/auth/demo.yaml"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("wrong token ReadFile: code = %v, want Unauthenticated", status.Code(err))
	}

	stream, err := client.ReadFileStream(ctx, &kvsv1.ReadFileRequest{LogicalPath: "/auth/demo.yaml"})
	if err != nil {
		t.Fatalf("authenticated ReadFileStream: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("authenticated ReadFileStream recv: %v", err)
	}

	badStream, err := client.ReadFileStream(context.Background(), &kvsv1.ReadFileRequest{LogicalPath: "/auth/demo.yaml"})
	if err == nil {
		_, err = badStream.Recv()
	}
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("unauthenticated ReadFileStream: code = %v, want Unauthenticated", status.Code(err))
	}
}
