package grpcserver

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	"github.com/radryc/kvs/internal/store/local"
	"github.com/radryc/tlsx"
	"github.com/radryc/tlsx/pki"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const testToken = "kvs-secret"

func generateBundle(t *testing.T) *pki.Bundle {
	t.Helper()
	bundle, err := pki.Generate(pki.Options{
		Dir:         t.TempDir(),
		TrustDomain: "strata.test",
		Validity:    time.Hour,
		Services: []pki.Service{
			{Name: "kvs", Namespace: "kvs", IPs: []string{"127.0.0.1"}},
			{Name: "client", Namespace: "kvs"},
		},
	})
	if err != nil {
		t.Fatalf("pki.Generate: %v", err)
	}
	return bundle
}

func configureStrataTLS(t *testing.T, bundle *pki.Bundle, service string, requireClientCert bool) {
	t.Helper()
	identity := bundle.Services[service]
	t.Setenv("STRATA_TLS_MODE", "required")
	t.Setenv("STRATA_TLS_CERT_FILE", identity.CertFile)
	t.Setenv("STRATA_TLS_KEY_FILE", identity.KeyFile)
	t.Setenv("STRATA_TLS_CA_FILE", bundle.CAFile)
	t.Setenv("STRATA_TLS_SERVER_NAME", "localhost")
	t.Setenv("STRATA_TLS_REQUIRE_CLIENT_CERT", strconv.FormatBool(requireClientCert))
	t.Setenv("STRATA_TLS_ALLOWED_SPIFFE_IDS", "")
	t.Setenv("STRATA_TLS_EXPECTED_SPIFFE_IDS", "")
}

func clearStrataTLS(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"STRATA_TLS_MODE",
		"STRATA_TLS_CERT_FILE",
		"STRATA_TLS_KEY_FILE",
		"STRATA_TLS_CA_FILE",
		"STRATA_TLS_SERVER_NAME",
		"STRATA_TLS_REQUIRE_CLIENT_CERT",
		"STRATA_TLS_ALLOWED_SPIFFE_IDS",
		"STRATA_TLS_EXPECTED_SPIFFE_IDS",
	} {
		t.Setenv(key, "")
	}
}

func newLocalStore(t *testing.T) *local.Store {
	t.Helper()
	store, err := local.Open(local.Config{
		DataDir:        t.TempDir(),
		MaxHotVersions: 2,
	})
	if err != nil {
		t.Fatalf("local.Open: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// serveTLS starts a KVStore gRPC server using the authenticated + TLS
// constructors and returns its address.
func serveTLS(t *testing.T, store *local.Store) string {
	t.Helper()
	service, opts, err := NewWithAuthAndTLS(store, Config{AuthToken: testToken})
	if err != nil {
		t.Fatalf("NewWithAuthAndTLS: %v", err)
	}
	srv := grpc.NewServer(opts...)
	kvsv1.RegisterKVStoreServer(srv, service)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		srv.Stop()
		_ = lis.Close()
	})
	return lis.Addr().String()
}

func TestGRPCTLSRoundTrip(t *testing.T) {
	bundle := generateBundle(t)
	configureStrataTLS(t, bundle, "kvs", true)
	store := newLocalStore(t)
	addr := serveTLS(t, store)

	clientTLS, err := tlsx.NewClientTLSConfig(tlsx.ClientConfig{
		CertFile:   bundle.Services["client"].CertFile,
		KeyFile:    bundle.Services["client"].KeyFile,
		CAFile:     bundle.CAFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(clientTLS)))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+testToken)

	client := kvsv1.NewKVStoreClient(conn)
	if _, err := client.UpsertFiles(ctx, &kvsv1.UpsertFilesRequest{
		Writes:  []*kvsv1.PathWrite{{LogicalPath: "/tls/hello", Content: []byte("mtls")}},
		Context: &kvsv1.MutationContext{PrincipalId: "tester", Reason: "tls test"},
	}); err != nil {
		t.Fatalf("UpsertFiles over mTLS: %v", err)
	}
	resp, err := client.ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: "/tls/hello"})
	if err != nil {
		t.Fatalf("ReadFile over mTLS: %v", err)
	}
	if string(resp.GetContent()) != "mtls" {
		t.Fatalf("content = %q, want mtls", resp.GetContent())
	}
}

func TestGRPCRejectsPlaintextInRequiredMode(t *testing.T) {
	bundle := generateBundle(t)
	configureStrataTLS(t, bundle, "kvs", true)
	store := newLocalStore(t)
	addr := serveTLS(t, store)

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+testToken)
	_, err = kvsv1.NewKVStoreClient(conn).ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: "/tls/hello"})
	if err == nil {
		t.Fatal("plaintext RPC must fail against a TLS KVStore server")
	}
}

func TestGRPCRejectsClientWithoutCertificate(t *testing.T) {
	bundle := generateBundle(t)
	configureStrataTLS(t, bundle, "kvs", true)
	store := newLocalStore(t)
	addr := serveTLS(t, store)

	anonymous, err := tlsx.NewClientTLSConfig(tlsx.ClientConfig{
		CAFile:     bundle.CAFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("anonymous client TLS config: %v", err)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(credentials.NewTLS(anonymous)))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+testToken)
	if _, err := kvsv1.NewKVStoreClient(conn).ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: "/tls/hello"}); err == nil {
		t.Fatal("mTLS server must reject clients without a certificate")
	}
}

func TestTLSOptionsOffByDefault(t *testing.T) {
	clearStrataTLS(t)

	opts, err := TLSOptions()
	if err != nil {
		t.Fatalf("TLSOptions: %v", err)
	}
	if len(opts) != 0 {
		t.Fatalf("options = %d, want 0 when STRATA_TLS_MODE is off", len(opts))
	}
}
