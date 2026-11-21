package raftstore

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func clearForwardEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		forwardTLSAllowPlaintextEnv,
		forwardTLSCAFileEnv,
		forwardTLSServerNameEnv,
		forwardTLSInsecureEnv,
		ForwardAuthTokenEnv,
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

func writeForwardTestCert(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	template := x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "kvs-forward-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:              []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &privateKey.PublicKey, privateKey)
	if err != nil {
		t.Fatalf("create certificate: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}

	dir := t.TempDir()
	certFile = filepath.Join(dir, "server.crt")
	keyFile = filepath.Join(dir, "server.key")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	return certFile, keyFile
}

type forwardTestServer struct {
	kvsv1.UnimplementedKVStoreServer
}

func (forwardTestServer) Stat(context.Context, *kvsv1.StatRequest) (*kvsv1.StatResponse, error) {
	return &kvsv1.StatResponse{Info: &kvsv1.FileInfo{Path: "/forward/test", Size: 1}}, nil
}

func startForwardTestServer(t *testing.T, tlsCertFile, tlsKeyFile, authToken string) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	opts := make([]grpc.ServerOption, 0, 2)
	if tlsCertFile != "" {
		creds, err := credentials.NewServerTLSFromFile(tlsCertFile, tlsKeyFile)
		if err != nil {
			t.Fatalf("server TLS credentials: %v", err)
		}
		opts = append(opts, grpc.Creds(creds))
	}
	if authToken != "" {
		opts = append(opts, grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			md, _ := metadata.FromIncomingContext(ctx)
			if len(md.Get("authorization")) == 0 || md.Get("authorization")[0] != "Bearer "+authToken {
				return nil, status.Error(codes.Unauthenticated, "bad token")
			}
			return handler(ctx, req)
		}))
	}
	server := grpc.NewServer(opts...)
	kvsv1.RegisterKVStoreServer(server, forwardTestServer{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	return listener.Addr().String()
}

func newForwardTestStore(t *testing.T, cfg ForwardTLSConfig, authToken string) *Store {
	t.Helper()
	creds, err := buildForwardCredentials(cfg)
	if err != nil {
		t.Fatalf("buildForwardCredentials: %v", err)
	}
	store := &Store{
		dialTimeout:      2 * time.Second,
		connections:      make(map[string]*grpc.ClientConn),
		clients:          make(map[string]kvsv1.KVStoreClient),
		forwardCreds:     creds,
		forwardAuthToken: authToken,
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func TestBuildForwardCredentialsDefaultsToTLS(t *testing.T) {
	clearForwardEnv(t)

	creds, err := buildForwardCredentials(ForwardTLSConfig{})
	if err != nil {
		t.Fatalf("buildForwardCredentials: %v", err)
	}
	if creds == nil {
		t.Fatal("forwarded gRPC must use TLS by default")
	}
}

func TestBuildForwardCredentialsPlaintextRequiresOptIn(t *testing.T) {
	clearForwardEnv(t)

	creds, err := buildForwardCredentials(ForwardTLSConfig{AllowPlaintext: true})
	if err != nil {
		t.Fatalf("buildForwardCredentials: %v", err)
	}
	if creds != nil {
		t.Fatal("explicit plaintext opt-in should return nil credentials")
	}

	t.Setenv(forwardTLSAllowPlaintextEnv, "true")
	creds, err = buildForwardCredentials(ForwardTLSConfig{})
	if err != nil {
		t.Fatalf("buildForwardCredentials with env: %v", err)
	}
	if creds != nil {
		t.Fatal("environment plaintext opt-in should return nil credentials")
	}
}

func TestBuildForwardCredentialsPrefersStrataTLS(t *testing.T) {
	clearForwardEnv(t)
	bundle := generateRaftBundle(t)
	identity := bundle.Services["kvs"]

	t.Setenv("STRATA_TLS_MODE", "required")
	t.Setenv("STRATA_TLS_CERT_FILE", identity.CertFile)
	t.Setenv("STRATA_TLS_KEY_FILE", identity.KeyFile)
	t.Setenv("STRATA_TLS_CA_FILE", bundle.CAFile)
	t.Setenv("STRATA_TLS_SERVER_NAME", "localhost")
	t.Setenv("STRATA_TLS_REQUIRE_CLIENT_CERT", "false")
	// The legacy plaintext opt-in must not override the shared contract.
	t.Setenv(forwardTLSAllowPlaintextEnv, "true")

	creds, err := buildForwardCredentials(ForwardTLSConfig{AllowPlaintext: true})
	if err != nil {
		t.Fatalf("buildForwardCredentials: %v", err)
	}
	if creds == nil {
		t.Fatal("STRATA_TLS_* must win over KVS_FORWARD_TLS_ALLOW_PLAINTEXT")
	}
	if got := creds.Info().SecurityProtocol; got != "tls" {
		t.Fatalf("security protocol = %q, want tls", got)
	}
}

func TestForwardClientTLSConfigCustomCA(t *testing.T) {
	clearForwardEnv(t)
	certFile, _ := writeForwardTestCert(t)

	tlsConfig, err := forwardClientTLSConfig(ForwardTLSConfig{
		CAFile:     certFile,
		ServerName: "kvs.internal",
	})
	if err != nil {
		t.Fatalf("forwardClientTLSConfig: %v", err)
	}
	if tlsConfig.RootCAs == nil {
		t.Fatal("custom CA should populate RootCAs")
	}
	if tlsConfig.ServerName != "kvs.internal" {
		t.Fatalf("ServerName = %q, want kvs.internal", tlsConfig.ServerName)
	}
	if tlsConfig.InsecureSkipVerify {
		t.Fatal("verification must remain enabled by default")
	}
	if tlsConfig.MinVersion != tls.VersionTLS12 {
		t.Fatalf("MinVersion = %x, want TLS 1.2", tlsConfig.MinVersion)
	}
}

func TestForwardClientTLSConfigRejectsBadCA(t *testing.T) {
	clearForwardEnv(t)

	if _, err := forwardClientTLSConfig(ForwardTLSConfig{CAFile: filepath.Join(t.TempDir(), "missing.pem")}); err == nil {
		t.Fatal("expected error for missing CA file")
	}

	badFile := filepath.Join(t.TempDir(), "bad.pem")
	if err := os.WriteFile(badFile, []byte("not a certificate"), 0o600); err != nil {
		t.Fatalf("write bad CA: %v", err)
	}
	if _, err := forwardClientTLSConfig(ForwardTLSConfig{CAFile: badFile}); err == nil {
		t.Fatal("expected error for CA file without certificates")
	}
}

func TestForwardClientVerifiesTLS(t *testing.T) {
	clearForwardEnv(t)
	certFile, keyFile := writeForwardTestCert(t)
	addr := startForwardTestServer(t, certFile, keyFile, "")

	// Default (system roots) must reject the self-signed server.
	untrusted := newForwardTestStore(t, ForwardTLSConfig{ServerName: "127.0.0.1"}, "")
	if _, err := untrusted.clientFor(addr); err == nil {
		t.Fatal("expected TLS verification failure against a self-signed server")
	}

	// A custom CA verifies the same server.
	trusted := newForwardTestStore(t, ForwardTLSConfig{CAFile: certFile, ServerName: "127.0.0.1"}, "")
	client, err := trusted.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor with custom CA: %v", err)
	}
	if _, err := client.Stat(context.Background(), &kvsv1.StatRequest{LogicalPath: "/forward/test"}); err != nil {
		t.Fatalf("forwarded Stat over TLS: %v", err)
	}
}

func TestForwardClientRejectsPlaintextByDefault(t *testing.T) {
	clearForwardEnv(t)
	addr := startForwardTestServer(t, "", "", "")

	store := newForwardTestStore(t, ForwardTLSConfig{}, "")
	if _, err := store.clientFor(addr); err == nil {
		t.Fatal("default TLS credentials must not silently fall back to plaintext")
	}
}

func TestForwardClientPlaintextOptIn(t *testing.T) {
	clearForwardEnv(t)
	addr := startForwardTestServer(t, "", "", "")

	store := newForwardTestStore(t, ForwardTLSConfig{AllowPlaintext: true}, "")
	client, err := store.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor with explicit plaintext: %v", err)
	}
	if _, err := client.Stat(context.Background(), &kvsv1.StatRequest{LogicalPath: "/forward/test"}); err != nil {
		t.Fatalf("forwarded Stat over plaintext: %v", err)
	}
}

func TestForwardClientPropagatesBearerToken(t *testing.T) {
	clearForwardEnv(t)
	certFile, keyFile := writeForwardTestCert(t)
	const token = "forward-secret"
	addr := startForwardTestServer(t, certFile, keyFile, token)

	// Without the token the forwarded call is denied.
	noToken := newForwardTestStore(t, ForwardTLSConfig{CAFile: certFile, ServerName: "127.0.0.1"}, "")
	client, err := noToken.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor without token: %v", err)
	}
	if _, err := client.Stat(context.Background(), &kvsv1.StatRequest{LogicalPath: "/forward/test"}); status.Code(err) != codes.Unauthenticated {
		t.Fatalf("Stat without token: code = %v, want Unauthenticated", status.Code(err))
	}

	// With the token, the same call succeeds.
	withToken := newForwardTestStore(t, ForwardTLSConfig{CAFile: certFile, ServerName: "127.0.0.1"}, token)
	client, err = withToken.clientFor(addr)
	if err != nil {
		t.Fatalf("clientFor with token: %v", err)
	}
	if _, err := client.Stat(context.Background(), &kvsv1.StatRequest{LogicalPath: "/forward/test"}); err != nil {
		t.Fatalf("Stat with token: %v", err)
	}
}
