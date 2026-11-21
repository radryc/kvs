package raftstore

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"testing"
	"time"

	"github.com/radryc/kvs/pkg/kvsapi"
	"github.com/radryc/tlsx"
	"github.com/radryc/tlsx/pki"
)

func generateRaftBundle(t *testing.T) *pki.Bundle {
	t.Helper()
	bundle, err := pki.Generate(pki.Options{
		Dir:         t.TempDir(),
		TrustDomain: "strata.test",
		Validity:    time.Hour,
		Services: []pki.Service{
			{Name: "kvs", Namespace: "kvs", IPs: []string{"127.0.0.1"}},
		},
	})
	if err != nil {
		t.Fatalf("pki.Generate: %v", err)
	}
	return bundle
}

// raftTLSEnv builds the peer transport configuration used by the TLS tests: a
// shared workload identity, mTLS between peers and SPIFFE allowlists on both
// sides.
func raftTLSEnv(bundle *pki.Bundle) *tlsx.EnvConfig {
	identity := bundle.Services["kvs"]
	return &tlsx.EnvConfig{
		Mode:              tlsx.ModeRequired,
		CertFile:          identity.CertFile,
		KeyFile:           identity.KeyFile,
		CAFile:            bundle.CAFile,
		ServerName:        "localhost",
		RequireClientCert: true,
		AllowedSPIFFEIDs:  []string{identity.SPIFFEID},
		ExpectedSPIFFEIDs: []string{identity.SPIFFEID},
	}
}

func allocateRaftAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()
	_ = lis.Close()
	return addr
}

func TestRaftSingleNodeTLSStartup(t *testing.T) {
	bundle := generateRaftBundle(t)
	raftAddr := allocateRaftAddr(t)

	store, err := Open(Config{
		NodeID:         "node-a",
		DataDir:        t.TempDir(),
		RaftAddress:    raftAddr,
		APIAddress:     allocateRaftAddr(t),
		Bootstrap:      true,
		MaxHotVersions: 2,
		LogOutput:      io.Discard,
		ForwardTLS:     ForwardTLSConfig{AllowPlaintext: true},
		RaftTLS:        raftTLSEnv(bundle),
	})
	if err != nil {
		t.Fatalf("Open with raft TLS: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitForLeader(t, ctx, store)

	if _, err := store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  []kvsapi.PathWrite{{LogicalPath: "/tls/raft", Content: []byte("encrypted")}},
		Context: kvsapi.MutationContext{PrincipalID: "tester", Reason: "raft tls"},
	}); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}
	content, err := store.ReadFile(ctx, "/tls/raft")
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(content) != "encrypted" {
		t.Fatalf("content = %q, want encrypted", content)
	}
}

func TestRaftPeersReplicateOverMutualTLS(t *testing.T) {
	bundle := generateRaftBundle(t)
	peerTLS := raftTLSEnv(bundle)

	raftAddrs := []string{allocateRaftAddr(t), allocateRaftAddr(t)}
	apiAddrs := []string{allocateRaftAddr(t), allocateRaftAddr(t)}
	peers := []Peer{
		{NodeID: "node-a", APIAddress: apiAddrs[0], RaftAddress: raftAddrs[0]},
		{NodeID: "node-b", APIAddress: apiAddrs[1], RaftAddress: raftAddrs[1]},
	}

	stores := make([]*Store, 0, len(peers))
	for index, peer := range peers {
		store, err := Open(Config{
			NodeID:         peer.NodeID,
			DataDir:        t.TempDir(),
			RaftAddress:    peer.RaftAddress,
			APIAddress:     peer.APIAddress,
			Peers:          peers,
			Bootstrap:      index == 0,
			MaxHotVersions: 2,
			LogOutput:      io.Discard,
			ForwardTLS:     ForwardTLSConfig{AllowPlaintext: true},
			RaftTLS:        peerTLS,
		})
		if err != nil {
			for _, opened := range stores {
				_ = opened.Close()
			}
			t.Fatalf("Open %s: %v", peer.NodeID, err)
		}
		stores = append(stores, store)
	}
	defer func() {
		for _, store := range stores {
			_ = store.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	waitForLeader(t, ctx, stores[0])
	waitForLeader(t, ctx, stores[1])

	leader, follower := stores[0], stores[1]
	if leader.Status().Role != "leader" {
		leader, follower = follower, leader
	}
	if leader.Status().Role != "leader" {
		t.Fatalf("no leader elected: roles %q/%q", stores[0].Status().Role, stores[1].Status().Role)
	}

	if _, err := leader.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  []kvsapi.PathWrite{{LogicalPath: "/tls/replicated", Content: []byte("payload")}},
		Context: kvsapi.MutationContext{PrincipalID: "tester", Reason: "raft peer tls"},
	}); err != nil {
		t.Fatalf("UpsertFiles on leader: %v", err)
	}
	waitForContent(t, ctx, follower, "/tls/replicated", "payload")
}

func TestRaftPeerListenerRejectsPlaintext(t *testing.T) {
	bundle := generateRaftBundle(t)
	raftAddr := allocateRaftAddr(t)

	store, err := Open(Config{
		NodeID:         "node-a",
		DataDir:        t.TempDir(),
		RaftAddress:    raftAddr,
		APIAddress:     allocateRaftAddr(t),
		Bootstrap:      true,
		MaxHotVersions: 2,
		LogOutput:      io.Discard,
		ForwardTLS:     ForwardTLSConfig{AllowPlaintext: true},
		RaftTLS:        raftTLSEnv(bundle),
	})
	if err != nil {
		t.Fatalf("Open with raft TLS: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitForLeader(t, ctx, store)

	conn, err := net.DialTimeout("tcp", raftAddr, 2*time.Second)
	if err != nil {
		t.Fatalf("dial raft address: %v", err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("this is not a TLS client\n")); err == nil {
		buf := make([]byte, 64)
		if _, err := conn.Read(buf); err == nil {
			t.Fatal("plaintext peer connection must be rejected")
		}
	}
}

func TestRaftPeerListenerRequiresClientCertificate(t *testing.T) {
	bundle := generateRaftBundle(t)
	raftAddr := allocateRaftAddr(t)

	store, err := Open(Config{
		NodeID:         "node-a",
		DataDir:        t.TempDir(),
		RaftAddress:    raftAddr,
		APIAddress:     allocateRaftAddr(t),
		Bootstrap:      true,
		MaxHotVersions: 2,
		LogOutput:      io.Discard,
		ForwardTLS:     ForwardTLSConfig{AllowPlaintext: true},
		RaftTLS:        raftTLSEnv(bundle),
	})
	if err != nil {
		t.Fatalf("Open with raft TLS: %v", err)
	}
	defer store.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitForLeader(t, ctx, store)

	anonymous, err := tlsx.NewClientTLSConfig(tlsx.ClientConfig{
		CAFile:     bundle.CAFile,
		ServerName: "localhost",
	})
	if err != nil {
		t.Fatalf("anonymous client TLS config: %v", err)
	}
	conn, err := tls.Dial("tcp", raftAddr, anonymous)
	if err != nil {
		// Rejected during the handshake: expected.
		return
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	if _, err := conn.Write([]byte("x")); err != nil {
		return
	}
	buf := make([]byte, 64)
	if _, err := conn.Read(buf); err == nil {
		t.Fatal("raft peer listener must require a client certificate")
	}
}
