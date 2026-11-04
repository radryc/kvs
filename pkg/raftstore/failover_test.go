package raftstore

import (
	"context"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/radryc/kvs/pkg/grpcserver"
	"github.com/radryc/kvs/pkg/kvsapi"
	"google.golang.org/grpc"
)

type testClusterNode struct {
	id       string
	store    *Store
	server   *grpc.Server
	listener net.Listener
	stopOnce sync.Once
}

func (n *testClusterNode) stop() {
	n.stopOnce.Do(func() {
		if n.server != nil {
			n.server.Stop()
		}
		if n.listener != nil {
			_ = n.listener.Close()
		}
		if n.store != nil {
			_ = n.store.Close()
		}
	})
}

func allocateTestAddr(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()
	return addr
}

func startTestCluster(t *testing.T, count int) ([]*testClusterNode, func()) {
	t.Helper()

	apiAddrs := make([]string, count)
	raftAddrs := make([]string, count)
	peers := make([]Peer, count)
	for i := 0; i < count; i++ {
		apiAddrs[i] = allocateTestAddr(t)
		raftAddrs[i] = allocateTestAddr(t)
	}
	for i := 0; i < count; i++ {
		peers[i] = Peer{
			NodeID:      "node-" + string(rune('a'+i)),
			APIAddress:  apiAddrs[i],
			RaftAddress: raftAddrs[i],
		}
	}

	nodes := make([]*testClusterNode, 0, count)
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
		})
		if err != nil {
			t.Fatalf("open %s: %v", peer.NodeID, err)
		}
		listener, err := net.Listen("tcp", peer.APIAddress)
		if err != nil {
			_ = store.Close()
			t.Fatalf("listen %s: %v", peer.NodeID, err)
		}
		server := grpc.NewServer()
		grpcserver.Register(server, store, grpcserver.Config{})
		go func() { _ = server.Serve(listener) }()
		nodes = append(nodes, &testClusterNode{
			id:       peer.NodeID,
			store:    store,
			server:   server,
			listener: listener,
		})
	}

	cleanup := func() {
		for _, node := range nodes {
			node.stop()
		}
	}
	return nodes, cleanup
}

func waitForClusterLeader(t *testing.T, ctx context.Context, nodes []*testClusterNode) *testClusterNode {
	t.Helper()
	for {
		for _, node := range nodes {
			if node.store.raft != nil && node.store.raft.State() == raft.Leader {
				return node
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for raft leader: %v", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestOpenDoesNotRequireDNSForAdvertisedAddress(t *testing.T) {
	store, err := Open(Config{
		NodeID:        "node-a",
		DataDir:       t.TempDir(),
		RaftAddress:   "127.0.0.1:0",
		RaftAdvertise: "node-a.does-not-exist.invalid:7000",
		APIAddress:    "127.0.0.1:19000",
		Bootstrap:     true,
		LogOutput:     io.Discard,
	})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer store.Close()

	if got, want := store.transport.LocalAddr(), raft.ServerAddress("node-a.does-not-exist.invalid:7000"); got != want {
		t.Fatalf("LocalAddr() = %q, want %q", got, want)
	}
	if _, ok := store.nodeByRaft["node-a.does-not-exist.invalid:7000"]; !ok {
		t.Fatal("advertised raft address was not registered in nodeByRaft")
	}
}

func TestFollowerWithUnknownLeaderDoesNotApplyLocally(t *testing.T) {
	nodes, cleanup := startTestCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	leader := waitForClusterLeader(t, ctx, nodes)

	var follower *testClusterNode
	for _, node := range nodes {
		if node != leader && node.store.raft.State() == raft.Follower {
			follower = node
			break
		}
	}
	if follower == nil {
		t.Fatal("no follower found")
	}

	// Simulate the exact failure mode: the follower knows there is a leader
	// but cannot resolve its API endpoint. It must return a retryable error,
	// not fall through to a local raft.Apply.
	follower.store.nodeByRaft = map[raft.ServerAddress]string{}
	follower.store.apiByRaft = map[raft.ServerAddress]string{}
	follower.store.apiByNode = map[string]string{}

	path := "/guardian-system/.queues/no-local-apply/task.json"
	_, err := follower.store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  []kvsapi.PathWrite{{LogicalPath: path, Content: []byte("payload")}},
		Context: kvsapi.MutationContext{PrincipalID: "tester", Reason: "unknown leader"},
	})
	if !errors.Is(err, kvsapi.ErrLeaderUnknown) {
		t.Fatalf("UpsertFiles() error = %v, want ErrLeaderUnknown", err)
	}

	if content, readErr := follower.store.local.ReadFile(context.Background(), path); readErr == nil {
		t.Fatalf("follower applied the write locally: %q", string(content))
	}
}

func TestLeaderFailoverRetriesWriteAgainstNewLeader(t *testing.T) {
	nodes, cleanup := startTestCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	leader := waitForClusterLeader(t, ctx, nodes)

	var survivors []*testClusterNode
	var writer *testClusterNode
	for _, node := range nodes {
		if node == leader {
			continue
		}
		survivors = append(survivors, node)
		if writer == nil {
			writer = node
		}
	}
	leader.stop()

	// Wait for one of the survivors to become the new leader before writing.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if writer.store.raft.State() == raft.Follower && writer.store.raft.Leader() != "" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	path := "/guardian-system/.queues/failover/task.json"
	batch, err := writer.store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  []kvsapi.PathWrite{{LogicalPath: path, Content: []byte("after-failover")}},
		Context: kvsapi.MutationContext{PrincipalID: "tester", Reason: "leader failover"},
	})
	if err != nil {
		t.Fatalf("UpsertFiles() after leader failover error = %v", err)
	}
	if len(batch.Files) != 1 {
		t.Fatalf("expected 1 file version, got %d", len(batch.Files))
	}

	waitForContent(t, ctx, survivors[0].store, path, "after-failover")
}

func TestStatusReportsLeaderAddressAndTerm(t *testing.T) {
	nodes, cleanup := startTestCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	leader := waitForClusterLeader(t, ctx, nodes)

	status := leader.store.Status()
	if status.LeaderID == "" {
		t.Fatal("leader status has empty LeaderID")
	}
	if status.LeaderAddress == "" {
		t.Fatal("leader status has empty LeaderAddress")
	}
	if status.Term == 0 {
		t.Fatal("leader status has zero Term")
	}
	if status.Role != "leader" {
		t.Fatalf("leader status role = %q, want leader", status.Role)
	}
}
