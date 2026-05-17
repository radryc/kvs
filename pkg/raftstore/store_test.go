package raftstore

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/rydzu/ainfra/kvs/pkg/grpcserver"
	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
	"google.golang.org/grpc"
)

func TestReplicatedStoreForwardsToLeaderAndReplicates(t *testing.T) {
	type node struct {
		store      *Store
		grpcServer *grpc.Server
		listener   net.Listener
		cleanup    func()
	}

	allocateAddr := func(t *testing.T) string {
		t.Helper()
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		addr := lis.Addr().String()
		_ = lis.Close()
		return addr
	}

	apiAddrs := []string{allocateAddr(t), allocateAddr(t), allocateAddr(t)}
	raftAddrs := []string{allocateAddr(t), allocateAddr(t), allocateAddr(t)}
	peers := []Peer{
		{NodeID: "node-a", APIAddress: apiAddrs[0], RaftAddress: raftAddrs[0]},
		{NodeID: "node-b", APIAddress: apiAddrs[1], RaftAddress: raftAddrs[1]},
		{NodeID: "node-c", APIAddress: apiAddrs[2], RaftAddress: raftAddrs[2]},
	}

	nodes := make([]node, 0, len(peers))
	for index, peer := range peers {
		cfg := Config{
			NodeID:         peer.NodeID,
			DataDir:        t.TempDir(),
			RaftAddress:    peer.RaftAddress,
			APIAddress:     peer.APIAddress,
			Peers:          peers,
			Bootstrap:      index == 0,
			MaxHotVersions: 2,
			LogOutput:      io.Discard,
		}
		store, err := Open(cfg)
		if err != nil {
			t.Fatalf("open %s: %v", peer.NodeID, err)
		}
		lis, err := net.Listen("tcp", peer.APIAddress)
		if err != nil {
			store.Close()
			t.Fatalf("listen %s: %v", peer.NodeID, err)
		}
		grpcServer := grpc.NewServer()
		grpcserver.Register(grpcServer, store, grpcserver.Config{})
		go func() { _ = grpcServer.Serve(lis) }()
		nodes = append(nodes, node{store: store, grpcServer: grpcServer, listener: lis})
	}
	defer func() {
		for _, node := range nodes {
			node.grpcServer.Stop()
			_ = node.listener.Close()
			_ = node.store.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	waitForLeader(t, ctx, nodes[0].store)

	batch, err := nodes[1].store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  []kvsapi.PathWrite{{LogicalPath: "/guardian-system/.queues/demo/task.json", Content: []byte("payload")}},
		Context: kvsapi.MutationContext{PrincipalID: "tester", Reason: "replicated write"},
	})
	if err != nil {
		t.Fatalf("upsert through follower: %v", err)
	}
	if len(batch.Files) != 1 {
		t.Fatalf("expected 1 file version, got %d", len(batch.Files))
	}

	waitForContent(t, ctx, nodes[2].store, "/guardian-system/.queues/demo/task.json", "payload")

	deleted, err := nodes[2].store.DeletePaths(ctx, kvsapi.DeleteBatch{
		Deletes: []kvsapi.PathDelete{{LogicalPath: "/guardian-system/.queues/demo/task.json", ExpectedVersionID: batch.Files[0].VersionID}},
	})
	if err != nil {
		t.Fatalf("delete through follower: %v", err)
	}
	if len(deleted.Files) != 1 || !deleted.Files[0].Tombstone {
		t.Fatalf("expected tombstone response, got %+v", deleted.Files)
	}

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, err := nodes[0].store.ReadFile(context.Background(), "/guardian-system/.queues/demo/task.json")
		if err != nil {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("expected delete to replicate to leader")
}

func waitForLeader(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	for {
		if store.raft != nil && store.raft.Leader() != "" {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for raft leader: %v", ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func waitForContent(t *testing.T, ctx context.Context, store *Store, logicalPath, expected string) {
	t.Helper()
	for {
		content, err := store.ReadFile(context.Background(), logicalPath)
		if err == nil && string(content) == expected {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for replicated content %s: %v", logicalPath, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}
