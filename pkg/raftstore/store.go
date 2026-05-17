package raftstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	kvsv1 "github.com/rydzu/ainfra/kvs/api/proto/kvs/v1"
	"github.com/rydzu/ainfra/kvs/internal/store/local"
	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultApplyTimeout = 10 * time.Second
	defaultDialTimeout  = 5 * time.Second
)

type Peer struct {
	NodeID      string
	APIAddress  string
	RaftAddress string
}

type Config struct {
	NodeID           string
	DataDir          string
	RaftAddress      string
	RaftAdvertise    string
	APIAddress       string
	Peers            []Peer
	Bootstrap        bool
	MaxHotVersions   int
	WatcherQueueSize int
	ApplyTimeout     time.Duration
	DialTimeout      time.Duration
	TransportTimeout time.Duration
	LogOutput        io.Writer
	Offloader        kvsapi.FetcherOffloader // optional; routes archived blobs to fetcher
}

type Store struct {
	local        *local.Store
	raft         *raft.Raft
	transport    *raft.NetworkTransport
	applyTimeout time.Duration
	dialTimeout  time.Duration
	apiByRaft    map[raft.ServerAddress]string
	nodeByRaft   map[raft.ServerAddress]string
	apiByNode    map[string]string
	connsMu      sync.Mutex
	connections  map[string]*grpc.ClientConn
	clients      map[string]kvsv1.KVStoreClient
}

type commandEnvelope struct {
	Op            string                `json:"op"`
	MutationBatch *kvsapi.MutationBatch `json:"mutationBatch,omitempty"`
	DeleteBatch   *kvsapi.DeleteBatch   `json:"deleteBatch,omitempty"`
}

type applyResult struct {
	Batch kvsapi.BatchRevision `json:"batch"`
	Error string               `json:"error,omitempty"`
}

func Open(cfg Config) (*Store, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("raftstore: data dir is required")
	}
	if cfg.MaxHotVersions <= 0 {
		cfg.MaxHotVersions = 5
	}
	if cfg.ApplyTimeout <= 0 {
		cfg.ApplyTimeout = defaultApplyTimeout
	}
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.TransportTimeout <= 0 {
		cfg.TransportTimeout = defaultDialTimeout
	}

	localStore, err := local.Open(local.Config{
		DataDir:          filepath.Join(cfg.DataDir, "state"),
		MaxHotVersions:   cfg.MaxHotVersions,
		WatcherQueueSize: cfg.WatcherQueueSize,
		Offloader:        cfg.Offloader,
	})
	if err != nil {
		return nil, err
	}

	store := &Store{
		local:        localStore,
		applyTimeout: cfg.ApplyTimeout,
		dialTimeout:  cfg.DialTimeout,
		apiByRaft:    make(map[raft.ServerAddress]string, len(cfg.Peers)+1),
		nodeByRaft:   make(map[raft.ServerAddress]string, len(cfg.Peers)+1),
		apiByNode:    make(map[string]string, len(cfg.Peers)+1),
		connections:  make(map[string]*grpc.ClientConn),
		clients:      make(map[string]kvsv1.KVStoreClient),
	}

	if cfg.NodeID != "" && cfg.APIAddress != "" {
		store.apiByNode[cfg.NodeID] = cfg.APIAddress
	}
	for _, peer := range cfg.Peers {
		if peer.NodeID != "" && peer.APIAddress != "" {
			store.apiByNode[peer.NodeID] = peer.APIAddress
		}
		if peer.RaftAddress != "" {
			store.nodeByRaft[raft.ServerAddress(peer.RaftAddress)] = peer.NodeID
			store.apiByRaft[raft.ServerAddress(peer.RaftAddress)] = peer.APIAddress
		}
	}
	if cfg.RaftAddress == "" {
		return store, nil
	}
	if cfg.NodeID == "" {
		localStore.Close()
		return nil, fmt.Errorf("raftstore: node id is required when raft is enabled")
	}

	localRaftAddress := cfg.RaftAddress
	var advertiseAddr net.Addr
	if strings.TrimSpace(cfg.RaftAdvertise) != "" {
		tcpAddr, err := net.ResolveTCPAddr("tcp", strings.TrimSpace(cfg.RaftAdvertise))
		if err != nil {
			store.Close()
			return nil, fmt.Errorf("raftstore: resolve advertise address: %w", err)
		}
		advertiseAddr = tcpAddr
		localRaftAddress = tcpAddr.String()
	}
	store.nodeByRaft[raft.ServerAddress(localRaftAddress)] = cfg.NodeID
	store.apiByRaft[raft.ServerAddress(localRaftAddress)] = cfg.APIAddress

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		localStore.Close()
		return nil, err
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "log.bolt"))
	if err != nil {
		localStore.Close()
		return nil, err
	}
	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "stable.bolt"))
	if err != nil {
		localStore.Close()
		return nil, err
	}
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(raftDir, "snapshots"), 1, cfg.LogOutput)
	if err != nil {
		localStore.Close()
		return nil, err
	}
	transport, err := raft.NewTCPTransport(cfg.RaftAddress, advertiseAddr, 3, cfg.TransportTimeout, cfg.LogOutput)
	if err != nil {
		localStore.Close()
		return nil, err
	}
	store.transport = transport

	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(cfg.NodeID)
	raftCfg.SnapshotInterval = 5 * time.Minute
	raftCfg.SnapshotThreshold = 4096

	nodeRaft, err := raft.NewRaft(raftCfg, &fsm{store: localStore}, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		localStore.Close()
		return nil, err
	}
	store.raft = nodeRaft

	if cfg.Bootstrap {
		hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
		if err != nil {
			store.Close()
			return nil, err
		}
		if !hasState {
			servers := make([]raft.Server, 0, len(cfg.Peers)+1)
			seen := make(map[string]struct{}, len(cfg.Peers)+1)
			servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(cfg.NodeID), Address: raft.ServerAddress(localRaftAddress)})
			seen[cfg.NodeID] = struct{}{}
			for _, peer := range cfg.Peers {
				if peer.NodeID == "" || peer.RaftAddress == "" {
					continue
				}
				if _, exists := seen[peer.NodeID]; exists {
					continue
				}
				seen[peer.NodeID] = struct{}{}
				servers = append(servers, raft.Server{Suffrage: raft.Voter, ID: raft.ServerID(peer.NodeID), Address: raft.ServerAddress(peer.RaftAddress)})
			}
			if err := nodeRaft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
				store.Close()
				return nil, err
			}
		}
	}

	return store, nil
}

// StartPurge starts a background goroutine that periodically purges old blob
// versions from the underlying local store.  It delegates to the embedded
// local.Store and is a no-op when the raft node is a follower that forwards
// writes; purge operates purely on local hot-storage so it is safe to run on
// every node.
func (s *Store) StartPurge(ctx context.Context, interval time.Duration) {
	s.local.StartPurge(ctx, interval)
}

func (s *Store) Close() error {
	var closeErr error
	if s.raft != nil {
		if err := s.raft.Shutdown().Error(); err != nil {
			closeErr = err
		}
	}
	s.connsMu.Lock()
	for endpoint, conn := range s.connections {
		if err := conn.Close(); err != nil && closeErr == nil {
			closeErr = fmt.Errorf("close grpc conn %s: %w", endpoint, err)
		}
	}
	s.connections = map[string]*grpc.ClientConn{}
	s.clients = map[string]kvsv1.KVStoreClient{}
	s.connsMu.Unlock()
	if s.local != nil {
		if err := s.local.Close(); err != nil && closeErr == nil {
			closeErr = err
		}
	}
	return closeErr
}

func (s *Store) Status() kvsapi.StoreStatus {
	if s == nil {
		return kvsapi.StoreStatus{Mode: "disabled", Role: "disabled"}
	}
	if s.raft == nil {
		return kvsapi.StoreStatus{
			Enabled:   true,
			Healthy:   s.local != nil,
			Mode:      "local",
			Role:      "local",
			PeerCount: 1,
			KeyCount:  s.local.KeyCount(),
		}
	}

	peerCount := int32(len(s.nodeByRaft))
	if future := s.raft.GetConfiguration(); future.Error() == nil {
		peerCount = int32(len(future.Configuration().Servers))
	}

	state := s.raft.State()
	role := strings.ToLower(state.String())
	leaderAddr := s.raft.Leader()
	leaderID := s.nodeByRaft[leaderAddr]
	healthy := false
	switch state {
	case raft.Leader:
		healthy = true
	case raft.Follower:
		healthy = leaderAddr != ""
	}

	return kvsapi.StoreStatus{
		Enabled:   true,
		Healthy:   healthy,
		Mode:      "raft",
		Role:      role,
		LeaderID:  leaderID,
		PeerCount: peerCount,
		KeyCount:  s.local.KeyCount(),
	}
}

func (s *Store) ReadFile(ctx context.Context, logicalPath string) ([]byte, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: logicalPath})
		if err != nil {
			return nil, err
		}
		return resp.GetContent(), nil
	}
	return s.local.ReadFile(ctx, logicalPath)
}

func (s *Store) ListDir(ctx context.Context, logicalDir string) ([]kvsapi.DirEntry, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.ListDir(ctx, &kvsv1.ListDirRequest{LogicalDir: logicalDir})
		if err != nil {
			return nil, err
		}
		entries := make([]kvsapi.DirEntry, 0, len(resp.GetEntries()))
		for _, entry := range resp.GetEntries() {
			entries = append(entries, kvsapi.DirEntry{Name: entry.GetName(), IsDir: entry.GetIsDir(), Size: entry.GetSize()})
		}
		return entries, nil
	}
	return s.local.ListDir(ctx, logicalDir)
}

func (s *Store) Stat(ctx context.Context, logicalPath string) (kvsapi.FileInfo, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.Stat(ctx, &kvsv1.StatRequest{LogicalPath: logicalPath})
		if err != nil {
			return kvsapi.FileInfo{}, err
		}
		info := resp.GetInfo()
		return kvsapi.FileInfo{Path: info.GetPath(), Size: info.GetSize(), VersionID: info.GetVersionId(), ModTime: info.GetModTime().AsTime()}, nil
	}
	return s.local.Stat(ctx, logicalPath)
}

func (s *Store) Watch(ctx context.Context, prefixes []string) (<-chan kvsapi.ChangeEvent, error) {
	return s.local.Watch(ctx, prefixes)
}

func (s *Store) UpsertFiles(ctx context.Context, batch kvsapi.MutationBatch) (kvsapi.BatchRevision, error) {
	if s.raft == nil {
		return s.local.UpsertFiles(ctx, batch)
	}
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.UpsertFiles(ctx, protoUpsertBatch(batch))
		if err != nil {
			return kvsapi.BatchRevision{}, err
		}
		return batchRevisionFromProto(resp), nil
	}
	if err := s.local.ValidateUpsertBatch(ctx, batch); err != nil {
		return kvsapi.BatchRevision{}, err
	}
	return s.applyCommand(commandEnvelope{Op: "upsert", MutationBatch: &batch})
}

func (s *Store) DeletePaths(ctx context.Context, batch kvsapi.DeleteBatch) (kvsapi.BatchRevision, error) {
	if s.raft == nil {
		return s.local.DeletePaths(ctx, batch)
	}
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.DeletePaths(ctx, protoDeleteBatch(batch))
		if err != nil {
			return kvsapi.BatchRevision{}, err
		}
		return batchRevisionFromProto(resp), nil
	}
	if err := s.local.ValidateDeleteBatch(ctx, batch); err != nil {
		return kvsapi.BatchRevision{}, err
	}
	return s.applyCommand(commandEnvelope{Op: "delete", DeleteBatch: &batch})
}

func (s *Store) ListVersions(ctx context.Context, logicalPath string) ([]kvsapi.FileVersion, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.ListVersions(ctx, &kvsv1.ListVersionsRequest{LogicalPath: logicalPath})
		if err != nil {
			return nil, err
		}
		versions := make([]kvsapi.FileVersion, 0, len(resp.GetVersions()))
		for _, version := range resp.GetVersions() {
			versions = append(versions, fileVersionFromProto(version))
		}
		return versions, nil
	}
	return s.local.ListVersions(ctx, logicalPath)
}

func (s *Store) GetVersion(ctx context.Context, logicalPath, versionID string) (kvsapi.VersionedFile, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.GetVersion(ctx, &kvsv1.GetVersionRequest{LogicalPath: logicalPath, VersionId: versionID})
		if err != nil {
			return kvsapi.VersionedFile{}, err
		}
		file := resp.GetFile()
		return kvsapi.VersionedFile{Version: fileVersionFromProto(file.GetVersion()), Content: append([]byte(nil), file.GetContent()...)}, nil
	}
	return s.local.GetVersion(ctx, logicalPath, versionID)
}

func (s *Store) applyCommand(command commandEnvelope) (kvsapi.BatchRevision, error) {
	data, err := json.Marshal(command)
	if err != nil {
		return kvsapi.BatchRevision{}, err
	}
	future := s.raft.Apply(data, s.applyTimeout)
	if err := future.Error(); err != nil {
		return kvsapi.BatchRevision{}, err
	}
	result, ok := future.Response().(applyResult)
	if !ok {
		return kvsapi.BatchRevision{}, fmt.Errorf("raftstore: unexpected apply response %T", future.Response())
	}
	if result.Error != "" {
		return kvsapi.BatchRevision{}, errors.New(result.Error)
	}
	return result.Batch, nil
}

func (s *Store) forwardClient() (kvsv1.KVStoreClient, error) {
	if s.raft == nil || s.raft.State() == raft.Leader {
		return nil, raft.ErrNotLeader
	}
	leaderAddr := s.raft.Leader()
	if leaderAddr == "" {
		return nil, raft.ErrNotLeader
	}
	endpoint := s.apiByRaft[leaderAddr]
	if endpoint == "" {
		if nodeID := s.nodeByRaft[leaderAddr]; nodeID != "" {
			endpoint = s.apiByNode[nodeID]
		}
	}
	if endpoint == "" {
		return nil, fmt.Errorf("raftstore: leader endpoint unavailable for %s", leaderAddr)
	}
	return s.clientFor(endpoint)
}

func (s *Store) clientFor(endpoint string) (kvsv1.KVStoreClient, error) {
	s.connsMu.Lock()
	defer s.connsMu.Unlock()
	if client, ok := s.clients[endpoint]; ok {
		return client, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.dialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	s.connections[endpoint] = conn
	client := kvsv1.NewKVStoreClient(conn)
	s.clients[endpoint] = client
	return client, nil
}

type fsm struct {
	store *local.Store
}

func (f *fsm) Apply(logEntry *raft.Log) interface{} {
	var command commandEnvelope
	if err := json.Unmarshal(logEntry.Data, &command); err != nil {
		return applyResult{Error: err.Error()}
	}
	switch command.Op {
	case "upsert":
		if command.MutationBatch == nil {
			return applyResult{Error: "raftstore: missing mutation batch"}
		}
		batch, err := f.store.UpsertFiles(context.Background(), *command.MutationBatch)
		if err != nil {
			return applyResult{Error: err.Error()}
		}
		return applyResult{Batch: batch}
	case "delete":
		if command.DeleteBatch == nil {
			return applyResult{Error: "raftstore: missing delete batch"}
		}
		batch, err := f.store.DeletePaths(context.Background(), *command.DeleteBatch)
		if err != nil {
			return applyResult{Error: err.Error()}
		}
		return applyResult{Batch: batch}
	default:
		return applyResult{Error: fmt.Sprintf("raftstore: unsupported command %q", command.Op)}
	}
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &fsmSnapshot{store: f.store}, nil
}

func (f *fsm) Restore(r io.ReadCloser) error {
	defer r.Close()
	return f.store.Restore(r)
}

type fsmSnapshot struct {
	store *local.Store
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.store.Snapshot(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}

func protoUpsertBatch(batch kvsapi.MutationBatch) *kvsv1.UpsertFilesRequest {
	req := &kvsv1.UpsertFilesRequest{Context: &kvsv1.MutationContext{PrincipalId: batch.Context.PrincipalID, Reason: batch.Context.Reason, CorrelationId: batch.Context.CorrelationID}}
	for _, write := range batch.Writes {
		req.Writes = append(req.Writes, &kvsv1.PathWrite{LogicalPath: write.LogicalPath, Content: write.Content, ExpectedVersionId: write.ExpectedVersionID})
	}
	return req
}

func protoDeleteBatch(batch kvsapi.DeleteBatch) *kvsv1.DeletePathsRequest {
	req := &kvsv1.DeletePathsRequest{Context: &kvsv1.MutationContext{PrincipalId: batch.Context.PrincipalID, Reason: batch.Context.Reason, CorrelationId: batch.Context.CorrelationID}}
	for _, deleteReq := range batch.Deletes {
		req.Deletes = append(req.Deletes, &kvsv1.PathDelete{LogicalPath: deleteReq.LogicalPath, ExpectedVersionId: deleteReq.ExpectedVersionID})
	}
	return req
}

func batchRevisionFromProto(batch *kvsv1.BatchRevision) kvsapi.BatchRevision {
	if batch == nil {
		return kvsapi.BatchRevision{}
	}
	result := kvsapi.BatchRevision{BatchRevisionID: batch.GetBatchRevisionId(), Files: make([]kvsapi.FileVersion, 0, len(batch.GetFiles()))}
	for _, file := range batch.GetFiles() {
		result.Files = append(result.Files, fileVersionFromProto(file))
	}
	return result
}

func fileVersionFromProto(file *kvsv1.FileVersion) kvsapi.FileVersion {
	if file == nil {
		return kvsapi.FileVersion{}
	}
	committedAt := time.Time{}
	if file.GetCommittedAt() != nil {
		committedAt = file.GetCommittedAt().AsTime()
	}
	return kvsapi.FileVersion{
		LogicalPath:     file.GetLogicalPath(),
		VersionID:       file.GetVersionId(),
		BatchRevisionID: file.GetBatchRevisionId(),
		ContentSHA256:   file.GetContentSha256(),
		CommittedAt:     committedAt,
		Tombstone:       file.GetTombstone(),
		PrincipalID:     file.GetPrincipalId(),
		Reason:          file.GetReason(),
	}
}
