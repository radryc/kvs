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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	"github.com/radryc/kvs/internal/store/local"
	"github.com/radryc/kvs/pkg/kvsapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

// staticAddr preserves a configured hostname as the advertised raft address.
// Hashicorp raft normally requires a *net.TCPAddr, which forces callers to
// resolve DNS at startup. A custom StreamLayer lets us advertise the stable
// service name instead and resolve peers at dial time.
type staticAddr struct {
	value string
}

func (a staticAddr) Network() string { return "tcp" }
func (a staticAddr) String() string  { return a.value }

type tcpStreamLayer struct {
	listener  net.Listener
	advertise net.Addr
}

func (s *tcpStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	return net.DialTimeout("tcp", string(address), timeout)
}

func (s *tcpStreamLayer) Accept() (net.Conn, error) { return s.listener.Accept() }
func (s *tcpStreamLayer) Close() error              { return s.listener.Close() }
func (s *tcpStreamLayer) Addr() net.Addr            { return s.advertise }

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
		store.registerRaftEndpoint(raft.ServerAddress(strings.TrimSpace(peer.RaftAddress)), peer.NodeID, peer.APIAddress)
	}
	if cfg.RaftAddress == "" {
		return store, nil
	}
	if cfg.NodeID == "" {
		localStore.Close()
		return nil, fmt.Errorf("raftstore: node id is required when raft is enabled")
	}

	listener, err := net.Listen("tcp", strings.TrimSpace(cfg.RaftAddress))
	if err != nil {
		localStore.Close()
		return nil, fmt.Errorf("raftstore: listen on %s: %w", cfg.RaftAddress, err)
	}

	// Prefer the operator-configured advertised address. If none is set, use
	// the actual listener address. Never resolve DNS here: a CoreDNS outage
	// must not prevent a node from starting.
	localRaftAddress := strings.TrimSpace(cfg.RaftAdvertise)
	if localRaftAddress == "" {
		localRaftAddress = listener.Addr().String()
	}
	if host, port, splitErr := net.SplitHostPort(localRaftAddress); splitErr == nil {
		if host == "" || host == "0.0.0.0" || host == "::" || host == "[::]" {
			if podIP := strings.TrimSpace(os.Getenv("POD_IP")); podIP != "" {
				localRaftAddress = net.JoinHostPort(podIP, port)
			} else {
				_ = listener.Close()
				localStore.Close()
				return nil, fmt.Errorf(
					"raftstore: advertised raft address %q is not routable; set --kvs-raft-advertise-addr or POD_IP",
					localRaftAddress,
				)
			}
		}
	}
	store.registerRaftEndpoint(raft.ServerAddress(localRaftAddress), cfg.NodeID, cfg.APIAddress)
	store.registerRaftAliases(localRaftAddress, cfg.NodeID, cfg.APIAddress, cfg.Peers)

	stream := &tcpStreamLayer{
		listener:  listener,
		advertise: staticAddr{value: localRaftAddress},
	}
	transport := raft.NewNetworkTransport(stream, 3, cfg.TransportTimeout, cfg.LogOutput)
	store.transport = transport

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

func (s *Store) registerRaftEndpoint(addr raft.ServerAddress, nodeID, api string) {
	if addr == "" {
		return
	}
	if nodeID != "" {
		s.nodeByRaft[addr] = nodeID
	}
	if api != "" {
		s.apiByRaft[addr] = api
	}
}

func (s *Store) registerRaftAliases(localAddr, localNodeID, localAPI string, peers []Peer) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.registerResolvedAlias(ctx, localAddr, localNodeID, localAPI)
	for _, peer := range peers {
		s.registerResolvedAlias(ctx, peer.RaftAddress, peer.NodeID, peer.APIAddress)
	}
}

// registerResolvedAlias records the IP form of a raft hostname so existing
// snapshots/configuration that still contain IP addresses keep working. DNS
// failures are intentionally ignored; the advertised name remains usable.
func (s *Store) registerResolvedAlias(ctx context.Context, address, nodeID, api string) {
	address = strings.TrimSpace(address)
	if address == "" {
		return
	}
	host, port, err := net.SplitHostPort(address)
	if err != nil || host == "" || net.ParseIP(host) != nil {
		return
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return
	}
	for _, ip := range ips {
		s.registerRaftEndpoint(raft.ServerAddress(net.JoinHostPort(ip.String(), port)), nodeID, api)
	}
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
	} else if s.transport != nil {
		if err := s.transport.Close(); err != nil {
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
	if leaderID == "" && leaderAddr != "" {
		leaderID = s.leaderNodeIDByAddress(leaderAddr)
	}
	healthy := false
	switch state {
	case raft.Leader:
		healthy = true
	case raft.Follower:
		healthy = leaderAddr != ""
	}

	var term uint64
	if raw := s.raft.Stats()["term"]; raw != "" {
		term, _ = strconv.ParseUint(raw, 10, 64)
	}
	leaderTermGauge.Set(float64(term))
	if state == raft.Leader {
		isLeaderGauge.Set(1)
	} else {
		isLeaderGauge.Set(0)
	}

	return kvsapi.StoreStatus{
		Enabled:       true,
		Healthy:       healthy,
		Mode:          "raft",
		Role:          role,
		LeaderID:      leaderID,
		LeaderAddress: string(leaderAddr),
		Term:          term,
		PeerCount:     peerCount,
		KeyCount:      s.local.KeyCount(),
	}
}

func (s *Store) leaderNodeIDByAddress(address raft.ServerAddress) string {
	if address == "" || s.raft == nil {
		return ""
	}
	future := s.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return ""
	}
	for _, server := range future.Configuration().Servers {
		if server.Address == address {
			return string(server.ID)
		}
	}
	return ""
}

func (s *Store) ReadFile(ctx context.Context, logicalPath string) ([]byte, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: logicalPath})
		if err != nil {
			return nil, mapReadError(err)
		}
		return resp.GetContent(), nil
	}
	return s.local.ReadFile(ctx, logicalPath)
}

func (s *Store) ListDir(ctx context.Context, logicalDir string) ([]kvsapi.DirEntry, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.ListDir(ctx, &kvsv1.ListDirRequest{LogicalDir: logicalDir})
		if err != nil {
			return nil, mapReadError(err)
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
			return kvsapi.FileInfo{}, mapReadError(err)
		}
		info := resp.GetInfo()
		return kvsapi.FileInfo{Path: info.GetPath(), Size: info.GetSize(), VersionID: info.GetVersionId(), ModTime: info.GetModTime().AsTime()}, nil
	}
	return s.local.Stat(ctx, logicalPath)
}

// mapReadError normalizes errors from a forwarded read. A follower forwards
// reads to the leader over gRPC; without this mapping a missing key would
// surface as codes.NotFound instead of the os.ErrNotExist contract used by the
// local store and by Guardian's history readers.
func mapReadError(err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) == codes.NotFound {
		return os.ErrNotExist
	}
	return err
}

func (s *Store) Watch(ctx context.Context, prefixes []string) (<-chan kvsapi.ChangeEvent, error) {
	return s.local.Watch(ctx, prefixes)
}

func (s *Store) UpsertFiles(ctx context.Context, batch kvsapi.MutationBatch) (kvsapi.BatchRevision, error) {
	if s.raft == nil {
		return s.local.UpsertFiles(ctx, batch)
	}
	return s.mutateWithLeader(ctx,
		func() (kvsapi.BatchRevision, error) {
			if err := s.local.ValidateUpsertBatch(ctx, batch); err != nil {
				return kvsapi.BatchRevision{}, err
			}
			return s.applyCommand(commandEnvelope{Op: "upsert", MutationBatch: &batch})
		},
		func(client kvsv1.KVStoreClient) (kvsapi.BatchRevision, error) {
			resp, err := client.UpsertFiles(ctx, protoUpsertBatch(batch))
			if err != nil {
				return kvsapi.BatchRevision{}, err
			}
			return batchRevisionFromProto(resp), nil
		},
	)
}

func (s *Store) DeletePaths(ctx context.Context, batch kvsapi.DeleteBatch) (kvsapi.BatchRevision, error) {
	if s.raft == nil {
		return s.local.DeletePaths(ctx, batch)
	}
	return s.mutateWithLeader(ctx,
		func() (kvsapi.BatchRevision, error) {
			if err := s.local.ValidateDeleteBatch(ctx, batch); err != nil {
				return kvsapi.BatchRevision{}, err
			}
			return s.applyCommand(commandEnvelope{Op: "delete", DeleteBatch: &batch})
		},
		func(client kvsv1.KVStoreClient) (kvsapi.BatchRevision, error) {
			resp, err := client.DeletePaths(ctx, protoDeleteBatch(batch))
			if err != nil {
				return kvsapi.BatchRevision{}, err
			}
			return batchRevisionFromProto(resp), nil
		},
	)
}

const maxLeaderAttempts = 6

// mutateWithLeader commits a mutation exactly once. Followers always forward;
// they never fall through to a local raft.Apply, which would surface a
// misleading "not the leader" error and could apply against stale state. If
// leadership moves between the role check and the apply, the mutation is
// retried against the newly elected leader.
func (s *Store) mutateWithLeader(
	ctx context.Context,
	local func() (kvsapi.BatchRevision, error),
	forward func(kvsv1.KVStoreClient) (kvsapi.BatchRevision, error),
) (kvsapi.BatchRevision, error) {
	var lastErr error
	for attempt := 0; attempt < maxLeaderAttempts; attempt++ {
		if attempt > 0 {
			writeRetryTotal.Inc()
			if err := waitForRetry(ctx, attempt); err != nil {
				return kvsapi.BatchRevision{}, err
			}
		}

		client, isLocal, err := s.leaderClient()
		if err != nil {
			lastErr = err
			notLeaderTotal.Inc()
			continue
		}
		if isLocal {
			revision, err := local()
			if err == nil {
				return revision, nil
			}
			lastErr = wrapNotLeader(err)
			if !isNotLeaderError(err) {
				return kvsapi.BatchRevision{}, err
			}
			notLeaderTotal.Inc()
			continue
		}

		revision, err := forward(client)
		if err == nil {
			return revision, nil
		}
		lastErr = wrapNotLeader(err)
		if !isNotLeaderError(err) && !isNotLeaderStatus(err) {
			return kvsapi.BatchRevision{}, err
		}
		notLeaderTotal.Inc()
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("%w: no leader available", kvsapi.ErrLeaderUnknown)
	}
	return kvsapi.BatchRevision{}, lastErr
}

func (s *Store) leaderClient() (kvsv1.KVStoreClient, bool, error) {
	if s.raft == nil {
		return nil, true, nil
	}
	if s.raft.State() == raft.Leader {
		return nil, true, nil
	}
	endpoint, nodeID, err := s.leaderEndpoint()
	if err != nil {
		forwardFailureTotal.Inc()
		return nil, false, err
	}
	client, err := s.clientFor(endpoint)
	if err != nil {
		return nil, false, fmt.Errorf(
			"%w: dial leader %s (%s): %v",
			kvsapi.ErrLeaderUnknown, nodeID, endpoint, err,
		)
	}
	return client, false, nil
}

func (s *Store) leaderEndpoint() (string, string, error) {
	if s.raft == nil {
		return "", "", nil
	}
	leaderAddr := s.raft.Leader()
	if leaderAddr == "" {
		return "", "", fmt.Errorf("%w: no leader elected", kvsapi.ErrLeaderUnknown)
	}
	nodeID := s.nodeByRaft[leaderAddr]
	endpoint := s.apiByRaft[leaderAddr]
	if endpoint == "" && nodeID != "" {
		endpoint = s.apiByNode[nodeID]
	}
	if endpoint == "" {
		nodeID = s.leaderNodeIDByAddress(leaderAddr)
		if nodeID != "" {
			endpoint = s.apiByNode[nodeID]
		}
	}
	if endpoint == "" {
		return "", nodeID, fmt.Errorf(
			"%w: leader %s has no API endpoint",
			kvsapi.ErrLeaderUnknown, leaderAddr,
		)
	}
	return endpoint, nodeID, nil
}

func waitForRetry(ctx context.Context, attempt int) error {
	delay := 100 * time.Millisecond
	for i := 1; i < attempt && delay < 800*time.Millisecond; i++ {
		delay *= 2
	}
	if delay > 800*time.Millisecond {
		delay = 800 * time.Millisecond
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isNotLeaderError(err error) bool {
	return errors.Is(err, raft.ErrNotLeader) ||
		errors.Is(err, raft.ErrLeadershipLost) ||
		errors.Is(err, raft.ErrLeadershipTransferInProgress) ||
		errors.Is(err, kvsapi.ErrNotLeader) ||
		errors.Is(err, kvsapi.ErrLeaderUnknown)
}

func wrapNotLeader(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, kvsapi.ErrNotLeader) || errors.Is(err, kvsapi.ErrLeaderUnknown) {
		return err
	}
	return fmt.Errorf("%w: %v", kvsapi.ErrNotLeader, err)
}

func isNotLeaderStatus(err error) bool {
	switch status.Code(err) {
	case codes.Aborted, codes.Unavailable:
		return true
	case codes.Internal:
		// Rolling upgrades may still be talking to an older node that wraps
		// raft.ErrNotLeader in codes.Internal.
		return strings.Contains(err.Error(), "not the leader")
	}
	return false
}

func (s *Store) ListVersions(ctx context.Context, logicalPath string) ([]kvsapi.FileVersion, error) {
	if client, err := s.forwardClient(); err == nil {
		resp, err := client.ListVersions(ctx, &kvsv1.ListVersionsRequest{LogicalPath: logicalPath})
		if err != nil {
			return nil, mapReadError(err)
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
			return kvsapi.VersionedFile{}, mapReadError(err)
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
	client, isLocal, err := s.leaderClient()
	if err != nil {
		return nil, err
	}
	if isLocal {
		return nil, raft.ErrNotLeader
	}
	return client, nil
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
