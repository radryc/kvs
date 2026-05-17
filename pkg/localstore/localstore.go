package localstore

import (
	"context"
	"time"

	"github.com/rydzu/ainfra/kvs/internal/store/local"
	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
)

type Config struct {
	DataDir             string
	MaxHotVersions      int
	MaxArchivedVersions int // 0 = unlimited; ignored when Offloader is set
	WatcherQueueSize    int
	Offloader           kvsapi.FetcherOffloader // optional; routes archived blobs to fetcher
}

type Store struct {
	inner *local.Store
}

func Open(cfg Config) (*Store, error) {
	inner, err := local.Open(local.Config{
		DataDir:             cfg.DataDir,
		MaxHotVersions:      cfg.MaxHotVersions,
		MaxArchivedVersions: cfg.MaxArchivedVersions,
		WatcherQueueSize:    cfg.WatcherQueueSize,
		Offloader:           cfg.Offloader,
	})
	if err != nil {
		return nil, err
	}
	return &Store{inner: inner}, nil
}

func (s *Store) Close() error {
	if s == nil || s.inner == nil {
		return nil
	}
	return s.inner.Close()
}

func (s *Store) AsStore() kvsapi.Store {
	if s == nil {
		return nil
	}
	return s.inner
}

func (s *Store) Status() kvsapi.StoreStatus {
	if s == nil || s.inner == nil {
		return kvsapi.StoreStatus{Mode: "disabled", Role: "disabled"}
	}
	return kvsapi.StoreStatus{
		Enabled:   true,
		Healthy:   true,
		Mode:      "local",
		Role:      "local",
		PeerCount: 1,
		KeyCount:  s.inner.KeyCount(),
	}
}

func (s *Store) ReadFile(ctx context.Context, logicalPath string) ([]byte, error) {
	return s.inner.ReadFile(ctx, logicalPath)
}

func (s *Store) ListDir(ctx context.Context, logicalDir string) ([]kvsapi.DirEntry, error) {
	return s.inner.ListDir(ctx, logicalDir)
}

func (s *Store) Stat(ctx context.Context, logicalPath string) (kvsapi.FileInfo, error) {
	return s.inner.Stat(ctx, logicalPath)
}

func (s *Store) Watch(ctx context.Context, prefixes []string) (<-chan kvsapi.ChangeEvent, error) {
	return s.inner.Watch(ctx, prefixes)
}

func (s *Store) UpsertFiles(ctx context.Context, batch kvsapi.MutationBatch) (kvsapi.BatchRevision, error) {
	return s.inner.UpsertFiles(ctx, batch)
}

func (s *Store) DeletePaths(ctx context.Context, batch kvsapi.DeleteBatch) (kvsapi.BatchRevision, error) {
	return s.inner.DeletePaths(ctx, batch)
}

func (s *Store) ListVersions(ctx context.Context, logicalPath string) ([]kvsapi.FileVersion, error) {
	return s.inner.ListVersions(ctx, logicalPath)
}

func (s *Store) GetVersion(ctx context.Context, logicalPath, versionID string) (kvsapi.VersionedFile, error) {
	return s.inner.GetVersion(ctx, logicalPath, versionID)
}

// StartPurge runs a background goroutine that periodically moves old hot-storage
// blobs to the archive tier.  It delegates directly to the underlying local store.
func (s *Store) StartPurge(ctx context.Context, interval time.Duration) {
	if s == nil || s.inner == nil {
		return
	}
	s.inner.StartPurge(ctx, interval)
}
