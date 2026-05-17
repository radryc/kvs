package local

import (
	"archive/tar"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/rydzu/ainfra/kvs/internal/versioning/digest"
	"github.com/rydzu/ainfra/kvs/internal/versioning/revisions"
	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
)

const (
	defaultWatcherQueueSize = 256
	storageHot              = "hot"
	storageArchive          = "archive"
	storageOffloaded        = "offloaded"
)

type Config struct {
	DataDir             string
	MaxHotVersions      int
	MaxArchivedVersions int // 0 = unlimited; ignored when Offloader is set
	WatcherQueueSize    int
	Offloader           kvsapi.FetcherOffloader // optional; routes archived blobs to fetcher
}

type PurgeReport struct {
	ArchivedVersions  int
	DeletedVersions   int
	OffloadedVersions int
}

type Store struct {
	db                  *pebble.DB
	dataDir             string
	hotDir              string
	archiveDir          string
	maxHotVersions      int
	maxArchivedVersions int
	watcherQueueSize    int
	offloader           kvsapi.FetcherOffloader

	writeMu   sync.Mutex
	watchMu   sync.Mutex
	watchers  map[uint64]*watchSubscription
	nextWatch uint64
	closed    bool
	keyCount  atomic.Int64
}

type watchSubscription struct {
	prefixes []string
	ch       chan kvsapi.ChangeEvent
}

type versionManifest struct {
	Version           kvsapi.FileVersion `json:"version"`
	PreviousVersionID string             `json:"previousVersionID,omitempty"`
	SizeBytes         int64              `json:"sizeBytes"`
	BlobPath          string             `json:"blobPath,omitempty"`
	StorageClass      string             `json:"storageClass"`
}

func Open(cfg Config) (*Store, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("local: data dir is required")
	}
	if cfg.MaxHotVersions <= 0 {
		return nil, fmt.Errorf("local: max hot versions must be greater than zero")
	}
	if cfg.WatcherQueueSize <= 0 {
		cfg.WatcherQueueSize = defaultWatcherQueueSize
	}

	hotDir := filepath.Join(cfg.DataDir, "blobs", "hot")
	archiveDir := filepath.Join(cfg.DataDir, "blobs", "archive")
	metaDir := filepath.Join(cfg.DataDir, "meta")

	if err := ensureDataDirs(cfg.DataDir); err != nil {
		return nil, err
	}

	db, err := openMetaDB(metaDir)
	if err != nil {
		return nil, err
	}

	store := &Store{
		db:                  db,
		dataDir:             cfg.DataDir,
		hotDir:              hotDir,
		archiveDir:          archiveDir,
		maxHotVersions:      cfg.MaxHotVersions,
		maxArchivedVersions: cfg.MaxArchivedVersions,
		watcherQueueSize:    cfg.WatcherQueueSize,
		offloader:           cfg.Offloader,
		watchers:            make(map[uint64]*watchSubscription),
	}

	keyCount, err := store.countActiveKeys()
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	store.keyCount.Store(keyCount)
	kvsActiveKeysGauge.Add(float64(keyCount))

	return store, nil
}

func ensureDataDirs(dataDir string) error {
	for _, dir := range []string{
		filepath.Join(dataDir, "blobs", "hot"),
		filepath.Join(dataDir, "blobs", "archive"),
		filepath.Join(dataDir, "meta"),
	} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	return nil
}

func openMetaDB(metaDir string) (*pebble.DB, error) {
	return pebble.Open(metaDir, &pebble.Options{
		// Raise L0 thresholds to batch more writes before compaction fires,
		// reducing write-amplification from the default 4/12 values.
		L0CompactionThreshold: 8,
		L0StopWritesThreshold: 24,
		// Larger MemTable absorbs more writes before flushing to L0,
		// keeping L0 files larger and fewer.
		MemTableSize: 16 * 1024 * 1024, // 16 MB
	})
}

func (s *Store) Close() error {
	s.watchMu.Lock()
	if s.closed {
		s.watchMu.Unlock()
		return nil
	}
	s.closed = true
	for id, watcher := range s.watchers {
		close(watcher.ch)
		delete(s.watchers, id)
	}
	s.watchMu.Unlock()
	return s.db.Close()
}

func (s *Store) KeyCount() int64 {
	if s == nil {
		return 0
	}
	return s.keyCount.Load()
}

func (s *Store) ValidateUpsertBatch(ctx context.Context, batch kvsapi.MutationBatch) error {
	_ = ctx
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.validateWrites(batch)
}

func (s *Store) ValidateDeleteBatch(ctx context.Context, batch kvsapi.DeleteBatch) error {
	_ = ctx
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.validateDeletes(batch)
}

func (s *Store) Snapshot(w io.Writer) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.db != nil {
		if err := s.db.Flush(); err != nil {
			return err
		}
	}

	tw := tar.NewWriter(w)
	defer tw.Close()

	return filepath.Walk(s.dataDir, func(currentPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if currentPath == s.dataDir {
			return nil
		}
		// Skip archive blobs: they are offloaded to the fetcher tier and
		// would bloat snapshots by hundreds of MB.
		if info.IsDir() && currentPath == s.archiveDir {
			return filepath.SkipDir
		}

		relPath, err := filepath.Rel(s.dataDir, currentPath)
		if err != nil {
			return err
		}
		relPath = filepath.ToSlash(relPath)

		header, err := tar.FileInfoHeader(info, "")
		if err != nil {
			return err
		}
		header.Name = relPath
		if err := tw.WriteHeader(header); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}

		file, err := os.Open(currentPath)
		if err != nil {
			return err
		}
		defer file.Close()

		_, err = io.Copy(tw, file)
		return err
	})
}

func (s *Store) Restore(r io.Reader) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	if s.db != nil {
		if err := s.db.Close(); err != nil {
			return err
		}
		s.db = nil
	}

	if err := os.RemoveAll(s.dataDir); err != nil {
		return err
	}
	if err := ensureDataDirs(s.dataDir); err != nil {
		return err
	}

	tr := tar.NewReader(r)
	for {
		header, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}

		cleanName := filepath.Clean(header.Name)
		targetPath := filepath.Join(s.dataDir, cleanName)
		if !strings.HasPrefix(targetPath, s.dataDir+string(filepath.Separator)) && targetPath != s.dataDir {
			return fmt.Errorf("local: snapshot entry escapes data dir: %s", header.Name)
		}

		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(targetPath, os.FileMode(header.Mode)); err != nil {
				return err
			}
		case tar.TypeReg, tar.TypeRegA:
			if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
				return err
			}
			file, err := os.OpenFile(targetPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, os.FileMode(header.Mode))
			if err != nil {
				return err
			}
			if _, err := io.Copy(file, tr); err != nil {
				file.Close()
				return err
			}
			if err := file.Close(); err != nil {
				return err
			}
		default:
			return fmt.Errorf("local: unsupported snapshot entry type %d", header.Typeflag)
		}
	}

	db, err := openMetaDB(filepath.Join(s.dataDir, "meta"))
	if err != nil {
		return err
	}
	s.db = db
	s.hotDir = filepath.Join(s.dataDir, "blobs", "hot")
	s.archiveDir = filepath.Join(s.dataDir, "blobs", "archive")
	keyCount, err := s.countActiveKeys()
	if err != nil {
		return err
	}
	s.keyCount.Store(keyCount)
	return nil
}

func (s *Store) ReadFile(ctx context.Context, logicalPath string) ([]byte, error) {
	_ = ctx
	normalized, err := normalizePath(logicalPath)
	if err != nil {
		return nil, err
	}

	manifest, err := s.loadLatestManifest(normalized)
	if err != nil {
		return nil, err
	}
	if manifest.Version.Tombstone {
		return nil, fs.ErrNotExist
	}
	data, err := s.readBlob(manifest)
	if err == nil {
		kvsReadOpsTotal.WithLabelValues("read_file").Inc()
		kvsReadBytesTotal.Add(float64(len(data)))
	}
	return data, err
}

func (s *Store) ListDir(ctx context.Context, logicalDir string) ([]kvsapi.DirEntry, error) {
	_ = ctx
	normalizedDir, err := normalizePath(logicalDir)
	if err != nil {
		return nil, err
	}

	children := make(map[string]kvsapi.DirEntry)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: latestPrefix(),
		UpperBound: prefixUpperBound(latestPrefix()),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		manifest, err := decodeManifest(iter.Value())
		if err != nil {
			return nil, err
		}
		if manifest.Version.Tombstone {
			continue
		}
		name, isDir, ok := childOf(normalizedDir, manifest.Version.LogicalPath)
		if !ok {
			continue
		}
		entry, exists := children[name]
		if !exists {
			entry = kvsapi.DirEntry{Name: name}
		}
		if isDir {
			entry.IsDir = true
			entry.Size = 0
		} else if !entry.IsDir {
			entry.Size = manifest.SizeBytes
		}
		children[name] = entry
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}

	entries := make([]kvsapi.DirEntry, 0, len(children))
	for _, entry := range children {
		entries = append(entries, entry)
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return entries[i].Name < entries[j].Name
	})
	kvsReadOpsTotal.WithLabelValues("list_dir").Inc()
	return entries, nil
}

func (s *Store) Stat(ctx context.Context, logicalPath string) (kvsapi.FileInfo, error) {
	_ = ctx
	normalized, err := normalizePath(logicalPath)
	if err != nil {
		return kvsapi.FileInfo{}, err
	}
	manifest, err := s.loadLatestManifest(normalized)
	if err != nil {
		return kvsapi.FileInfo{}, err
	}
	if manifest.Version.Tombstone {
		return kvsapi.FileInfo{}, fs.ErrNotExist
	}
	kvsReadOpsTotal.WithLabelValues("stat").Inc()
	return kvsapi.FileInfo{
		Path:      normalized,
		Size:      manifest.SizeBytes,
		VersionID: manifest.Version.VersionID,
		ModTime:   manifest.Version.CommittedAt,
	}, nil
}

func (s *Store) Watch(ctx context.Context, prefixes []string) (<-chan kvsapi.ChangeEvent, error) {
	normalized := make([]string, 0, len(prefixes))
	for _, prefix := range prefixes {
		cleaned, err := normalizePath(prefix)
		if err != nil {
			return nil, err
		}
		normalized = append(normalized, cleaned)
	}

	ch := make(chan kvsapi.ChangeEvent, s.watcherQueueSize)
	s.watchMu.Lock()
	if s.closed {
		s.watchMu.Unlock()
		close(ch)
		return ch, nil
	}
	id := s.nextWatch
	s.nextWatch++
	s.watchers[id] = &watchSubscription{prefixes: normalized, ch: ch}
	s.watchMu.Unlock()

	go func() {
		<-ctx.Done()
		s.watchMu.Lock()
		watcher, ok := s.watchers[id]
		if ok {
			delete(s.watchers, id)
			close(watcher.ch)
		}
		s.watchMu.Unlock()
	}()

	return ch, nil
}

func (s *Store) UpsertFiles(ctx context.Context, batch kvsapi.MutationBatch) (kvsapi.BatchRevision, error) {
	_ = ctx
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	prepared, err := s.prepareWrites(batch)
	if err != nil {
		return kvsapi.BatchRevision{}, err
	}
	if len(prepared.manifests) == 0 {
		return kvsapi.BatchRevision{BatchRevisionID: prepared.batchRevisionID}, nil
	}

	pbBatch := s.db.NewBatch()
	defer pbBatch.Close()
	for _, manifest := range prepared.manifests {
		encoded, err := encodeManifest(manifest)
		if err != nil {
			s.cleanupFiles(prepared.createdFiles)
			return kvsapi.BatchRevision{}, err
		}
		if err := pbBatch.Set(versionKey(manifest.Version.LogicalPath, manifest.Version.VersionID), encoded, nil); err != nil {
			s.cleanupFiles(prepared.createdFiles)
			return kvsapi.BatchRevision{}, err
		}
		if err := pbBatch.Set(latestKey(manifest.Version.LogicalPath), encoded, nil); err != nil {
			s.cleanupFiles(prepared.createdFiles)
			return kvsapi.BatchRevision{}, err
		}
	}
	if err := pbBatch.Commit(pebble.Sync); err != nil {
		s.cleanupFiles(prepared.createdFiles)
		return kvsapi.BatchRevision{}, err
	}
	if prepared.keyCountDelta != 0 {
		s.keyCount.Add(prepared.keyCountDelta)
		kvsActiveKeysGauge.Add(float64(prepared.keyCountDelta))
	}
	kvsWriteFilesTotal.WithLabelValues("upsert").Add(float64(len(prepared.manifests)))
	kvsPebbleBatchCommitsTotal.WithLabelValues("upsert").Inc()

	s.publishEvents(prepared.events)
	return kvsapi.BatchRevision{
		BatchRevisionID: prepared.batchRevisionID,
		Files:           prepared.versions,
	}, nil
}

func (s *Store) DeletePaths(ctx context.Context, batch kvsapi.DeleteBatch) (kvsapi.BatchRevision, error) {
	_ = ctx
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	batchRevisionID := revisions.NewBatchRevisionID()
	committedAt := time.Now().UTC()
	result := kvsapi.BatchRevision{BatchRevisionID: batchRevisionID}
	events := make([]kvsapi.ChangeEvent, 0, len(batch.Deletes))
	var deletedKeyCount int64

	pbBatch := s.db.NewBatch()
	defer pbBatch.Close()

	for _, deleteReq := range batch.Deletes {
		normalized, err := normalizePath(deleteReq.LogicalPath)
		if err != nil {
			return kvsapi.BatchRevision{}, err
		}
		latest, err := s.loadLatestManifest(normalized)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return kvsapi.BatchRevision{}, err
		}
		if err := validateExpectedVersion(latest, deleteReq.ExpectedVersionID); err != nil {
			return kvsapi.BatchRevision{}, err
		}
		if latest == nil || latest.Version.Tombstone {
			continue
		}
		deletedKeyCount++

		versionID := revisions.NewVersionID()
		manifest := versionManifest{
			Version: kvsapi.FileVersion{
				LogicalPath:     normalized,
				VersionID:       versionID,
				BatchRevisionID: batchRevisionID,
				CommittedAt:     committedAt,
				Tombstone:       true,
				PrincipalID:     batch.Context.PrincipalID,
				Reason:          batch.Context.Reason,
			},
			PreviousVersionID: latest.Version.VersionID,
			StorageClass:      storageHot,
		}
		encoded, err := encodeManifest(manifest)
		if err != nil {
			return kvsapi.BatchRevision{}, err
		}
		if err := pbBatch.Set(versionKey(normalized, versionID), encoded, nil); err != nil {
			return kvsapi.BatchRevision{}, err
		}
		if err := pbBatch.Set(latestKey(normalized), encoded, nil); err != nil {
			return kvsapi.BatchRevision{}, err
		}
		result.Files = append(result.Files, manifest.Version)
		events = append(events, kvsapi.ChangeEvent{
			LogicalPath:     normalized,
			Type:            kvsapi.ChangeDeleted,
			VersionID:       versionID,
			BatchRevisionID: batchRevisionID,
			CommittedAt:     committedAt,
		})
	}

	if len(result.Files) == 0 {
		return result, nil
	}
	if err := pbBatch.Commit(pebble.Sync); err != nil {
		return kvsapi.BatchRevision{}, err
	}
	if deletedKeyCount != 0 {
		s.keyCount.Add(-deletedKeyCount)
		kvsActiveKeysGauge.Sub(float64(deletedKeyCount))
	}
	kvsWriteFilesTotal.WithLabelValues("delete").Add(float64(len(result.Files)))
	kvsPebbleBatchCommitsTotal.WithLabelValues("delete").Inc()

	s.publishEvents(events)
	return result, nil
}

func (s *Store) ListVersions(ctx context.Context, logicalPath string) ([]kvsapi.FileVersion, error) {
	_ = ctx
	manifests, err := s.listVersionManifests(logicalPath)
	if err != nil {
		return nil, err
	}
	versions := make([]kvsapi.FileVersion, 0, len(manifests))
	for _, manifest := range manifests {
		versions = append(versions, manifest.Version)
	}
	return versions, nil
}

func (s *Store) GetVersion(ctx context.Context, logicalPath, versionID string) (kvsapi.VersionedFile, error) {
	_ = ctx
	normalized, err := normalizePath(logicalPath)
	if err != nil {
		return kvsapi.VersionedFile{}, err
	}
	manifest, err := s.loadManifest(versionKey(normalized, versionID))
	if err != nil {
		return kvsapi.VersionedFile{}, err
	}
	if manifest.Version.Tombstone {
		return kvsapi.VersionedFile{Version: manifest.Version}, nil
	}
	content, err := s.readBlob(manifest)
	if err != nil {
		return kvsapi.VersionedFile{}, err
	}
	return kvsapi.VersionedFile{Version: manifest.Version, Content: content}, nil
}

func (s *Store) Purge(ctx context.Context) (PurgeReport, error) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	paths, err := s.listTrackedPaths()
	if err != nil {
		return PurgeReport{}, err
	}

	var report PurgeReport
	pbBatch := s.db.NewBatch()
	defer pbBatch.Close()

	// blobFilesToDelete collects blob file paths to remove after the
	// metadata batch commits, so we never lose a file that is still referenced.
	var blobFilesToDelete []string
	changed := false

	for _, logicalPath := range paths {
		manifests, err := s.listVersionManifests(logicalPath)
		if err != nil {
			return PurgeReport{}, err
		}

		// manifests is sorted newest-first; liveCount tracks non-tombstone versions.
		liveCount := 0
		for _, manifest := range manifests {
			if manifest.Version.Tombstone {
				continue
			}
			liveCount++

			if liveCount <= s.maxHotVersions {
				// Keep in hot tier.
				continue
			}

			// Beyond the hot limit: offload to fetcher if available, otherwise
			// fall back to the local archive tier.
			if s.offloader != nil {
				if manifest.StorageClass == storageOffloaded {
					continue // already offloaded
				}
				// Read the blob (whether it lives in hot or archive locally).
				if manifest.BlobPath == "" {
					continue // no local file; nothing to offload
				}
				content, readErr := os.ReadFile(manifest.BlobPath)
				if readErr != nil {
					continue // skip; retry next cycle
				}
				hash := manifest.Version.ContentSHA256
				if hash == "" {
					hash = digest.ContentHash(content)
				}
				if offloadErr := s.offloader.StoreBlob(ctx, hash, content); offloadErr != nil {
					continue // upload failed; keep local, retry next cycle
				}
				blobFilesToDelete = append(blobFilesToDelete, manifest.BlobPath)
				manifest.BlobPath = ""
				manifest.StorageClass = storageOffloaded
				manifest.Version.ContentSHA256 = hash
				encoded, encErr := encodeManifest(manifest)
				if encErr != nil {
					return PurgeReport{}, encErr
				}
				if err := pbBatch.Set(versionKey(logicalPath, manifest.Version.VersionID), encoded, nil); err != nil {
					return PurgeReport{}, err
				}
				report.OffloadedVersions++
				changed = true
				continue
			}

			// No offloader: use local archive-then-delete tiers.
			switch {
			case s.maxArchivedVersions <= 0 || liveCount <= s.maxHotVersions+s.maxArchivedVersions:
				// Archive tier: move hot→archive if not already there.
				if manifest.StorageClass != storageArchive {
					archivePath := s.archiveBlobPath(manifest.Version.VersionID)
					if err := moveFileAtomic(manifest.BlobPath, archivePath); err != nil {
						return PurgeReport{}, err
					}
					manifest.StorageClass = storageArchive
					manifest.BlobPath = archivePath
					encoded, err := encodeManifest(manifest)
					if err != nil {
						return PurgeReport{}, err
					}
					if err := pbBatch.Set(versionKey(logicalPath, manifest.Version.VersionID), encoded, nil); err != nil {
						return PurgeReport{}, err
					}
					report.ArchivedVersions++
					changed = true
				}

			default:
				// Beyond maxArchivedVersions: delete the archive blob file.
				if manifest.BlobPath != "" {
					blobFilesToDelete = append(blobFilesToDelete, manifest.BlobPath)
					manifest.BlobPath = ""
					encoded, err := encodeManifest(manifest)
					if err != nil {
						return PurgeReport{}, err
					}
					if err := pbBatch.Set(versionKey(logicalPath, manifest.Version.VersionID), encoded, nil); err != nil {
						return PurgeReport{}, err
					}
					report.DeletedVersions++
					changed = true
				}
			}
		}
	}

	if !changed {
		return PurgeReport{}, nil
	}
	if err := pbBatch.Commit(pebble.Sync); err != nil {
		return PurgeReport{}, err
	}
	// Delete local files only after metadata commit succeeds.
	for _, path := range blobFilesToDelete {
		_ = os.Remove(path)
	}

	kvsPurgeArchivedTotal.Add(float64(report.ArchivedVersions))
	kvsPurgeDeletedTotal.Add(float64(report.DeletedVersions))
	kvsPurgeOffloadedTotal.Add(float64(report.OffloadedVersions))
	kvsPebbleBatchCommitsTotal.WithLabelValues("purge").Inc()
	return report, nil
}

func (s *Store) prepareWrites(batch kvsapi.MutationBatch) (*preparedWriteBatch, error) {
	prepared := &preparedWriteBatch{
		batchRevisionID: revisions.NewBatchRevisionID(),
		manifests:       make([]versionManifest, 0, len(batch.Writes)),
		versions:        make([]kvsapi.FileVersion, 0, len(batch.Writes)),
		events:          make([]kvsapi.ChangeEvent, 0, len(batch.Writes)),
	}
	if len(batch.Writes) == 0 {
		return prepared, nil
	}
	if err := s.validateWrites(batch); err != nil {
		return nil, err
	}

	committedAt := time.Now().UTC()
	for _, write := range batch.Writes {
		normalized, err := normalizePath(write.LogicalPath)
		if err != nil {
			return nil, err
		}

		latest, err := s.loadLatestManifest(normalized)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, err
		}

		versionID := revisions.NewVersionID()
		blobPath := s.hotBlobPath(versionID)
		if err := writeFileAtomic(blobPath, write.Content); err != nil {
			prepared.cleanupCreatedFiles()
			return nil, err
		}
		prepared.createdFiles = append(prepared.createdFiles, blobPath)
		kvsBlobsCreatedTotal.Inc()
		kvsWriteBytesTotal.Add(float64(len(write.Content)))

		manifest := versionManifest{
			Version: kvsapi.FileVersion{
				LogicalPath:     normalized,
				VersionID:       versionID,
				BatchRevisionID: prepared.batchRevisionID,
				ContentSHA256:   digest.ContentHash(write.Content),
				CommittedAt:     committedAt,
				PrincipalID:     batch.Context.PrincipalID,
				Reason:          batch.Context.Reason,
			},
			SizeBytes:    int64(len(write.Content)),
			BlobPath:     blobPath,
			StorageClass: storageHot,
		}
		if latest != nil {
			manifest.PreviousVersionID = latest.Version.VersionID
		}
		if latest == nil || latest.Version.Tombstone {
			prepared.keyCountDelta++
		}

		prepared.manifests = append(prepared.manifests, manifest)
		prepared.versions = append(prepared.versions, manifest.Version)
		eventType := kvsapi.ChangeAdded
		if latest != nil && !latest.Version.Tombstone {
			eventType = kvsapi.ChangeModified
		}
		prepared.events = append(prepared.events, kvsapi.ChangeEvent{
			LogicalPath:     normalized,
			Type:            eventType,
			VersionID:       versionID,
			BatchRevisionID: prepared.batchRevisionID,
			CommittedAt:     committedAt,
		})
	}

	return prepared, nil
}

func (s *Store) validateWrites(batch kvsapi.MutationBatch) error {
	seen := make(map[string]struct{}, len(batch.Writes))
	for _, write := range batch.Writes {
		normalized, err := normalizePath(write.LogicalPath)
		if err != nil {
			return err
		}
		if _, exists := seen[normalized]; exists {
			return fmt.Errorf("local: duplicate path in batch: %s", normalized)
		}
		seen[normalized] = struct{}{}

		latest, err := s.loadLatestManifest(normalized)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := validateExpectedVersion(latest, write.ExpectedVersionID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) validateDeletes(batch kvsapi.DeleteBatch) error {
	seen := make(map[string]struct{}, len(batch.Deletes))
	for _, deleteReq := range batch.Deletes {
		normalized, err := normalizePath(deleteReq.LogicalPath)
		if err != nil {
			return err
		}
		if _, exists := seen[normalized]; exists {
			return fmt.Errorf("local: duplicate path in delete batch: %s", normalized)
		}
		seen[normalized] = struct{}{}

		latest, err := s.loadLatestManifest(normalized)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := validateExpectedVersion(latest, deleteReq.ExpectedVersionID); err != nil {
			return err
		}
	}
	return nil
}

type preparedWriteBatch struct {
	batchRevisionID string
	manifests       []versionManifest
	versions        []kvsapi.FileVersion
	events          []kvsapi.ChangeEvent
	createdFiles    []string
	keyCountDelta   int64
}

func (p *preparedWriteBatch) cleanupCreatedFiles() {
	for _, filePath := range p.createdFiles {
		_ = os.Remove(filePath)
	}
}

func (s *Store) loadLatestManifest(logicalPath string) (*versionManifest, error) {
	return s.loadManifest(latestKey(logicalPath))
}

func (s *Store) loadManifest(key []byte) (*versionManifest, error) {
	value, closer, err := s.db.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, fs.ErrNotExist
		}
		return nil, err
	}
	defer closer.Close()
	manifest, err := decodeManifest(value)
	if err != nil {
		return nil, err
	}
	return &manifest, nil
}

func (s *Store) listVersionManifests(logicalPath string) ([]versionManifest, error) {
	normalized, err := normalizePath(logicalPath)
	if err != nil {
		return nil, err
	}
	prefix := versionPrefix(normalized)
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: prefix, UpperBound: prefixUpperBound(prefix)})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	manifests := make([]versionManifest, 0, 8)
	for iter.First(); iter.Valid(); iter.Next() {
		manifest, err := decodeManifest(iter.Value())
		if err != nil {
			return nil, err
		}
		manifests = append(manifests, manifest)
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	sort.Slice(manifests, func(i, j int) bool {
		return manifests[i].Version.CommittedAt.After(manifests[j].Version.CommittedAt)
	})
	return manifests, nil
}

func (s *Store) listTrackedPaths() ([]string, error) {
	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: latestPrefix(), UpperBound: prefixUpperBound(latestPrefix())})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	paths := make([]string, 0, 64)
	for iter.First(); iter.Valid(); iter.Next() {
		paths = append(paths, logicalPathFromLatestKey(iter.Key()))
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return paths, nil
}

func (s *Store) countActiveKeys() (int64, error) {
	if s == nil || s.db == nil {
		return 0, nil
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{LowerBound: latestPrefix(), UpperBound: prefixUpperBound(latestPrefix())})
	if err != nil {
		return 0, err
	}
	defer iter.Close()

	var count int64
	for iter.First(); iter.Valid(); iter.Next() {
		manifest, err := decodeManifest(iter.Value())
		if err != nil {
			return 0, err
		}
		if manifest.Version.Tombstone {
			continue
		}
		count++
	}
	if err := iter.Error(); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) readBlob(manifest *versionManifest) ([]byte, error) {
	if manifest.StorageClass == storageOffloaded {
		if s.offloader == nil {
			return nil, fmt.Errorf("blob %s is offloaded to fetcher but no offloader configured", manifest.Version.VersionID)
		}
		hash := manifest.Version.ContentSHA256
		if hash == "" {
			return nil, fmt.Errorf("offloaded blob %s has no content hash", manifest.Version.VersionID)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return s.offloader.FetchBlob(ctx, hash)
	}
	if manifest.BlobPath == "" {
		return nil, fs.ErrNotExist
	}
	content, err := os.ReadFile(manifest.BlobPath)
	if errors.Is(err, fs.ErrNotExist) && manifest.StorageClass == storageArchive {
		return nil, fs.ErrNotExist
	}
	return content, err
}

func (s *Store) publishEvents(events []kvsapi.ChangeEvent) {
	if len(events) == 0 {
		return
	}
	s.watchMu.Lock()
	defer s.watchMu.Unlock()

	for id, watcher := range s.watchers {
		remove := false
		for _, event := range events {
			if !watcherMatches(watcher.prefixes, event.LogicalPath) {
				continue
			}
			select {
			case watcher.ch <- event:
			default:
				close(watcher.ch)
				delete(s.watchers, id)
				remove = true
			}
			if remove {
				break
			}
		}
	}
}

func (s *Store) cleanupFiles(paths []string) {
	for _, filePath := range paths {
		_ = os.Remove(filePath)
	}
}

func (s *Store) hotBlobPath(versionID string) string {
	return filepath.Join(s.hotDir, versionID+".blob")
}

func (s *Store) archiveBlobPath(versionID string) string {
	return filepath.Join(s.archiveDir, versionID+".blob")
}

func validateExpectedVersion(latest *versionManifest, expectedVersionID string) error {
	if expectedVersionID == "" {
		return nil
	}
	if latest == nil || latest.Version.VersionID != expectedVersionID {
		return kvsapi.ErrConflict
	}
	return nil
}

func encodeManifest(manifest versionManifest) ([]byte, error) {
	return json.Marshal(manifest)
}

func decodeManifest(data []byte) (versionManifest, error) {
	var manifest versionManifest
	if err := json.Unmarshal(data, &manifest); err != nil {
		return versionManifest{}, err
	}
	return manifest, nil
}

func latestKey(logicalPath string) []byte {
	return []byte("latest\x00" + logicalPath)
}

func latestPrefix() []byte {
	return []byte("latest\x00")
}

func logicalPathFromLatestKey(key []byte) string {
	return strings.TrimPrefix(string(key), "latest\x00")
}

func versionKey(logicalPath, versionID string) []byte {
	return []byte("ver\x00" + logicalPath + "\x00" + versionID)
}

func versionPrefix(logicalPath string) []byte {
	return []byte("ver\x00" + logicalPath + "\x00")
}

func prefixUpperBound(prefix []byte) []byte {
	if len(prefix) == 0 {
		return nil
	}
	upper := append([]byte(nil), prefix...)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] < 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

func normalizePath(logicalPath string) (string, error) {
	if logicalPath == "" {
		return "", fmt.Errorf("local: logical path is required")
	}
	cleaned := path.Clean(logicalPath)
	if !strings.HasPrefix(cleaned, "/") {
		return "", fmt.Errorf("local: logical path must be absolute: %s", logicalPath)
	}
	return cleaned, nil
}

func childOf(dir, candidate string) (name string, isDir bool, ok bool) {
	if dir == candidate {
		return "", false, false
	}
	prefix := dir
	if prefix != "/" {
		prefix += "/"
	}
	if !strings.HasPrefix(candidate, prefix) {
		return "", false, false
	}
	remainder := strings.TrimPrefix(candidate, prefix)
	if remainder == "" {
		return "", false, false
	}
	parts := strings.SplitN(remainder, "/", 2)
	if len(parts) == 1 {
		return parts[0], false, true
	}
	return parts[0], true, true
}

func watcherMatches(prefixes []string, logicalPath string) bool {
	if len(prefixes) == 0 {
		return true
	}
	for _, prefix := range prefixes {
		if prefix == "/" || logicalPath == prefix || strings.HasPrefix(logicalPath, prefix+"/") {
			return true
		}
	}
	return false
}

func writeFileAtomic(filePath string, content []byte) error {
	if err := os.MkdirAll(filepath.Dir(filePath), 0o755); err != nil {
		return err
	}
	tempPath := filePath + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := file.Write(content); err != nil {
		file.Close()
		_ = os.Remove(tempPath)
		return err
	}
	// No explicit Sync here — durability is guaranteed by the Pebble WAL
	// commit (pebble.Sync) that follows immediately after this blob is written.
	// Removing the per-blob fsync roughly halves the number of forced disk
	// flushes per logical write.
	if err := file.Close(); err != nil {
		_ = os.Remove(tempPath)
		return err
	}
	return os.Rename(tempPath, filePath)
}

// StartPurge runs a background goroutine that moves old hot-storage blobs to
// the archive tier at the given interval.  It returns immediately; the goroutine
// stops when ctx is cancelled.
func (s *Store) StartPurge(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = s.Purge(ctx)
			}
		}
	}()
}

func moveFileAtomic(srcPath, dstPath string) error {
	if err := os.MkdirAll(filepath.Dir(dstPath), 0o755); err != nil {
		return err
	}
	return os.Rename(srcPath, dstPath)
}

var _ kvsapi.Store = (*Store)(nil)
