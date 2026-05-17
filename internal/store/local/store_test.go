package local

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
)

func TestStoreUpsertReadReopenAndListVersions(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, 2)

	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchCh, err := store.Watch(watchCtx, []string{"/partitions/demo"})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	resp1, err := store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  []kvsapi.PathWrite{{LogicalPath: "/partitions/demo/config.yaml", Content: []byte("v1")}},
		Context: kvsapi.MutationContext{PrincipalID: "tester", Reason: "seed"},
	})
	if err != nil {
		t.Fatalf("upsert v1: %v", err)
	}
	version1 := versionForPath(t, resp1, "/partitions/demo/config.yaml")

	assertEvent(t, watchCh, kvsapi.ChangeAdded)

	resp2, err := store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{{
			LogicalPath:       "/partitions/demo/config.yaml",
			Content:           []byte("v2"),
			ExpectedVersionID: version1,
		}},
		Context: kvsapi.MutationContext{PrincipalID: "tester", Reason: "update"},
	})
	if err != nil {
		t.Fatalf("upsert v2: %v", err)
	}
	version2 := versionForPath(t, resp2, "/partitions/demo/config.yaml")
	if version1 == version2 {
		t.Fatalf("expected distinct version ids")
	}

	assertEvent(t, watchCh, kvsapi.ChangeModified)

	content, err := store.ReadFile(ctx, "/partitions/demo/config.yaml")
	if err != nil {
		t.Fatalf("read latest: %v", err)
	}
	if string(content) != "v2" {
		t.Fatalf("unexpected latest content %q", string(content))
	}

	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	reopened, err := Open(Config{DataDir: store.dataDir, MaxHotVersions: 2})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	content, err = reopened.ReadFile(ctx, "/partitions/demo/config.yaml")
	if err != nil {
		t.Fatalf("read after reopen: %v", err)
	}
	if string(content) != "v2" {
		t.Fatalf("unexpected content after reopen %q", string(content))
	}

	versions, err := reopened.ListVersions(ctx, "/partitions/demo/config.yaml")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions, got %d", len(versions))
	}
	if versions[0].VersionID != version2 || versions[1].VersionID != version1 {
		t.Fatalf("unexpected version order: %+v", versions)
	}

	v1, err := reopened.GetVersion(ctx, "/partitions/demo/config.yaml", version1)
	if err != nil {
		t.Fatalf("get historical version: %v", err)
	}
	if string(v1.Content) != "v1" {
		t.Fatalf("unexpected historical content %q", string(v1.Content))
	}
}

func TestDeleteCreatesTombstoneAndConflict(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, 2)
	defer store.Close()

	resp, err := store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{{LogicalPath: "/partitions/demo/intents/core.yaml", Content: []byte("intent")}},
	})
	if err != nil {
		t.Fatalf("seed upsert: %v", err)
	}
	versionID := versionForPath(t, resp, "/partitions/demo/intents/core.yaml")

	if _, err := store.DeletePaths(ctx, kvsapi.DeleteBatch{
		Deletes: []kvsapi.PathDelete{{
			LogicalPath:       "/partitions/demo/intents/core.yaml",
			ExpectedVersionID: "wrong-version",
		}},
	}); !errors.Is(err, kvsapi.ErrConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}

	deleteResp, err := store.DeletePaths(ctx, kvsapi.DeleteBatch{
		Deletes: []kvsapi.PathDelete{{
			LogicalPath:       "/partitions/demo/intents/core.yaml",
			ExpectedVersionID: versionID,
		}},
	})
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	tombstoneVersion := versionForPath(t, deleteResp, "/partitions/demo/intents/core.yaml")

	if _, err := store.ReadFile(ctx, "/partitions/demo/intents/core.yaml"); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected not exist after delete, got %v", err)
	}

	versions, err := store.ListVersions(ctx, "/partitions/demo/intents/core.yaml")
	if err != nil {
		t.Fatalf("list versions after delete: %v", err)
	}
	if len(versions) != 2 {
		t.Fatalf("expected 2 versions after delete, got %d", len(versions))
	}
	if !versions[0].Tombstone || versions[0].VersionID != tombstoneVersion {
		t.Fatalf("expected tombstone as latest version: %+v", versions[0])
	}

	versioned, err := store.GetVersion(ctx, "/partitions/demo/intents/core.yaml", tombstoneVersion)
	if err != nil {
		t.Fatalf("get tombstone version: %v", err)
	}
	if !versioned.Version.Tombstone {
		t.Fatalf("expected tombstone metadata")
	}
	if len(versioned.Content) != 0 {
		t.Fatalf("expected empty tombstone content")
	}
}

func TestPurgeArchivesOlderVersions(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, 2)
	defer store.Close()

	var previous string
	versions := make([]string, 0, 4)
	for _, value := range []string{"v1", "v2", "v3", "v4"} {
		resp, err := store.UpsertFiles(ctx, kvsapi.MutationBatch{
			Writes: []kvsapi.PathWrite{{
				LogicalPath:       "/partitions/demo/config.yaml",
				Content:           []byte(value),
				ExpectedVersionID: previous,
			}},
		})
		if err != nil {
			t.Fatalf("upsert %s: %v", value, err)
		}
		previous = versionForPath(t, resp, "/partitions/demo/config.yaml")
		versions = append(versions, previous)
	}

	report, err := store.Purge(ctx)
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if report.ArchivedVersions != 2 {
		t.Fatalf("expected 2 archived versions, got %d", report.ArchivedVersions)
	}

	oldestManifest, err := store.loadManifest(versionKey("/partitions/demo/config.yaml", versions[0]))
	if err != nil {
		t.Fatalf("load oldest manifest: %v", err)
	}
	if oldestManifest.StorageClass != storageArchive {
		t.Fatalf("expected archived storage class, got %s", oldestManifest.StorageClass)
	}
	if _, err := osStat(store.hotBlobPath(versions[0])); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected archived blob to be removed from hot storage, got %v", err)
	}
	if _, err := osStat(store.archiveBlobPath(versions[0])); err != nil {
		t.Fatalf("expected archived blob to exist: %v", err)
	}

	versioned, err := store.GetVersion(ctx, "/partitions/demo/config.yaml", versions[0])
	if err != nil {
		t.Fatalf("get archived version: %v", err)
	}
	if string(versioned.Content) != "v1" {
		t.Fatalf("unexpected archived content %q", string(versioned.Content))
	}

	latest, err := store.ReadFile(ctx, "/partitions/demo/config.yaml")
	if err != nil {
		t.Fatalf("read latest after purge: %v", err)
	}
	if string(latest) != "v4" {
		t.Fatalf("unexpected latest content %q", string(latest))
	}
}

func TestListDirReturnsImmediateChildren(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, 2)
	defer store.Close()

	_, err := store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{
			{LogicalPath: "/partitions/demo/config.yaml", Content: []byte("cfg")},
			{LogicalPath: "/partitions/demo/intents/a.yaml", Content: []byte("a")},
			{LogicalPath: "/partitions/demo/intents/nested/b.yaml", Content: []byte("b")},
		},
	})
	if err != nil {
		t.Fatalf("seed listdir data: %v", err)
	}

	entries, err := store.ListDir(ctx, "/partitions/demo")
	if err != nil {
		t.Fatalf("list dir: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	if !entries[0].IsDir || entries[0].Name != "intents" {
		t.Fatalf("unexpected first entry: %+v", entries[0])
	}
	if entries[1].IsDir || entries[1].Name != "config.yaml" {
		t.Fatalf("unexpected second entry: %+v", entries[1])
	}
}

func TestStoreKeyCountTracksLiveKeys(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t, 2)

	if got := store.KeyCount(); got != 0 {
		t.Fatalf("expected empty store key count 0, got %d", got)
	}

	resp, err := store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{{LogicalPath: "/partitions/demo/config.yaml", Content: []byte("cfg")}},
	})
	if err != nil {
		t.Fatalf("seed upsert: %v", err)
	}
	versionID := versionForPath(t, resp, "/partitions/demo/config.yaml")
	if got := store.KeyCount(); got != 1 {
		t.Fatalf("expected 1 live key after insert, got %d", got)
	}

	_, err = store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{{
			LogicalPath:       "/partitions/demo/config.yaml",
			Content:           []byte("cfg-v2"),
			ExpectedVersionID: versionID,
		}},
	})
	if err != nil {
		t.Fatalf("update existing key: %v", err)
	}
	if got := store.KeyCount(); got != 1 {
		t.Fatalf("expected 1 live key after update, got %d", got)
	}

	_, err = store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{{LogicalPath: "/partitions/demo/intents/a.yaml", Content: []byte("intent")}},
	})
	if err != nil {
		t.Fatalf("insert second key: %v", err)
	}
	if got := store.KeyCount(); got != 2 {
		t.Fatalf("expected 2 live keys after second insert, got %d", got)
	}

	if _, err := store.DeletePaths(ctx, kvsapi.DeleteBatch{
		Deletes: []kvsapi.PathDelete{{LogicalPath: "/partitions/demo/config.yaml"}},
	}); err != nil {
		t.Fatalf("delete key: %v", err)
	}
	if got := store.KeyCount(); got != 1 {
		t.Fatalf("expected 1 live key after delete, got %d", got)
	}

	dataDir := store.dataDir
	if err := store.Close(); err != nil {
		t.Fatalf("close store: %v", err)
	}

	reopened, err := Open(Config{DataDir: dataDir, MaxHotVersions: 2})
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	if got := reopened.KeyCount(); got != 1 {
		t.Fatalf("expected reopened store key count 1, got %d", got)
	}
}

func openTestStore(t *testing.T, maxHotVersions int) *Store {
	t.Helper()
	dataDir := t.TempDir()
	store, err := Open(Config{DataDir: dataDir, MaxHotVersions: maxHotVersions})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	return store
}

func versionForPath(t *testing.T, batch kvsapi.BatchRevision, logicalPath string) string {
	t.Helper()
	for _, file := range batch.Files {
		if file.LogicalPath == logicalPath {
			return file.VersionID
		}
	}
	t.Fatalf("version for path %s not found in batch %+v", logicalPath, batch)
	return ""
}

func assertEvent(t *testing.T, ch <-chan kvsapi.ChangeEvent, expected kvsapi.ChangeType) {
	t.Helper()
	select {
	case event := <-ch:
		if event.Type != expected {
			t.Fatalf("expected event %s, got %+v", expected, event)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for event %s", expected)
	}
}

func osStat(path string) (fs.FileInfo, error) {
	return fs.Stat(os.DirFS(filepath.Dir(path)), filepath.Base(path))
}
