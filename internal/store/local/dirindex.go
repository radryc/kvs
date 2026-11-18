package local

import (
	"encoding/binary"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble"
	"github.com/radryc/kvs/pkg/kvsapi"
)

const (
	dirIndexKeyPrefix = "dir\x00"
	dirIndexMarkerKey = "idx\x00dir-index-v1"
	dirIndexValueSize = 9 // 1 byte isDir + 8 bytes size
)

// dirIndexKey is dir\x00<parent>\x00<child>. The trailing NUL makes the exact
// parent prefix unambiguous (so /a/b does not match /a/bc).
func dirIndexKey(parent, child string) []byte {
	buf := make([]byte, 0, len(dirIndexKeyPrefix)+len(parent)+1+len(child))
	buf = append(buf, dirIndexKeyPrefix...)
	buf = append(buf, parent...)
	buf = append(buf, 0)
	buf = append(buf, child...)
	return buf
}

func dirIndexPrefix(dir string) []byte {
	return dirIndexKey(dir, "")
}

func encodeDirIndexValue(isDir bool, size int64) []byte {
	buf := make([]byte, dirIndexValueSize)
	if isDir {
		buf[0] = 1
	}
	binary.BigEndian.PutUint64(buf[1:], uint64(size))
	return buf
}

func decodeDirIndexValue(value []byte) (isDir bool, size int64) {
	if len(value) < dirIndexValueSize {
		return false, 0
	}
	return value[0] == 1, int64(binary.BigEndian.Uint64(value[1:]))
}

// addDirIndexEntries writes index entries for a logical path and all of its
// ancestor directories. Writes are idempotent and are added to the same Pebble
// batch as the manifest, so Directory and file visibility commit atomically.
func addDirIndexEntries(batch *pebble.Batch, logicalPath string, isDir bool, size int64) {
	parent := path.Dir(logicalPath)
	child := path.Base(logicalPath)
	if child == "." || child == "/" {
		return
	}
	_ = batch.Set(dirIndexKey(parent, child), encodeDirIndexValue(isDir, size), nil)
	for p := parent; p != "/" && p != "."; p = path.Dir(p) {
		grandparent := path.Dir(p)
		name := path.Base(p)
		if name == "." || name == "/" || name == "" {
			break
		}
		_ = batch.Set(dirIndexKey(grandparent, name), encodeDirIndexValue(true, 0), nil)
	}
}

// ensureDirIndex backfills the directory index once for stores created before
// the index existed. The marker key makes subsequent opens O(1).
func (s *Store) ensureDirIndex() error {
	_, closer, err := s.db.Get([]byte(dirIndexMarkerKey))
	if err == nil {
		closer.Close()
		s.dirIndexReady.Store(true)
		return nil
	}
	if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: latestPrefix(),
		UpperBound: prefixUpperBound(latestPrefix()),
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	batch := s.db.NewBatch()
	defer batch.Close()
	written := 0
	for iter.First(); iter.Valid(); iter.Next() {
		logicalPath := logicalPathFromLatestKey(iter.Key())
		manifest, err := decodeManifest(iter.Value())
		if err != nil {
			return fmt.Errorf("dir index backfill %s: %w", logicalPath, err)
		}
		if manifest.Version.Tombstone {
			continue
		}
		addDirIndexEntries(batch, logicalPath, false, manifest.SizeBytes)
		written++
		if written%10000 == 0 {
			if err := batch.Commit(pebble.NoSync); err != nil {
				return err
			}
			batch.Reset()
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	if err := batch.Set([]byte(dirIndexMarkerKey), []byte("1"), nil); err != nil {
		return err
	}
	if err := batch.Commit(pebble.Sync); err != nil {
		return err
	}
	s.dirIndexReady.Store(true)
	return nil
}

func (s *Store) listDirFromIndex(logicalDir string) ([]kvsapi.DirEntry, error) {
	prefix := dirIndexPrefix(logicalDir)
	iter, err := s.db.NewIter(&pebble.IterOptions{
		LowerBound: prefix,
		UpperBound: prefixUpperBound(prefix),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	entries := make([]kvsapi.DirEntry, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) <= len(prefix) {
			continue
		}
		name := string(key[len(prefix):])
		isDir, size := decodeDirIndexValue(iter.Value())
		entries = append(entries, kvsapi.DirEntry{Name: name, IsDir: isDir, Size: size})
	}
	if err := iter.Error(); err != nil {
		return nil, err
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

// deleteDirIndexEntry removes a file's entry from its parent directory. Empty
// ancestor directories are intentionally left in place; they are cheap and a
// later upsert reuses them.
func deleteDirIndexEntry(batch *pebble.Batch, logicalPath string) {
	parent := path.Dir(logicalPath)
	child := path.Base(logicalPath)
	if child == "." || child == "/" || strings.TrimSpace(child) == "" {
		return
	}
	_ = batch.Delete(dirIndexKey(parent, child), nil)
}
