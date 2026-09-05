package kvsapi

import (
	"context"
	"errors"
	"time"
)

var ErrConflict = errors.New("kvsapi: version conflict")

type ReadStore interface {
	ReadFile(ctx context.Context, logicalPath string) ([]byte, error)
	ListDir(ctx context.Context, logicalDir string) ([]DirEntry, error)
	Stat(ctx context.Context, logicalPath string) (FileInfo, error)
}

type WatchStore interface {
	Watch(ctx context.Context, prefixes []string) (<-chan ChangeEvent, error)
}

type WriteStore interface {
	UpsertFiles(ctx context.Context, batch MutationBatch) (BatchRevision, error)
	DeletePaths(ctx context.Context, batch DeleteBatch) (BatchRevision, error)
	ListVersions(ctx context.Context, logicalPath string) ([]FileVersion, error)
	GetVersion(ctx context.Context, logicalPath, versionID string) (VersionedFile, error)
}

// FetcherOffloader offloads KVS archive blobs to an external storage tier.
// Implementations route blobs to a fetcher service so that the local KVS
// process does not need to keep archived blobs on disk.
type FetcherOffloader interface {
	// StoreBlob uploads a blob; blobHash is hex-encoded SHA-256 of content.
	StoreBlob(ctx context.Context, blobHash string, content []byte) error
	// FetchBlob downloads a blob by its hex-encoded SHA-256 hash.
	FetchBlob(ctx context.Context, blobHash string) ([]byte, error)
}

type Store interface {
	ReadStore
	WatchStore
	WriteStore
}

type StoreStatus struct {
	Enabled   bool   `json:"enabled"`
	Healthy   bool   `json:"healthy"`
	Mode      string `json:"mode"`
	Role      string `json:"role"`
	LeaderID  string `json:"leaderID,omitempty"`
	PeerCount int32  `json:"peerCount"`
	KeyCount  int64  `json:"keyCount"`
}

type DirEntry struct {
	Name  string `json:"name"`
	IsDir bool   `json:"isDir"`
	Size  int64  `json:"size"`
}

type FileInfo struct {
	Path      string    `json:"path"`
	Size      int64     `json:"size"`
	VersionID string    `json:"versionID"`
	ModTime   time.Time `json:"modTime"`
}

type ChangeType string

const (
	ChangeAdded    ChangeType = "Added"
	ChangeModified ChangeType = "Modified"
	ChangeDeleted  ChangeType = "Deleted"
)

type ChangeEvent struct {
	LogicalPath     string     `json:"logicalPath"`
	Type            ChangeType `json:"type"`
	VersionID       string     `json:"versionID"`
	BatchRevisionID string     `json:"batchRevisionID"`
	CommittedAt     time.Time  `json:"committedAt"`
}

type MutationBatch struct {
	Writes  []PathWrite     `json:"writes"`
	Context MutationContext `json:"context"`
}

type PathWrite struct {
	LogicalPath       string `json:"logicalPath"`
	Content           []byte `json:"content"`
	ExpectedVersionID string `json:"expectedVersionID"`
}

type DeleteBatch struct {
	Deletes []PathDelete    `json:"deletes"`
	Context MutationContext `json:"context"`
}

type PathDelete struct {
	LogicalPath       string `json:"logicalPath"`
	ExpectedVersionID string `json:"expectedVersionID"`
}

type MutationContext struct {
	PrincipalID   string `json:"principalID"`
	Reason        string `json:"reason"`
	CorrelationID string `json:"correlationID"`
}

type BatchRevision struct {
	BatchRevisionID string        `json:"batchRevisionID"`
	Files           []FileVersion `json:"files"`
}

type FileVersion struct {
	LogicalPath     string    `json:"logicalPath"`
	VersionID       string    `json:"versionID"`
	BatchRevisionID string    `json:"batchRevisionID"`
	ContentSHA256   string    `json:"contentSHA256"`
	CommittedAt     time.Time `json:"committedAt"`
	Tombstone       bool      `json:"tombstone"`
	PrincipalID     string    `json:"principalID"`
	Reason          string    `json:"reason"`
}

type VersionedFile struct {
	Version FileVersion `json:"version"`
	Content []byte      `json:"content"`
}
