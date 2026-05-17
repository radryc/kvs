package local

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// kvsWriteFilesTotal counts logical file writes (upserts/deletes) committed to KVS.
	kvsWriteFilesTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "write_files_total",
		Help:      "Total number of logical files written (upserted or deleted) in KVS, by operation.",
	}, []string{"op"}) // op: upsert, delete

	// kvsWriteBytesTotal counts raw blob bytes written during upserts.
	kvsWriteBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "write_bytes_total",
		Help:      "Total bytes written to KVS blob storage (upsert content only).",
	})

	// kvsBlobsCreatedTotal counts new blob files created in hot storage.
	kvsBlobsCreatedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "blobs_created_total",
		Help:      "Total number of blob files created in KVS hot storage.",
	})

	// kvsPebbleBatchCommitsTotal counts Pebble batch commits, by operation.
	kvsPebbleBatchCommitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "pebble_batch_commits_total",
		Help:      "Total Pebble batch commits flushed to disk, by operation.",
	}, []string{"op"}) // op: upsert, delete, purge

	// kvsPurgeArchivedTotal counts blobs moved from hot to archive storage.
	kvsPurgeArchivedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "purge_archived_versions_total",
		Help:      "Total blob versions moved to archive storage by Purge.",
	})

	// kvsPurgeDeletedTotal counts archive blob files permanently deleted by Purge.
	kvsPurgeDeletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "purge_deleted_versions_total",
		Help:      "Total archive blob versions permanently deleted by Purge (beyond maxArchivedVersions).",
	})

	// kvsPurgeOffloadedTotal counts blobs uploaded to the fetcher offloader and removed locally.
	kvsPurgeOffloadedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "purge_offloaded_versions_total",
		Help:      "Total blob versions offloaded to the fetcher tier by Purge.",
	})

	// kvsActiveKeysGauge tracks the live (non-tombstoned) key count.
	kvsActiveKeysGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Namespace: "kvs",
		Name:      "active_keys",
		Help:      "Current number of active (non-tombstoned) keys in KVS.",
	})

	// kvsReadOpsTotal counts read operations by type (read_file, list_dir, stat).
	kvsReadOpsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "read_ops_total",
		Help:      "Total number of read operations on KVS, labelled by op (read_file, list_dir, stat).",
	}, []string{"op"}) // op: read_file, list_dir, stat

	// kvsReadBytesTotal counts raw bytes returned by ReadFile.
	kvsReadBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "kvs",
		Name:      "read_bytes_total",
		Help:      "Total bytes read from KVS blob storage (ReadFile content only).",
	})
)
