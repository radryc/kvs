package grpcapi

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	"google.golang.org/grpc/status"
)

// maxAuditPaths bounds how many logical paths are recorded for a batch RPC.
const maxAuditPaths = 16

// AuditLogEnv is the environment variable consulted when Config.AuditLogPath
// is empty.
const AuditLogEnv = "KVS_AUDIT_LOG"

// Audit actions recorded for KVStore RPCs.
const (
	actionRead    = "read"
	actionWrite   = "write"
	actionDelete  = "delete"
	actionVersion = "version"
	actionWatch   = "watch"
)

type auditRecord struct {
	Timestamp   string `json:"ts"`
	Principal   string `json:"principal"`
	Action      string `json:"action"`
	LogicalPath string `json:"logical_path,omitempty"`
	Decision    string `json:"decision"`
	Code        string `json:"code"`
	Method      string `json:"method"`
	DurationMS  int64  `json:"duration_ms"`
}

type auditor struct {
	logger *slog.Logger
	closer io.Closer
	chain  *auditChain
}

func newAuditor(cfg Config) (*auditor, error) {
	if cfg.AuditLogger != nil {
		return &auditor{logger: cfg.AuditLogger, chain: newAuditChainFromEnv("")}, nil
	}
	path := strings.TrimSpace(cfg.AuditLogPath)
	if path == "" {
		path = strings.TrimSpace(os.Getenv(AuditLogEnv))
	}
	if path == "" {
		return &auditor{logger: slog.New(slog.NewJSONHandler(os.Stderr, nil)), chain: newAuditChainFromEnv("")}, nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	return &auditor{
		logger: slog.New(slog.NewJSONHandler(file, nil)),
		closer: file,
		chain:  newAuditChainFromEnv(path),
	}, nil
}

func (a *auditor) Close() error {
	if a == nil || a.closer == nil {
		return nil
	}
	return a.closer.Close()
}

func (a *auditor) record(ctx context.Context, action, logicalPath, method string, started time.Time, err error) {
	if a == nil || a.logger == nil {
		return
	}
	decision := "allow"
	code := "ok"
	if err != nil {
		decision = "deny"
		code = status.Code(err).String()
	}
	record := auditRecord{
		Timestamp:   started.UTC().Format(time.RFC3339Nano),
		Principal:   PrincipalFromContext(ctx),
		Action:      action,
		LogicalPath: logicalPath,
		Decision:    decision,
		Code:        code,
		Method:      method,
		DurationMS:  time.Since(started).Milliseconds(),
	}
	if a.chain != nil {
		a.chain.emit(ctx, record.Principal, record.Action, record.LogicalPath, record.Method, record.Decision, record.Code, started)
	}
	a.logger.LogAttrs(ctx, slog.LevelInfo, "kvs.audit",
		slog.String("ts", record.Timestamp),
		slog.String("principal", record.Principal),
		slog.String("action", record.Action),
		slog.String("logical_path", record.LogicalPath),
		slog.String("decision", record.Decision),
		slog.String("code", record.Code),
		slog.String("method", record.Method),
		slog.Int64("duration_ms", record.DurationMS),
	)
}

// auditCall returns a completion callback that records the outcome of a
// handler invocation. Path is read when the callback runs so streaming
// handlers can fill it in once metadata arrives.
func (s *Server) auditCall(ctx context.Context, action, method string, path *string) func(error) {
	started := time.Now()
	return func(err error) {
		logicalPath := ""
		if path != nil {
			logicalPath = strings.TrimSpace(*path)
		}
		s.auditor.record(ctx, action, logicalPath, method, started, err)
	}
}

func auditPaths(writes []*kvsv1.PathWrite) string {
	paths := make([]string, 0, len(writes))
	for _, write := range writes {
		if write == nil {
			continue
		}
		if path := strings.TrimSpace(write.GetLogicalPath()); path != "" {
			paths = append(paths, path)
		}
	}
	return joinAuditPaths(paths)
}

func auditDeletePaths(deletes []*kvsv1.PathDelete) string {
	paths := make([]string, 0, len(deletes))
	for _, deleteReq := range deletes {
		if deleteReq == nil {
			continue
		}
		if path := strings.TrimSpace(deleteReq.GetLogicalPath()); path != "" {
			paths = append(paths, path)
		}
	}
	return joinAuditPaths(paths)
}

func joinAuditPaths(paths []string) string {
	if len(paths) <= maxAuditPaths {
		return strings.Join(paths, ",")
	}
	return strings.Join(paths[:maxAuditPaths], ",") + ",..."
}

func actionForMethod(fullMethod string) string {
	switch {
	case strings.HasSuffix(fullMethod, "/ReadFile"),
		strings.HasSuffix(fullMethod, "/ReadFileStream"),
		strings.HasSuffix(fullMethod, "/Stat"),
		strings.HasSuffix(fullMethod, "/ListDir"):
		return actionRead
	case strings.HasSuffix(fullMethod, "/UpsertFiles"),
		strings.HasSuffix(fullMethod, "/PutFileStream"):
		return actionWrite
	case strings.HasSuffix(fullMethod, "/DeletePaths"):
		return actionDelete
	case strings.HasSuffix(fullMethod, "/ListVersions"),
		strings.HasSuffix(fullMethod, "/GetVersion"),
		strings.HasSuffix(fullMethod, "/GetVersionStream"):
		return actionVersion
	case strings.HasSuffix(fullMethod, "/Watch"):
		return actionWatch
	}
	return "unknown"
}

func requestLogicalPath(req any) string {
	switch typed := req.(type) {
	case *kvsv1.UpsertFilesRequest:
		return auditPaths(typed.GetWrites())
	case *kvsv1.DeletePathsRequest:
		return auditDeletePaths(typed.GetDeletes())
	case *kvsv1.WatchRequest:
		return strings.Join(typed.GetPrefixes(), ",")
	case interface{ GetLogicalPath() string }:
		return strings.TrimSpace(typed.GetLogicalPath())
	case interface{ GetLogicalDir() string }:
		return strings.TrimSpace(typed.GetLogicalDir())
	}
	return ""
}
