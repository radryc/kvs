package grpcapi

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"

	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	"github.com/radryc/kvs/pkg/kvsapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultChunkSize = 64 * 1024

// AuthTokenEnv is the environment variable consulted by NewServerWithAuth when
// Config.AuthToken is empty.
const AuthTokenEnv = "KVS_AUTH_TOKEN"

// ErrAuthTokenRequired is returned when the authenticated constructor is used
// without a bearer token configured.
var ErrAuthTokenRequired = errors.New("grpcapi: auth token is required (set Config.AuthToken or KVS_AUTH_TOKEN)")

type Config struct {
	ChunkSize int
	// AuthToken is the bearer token required on every RPC. NewServerWithAuth
	// falls back to KVS_AUTH_TOKEN when this is empty.
	AuthToken string
	// AuditLogPath enables a JSONL audit trail at the given path. When empty,
	// KVS_AUDIT_LOG is consulted; otherwise audit records go to stderr as
	// structured JSON.
	AuditLogPath string
	// AuditLogger overrides the audit destination. It takes precedence over
	// AuditLogPath and is primarily useful for tests and embedding.
	AuditLogger *slog.Logger
}

type Server struct {
	kvsv1.UnimplementedKVStoreServer
	store     kvsapi.Store
	chunkSize int
	authToken string
	auditor   *auditor
}

// NewServer builds an unauthenticated server. It is intended for tests and
// embedded callers that provide their own authorization layer. Production
// entry points should use NewServerWithAuth.
func NewServer(store kvsapi.Store, cfg Config) *Server {
	server, err := newServer(store, cfg)
	if err != nil {
		panic("grpcapi: NewServer: " + err.Error())
	}
	return server
}

// NewServerWithAuth builds a server that requires a bearer token on every RPC
// (health checks excepted). The token is taken from Config.AuthToken, falling
// back to KVS_AUTH_TOKEN. Callers must attach UnaryInterceptor and
// StreamInterceptor to their gRPC server.
func NewServerWithAuth(store kvsapi.Store, cfg Config) (*Server, error) {
	token := strings.TrimSpace(cfg.AuthToken)
	if token == "" {
		token = strings.TrimSpace(os.Getenv(AuthTokenEnv))
	}
	if token == "" {
		return nil, ErrAuthTokenRequired
	}
	cfg.AuthToken = token
	server, err := newServer(store, cfg)
	if err != nil {
		return nil, err
	}
	return server, nil
}

func newServer(store kvsapi.Store, cfg Config) (*Server, error) {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaultChunkSize
	}
	audit, err := newAuditor(cfg)
	if err != nil {
		return nil, err
	}
	return &Server{
		store:     store,
		chunkSize: cfg.ChunkSize,
		authToken: strings.TrimSpace(cfg.AuthToken),
		auditor:   audit,
	}, nil
}

// Close releases resources owned by the server, such as an audit log file.
func (s *Server) Close() error {
	if s == nil || s.auditor == nil {
		return nil
	}
	return s.auditor.Close()
}

// UnaryInterceptor returns the bearer-token interceptor for s. It is a no-op
// when no token is configured (for example, a server created with NewServer).
// Authentication failures are also written to the audit trail.
func (s *Server) UnaryInterceptor() grpc.UnaryServerInterceptor {
	auth := UnaryAuthInterceptor(s.authToken)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := auth(ctx, req, info, handler)
		if status.Code(err) == codes.Unauthenticated {
			path := requestLogicalPath(req)
			s.auditor.record(ctx, actionForMethod(info.FullMethod), path, info.FullMethod, time.Now(), err)
		}
		return resp, err
	}
}

// StreamInterceptor returns the streaming bearer-token interceptor for s.
func (s *Server) StreamInterceptor() grpc.StreamServerInterceptor {
	auth := StreamAuthInterceptor(s.authToken)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := auth(srv, stream, info, handler)
		if status.Code(err) == codes.Unauthenticated {
			s.auditor.record(stream.Context(), actionForMethod(info.FullMethod), "", info.FullMethod, time.Now(), err)
		}
		return err
	}
}

// UnaryAuthInterceptor enforces bearer-token authentication using a
// constant-time comparison. An empty token disables enforcement.
func UnaryAuthInterceptor(authToken string) grpc.UnaryServerInterceptor {
	expected := strings.TrimSpace(authToken)
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		principal, err := authenticate(ctx, info.FullMethod, expected)
		if err != nil {
			return nil, err
		}
		return handler(withPrincipal(ctx, principal), req)
	}
}

// StreamAuthInterceptor is the streaming equivalent of UnaryAuthInterceptor.
func StreamAuthInterceptor(authToken string) grpc.StreamServerInterceptor {
	expected := strings.TrimSpace(authToken)
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		principal, err := authenticate(stream.Context(), info.FullMethod, expected)
		if err != nil {
			return err
		}
		return handler(srv, &authenticatedStream{ServerStream: stream, ctx: withPrincipal(stream.Context(), principal)})
	}
}

func authenticate(ctx context.Context, fullMethod, expected string) (string, error) {
	if expected == "" || isHealthMethod(fullMethod) {
		return PrincipalFromContext(ctx), nil
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return "", status.Error(codes.Unauthenticated, "missing bearer token")
	}
	for _, value := range md.Get("authorization") {
		scheme, token, found := strings.Cut(strings.TrimSpace(value), " ")
		if !found || !strings.EqualFold(scheme, "Bearer") {
			continue
		}
		if constantTimeTokenEqual(strings.TrimSpace(token), expected) {
			return principalForToken(expected), nil
		}
	}
	return "", status.Error(codes.Unauthenticated, "invalid or missing bearer token")
}

// constantTimeTokenEqual compares tokens by comparing fixed-size digests, so
// neither the contents nor the length of the configured token leak through
// timing.
func constantTimeTokenEqual(got, expected string) bool {
	gotDigest := sha256.Sum256([]byte(got))
	expectedDigest := sha256.Sum256([]byte(expected))
	return subtle.ConstantTimeCompare(gotDigest[:], expectedDigest[:]) == 1
}

func isHealthMethod(fullMethod string) bool {
	return strings.HasPrefix(fullMethod, "/grpc.health.v1.Health/")
}

type authenticatedStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *authenticatedStream) Context() context.Context { return s.ctx }

func (s *Server) ReadFile(ctx context.Context, req *kvsv1.ReadFileRequest) (resp *kvsv1.ReadFileResponse, err error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
	done := s.auditCall(ctx, actionRead, "ReadFile", &logicalPath)
	defer func() { done(err) }()
	if logicalPath == "" {
		return nil, status.Error(codes.InvalidArgument, "logical_path is required")
	}
	content, err := s.store.ReadFile(ctx, logicalPath)
	if err != nil {
		return nil, toStatus(err)
	}
	info, err := s.store.Stat(ctx, logicalPath)
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvsv1.ReadFileResponse{
		Content: content,
		Version: &kvsv1.FileVersion{
			LogicalPath: logicalPath,
			VersionId:   info.VersionID,
			CommittedAt: timestamppb.New(info.ModTime),
		},
	}, nil
}

func (s *Server) ReadFileStream(req *kvsv1.ReadFileRequest, stream kvsv1.KVStore_ReadFileStreamServer) (err error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
	done := s.auditCall(stream.Context(), actionRead, "ReadFileStream", &logicalPath)
	defer func() { done(err) }()
	if logicalPath == "" {
		return status.Error(codes.InvalidArgument, "logical_path is required")
	}
	content, err := s.store.ReadFile(stream.Context(), logicalPath)
	if err != nil {
		return toStatus(err)
	}
	versions, err := s.store.ListVersions(stream.Context(), logicalPath)
	if err != nil {
		return toStatus(err)
	}
	if len(versions) == 0 {
		return status.Error(codes.NotFound, "file version not found")
	}
	return s.sendFileStream(stream, versions[0], content)
}

func (s *Server) Stat(ctx context.Context, req *kvsv1.StatRequest) (resp *kvsv1.StatResponse, err error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
	done := s.auditCall(ctx, actionRead, "Stat", &logicalPath)
	defer func() { done(err) }()
	if logicalPath == "" {
		return nil, status.Error(codes.InvalidArgument, "logical_path is required")
	}
	info, err := s.store.Stat(ctx, logicalPath)
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvsv1.StatResponse{Info: protoFileInfo(info)}, nil
}

func (s *Server) ListDir(ctx context.Context, req *kvsv1.ListDirRequest) (resp *kvsv1.ListDirResponse, err error) {
	logicalDir := strings.TrimSpace(req.GetLogicalDir())
	done := s.auditCall(ctx, actionRead, "ListDir", &logicalDir)
	defer func() { done(err) }()
	if logicalDir == "" {
		return nil, status.Error(codes.InvalidArgument, "logical_dir is required")
	}
	entries, err := s.store.ListDir(ctx, logicalDir)
	if err != nil {
		return nil, toStatus(err)
	}
	response := &kvsv1.ListDirResponse{Entries: make([]*kvsv1.DirEntry, 0, len(entries))}
	for _, entry := range entries {
		response.Entries = append(response.Entries, &kvsv1.DirEntry{Name: entry.Name, IsDir: entry.IsDir, Size: entry.Size})
	}
	return response, nil
}

func (s *Server) UpsertFiles(ctx context.Context, req *kvsv1.UpsertFilesRequest) (resp *kvsv1.BatchRevision, err error) {
	paths := auditPaths(req.GetWrites())
	done := s.auditCall(ctx, actionWrite, "UpsertFiles", &paths)
	defer func() { done(err) }()
	batch, err := s.store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  apiWrites(req.GetWrites()),
		Context: apiMutationContext(req.GetContext()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return protoBatchRevision(batch), nil
}

func (s *Server) DeletePaths(ctx context.Context, req *kvsv1.DeletePathsRequest) (resp *kvsv1.BatchRevision, err error) {
	paths := auditDeletePaths(req.GetDeletes())
	done := s.auditCall(ctx, actionDelete, "DeletePaths", &paths)
	defer func() { done(err) }()
	batch, err := s.store.DeletePaths(ctx, kvsapi.DeleteBatch{
		Deletes: apiDeletes(req.GetDeletes()),
		Context: apiMutationContext(req.GetContext()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return protoBatchRevision(batch), nil
}

func (s *Server) ListVersions(ctx context.Context, req *kvsv1.ListVersionsRequest) (resp *kvsv1.ListVersionsResponse, err error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
	done := s.auditCall(ctx, actionVersion, "ListVersions", &logicalPath)
	defer func() { done(err) }()
	if logicalPath == "" {
		return nil, status.Error(codes.InvalidArgument, "logical_path is required")
	}
	versions, err := s.store.ListVersions(ctx, logicalPath)
	if err != nil {
		return nil, toStatus(err)
	}
	response := &kvsv1.ListVersionsResponse{Versions: make([]*kvsv1.FileVersion, 0, len(versions))}
	for _, version := range versions {
		response.Versions = append(response.Versions, protoFileVersion(version))
	}
	return response, nil
}

func (s *Server) GetVersion(ctx context.Context, req *kvsv1.GetVersionRequest) (resp *kvsv1.GetVersionResponse, err error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
	done := s.auditCall(ctx, actionVersion, "GetVersion", &logicalPath)
	defer func() { done(err) }()
	versionID := strings.TrimSpace(req.GetVersionId())
	if logicalPath == "" || versionID == "" {
		return nil, status.Error(codes.InvalidArgument, "logical_path and version_id are required")
	}
	file, err := s.store.GetVersion(ctx, logicalPath, versionID)
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvsv1.GetVersionResponse{File: protoVersionedFile(file)}, nil
}

func (s *Server) GetVersionStream(req *kvsv1.GetVersionRequest, stream kvsv1.KVStore_GetVersionStreamServer) (err error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
	done := s.auditCall(stream.Context(), actionVersion, "GetVersionStream", &logicalPath)
	defer func() { done(err) }()
	versionID := strings.TrimSpace(req.GetVersionId())
	if logicalPath == "" || versionID == "" {
		return status.Error(codes.InvalidArgument, "logical_path and version_id are required")
	}
	file, err := s.store.GetVersion(stream.Context(), logicalPath, versionID)
	if err != nil {
		return toStatus(err)
	}
	return s.sendFileStream(stream, file.Version, file.Content)
}

func (s *Server) PutFileStream(stream kvsv1.KVStore_PutFileStreamServer) (err error) {
	var meta *kvsv1.PutFileMeta
	var content bytes.Buffer
	var logicalPath string

	done := s.auditCall(stream.Context(), actionWrite, "PutFileStream", &logicalPath)
	defer func() { done(err) }()

	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return err
		}
		switch payload := msg.GetPayload().(type) {
		case *kvsv1.PutFileStreamRequest_Meta:
			if meta != nil {
				return status.Error(codes.InvalidArgument, "meta can only be sent once")
			}
			if payload.Meta == nil || strings.TrimSpace(payload.Meta.GetLogicalPath()) == "" {
				return status.Error(codes.InvalidArgument, "meta.logical_path is required")
			}
			meta = payload.Meta
			logicalPath = strings.TrimSpace(payload.Meta.GetLogicalPath())
		case *kvsv1.PutFileStreamRequest_Chunk:
			if meta == nil {
				return status.Error(codes.InvalidArgument, "meta must be sent before chunks")
			}
			if _, err := content.Write(payload.Chunk); err != nil {
				return status.Error(codes.Internal, err.Error())
			}
		default:
			return status.Error(codes.InvalidArgument, "put file stream message is empty")
		}
	}

	if meta == nil {
		return status.Error(codes.InvalidArgument, "missing file metadata")
	}

	batch, err := s.store.UpsertFiles(stream.Context(), kvsapi.MutationBatch{
		Writes: []kvsapi.PathWrite{{
			LogicalPath:       meta.GetLogicalPath(),
			Content:           content.Bytes(),
			ExpectedVersionID: meta.GetExpectedVersionId(),
		}},
		Context: apiMutationContext(meta.GetContext()),
	})
	if err != nil {
		return toStatus(err)
	}
	if len(batch.Files) != 1 {
		return status.Error(codes.Internal, "expected exactly one file version in upload response")
	}
	return stream.SendAndClose(&kvsv1.PutFileStreamResponse{
		BatchRevisionId: batch.BatchRevisionID,
		Version:         protoFileVersion(batch.Files[0]),
	})
}

func (s *Server) Watch(req *kvsv1.WatchRequest, stream kvsv1.KVStore_WatchServer) (err error) {
	prefixes := strings.Join(req.GetPrefixes(), ",")
	done := s.auditCall(stream.Context(), actionWatch, "Watch", &prefixes)
	defer func() { done(err) }()
	changes, err := s.store.Watch(stream.Context(), req.GetPrefixes())
	if err != nil {
		return toStatus(err)
	}
	for {
		select {
		case <-stream.Context().Done():
			return nil
		case event, ok := <-changes:
			if !ok {
				return nil
			}
			if err := stream.Send(&kvsv1.WatchResponse{Event: protoChangeEvent(event)}); err != nil {
				return err
			}
		}
	}
}

type fileStreamSender interface {
	Send(*kvsv1.ReadFileStreamResponse) error
}

func (s *Server) sendFileStream(sender fileStreamSender, version kvsapi.FileVersion, content []byte) error {
	if err := sender.Send(&kvsv1.ReadFileStreamResponse{Payload: &kvsv1.ReadFileStreamResponse_Version{Version: protoFileVersion(version)}}); err != nil {
		return err
	}
	for offset := 0; offset < len(content); offset += s.chunkSize {
		end := offset + s.chunkSize
		if end > len(content) {
			end = len(content)
		}
		if err := sender.Send(&kvsv1.ReadFileStreamResponse{Payload: &kvsv1.ReadFileStreamResponse_Chunk{Chunk: content[offset:end]}}); err != nil {
			return err
		}
	}
	return nil
}

func apiWrites(writes []*kvsv1.PathWrite) []kvsapi.PathWrite {
	result := make([]kvsapi.PathWrite, 0, len(writes))
	for _, write := range writes {
		if write == nil {
			continue
		}
		result = append(result, kvsapi.PathWrite{
			LogicalPath:       write.GetLogicalPath(),
			Content:           write.GetContent(),
			ExpectedVersionID: write.GetExpectedVersionId(),
		})
	}
	return result
}

func apiDeletes(deletes []*kvsv1.PathDelete) []kvsapi.PathDelete {
	result := make([]kvsapi.PathDelete, 0, len(deletes))
	for _, deleteReq := range deletes {
		if deleteReq == nil {
			continue
		}
		result = append(result, kvsapi.PathDelete{
			LogicalPath:       deleteReq.GetLogicalPath(),
			ExpectedVersionID: deleteReq.GetExpectedVersionId(),
		})
	}
	return result
}

func apiMutationContext(ctx *kvsv1.MutationContext) kvsapi.MutationContext {
	if ctx == nil {
		return kvsapi.MutationContext{}
	}
	return kvsapi.MutationContext{
		PrincipalID:   ctx.GetPrincipalId(),
		Reason:        ctx.GetReason(),
		CorrelationID: ctx.GetCorrelationId(),
	}
}

func protoBatchRevision(batch kvsapi.BatchRevision) *kvsv1.BatchRevision {
	response := &kvsv1.BatchRevision{BatchRevisionId: batch.BatchRevisionID, Files: make([]*kvsv1.FileVersion, 0, len(batch.Files))}
	for _, file := range batch.Files {
		response.Files = append(response.Files, protoFileVersion(file))
	}
	return response
}

func protoFileVersion(file kvsapi.FileVersion) *kvsv1.FileVersion {
	return &kvsv1.FileVersion{
		LogicalPath:     file.LogicalPath,
		VersionId:       file.VersionID,
		BatchRevisionId: file.BatchRevisionID,
		ContentSha256:   file.ContentSHA256,
		CommittedAt:     timestamppb.New(file.CommittedAt),
		Tombstone:       file.Tombstone,
		PrincipalId:     file.PrincipalID,
		Reason:          file.Reason,
	}
}

func protoVersionedFile(file kvsapi.VersionedFile) *kvsv1.VersionedFile {
	return &kvsv1.VersionedFile{Version: protoFileVersion(file.Version), Content: file.Content}
}

func protoFileInfo(info kvsapi.FileInfo) *kvsv1.FileInfo {
	return &kvsv1.FileInfo{
		Path:      info.Path,
		Size:      info.Size,
		VersionId: info.VersionID,
		ModTime:   timestamppb.New(info.ModTime),
	}
}

func protoChangeEvent(event kvsapi.ChangeEvent) *kvsv1.ChangeEvent {
	return &kvsv1.ChangeEvent{
		LogicalPath:     event.LogicalPath,
		Type:            protoChangeType(event.Type),
		VersionId:       event.VersionID,
		BatchRevisionId: event.BatchRevisionID,
		CommittedAt:     timestamppb.New(event.CommittedAt),
	}
}

func protoChangeType(changeType kvsapi.ChangeType) kvsv1.ChangeType {
	switch changeType {
	case kvsapi.ChangeAdded:
		return kvsv1.ChangeType_CHANGE_TYPE_ADDED
	case kvsapi.ChangeModified:
		return kvsv1.ChangeType_CHANGE_TYPE_MODIFIED
	case kvsapi.ChangeDeleted:
		return kvsv1.ChangeType_CHANGE_TYPE_DELETED
	default:
		return kvsv1.ChangeType_CHANGE_TYPE_UNSPECIFIED
	}
}

func toStatus(err error) error {
	if err == nil {
		return nil
	}
	if s, ok := status.FromError(err); ok {
		return s.Err()
	}
	if errors.Is(err, kvsapi.ErrNotLeader) {
		return status.Error(codes.Aborted, err.Error())
	}
	if errors.Is(err, kvsapi.ErrLeaderUnknown) {
		return status.Error(codes.Unavailable, err.Error())
	}
	if errors.Is(err, kvsapi.ErrConflict) {
		return status.Error(codes.Aborted, err.Error())
	}
	if errors.Is(err, fs.ErrNotExist) {
		return status.Error(codes.NotFound, err.Error())
	}
	if strings.Contains(err.Error(), "logical path") {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}

var _ kvsv1.KVStoreServer = (*Server)(nil)
