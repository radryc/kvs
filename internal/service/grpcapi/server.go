package grpcapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"io/fs"
	"strings"

	kvsv1 "github.com/rydzu/ainfra/kvs/api/proto/kvs/v1"
	"github.com/rydzu/ainfra/kvs/pkg/kvsapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultChunkSize = 64 * 1024

type Config struct {
	ChunkSize int
}

type Server struct {
	kvsv1.UnimplementedKVStoreServer
	store     kvsapi.Store
	chunkSize int
}

func NewServer(store kvsapi.Store, cfg Config) *Server {
	if cfg.ChunkSize <= 0 {
		cfg.ChunkSize = defaultChunkSize
	}
	return &Server{store: store, chunkSize: cfg.ChunkSize}
}

func (s *Server) ReadFile(ctx context.Context, req *kvsv1.ReadFileRequest) (*kvsv1.ReadFileResponse, error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
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

func (s *Server) ReadFileStream(req *kvsv1.ReadFileRequest, stream kvsv1.KVStore_ReadFileStreamServer) error {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
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

func (s *Server) Stat(ctx context.Context, req *kvsv1.StatRequest) (*kvsv1.StatResponse, error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
	if logicalPath == "" {
		return nil, status.Error(codes.InvalidArgument, "logical_path is required")
	}
	info, err := s.store.Stat(ctx, logicalPath)
	if err != nil {
		return nil, toStatus(err)
	}
	return &kvsv1.StatResponse{Info: protoFileInfo(info)}, nil
}

func (s *Server) ListDir(ctx context.Context, req *kvsv1.ListDirRequest) (*kvsv1.ListDirResponse, error) {
	logicalDir := strings.TrimSpace(req.GetLogicalDir())
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

func (s *Server) UpsertFiles(ctx context.Context, req *kvsv1.UpsertFilesRequest) (*kvsv1.BatchRevision, error) {
	batch, err := s.store.UpsertFiles(ctx, kvsapi.MutationBatch{
		Writes:  apiWrites(req.GetWrites()),
		Context: apiMutationContext(req.GetContext()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return protoBatchRevision(batch), nil
}

func (s *Server) DeletePaths(ctx context.Context, req *kvsv1.DeletePathsRequest) (*kvsv1.BatchRevision, error) {
	batch, err := s.store.DeletePaths(ctx, kvsapi.DeleteBatch{
		Deletes: apiDeletes(req.GetDeletes()),
		Context: apiMutationContext(req.GetContext()),
	})
	if err != nil {
		return nil, toStatus(err)
	}
	return protoBatchRevision(batch), nil
}

func (s *Server) ListVersions(ctx context.Context, req *kvsv1.ListVersionsRequest) (*kvsv1.ListVersionsResponse, error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
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

func (s *Server) GetVersion(ctx context.Context, req *kvsv1.GetVersionRequest) (*kvsv1.GetVersionResponse, error) {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
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

func (s *Server) GetVersionStream(req *kvsv1.GetVersionRequest, stream kvsv1.KVStore_GetVersionStreamServer) error {
	logicalPath := strings.TrimSpace(req.GetLogicalPath())
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

func (s *Server) PutFileStream(stream kvsv1.KVStore_PutFileStreamServer) error {
	var meta *kvsv1.PutFileMeta
	var content bytes.Buffer

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

func (s *Server) Watch(req *kvsv1.WatchRequest, stream kvsv1.KVStore_WatchServer) error {
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
