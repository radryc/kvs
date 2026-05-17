package grpcapi

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	kvsv1 "github.com/rydzu/ainfra/kvs/api/proto/kvs/v1"
	"github.com/rydzu/ainfra/kvs/internal/store/local"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

func TestPutFileStreamAndReadBack(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	putStream, err := client.PutFileStream(ctx)
	if err != nil {
		t.Fatalf("put file stream: %v", err)
	}
	if err := putStream.Send(&kvsv1.PutFileStreamRequest{Payload: &kvsv1.PutFileStreamRequest_Meta{Meta: &kvsv1.PutFileMeta{
		LogicalPath: "/partitions/demo/config.yaml",
		Context:     &kvsv1.MutationContext{PrincipalId: "tester", Reason: "stream upload"},
	}}}); err != nil {
		t.Fatalf("send meta: %v", err)
	}
	for _, chunk := range [][]byte{[]byte("hello "), []byte("guardian "), []byte("store")} {
		if err := putStream.Send(&kvsv1.PutFileStreamRequest{Payload: &kvsv1.PutFileStreamRequest_Chunk{Chunk: chunk}}); err != nil {
			t.Fatalf("send chunk: %v", err)
		}
	}
	putResp, err := putStream.CloseAndRecv()
	if err != nil {
		t.Fatalf("close send: %v", err)
	}

	readResp, err := client.ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: "/partitions/demo/config.yaml"})
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	if string(readResp.GetContent()) != "hello guardian store" {
		t.Fatalf("unexpected read content %q", string(readResp.GetContent()))
	}
	if readResp.GetVersion().GetVersionId() != putResp.GetVersion().GetVersionId() {
		t.Fatalf("unexpected version ids: read=%s put=%s", readResp.GetVersion().GetVersionId(), putResp.GetVersion().GetVersionId())
	}

	stream, err := client.ReadFileStream(ctx, &kvsv1.ReadFileRequest{LogicalPath: "/partitions/demo/config.yaml"})
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	var streamed bytes.Buffer
	var seenVersion string
	for {
		msg, err := stream.Recv()
		if err != nil {
			if status.Code(err) == codes.OutOfRange {
				t.Fatalf("unexpected stream out of range: %v", err)
			}
			if err.Error() == "EOF" {
				break
			}
			if err != nil {
				break
			}
		}
		switch payload := msg.GetPayload().(type) {
		case *kvsv1.ReadFileStreamResponse_Version:
			seenVersion = payload.Version.GetVersionId()
		case *kvsv1.ReadFileStreamResponse_Chunk:
			streamed.Write(payload.Chunk)
		}
	}
	if streamed.String() != "hello guardian store" {
		t.Fatalf("unexpected streamed content %q", streamed.String())
	}
	if seenVersion != putResp.GetVersion().GetVersionId() {
		t.Fatalf("unexpected streamed version %s", seenVersion)
	}

	versionResp, err := client.GetVersion(ctx, &kvsv1.GetVersionRequest{
		LogicalPath: "/partitions/demo/config.yaml",
		VersionId:   putResp.GetVersion().GetVersionId(),
	})
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if string(versionResp.GetFile().GetContent()) != "hello guardian store" {
		t.Fatalf("unexpected versioned content %q", string(versionResp.GetFile().GetContent()))
	}
}

func TestWatchAndConflictMapping(t *testing.T) {
	client, cleanup := newTestClient(t)
	defer cleanup()

	ctx := context.Background()
	watchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	watchStream, err := client.Watch(watchCtx, &kvsv1.WatchRequest{Prefixes: []string{"/partitions/demo"}})
	if err != nil {
		t.Fatalf("watch: %v", err)
	}

	resp, err := client.UpsertFiles(ctx, &kvsv1.UpsertFilesRequest{
		Writes: []*kvsv1.PathWrite{{LogicalPath: "/partitions/demo/config.yaml", Content: []byte("v1")}},
	})
	if err != nil {
		t.Fatalf("upsert: %v", err)
	}

	eventCh := make(chan *kvsv1.WatchResponse, 1)
	errCh := make(chan error, 1)
	go func() {
		msg, err := watchStream.Recv()
		if err != nil {
			errCh <- err
			return
		}
		eventCh <- msg
	}()

	select {
	case msg := <-eventCh:
		if msg.GetEvent().GetType() != kvsv1.ChangeType_CHANGE_TYPE_ADDED {
			t.Fatalf("unexpected watch event: %+v", msg.GetEvent())
		}
	case err := <-errCh:
		t.Fatalf("watch recv: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for watch event")
	}

	_, err = client.UpsertFiles(ctx, &kvsv1.UpsertFilesRequest{
		Writes: []*kvsv1.PathWrite{{
			LogicalPath:       "/partitions/demo/config.yaml",
			Content:           []byte("v2"),
			ExpectedVersionId: "wrong-version",
		}},
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("expected aborted conflict, got %v", err)
	}

	versions, err := client.ListVersions(ctx, &kvsv1.ListVersionsRequest{LogicalPath: "/partitions/demo/config.yaml"})
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(versions.GetVersions()) != 1 || versions.GetVersions()[0].GetVersionId() != resp.GetFiles()[0].GetVersionId() {
		t.Fatalf("unexpected versions: %+v", versions.GetVersions())
	}
}

func newTestClient(t *testing.T) (kvsv1.KVStoreClient, func()) {
	t.Helper()
	listener := bufconn.Listen(bufSize)
	baseStore, err := local.Open(local.Config{DataDir: t.TempDir(), MaxHotVersions: 2})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}

	grpcServer := grpc.NewServer()
	kvsv1.RegisterKVStoreServer(grpcServer, NewServer(baseStore, Config{ChunkSize: 4}))
	go func() {
		_ = grpcServer.Serve(listener)
	}()

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		grpcServer.Stop()
		baseStore.Close()
		listener.Close()
		t.Fatalf("dial bufconn: %v", err)
	}

	cleanup := func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = baseStore.Close()
		_ = listener.Close()
	}

	return kvsv1.NewKVStoreClient(conn), cleanup
}
