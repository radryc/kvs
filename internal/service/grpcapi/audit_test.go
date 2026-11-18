package grpcapi

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	kvsv1 "github.com/radryc/kvs/api/proto/kvs/v1"
	"github.com/radryc/kvs/internal/store/local"
)

func parseAuditRecords(t *testing.T, buf *bytes.Buffer) []map[string]any {
	t.Helper()
	records := make([]map[string]any, 0)
	scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("parse audit record %q: %v", line, err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("scan audit records: %v", err)
	}
	return records
}

func findAuditRecord(records []map[string]any, action, decision, path string) map[string]any {
	for _, record := range records {
		if record["action"] != action || record["decision"] != decision {
			continue
		}
		if path != "" && record["logical_path"] != path {
			continue
		}
		return record
	}
	return nil
}

func TestAuditTrailRecordsReadWriteDeleteVersion(t *testing.T) {
	const token = "audit-secret"
	var buf bytes.Buffer
	client, cleanup := newAuthTestClient(t, Config{
		AuthToken:   token,
		AuditLogger: slog.New(slog.NewJSONHandler(&buf, nil)),
	})
	defer cleanup()

	ctx := authContext(context.Background(), token)
	const path = "/guardian-system/.queues/audit/task.json"

	if _, err := client.UpsertFiles(ctx, &kvsv1.UpsertFilesRequest{
		Writes: []*kvsv1.PathWrite{{LogicalPath: path, Content: []byte("payload")}},
	}); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}
	if _, err := client.ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: path}); err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if _, err := client.ListVersions(ctx, &kvsv1.ListVersionsRequest{LogicalPath: path}); err != nil {
		t.Fatalf("ListVersions: %v", err)
	}
	if _, err := client.DeletePaths(ctx, &kvsv1.DeletePathsRequest{
		Deletes: []*kvsv1.PathDelete{{LogicalPath: path}},
	}); err != nil {
		t.Fatalf("DeletePaths: %v", err)
	}

	// A rejected request must leave a deny record with the anonymous principal.
	if _, err := client.ReadFile(context.Background(), &kvsv1.ReadFileRequest{LogicalPath: path}); err == nil {
		t.Fatal("expected unauthenticated read to fail")
	}

	records := parseAuditRecords(t, &buf)
	if len(records) == 0 {
		t.Fatal("no audit records written")
	}

	for _, tc := range []struct {
		action   string
		decision string
	}{
		{actionWrite, "allow"},
		{actionRead, "allow"},
		{actionVersion, "allow"},
		{actionDelete, "allow"},
		{actionRead, "deny"},
	} {
		record := findAuditRecord(records, tc.action, tc.decision, "")
		if record == nil {
			t.Fatalf("missing audit record action=%s decision=%s; records=%+v", tc.action, tc.decision, records)
		}
		if ts, _ := record["ts"].(string); ts == "" {
			t.Errorf("record %+v is missing a timestamp", record)
		}
		if method, _ := record["method"].(string); method == "" {
			t.Errorf("record %+v is missing a method", record)
		}
	}

	allow := findAuditRecord(records, actionWrite, "allow", path)
	if allow == nil {
		t.Fatalf("missing write audit record for %s", path)
	}
	if principal, _ := allow["principal"].(string); !strings.HasPrefix(principal, "bearer:") {
		t.Errorf("write principal = %q, want bearer:<digest>", principal)
	}

	deny := findAuditRecord(records, actionRead, "deny", "")
	if deny == nil {
		t.Fatal("missing deny audit record")
	}
	if principal, _ := deny["principal"].(string); principal != anonymousPrincipal {
		t.Errorf("deny principal = %q, want %q", principal, anonymousPrincipal)
	}

	if strings.Contains(buf.String(), token) {
		t.Fatal("audit trail leaked the configured bearer token")
	}
}

func TestAuditLogPathWritesJSONLWithRestrictedPermissions(t *testing.T) {
	dataDir := t.TempDir()
	store, err := local.Open(local.Config{DataDir: dataDir, MaxHotVersions: 2})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	server, err := NewServerWithAuth(store, Config{AuthToken: "file-token", AuditLogPath: path})
	if err != nil {
		t.Fatalf("NewServerWithAuth: %v", err)
	}

	ctx := withPrincipal(context.Background(), principalForToken("file-token"))
	if _, err := server.UpsertFiles(ctx, &kvsv1.UpsertFilesRequest{
		Writes: []*kvsv1.PathWrite{{LogicalPath: "/audit/file.yaml", Content: []byte("v1")}},
	}); err != nil {
		t.Fatalf("UpsertFiles: %v", err)
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close server: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat audit log: %v", err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("audit log mode = %o, want no group/other access", perm)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read audit log: %v", err)
	}
	records := parseAuditRecords(t, bytes.NewBuffer(data))
	if len(records) != 1 {
		t.Fatalf("audit records = %d, want 1: %s", len(records), string(data))
	}
	if records[0]["action"] != actionWrite || records[0]["decision"] != "allow" {
		t.Fatalf("unexpected audit record: %+v", records[0])
	}
	if records[0]["logical_path"] != "/audit/file.yaml" {
		t.Fatalf("logical_path = %v, want /audit/file.yaml", records[0]["logical_path"])
	}
}

func TestAuditLogPathFromEnv(t *testing.T) {
	dataDir := t.TempDir()
	store, err := local.Open(local.Config{DataDir: dataDir, MaxHotVersions: 2})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	path := filepath.Join(t.TempDir(), "env-audit.jsonl")
	t.Setenv(AuditLogEnv, path)

	server, err := NewServerWithAuth(store, Config{AuthToken: "env-token"})
	if err != nil {
		t.Fatalf("NewServerWithAuth: %v", err)
	}
	ctx := withPrincipal(context.Background(), principalForToken("env-token"))
	if _, err := server.ReadFile(ctx, &kvsv1.ReadFileRequest{LogicalPath: "/missing"}); err == nil {
		t.Fatal("expected read of missing path to fail")
	}
	if err := server.Close(); err != nil {
		t.Fatalf("close server: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s audit log: %v", AuditLogEnv, err)
	}
	records := parseAuditRecords(t, bytes.NewBuffer(data))
	if len(records) != 1 || records[0]["decision"] != "deny" {
		t.Fatalf("unexpected audit records: %+v", records)
	}
}
