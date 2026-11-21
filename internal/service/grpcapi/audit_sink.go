package grpcapi

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// chAuditSink writes one JSONEachRow event to the shared audit.events table.
type chAuditSink struct {
	endpoint string
	database string
	user     string
	password string
	client   *http.Client
}

func newCHAuditSinkFromEnv() *chAuditSink {
	endpoint := strings.TrimSpace(os.Getenv("KVS_AUDIT_CH_URL"))
	if endpoint == "" {
		return nil
	}
	database := strings.TrimSpace(os.Getenv("KVS_AUDIT_CH_DATABASE"))
	if database == "" {
		database = "audit"
	}
	return &chAuditSink{
		endpoint: strings.TrimRight(endpoint, "/"),
		database: database,
		user:     strings.TrimSpace(os.Getenv("KVS_AUDIT_CH_USER")),
		password: os.Getenv("KVS_AUDIT_CH_PASSWORD"),
		client:   &http.Client{Timeout: 10 * time.Second},
	}
}

func (s *chAuditSink) write(ctx context.Context, event map[string]any) error {
	if s == nil {
		return nil
	}
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	u, err := url.Parse(s.endpoint)
	if err != nil {
		return err
	}
	q := u.Query()
	q.Set("query", fmt.Sprintf("INSERT INTO %s.events FORMAT JSONEachRow", s.database))
	q.Set("date_time_input_format", "best_effort")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(append(raw, '\n')))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	if s.user != "" || s.password != "" {
		req.SetBasicAuth(s.user, s.password)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return fmt.Errorf("kvs audit insert: status %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

type auditChainState struct {
	WriterID string `json:"writerId"`
	Seq      uint64 `json:"seq"`
	PrevHash string `json:"prevHash"`
}

type auditChain struct {
	mu        sync.Mutex
	statePath string
	writerID  string
	seq       uint64
	prevHash  string
	sink      *chAuditSink
	dropped   uint64
	broken    bool
}

func newAuditChainFromEnv(logPath string) *auditChain {
	sink := newCHAuditSinkFromEnv()
	writerID := strings.TrimSpace(os.Getenv("KVS_AUDIT_WRITER_ID"))
	if writerID == "" {
		host, _ := os.Hostname()
		var suffix [4]byte
		_, _ = rand.Read(suffix[:])
		writerID = "kvs-" + host + "-" + hex.EncodeToString(suffix[:])
	}
	statePath := strings.TrimSpace(os.Getenv("KVS_AUDIT_STATE_FILE"))
	if statePath == "" && strings.TrimSpace(logPath) != "" {
		statePath = logPath + ".state"
	}
	c := &auditChain{
		statePath: statePath,
		writerID:  writerID,
		prevHash:  strings.Repeat("0", 64),
		sink:      sink,
	}
	if statePath != "" {
		if raw, err := os.ReadFile(filepath.Clean(statePath)); err == nil {
			var state auditChainState
			if json.Unmarshal(raw, &state) == nil && state.WriterID == c.writerID {
				c.seq = state.Seq
				if strings.TrimSpace(state.PrevHash) != "" {
					c.prevHash = state.PrevHash
				}
			}
		}
	}
	return c
}

// emit records one KVS RPC event. It is a no-op when ClickHouse is not
// configured; the existing JSONL slog sink remains the local fallback.
func (c *auditChain) emit(ctx context.Context, principal, action, logicalPath, method, decision, code string, started time.Time) {
	if c == nil || c.sink == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.broken {
		return
	}

	event := map[string]any{
		"timestamp":      started.UTC().Format(time.RFC3339Nano),
		"event_id":       newAuditID(),
		"writer_id":      c.writerID,
		"seq":            c.seq,
		"prev_hash":      c.prevHash,
		"service":        "kvs",
		"env":            strings.TrimSpace(os.Getenv("AUDIT_ENV")),
		"scope":          "kv",
		"action":         action,
		"decision":       decision,
		"reason":         code,
		"resource_type":  "kv_key",
		"actor_subject":  principal,
		"resource":       logicalPath,
		"request_id":     "",
		"method":         method,
		"status":         0,
		"correlation_id": "",
	}
	canonical, err := json.Marshal(event)
	if err != nil {
		c.dropped++
		return
	}
	sum := sha256.Sum256(append([]byte(c.prevHash), canonical...))
	hash := hex.EncodeToString(sum[:])
	event["hash"] = hash
	event["raw_json"] = string(canonical)
	// Retry the same event/seq so an ambiguous ClickHouse success cannot make
	// the next event reuse the sequence with a different hash.
	ok := false
	for attempt := 0; attempt < 3; attempt++ {
		if err := c.sink.write(ctx, event); err == nil {
			ok = true
			break
		}
		time.Sleep(time.Duration(attempt+1) * 100 * time.Millisecond)
	}
	if !ok {
		// The event may or may not have committed. Stop this writer's chain
		// instead of reusing the sequence for a different event.
		c.broken = true
		c.dropped++
		return
	}
	c.seq++
	c.prevHash = hash
	if c.statePath != "" {
		state, _ := json.Marshal(auditChainState{WriterID: c.writerID, Seq: c.seq, PrevHash: c.prevHash})
		_ = os.WriteFile(filepath.Clean(c.statePath), state, 0o600)
	}
}

func newAuditID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 36)
	}
	return hex.EncodeToString(b[:])
}
