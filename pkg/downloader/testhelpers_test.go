package downloader

import (
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// mockServer is a configurable HTTP server for testing the downloader.
// Zero value serves the data correctly with full range support.
type mockServer struct {
	data []byte

	// failChunkOnce: byte range -> remaining failure count. The range
	// is keyed by the chunk's start byte (use mockServer.failChunkN
	// to populate from a chunk index given totalSize/concurrency).
	failChunkOnce map[int64]*atomic.Int32

	// truncateAt: if non-zero, body for range requests is cut to this
	// many bytes before EOF.
	truncateAt int64

	// serveDelay: per-request delay before responding (simulates slow CDN).
	serveDelay time.Duration

	// ignoreRange: respond 200 OK with full body even when Range is set.
	ignoreRange bool

	// requestLog: every request's Range header value, in order.
	mu         sync.Mutex
	requestLog []string

	server *httptest.Server
}

func newMockServer(t *testing.T, dataSize int) *mockServer {
	data := make([]byte, dataSize)
	// Deterministic content so test failures are reproducible.
	for i := range data {
		data[i] = byte(i % 251)
	}
	m := &mockServer{
		data:          data,
		failChunkOnce: make(map[int64]*atomic.Int32),
	}
	m.server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.server.Close)
	return m
}

func (m *mockServer) url() string { return m.server.URL }
func (m *mockServer) sha() string {
	h := sha256.Sum256(m.data)
	return fmt.Sprintf("%x", h)
}

func (m *mockServer) failChunkNTimes(chunkStart int64, n int32) {
	var c atomic.Int32
	c.Store(n)
	m.failChunkOnce[chunkStart] = &c
}

func (m *mockServer) requests() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.requestLog))
	copy(out, m.requestLog)
	return out
}

func (m *mockServer) handle(w http.ResponseWriter, r *http.Request) {
	rh := r.Header.Get("Range")
	m.mu.Lock()
	m.requestLog = append(m.requestLog, rh)
	m.mu.Unlock()

	if m.serveDelay > 0 {
		time.Sleep(m.serveDelay)
	}

	if rh == "" || m.ignoreRange {
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(m.data)))
		w.WriteHeader(http.StatusOK)
		w.Write(m.data)
		return
	}

	var start, end int64
	if _, err := fmt.Sscanf(rh, "bytes=%d-%d", &start, &end); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// Configurable per-chunk failures (for retry/resume tests).
	if counter, ok := m.failChunkOnce[start]; ok {
		if counter.Add(-1) >= 0 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	if start >= int64(len(m.data)) || end >= int64(len(m.data)) || start > end {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(m.data)))
	w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
	w.WriteHeader(http.StatusPartialContent)

	body := m.data[start : end+1]
	if m.truncateAt > 0 && int64(len(body)) > m.truncateAt {
		body = body[:m.truncateAt]
	}
	w.Write(body)
}
