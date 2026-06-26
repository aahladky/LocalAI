// Tests for parallel download internals. Lives in package downloader
// (internal) so we can test unexported functions.
package downloader

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestSplitChunks(t *testing.T) {
	t.Run("even split", func(t *testing.T) {
		chunks := splitChunks(1000, 4)
		if len(chunks) != 4 {
			t.Fatalf("expected 4 chunks, got %d", len(chunks))
		}
		if chunks[0].start != 0 || chunks[0].end != 250 {
			t.Fatalf("chunk 0: expected [0,250), got [%d,%d)", chunks[0].start, chunks[0].end)
		}
		if chunks[3].start != 750 || chunks[3].end != 1000 {
			t.Fatalf("chunk 3: expected [750,1000), got [%d,%d)", chunks[3].start, chunks[3].end)
		}
	})

	t.Run("remainder goes to last chunk", func(t *testing.T) {
		chunks := splitChunks(1003, 4)
		if chunks[3].end != 1003 {
			t.Fatalf("last chunk should end at 1003, got %d", chunks[3].end)
		}
	})

	t.Run("single chunk", func(t *testing.T) {
		chunks := splitChunks(500, 1)
		if len(chunks) != 1 {
			t.Fatalf("expected 1 chunk, got %d", len(chunks))
		}
		if chunks[0].start != 0 || chunks[0].end != 500 {
			t.Fatalf("expected [0,500), got [%d,%d)", chunks[0].start, chunks[0].end)
		}
	})

	t.Run("more chunks than bytes", func(t *testing.T) {
		chunks := splitChunks(3, 8)
		if len(chunks) != 8 {
			t.Fatalf("expected 8 chunks, got %d", len(chunks))
		}
	})

	t.Run("zero concurrency defaults to 1", func(t *testing.T) {
		chunks := splitChunks(100, 0)
		if len(chunks) != 1 {
			t.Fatalf("expected 1 chunk, got %d", len(chunks))
		}
	})
}
func TestSplitChunksEmpty(t *testing.T) {
	chunks := splitChunks(0, 8)
	if len(chunks) != 8 {
		t.Fatalf("expected 8 chunks for zero size, got %d", len(chunks))
	}
	for _, c := range chunks {
		if c.start != 0 || c.end != 0 {
			t.Fatalf("expected all empty chunks, got %v", c)
		}
	}

	// totalSize < concurrency produces mostly empty chunks with the
	// remainder in the last one.
	chunks = splitChunks(3, 8)
	nonEmpty := 0
	for _, c := range chunks {
		if c.end > c.start {
			nonEmpty++
		}
	}
	if nonEmpty != 1 {
		t.Fatalf("expected 1 non-empty chunk, got %d", nonEmpty)
	}
	last := chunks[len(chunks)-1]
	if last.start != 0 || last.end != 3 {
		t.Fatalf("expected last chunk [0,3), got %v", last)
	}
}

func TestPreAllocateFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.bin")

	t.Run("creates file of correct size", func(t *testing.T) {
		if err := preAllocateFile(path, 1024); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() != 1024 {
			t.Fatalf("expected size 1024, got %d", info.Size())
		}
	})

	t.Run("reuses file with correct size", func(t *testing.T) {
		if err := preAllocateFile(path, 1024); err != nil {
			t.Fatal(err)
		}
		info, _ := os.Stat(path)
		if info.Size() != 1024 {
			t.Fatalf("expected size 1024, got %d", info.Size())
		}
	})

	t.Run("truncates file with wrong size", func(t *testing.T) {
		if err := preAllocateFile(path, 2048); err != nil {
			t.Fatal(err)
		}
		info, _ := os.Stat(path)
		if info.Size() != 2048 {
			t.Fatalf("expected size 2048, got %d", info.Size())
		}
	})
}

func TestParallelMeta(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "meta.json")

	t.Run("save and load", func(t *testing.T) {
		meta := &parallelMeta{
			Version:   1,
			TotalSize: 1000,
			ChunkSize: 250,
			Completed: []bool{true, true, false, false},
		}
		saveParallelMeta(path, meta)

		chunks := splitChunks(1000, 4)
		loaded := loadParallelMeta(path, 1000, chunks)
		if loaded == nil {
			t.Fatal("expected to load meta, got nil")
		}
		if loaded.Completed[0] != true || loaded.Completed[2] != false {
			t.Fatalf("completed mismatch: %v", loaded.Completed)
		}
	})

	t.Run("returns nil for missing file", func(t *testing.T) {
		chunks := splitChunks(1000, 4)
		loaded := loadParallelMeta("/nonexistent/meta.json", 1000, chunks)
		if loaded != nil {
			t.Fatal("expected nil for missing file")
		}
	})

	t.Run("returns nil for mismatched total size", func(t *testing.T) {
		meta := &parallelMeta{Version: 1, TotalSize: 1000, ChunkSize: 250, Completed: []bool{true, true, false, false}}
		saveParallelMeta(path, meta)

		chunks := splitChunks(2000, 4)
		loaded := loadParallelMeta(path, 2000, chunks)
		if loaded != nil {
			t.Fatal("expected nil for mismatched size")
		}
	})
}

func TestDownloadParallel(t *testing.T) {
	m := newMockServer(t, 50000)

	t.Run("downloads full file with multiple chunks", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "model.bin")

		err := downloadParallel(
			context.Background(),
			m.url(),
			dest,
			int64(len(m.data)),
			4, 0, 0,
			func(string, string, string, float64) {},
			1, 1,
		)
		if err != nil {
			t.Fatal(err)
		}

		result, err := os.ReadFile(dest)
		if err != nil {
			t.Fatal(err)
		}
		if len(result) != len(m.data) {
			t.Fatalf("size mismatch: got %d, want %d", len(result), len(m.data))
		}
		if sha256.Sum256(result) != sha256.Sum256(m.data) {
			t.Fatal("content mismatch")
		}
	})

	t.Run("single chunk (concurrency=1)", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "model.bin")

		err := downloadParallel(
			context.Background(),
			m.url(),
			dest,
			int64(len(m.data)),
			1, 0, 0,
			func(string, string, string, float64) {},
			1, 1,
		)
		if err != nil {
			t.Fatal(err)
		}

		result, _ := os.ReadFile(dest)
		if len(result) != len(m.data) {
			t.Fatalf("size mismatch: got %d, want %d", len(result), len(m.data))
		}
	})

	t.Run("progress callback receives updates", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "model.bin")

		var updates atomic.Int32
		err := downloadParallel(
			context.Background(),
			m.url(),
			dest,
			int64(len(m.data)),
			4, 0, 0,
			func(fileName, current, total string, pct float64) {
				updates.Add(1)
			},
			1, 1,
		)
		if err != nil {
			t.Fatal(err)
		}
		if updates.Load() == 0 {
			t.Fatal("expected at least one progress update")
		}
	})
}

func TestDownloadParallel_CancelBeforeStart(t *testing.T) {
	m := newMockServer(t, 100000)

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := downloadParallel(
		ctx,
		m.url(),
		dest,
		int64(len(m.data)),
		4, 0, 0,
		func(string, string, string, float64) {},
		1, 1,
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDownloadChunkRejects200ForRange(t *testing.T) {
	m := newMockServer(t, 11)
	m.ignoreRange = true

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")
	if err := preAllocateFile(dest, int64(len(m.data))); err != nil {
		t.Fatalf("pre-allocate: %v", err)
	}

	var bw atomic.Int64
	chunk := chunkRange{index: 0, start: 0, end: 5}
	_, err := downloadChunk(context.Background(), m.url(), dest, chunk, &bw)
	if err == nil {
		t.Fatal("expected error when server returns 200 OK for Range request")
	}
	if bw.Load() != 0 {
		t.Fatalf("bytesWritten should be 0, got %d", bw.Load())
	}
}

// === Tier 1: Resume correctness ===

func TestDownloadParallel_ResumeAfterChunkFailure(t *testing.T) {
	m := newMockServer(t, 40000) // 40KB -> 4x10KB chunks
	m.failChunkNTimes(20000, 2)

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")

	err := downloadParallel(
		context.Background(), m.url(), dest,
		int64(len(m.data)),
		4, 1, 10*time.Millisecond,
		nil, 1, 1,
	)
	if err == nil {
		t.Fatal("expected first call to fail")
	}

	meta := loadParallelMeta(
		dest+parallelMetaFileSuffix,
		int64(len(m.data)),
		splitChunks(int64(len(m.data)), 4),
	)
	if meta == nil {
		t.Fatal("expected meta file after partial failure")
	}
	if !meta.Completed[0] || !meta.Completed[1] || !meta.Completed[3] {
		t.Fatalf("expected chunks 0,1,3 complete; got %v", meta.Completed)
	}
	if meta.Completed[2] {
		t.Fatal("chunk 2 should not be complete")
	}

	requestsBeforeResume := len(m.requests())

	err = downloadParallel(
		context.Background(), m.url(), dest,
		int64(len(m.data)),
		4, 3, 10*time.Millisecond,
		nil, 1, 1,
	)
	if err != nil {
		t.Fatalf("expected resume to succeed: %v", err)
	}

	newRequests := len(m.requests()) - requestsBeforeResume
	if newRequests != 1 {
		t.Fatalf("expected 1 request on resume, got %d", newRequests)
	}

	got, _ := os.ReadFile(dest)
	if sha256.Sum256(got) != sha256.Sum256(m.data) {
		t.Fatal("final content mismatch")
	}

	if _, err := os.Stat(dest + parallelMetaFileSuffix); !os.IsNotExist(err) {
		t.Fatal("meta file should be removed after success")
	}
}

func TestDownloadParallel_InvalidMetaTriggersFullRestart(t *testing.T) {
	cases := []struct {
		name    string
		writeOp func(path string)
	}{
		{"corrupt JSON", func(p string) { os.WriteFile(p, []byte("not json"), 0644) }},
		{"wrong total size", func(p string) {
			saveParallelMeta(p, &parallelMeta{
				Version: 1, TotalSize: 99999, ChunkSize: 10000,
				Completed: []bool{true, true, true, true},
			})
		}},
		{"wrong chunk count", func(p string) {
			saveParallelMeta(p, &parallelMeta{
				Version: 1, TotalSize: 40000, ChunkSize: 5000,
				Completed: []bool{true, true, true, true, true, true, true, true},
			})
		}},
		{"unknown version", func(p string) {
			saveParallelMeta(p, &parallelMeta{
				Version: 99, TotalSize: 40000, ChunkSize: 10000,
				Completed: []bool{true, true, true, true},
			})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newMockServer(t, 40000)
			dir := t.TempDir()
			dest := filepath.Join(dir, "model.bin")

			tc.writeOp(dest + parallelMetaFileSuffix)
			os.WriteFile(dest+".partial", []byte{}, 0644)

			err := downloadParallel(
				context.Background(), m.url(), dest, int64(len(m.data)),
				4, 0, 0, nil, 1, 1,
			)
			if err != nil {
				t.Fatal(err)
			}

			got, _ := os.ReadFile(dest)
			if sha256.Sum256(got) != sha256.Sum256(m.data) {
				t.Fatal("content mismatch — invalid meta should have caused full redownload")
			}
		})
	}
}

// === Tier 2: Server misbehavior defenses ===

func TestDownloadChunk_DetectsTruncatedBody(t *testing.T) {
	m := newMockServer(t, 10000)
	m.truncateAt = 100

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")
	if err := preAllocateFile(dest, int64(len(m.data))); err != nil {
		t.Fatal(err)
	}

	var bw atomic.Int64
	chunk := chunkRange{index: 0, start: 0, end: 2500}
	_, err := downloadChunk(context.Background(), m.url(), dest, chunk, &bw)
	if err == nil {
		t.Fatal("expected error for truncated body")
	}
}

func TestDownloadChunk_DetectsMismatchedContentRange(t *testing.T) {
	m := &mockServer{data: make([]byte, 10000)}
	for i := range m.data {
		m.data[i] = byte(i % 251)
	}
	m.failChunkOnce = make(map[int64]*atomic.Int32)
	m.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Return 206 but claim the range is 0-99 when 0-2499 was requested.
		w.Header().Set("Content-Range", "bytes 0-99/10000")
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		w.Write(m.data[:100])
	}))
	defer m.server.Close()

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")
	if err := preAllocateFile(dest, int64(len(m.data))); err != nil {
		t.Fatal(err)
	}

	var bw atomic.Int64
	chunk := chunkRange{index: 0, start: 0, end: 2500}
	_, err := downloadChunk(context.Background(), m.url(), dest, chunk, &bw)
	if err == nil {
		t.Fatal("expected error for mismatched Content-Range")
	}
}

// === Tier 3: Cancellation semantics ===

func TestDownloadParallel_CancelMidStream(t *testing.T) {
	m := newMockServer(t, 1_000_000)
	m.serveDelay = 200 * time.Millisecond

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := downloadParallel(ctx, m.url(), dest, int64(len(m.data)),
		4, 0, 0, nil, 1, 1)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestDownloadParallel_CancelDuringRetryBackoff(t *testing.T) {
	m := newMockServer(t, 40000)
	m.failChunkNTimes(0, 99)

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	done := make(chan error, 1)
	go func() {
		done <- downloadParallel(ctx, m.url(), dest, int64(len(m.data)),
			4, 10, 5*time.Second, nil, 1, 1)
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancel during backoff did not return promptly")
	}
}
