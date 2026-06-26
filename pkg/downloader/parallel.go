package downloader

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mudler/xlog"
)

// defaultParallelConcurrency is the number of concurrent chunk download
// goroutines. Overridable via WithConcurrency option or the
// LOCALAI_DOWNLOAD_CONCURRENCY env var (Phase 6).
const defaultParallelConcurrency = 8

// parallelSizeThreshold is the minimum file size (in bytes) to trigger
// parallel downloads. Files below this use the existing single-stream
// path. Exported so tests can lower it.
var parallelSizeThreshold int64 = 50 * 1024 * 1024 // 50MB

// SetParallelSizeThreshold overrides the parallel download size threshold.
// For testing only.
func SetParallelSizeThreshold(v int64) { parallelSizeThreshold = v }

// GetParallelSizeThreshold returns the current parallel download size threshold.
// For testing only.
func GetParallelSizeThreshold() int64 { return parallelSizeThreshold }

// parallelMetaFileSuffix is the extension for the resume metadata file
// that sits alongside the .partial file.
const parallelMetaFileSuffix = ".partial.meta"

// parallelMeta tracks which chunks of a parallel download have been
// written to disk. On resume, completed chunks are skipped.
type parallelMeta struct {
	Version    int   `json:"version"`
	TotalSize  int64 `json:"totalSize"`
	ChunkSize  int64 `json:"chunkSize"`
	Completed  []bool `json:"completed"`
}

// chunkRange describes a byte range [start, end) for one download goroutine.
type chunkRange struct {
	index int
	start int64
	end   int64 // exclusive
}

// splitChunks divides totalSize into n roughly-equal byte ranges.
func splitChunks(totalSize int64, n int) []chunkRange {
	if n <= 0 {
		n = 1
	}
	chunkSize := totalSize / int64(n)
	chunks := make([]chunkRange, n)
	var start int64
	for i := 0; i < n; i++ {
		end := start + chunkSize
		if i == n-1 {
			end = totalSize // last chunk gets the remainder
		}
		chunks[i] = chunkRange{index: i, start: start, end: end}
		start = end
	}
	return chunks
}

// downloadParallel downloads a file using N concurrent range requests,
// writing directly at offsets into a pre-allocated file. This is the
// fast path for range-capable servers with large files.
//
// The function:
//   - Pre-allocates the destination file at full size
//   - Spawns one goroutine per chunk, each doing GET with a Range header
//   - Writes each chunk at the correct offset via WriteAt
//   - Tracks total bytes written atomically for progress reporting
//   - Returns on first error; remaining goroutines are allowed to finish
//     so resume metadata accurately reflects what succeeded.
//
// Retry: each chunk is retried up to maxRetries times with exponential
// backoff + jitter before the whole parallel download fails.
func downloadParallel(
	ctx context.Context,
	url string,
	filePath string,
	totalSize int64,
	concurrency int,
	maxRetries int,
	retryBackoff time.Duration,
	downloadStatus func(string, string, string, float64),
	fileN, total int,
) error {
	if concurrency <= 0 {
		concurrency = defaultParallelConcurrency
	}
	// Cap to avoid abuse / resource exhaustion.
	const maxConcurrency = 64
	if concurrency > maxConcurrency {
		concurrency = maxConcurrency
	}
	// Don't use more goroutines than there are bytes.
	if int64(concurrency) > totalSize {
		concurrency = int(totalSize)
	}
	if concurrency <= 0 {
		concurrency = 1
	}

	chunks := splitChunks(totalSize, concurrency)

	// Ensure parent directory exists.
	if err := os.MkdirAll(filepath.Dir(filePath), 0750); err != nil {
		return fmt.Errorf("parallel: create parent dir for %s: %w", filePath, err)
	}

	// Try to load resume metadata.
	metaPath := filePath + parallelMetaFileSuffix
	meta := loadParallelMeta(metaPath, totalSize, chunks)
	if meta != nil {
		xlog.Info("[parallel] Resuming parallel download", "file", filePath, "chunks", len(chunks), "completed", countCompleted(meta))
	}

	// Pre-allocate the file.
	tmpPath := filePath + ".partial"
	if err := preAllocateFile(tmpPath, totalSize); err != nil {
		return fmt.Errorf("parallel: pre-allocate %s: %w", tmpPath, err)
	}

	// Progress tracking.
	var bytesWritten atomic.Int64

	// Spawn one goroutine per chunk.
	var wg sync.WaitGroup
	errCh := make(chan error, len(chunks))

	// Per-chunk completion tracking for resume metadata.
	var completedMu sync.Mutex
	completed := make([]bool, len(chunks))
	// Mark already-completed chunks from resume metadata.
	if meta != nil {
		for i, done := range meta.Completed {
			completed[i] = done
		}
	}

	for _, chunk := range chunks {
		// Skip empty chunks (can happen when totalSize < concurrency).
		if chunk.end <= chunk.start {
			completed[chunk.index] = true
			continue
		}

		// Skip already-completed chunks on resume.
		if completed[chunk.index] {
			bytesWritten.Add(chunk.end - chunk.start)
			continue
		}

		wg.Add(1)
		go func(c chunkRange) {
			defer wg.Done()
			written, err := downloadChunkWithRetry(ctx, url, tmpPath, c, &bytesWritten, maxRetries, retryBackoff)
			if err != nil {
				errCh <- fmt.Errorf("chunk %d (%d-%d): %w", c.index, c.start, c.end, err)
				return
			}
			if written != c.end-c.start {
				errCh <- fmt.Errorf("chunk %d wrote %d bytes, expected %d", c.index, written, c.end-c.start)
				return
			}
			completedMu.Lock()
			completed[c.index] = true
			completedMu.Unlock()
		}(chunk)
	}

	// Progress reporter goroutine — samples at ~2Hz.
	done := make(chan struct{})
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				// Final update after all chunks complete.
				reportProgress(&bytesWritten, totalSize, tmpPath, downloadStatus, fileN, total)
				return
			case <-ticker.C:
				reportProgress(&bytesWritten, totalSize, tmpPath, downloadStatus, fileN, total)
			}
		}
	}()

	// Wait for all chunks.
	wg.Wait()
	close(done)

	// Check for errors.
	select {
	case err := <-errCh:
		// Save resume metadata before returning. The completed[]
		// slice is already accurate — each goroutine marked its
		// chunk on success.
		saveParallelMeta(metaPath, &parallelMeta{
			Version:   1,
			TotalSize: totalSize,
			ChunkSize: chunks[0].end - chunks[0].start,
			Completed: completed,
		})
		return err
	default:
	}

	// All chunks succeeded. Remove resume metadata.
	os.Remove(metaPath)

	// Final progress report (the ticker may not have fired if the
	// download was faster than 500ms).
	reportProgress(&bytesWritten, totalSize, tmpPath, downloadStatus, fileN, total)

	// Verify total bytes.
	finalWritten := bytesWritten.Load()
	if finalWritten != totalSize {
		return fmt.Errorf("parallel: size mismatch: wrote %d of %d bytes", finalWritten, totalSize)
	}

	// Rename .partial to final path.
	if err := os.Rename(tmpPath, filePath); err != nil {
		return fmt.Errorf("parallel: rename %s -> %s: %w", tmpPath, filePath, err)
	}

	xlog.Info("[parallel] Download complete", "file", filePath, "bytes", finalWritten, "chunks", len(chunks))
	return nil
}

// downloadChunkWithRetry wraps downloadChunk with exponential backoff + jitter.
// On failure, it retries up to maxRetries times. Each retry re-creates the
// HTTP request (new connection, re-resolved URL) to avoid hitting the same
// flaky CDN edge node.
//
// Returns the number of bytes successfully written (which equals chunk size).
func downloadChunkWithRetry(
	ctx context.Context,
	url string,
	filePath string,
	chunk chunkRange,
	bytesWritten *atomic.Int64,
	maxRetries int,
	baseBackoff time.Duration,
) (int64, error) {
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff with jitter: base * 2^(attempt-1) + random [0, base)
			backoff := baseBackoff * time.Duration(1<<(attempt-1))
			jitter := time.Duration(rand.Int64N(int64(baseBackoff)))
			wait := backoff + jitter

			xlog.Warn("[parallel] Retrying chunk", "chunk", chunk.index, "attempt", attempt, "wait", wait, "lastError", lastErr)
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(wait):
			}
		}

		written, err := downloadChunk(ctx, url, filePath, chunk, bytesWritten)
		if err == nil {
			return written, nil
		}
		lastErr = err

		// Roll back bytes added by this failed attempt so the shared
		// progress counter doesn't drift above actual file content.
		if written > 0 {
			bytesWritten.Add(-written)
		}

		// Don't retry context cancellation — that's not a transient error.
		if ctx.Err() != nil {
			return 0, lastErr
		}
	}
	return 0, fmt.Errorf("chunk %d failed after %d attempts: %w", chunk.index, maxRetries+1, lastErr)
}

// downloadChunk downloads a single byte range and writes it at the correct
// offset in the target file. Each chunk gets its own HTTP client connection
// and stall watchdog.
//
// Returns the number of bytes written. If the server returns 200 OK instead
// of 206 Partial Content for the Range request, the chunk fails to avoid
// corrupting the assembled file with overlapping full-file data.
func downloadChunk(
	ctx context.Context,
	url string,
	filePath string,
	chunk chunkRange,
	bytesWritten *atomic.Int64,
) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", chunk.start, chunk.end-1))

	resp, err := downloadClient.Do(req)
	if err != nil {
		return 0, err
	}

	if resp.StatusCode != http.StatusPartialContent {
		if resp.StatusCode == http.StatusOK {
			return 0, fmt.Errorf("server ignored Range header and returned full content (200 OK)")
		}
		return 0, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	// Defend against a server that returned 206 but with a Content-Range
	// header that doesn't match our request. Writing data at the wrong
	// offset would corrupt the assembled file.
	cr := resp.Header.Get("Content-Range")
	var respStart, respEnd int64
	if _, err := fmt.Sscanf(cr, "bytes %d-%d/", &respStart, &respEnd); err != nil || respStart != chunk.start || respEnd != chunk.end-1 {
		return 0, fmt.Errorf("Content-Range %q does not match requested chunk %d-%d", cr, chunk.start, chunk.end-1)
	}

	// Wrap with stall watchdog.
	var source io.ReadCloser = resp.Body
	if DownloadStallTimeout > 0 {
		source = newIdleTimeoutReader(resp.Body, DownloadStallTimeout)
	}
	defer source.Close()

	// Open file for writing at offset.
	f, err := os.OpenFile(filePath, os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Read from body, write at offset.
	buf := make([]byte, 32*1024) // 32KB buffer
	offset := chunk.start
	var total int64
	for {
		n, readErr := source.Read(buf)
		if n > 0 {
			written, writeErr := f.WriteAt(buf[:n], offset)
			if writeErr != nil {
				return total, writeErr
			}
			offset += int64(written)
			total += int64(written)
			bytesWritten.Add(int64(written))
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			return total, readErr
		}
	}

	// Verify we wrote the expected amount.
	expected := chunk.end - chunk.start
	if total != expected {
		return total, fmt.Errorf("chunk size mismatch: wrote %d of %d bytes", total, expected)
	}

	return total, nil
}

// preAllocateFile creates a file of exactly the given size. If the file
// already exists with the correct size, it is reused (resume case). If
// it exists with a different size, it is truncated.
func preAllocateFile(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() == size {
		return nil // already correct size (resume)
	}
	return f.Truncate(size)
}

// loadParallelMeta reads resume metadata from disk. Returns nil if the
// file is missing, corrupt, or doesn't match the current download.
func loadParallelMeta(path string, totalSize int64, chunks []chunkRange) *parallelMeta {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var meta parallelMeta
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil
	}
	if meta.Version != 1 || meta.TotalSize != totalSize || len(meta.Completed) != len(chunks) {
		return nil
	}
	return &meta
}

// saveParallelMeta writes resume metadata to disk.
func saveParallelMeta(path string, meta *parallelMeta) {
	data, err := json.Marshal(meta)
	if err != nil {
		xlog.Warn("[parallel] Failed to marshal resume metadata", "error", err)
		return
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		xlog.Warn("[parallel] Failed to save resume metadata", "error", err)
	}
}

// resolveConcurrency returns the effective concurrency for parallel downloads.
// Priority: WithConcurrency option > LOCALAI_DOWNLOAD_CONCURRENCY env var > default (8).
// Returns 1 to force single-stream.
func resolveConcurrency(opt int) int {
	if opt > 0 {
		return opt
	}
	if v := os.Getenv("LOCALAI_DOWNLOAD_CONCURRENCY"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n >= 1 {
			return n
		}
	}
	return defaultParallelConcurrency
}

// defaultMaxRetries is the number of per-chunk retry attempts.
const defaultMaxRetries = 3

// defaultRetryBackoff is the base backoff duration for chunk retries.
const defaultRetryBackoff = 500 * time.Millisecond

// resolveMaxRetries returns the effective max retries for chunk downloads.
// Priority: WithMaxRetries option > LOCALAI_DOWNLOAD_RETRIES env var > default (3).
// An explicit option value of 0 disables retry.
func resolveMaxRetries(opt int) int {
	if opt >= 0 {
		return opt
	}
	if v := os.Getenv("LOCALAI_DOWNLOAD_RETRIES"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n >= 0 {
			return n
		}
	}
	return defaultMaxRetries
}

// resolveRetryBackoff returns the effective base backoff duration.
// Priority: WithRetryBackoff option > LOCALAI_DOWNLOAD_RETRY_BACKOFF env var > default (500ms).
func resolveRetryBackoff(opt time.Duration) time.Duration {
	if opt > 0 {
		return opt
	}
	if v := os.Getenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF"); v != "" {
		d, err := time.ParseDuration(v)
		if err == nil && d > 0 {
			return d
		}
	}
	return defaultRetryBackoff
}

func countCompleted(meta *parallelMeta) int {
	n := 0
	for _, done := range meta.Completed {
		if done {
			n++
		}
	}
	return n
}

// reportProgress sends a progress update via the downloadStatus callback.
func reportProgress(bytesWritten *atomic.Int64, totalSize int64, tmpPath string, downloadStatus func(string, string, string, float64), fileN, total int) {
	if downloadStatus == nil {
		return
	}
	written := bytesWritten.Load()
	if totalSize > 0 {
		pct := float64(written) / float64(totalSize) * 100
		if total > 1 {
			pct = pct / float64(total)
			if fileN > 0 {
				pct += float64(fileN) * 100 / float64(total)
			}
		}
		downloadStatus(tmpPath, formatBytes(written), formatBytes(totalSize), pct)
	} else {
		downloadStatus(tmpPath, formatBytes(written), "", 0)
	}
}
