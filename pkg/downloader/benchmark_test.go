// benchmark_test.go — download throughput benchmark for pkg/downloader.
//
// Run with:
//   go test -bench=BenchmarkDownload -benchtime=1x -timeout=30m ./pkg/downloader/ \
//     -hf-url="https://huggingface.co/TheBloke/TinyLlama-1.1B-Chat-v1.0-GGUF/resolve/main/tinyllama-1.1b-chat-v1.0.Q4_K_M.gguf"
//
// The default URL is a small (~670MB) GGUF that lives behind HF's xet-bridge CDN.
// Override with -hf-url to test a larger file. The benchmark reports ns/op (total
// wall-clock time for one download) and B/s (throughput). Run it before and after
// the parallel-download changes to get concrete before/after numbers for the PR.
//
// The benchmark downloads to a temp directory and cleans up after itself.
// It skips if -hf-url is empty and HF_BENCH_URL is also unset.

package downloader

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

var benchURL = flag.String("hf-url",
	getenvDefault("HF_BENCH_URL", ""),
	"HuggingFace direct download URL for the benchmark file")

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// BenchmarkDownload measures wall-clock time and throughput for a single
// download through the existing single-stream path. After the parallel
// changes land, the same benchmark exercises the new path (assuming the
// server supports ranges and the file exceeds the threshold).
func BenchmarkDownload(b *testing.B) {
	if *benchURL == "" {
		b.Skip("no benchmark URL: set -hf-url or HF_BENCH_URL")
	}

	b.Run("single_stream", func(b *testing.B) {
		benchmarkDownloadWithURL(b, *benchURL, WithConcurrency(1))
	})
}

func benchmarkDownloadWithURL(b *testing.B, url string, opts ...DownloadOption) {
	b.Helper()

	// Probe the file size so we can report throughput.
	size := probeSize(b, url)
	if size <= 0 {
		b.Fatalf("could not determine file size for %s", url)
	}

	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		dir := b.TempDir()
		dest := filepath.Join(dir, "model.gguf")

		uri := URI(url)
		start := time.Now()
		opts := append([]DownloadOption{}, opts...)
		err := uri.DownloadFile(dest, "", 1, 1, func(fileName, current, total string, pct float64) {
			// Optionally log progress; suppress for benchmark noise.
		}, opts...)
		elapsed := time.Since(start)

		if err != nil {
			b.Fatalf("download failed: %v", err)
		}

		// Verify the file landed at the expected size.
		info, err := os.Stat(dest)
		if err != nil {
			b.Fatalf("stat failed: %v", err)
		}
		if info.Size() != size {
			b.Fatalf("size mismatch: got %d, want %d", info.Size(), size)
		}

		throughput := float64(size) / elapsed.Seconds() / (1024 * 1024) // MiB/s
		b.ReportMetric(throughput, "MiB/s")
	}
}

// BenchmarkDownloadParallel is the "after" benchmark. It exercises the
// parallel path with a configurable concurrency. Before the parallel
// changes land, this is identical to BenchmarkDownload (concurrency=1).
//
//   go test -bench=BenchmarkDownloadParallel -benchtime=1x -timeout=30m ./pkg/downloader/ \
//     -hf-url="..." -concurrency=8
var concurrency = flag.Int("concurrency", 8, "number of parallel download chunks")

func BenchmarkDownloadParallel(b *testing.B) {
	if *benchURL == "" {
		b.Skip("no benchmark URL: set -hf-url or HF_BENCH_URL")
	}

	b.Run("parallel", func(b *testing.B) {
		benchmarkDownloadWithURL(b, *benchURL, WithConcurrency(*concurrency))
	})
}

// BenchmarkSHA256PostAssembly measures how long a post-download SHA256 pass
// takes on the target hardware. This is the cost we pay for giving up
// streaming hash in the parallel path. Report it in the PR so reviewers
// know the tradeoff is deliberate.
func BenchmarkSHA256PostAssembly(b *testing.B) {
	if *benchURL == "" {
		b.Skip("no benchmark URL: set -hf-url or HF_BENCH_URL")
	}

	size := probeSize(b, *benchURL)
	if size <= 0 {
		b.Fatalf("could not determine file size for %s", *benchURL)
	}

	// Download once to a temp file.
	dir := b.TempDir()
	dest := filepath.Join(dir, "model.gguf")
	uri := URI(*benchURL)
	if err := uri.DownloadFile(dest, "", 1, 1, func(_, _, _ string, _ float64) {}); err != nil {
		b.Fatalf("download failed: %v", err)
	}

	b.SetBytes(size)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		f, err := os.Open(dest)
		if err != nil {
			b.Fatal(err)
		}
		h := sha256.New()
		if _, err := io.Copy(h, f); err != nil {
			f.Close()
			b.Fatal(err)
		}
		f.Close()
		_ = fmt.Sprintf("%x", h.Sum(nil))
	}
}

// probeSize does a HEAD + optional Range probe to get the file size.
// Duplicates the logic in URI.ContentLength but stays local to the
// benchmark so it doesn't depend on that method's error semantics.
func probeSize(b *testing.B, url string) int64 {
	b.Helper()

	client := &http.Client{Timeout: 30 * time.Second}

	resp, err := client.Head(url)
	if err != nil {
		b.Logf("HEAD failed: %v", err)
		return 0
	}
	resp.Body.Close()

	if resp.ContentLength > 0 {
		return resp.ContentLength
	}

	// Fall back to Range probe.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return 0
	}
	req.Header.Set("Range", "bytes=0-0")
	resp2, err := client.Do(req)
	if err != nil {
		return 0
	}
	resp2.Body.Close()

	if resp2.StatusCode != http.StatusPartialContent {
		return 0
	}
	cr := resp2.Header.Get("Content-Range")
	if cr == "" {
		return 0
	}
	// Parse "bytes 0-0/TOTAL"
	var total int64
	if _, err := fmt.Sscanf(cr, "bytes 0-0/%d", &total); err != nil {
		return 0
	}
	return total
}
