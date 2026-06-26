package downloader

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// === Tier 4: Stale-state cleanup ===

func TestDownloadFile_DiscardsStaleParallelMetaOnFallback(t *testing.T) {
	m := newMockServer(t, 1024) // below parallelSizeThreshold, forces single-stream

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")

	os.WriteFile(dest+".partial", make([]byte, len(m.data)), 0644)
	saveParallelMeta(dest+parallelMetaFileSuffix, &parallelMeta{
		Version: 1, TotalSize: int64(len(m.data)), ChunkSize: 256,
		Completed: []bool{true, true, true, true},
	})

	err := URI(m.url()).DownloadFileWithContext(
		context.Background(), dest, m.sha(), 1, 1,
		func(string, string, string, float64) {},
	)
	if err != nil {
		t.Fatalf("download failed: %v", err)
	}

	if _, err := os.Stat(dest + parallelMetaFileSuffix); !os.IsNotExist(err) {
		t.Fatal("stale meta should have been removed")
	}
	got, _ := os.ReadFile(dest)
	if sha256.Sum256(got) != sha256.Sum256(m.data) {
		t.Fatal("content mismatch — stale .partial was likely used")
	}
}

// === Tier 5: User-cancel semantics ===

func TestDownloadFile_UserCancelDeletesPartial(t *testing.T) {
	m := newMockServer(t, 10_000_000)
	m.serveDelay = 50 * time.Millisecond

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")

	ctx, cancel := context.WithCancelCause(context.Background())

	var once sync.Once
	progress := func(string, string, string, float64) {
		once.Do(func() { cancel(ErrUserCancelled) })
	}

	err := URI(m.url()).DownloadFileWithContext(ctx, dest, "", 1, 1, progress)
	if err == nil {
		t.Fatal("expected cancel error")
	}

	if _, err := os.Stat(dest + ".partial"); !os.IsNotExist(err) {
		t.Fatal(".partial should be deleted on ErrUserCancelled")
	}
	if _, err := os.Stat(dest + parallelMetaFileSuffix); !os.IsNotExist(err) {
		t.Fatal(".partial.meta should be deleted on ErrUserCancelled")
	}
}

func TestDownloadFile_IncidentalCancelPreservesPartial(t *testing.T) {
	m := newMockServer(t, 10_000_000)
	m.serveDelay = 50 * time.Millisecond

	dir := t.TempDir()
	dest := filepath.Join(dir, "model.bin")

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel as soon as we see the first progress update, which means the
	// partial file has been created and bytes are flowing.
	var once sync.Once
	progress := func(string, string, string, float64) {
		once.Do(cancel)
	}

	err := URI(m.url()).DownloadFileWithContext(ctx, dest, "", 1, 1, progress)
	if err == nil {
		t.Fatal("expected cancel error")
	}

	if _, err := os.Stat(dest + ".partial"); os.IsNotExist(err) {
		t.Fatal(".partial should be preserved on incidental cancel")
	}
}
