// Tests for concurrency configuration. Internal package access.
package downloader

import (
	"os"
	"testing"
	"time"
)

func TestResolveConcurrency(t *testing.T) {
	t.Run("explicit option takes priority", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_CONCURRENCY", "4")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_CONCURRENCY")
		if got := resolveConcurrency(16); got != 16 {
			t.Fatalf("expected 16, got %d", got)
		}
	})

	t.Run("env var used when option is 0", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_CONCURRENCY", "4")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_CONCURRENCY")
		if got := resolveConcurrency(0); got != 4 {
			t.Fatalf("expected 4, got %d", got)
		}
	})

	t.Run("default used when option is 0 and no env var", func(t *testing.T) {
		os.Unsetenv("LOCALAI_DOWNLOAD_CONCURRENCY")
		if got := resolveConcurrency(0); got != defaultParallelConcurrency {
			t.Fatalf("expected %d, got %d", defaultParallelConcurrency, got)
		}
	})

	t.Run("invalid env var falls back to default", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_CONCURRENCY", "notanumber")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_CONCURRENCY")
		if got := resolveConcurrency(0); got != defaultParallelConcurrency {
			t.Fatalf("expected %d, got %d", defaultParallelConcurrency, got)
		}
	})

	t.Run("env var < 1 falls back to default", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_CONCURRENCY", "0")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_CONCURRENCY")
		if got := resolveConcurrency(0); got != defaultParallelConcurrency {
			t.Fatalf("expected %d, got %d", defaultParallelConcurrency, got)
		}
	})

	t.Run("option value of 1 forces single-stream", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_CONCURRENCY", "8")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_CONCURRENCY")
		if got := resolveConcurrency(1); got != 1 {
			t.Fatalf("expected 1, got %d", got)
		}
	})
}

func TestResolveMaxRetries(t *testing.T) {
	t.Run("explicit option takes priority", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_RETRIES", "5")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_RETRIES")
		if got := resolveMaxRetries(10); got != 10 {
			t.Fatalf("expected 10, got %d", got)
		}
	})

	t.Run("env var used when option is unset", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_RETRIES", "5")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_RETRIES")
		if got := resolveMaxRetries(-1); got != 5 {
			t.Fatalf("expected 5, got %d", got)
		}
	})

	t.Run("default when option is unset and no env", func(t *testing.T) {
		os.Unsetenv("LOCALAI_DOWNLOAD_RETRIES")
		if got := resolveMaxRetries(-1); got != defaultMaxRetries {
			t.Fatalf("expected %d, got %d", defaultMaxRetries, got)
		}
	})

	t.Run("option 0 disables retry", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_RETRIES", "5")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_RETRIES")
		if got := resolveMaxRetries(0); got != 0 {
			t.Fatalf("expected 0 (no retry), got %d", got)
		}
	})

	t.Run("invalid env falls back to default when option unset", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_RETRIES", "abc")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_RETRIES")
		if got := resolveMaxRetries(-1); got != defaultMaxRetries {
			t.Fatalf("expected %d, got %d", defaultMaxRetries, got)
		}
	})
}

func TestResolveRetryBackoff(t *testing.T) {
	t.Run("explicit option takes priority", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF", "1s")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF")
		if got := resolveRetryBackoff(2 * time.Second); got != 2*time.Second {
			t.Fatalf("expected 2s, got %v", got)
		}
	})

	t.Run("env var used when option is 0", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF", "1s")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF")
		if got := resolveRetryBackoff(0); got != time.Second {
			t.Fatalf("expected 1s, got %v", got)
		}
	})

	t.Run("default when option is 0 and no env", func(t *testing.T) {
		os.Unsetenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF")
		if got := resolveRetryBackoff(0); got != defaultRetryBackoff {
			t.Fatalf("expected %v, got %v", defaultRetryBackoff, got)
		}
	})

	t.Run("invalid env falls back to default", func(t *testing.T) {
		os.Setenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF", "notaduration")
		defer os.Unsetenv("LOCALAI_DOWNLOAD_RETRY_BACKOFF")
		if got := resolveRetryBackoff(0); got != defaultRetryBackoff {
			t.Fatalf("expected %v, got %v", defaultRetryBackoff, got)
		}
	})
}
