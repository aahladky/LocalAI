// probeRangeSupport is unexported, so its tests live in package downloader.
package downloader

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestProbeRangeSupport(t *testing.T) {
	t.Run("server supports ranges", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Range") == "bytes=0-0" {
				w.Header().Set("Content-Range", "bytes 0-0/12345")
				w.WriteHeader(http.StatusPartialContent)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		result := URI(server.URL).probeRangeSupport()
		if !result.supportsRanges {
			t.Fatal("expected supportsRanges=true")
		}
		if result.totalSize != 12345 {
			t.Fatalf("expected totalSize=12345, got %d", result.totalSize)
		}
	})

	t.Run("server ignores Range header (200 OK)", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Always return 200, ignoring Range header.
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		result := URI(server.URL).probeRangeSupport()
		if result.supportsRanges {
			t.Fatal("expected supportsRanges=false for 200 OK response")
		}
	})

	t.Run("server returns 206 but no Content-Range header", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusPartialContent)
			// Intentionally omit Content-Range.
		}))
		defer server.Close()

		result := URI(server.URL).probeRangeSupport()
		if result.supportsRanges {
			t.Fatal("expected supportsRanges=false when Content-Range is missing")
		}
	})

	t.Run("server returns 206 with malformed Content-Range", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Range", "garbage")
			w.WriteHeader(http.StatusPartialContent)
		}))
		defer server.Close()

		result := URI(server.URL).probeRangeSupport()
		if result.supportsRanges {
			t.Fatal("expected supportsRanges=false for malformed Content-Range")
		}
	})

	t.Run("server returns 206 with zero total size", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Range", "bytes 0-0/0")
			w.WriteHeader(http.StatusPartialContent)
		}))
		defer server.Close()

		result := URI(server.URL).probeRangeSupport()
		if result.supportsRanges {
			t.Fatal("expected supportsRanges=false for zero total size")
		}
	})

	t.Run("server returns 416 Range Not Satisfiable", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		}))
		defer server.Close()

		result := URI(server.URL).probeRangeSupport()
		if result.supportsRanges {
			t.Fatal("expected supportsRanges=false for 416 response")
		}
	})

	t.Run("realistic Content-Range with large file", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Range") == "bytes=0-0" {
				w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", 7516192768))
				w.WriteHeader(http.StatusPartialContent)
				return
			}
			w.WriteHeader(http.StatusOK)
		}))
		defer server.Close()

		result := URI(server.URL).probeRangeSupport()
		if !result.supportsRanges {
			t.Fatal("expected supportsRanges=true")
		}
		if result.totalSize != 7516192768 {
			t.Fatalf("expected totalSize=7516192768, got %d", result.totalSize)
		}
	})
}
