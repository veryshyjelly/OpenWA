package openwa

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestDownloadMediaPreservesMetadataAndUsesExplicitBoundedRoute(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" || r.URL.Path != "/api/sessions/s/messages/chat@g.us/message/media/download" || r.URL.Query().Get("maxBytes") != "64" {
			t.Error("incorrect bounded source route")
		}
		if r.Header.Get("X-API-Key") != "fixture-key" || r.Header.Get("Idempotency-Key") != "" {
			t.Error("authentication missing or read incorrectly carries a send key")
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Media-Mimetype", "application/pdf")
		w.Header().Set("Content-Disposition", "attachment; filename*=UTF-8''r%C3%A9sum%C3%A9.pdf")
		fmt.Fprint(w, "bytes")
	}))
	defer server.Close()
	client, _ := New(server.URL, "fixture-key")
	ctx := WithIdempotencyKey(context.Background(), "send-only-key")
	media, err := client.Messages.DownloadMedia(ctx, "s", "chat@g.us", "message", 64)
	if err != nil {
		t.Fatal(err)
	}
	defer media.Body.Close()
	data, err := io.ReadAll(media.Body)
	if err != nil || string(data) != "bytes" || media.Filename != "résumé.pdf" || media.Mimetype != "application/pdf" || media.ContentType != "application/octet-stream" {
		t.Fatalf("media metadata/read: %+v %v", media, err)
	}
	if calls.Load() != 1 {
		t.Fatal("download triggered extra history/read/archive calls")
	}
}

func TestDownloadMediaEnforcesDeclaredAndChunkedLimits(t *testing.T) {
	for _, declared := range []bool{true, false} {
		t.Run(fmt.Sprint(declared), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if declared {
					w.Header().Set("Content-Length", "8")
				} else {
					w.(http.Flusher).Flush()
				}
				fmt.Fprint(w, "12345678")
			}))
			defer server.Close()
			client, _ := New(server.URL, "fixture-key")
			media, err := client.Messages.DownloadMedia(context.Background(), "s", "c", "m", 4)
			if declared {
				if !errors.Is(err, ErrMediaTooLarge) {
					t.Fatalf("declared oversize: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			defer media.Body.Close()
			data, err := io.ReadAll(media.Body)
			if !errors.Is(err, ErrMediaTooLarge) || len(data) > 4 {
				t.Fatalf("stream cap: length=%d error=%v", len(data), err)
			}
		})
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.(http.Flusher).Flush(); fmt.Fprint(w, "1234") }))
	defer server.Close()
	client, _ := New(server.URL, "fixture-key")
	media, err := client.Messages.DownloadMedia(context.Background(), "s", "c", "m", 4)
	if err != nil {
		t.Fatal(err)
	}
	defer media.Body.Close()
	if data, err := io.ReadAll(media.Body); err != nil || string(data) != "1234" {
		t.Fatalf("exact limit: %q %v", data, err)
	}
}

func TestDownloadMediaPreservesTypedGatewayErrorsRegardlessOfMediaCap(t *testing.T) {
	for _, status := range []int{404, 409, 413, 429, 501, 503} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"code":"MEDIA_SOURCE_UNAVAILABLE","message":"source unavailable"}`)
			}))
			defer server.Close()
			client, _ := New(server.URL, "fixture-key")
			_, err := client.Messages.DownloadMedia(context.Background(), "s", "c", "m", 1)
			var api *APIError
			if !errors.As(err, &api) || api.StatusCode != status || api.Code != "MEDIA_SOURCE_UNAVAILABLE" || api.RetryAfter != time.Second {
				t.Fatalf("typed source error: %v", err)
			}
		})
	}
}

func TestDownloadMediaCancellationAndTimeoutCoverBodyReads(t *testing.T) {
	for _, timeout := range []bool{false, true} {
		t.Run(fmt.Sprint(timeout), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.(http.Flusher).Flush(); <-r.Context().Done() }))
			defer server.Close()
			options := []Option{}
			if timeout {
				options = append(options, WithTimeout(30*time.Millisecond))
			}
			client, _ := New(server.URL, "fixture-key", options...)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			media, err := client.Messages.DownloadMedia(ctx, "s", "c", "m", 16)
			if err != nil {
				t.Fatal(err)
			}
			defer media.Body.Close()
			if !timeout {
				cancel()
			}
			_, err = io.ReadAll(media.Body)
			if timeout {
				var expired *TimeoutError
				if !errors.As(err, &expired) {
					t.Fatalf("body timeout: %v", err)
				}
			} else if !errors.Is(err, context.Canceled) {
				t.Fatalf("body cancellation: %v", err)
			}
		})
	}
}

func TestDownloadMediaCloseReleasesSourceAndInvalidArgumentsNeverConnect(t *testing.T) {
	closed := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(closed)
	}))
	defer server.Close()
	client, _ := New(server.URL, "fixture-key")
	for _, cap := range []int64{0, -1, 1 << 53} {
		if _, err := client.Messages.DownloadMedia(context.Background(), "s", "c", "m", cap); err == nil {
			t.Fatal("invalid cap accepted")
		}
	}
	if _, err := client.Messages.DownloadMedia(nil, "s", "c", "m", 1); err == nil {
		t.Fatal("nil context accepted")
	}
	if _, err := client.Messages.DownloadMedia(context.Background(), "s", "..", "m", 1); err == nil {
		t.Fatal("invalid path accepted")
	}
	media, err := client.Messages.DownloadMedia(context.Background(), "s", "c", "m", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := media.Body.Close(); err != nil {
		t.Fatal(err)
	}
	if err := media.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("closing stream did not cancel source")
	}
}

func TestDownloadMediaDoesNotFollowCredentialRedirects(t *testing.T) {
	var escaped atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { escaped.Add(1) }))
	defer target.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, 307) }))
	defer server.Close()
	client, _ := New(server.URL, "fixture-key")
	_, err := client.Messages.DownloadMedia(context.Background(), "s", "c", "m", 16)
	var api *APIError
	if !errors.As(err, &api) || api.StatusCode != 307 || escaped.Load() != 0 {
		t.Fatal("credential redirect followed")
	}
}
