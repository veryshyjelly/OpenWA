package openwa

import (
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"sync"
)

// ErrMediaTooLarge means a declared or streamed response exceeds the caller's byte cap.
// Discard any partial bytes when a read returns this error.
var ErrMediaTooLarge = errors.New("openwa: media exceeds the requested byte cap")

// MessageMediaDownload is a bounded source-media stream. The caller must close Body.
// Filename is untrusted metadata, not a safe filesystem path. Size is -1 when unknown.
type MessageMediaDownload struct {
	Body        io.ReadCloser
	ContentType string
	Mimetype    string
	Filename    string
	Size        int64
}

// DownloadMedia explicitly reads one message's media from WhatsApp without marking it read,
// archiving bytes or importing history. Use only after a documented stored-media cache miss.
// The maintained gateway's /media/download route supports whatsapp-web.js; other engines return 501.
// maxBytes must be positive and can only tighten the gateway cap. It also bounds client reads,
// including chunked responses that omit Content-Length. The configured timeout covers Body reads.
func (s *MessagesService) DownloadMedia(ctx context.Context, sessionID, chatID, messageID string, maxBytes int64) (*MessageMediaDownload, error) {
	if ctx == nil {
		return nil, errors.New("openwa: context is required")
	}
	// The gateway accepts safe JavaScript integers, even on 64-bit Go hosts.
	if maxBytes < 1 || maxBytes > 1<<53-1 {
		return nil, errors.New("openwa: maxBytes must be a positive safe integer")
	}
	path := "/api/sessions/" + pathEscape(sessionID) + "/messages/" + pathEscape(chatID) + "/" + pathEscape(messageID) + "/media/download"
	if err := checkPathSegments(path, false); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.client.baseURL+path, nil)
	if err != nil {
		return nil, fmt.Errorf("openwa: building media request: %w", err)
	}
	query := url.Values{"maxBytes": []string{strconv.FormatInt(maxBytes, 10)}}
	req.URL.RawQuery = query.Encode()
	resp, err := s.client.httpClient.Do(req)
	if err != nil {
		if isTimeout(err) {
			return nil, s.client.timeoutErr(ctx, err)
		}
		return nil, fmt.Errorf("openwa: downloading media: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		// Error envelopes are independent of the media cap, but are themselves bounded.
		data, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
		if err != nil {
			if isTimeout(err) {
				return nil, s.client.timeoutErr(ctx, err)
			}
			return nil, fmt.Errorf("openwa: reading media error response: %w", err)
		}
		return nil, parseAPIError(resp.StatusCode, data, http.MethodGet+" "+path, resp.Header)
	}
	if resp.ContentLength > maxBytes {
		resp.Body.Close()
		return nil, ErrMediaTooLarge
	}
	_, disposition, _ := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
	mimetype := resp.Header.Get("X-Media-Mimetype")
	if mimetype == "" {
		mimetype = resp.Header.Get("Content-Type")
	}
	return &MessageMediaDownload{
		Body:        &boundedMediaBody{source: resp.Body, remaining: maxBytes, client: s.client, ctx: ctx},
		ContentType: resp.Header.Get("Content-Type"), Mimetype: mimetype,
		Filename: disposition["filename"], Size: resp.ContentLength,
	}, nil
}

type boundedMediaBody struct {
	source    io.ReadCloser
	remaining int64
	client    *Client
	ctx       context.Context
	once      sync.Once
	closeErr  error
}

func (b *boundedMediaBody) Close() error {
	b.once.Do(func() { b.closeErr = b.source.Close() })
	return b.closeErr
}

func (b *boundedMediaBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining == 0 {
		var probe [1]byte
		n, err := b.source.Read(probe[:])
		if n > 0 {
			b.Close()
			return 0, ErrMediaTooLarge
		}
		return 0, b.readError(err)
	}
	if int64(len(p)) > b.remaining {
		p = p[:int(b.remaining)]
	}
	n, err := b.source.Read(p)
	b.remaining -= int64(n)
	return n, b.readError(err)
}

func (b *boundedMediaBody) readError(err error) error {
	if err == nil {
		return nil
	}
	b.Close()
	if err == io.EOF {
		return err
	}
	if isTimeout(err) {
		return b.client.timeoutErr(b.ctx, err)
	}
	return fmt.Errorf("openwa: reading media response: %w", err)
}
