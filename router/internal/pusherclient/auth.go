package pusherclient

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"go.uber.org/zap"
)

// authResponse is the body monday's POST /pusher/auth returns on success.
type authResponse struct {
	Auth        string `json:"auth"`
	ChannelData string `json:"channel_data"`
	SharedKey   string `json:"shared_secret"`
}

// AuthError describes a failed channel authorization.
type AuthError struct {
	Channel    string
	StatusCode int
	Body       string
}

func (e *AuthError) Error() string {
	return fmt.Sprintf("pusher: authorization for channel %q failed with status %d: %s", e.Channel, e.StatusCode, e.Body)
}

// signChannel produces the subscription signature Pusher expects for a private
// channel: "<app_key>:<hex HMAC-SHA256 of "<socket_id>:<channel_name>" under the
// app secret>". This is the same computation every server-side Pusher SDK performs
// in its auth endpoint, so it is re-derived on every reconnect with the new
// socket_id.
//
// A presence channel would additionally need channel_data folded into the signed
// string; monday's channels are private, so that is not implemented.
func signChannel(appKey, appSecret, socketID, channel string) string {
	mac := hmac.New(sha256.New, []byte(appSecret))
	mac.Write([]byte(socketID + ":" + channel))
	return appKey + ":" + hex.EncodeToString(mac.Sum(nil))
}

// needsAuth reports whether a channel has to be authorized before subscribing.
// monday's encrypted channels use the "private-enc_" prefix, which is still a
// private channel as far as Pusher is concerned.
func needsAuth(channel string) bool {
	return strings.HasPrefix(channel, "private-") || strings.HasPrefix(channel, "presence-")
}

// authorize performs a single-channel POST to the configured auth endpoint. The
// monday web client batches these requests; we deliberately keep one request per
// channel here, which is the plain Pusher contract.
func (c *Client) authorize(ctx context.Context, channel, socketID string) (*authResponse, error) {
	if c.opts.AppSecret != "" {
		auth := signChannel(c.opts.AppKey, c.opts.AppSecret, socketID, channel)
		c.logger.Debug("signed pusher channel locally",
			zap.String("channel", channel),
			zap.String("socket_id", socketID),
		)
		return &authResponse{Auth: auth}, nil
	}
	if c.opts.AuthEndpoint == "" {
		return nil, fmt.Errorf("pusher: channel %q requires authorization but neither an auth endpoint nor an app secret is configured", channel)
	}

	form := url.Values{}
	form.Set("socket_id", socketID)
	form.Set("channel_name", channel)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.opts.AuthEndpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	for name, value := range c.opts.AuthHeaders {
		req.Header.Set(name, value)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, &AuthError{Channel: channel, StatusCode: resp.StatusCode, Body: truncate(string(body), 512)}
	}

	var parsed authResponse
	if err := json.Unmarshal(body, &parsed); err != nil {
		// Same failure mode as the key endpoint: a 200 with HTML means the monolith
		// rendered the login page because the session cookie was missing or expired.
		c.logger.Error("auth response is not JSON",
			zap.String("channel", channel),
			zap.String("endpoint", c.opts.AuthEndpoint),
			zap.Int("status", resp.StatusCode),
			zap.String("content_type", resp.Header.Get("Content-Type")),
			zap.Strings("request_headers_sent", headerNames(c.opts.AuthHeaders)),
			zap.String("body", truncate(string(body), 2048)),
			zap.Error(err),
		)
		return nil, fmt.Errorf("pusher: could not parse auth response for channel %q (status %d, content-type %q, body %s): %w",
			channel, resp.StatusCode, resp.Header.Get("Content-Type"), truncate(string(body), 512), err)
	}
	if parsed.Auth == "" {
		return nil, fmt.Errorf("pusher: auth response for channel %q contained no auth signature", channel)
	}

	return &parsed, nil
}
