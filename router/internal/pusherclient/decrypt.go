package pusherclient

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Decryptor transforms a raw event payload before it is handed to a subscriber.
// Payloads that are not encrypted must be returned unchanged.
type Decryptor interface {
	Decrypt(payload []byte) ([]byte, error)
}

// encryptedEnvelope is the payload shape monday.com publishes to its
// "private-enc_" channels. It is not Pusher's native end-to-end encryption.
type encryptedEnvelope struct {
	EncryptedBase64 string `json:"encrypted_base64"`
	IV              string `json:"iv"`
	EncDate         string `json:"enc_date"`
}

// MondayDecryptorOptions configures a MondayDecryptor.
type MondayDecryptorOptions struct {
	// KeysEndpoint is the absolute URL of monday's key endpoint, which answers with
	// {"pusher_enc_keys": {"YYYY-MM-DD": "<key>"}}.
	KeysEndpoint string
	// StaticKey is used for every payload regardless of its enc_date. It replaces
	// KeysEndpoint: when it is set no key request is made at all. Useful for local
	// development, where the key endpoint needs a monolith session.
	StaticKey string
	// Headers are sent with every key request. A session cookie belongs here.
	Headers map[string]string
	// HTTPClient is optional and defaults to a client with a 10 second timeout.
	HTTPClient *http.Client
	// RefreshInterval is how often the key set is refetched. Defaults to one hour,
	// matching the monday web client.
	RefreshInterval time.Duration
	Logger          *zap.Logger
}

// MondayDecryptor decrypts monday.com's encrypted channel payloads. The keys
// rotate daily and are addressed by the enc_date carried in each payload.
type MondayDecryptor struct {
	opts   MondayDecryptorOptions
	client *http.Client
	logger *zap.Logger

	mu          sync.RWMutex
	keys        map[string]string
	lastFetched time.Time
}

var _ Decryptor = (*MondayDecryptor)(nil)

// minKeyRefetchInterval throttles the on-miss refetch so a stream of payloads
// referencing an unknown date cannot turn into a request flood.
const minKeyRefetchInterval = 30 * time.Second

func NewMondayDecryptor(opts MondayDecryptorOptions) (*MondayDecryptor, error) {
	if opts.StaticKey == "" && opts.KeysEndpoint == "" {
		return nil, errors.New("pusher: either a static encryption key or a keys endpoint is required")
	}
	if opts.StaticKey != "" && opts.KeysEndpoint != "" {
		return nil, errors.New("pusher: a static encryption key and a keys endpoint are mutually exclusive")
	}
	if opts.RefreshInterval <= 0 {
		opts.RefreshInterval = time.Hour
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	return &MondayDecryptor{
		opts:   opts,
		client: client,
		logger: logger,
		keys:   map[string]string{},
	}, nil
}

// Start fetches the key set once and then refreshes it until ctx is done. With a
// static key it does nothing: there is no key set to fetch or rotate.
func (d *MondayDecryptor) Start(ctx context.Context) error {
	if d.opts.StaticKey != "" {
		return nil
	}
	if err := d.fetchKeys(ctx); err != nil {
		return err
	}

	go func() {
		ticker := time.NewTicker(d.opts.RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := d.fetchKeys(ctx); err != nil {
					d.logger.Error("failed to refresh pusher encryption keys", zap.Error(err))
				}
			}
		}
	}()

	return nil
}

func (d *MondayDecryptor) Decrypt(payload []byte) ([]byte, error) {
	var envelope encryptedEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		// Not a JSON object, so it cannot be an encrypted envelope.
		return payload, nil
	}
	if envelope.EncryptedBase64 == "" {
		return payload, nil
	}

	key, err := d.keyForDate(envelope.EncDate)
	if err != nil {
		return nil, err
	}

	return decryptAESCBC([]byte(key), []byte(envelope.IV), envelope.EncryptedBase64)
}

func (d *MondayDecryptor) keyForDate(date string) (string, error) {
	if d.opts.StaticKey != "" {
		return d.opts.StaticKey, nil
	}

	d.mu.RLock()
	key, ok := d.keys[date]
	staleEnough := time.Since(d.lastFetched) > minKeyRefetchInterval
	d.mu.RUnlock()
	if ok {
		return key, nil
	}
	if !staleEnough {
		return "", fmt.Errorf("pusher: no encryption key for date %q", date)
	}

	// The key set rotates daily, so an unknown date most likely means our cache is
	// behind. Refetch once before giving up.
	if err := d.fetchKeys(context.Background()); err != nil {
		return "", fmt.Errorf("pusher: no encryption key for date %q and refresh failed: %w", date, err)
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	key, ok = d.keys[date]
	if !ok {
		return "", fmt.Errorf("pusher: no encryption key for date %q", date)
	}
	return key, nil
}

func (d *MondayDecryptor) fetchKeys(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, d.opts.KeysEndpoint, nil)
	if err != nil {
		return err
	}
	for name, value := range d.opts.Headers {
		req.Header.Set(name, value)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("pusher: encryption keys endpoint returned %d: %s", resp.StatusCode, truncate(string(body), 256))
	}

	var parsed struct {
		Keys map[string]string `json:"pusher_enc_keys"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		// A 200 with a non-JSON body is almost always the monolith login page: the
		// endpoint runs authenticate_user!, so a missing or expired session cookie
		// produces HTML instead of keys. Log the response so that is visible.
		d.logger.Error("encryption keys response is not JSON",
			zap.String("endpoint", d.opts.KeysEndpoint),
			zap.Int("status", resp.StatusCode),
			zap.String("content_type", resp.Header.Get("Content-Type")),
			zap.Strings("request_headers_sent", headerNames(d.opts.Headers)),
			zap.String("body", truncate(string(body), 2048)),
			zap.Error(err),
		)
		return fmt.Errorf("pusher: could not parse encryption keys response (status %d, content-type %q, body %s): %w",
			resp.StatusCode, resp.Header.Get("Content-Type"), truncate(string(body), 512), err)
	}
	if len(parsed.Keys) == 0 {
		return errors.New("pusher: encryption keys response contained no keys")
	}

	d.mu.Lock()
	d.keys = parsed.Keys
	d.lastFetched = time.Now()
	d.mu.Unlock()

	return nil
}

// decryptAESCBC mirrors the monday web client, which calls
// CryptoJS.AES.decrypt(ciphertext, CryptoJS.enc.Utf8.parse(key), {iv: CryptoJS.enc.Utf8.parse(iv), mode: CBC}).
// Passing a WordArray as the key makes CryptoJS use it verbatim, so there is no
// EVP key derivation and no "Salted__" header: key and IV are the raw UTF-8
// bytes of their strings.
func decryptAESCBC(key, iv []byte, ciphertextBase64 string) ([]byte, error) {
	switch len(key) {
	case 16, 24, 32:
	default:
		return nil, fmt.Errorf("pusher: encryption key must be 16, 24 or 32 bytes, got %d", len(key))
	}
	if len(iv) != aes.BlockSize {
		return nil, fmt.Errorf("pusher: iv must be %d bytes, got %d", aes.BlockSize, len(iv))
	}

	ciphertext, err := base64.StdEncoding.DecodeString(ciphertextBase64)
	if err != nil {
		return nil, fmt.Errorf("pusher: could not base64 decode payload: %w", err)
	}
	if len(ciphertext) == 0 || len(ciphertext)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("pusher: ciphertext length %d is not a multiple of the block size", len(ciphertext))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	plaintext := make([]byte, len(ciphertext))
	cipher.NewCBCDecrypter(block, iv).CryptBlocks(plaintext, ciphertext)

	return removePKCS7Padding(plaintext)
}

func removePKCS7Padding(data []byte) ([]byte, error) {
	padding := int(data[len(data)-1])
	if padding == 0 || padding > aes.BlockSize || padding > len(data) {
		return nil, fmt.Errorf("pusher: invalid padding length %d", padding)
	}
	for _, b := range data[len(data)-padding:] {
		if int(b) != padding {
			return nil, errors.New("pusher: invalid padding bytes")
		}
	}
	return data[:len(data)-padding], nil
}

// headerNames lists the header names of a request, without their values: the
// values carry a session credential and must not reach a log.
func headerNames(headers map[string]string) []string {
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
