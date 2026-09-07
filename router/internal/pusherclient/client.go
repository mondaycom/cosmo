package pusherclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// Pusher protocol event names. See https://pusher.com/docs/channels/library_auth_reference/pusher-websockets-protocol/
const (
	eventConnectionEstablished = "pusher:connection_established"
	eventError                 = "pusher:error"
	eventPing                  = "pusher:ping"
	eventPong                  = "pusher:pong"
	eventSubscribe             = "pusher:subscribe"
	eventUnsubscribe           = "pusher:unsubscribe"
	eventSubscriptionSucceeded = "pusher_internal:subscription_succeeded"
	eventSubscriptionError     = "pusher_internal:subscription_error"

	protocolVersion = "7"
	clientName      = "cosmo-router-go"
	clientVersion   = "1.0.0"

	defaultActivityTimeout = 120 * time.Second
)

// Options configures a Client.
type Options struct {
	// AppKey is the public Pusher app key.
	AppKey string
	// Cluster is the Pusher cluster, e.g. "mt1". Ignored when WSURL is set.
	Cluster string
	// WSURL overrides the derived WebSocket URL. Used for tests and for
	// self-hosted Pusher-protocol servers.
	WSURL string
	// AuthEndpoint is the absolute URL of the endpoint that signs private and
	// presence channel subscriptions, e.g. https://monday.com/pusher/auth.
	AuthEndpoint string
	// AppSecret makes the client sign private channels itself instead of calling
	// AuthEndpoint. The signature is re-derived per connection, so reconnects keep
	// working. Requires AppKey, which is part of the signature.
	AppSecret string
	// AuthHeaders are sent with every authorization request. A session cookie
	// belongs here, since monday's /pusher/auth requires an authenticated user.
	AuthHeaders map[string]string
	// HTTPClient is used for authorization requests. Defaults to a client with a
	// 10 second timeout.
	HTTPClient *http.Client
	// Decryptor transforms payloads before they reach subscribers. Optional.
	Decryptor Decryptor
	Logger    *zap.Logger
	// HandshakeTimeout bounds the dial and the wait for the connection handshake.
	HandshakeTimeout time.Duration
	// PongTimeout is the grace period added to the server-provided activity
	// timeout before the connection is considered dead.
	PongTimeout time.Duration
	// MinReconnectBackoff and MaxReconnectBackoff bound the reconnect delay.
	MinReconnectBackoff time.Duration
	MaxReconnectBackoff time.Duration
	// EventBufferSize is the per-subscription buffer. Events are dropped when a
	// subscriber does not keep up.
	EventBufferSize int
}

// Event is a single message received on a channel, after decryption.
type Event struct {
	Channel string
	Name    string
	Data    []byte
}

// frame is the Pusher wire format. Data is a JSON-encoded string that itself
// contains JSON, so it is decoded in two steps.
type frame struct {
	Event   string          `json:"event"`
	Channel string          `json:"channel,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Client is a Pusher Channels subscriber. A single WebSocket connection carries
// every channel, and channels are re-subscribed after a reconnect because the
// socket_id — and therefore every auth signature — changes.
type Client struct {
	opts       Options
	httpClient *http.Client
	logger     *zap.Logger

	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	subs    map[string][]*Subscription
	session *session
	closed  bool

	wg sync.WaitGroup
}

// session holds the state that is only valid for one WebSocket connection.
type session struct {
	conn            *websocket.Conn
	socketID        string
	activityTimeout time.Duration
	writeMu         sync.Mutex
}

func (s *session) send(f frame) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.conn.WriteJSON(f)
}

// New validates the options and returns a client that is not yet connected.
func New(opts Options) (*Client, error) {
	if opts.AppKey == "" && opts.WSURL == "" {
		return nil, errors.New("pusher: either an app key or an explicit ws url is required")
	}
	if opts.WSURL == "" && opts.Cluster == "" {
		return nil, errors.New("pusher: a cluster is required when no explicit ws url is given")
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = 10 * time.Second
	}
	if opts.PongTimeout <= 0 {
		opts.PongTimeout = 30 * time.Second
	}
	if opts.MinReconnectBackoff <= 0 {
		opts.MinReconnectBackoff = time.Second
	}
	if opts.MaxReconnectBackoff <= 0 {
		opts.MaxReconnectBackoff = 30 * time.Second
	}
	if opts.EventBufferSize <= 0 {
		opts.EventBufferSize = 256
	}
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	logger := opts.Logger
	if logger == nil {
		logger = zap.NewNop()
	}

	return &Client{
		opts:       opts,
		httpClient: httpClient,
		logger:     logger,
		subs:       map[string][]*Subscription{},
	}, nil
}

// wsURL builds the connection URL the same way pusher-js does.
func (c *Client) wsURL() string {
	if c.opts.WSURL != "" {
		return c.opts.WSURL
	}
	query := url.Values{}
	query.Set("protocol", protocolVersion)
	query.Set("client", clientName)
	query.Set("version", clientVersion)

	return fmt.Sprintf("wss://ws-%s.pusher.com/app/%s?%s", c.opts.Cluster, c.opts.AppKey, query.Encode())
}

// Connect establishes the first connection and returns once the handshake
// completed, so that startup failures surface to the caller. Later connection
// losses are handled by a background supervisor.
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return errors.New("pusher: client is closed")
	}
	if c.ctx != nil {
		c.mu.Unlock()
		return errors.New("pusher: client is already connected")
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	clientCtx := c.ctx
	c.mu.Unlock()

	sess, err := c.dial(ctx)
	if err != nil {
		return err
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.supervise(clientCtx, sess)
	}()

	return nil
}

// Close terminates the connection and stops the supervisor.
func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	cancel := c.cancel
	sess := c.session
	c.session = nil
	c.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if sess != nil {
		c.logger.Info("closing pusher connection", zap.String("socket_id", sess.socketID))
		_ = sess.conn.Close()
	}
	c.wg.Wait()

	return nil
}

// dial opens a connection and waits for pusher:connection_established.
func (c *Client) dial(ctx context.Context) (*session, error) {
	dialCtx, cancel := context.WithTimeout(ctx, c.opts.HandshakeTimeout)
	defer cancel()

	dialer := websocket.Dialer{HandshakeTimeout: c.opts.HandshakeTimeout}
	conn, resp, err := dialer.DialContext(dialCtx, c.wsURL(), nil)
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("pusher: websocket dial failed with status %d: %w", resp.StatusCode, err)
		}
		return nil, fmt.Errorf("pusher: websocket dial failed: %w", err)
	}

	if deadline, ok := dialCtx.Deadline(); ok {
		_ = conn.SetReadDeadline(deadline)
	}

	sess := &session{conn: conn, activityTimeout: defaultActivityTimeout}
	for {
		var f frame
		if err := conn.ReadJSON(&f); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("pusher: reading handshake failed: %w", err)
		}

		switch f.Event {
		case eventConnectionEstablished:
			var payload struct {
				SocketID        string  `json:"socket_id"`
				ActivityTimeout float64 `json:"activity_timeout"`
			}
			if err := unmarshalFrameData(f.Data, &payload); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("pusher: could not parse connection_established: %w", err)
			}
			if payload.SocketID == "" {
				_ = conn.Close()
				return nil, errors.New("pusher: connection_established contained no socket_id")
			}
			sess.socketID = payload.SocketID
			if payload.ActivityTimeout > 0 {
				sess.activityTimeout = time.Duration(payload.ActivityTimeout) * time.Second
			}
			c.logger.Info("pusher connection established",
				zap.String("socket_id", sess.socketID),
				zap.Duration("activity_timeout", sess.activityTimeout),
			)
			return sess, nil
		case eventError:
			protoErr := parseProtocolError(f.Data)
			_ = conn.Close()
			return nil, protoErr
		default:
			// Pusher may send other frames before the handshake completes; ignore them.
		}
	}
}

// supervise serves the given session and reconnects until the client is closed.
func (c *Client) supervise(ctx context.Context, sess *session) {
	backoff := c.opts.MinReconnectBackoff

	for {
		c.mu.Lock()
		c.session = sess
		c.mu.Unlock()

		c.resubscribeAll(ctx, sess)

		err := c.serve(ctx, sess)

		c.mu.Lock()
		if c.session == sess {
			c.session = nil
		}
		c.mu.Unlock()
		_ = sess.conn.Close()

		c.logger.Info("pusher connection dropped",
			zap.String("socket_id", sess.socketID),
			zap.Error(err),
		)

		if ctx.Err() != nil {
			return
		}

		var protoErr *ProtocolError
		if errors.As(err, &protoErr) && !protoErr.ShouldReconnect() {
			c.logger.Error("pusher connection closed permanently, not reconnecting", zap.Error(err))
			return
		}
		c.logger.Warn("pusher connection lost, reconnecting", zap.Error(err), zap.Duration("backoff", backoff))

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}

		newSess, dialErr := c.dial(ctx)
		if dialErr != nil {
			var dialProtoErr *ProtocolError
			if errors.As(dialErr, &dialProtoErr) && !dialProtoErr.ShouldReconnect() {
				c.logger.Error("pusher reconnect rejected permanently", zap.Error(dialErr))
				return
			}
			c.logger.Warn("pusher reconnect failed", zap.Error(dialErr))
			backoff = nextBackoff(backoff, c.opts.MaxReconnectBackoff)
			continue
		}

		backoff = c.opts.MinReconnectBackoff
		sess = newSess
	}
}

// serve reads frames until the connection fails or the context is cancelled.
func (c *Client) serve(ctx context.Context, sess *session) error {
	// Close the connection when the context is cancelled so the blocking read returns.
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = sess.conn.Close()
		case <-done:
		}
	}()

	// The server pings after activity_timeout of silence, so no traffic within
	// that window plus the pong grace period means the connection is dead.
	readTimeout := sess.activityTimeout + c.opts.PongTimeout

	for {
		if err := sess.conn.SetReadDeadline(time.Now().Add(readTimeout)); err != nil {
			return err
		}

		var f frame
		if err := sess.conn.ReadJSON(&f); err != nil {
			return err
		}

		if err := c.handleFrame(sess, f); err != nil {
			return err
		}
	}
}

func (c *Client) handleFrame(sess *session, f frame) error {
	switch f.Event {
	case eventPing:
		return sess.send(frame{Event: eventPong})
	case eventPong:
		return nil
	case eventError:
		protoErr := parseProtocolError(f.Data)
		if !protoErr.ShouldReconnect() {
			return protoErr
		}
		c.logger.Warn("pusher protocol error", zap.Error(protoErr))
		return nil
	case eventSubscriptionSucceeded:
		c.logger.Debug("pusher subscription succeeded", zap.String("channel", f.Channel))
		return nil
	case eventSubscriptionError:
		c.logger.Error("pusher subscription rejected",
			zap.String("channel", f.Channel),
			zap.String("data", string(f.Data)),
		)
		return nil
	default:
		if f.Channel == "" {
			c.logger.Debug("ignoring pusher event without channel", zap.String("event", f.Event))
			return nil
		}
		c.dispatch(f)
		return nil
	}
}

// dispatch decrypts the payload and hands it to every subscriber of the channel.
func (c *Client) dispatch(f frame) {
	payload, err := decodeFrameData(f.Data)
	if err != nil {
		c.logger.Error("could not decode pusher event payload",
			zap.String("channel", f.Channel), zap.String("event", f.Event), zap.Error(err))
		return
	}

	c.logger.Info("pusher event received",
		zap.String("channel", f.Channel), zap.String("event", f.Event),
		zap.String("raw_payload", string(payload)))

	if c.opts.Decryptor != nil {
		decrypted, err := c.opts.Decryptor.Decrypt(payload)
		if err != nil {
			c.logger.Error("could not decrypt pusher event payload",
				zap.String("channel", f.Channel), zap.String("event", f.Event), zap.Error(err))
			return
		}
		// The decryptor passes non-encrypted payloads through unchanged; only report a
		// decrypted payload when decryption actually ran.
		if !bytes.Equal(decrypted, payload) {
			c.logger.Info("pusher event decrypted",
				zap.String("channel", f.Channel), zap.String("event", f.Event),
				zap.String("decrypted_payload", string(decrypted)))
		}
		payload = decrypted
	}

	c.mu.Lock()
	subs := make([]*Subscription, len(c.subs[f.Channel]))
	copy(subs, c.subs[f.Channel])
	c.mu.Unlock()

	evt := Event{Channel: f.Channel, Name: f.Event, Data: payload}
	for _, sub := range subs {
		sub.deliver(evt, c.logger)
	}
}

// resubscribeAll subscribes every registered channel on a fresh session.
func (c *Client) resubscribeAll(ctx context.Context, sess *session) {
	c.mu.Lock()
	channels := make([]string, 0, len(c.subs))
	for channel := range c.subs {
		channels = append(channels, channel)
	}
	c.mu.Unlock()

	for _, channel := range channels {
		if err := c.sendSubscribe(ctx, sess, channel); err != nil {
			c.logger.Error("could not subscribe to pusher channel",
				zap.String("channel", channel), zap.Error(err))
		}
	}
}

// sendSubscribe authorizes the channel if needed and sends pusher:subscribe.
func (c *Client) sendSubscribe(ctx context.Context, sess *session, channel string) error {
	data := map[string]string{"channel": channel}

	if needsAuth(channel) {
		auth, err := c.authorize(ctx, channel, sess.socketID)
		if err != nil {
			return err
		}
		data["auth"] = auth.Auth
		if auth.ChannelData != "" {
			data["channel_data"] = auth.ChannelData
		}
		if auth.SharedKey != "" {
			data["shared_secret"] = auth.SharedKey
		}
	}

	encoded, err := json.Marshal(data)
	if err != nil {
		return err
	}

	return sess.send(frame{Event: eventSubscribe, Data: encoded})
}

func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	return next
}

// unmarshalFrameData decodes the double-encoded data field into target.
func unmarshalFrameData(raw json.RawMessage, target any) error {
	payload, err := decodeFrameData(raw)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}
	return json.Unmarshal(payload, target)
}

// decodeFrameData unwraps the data field. Pusher sends it as a JSON string
// containing JSON, but some servers send the object directly.
func decodeFrameData(raw json.RawMessage) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if raw[0] != '"' {
		return raw, nil
	}
	var asString string
	if err := json.Unmarshal(raw, &asString); err != nil {
		return nil, err
	}
	return []byte(asString), nil
}
