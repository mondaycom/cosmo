package pusher

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/wundergraph/cosmo/router/internal/pusherclient"
	"github.com/wundergraph/cosmo/router/pkg/config"
	"github.com/wundergraph/cosmo/router/pkg/metric"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"go.uber.org/zap"
)

const pusherReceive = "receive"

// startupTimeout bounds a connection attempt so a black-holed broker cannot block
// the first subscription for longer than the caller's own timeout.
const startupTimeout = 3 * time.Second

var _ datasource.Adapter = (*ProviderAdapter)(nil)

// pooledClient is one Pusher WebSocket connection plus the credential it was
// authorized with. Channel subscriptions on a Pusher connection are authorized once
// against a single socket_id, so subscribers with different credentials cannot share
// a connection: one connection exists per distinct credential.
type pooledClient struct {
	client *pusherclient.Client
	// refs counts the live channel subscriptions using this connection. The
	// connection is closed when it drops to zero.
	refs int
}

// ProviderAdapter subscribes to Pusher channels. It owns a pool of WebSocket
// connections keyed by the forwarded credential.
type ProviderAdapter struct {
	ctx               context.Context
	cancel            context.CancelFunc
	logger            *zap.Logger
	source            config.PusherEventSource
	streamMetricStore metric.StreamMetricStore
	// skipUnavailable mirrors events.skip_unavailable_providers. When true, a failed
	// initial connection does not fail the subscription; the client reconnects in the
	// background and the affected fields recover without a restart.
	skipUnavailable bool

	// entityMapper rewrites payloads into entity representations. Nil when the provider
	// configures no mapping, which forwards payloads unchanged.
	entityMapper *entityMapper

	mu      sync.Mutex
	clients map[string]*pooledClient
	closed  bool

	closeWg sync.WaitGroup
}

func NewProviderAdapter(ctx context.Context, logger *zap.Logger, source config.PusherEventSource, opts datasource.ProviderOpts) (datasource.Adapter, error) {
	mapper, err := newEntityMapper(source.EntityMappings)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)
	if logger == nil {
		logger = zap.NewNop()
	}

	store := opts.StreamMetricStore
	if store == nil {
		store = metric.NewNoopStreamMetricStore()
	}

	return &ProviderAdapter{
		ctx:               ctx,
		cancel:            cancel,
		logger:            logger,
		source:            source,
		streamMetricStore: store,
		skipUnavailable:   opts.SkipUnavailableProviders,
		entityMapper:      mapper,
		clients:           make(map[string]*pooledClient),
	}, nil
}

// Startup does not connect: the credential used to authorize channels arrives with
// the subscription request, so connections are created on first use instead.
func (p *ProviderAdapter) Startup(ctx context.Context) error {
	return nil
}

func (p *ProviderAdapter) Shutdown(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	clients := make([]*pooledClient, 0, len(p.clients))
	for key, pooled := range p.clients {
		clients = append(clients, pooled)
		delete(p.clients, key)
	}
	p.mu.Unlock()

	// Cancel the context to stop the subscriptions
	p.cancel()

	// Wait for the subscriptions to be closed
	p.closeWg.Wait()

	var firstErr error
	for _, pooled := range clients {
		if err := pooled.client.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// authHeaders merges the static headers with the headers forwarded from the incoming
// request, and returns a key identifying the resulting credential. Forwarded headers
// win over static ones.
func (p *ProviderAdapter) authHeaders(ctx context.Context) (headers map[string]string, credentialKey string) {
	headers = make(map[string]string, len(p.source.AuthHeaders)+len(p.source.AuthHeadersFromRequest))
	for name, value := range p.source.AuthHeaders {
		headers[name] = value
	}

	requestHeader := datasource.RequestHeaderFromContext(ctx)
	forwarded := make([]string, 0, len(p.source.AuthHeadersFromRequest))
	for _, name := range p.source.AuthHeadersFromRequest {
		value := requestHeader.Get(name)
		if value == "" {
			continue
		}
		headers[http.CanonicalHeaderKey(name)] = value
		forwarded = append(forwarded, http.CanonicalHeaderKey(name)+": "+value)
	}

	if len(forwarded) == 0 {
		// No per-request credential: every subscriber shares the static-header client.
		return headers, ""
	}

	// The key is hashed so the credential itself never reaches a map key that could be
	// logged or reported.
	sort.Strings(forwarded)
	sum := sha256.Sum256([]byte(strings.Join(forwarded, "\n")))
	return headers, hex.EncodeToString(sum[:])
}

// acquireClient returns the connection for the given credential, creating and
// connecting it on first use. The caller must call releaseClient once per successful
// call.
func (p *ProviderAdapter) acquireClient(ctx context.Context, credentialKey string, headers map[string]string) (*pusherclient.Client, error) {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, datasource.NewError("pusher provider is shut down", nil)
	}
	if pooled, ok := p.clients[credentialKey]; ok {
		pooled.refs++
		p.mu.Unlock()
		return pooled.client, nil
	}
	p.mu.Unlock()

	client, err := p.newClient(ctx, headers)
	if err != nil {
		return nil, err
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		_ = client.Close()
		return nil, datasource.NewError("pusher provider is shut down", nil)
	}
	// Another subscription may have created a connection for the same credential
	// concurrently; keep the one already in the pool and discard ours.
	if pooled, ok := p.clients[credentialKey]; ok {
		pooled.refs++
		p.mu.Unlock()
		_ = client.Close()
		return pooled.client, nil
	}
	p.clients[credentialKey] = &pooledClient{client: client, refs: 1}
	p.mu.Unlock()

	return client, nil
}

func (p *ProviderAdapter) releaseClient(credentialKey string) {
	p.mu.Lock()
	pooled, ok := p.clients[credentialKey]
	if !ok {
		p.mu.Unlock()
		return
	}
	pooled.refs--
	if pooled.refs > 0 {
		p.mu.Unlock()
		return
	}
	delete(p.clients, credentialKey)
	p.mu.Unlock()

	if err := pooled.client.Close(); err != nil {
		p.logger.Debug("closing idle pusher connection", zap.String("provider_id", p.source.ID), zap.Error(err))
	}
}

// newClient builds and connects one Pusher connection for the given credential. The
// credential is retained by the client for its whole lifetime: a reconnect gets a new
// socket_id, so every channel must be re-authorized.
func (p *ProviderAdapter) newClient(ctx context.Context, headers map[string]string) (*pusherclient.Client, error) {
	logger := p.logger.With(zap.String("provider_id", p.source.ID))

	var decryptor pusherclient.Decryptor
	if p.source.Encryption.Enabled {
		mondayDecryptor, err := pusherclient.NewMondayDecryptor(pusherclient.MondayDecryptorOptions{
			StaticKey:       p.source.Encryption.EncryptionKey,
			KeysEndpoint:    p.source.Encryption.KeysEndpoint,
			Headers:         headers,
			RefreshInterval: p.source.Encryption.RefreshInterval,
			Logger:          logger,
		})
		if err != nil {
			return nil, err
		}
		// The key set is fetched here so a misconfigured endpoint fails on subscribe
		// instead of silently emitting ciphertext on the first event.
		if err := mondayDecryptor.Start(p.ctx); err != nil {
			if !p.skipUnavailable {
				return nil, err
			}
			logger.Error("could not load pusher encryption keys, events will not be decrypted until the keys become available",
				zap.Error(err))
		}
		decryptor = mondayDecryptor
	}

	client, err := pusherclient.New(pusherclient.Options{
		AppKey:       p.source.AppKey,
		Cluster:      p.source.Cluster,
		WSURL:        p.source.WSURL,
		AuthEndpoint: p.source.AuthEndpoint,
		AppSecret:    p.source.AppSecret,
		AuthHeaders:  headers,
		Decryptor:    decryptor,
		Logger:       logger,
	})
	if err != nil {
		return nil, err
	}

	connectCtx, cancel := context.WithTimeout(ctx, startupTimeout)
	defer cancel()

	if err := client.Connect(connectCtx); err != nil {
		if !p.skipUnavailable {
			_ = client.Close()
			return nil, err
		}
		// Lenient mode: keep the client. It reconnects in the background and
		// subscribes the registered channels once the connection is up.
		logger.Error("could not connect to pusher, retrying in the background", zap.Error(err))
	}

	return client, nil
}

func (p *ProviderAdapter) Subscribe(ctx context.Context, conf datasource.SubscriptionEventConfiguration, updater datasource.SubscriptionEventUpdater) error {
	subConf, ok := conf.(*SubscriptionEventConfiguration)
	if !ok {
		return datasource.NewError("subscription event not supported by pusher provider", nil)
	}

	log := p.logger.With(
		zap.String("provider_id", conf.ProviderID()),
		zap.String("method", "subscribe"),
		zap.Strings("channels", subConf.Channels),
	)

	headers, credentialKey := p.authHeaders(ctx)

	log.Debug("subscribing")

	client, err := p.acquireClient(ctx, credentialKey, headers)
	if err != nil {
		return datasource.NewError("failed to connect to pusher", err)
	}

	subscriptions := make([]*pusherclient.Subscription, 0, len(subConf.Channels))
	for _, channel := range subConf.Channels {
		subscription, err := client.Subscribe(ctx, channel)
		if err != nil {
			// Undo the subscriptions we already created so a partial failure does not
			// leak channels on the shared connection.
			for _, created := range subscriptions {
				created.Unsubscribe()
			}
			p.releaseClient(credentialKey)
			return datasource.NewError("failed to subscribe to pusher channel "+channel, err)
		}
		subscriptions = append(subscriptions, subscription)
	}

	if len(subscriptions) == 0 {
		p.releaseClient(credentialKey)
		return nil
	}

	// The pool reference taken by acquireClient is held until every channel goroutine
	// of this subscription has stopped.
	var channelsWg sync.WaitGroup
	p.closeWg.Add(1)
	go func() {
		defer p.closeWg.Done()
		channelsWg.Wait()
		p.releaseClient(credentialKey)
	}()

	for _, subscription := range subscriptions {
		p.closeWg.Add(1)
		channelsWg.Add(1)

		go func(subscription *pusherclient.Subscription) {
			defer p.closeWg.Done()
			defer channelsWg.Done()
			defer subscription.Unsubscribe()

			events := subscription.Events()

			for {
				select {
				case evt, ok := <-events:
					if !ok {
						log.Debug("subscription closed, stopping", zap.String("message_channel", subscription.Channel()))
						return
					}
					log.Debug("subscription update",
						zap.String("message_channel", evt.Channel),
						zap.String("event", evt.Name),
					)
					p.streamMetricStore.Consume(ctx, metric.StreamsEvent{
						ProviderId:          conf.ProviderID(),
						StreamOperationName: pusherReceive,
						ProviderType:        metric.ProviderTypePusher,
						DestinationName:     evt.Channel,
					})
					data, err := p.entityMapper.mapEvent(subConf.FieldName, evt.Data)
					if err != nil {
						// A payload that cannot be reduced to an entity key carries no
						// usable update, so it is dropped rather than forwarded as-is:
						// forwarding it would fail in the resolver instead, with less
						// context about which event was at fault.
						log.Error("could not map pusher event to an entity representation",
							zap.String("message_channel", evt.Channel),
							zap.String("event", evt.Name),
							zap.Error(err),
						)
						continue
					}
					updater.Update([]datasource.StreamEvent{
						&Event{evt: &MutableEvent{Data: data}},
					})
				case <-p.ctx.Done():
					// When the application context is done, we stop the subscription if it is not already done
					log.Debug("application context done, stopping subscription")
					return
				case <-ctx.Done():
					// When the subscription context is done, we stop the subscription if it is not already done
					log.Debug("subscription context done, stopping subscription")
					return
				}
			}
		}(subscription)
	}

	return nil
}

// Publish is not supported: monday's monolith is the only publisher to these
// channels, and the Pusher client protocol cannot publish at all.
func (p *ProviderAdapter) Publish(ctx context.Context, conf datasource.PublishEventConfiguration, events []datasource.StreamEvent) error {
	return datasource.NewError("publish is not supported by the pusher provider", nil)
}
