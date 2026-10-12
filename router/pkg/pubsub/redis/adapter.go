package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/wundergraph/cosmo/router/pkg/metric"

	rd "github.com/wundergraph/cosmo/router/internal/rediscloser"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"go.uber.org/zap"
)

const (
	redisPublish = "publish"
	redisReceive = "receive"
)

// Ensure ProviderAdapter implements ProviderSubscriptionHooks
var _ datasource.Adapter = (*ProviderAdapter)(nil)

func NewProviderAdapter(ctx context.Context, logger *zap.Logger, urls []string, clusterEnabled bool, opts datasource.ProviderOpts) datasource.Adapter {
	return NewProviderAdapterWithReplay(ctx, logger, urls, clusterEnabled, ReplayOptions{}, opts)
}

// NewProviderAdapterWithReplay creates an adapter that can also replay missed events from
// Redis Streams (see ProviderAdapter.Backfill).
func NewProviderAdapterWithReplay(ctx context.Context, logger *zap.Logger, urls []string, clusterEnabled bool, replay ReplayOptions, opts datasource.ProviderOpts) datasource.Adapter {
	ctx, cancel := context.WithCancel(ctx)
	if logger == nil {
		logger = zap.NewNop()
	}

	var store metric.StreamMetricStore
	if opts.StreamMetricStore != nil {
		store = opts.StreamMetricStore
	} else {
		store = metric.NewNoopStreamMetricStore()
	}

	return &ProviderAdapter{
		ctx:               ctx,
		cancel:            cancel,
		logger:            logger,
		urls:              urls,
		clusterEnabled:    clusterEnabled,
		streamMetricStore: store,
		skipUnavailable:   opts.SkipUnavailableProviders,
		replay:            replay.withDefaults(),
	}
}

type ProviderAdapter struct {
	ctx               context.Context
	cancel            context.CancelFunc
	conn              rd.RDCloser
	logger            *zap.Logger
	closeWg           sync.WaitGroup
	urls              []string
	clusterEnabled    bool
	streamMetricStore metric.StreamMetricStore
	// skipUnavailable mirrors events.skip_unavailable_providers. When true, Startup keeps
	// the resilient client even if the initial connection check fails, so go-redis can
	// reconnect on a later command and the provider recovers without a restart.
	skipUnavailable bool
	replay          ReplayOptions
	// replayConn is the connection to the Redis holding the replay streams, when it differs
	// from the pub/sub one. When nil, the streams are read through conn.
	replayConn rd.RDCloser
}

func (p *ProviderAdapter) Startup(ctx context.Context) error {
	// Bound the initial connectivity check so a black-holed broker (SYN dropped) cannot block
	// Startup for the full go-redis dial timeout, which could exceed the caller's startup
	// timeout and leave this call running in a leaked goroutine.
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rdCloser, err := rd.NewRedisCloser(&rd.RedisCloserOptions{
		Logger:         p.logger,
		URLs:           p.urls,
		ClusterEnabled: p.clusterEnabled,
		Context:        pingCtx,
	})
	if err != nil {
		if p.skipUnavailable && rdCloser != nil {
			// Lenient mode: retain the client even though the initial connection check
			// failed. go-redis is lazy and reconnects on the next command, so the provider
			// recovers without a restart once the broker is reachable. The error is still
			// returned so the caller can log a distinct "could not connect" message.
			p.conn = rdCloser
			return errors.Join(err, p.startupReplay(ctx))
		}
		// Strict mode: the router will abort startup, so close the client we won't keep.
		if rdCloser != nil {
			_ = rdCloser.Close()
		}
		return err
	}

	p.conn = rdCloser

	return p.startupReplay(ctx)
}

// startupReplay connects to the Redis holding the replay streams, if configured separately.
func (p *ProviderAdapter) startupReplay(ctx context.Context) error {
	if !p.replay.Enabled || len(p.replay.URLs) == 0 {
		return nil
	}
	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	rdCloser, err := rd.NewRedisCloser(&rd.RedisCloserOptions{
		Logger:         p.logger,
		URLs:           p.replay.URLs,
		ClusterEnabled: p.replay.ClusterEnabled,
		Context:        pingCtx,
	})
	if err != nil {
		if p.skipUnavailable && rdCloser != nil {
			p.replayConn = rdCloser
			return fmt.Errorf("replay redis: %w", err)
		}
		if rdCloser != nil {
			_ = rdCloser.Close()
		}
		return fmt.Errorf("replay redis: %w", err)
	}
	p.replayConn = rdCloser
	return nil
}

func (p *ProviderAdapter) Shutdown(ctx context.Context) error {
	if p.conn == nil {
		return nil
	}

	// Cancel the context to stop the subscriptions
	p.cancel()

	// Wait for the subscriptions to be closed
	p.closeWg.Wait()

	// Close the connections
	err := p.conn.Close()
	if p.replayConn != nil {
		err = errors.Join(err, p.replayConn.Close())
	}
	return err
}

func (p *ProviderAdapter) Subscribe(ctx context.Context, conf datasource.SubscriptionEventConfiguration, updater datasource.SubscriptionEventUpdater) error {
	subConf, ok := conf.(*SubscriptionEventConfiguration)
	if !ok {
		return datasource.NewError("subscription event not supported by redis provider", nil)
	}

	log := p.logger.With(
		zap.String("provider_id", conf.ProviderID()),
		zap.String("method", "subscribe"),
		zap.Strings("channels", subConf.Channels),
	)
	// Guard the possibly-nil connection: in strict mode a failed Startup leaves p.conn nil
	// (under skip_unavailable_providers the resilient client is retained instead), so return
	// an error rather than panicking if Subscribe is somehow reached without a connection.
	if p.conn == nil {
		return datasource.NewError("redis connection not initialized", nil)
	}

	// monday fork: SUBSCRIBE, not PSUBSCRIBE. AWS ElastiCache Serverless lists
	// psubscribe/punsubscribe among the commands unavailable on serverless caches,
	// so a pattern subscribe is rejected server-side — and go-redis swallows it in
	// the Channel() retry loop, so the subscription just silently never delivers.
	// Trade-off: glob channels in @edfs__redisSubscribe(channels: [...]) no longer
	// match. Channels templated from field arguments are unaffected.
	log.Debug("subscribing")
	sub := p.conn.Subscribe(ctx, subConf.Channels...)

	// With replay, the resolver reads the missed events once Subscribe returns and relies on
	// every later event being delivered live, so wait until Redis confirmed the subscriptions.
	var early []*redis.Message
	if p.replay.Enabled {
		var err error
		early, err = awaitSubscribed(ctx, sub, len(subConf.Channels))
		if err != nil {
			if closeErr := sub.Close(); closeErr != nil {
				log.Error("error closing redis subscription", zap.Error(closeErr))
			}
			return datasource.NewError("error subscribing to redis channels", err)
		}
	}
	msgChan := sub.Channel()

	cleanup := func() {
		if err := sub.Close(); err != nil {
			log.Error("error closing redis subscription", zap.Error(err))
		}
	}

	p.closeWg.Add(1)

	go func() {
		defer p.closeWg.Done()
		defer cleanup()

		for _, msg := range early {
			p.deliver(ctx, conf, updater, log, msg)
		}

		for {
			select {
			case msg, ok := <-msgChan:
				if !ok {
					log.Debug("subscription closed, stopping")
					return
				}
				if msg == nil {
					log.Debug("empty message received on subscription update, skipping")
					return
				}
				p.deliver(ctx, conf, updater, log, msg)
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
	}()

	return nil
}

func (p *ProviderAdapter) deliver(ctx context.Context, conf datasource.SubscriptionEventConfiguration, updater datasource.SubscriptionEventUpdater, log *zap.Logger, msg *redis.Message) {
	log.Debug("subscription update", zap.String("message_channel", msg.Channel), zap.String("data", msg.Payload))
	p.streamMetricStore.Consume(ctx, metric.StreamsEvent{
		ProviderId:          conf.ProviderID(),
		StreamOperationName: redisReceive,
		ProviderType:        metric.ProviderTypeRedis,
		DestinationName:     msg.Channel,
	})
	updater.Update([]datasource.StreamEvent{
		&Event{evt: &MutableEvent{
			Data: []byte(msg.Payload),
		}},
	})
}

// subscribeConfirmTimeout bounds how long Subscribe waits for Redis to confirm the subscriptions.
const subscribeConfirmTimeout = 5 * time.Second

// awaitSubscribed waits until Redis confirmed the subscription to channels channels. Messages
// received in between are returned, so they can be delivered before the ones that follow.
func awaitSubscribed(ctx context.Context, sub *redis.PubSub, channels int) ([]*redis.Message, error) {
	deadline := time.Now().Add(subscribeConfirmTimeout)
	var early []*redis.Message
	for confirmed := 0; confirmed < channels; {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return nil, errors.New("timed out waiting for the subscription confirmation")
		}
		msg, err := sub.ReceiveTimeout(ctx, timeout)
		if err != nil {
			return nil, err
		}
		switch m := msg.(type) {
		case *redis.Subscription:
			if m.Kind == "subscribe" {
				confirmed++
			}
		case *redis.Message:
			early = append(early, m)
		}
	}
	return early, nil
}

func (p *ProviderAdapter) Publish(ctx context.Context, conf datasource.PublishEventConfiguration, events []datasource.StreamEvent) error {
	pubConf, ok := conf.(*PublishEventConfiguration)
	if !ok {
		return datasource.NewError("publish event not supported by redis provider", nil)
	}

	log := p.logger.With(
		zap.String("provider_id", conf.ProviderID()),
		zap.String("method", "publish"),
		zap.String("channel", pubConf.Channel),
	)

	if p.conn == nil {
		return datasource.NewError("redis connection not initialized", nil)
	}

	if len(events) == 0 {
		return nil
	}

	log.Debug("publish", zap.Int("event_count", len(events)))

	var errs []error

	for _, streamEvent := range events {
		redisEvent, err := castToMutableEvent(streamEvent)
		if err != nil {
			errs = append(errs, err)
			continue
		}

		data, dataErr := redisEvent.Data.MarshalJSON()
		if dataErr != nil {
			errs = append(errs, fmt.Errorf("error marshalling data: %w", dataErr))
			continue
		}

		intCmd := p.conn.Publish(ctx, pubConf.Channel, data)
		if intCmd.Err() != nil {
			errs = append(errs, intCmd.Err())
		}
	}

	// Produce metrics for all failed and successfully published events
	successCount := len(events) - len(errs)
	for range successCount {
		p.streamMetricStore.Produce(ctx, metric.StreamsEvent{
			ProviderId:          pubConf.ProviderID(),
			StreamOperationName: redisPublish,
			ProviderType:        metric.ProviderTypeRedis,
			DestinationName:     pubConf.Channel,
		})
	}
	for range len(errs) {
		p.streamMetricStore.Produce(ctx, metric.StreamsEvent{
			ProviderId:          pubConf.ProviderID(),
			StreamOperationName: redisPublish,
			ProviderType:        metric.ProviderTypeRedis,
			ErrorType:           "publish_error",
			DestinationName:     pubConf.Channel,
		})
	}

	// Collect and return all errors if any occurred
	if len(errs) > 0 {
		combinedErr := errors.Join(errs...)
		log.Error("publish errors", zap.Error(combinedErr), zap.Int("failed_count", len(errs)), zap.Int("total_count", len(events)))
		return datasource.NewError(
			fmt.Sprintf("error publishing %d/%d events to Redis PubSub channel %s", len(errs), len(events), pubConf.Channel), combinedErr,
		)
	}

	return nil
}

func castToMutableEvent(event datasource.StreamEvent) (*MutableEvent, error) {
	switch evt := event.(type) {
	case *Event:
		return evt.evt, nil
	case *MutableEvent:
		return evt, nil
	default:
		return nil, errors.New("invalid event type for Redis adapter")
	}
}
