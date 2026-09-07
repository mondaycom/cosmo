package pusher

import (
	"context"
	"fmt"

	nodev1 "github.com/wundergraph/cosmo/router/gen/proto/wg/cosmo/node/v1"
	"github.com/wundergraph/cosmo/router/pkg/config"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"go.uber.org/zap"
)

const providerTypeID = "pusher"

// ProviderBuilder builds Pusher PubSub providers
type ProviderBuilder struct {
	ctx              context.Context
	logger           *zap.Logger
	hostName         string
	routerListenAddr string
}

// NewProviderBuilder creates a new Pusher PubSub provider builder
func NewProviderBuilder(
	ctx context.Context,
	logger *zap.Logger,
	hostName string,
	routerListenAddr string,
) *ProviderBuilder {
	return &ProviderBuilder{
		ctx:              ctx,
		logger:           logger,
		hostName:         hostName,
		routerListenAddr: routerListenAddr,
	}
}

// TypeID returns the provider type ID
func (b *ProviderBuilder) TypeID() string {
	return providerTypeID
}

// BuildEngineDataSourceFactory creates a Pusher data source for the given event configuration
func (b *ProviderBuilder) BuildEngineDataSourceFactory(data *nodev1.PusherEventConfiguration, providers map[string]datasource.Provider) (datasource.EngineDataSourceFactory, error) {
	providerId := data.GetEngineEventConfiguration().GetProviderId()
	provider, ok := providers[providerId]
	if !ok {
		return nil, fmt.Errorf("failed to get adapter for provider %s with ID %s", b.TypeID(), providerId)
	}

	eventType := data.GetEngineEventConfiguration().GetType()
	if eventType != nodev1.EventType_SUBSCRIBE {
		return nil, fmt.Errorf("unsupported event type for Pusher: %s, only subscriptions are supported", eventType)
	}

	return &EngineDataSourceFactory{
		fieldName:     data.GetEngineEventConfiguration().GetFieldName(),
		eventType:     EventTypeSubscribe,
		channels:      data.GetChannels(),
		providerId:    providerId,
		PusherAdapter: provider,
		logger:        b.logger,
	}, nil
}

// BuildProvider returns the Pusher PubSub provider for the given event source
func (b *ProviderBuilder) BuildProvider(provider config.PusherEventSource, providerOpts datasource.ProviderOpts) (datasource.Provider, error) {
	adapter, err := NewProviderAdapter(b.ctx, b.logger, provider, providerOpts)
	if err != nil {
		return nil, err
	}
	eventBuilder := func(data []byte) datasource.MutableStreamEvent {
		return &MutableEvent{Data: data}
	}

	pubSubProvider := datasource.NewPubSubProvider(provider.ID, providerTypeID, adapter, b.logger, eventBuilder)

	return pubSubProvider, nil
}
