package pusher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"

	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
)

type Event struct {
	evt *MutableEvent
}

func (e *Event) GetData() []byte {
	if e.evt == nil {
		return nil
	}
	return slices.Clone(e.evt.Data)
}

func (e *Event) Clone() datasource.MutableStreamEvent {
	return e.evt.Clone()
}

type MutableEvent struct {
	Data json.RawMessage `json:"data"`
}

func (e *MutableEvent) GetData() []byte {
	if e == nil {
		return nil
	}
	return e.Data
}

func (e *MutableEvent) SetData(data []byte) {
	if e == nil {
		return
	}
	e.Data = data
}

func (e *MutableEvent) Clone() datasource.MutableStreamEvent {
	if e == nil {
		return nil
	}

	return &MutableEvent{
		Data: slices.Clone(e.Data),
	}
}

// SubscriptionEventConfiguration contains configuration for subscription events
type SubscriptionEventConfiguration struct {
	Provider  string   `json:"providerId"`
	Channels  []string `json:"channels"`
	FieldName string   `json:"rootFieldName"`
}

// ProviderID returns the provider ID
func (s *SubscriptionEventConfiguration) ProviderID() string {
	return s.Provider
}

// ProviderType returns the provider type
func (s *SubscriptionEventConfiguration) ProviderType() datasource.ProviderType {
	return datasource.ProviderTypePusher
}

// RootFieldName returns the root field name
func (s *SubscriptionEventConfiguration) RootFieldName() string {
	return s.FieldName
}

// SubscriptionDataSource implements resolve.SubscriptionDataSource for Pusher
type SubscriptionDataSource struct {
	pubSub datasource.Adapter
}

func (s *SubscriptionDataSource) SubscriptionEventConfiguration(input []byte) datasource.SubscriptionEventConfiguration {
	var subscriptionConfiguration SubscriptionEventConfiguration
	err := json.Unmarshal(input, &subscriptionConfiguration)
	if err != nil {
		return nil
	}
	return &subscriptionConfiguration
}

// Start starts the subscription
func (s *SubscriptionDataSource) Start(ctx *resolve.Context, header http.Header, input []byte, updater datasource.SubscriptionEventUpdater) error {
	subConf := s.SubscriptionEventConfiguration(input)
	if subConf == nil {
		return fmt.Errorf("no subscription configuration found")
	}

	conf, ok := subConf.(*SubscriptionEventConfiguration)
	if !ok {
		return fmt.Errorf("invalid subscription configuration")
	}

	return s.pubSub.Subscribe(ctx.Context(), conf, updater)
}

// LoadInitialData implements the interface method (not used for this subscription type)
func (s *SubscriptionDataSource) LoadInitialData(ctx context.Context) (initial []byte, err error) {
	return nil, nil
}

// Interface compliance checks
var _ datasource.SubscriptionEventConfiguration = (*SubscriptionEventConfiguration)(nil)
var _ datasource.StreamEvent = (*Event)(nil)
var _ datasource.MutableStreamEvent = (*MutableEvent)(nil)
