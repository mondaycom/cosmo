package pusher

import (
	"encoding/json"
	"fmt"
	"slices"

	"github.com/buger/jsonparser"
	"github.com/cespare/xxhash/v2"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"go.uber.org/zap"
)

type EventType int

const (
	EventTypeSubscribe EventType = iota
)

// EngineDataSourceFactory implements the datasource.EngineDataSourceFactory interface for Pusher.
// Only subscriptions are supported.
type EngineDataSourceFactory struct {
	PusherAdapter datasource.Adapter

	fieldName  string
	eventType  EventType
	channels   []string
	providerId string
	logger     *zap.Logger
}

func (c *EngineDataSourceFactory) GetFieldName() string {
	return c.fieldName
}

// ResolveDataSource is only used for publishing, which Pusher does not support
func (c *EngineDataSourceFactory) ResolveDataSource() (resolve.DataSource, error) {
	return nil, fmt.Errorf("failed to configure fetch: publishing is not supported for Pusher")
}

// ResolveDataSourceInput is only used for publishing, which Pusher does not support
func (c *EngineDataSourceFactory) ResolveDataSourceInput(eventData []byte) (string, error) {
	return "", fmt.Errorf("publishing is not supported for Pusher")
}

// ResolveDataSourceSubscription returns the subscription data source
func (c *EngineDataSourceFactory) ResolveDataSourceSubscription() (datasource.SubscriptionDataSource, error) {
	triggerHashInputFn := func(input []byte, xxh *xxhash.Digest) error {
		val, _, _, err := jsonparser.Get(input, "channels")
		if err != nil {
			return err
		}

		_, err = xxh.Write(val)
		if err != nil {
			return err
		}

		val, _, _, err = jsonparser.Get(input, "providerId")
		if err != nil {
			return err
		}

		_, err = xxh.Write(val)
		return err
	}

	eventCreateFn := func(data []byte) datasource.MutableStreamEvent {
		return &MutableEvent{Data: data}
	}

	return datasource.NewPubSubSubscriptionDataSource[*SubscriptionEventConfiguration](
		c.PusherAdapter, triggerHashInputFn, c.logger, eventCreateFn,
	), nil
}

// ResolveDataSourceSubscriptionInput builds the input for the subscription data source
func (c *EngineDataSourceFactory) ResolveDataSourceSubscriptionInput() (string, error) {
	evtCfg := SubscriptionEventConfiguration{
		Provider:  c.providerId,
		Channels:  c.channels,
		FieldName: c.fieldName,
	}
	object, err := json.Marshal(evtCfg)
	if err != nil {
		return "", fmt.Errorf("failed to marshal event subscription configuration")
	}
	return string(object), nil
}

// TransformEventData expands the argument templates in the channel names
func (c *EngineDataSourceFactory) TransformEventData(extractFn datasource.ArgumentTemplateCallback) error {
	extractedChannels := make([]string, 0, len(c.channels))
	for _, rawChannel := range c.channels {
		extractedChannel, err := extractFn(rawChannel)
		if err != nil {
			return nil
		}
		extractedChannels = append(extractedChannels, extractedChannel)
	}
	slices.Sort(extractedChannels)
	c.channels = extractedChannels

	return nil
}
