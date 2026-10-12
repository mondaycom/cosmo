package datasource

import (
	"context"
	"errors"
	"fmt"

	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// BackfillAdapter is implemented by adapters that can replay the events a subscriber missed.
type BackfillAdapter interface {
	// BackfillEnabled reports whether replaying missed events is configured.
	BackfillEnabled() bool
	// Backfill returns, in publish order, the events published to cfg after cursor.
	// skip reports whether a live event is one of the returned events, so it is not
	// delivered twice. It is called on live events received while Backfill ran.
	Backfill(ctx context.Context, cfg SubscriptionEventConfiguration, cursor string) (events []StreamEvent, skip func(data []byte) bool, err error)
}

// BackfillPubSubSubscriptionDataSource is a PubSubSubscriptionDataSource that replays the
// events a subscriber missed before delivering live events. The resolver makes sure the
// hand-over is gap-free and ordered (see resolve.BackfillSubscriptionDataSource).
type BackfillPubSubSubscriptionDataSource[C SubscriptionEventConfiguration] struct {
	*PubSubSubscriptionDataSource[C]
	backfill BackfillAdapter
}

func NewBackfillPubSubSubscriptionDataSource[C SubscriptionEventConfiguration](ds *PubSubSubscriptionDataSource[C], backfill BackfillAdapter) *BackfillPubSubSubscriptionDataSource[C] {
	return &BackfillPubSubSubscriptionDataSource[C]{
		PubSubSubscriptionDataSource: ds,
		backfill:                     backfill,
	}
}

// SubscriptionBackfill emits the events the subscriber missed since its resume cursor.
// Replayed events pass through the same BeforeEventsDispatch and OnReceiveEvents hooks as live events.
func (s *BackfillPubSubSubscriptionDataSource[C]) SubscriptionBackfill(ctx resolve.StartupHookContext, input []byte) (skip func(data []byte) bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			s.logger.
				WithOptions(zap.AddStacktrace(zapcore.ErrorLevel)).
				Error("[Recovery from handler panic]",
					zap.String("handler_name", "SubscriptionBackfill"),
					zap.Any("error", r),
				)
			skip, err = nil, fmt.Errorf("%v", r)
		}
	}()

	if s.hooks.ResumeCursor == nil {
		return nil, nil
	}
	cursor := s.hooks.ResumeCursor(ctx.Context)
	if cursor == "" {
		return nil, nil
	}

	subConf, err := s.SubscriptionEventConfiguration(input)
	if err != nil {
		return nil, err
	}
	conf, ok := subConf.(C)
	if !ok {
		return nil, errors.New("invalid subscription configuration")
	}

	events, skip, err := s.backfill.Backfill(ctx.Context, conf, cursor)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return skip, nil
	}

	logger := s.logger.With(
		zap.String("component", "subscription_backfill"),
		zap.String("provider_id", conf.ProviderID()),
		zap.String("provider_type", string(conf.ProviderType())),
		zap.String("field_name", conf.RootFieldName()),
	)
	updater := NewSubscriptionEventUpdater(conf, s.hooks, nil, logger, s.eventBuilder).(*subscriptionEventUpdater)
	events, err = updater.applyHooksForSubscription(ctx.Context, events)

	// Like the live path, emit what the hooks returned even if they failed.
	for _, event := range events {
		if event == nil {
			continue
		}
		ctx.Updater(event.GetData())
	}
	if err != nil {
		return nil, err
	}
	return skip, nil
}

var _ resolve.BackfillSubscriptionDataSource = (*BackfillPubSubSubscriptionDataSource[SubscriptionEventConfiguration])(nil)
var _ SubscriptionDataSource = (*BackfillPubSubSubscriptionDataSource[SubscriptionEventConfiguration])(nil)
