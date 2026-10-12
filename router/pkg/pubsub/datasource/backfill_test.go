package datasource

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/cespare/xxhash/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"go.uber.org/zap"
)

type fakeBackfillAdapter struct {
	events  []StreamEvent
	skip    func(data []byte) bool
	err     error
	cursors []string
}

func (f *fakeBackfillAdapter) BackfillEnabled() bool { return true }

func (f *fakeBackfillAdapter) Backfill(_ context.Context, _ SubscriptionEventConfiguration, cursor string) ([]StreamEvent, func(data []byte) bool, error) {
	f.cursors = append(f.cursors, cursor)
	return f.events, f.skip, f.err
}

func newTestBackfillDataSource(t *testing.T, adapter *fakeBackfillAdapter, hooks Hooks) *BackfillPubSubSubscriptionDataSource[testSubscriptionEventConfiguration] {
	t.Helper()
	ds := NewPubSubSubscriptionDataSource[testSubscriptionEventConfiguration](
		NewMockProvider(t),
		func(input []byte, xxh *xxhash.Digest) error { return nil },
		zap.NewNop(),
		testSubscriptionDataSourceEventBuilder,
	)
	ds.SetHooks(hooks)
	return NewBackfillPubSubSubscriptionDataSource(ds, adapter)
}

func testBackfillInput(t *testing.T) []byte {
	t.Helper()
	input, err := json.Marshal(testSubscriptionEventConfiguration{Topic: "topic"})
	require.NoError(t, err)
	return input
}

func collectingStartupContext(out *[]string) resolve.StartupHookContext {
	return resolve.StartupHookContext{
		Context: context.Background(),
		Updater: func(data []byte) { *out = append(*out, string(data)) },
	}
}

func staticCursor(cursor string) ResumeCursorFn {
	return func(context.Context) string { return cursor }
}

func TestBackfillPubSubSubscriptionDataSource_NoCursor(t *testing.T) {
	for name, hooks := range map[string]Hooks{
		"no resume cursor hook": {},
		"empty cursor":          {ResumeCursor: staticCursor("")},
	} {
		t.Run(name, func(t *testing.T) {
			adapter := &fakeBackfillAdapter{events: []StreamEvent{&testEvent{evt: mutableTestEvent(`{"a":1}`)}}}
			ds := newTestBackfillDataSource(t, adapter, hooks)

			var emitted []string
			skip, err := ds.SubscriptionBackfill(collectingStartupContext(&emitted), testBackfillInput(t))
			require.NoError(t, err)
			assert.Nil(t, skip)
			assert.Empty(t, emitted)
			assert.Empty(t, adapter.cursors)
		})
	}
}

func TestBackfillPubSubSubscriptionDataSource_EmitsEventsThroughHooks(t *testing.T) {
	adapter := &fakeBackfillAdapter{
		events: []StreamEvent{
			&testEvent{evt: mutableTestEvent(`{"id":1}`)},
			&testEvent{evt: mutableTestEvent(`{"id":2,"private":true}`)},
			&testEvent{evt: mutableTestEvent(`{"id":3}`)},
		},
		skip: func(data []byte) bool { return true },
	}
	hooks := Hooks{
		ResumeCursor: staticCursor("100-0"),
		BeforeEventsDispatch: BeforeEventsDispatchHooks{Handlers: []BeforeEventsDispatchFn{
			func(_ context.Context, _ SubscriptionEventConfiguration, _ EventBuilderFn, evts []StreamEvent) ([]StreamEvent, error) {
				out := evts[:0]
				for _, e := range evts {
					if !bytes.Contains(e.GetData(), []byte("private")) {
						out = append(out, e)
					}
				}
				return out, nil
			},
		}},
		OnReceiveEvents: OnReceiveEventsHooks{Handlers: []OnReceiveEventsFn{
			func(_ context.Context, _ context.Context, _ SubscriptionEventConfiguration, build EventBuilderFn, evts []StreamEvent) ([]StreamEvent, error) {
				out := make([]StreamEvent, 0, len(evts))
				for _, e := range evts {
					out = append(out, build(bytes.ReplaceAll(e.GetData(), []byte(`"id"`), []byte(`"seen"`))))
				}
				return out, nil
			},
		}},
	}
	ds := newTestBackfillDataSource(t, adapter, hooks)

	var emitted []string
	skip, err := ds.SubscriptionBackfill(collectingStartupContext(&emitted), testBackfillInput(t))
	require.NoError(t, err)
	require.NotNil(t, skip)
	assert.Equal(t, []string{"100-0"}, adapter.cursors)
	assert.Equal(t, []string{`{"seen":1}`, `{"seen":3}`}, emitted)
}

func TestBackfillPubSubSubscriptionDataSource_AdapterError(t *testing.T) {
	adapter := &fakeBackfillAdapter{err: errors.New("RESYNC_REQUIRED: gone")}
	ds := newTestBackfillDataSource(t, adapter, Hooks{ResumeCursor: staticCursor("1-0")})

	var emitted []string
	_, err := ds.SubscriptionBackfill(collectingStartupContext(&emitted), testBackfillInput(t))
	require.ErrorContains(t, err, "RESYNC_REQUIRED")
	assert.Empty(t, emitted)
}

func TestBackfillPubSubSubscriptionDataSource_BeforeEventsDispatchFailure(t *testing.T) {
	adapter := &fakeBackfillAdapter{events: []StreamEvent{&testEvent{evt: mutableTestEvent(`{"id":1}`)}}}
	hooks := Hooks{
		ResumeCursor: staticCursor("1-0"),
		BeforeEventsDispatch: BeforeEventsDispatchHooks{Handlers: []BeforeEventsDispatchFn{
			func(context.Context, SubscriptionEventConfiguration, EventBuilderFn, []StreamEvent) ([]StreamEvent, error) {
				return nil, errors.New("denied")
			},
		}},
	}
	ds := newTestBackfillDataSource(t, adapter, hooks)

	var emitted []string
	_, err := ds.SubscriptionBackfill(collectingStartupContext(&emitted), testBackfillInput(t))
	require.Error(t, err)
	assert.Empty(t, emitted)
}
