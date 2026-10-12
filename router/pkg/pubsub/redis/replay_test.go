package redis

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
	"go.uber.org/zap/zaptest"
)

func newReplayAdapter(t *testing.T, pubsubAddr string, replay ReplayOptions) *ProviderAdapter {
	t.Helper()
	ctx := context.Background()
	replay.Enabled = true
	adapter := NewProviderAdapterWithReplay(ctx, zaptest.NewLogger(t), []string{"redis://" + pubsubAddr}, false, replay, datasource.ProviderOpts{}).(*ProviderAdapter)
	require.NoError(t, adapter.Startup(ctx))
	t.Cleanup(func() { _ = adapter.Shutdown(ctx) })
	return adapter
}

// streamIDAt returns a stream ID for an entry added offset before now.
func streamIDAt(offset time.Duration, seq int) string {
	return fmt.Sprintf("%d-%d", time.Now().Add(-offset).UnixMilli(), seq)
}

func xadd(t *testing.T, mr *miniredis.Miniredis, channel, id, data string) {
	t.Helper()
	_, err := mr.XAdd(replayStream(channel), id, []string{"data", data})
	require.NoError(t, err)
}

func eventData(events []datasource.StreamEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = string(e.GetData())
	}
	return out
}

func TestProviderAdapter_Backfill(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	adapter := newReplayAdapter(t, mr.Addr(), ReplayOptions{})

	cursor := streamIDAt(time.Minute, 0)
	a1, a2 := streamIDAt(50*time.Second, 0), streamIDAt(30*time.Second, 0)
	b1 := streamIDAt(40*time.Second, 0)

	xadd(t, mr, "board.1", streamIDAt(70*time.Second, 0), `{"n":"old"}`)
	xadd(t, mr, "board.1", cursor, `{"n":"cursor"}`)
	xadd(t, mr, "board.1", a1, `{"n":"a1"}`)
	xadd(t, mr, "board.1", a2, `{"n":"a2"}`)
	xadd(t, mr, "board.2", b1, `{"n":"b1"}`)

	conf := &SubscriptionEventConfiguration{Provider: "p", Channels: []string{"board.1", "board.2", "board.missing"}}
	events, skip, err := adapter.Backfill(context.Background(), conf, cursor)
	require.NoError(t, err)

	// Entries after the cursor only, merged across channels in ID order, with the ID injected.
	assert.Equal(t, []string{
		fmt.Sprintf(`{"n":"a1","eventId":"%s"}`, a1),
		fmt.Sprintf(`{"n":"b1","eventId":"%s"}`, b1),
		fmt.Sprintf(`{"n":"a2","eventId":"%s"}`, a2),
	}, eventData(events))

	assert.True(t, skip([]byte(fmt.Sprintf(`{"eventId":"%s","n":"a1"}`, a1))))
	assert.True(t, skip([]byte(fmt.Sprintf(`{"eventId":"%s"}`, b1))))
	// Events the subscriber already had before disconnecting, e.g. still queued on a lagging router.
	assert.True(t, skip([]byte(fmt.Sprintf(`{"eventId":"%s"}`, cursor))))
	assert.True(t, skip([]byte(`{"eventId":"1-0"}`)))
	assert.False(t, skip([]byte(`{"eventId":"9999999999999-0"}`)))
	assert.False(t, skip([]byte(`{"n":"no id"}`)))
	assert.False(t, skip([]byte(`not json`)))
}

func TestProviderAdapter_BackfillCustomFields(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	adapter := newReplayAdapter(t, mr.Addr(), ReplayOptions{DataField: "payload", EventIDField: "cursor"})

	id := streamIDAt(time.Second, 3)
	_, err := mr.XAdd(replayStream("c"), id, []string{"payload", `{"x":1}`, "data", "ignored"})
	require.NoError(t, err)

	events, skip, err := adapter.Backfill(context.Background(), &SubscriptionEventConfiguration{Channels: []string{"c"}}, streamIDAt(time.Minute, 0))
	require.NoError(t, err)
	require.Len(t, events, 1)

	got, err := jsonparser.GetString(events[0].GetData(), "cursor")
	require.NoError(t, err)
	assert.Equal(t, id, got)
	assert.True(t, skip(events[0].GetData()))
}

func TestProviderAdapter_BackfillResyncRequired(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	adapter := newReplayAdapter(t, mr.Addr(), ReplayOptions{MaxEvents: 2, Retention: time.Minute})
	conf := &SubscriptionEventConfiguration{Channels: []string{"a", "b"}}

	t.Run("cursor older than retention", func(t *testing.T) {
		_, _, err := adapter.Backfill(context.Background(), conf, streamIDAt(2*time.Minute, 0))
		require.ErrorContains(t, err, "RESYNC_REQUIRED")
	})

	t.Run("too many missed events across channels", func(t *testing.T) {
		cursor := streamIDAt(30*time.Second, 0)
		xadd(t, mr, "a", streamIDAt(20*time.Second, 0), `{}`)
		xadd(t, mr, "a", streamIDAt(10*time.Second, 0), `{}`)
		_, _, err := adapter.Backfill(context.Background(), conf, cursor)
		require.NoError(t, err, "exactly MaxEvents missed events can be replayed")

		xadd(t, mr, "b", streamIDAt(5*time.Second, 0), `{}`)
		_, _, err = adapter.Backfill(context.Background(), conf, cursor)
		require.ErrorContains(t, err, "RESYNC_REQUIRED")
	})
}

func TestProviderAdapter_BackfillInvalidInput(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	adapter := newReplayAdapter(t, mr.Addr(), ReplayOptions{})
	conf := &SubscriptionEventConfiguration{Channels: []string{"c"}}

	for _, cursor := range []string{"abc", "1-x", "-1"} {
		_, _, err := adapter.Backfill(context.Background(), conf, cursor)
		require.Error(t, err, cursor)
	}

	xadd(t, mr, "c", streamIDAt(time.Second, 0), `[1,2]`)
	_, _, err := adapter.Backfill(context.Background(), conf, streamIDAt(time.Minute, 0))
	require.ErrorContains(t, err, "error reading missed event")
}

func TestProviderAdapter_BackfillUsesReplayRedis(t *testing.T) {
	t.Parallel()

	pubsub := miniredis.RunT(t)
	replay := miniredis.RunT(t)
	adapter := newReplayAdapter(t, pubsub.Addr(), ReplayOptions{URLs: []string{"redis://" + replay.Addr()}})

	xadd(t, pubsub, "c", streamIDAt(2*time.Second, 0), `{"from":"pubsub"}`)
	xadd(t, replay, "c", streamIDAt(time.Second, 0), `{"from":"replay"}`)

	events, _, err := adapter.Backfill(context.Background(), &SubscriptionEventConfiguration{Channels: []string{"c"}}, streamIDAt(time.Minute, 0))
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Contains(t, string(events[0].GetData()), `"from":"replay"`)
}

func TestProviderAdapter_BackfillNeverFallsBackWhenReplayRedisIsSet(t *testing.T) {
	t.Parallel()

	pubsub := miniredis.RunT(t)
	replay := miniredis.RunT(t)
	replayAddr := replay.Addr()
	replay.Close()

	ctx := context.Background()
	adapter := NewProviderAdapterWithReplay(ctx, zaptest.NewLogger(t), []string{"redis://" + pubsub.Addr()}, false,
		ReplayOptions{Enabled: true, URLs: []string{"redis://" + replayAddr}}, datasource.ProviderOpts{}).(*ProviderAdapter)
	require.Error(t, adapter.Startup(ctx))
	t.Cleanup(func() { _ = adapter.Shutdown(ctx) })

	xadd(t, pubsub, "c", streamIDAt(time.Second, 0), `{}`)
	_, _, err := adapter.Backfill(ctx, &SubscriptionEventConfiguration{Channels: []string{"c"}}, streamIDAt(time.Minute, 0))
	require.Error(t, err)
}

type recordingUpdater struct {
	noopUpdater
	mu   sync.Mutex
	data []string
}

func (r *recordingUpdater) Update(events []datasource.StreamEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range events {
		r.data = append(r.data, string(e.GetData()))
	}
}

func (r *recordingUpdater) received() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.data...)
}

func TestProviderAdapter_SubscribeWithReplayIsActiveOnReturn(t *testing.T) {
	t.Parallel()

	mr := miniredis.RunT(t)
	adapter := newReplayAdapter(t, mr.Addr(), ReplayOptions{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	updater := &recordingUpdater{}
	conf := &SubscriptionEventConfiguration{Provider: "p", Channels: []string{"a", "b"}}
	require.NoError(t, adapter.Subscribe(ctx, conf, updater))

	// Subscribe returned, so both subscriptions are confirmed: nothing published now is lost.
	assert.Equal(t, 1, mr.Publish("a", `{"n":1}`))
	assert.Equal(t, 1, mr.Publish("b", `{"n":2}`))

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ElementsMatch(c, []string{`{"n":1}`, `{"n":2}`}, updater.received())
	}, 2*time.Second, 10*time.Millisecond)
}
