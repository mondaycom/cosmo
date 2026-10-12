package redis

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/buger/jsonparser"
	"github.com/redis/go-redis/v9"
	"github.com/wundergraph/cosmo/router/pkg/pubsub/datasource"
)

const (
	// replayStreamPrefix is prepended to a pub/sub channel to get the stream holding its events.
	replayStreamPrefix = "channel."

	defaultReplayDataField    = "data"
	defaultReplayEventIDField = "eventId"
	defaultReplayMaxEvents    = 10_000
	defaultReplayRetention    = 15 * time.Minute

	// resyncRequired prefixes errors telling the client it cannot resume and must refetch its state.
	resyncRequired = "RESYNC_REQUIRED"
)

// ReplayOptions configures replaying missed events from Redis Streams.
type ReplayOptions struct {
	Enabled bool
	// URLs of the Redis holding the streams. When empty, the pub/sub connection is used.
	URLs           []string
	ClusterEnabled bool
	DataField      string
	EventIDField   string
	MaxEvents      int
	Retention      time.Duration
}

func (o ReplayOptions) withDefaults() ReplayOptions {
	if o.DataField == "" {
		o.DataField = defaultReplayDataField
	}
	if o.EventIDField == "" {
		o.EventIDField = defaultReplayEventIDField
	}
	if o.MaxEvents <= 0 {
		o.MaxEvents = defaultReplayMaxEvents
	}
	if o.Retention <= 0 {
		o.Retention = defaultReplayRetention
	}
	return o
}

// replayStream returns the stream holding the events of a pub/sub channel.
func replayStream(channel string) string {
	return replayStreamPrefix + channel
}

// streamID is a parsed Redis stream entry ID ("<ms>-<seq>").
type streamID struct {
	ms, seq uint64
}

func parseStreamID(id string) (streamID, error) {
	msPart, seqPart, found := strings.Cut(id, "-")
	ms, err := strconv.ParseUint(msPart, 10, 64)
	if err != nil {
		return streamID{}, fmt.Errorf("invalid stream ID %q", id)
	}
	if !found {
		return streamID{ms: ms}, nil
	}
	seq, err := strconv.ParseUint(seqPart, 10, 64)
	if err != nil {
		return streamID{}, fmt.Errorf("invalid stream ID %q", id)
	}
	return streamID{ms: ms, seq: seq}, nil
}

func (a streamID) compare(b streamID) int {
	if c := cmp.Compare(a.ms, b.ms); c != 0 {
		return c
	}
	return cmp.Compare(a.seq, b.seq)
}

func (a streamID) String() string {
	return strconv.FormatUint(a.ms, 10) + "-" + strconv.FormatUint(a.seq, 10)
}

// BackfillEnabled reports whether replaying missed events is configured.
func (p *ProviderAdapter) BackfillEnabled() bool {
	return p.replay.Enabled
}

// Backfill reads the entries after cursor from the stream of every channel of the subscription
// and returns them in stream ID order, with the entry ID set on the EventIDField of each payload.
// The returned skip function drops live events whose EventIDField is the cursor, one before
// it, or one of the returned IDs, i.e. events the subscriber already has.
//
// Backfill fails with RESYNC_REQUIRED when it cannot guarantee a complete replay: the cursor is
// older than the retention, or more than MaxEvents events were missed.
func (p *ProviderAdapter) Backfill(ctx context.Context, conf datasource.SubscriptionEventConfiguration, cursor string) ([]datasource.StreamEvent, func(data []byte) bool, error) {
	subConf, ok := conf.(*SubscriptionEventConfiguration)
	if !ok {
		return nil, nil, datasource.NewError("subscription event not supported by redis provider", nil)
	}
	conn := p.replayClient()
	if conn == nil {
		return nil, nil, datasource.NewError("redis replay connection not initialized", nil)
	}

	from, err := parseStreamID(cursor)
	if err != nil {
		return nil, nil, datasource.NewError(fmt.Sprintf("invalid lastEventId %q", cursor), err)
	}
	if oldest := time.Now().Add(-p.replay.Retention).UnixMilli(); from.ms < uint64(max(oldest, 0)) {
		return nil, nil, datasource.NewError(fmt.Sprintf("%s: lastEventId %s is older than the replay retention", resyncRequired, cursor), nil)
	}

	type entry struct {
		id   streamID
		data []byte
	}
	var entries []entry

	// TODO: investigate limiting concurrent replays. The client picks the cursor, so every
	// resume can read up to MaxEvents+1 entries per channel from the replay Redis.
	for _, channel := range subConf.Channels {
		// Read one entry more than allowed to detect that too many events were missed.
		msgs, err := conn.XRangeN(ctx, replayStream(channel), "("+from.String(), "+", int64(p.replay.MaxEvents)+1).Result()
		if err != nil && err != redis.Nil {
			return nil, nil, datasource.NewError(fmt.Sprintf("error reading missed events of channel %s", channel), err)
		}
		for _, msg := range msgs {
			id, err := parseStreamID(msg.ID)
			if err != nil {
				return nil, nil, datasource.NewError("error reading missed events", err)
			}
			data, err := p.replayEntryData(msg)
			if err != nil {
				return nil, nil, datasource.NewError(fmt.Sprintf("error reading missed event %s of channel %s", msg.ID, channel), err)
			}
			entries = append(entries, entry{id: id, data: data})
		}
		if len(entries) > p.replay.MaxEvents {
			return nil, nil, datasource.NewError(fmt.Sprintf("%s: more than %d events were missed", resyncRequired, p.replay.MaxEvents), nil)
		}
	}

	// TODO: verify clocks in cluster mode. One cursor is applied to every channel's stream and
	// entries are merged by ID, which assumes all streams get IDs from the same clock. Streams on
	// different cluster nodes may not, and an entry could then fall before the cursor and be lost.
	// Entries of one stream are already ordered; merge those of several channels by ID.
	slices.SortStableFunc(entries, func(a, b entry) int { return a.id.compare(b.id) })

	events := make([]datasource.StreamEvent, 0, len(entries))
	replayed := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		events = append(events, &Event{evt: &MutableEvent{Data: e.data}})
		replayed[e.id.String()] = struct{}{}
	}

	eventIDField := p.replay.EventIDField
	skip := func(data []byte) bool {
		id, err := jsonparser.GetString(data, eventIDField)
		if err != nil {
			return false
		}
		parsed, err := parseStreamID(id)
		if err != nil {
			return false
		}
		// The subscriber received everything up to the cursor before it disconnected.
		if parsed.compare(from) <= 0 {
			return true
		}
		// Normalized, so "1-0" and "1" match.
		_, ok := replayed[parsed.String()]
		return ok
	}
	return events, skip, nil
}

// replayEntryData returns the payload of a stream entry with its ID set on the EventIDField.
func (p *ProviderAdapter) replayEntryData(msg redis.XMessage) ([]byte, error) {
	raw, ok := msg.Values[p.replay.DataField]
	if !ok {
		return nil, fmt.Errorf("stream entry has no %q field", p.replay.DataField)
	}
	var data []byte
	switch v := raw.(type) {
	case string:
		data = []byte(v)
	case []byte:
		data = v
	default:
		return nil, fmt.Errorf("stream entry field %q is a %T, not a string", p.replay.DataField, raw)
	}
	withID, err := jsonparser.Set(data, strconv.AppendQuote(nil, msg.ID), p.replay.EventIDField)
	if err != nil {
		return nil, fmt.Errorf("event payload is not a JSON object: %w", err)
	}
	return withID, nil
}

// replayClient returns the connection to the Redis holding the streams. Without separate
// replay URLs that is the pub/sub connection. With them it is never the pub/sub connection:
// reading non-existent streams there would silently replay nothing.
func (p *ProviderAdapter) replayClient() redis.UniversalClient {
	if len(p.replay.URLs) > 0 {
		if p.replayConn == nil {
			return nil
		}
		return p.replayConn
	}
	if p.conn == nil {
		return nil
	}
	return p.conn
}
