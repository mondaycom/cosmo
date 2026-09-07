package pusherclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"

	"go.uber.org/zap"
)

// Subscription is a single consumer of one channel. Several subscriptions can
// share a channel; the underlying Pusher subscription is created once and
// removed when the last subscription is closed.
type Subscription struct {
	client  *Client
	channel string
	events  chan Event

	closeOnce sync.Once
}

// Channel returns the subscribed channel name.
func (s *Subscription) Channel() string {
	return s.channel
}

// Events returns the stream of events. The channel is closed on Unsubscribe.
func (s *Subscription) Events() <-chan Event {
	return s.events
}

// deliver hands an event to the consumer without blocking the read loop. A slow
// consumer loses events rather than stalling every other channel on the socket.
func (s *Subscription) deliver(evt Event, logger *zap.Logger) {
	select {
	case s.events <- evt:
	default:
		logger.Warn("dropping pusher event because the subscriber is not keeping up",
			zap.String("channel", evt.Channel), zap.String("event", evt.Name))
	}
}

// Unsubscribe removes this consumer. When it was the last one for the channel, a
// pusher:unsubscribe is sent.
func (s *Subscription) Unsubscribe() {
	s.closeOnce.Do(func() {
		last := s.client.removeSubscription(s)
		if last {
			s.client.sendUnsubscribe(s.channel)
		}
		close(s.events)
	})
}

// Subscribe registers a consumer for the given channel. It returns as soon as
// the subscribe frame has been sent; the subscription confirmation is handled
// asynchronously, and a rejection is logged.
func (c *Client) Subscribe(ctx context.Context, channel string) (*Subscription, error) {
	if channel == "" {
		return nil, errors.New("pusher: channel must not be empty")
	}

	sub := &Subscription{
		client:  c,
		channel: channel,
		events:  make(chan Event, c.opts.EventBufferSize),
	}

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, errors.New("pusher: client is closed")
	}
	first := len(c.subs[channel]) == 0
	c.subs[channel] = append(c.subs[channel], sub)
	sess := c.session
	c.mu.Unlock()

	// Only the first subscription for a channel has to talk to the server. If
	// there is no live session, the supervisor subscribes on the next connect.
	if first && sess != nil {
		if err := c.sendSubscribe(ctx, sess, channel); err != nil {
			c.removeSubscription(sub)
			return nil, fmt.Errorf("pusher: could not subscribe to channel %q: %w", channel, err)
		}
	}

	return sub, nil
}

// removeSubscription drops the subscription and reports whether the channel has
// no consumers left.
func (c *Client) removeSubscription(sub *Subscription) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	subs := c.subs[sub.channel]
	if idx := slices.Index(subs, sub); idx >= 0 {
		subs = slices.Delete(subs, idx, idx+1)
	}
	if len(subs) == 0 {
		delete(c.subs, sub.channel)
		return true
	}
	c.subs[sub.channel] = subs

	return false
}

func (c *Client) sendUnsubscribe(channel string) {
	c.mu.Lock()
	sess := c.session
	c.mu.Unlock()
	if sess == nil {
		return
	}

	encoded, err := json.Marshal(map[string]string{"channel": channel})
	if err != nil {
		return
	}
	if err := sess.send(frame{Event: eventUnsubscribe, Data: encoded}); err != nil {
		c.logger.Debug("could not send pusher unsubscribe", zap.String("channel", channel), zap.Error(err))
	}
}
