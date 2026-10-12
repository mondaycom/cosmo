package core

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wundergraph/cosmo/router/internal/wsproto"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
	"go.uber.org/zap"
)

func writeSubscriptionMessage(t *testing.T, w resolve.SubscriptionResponseWriter, eventID, data string) {
	t.Helper()
	if eventID != "-" {
		w.(resolve.SubscriptionEventIDWriter).SetEventID(eventID)
	}
	_, err := w.Write([]byte(data))
	require.NoError(t, err)
	require.NoError(t, w.Flush())
}

func newTestHttpFlushWriter(t *testing.T, accept string) (resolve.SubscriptionResponseWriter, *httptest.ResponseRecorder) {
	t.Helper()
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/graphql", nil)
	req.Header.Set("Accept", accept)
	_, writer, ok := GetSubscriptionResponseWriter(resolve.NewContext(context.Background()), req, recorder, false)
	require.True(t, ok)
	return writer, recorder
}

func TestHttpFlushWriter_EventID(t *testing.T) {
	t.Run("SSE sends the event ID as the id field", func(t *testing.T) {
		writer, recorder := newTestHttpFlushWriter(t, sseMimeType)

		writeSubscriptionMessage(t, writer, "1-0", `{"data":{"a":1}}`)
		writeSubscriptionMessage(t, writer, "", `{"data":{"a":2}}`)
		// The ID is only sent with the message it was set for.
		writeSubscriptionMessage(t, writer, "-", `{"data":{"a":3}}`)
		// An ID with a line break would end the field and inject another one.
		writeSubscriptionMessage(t, writer, "4-0\ndata: x", `{"data":{"a":4}}`)

		assert.Equal(t, "event: next\nid: 1-0\ndata: {\"data\":{\"a\":1}}\n\n"+
			"event: next\ndata: {\"data\":{\"a\":2}}\n\n"+
			"event: next\ndata: {\"data\":{\"a\":3}}\n\n"+
			"event: next\ndata: {\"data\":{\"a\":4}}\n\n", recorder.Body.String())
	})

	t.Run("multipart sends the event ID in the response extensions", func(t *testing.T) {
		writer, recorder := newTestHttpFlushWriter(t, multipartMime)

		writeSubscriptionMessage(t, writer, "1-0", `{"data":{"a":1},"extensions":{"x":true}}`)
		writeSubscriptionMessage(t, writer, "-", `{"data":{"a":2}}`)

		body := recorder.Body.String()
		assert.Contains(t, body, `{"payload":{"data":{"a":1},"extensions":{"x":true,"eventId":"1-0"}}}`)
		assert.Contains(t, body, `{"payload":{"data":{"a":2}}}`)
	})
}

type recordingProto struct {
	wsproto.Proto
	payloads []string
}

func (p *recordingProto) WriteGraphQLData(_ string, data json.RawMessage, _ json.RawMessage) error {
	p.payloads = append(p.payloads, string(data))
	return nil
}

func TestWebsocketResponseWriter_EventID(t *testing.T) {
	proto := &recordingProto{}
	writer := newWebsocketResponseWriter("1", proto, true, zap.NewNop(), nil, nil)

	writeSubscriptionMessage(t, writer, "1-0", `{"data":{"a":1}}`)
	writeSubscriptionMessage(t, writer, "-", `{"data":{"a":2}}`)

	assert.Equal(t, []string{
		`{"data":{"a":1},"extensions":{"eventId":"1-0"}}`,
		`{"data":{"a":2}}`,
	}, proto.payloads)
}
