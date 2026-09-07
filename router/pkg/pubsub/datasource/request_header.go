package datasource

import (
	"context"
	"net/http"
)

type requestHeaderContextKey struct{}

// WithRequestHeader attaches the header set the resolver built for this subscription
// to the context handed to Adapter.Subscribe. The header set is the result of the
// subgraph header propagation rules, so it only contains headers the router was
// explicitly configured to forward.
//
// Adapters that authenticate per subscriber (Pusher) read the credential from here.
// Adapters that authenticate once per provider (NATS, Kafka, Redis) ignore it.
func WithRequestHeader(ctx context.Context, header http.Header) context.Context {
	if header == nil {
		return ctx
	}
	return context.WithValue(ctx, requestHeaderContextKey{}, header)
}

// RequestHeaderFromContext returns the propagated request header, or nil when the
// context carries none.
func RequestHeaderFromContext(ctx context.Context) http.Header {
	header, _ := ctx.Value(requestHeaderContextKey{}).(http.Header)
	return header
}
