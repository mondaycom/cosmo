package trace

import (
	"context"

	"github.com/wundergraph/cosmo/router/pkg/mondaytweaks"
	"go.opentelemetry.io/otel/attribute"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// semconvDropKeys lists new-semconv attribute keys emitted by otelhttp v0.67.0
// that had no equivalent in the old otelhttp and must be dropped for backward
// compatibility with downstream systems (dashboards, alerts, tracing UIs).
var semconvDropKeys = map[attribute.Key]struct{}{
	"url.path":              {}, // redundant with http.target already set by the router
	"client.address":        {}, // not emitted by old otelhttp; would surface as new http.client_ip
	"network.local.address": {}, // new in otelhttp v0.67.0, no old equivalent
	"network.local.port":    {}, // new in otelhttp v0.67.0, no old equivalent
}

// resourceRedundantDropKeys are deploy-constant wg.* attributes that are ALSO carried on the
// OTEL trace Resource (see NewTracerProvider / router bootstrap). They are dropped from the
// per-span attribute set ONLY when mondaytweaks.DropRedundantSpanAttributes is enabled — the
// exact same flag that adds them to the Resource. Because Datadog and Coralogix surface
// resource attributes as span-level tags under the identical key, dropping them here loses
// zero information downstream while eliminating per-span KeyValue boxing (F2-B).
//
// wg.router.config.version is intentionally NOT here: it changes on config hot-reload, so it
// stays per-span (it cannot live on the once-constructed Resource).
var resourceRedundantDropKeys = map[attribute.Key]struct{}{
	"wg.router.version":      {},
	"wg.router.cluster.name": {},
	"wg.federated_graph.id":  {},
}

// shouldDropAttribute reports whether an attribute key must be filtered out before it reaches
// the underlying span. The original semconv keys are always dropped; the resource-redundant
// wg.* keys are dropped only while the DropRedundantSpanAttributes tweak is enabled, keeping
// flag-off behavior byte-identical to upstream.
func shouldDropAttribute(key attribute.Key) bool {
	if _, drop := semconvDropKeys[key]; drop {
		return true
	}
	if mondaytweaks.DropRedundantSpanAttributes.Load() {
		if _, drop := resourceRedundantDropKeys[key]; drop {
			return true
		}
	}
	return false
}

// FilteringTracerProvider wraps a TracerProvider and returns spans that
// silently drop attributes listed in semconvDropKeys. This covers attributes
// set both at span creation time (via WithAttributes) and afterwards (via
// SetAttributes).
//
// This approach is necessary because:
//   - SpanProcessor.OnStart receives a ReadWriteSpan but SetAttributes only
//     appends — it cannot remove attributes already set via WithAttributes
//     during Start.
//   - SpanProcessor.OnEnd receives a ReadOnlySpan snapshot whose Attributes()
//     slice can be mutated in-place (values/keys) but cannot be resized.
//
// By intercepting at the Tracer.Start level, we filter the WithAttributes
// options before the underlying span is created, so dropped attributes never
// enter the span.
type FilteringTracerProvider struct {
	oteltrace.TracerProvider
}

func (p *FilteringTracerProvider) Tracer(name string, opts ...oteltrace.TracerOption) oteltrace.Tracer {
	return &filteringTracer{Tracer: p.TracerProvider.Tracer(name, opts...)}
}

type filteringTracer struct {
	oteltrace.Tracer
}

// Start filters the WithAttributes options before the underlying span is created, so dropped attributes never enter the span.
// This is a workaround because SpanProcessor.OnStart receives a ReadWriteSpan but SetAttributes only appends — it cannot remove attributes already set via WithAttributes during Start.
// The OTEL SDK does not provide a way to remove attributes from a span after it has been created.
// This is temporary solution to ensure that our metrics are backward compatible for now.
//
// TODO: Remove this once we want to ingest the new attributes.
func (t *filteringTracer) Start(ctx context.Context, name string, opts ...oteltrace.SpanStartOption) (context.Context, oteltrace.Span) {
	// Extract the SpanConfig to inspect attributes set via WithAttributes.
	cfg := oteltrace.NewSpanStartConfig(opts...)
	startAttrs := cfg.Attributes()

	if len(startAttrs) > 0 {
		// Filter out dropped keys and rebuild the options without the
		// original WithAttributes, replacing it with a filtered version.
		filtered := make([]attribute.KeyValue, 0, len(startAttrs))
		for _, a := range startAttrs {
			if !shouldDropAttribute(a.Key) {
				filtered = append(filtered, a)
			}
		}

		// Rebuild from the normalized config so we preserve other start options
		// such as explicit timestamps.
		// We need to do this because the options are abstracted as interfaces and the concrete types are
		// package private in the OTEL SDK. Therefore we cannot access the attributes directly or perform a
		// type switch and assume that `SpanStartEventOption` is always `attributeOption`.
		rebuilt := make([]oteltrace.SpanStartOption, 0, 5)
		if ts := cfg.Timestamp(); !ts.IsZero() {
			rebuilt = append(rebuilt, oteltrace.WithTimestamp(ts))
		}
		if kind := cfg.SpanKind(); kind != oteltrace.SpanKindInternal {
			rebuilt = append(rebuilt, oteltrace.WithSpanKind(kind))
		}
		if cfg.NewRoot() {
			rebuilt = append(rebuilt, oteltrace.WithNewRoot())
		}
		if links := cfg.Links(); len(links) > 0 {
			rebuilt = append(rebuilt, oteltrace.WithLinks(links...))
		}
		if len(filtered) > 0 {
			rebuilt = append(rebuilt, oteltrace.WithAttributes(filtered...))
		}
		opts = rebuilt
	}

	ctx, span := t.Tracer.Start(ctx, name, opts...)
	// We need to wrap the span in a filtering span to filter the attributes before setting them on the span.
	return ctx, &filteringSpan{Span: span}
}

type filteringSpan struct {
	oteltrace.Span
}

// SetAttributes filters the attributes before setting them on the span.
// The name SetAttributes is a bit misleading here because the underlying logic
// actually appends the attributes to the span based on limits. There is no way to override them so we
// need to make sure we filter out attributes that are not allowed for now.
func (s *filteringSpan) SetAttributes(kv ...attribute.KeyValue) {
	n := 0
	for i := range kv {
		if !shouldDropAttribute(kv[i].Key) {
			kv[n] = kv[i]
			n++
		}
	}
	s.Span.SetAttributes(kv[:n]...)
}
