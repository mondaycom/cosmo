package trace

import (
	"context"
	"testing"

	"github.com/wundergraph/cosmo/router/pkg/mondaytweaks"
	"github.com/wundergraph/cosmo/router/pkg/otel"
	"github.com/wundergraph/cosmo/router/pkg/trace/tracetest"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// benchTraceAttrs mimics the shared traceAttrs base set (~11 attrs) that the router appends to
// EVERY span (root + parse/normalize/validate/plan/execute + Engine-Fetch xN + transport xN).
// Three of them (wg.router.version, wg.router.cluster.name, wg.federated_graph.id) are
// deploy-constants that the DropRedundantSpanAttributes tweak carries on the Resource instead,
// so they are filtered out of the per-span set when the flag is on.
func benchTraceAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		otel.WgRouterVersion.String("1.2.3"),                     // resource-redundant (dropped when flag on)
		otel.WgRouterClusterName.String("prod-us-east-1"),        // resource-redundant (dropped when flag on)
		otel.WgFederatedGraphID.String("federated-graph-abc123"), // resource-redundant (dropped when flag on)
		otel.WgRouterConfigVersion.String("cfg-2026-07-10-abc"),  // reload-variable, stays per-span
		otel.WgOperationName.String("MyExpensiveQuery"),
		otel.WgOperationType.String("query"),
		otel.WgOperationHash.String("12345678901234567890"),
		otel.WgClientName.String("monday-web"),
		otel.WgClientVersion.String("2026.7.10"),
		otel.WgOperationProtocol.String("http"),
		otel.WgSubgraphName.String("boards"),
	}
}

// newBenchProvider builds a FilteringTracerProvider over an in-memory exporter so the whole
// span-start + SetAttributes path (including attribute.KeyValue boxing into the SDK span) is
// exercised end-to-end.
func newBenchProvider() *FilteringTracerProvider {
	exporter := tracetest.NewInMemoryExporter(&testing.T{})
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSpanProcessor(&semconvProcessor{}),
	)
	return &FilteringTracerProvider{TracerProvider: tp}
}

// benchmarkFilteringSpan drives the exact path the router uses per span: Start with the base
// attrs via WithAttributes, then a follow-up SetAttributes call, then End.
func benchmarkFilteringSpan(b *testing.B, dropRedundant bool) {
	prev := mondaytweaks.DropRedundantSpanAttributes.Load()
	mondaytweaks.DropRedundantSpanAttributes.Store(dropRedundant)
	b.Cleanup(func() { mondaytweaks.DropRedundantSpanAttributes.Store(prev) })

	provider := newBenchProvider()
	tracer := provider.Tracer("bench")
	ctx := context.Background()
	attrs := benchTraceAttrs()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, span := tracer.Start(ctx, "operation", oteltrace.WithAttributes(attrs...))
		span.SetAttributes(attrs...)
		span.End()
	}
}

// BenchmarkFilteringTracerProvider_FlagOff measures the current (upstream) behavior where the
// redundant wg.* attrs stay boxed on every span.
func BenchmarkFilteringTracerProvider_FlagOff(b *testing.B) {
	benchmarkFilteringSpan(b, false)
}

// BenchmarkFilteringTracerProvider_FlagOn measures the F2-B behavior where the three
// resource-redundant wg.* attrs are dropped from the per-span set.
func BenchmarkFilteringTracerProvider_FlagOn(b *testing.B) {
	benchmarkFilteringSpan(b, true)
}

// benchmarkAttributeBoxing isolates the hot allocation the drop actually removes: the filter
// rebuild in filteringTracer.Start followed by attribute.NewSet, which is how the OTEL SDK
// boxes/de-dupes the WithAttributes options into a span's immutable attribute set. This is the
// per-span cost that scales with the number of attributes, so dropping 3 of 11 shows up here
// directly, without the exporter-snapshot noise of the full-span benchmark.
func benchmarkAttributeBoxing(b *testing.B, dropRedundant bool) {
	prev := mondaytweaks.DropRedundantSpanAttributes.Load()
	mondaytweaks.DropRedundantSpanAttributes.Store(dropRedundant)
	b.Cleanup(func() { mondaytweaks.DropRedundantSpanAttributes.Store(prev) })

	attrs := benchTraceAttrs()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		filtered := make([]attribute.KeyValue, 0, len(attrs))
		for _, a := range attrs {
			if !shouldDropAttribute(a.Key) {
				filtered = append(filtered, a)
			}
		}
		set := attribute.NewSet(filtered...)
		_ = set.Len()
	}
}

func BenchmarkAttributeBoxing_FlagOff(b *testing.B) {
	benchmarkAttributeBoxing(b, false)
}

func BenchmarkAttributeBoxing_FlagOn(b *testing.B) {
	benchmarkAttributeBoxing(b, true)
}
