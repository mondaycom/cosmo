// Package mondaytweaks defines runtime-configurable feature flags for monday.com-specific
// behavioural overrides in the cosmo router. Keep only non-memory-leak behavior
// and performance toggles here; memory-reload cleanup notes live in
// `wiki/reference/cosmo-router-reload-memory-benchmark-tooling`.
//
// All flags are backed by sync/atomic so they are safe to read from concurrent
// request-handling goroutines and to write from the ignite provisioning goroutine.
// Use Flag.Store(v) to change a value (e.g. from an ignite module at boot) and
// Flag.Load() in production code paths.
package mondaytweaks

import "sync/atomic"

var (
	// ShareUpstreamSubscriptionClient uses one upstream GraphQLSubscriptionClient per
	// DefaultFactoryResolver instead of one per subgraph factory (behavior-altering).
	// Disabled: suspected of interfering with CDN config hot reload (subscription-client
	// lifecycle across reloads). Reverts to upstream default (one client per factory).
	ShareUpstreamSubscriptionClient atomic.Bool

	// UseNoopUpstreamSubscriptionClientWhenUnused skips upstream WS/SSE transport init
	// when subscriptions are not used (behavior-altering).
	// Disabled: suspected of interfering with CDN config hot reload (stale noop client
	// after a reload that newly requires subscriptions). Reverts to upstream default.
	UseNoopUpstreamSubscriptionClientWhenUnused atomic.Bool

	// DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled sets PingInterval=0 on
	// upstream subscription clients when client-facing websocket is disabled.
	// Re-enabled: client-facing websockets are disabled in prod (websocket.enabled: false),
	// yet upstream subscription clients still run ping loops. A goroutine profile showed
	// WSTransport.pingLoop at ~65% of all goroutines (1.5M) accumulating across reloads;
	// zeroing PingInterval when client WS is disabled stops that leak.
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled atomic.Bool

	// ExposeOperationSubgraphFetchCountContextField enables the
	// operation_subgraph_fetch_count access-log context field.
	ExposeOperationSubgraphFetchCountContextField atomic.Bool

	// AsyncBoundedOldGraphServerShutdown runs the previous graph server's Shutdown OFF the
	// config-reload goroutine, with a bounded in-flight drain. The graph-server swap is
	// synchronous on the config poller, so a slow/stuck in-flight request draining on the
	// old server freezes CDN config hot-reload (ticket #3286, observed >1h) and pins the old
	// generation's schema + caches in memory (GC pressure). Detaching + bounding the drain
	// (by the configured grace_period) lets reloads proceed and releases the old generation.
	AsyncBoundedOldGraphServerShutdown atomic.Bool

	// PlanCacheSizeAwareBudgetPerSlotBytes is the per-configured-slot byte budget used to
	// derive the size-aware execution-plan-cache MaxCost when SizeAwarePlanCache is enabled.
	PlanCacheSizeAwareBudgetPerSlotBytes atomic.Int64

	// SizeAwarePlanCache — monday perf tweak (#7 OPEN); not an upstream memory-leak fix.
	SizeAwarePlanCache atomic.Bool

	// DropRedundantSpanAttributes stops boxing the three deploy-constant wg.* attributes
	// (wg.router.version, wg.router.cluster.name, wg.federated_graph.id) onto every span,
	// because they are instead carried once on the OTEL trace Resource.
	//
	// Motivation (prod pprof, F2-B): the shared traceAttrs base set (~11 attrs) is appended
	// to EVERY span — root + parse/normalize/validate/plan/execute + Engine-Fetch xN subgraphs
	// + transport xN. attribute.KeyValue boxing of those redundant static attrs accounted for
	// ~30GB alloc / 90s in production. Three of them are deploy-constants known at
	// tracer-provider startup, so they belong on the Resource, which every exporter attaches
	// to every span implicitly. Datadog and Coralogix surface resource attributes as
	// span-level tags under the same key, so dropping them from the per-span attribute set
	// loses zero information downstream while eliminating the per-span boxing.
	//
	// When true (default): the three attrs are added to the trace Resource at provider
	// construction AND dropped from per-span WithAttributes/SetAttributes via
	// FilteringTracerProvider.
	// When false: byte-identical to upstream — the attrs stay per-span and are NOT added to
	// the Resource (no duplication).
	//
	// wg.router.config.version is deliberately excluded: it changes on every CDN config
	// hot-reload, so it cannot live on the once-constructed Resource and must remain per-span.
	DropRedundantSpanAttributes atomic.Bool
)

func init() {
	// ShareUpstreamSubscriptionClient and UseNoopUpstreamSubscriptionClientWhenUnused
	// default to false (zero value of atomic.Bool), matching the original const values.
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled.Store(true)
	ExposeOperationSubgraphFetchCountContextField.Store(true)
	AsyncBoundedOldGraphServerShutdown.Store(true)
	PlanCacheSizeAwareBudgetPerSlotBytes.Store(8 * 1024)
	SizeAwarePlanCache.Store(true)
	DropRedundantSpanAttributes.Store(true)
}
