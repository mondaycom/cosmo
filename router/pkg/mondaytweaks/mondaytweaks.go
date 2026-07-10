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

	// CacheMetricAttributeExcludeDecisions memoizes per-attribute-key exclude decisions in
	// the OTEL and Prometheus metric attribute-filter closures. In production profiling
	// (74.7s / 4.4% CPU) the regex loop runs on EVERY attribute of EVERY measurement.
	// Attribute keys are a small, bounded, static set of strings (http.method, wg.operation.name,
	// etc.), so computing the regex result once per distinct key and storing it in a sync.Map
	// reduces the hot path to a single map lookup — O(1) with zero regex calls and zero allocs
	// after warmup. Semantics are identical to the original path: the same regexes run on the
	// same input; only the result is cached. On the Prometheus path the isKeyInSlice +
	// SanitizeName steps are preserved inside the cache-miss branch so the final cached decision
	// is always correct. Enabled by default.
	CacheMetricAttributeExcludeDecisions atomic.Bool
)

func init() {
	// ShareUpstreamSubscriptionClient and UseNoopUpstreamSubscriptionClientWhenUnused
	// default to false (zero value of atomic.Bool), matching the original const values.
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled.Store(true)
	ExposeOperationSubgraphFetchCountContextField.Store(true)
	AsyncBoundedOldGraphServerShutdown.Store(true)
	PlanCacheSizeAwareBudgetPerSlotBytes.Store(8 * 1024)
	SizeAwarePlanCache.Store(true)
	CacheMetricAttributeExcludeDecisions.Store(true)
}
