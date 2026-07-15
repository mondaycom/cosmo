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

	// DisableFieldDependencies skips the per-fetch CoordinateDependencies allocation during
	// planning. CoordinateDependencies ([]FetchDependency per fetch) are query-plan metadata
	// used only for observability/visualisation — not read during request execution, tainted-
	// entity filtering, or subgraph propagation. With 200 unique cached operations this saves
	// ~30-40% of per-plan heap above the schema baseline (~40 MiB / 200 plans in the
	// cardinality-high benchmark, 500 KiB heap / ~1 MiB RSS per plan).
	//
	// Corresponds to plan.Configuration.DisableIncludeFieldDependencies. The flag is read
	// once in factoryresolver.Load() so it takes effect on the next config reload.
	DisableFieldDependencies atomic.Bool
)

func init() {
	// ShareUpstreamSubscriptionClient and UseNoopUpstreamSubscriptionClientWhenUnused
	// default to false (zero value of atomic.Bool), matching the original const values.
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled.Store(true)
	ExposeOperationSubgraphFetchCountContextField.Store(true)
	AsyncBoundedOldGraphServerShutdown.Store(true)
	PlanCacheSizeAwareBudgetPerSlotBytes.Store(8 * 1024)
	SizeAwarePlanCache.Store(true)
	DisableFieldDependencies.Store(true)
}
