// Package mondaytweaks defines compile-time feature flags for monday.com-specific
// behavioural overrides in the cosmo router. Keep only non-memory-leak behavior
// and performance toggles here; memory-reload cleanup notes live in
// `wiki/reference/cosmo-router-reload-memory-benchmark-tooling`.
package mondaytweaks

const (
	// ShareUpstreamSubscriptionClient uses one upstream GraphQLSubscriptionClient per
	// DefaultFactoryResolver instead of one per subgraph factory (behavior-altering).
	// Disabled: suspected of interfering with CDN config hot reload (subscription-client
	// lifecycle across reloads). Reverts to upstream default (one client per factory).
	ShareUpstreamSubscriptionClient = false

	// UseNoopUpstreamSubscriptionClientWhenUnused skips upstream WS/SSE transport init
	// when subscriptions are not used (behavior-altering).
	// Disabled: suspected of interfering with CDN config hot reload (stale noop client
	// after a reload that newly requires subscriptions). Reverts to upstream default.
	UseNoopUpstreamSubscriptionClientWhenUnused = false

	// DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled sets PingInterval=0 on
	// upstream subscription clients when client-facing websocket is disabled.
	// Re-enabled: client-facing websockets are disabled in prod (websocket.enabled: false),
	// yet upstream subscription clients still run ping loops. A goroutine profile showed
	// WSTransport.pingLoop at ~65% of all goroutines (1.5M) accumulating across reloads;
	// zeroing PingInterval when client WS is disabled stops that leak.
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled = true

	// ExposeOperationSubgraphFetchCountContextField enables the
	// operation_subgraph_fetch_count access-log context field.
	ExposeOperationSubgraphFetchCountContextField = true

	// AsyncBoundedOldGraphServerShutdown runs the previous graph server's Shutdown OFF the
	// config-reload goroutine, with a bounded in-flight drain. The graph-server swap is
	// synchronous on the config poller, so a slow/stuck in-flight request draining on the
	// old server freezes CDN config hot-reload (ticket #3286, observed >1h) and pins the old
	// generation's schema + caches in memory (GC pressure). Detaching + bounding the drain
	// (by the configured grace_period) lets reloads proceed and releases the old generation.
	AsyncBoundedOldGraphServerShutdown = true

	// PlanCacheSizeAwareBudgetPerSlotBytes is the per-configured-slot byte budget used to
	// derive the size-aware execution-plan-cache MaxCost when SizeAwarePlanCache is enabled.
	PlanCacheSizeAwareBudgetPerSlotBytes int64 = 8 * 1024
)

var (
	// SizeAwarePlanCache — monday perf tweak (#7 OPEN); not an upstream memory-leak fix.
	SizeAwarePlanCache = true
)
