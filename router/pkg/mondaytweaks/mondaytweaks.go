// Package mondaytweaks defines compile-time feature flags for monday.com-specific
// behavioural overrides in the cosmo router. Keep only non-memory-leak behavior
// and performance toggles here; memory-reload cleanup notes live in
// `wiki/reference/cosmo-router-reload-memory-benchmark-tooling`.
package mondaytweaks

const (
	// ShareUpstreamSubscriptionClient uses one upstream GraphQLSubscriptionClient per
	// DefaultFactoryResolver instead of one per subgraph factory (behavior-altering).
	ShareUpstreamSubscriptionClient = true

	// UseNoopUpstreamSubscriptionClientWhenUnused skips upstream WS/SSE transport init
	// when subscriptions are not used (behavior-altering).
	UseNoopUpstreamSubscriptionClientWhenUnused = true

	// DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled sets PingInterval=0 on
	// upstream subscription clients when client-facing websocket is disabled.
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled = true

	// ExposeOperationSubgraphFetchCountContextField enables the
	// operation_subgraph_fetch_count access-log context field.
	ExposeOperationSubgraphFetchCountContextField = true

	// PlanCacheSizeAwareBudgetPerSlotBytes is the per-configured-slot byte budget used to
	// derive the size-aware execution-plan-cache MaxCost when SizeAwarePlanCache is enabled.
	PlanCacheSizeAwareBudgetPerSlotBytes int64 = 8 * 1024
)

var (
	// SizeAwarePlanCache — monday perf tweak (#7 OPEN); not an upstream memory-leak fix.
	SizeAwarePlanCache = true
)
