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
	// PlanCacheCostCountsPlanTree enables the fetch-tree + response-field walk in the
	// execution-plan-cache cost estimator (estimatePlanCacheCost). When enabled, the estimate
	// adds ~32 KiB per subgraph fetch and ~768 B per response field on top of the AST-only
	// accounting, so the size-aware cache evicts by the heap the prepared plan tree actually
	// retains rather than by operation-document size alone. Only has an effect when
	// SizeAwarePlanCache is enabled.
	PlanCacheCostCountsPlanTree atomic.Bool

)

func init() {
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled.Store(true)
	ExposeOperationSubgraphFetchCountContextField.Store(true)
	PlanCacheSizeAwareBudgetPerSlotBytes.Store(8 * 1024)
	SizeAwarePlanCache.Store(true)
	DisableFieldDependencies.Store(true)
	PlanCacheCostCountsPlanTree.Store(true)

}
