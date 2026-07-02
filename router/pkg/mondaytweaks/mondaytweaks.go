// Package mondaytweaks defines compile-time feature flags for monday.com-specific
// behavioural overrides in the cosmo router. All monday-specific toggles live in
// one place so they are easy to audit and remove when upstreamed.
package mondaytweaks

const (
	// ClearSlowPlanCacheOnClose makes slowplancache.Close() clear all entries from
	// the sync.Map immediately, releasing references to cached values (including
	// *ast.Document schema pointers). Without this, entries survive until the Cache
	// struct itself is GC'd — which may be delayed by goroutines still referencing
	// the owning graphMux — causing ~200-300 MB of retained memory per config reload.
	ClearSlowPlanCacheOnClose = true

	// OmitSchemaDocumentFromCachedPlans removes the unused schemaDocument field from
	// planWithMetaData (compile-time structural change in operation_planner.go).
	OmitSchemaDocumentFromCachedPlans = true

	// CallOnRouterConfigReloadOnHotReload invokes ReloadPersistentState.OnRouterConfigReload
	// at the start of Router.newServer(), matching the supervisor restart path.
	CallOnRouterConfigReloadOnHotReload = true

	// SkipPlanCacheOnEvictDuringMuxShutdown disables ristretto OnEvict migration into
	// slowplancache while a graphMux is shutting down intentionally.
	SkipPlanCacheOnEvictDuringMuxShutdown = true

	// DrainWebsocketSubscriptionsBeforeCacheClose closes client websocket subscriptions
	// synchronously before plan caches are torn down on graphMux shutdown.
	DrainWebsocketSubscriptionsBeforeCacheClose = true

	// CloseExecutorOnGraphMuxShutdown nils federation schema refs held by Executor after
	// graphMux drain, allowing the old graph generation to be garbage-collected.
	CloseExecutorOnGraphMuxShutdown = true

	// NilGraphMuxCachesOnShutdown closes and nils Ristretto caches on shut-down graphMux,
	// drops wsHandler/mux references, and removes the mux from graphMuxList.
	NilGraphMuxCachesOnShutdown = true

	// ResetExecutionConfigProtoOnReload proto.Resets the previous staticExecutionConfig
	// after a successful manifest reload so decoded protojson strings can be collected.
	ResetExecutionConfigProtoOnReload = true

	// SkipManifestReloadWhenMapperUnchanged skips manifest watcher reload when mapper.json
	// bytes are unchanged. Disabled: latest.json / feature-flag files can change without
	// mapper.json changing, which would serve stale config.
	SkipManifestReloadWhenMapperUnchanged = false

	// ShareUpstreamSubscriptionClient uses one upstream GraphQLSubscriptionClient per
	// DefaultFactoryResolver instead of one per subgraph factory (behavior-altering).
	ShareUpstreamSubscriptionClient = true

	// UseNoopUpstreamSubscriptionClientWhenUnused skips upstream WS/SSE transport init
	// when subscriptions are not used (behavior-altering).
	UseNoopUpstreamSubscriptionClientWhenUnused = true

	// DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled sets PingInterval=0 on
	// upstream subscription clients when client-facing websocket is disabled.
	DisableUpstreamSubscriptionPingWhenClientWebSocketDisabled = true

	// PlanCacheSizeAwareBudgetPerSlotBytes is the per-configured-slot byte budget used to
	// derive the size-aware execution-plan-cache MaxCost when SizeAwarePlanCache is enabled.
	// The Ristretto MaxCost becomes ExecutionPlanCacheSize * this value (bytes), and each
	// entry is charged its estimated retained heap (see estimatePlanCacheCost). With the
	// historical count-based config a single giant aliased-batch plan occupied one of N
	// slots regardless of its true size, so a burst of structurally-unique giant plans (US
	// cluster group 02) could pin far more heap than the operator budgeted for. 8 KiB/slot
	// keeps normal-traffic capacity roughly unchanged (typical plans estimate well under
	// this) while charging a 200 KB+ giant plan tens of slots, and — crucially — bounds the
	// total plan-cache heap to a predictable ceiling instead of (entry count x worst case).
	PlanCacheSizeAwareBudgetPerSlotBytes int64 = 8 * 1024
)

var (
	// SizeAwarePlanCache switches the execution-plan Ristretto cache from count-based
	// eviction (every entry costs 1, MaxCost = ExecutionPlanCacheSize) to size-aware
	// eviction (each entry costs its estimated retained heap, MaxCost =
	// ExecutionPlanCacheSize * PlanCacheSizeAwareBudgetPerSlotBytes). This targets the RSS
	// gap on US cluster group 02, where structurally-unique aliased-batch mutation plans are
	// far larger than typical plans yet, under count-based eviction, could evict thousands of
	// small hot plans while collectively pinning most of the heap.
	//
	// Unlike the behaviour-preserving fixes above, this materially changes cache eviction
	// semantics and the plan-cache heap ceiling for every request, so it defaults OFF and is
	// intended to be enabled as a per-cluster canary (start with US cluster group 02) rather
	// than flipped on globally. It is a var so tests can exercise both cache configurations.
	// When false the original count-based path runs unchanged.
	SizeAwarePlanCache = true
)
