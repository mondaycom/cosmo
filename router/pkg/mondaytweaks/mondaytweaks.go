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
)
