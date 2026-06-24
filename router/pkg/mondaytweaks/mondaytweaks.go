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
)
