package core

import (
	"errors"
	"strconv"
	"time"

	"golang.org/x/sync/singleflight"

	graphqlmetricsv1 "github.com/wundergraph/cosmo/router/gen/proto/wg/cosmo/graphqlmetrics/v1"
	"github.com/wundergraph/cosmo/router/pkg/config"
	"github.com/wundergraph/cosmo/router/pkg/graphqlschemausage"
	"github.com/wundergraph/cosmo/router/pkg/mondaytweaks"
	"github.com/wundergraph/cosmo/router/pkg/slowplancache"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/astparser"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/postprocess"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"

)

type planWithMetaData struct {
	preparedPlan       plan.Plan
	operationDocument  *ast.Document
	typeFieldUsageInfo []*graphqlschemausage.TypeFieldUsageInfo
	argumentUsageInfo                 []*graphqlmetricsv1.ArgumentUsageInfo
	content                           string
	operationName                     string
	planningDuration                  time.Duration
}

// planCacheCostNodeBytes and planCacheCostUsageBytes approximate the average retained heap
// of a single AST structural element and a single usage-info entry. Ristretto cost is
// relative to MaxCost, so the constants only need to preserve ordering across cache entries;
// they are deliberately coarse and cheap to compute.
const (
	planCacheCostNodeBytes  = 48
	planCacheCostUsageBytes = 64
)

// estimatePlanCacheCost approximates the retained heap of a cached plan entry so the
// size-aware Ristretto config (mondaytweaks.SizeAwarePlanCache) evicts by memory footprint
// instead of by entry count. It keys off operationDocument, which is always populated (the
// content string is only set when the slow-plan cache is enabled), summing the raw operation
// bytes and the lengths of the operation-side AST slices — both of which scale with operation
// complexity and therefore with the size of the prepared plan tree the entry retains. The
// estimate is intentionally an O(number-of-slices) field read, not a deep walk.
func estimatePlanCacheCost(p *planWithMetaData) int64 {
	if p == nil {
		return 1
	}
	cost := int64(len(p.content) + len(p.operationName))
	if d := p.operationDocument; d != nil {
		cost += int64(len(d.Input.RawBytes) + len(d.Input.Variables))
		nodes := len(d.RootNodes) + len(d.Arguments) + len(d.Values) +
			len(d.Selections) + len(d.SelectionSets) + len(d.Fields) +
			len(d.ObjectFields) + len(d.ObjectValues) + len(d.ListValues) +
			len(d.VariableValues) + len(d.StringValues) + len(d.IntValues) +
			len(d.FloatValues) + len(d.EnumValues) + len(d.InlineFragments) +
			len(d.FragmentSpreads) + len(d.VariableDefinitions) + len(d.Directives)
		cost += int64(nodes) * planCacheCostNodeBytes
	}
	cost += int64(len(p.typeFieldUsageInfo)+len(p.argumentUsageInfo)) * planCacheCostUsageBytes
	if cost < 1 {
		return 1
	}
	return cost
}

// sizeAwarePlanCacheEnabled reports whether the execution-plan cache should evict by estimated
// retained heap (mondaytweaks.SizeAwarePlanCache) for this engine configuration. The per-config
// DisableSizeAwarePlanCache override forces count-based eviction (tests, or a targeted
// per-router rollback) without mutating the global flag, which matters under -race.
func sizeAwarePlanCacheEnabled(cfg config.EngineExecutionConfiguration) bool {
	return mondaytweaks.SizeAwarePlanCache.Load() && !cfg.DisableSizeAwarePlanCache
}

// planCacheCost returns the Ristretto cost for a plan-cache entry: the size-aware estimate
// when size-aware eviction is enabled for this planner, or the historical unit cost of 1. The
// MaxCost configured in buildOperationCaches must use the same decision so cost and budget
// agree.
func (op *OperationPlanner) planCacheCost(p *planWithMetaData) int64 {
	if op.sizeAwarePlanCache {
		return estimatePlanCacheCost(p)
	}
	return 1
}

type OperationPlanner struct {
	sf             singleflight.Group
	planCache      ExecutionPlanCache[uint64, *planWithMetaData]
	slowPlanCache  *slowplancache.Cache[*planWithMetaData]
	executor       *Executor
	trackUsageInfo bool

	// planningDurationOverride, when set, replaces the measured planning duration.
	// This is used in tests to simulate slow queries.
	planningDurationOverride func(content string) time.Duration

	// sizeAwarePlanCache mirrors the plan cache's eviction mode: when true, plan-cache Set
	// costs are the estimated retained heap (matching the byte budget MaxCost); when false,
	// the historical unit cost of 1 (count-based). Kept per-planner so it agrees with the
	// cache built for the same engine configuration.
	sizeAwarePlanCache bool
}

type operationPlannerOpts struct {
	operationContent bool
}

type ExecutionPlanCache[K any, V any] interface {
	// Get the value from the cache
	Get(key K) (V, bool)
	// Set the value in the cache with a cost. The cost depends on the cache implementation
	Set(key K, value V, cost int64) bool
	// Iterate over all items in the cache (non-deterministic)
	IterValues(cb func(v V) (stop bool))
	// Close the cache and free resources
	Close()
}

func NewOperationPlanner(
	executor *Executor,
	planCache ExecutionPlanCache[uint64, *planWithMetaData],
	fallbackCache *slowplancache.Cache[*planWithMetaData],
	planningDurationOverride func(content string) time.Duration,
	sizeAwarePlanCache bool,
) *OperationPlanner {
	return &OperationPlanner{
		planCache:                planCache,
		executor:                 executor,
		trackUsageInfo:           executor.TrackUsageInfo,
		slowPlanCache:            fallbackCache,
		planningDurationOverride: planningDurationOverride,
		sizeAwarePlanCache:       sizeAwarePlanCache,
	}
}

// planOperation performs the core planning work: parse, plan, and postprocess.
func (p *OperationPlanner) planOperation(content string, name string, includeQueryPlan bool) (*planWithMetaData, error) {
	doc, report := astparser.ParseGraphqlDocumentString(content)
	if report.HasErrors() {
		return nil, &reportError{report: &report}
	}

	planner, err := plan.NewPlanner(p.executor.PlanConfig)
	if err != nil {
		return nil, err
	}

	var (
		plannerOptions []plan.Opts
	)

	if includeQueryPlan {
		plannerOptions = append(plannerOptions, plan.IncludeQueryPlanInResponse())
	}

	// create the raw query plan
	// Note: planning uses the router schema as it needs access to all fields (including inaccessible), validation/introspection uses client schema
	preparedPlan := planner.Plan(&doc, p.executor.RouterSchema, name, &report, plannerOptions...)
	if report.HasErrors() {
		return nil, &reportError{report: &report}
	}

	// postprocess query plan to get its final state
	post := postprocess.NewProcessor(postprocess.CollectDataSourceInfo())
	post.Process(preparedPlan)

	return &planWithMetaData{
		preparedPlan:      preparedPlan,
		operationDocument: &doc,
	}, nil
}

func (p *OperationPlanner) preparePlan(ctx *operationContext, opts operationPlannerOpts) (*planWithMetaData, error) {
	out, err := p.planOperation(ctx.content, ctx.name, ctx.executionOptions.IncludeQueryPlanInResponse)
	if err != nil {
		return nil, err
	}

	out.operationName = ctx.name

	if opts.operationContent {
		out.content = ctx.Content()
	}

	if p.trackUsageInfo {
		out.typeFieldUsageInfo = graphqlschemausage.GetTypeFieldUsageInfo(out.preparedPlan)
		out.argumentUsageInfo, err = graphqlschemausage.GetArgumentUsageInfo(out.operationDocument, p.executor.RouterSchema, ctx.variables, out.preparedPlan, ctx.remapVariables)
		if err != nil {
			return nil, err
		}
	}

	return out, nil
}

type PlanOptions struct {
	ClientInfo           *ClientInfo
	TraceOptions         resolve.TraceOptions
	ExecutionOptions     resolve.ExecutionOptions
	TrackSchemaUsageInfo bool
}

func (p *OperationPlanner) plan(opContext *operationContext, options PlanOptions) (err error) {
	// if we have tracing enabled or want to include a query plan in the response we always prepare a new plan
	// this is because in case of tracing, we're writing trace data to the plan
	// in case of including the query plan, we don't want to cache this additional overhead

	skipCache := options.TraceOptions.Enable || options.ExecutionOptions.IncludeQueryPlanInResponse

	// Store plan config regardless of cache to enable costs calculation.
	opContext.planConfig = p.executor.PlanConfig

	if skipCache {
		prepared, err := p.preparePlan(opContext, operationPlannerOpts{operationContent: false})
		if err != nil {
			return err
		}
		opContext.preparedPlan = prepared
		if options.TrackSchemaUsageInfo {
			opContext.typeFieldUsageInfo = prepared.typeFieldUsageInfo
			opContext.argumentUsageInfo = prepared.argumentUsageInfo
			opContext.inputUsageInfo, err = graphqlschemausage.GetInputUsageInfo(prepared.operationDocument, p.executor.RouterSchema, opContext.variables, prepared.preparedPlan, opContext.remapVariables)
			if err != nil {
				return err
			}
		}
		return nil
	}

	operationID := opContext.internalHash
	// try to get a prepared plan for this operation ID from the cache
	cachedPlan, ok := p.planCache.Get(operationID)
	if ok && cachedPlan != nil {
		// re-use a prepared plan from the main cache
		opContext.preparedPlan = cachedPlan
		opContext.planCacheHit = true
	} else if p.slowPlanCache != nil {
		if cachedPlan, ok = p.slowPlanCache.Get(operationID); ok {
			// found in the plan fallback cache — re-use and re-insert into main cache
			opContext.preparedPlan = cachedPlan
			opContext.planCacheHit = true
			p.planCache.Set(operationID, cachedPlan, p.planCacheCost(cachedPlan))
		}
	}

	if opContext.preparedPlan == nil {
		// prepare a new plan using single flight
		// this ensures that we only prepare the plan once for this operation ID
		operationIDStr := strconv.FormatUint(operationID, 10)
		sharedPreparedPlan, err, _ := p.sf.Do(operationIDStr, func() (interface{}, error) {
			start := time.Now()
			prepared, err := p.preparePlan(opContext, operationPlannerOpts{operationContent: p.slowPlanCache != nil})
			if err != nil {
				return nil, err
			}
			prepared.planningDuration = time.Since(start)

			// This is only used for test cases
			if p.planningDurationOverride != nil {
				prepared.planningDuration = p.planningDurationOverride(prepared.content)
			}

			// Set into the main cache after planningDuration is finalized,
			// because the OnEvict callback reads planningDuration concurrently.
			p.planCache.Set(operationID, prepared, p.planCacheCost(prepared))
			p.slowPlanCache.Set(operationID, prepared, prepared.planningDuration)

			return prepared, nil
		})
		if err != nil {
			return err
		}
		opContext.preparedPlan, ok = sharedPreparedPlan.(*planWithMetaData)
		if !ok {
			return errors.New("unexpected prepared plan type")
		}
	}
	if options.TrackSchemaUsageInfo {
		opContext.typeFieldUsageInfo = opContext.preparedPlan.typeFieldUsageInfo
		opContext.argumentUsageInfo = opContext.preparedPlan.argumentUsageInfo
		opContext.inputUsageInfo, err = graphqlschemausage.GetInputUsageInfo(opContext.preparedPlan.operationDocument, p.executor.RouterSchema, opContext.variables, opContext.preparedPlan.preparedPlan, opContext.remapVariables)
		if err != nil {
			return err
		}
	}

	return nil
}
