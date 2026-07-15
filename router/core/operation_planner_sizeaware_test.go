package core

import (
	"testing"

	"github.com/wundergraph/cosmo/router/pkg/config"
	"github.com/wundergraph/cosmo/router/pkg/mondaytweaks"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/plan"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/engine/resolve"
)

// TestEstimatePlanCacheCost verifies the size-aware cost estimate is nil-safe, always
// positive, and monotonically larger for a structurally larger operation — the property the
// size-aware Ristretto config relies on to evict giant aliased-batch plans before hot small
// plans.
func TestEstimatePlanCacheCost(t *testing.T) {
	if got := estimatePlanCacheCost(nil); got != 1 {
		t.Fatalf("nil plan: want cost 1, got %d", got)
	}

	small := &planWithMetaData{operationDocument: &ast.Document{}, content: "query{a}"}
	small.operationDocument.Input.RawBytes = []byte("query{a}")
	small.operationDocument.Fields = make([]ast.Field, 1)
	small.operationDocument.Selections = make([]ast.Selection, 1)

	// Mimics the aliased-batch mutation shape: a large raw body and thousands of AST nodes.
	large := &planWithMetaData{operationDocument: &ast.Document{}, content: "large"}
	large.operationDocument.Input.RawBytes = make([]byte, 100_000)
	large.operationDocument.Fields = make([]ast.Field, 2_000)
	large.operationDocument.Arguments = make([]ast.Argument, 4_000)
	large.operationDocument.Selections = make([]ast.Selection, 2_000)
	large.operationDocument.Values = make([]ast.Value, 4_000)

	cs := estimatePlanCacheCost(small)
	cl := estimatePlanCacheCost(large)
	if cs < 1 {
		t.Fatalf("small plan: want cost >= 1, got %d", cs)
	}
	if cl <= cs {
		t.Fatalf("expected large plan to cost more than small: small=%d large=%d", cs, cl)
	}
}

// TestEstimatePlanCacheCostCountsPlanTree verifies the prepared-plan tree walk dominates the
// estimate: a plan retaining several subgraph fetches and a nested response tree must cost far
// more than the AST-only accounting for the same operation document, so Ristretto evicts by the
// heap the plan tree actually retains rather than by operation size alone.
func TestEstimatePlanCacheCostCountsPlanTree(t *testing.T) {
	doc := &ast.Document{}
	doc.Input.RawBytes = []byte("query{a{b{c}}}")
	doc.Fields = make([]ast.Field, 3)
	doc.Selections = make([]ast.Selection, 3)

	astOnly := &planWithMetaData{operationDocument: doc}
	baseline := estimatePlanCacheCost(astOnly)

	// Two subgraph fetches under a Sequence node (Item != nil ⇒ counted).
	fetches := &resolve.FetchTreeNode{
		Kind: resolve.FetchTreeNodeKindSequence,
		ChildNodes: []*resolve.FetchTreeNode{
			{Kind: resolve.FetchTreeNodeKindSingle, Item: &resolve.FetchItem{}},
			{Kind: resolve.FetchTreeNodeKindSingle, Item: &resolve.FetchItem{}},
		},
	}

	// Nested response shape: root object → array of objects → leaf field (3 Field nodes).
	data := &resolve.Object{
		Fields: []*resolve.Field{
			{
				Name: []byte("a"),
				Value: &resolve.Array{
					Item: &resolve.Object{
						Fields: []*resolve.Field{
							{
								Name: []byte("b"),
								Value: &resolve.Object{
									Fields: []*resolve.Field{
										{Name: []byte("c"), Value: &resolve.String{}},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	withPlan := &planWithMetaData{
		operationDocument: doc,
		preparedPlan: &plan.SynchronousResponsePlan{
			Response: &resolve.GraphQLResponse{Fetches: fetches, Data: data},
		},
	}
	withTree := estimatePlanCacheCost(withPlan)

	if withTree <= baseline {
		t.Fatalf("plan tree walk must increase cost: astOnly=%d withPlan=%d", baseline, withTree)
	}
	// 2 fetches + 3 response fields must be accounted for on top of the AST baseline.
	wantMin := baseline + 2*planCacheCostFetchBytes + 3*planCacheCostFieldBytes
	if withTree < wantMin {
		t.Fatalf("expected cost >= %d (baseline + 2 fetches + 3 fields), got %d", wantMin, withTree)
	}
}

// TestPlanCacheCostRespectsMode confirms a planner uses the historical unit cost when size-
// aware eviction is disabled, and the size-aware estimate when it is enabled.
func TestPlanCacheCostRespectsMode(t *testing.T) {
	p := &planWithMetaData{operationDocument: &ast.Document{}}
	p.operationDocument.Fields = make([]ast.Field, 100)

	countBased := &OperationPlanner{sizeAwarePlanCache: false}
	if got := countBased.planCacheCost(p); got != 1 {
		t.Fatalf("count-based: want cost 1, got %d", got)
	}

	sizeAware := &OperationPlanner{sizeAwarePlanCache: true}
	if got := sizeAware.planCacheCost(p); got <= 1 {
		t.Fatalf("size-aware: want cost > 1, got %d", got)
	}
}

// TestSizeAwarePlanCacheEnabled confirms the per-config DisableSizeAwarePlanCache override
// forces count-based eviction regardless of the mondaytweaks default, and that an unset
// config follows the mondaytweaks default. It reads the global flag but never mutates it, so
// it is safe under -race alongside parallel tests.
func TestSizeAwarePlanCacheEnabled(t *testing.T) {
	if sizeAwarePlanCacheEnabled(config.EngineExecutionConfiguration{DisableSizeAwarePlanCache: true}) {
		t.Fatal("DisableSizeAwarePlanCache must force count-based eviction")
	}
	if got := sizeAwarePlanCacheEnabled(config.EngineExecutionConfiguration{}); got != mondaytweaks.SizeAwarePlanCache.Load() {
		t.Fatalf("unset config should follow mondaytweaks.SizeAwarePlanCache=%v, got %v", mondaytweaks.SizeAwarePlanCache.Load(), got)
	}
}
