package core

import (
	"testing"

	"github.com/wundergraph/cosmo/router/pkg/config"
	"github.com/wundergraph/cosmo/router/pkg/mondaytweaks"
	"github.com/wundergraph/graphql-go-tools/v2/pkg/ast"
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
