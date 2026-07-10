package metric

import (
	"regexp"
	"sync"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

// benchAttrKeys is a realistic set of ~15 attribute keys seen on cosmo-router metrics.
var benchAttrKeys = []attribute.KeyValue{
	attribute.String("http.method", "POST"),
	attribute.String("http.status_code", "200"),
	attribute.String("http.target", "/graphql"),
	attribute.String("http.scheme", "https"),
	attribute.String("net.host.name", "api.example.com"),
	attribute.String("net.host.port", "443"),
	attribute.String("wg.operation.name", "GetUser"),
	attribute.String("wg.operation.type", "query"),
	attribute.String("wg.client.name", "web"),
	attribute.String("wg.client.version", "1.0.0"),
	attribute.String("wg.federated.graph.id", "abc123"),
	attribute.String("wg.router.config.version", "v42"),
	attribute.String("wg.subgraph.name", "users"),
	attribute.String("wg.subgraph.id", "sub-001"),
	attribute.String("service.name", "cosmo-router"),
}

// benchExcludePatterns mimics a realistic exclude-metric-labels config (~3 regexes).
var benchExcludePatterns = []*regexp.Regexp{
	regexp.MustCompile(`^http\..*`),
	regexp.MustCompile(`^net\..*`),
	regexp.MustCompile(`^service\..*`),
}

// BenchmarkAttributeExcludeFilterOld benchmarks the original regex-loop closure (no caching).
// Every call iterates all regexes for every key — O(keys * regexes) with allocs from
// string(value.Key) conversions.
func BenchmarkAttributeExcludeFilterOld(b *testing.B) {
	regexes := benchExcludePatterns
	keys := benchAttrKeys

	// Build the original closure exactly as in defaultOtlpMetricOptions when flag is off.
	attributeFilter := func(value attribute.KeyValue) bool {
		for _, re := range regexes {
			if re.MatchString(string(value.Key)) {
				return false
			}
		}
		return true
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, kv := range keys {
			_ = attributeFilter(kv)
		}
	}
}

// BenchmarkAttributeExcludeFilterNew benchmarks the cached closure (flag on).
// After the first pass all decisions are in sync.Map; subsequent calls are a single
// map lookup — O(1), zero regex calls, near-zero allocs.
func BenchmarkAttributeExcludeFilterNew(b *testing.B) {
	regexes := benchExcludePatterns
	keys := benchAttrKeys

	// Build the cached closure exactly as in defaultOtlpMetricOptions when flag is on.
	var cache sync.Map
	attributeFilter := func(value attribute.KeyValue) bool {
		if v, ok := cache.Load(value.Key); ok {
			return v.(bool)
		}
		result := true
		for _, re := range regexes {
			if re.MatchString(string(value.Key)) {
				result = false
				break
			}
		}
		cache.Store(value.Key, result)
		return result
	}

	// Warm up the cache so the benchmark measures the steady-state (cached) path.
	for _, kv := range keys {
		_ = attributeFilter(kv)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, kv := range keys {
			_ = attributeFilter(kv)
		}
	}
}

// BenchmarkAttributeExcludeFilterPromOld benchmarks the original Prometheus closure
// (isKeyInSlice + SanitizeName + regex loop, no caching).
func BenchmarkAttributeExcludeFilterPromOld(b *testing.B) {
	regexes := benchExcludePatterns
	keys := benchAttrKeys

	attributeFilter := func(value attribute.KeyValue) bool {
		if isKeyInSlice(value.Key, defaultExcludedOtelKeys) {
			return false
		}
		name := SanitizeName(string(value.Key))
		for _, re := range regexes {
			if re.MatchString(name) {
				return false
			}
		}
		return true
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, kv := range keys {
			_ = attributeFilter(kv)
		}
	}
}

// BenchmarkAttributeExcludeFilterPromNew benchmarks the cached Prometheus closure
// (full logic on first miss, sync.Map lookup on subsequent calls).
func BenchmarkAttributeExcludeFilterPromNew(b *testing.B) {
	regexes := benchExcludePatterns
	keys := benchAttrKeys

	var cache sync.Map
	attributeFilter := func(value attribute.KeyValue) bool {
		if v, ok := cache.Load(value.Key); ok {
			return v.(bool)
		}
		result := true
		if isKeyInSlice(value.Key, defaultExcludedOtelKeys) {
			result = false
		} else {
			name := SanitizeName(string(value.Key))
			for _, re := range regexes {
				if re.MatchString(name) {
					result = false
					break
				}
			}
		}
		cache.Store(value.Key, result)
		return result
	}

	// Warm up the cache.
	for _, kv := range keys {
		_ = attributeFilter(kv)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for _, kv := range keys {
			_ = attributeFilter(kv)
		}
	}
}
