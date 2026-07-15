package routerconfig

import (
	"fmt"
	"maps"
	"path/filepath"
	"slices"

	"github.com/cespare/xxhash/v2"
	nodev1 "github.com/wundergraph/cosmo/router/gen/proto/wg/cosmo/node/v1"
	"google.golang.org/protobuf/proto"
)

// DiffGraphHashes compares previously-applied graph hashes with a new set and returns
// the change summary and per-graph hash metadata used by graph_server hot reload.
func DiffGraphHashes(knownHashes, newHashes map[string]string) (Changes, map[string]HashInfo) {
	changes := Changes{
		AddedConfigs:   make(map[string]struct{}),
		RemovedConfigs: make(map[string]struct{}),
		ChangedConfigs: make(map[string]struct{}),
	}
	hashes := make(map[string]HashInfo, len(newHashes))

	for name, hash := range newHashes {
		if oldHash, exists := knownHashes[name]; !exists {
			changes.AddedConfigs[name] = struct{}{}
			hashes[name] = HashInfo{NewHash: hash}
		} else if oldHash != hash {
			changes.ChangedConfigs[name] = struct{}{}
			hashes[name] = HashInfo{NewHash: hash, OldHash: oldHash}
		}
	}
	for name, oldHash := range knownHashes {
		if newHash, exists := newHashes[name]; !exists {
			changes.RemovedConfigs[name] = struct{}{}
		} else if oldHash == newHash {
			hashes[name] = HashInfo{OldHash: oldHash, NewHash: newHash}
		}
	}

	return changes, hashes
}

// HasChanges reports whether any graph was added, removed, or changed.
func (c *Changes) HasChanges() bool {
	if c == nil {
		return true
	}
	return len(c.AddedConfigs)+len(c.RemovedConfigs)+len(c.ChangedConfigs) > 0
}

// GraphHashesChanged reports whether newHashes differs from knownHashes.
func GraphHashesChanged(knownHashes, newHashes map[string]string) bool {
	changes, _ := DiffGraphHashes(knownHashes, newHashes)
	return changes.HasChanges()
}

// ReadManifestMapperHashes reads mapper.json from a manifest directory and returns
// per-graph hashes. Ignored feature flags are removed from the result.
func ReadManifestMapperHashes(manifestConfigPath string, ignoredFeatureFlags []string) (map[string]string, error) {
	mapper, err := readMapperFile(filepath.Join(manifestConfigPath, "mapper.json"))
	if err != nil {
		return nil, fmt.Errorf("failed to read mapper file: %w", err)
	}

	if len(ignoredFeatureFlags) == 0 {
		return maps.Clone(mapper), nil
	}

	filtered := maps.Clone(mapper)
	for _, name := range ignoredFeatureFlags {
		delete(filtered, name)
	}
	return filtered, nil
}

// ComputeGraphHashesFromConfig derives stable per-graph hashes from an assembled router config.
// Used by the single-file execution config watcher where no mapper.json is available.
func ComputeGraphHashesFromConfig(cfg *nodev1.RouterConfig) (map[string]string, error) {
	if cfg == nil {
		return nil, fmt.Errorf("router config is nil")
	}

	hashes := make(map[string]string, 1+len(cfg.GetFeatureFlagConfigs().GetConfigByFeatureFlagName()))

	baseHash, err := hashBaseGraphConfig(cfg)
	if err != nil {
		return nil, err
	}
	hashes[""] = baseHash

	for name, ffCfg := range cfg.GetFeatureFlagConfigs().GetConfigByFeatureFlagName() {
		ffHash, err := hashFeatureFlagGraphConfig(ffCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to hash feature flag %q: %w", name, err)
		}
		hashes[name] = ffHash
	}

	return hashes, nil
}

func hashBaseGraphConfig(cfg *nodev1.RouterConfig) (string, error) {
	msg := &nodev1.RouterConfig{
		EngineConfig:         cfg.EngineConfig,
		Version:              cfg.Version,
		Subgraphs:            cfg.Subgraphs,
		CompatibilityVersion: cfg.CompatibilityVersion,
	}
	return hashProtoMessage(msg)
}

func hashFeatureFlagGraphConfig(cfg *nodev1.FeatureFlagRouterExecutionConfig) (string, error) {
	return hashProtoMessage(cfg)
}

func hashProtoMessage(msg proto.Message) (string, error) {
	data, err := proto.Marshal(msg)
	if err != nil {
		return "", fmt.Errorf("failed to marshal config for hashing: %w", err)
	}

	h := xxhash.New()
	if _, err := h.Write(data); err != nil {
		return "", fmt.Errorf("failed to hash config: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum64()), nil
}

// CompositeGraphHashVersion returns a deterministic version string derived from all graph hashes.
func CompositeGraphHashVersion(graphHashes map[string]string) string {
	keys := make([]string, 0, len(graphHashes))
	for k := range graphHashes {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	h := xxhash.New()
	for _, k := range keys {
		_, _ = h.Write([]byte(k + ":" + graphHashes[k] + ";"))
	}
	return fmt.Sprintf("graphs-%x", h.Sum64())
}
