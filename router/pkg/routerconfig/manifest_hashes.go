package routerconfig

import (
	"path/filepath"
)

// ReadManifestMapperGraphs loads mapper.json graph hashes (base key "") and applies
// the same ignored-feature-flag filtering as config assembly.
func ReadManifestMapperGraphs(manifestConfigPath string, rules AssembleConfigRules) (map[string]string, error) {
	mapper, err := readMapperFile(filepath.Join(manifestConfigPath, "mapper.json"))
	if err != nil {
		return nil, err
	}

	for _, ff := range rules.IgnoredFeatureFlags {
		delete(mapper, ff)
	}

	return mapper, nil
}

// ComputeGraphChangesAndHashes compares known mapper graph hashes with the current
// set. A nil known map indicates the initial load (Changes nil, rebuild everything).
func ComputeGraphChangesAndHashes(known, current map[string]string) (*Changes, map[string]HashInfo) {
	hashes := make(map[string]HashInfo, len(current))
	if known == nil {
		for name, hash := range current {
			hashes[name] = HashInfo{NewHash: hash}
		}
		return nil, hashes
	}

	changes := &Changes{
		AddedConfigs:   make(map[string]struct{}),
		RemovedConfigs: make(map[string]struct{}),
		ChangedConfigs: make(map[string]struct{}),
	}

	for name, hash := range current {
		oldHash, exists := known[name]
		if !exists {
			changes.AddedConfigs[name] = struct{}{}
			hashes[name] = HashInfo{NewHash: hash}
			continue
		}
		if oldHash != hash {
			changes.ChangedConfigs[name] = struct{}{}
			hashes[name] = HashInfo{NewHash: hash, OldHash: oldHash}
			continue
		}
		hashes[name] = HashInfo{OldHash: oldHash, NewHash: hash}
	}

	for name := range known {
		if _, exists := current[name]; !exists {
			changes.RemovedConfigs[name] = struct{}{}
		}
	}

	return changes, hashes
}
