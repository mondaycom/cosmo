package routerconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	nodev1 "github.com/wundergraph/cosmo/router/gen/proto/wg/cosmo/node/v1"
)

func TestDiffGraphHashes(t *testing.T) {
	t.Parallel()

	known := map[string]string{"": "hash-base", "ff1": "hash-ff1-old"}
	newHashes := map[string]string{"": "hash-base-new", "ff1": "hash-ff1-old", "ff2": "hash-ff2"}

	changes, hashes := DiffGraphHashes(known, newHashes)

	assert.Contains(t, changes.ChangedConfigs, "")
	assert.Contains(t, changes.AddedConfigs, "ff2")
	assert.NotContains(t, changes.ChangedConfigs, "ff1")
	assert.NotContains(t, changes.RemovedConfigs, "ff1")

	assert.Equal(t, HashInfo{OldHash: "hash-base", NewHash: "hash-base-new"}, hashes[""])
	assert.Equal(t, HashInfo{OldHash: "hash-ff1-old", NewHash: "hash-ff1-old"}, hashes["ff1"])
	assert.Equal(t, HashInfo{NewHash: "hash-ff2"}, hashes["ff2"])
}

func TestGraphHashesChanged(t *testing.T) {
	t.Parallel()

	known := map[string]string{"": "hash-base"}
	assert.False(t, GraphHashesChanged(known, map[string]string{"": "hash-base"}))
	assert.True(t, GraphHashesChanged(known, map[string]string{"": "hash-base-new"}))
}

func TestComputeGraphHashesFromConfig_DetectsBaseChangeOnly(t *testing.T) {
	t.Parallel()

	base := &nodev1.RouterConfig{
		Version: "v1",
		EngineConfig: &nodev1.EngineConfiguration{
			DefaultFlushInterval: 500,
		},
	}
	ff := &nodev1.RouterConfig{
		Version: "v1",
		EngineConfig: &nodev1.EngineConfiguration{
			DefaultFlushInterval: 500,
		},
		FeatureFlagConfigs: &nodev1.FeatureFlagRouterExecutionConfigs{
			ConfigByFeatureFlagName: map[string]*nodev1.FeatureFlagRouterExecutionConfig{
				"ff1": {
					Version: "ff-v1",
					EngineConfig: &nodev1.EngineConfiguration{
						DefaultFlushInterval: 100,
					},
				},
			},
		},
	}

	baseHashes, err := ComputeGraphHashesFromConfig(base)
	require.NoError(t, err)
	ffHashes, err := ComputeGraphHashesFromConfig(ff)
	require.NoError(t, err)

	assert.Equal(t, baseHashes[""], ffHashes[""])

	changedFF := protoCloneRouterConfig(ff)
	changedFF.FeatureFlagConfigs.ConfigByFeatureFlagName["ff1"].EngineConfig.DefaultFlushInterval = 200

	changedHashes, err := ComputeGraphHashesFromConfig(changedFF)
	require.NoError(t, err)

	assert.Equal(t, ffHashes[""], changedHashes[""])
	assert.NotEqual(t, ffHashes["ff1"], changedHashes["ff1"])
}

func protoCloneRouterConfig(cfg *nodev1.RouterConfig) *nodev1.RouterConfig {
	cloned := *cfg
	cloned.EngineConfig = cfg.EngineConfig
	cloned.FeatureFlagConfigs = &nodev1.FeatureFlagRouterExecutionConfigs{
		ConfigByFeatureFlagName: make(map[string]*nodev1.FeatureFlagRouterExecutionConfig, len(cfg.FeatureFlagConfigs.ConfigByFeatureFlagName)),
	}
	for name, ffCfg := range cfg.FeatureFlagConfigs.ConfigByFeatureFlagName {
		copied := *ffCfg
		copied.EngineConfig = ffCfg.EngineConfig
		cloned.FeatureFlagConfigs.ConfigByFeatureFlagName[name] = &copied
	}
	return &cloned
}
