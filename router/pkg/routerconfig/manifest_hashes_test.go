package routerconfig

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestComputeGraphChangesAndHashes_initial(t *testing.T) {
	current := map[string]string{"": "base-v1", "ff1": "ff-v1"}

	changes, hashes := ComputeGraphChangesAndHashes(nil, current)

	assert.Nil(t, changes)
	require.Len(t, hashes, 2)
	assert.Equal(t, HashInfo{NewHash: "base-v1"}, hashes[""])
	assert.Equal(t, HashInfo{NewHash: "ff-v1"}, hashes["ff1"])
}

func TestComputeGraphChangesAndHashes_baseChangedFFUnchanged(t *testing.T) {
	known := map[string]string{"": "base-v1", "ff1": "ff-v1"}
	current := map[string]string{"": "base-v2", "ff1": "ff-v1"}

	changes, hashes := ComputeGraphChangesAndHashes(known, current)

	require.NotNil(t, changes)
	assert.Contains(t, changes.ChangedConfigs, "")
	assert.NotContains(t, changes.ChangedConfigs, "ff1")
	assert.Equal(t, HashInfo{OldHash: "base-v1", NewHash: "base-v2"}, hashes[""])
	assert.Equal(t, HashInfo{OldHash: "ff-v1", NewHash: "ff-v1"}, hashes["ff1"])
}

func TestComputeGraphChangesAndHashes_ffAddedAndRemoved(t *testing.T) {
	known := map[string]string{"": "base-v1", "ff-old": "old"}
	current := map[string]string{"": "base-v1", "ff-new": "new"}

	changes, hashes := ComputeGraphChangesAndHashes(known, current)

	require.NotNil(t, changes)
	assert.Contains(t, changes.AddedConfigs, "ff-new")
	assert.Contains(t, changes.RemovedConfigs, "ff-old")
	assert.Equal(t, HashInfo{OldHash: "base-v1", NewHash: "base-v1"}, hashes[""])
}
