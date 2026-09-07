//go:build test && (test_small || test_all)

package watcher

import (
	"path"
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadStateOfMissingFileIsEmpty(t *testing.T) {
	t.Parallel()
	s, err := LoadState(path.Join(t.TempDir(), "nope.json"))
	require.NoError(t, err)
	assert.Empty(t, s.LastSuccess)
}

func TestSaveAndLoadStateRoundTrips(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})
	tr.Report(1, map[target.ID]bool{tgtBuild: true})

	file := path.Join(t.TempDir(), "sub", "state.json")
	require.NoError(t, SaveState(file, tr.Export()))

	loaded, err := LoadState(file)
	require.NoError(t, err)

	restored := newTracker(t)
	restored.Import(loaded)
	restored.Update(1, map[string]Stamp{fileA: {Size: 1}})

	assert.False(t, restored.Status([]target.ID{tgtBuild})[0].Dirty,
		"a restored server must not rebuild unchanged targets")
}

func TestRestoredStateIsDirtyAfterOfflineChange(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})
	tr.Report(1, map[target.ID]bool{tgtBuild: true})

	file := path.Join(t.TempDir(), "state.json")
	require.NoError(t, SaveState(file, tr.Export()))

	loaded, err := LoadState(file)
	require.NoError(t, err)

	restored := newTracker(t)
	restored.Import(loaded)
	// The file changed while the server was down.
	restored.Update(1, map[string]Stamp{fileA: {Size: 99}})

	assert.True(t, restored.Status([]target.ID{tgtBuild})[0].Dirty)
}
