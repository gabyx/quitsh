//go:build test && (test_small || test_all)

package watcher

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test fixture identifiers, shared by the tests in this package.
const (
	tgtBuild = "comp::build"
	inSrcs   = "comp::srcs"
	fileA    = "comp/a.go"
)

// newTracker builds a tracker with one component `comp` owning input set
// `comp::srcs` (all files below `comp/`) and one target `comp::build`.
func newTracker(t *testing.T) *Tracker {
	t.Helper()
	cfg := &input.Config{Patterns: []string{`^.*$`}, BaseDir: "/repo/comp"}
	cfg.Init(inSrcs)
	set, err := NewInputSet(cfg, "/repo")
	require.NoError(t, err)

	tr := NewTracker()
	tr.SetComponents(
		InputSets{inSrcs: set},
		map[target.ID][]input.ID{tgtBuild: {inSrcs}},
	)

	return tr
}

func TestStatusDirtyWithoutSuccessfulBuild(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})

	st := tr.Status([]target.ID{tgtBuild})
	require.Len(t, st, 1)
	assert.True(t, st[0].Dirty)
	assert.False(t, st[0].HasSuccessfulBuild)
}

func TestUpdateRecordsChangePointOnlyWhenDigestMoves(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)

	tr.Pin(1)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})
	tr.Update(2, map[string]Stamp{fileA: {Size: 1}})
	tr.Update(3, map[string]Stamp{fileA: {Size: 2}})

	d1, ok := tr.DigestAt(inSrcs, 1)
	require.True(t, ok)
	d2, ok := tr.DigestAt(inSrcs, 2)
	require.True(t, ok)
	d3, ok := tr.DigestAt(inSrcs, 3)
	require.True(t, ok)

	assert.Equal(t, d1, d2, "no change between scan 1 and 2")
	assert.NotEqual(t, d1, d3)
}

func TestDigestAtBeforeFirstScanIsUnknown(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(5, map[string]Stamp{fileA: {Size: 1}})

	_, ok := tr.DigestAt(inSrcs, 4)
	assert.False(t, ok)
}

func TestStatusOfUnknownTargetIsDirty(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})

	st := tr.Status([]target.ID{"nope::nope"})
	require.Len(t, st, 1)
	assert.True(t, st[0].Dirty)
}

func TestSetComponentsDropsVanishedTargets(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})
	tr.Report(1, map[target.ID]bool{tgtBuild: true})
	require.True(t, tr.Status([]target.ID{tgtBuild})[0].HasSuccessfulBuild)

	// Target disappears from the components.
	tr.SetComponents(InputSets{}, map[target.ID][]input.ID{})
	assert.False(t, tr.Status([]target.ID{tgtBuild})[0].HasSuccessfulBuild)
}

func TestReportSuccessMakesTargetClean(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})

	notRecorded := tr.Report(1, map[target.ID]bool{tgtBuild: true})
	assert.Empty(t, notRecorded)

	st := tr.Status([]target.ID{tgtBuild})[0]
	assert.False(t, st.Dirty)
	assert.True(t, st.HasSuccessfulBuild)
}

func TestReportFailureLeavesTargetDirty(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})

	tr.Report(1, map[target.ID]bool{tgtBuild: false})

	st := tr.Status([]target.ID{tgtBuild})[0]
	assert.True(t, st.Dirty)
	assert.False(t, st.HasSuccessfulBuild)
	require.NotNil(t, st.LastRun)
	assert.False(t, st.LastRun.Success)
}

// The edit-during-build case from the design spec: a file changes at scan 7
// while the build that was decided at scan 5 is still running.
func TestReportRecordsStateOfTheReportedScan(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Pin(5)
	tr.Update(5, map[string]Stamp{fileA: {Size: 1}}) // D1
	tr.Update(7, map[string]Stamp{fileA: {Size: 2}}) // D2

	tr.Report(5, map[target.ID]bool{tgtBuild: true})
	tr.Unpin(5)

	st := tr.Status([]target.ID{tgtBuild})[0]
	assert.True(t, st.Dirty, "the build built D1 but D2 is on disk")
	assert.Equal(t, []input.ID{inSrcs}, st.DirtyInputs)
}

func TestRevertingAnEditReadsCleanAgain(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})
	tr.Report(1, map[target.ID]bool{tgtBuild: true})

	tr.Update(2, map[string]Stamp{fileA: {Size: 2}})
	require.True(t, tr.Status([]target.ID{tgtBuild})[0].Dirty)

	tr.Update(3, map[string]Stamp{fileA: {Size: 1}}) // reverted
	assert.False(t, tr.Status([]target.ID{tgtBuild})[0].Dirty)
}

func TestReportWithPrunedScanIDRecordsNothing(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(10, map[string]Stamp{fileA: {Size: 1}})

	notRecorded := tr.Report(9, map[target.ID]bool{tgtBuild: true})

	assert.Equal(t, []target.ID{tgtBuild}, notRecorded)
	assert.True(t, tr.Status([]target.ID{tgtBuild})[0].Dirty)
}

func TestResetClearsSuccessfulBuild(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})
	tr.Report(1, map[target.ID]bool{tgtBuild: true})
	require.False(t, tr.Status([]target.ID{tgtBuild})[0].Dirty)

	tr.Reset(tgtBuild)
	assert.True(t, tr.Status([]target.ID{tgtBuild})[0].Dirty)
}

func TestPinKeepsHistoryForLongBuilds(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Pin(1)
	tr.Update(1, map[string]Stamp{fileA: {Size: 1}})

	// Many scans pass while the build runs.
	for i := ScanID(2); i < 100; i++ {
		tr.Update(i, map[string]Stamp{fileA: {Size: int64(i)}})
	}

	_, ok := tr.DigestAt(inSrcs, 1)
	assert.True(t, ok, "pinned scan id must stay resolvable")

	tr.Unpin(1)
}
