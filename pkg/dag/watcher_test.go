//go:build test && (test_small || test_all)

package dag

import (
	"path"
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test fixture identifiers used by the watcher tests.
const (
	tgtCompABuild = "comp-a::build"
	tgtCompBTest  = "comp-b::test"
	compBName     = "comp-b"
	stageBuild    = "build"
	stageTest     = "test"
	tgtRan        = "c::ran"
	tgtFailed     = "c::failed"
)

// testCompsWithDependency is `testCompsWithInputs` where `comp-b::test`
// depends on `comp-a::build`.
func testCompsWithDependency(t *testing.T) []*component.Component {
	t.Helper()

	confA := &component.Config{
		Name:     "comp-a",
		Language: "go",
		Inputs: map[string]*input.Config{
			"srcs": {Patterns: []string{`^src/.*\.go$`}},
		},
		Targets: map[string]*target.Config{
			stageBuild: {Stage: stageBuild, Inputs: []input.ID{"self::srcs"}},
		},
	}
	require.NoError(t, confA.Init())

	confB := &component.Config{
		Name:     compBName,
		Language: "go",
		Targets: map[string]*target.Config{
			stageTest: {Stage: stageTest, Dependencies: []target.ID{tgtCompABuild}},
		},
	}
	require.NoError(t, confB.Init())

	a := component.NewComponent(confA, path.Join(rootDir, "comp-a"), "", "")
	b := component.NewComponent(confB, path.Join(rootDir, "comp-b"), "", "")

	return []*component.Component{&a, &b}
}

// solvedGraph builds the graph of `comps` without solving any changes.
func solvedGraph(t *testing.T, comps []*component.Component) (TargetNodeMap, *graph) {
	t.Helper()

	nodes, _, _, err := constructNodes(comps, nil, rootDir, true)
	require.NoError(t, err)

	g, err := newGraph(nodes, nil)
	require.NoError(t, err)
	require.NoError(t, g.SolveExecutionOrder())

	return nodes, &g
}

func TestSolveWatcherChangesMarksOnlyDirtyTargets(t *testing.T) {
	t.Parallel()
	nodes, g := solvedGraph(t, testCompsWithInputs(t))

	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{
		tgtCompABuild: true,
		tgtCompBTest:  false,
	}))

	assert.True(t, nodes[target.ID(tgtCompABuild)].Inputs.IsChanged())
	assert.False(t, nodes[target.ID(tgtCompBTest)].Inputs.IsChanged())
}

func TestSolveWatcherChangesTreatsUnknownTargetsAsChanged(t *testing.T) {
	t.Parallel()
	nodes, g := solvedGraph(t, testCompsWithInputs(t))

	// The server answered for nothing at all.
	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{}))

	assert.True(t, nodes[target.ID(tgtCompABuild)].Inputs.IsChanged())
	assert.True(t, nodes[target.ID(tgtCompBTest)].Inputs.IsChanged())
}

func TestSolveWatcherChangesWithNilMapChangesEverything(t *testing.T) {
	t.Parallel()
	nodes, g := solvedGraph(t, testCompsWithInputs(t))

	require.NoError(t, g.SolveWatcherChanges(nil))

	for id := range nodes {
		assert.True(t, nodes[id].Inputs.IsChanged(), "target '%v' must be changed", id)
	}
}

func TestSolveWatcherChangesPropagatesToDependents(t *testing.T) {
	t.Parallel()
	nodes, g := solvedGraph(t, testCompsWithDependency(t))

	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{
		tgtCompABuild: true,
		tgtCompBTest:  false,
	}))

	assert.True(t, nodes[target.ID(tgtCompBTest)].Inputs.ChangedByDependency)
	assert.True(t, nodes[target.ID(tgtCompBTest)].Inputs.IsChanged())
}

func TestMarkSkippedFlagsUpToDateTargets(t *testing.T) {
	t.Parallel()
	nodes, g := solvedGraph(t, testCompsWithInputs(t))

	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{
		tgtCompABuild: true,
		tgtCompBTest:  false,
	}))
	markSkipped(nodes)

	assert.False(t, nodes[target.ID(tgtCompABuild)].Execution.Skip)
	assert.True(t, nodes[target.ID(tgtCompBTest)].Execution.Skip)
}

func TestCollectResultsIgnoresSkippedAndUnexecutedTargets(t *testing.T) {
	t.Parallel()

	ran := &TargetNode{Target: &target.Config{ID: tgtRan}}
	ran.Execution.Runners = RunnerStatuses{{Status: ExecStatusSuccess}}

	failed := &TargetNode{Target: &target.Config{ID: tgtFailed}}
	failed.Execution.Runners = RunnerStatuses{{Status: ExecStatusFailed}}

	skipped := &TargetNode{Target: &target.Config{ID: "c::skipped"}}
	skipped.Execution.Skip = true
	skipped.Execution.Runners = RunnerStatuses{{Status: ExecStatusSkipped}}

	cancelled := &TargetNode{Target: &target.Config{ID: "c::cancelled"}}
	cancelled.Execution.Cancel = true
	cancelled.Execution.Runners = RunnerStatuses{{Status: ExecStatusNotRun}}

	results := CollectResults(TargetNodeMap{
		tgtRan:         ran,
		tgtFailed:      failed,
		"c::skipped":   skipped,
		"c::cancelled": cancelled,
	})

	assert.Equal(t, map[target.ID]bool{tgtRan: true, tgtFailed: false}, results)
}

func TestWithWatcherIsANoOpWhenDisabled(t *testing.T) {
	t.Parallel()

	var o opts
	require.NoError(t, WithWatcher(&WatcherSession{})(&o))
	assert.Nil(t, o.watcher, "a session without args must not enable the watcher")
}

func TestSolveWatcherChangesSelectsNothingWhenAllClean(t *testing.T) {
	t.Parallel()
	nodes, g := solvedGraph(t, testCompsWithInputs(t))

	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{
		tgtCompABuild: false,
		tgtCompBTest:  false,
	}), "an up-to-date repository must not be an error")

	targets, prios := g.NodesToPriorityList()
	assert.Empty(t, targets, "nothing to run")
	assert.Empty(t, prios)

	markSkipped(nodes)
	for id := range nodes {
		assert.True(t, nodes[id].Execution.Skip, "target '%v' must be skipped", id)
	}
}
