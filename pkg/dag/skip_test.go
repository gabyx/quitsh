//go:build test && (test_small || test_all)

package dag

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
)

func TestSkippedNodeCountsAsSuccess(t *testing.T) {
	t.Parallel()
	n := &TargetNode{Target: &target.Config{ID: "c::a"}}
	n.Execution.Skip = true
	n.Execution.Runners = RunnerStatuses{{Status: ExecStatusSkipped}}

	assert.Equal(t, ExecStatus(ExecStatusSuccess), n.Status())
	assert.False(t, n.StatusAnyFailed())
}

func TestSkippedNodeDoesNotCancelDependents(t *testing.T) {
	t.Parallel()
	dep := &TargetNode{Target: &target.Config{ID: "c::dep"}}
	user := &TargetNode{Target: &target.Config{ID: "c::user"}}
	dep.Forward = []*TargetNode{user}
	user.Backward = []*TargetNode{dep}

	dep.Execution.Skip = true
	dep.Execution.Runners = RunnerStatuses{{Status: ExecStatusSkipped}}
	dep.PropagateExecStatus()

	assert.False(t, user.Execution.Cancel, "a skipped dependency must not cancel its dependents")
}

func TestNotRunNodeStillCancelsDependents(t *testing.T) {
	t.Parallel()
	dep := &TargetNode{Target: &target.Config{ID: "c::dep"}}
	user := &TargetNode{Target: &target.Config{ID: "c::user"}}
	dep.Forward = []*TargetNode{user}
	user.Backward = []*TargetNode{dep}

	dep.Execution.Runners = RunnerStatuses{{Status: ExecStatusNotRun}}
	dep.PropagateExecStatus()

	assert.True(t, user.Execution.Cancel, "failure propagation must keep working")
}
