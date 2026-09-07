package dag

import (
	stdctx "context"

	"github.com/sdsc-ordes/quitsh/pkg/common/set"
	"github.com/sdsc-ordes/quitsh/pkg/common/stack"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherc "github.com/sdsc-ordes/quitsh/pkg/watcher/client"
)

// WatcherSession carries the watcher settings through one `quitsh` run:
// [DefineExecutionOrder] fills in the scan id it decided on, and [Execute]
// reports the results back against that same scan id.
type WatcherSession struct {
	RootDir  string
	Settings *watcher.Args

	// ScanID is the scan the dirty decision was based on. Filled in by
	// [DefineExecutionOrder]; zero means the watcher was not consulted.
	ScanID int64
}

// WithWatcher makes the execution order ask the watcher server which targets
// changed. Targets which are up to date are marked [ExecStatusSkipped].
// When the server cannot be reached everything is treated as changed.
func WithWatcher(sess *WatcherSession) ExecOption {
	return func(o *opts) error {
		if sess == nil || !sess.Enabled() {
			return nil
		}

		o.watcher = sess

		return nil
	}
}

func (sess *WatcherSession) Enabled() bool {
	return !sess.Settings.Disabled
}

// queryWatcher asks the server about all targets in `nodes`.
func queryWatcher(sess *WatcherSession, nodes TargetNodeMap) map[target.ID]bool {
	ids := make([]target.ID, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}

	res := watcherc.QueryDirty(
		stdctx.Background(),
		sess.Settings,
		sess.RootDir, ids)
	sess.ScanID = res.ScanID

	if res.Dirty == nil {
		log.Info("Watcher not available: treating all targets as changed.")

		return nil
	}

	log.Debug("Watcher answered.", "scan", res.ScanID, "targets", len(res.Dirty))

	return res.Dirty
}

// SolveWatcherChanges seeds the changed flags from the watcher answer and
// propagates them forward, exactly like [graph.SolveInputChanges] does for
// path-based changes. A nil `dirty` map, or a target missing from it, means
// changed: we never skip what we do not know about.
func (graph *graph) SolveWatcherChanges(dirty map[target.ID]bool) error {
	log.Debug("Solve watcher changes and recompute selection subgraph.")

	var changedInSelection TargetSelection

	for _, root := range *graph.execRootNodesSel {
		dfsStack := stack.NewStack[*TargetNode]()
		dfsStack.Push(root)

		for dfsStack.Len() != 0 {
			n := dfsStack.Pop()

			if !graph.inSelection(n) {
				continue
			}

			currIn := &n.Inputs

			if !currIn.IsChanged() {
				changed, known := dirty[n.Target.ID]
				currIn.Changed = dirty == nil || !known || changed
			}

			log.Debug("Changes for target id.",
				"id", n.Target.ID.String(),
				"changed", currIn.Changed,
				"changedByDeps", currIn.ChangedByDependency)

			if currIn.IsChanged() {
				changedInSelection.Insert(n.Target.ID)
			}

			for _, c := range n.Forward {
				c.Inputs.Propagate(&n.Inputs)
			}

			dfsStack.Push(n.Forward...)
		}
	}

	if changedInSelection.Len() == 0 {
		// Everything is up to date. This is a legitimate outcome for the
		// watcher (unlike for `SolveInputChanges`, where an empty selection
		// is an error), so select nothing instead of failing.
		log.Info("Everything is up to date.")
		graph.selectNothing()

		return nil
	}

	return graph.recomputeSubgraph(&changedInSelection)
}

// selectNothing empties the selection subgraph so that no target is executed.
func (graph *graph) selectNothing() {
	empty := set.NewUnordered[target.ID]()
	graph.nodesSel = &empty
	graph.execLeafNodesSel = &[]*TargetNode{}
	graph.execRootNodesSel = &[]*TargetNode{}
}

// markSkipped marks every target in the subgraph which is up to date.
// Those are the clean dependencies pulled in by a changed dependent.
func markSkipped(targets TargetNodeMap) {
	for id := range targets {
		if targets[id].Inputs.IsChanged() {
			continue
		}

		targets[id].Execution.Skip = true
		log.Debug("Target is up to date, skipping.", "target", id)
	}
}

// collectResults returns the terminal result of every target which actually
// ran. Skipped, cancelled and never-started targets are left out: the watcher
// must only learn about builds that happened.
func collectResults(targets TargetNodeMap) map[target.ID]bool {
	results := make(map[target.ID]bool, len(targets))

	for id, n := range targets {
		if n.Execution.Skip || n.Execution.Cancel || len(n.Execution.Runners) == 0 {
			continue
		}

		executed := false

		for _, r := range n.Execution.Runners {
			if r.Status == ExecStatusSuccess || r.Status == ExecStatusFailed {
				executed = true

				break
			}
		}

		if !executed {
			continue
		}

		results[id] = n.Status() == ExecStatusSuccess
	}

	return results
}

// reportToWatcher sends the results of this run to the watcher server.
func reportToWatcher(sess *WatcherSession, targets TargetNodeMap) {
	if sess.Settings.Disabled || sess.ScanID == 0 {
		return
	}

	results := collectResults(targets)
	log.Debug("Reporting results to the watcher.", "scan", sess.ScanID, "targets", len(results))

	watcherc.ReportResults(
		stdctx.Background(), sess.Settings, sess.RootDir, sess.ScanID, results)
}
