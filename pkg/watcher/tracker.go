package watcher

import (
	"sort"
	"sync"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"
)

type (
	// ChangePoint records the scan at which an input set's digest changed.
	ChangePoint struct {
		ScanID ScanID `json:"scanID"`
		Digest Digest `json:"digest"`
	}

	// LastSuccessfulBuild is the input state a target was last built from.
	LastSuccessfulBuild struct {
		ScanID   ScanID              `json:"scanID"`
		AtUnixMs int64               `json:"atUnixMs"`
		Inputs   map[input.ID]Digest `json:"inputs"`
	}

	// LastRun is the outcome of the last reported run of a target.
	LastRun struct {
		Success  bool   `json:"success"`
		ScanID   ScanID `json:"scanID"`
		AtUnixMs int64  `json:"atUnixMs"`
	}

	// TargetStatus is the derived answer for one target.
	TargetStatus struct {
		ID                 target.ID
		Dirty              bool
		HasSuccessfulBuild bool
		DirtyInputs        []input.ID
		LastRun            *LastRun
	}

	// Tracker owns all change-tracking state. It is safe for concurrent use.
	Tracker struct {
		mu sync.Mutex

		sets    InputSets
		targets map[target.ID][]input.ID

		history  map[input.ID][]ChangePoint
		lastScan ScanID

		lastSuccess map[target.ID]LastSuccessfulBuild
		lastRun     map[target.ID]LastRun

		pins map[ScanID]int
	}
)

// NewTracker creates an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{
		sets:        InputSets{},
		targets:     map[target.ID][]input.ID{},
		history:     map[input.ID][]ChangePoint{},
		lastSuccess: map[target.ID]LastSuccessfulBuild{},
		lastRun:     map[target.ID]LastRun{},
		pins:        map[ScanID]int{},
	}
}

// SetComponents installs the input sets and the target to input-set mapping.
// State of targets and input sets which no longer exist is dropped.
func (t *Tracker) SetComponents(sets InputSets, targets map[target.ID][]input.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.sets = sets
	t.targets = targets

	for id := range t.lastSuccess {
		if _, exists := targets[id]; !exists {
			log.Debug("Dropping state of vanished target.", "target", id)
			delete(t.lastSuccess, id)
			delete(t.lastRun, id)
		}
	}

	for id := range t.history {
		if _, exists := sets[id]; !exists {
			delete(t.history, id)
		}
	}
}

// Update installs the result of scan `scanID` and appends change points for
// every input set whose digest moved.
func (t *Tracker) Update(scanID ScanID, files map[string]Stamp) {
	t.mu.Lock()
	defer t.mu.Unlock()

	assigned := t.sets.Assign(files)

	for id, paths := range assigned {
		d := DigestOf(paths, files)

		h := t.history[id]
		if len(h) != 0 && h[len(h)-1].Digest == d {
			continue
		}

		t.history[id] = append(h, ChangePoint{ScanID: scanID, Digest: d})
		log.Trace("Input set changed.", "input", id, "scan", scanID)
	}

	t.lastScan = scanID
	t.pruneLocked()
}

// DigestAt returns the digest of input set `id` as of scan `scanID`.
// It reports `false` when the history does not reach back that far.
func (t *Tracker) DigestAt(id input.ID, scanID ScanID) (Digest, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.digestAtLocked(id, scanID)
}

func (t *Tracker) digestAtLocked(id input.ID, scanID ScanID) (Digest, bool) {
	h := t.history[id]
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].ScanID <= scanID {
			return h[i].Digest, true
		}
	}

	return 0, false
}

// Status derives the dirty state of the given targets.
// An empty `ids` means all known targets.
func (t *Tracker) Status(ids []target.ID) []TargetStatus {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(ids) == 0 {
		ids = make([]target.ID, 0, len(t.targets))
		for id := range t.targets {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}

	statuses := make([]TargetStatus, 0, len(ids))
	for _, id := range ids {
		statuses = append(statuses, t.statusLocked(id))
	}

	return statuses
}

func (t *Tracker) statusLocked(id target.ID) TargetStatus {
	st := TargetStatus{ID: id}

	if lr, ok := t.lastRun[id]; ok {
		copied := lr
		st.LastRun = &copied
	}

	inputs, known := t.targets[id]
	if !known {
		// Unknown targets are always dirty: never skip what we cannot judge.
		st.Dirty = true

		return st
	}

	last, ok := t.lastSuccess[id]
	if !ok {
		st.Dirty = true

		return st
	}

	st.HasSuccessfulBuild = true

	for _, in := range inputs {
		current, hasCurrent := t.digestAtLocked(in, t.lastScan)
		built, hasBuilt := last.Inputs[in]

		if !hasCurrent || !hasBuilt || current != built {
			st.Dirty = true
			st.DirtyInputs = append(st.DirtyInputs, in)
		}
	}

	sort.Slice(st.DirtyInputs, func(i, j int) bool { return st.DirtyInputs[i] < st.DirtyInputs[j] })

	return st
}

// Report records the outcome of a run which was decided at scan `scanID`.
// For successful targets the input digests **as of that scan** are recorded as
// the target's last successful build. Targets whose history no longer reaches
// back to `scanID` are returned in `notRecorded` and stay dirty.
func (t *Tracker) Report(scanID ScanID, results map[target.ID]bool) (notRecorded []target.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := nowUnixMs()

	ids := make([]target.ID, 0, len(results))
	for id := range results {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		success := results[id]
		t.lastRun[id] = LastRun{Success: success, ScanID: scanID, AtUnixMs: now}

		if !success {
			continue
		}

		inputs, known := t.targets[id]
		if !known {
			notRecorded = append(notRecorded, id)

			continue
		}

		digests := make(map[input.ID]Digest, len(inputs))
		complete := true

		for _, in := range inputs {
			d, ok := t.digestAtLocked(in, scanID)
			if !ok {
				complete = false

				break
			}
			digests[in] = d
		}

		if !complete {
			log.Debug("Scan id no longer resolvable; not recording build.",
				"target", id, "scan", scanID)
			notRecorded = append(notRecorded, id)

			continue
		}

		t.lastSuccess[id] = LastSuccessfulBuild{
			ScanID:   scanID,
			AtUnixMs: now,
			Inputs:   digests,
		}
	}

	return notRecorded
}

// Pin keeps the change-point history resolvable for `scanID` until [Tracker.Unpin].
func (t *Tracker) Pin(scanID ScanID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pins[scanID]++
}

// Unpin releases a pin taken by [Tracker.Pin].
func (t *Tracker) Unpin(scanID ScanID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.pins[scanID] <= 1 {
		delete(t.pins, scanID)

		return
	}

	t.pins[scanID]--
}

// Reset clears the last successful build of the given targets, or of all
// targets when no id is given. They become dirty again.
func (t *Tracker) Reset(ids ...target.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(ids) == 0 {
		t.lastSuccess = map[target.ID]LastSuccessfulBuild{}
		t.lastRun = map[target.ID]LastRun{}

		return
	}

	for _, id := range ids {
		delete(t.lastSuccess, id)
		delete(t.lastRun, id)
	}
}

// LastScan returns the id of the most recent scan installed by [Tracker.Update].
func (t *Tracker) LastScan() ScanID {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.lastScan
}

// pruneLocked drops change points which no outstanding scan id can still need.
func (t *Tracker) pruneLocked() {
	keepFrom := t.lastScan
	for id := range t.pins {
		if id < keepFrom {
			keepFrom = id
		}
	}

	for id, h := range t.history {
		cut := 0
		for i := range h {
			if h[i].ScanID <= keepFrom {
				cut = i
			}
		}

		if cut > 0 {
			t.history[id] = append([]ChangePoint(nil), h[cut:]...)
		}
	}
}

// nowUnixMs is indirected so tests can stay deterministic if needed.
var nowUnixMs = func() int64 { return time.Now().UnixMilli() } //nolint:gochecknoglobals // test seam.
