package watcher

import (
	"encoding/json"
	"os"
	"path"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
)

// StateVersion is bumped whenever the on-disk format changes incompatibly.
const StateVersion = 1

const (
	stateDirPerms  = 0o750
	stateFilePerms = 0o600
)

// State is the durable part of the tracker.
// Digests and change points are deliberately not persisted; they are rebuilt
// by the scan performed at startup.
type State struct {
	Version     int                               `json:"version"`
	LastSuccess map[target.ID]LastSuccessfulBuild `json:"lastSuccessfulBuild"`
	LastRun     map[target.ID]LastRun             `json:"lastRun"`
}

// Export returns the durable state of the tracker.
func (t *Tracker) Export() State {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := State{
		Version:     StateVersion,
		LastSuccess: make(map[target.ID]LastSuccessfulBuild, len(t.lastSuccess)),
		LastRun:     make(map[target.ID]LastRun, len(t.lastRun)),
	}

	for id, v := range t.lastSuccess {
		s.LastSuccess[id] = v
	}

	for id, v := range t.lastRun {
		s.LastRun[id] = v
	}

	return s
}

// Import installs durable state into the tracker, replacing what is there.
func (t *Tracker) Import(s State) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.lastSuccess = map[target.ID]LastSuccessfulBuild{}
	t.lastRun = map[target.ID]LastRun{}

	for id, v := range s.LastSuccess {
		t.lastSuccess[id] = v
	}

	for id, v := range s.LastRun {
		t.lastRun[id] = v
	}
}

// LoadState reads the state file. A missing file yields an empty state.
// A file written by an incompatible version is ignored (everything rebuilds).
func LoadState(file string) (State, error) {
	data, err := os.ReadFile(file)
	if os.IsNotExist(err) {
		return State{Version: StateVersion}, nil
	} else if err != nil {
		return State{}, errors.AddContext(err, "could not read watcher state '%v'", file)
	}

	var s State
	if e := json.Unmarshal(data, &s); e != nil {
		return State{}, errors.AddContext(e, "could not parse watcher state '%v'", file)
	}

	if s.Version != StateVersion {
		return State{Version: StateVersion}, nil
	}

	return s, nil
}

// SaveState writes the state atomically (temp file + rename).
func SaveState(file string, s State) error {
	if err := os.MkdirAll(path.Dir(file), stateDirPerms); err != nil {
		return errors.AddContext(err, "could not create dir for watcher state '%v'", file)
	}

	data, err := json.Marshal(s)
	if err != nil {
		return errors.AddContext(err, "could not marshal watcher state")
	}

	tmp := file + ".tmp"
	if e := os.WriteFile(tmp, data, stateFilePerms); e != nil {
		return errors.AddContext(e, "could not write watcher state '%v'", tmp)
	}

	if e := os.Rename(tmp, file); e != nil {
		return errors.AddContext(e, "could not move watcher state to '%v'", file)
	}

	return nil
}
