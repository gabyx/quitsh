package watcher

import (
	"encoding/json"
	"os"
	"path"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	fs "github.com/sdsc-ordes/quitsh/pkg/filesystem"
	"github.com/sdsc-ordes/quitsh/pkg/log"
)

// StateVersion is bumped whenever the on-disk format changes incompatibly.
const StateVersion = 1

// State is the durable part of the tracker.
// Digests and change points are deliberately not persisted; they are rebuilt
// by the scan performed at startup.
type State struct {
	Version     int                          `json:"version"`
	LastSuccess map[target.ID]LastRunSuccess `json:"lastSuccessfulBuild"`
	LastRun     map[target.ID]LastRun        `json:"lastRun"`
}

// Export returns the durable state of the tracker.
func (t *Tracker) Export() State {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := State{
		Version:     StateVersion,
		LastSuccess: make(map[target.ID]LastRunSuccess, len(t.lastSuccessfulRun)),
		LastRun:     make(map[target.ID]LastRun, len(t.lastRun)),
	}

	for id, v := range t.lastSuccessfulRun {
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

	t.lastSuccessfulRun = map[target.ID]LastRunSuccess{}
	t.lastRun = map[target.ID]LastRun{}

	for id, v := range s.LastSuccess {
		t.lastSuccessfulRun[id] = v
	}

	for id, v := range s.LastRun {
		t.lastRun[id] = v
	}
}

// LoadState reads the state file. A missing file yields an empty state.
// A file written by an incompatible version is ignored (everything rebuilds).
func LoadState(file string) (State, error) {
	if !fs.Exists(file) {
		return State{Version: StateVersion}, nil
	}

	data, err := os.ReadFile(file)
	if err != nil {
		return State{}, errors.AddContext(err, "could not read watcher state '%v'", file)
	}

	var s State
	if e := json.Unmarshal(data, &s); e != nil {
		return State{}, errors.AddContext(e, "could not parse watcher state '%v'", file)
	}

	if s.Version != StateVersion {
		log.Info("Incompatible state file in '$file'... (ignored)")

		return State{Version: StateVersion}, nil
	}

	return s, nil
}

// SaveState writes the state atomically (temp file + rename).
func SaveState(file string, s State) error {
	log.Infof("Save state to '%v'.", file)
	if err := os.MkdirAll(path.Dir(file), fs.DefaultPermissionsDir); err != nil {
		return errors.AddContext(err, "could not create dir for watcher state '%v'", file)
	}

	data, err := json.Marshal(s)
	if err != nil {
		return errors.AddContext(err, "could not marshal watcher state")
	}

	tmp := file + ".tmp"
	if e := os.WriteFile(tmp, data, fs.DefaultPermissionsFile); e != nil {
		return errors.AddContext(e, "could not write watcher state '%v'", tmp)
	}

	if e := os.Rename(tmp, file); e != nil {
		return errors.AddContext(e, "could not move watcher state to '%v'", file)
	}

	return nil
}
