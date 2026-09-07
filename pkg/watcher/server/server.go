// Package server implements the `quitsh server` change-tracking watcher.
package server

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/dag"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	"github.com/sdsc-ordes/quitsh/pkg/watcher/scan"
)

// stateFlushInterval is how often durable state is written while running.
const stateFlushInterval = 30 * time.Second

type (
	// DiscoverFunc finds all components in the repository.
	DiscoverFunc func() ([]*component.Component, error)

	// Stats describes the last scan, for `Info` and `quitsh server status`.
	Stats struct {
		RootDir   string
		ScanID    watcher.ScanID
		StartedAt time.Time
		Duration  time.Duration
		Files     int
		Targets   int
	}

	// Server owns the tracker and the scan loop.
	// It is the only writer of tracker state.
	Server struct {
		args           *watcher.Args
		rootDir        string
		configFileName string
		discover       DiscoverFunc
		stateFile      string

		tracker *watcher.Tracker
		scanner *scan.Scanner

		mu          sync.Mutex
		files       map[string]watcher.Stamp
		configs     map[string]watcher.Stamp
		lastScanID  watcher.ScanID
		startedAt   time.Time
		duration    time.Duration
		targetCount int
		scanning    bool
		scanDone    chan struct{}
	}
)

// New creates a server for the repository at `rootDir`.
// The loaded durable state is installed immediately; the first scan (done by
// [Server.scanOnce] or [Server.Run]) then decides what is dirty.
func New(
	args *watcher.Args,
	rootDir string,
	configFileName string,
	discover DiscoverFunc,
) (*Server, error) {
	hasher, err := watcher.NewHasher(args.ResolveHashMode())
	if err != nil {
		return nil, err
	}

	scanner, err := scan.New(rootDir, hasher, args.ResolveExcludes())
	if err != nil {
		return nil, err
	}

	stateFile := args.ResolveStateFile(rootDir)

	state, err := watcher.LoadState(stateFile)
	if err != nil {
		// A broken state file must not stop the server; everything is dirty.
		log.WarnE(err, "Could not load watcher state, starting empty.", "file", stateFile)
		state = watcher.State{Version: watcher.StateVersion}
	}

	tracker := watcher.NewTracker()
	tracker.Import(state)

	return &Server{
		args:           args,
		rootDir:        rootDir,
		configFileName: configFileName,
		discover:       discover,
		stateFile:      stateFile,
		tracker:        tracker,
		scanner:        scanner,
		configs:        map[string]watcher.Stamp{},
	}, nil
}

// Tracker returns the tracker owned by this server.
func (s *Server) Tracker() *watcher.Tracker {
	return s.tracker
}

// RootDir returns the repository root this server watches.
func (s *Server) RootDir() string {
	return s.rootDir
}

// Stats returns information about the last scan.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		RootDir:   s.rootDir,
		ScanID:    s.lastScanID,
		StartedAt: s.startedAt,
		Duration:  s.duration,
		Files:     len(s.files),
		Targets:   s.targetCount,
	}
}

// scanOnce performs one scan, re-discovering components when any component
// config file changed, and installs the result into the tracker.
func (s *Server) scanOnce() (watcher.ScanID, error) {
	started := time.Now()
	log.Infof("Scan started.")

	s.mu.Lock()
	prev := s.files
	s.mu.Unlock()

	files, err := s.scanner.Scan(prev)
	if err != nil {
		return 0, err
	}

	configs := s.collectConfigs(files)

	s.mu.Lock()
	changedConfigs := !sameStamps(s.configs, configs)
	s.configs = configs
	s.mu.Unlock()

	if changedConfigs {
		if e := s.rediscover(); e != nil {
			return 0, e
		}
	}

	s.mu.Lock()
	s.lastScanID++
	id := s.lastScanID
	s.files = files
	s.startedAt = started
	s.duration = time.Since(started)
	s.mu.Unlock()

	s.tracker.Update(id, files)

	log.Info("Scan done.", "scan", id, "files", len(files), "took", time.Since(started))

	return id, nil
}

// ensureFresh returns the id of a scan which started at or after `notBefore`,
// running or joining a scan when needed.
func (s *Server) ensureFresh(ctx context.Context, notBefore time.Time) (watcher.ScanID, error) {
	for {
		s.mu.Lock()

		if s.lastScanID != 0 && !s.startedAt.Before(notBefore) {
			id := s.lastScanID
			s.mu.Unlock()

			return id, nil
		}

		// Check if we are already scanning, if yes join the scan.
		if s.scanning {
			done := s.scanDone
			s.mu.Unlock()

			select {
			case <-done:
			case <-ctx.Done():
				return 0, ctx.Err()
			}

			// renter the function to determine the outcome
			continue
		}

		// else... start the scan.

		s.scanning = true
		s.scanDone = make(chan struct{})
		done := s.scanDone
		s.mu.Unlock()

		id, err := s.scanOnce()

		s.mu.Lock()
		s.scanning = false
		s.mu.Unlock()
		close(done)

		return id, errors.AddContext(err, "could not scan once")
	}
}

// Run scans periodically and flushes durable state until `ctx` is done.
func (s *Server) Run(ctx context.Context) error {
	scanTicker := time.NewTicker(s.args.ResolveScanInterval())
	defer scanTicker.Stop()

	flushTicker := time.NewTicker(stateFlushInterval)
	defer flushTicker.Stop()

	if _, err := s.ensureFresh(ctx, time.Now()); err != nil {
		if ctx.Err() != nil {
			return s.SaveState()
		}

		return err
	}

	for {
		select {
		case <-ctx.Done():
			return s.SaveState()

		case <-scanTicker.C:
			if _, err := s.ensureFresh(ctx, time.Now()); err != nil {
				if ctx.Err() != nil {
					return s.SaveState()
				}
				log.WarnE(err, "Scan failed.")
			}

		case <-flushTicker.C:
			log.WarnE(s.SaveState(), "Could not save state.")
		}
	}
}

// SaveState writes the durable state to disk.
func (s *Server) SaveState() error {
	return watcher.SaveState(s.stateFile, s.tracker.Export())
}

// rediscover reloads components and installs their input sets into the tracker.
func (s *Server) rediscover() error {
	comps, err := s.discover()
	if err != nil {
		return errors.AddContext(err, "could not discover components")
	}

	targets, inputs, err := dag.ResolveTargetInputs(comps, s.rootDir)
	if err != nil {
		return err
	}

	sets := make(watcher.InputSets, len(inputs))

	for id, cfg := range inputs {
		set, e := watcher.NewInputSet(cfg, s.rootDir)
		if e != nil {
			return e
		}
		sets[id] = set
	}

	s.tracker.SetComponents(sets, targets)

	s.mu.Lock()
	s.targetCount = len(targets)
	s.mu.Unlock()

	log.Info("Components discovered.", "components", len(comps), "targets", len(targets))

	return nil
}

// collectConfigs returns the stamps of all component config files.
func (s *Server) collectConfigs(files map[string]watcher.Stamp) map[string]watcher.Stamp {
	configs := make(map[string]watcher.Stamp)

	for p, stamp := range files {
		if path.Base(p) == s.configFileName {
			configs[p] = stamp
		}
	}

	return configs
}

func sameStamps(a map[string]watcher.Stamp, b map[string]watcher.Stamp) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if o, ok := b[k]; !ok || o != v {
			return false
		}
	}

	return true
}
