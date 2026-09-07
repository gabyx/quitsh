//go:build test && (test_small || test_all)

package server

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	configFileName = ".component.yaml"
	tgtBuild       = "comp-a::build"
)

func testRepo(t *testing.T) (root string, discover DiscoverFunc) {
	t.Helper()
	root = t.TempDir()

	require.NoError(t, os.MkdirAll(path.Join(root, "comp-a/src"), 0o750))
	require.NoError(t, os.WriteFile(
		path.Join(root, "comp-a", configFileName), []byte("name: comp-a\n"), 0o600))
	require.NoError(t, os.WriteFile(
		path.Join(root, "comp-a/src/a.go"), []byte("package a"), 0o600))

	discover = func() ([]*component.Component, error) {
		conf := &component.Config{
			Name:     "comp-a",
			Language: "go",
			Inputs: map[string]*input.Config{
				"srcs": {Patterns: []string{`^src/.*\.go$`}},
			},
			Targets: map[string]*target.Config{
				"build": {Stage: "build", Inputs: []input.ID{"self::srcs"}},
			},
		}
		if err := conf.Init(); err != nil {
			return nil, err
		}
		c := component.NewComponent(conf, path.Join(root, "comp-a"), "", "")

		return []*component.Component{&c}, nil
	}

	return root, discover
}

func newServer(t *testing.T, root string, discover DiscoverFunc) *Server {
	t.Helper()
	args := &watcher.Args{
		ScanInterval: 10 * time.Millisecond,
		HashMode:     watcher.HashModeMTimeSize,
		StateFile:    path.Join(t.TempDir(), "state.json"),
	}
	s, err := New(args, root, configFileName, discover)
	require.NoError(t, err)

	return s
}

func TestScanOnceIncrementsScanID(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	first, err := s.ScanOnce()
	require.NoError(t, err)
	second, err := s.ScanOnce()
	require.NoError(t, err)

	assert.Equal(t, watcher.ScanID(1), first)
	assert.Equal(t, watcher.ScanID(2), second)
}

func TestServerTracksTargetOfDiscoveredComponents(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	_, err := s.ScanOnce()
	require.NoError(t, err)

	st := s.Tracker().Status([]target.ID{tgtBuild})
	require.Len(t, st, 1)
	assert.True(t, st[0].Dirty, "never built -> dirty")
	assert.False(t, st[0].HasSuccessfulBuild)
}

func TestEnsureFreshRunsAScanStartedAfterTheRequest(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	_, err := s.ScanOnce()
	require.NoError(t, err)

	notBefore := time.Now()
	id, err := s.EnsureFresh(t.Context(), notBefore)
	require.NoError(t, err)

	assert.Equal(t, watcher.ScanID(2), id)
	assert.False(t, s.Stats().StartedAt.Before(notBefore))
}

func TestEnsureFreshReusesAFreshEnoughScan(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	id, err := s.ScanOnce()
	require.NoError(t, err)

	// A scan that already started after this instant satisfies the request.
	got, err := s.EnsureFresh(t.Context(), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	assert.Equal(t, id, got)
}

func TestComponentConfigChangeTriggersRediscovery(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)

	calls := 0
	counting := func() ([]*component.Component, error) {
		calls++

		return discover()
	}

	s := newServer(t, root, counting)
	_, err := s.ScanOnce()
	require.NoError(t, err)
	before := calls
	require.Positive(t, before, "the first scan must discover components")

	_, err = s.ScanOnce()
	require.NoError(t, err)
	assert.Equal(t, before, calls, "unchanged configs must not re-discover")

	require.NoError(t, os.WriteFile(
		path.Join(root, "comp-a", configFileName),
		[]byte("name: comp-a\n# touched\n"), 0o600))

	_, err = s.ScanOnce()
	require.NoError(t, err)
	assert.Greater(t, calls, before)
}

func TestRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
