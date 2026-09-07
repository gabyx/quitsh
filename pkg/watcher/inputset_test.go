//go:build test && (test_small || test_all)

package watcher

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/input"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSet(t *testing.T, id string, baseDir string, patterns ...string) *InputSet {
	t.Helper()
	cfg := &input.Config{Patterns: patterns, BaseDir: baseDir}
	cfg.Init(input.ID(id))
	s, err := NewInputSet(cfg, "/repo")
	require.NoError(t, err)

	return s
}

func TestInputSetMatchesRelativeToBaseDir(t *testing.T) {
	t.Parallel()
	s := newSet(t, inSrcs, "/repo/comp", `^src/.*\.go$`)

	assert.True(t, s.Matches("comp/src/a.go"))
	assert.False(t, s.Matches("comp/src/a.txt"))
	assert.False(t, s.Matches("other/src/a.go"))
}

func TestInputSetHonoursExcludePatterns(t *testing.T) {
	t.Parallel()
	s := newSet(t, inSrcs, "/repo/comp", `^src/.*\.go$`, `!^src/gen/.*$`)

	assert.True(t, s.Matches("comp/src/a.go"))
	assert.False(t, s.Matches("comp/src/gen/a.go"))
}

func TestInputSetRelativeToRootMatchesEverywhere(t *testing.T) {
	t.Parallel()
	s := newSet(t, "comp::all", "/repo", `^.*\.nix$`)

	assert.True(t, s.Matches("tools/nix/flake.nix"))
	assert.False(t, s.Matches("tools/nix/flake.lock"))
}

func TestInputSetsAssignSortsPaths(t *testing.T) {
	t.Parallel()
	sets := InputSets{
		inSrcs:       newSet(t, inSrcs, "/repo/comp", `^src/.*\.go$`),
		"comp::docs": newSet(t, "comp::docs", "/repo/comp", `^docs/.*$`),
	}

	files := map[string]Stamp{
		"comp/src/b.go":  {Size: 1},
		"comp/src/a.go":  {Size: 2},
		"comp/docs/x.md": {Size: 3},
		"unrelated.txt":  {Size: 4},
	}

	got := sets.Assign(files)

	assert.Equal(t, []string{"comp/src/a.go", "comp/src/b.go"}, got[inSrcs])
	assert.Equal(t, []string{"comp/docs/x.md"}, got["comp::docs"])
}
