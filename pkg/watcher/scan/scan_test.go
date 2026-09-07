//go:build test && (test_small || test_all)

package scan

import (
	"os"
	"path"
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/watcher"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func write(t *testing.T, root string, rel string, content string) {
	t.Helper()
	p := path.Join(root, rel)
	require.NoError(t, os.MkdirAll(path.Dir(p), 0o750))
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
}

func newScanner(t *testing.T, root string) *Scanner {
	t.Helper()
	h, err := watcher.NewHasher(watcher.HashModeMTimeSize)
	require.NoError(t, err)
	s, err := New(root, h, watcher.DefaultExcludes())
	require.NoError(t, err)

	return s
}

func TestScanReturnsRelativePaths(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "src/a.go", "package a")
	write(t, root, "src/sub/b.go", "package b")

	files, err := newScanner(t, root).Scan(nil)
	require.NoError(t, err)

	assert.Len(t, files, 2)
	assert.Contains(t, files, "src/a.go")
	assert.Contains(t, files, "src/sub/b.go")
}

func TestScanAppliesExcludes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "src/a.go", "package a")
	write(t, root, ".git/objects/deadbeef", "junk")
	write(t, root, "node_modules/x/index.js", "x")

	files, err := newScanner(t, root).Scan(nil)
	require.NoError(t, err)

	assert.Len(t, files, 1)
	assert.Contains(t, files, "src/a.go")
}

func TestScanDetectsChangedFile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "a.txt", "one")

	s := newScanner(t, root)
	first, err := s.Scan(nil)
	require.NoError(t, err)

	write(t, root, "a.txt", "one-plus-more")
	second, err := s.Scan(first)
	require.NoError(t, err)

	assert.NotEqual(t, first["a.txt"], second["a.txt"])
}

func TestScanDropsDeletedFiles(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "a.txt", "one")
	write(t, root, "b.txt", "two")

	s := newScanner(t, root)
	first, err := s.Scan(nil)
	require.NoError(t, err)
	require.Len(t, first, 2)

	require.NoError(t, os.Remove(path.Join(root, "b.txt")))
	second, err := s.Scan(first)
	require.NoError(t, err)

	assert.Len(t, second, 1)
	assert.NotContains(t, second, "b.txt")
}
