//go:build test && (test_small || test_all)

package watcher

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	fileAGo = "a.go"
	fileBGo = "b.go"
)

func TestMTimeSizeHasherIgnoresContent(t *testing.T) {
	t.Parallel()
	h, err := NewHasher(HashModeMTimeSize)
	require.NoError(t, err)

	s, err := h.Stamp("/does/not/matter", 42, 7, Stamp{}, false)
	require.NoError(t, err)
	assert.Equal(t, Stamp{ModTimeNs: 42, Size: 7}, s)
}

func TestChecksumHasherReadsContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := path.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("hello"), 0o600))

	h, err := NewHasher(HashModeChecksum)
	require.NoError(t, err)

	s, err := h.Stamp(p, 1, 5, Stamp{}, false)
	require.NoError(t, err)
	assert.NotZero(t, s.Sum)

	// Same mtime+size -> reuses the previous stamp without reading.
	reused, err := h.Stamp("/deleted/by/now", 1, 5, s, true)
	require.NoError(t, err)
	assert.Equal(t, s, reused)
}

func TestDigestOfIsOrderStableAndContentSensitive(t *testing.T) {
	t.Parallel()
	stamps := map[string]Stamp{
		fileAGo: {ModTimeNs: 1, Size: 2},
		fileBGo: {ModTimeNs: 3, Size: 4},
	}
	d1 := DigestOf([]string{fileAGo, fileBGo}, stamps)
	d2 := DigestOf([]string{fileAGo, fileBGo}, stamps)
	assert.Equal(t, d1, d2)

	stamps[fileBGo] = Stamp{ModTimeNs: 9, Size: 4}
	assert.NotEqual(t, d1, DigestOf([]string{fileAGo, fileBGo}, stamps))
}

func TestUnknownHashMode(t *testing.T) {
	t.Parallel()
	_, err := NewHasher("banana")
	assert.Error(t, err)
}
