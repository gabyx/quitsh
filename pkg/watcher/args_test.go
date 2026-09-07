//go:build test && (test_small || test_all)

package watcher

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// A config which never went through `defaults.Set` must still work.
func TestZeroArgsAreUsable(t *testing.T) {
	t.Parallel()
	var a Args

	assert.True(t, a.IsEnabled(), "change tracking must be on by default")
	assert.Equal(t, HashModeMTimeSize, a.ResolveHashMode())
	assert.Equal(t, DefaultScanInterval, a.ResolveScanInterval())
	assert.Equal(t, DefaultTimeout, a.ResolveTimeout())
	assert.NotEmpty(t, a.ResolveExcludes())
	assert.NotEmpty(t, a.ResolveAddress("/repo"))
	assert.NotEmpty(t, a.ResolveStateFile("/repo"))
}

func TestDisabledArgs(t *testing.T) {
	t.Parallel()
	a := Args{Disabled: true}

	assert.False(t, a.IsEnabled())
}

func TestNilArgsAreNotEnabled(t *testing.T) {
	t.Parallel()
	var a *Args

	assert.False(t, a.IsEnabled())
}
