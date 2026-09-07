//go:build test && (test_small || test_all)

package client

import (
	"path"
	"testing"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryDirtyWithoutServerReportsNothingKnown(t *testing.T) {
	t.Parallel()
	args := &watcher.Args{
		Address: "unix://" + path.Join(t.TempDir(), "absent.sock"),
		Timeout: 200 * time.Millisecond,
	}

	res := QueryDirty(t.Context(), args, t.TempDir(), []target.ID{"a::b"})

	assert.Nil(t, res.Dirty, "an unreachable server must mean 'everything dirty'")
	assert.Zero(t, res.ScanID)
}

func TestReportResultsWithoutServerDoesNotPanic(t *testing.T) {
	t.Parallel()
	args := &watcher.Args{
		Address: "unix://" + path.Join(t.TempDir(), "absent.sock"),
		Timeout: 200 * time.Millisecond,
	}

	require.NotPanics(t, func() {
		ReportResults(t.Context(), args, t.TempDir(), 1,
			map[target.ID]bool{"a::b": true})
	})
}
