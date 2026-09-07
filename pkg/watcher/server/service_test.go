//go:build test && (test_small || test_all)

package server

import (
	"context"
	"path"
	"testing"
	"time"

	watcherv1 "github.com/sdsc-ordes/quitsh/pkg/watcher/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// serve starts a server on a unix socket and returns a connected client.
func serve(t *testing.T) (watcherv1.WatcherClient, string) {
	t.Helper()

	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	address := "unix://" + path.Join(t.TempDir(), "w.sock")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- s.Serve(ctx, address, "test") }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return watcherv1.NewWatcherClient(conn), root
}

func TestGetStatusReportsDirtyTarget(t *testing.T) {
	t.Parallel()
	client, _ := serve(t)

	resp, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{tgtBuild},
	})
	require.NoError(t, err)

	require.Len(t, resp.GetStatuses(), 1)
	assert.True(t, resp.GetStatuses()[0].GetDirty())
	assert.False(t, resp.GetStatuses()[0].GetHasSuccessfulBuild())
	assert.NotZero(t, resp.GetScanId())
}

func TestReportResultMakesTargetClean(t *testing.T) {
	t.Parallel()
	client, _ := serve(t)

	first, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{tgtBuild},
	})
	require.NoError(t, err)

	rep, err := client.ReportResult(t.Context(), &watcherv1.ReportResultRequest{
		ScanId: first.GetScanId(),
		Results: []*watcherv1.TargetResult{
			{TargetId: tgtBuild, Status: watcherv1.Status_STATUS_SUCCESS},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, rep.GetNotRecorded())

	second, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{tgtBuild},
	})
	require.NoError(t, err)
	assert.False(t, second.GetStatuses()[0].GetDirty())
}

func TestResetMakesTargetDirtyAgain(t *testing.T) {
	t.Parallel()
	client, _ := serve(t)

	first, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{tgtBuild},
	})
	require.NoError(t, err)

	_, err = client.ReportResult(t.Context(), &watcherv1.ReportResultRequest{
		ScanId: first.GetScanId(),
		Results: []*watcherv1.TargetResult{
			{TargetId: tgtBuild, Status: watcherv1.Status_STATUS_SUCCESS},
		},
	})
	require.NoError(t, err)

	_, err = client.Reset(t.Context(), &watcherv1.ResetRequest{
		TargetIds: []string{tgtBuild},
	})
	require.NoError(t, err)

	after, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{tgtBuild},
	})
	require.NoError(t, err)
	assert.True(t, after.GetStatuses()[0].GetDirty())
}

func TestInfoReportsRootAndVersion(t *testing.T) {
	t.Parallel()
	client, root := serve(t)

	info, err := client.Info(t.Context(), &watcherv1.InfoRequest{})
	require.NoError(t, err)

	assert.Equal(t, "test", info.GetVersion())
	assert.Equal(t, root, info.GetRootDir())
}

func TestListenRefusesWhenAnotherServerOwnsTheSocket(t *testing.T) {
	t.Parallel()

	address := "unix://" + path.Join(t.TempDir(), "taken.sock")

	l, err := Listen(address)
	require.NoError(t, err)
	defer l.Close()

	_, err = Listen(address)
	assert.Error(t, err, "a live socket must not be stolen")
}

func TestListenRemovesStaleSocket(t *testing.T) {
	t.Parallel()

	address := "unix://" + path.Join(t.TempDir(), "stale.sock")

	l, err := Listen(address)
	require.NoError(t, err)
	// Close the listener but leave the socket file behind.
	require.NoError(t, l.Close())

	l2, err := Listen(address)
	require.NoError(t, err, "a stale socket file must be cleaned up")
	require.NoError(t, l2.Close())
}
