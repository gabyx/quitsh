// Package client talks to the `quitsh server` watcher.
//
// Every helper in this package degrades instead of failing: when the server is
// absent, slow, or speaks another protocol version, callers are told that
// nothing is known, which means every target must be built.
package client

import (
	"context"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherv1 "github.com/sdsc-ordes/quitsh/pkg/watcher/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	// Client is a connection to the watcher server.
	Client struct {
		conn    *grpc.ClientConn
		api     watcherv1.WatcherClient
		timeout time.Duration
	}

	// Result is the answer to a status query.
	// A `nil` `Dirty` map means nothing is known: build everything.
	Result struct {
		ScanID      int64
		Dirty       map[target.ID]bool
		DirtyInputs map[target.ID][]string
		HasBuild    map[target.ID]bool
	}
)

// Dial connects to the watcher server for the repository at `rootDir`.
func Dial(args *watcher.Args, rootDir string) (*Client, error) {
	address := args.ResolveAddress(rootDir)

	conn, err := grpc.NewClient(
		address,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.AddContext(err, "could not connect to watcher '%v'", address)
	}

	return &Client{
		conn:    conn,
		api:     watcherv1.NewWatcherClient(conn),
		timeout: args.ResolveTimeout(),
	}, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, c.timeout)
}

// Info returns the server information.
func (c *Client) Info(ctx context.Context) (*watcherv1.InfoResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	return c.api.Info(ctx, &watcherv1.InfoRequest{})
}

// Status queries the dirty state of `ids` (all targets when empty).
// `maxAge` is how stale the answer may be; zero forces an answer from a scan
// started after this request, which is what a skip decision needs.
func (c *Client) Status(
	ctx context.Context,
	ids []target.ID,
	maxAge time.Duration,
) (Result, error) {
	req := &watcherv1.GetStatusRequest{
		TargetIds: make([]string, 0, len(ids)),
		MaxAgeMs:  maxAge.Milliseconds(),
	}

	for _, id := range ids {
		req.TargetIds = append(req.TargetIds, id.String())
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.api.GetStatus(ctx, req)
	if err != nil {
		return Result{}, errors.AddContext(err, "watcher status query failed")
	}

	res := Result{
		ScanID:      resp.GetScanId(),
		Dirty:       make(map[target.ID]bool, len(resp.GetStatuses())),
		DirtyInputs: make(map[target.ID][]string, len(resp.GetStatuses())),
		HasBuild:    make(map[target.ID]bool, len(resp.GetStatuses())),
	}

	for _, st := range resp.GetStatuses() {
		id := target.ID(st.GetTargetId())
		res.Dirty[id] = st.GetDirty()
		res.DirtyInputs[id] = st.GetDirtyInputs()
		res.HasBuild[id] = st.GetHasSuccessfulBuild()
	}

	return res, nil
}

// Report reports run results decided at `scanID`.
// It returns the targets the server could not record.
func (c *Client) Report(
	ctx context.Context,
	scanID int64,
	results map[target.ID]bool,
) ([]target.ID, error) {
	req := &watcherv1.ReportResultRequest{
		ScanId:  scanID,
		Results: make([]*watcherv1.TargetResult, 0, len(results)),
	}

	for id, success := range results {
		status := watcherv1.Status_STATUS_FAILED
		if success {
			status = watcherv1.Status_STATUS_SUCCESS
		}

		req.Results = append(req.Results,
			&watcherv1.TargetResult{TargetId: id.String(), Status: status})
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.api.ReportResult(ctx, req)
	if err != nil {
		return nil, errors.AddContext(err, "watcher report failed")
	}

	notRecorded := make([]target.ID, 0, len(resp.GetNotRecorded()))
	for _, id := range resp.GetNotRecorded() {
		notRecorded = append(notRecorded, target.ID(id))
	}

	return notRecorded, nil
}

// Rescan forces a scan and returns its id.
func (c *Client) Rescan(ctx context.Context) (int64, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.api.Rescan(ctx, &watcherv1.RescanRequest{})
	if err != nil {
		return 0, err
	}

	return resp.GetScanId(), nil
}

// Reset clears the last successful build of `ids` (all when empty).
func (c *Client) Reset(ctx context.Context, ids []target.ID) error {
	req := &watcherv1.ResetRequest{TargetIds: make([]string, 0, len(ids))}
	for _, id := range ids {
		req.TargetIds = append(req.TargetIds, id.String())
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.api.Reset(ctx, req)

	return err
}

// Shutdown asks the server to stop.
func (c *Client) Shutdown(ctx context.Context) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.api.Shutdown(ctx, &watcherv1.ShutdownRequest{})

	return err
}

// QueryDirty asks the watcher which of `ids` are dirty.
// It never fails: a zero [Result] (with a `nil` `Dirty` map) means the watcher
// could not be consulted and every target must be treated as dirty.
func QueryDirty(
	ctx context.Context,
	args *watcher.Args,
	rootDir string,
	ids []target.ID,
) Result {
	c, err := Dial(args, rootDir)
	if err != nil {
		log.WarnE(err, "Watcher not usable, building everything.")

		return Result{}
	}
	defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

	info, err := c.Info(ctx)
	if err != nil {
		log.Debug("Watcher not reachable, building everything.", "error", err.Error())

		return Result{}
	}

	if info.GetVersion() != watcher.ProtocolVersion {
		log.Warn("Watcher protocol mismatch, building everything.",
			"server", info.GetVersion(), "client", watcher.ProtocolVersion)

		return Result{}
	}

	res, err := c.Status(ctx, ids, 0)
	if err != nil {
		log.WarnE(err, "Watcher status query failed, building everything.")

		return Result{}
	}

	return res
}

// ReportResults reports run results to the watcher. It never fails.
func ReportResults(
	ctx context.Context,
	args *watcher.Args,
	rootDir string,
	scanID int64,
	results map[target.ID]bool,
) {
	if len(results) == 0 {
		return
	}

	c, err := Dial(args, rootDir)
	if err != nil {
		log.WarnE(err, "Could not report results to the watcher.")

		return
	}
	defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

	notRecorded, err := c.Report(ctx, scanID, results)
	if err != nil {
		log.Debug("Could not report results to the watcher.", "error", err.Error())

		return
	}

	if len(notRecorded) != 0 {
		log.Debug("Watcher did not record some targets (scan id too old).",
			"targets", notRecorded)
	}
}
