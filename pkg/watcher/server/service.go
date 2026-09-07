package server

import (
	"context"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherv1 "github.com/sdsc-ordes/quitsh/pkg/watcher/proto"
	"golang.org/x/sync/errgroup"

	"google.golang.org/grpc"
)

// pinTTL is how long a handed-out scan id stays resolvable for a reporting
// client. Builds longer than this simply do not get recorded.
const pinTTL = 6 * time.Hour

type service struct {
	watcherv1.UnimplementedWatcherServer

	srv      *Server
	version  string
	shutdown context.CancelFunc
}

// Serve runs the gRPC service and the scan loop until `ctx` is done.
func (s *Server) Serve(ctx context.Context, address string, version string) error {
	listener, err := Listen(address)
	if err != nil {
		return errors.AddContext(err, "Could not listen on '%v'.", address)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	grpcServer := grpc.NewServer()
	watcherv1.RegisterWatcherServer(grpcServer, &service{
		srv:      s,
		version:  version,
		shutdown: cancel,
	})

	errG, ctx := errgroup.WithContext(ctx)

	errG.Go(func() error { return s.Run(ctx) })
	errG.Go(func() error { return grpcServer.Serve(listener) })
	errG.Go(func() error {
		<-ctx.Done()
		grpcServer.GracefulStop()

		return nil
	})

	log.Info("Watcher server listening.", "address", address, "root", s.rootDir)

	err = errG.Wait()

	log.Info("Watcher server stopped.")

	return err
}

func (s *service) GetStatus(
	ctx context.Context,
	req *watcherv1.GetStatusRequest,
) (*watcherv1.GetStatusResponse, error) {
	notBefore := time.Now().Add(-time.Duration(req.GetMaxAgeMs()) * time.Millisecond)

	scanID, err := s.srv.ensureFresh(ctx, notBefore)
	if err != nil {
		return nil, err
	}

	ids := make([]target.ID, 0, len(req.GetTargetIds()))
	for _, id := range req.GetTargetIds() {
		ids = append(ids, target.ID(id))
	}

	statuses := s.srv.Tracker().Status(ids)

	// Keep the handed-out scan id resolvable until the client reports.
	s.srv.Tracker().Pin(scanID)
	time.AfterFunc(pinTTL,
		func() {
			s.srv.Tracker().Unpin(scanID)
		})

	stats := s.srv.Stats()
	resp := &watcherv1.GetStatusResponse{
		ScanId:          int64(scanID),
		ScannedAtUnixMs: stats.StartedAt.UnixMilli(),
		Statuses:        make([]*watcherv1.TargetStatus, 0, len(statuses)),
	}

	for i := range statuses {
		resp.Statuses = append(resp.Statuses, toProtoStatus(&statuses[i]))
	}

	return resp, nil
}

func toProtoStatus(st *watcher.TargetStatus) *watcherv1.TargetStatus {
	p := &watcherv1.TargetStatus{
		TargetId:           st.ID.String(),
		Dirty:              st.Dirty,
		HasSuccessfulBuild: st.HasSuccessfulBuild,
		DirtyInputs:        make([]string, 0, len(st.DirtyInputs)),
	}

	for _, in := range st.DirtyInputs {
		p.DirtyInputs = append(p.DirtyInputs, string(in))
	}

	if st.LastRun != nil {
		status := watcherv1.Status_STATUS_FAILED
		if st.LastRun.Success {
			status = watcherv1.Status_STATUS_SUCCESS
		}

		p.LastRun = &watcherv1.LastRun{
			Status:   status,
			ScanId:   int64(st.LastRun.ScanID),
			AtUnixMs: st.LastRun.AtUnixMs,
		}
	}

	return p
}

func (s *service) ReportResult(
	_ context.Context,
	req *watcherv1.ReportResultRequest,
) (*watcherv1.ReportResultResponse, error) {
	targetsSucc := make(map[target.ID]bool, len(req.GetResults()))

	for _, r := range req.GetResults() {
		targetsSucc[target.ID(r.GetTargetId())] = r.GetStatus() == watcherv1.Status_STATUS_SUCCESS
	}

	notRecorded := s.srv.Tracker().Report(watcher.ScanID(req.GetScanId()), targetsSucc)

	resp := &watcherv1.ReportResultResponse{
		NotRecorded: make([]string, 0, len(notRecorded)),
	}
	for _, id := range notRecorded {
		resp.NotRecorded = append(resp.NotRecorded, id.String())
	}

	return resp, nil
}

func (s *service) Rescan(
	ctx context.Context,
	_ *watcherv1.RescanRequest,
) (*watcherv1.RescanResponse, error) {
	id, err := s.srv.ensureFresh(ctx, time.Now())
	if err != nil {
		return nil, err
	}

	return &watcherv1.RescanResponse{ScanId: int64(id)}, nil
}

func (s *service) Reset(
	_ context.Context,
	req *watcherv1.ResetRequest,
) (*watcherv1.ResetResponse, error) {
	ids := make([]target.ID, 0, len(req.GetTargetIds()))
	for _, id := range req.GetTargetIds() {
		ids = append(ids, target.ID(id))
	}

	s.srv.Tracker().Reset(ids...)

	return &watcherv1.ResetResponse{}, nil
}

func (s *service) Info(
	_ context.Context,
	_ *watcherv1.InfoRequest,
) (*watcherv1.InfoResponse, error) {
	stats := s.srv.Stats()

	return &watcherv1.InfoResponse{
		Version:            s.version,
		RootDir:            stats.RootDir,
		LastScanId:         int64(stats.ScanID),
		LastScanAtUnixMs:   stats.StartedAt.UnixMilli(),
		LastScanDurationMs: stats.Duration.Milliseconds(),
		TargetCount:        int64(stats.Targets),
		FileCount:          int64(stats.Files),
	}, nil
}

func (s *service) Shutdown(
	_ context.Context,
	_ *watcherv1.ShutdownRequest,
) (*watcherv1.ShutdownResponse, error) {
	log.Info("Shutdown requested.")
	s.shutdown()

	return &watcherv1.ShutdownResponse{}, nil
}
