package servercmd

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	watcherclient "github.com/sdsc-ordes/quitsh/pkg/watcher/client"

	"github.com/spf13/cobra"
)

// staleMaxAge is the freshness allowed by `--stale-ok`.
const staleMaxAge = time.Hour

func addStatusCmd(cl cli.ICLI, parent *cobra.Command) {
	var (
		dirtyOnly bool
		staleOk   bool
	)

	cmd := &cobra.Command{
		Use:   "status [target-ids...]",
		Short: "Show which targets are out of date.",
		RunE: func(_ *cobra.Command, ids []string) error {
			return status(cl, ids, dirtyOnly, staleOk)
		},
	}

	cmd.Flags().BoolVar(&dirtyOnly, "dirty-only", false, "Only list out-of-date targets.")
	cmd.Flags().BoolVar(&staleOk, "stale-ok", false,
		"Answer from the last scan instead of forcing a fresh one.")

	parent.AddCommand(cmd)
}

func status(cl cli.ICLI, rawIDs []string, dirtyOnly bool, staleOk bool) error {
	c, err := dial(cl)
	if err != nil {
		return err
	}
	defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

	info, err := c.Info(cl.Ctx())
	if err != nil {
		return err
	}

	ids := make([]target.ID, 0, len(rawIDs))
	for _, id := range rawIDs {
		ids = append(ids, target.ID(id))
	}

	maxAge := time.Duration(0)
	if staleOk {
		maxAge = staleMaxAge
	}

	res, err := c.Status(cl.Ctx(), ids, maxAge)
	if err != nil {
		return err
	}

	log.Info("Watcher.",
		"root", info.GetRootDir(),
		"scan", res.ScanID,
		"targets", info.GetTargetCount(),
		"files", info.GetFileCount(),
		"lastScan", time.UnixMilli(info.GetLastScanAtUnixMs()).Format(time.RFC3339),
		"scanTook", time.Duration(info.GetLastScanDurationMs())*time.Millisecond)

	sorted := make([]target.ID, 0, len(res.Dirty))
	for id := range res.Dirty {
		sorted = append(sorted, id)
	}

	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for _, id := range sorted {
		dirty := res.Dirty[id]
		if dirtyOnly && !dirty {
			continue
		}

		fmt.Fprintf(os.Stdout, "%v %v%v\n", mark(dirty), id, reason(&res, id))
	}

	return nil
}

func mark(dirty bool) string {
	if dirty {
		return "●"
	}

	return "○"
}

func reason(res *watcherclient.Result, id target.ID) string {
	if !res.Dirty[id] {
		return ""
	}

	if !res.HasBuild[id] {
		return "  (never built successfully)"
	}

	if inputs := res.DirtyInputs[id]; len(inputs) != 0 {
		return fmt.Sprintf("  (changed inputs: %v)", inputs)
	}

	return "  (changed)"
}
