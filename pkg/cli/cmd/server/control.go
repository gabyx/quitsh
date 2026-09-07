package servercmd

import (
	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"

	"github.com/spf13/cobra"
)

func addControlCmds(cl cli.ICLI, parent *cobra.Command) {
	parent.AddCommand(&cobra.Command{
		Use:   "stop",
		Short: "Stop the running watcher server.",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := dial(cl)
			if err != nil {
				return err
			}
			defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

			return c.Shutdown(cl.Ctx())
		},
	})

	parent.AddCommand(&cobra.Command{
		Use:   "reset [target-ids...]",
		Short: "Forget the last successful build of targets (all when none given).",
		RunE: func(_ *cobra.Command, rawIDs []string) error {
			c, err := dial(cl)
			if err != nil {
				return err
			}
			defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

			ids := make([]target.ID, 0, len(rawIDs))
			for _, id := range rawIDs {
				ids = append(ids, target.ID(id))
			}

			return c.Reset(cl.Ctx(), ids)
		},
	})

	parent.AddCommand(&cobra.Command{
		Use:   "rescan",
		Short: "Force a rescan and print the new scan id.",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := dial(cl)
			if err != nil {
				return err
			}
			defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

			id, e := c.Rescan(cl.Ctx())
			if e != nil {
				return e
			}
			log.Info("Rescanned.", "scan", id)

			return nil
		},
	})
}
