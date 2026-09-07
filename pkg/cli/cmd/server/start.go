package server

import (
	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/cli/general"
	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherserver "github.com/sdsc-ordes/quitsh/pkg/watcher/server"

	"github.com/spf13/cobra"
)

func addServeCmd(cl cli.ICLI, parent *cobra.Command) {
	var address string

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Run the watcher server in the foreground.",
		RunE: func(_ *cobra.Command, _ []string) error {
			return start(cl, address)
		},
	}

	cmd.Flags().StringVar(&address, "address", "",
		"Address to listen on, e.g. 'unix:///run/user/1000/quitsh/a.sock' "+
			"or 'tcp://127.0.0.1:7777' (defaults to a per-repository unix socket).")

	parent.AddCommand(cmd)
}

func start(cl cli.ICLI, address string) error {
	args := &cl.RootArgs().Watcher
	if address == "" {
		address = args.ResolveAddress(cl.RootDir())
	}

	discover := func() ([]*component.Component, error) {
		_, all, _, e := cl.FindComponents(
			&general.ComponentArgs{ComponentPatterns: []string{"*"}})

		return all, e
	}

	srv, err := watcherserver.New(args, cl.RootDir(), cl.ConfigFilename(), discover)
	if err != nil {
		return err
	}

	log.Info("Starting watcher.",
		"root", cl.RootDir(),
		"address", address,
		"interval", args.ResolveScanInterval(),
		"hashMode", args.ResolveHashMode())

	return srv.Serve(cl.Ctx(), address, watcher.ProtocolVersion)
}
