// Package server adds the `quitsh server` change-tracking watcher commands.
package server

import (
	"errors"

	"github.com/sdsc-ordes/quitsh/pkg/cli"
	watcherclient "github.com/sdsc-ordes/quitsh/pkg/watcher/client"

	"github.com/spf13/cobra"
)

const longDesc = `
Run and control the quitsh server (change-tracking watcher).

The watcher rescans the repository periodically, remembers the input state each
target was last built from, and answers which targets are out of date. Builds
use it automatically; pass '--no-skip' to run everything anyway.
`

// AddCmd adds the 'server' command to 'parent'.
func AddCmd(cl cli.ICLI, parent *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "watcher",
		Short: "Run and control the quitsh watcher (change-tracking etc...)",
		Long:  longDesc,
		RunE: func(cmd *cobra.Command, _args []string) error {
			_ = cmd.Help()

			return errors.New("no command given")
		},
		SilenceUsage: true,
	}

	addServeCmd(cl, cmd)
	addStatusCmd(cl, cmd)
	addControlCmds(cl, cmd)

	parent.AddCommand(cmd)

	return cmd
}

// dial connects to a running watcher server.
func dial(cl cli.ICLI) (*watcherclient.Client, error) {
	return watcherclient.Dial(&cl.RootArgs().Watcher, cl.RootDir())
}
