// Package servercmd adds the `quitsh server` change-tracking watcher commands.
package servercmd

import (
	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherclient "github.com/sdsc-ordes/quitsh/pkg/watcher/client"

	"github.com/spf13/cobra"
)

const longDesc = `
Run and control the change-tracking watcher.

The watcher rescans the repository periodically, remembers the input state each
target was last built from, and answers which targets are out of date. Builds
use it automatically; pass '--no-skip' to run everything anyway.
`

// AddCmd adds the 'server' command to 'parent'.
func AddCmd(cl cli.ICLI, parent *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "server",
		Short:        "Run and control the change-tracking watcher.",
		Long:         longDesc,
		SilenceUsage: true,
	}

	addServeCmd(cl, cmd)
	addStatusCmd(cl, cmd)
	addControlCmds(cl, cmd)

	parent.AddCommand(cmd)

	return cmd
}

// watcherArgs returns the configured watcher settings or an error explaining
// how to enable them.
func watcherArgs(cl cli.ICLI) (*watcher.Args, error) {
	args := cl.WatcherArgs()
	if args == nil {
		return nil, errors.New(
			"this CLI was not built with 'cli.WithWatcher(...)', " +
				"so the watcher settings are unknown")
	}

	return args, nil
}

// dial connects to a running watcher server.
func dial(cl cli.ICLI) (*watcherclient.Client, error) {
	args, err := watcherArgs(cl)
	if err != nil {
		return nil, err
	}

	return watcherclient.Dial(args, cl.RootDir())
}
