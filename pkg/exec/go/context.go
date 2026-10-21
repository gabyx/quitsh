package gox

import "github.com/gabyx/quitsh/pkg/exec"

// NewCtxBuilder returns a new Go command context builder.
func NewCtxBuilder() exec.CmdContextBuilder {
	return exec.NewCmdCtxBuilder().BaseCmd("go")
}
