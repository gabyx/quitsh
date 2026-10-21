package yttx

import "github.com/gabyx/quitsh/pkg/exec"

// NewCtxBuilder returns a new YTT command context builder.
func NewCtxBuilder() exec.CmdContextBuilder {
	return exec.NewCmdCtxBuilder().BaseCmd("ytt")
}
