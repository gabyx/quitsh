package gitdiffrunner

import (
	"github.com/gabyx/quitsh/pkg/component/step"
	"github.com/gabyx/quitsh/pkg/errors"
	"github.com/gabyx/quitsh/pkg/log"
	"github.com/gabyx/quitsh/pkg/runner"
	"github.com/gabyx/quitsh/pkg/runner/config"
	"github.com/gabyx/quitsh/pkg/runner/factory"
)

func Register(
	factory factory.IFactory,
	nixSetts *config.NixSettings,
) (err error) {
	log.Trace("Register runner.", "id", GitDiffRunnerID)

	e := factory.Register(
		GitDiffRunnerID,
		runner.RunnerData{
			Creator: func(config step.AuxConfig) (runner.IRunner, error) {
				return NewGitDiffRunner(config, nixSetts)
			},
			DefaultToolchain:      "git",
			RunnerConfigUnmarshal: UnmarshalDiffConfig,
		})
	err = errors.Combine(err, e)

	for _, s := range factory.Stages() {
		e = factory.RegisterToKey(
			runner.NewRegisterKey(s.Stage, "git-diff"),
			GitDiffRunnerID,
		)
		err = errors.Combine(err, e)
	}

	return
}
