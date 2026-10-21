package trivyrunner

import (
	"github.com/gabyx/quitsh/pkg/component/stage"
	"github.com/gabyx/quitsh/pkg/component/step"
	"github.com/gabyx/quitsh/pkg/errors"
	"github.com/gabyx/quitsh/pkg/log"
	"github.com/gabyx/quitsh/pkg/runner"
	"github.com/gabyx/quitsh/pkg/runner/config"
	"github.com/gabyx/quitsh/pkg/runner/factory"
)

// Register registers the runners in the factory.
func Register(
	lintSettings *config.LintSettings,
	factory factory.IFactory,
) (err error) {
	log.Trace("Register runner.", "id", TrivyLintRunnerID)
	e := factory.Register(
		TrivyLintRunnerID,
		runner.RunnerData{
			Creator: func(config step.AuxConfig) (runner.IRunner, error) {
				return NewTrivyLintRunner(config, lintSettings)
			},
			RunnerConfigUnmarshal: UnmarshalTrivyConfig,
			DefaultToolchain:      "lint-trivy",
		})

	err = errors.Combine(err, e)
	e = factory.RegisterToKey(runner.NewRegisterKey(stage.Lint, "trivy"), TrivyLintRunnerID)
	err = errors.Combine(err, e)

	return err
}
