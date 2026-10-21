package coverage

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
	testSettings config.ITestSettings,
	factory factory.IFactory,
) (err error) {
	log.Trace("Register runner.", "id", CoverageUploadRunnerID)
	e := factory.Register(
		CoverageUploadRunnerID,
		runner.RunnerData{
			Creator: func(config step.AuxConfig) (runner.IRunner, error) {
				return NewCodecovRunner(config, testSettings)
			},
			RunnerConfigUnmarshal: UnmarshalCodecovConfig,
			DefaultToolchain:      "coverage-upload",
		})

	err = errors.Combine(err, e)
	e = factory.RegisterToKey(runner.NewRegisterKey(stage.Test,
		"coverage-upload"), CoverageUploadRunnerID)
	err = errors.Combine(err, e)
	e = factory.RegisterToKey(runner.NewRegisterKey(stage.Coverage,
		"coverage-upload"), CoverageUploadRunnerID)
	err = errors.Combine(err, e)

	return err
}
