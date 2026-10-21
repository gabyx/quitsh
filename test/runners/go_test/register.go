//go:build test

package gorunner

import (
	"github.com/gabyx/quitsh/pkg/component/step"
	"github.com/gabyx/quitsh/pkg/errors"
	"github.com/gabyx/quitsh/pkg/log"
	"github.com/gabyx/quitsh/pkg/runner"
	"github.com/gabyx/quitsh/pkg/runner/factory"
	settings "github.com/gabyx/quitsh/test/runners/settings_test"
)

// Register registers the runners in the factory.
func Register(
	buildSettings *settings.BuildSettings,
	factory factory.IFactory,
) (err error) {
	log.Trace("Register runner.", "id", GoBuildRunnerID)

	e := factory.Register(
		GoBuildRunnerID,
		runner.RunnerData{
			Creator: func(config step.AuxConfig) (runner.IRunner, error) {
				return NewGoBuildRunner(config, buildSettings)
			},
			RunnerConfigUnmarshal: UnmarshalBuildConfig,
			DefaultToolchain:      "go",
		})
	err = errors.Combine(err, e)

	e = factory.RegisterToKey(runner.NewRegisterKey("build", "go-custom"), GoBuildRunnerID)
	err = errors.Combine(err, e)

	e = factory.Register(
		GoLintRunnerID,
		runner.RunnerData{
			Creator: func(config step.AuxConfig) (runner.IRunner, error) {
				return NewGoLintRunner(config)
			},
			DefaultToolchain: "go",
		})
	err = errors.Combine(err, e)
	e = factory.RegisterToKey(runner.NewRegisterKey("lint", "go-custom"), GoLintRunnerID)
	err = errors.Combine(err, e)

	return err
}
