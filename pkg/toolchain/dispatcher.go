package toolchain

import (
	"github.com/gabyx/quitsh/pkg/common"
	"github.com/gabyx/quitsh/pkg/component/step"
	"github.com/gabyx/quitsh/pkg/component/target"
	"github.com/gabyx/quitsh/pkg/config"
	"github.com/gabyx/quitsh/pkg/runner"
)

// IDispatcher is the dispatcher interface to
// run the runner over a toolchain.
type IDispatcher interface {
	Run(
		repoDir string,
		dispatchArgs *DispatchArgs,
		config config.IConfig,
	) error
}

type DispatchArgs struct {
	ComponentDir string     `yaml:"componentDir" default:"." validate:"required"`
	TargetID     target.ID  `yaml:"targetID"     default:"" validate:"required"`
	StepIndex    step.Index `yaml:"stepIndex"    default:"-1" validate:"gte=0"`
	RunnerIndex  int        `yaml:"runnerIndex"  default:"-1" validate:"gte=0"`

	RunnerID  runner.RegisterID `yaml:"runnerID"  validate:""`
	Toolchain string            `yaml:"toolchain" validate:""`
}

// Validate validates the dispatch args.
func (c DispatchArgs) Validate() error {
	return common.Validator().Struct(c)
}
