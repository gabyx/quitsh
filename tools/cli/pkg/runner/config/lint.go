package config

import "github.com/gabyx/quitsh/pkg/runner/config"

type LintSettings = config.LintSettings

// NewLintSettings constructs a new build setting.
func NewLintSettings() LintSettings {
	return LintSettings{}
}
