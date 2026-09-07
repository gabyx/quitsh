package watcher

import "github.com/sdsc-ordes/quitsh/pkg/config"

// ArgsSelector returns the watcher settings out of the user's config.
// It mirrors `nixtoolchain.ArgsSelector`: the config is unmarshalled after
// `cli.New`, so the settings must be fetched lazily.
type ArgsSelector func(config.IConfig) *Args
