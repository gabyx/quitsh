package watcher

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path"
	"time"
)

// Args are the settings for the watcher server and its clients.
// Embed this into your own config and wire it with `cli.WithWatcher`.
//
// The zero value is usable: change tracking is on and every setting falls back
// to its default, so embedding this in a config which is not run through
// `defaults.Set` still works.
type Args struct {
	// Disabled turns change-tracking off. It is on by default; `--no-skip`
	// switches it off for one invocation.
	//
	// NOTE: This is negated on purpose. The zero value of `Args` must mean
	// "enabled", so that a config which never had defaults applied behaves
	// like the documented default.
	Disabled bool `yaml:"disabled"`

	// Address is a gRPC target, e.g. `unix:///run/user/1000/quitsh/ab12.sock`
	// or `tcp://127.0.0.1:7777`. Empty means the default unix socket.
	Address string `yaml:"address" default:""`

	// StateFile holds `last_successful_build` data.
	// Empty means `<rootDir>/.quitsh/watcher-state.json`.
	StateFile string `yaml:"stateFile" default:""`

	// ScanInterval is the period of the background rescan loop.
	ScanInterval time.Duration `yaml:"scanInterval" default:"2s"`

	// MaxAge is how stale a query answer may be. Zero means the answer must
	// come from a scan started after the request arrived.
	MaxAge time.Duration `yaml:"maxAge" default:"0s"`

	// HashMode selects the file stamping strategy.
	HashMode HashMode `yaml:"hashMode" default:"mtime-size"`

	// Excludes are regexes matched against repo-relative paths.
	// Empty means [DefaultExcludes].
	Excludes []string `yaml:"excludes"`

	// Timeout is the client dial and call budget.
	Timeout time.Duration `yaml:"timeout" default:"2s"`
}

const socketHashLen = 12

// DefaultExcludes returns the paths never tracked by the watcher.
func DefaultExcludes() []string {
	return []string{
		`^\.git($|/)`,
		`^\.output($|/)`,
		`^\.quitsh($|/)`,
		`(^|/)result($|/)`,
		`(^|/)node_modules($|/)`,
		`(^|/)\.direnv($|/)`,
		`(^|/)\.devenv($|/)`,
	}
}

// ResolveAddress returns the gRPC target for the repository at `rootDir`.
// The socket file name is a hash because `sun_path` is limited to 108 characters.
func (a *Args) ResolveAddress(rootDir string) string {
	if a.Address != "" {
		return a.Address
	}

	sum := sha256.Sum256([]byte(rootDir))
	name := hex.EncodeToString(sum[:])[:socketHashLen] + ".sock"

	if dir := os.Getenv("XDG_RUNTIME_DIR"); dir != "" {
		return "unix://" + path.Join(dir, "quitsh", name)
	}

	return "unix://" + path.Join(rootDir, ".quitsh", name)
}

// ResolveStateFile returns the state file path for the repository at `rootDir`.
func (a *Args) ResolveStateFile(rootDir string) string {
	if a.StateFile != "" {
		return a.StateFile
	}

	return path.Join(rootDir, ".quitsh", "watcher-state.json")
}

// ResolveExcludes returns the configured excludes or the defaults.
func (a *Args) ResolveExcludes() []string {
	if len(a.Excludes) == 0 {
		return DefaultExcludes()
	}

	return a.Excludes
}

// Default settings used when the corresponding field is left at its zero value.
const (
	DefaultScanInterval = 2 * time.Second
	DefaultTimeout      = 2 * time.Second
)

// IsEnabled reports whether change tracking is on.
func (a *Args) IsEnabled() bool {
	return a != nil && !a.Disabled
}

// ResolveHashMode returns the configured hash mode or the default.
func (a *Args) ResolveHashMode() HashMode {
	if a.HashMode == "" {
		return HashModeMTimeSize
	}

	return a.HashMode
}

// ResolveScanInterval returns the configured scan interval or the default.
func (a *Args) ResolveScanInterval() time.Duration {
	if a.ScanInterval <= 0 {
		return DefaultScanInterval
	}

	return a.ScanInterval
}

// ResolveTimeout returns the configured client timeout or the default.
func (a *Args) ResolveTimeout() time.Duration {
	if a.Timeout <= 0 {
		return DefaultTimeout
	}

	return a.Timeout
}
