# `quitsh server` Change-Tracking Watcher — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> superpowers:subagent-driven-development (recommended) or
> superpowers:executing-plans to implement this plan task-by-task. Steps use
> checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional local gRPC server (`quitsh server`) that tracks
per-target input changes so `quitsh` can skip targets that were already built
successfully.

**Architecture:** The server periodically rescans the repository, computes a
content digest per input set, and stores a `last_successful_build` per target.
It is a pure per-target dirty oracle — it knows nothing about target
dependencies. Clients query it, seed `node.Inputs.Changed` in `pkg/dag`, let the
existing forward propagation run, skip unchanged nodes, and report results back.

**Tech Stack:** Go 1.26, gRPC (`google.golang.org/grpc`) over a unix socket,
protobuf, `charlievieth/fastwalk`, `spf13/cobra`, `creasty/defaults`, Nix dev
shells.

**Design spec:** `docs/features/specs/2026-09-07-watcher-server-design.md` —
read it before starting.

## Global Constraints

- Module path is `github.com/sdsc-ordes/quitsh`; the CLI in `tools/cli` is a
  separate module (`quitsh-cli`).
- Go version floor: `go 1.26.0` (see `go.mod`). Do not raise it.
- Unit test files start with `//go:build test && (test_small || test_all)` and
  live in the same package as the code under test.
- Integration test files start with `//go:build test && integration` and live in
  `test/`.
- Run unit tests with:
  `go test -tags 'debug test test_small' ./pkg/watcher/... -v`
- Lint with `just lint`; format with `just format`. `golangci-lint` runs `mnd`
  (magic numbers) — name constants or append `//nolint:mnd // intentional.` as
  the codebase already does.
- Errors: use `pkg/errors` (`errors.New(format, args...)`,
  `errors.AddContext(err, format, args...)`, `errors.Combine`). Never
  `fmt.Errorf`.
- Logging: use `pkg/log` with key-value args, e.g.
  `log.Info("Scan done.", "files", n)`.
- Config structs use `yaml:"..."` plus `default:"..."` tags consumed by
  `creasty/defaults`.
- **Git identity is not configured in this repo.** Every commit must be run as:
  `git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' commit -m "..."`
- Commit messages follow Conventional Commits (`feat:`, `fix:`, `docs:`,
  `chore:`, `test:`).
- Accepted trade-off: `pkg/dag` imports `pkg/watcher/client`, so gRPC links into
  every quitsh binary. This was decided explicitly; do not introduce an
  interface to avoid it.

## File Structure

| Path                              | Responsibility                                                              |
| --------------------------------- | --------------------------------------------------------------------------- |
| `pkg/watcher/args.go`             | `Args` config struct, address/state-file resolution, default excludes       |
| `pkg/watcher/hash.go`             | `Stamp`, `Digest`, `HashMode`, `Hasher` implementations                     |
| `pkg/watcher/inputset.go`         | `InputSet` matching (regexes + base dir), assignment of files to input sets |
| `pkg/watcher/tracker.go`          | `Tracker`: digests, change points, status derivation, reporting, pinning    |
| `pkg/watcher/state.go`            | Persistence of `last_successful_build` / `last_run` (atomic write)          |
| `pkg/watcher/version.go`          | `ProtocolVersion` constant                                                  |
| `pkg/watcher/selector.go`         | `ArgsSelector` for the CLI option                                           |
| `pkg/watcher/scan/scan.go`        | `fastwalk` walk + excludes, incremental re-hash → file stamps               |
| `pkg/watcher/proto/watcher.proto` | `quitsh.watcher.v1` schema                                                  |
| `pkg/watcher/proto/*.pb.go`       | Generated code (checked in)                                                 |
| `pkg/watcher/server/server.go`    | Scan loop, freshness/join, tracker ownership                                |
| `pkg/watcher/server/service.go`   | gRPC service implementation                                                 |
| `pkg/watcher/server/socket.go`    | Socket lifecycle (stale detection, listener, shutdown)                      |
| `pkg/watcher/client/client.go`    | gRPC client, degrades when unreachable                                      |
| `pkg/dag/inputs-resolve.go`       | `ResolveTargetInputs` — shared target→input-set resolution                  |
| `pkg/dag/watcher.go`              | `WithWatcher`, `WatcherSession`, `SolveWatcherChanges`                      |
| `pkg/cli/cmd/watcher/*.go`        | `quitsh server serve\|status\|stop\|reset`                                  |
| `test/watcher_test.go`            | Integration test over a real unix socket                                    |

---

### Task 1: Watcher config and hashing primitives

**Files:**

- Create: `pkg/watcher/args.go`
- Create: `pkg/watcher/hash.go`
- Test: `pkg/watcher/hash_test.go`

**Interfaces:**

- Consumes: nothing.
- Produces: `watcher.Args`, `watcher.ScanID` (`int64`), `watcher.Digest`
  (`uint64`), `watcher.Stamp`, `watcher.HashMode`, `watcher.Hasher` interface
  with
  `Stamp(absPath string, modTimeNs int64, size int64, prev Stamp, prevOk bool) (Stamp, error)`,
  `watcher.NewHasher(HashMode) (Hasher, error)`,
  `watcher.DigestOf(sortedPaths []string, stamps map[string]Stamp) Digest`,
  `watcher.DefaultExcludes() []string`,
  `(*Args).ResolveAddress(rootDir string) string`,
  `(*Args).ResolveStateFile(rootDir string) string`.

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/hash_test.go`:

```go
//go:build test && (test_small || test_all)

package watcher

import (
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMTimeSizeHasherIgnoresContent(t *testing.T) {
	t.Parallel()
	h, err := NewHasher(HashModeMTimeSize)
	require.NoError(t, err)

	s, err := h.Stamp("/does/not/matter", 42, 7, Stamp{}, false)
	require.NoError(t, err)
	assert.Equal(t, Stamp{ModTimeNs: 42, Size: 7}, s)
}

func TestChecksumHasherReadsContent(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	p := path.Join(dir, "a.txt")
	require.NoError(t, os.WriteFile(p, []byte("hello"), 0o600))

	h, err := NewHasher(HashModeChecksum)
	require.NoError(t, err)

	s, err := h.Stamp(p, 1, 5, Stamp{}, false)
	require.NoError(t, err)
	assert.NotZero(t, s.Sum)

	// Same mtime+size -> reuses the previous stamp without reading.
	reused, err := h.Stamp("/deleted/by/now", 1, 5, s, true)
	require.NoError(t, err)
	assert.Equal(t, s, reused)
}

func TestDigestOfIsOrderStableAndContentSensitive(t *testing.T) {
	t.Parallel()
	stamps := map[string]Stamp{
		"a.go": {ModTimeNs: 1, Size: 2},
		"b.go": {ModTimeNs: 3, Size: 4},
	}
	d1 := DigestOf([]string{"a.go", "b.go"}, stamps)
	d2 := DigestOf([]string{"a.go", "b.go"}, stamps)
	assert.Equal(t, d1, d2)

	stamps["b.go"] = Stamp{ModTimeNs: 9, Size: 4}
	assert.NotEqual(t, d1, DigestOf([]string{"a.go", "b.go"}, stamps))
}

func TestUnknownHashMode(t *testing.T) {
	t.Parallel()
	_, err := NewHasher("banana")
	assert.Error(t, err)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -v` Expected: FAIL
— `undefined: NewHasher`, `undefined: HashModeMTimeSize`, …

- [ ] **Step 3: Write `pkg/watcher/hash.go`**

```go
package watcher

import (
	"encoding/binary"
	"hash/fnv"
	"io"
	"os"

	"github.com/sdsc-ordes/quitsh/pkg/errors"
)

type (
	// ScanID identifies one scan generation of the repository.
	ScanID int64

	// Digest is the content digest over an input set.
	Digest uint64

	// HashMode selects how file stamps are computed.
	HashMode string

	// Stamp identifies the state of a single file.
	Stamp struct {
		ModTimeNs int64  `json:"modTimeNs"`
		Size      int64  `json:"size"`
		Sum       uint64 `json:"sum"` // Only set in `HashModeChecksum`.
	}

	// Hasher computes a [Stamp] for a file.
	// `prev` is the stamp from the previous scan (if `prevOk`), which
	// implementations may reuse to avoid reading file content.
	Hasher interface {
		Stamp(absPath string, modTimeNs int64, size int64, prev Stamp, prevOk bool) (Stamp, error)
	}
)

const (
	// HashModeMTimeSize stamps files by modification time and size (fast, default).
	HashModeMTimeSize HashMode = "mtime-size"

	// HashModeChecksum stamps files by a checksum over their content.
	HashModeChecksum HashMode = "checksum"
)

// NewHasher returns the hasher for the given mode.
func NewHasher(mode HashMode) (Hasher, error) {
	switch mode {
	case HashModeMTimeSize:
		return mtimeSizeHasher{}, nil
	case HashModeChecksum:
		return checksumHasher{}, nil
	default:
		return nil, errors.New(
			"unknown hash mode '%v' (use '%v' or '%v')",
			mode, HashModeMTimeSize, HashModeChecksum)
	}
}

type mtimeSizeHasher struct{}

func (mtimeSizeHasher) Stamp(
	_ string, modTimeNs int64, size int64, _ Stamp, _ bool,
) (Stamp, error) {
	return Stamp{ModTimeNs: modTimeNs, Size: size}, nil
}

type checksumHasher struct{}

func (checksumHasher) Stamp(
	absPath string, modTimeNs int64, size int64, prev Stamp, prevOk bool,
) (Stamp, error) {
	// Content cannot have changed if neither mtime nor size moved.
	if prevOk && prev.ModTimeNs == modTimeNs && prev.Size == size {
		return prev, nil
	}

	f, err := os.Open(absPath)
	if err != nil {
		return Stamp{}, errors.AddContext(err, "could not open '%v' for hashing", absPath)
	}
	defer f.Close() //nolint:errcheck // read-only.

	h := fnv.New64a()
	if _, err = io.Copy(h, f); err != nil {
		return Stamp{}, errors.AddContext(err, "could not hash '%v'", absPath)
	}

	return Stamp{ModTimeNs: modTimeNs, Size: size, Sum: h.Sum64()}, nil
}

// DigestOf computes the digest over `sortedPaths` and their stamps.
// The paths must be sorted for the digest to be stable.
func DigestOf(sortedPaths []string, stamps map[string]Stamp) Digest {
	h := fnv.New64a()
	var buf [8]byte

	write := func(v uint64) {
		binary.LittleEndian.PutUint64(buf[:], v)
		_, _ = h.Write(buf[:])
	}

	for _, p := range sortedPaths {
		s := stamps[p]
		_, _ = h.Write([]byte(p))
		write(uint64(s.ModTimeNs)) //nolint:gosec // wrap-around is fine for hashing.
		write(uint64(s.Size))      //nolint:gosec // wrap-around is fine for hashing.
		write(s.Sum)
	}

	return Digest(h.Sum64())
}
```

- [ ] **Step 4: Write `pkg/watcher/args.go`**

```go
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
type Args struct {
	// Enabled turns change-tracking on. It is on by default; `--no-skip`
	// switches it off for one invocation.
	Enabled bool `yaml:"enabled" default:"true"`

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
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -v` Expected: PASS
— 4 tests.

- [ ] **Step 6: Commit**

```bash
git add pkg/watcher/args.go pkg/watcher/hash.go pkg/watcher/hash_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): add config args and file stamping primitives"
```

---

### Task 2: Repository scanner

**Files:**

- Create: `pkg/watcher/scan/scan.go`
- Test: `pkg/watcher/scan/scan_test.go`

**Interfaces:**

- Consumes: `watcher.Hasher`, `watcher.Stamp`, `watcher.NewHasher` (Task 1).
- Produces: `scan.Scanner`,
  `scan.New(rootDir string, hasher watcher.Hasher, excludes []string) (*Scanner, error)`,
  `(*Scanner).Scan(prev map[string]watcher.Stamp) (map[string]watcher.Stamp, error)`
  returning repo-relative slash paths → stamps.

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/scan/scan_test.go`:

```go
//go:build test && (test_small || test_all)

package scan

import (
	"os"
	"path"
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/watcher"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func write(t *testing.T, root string, rel string, content string) {
	t.Helper()
	p := path.Join(root, rel)
	require.NoError(t, os.MkdirAll(path.Dir(p), 0o750))
	require.NoError(t, os.WriteFile(p, []byte(content), 0o600))
}

func newScanner(t *testing.T, root string) *Scanner {
	t.Helper()
	h, err := watcher.NewHasher(watcher.HashModeMTimeSize)
	require.NoError(t, err)
	s, err := New(root, h, watcher.DefaultExcludes())
	require.NoError(t, err)

	return s
}

func TestScanReturnsRelativePaths(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "src/a.go", "package a")
	write(t, root, "src/sub/b.go", "package b")

	files, err := newScanner(t, root).Scan(nil)
	require.NoError(t, err)

	assert.Len(t, files, 2)
	assert.Contains(t, files, "src/a.go")
	assert.Contains(t, files, "src/sub/b.go")
}

func TestScanAppliesExcludes(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "src/a.go", "package a")
	write(t, root, ".git/objects/deadbeef", "junk")
	write(t, root, "node_modules/x/index.js", "x")

	files, err := newScanner(t, root).Scan(nil)
	require.NoError(t, err)

	assert.Len(t, files, 1)
	assert.Contains(t, files, "src/a.go")
}

func TestScanDetectsChangedFile(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "a.txt", "one")

	s := newScanner(t, root)
	first, err := s.Scan(nil)
	require.NoError(t, err)

	write(t, root, "a.txt", "one-plus-more")
	second, err := s.Scan(first)
	require.NoError(t, err)

	assert.NotEqual(t, first["a.txt"], second["a.txt"])
}

func TestScanDropsDeletedFiles(t *testing.T) {
	t.Parallel()
	root := t.TempDir()
	write(t, root, "a.txt", "one")
	write(t, root, "b.txt", "two")

	s := newScanner(t, root)
	first, err := s.Scan(nil)
	require.NoError(t, err)
	require.Len(t, first, 2)

	require.NoError(t, os.Remove(path.Join(root, "b.txt")))
	second, err := s.Scan(first)
	require.NoError(t, err)

	assert.Len(t, second, 1)
	assert.NotContains(t, second, "b.txt")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/scan/... -v` Expected:
FAIL — `undefined: New`, `undefined: Scanner`.

- [ ] **Step 3: Write `pkg/watcher/scan/scan.go`**

Note: `fastwalk.Walk` runs the callback on multiple goroutines, so the result
map is guarded by a mutex, exactly as `pkg/filesystem/glob.go` does.

```go
// Package scan walks a repository and stamps every tracked file.
package scan

import (
	"io/fs"
	stderr "errors"
	"path/filepath"
	"strings"
	"sync"

	"github.com/sdsc-ordes/quitsh/pkg/common/recache"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"

	"github.com/charlievieth/fastwalk"
)

// Scanner walks `rootDir` and produces file stamps.
type Scanner struct {
	rootDir  string
	hasher   watcher.Hasher
	excludes recache.List
}

// New creates a scanner over `rootDir`.
// `excludes` are regexes matched against repo-relative slash paths;
// a matching directory is pruned.
func New(rootDir string, hasher watcher.Hasher, excludes []string) (*Scanner, error) {
	cache := recache.NewCache(false)

	regexes, err := cache.Get(excludes...)
	if err != nil {
		return nil, errors.AddContext(err, "could not compile watcher exclude patterns")
	}

	return &Scanner{rootDir: rootDir, hasher: hasher, excludes: regexes}, nil
}

// Scan walks the repository and returns repo-relative slash paths to stamps.
// `prev` is the result of the previous scan and may be `nil`; it lets the
// hasher skip content reads for files whose mtime and size did not move.
func (s *Scanner) Scan(prev map[string]watcher.Stamp) (map[string]watcher.Stamp, error) {
	conf := fastwalk.Config{ToSlash: true}

	var (
		lock  sync.Mutex
		files = make(map[string]watcher.Stamp, len(prev))
	)

	walk := func(absPath string, d fs.DirEntry, err error) error {
		if err != nil {
			// A file vanishing mid-scan is normal; it is simply not recorded.
			return nil //nolint:nilerr // deliberate.
		}

		rel, e := filepath.Rel(s.rootDir, absPath)
		if e != nil {
			return nil //nolint:nilerr // outside the root, ignore.
		}
		rel = filepath.ToSlash(rel)

		if rel == "." {
			return nil
		}

		if s.excludes.Match(rel) {
			if d.IsDir() {
				return fastwalk.SkipDir
			}

			return nil
		}

		if d.IsDir() || !d.Type().IsRegular() {
			// Symlinks are not followed; only regular files are stamped.
			return nil
		}

		info, e := d.Info()
		if e != nil {
			return nil //nolint:nilerr // vanished, ignore.
		}

		lock.Lock()
		p, ok := prev[rel]
		lock.Unlock()

		stamp, e := s.hasher.Stamp(absPath, info.ModTime().UnixNano(), info.Size(), p, ok)
		if e != nil {
			return nil //nolint:nilerr // unreadable file: treated as untracked.
		}

		lock.Lock()
		files[rel] = stamp
		lock.Unlock()

		return nil
	}

	err := fastwalk.Walk(&conf, s.rootDir, walk)
	if stderr.Is(err, fastwalk.SkipDir) {
		err = nil
	}

	if err != nil {
		return nil, errors.AddContext(err, "could not walk root dir '%v'", s.rootDir)
	}

	return files, nil
}

// RootDir returns the scanned root directory.
func (s *Scanner) RootDir() string {
	return strings.TrimSuffix(s.rootDir, "/")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/scan/... -v` Expected:
PASS — 4 tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/watcher/scan
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): add repository scanner with excludes and incremental stamping"
```

---

### Task 3: Input-set matching and assignment

**Files:**

- Create: `pkg/watcher/inputset.go`
- Test: `pkg/watcher/inputset_test.go`

**Interfaces:**

- Consumes: `watcher.Stamp`, `watcher.Digest`, `watcher.DigestOf` (Task 1);
  `input.Config`, `input.ID` from `pkg/component/input`.
- Produces: `watcher.InputSet`,
  `watcher.NewInputSet(cfg *input.Config, rootDir string) (*InputSet, error)`,
  `(*InputSet).Matches(relPath string) bool`, `watcher.InputSets`
  (`map[input.ID]*InputSet`),
  `(InputSets).Assign(files map[string]Stamp) map[input.ID][]string` returning
  **sorted** paths per set.

Matching mirrors `pkg/dag/execution-order.go:determineChangedPaths`: the path is
made relative to the input's `BaseDir`, then `include && !exclude` decides, with
full-match regexes (`recache.NewCache(true)`).

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/inputset_test.go`:

```go
//go:build test && (test_small || test_all)

package watcher

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/input"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newSet(t *testing.T, id string, baseDir string, patterns ...string) *InputSet {
	t.Helper()
	cfg := &input.Config{Patterns: patterns, BaseDir: baseDir}
	cfg.Init(input.ID(id))
	s, err := NewInputSet(cfg, "/repo")
	require.NoError(t, err)

	return s
}

func TestInputSetMatchesRelativeToBaseDir(t *testing.T) {
	t.Parallel()
	s := newSet(t, "comp::srcs", "/repo/comp", `^src/.*\.go$`)

	assert.True(t, s.Matches("comp/src/a.go"))
	assert.False(t, s.Matches("comp/src/a.txt"))
	assert.False(t, s.Matches("other/src/a.go"))
}

func TestInputSetHonoursExcludePatterns(t *testing.T) {
	t.Parallel()
	s := newSet(t, "comp::srcs", "/repo/comp", `^src/.*\.go$`, `!^src/gen/.*$`)

	assert.True(t, s.Matches("comp/src/a.go"))
	assert.False(t, s.Matches("comp/src/gen/a.go"))
}

func TestInputSetRelativeToRootMatchesEverywhere(t *testing.T) {
	t.Parallel()
	s := newSet(t, "comp::all", "/repo", `^.*\.nix$`)

	assert.True(t, s.Matches("tools/nix/flake.nix"))
	assert.False(t, s.Matches("tools/nix/flake.lock"))
}

func TestInputSetsAssignSortsPaths(t *testing.T) {
	t.Parallel()
	sets := InputSets{
		"comp::srcs": newSet(t, "comp::srcs", "/repo/comp", `^src/.*\.go$`),
		"comp::docs": newSet(t, "comp::docs", "/repo/comp", `^docs/.*$`),
	}

	files := map[string]Stamp{
		"comp/src/b.go":  {Size: 1},
		"comp/src/a.go":  {Size: 2},
		"comp/docs/x.md": {Size: 3},
		"unrelated.txt":  {Size: 4},
	}

	got := sets.Assign(files)

	assert.Equal(t, []string{"comp/src/a.go", "comp/src/b.go"}, got["comp::srcs"])
	assert.Equal(t, []string{"comp/docs/x.md"}, got["comp::docs"])
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -run InputSet -v`
Expected: FAIL — `undefined: NewInputSet`, `undefined: InputSets`.

- [ ] **Step 3: Write `pkg/watcher/inputset.go`**

```go
package watcher

import (
	"path"
	"sort"
	"strings"

	"github.com/sdsc-ordes/quitsh/pkg/common/recache"
	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
)

type (
	// InputSet matches repository files against one input change set.
	InputSet struct {
		ID input.ID

		// baseRel is the input's base directory relative to the repository
		// root ("" when the base directory is the root itself).
		baseRel string

		includes recache.List
		excludes recache.List
	}

	// InputSets holds all input sets of all components.
	InputSets map[input.ID]*InputSet
)

// NewInputSet creates a matcher for `cfg`, whose `BaseDir` must be absolute
// and inside `rootDir`.
func NewInputSet(cfg *input.Config, rootDir string) (*InputSet, error) {
	cache := recache.NewCache(true)

	includes, err := cache.Get(cfg.Includes()...)
	if err != nil {
		return nil, errors.AddContext(err, "bad include patterns on input id '%v'", cfg.ID)
	}

	excludes, err := cache.Get(cfg.Excludes()...)
	if err != nil {
		return nil, errors.AddContext(err, "bad exclude patterns on input id '%v'", cfg.ID)
	}

	baseRel := strings.TrimPrefix(path.Clean(cfg.BaseDir), path.Clean(rootDir))
	baseRel = strings.Trim(baseRel, "/")

	return &InputSet{ID: cfg.ID, baseRel: baseRel, includes: includes, excludes: excludes}, nil
}

// Matches reports whether the repo-relative slash path belongs to this input set.
func (s *InputSet) Matches(relPath string) bool {
	p := relPath

	if s.baseRel != "" {
		prefix := s.baseRel + "/"
		if !strings.HasPrefix(p, prefix) {
			return false
		}
		p = strings.TrimPrefix(p, prefix)
	}

	return s.includes.Match(p) && !s.excludes.Match(p)
}

// Assign maps every input set to the sorted list of files it matches.
// Sets matching nothing are present with an empty list, so that a set
// losing its last file still produces a digest change.
func (s InputSets) Assign(files map[string]Stamp) map[input.ID][]string {
	assigned := make(map[input.ID][]string, len(s))
	for id := range s {
		assigned[id] = nil
	}

	for p := range files {
		for id, set := range s {
			if set.Matches(p) {
				assigned[id] = append(assigned[id], p)
			}
		}
	}

	for id := range assigned {
		sort.Strings(assigned[id])
	}

	return assigned
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -run InputSet -v`
Expected: PASS — 4 tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/watcher/inputset.go pkg/watcher/inputset_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): match repository files to component input sets"
```

---

### Task 4: Tracker — digests, change points and status derivation

**Files:**

- Create: `pkg/watcher/tracker.go`
- Test: `pkg/watcher/tracker_test.go`

**Interfaces:**

- Consumes: `InputSets`, `Stamp`, `Digest`, `DigestOf`, `ScanID` (Tasks 1, 3).
- Produces: `watcher.Tracker`, `watcher.NewTracker() *Tracker`,
  `(*Tracker).SetComponents(sets InputSets, targets map[target.ID][]input.ID)`,
  `(*Tracker).Update(scanID ScanID, files map[string]Stamp)`,
  `(*Tracker).DigestAt(id input.ID, scanID ScanID) (Digest, bool)`,
  `(*Tracker).Status(ids []target.ID) []TargetStatus`, and the types
  `ChangePoint`, `LastSuccessfulBuild`, `LastRun`, `TargetStatus`.

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/tracker_test.go`:

```go
//go:build test && (test_small || test_all)

package watcher

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTracker builds a tracker with one component `comp` owning input set
// `comp::srcs` (all files below `comp/`) and one target `comp::build`.
func newTracker(t *testing.T) *Tracker {
	t.Helper()
	cfg := &input.Config{Patterns: []string{`^.*$`}, BaseDir: "/repo/comp"}
	cfg.Init("comp::srcs")
	set, err := NewInputSet(cfg, "/repo")
	require.NoError(t, err)

	tr := NewTracker()
	tr.SetComponents(
		InputSets{"comp::srcs": set},
		map[target.ID][]input.ID{"comp::build": {"comp::srcs"}},
	)

	return tr
}

func TestStatusDirtyWithoutSuccessfulBuild(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})

	st := tr.Status([]target.ID{"comp::build"})
	require.Len(t, st, 1)
	assert.True(t, st[0].Dirty)
	assert.False(t, st[0].HasSuccessfulBuild)
}

func TestUpdateRecordsChangePointOnlyWhenDigestMoves(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)

	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})
	tr.Update(2, map[string]Stamp{"comp/a.go": {Size: 1}})
	tr.Update(3, map[string]Stamp{"comp/a.go": {Size: 2}})

	d1, ok := tr.DigestAt("comp::srcs", 1)
	require.True(t, ok)
	d2, ok := tr.DigestAt("comp::srcs", 2)
	require.True(t, ok)
	d3, ok := tr.DigestAt("comp::srcs", 3)
	require.True(t, ok)

	assert.Equal(t, d1, d2, "no change between scan 1 and 2")
	assert.NotEqual(t, d1, d3)
}

func TestDigestAtBeforeFirstScanIsUnknown(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(5, map[string]Stamp{"comp/a.go": {Size: 1}})

	_, ok := tr.DigestAt("comp::srcs", 4)
	assert.False(t, ok)
}

func TestStatusOfUnknownTargetIsDirty(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})

	st := tr.Status([]target.ID{"nope::nope"})
	require.Len(t, st, 1)
	assert.True(t, st[0].Dirty)
}

func TestSetComponentsDropsVanishedTargets(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})
	tr.Report(1, map[target.ID]bool{"comp::build": true})
	require.True(t, tr.Status([]target.ID{"comp::build"})[0].HasSuccessfulBuild)

	// Target disappears from the components.
	tr.SetComponents(InputSets{}, map[target.ID][]input.ID{})
	assert.False(t, tr.Status([]target.ID{"comp::build"})[0].HasSuccessfulBuild)
}
```

Note: `TestSetComponentsDropsVanishedTargets` uses `Report`, added in Task 5.
Expect it to fail compilation until Task 5 lands — implement `Report` in Task 5
and re-run this file then. To keep Task 4 green on its own, comment that single
test out and re-enable it as Step 1 of Task 5.

- [ ] **Step 2: Run test to verify it fails**

Run:
`go test -tags 'debug test test_small' ./pkg/watcher/... -run 'Status|Update|DigestAt' -v`
Expected: FAIL — `undefined: NewTracker`.

- [ ] **Step 3: Write `pkg/watcher/tracker.go`**

```go
package watcher

import (
	"sort"
	"sync"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"
)

type (
	// ChangePoint records the scan at which an input set's digest changed.
	ChangePoint struct {
		ScanID ScanID `json:"scanId"`
		Digest Digest `json:"digest"`
	}

	// LastSuccessfulBuild is the input state a target was last built from.
	LastSuccessfulBuild struct {
		ScanID   ScanID                `json:"scanId"`
		AtUnixMs int64                 `json:"atUnixMs"`
		Inputs   map[input.ID]Digest   `json:"inputs"`
	}

	// LastRun is the outcome of the last reported run of a target.
	LastRun struct {
		Success  bool   `json:"success"`
		ScanID   ScanID `json:"scanId"`
		AtUnixMs int64  `json:"atUnixMs"`
	}

	// TargetStatus is the derived answer for one target.
	TargetStatus struct {
		ID                 target.ID
		Dirty              bool
		HasSuccessfulBuild bool
		DirtyInputs        []input.ID
		LastRun            *LastRun
	}

	// Tracker owns all change-tracking state. It is safe for concurrent use.
	Tracker struct {
		mu sync.Mutex

		sets    InputSets
		targets map[target.ID][]input.ID

		history  map[input.ID][]ChangePoint
		lastScan ScanID

		lastSuccess map[target.ID]LastSuccessfulBuild
		lastRun     map[target.ID]LastRun

		pins map[ScanID]int
	}
)

// NewTracker creates an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{
		sets:        InputSets{},
		targets:     map[target.ID][]input.ID{},
		history:     map[input.ID][]ChangePoint{},
		lastSuccess: map[target.ID]LastSuccessfulBuild{},
		lastRun:     map[target.ID]LastRun{},
		pins:        map[ScanID]int{},
	}
}

// SetComponents installs the input sets and the target to input-set mapping.
// State of targets and input sets which no longer exist is dropped.
func (t *Tracker) SetComponents(sets InputSets, targets map[target.ID][]input.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.sets = sets
	t.targets = targets

	for id := range t.lastSuccess {
		if _, exists := targets[id]; !exists {
			log.Debug("Dropping state of vanished target.", "target", id)
			delete(t.lastSuccess, id)
			delete(t.lastRun, id)
		}
	}

	for id := range t.history {
		if _, exists := sets[id]; !exists {
			delete(t.history, id)
		}
	}
}

// Update installs the result of scan `scanID` and appends change points for
// every input set whose digest moved.
func (t *Tracker) Update(scanID ScanID, files map[string]Stamp) {
	t.mu.Lock()
	defer t.mu.Unlock()

	assigned := t.sets.Assign(files)

	for id, paths := range assigned {
		d := DigestOf(paths, files)

		h := t.history[id]
		if len(h) != 0 && h[len(h)-1].Digest == d {
			continue
		}

		t.history[id] = append(h, ChangePoint{ScanID: scanID, Digest: d})
		log.Trace("Input set changed.", "input", id, "scan", scanID)
	}

	t.lastScan = scanID
	t.pruneLocked()
}

// DigestAt returns the digest of input set `id` as of scan `scanID`.
// It reports `false` when the history does not reach back that far.
func (t *Tracker) DigestAt(id input.ID, scanID ScanID) (Digest, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.digestAtLocked(id, scanID)
}

func (t *Tracker) digestAtLocked(id input.ID, scanID ScanID) (Digest, bool) {
	h := t.history[id]
	for i := len(h) - 1; i >= 0; i-- {
		if h[i].ScanID <= scanID {
			return h[i].Digest, true
		}
	}

	return 0, false
}

// Status derives the dirty state of the given targets.
// An empty `ids` means all known targets.
func (t *Tracker) Status(ids []target.ID) []TargetStatus {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(ids) == 0 {
		ids = make([]target.ID, 0, len(t.targets))
		for id := range t.targets {
			ids = append(ids, id)
		}
		sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	}

	statuses := make([]TargetStatus, 0, len(ids))
	for _, id := range ids {
		statuses = append(statuses, t.statusLocked(id))
	}

	return statuses
}

func (t *Tracker) statusLocked(id target.ID) TargetStatus {
	st := TargetStatus{ID: id}

	if lr, ok := t.lastRun[id]; ok {
		copied := lr
		st.LastRun = &copied
	}

	inputs, known := t.targets[id]
	if !known {
		// Unknown targets are always dirty: never skip what we cannot judge.
		st.Dirty = true

		return st
	}

	last, ok := t.lastSuccess[id]
	if !ok {
		st.Dirty = true

		return st
	}

	st.HasSuccessfulBuild = true

	for _, in := range inputs {
		current, hasCurrent := t.digestAtLocked(in, t.lastScan)
		built, hasBuilt := last.Inputs[in]

		if !hasCurrent || !hasBuilt || current != built {
			st.Dirty = true
			st.DirtyInputs = append(st.DirtyInputs, in)
		}
	}

	sort.Slice(st.DirtyInputs, func(i, j int) bool { return st.DirtyInputs[i] < st.DirtyInputs[j] })

	return st
}

// nowUnixMs is indirected so tests can stay deterministic if needed.
var nowUnixMs = func() int64 { return time.Now().UnixMilli() } //nolint:gochecknoglobals // test seam.
```

- [ ] **Step 4: Add the pruning stub so the file compiles**

Append to `pkg/watcher/tracker.go` (fully implemented in Task 5):

```go
// pruneLocked drops change points which no outstanding scan id can still need.
func (t *Tracker) pruneLocked() {
	keepFrom := t.lastScan
	for id := range t.pins {
		if id < keepFrom {
			keepFrom = id
		}
	}

	for id, h := range t.history {
		cut := 0
		for i := range h {
			if h[i].ScanID <= keepFrom {
				cut = i
			}
		}

		if cut > 0 {
			t.history[id] = append([]ChangePoint(nil), h[cut:]...)
		}
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run:
`go test -tags 'debug test test_small' ./pkg/watcher/... -run 'Status|Update|DigestAt' -v`
Expected: PASS — 4 tests (the `SetComponents` test stays commented out until
Task 5).

- [ ] **Step 6: Commit**

```bash
git add pkg/watcher/tracker.go pkg/watcher/tracker_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): track input digests and derive per-target dirty status"
```

---

### Task 5: Tracker — reporting, pinning and reset

**Files:**

- Modify: `pkg/watcher/tracker.go`
- Modify: `pkg/watcher/tracker_test.go`

**Interfaces:**

- Consumes: everything from Task 4.
- Produces:
  `(*Tracker).Report(scanID ScanID, results map[target.ID]bool) (notRecorded []target.ID)`,
  `(*Tracker).Pin(scanID ScanID)`, `(*Tracker).Unpin(scanID ScanID)`,
  `(*Tracker).Reset(ids ...target.ID)`, `(*Tracker).LastScan() ScanID`.

- [ ] **Step 1: Re-enable and extend the tests**

Un-comment `TestSetComponentsDropsVanishedTargets` from Task 4 and append to
`pkg/watcher/tracker_test.go`:

```go
func TestReportSuccessMakesTargetClean(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})

	notRecorded := tr.Report(1, map[target.ID]bool{"comp::build": true})
	assert.Empty(t, notRecorded)

	st := tr.Status([]target.ID{"comp::build"})[0]
	assert.False(t, st.Dirty)
	assert.True(t, st.HasSuccessfulBuild)
}

func TestReportFailureLeavesTargetDirty(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})

	tr.Report(1, map[target.ID]bool{"comp::build": false})

	st := tr.Status([]target.ID{"comp::build"})[0]
	assert.True(t, st.Dirty)
	assert.False(t, st.HasSuccessfulBuild)
	require.NotNil(t, st.LastRun)
	assert.False(t, st.LastRun.Success)
}

// The edit-during-build case from the design spec: a file changes at scan 7
// while the build that was decided at scan 5 is still running.
func TestReportRecordsStateOfTheReportedScan(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Pin(5)
	tr.Update(5, map[string]Stamp{"comp/a.go": {Size: 1}}) // D1
	tr.Update(7, map[string]Stamp{"comp/a.go": {Size: 2}}) // D2

	tr.Report(5, map[target.ID]bool{"comp::build": true})
	tr.Unpin(5)

	st := tr.Status([]target.ID{"comp::build"})[0]
	assert.True(t, st.Dirty, "the build built D1 but D2 is on disk")
	assert.Equal(t, []input.ID{"comp::srcs"}, st.DirtyInputs)
}

func TestRevertingAnEditReadsCleanAgain(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})
	tr.Report(1, map[target.ID]bool{"comp::build": true})

	tr.Update(2, map[string]Stamp{"comp/a.go": {Size: 2}})
	require.True(t, tr.Status([]target.ID{"comp::build"})[0].Dirty)

	tr.Update(3, map[string]Stamp{"comp/a.go": {Size: 1}}) // reverted
	assert.False(t, tr.Status([]target.ID{"comp::build"})[0].Dirty)
}

func TestReportWithPrunedScanIDRecordsNothing(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(10, map[string]Stamp{"comp/a.go": {Size: 1}})

	notRecorded := tr.Report(9, map[target.ID]bool{"comp::build": true})

	assert.Equal(t, []target.ID{"comp::build"}, notRecorded)
	assert.True(t, tr.Status([]target.ID{"comp::build"})[0].Dirty)
}

func TestResetClearsSuccessfulBuild(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})
	tr.Report(1, map[target.ID]bool{"comp::build": true})
	require.False(t, tr.Status([]target.ID{"comp::build"})[0].Dirty)

	tr.Reset("comp::build")
	assert.True(t, tr.Status([]target.ID{"comp::build"})[0].Dirty)
}

func TestPinKeepsHistoryForLongBuilds(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Pin(1)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})

	// Many scans pass while the build runs.
	for i := ScanID(2); i < 100; i++ {
		tr.Update(i, map[string]Stamp{"comp/a.go": {Size: int64(i)}})
	}

	_, ok := tr.DigestAt("comp::srcs", 1)
	assert.True(t, ok, "pinned scan id must stay resolvable")

	tr.Unpin(1)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -v` Expected: FAIL
— `tr.Report undefined`, `tr.Pin undefined`, `tr.Reset undefined`.

- [ ] **Step 3: Append the implementation to `pkg/watcher/tracker.go`**

```go
// Report records the outcome of a run which was decided at scan `scanID`.
// For successful targets the input digests **as of that scan** are recorded as
// the target's last successful build. Targets whose history no longer reaches
// back to `scanID` are returned in `notRecorded` and stay dirty.
func (t *Tracker) Report(scanID ScanID, results map[target.ID]bool) (notRecorded []target.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := nowUnixMs()

	ids := make([]target.ID, 0, len(results))
	for id := range results {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	for _, id := range ids {
		success := results[id]
		t.lastRun[id] = LastRun{Success: success, ScanID: scanID, AtUnixMs: now}

		if !success {
			continue
		}

		inputs, known := t.targets[id]
		if !known {
			notRecorded = append(notRecorded, id)

			continue
		}

		digests := make(map[input.ID]Digest, len(inputs))
		complete := true

		for _, in := range inputs {
			d, ok := t.digestAtLocked(in, scanID)
			if !ok {
				complete = false

				break
			}
			digests[in] = d
		}

		if !complete {
			log.Debug("Scan id no longer resolvable; not recording build.",
				"target", id, "scan", scanID)
			notRecorded = append(notRecorded, id)

			continue
		}

		t.lastSuccess[id] = LastSuccessfulBuild{
			ScanID:   scanID,
			AtUnixMs: now,
			Inputs:   digests,
		}
	}

	return notRecorded
}

// Pin keeps the change-point history resolvable for `scanID` until [Unpin].
func (t *Tracker) Pin(scanID ScanID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.pins[scanID]++
}

// Unpin releases a pin taken by [Pin].
func (t *Tracker) Unpin(scanID ScanID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.pins[scanID] <= 1 {
		delete(t.pins, scanID)

		return
	}

	t.pins[scanID]--
}

// Reset clears the last successful build of the given targets, or of all
// targets when no id is given. They become dirty again.
func (t *Tracker) Reset(ids ...target.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(ids) == 0 {
		t.lastSuccess = map[target.ID]LastSuccessfulBuild{}
		t.lastRun = map[target.ID]LastRun{}

		return
	}

	for _, id := range ids {
		delete(t.lastSuccess, id)
		delete(t.lastRun, id)
	}
}

// LastScan returns the id of the most recent scan installed by [Update].
func (t *Tracker) LastScan() ScanID {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.lastScan
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -v` Expected: PASS
— all tracker, input-set and hash tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/watcher/tracker.go pkg/watcher/tracker_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): record last successful builds with scan-accurate digests"
```

---

### Task 6: Tracker persistence

**Files:**

- Create: `pkg/watcher/state.go`
- Test: `pkg/watcher/state_test.go`

**Interfaces:**

- Consumes: `Tracker`, `LastSuccessfulBuild`, `LastRun` (Tasks 4, 5).
- Produces: `watcher.State`, `(*Tracker).Export() State`,
  `(*Tracker).Import(s State)`, `watcher.SaveState(file string, s State) error`,
  `watcher.LoadState(file string) (State, error)` (a missing file yields an
  empty state and no error).

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/state_test.go`:

```go
//go:build test && (test_small || test_all)

package watcher

import (
	"path"
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadStateOfMissingFileIsEmpty(t *testing.T) {
	t.Parallel()
	s, err := LoadState(path.Join(t.TempDir(), "nope.json"))
	require.NoError(t, err)
	assert.Empty(t, s.LastSuccess)
}

func TestSaveAndLoadStateRoundTrips(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})
	tr.Report(1, map[target.ID]bool{"comp::build": true})

	file := path.Join(t.TempDir(), "sub", "state.json")
	require.NoError(t, SaveState(file, tr.Export()))

	loaded, err := LoadState(file)
	require.NoError(t, err)

	restored := newTracker(t)
	restored.Import(loaded)
	restored.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})

	assert.False(t, restored.Status([]target.ID{"comp::build"})[0].Dirty,
		"a restored server must not rebuild unchanged targets")
}

func TestRestoredStateIsDirtyAfterOfflineChange(t *testing.T) {
	t.Parallel()
	tr := newTracker(t)
	tr.Update(1, map[string]Stamp{"comp/a.go": {Size: 1}})
	tr.Report(1, map[target.ID]bool{"comp::build": true})

	file := path.Join(t.TempDir(), "state.json")
	require.NoError(t, SaveState(file, tr.Export()))

	loaded, err := LoadState(file)
	require.NoError(t, err)

	restored := newTracker(t)
	restored.Import(loaded)
	// The file changed while the server was down.
	restored.Update(1, map[string]Stamp{"comp/a.go": {Size: 99}})

	assert.True(t, restored.Status([]target.ID{"comp::build"})[0].Dirty)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -run State -v`
Expected: FAIL — `undefined: LoadState`.

- [ ] **Step 3: Write `pkg/watcher/state.go`**

```go
package watcher

import (
	"encoding/json"
	"os"
	"path"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
)

// StateVersion is bumped whenever the on-disk format changes incompatibly.
const StateVersion = 1

const (
	stateDirPerms  = 0o750
	stateFilePerms = 0o600
)

// State is the durable part of the tracker.
// Digests and change points are deliberately not persisted; they are rebuilt
// by the scan performed at startup.
type State struct {
	Version     int                                  `json:"version"`
	LastSuccess map[target.ID]LastSuccessfulBuild    `json:"lastSuccessfulBuild"`
	LastRun     map[target.ID]LastRun                `json:"lastRun"`
}

// Export returns the durable state of the tracker.
func (t *Tracker) Export() State {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := State{
		Version:     StateVersion,
		LastSuccess: make(map[target.ID]LastSuccessfulBuild, len(t.lastSuccess)),
		LastRun:     make(map[target.ID]LastRun, len(t.lastRun)),
	}

	for id, v := range t.lastSuccess {
		s.LastSuccess[id] = v
	}
	for id, v := range t.lastRun {
		s.LastRun[id] = v
	}

	return s
}

// Import installs durable state into the tracker, replacing what is there.
func (t *Tracker) Import(s State) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.lastSuccess = map[target.ID]LastSuccessfulBuild{}
	t.lastRun = map[target.ID]LastRun{}

	for id, v := range s.LastSuccess {
		t.lastSuccess[id] = v
	}
	for id, v := range s.LastRun {
		t.lastRun[id] = v
	}
}

// LoadState reads the state file. A missing file yields an empty state.
// A file written by an incompatible version is ignored (everything rebuilds).
func LoadState(file string) (State, error) {
	data, err := os.ReadFile(file) //nolint:gosec // path comes from our own config.
	if os.IsNotExist(err) {
		return State{Version: StateVersion}, nil
	} else if err != nil {
		return State{}, errors.AddContext(err, "could not read watcher state '%v'", file)
	}

	var s State
	if e := json.Unmarshal(data, &s); e != nil {
		return State{}, errors.AddContext(e, "could not parse watcher state '%v'", file)
	}

	if s.Version != StateVersion {
		return State{Version: StateVersion}, nil
	}

	return s, nil
}

// SaveState writes the state atomically (temp file + rename).
func SaveState(file string, s State) error {
	if err := os.MkdirAll(path.Dir(file), stateDirPerms); err != nil {
		return errors.AddContext(err, "could not create dir for watcher state '%v'", file)
	}

	data, err := json.Marshal(s)
	if err != nil {
		return errors.AddContext(err, "could not marshal watcher state")
	}

	tmp := file + ".tmp"
	if e := os.WriteFile(tmp, data, stateFilePerms); e != nil {
		return errors.AddContext(e, "could not write watcher state '%v'", tmp)
	}

	if e := os.Rename(tmp, file); e != nil {
		return errors.AddContext(e, "could not move watcher state to '%v'", file)
	}

	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/... -v` Expected:
PASS.

- [ ] **Step 5: Commit**

```bash
git add pkg/watcher/state.go pkg/watcher/state_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): persist last successful builds across server restarts"
```

---

### Task 7: Shared target → input-set resolution

The server must resolve `self::x` input ids and apply the "no `inputs:` means
the whole component" default exactly like the DAG does, or the two sides would
disagree about what a target depends on. `constructNodes` already implements
this, so expose it instead of duplicating it.

**Files:**

- Create: `pkg/dag/inputs-resolve.go`
- Test: `pkg/dag/inputs-resolve_test.go`

**Interfaces:**

- Consumes: unexported
  `constructNodes(components, targetSelection, rootDir, resolveInputs)` in
  `pkg/dag/execution-order.go:160`.
- Produces:
  `dag.ResolveTargetInputs(components []*component.Component, rootDir string) (targets map[target.ID][]input.ID, inputs map[input.ID]*input.Config, err error)`.
  Every returned `input.Config` has an absolute `BaseDir`. Component-wide inputs
  (targets without an `inputs:` key) are returned as a synthesized config with
  the pattern `^.*$` and the component root as base dir.

- [ ] **Step 1: Write the failing test**

Create `pkg/dag/inputs-resolve_test.go`. It reuses the fixture helpers already
used by `pkg/dag/execution-order_test.go` — open that file first and mirror how
it constructs components (`newComp`/literal `component.Config` values); build
two components, one target with `inputs: ["self::srcs"]` and one target without
any `inputs`.

```go
//go:build test && (test_small || test_all)

package dag

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResolveTargetInputsResolvesSelfReferences(t *testing.T) {
	t.Parallel()
	comps, rootDir := testCompsWithInputs(t) // see Step 2

	targets, inputs, err := ResolveTargetInputs(comps, rootDir)
	require.NoError(t, err)

	assert.Equal(t, []input.ID{"comp-a::srcs"}, targets[target.ID("comp-a::build")])
	require.Contains(t, inputs, input.ID("comp-a::srcs"))
	assert.NotEmpty(t, inputs["comp-a::srcs"].BaseDir)
}

func TestResolveTargetInputsDefaultsToWholeComponent(t *testing.T) {
	t.Parallel()
	comps, rootDir := testCompsWithInputs(t)

	targets, inputs, err := ResolveTargetInputs(comps, rootDir)
	require.NoError(t, err)

	// `comp-b::test` declares no `inputs:` -> whole component.
	assert.Equal(t, []input.ID{"comp-b"}, targets[target.ID("comp-b::test")])
	require.Contains(t, inputs, input.ID("comp-b"))
	assert.Equal(t, []string{"^.*$"}, inputs["comp-b"].Patterns)
}
```

- [ ] **Step 2: Add the fixture helper**

Append to `pkg/dag/inputs-resolve_test.go`, adapting the component construction
to whatever `execution-order_test.go` already uses in this repository:

```go
func testCompsWithInputs(t *testing.T) ([]*component.Component, string) {
	t.Helper()
	const rootDir = "/repo"

	confA := &component.Config{
		Name:     "comp-a",
		Language: "go",
		Inputs: map[string]*input.Config{
			"srcs": {Patterns: []string{`^src/.*\.go$`}},
		},
		Targets: map[string]*target.Config{
			"build": {Inputs: []input.ID{"self::srcs"}},
		},
	}
	require.NoError(t, confA.Init())

	confB := &component.Config{
		Name:     "comp-b",
		Language: "go",
		Targets: map[string]*target.Config{
			"test": {},
		},
	}
	require.NoError(t, confB.Init())

	a := component.NewComponent(confA, path.Join(rootDir, "comp-a"), "", "")
	b := component.NewComponent(confB, path.Join(rootDir, "comp-b"), "", "")

	return []*component.Component{&a, &b}, rootDir
}
```

Add the imports `path`, `github.com/sdsc-ordes/quitsh/pkg/component`.

- [ ] **Step 3: Run test to verify it fails**

Run:
`go test -tags 'debug test test_small' ./pkg/dag/... -run ResolveTargetInputs -v`
Expected: FAIL — `undefined: ResolveTargetInputs`.

- [ ] **Step 4: Write `pkg/dag/inputs-resolve.go`**

```go
package dag

import (
	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
)

// ResolveTargetInputs resolves, for every target in `components`, the input set
// ids it depends on, together with all input set configs (with absolute
// `BaseDir`).
//
// Targets which declare no `inputs:` get the component-wide input set, which is
// synthesized here as a config matching everything below the component root.
// This mirrors what `determineChangedPaths` does inside the DAG, so that the
// watcher and the DAG agree on what a target depends on.
func ResolveTargetInputs(
	components []*component.Component,
	rootDir string,
) (map[target.ID][]input.ID, map[input.ID]*input.Config, error) {
	const resolveInputs = true

	nodes, inputs, comps, err := constructNodes(components, nil, rootDir, resolveInputs)
	if err != nil {
		return nil, nil, errors.AddContext(err, "could not resolve target inputs")
	}

	targets := make(map[target.ID][]input.ID, len(nodes))

	for id, n := range nodes {
		ids := n.Target.Inputs

		if ids == nil {
			compID := input.DefineIDComp(n.Comp.Name())
			ids = []input.ID{compID}

			if _, exists := inputs[compID]; !exists {
				comp := comps[n.Comp.Name()]
				cfg := &input.Config{
					Patterns: []string{"^.*$"},
					BaseDir:  comp.Root(),
				}
				cfg.Init(compID)
				inputs[compID] = cfg
			}
		}

		targets[id] = ids
	}

	return targets, inputs, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run:
`go test -tags 'debug test test_small' ./pkg/dag/... -run ResolveTargetInputs -v`
Expected: PASS — 2 tests.

- [ ] **Step 6: Run the whole dag suite to check nothing regressed**

Run:
`go test -tags 'debug test test_small test_large test_all' ./pkg/dag/... -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/dag/inputs-resolve.go pkg/dag/inputs-resolve_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(dag): expose target to input-set resolution for the watcher"
```

---

### Task 8: Protobuf schema and code generation

**Files:**

- Create: `pkg/watcher/proto/watcher.proto`
- Create (generated, checked in): `pkg/watcher/proto/watcher.pb.go`,
  `pkg/watcher/proto/watcher_grpc.pb.go`
- Modify: `tools/nix/pkgs/shells.parts.nix` (add codegen tools to the `general`
  shell)
- Modify: `justfile` (add the `generate-proto` recipe)
- Modify: `go.mod`, `go.sum`

**Interfaces:**

- Consumes: nothing.
- Produces: Go package `watcherv1` at
  `github.com/sdsc-ordes/quitsh/pkg/watcher/proto` with `WatcherClient`,
  `WatcherServer`, `RegisterWatcherServer`, `UnimplementedWatcherServer`, and
  the message types `GetStatusRequest/Response`, `TargetStatus`, `LastRun`,
  `TargetResult`, `ReportResultRequest/Response`, `RescanRequest/Response`,
  `ResetRequest/Response`, `InfoRequest/Response`, `ShutdownRequest/Response`,
  plus `Status` enum values `STATUS_UNSPECIFIED`, `STATUS_SUCCESS`,
  `STATUS_FAILED`.

- [ ] **Step 1: Write `pkg/watcher/proto/watcher.proto`**

```proto
syntax = "proto3";

package quitsh.watcher.v1;

option go_package = "github.com/sdsc-ordes/quitsh/pkg/watcher/proto;watcherv1";

// Watcher answers whether a target's own inputs changed since the target was
// last built successfully. It knows nothing about target dependencies:
// dependency propagation happens client-side in `pkg/dag`.
service Watcher {
  rpc GetStatus(GetStatusRequest) returns (GetStatusResponse);
  rpc ReportResult(ReportResultRequest) returns (ReportResultResponse);
  rpc Rescan(RescanRequest) returns (RescanResponse);
  rpc Reset(ResetRequest) returns (ResetResponse);
  rpc Info(InfoRequest) returns (InfoResponse);
  rpc Shutdown(ShutdownRequest) returns (ShutdownResponse);
}

enum Status {
  STATUS_UNSPECIFIED = 0;
  STATUS_SUCCESS = 1;
  STATUS_FAILED = 2;
}

message LastRun {
  Status status = 1;
  int64 scan_id = 2;
  int64 at_unix_ms = 3;
}

message TargetStatus {
  string target_id = 1;
  // Own inputs only. No dependency propagation.
  bool dirty = 2;
  bool has_successful_build = 3;
  repeated string dirty_inputs = 4;
  LastRun last_run = 5;
}

message GetStatusRequest {
  // Empty means all known targets.
  repeated string target_ids = 1;
  // 0 means the answer must come from a scan started after this request.
  int64 max_age_ms = 2;
}

message GetStatusResponse {
  int64 scan_id = 1;
  int64 scanned_at_unix_ms = 2;
  repeated TargetStatus statuses = 3;
}

message TargetResult {
  string target_id = 1;
  Status status = 2;
}

message ReportResultRequest {
  // The scan the caller based its decision on.
  int64 scan_id = 1;
  repeated TargetResult results = 2;
}

message ReportResultResponse {
  // Targets whose scan id is no longer resolvable; they stay dirty.
  repeated string not_recorded = 1;
}

message RescanRequest {}
message RescanResponse {
  int64 scan_id = 1;
}

message ResetRequest {
  // Empty means all targets.
  repeated string target_ids = 1;
}
message ResetResponse {}

message InfoRequest {}
message InfoResponse {
  string version = 1;
  string root_dir = 2;
  int64 last_scan_id = 3;
  int64 last_scan_at_unix_ms = 4;
  int64 last_scan_duration_ms = 5;
  int64 target_count = 6;
  int64 file_count = 7;
}

message ShutdownRequest {}
message ShutdownResponse {}
```

- [ ] **Step 2: Add the codegen tools to the Nix dev shell**

In `tools/nix/pkgs/shells.parts.nix`, inside the `general` toolchain's
`packages = [ ... ]` list (the one that already holds
`pkgs.golangci-lint-langserver`, `pkgs.typos-lsp`, `pkgs.hyperfine`), add:

```nix
                    pkgs.protobuf
                    pkgs.protoc-gen-go
                    pkgs.protoc-gen-go-grpc
```

Generated code is committed, so `build-go` and `ci` shells deliberately do
**not** get these tools.

- [ ] **Step 3: Add the justfile recipe**

Append to `justfile` after the `format` recipe:

```just
# Generate the Go code for the watcher gRPC service.
generate-proto:
    protoc -I pkg/watcher/proto \
        --go_out=. --go_opt=module=github.com/sdsc-ordes/quitsh \
        --go-grpc_out=. --go-grpc_opt=module=github.com/sdsc-ordes/quitsh \
        pkg/watcher/proto/watcher.proto
```

- [ ] **Step 4: Add the Go dependencies**

Run:

```bash
go get google.golang.org/grpc@latest
go get google.golang.org/protobuf@latest
```

- [ ] **Step 5: Generate the code**

Run (inside the dev shell, i.e. `just develop` or `nix develop ./tools/nix`):

```bash
just generate-proto
```

Expected: `pkg/watcher/proto/watcher.pb.go` and
`pkg/watcher/proto/watcher_grpc.pb.go` appear.

- [ ] **Step 6: Verify it compiles**

Run: `go build ./...` Expected: no output (success).

- [ ] **Step 7: Commit**

```bash
git add pkg/watcher/proto justfile tools/nix/pkgs/shells.parts.nix go.mod go.sum
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): add quitsh.watcher.v1 gRPC schema and codegen"
```

---

### Task 9: Server core — scan loop, freshness and re-discovery

**Files:**

- Create: `pkg/watcher/server/server.go`
- Test: `pkg/watcher/server/server_test.go`

**Interfaces:**

- Consumes: `watcher.Args`, `watcher.Tracker`, `watcher.ScanID`,
  `watcher.Stamp`, `watcher.NewHasher`, `watcher.LoadState`, `watcher.SaveState`
  (Tasks 1–6); `scan.New` (Task 2); `dag.ResolveTargetInputs` (Task 7).
- Produces: `server.DiscoverFunc` (`func() ([]*component.Component, error)`),
  `server.New(args *watcher.Args, rootDir string, configFileName string, discover DiscoverFunc) (*Server, error)`,
  `(*Server).ScanOnce() (watcher.ScanID, error)`,
  `(*Server).EnsureFresh(ctx context.Context, notBefore time.Time) (watcher.ScanID, error)`,
  `(*Server).Run(ctx context.Context) error`,
  `(*Server).Tracker() *watcher.Tracker`, `(*Server).Flush() error`,
  `(*Server).Stats() Stats` where
  `Stats{ScanID watcher.ScanID; StartedAt time.Time; Duration time.Duration; Files int; Targets int; RootDir string}`.

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/server/server_test.go`:

```go
//go:build test && (test_small || test_all)

package server

import (
	"context"
	"os"
	"path"
	"testing"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const configFileName = ".component.yaml"

func testRepo(t *testing.T) (root string, discover DiscoverFunc) {
	t.Helper()
	root = t.TempDir()

	require.NoError(t, os.MkdirAll(path.Join(root, "comp-a/src"), 0o750))
	require.NoError(t, os.WriteFile(
		path.Join(root, "comp-a", configFileName), []byte("name: comp-a\n"), 0o600))
	require.NoError(t, os.WriteFile(
		path.Join(root, "comp-a/src/a.go"), []byte("package a"), 0o600))

	discover = func() ([]*component.Component, error) {
		conf := &component.Config{
			Name:     "comp-a",
			Language: "go",
			Inputs: map[string]*input.Config{
				"srcs": {Patterns: []string{`^src/.*\.go$`}},
			},
			Targets: map[string]*target.Config{
				"build": {Inputs: []input.ID{"self::srcs"}},
			},
		}
		if err := conf.Init(); err != nil {
			return nil, err
		}
		c := component.NewComponent(conf, path.Join(root, "comp-a"), "", "")

		return []*component.Component{&c}, nil
	}

	return root, discover
}

func newServer(t *testing.T, root string, discover DiscoverFunc) *Server {
	t.Helper()
	args := &watcher.Args{
		ScanInterval: 10 * time.Millisecond,
		HashMode:     watcher.HashModeMTimeSize,
		StateFile:    path.Join(t.TempDir(), "state.json"),
	}
	s, err := New(args, root, configFileName, discover)
	require.NoError(t, err)

	return s
}

func TestScanOnceIncrementsScanID(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	first, err := s.ScanOnce()
	require.NoError(t, err)
	second, err := s.ScanOnce()
	require.NoError(t, err)

	assert.Equal(t, watcher.ScanID(1), first)
	assert.Equal(t, watcher.ScanID(2), second)
}

func TestServerTracksTargetOfDiscoveredComponents(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	_, err := s.ScanOnce()
	require.NoError(t, err)

	st := s.Tracker().Status([]target.ID{"comp-a::build"})
	require.Len(t, st, 1)
	assert.True(t, st[0].Dirty, "never built -> dirty")
	assert.False(t, st[0].HasSuccessfulBuild)
}

func TestEnsureFreshRunsAScanStartedAfterTheRequest(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	_, err := s.ScanOnce()
	require.NoError(t, err)

	notBefore := time.Now()
	id, err := s.EnsureFresh(t.Context(), notBefore)
	require.NoError(t, err)

	assert.Equal(t, watcher.ScanID(2), id)
	assert.False(t, s.Stats().StartedAt.Before(notBefore))
}

func TestEnsureFreshReusesAFreshEnoughScan(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	id, err := s.ScanOnce()
	require.NoError(t, err)

	// A scan that already started after this instant satisfies the request.
	got, err := s.EnsureFresh(t.Context(), time.Now().Add(-time.Hour))
	require.NoError(t, err)
	assert.Equal(t, id, got)
}

func TestComponentConfigChangeTriggersRediscovery(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)

	calls := 0
	counting := func() ([]*component.Component, error) {
		calls++

		return discover()
	}

	s := newServer(t, root, counting)
	_, err := s.ScanOnce()
	require.NoError(t, err)
	before := calls

	_, err = s.ScanOnce()
	require.NoError(t, err)
	assert.Equal(t, before, calls, "unchanged configs must not re-discover")

	require.NoError(t, os.WriteFile(
		path.Join(root, "comp-a", configFileName),
		[]byte("name: comp-a\n# touched\n"), 0o600))

	_, err = s.ScanOnce()
	require.NoError(t, err)
	assert.Greater(t, calls, before)
}

func TestRunStopsOnContextCancel(t *testing.T) {
	t.Parallel()
	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- s.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/server/... -v`
Expected: FAIL — `undefined: New`, `undefined: DiscoverFunc`.

- [ ] **Step 3: Write `pkg/watcher/server/server.go`**

```go
// Package server implements the `quitsh server` change-tracking watcher.
package server

import (
	"context"
	"path"
	"sync"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/dag"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	"github.com/sdsc-ordes/quitsh/pkg/watcher/scan"
)

// stateFlushInterval is how often durable state is written while running.
const stateFlushInterval = 30 * time.Second

type (
	// DiscoverFunc finds all components in the repository.
	DiscoverFunc func() ([]*component.Component, error)

	// Stats describes the last scan, for `Info` and `quitsh server status`.
	Stats struct {
		RootDir   string
		ScanID    watcher.ScanID
		StartedAt time.Time
		Duration  time.Duration
		Files     int
		Targets   int
	}

	// Server owns the tracker and the scan loop.
	// It is the only writer of tracker state.
	Server struct {
		args           *watcher.Args
		rootDir        string
		configFileName string
		discover       DiscoverFunc
		stateFile      string

		tracker *watcher.Tracker
		scanner *scan.Scanner

		mu          sync.Mutex
		files       map[string]watcher.Stamp
		configs     map[string]watcher.Stamp
		lastScanID  watcher.ScanID
		startedAt   time.Time
		duration    time.Duration
		targetCount int
		scanning    bool
		scanDone    chan struct{}
	}
)

// New creates a server for the repository at `rootDir`.
// The loaded durable state is installed immediately; the first scan (done by
// [Server.ScanOnce] or [Server.Run]) then decides what is dirty.
func New(
	args *watcher.Args,
	rootDir string,
	configFileName string,
	discover DiscoverFunc,
) (*Server, error) {
	hasher, err := watcher.NewHasher(args.HashMode)
	if err != nil {
		return nil, err
	}

	scanner, err := scan.New(rootDir, hasher, args.ResolveExcludes())
	if err != nil {
		return nil, err
	}

	stateFile := args.ResolveStateFile(rootDir)

	state, err := watcher.LoadState(stateFile)
	if err != nil {
		// A broken state file must not stop the server; everything is dirty.
		log.WarnE(err, "Could not load watcher state, starting empty.", "file", stateFile)
		state = watcher.State{Version: watcher.StateVersion}
	}

	tracker := watcher.NewTracker()
	tracker.Import(state)

	return &Server{
		args:           args,
		rootDir:        rootDir,
		configFileName: configFileName,
		discover:       discover,
		stateFile:      stateFile,
		tracker:        tracker,
		scanner:        scanner,
		configs:        map[string]watcher.Stamp{},
	}, nil
}

// Tracker returns the tracker owned by this server.
func (s *Server) Tracker() *watcher.Tracker {
	return s.tracker
}

// Stats returns information about the last scan.
func (s *Server) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()

	return Stats{
		RootDir:   s.rootDir,
		ScanID:    s.lastScanID,
		StartedAt: s.startedAt,
		Duration:  s.duration,
		Files:     len(s.files),
		Targets:   s.targetCount,
	}
}

// ScanOnce performs one scan, re-discovering components when any component
// config file changed, and installs the result into the tracker.
func (s *Server) ScanOnce() (watcher.ScanID, error) {
	started := time.Now()

	s.mu.Lock()
	prev := s.files
	s.mu.Unlock()

	files, err := s.scanner.Scan(prev)
	if err != nil {
		return 0, err
	}

	configs := s.collectConfigs(files)

	s.mu.Lock()
	changedConfigs := !sameStamps(s.configs, configs)
	s.configs = configs
	s.mu.Unlock()

	if changedConfigs {
		if e := s.rediscover(); e != nil {
			return 0, e
		}
	}

	s.mu.Lock()
	s.lastScanID++
	id := s.lastScanID
	s.files = files
	s.startedAt = started
	s.duration = time.Since(started)
	s.mu.Unlock()

	s.tracker.Update(id, files)

	log.Trace("Scan done.", "scan", id, "files", len(files), "took", time.Since(started))

	return id, nil
}

// EnsureFresh returns the id of a scan which started at or after `notBefore`,
// running or joining a scan when needed.
func (s *Server) EnsureFresh(ctx context.Context, notBefore time.Time) (watcher.ScanID, error) {
	for {
		s.mu.Lock()

		if s.lastScanID != 0 && !s.startedAt.Before(notBefore) {
			id := s.lastScanID
			s.mu.Unlock()

			return id, nil
		}

		if s.scanning {
			done := s.scanDone
			s.mu.Unlock()

			select {
			case <-done:
			case <-ctx.Done():
				return 0, ctx.Err()
			}

			continue
		}

		s.scanning = true
		s.scanDone = make(chan struct{})
		done := s.scanDone
		s.mu.Unlock()

		id, err := s.ScanOnce()

		s.mu.Lock()
		s.scanning = false
		s.mu.Unlock()
		close(done)

		if err != nil {
			return 0, err
		}

		return id, nil
	}
}

// Run scans periodically and flushes durable state until `ctx` is done.
func (s *Server) Run(ctx context.Context) error {
	scanTicker := time.NewTicker(s.args.ScanInterval)
	defer scanTicker.Stop()

	flushTicker := time.NewTicker(stateFlushInterval)
	defer flushTicker.Stop()

	if _, err := s.EnsureFresh(ctx, time.Now()); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return s.Flush()

		case <-scanTicker.C:
			if _, err := s.EnsureFresh(ctx, time.Now()); err != nil {
				if ctx.Err() != nil {
					return s.Flush()
				}
				log.WarnE(err, "Scan failed.")
			}

		case <-flushTicker.C:
			log.WarnE(s.Flush(), "Could not flush watcher state.")
		}
	}
}

// Flush writes the durable state to disk.
func (s *Server) Flush() error {
	return watcher.SaveState(s.stateFile, s.tracker.Export())
}

// rediscover reloads components and installs their input sets into the tracker.
func (s *Server) rediscover() error {
	comps, err := s.discover()
	if err != nil {
		return errors.AddContext(err, "could not discover components")
	}

	targets, inputs, err := dag.ResolveTargetInputs(comps, s.rootDir)
	if err != nil {
		return err
	}

	sets := make(watcher.InputSets, len(inputs))
	for id, cfg := range inputs {
		set, e := watcher.NewInputSet(cfg, s.rootDir)
		if e != nil {
			return e
		}
		sets[id] = set
	}

	s.tracker.SetComponents(sets, targets)

	s.mu.Lock()
	s.targetCount = len(targets)
	s.mu.Unlock()

	log.Info("Components discovered.", "components", len(comps), "targets", len(targets))

	return nil
}

// collectConfigs returns the stamps of all component config files.
func (s *Server) collectConfigs(files map[string]watcher.Stamp) map[string]watcher.Stamp {
	configs := make(map[string]watcher.Stamp)

	for p, stamp := range files {
		if path.Base(p) == s.configFileName {
			configs[p] = stamp
		}
	}

	return configs
}

func sameStamps(a map[string]watcher.Stamp, b map[string]watcher.Stamp) bool {
	if len(a) != len(b) {
		return false
	}

	for k, v := range a {
		if o, ok := b[k]; !ok || o != v {
			return false
		}
	}

	return true
}
```

Note: the very first `ScanOnce` sees `s.configs` empty and `configs` non-empty,
so `rediscover` runs before the first `Update` — targets are known from scan 1
on. A repository with no component config files at all would never discover;
that is acceptable because there is nothing to track.

- [ ] **Step 4: Run test to verify it passes**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/server/... -v`
Expected: PASS — 6 tests.

- [ ] **Step 5: Commit**

```bash
git add pkg/watcher/server/server.go pkg/watcher/server/server_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): add server scan loop with freshness and re-discovery"
```

---

### Task 10: gRPC service and socket lifecycle

**Files:**

- Create: `pkg/watcher/server/service.go`
- Create: `pkg/watcher/server/socket.go`
- Test: `pkg/watcher/server/service_test.go`

**Interfaces:**

- Consumes: `Server` (Task 9), generated `watcherv1` package (Task 8).
- Produces: `server.Listen(address string) (net.Listener, error)`,
  `(*Server).Serve(ctx context.Context, address string, version string) error`,
  and the unexported `service` implementing `watcherv1.WatcherServer`.

Design points fixed here:

- `GetStatus` pins the scan id it hands out, and a `time.AfterFunc(pinTTL, …)`
  releases it. This keeps `digestAt` resolvable for builds of any length without
  unbounded growth.
- A unix address whose socket file exists is probed by dialling it: a refused
  connection means a stale file (remove it), a successful one means another
  server owns the repository (error out).

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/server/service_test.go`:

```go
//go:build test && (test_small || test_all)

package server

import (
	"context"
	"path"
	"testing"
	"time"

	watcherv1 "github.com/sdsc-ordes/quitsh/pkg/watcher/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// serve starts a server on a unix socket and returns a connected client.
func serve(t *testing.T) (watcherv1.WatcherClient, string) {
	t.Helper()

	root, discover := testRepo(t)
	s := newServer(t, root, discover)

	address := "unix://" + path.Join(t.TempDir(), "w.sock")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, address, "test") }()

	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("server did not stop")
		}
	})

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	return watcherv1.NewWatcherClient(conn), root
}

func TestGetStatusReportsDirtyTarget(t *testing.T) {
	t.Parallel()
	client, _ := serve(t)

	resp, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{"comp-a::build"},
	})
	require.NoError(t, err)

	require.Len(t, resp.GetStatuses(), 1)
	assert.True(t, resp.GetStatuses()[0].GetDirty())
	assert.False(t, resp.GetStatuses()[0].GetHasSuccessfulBuild())
	assert.NotZero(t, resp.GetScanId())
}

func TestReportResultMakesTargetClean(t *testing.T) {
	t.Parallel()
	client, _ := serve(t)

	first, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{"comp-a::build"},
	})
	require.NoError(t, err)

	rep, err := client.ReportResult(t.Context(), &watcherv1.ReportResultRequest{
		ScanId: first.GetScanId(),
		Results: []*watcherv1.TargetResult{
			{TargetId: "comp-a::build", Status: watcherv1.Status_STATUS_SUCCESS},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, rep.GetNotRecorded())

	second, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{"comp-a::build"},
	})
	require.NoError(t, err)
	assert.False(t, second.GetStatuses()[0].GetDirty())
}

func TestResetMakesTargetDirtyAgain(t *testing.T) {
	t.Parallel()
	client, _ := serve(t)

	first, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{"comp-a::build"},
	})
	require.NoError(t, err)

	_, err = client.ReportResult(t.Context(), &watcherv1.ReportResultRequest{
		ScanId: first.GetScanId(),
		Results: []*watcherv1.TargetResult{
			{TargetId: "comp-a::build", Status: watcherv1.Status_STATUS_SUCCESS},
		},
	})
	require.NoError(t, err)

	_, err = client.Reset(t.Context(), &watcherv1.ResetRequest{
		TargetIds: []string{"comp-a::build"},
	})
	require.NoError(t, err)

	after, err := client.GetStatus(t.Context(), &watcherv1.GetStatusRequest{
		TargetIds: []string{"comp-a::build"},
	})
	require.NoError(t, err)
	assert.True(t, after.GetStatuses()[0].GetDirty())
}

func TestInfoReportsRootAndVersion(t *testing.T) {
	t.Parallel()
	client, root := serve(t)

	info, err := client.Info(t.Context(), &watcherv1.InfoRequest{})
	require.NoError(t, err)

	assert.Equal(t, "test", info.GetVersion())
	assert.Equal(t, root, info.GetRootDir())
}

func TestListenRefusesWhenAnotherServerOwnsTheSocket(t *testing.T) {
	t.Parallel()
	_, _ = serve(t) // holds its own socket

	address := "unix://" + path.Join(t.TempDir(), "taken.sock")

	l, err := Listen(address)
	require.NoError(t, err)
	defer l.Close() //nolint:errcheck // test.

	_, err = Listen(address)
	assert.Error(t, err, "a live socket must not be stolen")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
`go test -tags 'debug test test_small' ./pkg/watcher/server/... -run 'GetStatus|Report|Reset|Info|Listen' -v`
Expected: FAIL — `undefined: Listen`, `s.Serve undefined`.

- [ ] **Step 3: Write `pkg/watcher/server/socket.go`**

```go
package server

import (
	"net"
	"os"
	"path"
	"strings"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
)

const (
	socketDirPerms  = 0o750
	socketFilePerms = 0o600
	probeTimeout    = 300 * time.Millisecond
)

// splitAddress splits a gRPC target into network and address.
func splitAddress(address string) (network string, addr string, err error) {
	switch {
	case strings.HasPrefix(address, "unix://"):
		return "unix", strings.TrimPrefix(address, "unix://"), nil
	case strings.HasPrefix(address, "tcp://"):
		return "tcp", strings.TrimPrefix(address, "tcp://"), nil
	default:
		return "", "", errors.New(
			"watcher address '%v' must start with 'unix://' or 'tcp://'", address)
	}
}

// Listen creates the listener for `address`, cleaning up a stale unix socket.
// It fails when another server is already listening.
func Listen(address string) (net.Listener, error) {
	network, addr, err := splitAddress(address)
	if err != nil {
		return nil, err
	}

	if network == "tcp" {
		l, e := net.Listen(network, addr)

		return l, errors.AddContext(e, "could not listen on '%v'", address)
	}

	if e := os.MkdirAll(path.Dir(addr), socketDirPerms); e != nil {
		return nil, errors.AddContext(e, "could not create socket dir for '%v'", addr)
	}

	if _, e := os.Stat(addr); e == nil {
		conn, de := net.DialTimeout(network, addr, probeTimeout)
		if de == nil {
			_ = conn.Close()

			return nil, errors.New(
				"a watcher server is already running on '%v'", address)
		}

		log.Debug("Removing stale socket.", "socket", addr)
		if re := os.Remove(addr); re != nil {
			return nil, errors.AddContext(re, "could not remove stale socket '%v'", addr)
		}
	}

	l, err := net.Listen(network, addr)
	if err != nil {
		return nil, errors.AddContext(err, "could not listen on '%v'", address)
	}

	if e := os.Chmod(addr, socketFilePerms); e != nil {
		_ = l.Close()

		return nil, errors.AddContext(e, "could not set permissions on '%v'", addr)
	}

	return l, nil
}
```

- [ ] **Step 4: Write `pkg/watcher/server/service.go`**

```go
package server

import (
	"context"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherv1 "github.com/sdsc-ordes/quitsh/pkg/watcher/proto"

	"google.golang.org/grpc"
)

// pinTTL is how long a handed-out scan id stays resolvable for a reporting
// client. Builds longer than this simply do not get recorded.
const pinTTL = time.Hour

type service struct {
	watcherv1.UnimplementedWatcherServer

	srv      *Server
	version  string
	shutdown context.CancelFunc
}

// Serve runs the gRPC service and the scan loop until `ctx` is done.
func (s *Server) Serve(ctx context.Context, address string, version string) error {
	listener, err := Listen(address)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	grpcServer := grpc.NewServer()
	watcherv1.RegisterWatcherServer(grpcServer, &service{
		srv:      s,
		version:  version,
		shutdown: cancel,
	})

	loopDone := make(chan error, 1)
	go func() { loopDone <- s.Run(ctx) }()

	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()

	log.Info("Watcher server listening.", "address", address, "root", s.rootDir)

	select {
	case <-ctx.Done():
	case err = <-serveDone:
		cancel()
	}

	grpcServer.GracefulStop()

	if e := <-loopDone; e != nil && err == nil {
		err = e
	}

	log.Info("Watcher server stopped.")

	return err
}

func (s *service) GetStatus(
	ctx context.Context,
	req *watcherv1.GetStatusRequest,
) (*watcherv1.GetStatusResponse, error) {
	notBefore := time.Now().Add(-time.Duration(req.GetMaxAgeMs()) * time.Millisecond)

	scanID, err := s.srv.EnsureFresh(ctx, notBefore)
	if err != nil {
		return nil, err
	}

	ids := make([]target.ID, 0, len(req.GetTargetIds()))
	for _, id := range req.GetTargetIds() {
		ids = append(ids, target.ID(id))
	}

	statuses := s.srv.Tracker().Status(ids)

	// Keep the handed-out scan id resolvable until the client reports.
	s.srv.Tracker().Pin(scanID)
	time.AfterFunc(pinTTL, func() { s.srv.Tracker().Unpin(scanID) })

	stats := s.srv.Stats()
	resp := &watcherv1.GetStatusResponse{
		ScanId:          int64(scanID),
		ScannedAtUnixMs: stats.StartedAt.UnixMilli(),
		Statuses:        make([]*watcherv1.TargetStatus, 0, len(statuses)),
	}

	for i := range statuses {
		resp.Statuses = append(resp.Statuses, toProtoStatus(&statuses[i]))
	}

	return resp, nil
}

func toProtoStatus(st *watcher.TargetStatus) *watcherv1.TargetStatus {
	p := &watcherv1.TargetStatus{
		TargetId:           st.ID.String(),
		Dirty:              st.Dirty,
		HasSuccessfulBuild: st.HasSuccessfulBuild,
		DirtyInputs:        make([]string, 0, len(st.DirtyInputs)),
	}

	for _, in := range st.DirtyInputs {
		p.DirtyInputs = append(p.DirtyInputs, string(in))
	}

	if st.LastRun != nil {
		status := watcherv1.Status_STATUS_FAILED
		if st.LastRun.Success {
			status = watcherv1.Status_STATUS_SUCCESS
		}

		p.LastRun = &watcherv1.LastRun{
			Status:   status,
			ScanId:   int64(st.LastRun.ScanID),
			AtUnixMs: st.LastRun.AtUnixMs,
		}
	}

	return p
}

func (s *service) ReportResult(
	_ context.Context,
	req *watcherv1.ReportResultRequest,
) (*watcherv1.ReportResultResponse, error) {
	results := make(map[target.ID]bool, len(req.GetResults()))
	for _, r := range req.GetResults() {
		results[target.ID(r.GetTargetId())] = r.GetStatus() == watcherv1.Status_STATUS_SUCCESS
	}

	notRecorded := s.srv.Tracker().Report(watcher.ScanID(req.GetScanId()), results)

	resp := &watcherv1.ReportResultResponse{
		NotRecorded: make([]string, 0, len(notRecorded)),
	}
	for _, id := range notRecorded {
		resp.NotRecorded = append(resp.NotRecorded, id.String())
	}

	return resp, nil
}

func (s *service) Rescan(
	ctx context.Context,
	_ *watcherv1.RescanRequest,
) (*watcherv1.RescanResponse, error) {
	id, err := s.srv.EnsureFresh(ctx, time.Now())
	if err != nil {
		return nil, err
	}

	return &watcherv1.RescanResponse{ScanId: int64(id)}, nil
}

func (s *service) Reset(
	_ context.Context,
	req *watcherv1.ResetRequest,
) (*watcherv1.ResetResponse, error) {
	ids := make([]target.ID, 0, len(req.GetTargetIds()))
	for _, id := range req.GetTargetIds() {
		ids = append(ids, target.ID(id))
	}

	s.srv.Tracker().Reset(ids...)

	return &watcherv1.ResetResponse{}, nil
}

func (s *service) Info(
	_ context.Context,
	_ *watcherv1.InfoRequest,
) (*watcherv1.InfoResponse, error) {
	stats := s.srv.Stats()

	return &watcherv1.InfoResponse{
		Version:            s.version,
		RootDir:            stats.RootDir,
		LastScanId:         int64(stats.ScanID),
		LastScanAtUnixMs:   stats.StartedAt.UnixMilli(),
		LastScanDurationMs: stats.Duration.Milliseconds(),
		TargetCount:        int64(stats.Targets),
		FileCount:          int64(stats.Files),
	}, nil
}

func (s *service) Shutdown(
	_ context.Context,
	_ *watcherv1.ShutdownRequest,
) (*watcherv1.ShutdownResponse, error) {
	log.Info("Shutdown requested.")
	s.shutdown()

	return &watcherv1.ShutdownResponse{}, nil
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/server/... -v`
Expected: PASS — all server tests.

- [ ] **Step 6: Commit**

```bash
git add pkg/watcher/server
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): serve the tracker over gRPC on a unix socket"
```

---

### Task 11: Watcher client

**Files:**

- Create: `pkg/watcher/version.go`
- Create: `pkg/watcher/client/client.go`
- Test: `pkg/watcher/client/client_test.go`

**Interfaces:**

- Consumes: generated `watcherv1` (Task 8), `server.Serve` (Task 10) in tests.
- Produces:
  - `watcher.ProtocolVersion` (`const string = "1"`).
  - `client.Client`,
    `client.Dial(args *watcher.Args, rootDir string) (*Client, error)`,
    `(*Client).Close() error`,
    `(*Client).Status(ctx context.Context, ids []target.ID, maxAge time.Duration) (Result, error)`,
    `(*Client).Report(ctx context.Context, scanID int64, results map[target.ID]bool) ([]target.ID, error)`,
    `(*Client).Info(ctx context.Context) (*watcherv1.InfoResponse, error)`,
    `(*Client).Rescan(ctx) (int64, error)`,
    `(*Client).Reset(ctx, ids []target.ID) error`,
    `(*Client).Shutdown(ctx) error`.
  - `client.Result` —
    `struct { ScanID int64; Dirty map[target.ID]bool; DirtyInputs map[target.ID][]string; HasBuild map[target.ID]bool }`.
  - `client.QueryDirty(ctx context.Context, args *watcher.Args, rootDir string, ids []target.ID) Result`
    — never fails: on any error it logs and returns a zero `Result`
    (`Dirty == nil`), which callers must read as **all targets dirty**.
  - `client.ReportResults(ctx context.Context, args *watcher.Args, rootDir string, scanID int64, results map[target.ID]bool)`
    — never fails; logs and returns.

- [ ] **Step 1: Write the failing test**

Create `pkg/watcher/client/client_test.go`:

```go
//go:build test && (test_small || test_all)

package client

import (
	"path"
	"testing"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryDirtyWithoutServerReportsNothingKnown(t *testing.T) {
	t.Parallel()
	args := &watcher.Args{
		Address: "unix://" + path.Join(t.TempDir(), "absent.sock"),
		Timeout: 200 * time.Millisecond,
	}

	res := QueryDirty(t.Context(), args, t.TempDir(), []target.ID{"a::b"})

	assert.Nil(t, res.Dirty, "an unreachable server must mean 'everything dirty'")
	assert.Zero(t, res.ScanID)
}

func TestReportResultsWithoutServerDoesNotPanic(t *testing.T) {
	t.Parallel()
	args := &watcher.Args{
		Address: "unix://" + path.Join(t.TempDir(), "absent.sock"),
		Timeout: 200 * time.Millisecond,
	}

	require.NotPanics(t, func() {
		ReportResults(t.Context(), args, t.TempDir(), 1,
			map[target.ID]bool{"a::b": true})
	})
}
```

A test against a live server lives in the integration test (Task 18); this
package only needs its degradation path covered here.

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/client/... -v`
Expected: FAIL — `undefined: QueryDirty`.

- [ ] **Step 3: Write `pkg/watcher/version.go`**

```go
package watcher

// ProtocolVersion is the version of the watcher gRPC contract.
// A client refusing to talk to a differing server degrades to
// "every target is dirty".
const ProtocolVersion = "1"
```

- [ ] **Step 4: Write `pkg/watcher/client/client.go`**

```go
// Package client talks to the `quitsh server` watcher.
//
// Every helper in this package degrades instead of failing: when the server is
// absent, slow, or speaks another protocol version, callers are told that
// nothing is known, which means every target must be built.
package client

import (
	"context"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherv1 "github.com/sdsc-ordes/quitsh/pkg/watcher/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type (
	// Client is a connection to the watcher server.
	Client struct {
		conn    *grpc.ClientConn
		api     watcherv1.WatcherClient
		timeout time.Duration
	}

	// Result is the answer to a status query.
	// A `nil` `Dirty` map means nothing is known: build everything.
	Result struct {
		ScanID      int64
		Dirty       map[target.ID]bool
		DirtyInputs map[target.ID][]string
		HasBuild    map[target.ID]bool
	}
)

// Dial connects to the watcher server for the repository at `rootDir`.
func Dial(args *watcher.Args, rootDir string) (*Client, error) {
	address := args.ResolveAddress(rootDir)

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, errors.AddContext(err, "could not connect to watcher '%v'", address)
	}

	return &Client{conn: conn, api: watcherv1.NewWatcherClient(conn), timeout: args.Timeout}, nil
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.timeout <= 0 {
		return context.WithCancel(ctx)
	}

	return context.WithTimeout(ctx, c.timeout)
}

// Info returns the server information.
func (c *Client) Info(ctx context.Context) (*watcherv1.InfoResponse, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	return c.api.Info(ctx, &watcherv1.InfoRequest{})
}

// Status queries the dirty state of `ids` (all targets when empty).
// `maxAge` is how stale the answer may be; zero forces an answer from a scan
// started after this request, which is what a skip decision needs.
func (c *Client) Status(
	ctx context.Context,
	ids []target.ID,
	maxAge time.Duration,
) (Result, error) {
	req := &watcherv1.GetStatusRequest{
		TargetIds: make([]string, 0, len(ids)),
		MaxAgeMs:  maxAge.Milliseconds(),
	}
	for _, id := range ids {
		req.TargetIds = append(req.TargetIds, id.String())
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.api.GetStatus(ctx, req)
	if err != nil {
		return Result{}, errors.AddContext(err, "watcher status query failed")
	}

	res := Result{
		ScanID:      resp.GetScanId(),
		Dirty:       make(map[target.ID]bool, len(resp.GetStatuses())),
		DirtyInputs: make(map[target.ID][]string, len(resp.GetStatuses())),
		HasBuild:    make(map[target.ID]bool, len(resp.GetStatuses())),
	}

	for _, st := range resp.GetStatuses() {
		id := target.ID(st.GetTargetId())
		res.Dirty[id] = st.GetDirty()
		res.DirtyInputs[id] = st.GetDirtyInputs()
		res.HasBuild[id] = st.GetHasSuccessfulBuild()
	}

	return res, nil
}

// Report reports run results decided at `scanID`.
// It returns the targets the server could not record.
func (c *Client) Report(
	ctx context.Context,
	scanID int64,
	results map[target.ID]bool,
) ([]target.ID, error) {
	req := &watcherv1.ReportResultRequest{
		ScanId:  scanID,
		Results: make([]*watcherv1.TargetResult, 0, len(results)),
	}

	for id, success := range results {
		status := watcherv1.Status_STATUS_FAILED
		if success {
			status = watcherv1.Status_STATUS_SUCCESS
		}

		req.Results = append(req.Results,
			&watcherv1.TargetResult{TargetId: id.String(), Status: status})
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.api.ReportResult(ctx, req)
	if err != nil {
		return nil, errors.AddContext(err, "watcher report failed")
	}

	notRecorded := make([]target.ID, 0, len(resp.GetNotRecorded()))
	for _, id := range resp.GetNotRecorded() {
		notRecorded = append(notRecorded, target.ID(id))
	}

	return notRecorded, nil
}

// Rescan forces a scan and returns its id.
func (c *Client) Rescan(ctx context.Context) (int64, error) {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	resp, err := c.api.Rescan(ctx, &watcherv1.RescanRequest{})
	if err != nil {
		return 0, err
	}

	return resp.GetScanId(), nil
}

// Reset clears the last successful build of `ids` (all when empty).
func (c *Client) Reset(ctx context.Context, ids []target.ID) error {
	req := &watcherv1.ResetRequest{TargetIds: make([]string, 0, len(ids))}
	for _, id := range ids {
		req.TargetIds = append(req.TargetIds, id.String())
	}

	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.api.Reset(ctx, req)

	return err
}

// Shutdown asks the server to stop.
func (c *Client) Shutdown(ctx context.Context) error {
	ctx, cancel := c.withTimeout(ctx)
	defer cancel()

	_, err := c.api.Shutdown(ctx, &watcherv1.ShutdownRequest{})

	return err
}

// QueryDirty asks the watcher which of `ids` are dirty.
// It never fails: a zero [Result] (with a `nil` `Dirty` map) means the watcher
// could not be consulted and every target must be treated as dirty.
func QueryDirty(
	ctx context.Context,
	args *watcher.Args,
	rootDir string,
	ids []target.ID,
) Result {
	c, err := Dial(args, rootDir)
	if err != nil {
		log.WarnE(err, "Watcher not usable, building everything.")

		return Result{}
	}
	defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

	info, err := c.Info(ctx)
	if err != nil {
		log.Debug("Watcher not reachable, building everything.", "error", err.Error())

		return Result{}
	}

	if info.GetVersion() != watcher.ProtocolVersion {
		log.Warn("Watcher protocol mismatch, building everything.",
			"server", info.GetVersion(), "client", watcher.ProtocolVersion)

		return Result{}
	}

	res, err := c.Status(ctx, ids, 0)
	if err != nil {
		log.WarnE(err, "Watcher status query failed, building everything.")

		return Result{}
	}

	return res
}

// ReportResults reports run results to the watcher. It never fails.
func ReportResults(
	ctx context.Context,
	args *watcher.Args,
	rootDir string,
	scanID int64,
	results map[target.ID]bool,
) {
	if len(results) == 0 {
		return
	}

	c, err := Dial(args, rootDir)
	if err != nil {
		log.WarnE(err, "Could not report results to the watcher.")

		return
	}
	defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

	notRecorded, err := c.Report(ctx, scanID, results)
	if err != nil {
		log.Debug("Could not report results to the watcher.", "error", err.Error())

		return
	}

	if len(notRecorded) != 0 {
		log.Debug("Watcher did not record some targets (scan id too old).",
			"targets", notRecorded)
	}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `go test -tags 'debug test test_small' ./pkg/watcher/client/... -v`
Expected: PASS — 2 tests.

- [ ] **Step 6: Commit**

```bash
git add pkg/watcher/version.go pkg/watcher/client
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(watcher): add gRPC client which degrades when the server is absent"
```

---

### Task 12: `ExecStatusSkipped` in the DAG executor

Without this, a clean dependency dragged into the subgraph by a dirty dependent
would still run. Marking it `Cancel` is not an option: cancelled nodes leave
their runners at `ExecStatusNotRun`, which makes `Status()` non-success and
`PropagateExecStatus` cancels everything downstream — including the dirty target
we actually wanted to build.

**Files:**

- Modify: `pkg/dag/status.go:17-19` (status constants),
  `pkg/dag/status.go:55-80` (`RunnerStatuses.log`)
- Modify: `pkg/dag/node.go:36-42` (`TargetExecStatus`), `pkg/dag/node.go:88-98`
  (`Status`)
- Modify: `pkg/dag/run.go:180-186` (`executeRunners` switch)
- Modify: `pkg/dag/run-concurrent.go:165-175` (task closure)
- Test: `pkg/dag/skip_test.go`

**Interfaces:**

- Consumes: nothing new.
- Produces: `dag.ExecStatusSkipped` (`ExecStatus = 3`), field
  `TargetExecStatus.Skip bool`.

- [ ] **Step 1: Write the failing test**

Create `pkg/dag/skip_test.go`:

```go
//go:build test && (test_small || test_all)

package dag

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
)

func TestSkippedNodeCountsAsSuccess(t *testing.T) {
	t.Parallel()
	n := &TargetNode{Target: &target.Config{ID: "c::a"}}
	n.Execution.Skip = true
	n.Execution.Runners = RunnerStatuses{{Status: ExecStatusSkipped}}

	assert.Equal(t, ExecStatus(ExecStatusSuccess), n.Status())
	assert.False(t, n.StatusAnyFailed())
}

func TestSkippedNodeDoesNotCancelDependents(t *testing.T) {
	t.Parallel()
	dep := &TargetNode{Target: &target.Config{ID: "c::dep"}}
	user := &TargetNode{Target: &target.Config{ID: "c::user"}}
	dep.Forward = []*TargetNode{user}
	user.Backward = []*TargetNode{dep}

	dep.Execution.Skip = true
	dep.Execution.Runners = RunnerStatuses{{Status: ExecStatusSkipped}}
	dep.PropagateExecStatus()

	assert.False(t, user.Execution.Cancel, "a skipped dependency must not cancel its dependents")
}

func TestNotRunNodeStillCancelsDependents(t *testing.T) {
	t.Parallel()
	dep := &TargetNode{Target: &target.Config{ID: "c::dep"}}
	user := &TargetNode{Target: &target.Config{ID: "c::user"}}
	dep.Forward = []*TargetNode{user}
	user.Backward = []*TargetNode{dep}

	dep.Execution.Runners = RunnerStatuses{{Status: ExecStatusNotRun}}
	dep.PropagateExecStatus()

	assert.True(t, user.Execution.Cancel, "failure propagation must keep working")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test -tags 'debug test test_small' ./pkg/dag/... -run Skip -v`
Expected: FAIL — `undefined: ExecStatusSkipped`, `Execution.Skip undefined`.

- [ ] **Step 3: Add the constant and log symbol in `pkg/dag/status.go`**

Replace the constant block:

```go
const ExecStatusNotRun = 0
const ExecStatusFailed = 1
const ExecStatusSuccess = 2

// ExecStatusSkipped marks a target which was up to date and therefore
// not executed. For propagation it counts as a success.
const ExecStatusSkipped = 3
```

In `RunnerStatuses.log`, add the symbol and the case:

```go
	const failedS = "❌"
	const successS = "🌻"
	const notRun = "🚫"
	const skipped = "⏭️"
```

```go
		case ExecStatusSkipped:
			statusS = skipped
```

- [ ] **Step 4: Add the `Skip` flag and fix `Status` in `pkg/dag/node.go`**

In `TargetExecStatus` add:

```go
	TargetExecStatus struct {
		// Marking the target to not run and skip.
		Cancel bool

		// Skip marks a target whose inputs did not change since its last
		// successful build. Its runners are not executed, but unlike `Cancel`
		// this counts as a success for dependents.
		Skip bool

		// All runner statuses for the steps.
		Runners RunnerStatuses
	}
```

Replace `Status`:

```go
// Status determines the overall status of the target.
// Skipped runners count as successful: a target which was up to date must not
// cancel the targets depending on it.
func (n *TargetNode) Status() ExecStatus {
	for _, r := range n.Execution.Runners {
		if r.Status != ExecStatusSuccess && r.Status != ExecStatusSkipped {
			return ExecStatusFailed
		}
	}

	return ExecStatusSuccess
}
```

- [ ] **Step 5: Skip runners in `pkg/dag/run.go`**

In `executeRunners`, add a case **before** the `Cancel` case:

```go
		switch {
		case rD.node.Execution.Skip:
			rD.status.Status = ExecStatusSkipped
			log.Debugf(
				"Target '%v' is up to date. Skip runner '%v'.",
				rD.node.Target.ID,
				rD.inst.RunnerID,
			)

		case rD.node.Execution.Cancel:
```

- [ ] **Step 6: Skip runners in `pkg/dag/run-concurrent.go`**

In the task closure, add before the `Cancel` check:

```go
			if node.Execution.Skip {
				status.Status = ExecStatusSkipped
				log.Debugf(
					"Target '%v' is up to date. Skip runner '%v'.",
					node.Target.ID,
					runner.RunnerID,
				)

				return
			}

			if node.Execution.Cancel {
```

(The existing `else if node.StatusAnyFailed()` chain stays as it is; convert the
`if/else if` into two statements as shown so the skip check comes first.)

- [ ] **Step 7: Run tests to verify they pass**

Run:
`go test -tags 'debug test test_small test_large test_all' ./pkg/dag/... -v`
Expected: PASS — including all pre-existing dag tests.

- [ ] **Step 8: Commit**

```bash
git add pkg/dag/status.go pkg/dag/node.go pkg/dag/run.go pkg/dag/run-concurrent.go pkg/dag/skip_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(dag): add skipped execution status for up-to-date targets"
```

---

### Task 13: Seed the DAG from the watcher

**Files:**

- Create: `pkg/dag/watcher.go`
- Modify: `pkg/dag/execution-order.go:55-61` (`opts`),
  `pkg/dag/execution-order.go:99-158` (`defineExecutionOrder`)
- Test: `pkg/dag/watcher_test.go`

**Interfaces:**

- Consumes: `client.QueryDirty`, `client.Result` (Task 11);
  `TargetNodeChanges.Propagate`, `graph.recomputeSubgraph`, `graph.inSelection`
  (existing).
- Produces: `dag.WatcherSession` —
  `struct { Args *watcher.Args; RootDir string; ScanID int64 }`,
  `dag.WithWatcher(sess *WatcherSession) ExecOption`,
  `(*graph).SolveWatcherChanges(dirty map[target.ID]bool) error`.

Semantics: a `nil` `dirty` map means "nothing known" → every target changed
(today's behaviour). A target id **missing** from a non-nil map is also treated
as changed — never skip what the server did not answer for.

- [ ] **Step 1: Write the failing test**

Create `pkg/dag/watcher_test.go`. Reuse the component fixture from
`pkg/dag/execution-order_test.go`; the graph here has `comp-a::build` depending
on `comp-a::gen`.

```go
//go:build test && (test_small || test_all)

package dag

import (
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSolveWatcherChangesMarksOnlyDirtyTargets(t *testing.T) {
	t.Parallel()
	comps, rootDir := testCompsWithInputs(t)

	nodes, _, err := DefineExecutionOrder(comps, rootDir)
	require.NoError(t, err)

	g, err := newGraph(nodes, nil)
	require.NoError(t, err)
	require.NoError(t, g.SolveExecutionOrder())

	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{
		"comp-a::build": true,
		"comp-b::test":  false,
	}))

	assert.True(t, nodes[target.ID("comp-a::build")].Inputs.IsChanged())
	assert.False(t, nodes[target.ID("comp-b::test")].Inputs.IsChanged())
}

func TestSolveWatcherChangesTreatsUnknownTargetsAsChanged(t *testing.T) {
	t.Parallel()
	comps, rootDir := testCompsWithInputs(t)

	nodes, _, err := DefineExecutionOrder(comps, rootDir)
	require.NoError(t, err)

	g, err := newGraph(nodes, nil)
	require.NoError(t, err)
	require.NoError(t, g.SolveExecutionOrder())

	// The server answered for nothing at all.
	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{}))

	assert.True(t, nodes[target.ID("comp-a::build")].Inputs.IsChanged())
	assert.True(t, nodes[target.ID("comp-b::test")].Inputs.IsChanged())
}

func TestSolveWatcherChangesWithNilMapChangesEverything(t *testing.T) {
	t.Parallel()
	comps, rootDir := testCompsWithInputs(t)

	nodes, _, err := DefineExecutionOrder(comps, rootDir)
	require.NoError(t, err)

	g, err := newGraph(nodes, nil)
	require.NoError(t, err)
	require.NoError(t, g.SolveExecutionOrder())
	require.NoError(t, g.SolveWatcherChanges(nil))

	for id := range nodes {
		assert.True(t, nodes[id].Inputs.IsChanged(), "target '%v' must be changed", id)
	}
}
```

For a test proving forward propagation, extend `testCompsWithInputs` so that
`comp-b::test` declares `depends: ["comp-a::build"]`, then assert that marking
only `comp-a::build` dirty leaves `comp-b::test` with
`Inputs.ChangedByDependency == true`. Add:

```go
func TestSolveWatcherChangesPropagatesToDependents(t *testing.T) {
	t.Parallel()
	comps, rootDir := testCompsWithDependency(t) // comp-b::test depends on comp-a::build

	nodes, _, err := DefineExecutionOrder(comps, rootDir)
	require.NoError(t, err)

	g, err := newGraph(nodes, nil)
	require.NoError(t, err)
	require.NoError(t, g.SolveExecutionOrder())
	require.NoError(t, g.SolveWatcherChanges(map[target.ID]bool{
		"comp-a::build": true,
		"comp-b::test":  false,
	}))

	assert.True(t, nodes[target.ID("comp-b::test")].Inputs.ChangedByDependency)
	assert.True(t, nodes[target.ID("comp-b::test")].Inputs.IsChanged())
}
```

`testCompsWithDependency` is a copy of `testCompsWithInputs` with
`Dependencies: []target.ID{"comp-a::build"}` on `comp-b`'s `test` target.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test -tags 'debug test test_small' ./pkg/dag/... -run Watcher -v`
Expected: FAIL — `g.SolveWatcherChanges undefined`.

- [ ] **Step 3: Write `pkg/dag/watcher.go`**

```go
package dag

import (
	"context"

	"github.com/sdsc-ordes/quitsh/pkg/common/stack"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherclient "github.com/sdsc-ordes/quitsh/pkg/watcher/client"
)

// WatcherSession carries the watcher settings through one `quitsh` run:
// [DefineExecutionOrder] fills in the scan id it decided on, and [Execute]
// reports the results back against that same scan id.
type WatcherSession struct {
	// Args are the watcher settings. Change tracking is off when nil or
	// when `Args.Enabled` is false.
	Args *watcher.Args

	// RootDir is the repository root.
	RootDir string

	// ScanID is the scan the dirty decision was based on. Filled in by
	// [DefineExecutionOrder]; zero means the watcher was not consulted.
	ScanID int64
}

// enabled reports whether the session may talk to a watcher server.
func (s *WatcherSession) enabled() bool {
	return s != nil && s.Args != nil && s.Args.Enabled
}

// WithWatcher makes the execution order ask the watcher server which targets
// changed. Targets which are up to date are marked [ExecStatusSkipped].
// When the server cannot be reached everything is treated as changed.
func WithWatcher(sess *WatcherSession) ExecOption {
	return func(o *opts) error {
		if !sess.enabled() {
			return nil
		}

		o.watcher = sess

		return nil
	}
}

// queryWatcher asks the server about all targets in `nodes`.
func queryWatcher(sess *WatcherSession, nodes TargetNodeMap) map[target.ID]bool {
	ids := make([]target.ID, 0, len(nodes))
	for id := range nodes {
		ids = append(ids, id)
	}

	res := watcherclient.QueryDirty(context.Background(), sess.Args, sess.RootDir, ids)
	sess.ScanID = res.ScanID

	if res.Dirty == nil {
		log.Info("Watcher not available: treating all targets as changed.")

		return nil
	}

	log.Debug("Watcher answered.", "scan", res.ScanID, "targets", len(res.Dirty))

	return res.Dirty
}

// SolveWatcherChanges seeds the changed flags from the watcher answer and
// propagates them forward, exactly like [graph.SolveInputChanges] does for
// path-based changes. A nil `dirty` map, or a target missing from it, means
// changed: we never skip what we do not know about.
func (graph *graph) SolveWatcherChanges(dirty map[target.ID]bool) error {
	log.Debug("Solve watcher changes and recompute selection subgraph.")

	var changedInSelection TargetSelection

	for _, root := range *graph.execRootNodesSel {
		dfsStack := stack.NewStack[*TargetNode]()
		dfsStack.Push(root)

		for dfsStack.Len() != 0 {
			n := dfsStack.Pop()

			if !graph.inSelection(n) {
				continue
			}

			currIn := &n.Inputs

			if !currIn.IsChanged() {
				changed, known := dirty[n.Target.ID]
				currIn.Changed = dirty == nil || !known || changed
			}

			log.Debug("Changes for target id.",
				"id", n.Target.ID.String(),
				"changed", currIn.Changed,
				"changedByDeps", currIn.ChangedByDependency)

			if currIn.IsChanged() {
				changedInSelection.Insert(n.Target.ID)
			}

			for _, c := range n.Forward {
				c.Inputs.Propagate(&n.Inputs)
			}

			dfsStack.Push(n.Forward...)
		}
	}

	return graph.recomputeSubgraph(&changedInSelection)
}
```

- [ ] **Step 4: Wire it into `defineExecutionOrder`**

In `pkg/dag/execution-order.go`, add the field to `opts`:

```go
	opts struct {
		targetSelection *TargetSelection

		nodeCount int

		inputPathChanges []string

		watcher *WatcherSession
	}
```

Replace the change-solving part of `defineExecutionOrder` (currently the block
computing `resolveInputs`, making paths absolute and calling
`SolveInputChanges`):

```go
	// When we have resolved input ids, we can solve input changes.
	// Note: We do not want this to happen but really only if some `inputPathChanges`
	//       are given (can be []).
	resolveInputs := o.inputPathChanges != nil
	allNodes, allInputs, allComps, err := constructNodes(
		components,
		o.targetSelection,
		rootDir,
		resolveInputs,
	)
	if err != nil {
		return nil, nil, err
	}

	g, err := newGraph(allNodes, o.targetSelection)
	if err != nil {
		return nil, nil, err
	}

	err = g.SolveExecutionOrder()
	if err != nil {
		return nil, nil, err
	}

	if o.watcher != nil {
		// The watcher answers per target; dependency propagation stays here.
		err = g.SolveWatcherChanges(queryWatcher(o.watcher, allNodes))
	} else {
		// Make all input path changes absolute.
		for i := range o.inputPathChanges {
			o.inputPathChanges[i] = fs.MakeAbsoluteTo(rootDir, o.inputPathChanges[i])
		}
		log.Debug("Changed paths.", "paths", o.inputPathChanges)
		err = g.SolveInputChanges(allInputs, allComps, &regexCache, o.inputPathChanges)
	}

	if err != nil {
		return nil, nil, err
	}

	targets, prios = g.NodesToPriorityList()

	if o.watcher != nil {
		markSkipped(targets)
	}

	return targets, prios, nil
```

Add to `pkg/dag/watcher.go`:

```go
// markSkipped marks every target in the subgraph which is up to date.
// Those are the clean dependencies pulled in by a changed dependent.
func markSkipped(targets TargetNodeMap) {
	for id := range targets {
		if targets[id].Inputs.IsChanged() {
			continue
		}

		targets[id].Execution.Skip = true
		log.Debug("Target is up to date, skipping.", "target", id)
	}
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run:
`go test -tags 'debug test test_small test_large test_all' ./pkg/dag/... -v`
Expected: PASS — new watcher tests plus all pre-existing dag tests.

- [ ] **Step 6: Commit**

```bash
git add pkg/dag/watcher.go pkg/dag/watcher_test.go pkg/dag/execution-order.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(dag): seed changed targets from the watcher server"
```

---

### Task 14: Report results from `dag.Execute`

Reporting belongs in `Execute`, not in each command: downstream repositories
write their own commands, and they must get change tracking without extra
wiring.

**Files:**

- Modify: `pkg/dag/run.go:33-40` (`execOption`), `pkg/dag/run.go:42-69`
  (`Execute`), `pkg/dag/run.go:334` (options)
- Modify: `pkg/dag/watcher.go`
- Test: `pkg/dag/watcher_test.go`

**Interfaces:**

- Consumes: `WatcherSession` (Task 13), `client.ReportResults` (Task 11).
- Produces: `dag.WithWatcherReport(sess *WatcherSession) ExecuteOption`,
  `dag.CollectResults(targets TargetNodeMap) map[target.ID]bool`.

- [ ] **Step 1: Write the failing test**

Append to `pkg/dag/watcher_test.go`:

```go
func TestCollectResultsIgnoresSkippedAndUnexecutedTargets(t *testing.T) {
	t.Parallel()

	ran := &TargetNode{Target: &target.Config{ID: "c::ran"}}
	ran.Execution.Runners = RunnerStatuses{{Status: ExecStatusSuccess}}

	failed := &TargetNode{Target: &target.Config{ID: "c::failed"}}
	failed.Execution.Runners = RunnerStatuses{{Status: ExecStatusFailed}}

	skipped := &TargetNode{Target: &target.Config{ID: "c::skipped"}}
	skipped.Execution.Skip = true
	skipped.Execution.Runners = RunnerStatuses{{Status: ExecStatusSkipped}}

	cancelled := &TargetNode{Target: &target.Config{ID: "c::cancelled"}}
	cancelled.Execution.Cancel = true
	cancelled.Execution.Runners = RunnerStatuses{{Status: ExecStatusNotRun}}

	results := CollectResults(TargetNodeMap{
		"c::ran":       ran,
		"c::failed":    failed,
		"c::skipped":   skipped,
		"c::cancelled": cancelled,
	})

	assert.Equal(t, map[target.ID]bool{"c::ran": true, "c::failed": false}, results)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run:
`go test -tags 'debug test test_small' ./pkg/dag/... -run CollectResults -v`
Expected: FAIL — `undefined: CollectResults`.

- [ ] **Step 3: Append to `pkg/dag/watcher.go`**

```go
// CollectResults returns the terminal result of every target which actually
// ran. Skipped, cancelled and never-started targets are left out: the watcher
// must only learn about builds that happened.
func CollectResults(targets TargetNodeMap) map[target.ID]bool {
	results := make(map[target.ID]bool, len(targets))

	for id, n := range targets {
		if n.Execution.Skip || n.Execution.Cancel || len(n.Execution.Runners) == 0 {
			continue
		}

		executed := false
		for _, r := range n.Execution.Runners {
			if r.Status == ExecStatusSuccess || r.Status == ExecStatusFailed {
				executed = true

				break
			}
		}

		if !executed {
			continue
		}

		results[id] = n.Status() == ExecStatusSuccess
	}

	return results
}

// reportToWatcher sends the results of this run to the watcher server.
func reportToWatcher(sess *WatcherSession, targets TargetNodeMap) {
	if !sess.enabled() || sess.ScanID == 0 {
		return
	}

	results := CollectResults(targets)
	log.Debug("Reporting results to the watcher.", "scan", sess.ScanID, "targets", len(results))

	watcherclient.ReportResults(
		context.Background(), sess.Args, sess.RootDir, sess.ScanID, results)
}
```

- [ ] **Step 4: Add the execute option in `pkg/dag/run.go`**

Extend `execOption`:

```go
	execOption struct {
		Tags    []tags.Tag
		Watcher *WatcherSession
	}
```

Add next to `WithTags`:

```go
// WithWatcherReport reports the outcome of this run to the watcher server,
// against the scan id the execution order was decided on.
func WithWatcherReport(sess *WatcherSession) ExecuteOption {
	return func(o *execOption) error {
		o.Watcher = sess

		return nil
	}
}
```

- [ ] **Step 5: Report at the end of `Execute`**

Replace the body of `Execute`:

```go
func Execute(
	targets TargetNodeMap,
	prios Priorities,
	runnerFactory factory.IFactory,
	dispatcher toolchain.IDispatcher,
	config config.IConfig,
	rootDir string,
	parallel bool,
	opts ...ExecuteOption,
) error {
	var o execOption
	if err := o.Apply(opts...); err != nil {
		return err
	}

	var err error
	if parallel {
		err = executeConcurrent(
			targets,
			runnerFactory,
			dispatcher,
			config,
			rootDir, opts...)
	} else {
		err = executeNormal(
			prios,
			runnerFactory,
			dispatcher,
			config,
			rootDir, opts...,
		)
	}

	if o.Watcher != nil {
		reportToWatcher(o.Watcher, targets)
	}

	return err
}
```

Check the exact name of the options-applying method on `execOption` in
`pkg/dag/run.go` (the file already applies `opts` inside `executeNormal`/
`executeConcurrent`) and use the same one here. Applying twice is harmless: the
option functions only assign fields.

- [ ] **Step 6: Run tests to verify they pass**

Run:
`go test -tags 'debug test test_small test_large test_all' ./pkg/dag/... -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add pkg/dag/run.go pkg/dag/watcher.go pkg/dag/watcher_test.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(dag): report executed target results to the watcher"
```

---

### Task 15: Wire the watcher into the CLI framework

**Files:**

- Create: `pkg/watcher/selector.go`
- Modify: `pkg/cli/cli.go:20-63` (`ICLI`), `pkg/cli/cli.go:96-120` (`cliApp`
  fields)
- Modify: `pkg/cli/cli-impl.go` (accessors)
- Modify: `pkg/cli/options.go` (new option, after `WithConfigFilename`)

**Interfaces:**

- Consumes: `watcher.Args` (Task 1), `component.ConfigFilename`
  (`pkg/component/component-paths.go:13`).
- Produces: `watcher.ArgsSelector` (`func(config.IConfig) *watcher.Args`),
  `cli.WithWatcher(selector watcher.ArgsSelector) Option`,
  `ICLI.WatcherArgs() *watcher.Args` (nil when unconfigured),
  `ICLI.ConfigFilename() string`.

- [ ] **Step 1: Write `pkg/watcher/selector.go`**

```go
package watcher

import "github.com/sdsc-ordes/quitsh/pkg/config"

// ArgsSelector returns the watcher settings out of the user's config.
// It mirrors `nixtoolchain.ArgsSelector`: the config is unmarshalled after
// `cli.New`, so the settings must be fetched lazily.
type ArgsSelector func(config.IConfig) *Args
```

- [ ] **Step 2: Add the option in `pkg/cli/options.go`**

```go
// WithWatcher enables the `quitsh server` change tracking for this CLI.
// The selector points into your own config, e.g.:
//
//	cli.WithWatcher(func(c config.IConfig) *watcher.Args {
//		return &common.Cast[*cliconfig.Config](c).Watcher
//	})
//
// NOTE: When you use this option, add the `server` command with
// `servercmd.AddCmd` to the root command.
func WithWatcher(selector watcher.ArgsSelector) Option {
	return func(c *cliApp) error {
		c.watcherArgsSelector = selector

		return nil
	}
}
```

Add the import `"github.com/sdsc-ordes/quitsh/pkg/watcher"`.

- [ ] **Step 3: Add the field and the accessors**

In `pkg/cli/cli.go`, add to `cliApp`:

```go
	watcherArgsSelector watcher.ArgsSelector
```

and to the `ICLI` interface:

```go
	// WatcherArgs returns the watcher settings, or `nil` when the CLI was not
	// built with `WithWatcher`.
	WatcherArgs() *watcher.Args

	// ConfigFilename returns the components config file name.
	ConfigFilename() string
```

In `pkg/cli/cli-impl.go`:

```go
func (c *cliApp) WatcherArgs() *watcher.Args {
	if c.watcherArgsSelector == nil {
		return nil
	}

	return c.watcherArgsSelector(c.config)
}

func (c *cliApp) ConfigFilename() string {
	if c.configFilename == "" {
		return component.ConfigFilename
	}

	return c.configFilename
}
```

- [ ] **Step 4: Verify it compiles**

Run: `go build ./...` Expected: no output.

- [ ] **Step 5: Run the full unit test suite**

Run: `just go-test-unit-tests` Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add pkg/watcher/selector.go pkg/cli
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(cli): add WithWatcher option and watcher accessors"
```

---

### Task 16: The `quitsh server` command

**Files:**

- Create: `pkg/cli/cmd/watcher/server.go`
- Create: `pkg/cli/cmd/watcher/serve.go`
- Create: `pkg/cli/cmd/watcher/status.go`
- Create: `pkg/cli/cmd/watcher/control.go`

**Interfaces:**

- Consumes: `cli.ICLI` (Task 15), `server.New`, `(*Server).Serve` (Tasks 9–10),
  `client.Dial` (Task 11), `watcher.ProtocolVersion`.
- Produces: `servercmd.AddCmd(cl cli.ICLI, parent *cobra.Command)` adding
  `server serve|status|stop|reset`.

- [ ] **Step 1: Write `pkg/cli/cmd/watcher/server.go`**

```go
// Package servercmd adds the `quitsh server` change-tracking watcher commands.
package servercmd

import (
	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/errors"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherclient "github.com/sdsc-ordes/quitsh/pkg/watcher/client"

	"github.com/spf13/cobra"
)

const longDesc = `
Run and control the change-tracking watcher.

The watcher rescans the repository periodically, remembers the input state each
target was last built from, and answers which targets are out of date. Run
targets, and answers which targets are out of date. Builds use it automatically;
pass '--no-skip' to run everything anyway.
`

// AddCmd adds the 'server' command to 'parent'.
func AddCmd(cl cli.ICLI, parent *cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:          "server",
		Short:        "Run and control the change-tracking watcher.",
		Long:         longDesc,
		SilenceUsage: true,
	}

	addServeCmd(cl, cmd)
	addStatusCmd(cl, cmd)
	addControlCmds(cl, cmd)

	parent.AddCommand(cmd)

	return cmd
}

// watcherArgs returns the configured watcher settings or an error explaining
// how to enable them.
func watcherArgs(cl cli.ICLI) (*watcher.Args, error) {
	args := cl.WatcherArgs()
	if args == nil {
		return nil, errors.New(
			"this CLI was not built with 'cli.WithWatcher(...)', " +
				"so the watcher settings are unknown")
	}

	return args, nil
}

// dial connects to a running watcher server.
func dial(cl cli.ICLI) (*watcherclient.Client, error) {
	args, err := watcherArgs(cl)
	if err != nil {
		return nil, err
	}

	return watcherclient.Dial(args, cl.RootDir())
}
```

- [ ] **Step 2: Write `pkg/cli/cmd/watcher/serve.go`**

```go
package servercmd

import (
	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/cli/general"
	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	"github.com/sdsc-ordes/quitsh/pkg/watcher"
	watcherserver "github.com/sdsc-ordes/quitsh/pkg/watcher/server"

	"github.com/spf13/cobra"
)

func addServeCmd(cl cli.ICLI, parent *cobra.Command) {
	var address string

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run the watcher server in the foreground.",
		RunE: func(_ *cobra.Command, _ []string) error {
			return serve(cl, address)
		},
	}

	cmd.Flags().StringVar(&address, "address", "",
		"Address to listen on, e.g. 'unix:///run/user/1000/quitsh/a.sock' "+
			"or 'tcp://127.0.0.1:7777' (defaults to a per-repository unix socket).")

	parent.AddCommand(cmd)
}

func serve(cl cli.ICLI, address string) error {
	args, err := watcherArgs(cl)
	if err != nil {
		return err
	}

	if address == "" {
		address = args.ResolveAddress(cl.RootDir())
	}

	discover := func() ([]*component.Component, error) {
		_, all, _, e := cl.FindComponents(
			&general.ComponentArgs{ComponentPatterns: []string{"*"}})

		return all, e
	}

	srv, err := watcherserver.New(args, cl.RootDir(), cl.ConfigFilename(), discover)
	if err != nil {
		return err
	}

	log.Info("Starting watcher.",
		"root", cl.RootDir(),
		"address", address,
		"interval", args.ScanInterval,
		"hashMode", args.HashMode)

	return srv.Serve(cl.Ctx(), address, watcher.ProtocolVersion)
}
```

`cl.Ctx()` is already a signal context when the CLI was built with
`cli.WithSignalContext(true)`, so Ctrl-C flushes state and removes the socket.

- [ ] **Step 3: Write `pkg/cli/cmd/watcher/status.go`**

```go
package servercmd

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"
	watcherclient "github.com/sdsc-ordes/quitsh/pkg/watcher/client"

	"github.com/spf13/cobra"
)

func addStatusCmd(cl cli.ICLI, parent *cobra.Command) {
	var (
		dirtyOnly bool
		staleOk   bool
	)

	cmd := &cobra.Command{
		Use:   "status [target-ids...]",
		Short: "Show which targets are out of date.",
		RunE: func(_ *cobra.Command, ids []string) error {
			return status(cl, ids, dirtyOnly, staleOk)
		},
	}

	cmd.Flags().BoolVar(&dirtyOnly, "dirty-only", false, "Only list out-of-date targets.")
	cmd.Flags().BoolVar(&staleOk, "stale-ok", false,
		"Answer from the last scan instead of forcing a fresh one.")

	parent.AddCommand(cmd)
}

// staleMaxAge is the freshness allowed by `--stale-ok`.
const staleMaxAge = time.Hour

func status(cl cli.ICLI, rawIDs []string, dirtyOnly bool, staleOk bool) error {
	c, err := dial(cl)
	if err != nil {
		return err
	}
	defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

	info, err := c.Info(cl.Ctx())
	if err != nil {
		return err
	}

	ids := make([]target.ID, 0, len(rawIDs))
	for _, id := range rawIDs {
		ids = append(ids, target.ID(id))
	}

	maxAge := time.Duration(0)
	if staleOk {
		maxAge = staleMaxAge
	}

	res, err := c.Status(cl.Ctx(), ids, maxAge)
	if err != nil {
		return err
	}

	log.Info("Watcher.",
		"root", info.GetRootDir(),
		"scan", res.ScanID,
		"targets", info.GetTargetCount(),
		"files", info.GetFileCount(),
		"lastScan", time.UnixMilli(info.GetLastScanAtUnixMs()).Format(time.RFC3339),
		"scanTook", time.Duration(info.GetLastScanDurationMs())*time.Millisecond)

	sorted := make([]target.ID, 0, len(res.Dirty))
	for id := range res.Dirty {
		sorted = append(sorted, id)
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	for _, id := range sorted {
		dirty := res.Dirty[id]
		if dirtyOnly && !dirty {
			continue
		}

		fmt.Fprintf(os.Stdout, "%v %v%v\n", mark(dirty), id, reason(&res, id))
	}

	return nil
}

func mark(dirty bool) string {
	if dirty {
		return "●"
	}

	return "○"
}

func reason(res *watcherclient.Result, id target.ID) string {
	if !res.Dirty[id] {
		return ""
	}

	if !res.HasBuild[id] {
		return "  (never built successfully)"
	}

	if inputs := res.DirtyInputs[id]; len(inputs) != 0 {
		return fmt.Sprintf("  (changed inputs: %v)", inputs)
	}

	return "  (changed)"
}
```

- [ ] **Step 4: Write `pkg/cli/cmd/watcher/control.go`**

```go
package servercmd

import (
	"github.com/sdsc-ordes/quitsh/pkg/cli"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"
	"github.com/sdsc-ordes/quitsh/pkg/log"

	"github.com/spf13/cobra"
)

func addControlCmds(cl cli.ICLI, parent *cobra.Command) {
	parent.AddCommand(&cobra.Command{
		Use:   "stop",
		Short: "Stop the running watcher server.",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := dial(cl)
			if err != nil {
				return err
			}
			defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

			return c.Shutdown(cl.Ctx())
		},
	})

	parent.AddCommand(&cobra.Command{
		Use:   "reset [target-ids...]",
		Short: "Forget the last successful build of targets (all when none given).",
		RunE: func(_ *cobra.Command, rawIDs []string) error {
			c, err := dial(cl)
			if err != nil {
				return err
			}
			defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

			ids := make([]target.ID, 0, len(rawIDs))
			for _, id := range rawIDs {
				ids = append(ids, target.ID(id))
			}

			return c.Reset(cl.Ctx(), ids)
		},
	})

	parent.AddCommand(&cobra.Command{
		Use:   "rescan",
		Short: "Force a rescan and print the new scan id.",
		RunE: func(_ *cobra.Command, _ []string) error {
			c, err := dial(cl)
			if err != nil {
				return err
			}
			defer func() { log.WarnE(c.Close(), "Could not close watcher connection.") }()

			id, e := c.Rescan(cl.Ctx())
			if e != nil {
				return e
			}
			log.Info("Rescanned.", "scan", id)

			return nil
		},
	})
}
```

- [ ] **Step 5: Verify it compiles**

Run: `go build ./...` Expected: no output.

- [ ] **Step 6: Commit**

```bash
git add pkg/cli/cmd/watcher
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(cli): add 'quitsh server' watcher commands"
```

---

### Task 17: `--no-skip` on the exec commands

Change tracking is **on by default**. Without a running `quitsh server` the
client cannot reach anything, so every target is treated as changed and
behaviour is exactly as it is today. `--no-skip` forces that same "run
everything" behaviour even when a server _is_ running.

**Files:**

- Modify: `pkg/cli/general/general.go` (flag helper)
- Modify: `pkg/cli/cmd/exec-target/exec.go`
- Modify: `pkg/cli/cmd/exec-stage/exec.go`
- Modify: `tools/cli/pkg/config/config.go`, `tools/cli/cmd/cli/main.go`

**Interfaces:**

- Consumes: `dag.WatcherSession`, `dag.WithWatcher`, `dag.WithWatcherReport`
  (Tasks 13–14); `ICLI.WatcherArgs` (Task 15).
- Produces: `general.AddFlagWatcher(cmd *cobra.Command, noSkip *bool)`.

The flag is resolved **late** (inside `RunE`), because the config is
unmarshalled after the commands are built — binding a pointer into the config at
flag-registration time would bind the wrong struct.

- [ ] **Step 1: Add the flag helper in `pkg/cli/general/general.go`**

```go
// AddFlagWatcher adds the `--no-skip` flag which turns the change-tracking
// watcher off for this invocation. Change tracking is on by default; without a
// running `quitsh server` it has no effect anyway.
func AddFlagWatcher(cmd *cobra.Command, noSkip *bool) {
	cmd.Flags().BoolVar(noSkip, "no-skip", false,
		"Run every selected target, even ones which did not change since "+
			"their last successful build (ignores a running 'quitsh server').")
}
```

- [ ] **Step 2: Use it in `pkg/cli/cmd/exec-target/exec.go`**

Add `noSkip bool` to `execTargetArgs`, register the flag in `AddCmd`:

```go
	general.AddFlagWatcher(execCmd, &args.noSkip)
```

and replace the body of `runExec` from `dag.DefineExecutionOrder` onwards:

```go
	sess := dag.WatcherSession{Args: cli.WatcherArgs(), RootDir: rootDir}
	if args.noSkip && sess.Args != nil {
		sess.Args.Enabled = false
	}

	targets, prios, err := dag.DefineExecutionOrder(
		all,
		rootDir,
		dag.WithTargetSelection(&selection),
		dag.WithWatcher(&sess),
	)
	if err != nil {
		return err
	}

	var dispatcher toolchain.IDispatcher
	if !cli.RootArgs().SkipToolchainDispatch {
		dispatcher = cli.ToolchainDispatcher()
	}

	return dag.Execute(
		targets,
		prios,
		cli.RunnerFactory(),
		dispatcher,
		cli.Config(),
		rootDir,
		cli.RootArgs().Parallel,
		dag.WithTags(execArgs.Tags...),
		dag.WithWatcherReport(&sess),
	)
```

- [ ] **Step 3: Use it in `pkg/cli/cmd/exec-stage/exec.go`**

`ExecuteStage` needs the flag value, so extend its signature and both call sites
(`AddCmdGeneral` and `AddCmdAlias` each hold their own `compArgs`; add a
`noSkip bool` next to it and register `general.AddFlagWatcher(cmd, &noSkip)`):

```go
func ExecuteStage(
	cl cli.ICLI,
	compArgs *general.ComponentArgs,
	stage stage.Stage,
	execArgs *dag.ExecArgs,
	noSkip bool,
) error {
	comps, all, rootDir, err := cl.FindComponents(compArgs)
	if err != nil {
		return err
	}

	sess := dag.WatcherSession{Args: cl.WatcherArgs(), RootDir: rootDir}
	if noSkip && sess.Args != nil {
		sess.Args.Enabled = false
	}

	targets, prios, err := dag.DefineExecutionOrder(
		all, rootDir,
		dag.WithTargetsByStageFromComponents(comps, stage),
		dag.WithWatcher(&sess),
	)
	if err != nil {
		return err
	} else if len(targets) == 0 {
		log.Info("Nothing to do: no targets selected or everything is up to date.")

		return nil
	}

	var dispatcher toolchain.IDispatcher
	if !cl.RootArgs().SkipToolchainDispatch {
		dispatcher = cl.ToolchainDispatcher()
	}

	return dag.Execute(
		targets,
		prios,
		cl.RunnerFactory(),
		dispatcher,
		cl.Config(),
		rootDir,
		cl.RootArgs().Parallel,
		dag.WithTags(execArgs.Tags...),
		dag.WithWatcherReport(&sess),
	)
}
```

Note the behaviour change: an empty target set is no longer an error, because
"everything is up to date" is a legitimate outcome. Add the `log` import and
drop the `errors` import if it becomes unused.

- [ ] **Step 4: Wire the repository's own CLI**

In `tools/cli/pkg/config/config.go`, add to `Config`:

```go
	// The watcher settings for `quitsh server`.
	Watcher watcher.Args `yaml:"watcher"`
```

with the import `"github.com/sdsc-ordes/quitsh/pkg/watcher"`.

In `tools/cli/cmd/cli/main.go`, add the option to `cli.New(...)`:

```go
		cli.WithWatcher(func(c config.IConfig) *watcher.Args {
			cc := common.Cast[*cliconfig.Config](c)

			return &cc.Watcher
		}),
```

and register the command next to the other `AddCmd` calls:

```go
	servercmd.AddCmd(cli, cli.RootCmd())
```

with the imports `servercmd "github.com/sdsc-ordes/quitsh/pkg/cli/cmd/watcher"`
and `"github.com/sdsc-ordes/quitsh/pkg/watcher"`.

While you are there, remove the duplicated
`exectarget.AddCmd(cli, cli.RootCmd(), &conf.Commands.ExecArgs)` line — it is
registered twice.

- [ ] **Step 5: Verify it builds and the suite passes**

Run:

```bash
go build ./...
cd tools/cli && go build ./... && cd ../..
just go-test-unit-tests
```

Expected: all succeed.

- [ ] **Step 6: Smoke-test by hand**

Terminal 1:

```bash
just go-cli server serve
```

Expected: `Watcher server listening.` with the socket path and root dir.

Terminal 2:

```bash
just go-cli server status --dirty-only
```

Expected: a list of `● quitsh::…` targets, all dirty (nothing built yet).

- [ ] **Step 7: Commit**

```bash
git add pkg/cli tools/cli
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "feat(cli): track changes by default and add --no-skip to opt out"
```

---

### Task 18: Integration test

The existing integration tests drive the **built CLI binary** as a subprocess
against the `test/repo` fixture (see `test/integration_test.go:20` — `setup`
builds an `exec.CmdContextBuilder` around
`$QUITSH_BIN_DIR/quitsh-integration-test` with `--root-dir repo`). This test
follows that style, so it exercises the real command wiring rather than the Go
API.

**Files:**

- Modify: `test/cmd/quitsh-integration-test/main.go`
- Create: `test/watcher_test.go`

**Interfaces:**

- Consumes: `servercmd.AddCmd`, `cli.WithWatcher` (Tasks 15–16), `--no-skip`
  (Task 17).
- Produces: nothing.

- [ ] **Step 1: Enable the watcher in the integration CLI**

In `test/cmd/quitsh-integration-test/main.go`, add to the `Config` struct:

```go
	// The watcher settings for `quitsh server`.
	Watcher watcher.Args `yaml:"watcher"`
```

add the option to `cli.New(...)`:

```go
		cli.WithWatcher(func(c config.IConfig) *watcher.Args {
			return &common.Cast[*Config](c).Watcher
		}),
```

and register the command next to the other `AddCmd` calls:

```go
	servercmd.AddCmd(cli, cli.RootCmd())
```

Imports to add: `servercmd "github.com/sdsc-ordes/quitsh/pkg/cli/cmd/watcher"`,
`"github.com/sdsc-ordes/quitsh/pkg/watcher"`, and
`"github.com/sdsc-ordes/quitsh/pkg/common"` (if not already imported). Match the
existing cast style in that file — it may already use `common.Cast` or a direct
type assertion.

- [ ] **Step 2: Write the test**

Create `test/watcher_test.go`:

```go
//go:build test && integration

package test

import (
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// startWatcher runs `server serve` as a background process against the
// `repo` fixture and returns the config values every client needs.
func startWatcher(t *testing.T) (configValues []string) {
	t.Helper()

	dir := t.TempDir()
	address := "unix://" + path.Join(dir, "w.sock")
	stateFile := path.Join(dir, "state.json")

	configValues = []string{
		"--config-value", "watcher.address: " + address,
		"--config-value", "watcher.stateFile: " + stateFile,
		"--config-value", "watcher.scanInterval: 100ms",
		"--config-value", "watcher.timeout: 10s",
	}

	binDir := os.Getenv("QUITSH_BIN_DIR")
	require.DirExists(t, binDir)

	args := append([]string{"--root-dir", "repo"}, configValues...)
	args = append(args, "server", "serve")

	cmd := exec.Command(path.Join(binDir, "quitsh-integration-test"), args...) //nolint:gosec // test.
	cmd.Env = append(os.Environ(), "GOCOVERDIR="+os.Getenv("QUITSH_COVERAGE_DIR"))
	require.NoError(t, cmd.Start())

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	// Wait until the socket answers.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		cli := setup(t).Build()
		if _, err := cli.Get(append(configValues, "server", "status", "--stale-ok")...); err == nil {
			return configValues
		}
		time.Sleep(100 * time.Millisecond)
	}

	t.Fatal("watcher server did not come up")

	return nil
}

func TestWatcherSkipsUnchangedTarget(t *testing.T) {
	configValues := startWatcher(t)
	cli := setup(t).Build()

	run := func() string {
		args := append([]string{}, configValues...)
		args = append(args,
			"exec-target", "component-a::build")

		stdout, err := cli.Get(args...)
		require.NoError(t, err)

		return stdout
	}

	first := run()
	assert.NotContains(t, first, "is up to date, skipping",
		"a target which was never built must run")

	second := run()
	assert.Contains(t, second, "up to date",
		"an unchanged target must be skipped on the second run")
}

func TestWatcherRebuildsAfterEdit(t *testing.T) {
	configValues := startWatcher(t)
	cli := setup(t).Build()

	run := func() string {
		args := append([]string{}, configValues...)
		args = append(args,
			"exec-target", "component-a::build")

		stdout, err := cli.Get(args...)
		require.NoError(t, err)

		return stdout
	}

	run() // build once so a successful build is recorded
	require.Contains(t, run(), "up to date")

	touched := path.Join("repo", "component-a", "watcher-touch.txt")
	require.NoError(t, os.WriteFile(touched, []byte("changed"), 0o600))
	t.Cleanup(func() { _ = os.Remove(touched) })

	// Give the scan loop a moment; the query forces a fresh scan anyway.
	out := run()
	assert.NotContains(t, out, "up to date",
		"editing a source file must invalidate the target")
}

func TestWatcherStatusListsDirtyTargets(t *testing.T) {
	configValues := startWatcher(t)
	cli := setup(t).Build()

	args := append([]string{}, configValues...)
	args = append(args, "server", "status", "--dirty-only")

	stdout, err := cli.Get(args...)
	require.NoError(t, err)

	assert.True(t, strings.Contains(stdout, "component-a::"),
		"targets of the fixture repository must be listed as dirty")
}

func TestWithoutServerEverythingRuns(t *testing.T) {
	cli := setup(t).Build()

	stdout, err := cli.Get(
		"--config-value", "watcher.address: unix://"+path.Join(t.TempDir(), "absent.sock"),
		"--config-value", "watcher.timeout: 500ms",
		"exec-target", "component-a::build",
	)

	require.NoError(t, err, "an absent watcher must never break a build")
	assert.NotContains(t, stdout, "up to date")
}

func TestNoSkipRunsEvenWhenUpToDate(t *testing.T) {
	configValues := startWatcher(t)
	cli := setup(t).Build()

	run := func(extra ...string) string {
		args := append([]string{}, configValues...)
		args = append(args, "exec-target", "component-a::build")
		args = append(args, extra...)

		stdout, err := cli.Get(args...)
		require.NoError(t, err)

		return stdout
	}

	run()
	require.Contains(t, run(), "up to date")

	assert.NotContains(t, run("--no-skip"), "up to date",
		"'--no-skip' must run the target even though it is up to date")
}
```

The `setup` helper already passes `--root-dir repo`; keep the ordering of
`--config-value` flags before the sub-command, as the other tests in that file
do. If `exec.CmdContextBuilder` cannot express a background process, keep using
`os/exec` directly as shown — the assertion helpers stay the same.

- [ ] **Step 3: Run the integration test**

Run: `just go-test-integration` Expected: PASS, including the four new watcher
tests.

- [ ] **Step 4: Commit**

```bash
git add test/watcher_test.go test/cmd/quitsh-integration-test/main.go
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "test(watcher): cover skip, invalidation and degradation end to end"
```

---

### Task 19: Documentation and final verification

**Files:**

- Modify: `.gitignore`
- Modify: `README.md` (new section after "Execution of Targets")
- Modify: `docs/development-guide.md` (mention `just generate-proto`)

**Interfaces:**

- Consumes: everything.
- Produces: nothing.

- [ ] **Step 1: Ignore the local watcher state**

Append to `.gitignore`, following its "top-level only" rule:

```gitignore
# Local watcher state of `quitsh server`.
.quitsh
```

- [ ] **Step 2: Document the feature in `README.md`**

Insert after the "Execution of Targets" section:

````markdown
## Change Tracking (`quitsh server`)

`quitsh server` runs a local watcher which remembers the input state each target
was last built from, so repeated builds only run what actually changed.

```shell
# Terminal 1: run the watcher for this repository.
quitsh server serve

# Terminal 2: build only what changed.
quitsh build

# What does it think is out of date, and why?
quitsh server status --dirty-only

# Run everything anyway, ignoring the watcher.
quitsh build --no-skip

# Forget what it learned about a target.
quitsh server reset mycomp::build
```

How it works:

- The server rescans the repository every `scanInterval` and computes a digest
  per [input change set](#targets-and-steps). Stamps are `mtime+size` by
  default; set `hashMode: checksum` to hash content instead.
- A target is **out of date** when it has no recorded successful build, or when
  any of its input sets differs from the state recorded at that build.
- After a run, `quitsh` reports which targets succeeded, and the server records
  the input state **as of the scan the decision was made on** — so a file you
  edit while the build runs correctly keeps the target out of date.
- The server answers only about a target's _own_ inputs. Propagation to
  dependent targets happens in the DAG, exactly as it does for
  `--changed-paths`-style CI runs.
- It is a pure accelerator: if it is not running, is unreachable, or speaks a
  different protocol version, `quitsh` builds everything, as it always did.

Enable it in your CLI:

```go
cli.New(
	// ...
	cli.WithWatcher(func(c config.IConfig) *watcher.Args {
		return &common.Cast[*cliconfig.Config](c).Watcher
	}),
)

servercmd.AddCmd(cli, cli.RootCmd())
```

with `Watcher watcher.Args` in your config struct. Settings:

| Key            | Default                                        | Meaning                                             |
| -------------- | ---------------------------------------------- | --------------------------------------------------- |
| `enabled`      | `true`                                         | Change tracking; `--no-skip` turns it off per run   |
| `address`      | per-repo unix socket                           | gRPC target, `unix://…` or `tcp://…`                |
| `stateFile`    | `<root>/.quitsh/watcher-state.json`            | Where successful builds are remembered              |
| `scanInterval` | `2s`                                           | Background rescan period                            |
| `maxAge`       | `0s`                                           | How stale an answer may be; `0` forces a fresh scan |
| `hashMode`     | `mtime-size`                                   | Or `checksum`                                       |
| `excludes`     | `.git`, `.output`, `result`, `node_modules`, … | Regexes on repo-relative paths                      |
| `timeout`      | `2s`                                           | Client dial and call budget                         |
````

- [ ] **Step 3: Document the codegen step in `docs/development-guide.md`**

Add a short section:

```markdown
### Regenerating the watcher gRPC code

The generated Go code in `pkg/watcher/proto` is committed so that `go build`
works without a protobuf toolchain. After editing `watcher.proto`, run
`just generate-proto` inside the default dev shell and commit the result.
```

- [ ] **Step 4: Format everything**

Run: `just format` Expected: files reformatted in place, no errors.

- [ ] **Step 5: Lint**

Run: `just lint` Expected: PASS. Fix any `mnd`, `errcheck` or `gocognit`
findings by naming constants, handling errors with `log.WarnE`, or splitting
functions — do not add blanket `//nolint` directives.

- [ ] **Step 6: Full test run**

Run: `just test` Expected: PASS — `test-small`, `test-large` and
`test-integration` targets.

- [ ] **Step 7: Verify the real workflow end to end**

```bash
just go-cli server serve &
sleep 3
just go-cli exec-target quitsh::lint             # runs
just go-cli exec-target quitsh::lint             # skips
just go-cli server status --dirty-only
just go-cli server stop
```

Expected: the second invocation logs `Target is up to date, skipping.` for
`quitsh::lint` and finishes without running the linter.

- [ ] **Step 8: Commit**

```bash
git add .gitignore README.md docs/development-guide.md
git -c user.name='Gabriel Nützi' -c user.email='647437+gabyx@users.noreply.github.com' \
  commit -m "docs: document the quitsh server change-tracking watcher"
```

---

## Notes for the implementer

- **Never let the watcher cause a skip it is not sure about.** Every unknown —
  no server, no answer for a target, an unresolvable scan id, an unreadable file
  — must resolve to "dirty". Tests for each of these exist in Tasks 11, 13 and
  18; keep them passing.
- **The server never learns about target dependencies.** If you find yourself
  wanting `Forward` edges inside `pkg/watcher/server`, the design has drifted:
  propagation belongs to `SolveWatcherChanges`.
- `WithInputChanges` and `SolveInputChanges` stay untouched. They are the CI
  path and remain the fallback until someone decides to remove them.

---

## Implementation Notes

What the implementation did differently from the plan above, and why. The plan
task bodies are left as written; this section is the record of the deltas.

**Codegen uses `buf`, not `protoc`** (Task 8). `nixpkgs` was unreachable in the
implementation environment, so no `protoc` binary was available. `buf` compiles
protobuf in pure Go and the code generator plugins run through `go run`, so
`just generate-proto` now needs nothing beyond Go itself. The Nix dev-shell step
of Task 8 was therefore dropped rather than adding `pkgs.protobuf`.

**`Args.Enabled` became `Args.Disabled`** (Tasks 1, 17). Change tracking is on
by default, but a zero-valued `bool` is `false`. Embedders whose config never
goes through `defaults.Set` — the integration test CLI is one — would have
silently had the feature off. Negating the field makes the zero value mean
"enabled". For the same reason `HashMode`, `ScanInterval` and `Timeout` gained
`Resolve…()` accessors, so a zero-valued `Args` is fully usable.

**Two bugs were found by the integration test** (Task 18):

- `--config-val` could not set any `time.Duration` field:
  `pkg/config/config-key-values.go` had no `StringToTimeDurationHookFunc` in its
  mapstructure hook chain, so `watcher.timeout: 2s` failed with
  `cannot parse as int`. Fixed for all duration settings, not just the
  watcher's.
- An up-to-date repository made `DefineExecutionOrder` fail with
  `graph selection must contain elements if not nil`, because
  `recomputeSubgraph` rejects an empty selection. `SolveWatcherChanges` now
  selects nothing explicitly instead, which is the legitimate "nothing to do"
  outcome. The `SolveInputChanges` path keeps its old behaviour.

**The root flag is `--config-val` / `-K`**, not `--config-value` as the plan's
test code assumed.

**`addRunnerTasks` was refactored** (Task 12): adding the skip branch pushed it
past the `gocognit` threshold, so the three pre-run conditions moved into a
`skipRunner` helper.

**Verification status.** `golangci-lint` over `./pkg/...` reports 24 issues, all
`goconst` in pre-existing test files — byte-identical to the count at the base
commit, so the new code adds none. All unit tests and all integration tests pass
except `TestProcessCompose`, `TestProcessComposeServicesFlake` and
`TestCLIProcessCompose`, which fail identically at the base commit because they
need `process-compose` and a working `nix eval`, neither available in the
implementation environment.
