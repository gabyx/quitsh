// Package scan walks a repository and stamps every tracked file.
package scan

import (
	stderr "errors"
	"io/fs"
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
