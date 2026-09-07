package watcher

import (
	"path"
	"slices"
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
		slices.Sort(assigned[id])
	}

	return assigned
}
