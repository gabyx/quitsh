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
