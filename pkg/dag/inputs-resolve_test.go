//go:build test && (test_small || test_all)

package dag

import (
	"path"
	"testing"

	"github.com/sdsc-ordes/quitsh/pkg/component"
	"github.com/sdsc-ordes/quitsh/pkg/component/input"
	"github.com/sdsc-ordes/quitsh/pkg/component/target"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testCompsWithInputs builds two components: `comp-a` with an explicit input
// set and `comp-b` whose target declares none (whole-component default).
func testCompsWithInputs(t *testing.T) []*component.Component {
	t.Helper()

	confA := &component.Config{
		Name:     "comp-a",
		Language: "go",
		Inputs: map[string]*input.Config{
			"srcs": {Patterns: []string{`^src/.*\.go$`}},
		},
		Targets: map[string]*target.Config{
			stageBuild: {Stage: stageBuild, Inputs: []input.ID{"self::srcs"}},
		},
	}
	require.NoError(t, confA.Init())

	confB := &component.Config{
		Name:     compBName,
		Language: "go",
		Targets: map[string]*target.Config{
			stageTest: {Stage: stageTest},
		},
	}
	require.NoError(t, confB.Init())

	a := component.NewComponent(confA, path.Join(rootDir, "comp-a"), "", "")
	b := component.NewComponent(confB, path.Join(rootDir, "comp-b"), "", "")

	return []*component.Component{&a, &b}
}

func TestResolveTargetInputsResolvesSelfReferences(t *testing.T) {
	t.Parallel()
	comps := testCompsWithInputs(t)

	targets, inputs, err := ResolveTargetInputs(comps, rootDir)
	require.NoError(t, err)

	assert.Equal(t, []input.ID{"comp-a::srcs"}, targets[target.ID(tgtCompABuild)])
	require.Contains(t, inputs, input.ID("comp-a::srcs"))
	assert.NotEmpty(t, inputs["comp-a::srcs"].BaseDir)
}

func TestResolveTargetInputsDefaultsToWholeComponent(t *testing.T) {
	t.Parallel()
	comps := testCompsWithInputs(t)

	targets, inputs, err := ResolveTargetInputs(comps, rootDir)
	require.NoError(t, err)

	// `comp-b::test` declares no `inputs:` -> whole component.
	assert.Equal(t, []input.ID{"comp-b"}, targets[target.ID(tgtCompBTest)])
	require.Contains(t, inputs, input.ID("comp-b"))
	assert.Equal(t, []string{"^.*$"}, inputs["comp-b"].Patterns)
	assert.Equal(t, path.Join(rootDir, "comp-b"), inputs["comp-b"].BaseDir)
}
