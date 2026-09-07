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

// startWatcher runs `server serve` as a background process against the `repo`
// fixture and returns the config overrides every client needs.
func startWatcher(t *testing.T) []string {
	t.Helper()

	dir := t.TempDir()
	address := "unix://" + path.Join(dir, "w.sock")

	configValues := []string{
		"-K", "watcher.address: " + address,
		"-K", "watcher.stateFile: " + path.Join(dir, "state.json"),
		"-K", "watcher.scanInterval: 100ms",
		"-K", "watcher.timeout: 20s",
	}

	binDir := os.Getenv("QUITSH_BIN_DIR")
	require.DirExists(t, binDir)

	args := append([]string{"--root-dir", "repo"}, configValues...)
	args = append(args, "server", "serve")

	cmd := exec.Command(
		path.Join(binDir, "quitsh-integration-test"),
		args...) //nolint:gosec // test.
	cmd.Env = serverEnv()
	require.NoError(t, cmd.Start())

	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	})

	// Wait until the socket answers.
	cli := setup(t).Build()
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		probe := append(append([]string{}, configValues...), "server", "status", "--stale-ok")
		if _, _, err := cli.GetStdErr(probe...); err == nil {
			return configValues
		}

		time.Sleep(200 * time.Millisecond)
	}

	t.Fatal("watcher server did not come up")

	return nil
}

// buildTarget runs `component-a::build` and returns the combined log output.
func buildTarget(t *testing.T, configValues []string, extra ...string) string {
	t.Helper()

	cli := setup(t).Build()

	args := append([]string{}, configValues...)
	args = append(args, "exec-target", "--tag", "do-echo", "--log-level", "debug")
	args = append(args, extra...)
	args = append(args, "component-a::build")

	_, stderr, err := cli.GetStdErr(args...)
	require.NoError(t, err, "Stderr:\n"+stderr)

	return stderr
}

func TestWatcherSkipsUnchangedTarget(t *testing.T) {
	configValues := startWatcher(t)

	first := buildTarget(t, configValues)
	assert.Contains(t, first, "Hello from integration test Go build runner",
		"a target which was never built must run")

	second := buildTarget(t, configValues)
	assert.NotContains(t, second, "Hello from integration test Go build runner",
		"an unchanged target must be skipped on the second run")
	assert.Contains(t, second, "up to date")
}

func TestWatcherRebuildsAfterEdit(t *testing.T) {
	configValues := startWatcher(t)

	buildTarget(t, configValues)
	require.Contains(t, buildTarget(t, configValues), "up to date")

	touched := path.Join("repo", "component-a", "watcher-touch.txt")
	require.NoError(t, os.WriteFile(touched, []byte("changed"), 0o600))

	t.Cleanup(func() { _ = os.Remove(touched) })

	out := buildTarget(t, configValues)
	assert.Contains(t, out, "Hello from integration test Go build runner",
		"editing a source file must invalidate the target")
}

func TestWatcherNoSkipRunsEvenWhenUpToDate(t *testing.T) {
	configValues := startWatcher(t)

	buildTarget(t, configValues)
	require.Contains(t, buildTarget(t, configValues), "up to date")

	out := buildTarget(t, configValues, "--no-skip")
	assert.Contains(t, out, "Hello from integration test Go build runner",
		"'--no-skip' must run the target even though it is up to date")
}

func TestWatcherStatusListsDirtyTargets(t *testing.T) {
	configValues := startWatcher(t)

	cli := setup(t).Build()
	args := append(append([]string{}, configValues...), "server", "status", "--dirty-only")

	stdout, stderr, err := cli.GetStdErr(args...)
	require.NoError(t, err, "Stderr:\n"+stderr)

	assert.True(t, strings.Contains(stdout, "component-a::"),
		"targets of the fixture repository must be listed as dirty, got:\n"+stdout)
}

func TestWatcherAbsentMeansEverythingRuns(t *testing.T) {
	configValues := []string{
		"-K", "watcher.address: unix://" + path.Join(t.TempDir(), "absent.sock"),
		"-K", "watcher.timeout: 2s",
	}

	out := buildTarget(t, configValues)
	assert.Contains(t, out, "Hello from integration test Go build runner",
		"an absent watcher must never stop a build")
	assert.NotContains(t, out, "up to date")
}

// serverEnv returns the environment for the background watcher server.
// The config env variables are dropped, exactly as `setup` does for the
// foreground CLI: they point at the config of quitsh's own CLI, whose schema
// differs from the one of this integration test binary.
func serverEnv() []string {
	env := []string{"GOCOVERDIR=" + os.Getenv("QUITSH_COVERAGE_DIR")}

	for _, e := range os.Environ() {
		if strings.HasPrefix(e, "QUITSH_CONFIG=") ||
			strings.HasPrefix(e, "QUITSH_CONFIG_USER=") ||
			strings.HasPrefix(e, "GOCOVERDIR=") {
			continue
		}

		env = append(env, e)
	}

	return env
}
