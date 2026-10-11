package regression

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"slopdb/db"
	"slopdb/storage"
)

// Each suite shares a directory only with its own reopened databases and files.
type regressionEnv struct {
	t                *testing.T
	root             string
	checks           int
	concurrentChecks atomic.Int64
}

func newRegressionEnv(t *testing.T) *regressionEnv {
	t.Helper()
	return &regressionEnv{t: t, root: t.TempDir()}
}

func (e *regressionEnv) path(name string) string {
	return filepath.Join(e.root, name)
}

func (e *regressionEnv) openDatabase(dir string, options *storage.Options) (*db.Database, error) {
	e.t.Helper()
	d, err := db.Open(dir, options)
	if err == nil {
		// Suites explicitly check successful Close calls. This fallback also
		// releases resources if a preceding assertion fails.
		e.t.Cleanup(func() { _ = d.Close() })
	}
	return d, err
}

func (e *regressionEnv) openStorage(dir string, options *storage.Options) (*storage.LSMTree, error) {
	e.t.Helper()
	l, err := storage.NewLSMTree(dir, options)
	if err == nil {
		e.t.Cleanup(func() { _ = l.Close() })
	}
	return l, err
}

var (
	cliDirectory string
	cliOnce      sync.Once
	cliPath      string
	cliErr       error
)

func TestMain(m *testing.M) {
	var err error
	cliDirectory, err = os.MkdirTemp("", "slopdb-regression-cli-")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	status := m.Run()
	if err := os.RemoveAll(cliDirectory); err != nil {
		fmt.Fprintln(os.Stderr, err)
		status = 1
	}
	os.Exit(status)
}

func repositoryRoot() string {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		panic("cannot locate repository")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(source), "../.."))
}

func (e *regressionEnv) cli() string {
	e.t.Helper()
	cliOnce.Do(func() {
		cliPath = filepath.Join(cliDirectory, "slopdb")
		args := []string{"build", "-o", cliPath}
		if raceEnabled {
			args = append(args, "-race")
		}
		args = append(args, ".")
		cmd := exec.Command("go", args...)
		cmd.Dir = repositoryRoot()
		if output, err := cmd.CombinedOutput(); err != nil {
			cliErr = fmt.Errorf("build CLI: %w\n%s", err, output)
		}
	})
	if cliErr != nil {
		e.t.Fatal(cliErr)
	}
	return cliPath
}
