package python

import (
	"bufio"
	"context"
	"io"
	"os"
	"os/exec"

	"github.com/datablast-analytics/blast-cli/pkg/executor"
	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/pkg/errors"
)

type localCmdRunner struct {
	PythonExecutable string
}

func (l *localCmdRunner) Run(ctx context.Context, repo *git.Repo, module string) error {
	// TODO: support changing the python executable path //nolint:godox
	// TODO: support secrets / env variables //nolint:godox
	// TODO: support dependencies //nolint:godox

	cmd := exec.Command(l.PythonExecutable, "-u", "-m", module) //nolint:gosec
	cmd.Dir = repo.Path

	var output io.Writer = os.Stdout
	if ctx.Value(executor.KeyPrinter) != nil {
		output = ctx.Value(executor.KeyPrinter).(io.Writer)
	}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return errors.Wrap(err, "failed to get stdout")
	}

	stderr, err := cmd.StderrPipe()
	if err != nil {
		return errors.Wrap(err, "failed to get stdout")
	}

	go consumePipe(stdout, output)
	go consumePipe(stderr, output)

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "failed to start command")
	}

	return cmd.Wait()
}

func consumePipe(pipe io.Reader, output io.Writer) {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		output.Write(append(scanner.Bytes(), '\n'))
	}
}
