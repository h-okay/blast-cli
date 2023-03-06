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
	"golang.org/x/sync/errgroup"
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

	wg := new(errgroup.Group)
	wg.Go(func() error { return consumePipe(stdout, output) })
	wg.Go(func() error { return consumePipe(stderr, output) })

	err = cmd.Start()
	if err != nil {
		return errors.Wrap(err, "failed to start command")
	}

	res := cmd.Wait()
	if res != nil {
		return res
	}

	err = wg.Wait()
	if err != nil {
		return errors.Wrap(err, "failed to consume pipe")
	}

	return nil
}

func consumePipe(pipe io.Reader, output io.Writer) error {
	scanner := bufio.NewScanner(pipe)
	for scanner.Scan() {
		_, err := output.Write(append(scanner.Bytes(), '\n'))
		if err != nil {
			return err
		}
	}

	return nil
}
