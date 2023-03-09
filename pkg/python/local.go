package python

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"

	"github.com/datablast-analytics/blast-cli/pkg/executor"
	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/datablast-analytics/blast-cli/pkg/path"
	"github.com/datablast-analytics/blast-cli/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
	"golang.org/x/sync/errgroup"
)

type cmd interface {
	Run(ctx context.Context, repo *git.Repo, command *command) error
}

type installReqsToHomeDir struct {
	fs     afero.Fs
	config *user.ConfigManager
	cmd    cmd

	lock sync.Mutex
}

func (i *installReqsToHomeDir) EnsureVirtualEnvExists(ctx context.Context, repo *git.Repo, requirementsTxt string) (string, error) {
	relPath, err := filepath.Rel(repo.Path, requirementsTxt)
	if err != nil {
		return "", errors.Wrap(err, "failed to get relative path to the repo for requirements.txt")
	}

	err = i.config.EnsureVirtualenvDirExists()
	if err != nil {
		return "", err
	}

	sum := sha256.Sum256([]byte(relPath))
	venvPath := i.config.MakeVirtualenvPath(hex.EncodeToString(sum[:]))

	i.lock.Lock()
	defer i.lock.Unlock()

	reqsPathExists := path.DirExists(i.fs, venvPath)
	if reqsPathExists {
		return venvPath, nil
	}

	err = i.cmd.Run(ctx, repo, &command{
		Name: "python3",
		Args: []string{"-m", "venv", venvPath},
	})

	if err != nil {
		return "", err
	}

	return venvPath, nil
}

type requirementsInstaller interface {
	EnsureVirtualEnvExists(ctx context.Context, repo *git.Repo, requirementsTxt string) (string, error)
}

type localPythonRunner struct {
	cmd cmd

	requirementsInstaller requirementsInstaller
	fs                    afero.Fs
}

func (l *localPythonRunner) Run(ctx context.Context, repo *git.Repo, module, requirementsTxt string) error {
	var output io.Writer = os.Stdout
	if ctx.Value(executor.KeyPrinter) != nil {
		output = ctx.Value(executor.KeyPrinter).(io.Writer)
	}

	if requirementsTxt == "" {
		return l.cmd.Run(ctx, repo, &command{
			Name: "python3",
			Args: []string{"-u", "-m", module},
		})
	}

	_, err := output.Write([]byte("asset has dependencies, installing the packages to an isolated environment...\n"))
	if err != nil {
		return err
	}

	depsPath, err := l.requirementsInstaller.EnsureVirtualEnvExists(ctx, repo, requirementsTxt)
	if err != nil {
		return err
	}

	_, err = output.Write([]byte("asset dependencies are successfully installed, starting execution...\n"))
	if err != nil {
		return err
	}

	fullCommand := fmt.Sprintf("source %s/bin/activate && echo 'activated virtualenv' && pip3 install -r %s --quiet --quiet && echo 'installed all the dependencies' && python3 -u -m %s", depsPath, requirementsTxt, module)

	return l.cmd.Run(ctx, repo, &command{
		Name: "/bin/sh",
		Args: []string{"-c", fullCommand},
	})
}

type commandRunner struct{}

type command struct {
	Name string
	Args []string
}

func (l *commandRunner) Run(ctx context.Context, repo *git.Repo, command *command) error {
	// TODO: support secrets / env variables //nolint:godox

	cmd := exec.Command(command.Name, command.Args...) //nolint:gosec
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
		// the size of the slice here is important, the added 4 at the end includes the 3 bytes for the prefix and the 1 byte for the newline
		msg := make([]byte, len(scanner.Bytes())+4)
		copy(msg, ">> ")
		copy(msg[3:], scanner.Bytes())
		msg[len(msg)-1] = '\n'

		_, err := output.Write(msg)
		if err != nil {
			return err
		}
	}

	return nil
}
