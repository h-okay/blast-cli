package python

import (
	"context"

	"github.com/datablast-analytics/blast-cli/pkg/git"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
)

type modulePathFinder interface {
	FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error)
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type localRunner interface {
	Run(ctx context.Context, repo *git.Repo, module string) error
}

type LocalOperator struct {
	repoFinder repoFinder
	module     modulePathFinder
	runner     localRunner
}

func NewLocalOperator() *LocalOperator {
	return &LocalOperator{
		repoFinder: &git.RepoFinder{},
		module:     &ModulePathFinder{},
		runner: &localCmdRunner{
			PythonExecutable: "python3",
		},
	}
}

func (o *LocalOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Task) error {
	repo, err := o.repoFinder.Repo(t.ExecutableFile.Path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Python")
	}

	module, err := o.module.FindModulePath(repo, &t.ExecutableFile)
	if err != nil {
		return errors.Wrap(err, "failed to build a module path")
	}

	err = o.runner.Run(ctx, repo, module)
	if err != nil {
		return errors.Wrap(err, "failed to execute Python script")
	}

	return nil
}
