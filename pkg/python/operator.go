package python

import (
	"context"

	"github.com/datablast-analytics/blast/pkg/git"
	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/datablast-analytics/blast/pkg/user"
	"github.com/pkg/errors"
	"github.com/spf13/afero"
)

type executionContext struct {
	repo            *git.Repo
	module          string
	requirementsTxt string

	envVariables map[string]string
	pipeline     *pipeline.Pipeline
	task         *pipeline.Asset
}

type modulePathFinder interface {
	FindModulePath(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error)
	FindRequirementsTxt(repo *git.Repo, executable *pipeline.ExecutableFile) (string, error)
}

type repoFinder interface {
	Repo(path string) (*git.Repo, error)
}

type localRunner interface {
	Run(ctx context.Context, execution *executionContext) error
}

type LocalOperator struct {
	repoFinder   repoFinder
	module       modulePathFinder
	runner       localRunner
	envVariables map[string]string
}

func NewLocalOperator(envVariables map[string]string) *LocalOperator {
	cmdRunner := &commandRunner{}
	fs := afero.NewOsFs()

	return &LocalOperator{
		repoFinder: &git.RepoFinder{},
		module:     &ModulePathFinder{},
		runner: &localPythonRunner{
			cmd: cmdRunner,
			requirementsInstaller: &installReqsToHomeDir{
				fs:     fs,
				cmd:    cmdRunner,
				config: user.NewConfigManager(fs),
			},
		},
		envVariables: envVariables,
	}
}

func (o *LocalOperator) Run(ctx context.Context, ti scheduler.TaskInstance) error {
	_, ok := ti.(*scheduler.AssetInstance)
	if !ok {
		return errors.New("python assets can only be run as a main task")
	}

	return o.RunTask(ctx, ti.GetPipeline(), ti.GetAsset())
}

func (o *LocalOperator) RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error {
	repo, err := o.repoFinder.Repo(t.ExecutableFile.Path)
	if err != nil {
		return errors.Wrap(err, "failed to find repo to run Python")
	}

	module, err := o.module.FindModulePath(repo, &t.ExecutableFile)
	if err != nil {
		return errors.Wrap(err, "failed to build a module path")
	}

	requirementsTxt, err := o.module.FindRequirementsTxt(repo, &t.ExecutableFile)
	if err != nil {
		var noReqsError *NoRequirementsFoundError
		switch {
		case !errors.As(err, &noReqsError):
			return errors.Wrap(err, "failed to find requirements.txt")
		default:
			//
		}
	}

	err = o.runner.Run(ctx, &executionContext{
		repo:            repo,
		module:          module,
		requirementsTxt: requirementsTxt,
		pipeline:        p,
		task:            t,
		envVariables:    o.envVariables,
	})
	if err != nil {
		return errors.Wrap(err, "failed to execute Python script")
	}

	return nil
}
