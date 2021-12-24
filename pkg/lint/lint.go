package lint

import (
	"fmt"
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
	"os"
	"sort"
	"strings"
)

type pipelineFinder func(root, pipelineDefinitionFile string) ([]string, error)

type pipelineBuilder interface {
	CreatePipelineFromPath(pathToPipeline string) (*pipeline.Pipeline, error)
}

type Rule func(pipeline *pipeline.Pipeline) error

type Linter struct {
	findPipelines pipelineFinder
	builder       pipelineBuilder
	rules         []Rule
}

func NewLinter(findPipelines pipelineFinder, builder pipelineBuilder, rules []Rule) *Linter {
	return &Linter{
		findPipelines: findPipelines,
		builder:       builder,
		rules:         rules,
	}
}

func (l *Linter) Lint(rootPath, pipelineDefinitionFileName string) error {
	pipelinePaths, err := l.findPipelines(rootPath, pipelineDefinitionFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("the given pipeline path does not exist, please make sure you gave the right path")
		}

		return fmt.Errorf("error getting pipelinePath paths: %w", err)
	}

	if len(pipelinePaths) == 0 {
		return fmt.Errorf("no pipelines found in path '%s'", rootPath)
	}

	sort.Strings(pipelinePaths)

	err = ensureNoNestedPipelines(pipelinePaths)
	if err != nil {
		return err
	}

	pipelines := make([]*pipeline.Pipeline, len(pipelinePaths))
	for _, pipelinePath := range pipelinePaths {
		p, err := l.builder.CreatePipelineFromPath(pipelinePath)
		if err != nil {
			return errors.Wrapf(err, "error creating pipeline from path '%s'", pipelinePath)
		}
		pipelines = append(pipelines, p)
	}

	return l.lint(pipelines)
}

func (l *Linter) lint(pipelines []*pipeline.Pipeline) error {
	for _, p := range pipelines {
		for _, rule := range l.rules {
			err := rule(p)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func ensureNoNestedPipelines(pipelinePaths []string) error {
	var previousPath string
	for i, path := range pipelinePaths {
		if i != 0 && strings.HasPrefix(path, previousPath) {
			return fmt.Errorf("nested pipelines are not allowed: seems like '%s' is already a parent pipeline for '%s'", previousPath, path)
		}

		previousPath = path
	}

	return nil
}
