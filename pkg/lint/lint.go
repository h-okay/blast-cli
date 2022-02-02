package lint

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type (
	pipelineFinder    func(root, pipelineDefinitionFile string) ([]string, error)
	PipelineValidator func(pipeline *pipeline.Pipeline) ([]*Issue, error)
)

type pipelineBuilder interface {
	CreatePipelineFromPath(pathToPipeline string) (*pipeline.Pipeline, error)
}

type Issue struct {
	Task        *pipeline.Task
	Description string
}

type Rule struct {
	Name        string
	Description string
	Checker     PipelineValidator
}

type Linter struct {
	findPipelines pipelineFinder
	builder       pipelineBuilder
	rules         []*Rule
	logger        *zap.SugaredLogger
}

func NewLinter(findPipelines pipelineFinder, builder pipelineBuilder, rules []*Rule, logger *zap.SugaredLogger) *Linter {
	return &Linter{
		findPipelines: findPipelines,
		builder:       builder,
		rules:         rules,
		logger:        logger,
	}
}

func (l *Linter) Lint(rootPath, pipelineDefinitionFileName string) (*PipelineAnalysisResult, error) {
	pipelinePaths, err := l.findPipelines(rootPath, pipelineDefinitionFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.New("the given pipeline path does not exist, please make sure you gave the right path")
		}

		return nil, fmt.Errorf("error getting pipelinePath paths: %w", err)
	}

	if len(pipelinePaths) == 0 {
		return nil, fmt.Errorf("no pipelines found in path '%s'", rootPath)
	}

	l.logger.Debugf("found %d pipelines", len(pipelinePaths))
	sort.Strings(pipelinePaths)

	err = ensureNoNestedPipelines(pipelinePaths)
	if err != nil {
		return nil, err
	}

	l.logger.Debug("no nested pipelines found, moving forward")
	pipelines := make([]*pipeline.Pipeline, 0, len(pipelinePaths))
	for _, pipelinePath := range pipelinePaths {
		l.logger.Debugf("creating pipeline from path '%s'", pipelinePath)

		p, err := l.builder.CreatePipelineFromPath(pipelinePath)
		if err != nil {
			return nil, errors.Wrapf(err, "error creating pipeline from path '%s'", pipelinePath)
		}
		pipelines = append(pipelines, p)
	}

	l.logger.Debugf("constructed %d pipelines", len(pipelines))

	return l.lint(pipelines)
}

type PipelineAnalysisResult struct {
	Pipelines []*PipelineIssues
}

// HasErrors returns true if any of the pipelines has errors.
func (p *PipelineAnalysisResult) HasErrors() bool {
	for _, pipelineIssues := range p.Pipelines {
		if len(pipelineIssues.Issues) > 0 {
			return true
		}
	}

	return false
}

type PipelineIssues struct {
	Pipeline *pipeline.Pipeline
	Issues   map[*Rule][]*Issue
}

func (l *Linter) lint(pipelines []*pipeline.Pipeline) (*PipelineAnalysisResult, error) {
	result := &PipelineAnalysisResult{}

	for _, p := range pipelines {
		pipelineResult := &PipelineIssues{
			Pipeline: p,
			Issues:   make(map[*Rule][]*Issue),
		}

		for _, rule := range l.rules {
			l.logger.Debugf("checking rule '%s' for pipeline '%s'", rule.Name, p.Name)

			issues, err := rule.Checker(p)
			if err != nil {
				return nil, err
			}

			if len(issues) > 0 {
				pipelineResult.Issues[rule] = issues
			}
		}

		result.Pipelines = append(result.Pipelines, pipelineResult)
	}

	return result, nil
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
