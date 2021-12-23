package lint

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
)

type pipelineFinder func(root, pipelineDefinitionFile string) ([]string, error)

type Rule func(pipelinePath string) error

type Linter struct {
	findPipelines pipelineFinder
	rules         []Rule
}

func NewLinter(findPipelines pipelineFinder, rules []Rule) *Linter {
	return &Linter{
		findPipelines: findPipelines,
		rules:         rules,
	}
}

func (l *Linter) Lint(rootPath, pipelineDefinitionFileName string) error {
	pipelinePaths, err := l.findPipelines(rootPath, pipelineDefinitionFileName)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return errors.New("the given pipelinePath path does not exist, please make sure you gave the right path")
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

	for _, pipelinePath := range pipelinePaths {
		for _, r := range l.rules {
			err = r(pipelinePath)
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
