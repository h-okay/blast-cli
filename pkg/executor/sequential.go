package executor

import (
	"context"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
)

type Operator interface {
	RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Task) error
}

type Sequential struct {
	TaskTypeMap map[string]Operator
}

func (s Sequential) RunSingleTask(ctx context.Context, pipeline *pipeline.Pipeline, task *pipeline.Task) error {
	// check if task type exists in map
	if _, ok := s.TaskTypeMap[task.Type]; !ok {
		return errors.New("there is no executor configured for the task type, task cannot be run: " + task.Type)
	}

	return s.TaskTypeMap[task.Type].RunTask(ctx, pipeline, task)
}

func (s Sequential) RunPipeline(ctx context.Context, pipeline *pipeline.Pipeline, task *pipeline.Task) error {
	// check if task type exists in map
	notRunnableTaskTypes := s.getNotRunnableTaskTypes(pipeline)
	if len(notRunnableTaskTypes) != 0 {
		return errors.Errorf("some of the task types are not runnable yet: %v", notRunnableTaskTypes)
	}

	return s.TaskTypeMap[task.Type].RunTask(ctx, pipeline, task)
}

func (s Sequential) getNotRunnableTaskTypes(pipeline *pipeline.Pipeline) []string {
	types := make([]string, 0)
	for _, task := range pipeline.Tasks {
		if _, ok := s.TaskTypeMap[task.Type]; !ok {
			types = append(types, task.Type)
		}
	}

	return types
}
