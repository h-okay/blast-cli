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
