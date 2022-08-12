package executor

import (
	"context"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
)

type operator interface {
	RunTask(ctx context.Context, p pipeline.Pipeline, t pipeline.Task) error
}

type Sequential struct {
	TaskTypeMap map[string]operator
}

func (l Sequential) RunSingleTask(ctx context.Context, pipeline pipeline.Pipeline, task pipeline.Task) error {
	// check if task type exists in map
	if _, ok := l.TaskTypeMap[task.Type]; !ok {
		return errors.New("there is no executor configured for the task type, task cannot be run: " + task.Type)
	}

	return l.TaskTypeMap[task.Type].RunTask(ctx, pipeline, task)
}
