package executor

import (
	"context"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/pkg/errors"
)

type Operator interface {
	RunTask(ctx context.Context, p *pipeline.Pipeline, t *pipeline.Asset) error
}

type OperatorMap map[string]Operator

type Sequential struct {
	TaskTypeMap map[scheduler.TaskInstanceType]OperatorMap
}

func (s Sequential) RunSingleTask(ctx context.Context, pipeline *pipeline.Pipeline, instance scheduler.TaskInstance) error {
	task := instance.GetAsset()

	executors, ok := s.TaskTypeMap[instance.GetType()]
	if !ok {
		return errors.New("there is no executor configured for the asset class: " + task.Type)
	}

	// check if task type exists in map
	executor, ok := executors[task.Type]
	if !ok {
		return errors.New("there is no executor configured for the task type, task cannot be run: " + task.Type)
	}

	if instance.GetType() == scheduler.TaskInstanceTypeMain {
		return executor.RunTask(ctx, pipeline, task)
	}

	return nil
}
