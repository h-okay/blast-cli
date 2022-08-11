package executor

import (
	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"github.com/pkg/errors"
)

type operator interface {
	RunTask(p pipeline.Pipeline, t pipeline.Task) error
}

type Sequential struct {
	TaskTypeMap map[string]operator
}

func (l Sequential) RunSingleTask(pipeline pipeline.Pipeline, task pipeline.Task) error {
	// check if task type exists in map
	if _, ok := l.TaskTypeMap[task.Type]; !ok {
		return errors.New("there is no executor configured for the task type, task cannot be run: " + task.Type)
	}

	return l.TaskTypeMap[task.Type].RunTask(pipeline, task)
}
