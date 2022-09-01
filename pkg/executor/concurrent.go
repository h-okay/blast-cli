package executor

import (
	"context"
	"fmt"

	"github.com/datablast-analytics/blast-cli/pkg/scheduler"
)

type Concurrent struct {
	workerCount int
	workers     []*worker
}

func NewConcurrent(
	taskTypeMap map[string]Operator,
	workerCount int,
) *Concurrent {
	executor := &Sequential{
		TaskTypeMap: taskTypeMap,
	}

	workers := make([]*worker, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = &worker{
			id:       fmt.Sprintf("worker-%d", i),
			executor: executor,
		}
	}

	return &Concurrent{
		workerCount: workerCount,
		workers:     workers,
	}
}

func (c Concurrent) Start(input chan *scheduler.TaskInstance, result chan<- *scheduler.TaskExecutionResult) {
	for i := 0; i < c.workerCount; i++ {
		go c.workers[i].run(input, result)
	}
}

type worker struct {
	id       string
	executor *Sequential
}

func (w worker) run(taskChannel <-chan *scheduler.TaskInstance, results chan<- *scheduler.TaskExecutionResult) {
	for task := range taskChannel {
		err := w.executor.RunSingleTask(context.Background(), task.Pipeline, task.Task)
		results <- &scheduler.TaskExecutionResult{
			Instance: task,
			Error:    err,
		}
	}
}
