package executor

import (
	"context"
	"fmt"

	"github.com/datablast-analytics/blast-cli/pkg/scheduler"
	"go.uber.org/zap"
)

type Concurrent struct {
	workerCount int
	workers     []*worker
}

func NewConcurrent(
	logger *zap.SugaredLogger,
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
			logger:   logger,
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
	logger   *zap.SugaredLogger
}

func (w worker) run(taskChannel <-chan *scheduler.TaskInstance, results chan<- *scheduler.TaskExecutionResult) {
	for task := range taskChannel {
		w.logger.Debugf("[%s] Running task: %s", w.id, task.Task.Name)
		err := w.executor.RunSingleTask(context.Background(), task.Pipeline, task.Task)
		w.logger.Debugf("[%s] Completed task: %s", w.id, task.Task.Name)
		results <- &scheduler.TaskExecutionResult{
			Instance: task,
			Error:    err,
		}
	}
}
