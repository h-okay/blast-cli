package executor

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/datablast-analytics/blast-cli/pkg/scheduler"
	"github.com/fatih/color"
	"go.uber.org/zap"
)

var (
	randomColors = []color.Attribute{
		color.FgBlue,
		color.FgMagenta,
		color.FgCyan,
		color.FgWhite,
	}
	faint = color.New(color.Faint).SprintFunc()
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

	var printLock sync.Mutex

	workers := make([]*worker, workerCount)
	for i := 0; i < workerCount; i++ {
		workers[i] = &worker{
			id:        fmt.Sprintf("worker-%d", i),
			executor:  executor,
			logger:    logger,
			printer:   color.New(randomColors[i%len(randomColors)]),
			printLock: &printLock,
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
	id        string
	executor  *Sequential
	logger    *zap.SugaredLogger
	printer   *color.Color
	printLock *sync.Mutex
}

func (w worker) run(taskChannel <-chan *scheduler.TaskInstance, results chan<- *scheduler.TaskExecutionResult) {
	for task := range taskChannel {
		w.printer.Printf("[%s] Running: %s\n", w.id, task.Task.Name)
		start := time.Now()
		err := w.executor.RunSingleTask(context.Background(), task.Pipeline, task.Task)

		duration := time.Since(start)
		durationString := fmt.Sprintf("(%s)", duration.Truncate(time.Millisecond).String())
		w.printLock.Lock()
		w.printer.Printf("[%s] Completed: %s %s\n", w.id, task.Task.Name, faint(durationString))
		w.printLock.Unlock()

		results <- &scheduler.TaskExecutionResult{
			Instance: task,
			Error:    err,
		}
	}
}
