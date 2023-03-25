package executor

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"github.com/datablast-analytics/blast/pkg/scheduler"
	"github.com/fatih/color"
	"go.uber.org/zap"
)

var (
	colors = []color.Attribute{
		color.FgBlue,
		color.FgMagenta,
		color.FgCyan,
		color.FgWhite,
		color.FgHiMagenta,
		color.FgHiBlue,
		color.FgHiCyan,
	}
	faint = color.New(color.Faint).SprintFunc()
)

type contextKey int

const (
	KeyPrinter contextKey = iota
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
			printer:   color.New(colors[i%len(colors)]),
			printLock: &printLock,
		}
	}

	return &Concurrent{
		workerCount: workerCount,
		workers:     workers,
	}
}

func (c Concurrent) Start(input chan scheduler.TaskInstance, result chan<- *scheduler.TaskExecutionResult) {
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

func (w worker) run(taskChannel <-chan scheduler.TaskInstance, results chan<- *scheduler.TaskExecutionResult) {
	for task := range taskChannel {
		w.printer.Printf("[%s] [%s] Running: %s\n", time.Now().Format(time.RFC3339), w.id, task.GetAsset().Name)
		start := time.Now()

		printer := &workerWriter{
			w:           os.Stdout,
			task:        task.GetAsset(),
			sprintfFunc: w.printer.SprintfFunc(),
			worker:      w.id,
		}

		ctx := context.WithValue(context.Background(), KeyPrinter, printer)
		err := w.executor.RunSingleTask(ctx, task.GetPipeline(), task)

		duration := time.Since(start)
		durationString := fmt.Sprintf("(%s)", duration.Truncate(time.Millisecond).String())
		w.printLock.Lock()
		w.printer.Printf("[%s] [%s] Completed: %s %s\n", time.Now().Format(time.RFC3339), w.id, task.GetAsset().Name, faint(durationString))
		w.printLock.Unlock()

		results <- &scheduler.TaskExecutionResult{
			Instance: task,
			Error:    err,
		}
	}
}

type workerWriter struct {
	w           io.Writer
	task        *pipeline.Asset
	sprintfFunc func(format string, a ...interface{}) string
	worker      string
}

func (w *workerWriter) Write(p []byte) (int, error) {
	formatted := w.sprintfFunc("[%s] [%s] [%s] %s", time.Now().Format(time.RFC3339), w.worker, w.task.Name, string(p))

	n, err := w.w.Write([]byte(formatted))
	if err != nil {
		return n, err
	}
	if n != len(formatted) {
		return n, io.ErrShortWrite
	}
	return len(p), nil
}
