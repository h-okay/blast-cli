package scheduler

import (
	"context"
	"sync"

	"github.com/datablast-analytics/blast-cli/pkg/pipeline"
	"go.uber.org/zap"
)

type TaskInstanceStatus int

const (
	Pending TaskInstanceStatus = iota
	Queued
	Running
	Failed
	Succeeded
)

type TaskInstance struct {
	Pipeline *pipeline.Pipeline
	Task     *pipeline.Task
	status   TaskInstanceStatus
}

func (t *TaskInstance) Completed() bool {
	return t.status == Failed || t.status == Succeeded
}

func (t *TaskInstance) MarkAs(status TaskInstanceStatus) {
	t.status = status
}

type TaskExecutionResult struct {
	Instance *TaskInstance
	Error    error
}

type Scheduler struct {
	logger *zap.SugaredLogger

	taskInstances    []*TaskInstance
	taskScheduleLock sync.Mutex
	taskNameMap      map[string]*TaskInstance

	WorkQueue chan *TaskInstance
	Results   chan *TaskExecutionResult
}

func NewScheduler(logger *zap.SugaredLogger, p *pipeline.Pipeline) *Scheduler {
	instances := make([]*TaskInstance, 0, len(p.Tasks))
	for _, task := range p.Tasks {
		instances = append(instances, &TaskInstance{
			Pipeline: p,
			Task:     task,
			status:   Pending,
		})
	}

	return &Scheduler{
		logger:           logger,
		taskInstances:    instances,
		taskScheduleLock: sync.Mutex{},
		WorkQueue:        make(chan *TaskInstance, 100),
		Results:          make(chan *TaskExecutionResult),
	}
}

func (s *Scheduler) Run(ctx context.Context, wg *sync.WaitGroup) {
	go func() {
		s.Tick(&TaskExecutionResult{
			Instance: &TaskInstance{
				Task: &pipeline.Task{
					Name: "start",
				},
				status: Succeeded,
			},
		})
		s.logger.Debug("initiated the scheduler start task")
	}()

	s.logger.Debug("started the scheduler loop")
	for {
		select {
		case <-ctx.Done():
			wg.Done()
			close(s.WorkQueue)
			return
		case result := <-s.Results:
			s.logger.Debug("received task result: ", result.Instance.Task.Name)
			finished := s.Tick(result)
			if finished {
				s.logger.Debug("pipeline has completed, finishing the scheduler loop")
				wg.Done()
				return
			}
		}
	}
}

// Tick marks an iteration of the scheduler loop. It is called when a result is received.
// Mainly, the results are fed from a channel, but Tick allows implementing additional methods of passing
// Task results and simulating scheduler loops, e.g. time travel. It is also useful for testing purposes.
func (s *Scheduler) Tick(result *TaskExecutionResult) bool {
	s.taskScheduleLock.Lock()
	defer s.taskScheduleLock.Unlock()

	result.Instance.MarkAs(Succeeded)

	if s.hasPipelineFinished() {
		close(s.WorkQueue)
		return true
	}

	tasks := s.getScheduleableTasks()
	if len(tasks) == 0 {
		return false
	}

	for _, task := range tasks {
		task.MarkAs(Queued)
		s.WorkQueue <- task
	}

	return false
}

func (s *Scheduler) constructTaskNameMap() {
	s.taskNameMap = make(map[string]*TaskInstance)
	for _, ti := range s.taskInstances {
		s.taskNameMap[ti.Task.Name] = ti
	}
}

func (s *Scheduler) getScheduleableTasks() []*TaskInstance {
	if s.taskNameMap == nil {
		s.constructTaskNameMap()
	}

	tasks := make([]*TaskInstance, 0)
	for _, task := range s.taskInstances {
		if task.status != Pending {
			continue
		}

		if !s.allDependenciesSucceededForTask(task) {
			continue
		}

		tasks = append(tasks, task)
	}

	return tasks
}

func (s *Scheduler) allDependenciesSucceededForTask(t *TaskInstance) bool {
	if len(t.Task.DependsOn) == 0 {
		return true
	}

	for _, dep := range t.Task.DependsOn {
		upstream, ok := s.taskNameMap[dep]
		if !ok {
			continue
		}

		if upstream.status == Pending || upstream.status == Queued || upstream.status == Running {
			return false
		}
	}

	return true
}

func (s *Scheduler) hasPipelineFinished() bool {
	for _, task := range s.taskInstances {
		if !task.Completed() {
			return false
		}
	}

	return true
}
