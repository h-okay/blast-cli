package scheduler

import (
	"context"
	"sync"

	"github.com/datablast-analytics/blast/pkg/pipeline"
	"go.uber.org/zap"
)

type TaskInstanceStatus int

type TaskInstanceType int

const (
	Pending TaskInstanceStatus = iota
	Queued
	Running
	Failed
	UpstreamFailed
	Succeeded
)

const (
	TaskInstanceTypeMain TaskInstanceType = iota
	TaskInstanceTypeColumnTest
	TaskInstanceTypeCustomTest
)

type TaskInstance interface {
	GetPipeline() *pipeline.Pipeline
	GetAsset() *pipeline.Asset
	GetType() TaskInstanceType

	GetStatus() TaskInstanceStatus
	MarkAs(status TaskInstanceStatus)
	Completed() bool

	GetUpstream() []TaskInstance
	GetDownstream() []TaskInstance
	AddUpstream(t TaskInstance)
	AddDownstream(t TaskInstance)
}

type AssetInstance struct {
	Name     string
	Pipeline *pipeline.Pipeline
	Asset    *pipeline.Asset

	status     TaskInstanceStatus
	upstream   []TaskInstance
	downstream []TaskInstance
}

func (t *AssetInstance) GetStatus() TaskInstanceStatus {
	return t.status
}

func (t *AssetInstance) Completed() bool {
	return t.status == Failed || t.status == Succeeded || t.status == UpstreamFailed
}

func (t *AssetInstance) MarkAs(status TaskInstanceStatus) {
	t.status = status
}

func (t *AssetInstance) GetPipeline() *pipeline.Pipeline {
	return t.Pipeline
}

func (t *AssetInstance) GetAsset() *pipeline.Asset {
	return t.Asset
}

func (t *AssetInstance) GetType() TaskInstanceType {
	return TaskInstanceTypeMain
}

func (t *AssetInstance) GetUpstream() []TaskInstance {
	return t.upstream
}

func (t *AssetInstance) GetDownstream() []TaskInstance {
	return t.downstream
}

func (t *AssetInstance) AddUpstream(task TaskInstance) {
	t.upstream = append(t.upstream, task)
}

func (t *AssetInstance) AddDownstream(task TaskInstance) {
	t.downstream = append(t.downstream, task)
}

type ColumnTestInstance struct {
	*AssetInstance

	Column *pipeline.Column
	Test   *pipeline.ColumnTest
}

type TaskExecutionResult struct {
	Instance TaskInstance
	Error    error
}

type Scheduler struct {
	logger *zap.SugaredLogger

	taskInstances    []TaskInstance
	taskScheduleLock sync.Mutex
	taskNameMap      map[string]TaskInstance

	WorkQueue chan TaskInstance
	Results   chan *TaskExecutionResult
}

func (s *Scheduler) MarkAll(status TaskInstanceStatus) {
	for _, instance := range s.taskInstances {
		instance.MarkAs(status)
	}
}

func (s *Scheduler) MarkTask(task *pipeline.Asset, status TaskInstanceStatus, downstream bool) {
	instance := s.taskNameMap[task.Name]
	s.MarkTaskInstance(instance, status, downstream)
}

func (s *Scheduler) MarkTaskInstance(instance TaskInstance, status TaskInstanceStatus, downstream bool) {
	instance.MarkAs(status)
	if !downstream {
		return
	}

	downstreams := instance.GetDownstream()
	if len(downstreams) == 0 {
		return
	}

	for _, d := range downstreams {
		s.MarkTaskInstance(d, status, downstream)
	}
}

func (s *Scheduler) markTaskInstanceFailedWithDownstream(instance TaskInstance) {
	s.MarkTaskInstance(instance, UpstreamFailed, true)
	s.MarkTaskInstance(instance, Failed, false)
}

func (s *Scheduler) GetTaskInstancesByStatus(status TaskInstanceStatus) []TaskInstance {
	instances := make([]TaskInstance, 0)
	for _, i := range s.taskInstances {
		if i.GetStatus() != status {
			continue
		}

		instances = append(instances, i)
	}

	return instances
}

func (s *Scheduler) WillRunTaskOfType(taskType string) bool {
	instances := s.GetTaskInstancesByStatus(Pending)
	for _, instance := range instances {
		if instance.GetAsset().Type == taskType {
			return true
		}
	}

	return false
}

func NewScheduler(logger *zap.SugaredLogger, p *pipeline.Pipeline) *Scheduler {
	instances := make([]TaskInstance, 0, len(p.Tasks))
	for _, task := range p.Tasks {
		instances = append(instances, &AssetInstance{
			Pipeline:   p,
			Asset:      task,
			status:     Pending,
			upstream:   make([]TaskInstance, 0),
			downstream: make([]TaskInstance, 0),
		})

		for _, column := range task.Columns {
			col := column
			for _, test := range column.Tests {
				t := test
				instances = append(instances, ColumnTestInstance{
					AssetInstance: &AssetInstance{
						Pipeline:   p,
						Asset:      task,
						status:     Pending,
						upstream:   make([]TaskInstance, 0),
						downstream: make([]TaskInstance, 0),
					},
					Column: &col,
					Test:   &t,
				})
			}
		}
	}

	s := &Scheduler{
		logger:           logger,
		taskInstances:    instances,
		taskScheduleLock: sync.Mutex{},
		WorkQueue:        make(chan TaskInstance, 100),
		Results:          make(chan *TaskExecutionResult),
	}
	s.constructTaskNameMap()
	s.constructInstanceRelationships()

	return s
}

func (s *Scheduler) constructTaskNameMap() {
	s.taskNameMap = make(map[string]TaskInstance)
	for _, ti := range s.taskInstances {
		s.taskNameMap[ti.GetAsset().Name] = ti
	}
}

func (s *Scheduler) constructInstanceRelationships() {
	for _, ti := range s.taskInstances {
		for _, dep := range ti.GetAsset().DependsOn {
			upstream, ok := s.taskNameMap[dep]
			if !ok {
				continue
			}

			ti.AddUpstream(upstream)
			upstream.AddDownstream(ti)
		}
	}
}

func (s *Scheduler) Run(ctx context.Context) []*TaskExecutionResult {
	go s.Kickstart()

	results := make([]*TaskExecutionResult, 0)

	s.logger.Debug("started the scheduler loop")
	for {
		select {
		case <-ctx.Done():
			close(s.WorkQueue)
			return results
		case result := <-s.Results:
			s.logger.Debug("received task result: ", result.Instance.GetAsset().Name)
			results = append(results, result)
			finished := s.Tick(result)
			if finished {
				s.logger.Debug("pipeline has completed, finishing the scheduler loop")
				return results
			}
		}
	}
}

// Tick marks an iteration of the scheduler loop. It is called when a result is received.
// The results are mainly fed from a channel, but Tick allows implementing additional methods of passing
// Asset results and simulating scheduler loops, e.g. time travel. It is also useful for testing purposes.
func (s *Scheduler) Tick(result *TaskExecutionResult) bool {
	s.taskScheduleLock.Lock()
	defer s.taskScheduleLock.Unlock()

	s.MarkTaskInstance(result.Instance, Succeeded, false)
	if result.Error != nil {
		s.markTaskInstanceFailedWithDownstream(result.Instance)
	}

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

// Kickstart initiates the scheduler process by sending a "start" task for the processing.
func (s *Scheduler) Kickstart() {
	s.Tick(&TaskExecutionResult{
		Instance: &AssetInstance{
			Asset: &pipeline.Asset{
				Name: "start",
			},
			status: Succeeded,
		},
	})
}

func (s *Scheduler) getScheduleableTasks() []TaskInstance {
	if s.taskNameMap == nil {
		s.constructTaskNameMap()
	}

	tasks := make([]TaskInstance, 0)
	for _, task := range s.taskInstances {
		if task.GetStatus() != Pending {
			continue
		}

		if !s.allDependenciesSucceededForTask(task) {
			continue
		}

		tasks = append(tasks, task)
	}

	return tasks
}

func (s *Scheduler) allDependenciesSucceededForTask(t TaskInstance) bool {
	if len(t.GetAsset().DependsOn) == 0 {
		return true
	}

	for _, dep := range t.GetAsset().DependsOn {
		upstream, ok := s.taskNameMap[dep]
		if !ok {
			continue
		}

		status := upstream.GetStatus()
		if status == Pending || status == Queued || status == Running {
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
